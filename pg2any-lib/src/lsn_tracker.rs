//! Thread-safe LSN tracking for CDC replication
//!
//! This module provides LSN tracking for CDC replication with file persistence:
//!
//! 1. **SharedLsnFeedback**: Re-exported from `pg_walstream` - Thread-safe tracking
//!    of LSN positions for replication feedback to PostgreSQL
//!
//! 2. **LsnTracker**: Thread-safe tracker for the last committed LSN with file persistence
//!    for graceful shutdown and restart recovery
//!
//! ## Simplified LSN Tracking
//!
//! This implementation uses a simplified approach with a single flush_lsn value:
//!
//! - `flush_lsn`: Last WAL location successfully committed to destination database
//!   - Updated by consumer when transactions are successfully committed
//!   - Serves as the start_lsn for recovery on restart
//!   - Represents actual application of changes to the destination
//!
//! ## pg2any LSN Tracking Implementation
//!
//! ### File-Based Workflow
//! - **Consumer**: Updates `flush_lsn` when transaction is successfully executed and committed
//!   - This happens in `client.rs` after successful `execute_sql_batch()` and `delete_pending_transaction()`
//!   - Represents that changes have been applied to the destination database
//!   - This LSN is persisted and used as start_lsn on restart
//!
//! ## Monitoring Replication Progress
//!
//! You can monitor the replication progress using:
//! ```sql
//! SELECT
//!     application_name,
//!     state,
//!     sent_lsn,
//!     write_lsn,
//!     flush_lsn,
//!     replay_lsn
//! FROM pg_stat_replication
//! WHERE application_name = 'pg2any';
//! ```
//!
//! ## Optimization Strategy
//!
//! Instead of persisting on every commit (which causes excessive I/O), the LsnTracker:
//! - Batches multiple commits and persists periodically (default: every 1 second)
//! - Uses a background tokio task for non-blocking persistence
//! - Tracks a "dirty" flag to skip unnecessary writes
//! - Ensures graceful shutdown with final persistence

use crate::types::Lsn;
use chrono::{DateTime, Utc};
use pg_walstream::format_lsn;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

// Re-export SharedLsnFeedback from pg_walstream to avoid duplication
pub use pg_walstream::SharedLsnFeedback;

/// Metadata format version for the LSN persistence file
const METADATA_VERSION: &str = "1.0";

/// CDC replication metadata stored in the persistence file
///
/// This structure stores comprehensive state information for recovery:
/// - LSN tracking for all three replication positions (write, flush, replay)
/// - Consumer execution state for resuming from correct position
/// - Timestamp information for monitoring and debugging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcMetadata {
    /// Metadata format version (for future compatibility)
    pub version: String,

    /// Timestamp when metadata was last updated
    pub last_updated: DateTime<Utc>,

    /// LSN tracking for PostgreSQL replication protocol
    pub lsn_tracking: LsnTracking,

    /// Consumer execution state
    pub consumer_state: ConsumerState,
}

/// LSN position for CDC replication tracking
///
/// Simplified to track only the last committed LSN (flush_lsn) which serves as start_lsn for recovery.
/// This LSN is updated when transactions are successfully committed to the destination database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LsnTracking {
    /// Last WAL location successfully committed to destination database
    /// This serves as the start_lsn for recovery on restart
    pub flush_lsn: u64,
}

/// Consumer execution state for recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerState {
    /// Last successfully processed transaction ID
    pub last_processed_tx_id: Option<u32>,

    /// Timestamp of last processed transaction
    pub last_processed_timestamp: Option<DateTime<Utc>>,

    /// Number of pending transaction files at last update
    pub pending_file_count: usize,

    /// Path to the transaction file currently being processed (metadata file path in sql_pending_tx/)
    /// This allows resuming from the exact file on restart
    pub current_file_path: Option<String>,

    /// Index of the last successfully executed SQL command within the current transaction file
    /// Commands are 0-indexed. None means no commands have been executed yet.
    /// This allows resuming from the exact position within a large transaction file.
    pub last_executed_command_index: Option<usize>,
}

impl Default for CdcMetadata {
    fn default() -> Self {
        Self {
            version: METADATA_VERSION.to_string(),
            last_updated: Utc::now(),
            lsn_tracking: LsnTracking { flush_lsn: 0 },
            consumer_state: ConsumerState {
                last_processed_tx_id: None,
                last_processed_timestamp: None,
                pending_file_count: 0,
                current_file_path: None,
                last_executed_command_index: None,
            },
        }
    }
}

impl CdcMetadata {
    /// Create new metadata with current timestamp
    pub fn new() -> Self {
        Self::default()
    }

    /// Update flush LSN (transaction committed to destination)
    pub fn update_flush_lsn(&mut self, lsn: u64) {
        if lsn > self.lsn_tracking.flush_lsn {
            self.lsn_tracking.flush_lsn = lsn;
            self.last_updated = Utc::now();
        }
    }

    /// Update consumer state after successful transaction execution
    pub fn update_consumer_state(
        &mut self,
        tx_id: u32,
        timestamp: DateTime<Utc>,
        pending_count: usize,
    ) {
        self.consumer_state.last_processed_tx_id = Some(tx_id);
        self.consumer_state.last_processed_timestamp = Some(timestamp);
        self.consumer_state.pending_file_count = pending_count;
        self.last_updated = Utc::now();
    }

    /// Update consumer position within a transaction file
    /// This tracks the exact command index being processed for fine-grained recovery
    pub fn update_consumer_position(
        &mut self,
        file_path: String,
        last_executed_command_index: usize,
    ) {
        self.consumer_state.current_file_path = Some(file_path);
        self.consumer_state.last_executed_command_index = Some(last_executed_command_index);
        self.last_updated = Utc::now();
    }

    /// Clear consumer position when a file is fully processed
    /// This resets the position tracking for the next transaction file
    pub fn clear_consumer_position(&mut self) {
        self.consumer_state.current_file_path = None;
        self.consumer_state.last_executed_command_index = None;
        self.last_updated = Utc::now();
    }

    /// Get the resume position for recovery
    /// Returns (file_path, start_command_index) if there's a partially processed file
    pub fn get_resume_position(&self) -> Option<(String, usize)> {
        if let Some(ref file_path) = self.consumer_state.current_file_path {
            // Resume from the next command after the last executed one
            let start_index = self
                .consumer_state
                .last_executed_command_index
                .map(|idx| idx + 1)
                .unwrap_or(0);
            Some((file_path.clone(), start_index))
        } else {
            None
        }
    }

    /// Get the flush LSN (last committed to destination)
    pub fn get_lsn(&self) -> u64 {
        self.lsn_tracking.flush_lsn
    }
}

/// Thread-safe tracker for the last committed LSN with file persistence
///
/// This tracker maintains simplified CDC replication metadata including:
/// - flush_lsn for tracking last committed position
/// - Consumer execution state for recovery
/// - Timestamp information for monitoring
///
/// ## Optimization Strategy
///
/// Instead of persisting on every commit (which causes excessive I/O), this tracker:
/// - Batches multiple commits and persists periodically (default: every 1 second)
/// - Uses a background tokio task for non-blocking persistence
/// - Tracks a "dirty" flag to skip unnecessary writes
/// - Ensures graceful shutdown with final persistence
///
/// ## Key Design Principle
///
/// The LSN tracked here represents what was actually **committed to the destination**
/// (e.g., MySQL), NOT what was received from the PostgreSQL replication stream.
/// This distinction is critical for graceful shutdown during large transactions:
///
/// - If shutdown occurs mid-transaction, only the LSN of the last *committed* batch is saved
/// - On restart, replication resumes from the last committed point, preventing data loss
/// - Incomplete transactions are replayed from their last committed batch position
///
/// ## Metadata Format
///
/// The persistence file uses JSON format for comprehensive state tracking:
/// ```json
/// {
///   "version": "1.0",
///   "last_updated": "2025-12-28T10:30:45Z",
///   "lsn_tracking": {
///     "flush_lsn": "0/2E00000"
///   },
///   "consumer_state": {
///     "last_processed_tx_id": 12345,
///     "last_processed_timestamp": "2025-12-28T10:30:40Z",
///     "pending_file_count": 3
///   }
/// }
/// ```
#[derive(Debug)]
pub struct LsnTracker {
    /// The comprehensive CDC metadata
    metadata: Arc<Mutex<CdcMetadata>>,
    /// Last persisted replay LSN (to avoid unnecessary writes)
    last_persisted_lsn: AtomicU64,
    /// Flag indicating if metadata needs to be persisted
    dirty: AtomicBool,
    /// Path to the metadata persistence file
    lsn_file_path: String,
    /// Persistence interval in milliseconds
    interval_ms: u64,
    /// Cancellation token for background task
    shutdown_token: CancellationToken,
    /// Handle to the background persistence task
    background_task: Arc<Mutex<JoinHandle<()>>>,
}

impl LsnTracker {
    /// Default persistence interval in milliseconds (1 second)
    pub const DEFAULT_PERSIST_INTERVAL_MS: u64 = 1000;

    /// Create a new LSN tracker with the specified file path
    pub fn new(lsn_file_path: Option<&str>) -> Arc<Self> {
        Self::new_with_interval(lsn_file_path, Self::DEFAULT_PERSIST_INTERVAL_MS)
    }

    /// Create a new LSN tracker with custom persistence interval
    /// The background persistence task is started automatically and will run
    /// until the provided shutdown_token is cancelled or the tracker is dropped.
    ///
    /// # Arguments
    /// * `lsn_file_path` - Optional path to the LSN metadata file
    /// * `interval_ms` - Persistence interval in milliseconds
    pub fn new_with_interval(lsn_file_path: Option<&str>, interval_ms: u64) -> Arc<Self> {
        let mut path = lsn_file_path
            .map(String::from)
            .or_else(|| std::env::var("CDC_LAST_LSN_FILE").ok())
            .unwrap_or_else(|| "./pg2any_last_lsn.metadata".to_string());

        // Ensure path has .metadata extension
        if !path.ends_with(".metadata") {
            path.push_str(".metadata");
        }

        // Create parent directory if it doesn't exist
        if let Some(parent) = std::path::Path::new(&path).parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    warn!("Failed to create directory for LSN file {}: {}", path, e);
                } else {
                    info!("Created directory for LSN metadata: {:?}", parent);
                }
            }
        }

        let interval = Duration::from_millis(interval_ms);
        let shutdown_token = CancellationToken::new();
        // Use Arc::new_cyclic to create the tracker and spawn the background task with a reference to the tracker in a single step
        let tracker = Arc::new_cyclic(|weak_tracker: &std::sync::Weak<Self>| {
            let shutdown_token_clone = shutdown_token.clone();

            // Spawn the background task with a weak reference
            let weak_tracker_clone = weak_tracker.clone();
            let handle = tokio::spawn(async move {
                info!(
                    "Background LSN persistence task started (interval: {:?})",
                    interval
                );

                loop {
                    tokio::select! {
                        biased;

                        // Shutdown signal
                        _ = shutdown_token_clone.cancelled() => {
                            info!("LSN Tracker background persistence task received shutdown signal");
                            break;
                        }

                        // Periodic persistence
                        _ = tokio::time::sleep(interval) => {
                            if let Some(tracker) = weak_tracker_clone.upgrade() {
                                if tracker.dirty.load(Ordering::Acquire) {
                                    if let Err(e) = tracker.persist_async().await {
                                        warn!("Background persistence failed: {}", e);
                                    }
                                }
                            }
                        }
                    }
                }

                // Final persist on shutdown - force to ensure consumer position is saved
                if let Some(tracker) = weak_tracker_clone.upgrade() {
                    info!("Background task: Performing final forced persistence on shutdown");

                    // Log state before persist
                    let metadata = tracker.get_metadata();
                    info!(
                        "Final state before persist - flush_lsn={}, pending_files={}, current_file={:?}",
                        format_lsn(metadata.lsn_tracking.flush_lsn),
                        metadata.consumer_state.pending_file_count,
                        metadata.consumer_state.current_file_path
                    );

                    // Force persist to save consumer position even if LSN unchanged
                    if let Err(e) = tracker.persist_async().await {
                        warn!("Final forced persistence on shutdown failed: {}", e);
                    } else {
                        let final_lsn = metadata.get_lsn();
                        if final_lsn > 0 {
                            info!("Final LSN persisted on shutdown: {}", format_lsn(final_lsn));
                        }
                    }
                }

                debug!("Background LSN persistence task stopped");
            });

            // Create the tracker with the actual handle
            Self {
                metadata: Arc::new(Mutex::new(CdcMetadata::default())),
                last_persisted_lsn: AtomicU64::new(0),
                dirty: AtomicBool::new(false),
                lsn_file_path: path,
                interval_ms,
                shutdown_token,
                background_task: Arc::new(Mutex::new(handle)),
            }
        });

        info!(
            "Started background LSN persistence (interval: {}ms)",
            interval_ms
        );

        tracker
    }

    /// Create a new LSN tracker with custom interval and load the initial value from file
    ///
    /// The background persistence task is started automatically.
    ///
    /// # Arguments
    /// * `lsn_file_path` - Optional path to the LSN metadata file
    /// * `interval_ms` - Persistence interval in milliseconds
    pub async fn new_with_load_and_interval(
        lsn_file_path: Option<&str>,
        interval_ms: u64,
    ) -> (Arc<Self>, Option<Lsn>) {
        let tracker = Self::new_with_interval(lsn_file_path, interval_ms);

        let loaded_metadata = tracker.load_from_file().await;

        // Initialize the metadata with loaded values
        if let Some(metadata) = loaded_metadata {
            let mut current_metadata = tracker.metadata.lock().unwrap();
            *current_metadata = metadata.clone();
            drop(current_metadata);

            let flush_lsn = metadata.get_lsn();
            tracker
                .last_persisted_lsn
                .store(flush_lsn, Ordering::Release);

            let loaded_lsn = if flush_lsn > 0 {
                Some(Lsn(flush_lsn))
            } else {
                None
            };

            (tracker, loaded_lsn)
        } else {
            (tracker, None)
        }
    }

    /// Shutdown the background persistence task gracefully
    /// This ensures the final LSN is persisted before the task exits.
    /// Should be called before dropping the tracker.
    pub async fn shutdown_async(&self) {
        if self.shutdown_token.is_cancelled() {
            debug!("LSN tracker already shutting down");
            return;
        }

        info!("Shutting down LSN persistence task");

        // Signal the background task to shutdown
        self.shutdown_token.cancel();

        let handle = {
            let mut task_guard = self.background_task.lock().unwrap();
            std::mem::replace(&mut *task_guard, tokio::spawn(async {}))
        };

        debug!("Waiting for background persistence task to complete...");
        match handle.await {
            Ok(_) => info!("Background persistence task completed successfully"),
            Err(e) => {
                error!("Shutdown Async persist failed: {}", e);
            }
        }

        // Force an immediate persist BEFORE signaling shutdown
        // This ensures the latest state is written to disk even if the background task
        // doesn't get a chance to run its final persist
        if self.dirty.load(Ordering::Acquire) {
            if let Err(e) = self.persist_async().await {
                warn!("Failed to force persist before shutdown signal: {}", e);
            } else {
                info!("Pre-shutdown persist completed successfully");
            }
        }

        info!("LSN persistence task stopped gracefully");
    }

    /// Synchronous shutdown for use in Drop or non-async contexts
    /// Note: This cannot wait for async operations to complete.
    pub fn shutdown_sync(&self) {
        if self.shutdown_token.is_cancelled() {
            return;
        }

        info!("Initiating sync shutdown of LSN persistence");
        self.shutdown_token.cancel();

        // Perform synchronous final persist
        if let Err(e) = self.persist_sync() {
            warn!("Failed to persist LSN on sync shutdown: {}", e);
        }
    }

    /// Load metadata from the persistence file
    ///
    /// Returns `Some(CdcMetadata)` if valid JSON metadata was found and parsed, `None` otherwise.
    /// Logs warnings for parsing errors but doesn't fail.
    pub async fn load_from_file(&self) -> Option<CdcMetadata> {
        match tokio::fs::read_to_string(&self.lsn_file_path).await {
            Ok(contents) => {
                let s = contents.trim();
                if s.is_empty() {
                    info!(
                        "LSN metadata file {} is empty, starting from latest",
                        self.lsn_file_path
                    );
                    None
                } else {
                    // Parse as JSON metadata format
                    match serde_json::from_str::<CdcMetadata>(s) {
                        Ok(metadata) => {
                            info!(
                                "Loaded CDC metadata from {} (flush_lsn: {})",
                                self.lsn_file_path,
                                format_lsn(metadata.lsn_tracking.flush_lsn)
                            );

                            Some(metadata)
                        }
                        Err(e) => {
                            warn!(
                                "Failed to parse metadata from {}: {}. File must contain valid JSON.",
                                self.lsn_file_path, e
                            );
                            None
                        }
                    }
                }
            }
            Err(_) => {
                info!(
                    "No persisted metadata file found at {}, starting from latest",
                    self.lsn_file_path
                );
                None
            }
        }
    }

    /// Update the last committed LSN if the new value is greater
    ///
    /// This ensures monotonic LSN progression and prevents regression
    /// in case of out-of-order processing.
    #[inline]
    pub fn update_if_greater(&self, lsn: u64) {
        if lsn == 0 {
            return;
        }

        let mut metadata = self.metadata.lock().unwrap();
        if lsn > metadata.lsn_tracking.flush_lsn {
            metadata.update_flush_lsn(lsn);
            drop(metadata);
            self.dirty.store(true, Ordering::Release);
        }
    }

    /// Update and mark LSN for persistence
    /// This should be called after each successful transaction commit to the destination.
    /// It updates the in-memory LSN and marks it as dirty for background persistence.
    /// Note: With background persistence enabled, this does NOT immediately persist to disk.
    /// The background task handles periodic persistence to reduce I/O overhead.
    #[inline]
    pub fn commit_lsn(&self, lsn: u64) {
        self.update_if_greater(lsn);
    }

    /// Update consumer state after successful transaction execution
    pub fn update_consumer_state(
        &self,
        tx_id: u32,
        timestamp: DateTime<Utc>,
        pending_count: usize,
    ) {
        let mut metadata = self.metadata.lock().unwrap();
        metadata.update_consumer_state(tx_id, timestamp, pending_count);
        drop(metadata);
        self.dirty.store(true, Ordering::Release);
        debug!(
            "Updated consumer state: tx_id={}, pending_count={}",
            tx_id, pending_count
        );
    }

    /// Update consumer position within a transaction file
    /// Call this after each successful batch execution to enable fine-grained recovery
    pub fn update_consumer_position(&self, file_path: String, last_executed_command_index: usize) {
        let mut metadata = self.metadata.lock().unwrap();
        metadata.update_consumer_position(file_path.clone(), last_executed_command_index);
        drop(metadata);
        self.dirty.store(true, Ordering::Release);
        debug!(
            "Updated consumer position: file={}, command_index={}",
            file_path, last_executed_command_index
        );
    }

    /// Clear consumer position when a file is fully processed
    pub fn clear_consumer_position(&self) {
        let mut metadata = self.metadata.lock().unwrap();
        metadata.clear_consumer_position();
        drop(metadata);
        self.dirty.store(true, Ordering::Release);
        debug!("Cleared consumer position after file completion");
    }

    /// Get the resume position for recovery
    /// Returns (file_path, start_command_index) if there's a partially processed file
    pub fn get_resume_position(&self) -> Option<(String, usize)> {
        let metadata = self.metadata.lock().unwrap();
        metadata.get_resume_position()
    }

    /// Get the current last committed LSN (flush_lsn)
    #[inline]
    pub fn get(&self) -> u64 {
        let metadata = self.metadata.lock().unwrap();
        metadata.get_lsn()
    }

    /// Get the current last committed LSN as `Option<Lsn>`
    pub fn get_lsn(&self) -> Option<Lsn> {
        let v = self.get();
        if v == 0 {
            None
        } else {
            Some(Lsn(v))
        }
    }

    /// Get a copy of the current metadata
    pub fn get_metadata(&self) -> CdcMetadata {
        let metadata = self.metadata.lock().unwrap();
        metadata.clone()
    }

    /// Persist the current metadata to file asynchronously
    /// This method writes the metadata atomically by first writing to a temp file
    /// and then renaming to ensure crash safety. Returns early if the metadata
    /// is already persisted.
    pub async fn persist_async(&self) -> std::io::Result<()> {
        self.persist_internal().await
    }

    // / Force persist the current metadata to file, bypassing LSN comparison checks
    // / This is useful for saving consumer position updates during shutdown
    // pub async fn persist_async_force(&self) -> std::io::Result<()> {
    //     self.persist_internal(true).await
    // }

    /// Internal persist implementation that updates the persisted LSN tracker
    async fn persist_internal(&self) -> std::io::Result<()> {
        let metadata = {
            let m = self.metadata.lock().unwrap();
            m.clone()
        };

        let flush_lsn = metadata.get_lsn();

        // Serialize metadata to JSON
        let json_content = serde_json::to_string_pretty(&metadata)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Write to temp file first for atomic write
        let temp_path = format!("{}.tmp", self.lsn_file_path);
        tokio::fs::write(&temp_path, &json_content).await?;
        tokio::fs::rename(&temp_path, &self.lsn_file_path).await?;

        // Update persisted LSN and clear dirty flag
        self.last_persisted_lsn.store(flush_lsn, Ordering::Release);
        self.dirty.store(false, Ordering::Release);

        debug!(
            "Persisted CDC metadata to {} (flush_lsn: {}, consumer_file: {:?})",
            self.lsn_file_path,
            format_lsn(flush_lsn),
            metadata.consumer_state.current_file_path
        );
        Ok(())
    }

    /// Synchronous persist for use in Drop or non-async contexts
    fn persist_sync(&self) -> std::io::Result<()> {
        let metadata = {
            let m = self.metadata.lock().unwrap();
            m.clone()
        };

        let flush_lsn = metadata.get_lsn();

        // Serialize metadata to JSON
        let json_content = serde_json::to_string_pretty(&metadata)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Write to temp file first for atomic write
        let temp_path = format!("{}.tmp", self.lsn_file_path);
        debug!("Writing LSN metadata to temp file (sync): {}", temp_path);
        std::fs::write(&temp_path, &json_content)?;
        debug!("Renaming (sync) {} -> {}", temp_path, self.lsn_file_path);
        std::fs::rename(&temp_path, &self.lsn_file_path)?;

        // Update persisted LSN and clear dirty flag
        self.last_persisted_lsn.store(flush_lsn, Ordering::Release);
        self.dirty.store(false, Ordering::Release);

        debug!(
            "Persisted CDC metadata to {} (sync) - flush_lsn: {}, consumer_file: {:?}",
            self.lsn_file_path,
            format_lsn(flush_lsn),
            metadata.consumer_state.current_file_path
        );
        Ok(())
    }

    /// Get the file path for LSN persistence
    pub fn file_path(&self) -> &str {
        &self.lsn_file_path
    }

    /// Get the persistence interval in milliseconds
    pub fn interval_ms(&self) -> u64 {
        self.interval_ms
    }

    /// Check if there are pending changes to persist
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }
}

impl Drop for LsnTracker {
    fn drop(&mut self) {
        // Ensure graceful shutdown on drop
        self.shutdown_sync();
    }
}

/// Create a shared LSN tracker with initial load from file
///
/// The background persistence task is started automatically.
/// The returned `Arc<LsnTracker>` should be used throughout the application lifecycle.
/// Background persistence will be stopped when the shutdown_token is cancelled.
///
/// # Arguments
/// * `lsn_file_path` - Optional path to the LSN metadata file
pub async fn create_lsn_tracker_with_load_async(
    lsn_file_path: Option<&str>,
) -> (Arc<LsnTracker>, Option<Lsn>) {
    LsnTracker::new_with_load_and_interval(lsn_file_path, LsnTracker::DEFAULT_PERSIST_INTERVAL_MS)
        .await
}

#[cfg(test)]
mod lsn_tracker_tests {
    use super::*;

    #[tokio::test]
    async fn test_lsn_tracker_new() {
        let tracker = LsnTracker::new_with_interval(Some("/tmp/test_lsn_new"), 60000);
        assert_eq!(tracker.get(), 0);
        assert_eq!(tracker.file_path(), "/tmp/test_lsn_new.metadata");
        assert_eq!(tracker.interval_ms(), 60000);
    }

    #[tokio::test]
    async fn test_lsn_tracker_update_if_greater() {
        let tracker = LsnTracker::new_with_interval(Some("/tmp/test_lsn_update"), 60000);

        tracker.update_if_greater(100);
        assert_eq!(tracker.get(), 100);
        assert!(tracker.is_dirty());

        // Should not update with smaller value
        tracker.update_if_greater(50);
        assert_eq!(tracker.get(), 100);

        // Should update with larger value
        tracker.update_if_greater(150);
        assert_eq!(tracker.get(), 150);

        // Should ignore zero
        tracker.update_if_greater(0);
        assert_eq!(tracker.get(), 150);
    }

    #[tokio::test]
    async fn test_lsn_tracker_get_lsn() {
        let tracker = LsnTracker::new_with_interval(Some("/tmp/test_lsn_get"), 60000);

        assert_eq!(tracker.get_lsn(), None);

        tracker.update_if_greater(100);
        assert_eq!(tracker.get_lsn(), Some(Lsn(100)));
    }

    #[tokio::test]
    async fn test_commit_lsn_marks_dirty() {
        let tracker = LsnTracker::new_with_interval(Some("/tmp/test_lsn_commit"), 60000);
        assert!(!tracker.is_dirty());

        tracker.commit_lsn(100);
        assert_eq!(tracker.get(), 100);
        assert!(tracker.is_dirty());
    }

    #[tokio::test]
    async fn test_persist_async() {
        let path = "/tmp/test_lsn_persist_async";
        let metadata_path = format!("{}.metadata", path);
        let tracker = LsnTracker::new_with_interval(Some(path), 60000);

        tracker.commit_lsn(12345678);
        assert!(tracker.is_dirty());

        tracker.persist_async().await.unwrap();

        // After persist, dirty flag should be cleared
        assert!(!tracker.is_dirty());

        // Verify file contents - should be JSON
        let contents = tokio::fs::read_to_string(&metadata_path).await.unwrap();
        assert!(!contents.is_empty());
        assert!(contents.contains("version"));
        assert!(contents.contains("lsn_tracking"));

        // Cleanup
        let _ = tokio::fs::remove_file(&metadata_path).await;
    }

    #[tokio::test]
    async fn test_persist_skips_when_not_dirty() {
        let path = "/tmp/test_lsn_skip_persist";
        let tracker = LsnTracker::new_with_interval(Some(path), 60000);

        tracker.commit_lsn(100);
        tracker.persist_async().await.unwrap();

        // Persist again without changes - should skip
        let result = tracker.persist_async().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_load_from_file() {
        let path = "/tmp/test_lsn_load";
        let metadata_path = format!("{}.metadata", path);

        // Write test JSON metadata
        let test_metadata = CdcMetadata {
            version: "1.0".to_string(),
            last_updated: Utc::now(),
            lsn_tracking: LsnTracking {
                flush_lsn: 11259375,
            },
            consumer_state: ConsumerState {
                last_processed_tx_id: None,
                last_processed_timestamp: None,
                pending_file_count: 0,
                current_file_path: None,
                last_executed_command_index: None,
            },
        };
        let json = serde_json::to_string_pretty(&test_metadata).unwrap();
        tokio::fs::write(&metadata_path, json).await.unwrap();

        let tracker = LsnTracker::new_with_interval(Some(path), 60000);
        let loaded = tracker.load_from_file().await;

        assert!(loaded.is_some());
        let metadata = loaded.unwrap();
        assert_eq!(metadata.lsn_tracking.flush_lsn, 11259375);

        // Cleanup
        let _ = tokio::fs::remove_file(&metadata_path).await;
    }

    #[tokio::test]
    async fn test_new_with_load() {
        let path = "/tmp/test_lsn_with_load";
        let metadata_path = format!("{}.metadata", path);

        // Write test JSON metadata
        let test_metadata = CdcMetadata {
            version: "1.0".to_string(),
            last_updated: Utc::now(),
            lsn_tracking: LsnTracking { flush_lsn: 1193046 },
            consumer_state: ConsumerState {
                last_processed_tx_id: None,
                last_processed_timestamp: None,
                pending_file_count: 0,
                current_file_path: None,
                last_executed_command_index: None,
            },
        };
        let json = serde_json::to_string_pretty(&test_metadata).unwrap();
        tokio::fs::write(&metadata_path, json).await.unwrap();

        let (tracker, lsn) = LsnTracker::new_with_load_and_interval(Some(path), 1000).await;

        assert!(lsn.is_some());
        assert_eq!(tracker.get(), 1193046);

        // Cleanup
        let _ = tokio::fs::remove_file(&metadata_path).await;
    }

    #[tokio::test]
    async fn test_background_persistence_with_shutdown() {
        let path = "/tmp/test_lsn_bg_shutdown";
        let metadata_path = format!("{}.metadata", path);

        // Create tracker with short interval for testing
        let arc_tracker = LsnTracker::new_with_interval(Some(path), 100);

        // Commit some LSNs
        arc_tracker.commit_lsn(100);
        arc_tracker.commit_lsn(200);
        arc_tracker.commit_lsn(300);

        // Wait a bit for background task to persist
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Commit final LSN
        arc_tracker.commit_lsn(400);

        // Shutdown gracefully - this should persist the final LSN
        arc_tracker.shutdown_async().await;

        // Verify final LSN was persisted
        let contents = tokio::fs::read_to_string(&metadata_path).await.unwrap();
        assert!(!contents.is_empty());

        // Load from file to verify
        let (new_tracker, loaded_lsn) =
            LsnTracker::new_with_load_and_interval(Some(path), 1000).await;
        assert!(loaded_lsn.is_some());
        assert_eq!(new_tracker.get(), 400);

        // Cleanup
        let _ = tokio::fs::remove_file(&metadata_path).await;
    }

    #[tokio::test]
    async fn test_shutdown_without_background_task() {
        let path = "/tmp/test_lsn_no_bg_shutdown";
        let metadata_path = format!("{}.metadata", path);
        let tracker = LsnTracker::new_with_interval(Some(path), 60000);

        // Commit LSN (background task is already running from new_with_interval)
        tracker.commit_lsn(999);

        // Shutdown should still persist
        tracker.shutdown_async().await;

        // Verify LSN was persisted
        let contents = tokio::fs::read_to_string(&metadata_path).await.unwrap();
        assert!(!contents.is_empty());

        let (new_tracker, loaded_lsn) =
            LsnTracker::new_with_load_and_interval(Some(path), 1000).await;
        assert!(loaded_lsn.is_some());
        assert_eq!(new_tracker.get(), 999);

        // Cleanup
        let _ = tokio::fs::remove_file(&metadata_path).await;
    }

    #[tokio::test]
    async fn test_double_shutdown_is_safe() {
        let path = "/tmp/test_lsn_double_shutdown";
        let metadata_path = format!("{}.metadata", path);
        let (arc_tracker, _) = LsnTracker::new_with_load_and_interval(Some(path), 1000).await;

        arc_tracker.commit_lsn(555);

        // First shutdown
        arc_tracker.shutdown_async().await;

        // Second shutdown should be safe (no-op)
        arc_tracker.shutdown_async().await;

        // Cleanup
        let _ = tokio::fs::remove_file(&metadata_path).await;
    }

    #[test]
    fn test_shared_lsn_feedback_new() {
        let feedback = SharedLsnFeedback::new();
        assert_eq!(feedback.get_flushed_lsn(), 0);
        assert_eq!(feedback.get_applied_lsn(), 0);
    }

    #[test]
    fn test_update_flushed_lsn() {
        let feedback = SharedLsnFeedback::new();

        feedback.update_flushed_lsn(100);
        assert_eq!(feedback.get_flushed_lsn(), 100);

        // Should not update with smaller value
        feedback.update_flushed_lsn(50);
        assert_eq!(feedback.get_flushed_lsn(), 100);

        // Should update with larger value
        feedback.update_flushed_lsn(200);
        assert_eq!(feedback.get_flushed_lsn(), 200);
    }

    #[test]
    fn test_update_applied_lsn() {
        let feedback = SharedLsnFeedback::new();

        feedback.update_applied_lsn(100);
        assert_eq!(feedback.get_applied_lsn(), 100);
        // Applied also updates flushed
        assert_eq!(feedback.get_flushed_lsn(), 100);

        // Should not update with smaller value
        feedback.update_applied_lsn(50);
        assert_eq!(feedback.get_applied_lsn(), 100);
    }

    #[test]
    fn test_get_feedback_lsn() {
        let feedback = SharedLsnFeedback::new();

        feedback.update_flushed_lsn(100);
        feedback.update_applied_lsn(50);

        let (flushed, applied) = feedback.get_feedback_lsn();
        assert_eq!(flushed, 100);
        assert_eq!(applied, 50);
    }

    #[test]
    fn test_zero_lsn_ignored() {
        let feedback = SharedLsnFeedback::new();
        feedback.update_flushed_lsn(100);
        feedback.update_applied_lsn(100);

        // Zero should be ignored
        feedback.update_flushed_lsn(0);
        feedback.update_applied_lsn(0);

        assert_eq!(feedback.get_flushed_lsn(), 100);
        assert_eq!(feedback.get_applied_lsn(), 100);
    }

    #[test]
    fn test_shared_across_threads() {
        use std::thread;

        let feedback = Arc::new(SharedLsnFeedback::new());
        let feedback_clone = feedback.clone();

        let handle = thread::spawn(move || {
            for i in 1..=100 {
                feedback_clone.update_applied_lsn(i);
            }
        });

        handle.join().unwrap();

        assert_eq!(feedback.get_applied_lsn(), 100);
        assert_eq!(feedback.get_flushed_lsn(), 100);
    }

    #[tokio::test]
    async fn test_metadata_extension_already_present() {
        // Test that if path already has .metadata extension, it doesn't add another one
        let path_with_extension = "/tmp/test_lsn_with_ext.metadata";
        let tracker = LsnTracker::new_with_interval(Some(path_with_extension), 60000);

        // Should not add .metadata.metadata
        assert_eq!(tracker.file_path(), "/tmp/test_lsn_with_ext.metadata");
        assert!(!tracker.file_path().ends_with(".metadata.metadata"));

        // Test persistence works correctly
        tracker.commit_lsn(12345);
        tracker.persist_async().await.unwrap();

        // Verify file was created at correct location
        let contents = tokio::fs::read_to_string("/tmp/test_lsn_with_ext.metadata")
            .await
            .unwrap();
        assert!(contents.contains("lsn_tracking"));

        // Cleanup
        let _ = tokio::fs::remove_file("/tmp/test_lsn_with_ext.metadata").await;
    }

    #[tokio::test]
    async fn test_metadata_extension_not_present() {
        // Test that if path doesn't have .metadata extension, it adds it
        let path_without_extension = "/tmp/test_lsn_without_ext";
        let tracker = LsnTracker::new_with_interval(Some(path_without_extension), 60000);

        // Should add .metadata
        assert_eq!(tracker.file_path(), "/tmp/test_lsn_without_ext.metadata");
        assert!(tracker.file_path().ends_with(".metadata"));

        // Test persistence works correctly
        tracker.commit_lsn(54321);
        tracker.persist_async().await.unwrap();

        // Verify file was created at correct location
        let contents = tokio::fs::read_to_string("/tmp/test_lsn_without_ext.metadata")
            .await
            .unwrap();
        assert!(contents.contains("lsn_tracking"));

        // Shutdown gracefully
        tracker.shutdown_async().await;

        // Cleanup
        let _ = tokio::fs::remove_file("/tmp/test_lsn_without_ext.metadata").await;
    }
}
