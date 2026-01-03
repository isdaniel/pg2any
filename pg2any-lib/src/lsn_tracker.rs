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
//! ## Persistence Strategy
//!
//! The LsnTracker persists immediately after critical state changes:
//! - After each batch execution in the consumer (explicit calls to `persist_async()`)
//! - On shutdown for final state preservation
//! - On error conditions for durability guarantees
//! - Tracks a "dirty" flag to skip unnecessary writes when state hasn't changed
//! - All persistence is explicit and immediate for data safety

use crate::types::Lsn;
use chrono::{DateTime, Utc};
use pg_walstream::format_lsn;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

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
/// ## Persistence Strategy
///
/// This tracker persists immediately when explicitly requested:
/// - After each batch execution in the consumer (via `persist_async()`)
/// - On shutdown for final state preservation
/// - On error conditions for durability guarantees
/// - Tracks a "dirty" flag to skip unnecessary writes when state hasn't changed
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
}

impl LsnTracker {
    /// Create a new LSN tracker with the specified file path
    pub fn new(lsn_file_path: Option<&str>) -> Arc<Self> {
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

        Arc::new(Self {
            metadata: Arc::new(Mutex::new(CdcMetadata::default())),
            last_persisted_lsn: AtomicU64::new(0),
            dirty: AtomicBool::new(false),
            lsn_file_path: path,
        })
    }

    /// Create a new LSN tracker and load the initial value from file
    ///
    /// # Arguments
    /// * `lsn_file_path` - Optional path to the LSN metadata file
    pub async fn new_with_load(lsn_file_path: Option<&str>) -> (Arc<Self>, Option<Lsn>) {
        let tracker = Self::new(lsn_file_path);

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

    /// Shutdown the tracker gracefully
    /// This ensures the final LSN is persisted before exit.
    pub async fn shutdown_async(&self) {
        info!("Shutting down LSN tracker");

        // Force an immediate persist to ensure final state is written
        if let Err(e) = self.persist_async().await {
            warn!("Failed to persist on shutdown: {}", e);
        } else {
            info!("Final state persisted on shutdown");
        }

        info!("LSN tracker stopped gracefully");
    }

    /// Synchronous shutdown for use in Drop or non-async contexts
    pub fn shutdown_sync(&self) {
        info!("Initiating sync shutdown of LSN tracker");

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
    /// It updates the in-memory LSN and marks it as dirty.
    /// Note: This does NOT automatically persist to disk - caller must explicitly call `persist_async()`.
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
        {
            let mut metadata = self.metadata.lock().unwrap();
            metadata.update_consumer_state(tx_id, timestamp, pending_count);
        }
        self.dirty.store(true, Ordering::Release);
        debug!(
            "Updated consumer state: tx_id={}, pending_count={}",
            tx_id, pending_count
        );
    }

    /// Update consumer position within a transaction file (non-persistent)
    pub fn update_consumer_position(&self, file_path: String, last_executed_command_index: usize) {
        {
            let mut metadata = self.metadata.lock().unwrap();
            metadata.update_consumer_position(file_path.clone(), last_executed_command_index);
        }
        self.dirty.store(true, Ordering::Release);
        debug!(
            "Updated consumer position: file={}, command_index={}",
            file_path, last_executed_command_index
        );
    }

    /// Update consumer position AND persist immediately in one atomic operation
    ///
    /// This method is designed to prevent data inconsistency during graceful shutdown.
    /// It combines the position update with immediate persistence to eliminate the
    /// race condition where:
    /// 1. DB transaction commits
    /// 2. Position updated in memory
    /// 3. **SIGTERM arrives before persist_async() is called**
    /// 4. On restart: old position → duplicate replay
    ///
    /// By persisting immediately after position update, we ensure the tracking file
    /// reflects the actual state of committed data in the destination database.
    ///
    /// # Arguments
    /// * `file_path` - Path to the transaction file being processed
    /// * `last_executed_command_index` - 0-based index of the last executed command
    ///
    /// # Returns
    /// * `Ok(())` - Position updated and persisted successfully
    /// * `Err(io::Error)` - Persistence failed (position still updated in memory)
    pub async fn update_consumer_position_and_persist_immediately(
        &self,
        file_path: String,
        last_executed_command_index: usize,
    ) -> std::io::Result<()> {
        // Update position in memory
        {
            let mut metadata = self.metadata.lock().unwrap();
            metadata.update_consumer_position(file_path.clone(), last_executed_command_index);
        }
        self.dirty.store(true, Ordering::Release);

        // CRITICAL: Immediately persist to disk to prevent race condition
        self.persist_internal().await?;

        Ok(())
    }

    /// Clear consumer position when a file is fully processed
    pub fn clear_consumer_position(&self) {
        {
            let mut metadata = self.metadata.lock().unwrap();
            metadata.clear_consumer_position();
        }
        self.dirty.store(true, Ordering::Release);
        debug!("Cleared consumer position after file completion");
    }

    /// Clear consumer position AND persist immediately in one atomic operation
    ///
    /// Used when a transaction file is fully processed to ensure the cleared
    /// state is immediately written to disk.
    ///
    /// # Returns
    /// * `Ok(())` - Position cleared and persisted successfully
    /// * `Err(io::Error)` - Persistence failed (position still cleared in memory)
    pub async fn clear_consumer_position_and_persist_immediately(&self) -> std::io::Result<()> {
        // Clear position in memory
        {
            let mut metadata = self.metadata.lock().unwrap();
            metadata.clear_consumer_position();
        }
        self.dirty.store(true, Ordering::Release);

        // CRITICAL: Immediately persist to disk
        self.persist_async().await?;

        debug!("Atomically cleared and persisted consumer position");

        Ok(())
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

    /// Internal persist implementation
    async fn persist_internal(&self) -> std::io::Result<()> {
        let metadata = {
            let m = self.metadata.lock().unwrap();
            m.clone()
        };

        let flush_lsn = metadata.get_lsn();

        // Serialize metadata to JSON
        let json_content =
            serde_json::to_string_pretty(&metadata).map_err(std::io::Error::other)?;

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
        let json_content =
            serde_json::to_string_pretty(&metadata).map_err(std::io::Error::other)?;

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

    /// Check if there are pending changes to persist
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }
}

/// Helper function to create a tracker with loaded LSN value
/// This is a convenience wrapper for the most common usage pattern.
pub async fn create_lsn_tracker_with_load(
    lsn_file_path: Option<&str>,
) -> (Arc<LsnTracker>, Option<Lsn>) {
    LsnTracker::new_with_load(lsn_file_path).await
}

#[cfg(test)]
mod lsn_tracker_tests {
    use super::*;

    #[tokio::test]
    async fn test_lsn_tracker_new() {
        let tracker = LsnTracker::new(Some("/tmp/test_lsn"));
        assert_eq!(tracker.get(), 0);
        assert_eq!(tracker.file_path(), "/tmp/test_lsn.metadata");
    }

    #[tokio::test]
    async fn test_lsn_tracker_get_lsn() {
        let tracker = LsnTracker::new(Some("/tmp/test_lsn_get_lsn"));
        tracker.commit_lsn(100);
        let lsn = tracker.get_lsn();
        assert_eq!(lsn, Some(Lsn(100)));
    }

    #[tokio::test]
    async fn test_lsn_tracker_update_if_greater() {
        let tracker = LsnTracker::new(Some("/tmp/test_lsn_update_if_greater"));

        tracker.update_if_greater(100);
        assert_eq!(tracker.get(), 100);

        // Smaller value should be ignored
        tracker.update_if_greater(50);
        assert_eq!(tracker.get(), 100);

        // Greater value should update
        tracker.update_if_greater(200);
        assert_eq!(tracker.get(), 200);
    }

    #[tokio::test]
    async fn test_zero_lsn_ignored() {
        let tracker = LsnTracker::new(Some("/tmp/test_lsn_zero"));

        tracker.update_if_greater(100);
        tracker.update_if_greater(0); // Should be ignored

        assert_eq!(tracker.get(), 100);
    }

    #[tokio::test]
    async fn test_persist_async() {
        let path = "/tmp/test_lsn_persist_async";
        let _ = std::fs::remove_file(format!("{}.metadata", path));

        let tracker = LsnTracker::new(Some(path));
        tracker.commit_lsn(12345);
        tracker.persist_async().await.unwrap();

        // Read back the file
        let content = tokio::fs::read_to_string(format!("{}.metadata", path))
            .await
            .unwrap();
        let metadata: CdcMetadata = serde_json::from_str(&content).unwrap();
        assert_eq!(metadata.lsn_tracking.flush_lsn, 12345);

        // Clean up
        let _ = std::fs::remove_file(format!("{}.metadata", path));
    }

    #[tokio::test]
    async fn test_persist_skips_when_not_dirty() {
        let path = "/tmp/test_lsn_persist_skip";
        let _ = std::fs::remove_file(format!("{}.metadata", path));

        let tracker = LsnTracker::new(Some(path));
        tracker.commit_lsn(12345);
        tracker.persist_async().await.unwrap();

        // Second persist should be skipped (not dirty)
        let result = tracker.persist_async().await;
        assert!(result.is_ok());

        // Clean up
        let _ = std::fs::remove_file(format!("{}.metadata", path));
    }

    #[tokio::test]
    async fn test_commit_lsn_marks_dirty() {
        let tracker = LsnTracker::new(Some("/tmp/test_lsn_dirty"));

        assert!(!tracker.is_dirty());

        tracker.commit_lsn(100);
        assert!(tracker.is_dirty());
    }

    #[tokio::test]
    async fn test_load_from_file() {
        let path = "/tmp/test_lsn_load";
        let metadata_path = format!("{}.metadata", path);

        // Create a metadata file
        let metadata = CdcMetadata {
            version: "1.0".to_string(),
            last_updated: Utc::now(),
            lsn_tracking: LsnTracking { flush_lsn: 54321 },
            consumer_state: ConsumerState {
                last_processed_tx_id: Some(999),
                last_processed_timestamp: Some(Utc::now()),
                pending_file_count: 5,
                current_file_path: None,
                last_executed_command_index: None,
            },
        };

        let json = serde_json::to_string_pretty(&metadata).unwrap();
        tokio::fs::write(&metadata_path, json).await.unwrap();

        // Load it
        let tracker = LsnTracker::new(Some(path));
        let loaded = tracker.load_from_file().await;

        assert!(loaded.is_some());
        let loaded_metadata = loaded.unwrap();
        assert_eq!(loaded_metadata.lsn_tracking.flush_lsn, 54321);
        assert_eq!(
            loaded_metadata.consumer_state.last_processed_tx_id,
            Some(999)
        );

        // Clean up
        let _ = std::fs::remove_file(metadata_path);
    }

    #[tokio::test]
    async fn test_new_with_load() {
        let path = "/tmp/test_lsn_new_with_load";
        let metadata_path = format!("{}.metadata", path);

        // Create a metadata file
        let metadata = CdcMetadata {
            version: "1.0".to_string(),
            last_updated: Utc::now(),
            lsn_tracking: LsnTracking { flush_lsn: 67890 },
            consumer_state: ConsumerState {
                last_processed_tx_id: None,
                last_processed_timestamp: None,
                pending_file_count: 0,
                current_file_path: None,
                last_executed_command_index: None,
            },
        };

        let json = serde_json::to_string_pretty(&metadata).unwrap();
        tokio::fs::write(&metadata_path, json).await.unwrap();

        // Load using new_with_load
        let (tracker, loaded_lsn) = LsnTracker::new_with_load(Some(path)).await;

        assert_eq!(loaded_lsn, Some(Lsn(67890)));
        assert_eq!(tracker.get(), 67890);

        // Clean up
        let _ = std::fs::remove_file(metadata_path);
    }

    #[tokio::test]
    async fn test_metadata_extension_not_present() {
        let tracker = LsnTracker::new(Some("/tmp/test_lsn_no_ext"));
        assert!(tracker.file_path().ends_with(".metadata"));
    }

    #[tokio::test]
    async fn test_metadata_extension_already_present() {
        let tracker = LsnTracker::new(Some("/tmp/test_lsn.metadata"));
        // Should not double-add the extension
        assert_eq!(tracker.file_path().matches(".metadata").count(), 1);
    }

    #[tokio::test]
    async fn test_shared_across_threads() {
        let tracker = LsnTracker::new(Some("/tmp/test_lsn_threads"));
        let tracker_clone = tracker.clone();

        let handle = tokio::spawn(async move {
            tracker_clone.commit_lsn(12345);
        });

        handle.await.unwrap();
        assert_eq!(tracker.get(), 12345);
    }

    #[tokio::test]
    async fn test_shutdown_without_background_task() {
        let path = "/tmp/test_lsn_shutdown_simple";
        let _ = std::fs::remove_file(format!("{}.metadata", path));

        let tracker = LsnTracker::new(Some(path));
        tracker.commit_lsn(99999);

        // Shutdown should persist
        tracker.shutdown_async().await;

        // Verify file was written
        let content = tokio::fs::read_to_string(format!("{}.metadata", path))
            .await
            .unwrap();
        let metadata: CdcMetadata = serde_json::from_str(&content).unwrap();
        assert_eq!(metadata.lsn_tracking.flush_lsn, 99999);

        // Clean up
        let _ = std::fs::remove_file(format!("{}.metadata", path));
    }

    #[tokio::test]
    async fn test_double_shutdown_is_safe() {
        let tracker = LsnTracker::new(Some("/tmp/test_lsn_double_shutdown"));
        tracker.shutdown_async().await;
        // Second shutdown should be safe
        tracker.shutdown_async().await;
    }

    // Shared LSN Feedback tests
    #[test]
    fn test_shared_lsn_feedback_new() {
        let feedback = SharedLsnFeedback::new_shared();
        assert_eq!(feedback.get_flushed_lsn(), 0);
        assert_eq!(feedback.get_applied_lsn(), 0);
    }

    #[test]
    fn test_update_flushed_lsn() {
        let feedback = SharedLsnFeedback::new_shared();
        feedback.update_flushed_lsn(100);
        assert_eq!(feedback.get_flushed_lsn(), 100);
    }

    #[test]
    fn test_update_applied_lsn() {
        let feedback = SharedLsnFeedback::new_shared();
        feedback.update_applied_lsn(200);
        assert_eq!(feedback.get_applied_lsn(), 200);
    }

    #[test]
    fn test_get_feedback_lsn() {
        let feedback = SharedLsnFeedback::new_shared();
        feedback.update_flushed_lsn(150);
        feedback.update_applied_lsn(200);

        // get_feedback_lsn returns a tuple (flushed, applied)
        // Both update to the max value seen
        let (flushed, applied) = feedback.get_feedback_lsn();
        assert_eq!(flushed, 200);
        assert_eq!(applied, 200);
    }
}
