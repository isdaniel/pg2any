//! Thread-safe LSN tracking for CDC replication feedback
//!
//! This module provides two types of LSN tracking:
//!
//! 1. **SharedLsnFeedback**: Thread-safe tracking of LSN positions for replication feedback
//!    to PostgreSQL (write_lsn, flush_lsn, replay_lsn)
//!
//! 2. **LsnTracker**: Thread-safe tracker for the last committed LSN with file persistence
//!    for graceful shutdown and restart recovery
//!
//! The PostgreSQL replication protocol expects three different LSN values:
//! - `write_lsn`: Data received from the stream
//! - `flush_lsn`: Data written to destination (before commit)  
//! - `replay_lsn`: Data committed to destination
//!
//! Since the producer reads from PostgreSQL and the consumer writes to the destination,
//! we need a thread-safe way to share the committed LSN from consumer back to producer
//! for accurate feedback to PostgreSQL.
//!
//! ## Optimization Strategy
//!
//! Instead of persisting on every commit (which causes excessive I/O), the LsnTracker:
//! - Batches multiple commits and persists periodically (default: every 1 second)
//! - Uses a background tokio task for non-blocking persistence
//! - Tracks a "dirty" flag to skip unnecessary writes
//! - Ensures graceful shutdown with final persistence

use crate::pg_replication::XLogRecPtr;
use crate::types::Lsn;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Thread-safe tracker for LSN positions used in replication feedback
///
/// This tracker is designed to be shared between the producer (which sends feedback
/// to PostgreSQL) and the consumer (which commits transactions to the destination).
#[derive(Debug)]
pub struct SharedLsnFeedback {
    /// Last flushed LSN - data written to destination before commit
    flushed_lsn: AtomicU64,
    /// Last applied/replayed LSN - data committed to destination
    applied_lsn: AtomicU64,
}

impl SharedLsnFeedback {
    /// Create a new shared LSN feedback tracker
    pub fn new() -> Self {
        Self {
            flushed_lsn: AtomicU64::new(0),
            applied_lsn: AtomicU64::new(0),
        }
    }

    /// Create a new shared LSN feedback tracker wrapped in Arc for sharing
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Update the flushed LSN if the new value is greater
    ///
    /// This should be called when data has been written/flushed to the destination
    /// database, but not yet committed (e.g., during batch writes).
    #[inline]
    pub fn update_flushed_lsn(&self, lsn: XLogRecPtr) {
        if lsn == 0 {
            return;
        }
        loop {
            let current = self.flushed_lsn.load(Ordering::Acquire);
            if lsn <= current {
                return;
            }
            match self.flushed_lsn.compare_exchange(
                current,
                lsn,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    debug!(
                        "SharedLsnFeedback: Updated flushed LSN from {} to {}",
                        current, lsn
                    );
                    return;
                }
                Err(_) => continue,
            }
        }
    }

    /// Update the applied/replayed LSN if the new value is greater
    ///
    /// This should be called when a transaction has been successfully committed
    /// to the destination database. This is the most important LSN as PostgreSQL
    /// uses it to determine which WAL can be recycled.
    #[inline]
    pub fn update_applied_lsn(&self, lsn: XLogRecPtr) {
        if lsn == 0 {
            return;
        }
        // Update applied LSN
        loop {
            let current = self.applied_lsn.load(Ordering::Acquire);
            if lsn <= current {
                break;
            }
            match self.applied_lsn.compare_exchange(
                current,
                lsn,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    debug!(
                        "SharedLsnFeedback: Updated applied LSN from {} to {}",
                        current, lsn
                    );
                    break;
                }
                Err(_) => continue,
            }
        }

        // Applied data is implicitly flushed, update flushed as well
        self.update_flushed_lsn(lsn);
    }

    /// Get the current flushed LSN
    #[inline]
    pub fn get_flushed_lsn(&self) -> XLogRecPtr {
        self.flushed_lsn.load(Ordering::Acquire)
    }

    /// Get the current applied LSN
    #[inline]
    pub fn get_applied_lsn(&self) -> XLogRecPtr {
        self.applied_lsn.load(Ordering::Acquire)
    }

    /// Get both LSN values atomically for feedback
    ///
    /// Returns (flushed_lsn, applied_lsn)
    #[inline]
    pub fn get_feedback_lsn(&self) -> (XLogRecPtr, XLogRecPtr) {
        let flushed = self.flushed_lsn.load(Ordering::Acquire);
        let applied = self.applied_lsn.load(Ordering::Acquire);
        (flushed, applied)
    }

    /// Log current LSN state (for debugging)
    pub fn log_state(&self, prefix: &str) {
        let flushed = self.get_flushed_lsn();
        let applied = self.get_applied_lsn();
        info!(
            "{}: flushed_lsn={}, applied_lsn={}",
            prefix,
            crate::pg_replication::format_lsn(flushed),
            crate::pg_replication::format_lsn(applied)
        );
    }
}

impl Default for SharedLsnFeedback {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for SharedLsnFeedback {
    fn clone(&self) -> Self {
        Self {
            flushed_lsn: AtomicU64::new(self.flushed_lsn.load(Ordering::Acquire)),
            applied_lsn: AtomicU64::new(self.applied_lsn.load(Ordering::Acquire)),
        }
    }
}

/// Thread-safe tracker for the last committed LSN with file persistence
///
/// This tracker maintains the LSN of the last successfully committed transaction
/// and provides optimized batched persistence to reduce I/O overhead.
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
#[derive(Debug)]
pub struct LsnTracker {
    /// The last committed LSN (atomic for thread-safety)
    last_committed_lsn: AtomicU64,
    /// Last persisted LSN (to avoid unnecessary writes)
    last_persisted_lsn: AtomicU64,
    /// Flag indicating if LSN needs to be persisted
    dirty: AtomicBool,
    /// Path to the LSN persistence file
    lsn_file_path: String,
    /// Persistence interval in milliseconds
    interval_ms: u64,
    /// Cancellation token for background task
    shutdown_token: CancellationToken,
    /// Notify for immediate persistence request
    persist_notify: Arc<Notify>,
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
    /// until shutdown_async() is called or the tracker is dropped.
    pub fn new_with_interval(lsn_file_path: Option<&str>, interval_ms: u64) -> Arc<Self> {
        let path = lsn_file_path
            .map(String::from)
            .or_else(|| std::env::var("CDC_LAST_LSN_FILE").ok())
            .unwrap_or_else(|| "./pg2any_last_lsn".to_string());

        let shutdown_token = CancellationToken::new();
        let persist_notify = Arc::new(Notify::new());
        let interval = Duration::from_millis(interval_ms);

        // Use Arc::new_cyclic to create the tracker and spawn the background task
        // with a reference to the tracker in a single step
        let tracker = Arc::new_cyclic(|weak_tracker: &std::sync::Weak<Self>| {
            let shutdown_token_clone = shutdown_token.clone();
            let persist_notify_clone = persist_notify.clone();
            
            // Spawn the background task with a weak reference
            let weak_tracker_clone = weak_tracker.clone();
            let handle = tokio::spawn(async move {
                debug!(
                    "Background LSN persistence task started (interval: {:?})",
                    interval
                );

                loop {
                    tokio::select! {
                        biased;

                        // Shutdown signal
                        _ = shutdown_token_clone.cancelled() => {
                            debug!("Background persistence task received shutdown signal");
                            break;
                        }

                        // Immediate persist request
                        _ = persist_notify_clone.notified() => {
                            if let Some(tracker) = weak_tracker_clone.upgrade() {
                                if let Err(e) = tracker.persist_internal().await {
                                    warn!("Immediate persistence failed: {}", e);
                                }
                            }
                        }

                        // Periodic persistence
                        _ = tokio::time::sleep(interval) => {
                            if let Some(tracker) = weak_tracker_clone.upgrade() {
                                if tracker.dirty.load(Ordering::Acquire) {
                                    if let Err(e) = tracker.persist_internal().await {
                                        warn!("Background persistence failed: {}", e);
                                    }
                                }
                            } else {
                                // Tracker was dropped, exit the loop
                                debug!("Tracker dropped, stopping background persistence");
                                break;
                            }
                        }
                    }
                }

                // Final persist on shutdown
                if let Some(tracker) = weak_tracker_clone.upgrade() {
                    if let Err(e) = tracker.persist_internal().await {
                        warn!("Final persistence on shutdown failed: {}", e);
                    } else {
                        let final_lsn = tracker.last_committed_lsn.load(Ordering::Acquire);
                        if final_lsn > 0 {
                            info!(
                                "Final LSN persisted on shutdown: {}",
                                crate::pg_replication::format_lsn(final_lsn)
                            );
                        }
                    }
                }

                debug!("Background LSN persistence task stopped");
            });

            // Create the tracker with the actual handle
            Self {
                last_committed_lsn: AtomicU64::new(0),
                last_persisted_lsn: AtomicU64::new(0),
                dirty: AtomicBool::new(false),
                lsn_file_path: path,
                interval_ms,
                shutdown_token,
                persist_notify,
                background_task: Arc::new(Mutex::new(handle)),
            }
        });

        info!(
            "Started background LSN persistence (interval: {}ms)",
            interval_ms
        );

        tracker
    }

    /// Create a new LSN tracker and load the initial value from file
    ///
    /// This is the preferred way to create a tracker that should resume
    /// from a previously persisted LSN.
    pub async fn new_with_load(lsn_file_path: Option<&str>) -> (Arc<Self>, Option<Lsn>) {
        Self::new_with_load_and_interval(lsn_file_path, Self::DEFAULT_PERSIST_INTERVAL_MS).await
    }

    /// Create a new LSN tracker with custom interval and load the initial value from file
    /// 
    /// The background persistence task is started automatically.
    pub async fn new_with_load_and_interval(
        lsn_file_path: Option<&str>,
        interval_ms: u64,
    ) -> (Arc<Self>, Option<Lsn>) {
        let tracker = Self::new_with_interval(lsn_file_path, interval_ms);
        let loaded_lsn = tracker.load_from_file().await;

        // Initialize the atomics with the loaded value
        if let Some(lsn) = loaded_lsn {
            tracker.last_committed_lsn.store(lsn.0, Ordering::Release);
            tracker.last_persisted_lsn.store(lsn.0, Ordering::Release);
        }

        (tracker, loaded_lsn)
    }

    /// Shutdown the background persistence task gracefully
    /// This ensures the final LSN is persisted before the task exits.
    /// Should be called before dropping the tracker.
    /// **CRITICAL**: This method waits for the background task to complete its, final persistence, ensuring no LSN data loss during shutdown.
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

    /// Load LSN from the persistence file
    ///
    /// Returns `Some(Lsn)` if a valid LSN was found and parsed, `None` otherwise.
    /// Logs warnings for parsing errors but doesn't fail.
    pub async fn load_from_file(&self) -> Option<Lsn> {
        match tokio::fs::read_to_string(&self.lsn_file_path).await {
            Ok(contents) => {
                let s = contents.trim();
                if s.is_empty() {
                    info!(
                        "LSN file {} is empty, starting from latest",
                        self.lsn_file_path
                    );
                    None
                } else {
                    match crate::pg_replication::parse_lsn(s) {
                        Ok(v) => {
                            info!("Loaded LSN {} from {}", s, self.lsn_file_path);
                            Some(Lsn(v))
                        }
                        Err(e) => {
                            warn!(
                                "Failed to parse persisted LSN from {}: {}",
                                self.lsn_file_path, e
                            );
                            None
                        }
                    }
                }
            }
            Err(_) => {
                info!(
                    "No persisted LSN file found at {}, starting from latest",
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
        loop {
            let current = self.last_committed_lsn.load(Ordering::Acquire);
            if lsn <= current {
                return; // No update needed
            }
            match self.last_committed_lsn.compare_exchange(
                current,
                lsn,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    debug!("Updated last committed LSN from {} to {}", current, lsn);
                    self.dirty.store(true, Ordering::Release);
                    return;
                }
                Err(_) => continue, // Retry if another thread updated concurrently
            }
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

    /// Get the current last committed LSN
    #[inline]
    pub fn get(&self) -> u64 {
        self.last_committed_lsn.load(Ordering::Acquire)
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

    /// Request immediate persistence (non-blocking)
    /// This notifies the background task to persist immediately.
    /// Useful for critical checkpoints where waiting for the next interval is not acceptable.
    pub fn request_persist(&self) {
        self.persist_notify.notify_one();
    }

    /// Persist the current LSN to file asynchronously
    /// This method writes the LSN atomically by first writing to a temp file
    /// and then renaming to ensure crash safety. Returns early if the LSN
    /// is already persisted.
    pub async fn persist_async(&self) -> std::io::Result<()> {
        self.persist_internal().await
    }

    /// Internal persist implementation that updates the persisted LSN tracker
    async fn persist_internal(&self) -> std::io::Result<()> {
        let lsn = self.last_committed_lsn.load(Ordering::Acquire);
        let persisted = self.last_persisted_lsn.load(Ordering::Acquire);

        // Skip if already persisted or no LSN yet
        if lsn == 0 || lsn <= persisted {
            return Ok(());
        }

        let lsn_str = crate::pg_replication::format_lsn(lsn);

        // Write to temp file first for atomic write
        let temp_path = format!("{}.tmp", self.lsn_file_path);
        tokio::fs::write(&temp_path, &lsn_str).await?;
        tokio::fs::rename(&temp_path, &self.lsn_file_path).await?;

        // Update persisted LSN and clear dirty flag
        self.last_persisted_lsn.store(lsn, Ordering::Release);
        self.dirty.store(false, Ordering::Release);

        info!(
            "Persisted committed LSN {} to {}",
            lsn_str, self.lsn_file_path
        );
        Ok(())
    }

    /// Synchronous persist for use in Drop or non-async contexts
    fn persist_sync(&self) -> std::io::Result<()> {
        let lsn = self.last_committed_lsn.load(Ordering::Acquire);
        let persisted = self.last_persisted_lsn.load(Ordering::Acquire);

        // Skip if already persisted or no LSN yet
        if lsn == 0 || lsn <= persisted {
            return Ok(());
        }

        let lsn_str = crate::pg_replication::format_lsn(lsn);

        // Write to temp file first for atomic write
        let temp_path = format!("{}.tmp", self.lsn_file_path);
        std::fs::write(&temp_path, &lsn_str)?;
        std::fs::rename(&temp_path, &self.lsn_file_path)?;

        // Update persisted LSN and clear dirty flag
        self.last_persisted_lsn.store(lsn, Ordering::Release);
        self.dirty.store(false, Ordering::Release);

        info!(
            "Persisted committed LSN {} to {} (sync)",
            lsn_str, self.lsn_file_path
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
/// The returned Arc<LsnTracker> should be used throughout the application lifecycle.
/// Background persistence will be stopped when the tracker is dropped.
pub async fn create_lsn_tracker_with_load_async(
    lsn_file_path: Option<&str>,
) -> (Arc<LsnTracker>, Option<Lsn>) {
    LsnTracker::new_with_load(lsn_file_path).await
}

/// Create a shared LSN tracker with custom persistence interval
///
/// This is useful for tuning I/O performance based on workload characteristics:
/// - High-frequency, low-latency: Use shorter intervals (100-500ms)
/// - Batch processing: Use longer intervals (2000-5000ms)
/// - Default: 1000ms provides good balance
///
/// # Arguments
///
/// * `lsn_file_path` - Optional path to the LSN persistence file
/// * `interval_ms` - Persistence interval in milliseconds
pub async fn create_lsn_tracker_with_interval_async(
    lsn_file_path: Option<&str>,
    interval_ms: u64,
) -> (Arc<LsnTracker>, Option<Lsn>) {
    LsnTracker::new_with_load_and_interval(lsn_file_path, interval_ms).await
}

#[cfg(test)]
mod lsn_tracker_tests {
    use super::*;

    #[tokio::test]
    async fn test_lsn_tracker_new() {
        let tracker = LsnTracker::new_with_interval(Some("/tmp/test_lsn_new"), 60000);
        assert_eq!(tracker.get(), 0);
        assert_eq!(tracker.file_path(), "/tmp/test_lsn_new");
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
        let tracker = LsnTracker::new_with_interval(Some(path), 60000);

        tracker.commit_lsn(12345678);
        assert!(tracker.is_dirty());

        tracker.persist_async().await.unwrap();

        // After persist, dirty flag should be cleared
        assert!(!tracker.is_dirty());

        // Verify file contents
        let contents = tokio::fs::read_to_string(path).await.unwrap();
        assert!(!contents.is_empty());

        // Cleanup
        let _ = tokio::fs::remove_file(path).await;
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

        // Write a test LSN
        tokio::fs::write(path, "0/ABCDEF").await.unwrap();

        let tracker = LsnTracker::new_with_interval(Some(path), 60000);
        let loaded = tracker.load_from_file().await;

        assert!(loaded.is_some());

        // Cleanup
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn test_new_with_load() {
        let path = "/tmp/test_lsn_with_load";

        // Write a test LSN
        tokio::fs::write(path, "0/123456").await.unwrap();

        let (tracker, lsn) = LsnTracker::new_with_load(Some(path)).await;

        assert!(lsn.is_some());
        assert!(tracker.get() > 0);

        // Cleanup
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn test_background_persistence_with_shutdown() {
        let path = "/tmp/test_lsn_bg_shutdown";
        
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
        let contents = tokio::fs::read_to_string(path).await.unwrap();
        assert!(!contents.is_empty());
        
        // Load from file to verify
        let (new_tracker, loaded_lsn) = LsnTracker::new_with_load(Some(path)).await;
        assert!(loaded_lsn.is_some());
        assert_eq!(new_tracker.get(), 400);
        
        // Cleanup
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn test_shutdown_without_background_task() {
        let path = "/tmp/test_lsn_no_bg_shutdown";
        let tracker = LsnTracker::new_with_interval(Some(path), 60000);
        
        // Commit LSN (background task is already running from new_with_interval)
        tracker.commit_lsn(999);
        
        // Shutdown should still persist
        tracker.shutdown_async().await;
        
        // Verify LSN was persisted
        let contents = tokio::fs::read_to_string(path).await.unwrap();
        assert!(!contents.is_empty());
        
        let (new_tracker, loaded_lsn) = LsnTracker::new_with_load(Some(path)).await;
        assert!(loaded_lsn.is_some());
        assert_eq!(new_tracker.get(), 999);
        
        // Cleanup
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn test_double_shutdown_is_safe() {
        let path = "/tmp/test_lsn_double_shutdown";
        let (arc_tracker, _) = LsnTracker::new_with_load(Some(path)).await;
        
        arc_tracker.commit_lsn(555);
        
        // First shutdown
        arc_tracker.shutdown_async().await;
        
        // Second shutdown should be safe (no-op)
        arc_tracker.shutdown_async().await;
        
        // Cleanup
        let _ = tokio::fs::remove_file(path).await;
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
}
