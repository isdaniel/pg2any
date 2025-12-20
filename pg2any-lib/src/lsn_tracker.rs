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

use crate::pg_replication::XLogRecPtr;
use crate::types::Lsn;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};

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
/// and provides atomic persistence to ensure no data loss on graceful shutdown.
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
    /// Path to the LSN persistence file
    lsn_file_path: String,
}

impl LsnTracker {
    /// Create a new LSN tracker with the specified file path
    pub fn new(lsn_file_path: Option<&str>) -> Self {
        let path = lsn_file_path
            .map(String::from)
            .or_else(|| std::env::var("CDC_LAST_LSN_FILE").ok())
            .unwrap_or_else(|| "./pg2any_last_lsn".to_string());

        Self {
            last_committed_lsn: AtomicU64::new(0),
            lsn_file_path: path,
        }
    }

    /// Create a new LSN tracker and load the initial value from file
    ///
    /// This is the preferred way to create a tracker that should resume
    /// from a previously persisted LSN.
    pub fn new_with_load(lsn_file_path: Option<&str>) -> (Self, Option<Lsn>) {
        let tracker = Self::new(lsn_file_path);
        let loaded_lsn = tracker.load_from_file();

        // Initialize the atomic with the loaded value
        if let Some(lsn) = loaded_lsn {
            tracker.last_committed_lsn.store(lsn.0, Ordering::Release);
        }

        (tracker, loaded_lsn)
    }

    /// Load LSN from the persistence file
    ///
    /// Returns `Some(Lsn)` if a valid LSN was found and parsed, `None` otherwise.
    /// Logs warnings for parsing errors but doesn't fail.
    pub fn load_from_file(&self) -> Option<Lsn> {
        match std::fs::read_to_string(&self.lsn_file_path) {
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
                    return;
                }
                Err(_) => continue, // Retry if another thread updated concurrently
            }
        }
    }

    /// Update and persist the LSN atomically
    ///
    /// This should be called after each successful transaction commit to the destination.
    /// It updates the in-memory LSN and persists it to disk.
    pub fn commit_lsn(&self, lsn: u64) {
        self.update_if_greater(lsn);
        self.persist_async();
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

    /// Persist the current LSN to file
    ///
    /// This method writes the LSN atomically by first writing to a temp file
    /// and then renaming to ensure crash safety.
    pub fn persist(&self) -> std::io::Result<()> {
        let lsn = self.get();
        if lsn == 0 {
            tracing::debug!("Skipping LSN persistence: no committed LSN yet");
            return Ok(());
        }

        let lsn_str = crate::pg_replication::format_lsn(lsn);

        // Write to temp file first for atomic write
        let temp_path = format!("{}.tmp", self.lsn_file_path);
        std::fs::write(&temp_path, &lsn_str)?;
        std::fs::rename(&temp_path, &self.lsn_file_path)?;

        info!(
            "Persisted committed LSN {} to {}",
            lsn_str, self.lsn_file_path
        );
        Ok(())
    }

    /// Persist the current LSN to file (non-blocking, logs errors)
    pub fn persist_async(&self) {
        if let Err(e) = self.persist() {
            warn!("Failed to persist LSN: {}", e);
        }
    }

    /// Get the file path for LSN persistence
    pub fn file_path(&self) -> &str {
        &self.lsn_file_path
    }
}

impl Clone for LsnTracker {
    fn clone(&self) -> Self {
        Self {
            last_committed_lsn: AtomicU64::new(self.last_committed_lsn.load(Ordering::Acquire)),
            lsn_file_path: self.lsn_file_path.clone(),
        }
    }
}

/// Create a shared LSN tracker with initial load from file
pub fn create_lsn_tracker_with_load(lsn_file_path: Option<&str>) -> (Arc<LsnTracker>, Option<Lsn>) {
    let (tracker, lsn) = LsnTracker::new_with_load(lsn_file_path);
    (Arc::new(tracker), lsn)
}

#[cfg(test)]
mod lsn_tracker_tests {
    use super::*;

    #[test]
    fn test_lsn_tracker_new() {
        let tracker = LsnTracker::new(Some("/tmp/test_lsn"));
        assert_eq!(tracker.get(), 0);
        assert_eq!(tracker.file_path(), "/tmp/test_lsn");
    }

    #[test]
    fn test_lsn_tracker_update_if_greater() {
        let tracker = LsnTracker::new(Some("/tmp/test_lsn"));

        tracker.update_if_greater(100);
        assert_eq!(tracker.get(), 100);

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

    #[test]
    fn test_lsn_tracker_get_lsn() {
        let tracker = LsnTracker::new(Some("/tmp/test_lsn"));

        assert_eq!(tracker.get_lsn(), None);

        tracker.update_if_greater(100);
        assert_eq!(tracker.get_lsn(), Some(Lsn(100)));
    }

    #[test]
    fn test_lsn_tracker_clone() {
        let tracker = LsnTracker::new(Some("/tmp/test_lsn"));
        tracker.update_if_greater(42);

        let cloned = tracker.clone();
        assert_eq!(cloned.get(), 42);
        assert_eq!(cloned.file_path(), "/tmp/test_lsn");
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
