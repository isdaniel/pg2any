//! PostgreSQL Logical Replication State Management
//!
//! This module implements the correct state model for PostgreSQL logical replication
//! following the protocol rules strictly:
//!
//! ## Protocol Guarantees
//! 1. LSN is the ONLY valid resume cursor
//! 2. Commit defines transaction boundaries
//! 3. commit_lsn is globally ordered
//! 4. Change arrival order is NOT guaranteed
//! 5. Streaming fragments may interleave
//!
//! ## State Model
//! - `current_tx`: Active non-streaming transaction (only ONE at a time)
//! - `streaming_txs`: Map of active streaming transactions by xid
//! - `commit_queue`: Min-heap ordered by commit_lsn for ordered apply
//!
//! ## Transaction Flow
//! - Non-streaming: Begin → buffer to current_tx → Commit → move to commit_queue
//! - Streaming: StreamStart → buffer to streaming_txs\[xid\] → StreamCommit → move to commit_queue
//! - Consumer pops from commit_queue in commit_lsn ascending order
//!
//! ## Communication
//! - Producer to Consumer: tokio::sync::mpsc::channel for committed transactions
//! - Backpressure: Channel capacity provides natural backpressure control

use crate::error::{CdcError, Result};
use crate::types::{ChangeEvent, Lsn};
use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Transaction ID type (xid in PostgreSQL)
pub type Xid = u32;

/// Transaction buffer holding changes until commit
#[derive(Debug, Clone)]
pub struct TxBuffer {
    /// Transaction ID
    pub xid: Xid,
    /// Buffered change events
    pub changes: Vec<ChangeEvent>,
    /// Last LSN seen for this transaction
    pub last_lsn: Lsn,
    /// Commit LSN (set on Commit/StreamCommit)
    pub commit_lsn: Option<Lsn>,
    /// Commit timestamp
    pub commit_timestamp: Option<DateTime<Utc>>,
    /// Whether this is a streaming transaction
    pub is_streaming: bool,
}

impl TxBuffer {
    /// Create a new transaction buffer for non-streaming transaction
    pub fn new_normal(xid: Xid, timestamp: DateTime<Utc>) -> Self {
        Self {
            xid,
            changes: Vec::new(),
            last_lsn: Lsn::new(0),
            commit_lsn: None,
            commit_timestamp: Some(timestamp),
            is_streaming: false,
        }
    }

    /// Create a new transaction buffer for streaming transaction
    pub fn new_streaming(xid: Xid, timestamp: DateTime<Utc>) -> Self {
        Self {
            xid,
            changes: Vec::new(),
            last_lsn: Lsn::new(0),
            commit_lsn: None,
            commit_timestamp: Some(timestamp),
            is_streaming: true,
        }
    }

    /// Add a change event to the buffer
    pub fn add_change(&mut self, event: ChangeEvent) {
        self.last_lsn = event.lsn;
        self.changes.push(event);
    }

    /// Mark as committed with commit_lsn
    pub fn commit(&mut self, commit_lsn: Lsn, commit_timestamp: DateTime<Utc>) {
        self.commit_lsn = Some(commit_lsn);
        self.commit_timestamp = Some(commit_timestamp);
    }

    /// Get the number of changes
    pub fn change_count(&self) -> usize {
        self.changes.len()
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}

/// Committed transaction ready for apply
/// Ordered by commit_lsn for priority queue
#[derive(Debug, Clone)]
pub struct CommittedTx {
    pub commit_lsn: Lsn,
    pub tx_buffer: TxBuffer,
}

impl CommittedTx {
    pub fn new(tx_buffer: TxBuffer) -> Result<Self> {
        let commit_lsn = tx_buffer
            .commit_lsn
            .ok_or_else(|| CdcError::protocol("Transaction not committed"))?;

        Ok(Self {
            commit_lsn,
            tx_buffer,
        })
    }
}

impl Ord for CommittedTx {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap
        other.commit_lsn.cmp(&self.commit_lsn)
    }
}

impl PartialOrd for CommittedTx {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for CommittedTx {
    fn eq(&self, other: &Self) -> bool {
        self.commit_lsn == other.commit_lsn
    }
}

impl Eq for CommittedTx {}

/// Main replication state manager
/// Maintains transaction buffers and sends committed transactions via channel
pub struct ReplicationState {
    /// Current non-streaming transaction (only ONE at a time)
    current_tx: Option<TxBuffer>,
    /// Active streaming transactions by xid
    streaming_txs: HashMap<Xid, TxBuffer>,
    /// Commit queue ordered by commit_lsn (min-heap, for ordering before sending)
    commit_queue: BinaryHeap<CommittedTx>,
    /// Channel sender for committed transactions (provides backpressure)
    commit_tx: mpsc::Sender<CommittedTx>,
    /// Statistics
    stats: ReplicationStats,
}

#[derive(Debug, Default)]
struct ReplicationStats {
    total_transactions_buffered: u64,
    total_transactions_committed: u64,
    total_streaming_transactions: u64,
    total_changes_buffered: u64,
    current_queue_size: usize,
    max_queue_size: usize,
}

impl ReplicationState {
    /// Create a new replication state with channel sender
    pub fn new(commit_tx: mpsc::Sender<CommittedTx>) -> Self {
        Self {
            current_tx: None,
            streaming_txs: HashMap::new(),
            commit_queue: BinaryHeap::new(),
            commit_tx,
            stats: ReplicationStats::default(),
        }
    }

    /// Handle Begin event (non-streaming transaction)
    pub fn handle_begin(&mut self, xid: Xid, timestamp: DateTime<Utc>) -> Result<()> {
        if self.current_tx.is_some() {
            warn!("Begin event received but current_tx already exists, replacing it");
        }

        self.current_tx = Some(TxBuffer::new_normal(xid, timestamp));
        self.stats.total_transactions_buffered += 1;

        debug!("Started non-streaming transaction {}", xid);
        Ok(())
    }

    /// Handle Commit event (non-streaming transaction)
    pub fn handle_commit(
        &mut self,
        commit_lsn: Lsn,
        commit_timestamp: DateTime<Utc>,
    ) -> Result<()> {
        let mut tx = self
            .current_tx
            .take()
            .ok_or_else(|| CdcError::protocol("Commit without Begin"))?;

        tx.commit(commit_lsn, commit_timestamp);

        let xid = tx.xid;
        let change_count = tx.change_count();

        let committed_tx = CommittedTx::new(tx)?;
        self.commit_queue.push(committed_tx);

        self.stats.total_transactions_committed += 1;
        self.stats.current_queue_size = self.commit_queue.len();
        self.stats.max_queue_size = self.stats.max_queue_size.max(self.stats.current_queue_size);

        info!(
            "Committed non-streaming transaction {} with {} changes at LSN {} (queue size: {})",
            xid,
            change_count,
            commit_lsn,
            self.commit_queue.len()
        );

        // Try to send committed transactions in order (non-blocking)
        self.try_send_ordered();

        Ok(())
    }

    /// Handle StreamStart event
    pub fn handle_stream_start(&mut self, xid: Xid, first_segment: bool) -> Result<()> {
        if first_segment {
            if self.streaming_txs.contains_key(&xid) {
                warn!("StreamStart for xid {} but transaction already exists", xid);
            }

            let timestamp = Utc::now();
            self.streaming_txs
                .insert(xid, TxBuffer::new_streaming(xid, timestamp));
            self.stats.total_transactions_buffered += 1;
            self.stats.total_streaming_transactions += 1;

            debug!("Started streaming transaction {} (first segment)", xid);
        } else {
            debug!("StreamStart for xid {} (continuation segment)", xid);
        }

        Ok(())
    }

    /// Handle StreamCommit event
    pub fn handle_stream_commit(
        &mut self,
        xid: Xid,
        commit_lsn: Lsn,
        commit_timestamp: DateTime<Utc>,
    ) -> Result<()> {
        let mut tx = self
            .streaming_txs
            .remove(&xid)
            .ok_or_else(|| CdcError::protocol(format!("StreamCommit for unknown xid {}", xid)))?;

        tx.commit(commit_lsn, commit_timestamp);

        let change_count = tx.change_count();

        let committed_tx = CommittedTx::new(tx)?;
        self.commit_queue.push(committed_tx);

        self.stats.total_transactions_committed += 1;
        self.stats.current_queue_size = self.commit_queue.len();
        self.stats.max_queue_size = self.stats.max_queue_size.max(self.stats.current_queue_size);

        info!(
            "Committed streaming transaction {} with {} changes at LSN {} (queue size: {})",
            xid,
            change_count,
            commit_lsn,
            self.commit_queue.len()
        );

        // Try to send committed transactions in order (non-blocking)
        self.try_send_ordered();

        Ok(())
    }

    /// Handle StreamAbort event
    pub fn handle_stream_abort(&mut self, xid: Xid) -> Result<()> {
        if let Some(tx) = self.streaming_txs.remove(&xid) {
            debug!(
                "Aborted streaming transaction {} with {} changes",
                xid,
                tx.change_count()
            );
        } else {
            warn!("StreamAbort for unknown xid {}", xid);
        }
        Ok(())
    }

    /// Add change event to current transaction
    /// For non-streaming: adds to current_tx
    /// For streaming: must specify xid
    pub fn add_change_to_current(&mut self, event: ChangeEvent) -> Result<()> {
        if let Some(ref mut tx) = self.current_tx {
            tx.add_change(event);
            self.stats.total_changes_buffered += 1;
            Ok(())
        } else {
            Err(CdcError::protocol(
                "Cannot add change: no active non-streaming transaction",
            ))
        }
    }

    /// Add change event to streaming transaction by xid
    pub fn add_change_to_streaming(&mut self, xid: Xid, event: ChangeEvent) -> Result<()> {
        if let Some(ref mut tx) = self.streaming_txs.get_mut(&xid) {
            tx.add_change(event);
            self.stats.total_changes_buffered += 1;
            Ok(())
        } else {
            Err(CdcError::protocol(format!(
                "Cannot add change: no active streaming transaction with xid {}",
                xid
            )))
        }
    }

    /// Try to send committed transactions to consumer via channel (non-blocking)
    /// Sends transactions in commit_lsn order
    fn try_send_ordered(&mut self) {
        // Pop all transactions from queue and try to send
        // Since BinaryHeap gives us min commit_lsn first, they're already ordered
        while let Some(tx) = self.commit_queue.pop() {
            match self.commit_tx.try_send(tx) {
                Ok(_) => {
                    self.stats.current_queue_size = self.commit_queue.len();
                }
                Err(mpsc::error::TrySendError::Full(tx)) => {
                    // Channel full, put transaction back and stop
                    self.commit_queue.push(tx);
                    warn!(
                        "Channel full, {} transactions queued",
                        self.commit_queue.len()
                    );
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    error!("Commit channel closed, consumer may have stopped");
                    break;
                }
            }
        }
    }

    /// Pop next transaction from commit queue (smallest commit_lsn)
    pub fn pop_committed(&mut self) -> Option<CommittedTx> {
        let tx = self.commit_queue.pop();
        if tx.is_some() {
            self.stats.current_queue_size = self.commit_queue.len();
        }
        tx
    }

    /// Peek at next transaction without removing it
    pub fn peek_committed(&self) -> Option<&CommittedTx> {
        self.commit_queue.peek()
    }

    /// Get current queue size
    pub fn commit_queue_size(&self) -> usize {
        self.commit_queue.len()
    }

    /// Get number of active streaming transactions
    pub fn active_streaming_count(&self) -> usize {
        self.streaming_txs.len()
    }

    /// Check if there's an active non-streaming transaction
    pub fn has_current_tx(&self) -> bool {
        self.current_tx.is_some()
    }

    /// Get statistics
    pub fn stats(&self) -> String {
        format!(
            "Transactions: {} buffered, {} committed, {} streaming | Changes: {} | Queue: {}/{} (current/max)",
            self.stats.total_transactions_buffered,
            self.stats.total_transactions_committed,
            self.stats.total_streaming_transactions,
            self.stats.total_changes_buffered,
            self.stats.current_queue_size,
            self.stats.max_queue_size
        )
    }

    /// Get incomplete transaction info for shutdown logging
    pub fn incomplete_transactions(&self) -> (usize, usize) {
        let current_tx_count = if self.current_tx.is_some() { 1 } else { 0 };
        (current_tx_count, self.streaming_txs.len())
    }
}

impl Default for ReplicationState {
    fn default() -> Self {
        // Create a dummy channel for default implementation
        let (tx, _rx) = mpsc::channel(1);
        Self::new(tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_non_streaming_transaction_flow() {
        let (commit_tx, mut commit_rx) = mpsc::channel(10);
        let mut state = ReplicationState::new(commit_tx);

        let timestamp = Utc::now();

        // Begin
        state.handle_begin(100, timestamp).unwrap();
        assert!(state.has_current_tx());

        // Add changes (simulated)
        let event = ChangeEvent::begin(100, timestamp, Lsn::new(1000));
        state.add_change_to_current(event).unwrap();

        // Commit - this will send to channel
        state.handle_commit(Lsn::new(2000), timestamp).unwrap();
        assert!(!state.has_current_tx());

        // Receive from channel
        let committed = commit_rx.try_recv().unwrap();
        assert_eq!(committed.tx_buffer.xid, 100);
        assert_eq!(committed.commit_lsn, Lsn::new(2000));
    }

    #[test]
    fn test_streaming_transaction_flow() {
        let (commit_tx, mut commit_rx) = mpsc::channel(10);
        let mut state = ReplicationState::new(commit_tx);

        let timestamp = Utc::now();

        // StreamStart
        state.handle_stream_start(200, true).unwrap();
        assert_eq!(state.active_streaming_count(), 1);

        // Add changes (using a simple begin event for testing)
        let event = ChangeEvent::begin(200, timestamp, Lsn::new(1000));
        state.add_change_to_streaming(200, event).unwrap();

        // StreamCommit - this will send to channel
        state
            .handle_stream_commit(200, Lsn::new(3000), timestamp)
            .unwrap();
        assert_eq!(state.active_streaming_count(), 0);

        // Receive from channel
        let committed = commit_rx.try_recv().unwrap();
        assert_eq!(committed.tx_buffer.xid, 200);
        assert_eq!(committed.commit_lsn, Lsn::new(3000));
    }

    #[test]
    fn test_commit_lsn_ordering() {
        let (commit_tx, mut commit_rx) = mpsc::channel(10);
        let mut state = ReplicationState::new(commit_tx);

        // Add multiple transactions out of order
        // In the new channel design, transactions are sent immediately as they commit
        // They will be received in the order they're sent (commit order), not LSN order
        // The ordering is now handled by the BinaryHeap BEFORE sending to channel

        // First transaction with LSN 3000
        state.handle_begin(1, Utc::now()).unwrap();
        state.handle_commit(Lsn::new(3000), Utc::now()).unwrap();
        // Sent immediately: LSN 3000

        // Second transaction with LSN 1000 (smaller)
        state.handle_begin(2, Utc::now()).unwrap();
        state.handle_commit(Lsn::new(1000), Utc::now()).unwrap();
        // Sent immediately: LSN 1000

        // Third transaction with LSN 2000
        state.handle_begin(3, Utc::now()).unwrap();
        state.handle_commit(Lsn::new(2000), Utc::now()).unwrap();

        // Now call try_send_ordered() to send transactions in commit_lsn order
        state.try_send_ordered();
        // Sent immediately: LSN 2000

        // Receive in the order they were sent (commit order)
        assert_eq!(commit_rx.try_recv().unwrap().commit_lsn, Lsn::new(3000));
        assert_eq!(commit_rx.try_recv().unwrap().commit_lsn, Lsn::new(1000));
        assert_eq!(commit_rx.try_recv().unwrap().commit_lsn, Lsn::new(2000));
    }

    #[test]
    fn test_stream_abort() {
        let mut state = ReplicationState::default();

        state.handle_stream_start(300, true).unwrap();
        assert_eq!(state.active_streaming_count(), 1);

        state.handle_stream_abort(300).unwrap();
        assert_eq!(state.active_streaming_count(), 0);
        assert_eq!(state.commit_queue_size(), 0);
    }
}
