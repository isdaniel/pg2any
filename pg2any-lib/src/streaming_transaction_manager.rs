use crate::types::{ChangeEvent, Lsn, Transaction};
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Manages streaming transactions for protocol v2+ with batch support
/// This manager accumulates events and sends batches for high-performance processing.
pub(crate) struct StreamingTransactionManager {
    /// Active streaming transactions indexed by transaction ID
    /// Each entry contains the accumulated events for that transaction
    active_transactions: HashMap<u32, StreamingTransactionState>,

    /// Batch size for sending intermediate batches
    batch_size: usize,
}

/// State for a single streaming transaction
struct StreamingTransactionState {
    /// Transaction ID (for logging/debugging)
    #[allow(dead_code)]
    transaction_id: u32,

    /// Commit timestamp (from first StreamStart or will be updated at StreamCommit)
    commit_timestamp: chrono::DateTime<chrono::Utc>,

    /// Accumulated events for the current batch
    pending_events: Vec<ChangeEvent>,

    /// Last LSN seen for this transaction
    last_lsn: Option<Lsn>,

    /// Whether we're currently inside a stream segment (between StreamStart and StreamStop)
    in_stream_segment: bool,
}

impl StreamingTransactionManager {
    pub(crate) fn new(batch_size: usize) -> Self {
        Self {
            active_transactions: HashMap::new(),
            batch_size,
        }
    }

    #[inline]
    fn new_streaming_state(transaction_id: u32, lsn: Option<Lsn>) -> StreamingTransactionState {
        StreamingTransactionState {
            transaction_id,
            commit_timestamp: chrono::Utc::now(), // updated at commit
            pending_events: Vec::new(),
            last_lsn: lsn,
            in_stream_segment: true,
        }
    }

    /// Handle StreamStart event - opens a new stream segment
    pub(crate) fn handle_stream_start(
        &mut self,
        transaction_id: u32,
        first_segment: bool,
        lsn: Option<Lsn>,
    ) {
        if first_segment {
            info!(
                "Opening first stream segment for transaction {}",
                transaction_id
            );

            self.active_transactions.insert(
                transaction_id,
                Self::new_streaming_state(transaction_id, lsn),
            );
            return;
        }

        match self.active_transactions.get_mut(&transaction_id) {
            Some(state) => {
                info!(
                    "Opening continuation stream segment for transaction {}",
                    transaction_id
                );
                state.in_stream_segment = true;
                if lsn.is_some() {
                    state.last_lsn = lsn;
                }
            }
            None => {
                warn!(
                    "StreamStart for unknown transaction {}, creating new state",
                    transaction_id
                );
                self.active_transactions.insert(
                    transaction_id,
                    Self::new_streaming_state(transaction_id, lsn),
                );
            }
        }
    }

    /// Add an event to a streaming transaction
    pub(crate) fn add_event(&mut self, transaction_id: u32, event: ChangeEvent) {
        if let Some(state) = self.active_transactions.get_mut(&transaction_id) {
            if event.lsn.is_some() {
                state.last_lsn = event.lsn;
            }
            state.pending_events.push(event);
        } else {
            warn!("Event for unknown streaming transaction {}", transaction_id);
        }
    }

    /// Check if a streaming transaction has reached batch size
    pub(crate) fn should_send_batch(&self, transaction_id: u32) -> bool {
        if let Some(state) = self.active_transactions.get(&transaction_id) {
            state.pending_events.len() >= self.batch_size
        } else {
            false
        }
    }

    /// Take a batch of events from a streaming transaction (non-final batch)
    pub(crate) fn take_batch(&mut self, transaction_id: u32) -> Option<Transaction> {
        if let Some(state) = self.active_transactions.get_mut(&transaction_id) {
            if state.pending_events.is_empty() {
                return None;
            }

            let events = std::mem::take(&mut state.pending_events);
            let mut tx = Transaction::new_streaming(
                transaction_id,
                state.commit_timestamp,
                false, // Not final batch
            );
            tx.events = events;
            if let Some(lsn) = state.last_lsn {
                tx.set_commit_lsn(lsn);
            }

            debug!(
                "Taking batch of {} events for streaming transaction {}",
                tx.event_count(),
                transaction_id
            );
            Some(tx)
        } else {
            None
        }
    }

    /// Handle StreamStop event - closes current stream segment
    /// Returns a batch transaction if there are pending events and batch_size is reached
    pub(crate) fn handle_stream_stop(
        &mut self,
        transaction_id: u32,
        lsn: Option<Lsn>,
    ) -> Option<Transaction> {
        if let Some(state) = self.active_transactions.get_mut(&transaction_id) {
            debug!(
                "Closing stream segment for transaction {} with {} pending events",
                transaction_id,
                state.pending_events.len()
            );
            state.in_stream_segment = false;
            if lsn.is_some() {
                state.last_lsn = lsn;
            }

            // Send a batch if we have enough events accumulated
            if state.pending_events.len() >= self.batch_size {
                return self.take_batch(transaction_id);
            }
        }
        None
    }

    /// Handle StreamCommit - final commit of the streaming transaction
    /// Returns the final transaction with all remaining events
    pub(crate) fn handle_stream_commit(
        &mut self,
        transaction_id: u32,
        commit_timestamp: chrono::DateTime<chrono::Utc>,
        lsn: Option<Lsn>,
    ) -> Option<Transaction> {
        if let Some(state) = self.active_transactions.remove(&transaction_id) {
            debug!(
                "Committing streaming transaction {} with {} remaining events",
                transaction_id,
                state.pending_events.len()
            );

            // Create final transaction with remaining events
            let mut tx = Transaction::new_streaming(
                transaction_id,
                commit_timestamp,
                true, // Final batch
            );
            tx.events = state.pending_events;
            if let Some(l) = lsn {
                tx.set_commit_lsn(l);
            } else if let Some(l) = state.last_lsn {
                tx.set_commit_lsn(l);
            }

            Some(tx)
        } else {
            warn!("StreamCommit for unknown transaction {}", transaction_id);
            None
        }
    }

    /// Handle StreamAbort - transaction was rolled back
    pub(crate) fn handle_stream_abort(&mut self, transaction_id: u32) {
        if let Some(state) = self.active_transactions.remove(&transaction_id) {
            info!(
                "Aborting streaming transaction {} with {} events",
                transaction_id,
                state.pending_events.len()
            );
        } else {
            warn!("StreamAbort for unknown transaction {}", transaction_id);
        }
    }

    /// Check if there are any active streaming transactions
    pub(crate) fn has_active_transactions(&self) -> bool {
        !self.active_transactions.is_empty()
    }

    /// Clear all active transactions (for shutdown)
    pub(crate) fn clear(&mut self) -> usize {
        let count = self.active_transactions.len();
        self.active_transactions.clear();
        count
    }
}

/// Transaction processing context for the CDC client
pub(crate) enum TransactionContext {
    /// No active transaction
    None,
    /// Normal transaction (BEGIN...COMMIT)
    Normal(Transaction),
    /// Streaming transaction (StreamStart...StreamCommit)
    Streaming(u32), // transaction_id
}
