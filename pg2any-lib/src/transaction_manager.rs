use crate::types::{ChangeEvent, Lsn, Transaction};
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Unified transaction manager that handles both normal (BEGIN/COMMIT) and
/// streaming (StreamStart/StreamStop/StreamCommit/StreamAbort) transactions.
///
/// ## Design
/// - Both transaction types are stored in the same HashMap and processed identically
/// - When event count reaches batch_size, a batch is sent to the consumer
/// - LSN is updated on each batch send for graceful shutdown and preventing double consumption
/// - Only the is_final_batch flag matters - when true, destination commits the transaction
///
/// ## Normal Transactions (BEGIN...COMMIT)
/// - BEGIN: Creates a new transaction entry
/// - DML events: Added to the transaction, batch sent if batch_size reached
/// - COMMIT: Sends final batch with is_final_batch=true
///
/// ## Streaming Transactions (StreamStart...StreamCommit)
/// - StreamStart: Creates a new transaction entry (if first_segment) or continues existing
/// - DML events: Added to the transaction, batch sent if batch_size reached
/// - StreamStop: Marks end of segment, sends batch if batch_size reached
/// - StreamCommit: Sends final batch with is_final_batch=true
/// - StreamAbort: Removes transaction (rollback)
pub(crate) struct TransactionManager {
    /// Active transactions indexed by transaction ID
    /// Contains both normal and streaming transactions
    active_transactions: HashMap<u32, TransactionState>,

    /// Batch size for sending intermediate batches
    batch_size: usize,
}

/// State for a single transaction (normal or streaming)
struct TransactionState {
    /// Transaction ID
    #[allow(dead_code)]
    transaction_id: u32,

    /// Commit timestamp
    commit_timestamp: chrono::DateTime<chrono::Utc>,

    /// Accumulated events for the current batch
    pending_events: Vec<ChangeEvent>,

    /// Last LSN seen for this transaction
    last_lsn: Option<Lsn>,

    /// For streaming: whether we're inside a stream segment
    in_stream_segment: bool,
}

impl TransactionManager {
    pub(crate) fn new(batch_size: usize) -> Self {
        Self {
            active_transactions: HashMap::new(),
            batch_size,
        }
    }

    #[inline]
    fn new_state(
        transaction_id: u32,
        commit_timestamp: chrono::DateTime<chrono::Utc>,
        lsn: Option<Lsn>,
        is_streaming: bool,
    ) -> TransactionState {
        TransactionState {
            transaction_id,
            commit_timestamp,
            pending_events: Vec::new(),
            last_lsn: lsn,
            in_stream_segment: is_streaming, // Streaming starts in segment
        }
    }

    /// Handle BEGIN event - creates a new normal transaction
    pub(crate) fn handle_begin(
        &mut self,
        transaction_id: u32,
        commit_timestamp: chrono::DateTime<chrono::Utc>,
        lsn: Option<Lsn>,
    ) {
        debug!(
            "BEGIN: Creating normal transaction {} at {:?}",
            transaction_id, commit_timestamp
        );
        self.active_transactions.insert(
            transaction_id,
            Self::new_state(transaction_id, commit_timestamp, lsn, false),
        );
    }

    /// Handle COMMIT event - returns final transaction with all remaining events
    pub(crate) fn handle_commit(
        &mut self,
        transaction_id: u32,
        lsn: Option<Lsn>,
    ) -> Option<Transaction> {
        if let Some(state) = self.active_transactions.remove(&transaction_id) {
            debug!(
                "COMMIT: Finalizing transaction {} with {} events",
                transaction_id,
                state.pending_events.len()
            );

            let mut tx = Transaction::new(transaction_id, state.commit_timestamp);
            tx.events = state.pending_events;
            tx.is_final_batch = true;

            if let Some(l) = lsn {
                tx.set_commit_lsn(l);
            } else if let Some(l) = state.last_lsn {
                tx.set_commit_lsn(l);
            }

            Some(tx)
        } else {
            warn!("COMMIT for unknown transaction {}", transaction_id);
            None
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
                "StreamStart: Opening first segment for transaction {}",
                transaction_id
            );
            self.active_transactions.insert(
                transaction_id,
                Self::new_state(transaction_id, chrono::Utc::now(), lsn, true),
            );
            return;
        }

        match self.active_transactions.get_mut(&transaction_id) {
            Some(state) => {
                debug!(
                    "StreamStart: Continuing segment for transaction {}",
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
                    Self::new_state(transaction_id, chrono::Utc::now(), lsn, true),
                );
            }
        }
    }

    /// Handle StreamStop event - closes current stream segment
    /// Returns a batch transaction if batch_size is reached
    pub(crate) fn handle_stream_stop(
        &mut self,
        transaction_id: u32,
        lsn: Option<Lsn>,
    ) -> Option<Transaction> {
        if let Some(state) = self.active_transactions.get_mut(&transaction_id) {
            debug!(
                "StreamStop: Closing segment for transaction {} with {} pending events",
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
                "StreamCommit: Finalizing streaming transaction {} with {} events",
                transaction_id,
                state.pending_events.len()
            );

            let mut tx = Transaction::new_streaming(transaction_id, commit_timestamp, true);
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
                "StreamAbort: Aborting transaction {} with {} events",
                transaction_id,
                state.pending_events.len()
            );
        } else {
            warn!("StreamAbort for unknown transaction {}", transaction_id);
        }
    }

    /// Add an event to a transaction (normal or streaming)
    pub(crate) fn add_event(&mut self, transaction_id: u32, event: ChangeEvent) {
        if let Some(state) = self.active_transactions.get_mut(&transaction_id) {
            if event.lsn.is_some() {
                state.last_lsn = event.lsn;
            }
            state.pending_events.push(event);
        } else {
            warn!("Event for unknown transaction {}", transaction_id);
        }
    }

    /// Check if a transaction has reached batch size
    pub(crate) fn should_send_batch(&self, transaction_id: u32) -> bool {
        if let Some(state) = self.active_transactions.get(&transaction_id) {
            state.pending_events.len() >= self.batch_size
        } else {
            false
        }
    }

    /// Take a batch of events from a transaction and commit it
    /// Updates LSN for graceful shutdown recovery
    pub(crate) fn take_batch(&mut self, transaction_id: u32) -> Option<Transaction> {
        if let Some(state) = self.active_transactions.get_mut(&transaction_id) {
            if state.pending_events.is_empty() {
                return None;
            }

            let events = std::mem::take(&mut state.pending_events);
            let mut tx = Transaction::new_batch(transaction_id, state.commit_timestamp, true);
            tx.events = events;

            if let Some(lsn) = state.last_lsn {
                tx.set_commit_lsn(lsn);
            }

            debug!(
                "Taking batch of {} events for transaction {}",
                tx.event_count(),
                transaction_id
            );
            Some(tx)
        } else {
            None
        }
    }

    /// Get event count for a transaction
    pub(crate) fn event_count(&self, transaction_id: u32) -> usize {
        self.active_transactions
            .get(&transaction_id)
            .map(|s| s.pending_events.len())
            .unwrap_or(0)
    }

    /// Check if there are any active transactions
    pub(crate) fn has_active_transactions(&self) -> bool {
        !self.active_transactions.is_empty()
    }

    /// Clear all active transactions (for shutdown)
    /// Returns the count of cleared transactions
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
    Normal(u32), // transaction_id
    /// Streaming transaction (StreamStart...StreamCommit)
    Streaming(u32), // transaction_id
}
