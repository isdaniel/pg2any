use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
// Re-export types from pg_walstream for convenience
pub use pg_walstream::{ChangeEvent, EventType, Lsn, ReplicaIdentity, RowData};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DestinationType {
    MySQL,
    SqlServer,
    SQLite,
}

impl std::fmt::Display for DestinationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DestinationType::MySQL => write!(f, "mysql"),
            DestinationType::SqlServer => write!(f, "sqlserver"),
            DestinationType::SQLite => write!(f, "sqlite"),
        }
    }
}

/// Represents a complete PostgreSQL transaction from BEGIN to COMMIT
///
/// A Transaction contains all the change events that occurred within a single
/// database transaction. Workers process entire transactions atomically to
/// ensure consistency at the destination.
///
/// When PostgreSQL streaming mode is enabled (protocol v2+), large in-progress
/// transactions are sent in chunks between `StreamStart` and `StreamStop` messages.
/// The final commit is signaled by `StreamCommit`. This struct supports both modes:
///
/// - **Normal Mode**: Events collected between BEGIN and COMMIT
/// - **Streaming Mode**: Events collected between StreamStart and StreamStop, with batch processing for high-performance ingestion
///
/// Both modes use batch-based processing where transactions are sent to the consumer
/// when event count reaches batch_size. The `is_final_batch` flag indicates when
/// to commit the database transaction.
#[derive(Debug, Serialize, Deserialize)]
pub struct Transaction {
    /// Transaction ID from PostgreSQL
    pub transaction_id: u32,

    /// Commit timestamp of the transaction
    pub commit_timestamp: DateTime<Utc>,

    /// LSN of the commit
    pub commit_lsn: Option<Lsn>,

    /// All change events in this transaction (INSERT, UPDATE, DELETE, TRUNCATE)
    /// Events are in the order they occurred within the transaction
    pub events: Vec<ChangeEvent>,

    /// Whether this is the final batch of a transaction
    /// When true, destination should commit the database transaction
    /// When false, destination should keep the transaction open for more batches
    #[serde(default = "default_is_final_batch")]
    pub is_final_batch: bool,
}

/// Default value for is_final_batch (true for backward compatibility)
fn default_is_final_batch() -> bool {
    true
}

impl Transaction {
    /// Create a new transaction with the given ID and timestamp
    pub fn new(transaction_id: u32, commit_timestamp: DateTime<Utc>) -> Self {
        Self {
            transaction_id,
            commit_timestamp,
            commit_lsn: None,
            events: Vec::new(),
            is_final_batch: true, // Normal transactions are final by default
        }
    }

    /// Add an event to this transaction
    pub fn add_event(&mut self, event: ChangeEvent) {
        self.events.push(event);
    }

    /// Set the commit LSN
    pub fn set_commit_lsn(&mut self, lsn: Lsn) {
        self.commit_lsn = Some(lsn);
    }

    /// Get the number of events in this transaction
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Check if this transaction is empty (no events)
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Mark this as the final batch of a transaction
    pub fn set_final_batch(&mut self, is_final: bool) {
        self.is_final_batch = is_final;
    }
}

/// Transaction processing context for the CDC client
///
/// This enum tracks the current transaction state during replication streaming.
/// It helps distinguish between normal transactions (BEGIN...COMMIT) and
/// streaming transactions (StreamStart...StreamCommit) for proper event handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionContext {
    /// No active transaction
    None,
    /// Normal transaction (BEGIN...COMMIT)
    Normal(u32), // transaction_id
    /// Streaming transaction (StreamStart...StreamCommit)
    Streaming(u32), // transaction_id
}

/// Tracks state of an in-flight normal (non-streaming) transaction.
/// Required because EventType::Commit doesn't include transaction_id,
/// so we must maintain the Begin context until Commit arrives.
#[derive(Debug, Clone)]
pub struct InFlightNormalTransaction {
    pub transaction_id: u32,
    pub event_count: usize,
}
