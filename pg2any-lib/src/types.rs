use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::pg_replication::format_lsn;

/// Represents the type of change event
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    Insert {
        schema: String,
        table: String,
        relation_oid: u32,
        data: HashMap<String, serde_json::Value>,
    },
    Update {
        schema: String,
        table: String,
        relation_oid: u32,
        old_data: Option<HashMap<String, serde_json::Value>>,
        new_data: HashMap<String, serde_json::Value>,
        replica_identity: ReplicaIdentity,
        key_columns: Vec<String>,
    },
    Delete {
        schema: String,
        table: String,
        relation_oid: u32,
        old_data: HashMap<String, serde_json::Value>,
        replica_identity: ReplicaIdentity,
        key_columns: Vec<String>,
    },
    Truncate(Vec<String>),
    Begin {
        transaction_id: u32,
        commit_timestamp: DateTime<Utc>,
    },
    Commit {
        commit_timestamp: DateTime<Utc>,
    },
    /// Streaming transaction start (protocol v2+)
    StreamStart {
        transaction_id: u32,
        first_segment: bool,
    },
    /// Streaming transaction stop (end of current segment)
    StreamStop,
    /// Streaming transaction commit (final commit of streamed transaction)
    StreamCommit {
        transaction_id: u32,
        commit_timestamp: DateTime<Utc>,
    },
    /// Streaming transaction abort
    StreamAbort {
        transaction_id: u32,
    },
    Relation,
    Type,
    Origin,
    Message,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DestinationType {
    MySQL,
    SqlServer,
    // Future support for other databases
    PostgreSQL,
    SQLite,
}

impl std::fmt::Display for DestinationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DestinationType::MySQL => write!(f, "mysql"),
            DestinationType::SqlServer => write!(f, "sqlserver"),
            DestinationType::PostgreSQL => write!(f, "postgresql"),
            DestinationType::SQLite => write!(f, "sqlite"),
        }
    }
}

/// Represents a single change event from PostgreSQL logical replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Type of the event with embedded data
    pub event_type: EventType,

    /// LSN (Log Sequence Number) position
    pub lsn: Option<Lsn>,

    /// Additional metadata
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

impl ChangeEvent {
    /// Create a new INSERT event
    pub fn insert(
        schema_name: String,
        table_name: String,
        relation_oid: u32,
        data: HashMap<String, serde_json::Value>,
    ) -> Self {
        Self {
            event_type: EventType::Insert {
                schema: schema_name,
                table: table_name,
                relation_oid,
                data,
            },
            lsn: None,
            metadata: None,
        }
    }

    /// Create a new UPDATE event
    pub fn update(
        schema_name: String,
        table_name: String,
        relation_oid: u32,
        old_data: Option<HashMap<String, serde_json::Value>>,
        new_data: HashMap<String, serde_json::Value>,
        replica_identity: ReplicaIdentity,
        key_columns: Vec<String>,
    ) -> Self {
        Self {
            event_type: EventType::Update {
                schema: schema_name,
                table: table_name,
                relation_oid,
                old_data,
                new_data,
                replica_identity,
                key_columns,
            },
            lsn: None,
            metadata: None,
        }
    }

    /// Create a new DELETE event
    pub fn delete(
        schema_name: String,
        table_name: String,
        relation_oid: u32,
        old_data: HashMap<String, serde_json::Value>,
        replica_identity: ReplicaIdentity,
        key_columns: Vec<String>,
    ) -> Self {
        Self {
            event_type: EventType::Delete {
                schema: schema_name,
                table: table_name,
                relation_oid,
                old_data,
                replica_identity,
                key_columns,
            },
            lsn: None,
            metadata: None,
        }
    }

    /// Create a BEGIN transaction event
    pub fn begin(transaction_id: u32, commit_timestamp: DateTime<Utc>) -> Self {
        Self {
            event_type: EventType::Begin {
                transaction_id,
                commit_timestamp,
            },
            lsn: None,
            metadata: None,
        }
    }

    /// Create a COMMIT transaction event
    pub fn commit(_transaction_id: u32, commit_timestamp: DateTime<Utc>) -> Self {
        Self {
            event_type: EventType::Commit { commit_timestamp },
            lsn: None,
            metadata: None,
        }
    }

    /// Create a TRUNCATE event
    pub fn truncate(tables: Vec<String>) -> Self {
        Self {
            event_type: EventType::Truncate(tables),
            lsn: None,
            metadata: None,
        }
    }

    /// Create a RELATION event
    pub fn relation() -> Self {
        Self {
            event_type: EventType::Relation,
            lsn: None,
            metadata: None,
        }
    }

    /// Create a TYPE event
    pub fn type_event() -> Self {
        Self {
            event_type: EventType::Type,
            lsn: None,
            metadata: None,
        }
    }

    /// Create an ORIGIN event
    pub fn origin() -> Self {
        Self {
            event_type: EventType::Origin,
            lsn: None,
            metadata: None,
        }
    }

    /// Create a MESSAGE event
    pub fn message() -> Self {
        Self {
            event_type: EventType::Message,
            lsn: None,
            metadata: None,
        }
    }

    /// Set metadata for this event
    pub fn with_metadata(mut self, metadata: HashMap<String, serde_json::Value>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Get key columns from UPDATE or DELETE events
    pub fn get_key_columns(&self) -> Option<&Vec<String>> {
        match &self.event_type {
            EventType::Update { key_columns, .. } => Some(key_columns),
            EventType::Delete { key_columns, .. } => Some(key_columns),
            _ => None,
        }
    }

    /// Get replica identity from UPDATE or DELETE events
    pub fn get_replica_identity(&self) -> Option<&ReplicaIdentity> {
        match &self.event_type {
            EventType::Update {
                replica_identity, ..
            } => Some(replica_identity),
            EventType::Delete {
                replica_identity, ..
            } => Some(replica_identity),
            _ => None,
        }
    }

    pub fn event_type_str(&self) -> &str {
        match self.event_type {
            EventType::Insert { .. } => "insert",
            EventType::Update { .. } => "update",
            EventType::Delete { .. } => "delete",
            EventType::Truncate(_) => "truncate",
            _ => "other",
        }
    }
}

/// Represents a PostgreSQL logical replication message
#[derive(Debug, Clone)]
pub struct ReplicationMessage {
    /// The raw message type byte
    pub message_type: u8,

    /// The decoded message payload
    pub payload: Vec<u8>,

    /// WAL position
    pub wal_position: u64,

    /// Server clock when message was created
    pub server_time: DateTime<Utc>,
}

/// PostgreSQL replica identity settings
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaIdentity {
    /// Default replica identity (primary key)
    Default,

    /// No replica identity  
    Nothing,

    /// Full replica identity (all columns)
    Full,

    /// Index-based replica identity
    Index,
}

impl ReplicaIdentity {
    /// Create replica identity from byte
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            b'd' => Some(ReplicaIdentity::Default),
            b'n' => Some(ReplicaIdentity::Nothing),
            b'f' => Some(ReplicaIdentity::Full),
            b'i' => Some(ReplicaIdentity::Index),
            _ => None,
        }
    }
}

/// PostgreSQL type information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeInfo {
    /// Type OID
    pub type_oid: u32,

    /// Type name
    pub type_name: String,

    /// Namespace name
    pub namespace: String,
}

/// Transaction information
#[derive(Debug, Clone)]
pub struct TransactionInfo {
    /// Transaction ID
    pub xid: u32,

    /// Commit timestamp
    pub commit_timestamp: DateTime<Utc>,

    /// Transaction start LSN
    pub start_lsn: u64,

    /// Transaction commit LSN
    pub commit_lsn: u64,
}

/// LSN (Log Sequence Number) representation
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Lsn(pub u64);

impl Lsn {
    /// Create a new LSN from a u64 value
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Get the raw u64 value
    pub fn value(&self) -> u64 {
        self.0
    }
}

/// Parse LSN from PostgreSQL string format (e.g., "16/B374D848")
impl std::str::FromStr for Lsn {
    type Err = crate::error::CdcError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 2 {
            return Err(crate::error::CdcError::protocol("Invalid LSN format"));
        }

        let high = u64::from_str_radix(parts[0], 16)
            .map_err(|_| crate::error::CdcError::protocol("Invalid LSN high part"))?;
        let low = u64::from_str_radix(parts[1], 16)
            .map_err(|_| crate::error::CdcError::protocol("Invalid LSN low part"))?;

        Ok(Self((high << 32) | low))
    }
}

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format_lsn(self.0))
    }
}

impl From<u64> for Lsn {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Lsn> for u64 {
    fn from(lsn: Lsn) -> Self {
        lsn.0
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

    /// Create a new transaction batch (can be used for both normal and streaming)
    pub fn new_batch(transaction_id: u32, commit_timestamp: DateTime<Utc>, is_final: bool) -> Self {
        Self {
            transaction_id,
            commit_timestamp,
            commit_lsn: None,
            events: Vec::new(),
            is_final_batch: is_final,
        }
    }

    /// Create a new streaming transaction batch (alias for new_batch for compatibility)
    pub fn new_streaming(
        transaction_id: u32,
        commit_timestamp: DateTime<Utc>,
        is_final: bool,
    ) -> Self {
        Self::new_batch(transaction_id, commit_timestamp, is_final)
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

    /// Take all events from this transaction, leaving it empty
    pub fn take_events(&mut self) -> Vec<ChangeEvent> {
        std::mem::take(&mut self.events)
    }
}
