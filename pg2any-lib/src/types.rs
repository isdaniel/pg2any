use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents the type of change event
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    Insert,
    Update,
    Delete,
    Truncate,
    Begin,
    Commit,
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

/// Represents a single change event from PostgreSQL logical replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Type of the event
    pub event_type: EventType,
    
    /// Transaction ID
    pub transaction_id: Option<u32>,
    
    /// Commit timestamp
    pub commit_timestamp: Option<DateTime<Utc>>,
    
    /// Schema name
    pub schema_name: Option<String>,
    
    /// Table name
    pub table_name: Option<String>,
    
    /// Relation OID
    pub relation_oid: Option<u32>,
    
    /// Column data before the change (for UPDATE and DELETE)
    pub old_data: Option<HashMap<String, serde_json::Value>>,
    
    /// Column data after the change (for INSERT and UPDATE)
    pub new_data: Option<HashMap<String, serde_json::Value>>,
    
    /// LSN (Log Sequence Number) position
    pub lsn: Option<String>,
    
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
            event_type: EventType::Insert,
            transaction_id: None,
            commit_timestamp: None,
            schema_name: Some(schema_name),
            table_name: Some(table_name),
            relation_oid: Some(relation_oid),
            old_data: None,
            new_data: Some(data),
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
    ) -> Self {
        Self {
            event_type: EventType::Update,
            transaction_id: None,
            commit_timestamp: None,
            schema_name: Some(schema_name),
            table_name: Some(table_name),
            relation_oid: Some(relation_oid),
            old_data,
            new_data: Some(new_data),
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
    ) -> Self {
        Self {
            event_type: EventType::Delete,
            transaction_id: None,
            commit_timestamp: None,
            schema_name: Some(schema_name),
            table_name: Some(table_name),
            relation_oid: Some(relation_oid),
            old_data: Some(old_data),
            new_data: None,
            lsn: None,
            metadata: None,
        }
    }

    /// Create a BEGIN transaction event
    pub fn begin(transaction_id: u32, commit_timestamp: DateTime<Utc>) -> Self {
        Self {
            event_type: EventType::Begin,
            transaction_id: Some(transaction_id),
            commit_timestamp: Some(commit_timestamp),
            schema_name: None,
            table_name: None,
            relation_oid: None,
            old_data: None,
            new_data: None,
            lsn: None,
            metadata: None,
        }
    }

    /// Create a COMMIT transaction event
    pub fn commit(transaction_id: u32, commit_timestamp: DateTime<Utc>) -> Self {
        Self {
            event_type: EventType::Commit,
            transaction_id: Some(transaction_id),
            commit_timestamp: Some(commit_timestamp),
            schema_name: None,
            table_name: None,
            relation_oid: None,
            old_data: None,
            new_data: None,
            lsn: None,
            metadata: None,
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

/// Table relationship information from PostgreSQL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationInfo {
    /// Relation OID
    pub relation_oid: u32,
    
    /// Namespace (schema) name
    pub namespace: String,
    
    /// Relation (table) name 
    pub relation_name: String,
    
    /// Replica identity setting
    pub replica_identity: ReplicaIdentity,
    
    /// Column information
    pub columns: Vec<ColumnInfo>,
}

/// Column information from relation message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    
    /// PostgreSQL type OID
    pub type_oid: u32,
    
    /// Type modifier
    pub type_modifier: i32,
    
    /// Whether column is part of the key
    pub is_key: bool,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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
    
    /// Parse LSN from PostgreSQL string format (e.g., "16/B374D848")
    pub fn from_str(s: &str) -> Result<Self, crate::error::CdcError> {
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
    
    /// Convert LSN to PostgreSQL string format
    pub fn to_string(&self) -> String {
        format!("{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFFFFFF)
    }
}

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
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
