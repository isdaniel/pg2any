//! Data structures for PostgreSQL logical replication messages
//!
//! This module defines all the message types used in PostgreSQL's logical replication protocol.

use crate::pg_replication::{Oid, TimestampTz, Xid, XLogRecPtr};
use std::collections::HashMap;

/// PostgreSQL logical replication message types
#[derive(Debug, Clone)]
pub enum LogicalReplicationMessage {
    /// Begin transaction
    Begin {
        final_lsn: XLogRecPtr,
        timestamp: TimestampTz,
        xid: Xid,
    },
    
    /// Commit transaction
    Commit {
        flags: u8,
        commit_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        timestamp: TimestampTz,
    },
    
    /// Relation information (table schema)
    Relation {
        relation_id: Oid,
        namespace: String,
        relation_name: String,
        replica_identity: u8,
        columns: Vec<ColumnInfo>,
    },
    
    /// Insert operation
    Insert {
        relation_id: Oid,
        tuple: TupleData,
    },
    
    /// Update operation
    Update {
        relation_id: Oid,
        old_tuple: Option<TupleData>,
        new_tuple: TupleData,
        key_type: Option<char>,
    },
    
    /// Delete operation
    Delete {
        relation_id: Oid,
        old_tuple: TupleData,
        key_type: char,
    },
    
    /// Truncate operation
    Truncate {
        relation_ids: Vec<Oid>,
        flags: u8,
    },
    
    /// Type information
    Type {
        type_id: Oid,
        namespace: String,
        type_name: String,
    },
    
    /// Origin information
    Origin {
        origin_lsn: XLogRecPtr,
        origin_name: String,
    },
    
    /// Message (used for keepalive and custom messages)
    Message {
        flags: u8,
        lsn: XLogRecPtr,
        prefix: String,
        content: Vec<u8>,
    },
    
    /// Streaming transaction start
    StreamStart {
        xid: Xid,
        first_segment: bool,
    },
    
    /// Streaming transaction stop
    StreamStop,
    
    /// Streaming transaction commit
    StreamCommit {
        xid: Xid,
        flags: u8,
        commit_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        timestamp: TimestampTz,
    },
    
    /// Streaming transaction abort
    StreamAbort {
        xid: Xid,
        subtransaction_xid: Xid,
    },
}

/// Column information in a relation
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column flags (1 = key column)
    pub flags: u8,
    /// Column name
    pub name: String,
    /// Column data type OID
    pub type_id: Oid,
    /// Type modifier
    pub type_modifier: i32,
}

impl ColumnInfo {
    pub fn new(flags: u8, name: String, type_id: Oid, type_modifier: i32) -> Self {
        Self {
            flags,
            name,
            type_id,
            type_modifier,
        }
    }
    
    /// Check if this column is part of the key
    pub fn is_key_column(&self) -> bool {
        self.flags & 1 != 0
    }
}

/// Tuple (row) data
#[derive(Debug, Clone)]
pub struct TupleData {
    /// Column data
    pub columns: Vec<ColumnData>,
}

impl TupleData {
    pub fn new(columns: Vec<ColumnData>) -> Self {
        Self { columns }
    }
    
    /// Get column data by index
    pub fn get_column(&self, index: usize) -> Option<&ColumnData> {
        self.columns.get(index)
    }
    
    /// Get number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

/// Data for a single column
#[derive(Debug, Clone)]
pub struct ColumnData {
    /// Data type identifier ('n' = null, 't' = text, 'u' = unchanged, 'b' = binary)
    pub data_type: char,
    /// Raw data (empty for null or unchanged)
    pub data: Vec<u8>,
}

impl ColumnData {
    /// Create null column data
    pub fn null() -> Self {
        Self {
            data_type: 'n',
            data: Vec::new(),
        }
    }
    
    /// Create text column data
    pub fn text(data: Vec<u8>) -> Self {
        Self {
            data_type: 't',
            data,
        }
    }
    
    /// Create unchanged column data (for updates where column didn't change)
    pub fn unchanged() -> Self {
        Self {
            data_type: 'u',
            data: Vec::new(),
        }
    }
    
    /// Create binary column data
    pub fn binary(data: Vec<u8>) -> Self {
        Self {
            data_type: 'b',
            data,
        }
    }
    
    /// Check if the column is null
    pub fn is_null(&self) -> bool {
        self.data_type == 'n'
    }
    
    /// Check if the column is unchanged (in updates)
    pub fn is_unchanged(&self) -> bool {
        self.data_type == 'u'
    }
    
    /// Get the data as a string (for text columns)
    pub fn as_string(&self) -> Option<String> {
        if self.is_null() || self.is_unchanged() {
            None
        } else {
            String::from_utf8(self.data.clone()).ok()
        }
    }
    
    /// Get the raw data
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

/// Information about a relation (table)
#[derive(Debug, Clone)]
pub struct RelationInfo {
    pub relation_id: Oid,
    pub namespace: String,
    pub relation_name: String,
    pub replica_identity: u8,
    pub columns: Vec<ColumnInfo>,
}

impl RelationInfo {
    pub fn new(
        relation_id: Oid,
        namespace: String,
        relation_name: String,
        replica_identity: u8,
        columns: Vec<ColumnInfo>,
    ) -> Self {
        Self {
            relation_id,
            namespace,
            relation_name,
            replica_identity,
            columns,
        }
    }
    
    /// Get the full table name (namespace.relation_name)
    pub fn full_name(&self) -> String {
        format!("{}.{}", self.namespace, self.relation_name)
    }
    
    /// Get column by name
    pub fn get_column_by_name(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|col| col.name == name)
    }
    
    /// Get column by index
    pub fn get_column_by_index(&self, index: usize) -> Option<&ColumnInfo> {
        self.columns.get(index)
    }
    
    /// Get key columns
    pub fn get_key_columns(&self) -> Vec<&ColumnInfo> {
        self.columns.iter().filter(|col| col.is_key_column()).collect()
    }
}

/// State for managing replication relations and tracking
#[derive(Debug)]
pub struct ReplicationState {
    /// Relations by OID
    pub relations: HashMap<Oid, RelationInfo>,
    /// Last received LSN
    pub last_received_lsn: XLogRecPtr,
    /// Last flushed LSN
    pub last_flushed_lsn: XLogRecPtr,
    /// Last applied LSN
    pub last_applied_lsn: XLogRecPtr,
    /// Last feedback time
    pub last_feedback_time: std::time::Instant,
}

impl ReplicationState {
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
            last_received_lsn: 0,
            last_flushed_lsn: 0,
            last_applied_lsn: 0,
            last_feedback_time: std::time::Instant::now(),
        }
    }
    
    /// Add or update relation information
    pub fn add_relation(&mut self, relation: RelationInfo) {
        self.relations.insert(relation.relation_id, relation);
    }
    
    /// Get relation by OID
    pub fn get_relation(&self, relation_id: Oid) -> Option<&RelationInfo> {
        self.relations.get(&relation_id)
    }
    
    /// Update LSN positions
    pub fn update_lsn(&mut self, lsn: XLogRecPtr) {
        if lsn > self.last_received_lsn {
            self.last_received_lsn = lsn;
            // For simplicity, treat received as flushed and applied
            self.last_flushed_lsn = lsn;
            self.last_applied_lsn = lsn;
        }
    }
    
    /// Check if feedback should be sent
    pub fn should_send_feedback(&self, interval: std::time::Duration) -> bool {
        self.last_feedback_time.elapsed() >= interval
    }
    
    /// Mark feedback as sent
    pub fn mark_feedback_sent(&mut self) {
        self.last_feedback_time = std::time::Instant::now();
    }
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self::new()
    }
}

/// Replication slot information
#[derive(Debug, Clone)]
pub struct ReplicationSlot {
    pub slot_name: String,
    pub plugin: String,
    pub slot_type: String,
    pub database: String,
    pub active: bool,
    pub restart_lsn: Option<XLogRecPtr>,
    pub confirmed_flush_lsn: Option<XLogRecPtr>,
}

/// Publication information
#[derive(Debug, Clone)]
pub struct PublicationInfo {
    pub name: String,
    pub owner: String,
    pub all_tables: bool,
    pub inserts: bool,
    pub updates: bool,
    pub deletes: bool,
    pub truncates: bool,
    pub tables: Vec<String>,
}

/// System identification result
#[derive(Debug, Clone)]
pub struct SystemIdentification {
    pub system_id: String,
    pub timeline: u32,
    pub xlog_pos: XLogRecPtr,
    pub db_name: Option<String>,
}

/// Message type constants for logical replication protocol
pub mod message_types {
    pub const BEGIN: u8 = b'B';
    pub const COMMIT: u8 = b'C';
    pub const ORIGIN: u8 = b'O';
    pub const RELATION: u8 = b'R';
    pub const TYPE: u8 = b'Y';
    pub const INSERT: u8 = b'I';
    pub const UPDATE: u8 = b'U';
    pub const DELETE: u8 = b'D';
    pub const TRUNCATE: u8 = b'T';
    pub const MESSAGE: u8 = b'M';
    pub const STREAM_START: u8 = b'S';
    pub const STREAM_STOP: u8 = b'E';
    pub const STREAM_COMMIT: u8 = b'c';
    pub const STREAM_ABORT: u8 = b'A';
}

/// Replication message with streaming context
#[derive(Debug, Clone)]
pub struct StreamingReplicationMessage {
    pub message: LogicalReplicationMessage,
    pub is_streaming: bool,
    pub xid: Option<Xid>,
}

impl StreamingReplicationMessage {
    pub fn new(message: LogicalReplicationMessage) -> Self {
        Self {
            message,
            is_streaming: false,
            xid: None,
        }
    }
    
    pub fn new_streaming(message: LogicalReplicationMessage, xid: Xid) -> Self {
        Self {
            message,
            is_streaming: true,
            xid: Some(xid),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_data_creation() {
        let null_col = ColumnData::null();
        assert!(null_col.is_null());
        assert_eq!(null_col.data_type, 'n');

        let text_col = ColumnData::text(b"test".to_vec());
        assert!(!text_col.is_null());
        assert_eq!(text_col.data_type, 't');
        assert_eq!(text_col.as_string(), Some("test".to_string()));

        let unchanged_col = ColumnData::unchanged();
        assert!(unchanged_col.is_unchanged());
        assert_eq!(unchanged_col.data_type, 'u');
    }

    #[test]
    fn test_relation_info() {
        let columns = vec![
            ColumnInfo::new(1, "id".to_string(), 23, -1),  // Key column
            ColumnInfo::new(0, "name".to_string(), 25, -1), // Regular column
        ];
        
        let relation = RelationInfo::new(
            12345,
            "public".to_string(),
            "users".to_string(),
            1,
            columns,
        );
        
        assert_eq!(relation.full_name(), "public.users");
        assert_eq!(relation.get_key_columns().len(), 1);
        assert_eq!(relation.get_key_columns()[0].name, "id");
    }

    #[test]
    fn test_replication_state() {
        let mut state = ReplicationState::new();
        
        state.update_lsn(100);
        assert_eq!(state.last_received_lsn, 100);
        assert_eq!(state.last_flushed_lsn, 100);
        
        // LSN should not go backwards
        state.update_lsn(50);
        assert_eq!(state.last_received_lsn, 100);
    }
}
