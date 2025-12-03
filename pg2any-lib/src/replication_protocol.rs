//! Unified PostgreSQL logical replication protocol implementation
//!
//! This module consolidates message types, parsing logic, and protocol definitions
//! that were previously scattered across multiple files.

use crate::buffer::BufferReader;
use crate::error::{CdcError, Result};
use crate::pg_replication::{format_lsn, Oid, TimestampTz, XLogRecPtr, Xid};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use tracing::debug;

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

/// PostgreSQL logical replication message types enum
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Begin = message_types::BEGIN,
    Commit = message_types::COMMIT,
    Origin = message_types::ORIGIN,
    Relation = message_types::RELATION,
    Type = message_types::TYPE,
    Insert = message_types::INSERT,
    Update = message_types::UPDATE,
    Delete = message_types::DELETE,
    Truncate = message_types::TRUNCATE,
    Message = message_types::MESSAGE,
    StreamStart = message_types::STREAM_START,
    StreamStop = message_types::STREAM_STOP,
    StreamCommit = message_types::STREAM_COMMIT,
    StreamAbort = message_types::STREAM_ABORT,
}

/// Unified logical replication message enum
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
    Insert { relation_id: Oid, tuple: TupleData },

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
    Truncate { relation_ids: Vec<Oid>, flags: u8 },

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
    StreamStart { xid: Xid, first_segment: bool },

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
    StreamAbort { xid: Xid, subtransaction_xid: Xid },
}

/// Column information in a relation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// Column flags (bit 0 = key column)
    pub flags: u8,
    /// Column name
    pub name: String,
    /// PostgreSQL type OID
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

    /// Check if this column is part of the primary/replica key
    pub fn is_key(&self) -> bool {
        self.flags & 0x01 != 0
    }
}

/// Tuple (row) data
#[derive(Debug, Clone)]
pub struct TupleData {
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

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Convert to a HashMap with column names as keys
    pub fn to_hash_map(&self, relation: &RelationInfo) -> HashMap<String, serde_json::Value> {
        let mut map = HashMap::new();

        for (i, col_data) in self.columns.iter().enumerate() {
            let column_name = relation
                .get_column_by_index(i)
                .map(|col| col.name.clone())
                .unwrap_or_else(|| format!("col_{}", i));

            let value = match col_data.data_type {
                'n' => serde_json::Value::Null,
                't' => serde_json::Value::String(col_data.as_string().unwrap_or_default()),
                'u' => continue, // Skip unchanged TOAST values
                _ => serde_json::Value::String(col_data.as_string().unwrap_or_default()),
            };

            map.insert(column_name, value);
        }

        map
    }
}

/// Data for a single column
#[derive(Debug, Clone)]
pub struct ColumnData {
    pub data_type: char, // 'n' = null, 't' = text, 'u' = unchanged toast
    pub data: Vec<u8>,
}

impl ColumnData {
    #[inline]
    pub fn null() -> Self {
        Self {
            data_type: 'n',
            data: Vec::new(),
        }
    }

    #[inline]
    pub fn text(data: Vec<u8>) -> Self {
        Self {
            data_type: 't',
            data,
        }
    }

    #[inline]
    pub fn unchanged() -> Self {
        Self {
            data_type: 'u',
            data: Vec::new(),
        }
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        self.data_type == 'n'
    }

    #[inline]
    pub fn is_unchanged(&self) -> bool {
        self.data_type == 'u'
    }

    /// Convert to string, returning a Cow to avoid allocation when possible
    /// If the data is valid UTF-8, returns a borrowed reference
    #[inline]
    pub fn as_str(&self) -> Option<Cow<'_, str>> {
        if self.data_type == 't' && !self.data.is_empty() {
            // Try to borrow first (zero-copy), fall back to lossy conversion
            match std::str::from_utf8(&self.data) {
                Ok(s) => Some(Cow::Borrowed(s)),
                Err(_) => Some(Cow::Owned(String::from_utf8_lossy(&self.data).into_owned())),
            }
        } else {
            None
        }
    }

    /// Legacy method that returns owned String for compatibility
    /// Prefer `as_str()` for better performance
    #[inline]
    pub fn as_string(&self) -> Option<String> {
        self.as_str().map(|cow| cow.into_owned())
    }

    /// Get raw bytes data (zero-copy reference)
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

/// Information about a relation (table)
#[derive(Debug, Clone, Serialize, Deserialize)]
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
        self.columns.iter().filter(|col| col.is_key()).collect()
    }
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

/// Unified logical replication parser
pub struct LogicalReplicationParser {
    streaming_context: Option<Xid>,
    /// Protocol version (1, 2, 3, or 4)
    protocol_version: u32,
}

impl LogicalReplicationParser {
    /// Create a new parser with specified protocol version
    pub fn with_protocol_version(protocol_version: u32) -> Self {
        Self {
            streaming_context: None,
            protocol_version,
        }
    }

    /// Check if we're currently inside a streaming transaction context
    #[inline]
    fn is_streaming(&self) -> bool {
        self.streaming_context.is_some()
    }

    /// Parse a WAL data message from the replication stream
    pub fn parse_wal_message(&mut self, data: &[u8]) -> Result<StreamingReplicationMessage> {
        if data.is_empty() {
            return Err(CdcError::protocol("Empty WAL message".to_string()));
        }

        let mut reader = BufferReader::new(data);
        let message_type = reader.read_u8()?;

        debug!(
            "Parsing message type: {} ('{}')",
            message_type, message_type as char
        );

        let message = match message_type {
            message_types::BEGIN => self.parse_begin_message(&mut reader)?,
            message_types::COMMIT => self.parse_commit_message(&mut reader)?,
            message_types::RELATION => self.parse_relation_message(&mut reader)?,
            message_types::INSERT => self.parse_insert_message(&mut reader)?,
            message_types::UPDATE => self.parse_update_message(&mut reader)?,
            message_types::DELETE => self.parse_delete_message(&mut reader)?,
            message_types::TRUNCATE => self.parse_truncate_message(&mut reader)?,
            message_types::TYPE => self.parse_type_message(&mut reader)?,
            message_types::ORIGIN => self.parse_origin_message(&mut reader)?,
            message_types::MESSAGE => self.parse_message(&mut reader)?,
            message_types::STREAM_START => {
                let msg = self.parse_stream_start_message(&mut reader)?;
                self.streaming_context =
                    if let LogicalReplicationMessage::StreamStart { xid, .. } = &msg {
                        Some(*xid)
                    } else {
                        None
                    };
                msg
            }
            message_types::STREAM_STOP => {
                let msg = self.parse_stream_stop_message(&mut reader)?;
                self.streaming_context = None;
                msg
            }
            message_types::STREAM_COMMIT => self.parse_stream_commit_message(&mut reader)?,
            message_types::STREAM_ABORT => self.parse_stream_abort_message(&mut reader)?,
            _ => {
                return Err(CdcError::protocol(format!(
                    "Unknown message type: {} ('{}')",
                    message_type, message_type as char
                )));
            }
        };

        let streaming_message = if let Some(xid) = self.streaming_context {
            StreamingReplicationMessage::new_streaming(message, xid)
        } else {
            StreamingReplicationMessage::new(message)
        };

        Ok(streaming_message)
    }

    /// Parse BEGIN message
    fn parse_begin_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let final_lsn = reader.read_u64()?;
        let timestamp = reader.read_i64()?;
        let xid = reader.read_u32()?;

        debug!(
            "BEGIN: final_lsn={}, timestamp={}, xid={}",
            format_lsn(final_lsn),
            timestamp,
            xid
        );

        Ok(LogicalReplicationMessage::Begin {
            final_lsn,
            timestamp,
            xid,
        })
    }

    /// Parse COMMIT message
    fn parse_commit_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let flags = reader.read_u8()?;
        let commit_lsn = reader.read_u64()?;
        let end_lsn = reader.read_u64()?;
        let timestamp = reader.read_i64()?;

        debug!(
            "COMMIT: flags={}, commit_lsn={}, end_lsn={}, timestamp={}",
            flags,
            format_lsn(commit_lsn),
            format_lsn(end_lsn),
            timestamp
        );

        Ok(LogicalReplicationMessage::Commit {
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
        })
    }

    /// Parse RELATION message
    fn parse_relation_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        // In protocol version 2+ with streaming, there's an extra xid field
        if self.protocol_version >= 2 && self.is_streaming() {
            let _xid = reader.read_u32()?; // Skip streaming transaction id
        }

        let relation_id = reader.read_u32()?;
        let namespace = reader.read_cstring()?;
        let relation_name = reader.read_cstring()?;
        let replica_identity = reader.read_u8()?;
        let column_count = reader.read_u16()?;

        debug!(
            "RELATION: id={}, {}.{}, replica_identity={}, columns={}",
            relation_id, namespace, relation_name, replica_identity, column_count
        );

        let mut columns = Vec::with_capacity(column_count as usize);
        for i in 0..column_count {
            let flags = reader.read_u8()?;
            let name = reader.read_cstring()?;
            let type_id = reader.read_u32()?;
            let type_modifier = reader.read_i32()?;

            debug!(
                "  Column {}: {} (type={}, mod={}, flags={})",
                i, name, type_id, type_modifier, flags
            );

            columns.push(ColumnInfo::new(flags, name, type_id, type_modifier));
        }

        Ok(LogicalReplicationMessage::Relation {
            relation_id,
            namespace,
            relation_name,
            replica_identity,
            columns,
        })
    }

    /// Parse INSERT message
    fn parse_insert_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        // In protocol version 2+ with streaming, there's an extra xid field
        if self.protocol_version >= 2 && self.is_streaming() {
            let _xid = reader.read_u32()?; // Skip streaming transaction id
        }

        let relation_id = reader.read_u32()?;
        let tuple_type = reader.read_u8()?;

        debug!(
            "INSERT: relation_id={}, tuple_type={} (0x{:02x}), streaming={}",
            relation_id,
            tuple_type as char,
            tuple_type,
            self.is_streaming()
        );

        if tuple_type != b'N' {
            return Err(CdcError::protocol(format!(
                "Unexpected tuple type in INSERT: '{}' (0x{:02x}) (expected 'N'), streaming={}, protocol_version={}",
                tuple_type as char, tuple_type, self.is_streaming(), self.protocol_version
            )));
        }

        let tuple = self.parse_tuple_data(reader)?;

        Ok(LogicalReplicationMessage::Insert { relation_id, tuple })
    }

    /// Parse UPDATE message
    fn parse_update_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        // In protocol version 2+ with streaming, there's an extra xid field
        if self.protocol_version >= 2 && self.is_streaming() {
            let _xid = reader.read_u32()?; // Skip streaming transaction id
        }

        let relation_id = reader.read_u32()?;

        debug!(
            "UPDATE: relation_id={}, streaming={}",
            relation_id,
            self.is_streaming()
        );

        let mut old_tuple = None;
        let mut key_type = None;

        // Check if there's old tuple data
        if reader.remaining() > 0 {
            let tuple_type = reader.peek_u8()?;
            if tuple_type == b'K' || tuple_type == b'O' {
                reader.read_u8()?; // consume the type byte
                key_type = Some(tuple_type as char);
                old_tuple = Some(self.parse_tuple_data(reader)?);
                debug!("  Old tuple type: {}", tuple_type as char);
            }
        }

        // New tuple data (required)
        let new_tuple_type = reader.read_u8()?;
        if new_tuple_type != b'N' {
            return Err(CdcError::protocol(format!(
                "Unexpected new tuple type in UPDATE: '{}' (0x{:02x}) (expected 'N'), streaming={}",
                new_tuple_type as char,
                new_tuple_type,
                self.is_streaming()
            )));
        }

        let new_tuple = self.parse_tuple_data(reader)?;

        Ok(LogicalReplicationMessage::Update {
            relation_id,
            old_tuple,
            new_tuple,
            key_type,
        })
    }

    /// Parse DELETE message
    fn parse_delete_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        // In protocol version 2+ with streaming, there's an extra xid field
        if self.protocol_version >= 2 && self.is_streaming() {
            let _xid = reader.read_u32()?; // Skip streaming transaction id
        }

        let relation_id = reader.read_u32()?;
        let key_type = reader.read_u8()? as char;

        debug!(
            "DELETE: relation_id={}, key_type={}, streaming={}",
            relation_id,
            key_type,
            self.is_streaming()
        );

        let old_tuple = self.parse_tuple_data(reader)?;

        Ok(LogicalReplicationMessage::Delete {
            relation_id,
            old_tuple,
            key_type,
        })
    }

    /// Parse TRUNCATE message
    fn parse_truncate_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        // In protocol version 2+ with streaming, there's an extra xid field
        if self.protocol_version >= 2 && self.is_streaming() {
            let _xid = reader.read_u32()?; // Skip streaming transaction id
        }

        let relation_count = reader.read_u32()?;
        let flags = reader.read_u8()?;

        debug!(
            "TRUNCATE: relation_count={}, flags={}",
            relation_count, flags
        );

        let mut relation_ids = Vec::with_capacity(relation_count as usize);
        for _ in 0..relation_count {
            let relation_id = reader.read_u32()?;
            relation_ids.push(relation_id);
        }

        Ok(LogicalReplicationMessage::Truncate {
            relation_ids,
            flags,
        })
    }

    /// Parse TYPE message
    fn parse_type_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        // In protocol version 2+ with streaming, there's an extra xid field
        if self.protocol_version >= 2 && self.is_streaming() {
            let _xid = reader.read_u32()?; // Skip streaming transaction id
        }

        let type_id = reader.read_u32()?;
        let namespace = reader.read_cstring()?;
        let type_name = reader.read_cstring()?;

        debug!("TYPE: id={}, {}.{}", type_id, namespace, type_name);

        Ok(LogicalReplicationMessage::Type {
            type_id,
            namespace,
            type_name,
        })
    }

    /// Parse ORIGIN message
    fn parse_origin_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let origin_lsn = reader.read_u64()?;
        let origin_name = reader.read_cstring()?;

        debug!(
            "ORIGIN: lsn={}, name={}",
            format_lsn(origin_lsn),
            origin_name
        );

        Ok(LogicalReplicationMessage::Origin {
            origin_lsn,
            origin_name,
        })
    }

    /// Parse MESSAGE (logical decoding message)
    fn parse_message(&mut self, reader: &mut BufferReader) -> Result<LogicalReplicationMessage> {
        // In protocol version 2+ with streaming, there's an extra xid field
        if self.protocol_version >= 2 && self.is_streaming() {
            let _xid = reader.read_u32()?; // Skip streaming transaction id
        }

        let flags = reader.read_u8()?;
        let lsn = reader.read_u64()?;
        let prefix = reader.read_cstring()?;
        let content_length = reader.read_u32()?;
        let content = reader.read_bytes(content_length as usize)?;

        debug!(
            "MESSAGE: flags={}, lsn={}, prefix={}, content_length={}",
            flags,
            format_lsn(lsn),
            prefix,
            content_length
        );

        Ok(LogicalReplicationMessage::Message {
            flags,
            lsn,
            prefix,
            content,
        })
    }

    /// Parse STREAM START message
    fn parse_stream_start_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let xid = reader.read_u32()?;
        let first_segment = reader.read_u8()? != 0;

        debug!("STREAM START: xid={}, first_segment={}", xid, first_segment);

        Ok(LogicalReplicationMessage::StreamStart { xid, first_segment })
    }

    /// Parse STREAM STOP message
    fn parse_stream_stop_message(
        &mut self,
        _reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        debug!("STREAM STOP");
        Ok(LogicalReplicationMessage::StreamStop)
    }

    /// Parse STREAM COMMIT message
    fn parse_stream_commit_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let xid = reader.read_u32()?;
        let flags = reader.read_u8()?;
        let commit_lsn = reader.read_u64()?;
        let end_lsn = reader.read_u64()?;
        let timestamp = reader.read_i64()?;

        debug!(
            "STREAM COMMIT: xid={}, flags={}, commit_lsn={}, end_lsn={}",
            xid,
            flags,
            format_lsn(commit_lsn),
            format_lsn(end_lsn)
        );

        Ok(LogicalReplicationMessage::StreamCommit {
            xid,
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
        })
    }

    /// Parse STREAM ABORT message
    fn parse_stream_abort_message(
        &mut self,
        reader: &mut BufferReader,
    ) -> Result<LogicalReplicationMessage> {
        let xid = reader.read_u32()?;
        let subtransaction_xid = reader.read_u32()?;

        debug!(
            "STREAM ABORT: xid={}, subtxn_xid={}",
            xid, subtransaction_xid
        );

        Ok(LogicalReplicationMessage::StreamAbort {
            xid,
            subtransaction_xid,
        })
    }

    /// Parse tuple data (column values)
    fn parse_tuple_data(&mut self, reader: &mut BufferReader) -> Result<TupleData> {
        let column_count = reader.read_u16()?;
        let mut columns = Vec::with_capacity(column_count as usize);

        for _ in 0..column_count {
            let column_type = reader.read_u8()? as char;

            let column_data = match column_type {
                'n' => ColumnData::null(),
                'u' => ColumnData::unchanged(),
                't' => {
                    let length = reader.read_u32()?;
                    let data = reader.read_bytes(length as usize)?;
                    ColumnData::text(data)
                }
                _ => {
                    return Err(CdcError::protocol(format!(
                        "Unknown column data type: '{}'",
                        column_type
                    )));
                }
            };

            columns.push(column_data);
        }

        Ok(TupleData::new(columns))
    }
}

/// Parse keepalive message from the replication stream
pub fn parse_keepalive_message(data: &[u8]) -> Result<KeepaliveMessage> {
    if data.len() < 18 {
        return Err(CdcError::protocol(
            "Keepalive message too short".to_string(),
        ));
    }

    let mut reader = BufferReader::new(data);
    let _msg_type = reader.skip_message_type()?; // Skip 'k'
    let wal_end = reader.read_u64()?;
    let timestamp = reader.read_i64()?;
    let reply_requested = reader.read_u8()? != 0;

    Ok(KeepaliveMessage {
        wal_end,
        timestamp,
        reply_requested,
    })
}

/// Keepalive message from the server
#[derive(Debug, Clone)]
pub struct KeepaliveMessage {
    pub wal_end: XLogRecPtr,
    pub timestamp: TimestampTz,
    pub reply_requested: bool,
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
            ColumnInfo::new(1, "id".to_string(), 23, -1), // Key column
            ColumnInfo::new(0, "name".to_string(), 25, -1), // Regular column
        ];

        let relation =
            RelationInfo::new(12345, "public".to_string(), "users".to_string(), 1, columns);

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
