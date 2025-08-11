use crate::error::{CdcError, Result};
use crate::types::*;
use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

/// PostgreSQL logical replication message types
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Begin = b'B',
    Commit = b'C',
    Origin = b'O',
    Relation = b'R',
    Type = b'Y',
    Insert = b'I',
    Update = b'U',
    Delete = b'D',
    Truncate = b'T',
    Message = b'M',
    StreamStart = b'S',
    StreamStop = b'E',
    StreamCommit = b's',
    StreamAbort = b'A',
}

impl MessageType {
    /// Create a message type from a byte
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            b'B' => Some(MessageType::Begin),
            b'C' => Some(MessageType::Commit),
            b'O' => Some(MessageType::Origin),
            b'R' => Some(MessageType::Relation),
            b'Y' => Some(MessageType::Type),
            b'I' => Some(MessageType::Insert),
            b'U' => Some(MessageType::Update),
            b'D' => Some(MessageType::Delete),
            b'T' => Some(MessageType::Truncate),
            b'M' => Some(MessageType::Message),
            b'S' => Some(MessageType::StreamStart),
            b'E' => Some(MessageType::StreamStop),
            b's' => Some(MessageType::StreamCommit),
            b'A' => Some(MessageType::StreamAbort),
            _ => None,
        }
    }
}

/// Parsed logical replication message
#[derive(Debug, Clone)]
pub enum LogicalReplicationMessage {
    Begin(BeginMessage),
    Commit(CommitMessage),
    Origin(OriginMessage),
    Relation(RelationMessage),
    Type(TypeMessage),
    Insert(InsertMessage),
    Update(UpdateMessage),
    Delete(DeleteMessage),
    Truncate(TruncateMessage),
    Message(MessageMessage),
    StreamStart(StreamStartMessage),
    StreamStop(StreamStopMessage),
    StreamCommit(StreamCommitMessage),
    StreamAbort(StreamAbortMessage),
}

/// BEGIN message
#[derive(Debug, Clone)]
pub struct BeginMessage {
    pub final_lsn: u64,
    pub commit_timestamp: DateTime<Utc>,
    pub xid: u32,
}

/// COMMIT message
#[derive(Debug, Clone)]
pub struct CommitMessage {
    pub flags: u8,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    pub commit_timestamp: DateTime<Utc>,
}

/// ORIGIN message
#[derive(Debug, Clone)]
pub struct OriginMessage {
    pub commit_lsn: u64,
    pub name: String,
}

/// RELATION message
#[derive(Debug, Clone)]
pub struct RelationMessage {
    pub relation_oid: u32,
    pub namespace: String,
    pub relation_name: String,
    pub replica_identity: ReplicaIdentity,
    pub columns: Vec<ColumnInfo>,
}

/// TYPE message
#[derive(Debug, Clone)]
pub struct TypeMessage {
    pub type_oid: u32,
    pub namespace: String,
    pub type_name: String,
}

/// INSERT message
#[derive(Debug, Clone)]
pub struct InsertMessage {
    pub relation_oid: u32,
    pub tuple_data: HashMap<String, serde_json::Value>,
}

/// UPDATE message
#[derive(Debug, Clone)]
pub struct UpdateMessage {
    pub relation_oid: u32,
    pub old_tuple_data: Option<HashMap<String, serde_json::Value>>,
    pub new_tuple_data: HashMap<String, serde_json::Value>,
}

/// DELETE message
#[derive(Debug, Clone)]
pub struct DeleteMessage {
    pub relation_oid: u32,
    pub tuple_data: HashMap<String, serde_json::Value>,
}

/// TRUNCATE message
#[derive(Debug, Clone)]
pub struct TruncateMessage {
    pub cascade: bool,
    pub restart_seqs: bool,
    pub relation_oids: Vec<u32>,
}

/// MESSAGE message (user-defined logical message)
#[derive(Debug, Clone)]
pub struct MessageMessage {
    pub transactional: bool,
    pub lsn: u64,
    pub prefix: String,
    pub content: Vec<u8>,
}

/// STREAM START message
#[derive(Debug, Clone)]
pub struct StreamStartMessage {
    pub xid: u32,
    pub first_segment: bool,
}

/// STREAM STOP message
#[derive(Debug, Clone)]
pub struct StreamStopMessage;

/// STREAM COMMIT message
#[derive(Debug, Clone)]
pub struct StreamCommitMessage {
    pub xid: u32,
    pub flags: u8,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    pub commit_timestamp: DateTime<Utc>,
}

/// STREAM ABORT message
#[derive(Debug, Clone)]
pub struct StreamAbortMessage {
    pub xid: u32,
    pub subtxn_xid: u32,
}

/// Tuple data types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleType {
    Null,
    UnchangedToast,
    Text(String),
}

/// Message parser for PostgreSQL logical replication protocol
pub struct MessageParser {
    relation_map: HashMap<u32, RelationInfo>,
    type_map: HashMap<u32, TypeInfo>,
}

impl MessageParser {
    /// Create a new message parser
    pub fn new() -> Self {
        Self {
            relation_map: HashMap::new(),
            type_map: HashMap::new(),
        }
    }

    /// Parse a logical replication message
    pub fn parse(&mut self, data: &[u8]) -> Result<LogicalReplicationMessage> {
        if data.is_empty() {
            return Err(CdcError::protocol("Empty message data"));
        }

        let mut cursor = Bytes::copy_from_slice(data);
        let message_type = cursor.get_u8();

        let msg_type = MessageType::from_byte(message_type)
            .ok_or_else(|| CdcError::protocol(format!("Unknown message type: {}", message_type)))?;

        match msg_type {
            MessageType::Begin => self.parse_begin(&mut cursor).map(LogicalReplicationMessage::Begin),
            MessageType::Commit => self.parse_commit(&mut cursor).map(LogicalReplicationMessage::Commit),
            MessageType::Origin => self.parse_origin(&mut cursor).map(LogicalReplicationMessage::Origin),
            MessageType::Relation => {
                let msg = self.parse_relation(&mut cursor)?;
                // Store relation info for future reference
                self.relation_map.insert(msg.relation_oid, RelationInfo {
                    relation_oid: msg.relation_oid,
                    namespace: msg.namespace.clone(),
                    relation_name: msg.relation_name.clone(),
                    replica_identity: msg.replica_identity.clone(),
                    columns: msg.columns.clone(),
                });
                Ok(LogicalReplicationMessage::Relation(msg))
            },
            MessageType::Type => {
                let msg = self.parse_type(&mut cursor)?;
                // Store type info for future reference
                self.type_map.insert(msg.type_oid, TypeInfo {
                    type_oid: msg.type_oid,
                    type_name: msg.type_name.clone(),
                    namespace: msg.namespace.clone(),
                });
                Ok(LogicalReplicationMessage::Type(msg))
            },
            MessageType::Insert => self.parse_insert(&mut cursor).map(LogicalReplicationMessage::Insert),
            MessageType::Update => self.parse_update(&mut cursor).map(LogicalReplicationMessage::Update),
            MessageType::Delete => self.parse_delete(&mut cursor).map(LogicalReplicationMessage::Delete),
            MessageType::Truncate => self.parse_truncate(&mut cursor).map(LogicalReplicationMessage::Truncate),
            MessageType::Message => self.parse_message(&mut cursor).map(LogicalReplicationMessage::Message),
            MessageType::StreamStart => self.parse_stream_start(&mut cursor).map(LogicalReplicationMessage::StreamStart),
            MessageType::StreamStop => Ok(LogicalReplicationMessage::StreamStop(StreamStopMessage)),
            MessageType::StreamCommit => self.parse_stream_commit(&mut cursor).map(LogicalReplicationMessage::StreamCommit),
            MessageType::StreamAbort => self.parse_stream_abort(&mut cursor).map(LogicalReplicationMessage::StreamAbort),
        }
    }

    /// Parse BEGIN message
    fn parse_begin(&self, cursor: &mut Bytes) -> Result<BeginMessage> {
        let final_lsn = cursor.get_u64();
        let commit_timestamp = self.parse_timestamp(cursor.get_u64())?;
        let xid = cursor.get_u32();

        Ok(BeginMessage {
            final_lsn,
            commit_timestamp,
            xid,
        })
    }

    /// Parse COMMIT message
    fn parse_commit(&self, cursor: &mut Bytes) -> Result<CommitMessage> {
        let flags = cursor.get_u8();
        let commit_lsn = cursor.get_u64();
        let end_lsn = cursor.get_u64();
        let commit_timestamp = self.parse_timestamp(cursor.get_u64())?;

        Ok(CommitMessage {
            flags,
            commit_lsn,
            end_lsn,
            commit_timestamp,
        })
    }

    /// Parse ORIGIN message
    fn parse_origin(&self, cursor: &mut Bytes) -> Result<OriginMessage> {
        let commit_lsn = cursor.get_u64();
        let name = self.read_cstring(cursor)?;

        Ok(OriginMessage { commit_lsn, name })
    }

    /// Parse RELATION message
    fn parse_relation(&self, cursor: &mut Bytes) -> Result<RelationMessage> {
        let relation_oid = cursor.get_u32();
        let namespace = self.read_cstring(cursor)?;
        let relation_name = self.read_cstring(cursor)?;
        let replica_identity_byte = cursor.get_u8();
        let column_count = cursor.get_u16();

        let replica_identity = match replica_identity_byte {
            b'd' => ReplicaIdentity::Default,
            b'n' => ReplicaIdentity::Nothing,
            b'f' => ReplicaIdentity::Full,
            b'i' => ReplicaIdentity::Index,
            _ => return Err(CdcError::protocol("Unknown replica identity")),
        };

        let mut columns = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            let flags = cursor.get_u8();
            let name = self.read_cstring(cursor)?;
            let type_oid = cursor.get_u32();
            let type_modifier = cursor.get_i32();

            columns.push(ColumnInfo {
                name,
                type_oid,
                type_modifier,
                is_key: flags & 0x01 != 0,
            });
        }

        Ok(RelationMessage {
            relation_oid,
            namespace,
            relation_name,
            replica_identity,
            columns,
        })
    }

    /// Parse TYPE message
    fn parse_type(&self, cursor: &mut Bytes) -> Result<TypeMessage> {
        let type_oid = cursor.get_u32();
        let namespace = self.read_cstring(cursor)?;
        let type_name = self.read_cstring(cursor)?;

        Ok(TypeMessage {
            type_oid,
            namespace,
            type_name,
        })
    }

    /// Parse INSERT message
    fn parse_insert(&self, cursor: &mut Bytes) -> Result<InsertMessage> {
        let relation_oid = cursor.get_u32();
        let _tuple_type = cursor.get_u8(); // 'N' for new tuple
        let tuple_data = self.parse_tuple_data(cursor, relation_oid)?;

        Ok(InsertMessage {
            relation_oid,
            tuple_data,
        })
    }

    /// Parse UPDATE message
    fn parse_update(&self, cursor: &mut Bytes) -> Result<UpdateMessage> {
        let relation_oid = cursor.get_u32();
        
        let first_type = cursor.get_u8();
        let (old_tuple_data, new_tuple_data) = match first_type {
            b'K' | b'O' => {
                // Old tuple present
                let old_data = self.parse_tuple_data(cursor, relation_oid)?;
                let _new_type = cursor.get_u8(); // Should be 'N'
                let new_data = self.parse_tuple_data(cursor, relation_oid)?;
                (Some(old_data), new_data)
            },
            b'N' => {
                // Only new tuple
                let new_data = self.parse_tuple_data(cursor, relation_oid)?;
                (None, new_data)
            },
            _ => return Err(CdcError::protocol("Invalid UPDATE tuple type")),
        };

        Ok(UpdateMessage {
            relation_oid,
            old_tuple_data,
            new_tuple_data,
        })
    }

    /// Parse DELETE message
    fn parse_delete(&self, cursor: &mut Bytes) -> Result<DeleteMessage> {
        let relation_oid = cursor.get_u32();
        let _tuple_type = cursor.get_u8(); // 'K' or 'O'
        let tuple_data = self.parse_tuple_data(cursor, relation_oid)?;

        Ok(DeleteMessage {
            relation_oid,
            tuple_data,
        })
    }

    /// Parse TRUNCATE message
    fn parse_truncate(&self, cursor: &mut Bytes) -> Result<TruncateMessage> {
        let relation_count = cursor.get_u32();
        let option_flags = cursor.get_u8();
        
        let cascade = option_flags & 0x01 != 0;
        let restart_seqs = option_flags & 0x02 != 0;

        let mut relation_oids = Vec::with_capacity(relation_count as usize);
        for _ in 0..relation_count {
            relation_oids.push(cursor.get_u32());
        }

        Ok(TruncateMessage {
            cascade,
            restart_seqs,
            relation_oids,
        })
    }

    /// Parse MESSAGE message
    fn parse_message(&self, cursor: &mut Bytes) -> Result<MessageMessage> {
        let flags = cursor.get_u8();
        let transactional = flags & 0x01 != 0;
        let lsn = cursor.get_u64();
        let prefix = self.read_cstring(cursor)?;
        let content_length = cursor.get_u32();
        let mut content = vec![0u8; content_length as usize];
        cursor.copy_to_slice(&mut content);

        Ok(MessageMessage {
            transactional,
            lsn,
            prefix,
            content,
        })
    }

    /// Parse STREAM START message
    fn parse_stream_start(&self, cursor: &mut Bytes) -> Result<StreamStartMessage> {
        let xid = cursor.get_u32();
        let first_segment = cursor.get_u8() != 0;

        Ok(StreamStartMessage { xid, first_segment })
    }

    /// Parse STREAM COMMIT message
    fn parse_stream_commit(&self, cursor: &mut Bytes) -> Result<StreamCommitMessage> {
        let xid = cursor.get_u32();
        let flags = cursor.get_u8();
        let commit_lsn = cursor.get_u64();
        let end_lsn = cursor.get_u64();
        let commit_timestamp = self.parse_timestamp(cursor.get_u64())?;

        Ok(StreamCommitMessage {
            xid,
            flags,
            commit_lsn,
            end_lsn,
            commit_timestamp,
        })
    }

    /// Parse STREAM ABORT message
    fn parse_stream_abort(&self, cursor: &mut Bytes) -> Result<StreamAbortMessage> {
        let xid = cursor.get_u32();
        let subtxn_xid = cursor.get_u32();

        Ok(StreamAbortMessage { xid, subtxn_xid })
    }

    /// Parse tuple data
    fn parse_tuple_data(&self, cursor: &mut Bytes, relation_oid: u32) -> Result<HashMap<String, serde_json::Value>> {
        let column_count = cursor.get_u16();
        let mut data = HashMap::new();

        let relation = self.relation_map.get(&relation_oid);
        
        for i in 0..column_count {
            let column_type = cursor.get_u8();
            let value = match column_type {
                b'n' => serde_json::Value::Null,
                b'u' => {
                    // Unchanged TOAST value, skip
                    continue;
                },
                b't' => {
                    let length = cursor.get_u32();
                    let mut data_bytes = vec![0u8; length as usize];
                    cursor.copy_to_slice(&mut data_bytes);
                    
                    // Convert to string (assuming UTF-8 for simplicity)
                    let value_str = String::from_utf8_lossy(&data_bytes);
                    serde_json::Value::String(value_str.to_string())
                },
                _ => return Err(CdcError::protocol("Unknown tuple data type")),
            };

            // Get column name from relation info
            let column_name = if let Some(rel) = relation {
                if let Some(col) = rel.columns.get(i as usize) {
                    col.name.clone()
                } else {
                    format!("col_{}", i)
                }
            } else {
                format!("col_{}", i)
            };

            data.insert(column_name, value);
        }

        Ok(data)
    }

    /// Read a null-terminated string
    fn read_cstring(&self, cursor: &mut Bytes) -> Result<String> {
        let mut bytes = Vec::new();
        while cursor.has_remaining() {
            let byte = cursor.get_u8();
            if byte == 0 {
                break;
            }
            bytes.push(byte);
        }
        
        String::from_utf8(bytes)
            .map_err(|_| CdcError::protocol("Invalid UTF-8 in string"))
    }

    /// Parse PostgreSQL timestamp to DateTime<Utc>
    fn parse_timestamp(&self, timestamp: u64) -> Result<DateTime<Utc>> {
        // PostgreSQL timestamp is microseconds since 2000-01-01 00:00:00 UTC
        const POSTGRES_EPOCH_OFFSET: i64 = 946_684_800_000_000; // microseconds
        
        let microseconds = timestamp as i64;
        let unix_microseconds = microseconds + POSTGRES_EPOCH_OFFSET;
        
        let seconds = unix_microseconds / 1_000_000;
        let nanoseconds = ((unix_microseconds % 1_000_000) * 1_000) as u32;
        
        DateTime::from_timestamp(seconds, nanoseconds)
            .ok_or_else(|| CdcError::protocol("Invalid timestamp"))
    }

    /// Get relation info by OID
    pub fn get_relation_info(&self, relation_oid: u32) -> Option<&RelationInfo> {
        self.relation_map.get(&relation_oid)
    }

    /// Get type info by OID
    pub fn get_type_info(&self, type_oid: u32) -> Option<&TypeInfo> {
        self.type_map.get(&type_oid)
    }
}

impl Default for MessageParser {
    fn default() -> Self {
        Self::new()
    }
}
