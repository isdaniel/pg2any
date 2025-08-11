//! Parser for PostgreSQL logical replication protocol messages
//!
//! This module implements parsing of all logical replication message types according to
//! the PostgreSQL logical replication protocol specification.

use crate::buffer::BufferReader;
use crate::error::{CdcError, Result};
use crate::pg_replication::{TimestampTz, Xid, XLogRecPtr};
use crate::replication_messages::*;
use tracing::debug;

/// Parser for logical replication messages
pub struct LogicalReplicationParser {
    streaming_context: Option<Xid>,
}

impl LogicalReplicationParser {
    pub fn new() -> Self {
        Self {
            streaming_context: None,
        }
    }

    /// Parse a WAL data message from the replication stream
    pub fn parse_wal_message(&mut self, data: &[u8]) -> Result<StreamingReplicationMessage> {
        if data.is_empty() {
            return Err(CdcError::protocol("Empty WAL message".to_string()));
        }

        let mut reader = BufferReader::new(data);
        let message_type = reader.read_u8()?;

        debug!("Parsing message type: {} ('{}')", message_type, message_type as char);

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
                self.streaming_context = if let LogicalReplicationMessage::StreamStart { xid, .. } = &msg {
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
                    message_type,
                    message_type as char
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
    fn parse_begin_message(&mut self, reader: &mut BufferReader) -> Result<LogicalReplicationMessage> {
        let final_lsn = reader.read_u64()?;
        let timestamp = reader.read_i64()?;
        let xid = reader.read_u32()?;

        debug!("BEGIN: final_lsn={:X}/{:X}, timestamp={}, xid={}", 
               final_lsn >> 32, final_lsn & 0xFFFFFFFF, timestamp, xid);

        Ok(LogicalReplicationMessage::Begin {
            final_lsn,
            timestamp,
            xid,
        })
    }

    /// Parse COMMIT message
    fn parse_commit_message(&mut self, reader: &mut BufferReader) -> Result<LogicalReplicationMessage> {
        let flags = reader.read_u8()?;
        let commit_lsn = reader.read_u64()?;
        let end_lsn = reader.read_u64()?;
        let timestamp = reader.read_i64()?;

        debug!("COMMIT: flags={}, commit_lsn={:X}/{:X}, end_lsn={:X}/{:X}, timestamp={}", 
               flags, commit_lsn >> 32, commit_lsn & 0xFFFFFFFF, 
               end_lsn >> 32, end_lsn & 0xFFFFFFFF, timestamp);

        Ok(LogicalReplicationMessage::Commit {
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
        })
    }

    /// Parse RELATION message
    fn parse_relation_message(&mut self, reader: &mut BufferReader) -> Result<LogicalReplicationMessage> {
        let relation_id = reader.read_u32()?;
        let namespace = reader.read_cstring()?;
        let relation_name = reader.read_cstring()?;
        let replica_identity = reader.read_u8()?;
        let column_count = reader.read_u16()?;

        debug!("RELATION: id={}, {}.{}, replica_identity={}, columns={}", 
               relation_id, namespace, relation_name, replica_identity, column_count);

        let mut columns = Vec::with_capacity(column_count as usize);
        for i in 0..column_count {
            let flags = reader.read_u8()?;
            let name = reader.read_cstring()?;
            let type_id = reader.read_u32()?;
            let type_modifier = reader.read_i32()?;

            debug!("  Column {}: {} (type={}, mod={}, flags={})", 
                   i, name, type_id, type_modifier, flags);

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
    fn parse_insert_message(&mut self, reader: &mut BufferReader) -> Result<LogicalReplicationMessage> {
        let relation_id = reader.read_u32()?;
        let tuple_type = reader.read_u8()?;

        debug!("INSERT: relation_id={}, tuple_type={}", relation_id, tuple_type as char);

        if tuple_type != b'N' {
            return Err(CdcError::protocol(format!(
                "Unexpected tuple type in INSERT: {} (expected 'N')",
                tuple_type as char
            )));
        }

        let tuple = self.parse_tuple_data(reader)?;

        Ok(LogicalReplicationMessage::Insert {
            relation_id,
            tuple,
        })
    }

    /// Parse UPDATE message
    fn parse_update_message(&mut self, reader: &mut BufferReader) -> Result<LogicalReplicationMessage> {
        let relation_id = reader.read_u32()?;

        debug!("UPDATE: relation_id={}", relation_id);

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
                "Unexpected new tuple type in UPDATE: {} (expected 'N')",
                new_tuple_type as char
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
    fn parse_delete_message(&mut self, reader: &mut BufferReader) -> Result<LogicalReplicationMessage> {
        let relation_id = reader.read_u32()?;
        let tuple_type = reader.read_u8()?;

        debug!("DELETE: relation_id={}, tuple_type={}", relation_id, tuple_type as char);

        if tuple_type != b'K' && tuple_type != b'O' {
            return Err(CdcError::protocol(format!(
                "Unexpected tuple type in DELETE: {} (expected 'K' or 'O')",
                tuple_type as char
            )));
        }

        let old_tuple = self.parse_tuple_data(reader)?;

        Ok(LogicalReplicationMessage::Delete {
            relation_id,
            old_tuple,
            key_type: tuple_type as char,
        })
    }

    /// Parse TRUNCATE message
    fn parse_truncate_message(&mut self, reader: &mut BufferReader) -> Result<LogicalReplicationMessage> {
        let relation_count = reader.read_u32()?;
        let flags = reader.read_u8()?;

        debug!("TRUNCATE: relation_count={}, flags={}", relation_count, flags);

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
    fn parse_type_message(&mut self, reader: &mut BufferReader) -> Result<LogicalReplicationMessage> {
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
    fn parse_origin_message(&mut self, reader: &mut BufferReader) -> Result<LogicalReplicationMessage> {
        let origin_lsn = reader.read_u64()?;
        let origin_name = reader.read_cstring()?;

        debug!("ORIGIN: lsn={:X}/{:X}, name={}", 
               origin_lsn >> 32, origin_lsn & 0xFFFFFFFF, origin_name);

        Ok(LogicalReplicationMessage::Origin {
            origin_lsn,
            origin_name,
        })
    }

    /// Parse MESSAGE
    fn parse_message(&mut self, reader: &mut BufferReader) -> Result<LogicalReplicationMessage> {
        let flags = reader.read_u8()?;
        let lsn = reader.read_u64()?;
        let prefix = reader.read_cstring()?;
        let content_length = reader.read_u32()?;
        let content = reader.read_bytes(content_length as usize)?;

        debug!("MESSAGE: flags={}, lsn={:X}/{:X}, prefix={}, content_length={}", 
               flags, lsn >> 32, lsn & 0xFFFFFFFF, prefix, content_length);

        Ok(LogicalReplicationMessage::Message {
            flags,
            lsn,
            prefix,
            content,
        })
    }

    /// Parse STREAM START message
    fn parse_stream_start_message(&mut self, reader: &mut BufferReader) -> Result<LogicalReplicationMessage> {
        let xid = reader.read_u32()?;
        let first_segment = reader.read_u8()? != 0;

        debug!("STREAM START: xid={}, first_segment={}", xid, first_segment);

        Ok(LogicalReplicationMessage::StreamStart {
            xid,
            first_segment,
        })
    }

    /// Parse STREAM STOP message
    fn parse_stream_stop_message(&mut self, _reader: &mut BufferReader) -> Result<LogicalReplicationMessage> {
        debug!("STREAM STOP");
        Ok(LogicalReplicationMessage::StreamStop)
    }

    /// Parse STREAM COMMIT message
    fn parse_stream_commit_message(&mut self, reader: &mut BufferReader) -> Result<LogicalReplicationMessage> {
        let xid = reader.read_u32()?;
        let flags = reader.read_u8()?;
        let commit_lsn = reader.read_u64()?;
        let end_lsn = reader.read_u64()?;
        let timestamp = reader.read_i64()?;

        debug!("STREAM COMMIT: xid={}, flags={}, commit_lsn={:X}/{:X}, end_lsn={:X}/{:X}", 
               xid, flags, commit_lsn >> 32, commit_lsn & 0xFFFFFFFF, 
               end_lsn >> 32, end_lsn & 0xFFFFFFFF);

        Ok(LogicalReplicationMessage::StreamCommit {
            xid,
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
        })
    }

    /// Parse STREAM ABORT message
    fn parse_stream_abort_message(&mut self, reader: &mut BufferReader) -> Result<LogicalReplicationMessage> {
        let xid = reader.read_u32()?;
        let subtransaction_xid = reader.read_u32()?;

        debug!("STREAM ABORT: xid={}, subtxn_xid={}", xid, subtransaction_xid);

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
                'b' => {
                    let length = reader.read_u32()?;
                    let data = reader.read_bytes(length as usize)?;
                    ColumnData::binary(data)
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

impl Default for LogicalReplicationParser {
    fn default() -> Self {
        Self::new()
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
    fn test_parse_begin_message() {
        let mut parser = LogicalReplicationParser::new();
        
        // Create a BEGIN message: B + final_lsn (8 bytes) + timestamp (8 bytes) + xid (4 bytes)
        let mut data = vec![message_types::BEGIN];
        data.extend_from_slice(&0x123456789ABCDEF0u64.to_be_bytes());  // final_lsn
        data.extend_from_slice(&0x1234567890ABCDEFi64.to_be_bytes());  // timestamp
        data.extend_from_slice(&0x12345678u32.to_be_bytes());          // xid

        let result = parser.parse_wal_message(&data).unwrap();
        
        match result.message {
            LogicalReplicationMessage::Begin { final_lsn, timestamp, xid } => {
                assert_eq!(final_lsn, 0x123456789ABCDEF0);
                assert_eq!(timestamp, 0x1234567890ABCDEF);
                assert_eq!(xid, 0x12345678);
            }
            _ => panic!("Expected Begin message"),
        }
    }

    #[test]
    fn test_parse_relation_message() {
        let mut parser = LogicalReplicationParser::new();
        
        // Create a RELATION message with one column
        let mut data = vec![message_types::RELATION];
        data.extend_from_slice(&0x12345678u32.to_be_bytes());  // relation_id
        data.extend_from_slice(b"public\0");                    // namespace
        data.extend_from_slice(b"test_table\0");                // relation_name
        data.push(1);                                           // replica_identity
        data.extend_from_slice(&1u16.to_be_bytes());           // column_count
        
        // Column data
        data.push(1);                                           // flags (key column)
        data.extend_from_slice(b"id\0");                        // column name
        data.extend_from_slice(&23u32.to_be_bytes());          // type_id (int4)
        data.extend_from_slice(&(-1i32).to_be_bytes());        // type_modifier

        let result = parser.parse_wal_message(&data).unwrap();
        
        match result.message {
            LogicalReplicationMessage::Relation { relation_id, namespace, relation_name, columns, .. } => {
                assert_eq!(relation_id, 0x12345678);
                assert_eq!(namespace, "public");
                assert_eq!(relation_name, "test_table");
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[0].type_id, 23);
                assert!(columns[0].is_key_column());
            }
            _ => panic!("Expected Relation message"),
        }
    }
}
