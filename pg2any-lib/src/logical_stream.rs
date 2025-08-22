//! PostgreSQL logical replication stream management
//!
//! This module provides high-level management of logical replication streams,
//! including connection management, slot creation, and message processing.

use crate::buffer::BufferReader;
use crate::config::Config;
use crate::error::{CdcError, Result};
use crate::pg_replication::{
    format_lsn, postgres_timestamp_to_chrono, PgReplicationConnection, XLogRecPtr,
    INVALID_XLOG_REC_PTR,
};
use crate::replication_protocol::{parse_keepalive_message, LogicalReplicationParser};
use crate::replication_protocol::{
    LogicalReplicationMessage, ReplicationState, StreamingReplicationMessage,
};
use crate::types::{ChangeEvent, EventType, Lsn, ReplicaIdentity};
use crate::{RelationInfo, TupleData};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// PostgreSQL logical replication stream
pub struct LogicalReplicationStream {
    connection: PgReplicationConnection,
    parser: LogicalReplicationParser,
    pub state: ReplicationState,
    config: ReplicationStreamConfig,
    slot_created: bool,
    //last_feedback_time: std::time::Instant,
}

/// Configuration for the replication stream
#[derive(Debug, Clone)]
pub struct ReplicationStreamConfig {
    pub slot_name: String,
    pub publication_name: String,
    pub protocol_version: u32,
    pub streaming_enabled: bool,
    pub feedback_interval: Duration,
    pub connection_timeout: Duration,
}

impl LogicalReplicationStream {
    /// Create a new logical replication stream
    pub async fn new(connection_string: &str, config: ReplicationStreamConfig) -> Result<Self> {
        info!("Creating logical replication stream");

        let connection = PgReplicationConnection::connect(connection_string)?;
        let parser = LogicalReplicationParser::new();
        let state = ReplicationState::new();

        Ok(Self {
            connection,
            parser,
            state,
            config,
            slot_created: false,
            //last_feedback_time: std::time::Instant::now(),
        })
    }

    /// Initialize the replication stream
    pub async fn initialize(&mut self) -> Result<()> {
        info!("Initializing replication stream");

        // Identify the system
        let _system_id = self.connection.identify_system()?;
        info!("System identification successful");

        // Create replication slot if it doesn't exist
        self.ensure_replication_slot().await?;

        info!("Replication stream initialized");
        Ok(())
    }

    /// Ensure the replication slot exists
    async fn ensure_replication_slot(&mut self) -> Result<()> {
        if self.slot_created {
            return Ok(());
        }

        info!("Creating replication slot: {}", self.config.slot_name);

        match self
            .connection
            .create_replication_slot(&self.config.slot_name, "pgoutput")
        {
            Ok(_) => {
                info!("Replication slot created successfully");
                self.slot_created = true;
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("already exists") {
                    warn!("Replication slot already exists, continuing");
                    self.slot_created = true;
                } else {
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Start the replication stream
    pub async fn start(&mut self, start_lsn: Option<XLogRecPtr>) -> Result<()> {
        info!("Starting logical replication stream");

        let start_lsn = start_lsn.unwrap_or(INVALID_XLOG_REC_PTR);

        // Build replication options
        let proto_version = self.config.protocol_version.to_string();
        let publication_names = format!("\"{}\"", self.config.publication_name);
        let mut options = vec![
            ("proto_version", proto_version.as_str()),
            ("publication_names", publication_names.as_str()),
        ];

        if self.config.streaming_enabled {
            options.push(("streaming", "on"));
        }

        // Start replication
        self.connection
            .start_replication(&self.config.slot_name, start_lsn, &options)?;

        info!(
            "Logical replication started with LSN: {}",
            format_lsn(start_lsn)
        );
        Ok(())
    }

    /// Process the next single replication event
    pub async fn next_event(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<Option<ChangeEvent>> {
        // Send proactive feedback if enough time has passed
        self.maybe_send_feedback();

        match self
            .connection
            .get_copy_data_async(cancellation_token)
            .await?
        {
            Some(data) => {
                if data.is_empty() {
                    return Ok(None);
                }
                match data[0] as char {
                    'w' => {
                        // WAL data message
                        if let Some(event) = self.process_wal_message(&data)? {
                            // Send feedback after processing WAL data
                            self.maybe_send_feedback();
                            return Ok(Some(event));
                        }
                    }
                    'k' => {
                        // Keepalive message
                        self.process_keepalive_message(&data)?;
                    }
                    _ => {
                        debug!("Received unknown message type: {}", data[0] as char);
                    }
                }
            }
            None => {
                // No data available or cancelled - still send feedback
                self.maybe_send_feedback();
                return Ok(None);
            }
        }

        // No event received
        Ok(None)
    }

    /// Process a WAL data message
    fn process_wal_message(&mut self, data: &[u8]) -> Result<Option<ChangeEvent>> {
        // Use BufferReader for safe parsing of WAL message
        let mut reader = BufferReader::new(data);

        // Check minimum message length (1 + 8 + 8 + 8 = 25 bytes)
        if data.len() < 25 {
            return Err(CdcError::protocol("WAL message too short".to_string()));
        }

        // Skip the message type ('w')
        let _msg_type = reader.skip_message_type()?;

        // Parse WAL message header
        // Format: 'w' + start_lsn (8) + end_lsn (8) + send_time (8) + message_data
        let start_lsn = reader.read_u64()?;
        let _end_lsn = reader.read_u64()?;
        let _send_time = reader.read_i64()?;
        // Update LSN tracking
        if start_lsn > 0 {
            self.state.update_lsn(start_lsn);
        }

        // Check if there's message data remaining
        if reader.remaining() == 0 {
            return Ok(None);
        }

        // Get the remaining bytes for message parsing
        let message_data = reader.read_bytes(reader.remaining())?;
        let replication_message = self.parser.parse_wal_message(&message_data)?;
        self.convert_to_change_event(replication_message, start_lsn)
    }

    /// Process a keepalive message
    fn process_keepalive_message(&mut self, data: &[u8]) -> Result<()> {
        let keepalive = parse_keepalive_message(data)?;

        info!(
            "Received keepalive: wal_end={}, reply_requested={}",
            format_lsn(keepalive.wal_end),
            keepalive.reply_requested
        );

        self.state.update_lsn(keepalive.wal_end);

        if keepalive.reply_requested {
            self.send_feedback()?;
        }

        Ok(())
    }

    /// Convert a logical replication message to a ChangeEvent
    fn convert_to_change_event(
        &mut self,
        message: StreamingReplicationMessage,
        lsn: XLogRecPtr,
    ) -> Result<Option<ChangeEvent>> {
        let event = match message.message {
            LogicalReplicationMessage::Relation {
                relation_id,
                namespace,
                relation_name,
                replica_identity,
                columns,
            } => {
                let relation_info = RelationInfo::new(
                    relation_id,
                    namespace.clone(),
                    relation_name.clone(),
                    replica_identity,
                    columns,
                );

                self.state.add_relation(relation_info);

                // Don't generate events for relation messages
                return Ok(None);
            }

            LogicalReplicationMessage::Insert { relation_id, tuple } => {
                if let Some(relation) = self.state.get_relation(relation_id) {
                    let full_name = relation.full_name();
                    let parts: Vec<&str> = full_name.split('.').collect();
                    let (schema_name, table_name) = if parts.len() >= 2 {
                        (parts[0].to_string(), parts[1].to_string())
                    } else {
                        ("public".to_string(), relation.full_name())
                    };
                    let data = self.convert_tuple_to_data(&tuple, relation)?;

                    ChangeEvent {
                        event_type: EventType::Insert {
                            schema: schema_name,
                            table: table_name,
                            relation_oid: relation_id,
                            data,
                        },
                        lsn: Some(Lsn::new(lsn)),
                        metadata: None,
                    }
                } else {
                    warn!("Received INSERT for unknown relation: {}", relation_id);
                    return Ok(None);
                }
            }

            LogicalReplicationMessage::Update {
                relation_id,
                old_tuple,
                new_tuple,
                key_type,
            } => {
                if let Some((schema_name, table_name, replica_identity, key_columns, relation)) =
                    self.relation_metadata(relation_id, key_type)
                {
                    let old_data = if let Some(old_tuple) = old_tuple {
                        Some(self.convert_tuple_to_data(&old_tuple, relation)?)
                    } else {
                        None
                    };
                    let new_data = self.convert_tuple_to_data(&new_tuple, relation)?;

                    ChangeEvent {
                        event_type: EventType::Update {
                            schema: schema_name,
                            table: table_name,
                            relation_oid: relation_id,
                            old_data,
                            new_data,
                            replica_identity,
                            key_columns,
                        },
                        lsn: Some(Lsn::new(lsn)),
                        metadata: None,
                    }
                } else {
                    warn!("Received UPDATE for unknown relation: {}", relation_id);
                    return Ok(None);
                }
            }

            LogicalReplicationMessage::Delete {
                relation_id,
                old_tuple,
                key_type,
            } => {
                if let Some((schema_name, table_name, replica_identity, key_columns, relation)) =
                    self.relation_metadata(relation_id, Some(key_type))
                {
                    let old_data = self.convert_tuple_to_data(&old_tuple, relation)?;

                    ChangeEvent {
                        event_type: EventType::Delete {
                            schema: schema_name,
                            table: table_name,
                            relation_oid: relation_id,
                            old_data,
                            replica_identity,
                            key_columns,
                        },
                        lsn: Some(Lsn::new(lsn)),
                        metadata: None,
                    }
                } else {
                    warn!("Received DELETE for unknown relation: {}", relation_id);
                    return Ok(None);
                }
            }

            LogicalReplicationMessage::Begin { xid, timestamp, .. } => {
                debug!("Transaction begin: xid={}", xid);
                ChangeEvent {
                    event_type: EventType::Begin {
                        transaction_id: xid,
                        commit_timestamp: postgres_timestamp_to_chrono(timestamp),
                    },
                    lsn: Some(Lsn::new(lsn)),
                    metadata: None,
                }
            }

            LogicalReplicationMessage::Commit { timestamp, commit_lsn, end_lsn, .. } => {
                debug!("Transaction commit, commit_lsn:{}, end_lsn:{}", format_lsn(commit_lsn), format_lsn(end_lsn));
                ChangeEvent {
                    event_type: EventType::Commit {
                        commit_timestamp: postgres_timestamp_to_chrono(timestamp),
                    },
                    lsn: Some(Lsn::new(lsn)),
                    metadata: None,
                }
            }

            LogicalReplicationMessage::Truncate {
                relation_ids,
                flags: _,
            } => {
                let mut truncate_tables = Vec::with_capacity(relation_ids.len());
                for relation_id in relation_ids {
                    if let Some(relation) = self.state.get_relation(relation_id) {
                        info!("Table truncated: {}", relation.full_name());
                        truncate_tables.push(relation.full_name());
                    }
                }

                ChangeEvent {
                    event_type: EventType::Truncate(truncate_tables),
                    lsn: Some(Lsn::new(lsn)),
                    metadata: None,
                }
            }

            _ => {
                debug!("Ignoring message type: {:?}", message.message);
                return Ok(None);
            }
        };

        Ok(Some(event))
    }

    /// Convert tuple data to a HashMap for ChangeEvent
    fn convert_tuple_to_data(
        &self,
        tuple: &TupleData,
        relation: &RelationInfo,
    ) -> Result<std::collections::HashMap<String, serde_json::Value>> {
        let mut data = std::collections::HashMap::new();

        for (i, column_data) in tuple.columns.iter().enumerate() {
            if let Some(column_info) = relation.get_column_by_index(i) {
                let value = if column_data.is_null() {
                    serde_json::Value::Null
                } else if let Some(text) = column_data.as_string() {
                    serde_json::Value::String(text)
                } else {
                    // For binary data, convert to base64 or hex string
                    let hex_string = hex::encode(column_data.as_bytes());
                    serde_json::Value::String(format!("\\x{}", hex_string))
                };

                data.insert(column_info.name.clone(), value);
            }
        }

        Ok(data)
    }

    /// Check if feedback should be sent and send it
    pub fn maybe_send_feedback(&mut self) {
        if self
            .state
            .should_send_feedback(self.config.feedback_interval)
        {
            self.send_feedback().unwrap_or_else(|e| {
                warn!("Failed to send feedback: {}", e);
            });
            self.state.mark_feedback_sent();
        }
    }

    /// Send feedback to the server
    pub fn send_feedback(&mut self) -> Result<()> {
        if self.state.last_received_lsn == 0 {
            return Ok(());
        }

        self.connection.send_standby_status_update(
            self.state.last_received_lsn,
            self.state.last_flushed_lsn,
            self.state.last_applied_lsn,
            false, // Don't request reply
        )?;

        debug!(
            "Sent feedback: received={}",
            format_lsn(self.state.last_received_lsn)
        );
        Ok(())
    }

    /// Extract key columns from relation info based on key_type from the protocol
    fn get_key_columns_for_relation(
        &self,
        relation: &RelationInfo,
        key_type: Option<char>,
    ) -> Vec<String> {
        // Get key columns based on the relation's replica identity and key_type from protocol
        match key_type {
            Some('K') => {
                // Key tuple - use replica identity index columns or primary key
                relation
                    .get_key_columns()
                    .iter()
                    .map(|col| col.name.clone())
                    .collect()
            }
            Some('O') => {
                // Old tuple - means REPLICA IDENTITY FULL, use all columns
                relation
                    .columns
                    .iter()
                    .map(|col| col.name.clone())
                    .collect()
            }
            None => {
                // No old tuple data - means REPLICA IDENTITY NOTHING or DEFAULT without changes to key columns
                // Fall back to using any available key columns from relation info
                let key_cols: Vec<String> = relation
                    .get_key_columns()
                    .iter()
                    .map(|col| col.name.clone())
                    .collect();
                if key_cols.is_empty() {
                    // Try to infer primary key from column flags or use all columns as last resort
                    relation
                        .columns
                        .iter()
                        .filter(|col| col.is_key())
                        .map(|col| col.name.clone())
                        .collect()
                } else {
                    key_cols
                }
            }
            _ => {
                // Unknown key type, use available key columns
                relation
                    .get_key_columns()
                    .iter()
                    .map(|col| col.name.clone())
                    .collect()
            }
        }
    }

    /// Extract schema/table name, replica identity, and key columns for a relation
    fn relation_metadata(
        &self,
        relation_id: u32,
        key_type: Option<char>,
    ) -> Option<(String, String, ReplicaIdentity, Vec<String>, &RelationInfo)> {
        let relation = self.state.get_relation(relation_id)?;
        let full_name = relation.full_name();
        let parts: Vec<&str> = full_name.split('.').collect();

        let (schema_name, table_name) = if parts.len() >= 2 {
            (parts[0].to_string(), parts[1].to_string())
        } else {
            ("public".to_string(), relation.full_name())
        };

        let replica_identity = ReplicaIdentity::from_byte(relation.replica_identity)
            .unwrap_or(ReplicaIdentity::Default);

        let key_columns = self.get_key_columns_for_relation(relation, key_type);

        Some((
            schema_name,
            table_name,
            replica_identity,
            key_columns,
            relation,
        ))
    }

    /// Stop the replication stream
    pub async fn stop(&mut self) -> Result<()> {
        //info!("Stopping logical replication stream");
        // The connection will be closed when dropped
        Ok(())
    }
    
    /// Get the current LSN position
    pub fn current_lsn(&self) -> XLogRecPtr {
        self.state.last_received_lsn
    }
}

/// Convert from CDC config to replication stream config
impl From<&Config> for ReplicationStreamConfig {
    fn from(config: &Config) -> Self {
        Self {
            slot_name: config.replication_slot_name.clone(),
            publication_name: config.publication_name.clone(),
            protocol_version: config.protocol_version,
            streaming_enabled: config.streaming,
            feedback_interval: config.heartbeat_interval,
            connection_timeout: config.connection_timeout,
        }
    }
}

// Add hex dependency to convert binary data
// Note: This is a simple hex encoding implementation to avoid adding another dependency
mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::types::DestinationType;

    #[test]
    fn test_replication_stream_config_from_cdc_config() {
        let cdc_config = Config::builder()
            .source_connection_string("postgresql://user:pass@host:5432/db".to_string())
            .destination_type(DestinationType::MySQL)
            .destination_connection_string("mysql://user:pass@host:3306/db".to_string())
            .replication_slot_name("test_slot".to_string())
            .publication_name("test_pub".to_string())
            .protocol_version(2)
            .streaming(true)
            .connection_timeout(Duration::from_secs(30))
            .build()
            .unwrap();

        let replication_config: ReplicationStreamConfig = (&cdc_config).into();

        assert_eq!(replication_config.slot_name, "test_slot");
        assert_eq!(replication_config.publication_name, "test_pub");
        assert_eq!(replication_config.protocol_version, 2);
        assert!(replication_config.streaming_enabled);
        assert_eq!(
            replication_config.connection_timeout,
            Duration::from_secs(30)
        );
    }
}
