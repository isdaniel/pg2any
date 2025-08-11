//! PostgreSQL logical replication stream management
//!
//! This module provides high-level management of logical replication streams,
//! including connection management, slot creation, and message processing.

use crate::config::Config;
use crate::error::{CdcError, Result};
use crate::logical_parser::{parse_keepalive_message, LogicalReplicationParser};
use crate::pg_replication::{format_lsn, PgReplicationConnection, XLogRecPtr, INVALID_XLOG_REC_PTR};
use crate::replication_messages::{LogicalReplicationMessage, ReplicationState, StreamingReplicationMessage};
use crate::types::{ChangeEvent, EventType};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// PostgreSQL logical replication stream
pub struct LogicalReplicationStream {
    connection: PgReplicationConnection,
    parser: LogicalReplicationParser,
    state: ReplicationState,
    config: ReplicationStreamConfig,
    slot_created: bool,
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
        
        match self.connection.create_replication_slot(&self.config.slot_name, "pgoutput") {
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
        self.connection.start_replication(&self.config.slot_name, start_lsn, &options)?;

        info!("Logical replication started with LSN: {}", format_lsn(start_lsn));
        Ok(())
    }

    /// Process the next batch of replication messages
    pub async fn next_batch(&mut self, batch_size: usize) -> Result<Vec<ChangeEvent>> {
        let mut events = Vec::with_capacity(batch_size);
        let start_time = Instant::now();
        let timeout = Duration::from_millis(100); // Short timeout to allow periodic checks
        
        while events.len() < batch_size && start_time.elapsed() < timeout {
            // Check if we should send feedback
            self.maybe_send_feedback()?;

            // Try to get data from the replication stream
            match self.connection.get_copy_data(0)? { // 10ms timeout
                Some(data) => {
                    if data.is_empty() {
                        continue;
                    }
                    match data[0] as char {
                        'w' => {
                            // WAL data message
                            if let Some(event) = self.process_wal_message(&data)? {
                                events.push(event);
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
                    // No data available, continue
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                }
            }
        }

        Ok(events)
    }

    /// Process a WAL data message
    fn process_wal_message(&mut self, data: &[u8]) -> Result<Option<ChangeEvent>> {
        if data.len() < 25 {
            return Err(CdcError::protocol("WAL message too short".to_string()));
        }

        // Parse WAL message header
        // Format: 'w' + start_lsn (8) + end_lsn (8) + send_time (8) + message_data
        let start_lsn = u64::from_be_bytes([
            data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
        ]);
        let _end_lsn = u64::from_be_bytes([
            data[9], data[10], data[11], data[12], data[13], data[14], data[15], data[16],
        ]);
        let _send_time = i64::from_be_bytes([
            data[17], data[18], data[19], data[20], data[21], data[22], data[23], data[24],
        ]);

        // Update LSN tracking
        if start_lsn > 0 {
            self.state.update_lsn(start_lsn);
        }

        // Parse the logical replication message
        let message_data = &data[25..];
        if message_data.is_empty() {
            return Ok(None);
        }

        let replication_message = self.parser.parse_wal_message(message_data)?;
        self.convert_to_change_event(replication_message, start_lsn)
    }

    /// Process a keepalive message
    fn process_keepalive_message(&mut self, data: &[u8]) -> Result<()> {
        let keepalive = parse_keepalive_message(data)?;
        
        debug!("Received keepalive: wal_end={}, reply_requested={}", 
               format_lsn(keepalive.wal_end), keepalive.reply_requested);

        self.state.update_lsn(keepalive.wal_end);

        if keepalive.reply_requested {
            self.send_feedback()?;
        }

        Ok(())
    }

    /// Convert a logical replication message to a ChangeEvent
    fn convert_to_change_event(&mut self, message: StreamingReplicationMessage, lsn: XLogRecPtr) -> Result<Option<ChangeEvent>> {
        let event = match message.message {
            LogicalReplicationMessage::Relation { relation_id, namespace, relation_name, replica_identity, columns } => {
                let relation_info = crate::replication_messages::RelationInfo::new(
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
                        event_type: EventType::Insert,
                        transaction_id: None,
                        commit_timestamp: Some(chrono::Utc::now()),
                        schema_name: Some(schema_name),
                        table_name: Some(table_name),
                        relation_oid: Some(relation_id),
                        old_data: None,
                        new_data: Some(data),
                        lsn: Some(format_lsn(lsn)),
                        metadata: None,
                    }
                } else {
                    warn!("Received INSERT for unknown relation: {}", relation_id);
                    return Ok(None);
                }
            }
            
            LogicalReplicationMessage::Update { relation_id, old_tuple, new_tuple, .. } => {
                if let Some(relation) = self.state.get_relation(relation_id) {
                    let full_name = relation.full_name();
                    let parts: Vec<&str> = full_name.split('.').collect();
                    let (schema_name, table_name) = if parts.len() >= 2 {
                        (parts[0].to_string(), parts[1].to_string())
                    } else {
                        ("public".to_string(), relation.full_name())
                    };
                    let old_data = if let Some(old_tuple) = old_tuple {
                        Some(self.convert_tuple_to_data(&old_tuple, relation)?)
                    } else {
                        None
                    };
                    let new_data = Some(self.convert_tuple_to_data(&new_tuple, relation)?);
                    
                    ChangeEvent {
                        event_type: EventType::Update,
                        transaction_id: None,
                        commit_timestamp: Some(chrono::Utc::now()),
                        schema_name: Some(schema_name),
                        table_name: Some(table_name),
                        relation_oid: Some(relation_id),
                        old_data,
                        new_data,
                        lsn: Some(format_lsn(lsn)),
                        metadata: None,
                    }
                } else {
                    warn!("Received UPDATE for unknown relation: {}", relation_id);
                    return Ok(None);
                }
            }
            
            LogicalReplicationMessage::Delete { relation_id, old_tuple, .. } => {
                if let Some(relation) = self.state.get_relation(relation_id) {
                    let full_name = relation.full_name();
                    let parts: Vec<&str> = full_name.split('.').collect();
                    let (schema_name, table_name) = if parts.len() >= 2 {
                        (parts[0].to_string(), parts[1].to_string())
                    } else {
                        ("public".to_string(), relation.full_name())
                    };
                    let old_data = Some(self.convert_tuple_to_data(&old_tuple, relation)?);
                    
                    ChangeEvent {
                        event_type: EventType::Delete,
                        transaction_id: None,
                        commit_timestamp: Some(chrono::Utc::now()),
                        schema_name: Some(schema_name),
                        table_name: Some(table_name),
                        relation_oid: Some(relation_id),
                        old_data,
                        new_data: None,
                        lsn: Some(format_lsn(lsn)),
                        metadata: None,
                    }
                } else {
                    warn!("Received DELETE for unknown relation: {}", relation_id);
                    return Ok(None);
                }
            }
            
            LogicalReplicationMessage::Begin { xid, .. } => {
                debug!("Transaction begin: xid={}", xid);
                return Ok(None);
            }
            
            LogicalReplicationMessage::Commit { .. } => {
                debug!("Transaction commit");
                return Ok(None);
            }
            
            LogicalReplicationMessage::Truncate { relation_ids, .. } => {
                // For now, just log truncate operations
                for relation_id in relation_ids {
                    if let Some(relation) = self.state.get_relation(relation_id) {
                        info!("Table truncated: {}", relation.full_name());
                    }
                }
                return Ok(None);
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
        tuple: &crate::replication_messages::TupleData,
        relation: &crate::replication_messages::RelationInfo,
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
    fn maybe_send_feedback(&mut self) -> Result<()> {
        if self.state.should_send_feedback(self.config.feedback_interval) {
            self.send_feedback()?;
            self.state.mark_feedback_sent();
        }
        Ok(())
    }

    /// Send feedback to the server
    fn send_feedback(&mut self) -> Result<()> {
        if self.state.last_received_lsn == 0 {
            return Ok(());
        }

        self.connection.send_standby_status_update(
            self.state.last_received_lsn,
            self.state.last_flushed_lsn,
            self.state.last_applied_lsn,
            false, // Don't request reply
        )?;

        debug!("Sent feedback: received={}", format_lsn(self.state.last_received_lsn));
        Ok(())
    }

    /// Stop the replication stream
    pub async fn stop(&mut self) -> Result<()> {
        info!("Stopping logical replication stream");
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
            feedback_interval: Duration::from_secs(1), // Default 1 second
            connection_timeout: config.connection_timeout,
        }
    }
}

// Add hex dependency to convert binary data
// Note: This is a simple hex encoding implementation to avoid adding another dependency
mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter()
            .map(|b| format!("{:02x}", b))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::types::DestinationType;
    use crate::config::Config;
    use super::*;

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
        assert_eq!(replication_config.connection_timeout, Duration::from_secs(30));
    }
}
