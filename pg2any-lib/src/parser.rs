use crate::error::{CdcError, Result};
use crate::message::{LogicalReplicationMessage, MessageParser};
use crate::types::{ChangeEvent, EventType, RelationInfo};
use std::collections::HashMap;
use tracing::{debug, warn};

/// High-level parser that converts logical replication messages to change events
pub struct CdcParser {
    message_parser: MessageParser,
    current_transaction: Option<TransactionContext>,
    relations: HashMap<u32, RelationInfo>,
}

/// Transaction context for tracking transaction state
#[derive(Debug, Clone)]
struct TransactionContext {
    xid: u32,
    commit_timestamp: chrono::DateTime<chrono::Utc>,
    events: Vec<ChangeEvent>,
}

impl CdcParser {
    /// Create a new CDC parser
    pub fn new() -> Self {
        Self {
            message_parser: MessageParser::new(),
            current_transaction: None,
            relations: HashMap::new(),
        }
    }

    /// Parse raw message data into change events
    pub fn parse_message(&mut self, data: &[u8]) -> Result<Vec<ChangeEvent>> {
        let message = self.message_parser.parse(data)?;
        self.process_logical_message(message)
    }

    /// Process a logical replication message and convert to change events
    fn process_logical_message(&mut self, message: LogicalReplicationMessage) -> Result<Vec<ChangeEvent>> {
        match message {
            LogicalReplicationMessage::Begin(msg) => {
                debug!("Starting transaction: XID={}", msg.xid);
                self.current_transaction = Some(TransactionContext {
                    xid: msg.xid,
                    commit_timestamp: msg.commit_timestamp,
                    events: Vec::new(),
                });
                Ok(vec![ChangeEvent::begin(msg.xid, msg.commit_timestamp)])
            },

            LogicalReplicationMessage::Commit(msg) => {
                debug!("Committing transaction: commit_lsn={}", msg.commit_lsn);
                let mut events = Vec::new();
                
                if let Some(mut txn) = self.current_transaction.take() {
                    // Add all transaction events
                    events.append(&mut txn.events);
                    
                    // Add commit event
                    events.push(ChangeEvent::commit(txn.xid, msg.commit_timestamp));
                } else {
                    warn!("Received COMMIT without BEGIN");
                    // Create a standalone commit event
                    events.push(ChangeEvent::commit(0, msg.commit_timestamp));
                }
                
                Ok(events)
            },

            LogicalReplicationMessage::Relation(msg) => {
                debug!("Relation info: {}.{} (OID: {})", msg.namespace, msg.relation_name, msg.relation_oid);
                
                let relation_info = RelationInfo {
                    relation_oid: msg.relation_oid,
                    namespace: msg.namespace.clone(),
                    relation_name: msg.relation_name.clone(),
                    replica_identity: msg.replica_identity,
                    columns: msg.columns,
                };
                
                self.relations.insert(msg.relation_oid, relation_info);
                
                // Relation messages don't produce change events by themselves
                Ok(vec![])
            },

            LogicalReplicationMessage::Type(msg) => {
                debug!("Type info: {}.{} (OID: {})", msg.namespace, msg.type_name, msg.type_oid);
                // Type messages don't produce change events by themselves
                Ok(vec![])
            },

            LogicalReplicationMessage::Insert(msg) => {
                let event = self.create_insert_event(msg)?;
                self.add_to_current_transaction(event.clone());
                Ok(vec![event])
            },

            LogicalReplicationMessage::Update(msg) => {
                let event = self.create_update_event(msg)?;
                self.add_to_current_transaction(event.clone());
                Ok(vec![event])
            },

            LogicalReplicationMessage::Delete(msg) => {
                let event = self.create_delete_event(msg)?;
                self.add_to_current_transaction(event.clone());
                Ok(vec![event])
            },

            LogicalReplicationMessage::Truncate(msg) => {
                let events = self.create_truncate_events(msg)?;
                for event in &events {
                    self.add_to_current_transaction(event.clone());
                }
                Ok(events)
            },

            LogicalReplicationMessage::Message(msg) => {
                debug!("Logical message: prefix={}, transactional={}", msg.prefix, msg.transactional);
                // User messages don't produce change events by default
                // But could be extended to support custom events
                Ok(vec![])
            },

            LogicalReplicationMessage::Origin(_) => {
                debug!("Origin message received");
                // Origin messages don't produce change events
                Ok(vec![])
            },

            // Streaming messages (for large transactions)
            LogicalReplicationMessage::StreamStart(_) |
            LogicalReplicationMessage::StreamStop(_) |
            LogicalReplicationMessage::StreamCommit(_) |
            LogicalReplicationMessage::StreamAbort(_) => {
                debug!("Streaming message received");
                // For now, we don't handle streaming messages differently
                // In a full implementation, you'd want to handle these for large transactions
                Ok(vec![])
            },
        }
    }

    /// Create an INSERT change event
    fn create_insert_event(&self, msg: crate::message::InsertMessage) -> Result<ChangeEvent> {
        let relation = self.get_relation_info(msg.relation_oid)?;
        
        let mut event = ChangeEvent::insert(
            relation.namespace.clone(),
            relation.relation_name.clone(),
            msg.relation_oid,
            msg.tuple_data,
        );

        // Add transaction context if available
        if let Some(txn) = &self.current_transaction {
            event.transaction_id = Some(txn.xid);
            event.commit_timestamp = Some(txn.commit_timestamp);
        }

        Ok(event)
    }

    /// Create an UPDATE change event
    fn create_update_event(&self, msg: crate::message::UpdateMessage) -> Result<ChangeEvent> {
        let relation = self.get_relation_info(msg.relation_oid)?;
        
        let mut event = ChangeEvent::update(
            relation.namespace.clone(),
            relation.relation_name.clone(),
            msg.relation_oid,
            msg.old_tuple_data,
            msg.new_tuple_data,
        );

        // Add transaction context if available
        if let Some(txn) = &self.current_transaction {
            event.transaction_id = Some(txn.xid);
            event.commit_timestamp = Some(txn.commit_timestamp);
        }

        Ok(event)
    }

    /// Create a DELETE change event
    fn create_delete_event(&self, msg: crate::message::DeleteMessage) -> Result<ChangeEvent> {
        let relation = self.get_relation_info(msg.relation_oid)?;
        
        let mut event = ChangeEvent::delete(
            relation.namespace.clone(),
            relation.relation_name.clone(),
            msg.relation_oid,
            msg.tuple_data,
        );

        // Add transaction context if available
        if let Some(txn) = &self.current_transaction {
            event.transaction_id = Some(txn.xid);
            event.commit_timestamp = Some(txn.commit_timestamp);
        }

        Ok(event)
    }

    /// Create TRUNCATE change events (one per table)
    fn create_truncate_events(&self, msg: crate::message::TruncateMessage) -> Result<Vec<ChangeEvent>> {
        let mut events = Vec::new();

        for &relation_oid in &msg.relation_oids {
            let relation = self.get_relation_info(relation_oid)?;
            
            let mut event = ChangeEvent {
                event_type: EventType::Truncate,
                transaction_id: None,
                commit_timestamp: None,
                schema_name: Some(relation.namespace.clone()),
                table_name: Some(relation.relation_name.clone()),
                relation_oid: Some(relation_oid),
                old_data: None,
                new_data: None,
                lsn: None,
                metadata: Some({
                    let mut metadata = HashMap::new();
                    metadata.insert("cascade".to_string(), serde_json::Value::Bool(msg.cascade));
                    metadata.insert("restart_seqs".to_string(), serde_json::Value::Bool(msg.restart_seqs));
                    metadata
                }),
            };

            // Add transaction context if available
            if let Some(txn) = &self.current_transaction {
                event.transaction_id = Some(txn.xid);
                event.commit_timestamp = Some(txn.commit_timestamp);
            }

            events.push(event);
        }

        Ok(events)
    }

    /// Add an event to the current transaction
    fn add_to_current_transaction(&mut self, event: ChangeEvent) {
        if let Some(txn) = &mut self.current_transaction {
            txn.events.push(event);
        }
    }

    /// Get relation info by OID
    fn get_relation_info(&self, relation_oid: u32) -> Result<&RelationInfo> {
        self.relations.get(&relation_oid)
            .or_else(|| self.message_parser.get_relation_info(relation_oid))
            .ok_or_else(|| CdcError::protocol(format!(
                "Unknown relation OID: {}. Make sure RELATION messages are processed before DML messages.",
                relation_oid
            )))
    }

    /// Clear the current transaction state
    pub fn clear_transaction(&mut self) {
        self.current_transaction = None;
    }

    /// Get information about all known relations
    pub fn get_relations(&self) -> &HashMap<u32, RelationInfo> {
        &self.relations
    }

    /// Check if we're currently in a transaction
    pub fn in_transaction(&self) -> bool {
        self.current_transaction.is_some()
    }

    /// Get current transaction ID if in transaction
    pub fn current_transaction_id(&self) -> Option<u32> {
        self.current_transaction.as_ref().map(|txn| txn.xid)
    }
}

impl Default for CdcParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_creation() {
        let parser = CdcParser::new();
        assert!(!parser.in_transaction());
        assert!(parser.get_relations().is_empty());
    }

    #[test]
    fn test_transaction_state() {
        let mut parser = CdcParser::new();
        assert!(!parser.in_transaction());
        assert!(parser.current_transaction_id().is_none());
        
        parser.current_transaction = Some(TransactionContext {
            xid: 12345,
            commit_timestamp: chrono::Utc::now(),
            events: Vec::new(),
        });
        
        assert!(parser.in_transaction());
        assert_eq!(parser.current_transaction_id(), Some(12345));
        
        parser.clear_transaction();
        assert!(!parser.in_transaction());
        assert!(parser.current_transaction_id().is_none());
    }
}
