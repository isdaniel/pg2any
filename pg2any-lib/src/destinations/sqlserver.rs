use super::destination_factory::DestinationHandler;
use crate::{
    error::{CdcError, Result},
    types::{ChangeEvent, EventType, Transaction},
};
use async_trait::async_trait;
use std::collections::HashMap;
use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{debug, info};

/// SQL Server destination implementation
pub struct SqlServerDestination {
    client: Option<Client<Compat<TcpStream>>>,
    /// Schema mappings: maps source schema to destination schema
    schema_mappings: HashMap<String, String>,
}

impl SqlServerDestination {
    /// Create a new SQL Server destination instance
    pub fn new() -> Self {
        Self {
            client: None,
            schema_mappings: HashMap::new(),
        }
    }

    /// Map a source schema to the destination schema
    /// If no mapping exists, returns the original schema name
    fn map_schema(source_schema: &str, schema_mappings: &HashMap<String, String>) -> String {
        schema_mappings
            .get(source_schema)
            .cloned()
            .unwrap_or_else(|| source_schema.to_string())
    }

    /// Process a single event (helper for transaction processing)
    async fn execute_event_with_mappings(
        client: &mut Client<Compat<TcpStream>>,
        event: &ChangeEvent,
        schema_mappings: &HashMap<String, String>,
    ) -> Result<()> {
        let dest_schema = match &event.event_type {
            EventType::Insert { schema, .. }
            | EventType::Update { schema, .. }
            | EventType::Delete { schema, .. } => Self::map_schema(schema, schema_mappings),
            _ => return Ok(()), // Skip non-DML events
        };

        match &event.event_type {
            EventType::Insert { table, data, .. } => {
                let columns: Vec<String> = data.keys().map(|k| format!("[{}]", k)).collect();
                let placeholders: Vec<String> =
                    (1..=columns.len()).map(|i| format!("@P{}", i)).collect();

                let sql = format!(
                    "INSERT INTO [{}].[{}] ({}) VALUES ({})",
                    dest_schema,
                    table,
                    columns.join(", "),
                    placeholders.join(", ")
                );

                // Note: This is a simplified implementation
                // In a real implementation, you'd need to handle parameters properly
                client
                    .execute(&sql, &[])
                    .await
                    .map_err(|e| CdcError::generic(format!("SQL Server INSERT failed: {}", e)))?;
            }
            EventType::Update {
                table,
                old_data,
                new_data,
                ..
            } => {
                let set_clauses: Vec<String> =
                    new_data.keys().map(|k| format!("[{k}] = ?")).collect();

                let (where_clause, _where_values) = if let Some(old_data) = old_data {
                    let where_clauses: Vec<String> =
                        old_data.keys().map(|k| format!("[{}] = ?", k)).collect();
                    (where_clauses.join(" AND "), old_data)
                } else {
                    let where_clauses: Vec<String> = new_data
                        .keys()
                        .take(1)
                        .map(|k| format!("[{}] = ?", k))
                        .collect();
                    (where_clauses.join(" AND "), new_data)
                };

                let sql = format!(
                    "UPDATE [{}].[{}] SET {} WHERE {}",
                    dest_schema,
                    table,
                    set_clauses.join(", "),
                    where_clause
                );

                client
                    .execute(&sql, &[])
                    .await
                    .map_err(|e| CdcError::generic(format!("SQL Server UPDATE failed: {}", e)))?;
            }
            EventType::Delete {
                table, old_data, ..
            } => {
                let where_clauses: Vec<String> =
                    old_data.keys().map(|k| format!("[{}] = ?", k)).collect();

                let sql = format!(
                    "DELETE FROM [{}].[{}] WHERE {}",
                    dest_schema,
                    table,
                    where_clauses.join(" AND ")
                );

                client
                    .execute(&sql, &[])
                    .await
                    .map_err(|e| CdcError::generic(format!("SQL Server DELETE failed: {}", e)))?;
            }
            _ => {
                debug!("Skipping non-DML event: {:?}", event.event_type);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl DestinationHandler for SqlServerDestination {
    async fn connect(&mut self, connection_string: &str) -> Result<()> {
        let config = Config::from_ado_string(connection_string).map_err(|e| {
            CdcError::generic(format!("Invalid SQL Server connection string: {}", e))
        })?;

        let tcp = TcpStream::connect(config.get_addr())
            .await
            .map_err(|e| CdcError::generic(format!("Failed to connect to SQL Server: {}", e)))?;
        tcp.set_nodelay(true)
            .map_err(|e| CdcError::generic(format!("Failed to set TCP_NODELAY: {}", e)))?;

        let client = Client::connect(config, tcp.compat_write())
            .await
            .map_err(|e| {
                CdcError::generic(format!("Failed to establish SQL Server connection: {}", e))
            })?;

        self.client = Some(client);
        Ok(())
    }

    fn set_schema_mappings(&mut self, mappings: HashMap<String, String>) {
        self.schema_mappings = mappings;
    }

    async fn process_transaction(&mut self, transaction: &Transaction) -> Result<()> {
        // Skip empty transactions
        if transaction.is_empty() {
            debug!(
                "Skipping empty transaction {}",
                transaction.transaction_id
            );
            return Ok(());
        }

        debug!(
            "Processing transaction {} with {} events",
            transaction.transaction_id,
            transaction.event_count()
        );

        // Clone schema_mappings to avoid borrow issues
        let schema_mappings = self.schema_mappings.clone();

        let client = self
            .client
            .as_mut()
            .ok_or_else(|| CdcError::generic("SQL Server connection not established"))?;

        // Begin transaction
        client
            .simple_query("BEGIN TRANSACTION")
            .await
            .map_err(|e| CdcError::generic(format!("Failed to begin SQL Server transaction: {}", e)))?;

        // Process each event
        for event in &transaction.events {
            if let Err(e) = Self::execute_event_with_mappings(client, event, &schema_mappings).await {
                // Rollback on error
                let _ = client.simple_query("ROLLBACK TRANSACTION").await;
                return Err(e);
            }
        }

        // Commit transaction
        client
            .simple_query("COMMIT TRANSACTION")
            .await
            .map_err(|e| CdcError::generic(format!("Failed to commit SQL Server transaction: {}", e)))?;

        info!(
            "Transaction {} committed successfully ({} events)",
            transaction.transaction_id,
            transaction.event_count()
        );

        Ok(())
    }

    async fn health_check(&mut self) -> Result<bool> {
        let client = self
            .client
            .as_mut()
            .ok_or_else(|| CdcError::generic("SQL Server connection not established"))?;

        match client.simple_query("SELECT 1").await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(client) = self.client.take() {
            let _ = client.close().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlserver_destination_creation() {
        let destination = SqlServerDestination::new();
        assert!(destination.client.is_none());
    }
}
