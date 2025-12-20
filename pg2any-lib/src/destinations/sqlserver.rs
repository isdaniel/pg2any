use super::{common, destination_factory::DestinationHandler};
use crate::{
    error::{CdcError, Result},
    types::{ChangeEvent, EventType, ReplicaIdentity, Transaction},
};
use async_trait::async_trait;
use std::collections::HashMap;
use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{debug, info};

/// Maximum number of rows per batch INSERT statement for SQL Server
const MAX_BATCH_INSERT_SIZE: usize = 1000;

/// SQL Server-specific transaction handle (just a flag since connection is stored separately)
pub struct SqlServerTransactionHandle {
    /// Indicates if we have an active transaction (SQL Server keeps the connection, not a separate transaction object)
    pub has_open_transaction: bool,
}

/// Type alias for SQL Server's open transaction
/// Uses generic OpenTransaction with SQL Server-specific handle
type OpenTransaction = common::OpenTransaction<SqlServerTransactionHandle>;

/// SQL Server destination implementation
pub struct SqlServerDestination {
    client: Option<Client<Compat<TcpStream>>>,
    /// Schema mappings: maps source schema to destination schema
    schema_mappings: HashMap<String, String>,
    /// Active transaction state (if any) - used for both streaming and normal transactions
    active_tx: Option<OpenTransaction>,
}

impl SqlServerDestination {
    /// Create a new SQL Server destination instance
    pub fn new() -> Self {
        Self {
            client: None,
            schema_mappings: HashMap::new(),
            active_tx: None,
        }
    }

    /// Helper function to escape SQL string values for SQL Server
    fn escape_sql_string(value: &str) -> String {
        value.replace("'", "''")
    }

    /// Helper function to format a JSON value as SQL Server literal
    fn format_sql_value(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::String(s) => format!("'{}'", Self::escape_sql_string(s)),
            serde_json::Value::Number(n) if n.is_i64() => n.to_string(),
            serde_json::Value::Number(n) if n.is_f64() => n.to_string(),
            serde_json::Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
            serde_json::Value::Null => "NULL".to_string(),
            _ => format!("'{}'", Self::escape_sql_string(&value.to_string())),
        }
    }

    /// Groups consecutive INSERT events for the same table and executes them as batch inserts
    /// This is a static method to avoid borrowing issues
    async fn process_events_with_batching_static<'a>(
        client: &mut Client<Compat<TcpStream>>,
        events: &'a [ChangeEvent],
        schema_mappings: &HashMap<String, String>,
    ) -> Result<()> {
        let mut current_batch: Option<common::InsertBatch<'a>> = None;

        for event in events {
            match &event.event_type {
                EventType::Insert {
                    schema,
                    table,
                    data,
                    ..
                } => {
                    let dest_schema = common::map_schema(schema_mappings, schema);

                    // Get column names in sorted order for consistency
                    let mut columns: Vec<String> = data.keys().cloned().collect();
                    columns.sort();

                    // Check if we can add to current batch
                    let can_add = current_batch
                        .as_ref()
                        .map(|b| b.can_add(Some(&dest_schema), table, &columns))
                        .unwrap_or(false);

                    if can_add {
                        // Add to existing batch
                        let batch = current_batch.as_mut().unwrap();
                        batch.add_row(data);

                        // Execute if batch is full
                        if batch.len() >= MAX_BATCH_INSERT_SIZE {
                            Self::execute_batch_insert_static(client, batch).await?;
                            current_batch = None;
                        }
                    } else {
                        // Flush existing batch and start new one
                        if let Some(batch) = current_batch.take() {
                            Self::execute_batch_insert_static(client, &batch).await?;
                        }

                        let mut new_batch = common::InsertBatch::new_with_schema(
                            dest_schema,
                            table.clone(),
                            columns,
                        );
                        new_batch.add_row(data);
                        current_batch = Some(new_batch);
                    }
                }
                EventType::Update { .. } => {
                    // Non-INSERT event: flush any pending batch and process event
                    if let Some(batch) = current_batch.take() {
                        Self::execute_batch_insert_static(client, &batch).await?;
                    }
                    Self::process_event_static(client, event, schema_mappings).await?;
                }
                EventType::Delete { .. } => {
                    // Non-INSERT event: flush any pending batch and process event
                    if let Some(batch) = current_batch.take() {
                        Self::execute_batch_insert_static(client, &batch).await?;
                    }
                    Self::process_event_static(client, event, schema_mappings).await?;
                }
                _ => {
                    // Other events: flush any pending batch and process event
                    if let Some(batch) = current_batch.take() {
                        Self::execute_batch_insert_static(client, &batch).await?;
                    }
                    Self::process_event_static(client, event, schema_mappings).await?;
                }
            }
        }

        // Flush any remaining batch
        if let Some(batch) = current_batch.take() {
            Self::execute_batch_insert_static(client, &batch).await?;
        }

        Ok(())
    }

    /// Static version of execute_batch_insert for use in static context
    async fn execute_batch_insert_static<'a>(
        client: &mut Client<Compat<TcpStream>>,
        batch: &common::InsertBatch<'a>,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let columns: Vec<String> = batch.columns.iter().map(|k| format!("[{}]", k)).collect();

        // Build VALUES rows: (val1, val2), (val3, val4), ...
        let rows_sql: Vec<String> = batch
            .rows
            .iter()
            .map(|row| {
                let values: Vec<String> = row.iter().map(|v| Self::format_sql_value(v)).collect();
                format!("({})", values.join(", "))
            })
            .collect();

        let schema = batch.schema.as_ref().unwrap();
        let sql = format!(
            "INSERT INTO [{}].[{}] ({}) VALUES {}",
            schema,
            batch.table,
            columns.join(", "),
            rows_sql.join(", ")
        );

        let batch_start = std::time::Instant::now();
        client
            .execute(&sql, &[])
            .await
            .map_err(|e| CdcError::generic(format!("SQL Server batch INSERT failed: {}", e)))?;
        let batch_duration = batch_start.elapsed();

        info!(
            "SQL Server Batch INSERT: {} rows into [{}].[{}] in {:?}",
            batch.rows.len(),
            schema,
            batch.table,
            batch_duration
        );

        Ok(())
    }

    /// Static version of process_event_in_transaction for use in static context
    async fn process_event_static<'a>(
        client: &mut Client<Compat<TcpStream>>,
        event: &'a ChangeEvent,
        schema_mappings: &HashMap<String, String>,
    ) -> Result<()> {
        match &event.event_type {
            EventType::Insert {
                schema,
                table,
                data,
                ..
            } => {
                let dest_schema = schema_mappings
                    .get(schema)
                    .cloned()
                    .unwrap_or_else(|| schema.to_string());
                let columns: Vec<String> = data.keys().map(|k| format!("[{}]", k)).collect();
                let values: Vec<String> =
                    data.values().map(|v| Self::format_sql_value(v)).collect();

                let sql = format!(
                    "INSERT INTO [{}].[{}] ({}) VALUES ({})",
                    dest_schema,
                    table,
                    columns.join(", "),
                    values.join(", ")
                );

                client
                    .execute(&sql, &[])
                    .await
                    .map_err(|e| CdcError::generic(format!("SQL Server INSERT failed: {}", e)))?;
                debug!(
                    "Successfully inserted record into {}.{}",
                    dest_schema, table
                );
            }
            EventType::Update {
                schema,
                table,
                old_data,
                new_data,
                replica_identity,
                key_columns,
                ..
            } => {
                let dest_schema = schema_mappings
                    .get(schema)
                    .cloned()
                    .unwrap_or_else(|| schema.to_string());
                let set_clauses: Vec<String> = new_data
                    .iter()
                    .map(|(k, v)| format!("[{}] = {}", k, Self::format_sql_value(v)))
                    .collect();

                let where_clause = Self::build_where_clause_for_update_static(
                    old_data,
                    new_data,
                    replica_identity,
                    key_columns,
                    schema,
                    table,
                )?;

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
                debug!("Successfully updated record in {}.{}", dest_schema, table);
            }
            EventType::Delete {
                schema,
                table,
                old_data,
                replica_identity,
                key_columns,
                ..
            } => {
                let dest_schema = schema_mappings
                    .get(schema)
                    .cloned()
                    .unwrap_or_else(|| schema.to_string());
                let where_clause = Self::build_where_clause_for_delete_static(
                    old_data,
                    replica_identity,
                    key_columns,
                    schema,
                    table,
                )?;

                let sql = format!(
                    "DELETE FROM [{}].[{}] WHERE {}",
                    dest_schema, table, where_clause
                );

                client
                    .execute(&sql, &[])
                    .await
                    .map_err(|e| CdcError::generic(format!("SQL Server DELETE failed: {}", e)))?;
                debug!("Successfully deleted record from {}.{}", dest_schema, table);
            }
            EventType::Truncate(truncate_tables) => {
                for table_full_name in truncate_tables {
                    let mut parts = table_full_name.splitn(2, '.');
                    let sql = match (parts.next(), parts.next()) {
                        (Some(schema), Some(table)) => {
                            let dest_schema = schema_mappings
                                .get(schema)
                                .cloned()
                                .unwrap_or_else(|| schema.to_string());
                            format!("TRUNCATE TABLE [{}].[{}]", dest_schema, table)
                        }
                        _ => format!("TRUNCATE TABLE [{}]", table_full_name),
                    };
                    client.execute(&sql, &[]).await.map_err(|e| {
                        CdcError::generic(format!("SQL Server TRUNCATE failed: {}", e))
                    })?;
                }
            }
            _ => {
                debug!("Skipping non-DML event: {:?}", event.event_type);
            }
        }

        Ok(())
    }

    /// Build WHERE clause for UPDATE operations
    fn build_where_clause_for_update_static(
        old_data: &Option<HashMap<String, serde_json::Value>>,
        new_data: &HashMap<String, serde_json::Value>,
        replica_identity: &ReplicaIdentity,
        key_columns: &[String],
        schema: &str,
        table: &str,
    ) -> Result<String> {
        let conditions = common::build_where_conditions_for_update(
            old_data,
            new_data,
            replica_identity,
            key_columns,
            schema,
            table,
        )?;

        // Format conditions for SQL Server (using brackets)
        let formatted_conditions: Vec<String> = conditions
            .iter()
            .map(|(column, value)| format!("[{}] = {}", column, Self::format_sql_value(value)))
            .collect();

        Ok(formatted_conditions.join(" AND "))
    }

    /// Build WHERE clause for DELETE operations
    fn build_where_clause_for_delete_static(
        old_data: &HashMap<String, serde_json::Value>,
        replica_identity: &ReplicaIdentity,
        key_columns: &[String],
        schema: &str,
        table: &str,
    ) -> Result<String> {
        let conditions = common::build_where_conditions_for_delete(
            old_data,
            replica_identity,
            key_columns,
            schema,
            table,
        )?;

        // Format conditions for SQL Server (using brackets)
        let formatted_conditions: Vec<String> = conditions
            .iter()
            .map(|(column, value)| format!("[{}] = {}", column, Self::format_sql_value(value)))
            .collect();

        Ok(formatted_conditions.join(" AND "))
    }

    /// Rollback the active streaming transaction (for graceful shutdown)
    async fn rollback_active_transaction(&mut self) -> Result<()> {
        if let Some(active) = self.active_tx.take() {
            if !active.handle.has_open_transaction {
                return Ok(());
            }

            tracing::warn!(
                "SQL Server: Rolling back streaming transaction {} ({} events processed)",
                active.state.transaction_id,
                active.state.events_processed
            );

            if let Some(client) = self.client.as_mut() {
                if let Err(e) = client.simple_query("ROLLBACK TRANSACTION").await {
                    tracing::warn!(
                        "SQL Server: Failed to rollback streaming transaction {}: {}",
                        active.state.transaction_id,
                        e
                    );
                }
            }
        }

        Ok(())
    }

    /// Process a transaction batch
    ///
    /// Each batch is processed and committed immediately in its own transaction.
    /// This applies to both intermediate batches (batch_size reached) and
    /// final batches (Commit/StreamCommit received).
    async fn process_transaction_batch(&mut self, transaction: &Transaction) -> Result<()> {
        let tx_id = transaction.transaction_id;

        // Skip empty batches
        if transaction.is_empty() {
            debug!("Skipping empty batch for transaction {}", tx_id);
            return Ok(());
        }

        let tx_start = std::time::Instant::now();

        debug!(
            "SQL Server: Processing transaction {} ({} events)",
            tx_id,
            transaction.event_count()
        );

        let client = self
            .client
            .as_mut()
            .ok_or_else(|| CdcError::generic("SQL Server connection not established"))?;

        // Begin a new transaction for this batch
        client
            .simple_query("BEGIN TRANSACTION")
            .await
            .map_err(|e| {
                CdcError::generic(format!("Failed to begin SQL Server transaction: {}", e))
            })?;

        let schema_mappings = self.schema_mappings.clone();

        let result = Self::process_events_with_batching_static(
            client,
            &transaction.events,
            &schema_mappings,
        )
        .await;

        if let Err(e) = result {
            tracing::warn!(
                "SQL Server: Error processing batch, rolling back transaction {}: {}",
                tx_id,
                e
            );
            let _ = client.simple_query("ROLLBACK TRANSACTION").await;
            return Err(e);
        }

        // Commit the transaction
        client
            .simple_query("COMMIT TRANSACTION")
            .await
            .map_err(|e| {
                CdcError::generic(format!("Failed to commit SQL Server transaction: {}", e))
            })?;

        info!(
            "SQL Server: Committed batch for transaction {} ({} events) in {:?}",
            tx_id,
            transaction.event_count(),
            tx_start.elapsed()
        );

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
        if !self.schema_mappings.is_empty() {
            debug!(
                "SQL Server destination schema mappings set: {:?}",
                self.schema_mappings
            );
        }
    }

    async fn process_transaction(&mut self, transaction: &Transaction) -> Result<()> {
        // Unified transaction processing handles both normal and streaming transactions
        self.process_transaction_batch(transaction).await
    }

    fn get_active_streaming_transaction_id(&self) -> Option<u32> {
        self.active_tx
            .as_ref()
            .filter(|tx| tx.handle.has_open_transaction)
            .map(|tx| tx.state.transaction_id)
    }

    async fn rollback_streaming_transaction(&mut self) -> Result<()> {
        self.rollback_active_transaction().await
    }

    async fn close(&mut self) -> Result<()> {
        if self.active_tx.is_some() {
            tracing::warn!("SQL Server: Closing with active transaction - rolling back");
            self.rollback_active_transaction().await?;
        }

        if let Some(client) = self.client.take() {
            let _ = client.close().await;
        }
        self.client = None;
        info!("SQL Server connection closed successfully");
        Ok(())
    }
}

impl Default for SqlServerDestination {
    fn default() -> Self {
        Self::new()
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
