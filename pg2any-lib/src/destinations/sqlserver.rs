use super::destination_factory::DestinationHandler;
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

/// Represents a group of INSERT events for the same table that can be batched
struct InsertBatch<'a> {
    schema: String,
    table: String,
    /// Column names in consistent order
    columns: Vec<String>,
    /// Values for each row, in the same column order
    rows: Vec<Vec<&'a serde_json::Value>>,
}

impl<'a> InsertBatch<'a> {
    fn new(schema: String, table: String, columns: Vec<String>) -> Self {
        Self {
            schema,
            table,
            columns,
            rows: Vec::new(),
        }
    }

    fn add_row(&mut self, data: &'a HashMap<String, serde_json::Value>) {
        let values: Vec<&serde_json::Value> = self
            .columns
            .iter()
            .map(|col| data.get(col).unwrap_or(&serde_json::Value::Null))
            .collect();
        self.rows.push(values);
    }

    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn can_add(&self, schema: &str, table: &str, columns: &[String]) -> bool {
        self.schema == schema && self.table == table && self.columns == columns
    }
}

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
        let mut current_batch: Option<InsertBatch<'a>> = None;

        for event in events {
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

                    // Get column names in sorted order for consistency
                    let mut columns: Vec<String> = data.keys().cloned().collect();
                    columns.sort();

                    // Check if we can add to current batch
                    let can_add = current_batch
                        .as_ref()
                        .map(|b| b.can_add(&dest_schema, table, &columns))
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

                        let mut new_batch = InsertBatch::new(dest_schema, table.clone(), columns);
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
        batch: &InsertBatch<'a>,
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

        let sql = format!(
            "INSERT INTO [{}].[{}] ({}) VALUES {}",
            batch.schema,
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
            batch.schema,
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
        match replica_identity {
            ReplicaIdentity::Full => {
                if let Some(old) = old_data {
                    let conditions: Vec<String> = old
                        .iter()
                        .map(|(column, value)| {
                            format!("[{}] = {}", column, Self::format_sql_value(value))
                        })
                        .collect();
                    Ok(conditions.join(" AND "))
                } else {
                    Err(CdcError::generic(format!(
                        "REPLICA IDENTITY FULL requires old_data but none provided for {}.{} during UPDATE",
                        schema, table
                    )))
                }
            }
            ReplicaIdentity::Default | ReplicaIdentity::Index => {
                if key_columns.is_empty() {
                    return Err(CdcError::generic(format!(
                        "No key columns available for UPDATE operation on {}.{}. Check table's replica identity setting.",
                        schema, table
                    )));
                }

                let data_source = match old_data {
                    Some(old) => old,
                    None => new_data,
                };

                let mut conditions = Vec::with_capacity(key_columns.len());
                for key_column in key_columns {
                    if let Some(value) = data_source.get(key_column) {
                        conditions.push(format!(
                            "[{}] = {}",
                            key_column,
                            Self::format_sql_value(value)
                        ));
                    } else {
                        return Err(CdcError::generic(format!(
                            "Key column '{}' not found in data for UPDATE on {}.{}",
                            key_column, schema, table
                        )));
                    }
                }

                Ok(conditions.join(" AND "))
            }
            ReplicaIdentity::Nothing => {
                if key_columns.is_empty() {
                    return Err(CdcError::generic(format!(
                        "Cannot perform UPDATE on {}.{} with REPLICA IDENTITY NOTHING and no key columns.",
                        schema, table
                    )));
                }

                let mut conditions = Vec::new();
                for key_column in key_columns {
                    if let Some(value) = new_data.get(key_column) {
                        conditions.push(format!(
                            "[{}] = {}",
                            key_column,
                            Self::format_sql_value(value)
                        ));
                    }
                }

                if conditions.is_empty() {
                    return Err(CdcError::generic(format!(
                        "No usable key columns found for UPDATE on {}.{} with REPLICA IDENTITY NOTHING",
                        schema, table
                    )));
                }

                debug!(
                    "WARNING: Using potentially unreliable WHERE clause for UPDATE on {}.{} due to REPLICA IDENTITY NOTHING",
                    schema, table
                );

                Ok(conditions.join(" AND "))
            }
        }
    }

    /// Build WHERE clause for DELETE operations
    fn build_where_clause_for_delete_static(
        old_data: &HashMap<String, serde_json::Value>,
        replica_identity: &ReplicaIdentity,
        key_columns: &[String],
        schema: &str,
        table: &str,
    ) -> Result<String> {
        match replica_identity {
            ReplicaIdentity::Full => {
                let conditions: Vec<String> = old_data
                    .iter()
                    .map(|(column, value)| {
                        format!("[{}] = {}", column, Self::format_sql_value(value))
                    })
                    .collect();
                Ok(conditions.join(" AND "))
            }
            ReplicaIdentity::Default | ReplicaIdentity::Index => {
                if key_columns.is_empty() {
                    return Err(CdcError::generic(format!(
                        "No key columns available for DELETE operation on {}.{}. Check table's replica identity setting.",
                        schema, table
                    )));
                }

                let mut conditions = Vec::with_capacity(key_columns.len());
                for key_column in key_columns {
                    if let Some(value) = old_data.get(key_column) {
                        conditions.push(format!(
                            "[{}] = {}",
                            key_column,
                            Self::format_sql_value(value)
                        ));
                    } else {
                        return Err(CdcError::generic(format!(
                            "Key column '{}' not found in data for DELETE on {}.{}",
                            key_column, schema, table
                        )));
                    }
                }

                Ok(conditions.join(" AND "))
            }
            ReplicaIdentity::Nothing => Err(CdcError::generic(format!(
                "Cannot perform DELETE on {}.{} with REPLICA IDENTITY NOTHING. \
                DELETE requires a replica identity.",
                schema, table
            ))),
        }
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
        // Skip empty transactions
        if transaction.is_empty() {
            debug!(
                "Skipping empty transaction {} (streaming={}, final={})",
                transaction.transaction_id, transaction.is_streaming, transaction.is_final_batch
            );
            return Ok(());
        }

        let tx_start = std::time::Instant::now();

        debug!(
            "SQL Server: Starting to process transaction {} with {} events (streaming={}, final={})",
            transaction.transaction_id,
            transaction.event_count(),
            transaction.is_streaming,
            transaction.is_final_batch
        );

        // Copy schema_mappings to avoid borrowing self
        let schema_mappings = self.schema_mappings.clone();

        let client = self
            .client
            .as_mut()
            .ok_or_else(|| CdcError::generic("SQL Server connection not established"))?;

        // Begin transaction
        let begin_time = std::time::Instant::now();
        client
            .simple_query("BEGIN TRANSACTION")
            .await
            .map_err(|e| {
                CdcError::generic(format!("Failed to begin SQL Server transaction: {}", e))
            })?;
        debug!("SQL Server: BEGIN took {:?}", begin_time.elapsed());

        // Process events with batching optimization
        let process_time = std::time::Instant::now();
        let result = Self::process_events_with_batching_static(
            client,
            &transaction.events,
            &schema_mappings,
        )
        .await;
        debug!(
            "SQL Server: Event processing took {:?}",
            process_time.elapsed()
        );

        if let Err(e) = result {
            // Rollback on error
            let _ = client.simple_query("ROLLBACK TRANSACTION").await;
            return Err(e);
        }

        // Commit transaction
        let commit_time = std::time::Instant::now();
        client
            .simple_query("COMMIT TRANSACTION")
            .await
            .map_err(|e| {
                CdcError::generic(format!("Failed to commit SQL Server transaction: {}", e))
            })?;
        let commit_duration = commit_time.elapsed();

        debug!(
            "SQL Server: COMMIT took {:?} for transaction {} ({} events, total time: {:?})",
            commit_duration,
            transaction.transaction_id,
            transaction.event_count(),
            tx_start.elapsed()
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
