use super::destination_factory::DestinationHandler;
use crate::{
    error::{CdcError, Result},
    types::{ChangeEvent, EventType, Transaction},
};
use async_trait::async_trait;
use sqlx::MySqlPool;
use std::collections::HashMap;
use tracing::{debug, info};

/// Maximum number of rows per batch INSERT statement, This is limited by MySQL's max_allowed_packet and number of placeholders
const MAX_BATCH_INSERT_SIZE: usize = 1000;

/// MySQL destination implementation
pub struct MySQLDestination {
    pool: Option<MySqlPool>,
    /// Schema mappings: maps source schema to destination database
    /// e.g., "public" -> "cdc_db"
    schema_mappings: HashMap<String, String>,
}

/// Helper struct for building WHERE clauses with proper parameter binding
/// Uses references to avoid unnecessary cloning of JSON values
#[derive(Debug)]
struct WhereClause<'a> {
    sql: String,
    bind_values: Vec<&'a serde_json::Value>,
}

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

impl MySQLDestination {
    /// Create a new MySQL destination instance
    pub fn new() -> Self {
        Self {
            pool: None,
            schema_mappings: HashMap::new(),
        }
    }

    /// Map a source schema to the destination schema/database
    /// If no mapping exists, returns the original schema name
    fn map_schema(&self, source_schema: &str) -> String {
        self.schema_mappings
            .get(source_schema)
            .cloned()
            .unwrap_or_else(|| source_schema.to_string())
    }

    fn bind_value<'a>(
        &self,
        query: sqlx::query::Query<'a, sqlx::MySql, sqlx::mysql::MySqlArguments>,
        value: &'a serde_json::Value,
    ) -> sqlx::query::Query<'a, sqlx::MySql, sqlx::mysql::MySqlArguments> {
        match value {
            serde_json::Value::String(s) => query.bind(s.as_str()),
            serde_json::Value::Number(n) if n.is_i64() => query.bind(n.as_i64().unwrap()),
            serde_json::Value::Number(n) if n.is_f64() => query.bind(n.as_f64().unwrap()),
            serde_json::Value::Bool(b) => query.bind(*b),
            serde_json::Value::Null => query.bind(Option::<String>::None),
            _ => query.bind(value.to_string()),
        }
    }

    fn build_where_clause_for_update<'a>(
        &self,
        old_data: &'a Option<HashMap<String, serde_json::Value>>,
        new_data: &'a HashMap<String, serde_json::Value>,
        replica_identity: &crate::types::ReplicaIdentity,
        key_columns: &[String],
        schema: &str,
        table: &str,
    ) -> Result<WhereClause<'a>> {
        use crate::types::ReplicaIdentity;

        match replica_identity {
            ReplicaIdentity::Full => {
                // Always require old_data
                if let Some(old) = old_data {
                    let mut conditions = Vec::with_capacity(old.len());
                    let mut bind_values = Vec::with_capacity(old.len());

                    for (column, value) in old {
                        conditions.push(format!("`{}` = ?", column));
                        bind_values.push(value);
                    }

                    Ok(WhereClause {
                        sql: conditions.join(" AND "),
                        bind_values,
                    })
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

                let mut conditions = Vec::with_capacity(key_columns.len());
                let mut bind_values = Vec::with_capacity(key_columns.len());

                // Use old_data if available, otherwise use new_data
                let data_source: &HashMap<String, serde_json::Value> = match old_data {
                    Some(old) => old,
                    None => new_data,
                };

                for key_column in key_columns {
                    if let Some(value) = data_source.get(key_column) {
                        conditions.push(format!("`{}` = ?", key_column));
                        bind_values.push(value);
                    } else {
                        return Err(CdcError::generic(format!(
                            "Key column '{}' not found in data for UPDATE on {}.{}",
                            key_column, schema, table
                        )));
                    }
                }

                Ok(WhereClause {
                    sql: conditions.join(" AND "),
                    bind_values,
                })
            }

            ReplicaIdentity::Nothing => {
                if key_columns.is_empty() {
                    return Err(CdcError::generic(format!(
                        "Cannot perform UPDATE on {}.{} with REPLICA IDENTITY NOTHING and no key columns.",
                        schema, table
                    )));
                }

                let mut conditions = Vec::with_capacity(key_columns.len());
                let mut bind_values = Vec::with_capacity(key_columns.len());

                for key_column in key_columns {
                    if let Some(value) = new_data.get(key_column) {
                        conditions.push(format!("`{}` = ?", key_column));
                        bind_values.push(value);
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

                Ok(WhereClause {
                    sql: conditions.join(" AND "),
                    bind_values,
                })
            }
        }
    }

    fn build_where_clause_for_delete<'a>(
        &self,
        old_data: &'a HashMap<String, serde_json::Value>,
        replica_identity: &crate::types::ReplicaIdentity,
        key_columns: &[String],
        schema: &str,
        table: &str,
    ) -> Result<WhereClause<'a>> {
        use crate::types::ReplicaIdentity;

        // For DELETE, old_data is always provided, handle each replica identity case directly
        match replica_identity {
            ReplicaIdentity::Full => {
                let mut conditions = Vec::with_capacity(old_data.len());
                let mut bind_values = Vec::with_capacity(old_data.len());

                for (column, value) in old_data {
                    conditions.push(format!("`{}` = ?", column));
                    bind_values.push(value);
                }

                Ok(WhereClause {
                    sql: conditions.join(" AND "),
                    bind_values,
                })
            }

            ReplicaIdentity::Default | ReplicaIdentity::Index => {
                if key_columns.is_empty() {
                    return Err(CdcError::generic(format!(
                        "No key columns available for DELETE operation on {}.{}. Check table's replica identity setting.",
                        schema, table
                    )));
                }

                let mut conditions = Vec::with_capacity(key_columns.len());
                let mut bind_values = Vec::with_capacity(key_columns.len());

                for key_column in key_columns {
                    if let Some(value) = old_data.get(key_column) {
                        conditions.push(format!("`{}` = ?", key_column));
                        bind_values.push(value);
                    } else {
                        return Err(CdcError::generic(format!(
                            "Key column '{}' not found in data for DELETE on {}.{}",
                            key_column, schema, table
                        )));
                    }
                }

                Ok(WhereClause {
                    sql: conditions.join(" AND "),
                    bind_values,
                })
            }

            ReplicaIdentity::Nothing => Err(CdcError::generic(format!(
                "Cannot perform DELETE on {}.{} with REPLICA IDENTITY NOTHING. \
                DELETE requires a replica identity.",
                schema, table
            ))),
        }
    }

    /// Process a single event within an existing database transaction
    async fn process_event_in_transaction<'a>(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        event: &'a ChangeEvent,
    ) -> Result<()> {
        match &event.event_type {
            EventType::Insert {
                schema,
                table,
                data,
                ..
            } => {
                let dest_schema = self.map_schema(schema);
                let columns: Vec<String> = data.keys().map(|k| format!("`{}`", k)).collect();
                let placeholders: Vec<String> =
                    (0..columns.len()).map(|_| "?".to_string()).collect();

                let sql = format!(
                    "INSERT INTO `{}`.`{}` ({}) VALUES ({})",
                    dest_schema,
                    table,
                    columns.join(", "),
                    placeholders.join(", ")
                );

                let mut query = sqlx::query(&sql);
                for (_, value) in data {
                    query = match value {
                        serde_json::Value::String(s) => query.bind(s),
                        serde_json::Value::Number(n) if n.is_i64() => {
                            query.bind(n.as_i64().unwrap())
                        }
                        serde_json::Value::Number(n) if n.is_f64() => {
                            query.bind(n.as_f64().unwrap())
                        }
                        serde_json::Value::Bool(b) => query.bind(*b),
                        _ => query.bind(value.to_string()),
                    };
                }

                query.execute(&mut **tx).await?;
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
                let dest_schema = self.map_schema(schema);
                let set_clauses: Vec<String> =
                    new_data.keys().map(|k| format!("`{}` = ?", k)).collect();

                let where_clause = self.build_where_clause_for_update(
                    old_data,
                    new_data,
                    replica_identity,
                    key_columns,
                    schema,
                    table,
                )?;

                let sql = format!(
                    "UPDATE `{}`.`{}` SET {} WHERE {}",
                    dest_schema,
                    table,
                    set_clauses.join(", "),
                    where_clause.sql
                );

                let mut query = sqlx::query(&sql);

                for (_, value) in new_data {
                    query = self.bind_value(query, value);
                }

                for value in where_clause.bind_values {
                    query = self.bind_value(query, value);
                }

                query.execute(&mut **tx).await?;
            }
            EventType::Delete {
                schema,
                table,
                old_data,
                replica_identity,
                key_columns,
                ..
            } => {
                let dest_schema = self.map_schema(schema);
                let where_clause = self.build_where_clause_for_delete(
                    old_data,
                    replica_identity,
                    key_columns,
                    schema,
                    table,
                )?;

                let sql = format!(
                    "DELETE FROM `{}`.`{}` WHERE {}",
                    dest_schema, table, where_clause.sql
                );

                let mut query = sqlx::query(&sql);

                for value in where_clause.bind_values {
                    query = self.bind_value(query, value);
                }

                query.execute(&mut **tx).await?;
            }
            EventType::Truncate(truncate_tables) => {
                for table_full_name in truncate_tables {
                    let mut parts = table_full_name.splitn(2, '.');
                    let sql = match (parts.next(), parts.next()) {
                        (Some(schema), Some(table)) => {
                            let dest_schema = self.map_schema(schema);
                            format!("TRUNCATE TABLE `{}`.`{}`", dest_schema, table)
                        }
                        _ => format!("TRUNCATE TABLE `{}`", table_full_name),
                    };
                    sqlx::query(&sql).execute(&mut **tx).await?;
                }
            }
            _ => {
                // Skip non-DML events (BEGIN, COMMIT, RELATION, etc.)
                debug!("Skipping non-DML event: {:?}", event.event_type);
            }
        }

        Ok(())
    }

    /// Uses multi-value INSERT: INSERT INTO table (cols) VALUES (v1), (v2), ...
    async fn execute_batch_insert<'a>(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        batch: &InsertBatch<'a>,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let dest_schema = self.map_schema(&batch.schema);
        let columns: Vec<String> = batch.columns.iter().map(|k| format!("`{}`", k)).collect();
        let num_columns = columns.len();

        // Build placeholders for one row: (?, ?, ?)
        let row_placeholder = format!(
            "({})",
            (0..num_columns).map(|_| "?").collect::<Vec<_>>().join(", ")
        );

        // Build all row placeholders: (?, ?, ?), (?, ?, ?), ...
        let all_placeholders: Vec<String> = (0..batch.rows.len())
            .map(|_| row_placeholder.clone())
            .collect();

        let sql = format!(
            "INSERT INTO `{}`.`{}` ({}) VALUES {}",
            dest_schema,
            batch.table,
            columns.join(", "),
            all_placeholders.join(", ")
        );

        let mut query = sqlx::query(&sql);

        // Bind all values for all rows
        for row in &batch.rows {
            for value in row {
                query = self.bind_value(query, value);
            }
        }

        let batch_start = std::time::Instant::now();
        query.execute(&mut **tx).await?;
        let batch_duration = batch_start.elapsed();

        info!(
            "MySQL Batch INSERT: {} rows into `{}`.`{}` in {:?}",
            batch.rows.len(),
            dest_schema,
            batch.table,
            batch_duration
        );

        Ok(())
    }

    /// Process events with batch INSERT optimization for streaming transactions
    /// Groups consecutive INSERT events for the same table and executes them as batch inserts
    async fn process_events_with_batching<'a>(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        events: &'a [ChangeEvent],
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
                    let dest_schema = self.map_schema(schema);
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
                            self.execute_batch_insert(tx, batch).await?;
                            current_batch = None;
                        }
                    } else {
                        // Flush existing batch and start new one
                        if let Some(batch) = current_batch.take() {
                            self.execute_batch_insert(tx, &batch).await?;
                        }

                        let mut new_batch = InsertBatch::new(dest_schema, table.clone(), columns);
                        new_batch.add_row(data);
                        current_batch = Some(new_batch);
                    }
                }
                EventType::Update { .. } => {
                    // Non-INSERT event: flush any pending batch and process event
                    if let Some(batch) = current_batch.take() {
                        self.execute_batch_insert(tx, &batch).await?;
                    }
                    self.process_event_in_transaction(tx, event).await?;
                }
                EventType::Delete { .. } => {
                    // Non-INSERT event: flush any pending batch and process event
                    if let Some(batch) = current_batch.take() {
                        self.execute_batch_insert(tx, &batch).await?;
                    }
                    self.process_event_in_transaction(tx, event).await?;
                }
                _ => {
                    // Other events: flush any pending batch and process event
                    if let Some(batch) = current_batch.take() {
                        self.execute_batch_insert(tx, &batch).await?;
                    }
                    self.process_event_in_transaction(tx, event).await?;
                }
            }
        }

        // Flush any remaining batch
        if let Some(batch) = current_batch.take() {
            self.execute_batch_insert(tx, &batch).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl DestinationHandler for MySQLDestination {
    async fn connect(&mut self, connection_string: &str) -> Result<()> {
        let pool = MySqlPool::connect(connection_string).await?;
        self.pool = Some(pool);
        Ok(())
    }

    fn set_schema_mappings(&mut self, mappings: HashMap<String, String>) {
        self.schema_mappings = mappings;
        if !self.schema_mappings.is_empty() {
            debug!(
                "MySQL destination schema mappings set: {:?}",
                self.schema_mappings
            );
        }
    }

    async fn process_transaction(&mut self, transaction: &Transaction) -> Result<()> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("MySQL connection not established"))?;

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
            "MySQL: Starting to process transaction {} with {} events (streaming={}, final={})",
            transaction.transaction_id,
            transaction.event_count(),
            transaction.is_streaming,
            transaction.is_final_batch
        );

        // Start a database transaction
        let begin_time = std::time::Instant::now();
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to begin MySQL transaction: {}", e)))?;
        debug!("MySQL: BEGIN took {:?}", begin_time.elapsed());

        let process_time = std::time::Instant::now();
        let result = self
            .process_events_with_batching(&mut tx, &transaction.events)
            .await;
        debug!("MySQL: Event processing took {:?}", process_time.elapsed());

        if let Err(e) = result {
            // Rollback on error
            tx.rollback().await.map_err(|re| {
                CdcError::generic(format!(
                    "Failed to rollback transaction after error '{}': {}",
                    e, re
                ))
            })?;
            return Err(e);
        }

        // Commit the transaction
        let commit_time = std::time::Instant::now();
        tx.commit()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to commit MySQL transaction: {}", e)))?;
        let commit_duration = commit_time.elapsed();

        debug!(
            "MySQL: COMMIT took {:?} for transaction {} ({} events, total time: {:?})",
            commit_duration,
            transaction.transaction_id,
            transaction.event_count(),
            tx_start.elapsed()
        );

        Ok(())
    }

    async fn health_check(&mut self) -> Result<bool> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("MySQL connection not established"))?;

        match sqlx::query("SELECT 1").fetch_one(pool).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(pool) = &self.pool {
            pool.close().await;
        }
        self.pool = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_destination_creation() {
        let destination = MySQLDestination::new();
        assert!(destination.pool.is_none());
    }
}
