use super::destination_factory::DestinationHandler;
use crate::{
    error::{CdcError, Result},
    types::{ChangeEvent, EventType, ReplicaIdentity, Transaction},
};
use async_trait::async_trait;
use sqlx::{sqlite::SqliteConnectOptions, SqlitePool};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use tracing::{debug, error, info};

/// Maximum number of rows per batch INSERT statement for SQLite
/// SQLite has a limit on the number of variables per statement (SQLITE_MAX_VARIABLE_NUMBER, default 999)
const MAX_BATCH_INSERT_SIZE: usize = 500;

/// Helper struct for building WHERE clauses with proper parameter binding
/// Uses references to avoid unnecessary cloning of JSON values
#[derive(Debug)]
struct WhereClause<'a> {
    sql: String,
    bind_values: Vec<&'a serde_json::Value>,
}

/// SQLite destination implementation with batch INSERT support
///
/// This implementation uses sqlx for database operations with async support.
/// SQLite connections are managed through sqlx's connection pool.
pub struct SQLiteDestination {
    pool: Option<SqlitePool>,
    database_path: Option<String>,
}

/// Represents a group of INSERT events for the same table that can be batched
struct InsertBatch<'a> {
    table: String,
    /// Column names in consistent order
    columns: Vec<String>,
    /// Values for each row, in the same column order
    rows: Vec<Vec<&'a serde_json::Value>>,
}

impl<'a> InsertBatch<'a> {
    fn new(table: String, columns: Vec<String>) -> Self {
        Self {
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

    fn can_add(&self, table: &str, columns: &[String]) -> bool {
        self.table == table && self.columns == columns
    }
}

impl SQLiteDestination {
    /// Create a new SQLite destination instance
    pub fn new() -> Self {
        Self {
            pool: None,
            database_path: None,
        }
    }

    /// Helper function to bind serde_json::Value to sqlx query
    fn bind_value<'a>(
        &self,
        query: sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>>,
        value: &'a serde_json::Value,
    ) -> sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>> {
        match value {
            serde_json::Value::String(s) => query.bind(s.as_str()),
            serde_json::Value::Number(n) if n.is_i64() => query.bind(n.as_i64().unwrap()),
            serde_json::Value::Number(n) if n.is_f64() => query.bind(n.as_f64().unwrap()),
            serde_json::Value::Bool(b) => query.bind(*b),
            serde_json::Value::Null => query.bind(Option::<String>::None),
            _ => query.bind(value.to_string()), // Store complex types as JSON strings
        }
    }

    /// Build WHERE clause for UPDATE operations
    fn build_where_clause_for_update<'a>(
        &self,
        old_data: &'a Option<HashMap<String, serde_json::Value>>,
        new_data: &'a HashMap<String, serde_json::Value>,
        replica_identity: &ReplicaIdentity,
        key_columns: &[String],
        schema: &str,
        table: &str,
    ) -> Result<WhereClause<'a>> {
        match replica_identity {
            ReplicaIdentity::Full => {
                // Always require old_data for FULL replica identity
                if let Some(old) = old_data {
                    let mut conditions = Vec::with_capacity(old.len());
                    let mut bind_values = Vec::with_capacity(old.len());

                    for (column, value) in old {
                        conditions.push(format!("\"{}\" = ?", column));
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

                // Use old_data if available, otherwise fall back to new_data
                let data_source: &HashMap<String, serde_json::Value> = match old_data {
                    Some(old) => old,
                    None => new_data,
                };

                for key_column in key_columns {
                    if let Some(value) = data_source.get(key_column) {
                        conditions.push(format!("\"{}\" = ?", key_column));
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
                // For UPDATE with NOTHING, try to use key columns if available
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
                        conditions.push(format!("\"{}\" = ?", key_column));
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

    /// Build WHERE clause specifically for DELETE operations
    fn build_where_clause_for_delete<'a>(
        &self,
        old_data: &'a HashMap<String, serde_json::Value>,
        replica_identity: &ReplicaIdentity,
        key_columns: &[String],
        schema: &str,
        table: &str,
    ) -> Result<WhereClause<'a>> {
        match replica_identity {
            ReplicaIdentity::Full => {
                let mut conditions = Vec::with_capacity(old_data.len());
                let mut bind_values = Vec::with_capacity(old_data.len());

                for (column, value) in old_data {
                    conditions.push(format!("\"{}\" = ?", column));
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
                        conditions.push(format!("\"{}\" = ?", key_column));
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
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        event: &'a ChangeEvent,
    ) -> Result<()> {
        match &event.event_type {
            EventType::Insert { table, data, .. } => {
                let columns: Vec<String> = data.keys().map(|k| format!("\"{}\"", k)).collect();
                let placeholders: Vec<String> = (0..data.len()).map(|_| "?".to_string()).collect();
                let table_ref = format!("\"{}\"", table);

                let sql = format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    table_ref,
                    columns.join(", "),
                    placeholders.join(", ")
                );

                let mut query = sqlx::query(&sql);
                for (_, value) in data {
                    query = self.bind_value(query, value);
                }

                query
                    .execute(&mut **tx)
                    .await
                    .map_err(|e| CdcError::generic(format!("SQLite INSERT failed: {}", e)))?;
                debug!("Successfully inserted record into {}", table);
            }

            EventType::Update {
                table,
                old_data,
                new_data,
                replica_identity,
                key_columns,
                ..
            } => {
                let set_clauses: Vec<String> =
                    new_data.keys().map(|k| format!("\"{}\" = ?", k)).collect();
                let where_clause = self.build_where_clause_for_update(
                    old_data,
                    new_data,
                    replica_identity,
                    key_columns,
                    "main",
                    table,
                )?;

                let table_ref = format!("\"{}\"", table);
                let sql = format!(
                    "UPDATE {} SET {} WHERE {}",
                    table_ref,
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

                query
                    .execute(&mut **tx)
                    .await
                    .map_err(|e| CdcError::generic(format!("SQLite UPDATE failed: {}", e)))?;
                debug!("Successfully updated record in {}", table);
            }

            EventType::Delete {
                table,
                old_data,
                replica_identity,
                key_columns,
                ..
            } => {
                let where_clause = self.build_where_clause_for_delete(
                    old_data,
                    replica_identity,
                    key_columns,
                    "main",
                    table,
                )?;

                let table_ref = format!("\"{}\"", table);
                let sql = format!("DELETE FROM {} WHERE {}", table_ref, where_clause.sql);

                let mut query = sqlx::query(&sql);
                for value in where_clause.bind_values {
                    query = self.bind_value(query, value);
                }

                query
                    .execute(&mut **tx)
                    .await
                    .map_err(|e| CdcError::generic(format!("SQLite DELETE failed: {}", e)))?;
                debug!("Successfully deleted record from {}", table);
            }

            EventType::Truncate(tables) => {
                for table_ref in tables {
                    let parts: Vec<&str> = table_ref.split('.').collect();
                    let table = if parts.len() >= 2 { parts[1] } else { parts[0] };
                    let table_name = format!("\"{}\"", table);
                    let sql = format!("DELETE FROM {}", table_name);

                    sqlx::query(&sql)
                        .execute(&mut **tx)
                        .await
                        .map_err(|e| CdcError::generic(format!("SQLite TRUNCATE failed: {}", e)))?;
                }
                debug!("Successfully truncated {} table(s)", tables.len());
            }

            _ => {
                // Skip non-DML events (BEGIN, COMMIT, RELATION, etc.)
                debug!("Skipping non-DML event: {:?}", event.event_type);
            }
        }

        Ok(())
    }

    /// Execute a batch INSERT statement for multiple rows
    async fn execute_batch_insert<'a>(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        batch: &InsertBatch<'a>,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let columns: Vec<String> = batch.columns.iter().map(|k| format!("\"{}\"", k)).collect();
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

        let table_ref = format!("\"{}\"", batch.table);
        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            table_ref,
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
        query
            .execute(&mut **tx)
            .await
            .map_err(|e| CdcError::generic(format!("SQLite batch INSERT failed: {}", e)))?;
        let batch_duration = batch_start.elapsed();

        info!(
            "SQLite Batch INSERT: {} rows into \"{}\" in {:?}",
            batch.rows.len(),
            batch.table,
            batch_duration
        );

        Ok(())
    }

    /// Process events with batch INSERT optimization
    /// Groups consecutive INSERT events for the same table and executes them as batch inserts
    async fn process_events_with_batching<'a>(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        events: &'a [ChangeEvent],
    ) -> Result<()> {
        let mut current_batch: Option<InsertBatch<'a>> = None;

        for event in events {
            match &event.event_type {
                EventType::Insert { table, data, .. } => {
                    // Get column names in sorted order for consistency
                    let mut columns: Vec<String> = data.keys().cloned().collect();
                    columns.sort();

                    // Check if we can add to current batch
                    let can_add = current_batch
                        .as_ref()
                        .map(|b| b.can_add(table, &columns))
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

                        let mut new_batch = InsertBatch::new(table.clone(), columns);
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
impl DestinationHandler for SQLiteDestination {
    async fn connect(&mut self, connection_string: &str) -> Result<()> {
        // Parse connection string - for SQLite, this is typically just a file path
        // Support formats like:
        // - "file:///path/to/database.db"
        // - "sqlite:///path/to/database.db"
        // - "/path/to/database.db"
        // - "database.db"
        let db_path = if connection_string.starts_with("file://") {
            connection_string
                .strip_prefix("file://")
                .unwrap_or(connection_string)
        } else if connection_string.starts_with("sqlite://") {
            connection_string
                .strip_prefix("sqlite://")
                .unwrap_or(connection_string)
        } else {
            connection_string
        };

        // Create parent directory if it doesn't exist
        if let Some(parent) = Path::new(db_path).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    CdcError::generic(format!(
                        "Failed to create directory for SQLite database: {}",
                        e
                    ))
                })?;
            }
        }

        // Create connection options
        let mut options = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path))
            .map_err(|e| {
                CdcError::generic(format!(
                    "Invalid SQLite connection string '{}': {}",
                    db_path, e
                ))
            })?;

        // Configure options
        options = options
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .foreign_keys(true);

        // Create connection pool
        let pool = SqlitePool::connect_with(options).await.map_err(|e| {
            CdcError::generic(format!(
                "Failed to connect to SQLite database '{}': {}",
                db_path, e
            ))
        })?;

        self.pool = Some(pool);
        self.database_path = Some(db_path.to_string());

        info!("Connected to SQLite database: {}", db_path);
        Ok(())
    }

    // SQLite does not use schema/database namespacing like MySQL, so schema mappings are not needed - tables are referenced by name only
    fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

    async fn process_transaction(&mut self, transaction: &Transaction) -> Result<()> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("SQLite connection not established"))?;

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
            "SQLite: Starting to process transaction {} with {} events (streaming={}, final={})",
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
            .map_err(|e| CdcError::generic(format!("Failed to begin SQLite transaction: {}", e)))?;
        debug!("SQLite: BEGIN took {:?}", begin_time.elapsed());

        // Process events with batching optimization
        let process_time = std::time::Instant::now();
        let result = self
            .process_events_with_batching(&mut tx, &transaction.events)
            .await;
        debug!("SQLite: Event processing took {:?}", process_time.elapsed());

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
        tx.commit().await.map_err(|e| {
            CdcError::generic(format!("Failed to commit SQLite transaction: {}", e))
        })?;
        let commit_duration = commit_time.elapsed();

        debug!(
            "SQLite: COMMIT took {:?} for transaction {} ({} events, total time: {:?})",
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
            .ok_or_else(|| CdcError::generic("SQLite connection not established"))?;

        // Try a simple query to check if the connection is working
        match sqlx::query("SELECT 1").execute(pool).await {
            Ok(_) => {
                debug!("SQLite health check passed");
                Ok(true)
            }
            Err(e) => {
                error!("SQLite health check failed: {}", e);
                Ok(false)
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(pool) = &self.pool {
            pool.close().await;
        }
        self.pool = None;
        self.database_path = None;
        info!("SQLite connection closed successfully");
        Ok(())
    }
}

impl Default for SQLiteDestination {
    fn default() -> Self {
        Self::new()
    }
}
