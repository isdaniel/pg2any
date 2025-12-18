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
use tracing::{debug, info};

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

/// State for an open transaction
/// Only used for streaming transactions (normal transactions are committed immediately)
struct OpenTransaction {
    /// PostgreSQL transaction ID
    transaction_id: u32,
    /// The open SQLite transaction
    transaction: sqlx::Transaction<'static, sqlx::Sqlite>,
    /// Number of events processed so far
    events_processed: usize,
}

impl OpenTransaction {
    fn new(transaction_id: u32, transaction: sqlx::Transaction<'static, sqlx::Sqlite>) -> Self {
        Self {
            transaction_id,
            transaction,
            events_processed: 0,
        }
    }
}

/// SQLite destination implementation with batch INSERT support
///
/// This implementation uses sqlx for database operations with async support.
/// SQLite connections are managed through sqlx's connection pool.
pub struct SQLiteDestination {
    pool: Option<SqlitePool>,
    database_path: Option<String>,
    /// Active transaction state (if any) - used for both streaming and normal transactions
    active_tx: Option<OpenTransaction>,
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
            active_tx: None,
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

    /// Process events on an open transaction with batching optimization
    async fn process_events_on_open_transaction<'a>(
        &self,
        open_tx: &mut OpenTransaction,
        events: &'a [ChangeEvent],
    ) -> Result<()> {
        let mut current_batch: Option<InsertBatch<'a>> = None;

        for event in events {
            match &event.event_type {
                EventType::Insert { table, data, .. } => {
                    let mut columns: Vec<String> = data.keys().cloned().collect();
                    columns.sort();

                    let can_add = current_batch
                        .as_ref()
                        .map(|b| b.can_add(table, &columns))
                        .unwrap_or(false);

                    if can_add {
                        let batch = current_batch.as_mut().unwrap();
                        batch.add_row(data);

                        if batch.len() >= MAX_BATCH_INSERT_SIZE {
                            self.execute_batch_insert(&mut open_tx.transaction, batch)
                                .await?;
                            current_batch = None;
                        }
                    } else {
                        if let Some(batch) = current_batch.take() {
                            self.execute_batch_insert(&mut open_tx.transaction, &batch)
                                .await?;
                        }

                        let mut new_batch = InsertBatch::new(table.clone(), columns);
                        new_batch.add_row(data);
                        current_batch = Some(new_batch);
                    }
                }
                _ => {
                    if let Some(batch) = current_batch.take() {
                        self.execute_batch_insert(&mut open_tx.transaction, &batch)
                            .await?;
                    }
                    self.process_event_in_transaction(&mut open_tx.transaction, event)
                        .await?;
                }
            }
        }

        if let Some(batch) = current_batch.take() {
            self.execute_batch_insert(&mut open_tx.transaction, &batch)
                .await?;
        }

        Ok(())
    }

    /// Begin a new streaming transaction
    async fn begin_transaction(&mut self, transaction_id: u32) -> Result<()> {
        let pool = self
            .pool
            .clone()
            .ok_or_else(|| CdcError::generic("SQLite connection not established"))?;

        let tx = pool
            .begin()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to begin SQLite transaction: {}", e)))?;

        info!(
            "SQLite: Started streaming transaction {} (first batch)",
            transaction_id
        );

        self.active_tx = Some(OpenTransaction::new(transaction_id, tx));
        Ok(())
    }

    /// Commit the active streaming transaction
    async fn commit_active_transaction(&mut self) -> Result<()> {
        if let Some(active) = self.active_tx.take() {
            let commit_start = std::time::Instant::now();
            // Extract values before consuming transaction
            let tx_id = active.transaction_id;
            let events_processed = active.events_processed;

            active.transaction.commit().await.map_err(|e| {
                CdcError::generic(format!(
                    "Failed to commit SQLite streaming transaction {}: {}",
                    tx_id, e
                ))
            })?;

            info!(
                "SQLite: Committed streaming transaction {} ({} events) in {:?}",
                tx_id,
                events_processed,
                commit_start.elapsed()
            );
        }

        Ok(())
    }

    /// Rollback the active streaming transaction
    async fn rollback_active_transaction(&mut self) -> Result<()> {
        if let Some(active) = self.active_tx.take() {
            // Extract values before consuming transaction
            let tx_id = active.transaction_id;
            let events_processed = active.events_processed;

            tracing::warn!(
                "SQLite: Rolling back streaming transaction {} ({} events processed)",
                tx_id,
                events_processed
            );

            if let Err(e) = active.transaction.rollback().await {
                tracing::warn!(
                    "SQLite: Failed to rollback streaming transaction {}: {}",
                    tx_id,
                    e
                );
            }
        }

        Ok(())
    }

    /// Unified method to process a transaction batch
    ///
    /// This method handles both normal and streaming transactions:
    /// - Normal transactions: BEGIN → process → COMMIT in one call
    /// - Streaming transactions: BEGIN (first batch) → process → ... → COMMIT (final batch)
    async fn process_transaction_batch(&mut self, transaction: &Transaction) -> Result<()> {
        let tx_id = transaction.transaction_id;
        let is_streaming = transaction.is_streaming;
        let is_final = transaction.is_final_batch;

        // Skip empty non-final batches
        if transaction.is_empty() && !is_final {
            debug!(
                "Skipping empty non-final batch for transaction {} (streaming={})",
                tx_id, is_streaming
            );
            return Ok(());
        }

        // Skip empty final batches for non-streaming transactions
        if transaction.is_empty() && !is_streaming {
            debug!(
                "Skipping empty transaction {} (streaming={}, final={})",
                tx_id, is_streaming, is_final
            );
            return Ok(());
        }

        let tx_start = std::time::Instant::now();
        let tx_type = if is_streaming { "streaming" } else { "normal" };

        debug!(
            "SQLite: Processing {} transaction {} ({} events, final={})",
            tx_type,
            tx_id,
            transaction.event_count(),
            is_final
        );

        // Check for mismatched streaming transactions
        if let Some(ref active) = self.active_tx {
            if active.transaction_id != tx_id {
                tracing::warn!(
                    "SQLite: Received batch for transaction {} but have active transaction {}. Rolling back active.",
                    tx_id, active.transaction_id
                );
                self.rollback_active_transaction().await?;
            }
        }

        // Begin transaction if not already active
        if self.active_tx.is_none() {
            if is_streaming {
                // Streaming transaction: keep transaction open across batches
                self.begin_transaction(tx_id).await?;
            } else {
                // Normal transaction: begin inline, will be committed immediately
                self.begin_transaction(tx_id).await?;
            }
        }

        // Process events if not empty
        if !transaction.is_empty() {
            let mut active = self.active_tx.take().unwrap();

            let result = self
                .process_events_on_open_transaction(&mut active, &transaction.events)
                .await;

            if let Err(e) = result {
                tracing::warn!(
                    "SQLite: Error processing batch, rolling back transaction {}: {}",
                    tx_id,
                    e
                );
                let _ = active.transaction.rollback().await;
                return Err(e);
            }

            active.events_processed += transaction.event_count();

            debug!(
                "SQLite: Processed {} events for {} transaction {} (total: {}, elapsed: {:?})",
                transaction.event_count(),
                tx_type,
                tx_id,
                active.events_processed,
                tx_start.elapsed()
            );

            self.active_tx = Some(active);
        }

        // Commit if final batch or non-streaming (normal transactions always commit immediately)
        if is_final || !is_streaming {
            self.commit_active_transaction().await?;
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
        // Unified transaction processing handles both normal and streaming transactions
        self.process_transaction_batch(transaction).await
    }

    fn get_active_streaming_transaction_id(&self) -> Option<u32> {
        self.active_tx.as_ref().map(|tx| tx.transaction_id)
    }

    async fn rollback_streaming_transaction(&mut self) -> Result<()> {
        self.rollback_active_transaction().await
    }

    async fn close(&mut self) -> Result<()> {
        if self.active_tx.is_some() {
            tracing::warn!("SQLite: Closing with active transaction - rolling back");
            self.rollback_active_transaction().await?;
        }

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
