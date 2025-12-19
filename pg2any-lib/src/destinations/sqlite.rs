use super::{common, destination_factory::DestinationHandler};
use crate::{
    error::{CdcError, Result},
    types::{ChangeEvent, EventType, Transaction},
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

/// Type alias for SQLite's open transaction
/// Uses generic OpenTransaction with SQLite-specific transaction type
type OpenTransaction = common::OpenTransaction<sqlx::Transaction<'static, sqlx::Sqlite>>;

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
        replica_identity: &crate::types::ReplicaIdentity,
        key_columns: &'a [String],
        schema: &str,
        table: &str,
    ) -> Result<common::WhereClause<'a>> {
        let conditions = common::build_where_conditions_for_update(
            old_data,
            new_data,
            replica_identity,
            key_columns,
            schema,
            table,
        )?;

        let mut sql_conditions = Vec::with_capacity(conditions.len());
        let mut bind_values = Vec::with_capacity(conditions.len());

        for (column, value) in conditions {
            sql_conditions.push(format!("\"{}\" = ?", column));
            bind_values.push(value);
        }

        Ok(common::WhereClause {
            sql: sql_conditions.join(" AND "),
            bind_values,
        })
    }

    /// Build WHERE clause specifically for DELETE operations using common utilities
    fn build_where_clause_for_delete<'a>(
        &self,
        old_data: &'a HashMap<String, serde_json::Value>,
        replica_identity: &crate::types::ReplicaIdentity,
        key_columns: &'a [String],
        schema: &str,
        table: &str,
    ) -> Result<common::WhereClause<'a>> {
        let conditions = common::build_where_conditions_for_delete(
            old_data,
            replica_identity,
            key_columns,
            schema,
            table,
        )?;

        let mut sql_conditions = Vec::with_capacity(conditions.len());
        let mut bind_values = Vec::with_capacity(conditions.len());

        for (column, value) in conditions {
            sql_conditions.push(format!("\"{}\" = ?", column));
            bind_values.push(value);
        }

        Ok(common::WhereClause {
            sql: sql_conditions.join(" AND "),
            bind_values,
        })
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
        batch: &common::InsertBatch<'a>,
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

    /// Rollback the active streaming transaction (for graceful shutdown)
    async fn rollback_active_transaction(&mut self) -> Result<()> {
        if let Some(active) = self.active_tx.take() {
            // Extract values before consuming transaction
            let tx_id = active.state.transaction_id;
            let events_processed = active.state.events_processed;

            tracing::warn!(
                "SQLite: Rolling back streaming transaction {} ({} events processed)",
                tx_id,
                events_processed
            );

            if let Err(e) = active.handle.rollback().await {
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
            "SQLite: Processing transaction {} ({} events)",
            tx_id,
            transaction.event_count()
        );

        let pool = self
            .pool
            .clone()
            .ok_or_else(|| CdcError::generic("SQLite connection not established"))?;

        // Start a new transaction for this batch
        let mut db_tx = pool
            .begin()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to begin SQLite transaction: {}", e)))?;

        // Process all events with batching
        let mut current_batch: Option<common::InsertBatch<'_>> = None;

        for event in &transaction.events {
            match &event.event_type {
                EventType::Insert { table, data, .. } => {
                    let mut columns: Vec<String> = data.keys().cloned().collect();
                    columns.sort();

                    let can_add = current_batch
                        .as_ref()
                        .map(|b| b.can_add(None, table, &columns))
                        .unwrap_or(false);

                    if can_add {
                        let batch = current_batch.as_mut().unwrap();
                        batch.add_row(data);

                        if batch.len() >= MAX_BATCH_INSERT_SIZE {
                            self.execute_batch_insert(&mut db_tx, batch).await?;
                            current_batch = None;
                        }
                    } else {
                        if let Some(batch) = current_batch.take() {
                            self.execute_batch_insert(&mut db_tx, &batch).await?;
                        }

                        let mut new_batch = common::InsertBatch::new(table.clone(), columns);
                        new_batch.add_row(data);
                        current_batch = Some(new_batch);
                    }
                }
                _ => {
                    if let Some(batch) = current_batch.take() {
                        self.execute_batch_insert(&mut db_tx, &batch).await?;
                    }
                    self.process_event_in_transaction(&mut db_tx, event).await?;
                }
            }
        }

        // Flush any remaining batch
        if let Some(batch) = current_batch.take() {
            self.execute_batch_insert(&mut db_tx, &batch).await?;
        }

        // Commit the transaction
        db_tx.commit().await.map_err(|e| {
            CdcError::generic(format!("Failed to commit SQLite transaction: {}", e))
        })?;

        info!(
            "SQLite: Committed batch for transaction {} ({} events) in {:?}",
            tx_id,
            transaction.event_count(),
            tx_start.elapsed()
        );

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
        self.active_tx.as_ref().map(|tx| tx.state.transaction_id)
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
