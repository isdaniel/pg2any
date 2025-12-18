use super::destination_factory::DestinationHandler;
use crate::{
    error::{CdcError, Result},
    types::{ChangeEvent, EventType, ReplicaIdentity, Transaction},
};
use async_trait::async_trait;
use sqlx::{Executor, MySql, MySqlPool};
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Maximum number of rows per batch INSERT statement
/// Limited by MySQL's max_allowed_packet and number of placeholders
const MAX_BATCH_INSERT_SIZE: usize = 1000;

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

/// Represents an open database transaction that can span multiple batches
/// Only used for streaming transactions (normal transactions use sqlx::Transaction directly)
struct OpenTransaction {
    /// PostgreSQL transaction ID
    transaction_id: u32,
    /// The open MySQL connection with active transaction
    connection: sqlx::pool::PoolConnection<MySql>,
    /// Number of events processed so far
    events_processed: usize,
}

impl OpenTransaction {
    /// Create a new open transaction
    fn new(transaction_id: u32, connection: sqlx::pool::PoolConnection<MySql>) -> Self {
        Self {
            transaction_id,
            connection,
            events_processed: 0,
        }
    }
}

// ============================================================================
// SQL Building Utilities (Free Functions)
// ============================================================================

/// Map a source schema to destination schema using provided mappings
fn map_schema(schema_mappings: &HashMap<String, String>, source_schema: &str) -> String {
    schema_mappings
        .get(source_schema)
        .cloned()
        .unwrap_or_else(|| source_schema.to_string())
}

/// Bind a JSON value to a sqlx query
fn bind_value<'a>(
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

/// Build WHERE clause for UPDATE operations based on replica identity
fn build_where_clause_for_update<'a>(
    old_data: &'a Option<HashMap<String, serde_json::Value>>,
    new_data: &'a HashMap<String, serde_json::Value>,
    replica_identity: &ReplicaIdentity,
    key_columns: &[String],
    schema: &str,
    table: &str,
) -> Result<WhereClause<'a>> {
    match replica_identity {
        ReplicaIdentity::Full => {
            // FULL requires old_data
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

            // Use old_data if available, otherwise use new_data
            let data_source: &HashMap<String, serde_json::Value> = match old_data {
                Some(old) => old,
                None => new_data,
            };

            let mut conditions = Vec::with_capacity(key_columns.len());
            let mut bind_values = Vec::with_capacity(key_columns.len());

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

/// Build WHERE clause for DELETE operations based on replica identity
fn build_where_clause_for_delete<'a>(
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

/// Process a single DML event using any MySQL executor
async fn process_single_event<'c, E>(
    executor: E,
    event: &ChangeEvent,
    schema_mappings: &HashMap<String, String>,
) -> Result<()>
where
    E: sqlx::Executor<'c, Database = MySql>,
{
    match &event.event_type {
        EventType::Insert {
            schema,
            table,
            data,
            ..
        } => {
            let dest_schema = map_schema(schema_mappings, schema);
            let columns: Vec<String> = data.keys().map(|k| format!("`{}`", k)).collect();
            let placeholders: Vec<String> = (0..columns.len()).map(|_| "?".to_string()).collect();

            let sql = format!(
                "INSERT INTO `{}`.`{}` ({}) VALUES ({})",
                dest_schema,
                table,
                columns.join(", "),
                placeholders.join(", ")
            );

            let mut query = sqlx::query(&sql);
            for (_, value) in data {
                query = bind_value(query, value);
            }

            query.execute(executor).await?;
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
            let dest_schema = map_schema(schema_mappings, schema);
            let set_clauses: Vec<String> =
                new_data.keys().map(|k| format!("`{}` = ?", k)).collect();

            let where_clause = build_where_clause_for_update(
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
                query = bind_value(query, value);
            }
            for value in where_clause.bind_values {
                query = bind_value(query, value);
            }

            query.execute(executor).await?;
        }
        EventType::Delete {
            schema,
            table,
            old_data,
            replica_identity,
            key_columns,
            ..
        } => {
            let dest_schema = map_schema(schema_mappings, schema);
            let where_clause = build_where_clause_for_delete(
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
                query = bind_value(query, value);
            }

            query.execute(executor).await?;
        }
        EventType::Truncate(truncate_tables) => {
            for table_full_name in truncate_tables {
                let mut parts = table_full_name.splitn(2, '.');
                let sql = match (parts.next(), parts.next()) {
                    (Some(schema), Some(table)) => {
                        let dest_schema = map_schema(schema_mappings, schema);
                        format!("TRUNCATE TABLE `{}`.`{}`", dest_schema, table)
                    }
                    _ => format!("TRUNCATE TABLE `{}`", table_full_name),
                };
                sqlx::query(&sql).execute(executor).await?;
                // For TRUNCATE with multiple tables, each needs its own executor
                break;
            }
        }
        _ => {
            // Skip non-DML events (BEGIN, COMMIT, RELATION, etc.)
            debug!("Skipping non-DML event: {:?}", event.event_type);
        }
    }

    Ok(())
}

/// Execute a batch INSERT using multi-value syntax
async fn execute_batch_insert<'c, E>(
    executor: E,
    batch: &InsertBatch<'_>,
    schema_mappings: &HashMap<String, String>,
) -> Result<()>
where
    E: sqlx::Executor<'c, Database = MySql>,
{
    if batch.is_empty() {
        return Ok(());
    }

    let dest_schema = map_schema(schema_mappings, &batch.schema);
    let columns: Vec<String> = batch.columns.iter().map(|k| format!("`{}`", k)).collect();
    let num_columns = columns.len();

    let row_placeholder = format!(
        "({})",
        (0..num_columns).map(|_| "?").collect::<Vec<_>>().join(", ")
    );

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

    for row in &batch.rows {
        for value in row {
            query = bind_value(query, value);
        }
    }

    let batch_start = std::time::Instant::now();
    query.execute(executor).await?;
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

/// Process TRUNCATE events on a connection
async fn process_truncate_on_connection(
    conn: &mut sqlx::pool::PoolConnection<MySql>,
    tables: &[String],
    schema_mappings: &HashMap<String, String>,
) -> Result<()> {
    for table_full_name in tables {
        let mut parts = table_full_name.splitn(2, '.');
        let sql = match (parts.next(), parts.next()) {
            (Some(schema), Some(table)) => {
                let dest_schema = map_schema(schema_mappings, schema);
                format!("TRUNCATE TABLE `{}`.`{}`", dest_schema, table)
            }
            _ => format!("TRUNCATE TABLE `{}`", table_full_name),
        };
        sqlx::query(&sql).execute(&mut **conn).await?;
    }
    Ok(())
}

/// Process TRUNCATE events in a transaction
async fn process_truncate_in_tx(
    tx: &mut sqlx::Transaction<'_, MySql>,
    tables: &[String],
    schema_mappings: &HashMap<String, String>,
) -> Result<()> {
    for table_full_name in tables {
        let mut parts = table_full_name.splitn(2, '.');
        let sql = match (parts.next(), parts.next()) {
            (Some(schema), Some(table)) => {
                let dest_schema = map_schema(schema_mappings, schema);
                format!("TRUNCATE TABLE `{}`.`{}`", dest_schema, table)
            }
            _ => format!("TRUNCATE TABLE `{}`", table_full_name),
        };
        sqlx::query(&sql).execute(&mut **tx).await?;
    }
    Ok(())
}

// ============================================================================
// MySQL Destination Implementation
// ============================================================================

pub struct MySQLDestination {
    pool: Option<MySqlPool>,
    /// Schema mappings: maps source schema to destination database
    schema_mappings: HashMap<String, String>,
    /// Active open transaction state
    active_tx: Option<OpenTransaction>,
}

impl MySQLDestination {
    /// Create a new MySQL destination instance
    pub fn new() -> Self {
        Self {
            pool: None,
            schema_mappings: HashMap::new(),
            active_tx: None,
        }
    }

    async fn process_events_with_batching(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        events: &[ChangeEvent],
    ) -> Result<()> {
        let mut current_batch: Option<InsertBatch<'_>> = None;

        for event in events {
            match &event.event_type {
                EventType::Insert {
                    schema,
                    table,
                    data,
                    ..
                } => {
                    let dest_schema = map_schema(&self.schema_mappings, schema);
                    let mut columns: Vec<String> = data.keys().cloned().collect();
                    columns.sort();

                    let can_add = current_batch
                        .as_ref()
                        .map(|b| b.can_add(&dest_schema, table, &columns))
                        .unwrap_or(false);

                    if can_add {
                        let batch = current_batch.as_mut().unwrap();
                        batch.add_row(data);

                        if batch.len() >= MAX_BATCH_INSERT_SIZE {
                            execute_batch_insert(&mut **tx, batch, &self.schema_mappings).await?;
                            current_batch = None;
                        }
                    } else {
                        if let Some(batch) = current_batch.take() {
                            execute_batch_insert(&mut **tx, &batch, &self.schema_mappings).await?;
                        }

                        let mut new_batch = InsertBatch::new(dest_schema, table.clone(), columns);
                        new_batch.add_row(data);
                        current_batch = Some(new_batch);
                    }
                }
                EventType::Truncate(tables) => {
                    // Flush any pending batch before TRUNCATE
                    if let Some(batch) = current_batch.take() {
                        execute_batch_insert(&mut **tx, &batch, &self.schema_mappings).await?;
                    }
                    process_truncate_in_tx(tx, tables, &self.schema_mappings).await?;
                }
                _ => {
                    // Flush any pending batch before processing non-INSERT events
                    if let Some(batch) = current_batch.take() {
                        execute_batch_insert(&mut **tx, &batch, &self.schema_mappings).await?;
                    }
                    process_single_event(&mut **tx, event, &self.schema_mappings).await?;
                }
            }
        }

        if let Some(batch) = current_batch.take() {
            execute_batch_insert(&mut **tx, &batch, &self.schema_mappings).await?;
        }

        Ok(())
    }

    /// Process events with batching on an open transaction (unified for streaming and normal)
    async fn process_events_on_open_transaction(
        open_tx: &mut OpenTransaction,
        events: &[ChangeEvent],
        schema_mappings: &HashMap<String, String>,
    ) -> Result<()> {
        let mut current_batch: Option<InsertBatch<'_>> = None;

        for event in events {
            match &event.event_type {
                EventType::Insert {
                    schema,
                    table,
                    data,
                    ..
                } => {
                    let dest_schema = map_schema(schema_mappings, schema);
                    let mut columns: Vec<String> = data.keys().cloned().collect();
                    columns.sort();

                    let can_add = current_batch
                        .as_ref()
                        .map(|b| b.can_add(&dest_schema, table, &columns))
                        .unwrap_or(false);

                    if can_add {
                        let batch = current_batch.as_mut().unwrap();
                        batch.add_row(data);

                        if batch.len() >= MAX_BATCH_INSERT_SIZE {
                            execute_batch_insert(&mut *open_tx.connection, batch, schema_mappings)
                                .await?;
                            current_batch = None;
                        }
                    } else {
                        if let Some(batch) = current_batch.take() {
                            execute_batch_insert(&mut *open_tx.connection, &batch, schema_mappings)
                                .await?;
                        }

                        let mut new_batch = InsertBatch::new(dest_schema, table.clone(), columns);
                        new_batch.add_row(data);
                        current_batch = Some(new_batch);
                    }
                }
                EventType::Truncate(tables) => {
                    if let Some(batch) = current_batch.take() {
                        execute_batch_insert(&mut *open_tx.connection, &batch, schema_mappings)
                            .await?;
                    }
                    process_truncate_on_connection(
                        &mut open_tx.connection,
                        tables,
                        schema_mappings,
                    )
                    .await?;
                }
                _ => {
                    if let Some(batch) = current_batch.take() {
                        execute_batch_insert(&mut *open_tx.connection, &batch, schema_mappings)
                            .await?;
                    }
                    process_single_event(&mut *open_tx.connection, event, schema_mappings).await?;
                }
            }
        }

        if let Some(batch) = current_batch.take() {
            execute_batch_insert(&mut *open_tx.connection, &batch, schema_mappings).await?;
        }

        Ok(())
    }

    /// Begin a new streaming transaction on the connection pool
    async fn begin_transaction(&mut self, transaction_id: u32) -> Result<()> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("MySQL connection not established"))?;

        let mut conn = pool
            .acquire()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to acquire MySQL connection: {}", e)))?;

        conn.execute("START TRANSACTION")
            .await
            .map_err(|e| CdcError::generic(format!("Failed to start MySQL transaction: {}", e)))?;

        info!(
            "MySQL: Started streaming transaction {} (first batch)",
            transaction_id
        );

        self.active_tx = Some(OpenTransaction::new(transaction_id, conn));
        Ok(())
    }

    /// Commit the active streaming transaction
    async fn commit_active_transaction(&mut self) -> Result<()> {
        if let Some(mut active) = self.active_tx.take() {
            let commit_start = std::time::Instant::now();

            active.connection.execute("COMMIT").await.map_err(|e| {
                CdcError::generic(format!(
                    "Failed to commit MySQL streaming transaction {}: {}",
                    active.transaction_id, e
                ))
            })?;

            info!(
                "MySQL: Committed streaming transaction {} ({} events) in {:?}",
                active.transaction_id,
                active.events_processed,
                commit_start.elapsed()
            );
        }

        Ok(())
    }

    /// Rollback the active streaming transaction
    async fn rollback_active_transaction(&mut self) -> Result<()> {
        if let Some(mut active) = self.active_tx.take() {
            warn!(
                "MySQL: Rolling back streaming transaction {} ({} events processed)",
                active.transaction_id, active.events_processed
            );

            if let Err(e) = active.connection.execute("ROLLBACK").await {
                warn!(
                    "MySQL: Failed to rollback streaming transaction {}: {}",
                    active.transaction_id, e
                );
            }
        }

        Ok(())
    }

    /// This method handles both normal and streaming transactions:
    /// - Normal transactions: BEGIN → process → COMMIT in one call
    /// - Streaming transactions: BEGIN (first batch) → process → ... → COMMIT (final batch)
    async fn process_transaction_batch(&mut self, transaction: &Transaction) -> Result<()> {
        // Clone pool early to avoid borrow issues
        let pool = self
            .pool
            .clone()
            .ok_or_else(|| CdcError::generic("MySQL connection not established"))?;

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
            "MySQL: Processing {} transaction {} ({} events, final={})",
            tx_type,
            tx_id,
            transaction.event_count(),
            is_final
        );

        // Check for mismatched streaming transactions
        if let Some(ref active) = self.active_tx {
            if active.transaction_id != tx_id {
                warn!(
                    "MySQL: Received batch for transaction {} but have active transaction {}. Rolling back active.",
                    tx_id, active.transaction_id
                );
                self.rollback_active_transaction().await?;
            }
        }

        // Begin transaction if not already active
        if self.active_tx.is_none() {
            if is_streaming {
                // Streaming transaction: use raw connection with explicit BEGIN
                self.begin_transaction(tx_id).await?;
            } else {
                // Normal transaction: use sqlx::Transaction for simple atomic operation
                let begin_time = std::time::Instant::now();
                let mut tx = pool.begin().await.map_err(|e| {
                    CdcError::generic(format!("Failed to begin MySQL transaction: {}", e))
                })?;
                debug!("MySQL: BEGIN took {:?}", begin_time.elapsed());

                let process_time = std::time::Instant::now();
                let result = self
                    .process_events_with_batching(&mut tx, &transaction.events)
                    .await;
                debug!("MySQL: Event processing took {:?}", process_time.elapsed());

                if let Err(e) = result {
                    tx.rollback().await.map_err(|re| {
                        CdcError::generic(format!(
                            "Failed to rollback transaction after error '{}': {}",
                            e, re
                        ))
                    })?;
                    return Err(e);
                }

                let commit_time = std::time::Instant::now();
                tx.commit().await.map_err(|e| {
                    CdcError::generic(format!("Failed to commit MySQL transaction: {}", e))
                })?;

                debug!(
                    "MySQL: COMMIT took {:?} for transaction {} ({} events, total time: {:?})",
                    commit_time.elapsed(),
                    tx_id,
                    transaction.event_count(),
                    tx_start.elapsed()
                );

                return Ok(());
            }
        }

        // Process events on the open transaction (streaming mode)
        if !transaction.is_empty() {
            let mut active = self.active_tx.take().unwrap();

            let result = Self::process_events_on_open_transaction(
                &mut active,
                &transaction.events,
                &self.schema_mappings,
            )
            .await;

            if let Err(e) = result {
                warn!(
                    "MySQL: Error processing batch, rolling back transaction {}: {}",
                    tx_id, e
                );
                let _ = active.connection.execute("ROLLBACK").await;
                return Err(e);
            }

            active.events_processed += transaction.event_count();

            debug!(
                "MySQL: Processed {} events for transaction {} (total: {})",
                transaction.event_count(),
                tx_id,
                active.events_processed
            );

            self.active_tx = Some(active);
        }

        // Commit if this is the final batch
        if is_final {
            self.commit_active_transaction().await?;
        }

        Ok(())
    }
}

impl Default for MySQLDestination {
    fn default() -> Self {
        Self::new()
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
            warn!("MySQL: Closing with active transaction - rolling back");
            self.rollback_active_transaction().await?;
        }

        if let Some(pool) = &self.pool {
            pool.close().await;
        }
        self.pool = None;
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_destination_creation() {
        let destination = MySQLDestination::new();
        assert!(destination.pool.is_none());
    }

    #[test]
    fn test_insert_batch_operations() {
        let mut batch = InsertBatch::new(
            "test_schema".to_string(),
            "test_table".to_string(),
            vec!["col1".to_string(), "col2".to_string()],
        );

        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);

        let mut data = HashMap::new();
        data.insert("col1".to_string(), serde_json::json!("value1"));
        data.insert("col2".to_string(), serde_json::json!(42));

        batch.add_row(&data);
        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);

        assert!(batch.can_add(
            "test_schema",
            "test_table",
            &["col1".to_string(), "col2".to_string()]
        ));
        assert!(!batch.can_add(
            "other_schema",
            "test_table",
            &["col1".to_string(), "col2".to_string()]
        ));
        assert!(!batch.can_add(
            "test_schema",
            "other_table",
            &["col1".to_string(), "col2".to_string()]
        ));
        assert!(!batch.can_add("test_schema", "test_table", &["col1".to_string()]));
    }

    #[test]
    fn test_map_schema() {
        let mut mappings = HashMap::new();
        mappings.insert("public".to_string(), "cdc_db".to_string());

        assert_eq!(map_schema(&mappings, "public"), "cdc_db");
        assert_eq!(map_schema(&mappings, "other"), "other");
    }

    #[test]
    fn test_where_clause_for_update_full_identity() {
        let mut old_data = HashMap::new();
        old_data.insert("id".to_string(), serde_json::json!(1));
        old_data.insert("name".to_string(), serde_json::json!("test"));

        let new_data = HashMap::new();
        let old_data_opt = Some(old_data);

        let result = build_where_clause_for_update(
            &old_data_opt,
            &new_data,
            &ReplicaIdentity::Full,
            &[],
            "public",
            "users",
        );

        assert!(result.is_ok());
        let clause = result.unwrap();
        assert!(clause.sql.contains("`id` = ?"));
        assert!(clause.sql.contains("`name` = ?"));
        assert_eq!(clause.bind_values.len(), 2);
    }

    #[test]
    fn test_where_clause_for_update_full_identity_no_old_data() {
        let new_data = HashMap::new();
        let result = build_where_clause_for_update(
            &None,
            &new_data,
            &ReplicaIdentity::Full,
            &[],
            "public",
            "users",
        );

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("REPLICA IDENTITY FULL requires old_data"));
    }

    #[test]
    fn test_where_clause_for_update_default_identity() {
        let mut old_data = HashMap::new();
        old_data.insert("id".to_string(), serde_json::json!(1));
        old_data.insert("name".to_string(), serde_json::json!("test"));

        let new_data = HashMap::new();
        let old_data_opt = Some(old_data);

        let result = build_where_clause_for_update(
            &old_data_opt,
            &new_data,
            &ReplicaIdentity::Default,
            &["id".to_string()],
            "public",
            "users",
        );

        assert!(result.is_ok());
        let clause = result.unwrap();
        assert_eq!(clause.sql, "`id` = ?");
        assert_eq!(clause.bind_values.len(), 1);
    }

    #[test]
    fn test_where_clause_for_delete_full_identity() {
        let mut old_data = HashMap::new();
        old_data.insert("id".to_string(), serde_json::json!(1));
        old_data.insert("name".to_string(), serde_json::json!("test"));

        let result = build_where_clause_for_delete(
            &old_data,
            &ReplicaIdentity::Full,
            &[],
            "public",
            "users",
        );

        assert!(result.is_ok());
        let clause = result.unwrap();
        assert!(clause.sql.contains("`id` = ?"));
        assert!(clause.sql.contains("`name` = ?"));
    }

    #[test]
    fn test_where_clause_for_delete_nothing_identity() {
        let old_data = HashMap::new();

        let result = build_where_clause_for_delete(
            &old_data,
            &ReplicaIdentity::Nothing,
            &[],
            "public",
            "users",
        );

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("REPLICA IDENTITY NOTHING"));
    }
}
