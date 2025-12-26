use super::{common, destination_factory::DestinationHandler};
use crate::{
    error::{CdcError, Result},
    types::{EventType, Transaction},
};
use async_trait::async_trait;
use sqlx::{MySql, MySqlPool};
use std::collections::HashMap;
use tracing::{debug, info};

// ============================================================================
// SQL Building Utilities (Free Functions)
// ============================================================================

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
        sql_conditions.push(format!("`{}` = ?", column));
        bind_values.push(value);
    }

    Ok(common::WhereClause {
        sql: sql_conditions.join(" AND "),
        bind_values,
    })
}

/// Build WHERE clause for DELETE operations using common utilities
fn build_where_clause_for_delete<'a>(
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
        sql_conditions.push(format!("`{}` = ?", column));
        bind_values.push(value);
    }

    Ok(common::WhereClause {
        sql: sql_conditions.join(" AND "),
        bind_values,
    })
}

/// Execute a batch INSERT using multi-value syntax
async fn execute_batch_insert<'c, E>(
    executor: E,
    batch: &common::InsertBatch<'_>,
    schema_mappings: &HashMap<String, String>,
) -> Result<()>
where
    E: sqlx::Executor<'c, Database = MySql>,
{
    if batch.is_empty() {
        return Ok(());
    }

    let dest_schema = common::map_schema(schema_mappings, batch.schema.as_ref().unwrap());
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

/// Execute a batch UPDATE using multiple UPDATE statements
///
/// Executes multiple simple UPDATE statements:
/// UPDATE table SET col1=?, col2=? WHERE key1=? AND key2=?;
///
/// This approach is simpler and more maintainable than CASE-based batch updates.
/// While it executes multiple statements, they're all within the same transaction.
async fn execute_batch_update<'c>(
    db_tx: &mut sqlx::Transaction<'c, MySql>,
    batch: &common::UpdateBatch<'_>,
    schema_mappings: &HashMap<String, String>,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let dest_schema = common::map_schema(schema_mappings, batch.schema.as_ref().unwrap());

    // Build SET clause (same for all rows)
    let set_clause: Vec<String> = batch
        .update_columns
        .iter()
        .map(|col| format!("`{}` = ?", col))
        .collect();

    // Build WHERE clause (same structure for all rows)
    let where_clause: Vec<String> = batch
        .key_columns
        .iter()
        .map(|col| format!("`{}` = ?", col))
        .collect();

    let base_sql = format!(
        "UPDATE `{}`.`{}` SET {} WHERE {}",
        dest_schema,
        batch.table,
        set_clause.join(", "),
        where_clause.join(" AND ")
    );

    let batch_start = std::time::Instant::now();

    // Execute each UPDATE statement - they all run in the same transaction
    for (key_values, update_values) in &batch.rows {
        let mut query = sqlx::query(&base_sql);

        // Bind SET values
        for value in update_values {
            query = bind_value(query, value);
        }

        // Bind WHERE key values
        for key_value in key_values {
            query = bind_value(query, key_value);
        }

        // Reborrow the connection from the transaction for each query
        query.execute(&mut **db_tx).await?;
    }

    let batch_duration = batch_start.elapsed();

    info!(
        "MySQL Batch UPDATE: {} statements on `{}`.`{}` in {:?}",
        batch.rows.len(),
        dest_schema,
        batch.table,
        batch_duration
    );

    Ok(())
}

/// Execute a batch DELETE using WHERE IN clause
///
/// Generates SQL like:
/// DELETE FROM table WHERE (key1, key2) IN ((?, ?), (?, ?), ...)
/// Or for single key: DELETE FROM table WHERE key IN (?, ?, ?)
async fn execute_batch_delete<'c, E>(
    executor: E,
    batch: &common::DeleteBatch<'_>,
    schema_mappings: &HashMap<String, String>,
) -> Result<()>
where
    E: sqlx::Executor<'c, Database = MySql>,
{
    if batch.is_empty() {
        return Ok(());
    }

    let dest_schema = common::map_schema(schema_mappings, batch.schema.as_ref().unwrap());

    // Build WHERE IN clause for key columns
    let where_clause = if batch.key_columns.len() == 1 {
        // Single key column: WHERE key IN (?, ?, ?)
        let placeholders = (0..batch.rows.len())
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");
        format!("`{}` IN ({})", batch.key_columns[0], placeholders)
    } else {
        // Multiple key columns: WHERE (key1, key2) IN ((?, ?), (?, ?))
        let key_cols = batch
            .key_columns
            .iter()
            .map(|k| format!("`{}`", k))
            .collect::<Vec<_>>()
            .join(", ");
        let row_placeholder = format!(
            "({})",
            (0..batch.key_columns.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ")
        );
        let all_placeholders = (0..batch.rows.len())
            .map(|_| row_placeholder.clone())
            .collect::<Vec<_>>()
            .join(", ");
        format!("({}) IN ({})", key_cols, all_placeholders)
    };

    let sql = format!(
        "DELETE FROM `{}`.`{}` WHERE {}",
        dest_schema, batch.table, where_clause
    );

    let mut query = sqlx::query(&sql);

    // Bind key values
    for key_values in &batch.rows {
        for key_value in key_values {
            query = bind_value(query, key_value);
        }
    }

    let batch_start = std::time::Instant::now();
    query.execute(executor).await?;
    let batch_duration = batch_start.elapsed();

    info!(
        "MySQL Batch DELETE: {} rows from `{}`.`{}` in {:?}",
        batch.rows.len(),
        dest_schema,
        batch.table,
        batch_duration
    );

    Ok(())
}

// ============================================================================
// MySQL Destination Implementation
// ============================================================================

pub struct MySQLDestination {
    pool: Option<MySqlPool>,
    /// Schema mappings: maps source schema to destination database
    schema_mappings: HashMap<String, String>,
    /// MySQL max_allowed_packet value in bytes (default 64MB, max 1GB), Used to calculate safe batch sizes dynamically
    max_allowed_packet: u64,
}

impl MySQLDestination {
    /// Create a new MySQL destination instance
    pub fn new() -> Self {
        Self {
            pool: None,
            schema_mappings: HashMap::new(),
            max_allowed_packet: 67108864, // Default 64MB
        }
    }

    /// Calculate maximum safe batch size for INSERT operations using common utilities
    fn calculate_insert_batch_size(&self, batch: &common::InsertBatch<'_>) -> usize {
        let num_columns = batch.columns.len();
        if num_columns == 0 {
            return 1000; // Fallback default
        }

        // Estimate bytes per row based on actual data
        let estimated_bytes_per_row = common::estimate_insert_batch_row_size(batch, 10);

        // SQL overhead: column names, placeholders, parentheses
        let column_overhead: usize = batch
            .columns
            .iter()
            .map(|col| col.len() + 3) // `col` + ", "
            .sum();
        let row_overhead = num_columns * 3; // Placeholders "?, " per value
        let statement_overhead = row_overhead + column_overhead / num_columns.max(1);
        let fixed_overhead = 100; // INSERT INTO, VALUES, etc.

        common::calculate_batch_size(
            estimated_bytes_per_row,
            statement_overhead,
            fixed_overhead,
            self.max_allowed_packet,
            0.75, // 75% safety margin
            10,
            50000,
        )
    }

    /// Calculate maximum safe batch size for UPDATE operations using common utilities
    fn calculate_update_batch_size(&self, batch: &common::UpdateBatch<'_>) -> usize {
        if batch.update_columns.is_empty() || batch.key_columns.is_empty() {
            return 100; // Conservative fallback
        }

        // Estimate bytes per row based on actual data
        let estimated_bytes_per_row = common::estimate_update_batch_row_size(batch, 5);

        // Per-statement overhead for simple UPDATE
        let statement_overhead = batch.update_columns.len() * 20 + batch.key_columns.len() * 20;
        let fixed_overhead = 200; // UPDATE, SET, WHERE keywords per statement

        common::calculate_batch_size(
            estimated_bytes_per_row,
            statement_overhead,
            fixed_overhead,
            self.max_allowed_packet,
            0.75, // 75% safety margin
            10,
            1000,
        )
    }

    /// Calculate maximum safe batch size for DELETE operations using common utilities
    fn calculate_delete_batch_size(&self, batch: &common::DeleteBatch<'_>) -> usize {
        if batch.key_columns.is_empty() {
            return 100; // Conservative fallback
        }

        // Estimate bytes per row based on actual data
        let estimated_bytes_per_row = common::estimate_delete_batch_row_size(batch, 10);

        // WHERE IN clause overhead
        let statement_overhead = batch.key_columns.len() * 20;
        let fixed_overhead = 150; // DELETE FROM, WHERE keywords

        common::calculate_batch_size(
            estimated_bytes_per_row,
            statement_overhead,
            fixed_overhead,
            self.max_allowed_packet,
            0.75, // 75% safety margin
            10,
            5000,
        )
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
            "MySQL: Processing transaction {} ({} events)",
            tx_id,
            transaction.event_count()
        );

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("MySQL connection not established"))?;

        // Start a new transaction for this batch
        let mut db_tx = pool
            .begin()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to begin MySQL transaction: {}", e)))?;

        // Process all events in this batch - maintain separate batches for INSERT, UPDATE, DELETE
        let mut insert_batch: Option<common::InsertBatch<'_>> = None;
        let mut update_batch: Option<common::UpdateBatch<'_>> = None;
        let mut delete_batch: Option<common::DeleteBatch<'_>> = None;

        for event in &transaction.events {
            match &event.event_type {
                EventType::Insert {
                    schema,
                    table,
                    data,
                    ..
                } => {
                    // Flush other batches before processing INSERT
                    if let Some(batch) = update_batch.take() {
                        execute_batch_update(&mut db_tx, &batch, &self.schema_mappings).await?;
                    }
                    if let Some(batch) = delete_batch.take() {
                        execute_batch_delete(&mut *db_tx, &batch, &self.schema_mappings).await?;
                    }

                    let dest_schema = common::map_schema(&self.schema_mappings, schema);
                    let mut columns: Vec<String> = data.keys().cloned().collect();
                    columns.sort();

                    let can_add = insert_batch
                        .as_ref()
                        .map(|b| b.can_add(Some(&dest_schema), table, &columns))
                        .unwrap_or(false);

                    if can_add {
                        let batch = insert_batch.as_mut().unwrap();
                        batch.add_row(data);

                        // Calculate dynamic batch size based on current batch characteristics
                        let max_batch_size = self.calculate_insert_batch_size(batch);

                        if batch.len() >= max_batch_size {
                            execute_batch_insert(&mut *db_tx, batch, &self.schema_mappings).await?;
                            insert_batch = None;
                        }
                    } else {
                        if let Some(batch) = insert_batch.take() {
                            execute_batch_insert(&mut *db_tx, &batch, &self.schema_mappings)
                                .await?;
                        }

                        let mut new_batch = common::InsertBatch::new_with_schema(
                            dest_schema,
                            table.clone(),
                            columns,
                        );
                        new_batch.add_row(data);
                        insert_batch = Some(new_batch);
                    }
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
                    // Flush other batches before processing UPDATE
                    if let Some(batch) = insert_batch.take() {
                        execute_batch_insert(&mut *db_tx, &batch, &self.schema_mappings).await?;
                    }
                    if let Some(batch) = delete_batch.take() {
                        execute_batch_delete(&mut *db_tx, &batch, &self.schema_mappings).await?;
                    }

                    let dest_schema = common::map_schema(&self.schema_mappings, schema);

                    // Determine key columns for WHERE clause
                    let where_conditions = build_where_clause_for_update(
                        old_data,
                        new_data,
                        replica_identity,
                        key_columns,
                        schema,
                        table,
                    )?;

                    let key_cols: Vec<String> = where_conditions
                        .bind_values
                        .iter()
                        .enumerate()
                        .filter_map(|(idx, _)| {
                            where_conditions
                                .sql
                                .split(" AND ")
                                .nth(idx)
                                .and_then(|cond| cond.split(" = ").next())
                                .map(|s| s.trim_matches('`').to_string())
                        })
                        .collect();

                    // Collect key values
                    let key_values: Vec<&serde_json::Value> = where_conditions.bind_values;

                    // Get update columns (all columns in new_data)
                    let mut update_columns: Vec<String> = new_data.keys().cloned().collect();
                    update_columns.sort();

                    let can_add = update_batch
                        .as_ref()
                        .map(|b| {
                            b.can_add(
                                Some(&dest_schema),
                                table,
                                &update_columns,
                                &key_cols,
                                replica_identity,
                            )
                        })
                        .unwrap_or(false);

                    if can_add {
                        let batch = update_batch.as_mut().unwrap();
                        batch.add_row(new_data, key_values);

                        let max_batch_size = self.calculate_update_batch_size(batch);

                        if batch.len() >= max_batch_size {
                            execute_batch_update(&mut db_tx, batch, &self.schema_mappings).await?;
                            update_batch = None;
                        }
                    } else {
                        if let Some(batch) = update_batch.take() {
                            execute_batch_update(&mut db_tx, &batch, &self.schema_mappings).await?;
                        }

                        let mut new_batch = common::UpdateBatch::new_with_schema(
                            dest_schema,
                            table.clone(),
                            update_columns,
                            key_cols,
                            replica_identity.clone(),
                        );
                        new_batch.add_row(new_data, key_values);
                        update_batch = Some(new_batch);
                    }
                }
                EventType::Delete {
                    schema,
                    table,
                    old_data,
                    replica_identity,
                    key_columns,
                    ..
                } => {
                    // Flush other batches before processing DELETE
                    if let Some(batch) = insert_batch.take() {
                        execute_batch_insert(&mut *db_tx, &batch, &self.schema_mappings).await?;
                    }
                    if let Some(batch) = update_batch.take() {
                        execute_batch_update(&mut db_tx, &batch, &self.schema_mappings).await?;
                    }

                    let dest_schema = common::map_schema(&self.schema_mappings, schema);

                    // Determine key columns for WHERE clause
                    let where_conditions = build_where_clause_for_delete(
                        old_data,
                        replica_identity,
                        key_columns,
                        schema,
                        table,
                    )?;

                    let key_cols: Vec<String> = where_conditions
                        .bind_values
                        .iter()
                        .enumerate()
                        .filter_map(|(idx, _)| {
                            where_conditions
                                .sql
                                .split(" AND ")
                                .nth(idx)
                                .and_then(|cond| cond.split(" = ").next())
                                .map(|s| s.trim_matches('`').to_string())
                        })
                        .collect();

                    let key_values: Vec<&serde_json::Value> = where_conditions.bind_values;

                    let can_add = delete_batch
                        .as_ref()
                        .map(|b| b.can_add(Some(&dest_schema), table, &key_cols, replica_identity))
                        .unwrap_or(false);

                    if can_add {
                        let batch = delete_batch.as_mut().unwrap();
                        batch.add_row(key_values);

                        let max_batch_size = self.calculate_delete_batch_size(batch);

                        if batch.len() >= max_batch_size {
                            execute_batch_delete(&mut *db_tx, batch, &self.schema_mappings).await?;
                            delete_batch = None;
                        }
                    } else {
                        if let Some(batch) = delete_batch.take() {
                            execute_batch_delete(&mut *db_tx, &batch, &self.schema_mappings)
                                .await?;
                        }

                        let mut new_batch = common::DeleteBatch::new_with_schema(
                            dest_schema,
                            table.clone(),
                            key_cols,
                            replica_identity.clone(),
                        );
                        new_batch.add_row(key_values);
                        delete_batch = Some(new_batch);
                    }
                }
                EventType::Truncate(tables) => {
                    // Flush all pending batches before TRUNCATE
                    if let Some(batch) = insert_batch.take() {
                        execute_batch_insert(&mut *db_tx, &batch, &self.schema_mappings).await?;
                    }
                    if let Some(batch) = update_batch.take() {
                        execute_batch_update(&mut db_tx, &batch, &self.schema_mappings).await?;
                    }
                    if let Some(batch) = delete_batch.take() {
                        execute_batch_delete(&mut *db_tx, &batch, &self.schema_mappings).await?;
                    }

                    for table_full_name in tables {
                        let mut parts = table_full_name.splitn(2, '.');
                        let sql = match (parts.next(), parts.next()) {
                            (Some(schema), Some(table)) => {
                                let dest_schema = common::map_schema(&self.schema_mappings, schema);
                                format!("TRUNCATE TABLE `{}`.`{}`", dest_schema, table)
                            }
                            _ => format!("TRUNCATE TABLE `{}`", table_full_name),
                        };
                        sqlx::query(&sql).execute(&mut *db_tx).await?;
                    }
                }
                _ => {
                    // For other event types (BEGIN, COMMIT, RELATION, etc.), skip
                    debug!("Skipping non-DML event: {:?}", event.event_type);
                }
            }
        }

        // Flush any remaining batches
        if let Some(batch) = insert_batch.take() {
            execute_batch_insert(&mut *db_tx, &batch, &self.schema_mappings).await?;
        }
        if let Some(batch) = update_batch.take() {
            execute_batch_update(&mut db_tx, &batch, &self.schema_mappings).await?;
        }
        if let Some(batch) = delete_batch.take() {
            execute_batch_delete(&mut *db_tx, &batch, &self.schema_mappings).await?;
        }

        // Commit the transaction
        db_tx
            .commit()
            .await
            .map_err(|e| CdcError::generic(format!("Failed to commit MySQL transaction: {}", e)))?;

        info!(
            "MySQL: Committed batch for transaction {} ({} events) in {:?}",
            tx_id,
            transaction.event_count(),
            tx_start.elapsed()
        );

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

        // Query max_allowed_packet to calculate optimal batch sizes
        let row: (String, String) = sqlx::query_as("SHOW VARIABLES LIKE 'max_allowed_packet'")
            .fetch_one(&pool)
            .await
            .map_err(|e| CdcError::generic(format!("Failed to query max_allowed_packet: {}", e)))?;

        // Parse the string value to u64
        self.max_allowed_packet = row.1.parse::<u64>().unwrap_or(67108864); // Default to 64MB if parse fails

        info!(
            "MySQL max_allowed_packet: {} bytes ({:.2} MB)",
            self.max_allowed_packet,
            self.max_allowed_packet as f64 / 1_048_576.0
        );

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

    async fn execute_sql_batch(&mut self, commands: &[String]) -> Result<()> {
        if commands.is_empty() {
            return Ok(());
        }

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("MySQL pool not initialized"))?;

        // Begin a transaction
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| CdcError::generic(format!("MySQL BEGIN transaction failed: {}", e)))?;

        // Execute all commands in the transaction
        for (idx, sql) in commands.iter().enumerate() {
            if let Err(e) = sqlx::query(sql).execute(&mut *tx).await {
                // Rollback on error
                if let Err(rollback_err) = tx.rollback().await {
                    tracing::error!(
                        "MySQL ROLLBACK failed after execution error: {}",
                        rollback_err
                    );
                }
                return Err(CdcError::generic(format!(
                    "MySQL execute_sql_batch failed at command {}/{}: {}",
                    idx + 1,
                    commands.len(),
                    e
                )));
            }
        }

        // Commit the transaction
        tx.commit()
            .await
            .map_err(|e| CdcError::generic(format!("MySQL COMMIT transaction failed: {}", e)))?;

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(pool) = &self.pool {
            pool.close().await;
        }
        self.pool = None;
        info!("MySQL connection closed successfully");
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
        let mut batch = common::InsertBatch::new_with_schema(
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
            Some("test_schema"),
            "test_table",
            &["col1".to_string(), "col2".to_string()]
        ));
        assert!(!batch.can_add(
            Some("other_schema"),
            "test_table",
            &["col1".to_string(), "col2".to_string()]
        ));
        assert!(!batch.can_add(
            Some("test_schema"),
            "other_table",
            &["col1".to_string(), "col2".to_string()]
        ));
        assert!(!batch.can_add(Some("test_schema"), "test_table", &["col1".to_string()]));
    }

    #[test]
    fn test_map_schema() {
        let mut mappings = HashMap::new();
        mappings.insert("public".to_string(), "cdc_db".to_string());

        assert_eq!(common::map_schema(&mappings, "public"), "cdc_db");
        assert_eq!(common::map_schema(&mappings, "other"), "other");
    }

    #[test]
    fn test_where_clause_for_update_full_identity() {
        use crate::types::ReplicaIdentity;

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
        use crate::types::ReplicaIdentity;

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
            .contains("requires old_data"));
    }

    #[test]
    fn test_where_clause_for_update_default_identity() {
        use crate::types::ReplicaIdentity;

        let mut old_data = HashMap::new();
        old_data.insert("id".to_string(), serde_json::json!(1));
        old_data.insert("name".to_string(), serde_json::json!("test"));

        let new_data = HashMap::new();
        let old_data_opt = Some(old_data);
        let key_columns = vec!["id".to_string()];

        let result = build_where_clause_for_update(
            &old_data_opt,
            &new_data,
            &ReplicaIdentity::Default,
            &key_columns,
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
        use crate::types::ReplicaIdentity;

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
        use crate::types::ReplicaIdentity;

        let old_data = HashMap::new();

        let result = build_where_clause_for_delete(
            &old_data,
            &ReplicaIdentity::Nothing,
            &[],
            "public",
            "users",
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot DELETE"));
    }
}
