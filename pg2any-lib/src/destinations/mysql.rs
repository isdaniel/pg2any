use super::{common, destination_factory::DestinationHandler};
use crate::{
    error::{CdcError, Result},
    types::{ChangeEvent, EventType, Transaction},
};
use async_trait::async_trait;
use sqlx::{Executor, MySql, MySqlPool};
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Maximum number of rows per batch INSERT statement
/// Limited by MySQL's max_allowed_packet and number of placeholders
const MAX_BATCH_INSERT_SIZE: usize = 1000;

/// Type alias for MySQL's open transaction
/// Uses generic OpenTransaction with MySQL-specific connection type
type OpenTransaction = common::OpenTransaction<sqlx::pool::PoolConnection<MySql>>;

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
            let dest_schema = common::map_schema(schema_mappings, schema);
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
            let dest_schema = common::map_schema(schema_mappings, schema);
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
            let dest_schema = common::map_schema(schema_mappings, schema);
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
                        let dest_schema = common::map_schema(schema_mappings, schema);
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

// ============================================================================
// MySQL Destination Implementation
// ============================================================================

pub struct MySQLDestination {
    pool: Option<MySqlPool>,
    /// Schema mappings: maps source schema to destination database
    schema_mappings: HashMap<String, String>,
    /// Active open transaction state (kept for compatibility with rollback_streaming_transaction)
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

    /// Rollback the active streaming transaction (for graceful shutdown)
    async fn rollback_active_transaction(&mut self) -> Result<()> {
        if let Some(mut active) = self.active_tx.take() {
            warn!(
                "MySQL: Rolling back streaming transaction {} ({} events processed)",
                active.state.transaction_id, active.state.events_processed
            );

            if let Err(e) = active.handle.execute("ROLLBACK").await {
                warn!(
                    "MySQL: Failed to rollback streaming transaction {}: {}",
                    active.state.transaction_id, e
                );
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

        // Process all events in this batch
        let mut current_batch: Option<common::InsertBatch<'_>> = None;

        for event in &transaction.events {
            match &event.event_type {
                EventType::Insert {
                    schema,
                    table,
                    data,
                    ..
                } => {
                    let dest_schema = common::map_schema(&self.schema_mappings, schema);
                    let mut columns: Vec<String> = data.keys().cloned().collect();
                    columns.sort();

                    let can_add = current_batch
                        .as_ref()
                        .map(|b| b.can_add(Some(&dest_schema), table, &columns))
                        .unwrap_or(false);

                    if can_add {
                        let batch = current_batch.as_mut().unwrap();
                        batch.add_row(data);

                        if batch.len() >= MAX_BATCH_INSERT_SIZE {
                            execute_batch_insert(&mut *db_tx, batch, &self.schema_mappings).await?;
                            current_batch = None;
                        }
                    } else {
                        if let Some(batch) = current_batch.take() {
                            execute_batch_insert(&mut *db_tx, &batch, &self.schema_mappings)
                                .await?;
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
                EventType::Truncate(tables) => {
                    if let Some(batch) = current_batch.take() {
                        execute_batch_insert(&mut *db_tx, &batch, &self.schema_mappings).await?;
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
                    if let Some(batch) = current_batch.take() {
                        execute_batch_insert(&mut *db_tx, &batch, &self.schema_mappings).await?;
                    }
                    process_single_event(&mut *db_tx, event, &self.schema_mappings).await?;
                }
            }
        }

        // Flush any remaining batch
        if let Some(batch) = current_batch.take() {
            execute_batch_insert(&mut *db_tx, &batch, &self.schema_mappings).await?;
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
        self.active_tx.as_ref().map(|tx| tx.state.transaction_id)
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
