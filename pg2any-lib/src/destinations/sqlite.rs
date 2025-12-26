use super::{common, destination_factory::DestinationHandler};
use crate::{
    error::{CdcError, Result},
    types::{EventType, ReplicaIdentity, Transaction},
};
use async_trait::async_trait;
use sqlx::{sqlite::SqliteConnectOptions, Sqlite, SqlitePool};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use tracing::{debug, info};

// SQLite has SQLITE_MAX_VARIABLE_NUMBER (default 999)
// We need to account for this when batching to avoid exceeding the limit
const SQLITE_MAX_VARIABLES: usize = 999;

// ============================================================================
// SQL Building Utilities (Free Functions)
// ============================================================================

/// Bind a JSON value to a sqlx query
fn bind_value<'a>(
    query: sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>>,
    value: &'a serde_json::Value,
) -> sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>> {
    match value {
        serde_json::Value::String(s) => query.bind(s.as_str()),
        serde_json::Value::Number(n) if n.is_i64() => query.bind(n.as_i64().unwrap()),
        serde_json::Value::Number(n) if n.is_f64() => query.bind(n.as_f64().unwrap()),
        serde_json::Value::Bool(b) => query.bind(*b),
        serde_json::Value::Null => query.bind(Option::<String>::None),
        _ => query.bind(value.to_string()),
    }
}

/// Execute a batch INSERT statement for multiple rows
async fn execute_batch_insert<'c, E>(executor: E, batch: &common::InsertBatch<'_>) -> Result<()>
where
    E: sqlx::Executor<'c, Database = Sqlite>,
{
    if batch.is_empty() {
        return Ok(());
    }

    let columns: Vec<String> = batch.columns.iter().map(|k| format!("\"{}\"", k)).collect();
    let num_columns = columns.len();

    let row_placeholder = format!(
        "({})",
        (0..num_columns).map(|_| "?").collect::<Vec<_>>().join(", ")
    );

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

    for row in &batch.rows {
        for value in row {
            query = bind_value(query, value);
        }
    }

    let batch_start = std::time::Instant::now();
    query.execute(executor).await?;
    let batch_duration = batch_start.elapsed();

    info!(
        "SQLite Batch INSERT: {} rows into \"{}\" in {:?}",
        batch.rows.len(),
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
    db_tx: &mut sqlx::Transaction<'c, Sqlite>,
    batch: &common::UpdateBatch<'_>,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    // Build SET clause (same for all rows)
    let set_clause: Vec<String> = batch
        .update_columns
        .iter()
        .map(|col| format!("\"{}\" = ?", col))
        .collect();

    // Build WHERE clause (same structure for all rows)
    let where_clause: Vec<String> = batch
        .key_columns
        .iter()
        .map(|col| format!("\"{}\" = ?", col))
        .collect();

    let base_sql = format!(
        "UPDATE \"{}\" SET {} WHERE {}",
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
        "SQLite Batch UPDATE: {} statements on \"{}\" in {:?}",
        batch.rows.len(),
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
async fn execute_batch_delete<'c, E>(executor: E, batch: &common::DeleteBatch<'_>) -> Result<()>
where
    E: sqlx::Executor<'c, Database = Sqlite>,
{
    if batch.is_empty() {
        return Ok(());
    }

    // Build WHERE IN clause for key columns
    let where_clause = if batch.key_columns.len() == 1 {
        // Single key column: WHERE key IN (?, ?, ?)
        let placeholders = (0..batch.rows.len())
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");
        format!("\"{}\" IN ({})", batch.key_columns[0], placeholders)
    } else {
        // Multiple key columns: WHERE (key1, key2) IN ((?, ?), (?, ?))
        let key_cols = batch
            .key_columns
            .iter()
            .map(|col| format!("\"{}\"", col))
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

    let table_ref = format!("\"{}\"", batch.table);
    let sql = format!("DELETE FROM {} WHERE {}", table_ref, where_clause);

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
        "SQLite Batch DELETE: {} rows from \"{}\" in {:?}",
        batch.rows.len(),
        batch.table,
        batch_duration
    );

    Ok(())
}

// ============================================================================
// SQLite Destination Implementation
// ============================================================================

/// SQLite destination implementation with batch INSERT/UPDATE/DELETE support
///
/// This implementation uses sqlx for database operations with async support.
/// SQLite connections are managed through sqlx's connection pool.
pub struct SQLiteDestination {
    pool: Option<SqlitePool>,
    database_path: Option<String>,
}

impl SQLiteDestination {
    /// Create a new SQLite destination instance
    pub fn new() -> Self {
        Self {
            pool: None,
            database_path: None,
        }
    }

    /// Calculate maximum safe batch size for INSERT operations
    ///
    /// SQLite has SQLITE_MAX_VARIABLE_NUMBER (default 999) limit on placeholders
    /// We must ensure rows * columns doesn't exceed this limit
    fn calculate_insert_batch_size(&self, batch: &common::InsertBatch<'_>) -> usize {
        let num_columns = batch.columns.len();
        if num_columns == 0 {
            return 500; // Fallback default
        }

        // SQLite variable limit constraint
        let max_rows_by_variables = SQLITE_MAX_VARIABLES / num_columns.max(1);

        // Estimate bytes per row based on actual data
        let estimated_bytes_per_row = common::estimate_insert_batch_row_size(batch, 10);

        // SQL overhead
        let statement_overhead = num_columns * 3; // Placeholders "?, "
        let fixed_overhead = 100;

        // No packet size limit for SQLite (0 means unlimited)
        let calculated = common::calculate_batch_size(
            estimated_bytes_per_row,
            statement_overhead,
            fixed_overhead,
            0, // No packet size limit
            0.75,
            10,
            5000,
        );

        // Return the minimum of calculated size and variable limit
        calculated.min(max_rows_by_variables).max(10)
    }

    /// Calculate maximum safe batch size for UPDATE operations
    fn calculate_update_batch_size(&self, batch: &common::UpdateBatch<'_>) -> usize {
        if batch.update_columns.is_empty() || batch.key_columns.is_empty() {
            return 200; // Conservative fallback
        }

        // SQLite variable limit: each UPDATE uses update_columns + key_columns variables
        let variables_per_row = batch.update_columns.len() + batch.key_columns.len();
        let max_rows_by_variables = SQLITE_MAX_VARIABLES / variables_per_row.max(1);

        // Estimate bytes per row
        let estimated_bytes_per_row = common::estimate_update_batch_row_size(batch, 5);

        // Per-statement overhead
        let statement_overhead = variables_per_row * 20;
        let fixed_overhead = 200;

        let calculated = common::calculate_batch_size(
            estimated_bytes_per_row,
            statement_overhead,
            fixed_overhead,
            0, // No packet size limit
            0.75,
            10,
            1000,
        );

        calculated.min(max_rows_by_variables).max(10)
    }

    /// Calculate maximum safe batch size for DELETE operations
    fn calculate_delete_batch_size(&self, batch: &common::DeleteBatch<'_>) -> usize {
        if batch.key_columns.is_empty() {
            return 500; // Conservative fallback
        }

        // SQLite variable limit for WHERE IN clause
        let variables_per_row = batch.key_columns.len();
        let max_rows_by_variables = SQLITE_MAX_VARIABLES / variables_per_row.max(1);

        // Estimate bytes per row
        let estimated_bytes_per_row = common::estimate_delete_batch_row_size(batch, 10);

        // WHERE IN overhead
        let statement_overhead = variables_per_row * 20;
        let fixed_overhead = 150;

        let calculated = common::calculate_batch_size(
            estimated_bytes_per_row,
            statement_overhead,
            fixed_overhead,
            0, // No packet size limit
            0.75,
            10,
            5000,
        );

        calculated.min(max_rows_by_variables).max(10)
    }
}

impl Default for SQLiteDestination {
    fn default() -> Self {
        Self::new()
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
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
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
                    "Failed to parse SQLite connection string '{}': {}",
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

    // SQLite does not use schema/database namespacing like MySQL, so schema mappings are not needed
    fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

    async fn execute_sql_batch(&mut self, commands: &[String]) -> Result<()> {
        if commands.is_empty() {
            return Ok(());
        }

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("SQLite pool not initialized"))?;

        // Begin a transaction
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| CdcError::generic(format!("SQLite BEGIN transaction failed: {}", e)))?;

        // Execute all commands in the transaction
        for (idx, sql) in commands.iter().enumerate() {
            if let Err(e) = sqlx::query(sql).execute(&mut *tx).await {
                // Rollback on error
                if let Err(rollback_err) = tx.rollback().await {
                    tracing::error!(
                        "SQLite ROLLBACK failed after execution error: {}",
                        rollback_err
                    );
                }
                return Err(CdcError::generic(format!(
                    "SQLite execute_sql_batch failed at command {}/{}: {}",
                    idx + 1,
                    commands.len(),
                    e
                )));
            }
        }

        // Commit the transaction
        tx.commit()
            .await
            .map_err(|e| CdcError::generic(format!("SQLite COMMIT transaction failed: {}", e)))?;

        Ok(())
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
