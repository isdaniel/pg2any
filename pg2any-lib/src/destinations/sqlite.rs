use super::coalescing::{coalesce_commands, QuoteStyle};
use super::destination_factory::{DestinationHandler, PreCommitHook};
use crate::destinations::dialect::SqlDialect;
use crate::error::{CdcError, Result};
use async_trait::async_trait;
use sqlx::{sqlite::SqliteConnectOptions, SqlitePool};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use tracing::{debug, info};

// ============================================================================
// SQLite Destination Implementation
// ============================================================================

/// SQLite's default `SQLITE_MAX_COMPOUND_SELECT`: the coalesced UPDATE statements join their rows
/// with `UNION ALL`, so a group is never larger than this.
const MAX_ROWS_PER_INSERT: usize = 500;

/// SQLite destination implementation with batch INSERT/UPDATE/DELETE support
///
/// This implementation uses sqlx for database operations with async support.
/// SQLite connections are managed through sqlx's connection pool.
pub struct SQLiteDestination {
    pool: Option<SqlitePool>,
    database_path: Option<String>,
    max_rows_per_insert: usize,
}

impl SQLiteDestination {
    /// Create a new SQLite destination instance
    pub fn new() -> Self {
        Self {
            pool: None,
            database_path: None,
            max_rows_per_insert: MAX_ROWS_PER_INSERT,
        }
    }
}

impl SQLiteDestination {
    /// The identifiers go through the dialect's quoting, which doubles an embedded quote.
    fn snapshot_probe_sql(&self, schema: &str, table: &str) -> String {
        let mut qualified = String::new();
        self.dialect().qualify_table(schema, table, &mut qualified);
        format!("SELECT 1 FROM {qualified} LIMIT 1")
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
            if tokio::fs::metadata(parent).await.is_err() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    CdcError::generic(format!(
                        "Failed to create directory for SQLite database: {e}"
                    ))
                })?;
            }
        }

        // Create connection options
        let mut options =
            SqliteConnectOptions::from_str(&format!("sqlite://{db_path}")).map_err(|e| {
                CdcError::generic(format!(
                    "Failed to parse SQLite connection string '{db_path}': {e}"
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
                "Failed to connect to SQLite database '{db_path}': {e}"
            ))
        })?;

        self.pool = Some(pool);
        self.database_path = Some(db_path.to_string());

        info!("Connected to SQLite database: {}", db_path);
        Ok(())
    }

    // SQLite does not use schema/database namespacing like MySQL, so schema mappings are not needed
    fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

    fn set_max_rows_per_insert(&mut self, max_rows: usize) {
        if max_rows > 0 {
            self.max_rows_per_insert = max_rows.min(MAX_ROWS_PER_INSERT);
        }
    }

    fn dialect(&self) -> &'static dyn SqlDialect {
        use crate::destinations::dialects::SqliteDialect;
        static D: SqliteDialect = SqliteDialect;
        &D
    }

    async fn execute_sql_batch_with_hook(
        &mut self,
        commands: &[String],
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        if commands.is_empty() {
            return Ok(());
        }

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("SQLite pool not initialized"))?;

        // Coalesce consecutive DML statements before executing:
        // - INSERT → multi-value INSERT
        // - UPDATE → CASE-WHEN batch UPDATE
        // - DELETE → OR-combined WHERE clause
        let coalesced = coalesce_commands(
            commands,
            u64::MAX,
            QuoteStyle::DoubleQuote,
            self.max_rows_per_insert,
        );

        if coalesced.len() < commands.len() {
            debug!(
                "Coalesced {} commands into {} statements (reduction: {:.1}%)",
                commands.len(),
                coalesced.len(),
                (1.0 - coalesced.len() as f64 / commands.len() as f64) * 100.0
            );
        }

        super::common::execute_sqlx_batch_with_hook(pool, coalesced, pre_commit_hook, "SQLite")
            .await
    }

    async fn execute_bulk_insert_with_hook(
        &mut self,
        table: &str,
        columns: &[String],
        rows: &[Vec<String>],
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let sqls = super::bulk_insert::build_chunked_multi_value_inserts(
            table,
            columns,
            rows,
            None,
            Some(self.max_rows_per_insert),
        );
        // Already chunked by `max_rows_per_insert`: coalescing would only copy each statement.
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("SQLite pool not initialized"))?;
        let statements = sqls.into_iter().map(std::borrow::Cow::Owned).collect();
        super::common::execute_sqlx_batch_with_hook(pool, statements, pre_commit_hook, "SQLite")
            .await
    }

    async fn snapshot_table_is_empty(&mut self, schema: &str, table: &str) -> Result<bool> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| CdcError::generic("SQLite pool not initialized"))?;
        let sql = self.snapshot_probe_sql(schema, table);
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                CdcError::generic(format!("SQLite snapshot emptiness probe failed: {e}"))
            })?;
        Ok(row.is_none())
    }

    fn max_snapshot_writers(&self) -> usize {
        1
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_probe_quotes_hostile_identifiers() {
        let destination = SQLiteDestination::new();
        assert_eq!(
            destination.snapshot_probe_sql("main", "Snap\"Mixed"),
            "SELECT 1 FROM \"Snap\"\"Mixed\" LIMIT 1"
        );
        assert_eq!(
            destination.snapshot_probe_sql("main", "x\"; DROP TABLE users; --"),
            "SELECT 1 FROM \"x\"\"; DROP TABLE users; --\" LIMIT 1"
        );
    }

    async fn connected(dir: &tempfile::TempDir) -> SQLiteDestination {
        let mut dest = SQLiteDestination::new();
        let path = dir.path().join("snap.db");
        dest.connect(path.to_str().unwrap()).await.unwrap();
        dest
    }

    #[tokio::test]
    async fn snapshot_table_is_empty_true_then_false() {
        let dir = tempfile::tempdir().unwrap();
        let mut dest = connected(&dir).await;
        dest.execute_sql_batch_with_hook(
            &["CREATE TABLE \"users\" (id INTEGER PRIMARY KEY);".to_string()],
            None,
        )
        .await
        .unwrap();

        assert!(dest
            .snapshot_table_is_empty("public", "users")
            .await
            .unwrap());

        dest.execute_sql_batch_with_hook(
            &["INSERT INTO \"users\" (id) VALUES (1);".to_string()],
            None,
        )
        .await
        .unwrap();

        assert!(!dest
            .snapshot_table_is_empty("public", "users")
            .await
            .unwrap());
    }

    // Each batch inlines its values, so its text is unique; a cached statement per batch would
    // keep up to 100 batches (megabytes each) alive per connection.
    #[tokio::test]
    async fn batches_do_not_fill_the_statement_cache() {
        use sqlx::Connection;
        let dir = tempfile::tempdir().unwrap();
        let mut dest = connected(&dir).await;
        dest.execute_sql_batch_with_hook(
            &["CREATE TABLE \"users\" (id INTEGER PRIMARY KEY);".to_string()],
            None,
        )
        .await
        .unwrap();
        for id in 1..=3 {
            dest.execute_bulk_insert_with_hook(
                "\"users\"",
                &["id".to_string()],
                &[vec![id.to_string()]],
                None,
            )
            .await
            .unwrap();
        }

        let conn = dest.pool.as_ref().unwrap().acquire().await.unwrap();
        assert_eq!(conn.cached_statements_size(), 0);
    }

    #[tokio::test]
    async fn snapshot_table_is_empty_errors_for_missing_table() {
        let dir = tempfile::tempdir().unwrap();
        let mut dest = connected(&dir).await;

        assert!(dest
            .snapshot_table_is_empty("public", "no_such_table")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn snapshot_truncate_table_removes_rows() {
        let dir = tempfile::tempdir().unwrap();
        let mut dest = connected(&dir).await;
        dest.execute_sql_batch_with_hook(
            &[
                "CREATE TABLE \"users\" (id INTEGER PRIMARY KEY);".to_string(),
                "INSERT INTO \"users\" (id) VALUES (1), (2);".to_string(),
            ],
            None,
        )
        .await
        .unwrap();
        assert!(!dest
            .snapshot_table_is_empty("public", "users")
            .await
            .unwrap());

        dest.snapshot_truncate_table("public", "users")
            .await
            .unwrap();

        assert!(dest
            .snapshot_table_is_empty("public", "users")
            .await
            .unwrap());
    }

    #[test]
    fn max_snapshot_writers_is_one() {
        assert_eq!(SQLiteDestination::new().max_snapshot_writers(), 1);
    }

    #[test]
    fn max_rows_per_insert_is_capped_at_the_compound_select_limit() {
        let mut destination = SQLiteDestination::new();
        destination.set_max_rows_per_insert(5000);
        assert_eq!(destination.max_rows_per_insert, 500);
        destination.set_max_rows_per_insert(10);
        assert_eq!(destination.max_rows_per_insert, 10);
        destination.set_max_rows_per_insert(0);
        assert_eq!(destination.max_rows_per_insert, 10);
    }
}
