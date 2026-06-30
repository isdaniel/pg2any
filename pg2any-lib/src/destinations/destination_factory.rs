use crate::{
    destinations::dialect::SqlDialect,
    destinations::dialects::AnsiDialect,
    error::{CdcError, Result},
    types::Lsn,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pg_walstream::ChangeEvent;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

pub type PreCommitHook =
    Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send>;

/// Factory closure for constructing a destination handler.
///
/// `Fn` (not `FnOnce`) — the consumer creates an additional handler during
/// `start_file_based_workflow`, so the factory may be invoked multiple times.
pub type DestinationFactoryFn = Arc<dyn Fn() -> Result<Box<dyn DestinationHandler>> + Send + Sync>;

/// Trait for database destination handlers
///
/// Each worker has its own destination handler with its own connection.
/// Workers process complete transactions atomically to ensure data consistency.
///
/// ## File-Based Transaction Processing
///
/// The current architecture uses file-based transaction processing:
/// - Transactions are written to files in sql_data_tx/ as events arrive
/// - Metadata for completed transactions are stored in sql_pending_tx/ on COMMIT/StreamCommit
/// - Consumer reads metadata and executes SQL via `execute_sql_batch()` in atomic transactions
/// - No database transactions are kept open between batches
#[async_trait]
pub trait DestinationHandler: Send + Sync {
    /// Initialize the destination connection
    async fn connect(&mut self, connection_string: &str) -> Result<()>;

    /// Set schema mappings for translating source schemas to destination schemas/databases
    /// Maps source schema (e.g., PostgreSQL "public") to destination schema/database (e.g., MySQL "cdc_db")
    fn set_schema_mappings(&mut self, mappings: HashMap<String, String>);

    /// Set the maximum number of rows per multi-value INSERT statement.
    /// 0 means no limit (database default applies).
    fn set_max_rows_per_insert(&mut self, _max_rows: usize) {}

    /// Return the SQL dialect this destination renders for.
    ///
    /// Defaults to a generic ANSI-flavored dialect — matches the historical
    /// `DestinationType::Custom(_)` fallback in `TransactionManager`, so
    /// existing custom destinations registered via
    /// `Config::custom_destination` keep working without any changes.
    ///
    /// Override this to provide destination-specific quoting, hex literals,
    /// value rendering, and TRUNCATE strategy.
    fn dialect(&self) -> &'static dyn SqlDialect {
        static ANSI: AnsiDialect = AnsiDialect;
        &ANSI
    }

    /// Execute a batch of SQL commands within a single transaction with optional pre-commit hook
    ///
    /// This is the primary method used by the consumer to execute transaction files.
    /// All commands are executed atomically - if any command fails, the entire batch is rolled back.
    ///
    /// ## Transactional Checkpoint Strategy
    ///
    /// The `pre_commit_hook` enables true atomicity between data changes and checkpoint updates:
    /// ```ignore
    /// BEGIN;
    ///   INSERT INTO users ...;  // Your CDC data
    ///   UPDATE products ...;
    ///   
    ///   pre_commit_hook();      // Update checkpoint (in-memory + persist to file)
    /// COMMIT;                   // Both data AND checkpoint committed atomically
    /// ```
    ///
    /// If transaction fails or crashes:
    /// - Before COMMIT: Both data and checkpoint rolled back → Safe
    /// - After COMMIT: Both data and checkpoint committed → Safe
    /// - No race condition possible!
    ///
    /// # Arguments
    /// * `commands` - Vector of SQL commands to execute in a single transaction
    /// * `pre_commit_hook` - Optional boxed async function called BEFORE COMMIT within the transaction
    ///
    /// # Returns
    /// * `Ok(())` - All SQL commands and pre-commit hook executed successfully, transaction committed
    /// * `Err(...)` - Execution or hook failed, entire transaction was rolled back
    async fn execute_sql_batch_with_hook(
        &mut self,
        commands: &[String],
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()>;

    /// Close the connection
    async fn close(&mut self) -> Result<()>;

    fn supports_event_mode(&self) -> bool {
        false
    }

    async fn execute_events_batch_with_hook(
        &mut self,
        _events: &[ChangeEvent],
        _transaction_id: u32,
        _commit_timestamp: DateTime<Utc>,
        _commit_lsn: Option<Lsn>,
        _pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        Err(CdcError::unsupported(
            "Event mode not supported by this destination",
        ))
    }

    fn supports_bulk_insert(&self) -> bool {
        false
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
        let sqls = crate::destinations::bulk_insert::build_chunked_multi_value_inserts(
            table, columns, rows, None, None,
        );
        self.execute_sql_batch_with_hook(&sqls, pre_commit_hook)
            .await
    }

    /// Bulk-insert a homogeneous run of INSERT rows DIRECTLY from structured
    /// `RowData`, skipping the render-SQL-then-reparse-SQL round trip.
    ///
    /// `table` is the already-mapped, dialect-qualified target table reference
    /// (the consumer applies `RenderContext::map_schema` + `qualify_table`, so
    /// it matches the SQL path exactly). `columns` are the raw source column
    /// names (in row order); each destination quotes them with its own dialect.
    /// `rows` borrows each event's `&RowData`.
    ///
    /// The default impl renders each `ColumnValue` to the SQL value string the
    /// dialect would produce and delegates to `execute_bulk_insert_with_hook`,
    /// so every destination keeps working unchanged. Destinations with a native
    /// bulk path (MySQL `LOAD DATA`) override this to build the load buffer
    /// directly from the structured rows.
    async fn execute_bulk_insert_rows_with_hook(
        &mut self,
        table: &str,
        columns: &[Arc<str>],
        rows: &[&pg_walstream::RowData],
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let dialect = self.dialect();

        // Quote columns once with this destination's dialect.
        let quoted_columns: Vec<String> = columns
            .iter()
            .map(|c| {
                let mut s = String::with_capacity(c.len() + 2);
                dialect.quote_identifier(c, &mut s);
                s
            })
            .collect();

        // Render each ColumnValue to its SQL literal, aligning to `columns` BY NAME. The trait contract takes `columns` and `rows` separately; a custom caller may pass rows whose `(name, value)` order differs from  `columns`. Looking up by name (absent column → Null) prevents silently writing a value into the wrong column.
        let mut rendered_rows: Vec<Vec<String>> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut rendered = Vec::with_capacity(columns.len());
            for col in columns {
                let mut s = String::new();
                match row
                    .iter()
                    .find(|(name, _)| name.as_ref() == col.as_ref())
                    .map(|(_, value)| value)
                {
                    Some(value) => dialect.render_value(value, &mut s),
                    None => dialect.render_value(&pg_walstream::ColumnValue::Null, &mut s),
                }
                rendered.push(s);
            }
            rendered_rows.push(rendered);
        }

        self.execute_bulk_insert_with_hook(table, &quoted_columns, &rendered_rows, pre_commit_hook)
            .await
    }
}
