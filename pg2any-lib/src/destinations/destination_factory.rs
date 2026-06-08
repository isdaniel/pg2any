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
}
