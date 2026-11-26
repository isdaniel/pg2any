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

/// One source transaction's events, as handed to `execute_events_group_with_hook`.
pub struct EventTransaction<'a> {
    pub transaction_id: u32,
    pub commit_timestamp: DateTime<Utc>,
    pub commit_lsn: Option<Lsn>,
    pub events: &'a [ChangeEvent],
}

pub type PreCommitHook =
    Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send>;

/// Factory closure for constructing a destination handler.
///
/// `Fn` (not `FnOnce`): the factory is invoked once for the live pipeline (the recovery and the
/// consumer share that handler) and once per snapshot writer.
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

    /// Group commit: apply several whole transactions (ascending `commit_lsn`) in one go. There
    /// is no resume checkpoint: on error the caller retries the transactions one by one, so a
    /// destination must either deliver the whole group or fail without delivering any of it
    /// (or tolerate a repeat, like Kafka). The default delivers nothing and returns
    /// `Unsupported`: the caller then applies the transactions one by one.
    async fn execute_events_group_with_hook(
        &mut self,
        _group: &[EventTransaction<'_>],
    ) -> Result<()> {
        Err(CdcError::unsupported(
            "Grouped event delivery not supported by this destination",
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

    /// Snapshot: whether the destination table `schema.table` holds no rows.
    /// `schema` is the already-mapped destination schema. Errors when the table
    /// is missing or unreadable. Destinations that do not override this cannot
    /// be used with `SnapshotTargetPolicy::FailIfNotEmpty`.
    async fn snapshot_table_is_empty(&mut self, _schema: &str, _table: &str) -> Result<bool> {
        Err(CdcError::unsupported(
            "destination does not support snapshot target checks",
        ))
    }

    /// Snapshot: clear the destination table `schema.table` (mapped schema).
    /// A dialect without TRUNCATE (e.g. Kafka) makes this a no-op.
    async fn snapshot_truncate_table(&mut self, schema: &str, table: &str) -> Result<()> {
        match self.dialect().truncate_table_sql(schema, table) {
            None => Ok(()),
            Some(sql) => self.execute_sql_batch_with_hook(&[sql], None).await,
        }
    }

    /// Snapshot: apply a batch of row events captured at `consistent_point`.
    async fn execute_snapshot_events_batch(
        &mut self,
        events: &[ChangeEvent],
        consistent_point: Lsn,
    ) -> Result<()> {
        self.execute_events_batch_with_hook(events, 0, Utc::now(), Some(consistent_point), None)
            .await
    }

    /// Key columns of a source table (`schema.table` as named in the source), for destinations
    /// that key records (Kafka overrides). Given for snapshot rows and, at start, for the live
    /// path's inserts.
    fn set_snapshot_key_columns(&mut self, _source_table_key: &str, _columns: Vec<Arc<str>>) {}

    /// Maximum concurrent snapshot writers; SQLite and Kafka override to 1.
    fn max_snapshot_writers(&self) -> usize {
        usize::MAX
    }

    /// Snapshot: called once on every snapshot writer handler before its first batch, so a
    /// destination can enable fast paths that are only safe for the snapshot's rendered rows, or
    /// drop checks that only the live path needs (SQL Server's triggers and constraints).
    fn begin_snapshot_writes(&mut self) {}
}
