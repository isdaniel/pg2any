use crate::{
    error::{CdcError, Result},
    types::{DestinationType, Lsn},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pg_walstream::ChangeEvent;
use std::{collections::HashMap, future::Future, pin::Pin};

pub type PreCommitHook =
    Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send>;

// Import destination implementations
#[cfg(feature = "mysql")]
use super::mysql::MySQLDestination;

#[cfg(feature = "sqlserver")]
use super::sqlserver::SqlServerDestination;

#[cfg(feature = "sqlite")]
use super::sqlite::SQLiteDestination;

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

    /// Configure session tuning parameters (no-op for destinations that don't support it)
    fn set_session_tuning(&mut self, _enabled: bool, _threshold: usize) {}

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
        #[cfg(any(feature = "mysql", feature = "sqlserver"))]
        {
            let sql =
                crate::destinations::bulk_insert::build_multi_value_insert(table, columns, rows);
            return self
                .execute_sql_batch_with_hook(&[sql], pre_commit_hook)
                .await;
        }
        #[cfg(not(any(feature = "mysql", feature = "sqlserver")))]
        {
            let _ = (table, columns, rows, pre_commit_hook);
            Err(CdcError::unsupported(
                "Bulk insert not available without mysql or sqlserver feature",
            ))
        }
    }
}

/// Factory for creating destination handlers
pub struct DestinationFactory;

impl DestinationFactory {
    /// Create a new destination handler for the specified type
    pub fn create(destination_type: &DestinationType) -> Result<Box<dyn DestinationHandler>> {
        match *destination_type {
            #[cfg(feature = "mysql")]
            DestinationType::MySQL => Ok(Box::new(MySQLDestination::new())),

            #[cfg(feature = "sqlserver")]
            DestinationType::SqlServer => Ok(Box::new(SqlServerDestination::new())),

            #[cfg(feature = "sqlite")]
            DestinationType::SQLite => Ok(Box::new(SQLiteDestination::new())),

            #[cfg(feature = "kafka")]
            DestinationType::Kafka => Ok(Box::new(super::kafka::KafkaDestination::new())),

            #[allow(unreachable_patterns)]
            _ => Err(CdcError::unsupported(format!(
                "Destination type {destination_type:?} is not supported or not enabled"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_destination_factory_create() {
        // Test factory creation for different destination types
        #[cfg(feature = "mysql")]
        {
            let result = DestinationFactory::create(&DestinationType::MySQL);
            assert!(result.is_ok());
        }

        #[cfg(feature = "sqlserver")]
        {
            let result = DestinationFactory::create(&DestinationType::SqlServer);
            assert!(result.is_ok());
        }

        #[cfg(feature = "sqlite")]
        {
            let result = DestinationFactory::create(&DestinationType::SQLite);
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_destination_types_serialization() {
        use serde_json;

        let mysql_type = DestinationType::MySQL;
        let json = serde_json::to_string(&mysql_type).unwrap();
        assert_eq!(json, "\"MySQL\"");

        let deserialized: DestinationType = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, mysql_type);

        let sqlite_type = DestinationType::SQLite;
        let sqlite_json = serde_json::to_string(&sqlite_type).unwrap();
        assert_eq!(sqlite_json, "\"SQLite\"");

        let sqlite_deserialized: DestinationType = serde_json::from_str(&sqlite_json).unwrap();
        assert_eq!(sqlite_deserialized, sqlite_type);
    }
}
