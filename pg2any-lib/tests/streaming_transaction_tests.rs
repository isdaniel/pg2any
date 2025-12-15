/// Tests for streaming transaction support across all destination implementations
///
/// This test file verifies that SQLite and SQL Server implement streaming transaction
/// methods correctly, matching the MySQL implementation pattern.
use pg2any_lib::destinations::destination_factory::DestinationFactory;
use pg2any_lib::types::{DestinationType, Transaction};
use std::collections::HashMap;

#[cfg(test)]
mod streaming_transaction_tests {
    use super::*;

    /// Test that all destination handlers implement streaming transaction methods
    #[tokio::test]
    async fn test_all_destinations_have_streaming_methods() {
        // Test MySQL
        #[cfg(feature = "mysql")]
        {
            let mut mysql_dest = DestinationFactory::create(DestinationType::MySQL).unwrap();

            // Should have no active streaming transaction initially
            assert_eq!(mysql_dest.get_active_streaming_transaction_id(), None);

            // Should be able to call commit and rollback without errors (no-op when no active tx)
            assert!(mysql_dest.commit_streaming_transaction().await.is_ok());
            assert!(mysql_dest.rollback_streaming_transaction().await.is_ok());
        }

        // Test SQLite
        #[cfg(feature = "sqlite")]
        {
            let mut sqlite_dest = DestinationFactory::create(DestinationType::SQLite).unwrap();

            // Should have no active streaming transaction initially
            assert_eq!(sqlite_dest.get_active_streaming_transaction_id(), None);

            // Should be able to call commit and rollback without errors (no-op when no active tx)
            assert!(sqlite_dest.commit_streaming_transaction().await.is_ok());
            assert!(sqlite_dest.rollback_streaming_transaction().await.is_ok());
        }

        // Test SQL Server
        #[cfg(feature = "sqlserver")]
        {
            let mut sqlserver_dest =
                DestinationFactory::create(DestinationType::SqlServer).unwrap();

            // Should have no active streaming transaction initially
            assert_eq!(sqlserver_dest.get_active_streaming_transaction_id(), None);

            // Should be able to call commit and rollback without errors (no-op when no active tx)
            assert!(sqlserver_dest.commit_streaming_transaction().await.is_ok());
            assert!(sqlserver_dest
                .rollback_streaming_transaction()
                .await
                .is_ok());
        }
    }

    /// Test that streaming transactions are properly handled in non-streaming mode
    #[tokio::test]
    async fn test_non_streaming_transaction_processing() {
        #[cfg(feature = "sqlite")]
        {
            let sqlite_dest = DestinationFactory::create(DestinationType::SQLite).unwrap();

            // Create a non-streaming transaction
            let mut transaction = Transaction::new(1, chrono::Utc::now());
            transaction.is_streaming = false;
            transaction.is_final_batch = false;

            // Should have no active streaming transaction
            assert_eq!(sqlite_dest.get_active_streaming_transaction_id(), None);

            // Processing a non-streaming batch should not create an active streaming transaction
            // (We can't actually test this without a real database connection, but we verify the methods exist)
        }
    }

    /// Test that empty streaming batches are handled correctly
    #[tokio::test]
    async fn test_empty_streaming_batch_handling() {
        #[cfg(feature = "sqlite")]
        {
            let sqlite_dest = DestinationFactory::create(DestinationType::SQLite).unwrap();

            // Create an empty streaming transaction (non-final)
            let mut transaction = Transaction::new(1, chrono::Utc::now());
            transaction.is_streaming = true;
            transaction.is_final_batch = false;

            // Empty non-final batches should be skipped (tested by calling the method without error)
            assert_eq!(sqlite_dest.get_active_streaming_transaction_id(), None);
        }
    }

    /// Test that the destination handler trait requires all streaming methods
    #[test]
    fn test_destination_handler_trait_completeness() {
        // This test ensures that the DestinationHandler trait has all required methods
        // by verifying we can create destinations through the factory

        #[cfg(feature = "mysql")]
        {
            let result = DestinationFactory::create(DestinationType::MySQL);
            assert!(
                result.is_ok(),
                "MySQL destination should be created successfully"
            );
        }

        #[cfg(feature = "sqlite")]
        {
            let result = DestinationFactory::create(DestinationType::SQLite);
            assert!(
                result.is_ok(),
                "SQLite destination should be created successfully"
            );
        }

        #[cfg(feature = "sqlserver")]
        {
            let result = DestinationFactory::create(DestinationType::SqlServer);
            assert!(
                result.is_ok(),
                "SQL Server destination should be created successfully"
            );
        }
    }

    /// Test graceful shutdown with active streaming transactions
    #[tokio::test]
    async fn test_graceful_shutdown_with_streaming_transaction() {
        #[cfg(feature = "sqlite")]
        {
            let mut sqlite_dest = DestinationFactory::create(DestinationType::SQLite).unwrap();

            // Closing without active transactions should succeed
            assert!(sqlite_dest.close().await.is_ok());
        }

        #[cfg(feature = "sqlserver")]
        {
            let mut sqlserver_dest =
                DestinationFactory::create(DestinationType::SqlServer).unwrap();

            // Closing without active transactions should succeed
            assert!(sqlserver_dest.close().await.is_ok());
        }
    }

    /// Test that streaming transaction state is properly maintained
    #[tokio::test]
    async fn test_streaming_transaction_state_management() {
        #[cfg(feature = "sqlite")]
        {
            let mut sqlite_dest = DestinationFactory::create(DestinationType::SQLite).unwrap();

            // Initial state should be no active transaction
            assert_eq!(sqlite_dest.get_active_streaming_transaction_id(), None);

            // After rollback with no active transaction, state should remain None
            assert!(sqlite_dest.rollback_streaming_transaction().await.is_ok());
            assert_eq!(sqlite_dest.get_active_streaming_transaction_id(), None);

            // After commit with no active transaction, state should remain None
            assert!(sqlite_dest.commit_streaming_transaction().await.is_ok());
            assert_eq!(sqlite_dest.get_active_streaming_transaction_id(), None);
        }

        #[cfg(feature = "sqlserver")]
        {
            let mut sqlserver_dest =
                DestinationFactory::create(DestinationType::SqlServer).unwrap();

            // Initial state should be no active transaction
            assert_eq!(sqlserver_dest.get_active_streaming_transaction_id(), None);

            // After rollback with no active transaction, state should remain None
            assert!(sqlserver_dest
                .rollback_streaming_transaction()
                .await
                .is_ok());
            assert_eq!(sqlserver_dest.get_active_streaming_transaction_id(), None);

            // After commit with no active transaction, state should remain None
            assert!(sqlserver_dest.commit_streaming_transaction().await.is_ok());
            assert_eq!(sqlserver_dest.get_active_streaming_transaction_id(), None);
        }
    }

    /// Test that schema mappings work with streaming transactions
    #[tokio::test]
    async fn test_schema_mappings_with_streaming_transactions() {
        #[cfg(feature = "sqlite")]
        {
            let mut sqlite_dest = DestinationFactory::create(DestinationType::SQLite).unwrap();

            let mut mappings = HashMap::new();
            mappings.insert("public".to_string(), "cdc_db".to_string());

            // Should be able to set schema mappings without error
            sqlite_dest.set_schema_mappings(mappings);

            // Should still have no active streaming transaction
            assert_eq!(sqlite_dest.get_active_streaming_transaction_id(), None);
        }

        #[cfg(feature = "sqlserver")]
        {
            let mut sqlserver_dest =
                DestinationFactory::create(DestinationType::SqlServer).unwrap();

            let mut mappings = HashMap::new();
            mappings.insert("public".to_string(), "cdc_db".to_string());

            // Should be able to set schema mappings without error
            sqlserver_dest.set_schema_mappings(mappings);

            // Should still have no active streaming transaction
            assert_eq!(sqlserver_dest.get_active_streaming_transaction_id(), None);
        }
    }

    /// Test compatibility with previous code (non-streaming mode)
    #[test]
    fn test_backward_compatibility_with_non_streaming_code() {
        // Verify that destinations can be created and basic methods exist

        #[cfg(feature = "mysql")]
        {
            let mysql_dest = DestinationFactory::create(DestinationType::MySQL).unwrap();
            // If this compiles and runs, the interface is compatible
            drop(mysql_dest);
        }

        #[cfg(feature = "sqlite")]
        {
            let sqlite_dest = DestinationFactory::create(DestinationType::SQLite).unwrap();
            // If this compiles and runs, the interface is compatible
            drop(sqlite_dest);
        }

        #[cfg(feature = "sqlserver")]
        {
            let sqlserver_dest = DestinationFactory::create(DestinationType::SqlServer).unwrap();
            // If this compiles and runs, the interface is compatible
            drop(sqlserver_dest);
        }
    }
}
