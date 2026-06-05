/// Tests for destination handler trait and factory functionality
use pg2any_lib::types::{DestinationType, Transaction};
use std::collections::HashMap;

fn make(
    dt: DestinationType,
) -> pg2any_lib::CdcResult<Box<dyn pg2any_lib::destinations::DestinationHandler>> {
    let mut cfg = pg2any_lib::Config::default();
    cfg.destination_type = dt;
    cfg.create_destination()
}

#[cfg(test)]
mod destination_tests {
    use super::*;

    #[tokio::test]
    async fn test_all_destinations_can_be_created() {
        #[cfg(feature = "mysql")]
        {
            let mysql_dest = make(DestinationType::MySQL);
            assert!(mysql_dest.is_ok());
        }

        #[cfg(feature = "sqlite")]
        {
            let sqlite_dest = make(DestinationType::SQLite);
            assert!(sqlite_dest.is_ok());
        }

        #[cfg(feature = "sqlserver")]
        {
            let sqlserver_dest = make(DestinationType::SqlServer);
            assert!(sqlserver_dest.is_ok());
        }
    }

    #[test]
    fn test_destination_handler_trait_completeness() {
        #[cfg(feature = "mysql")]
        {
            let result = make(DestinationType::MySQL);
            assert!(result.is_ok());
        }

        #[cfg(feature = "sqlite")]
        {
            let result = make(DestinationType::SQLite);
            assert!(result.is_ok());
        }

        #[cfg(feature = "sqlserver")]
        {
            let result = make(DestinationType::SqlServer);
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        #[cfg(feature = "sqlite")]
        {
            let mut sqlite_dest = make(DestinationType::SQLite).unwrap();
            assert!(sqlite_dest.close().await.is_ok());
        }

        #[cfg(feature = "sqlserver")]
        {
            let mut sqlserver_dest = make(DestinationType::SqlServer).unwrap();
            assert!(sqlserver_dest.close().await.is_ok());
        }
    }

    #[tokio::test]
    async fn test_schema_mappings() {
        #[cfg(feature = "sqlite")]
        {
            let mut sqlite_dest = make(DestinationType::SQLite).unwrap();
            let mut mappings = HashMap::new();
            mappings.insert("public".to_string(), "cdc_db".to_string());
            sqlite_dest.set_schema_mappings(mappings);
        }

        #[cfg(feature = "sqlserver")]
        {
            let mut sqlserver_dest = make(DestinationType::SqlServer).unwrap();
            let mut mappings = HashMap::new();
            mappings.insert("public".to_string(), "cdc_db".to_string());
            sqlserver_dest.set_schema_mappings(mappings);
        }
    }

    #[tokio::test]
    async fn test_backward_compatibility() {
        let transaction = Transaction::new(1, chrono::Utc::now());
        assert_eq!(transaction.events.len(), 0);
        assert!(transaction.is_final_batch);
    }
}
