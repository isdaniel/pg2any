use pg2any_lib::{
    destinations::{mysql::MySQLDestination, sqlserver::SqlServerDestination, DestinationFactory},
    types::{ChangeEvent, EventType, ReplicaIdentity},
    DestinationType,
};
use pg_walstream::{Lsn, RowData};
use std::sync::Arc;

/// Test that destination handlers have consistent interfaces
#[tokio::test]
async fn test_destination_handler_interface() {
    let _events = vec![
        create_test_event(),
        create_update_event(),
        create_delete_event(),
    ];

    #[cfg(feature = "mysql")]
    {
        let mut destination = DestinationFactory::create(&DestinationType::MySQL).unwrap();

        // Test execute_sql_batch interface with empty batch (should succeed)
        let empty_batch_result = destination.execute_sql_batch_with_hook(&[], None).await;
        assert!(empty_batch_result.is_ok());

        // Test execute_sql_batch with SQL that will fail due to no connection
        let sql_batch_result = destination
            .execute_sql_batch_with_hook(&["INSERT INTO test (id) VALUES (1);".to_string()], None)
            .await;
        // Should fail due to no connection, but not panic
        assert!(sql_batch_result.is_err());

        // Test close method
        let close_result = destination.close().await;
        assert!(close_result.is_ok());
    }

    #[cfg(feature = "sqlserver")]
    {
        let mut destination = DestinationFactory::create(&DestinationType::SqlServer).unwrap();

        // Test execute_sql_batch interface with empty batch (should succeed)
        let empty_batch_result = destination.execute_sql_batch_with_hook(&[], None).await;
        assert!(empty_batch_result.is_ok());

        // Test execute_sql_batch with SQL that will fail due to no connection
        let sql_batch_result = destination
            .execute_sql_batch_with_hook(&["INSERT INTO test (id) VALUES (1);".to_string()], None)
            .await;
        // Should fail due to no connection, but not panic
        assert!(sql_batch_result.is_err());

        // Test close method
        let close_result = destination.close().await;
        assert!(close_result.is_ok());
    }
}

/// Test serialization/deserialization of destination types
#[test]
fn test_destination_type_serialization() {
    use serde_json;

    let mysql_type = DestinationType::MySQL;
    let sqlserver_type = DestinationType::SqlServer;
    let sqlite_type = DestinationType::SQLite;

    // Test serialization
    let mysql_json = serde_json::to_string(&mysql_type).unwrap();
    let sqlserver_json = serde_json::to_string(&sqlserver_type).unwrap();
    let sqlite_json = serde_json::to_string(&sqlite_type).unwrap();

    assert_eq!(mysql_json, "\"MySQL\"");
    assert_eq!(sqlserver_json, "\"SqlServer\"");
    assert_eq!(sqlite_json, "\"SQLite\"");

    // Test deserialization
    let deserialized_mysql: DestinationType = serde_json::from_str(&mysql_json).unwrap();
    let deserialized_sqlserver: DestinationType = serde_json::from_str(&sqlserver_json).unwrap();

    assert_eq!(deserialized_mysql, mysql_type);
    assert_eq!(deserialized_sqlserver, sqlserver_type);
}

/// Test that unsupported destination types return proper errors
#[test]
fn test_unsupported_destination_types() {
    // SQLite is now supported, so it should succeed
    #[cfg(feature = "sqlite")]
    {
        let sqlite_result = DestinationFactory::create(&DestinationType::SQLite);
        assert!(sqlite_result.is_ok());
    }

    // If SQLite feature is not enabled, it should fail
    #[cfg(not(feature = "sqlite"))]
    {
        let sqlite_result = DestinationFactory::create(&DestinationType::SQLite);
        assert!(sqlite_result.is_err());
    }
}

// Helper functions to create test events
fn create_test_event() -> ChangeEvent {
    let data = RowData::from_pairs(vec![
        ("id", serde_json::Value::Number(serde_json::Number::from(1))),
        ("name", serde_json::Value::String("test".to_string())),
        ("active", serde_json::Value::Bool(true)),
    ]);

    ChangeEvent::insert("public", "test_table", 456, data, Lsn::from(100))
}

fn create_update_event() -> ChangeEvent {
    let old_data = RowData::from_pairs(vec![
        ("id", serde_json::Value::Number(serde_json::Number::from(1))),
        ("name", serde_json::Value::String("old_name".to_string())),
    ]);

    let new_data = RowData::from_pairs(vec![
        ("id", serde_json::Value::Number(serde_json::Number::from(1))),
        ("name", serde_json::Value::String("new_name".to_string())),
    ]);

    ChangeEvent::update(
        "public",
        "test_table",
        456,
        Some(old_data),
        new_data,
        ReplicaIdentity::Default,
        vec![Arc::from("id")],
        Lsn::from(300),
    )
}

fn create_delete_event() -> ChangeEvent {
    let old_data = RowData::from_pairs(vec![
        ("id", serde_json::Value::Number(serde_json::Number::from(1))),
        (
            "name",
            serde_json::Value::String("deleted_name".to_string()),
        ),
    ]);

    ChangeEvent::delete(
        "public",
        "test_table",
        456,
        old_data,
        ReplicaIdentity::Default,
        vec![Arc::from("id")],
        Lsn::from(200),
    )
}

fn create_update_event_without_old_data() -> ChangeEvent {
    let new_data = RowData::from_pairs(vec![
        ("id", serde_json::Value::Number(serde_json::Number::from(1))),
        ("name", serde_json::Value::String("new_name".to_string())),
    ]);

    ChangeEvent::update(
        "public",
        "test_table",
        456,
        None, // This simulates REPLICA IDENTITY NOTHING
        new_data,
        ReplicaIdentity::Nothing,
        vec![Arc::from("id")], // fallback key columns
        Lsn::from(300),
    )
}

#[test]
fn test_mysql_destination_update_with_old_data() {
    let _mysql_dest = MySQLDestination::new();
    let update_event = create_update_event();

    // Verify that we have both old_data and new_data using pattern matching
    match &update_event.event_type {
        EventType::Update {
            old_data, new_data, ..
        } => {
            assert!(old_data.is_some());
            assert!(!new_data.is_empty());
        }
        _ => panic!("Expected Update event"),
    }

    // The actual connection test would require a real MySQL instance
    // Here we're testing that the event structure is correct for proper WHERE clause generation
    if let EventType::Update {
        old_data, new_data, ..
    } = &update_event.event_type
    {
        let old_data = old_data.as_ref().unwrap();

        // Verify that old_data contains key information for WHERE clause
        assert!(old_data.get("id").is_some());
        assert_eq!(
            old_data.get("id").unwrap(),
            &serde_json::Value::Number(serde_json::Number::from(1))
        );

        // Verify that new_data contains updated information
        assert!(new_data.get("id").is_some());
        assert!(new_data.get("name").is_some());
        assert_eq!(
            new_data.get("name").unwrap(),
            &serde_json::Value::String("new_name".to_string())
        );
    } else {
        panic!("Expected Update event");
    }
}

#[test]
fn test_mysql_destination_update_without_old_data() {
    let _mysql_dest = MySQLDestination::new();
    let update_event = create_update_event_without_old_data();

    // Verify that we have no old_data (simulating REPLICA IDENTITY NOTHING)
    match &update_event.event_type {
        EventType::Update {
            old_data, new_data, ..
        } => {
            assert!(old_data.is_none());
            assert!(!new_data.is_empty());

            // This tests the fallback behavior when old_data is not available
            assert!(new_data.get("id").is_some());
            assert!(new_data.get("name").is_some());
        }
        _ => panic!("Expected Update event"),
    }
}

#[test]
fn test_sqlserver_destination_update_with_old_data() {
    let _sqlserver_dest = SqlServerDestination::new();
    let update_event = create_update_event();

    // Verify that we have both old_data and new_data
    match &update_event.event_type {
        EventType::Update {
            old_data, new_data, ..
        } => {
            assert!(old_data.is_some());
            assert!(!new_data.is_empty());

            // Similar to MySQL test, verify event structure
            let old_data = old_data.as_ref().unwrap();

            // Verify that old_data contains key information for WHERE clause
            assert!(old_data.get("id").is_some());
            assert_eq!(
                old_data.get("id").unwrap(),
                &serde_json::Value::Number(serde_json::Number::from(1))
            );

            // Verify that new_data contains updated information
            assert!(new_data.get("id").is_some());
            assert!(new_data.get("name").is_some());
            assert_eq!(
                new_data.get("name").unwrap(),
                &serde_json::Value::String("new_name".to_string())
            );
        }
        _ => panic!("Expected Update event"),
    }
}

// Test to verify that DELETE operations properly use old_data
#[test]
fn test_delete_event_uses_old_data() {
    let delete_event = create_delete_event();

    // DELETE operations should have old_data for WHERE clause
    match &delete_event.event_type {
        EventType::Delete { old_data, .. } => {
            assert!(old_data.get("id").is_some());
            assert!(old_data.get("name").is_some());
            assert_eq!(
                old_data.get("name").unwrap(),
                &serde_json::Value::String("deleted_name".to_string())
            );
        }
        _ => panic!("Expected Delete event"),
    }
}
