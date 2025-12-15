use chrono::Utc;
use pg2any_lib::{
    destinations::{mysql::MySQLDestination, sqlserver::SqlServerDestination, DestinationFactory},
    types::{ChangeEvent, EventType, ReplicaIdentity},
    DestinationType,
    Transaction,
};
use std::collections::HashMap;

/// Helper function to wrap a single event in a transaction for testing
fn wrap_in_transaction(event: ChangeEvent) -> Transaction {
    let mut tx = Transaction::new(1, Utc::now());
    tx.add_event(event);
    tx
}

/// Test that the factory can create destination instances
#[tokio::test]
async fn test_destination_factory_integration() {
    // Test MySQL creation when feature is enabled
    #[cfg(feature = "mysql")]
    {
        let result = DestinationFactory::create(DestinationType::MySQL);
        assert!(result.is_ok());
        let mut destination = result.unwrap();

        // Test basic operations (without actual database connection)
        // These should fail gracefully without panicking
        let health_result = destination.health_check().await;
        assert!(health_result.is_err());
    }

    // Test SQL Server creation when feature is enabled
    #[cfg(feature = "sqlserver")]
    {
        let result = DestinationFactory::create(DestinationType::SqlServer);
        assert!(result.is_ok());
        let mut destination = result.unwrap();

        let health_result = destination.health_check().await;
        assert!(health_result.is_err());
    }
}

/// Test that destination handlers have consistent interfaces
#[tokio::test]
async fn test_destination_handler_interface() {
    let events = vec![
        create_test_event(),
        create_update_event(),
        create_delete_event(),
    ];

    #[cfg(feature = "mysql")]
    {
        let mut destination = DestinationFactory::create(DestinationType::MySQL).unwrap();

        // Test single event processing interface
        for event in &events {
            let event_result = destination.process_transaction(&wrap_in_transaction(event.clone())).await;
            // Should fail due to no connection, but not panic
            assert!(event_result.is_err());
        }

        // Test close method
        let close_result = destination.close().await;
        assert!(close_result.is_ok());
    }

    #[cfg(feature = "sqlserver")]
    {
        let mut destination = DestinationFactory::create(DestinationType::SqlServer).unwrap();

        // Test single event processing interface
        for event in &events {
            let event_result = destination.process_transaction(&wrap_in_transaction(event.clone())).await;
            // Should fail due to no connection, but not panic
            assert!(event_result.is_err());
        }

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
    let postgres_type = DestinationType::PostgreSQL;
    let sqlite_type = DestinationType::SQLite;

    // Test serialization
    let mysql_json = serde_json::to_string(&mysql_type).unwrap();
    let sqlserver_json = serde_json::to_string(&sqlserver_type).unwrap();
    let postgres_json = serde_json::to_string(&postgres_type).unwrap();
    let sqlite_json = serde_json::to_string(&sqlite_type).unwrap();

    assert_eq!(mysql_json, "\"MySQL\"");
    assert_eq!(sqlserver_json, "\"SqlServer\"");
    assert_eq!(postgres_json, "\"PostgreSQL\"");
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
    let postgres_result = DestinationFactory::create(DestinationType::PostgreSQL);
    assert!(postgres_result.is_err());

    // SQLite is now supported, so it should succeed
    #[cfg(feature = "sqlite")]
    {
        let sqlite_result = DestinationFactory::create(DestinationType::SQLite);
        assert!(sqlite_result.is_ok());
    }

    // If SQLite feature is not enabled, it should fail
    #[cfg(not(feature = "sqlite"))]
    {
        let sqlite_result = DestinationFactory::create(DestinationType::SQLite);
        assert!(sqlite_result.is_err());
    }

    // Verify error messages contain helpful information
    if let Err(error) = postgres_result {
        let error_msg = error.to_string();
        assert!(error_msg.contains("PostgreSQL"));
        assert!(error_msg.contains("not supported") || error_msg.contains("not enabled"));
    }
}

// Helper functions to create test events
fn create_test_event() -> ChangeEvent {
    let mut data = HashMap::new();
    data.insert(
        "id".to_string(),
        serde_json::Value::Number(serde_json::Number::from(1)),
    );
    data.insert(
        "name".to_string(),
        serde_json::Value::String("test".to_string()),
    );
    data.insert("active".to_string(), serde_json::Value::Bool(true));

    ChangeEvent::insert("public".to_string(), "test_table".to_string(), 456, data)
}

fn create_update_event() -> ChangeEvent {
    let mut old_data = HashMap::new();
    old_data.insert(
        "id".to_string(),
        serde_json::Value::Number(serde_json::Number::from(1)),
    );
    old_data.insert(
        "name".to_string(),
        serde_json::Value::String("old_name".to_string()),
    );

    let mut new_data = HashMap::new();
    new_data.insert(
        "id".to_string(),
        serde_json::Value::Number(serde_json::Number::from(1)),
    );
    new_data.insert(
        "name".to_string(),
        serde_json::Value::String("new_name".to_string()),
    );

    ChangeEvent::update(
        "public".to_string(),
        "test_table".to_string(),
        456,
        Some(old_data),
        new_data,
        ReplicaIdentity::Default,
        vec!["id".to_string()],
    )
}

fn create_delete_event() -> ChangeEvent {
    let mut old_data = HashMap::new();
    old_data.insert(
        "id".to_string(),
        serde_json::Value::Number(serde_json::Number::from(1)),
    );
    old_data.insert(
        "name".to_string(),
        serde_json::Value::String("deleted_name".to_string()),
    );

    ChangeEvent::delete(
        "public".to_string(),
        "test_table".to_string(),
        456,
        old_data,
        ReplicaIdentity::Default,
        vec!["id".to_string()],
    )
}

fn create_update_event_without_old_data() -> ChangeEvent {
    let mut new_data = HashMap::new();
    new_data.insert(
        "id".to_string(),
        serde_json::Value::Number(serde_json::Number::from(1)),
    );
    new_data.insert(
        "name".to_string(),
        serde_json::Value::String("new_name".to_string()),
    );

    ChangeEvent::update(
        "public".to_string(),
        "test_table".to_string(),
        456,
        None, // This simulates REPLICA IDENTITY NOTHING
        new_data,
        ReplicaIdentity::Nothing,
        vec!["id".to_string()], // fallback key columns
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
        assert!(old_data.contains_key("id"));
        assert_eq!(
            old_data.get("id").unwrap(),
            &serde_json::Value::Number(serde_json::Number::from(1))
        );

        // Verify that new_data contains updated information
        assert!(new_data.contains_key("id"));
        assert!(new_data.contains_key("name"));
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
            assert!(new_data.contains_key("id"));
            assert!(new_data.contains_key("name"));
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
            assert!(old_data.contains_key("id"));
            assert_eq!(
                old_data.get("id").unwrap(),
                &serde_json::Value::Number(serde_json::Number::from(1))
            );

            // Verify that new_data contains updated information
            assert!(new_data.contains_key("id"));
            assert!(new_data.contains_key("name"));
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
            assert!(old_data.contains_key("id"));
            assert!(old_data.contains_key("name"));
            assert_eq!(
                old_data.get("name").unwrap(),
                &serde_json::Value::String("deleted_name".to_string())
            );
        }
        _ => panic!("Expected Delete event"),
    }
}
