use pg2any_lib::{
    destinations::DestinationFactory,
    types::{ChangeEvent, EventType},
    DestinationType,
};
use std::collections::HashMap;
use chrono::Utc;

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
    let events = vec![create_test_event(), create_update_event(), create_delete_event()];
    
    #[cfg(feature = "mysql")]
    {
        let mut destination = DestinationFactory::create(DestinationType::MySQL).unwrap();
        
        // Test batch processing interface
        let batch_result = destination.process_batch(&events).await;
        // Should fail due to no connection, but not panic
        assert!(batch_result.is_err());
        
        // Test close method
        let close_result = destination.close().await;
        assert!(close_result.is_ok());
    }

    #[cfg(feature = "sqlserver")]
    {
        let mut destination = DestinationFactory::create(DestinationType::SqlServer).unwrap();
        
        // Test batch processing interface
        let batch_result = destination.process_batch(&events).await;
        // Should fail due to no connection, but not panic
        assert!(batch_result.is_err());
        
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
    
    let sqlite_result = DestinationFactory::create(DestinationType::SQLite);
    assert!(sqlite_result.is_err());
    
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
    data.insert("id".to_string(), serde_json::Value::Number(serde_json::Number::from(1)));
    data.insert("name".to_string(), serde_json::Value::String("test".to_string()));
    data.insert("active".to_string(), serde_json::Value::Bool(true));
    
    ChangeEvent {
        event_type: EventType::Insert,
        transaction_id: Some(123),
        commit_timestamp: Some(Utc::now()),
        schema_name: Some("public".to_string()),
        table_name: Some("test_table".to_string()),
        relation_oid: Some(456),
        old_data: None,
        new_data: Some(data),
        lsn: Some("0/1234567".to_string()),
        metadata: None,
    }
}

fn create_update_event() -> ChangeEvent {
    let mut old_data = HashMap::new();
    old_data.insert("id".to_string(), serde_json::Value::Number(serde_json::Number::from(1)));
    old_data.insert("name".to_string(), serde_json::Value::String("old_name".to_string()));
    
    let mut new_data = HashMap::new();
    new_data.insert("id".to_string(), serde_json::Value::Number(serde_json::Number::from(1)));
    new_data.insert("name".to_string(), serde_json::Value::String("new_name".to_string()));
    
    ChangeEvent {
        event_type: EventType::Update,
        transaction_id: Some(124),
        commit_timestamp: Some(Utc::now()),
        schema_name: Some("public".to_string()),
        table_name: Some("test_table".to_string()),
        relation_oid: Some(456),
        old_data: Some(old_data),
        new_data: Some(new_data),
        lsn: Some("0/1234568".to_string()),
        metadata: None,
    }
}

fn create_delete_event() -> ChangeEvent {
    let mut old_data = HashMap::new();
    old_data.insert("id".to_string(), serde_json::Value::Number(serde_json::Number::from(1)));
    old_data.insert("name".to_string(), serde_json::Value::String("deleted_name".to_string()));
    
    ChangeEvent {
        event_type: EventType::Delete,
        transaction_id: Some(125),
        commit_timestamp: Some(Utc::now()),
        schema_name: Some("public".to_string()),
        table_name: Some("test_table".to_string()),
        relation_oid: Some(456),
        old_data: Some(old_data),
        new_data: None,
        lsn: Some("0/1234569".to_string()),
        metadata: None,
    }
}
