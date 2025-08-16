use pg2any_lib::types::{ChangeEvent, EventType, ReplicaIdentity};
use serde_json::Value;
use std::collections::HashMap;

#[tokio::test]
async fn test_replica_identity_default_with_primary_key() {
    let mut new_data = HashMap::new();
    new_data.insert("id".to_string(), Value::String("123".to_string()));
    new_data.insert("name".to_string(), Value::String("John".to_string()));

    let mut old_data = HashMap::new();
    old_data.insert("id".to_string(), Value::String("123".to_string()));
    old_data.insert("name".to_string(), Value::String("Jane".to_string()));

    let event = ChangeEvent::update(
        "public".to_string(),
        "users".to_string(),
        12345, // relation_oid
        Some(old_data),
        new_data,
        ReplicaIdentity::Default,
        vec!["id".to_string()], // Primary key column
    );

    // Test the WHERE clause generation directly
    if let Some(old_data) = get_old_data(&event) {
        let key_columns = event.get_key_columns().unwrap();

        // Should only use primary key column from key_columns
        assert_eq!(key_columns.len(), 1);
        assert_eq!(key_columns[0], "id");
        assert!(old_data.contains_key("id"));
        assert!(old_data.contains_key("name")); // old_data has both, but key_columns should only have id
    }
}

#[tokio::test]
async fn test_replica_identity_full() {
    let mut new_data = HashMap::new();
    new_data.insert("id".to_string(), Value::String("123".to_string()));
    new_data.insert("name".to_string(), Value::String("John".to_string()));
    new_data.insert(
        "email".to_string(),
        Value::String("john@example.com".to_string()),
    );

    let mut old_data = HashMap::new();
    old_data.insert("id".to_string(), Value::String("123".to_string()));
    old_data.insert("name".to_string(), Value::String("Jane".to_string()));
    old_data.insert(
        "email".to_string(),
        Value::String("jane@example.com".to_string()),
    );

    let event = ChangeEvent::update(
        "public".to_string(),
        "users".to_string(),
        12345, // relation_oid
        Some(old_data),
        new_data,
        ReplicaIdentity::Full,
        vec!["id".to_string(), "name".to_string(), "email".to_string()], // All columns
    );

    // Test the key columns for FULL replica identity
    let key_columns = event.get_key_columns().unwrap();

    // Should use all columns for FULL replica identity
    assert_eq!(key_columns.len(), 3);
    assert!(key_columns.contains(&"id".to_string()));
    assert!(key_columns.contains(&"name".to_string()));
    assert!(key_columns.contains(&"email".to_string()));
}

#[tokio::test]
async fn test_replica_identity_index() {
    let mut new_data = HashMap::new();
    new_data.insert("id".to_string(), Value::String("123".to_string()));
    new_data.insert("name".to_string(), Value::String("John".to_string()));
    new_data.insert(
        "email".to_string(),
        Value::String("john@example.com".to_string()),
    );

    let mut old_data = HashMap::new();
    old_data.insert("id".to_string(), Value::String("123".to_string()));
    old_data.insert(
        "email".to_string(),
        Value::String("jane@example.com".to_string()),
    );

    let event = ChangeEvent::update(
        "public".to_string(),
        "users".to_string(),
        12345, // relation_oid
        Some(old_data),
        new_data,
        ReplicaIdentity::Index,
        vec!["id".to_string(), "email".to_string()], // Index columns
    );

    // Test the key columns for INDEX replica identity
    let key_columns = event.get_key_columns().unwrap();

    // Should use only index columns
    assert_eq!(key_columns.len(), 2);
    assert!(key_columns.contains(&"id".to_string()));
    assert!(key_columns.contains(&"email".to_string()));
    assert!(!key_columns.contains(&"name".to_string()));
}

#[tokio::test]
async fn test_replica_identity_nothing() {
    let mut new_data = HashMap::new();
    new_data.insert("id".to_string(), Value::String("123".to_string()));
    new_data.insert("name".to_string(), Value::String("John".to_string()));

    let event = ChangeEvent::update(
        "public".to_string(),
        "users".to_string(),
        12345, // relation_oid
        None,  // No old data for NOTHING replica identity
        new_data,
        ReplicaIdentity::Nothing,
        vec![], // No key columns
    );

    // Test that NOTHING replica identity has no key columns
    let key_columns = event.get_key_columns().unwrap();
    assert!(key_columns.is_empty());

    // And no old data
    assert!(get_old_data(&event).is_none());
}

#[tokio::test]
async fn test_delete_with_replica_identity_default() {
    let mut old_data = HashMap::new();
    old_data.insert("id".to_string(), Value::String("123".to_string()));
    old_data.insert("name".to_string(), Value::String("Jane".to_string()));

    let event = ChangeEvent::delete(
        "public".to_string(),
        "users".to_string(),
        12345, // relation_oid
        old_data,
        ReplicaIdentity::Default,
        vec!["id".to_string()], // Primary key
    );

    // Test DELETE event structure
    let key_columns = event.get_key_columns().unwrap();
    assert_eq!(key_columns.len(), 1);
    assert_eq!(key_columns[0], "id");

    if let Some(old_data) = get_old_data(&event) {
        assert!(old_data.contains_key("id"));
        assert!(old_data.contains_key("name"));
    }
}

#[tokio::test]
async fn test_delete_with_replica_identity_full() {
    let mut old_data = HashMap::new();
    old_data.insert("id".to_string(), Value::String("123".to_string()));
    old_data.insert("name".to_string(), Value::String("Jane".to_string()));
    old_data.insert(
        "email".to_string(),
        Value::String("jane@example.com".to_string()),
    );

    let event = ChangeEvent::delete(
        "public".to_string(),
        "users".to_string(),
        12345, // relation_oid
        old_data,
        ReplicaIdentity::Full,
        vec!["id".to_string(), "name".to_string(), "email".to_string()], // All columns
    );

    // Test DELETE with FULL replica identity
    let key_columns = event.get_key_columns().unwrap();
    assert_eq!(key_columns.len(), 3);
    assert!(key_columns.contains(&"id".to_string()));
    assert!(key_columns.contains(&"name".to_string()));
    assert!(key_columns.contains(&"email".to_string()));
}

fn get_old_data(event: &ChangeEvent) -> Option<HashMap<String, serde_json::Value>> {
    match &event.event_type {
        EventType::Update { old_data, .. } => old_data.clone(),
        EventType::Delete { old_data, .. } => Some(old_data.clone()),
        _ => None,
    }
}
