use pg2any_lib::types::{ChangeEvent, EventType, ReplicaIdentity};
use pg_walstream::{Lsn, RowData};
use serde_json::Value;
use std::sync::Arc;

#[tokio::test]
async fn test_replica_identity_default_with_primary_key() {
    let new_data = RowData::from_pairs(vec![
        ("id", Value::String("123".to_string())),
        ("name", Value::String("John".to_string())),
    ]);

    let old_data = RowData::from_pairs(vec![
        ("id", Value::String("123".to_string())),
        ("name", Value::String("Jane".to_string())),
    ]);

    let event = ChangeEvent::update(
        "public",
        "users",
        12345, // relation_oid
        Some(old_data),
        new_data,
        ReplicaIdentity::Default,
        vec![Arc::from("id")], // Primary key column
        Lsn::from(300),
    );

    // Test the WHERE clause generation directly
    if let Some(old_data) = get_old_data(&event) {
        let key_columns = event.get_key_columns().unwrap();

        // Should only use primary key column from key_columns
        assert_eq!(key_columns.len(), 1);
        assert_eq!(key_columns[0].as_ref(), "id");
        assert!(old_data.get("id").is_some());
        assert!(old_data.get("name").is_some()); // old_data has both, but key_columns should only have id
    }
}

#[tokio::test]
async fn test_replica_identity_full() {
    let new_data = RowData::from_pairs(vec![
        ("id", Value::String("123".to_string())),
        ("name", Value::String("John".to_string())),
        ("email", Value::String("john@example.com".to_string())),
    ]);

    let old_data = RowData::from_pairs(vec![
        ("id", Value::String("123".to_string())),
        ("name", Value::String("Jane".to_string())),
        ("email", Value::String("jane@example.com".to_string())),
    ]);

    let event = ChangeEvent::update(
        "public",
        "users",
        12345, // relation_oid
        Some(old_data),
        new_data,
        ReplicaIdentity::Full,
        vec![Arc::from("id"), Arc::from("name"), Arc::from("email")], // All columns
        Lsn::from(300),
    );

    // Test the key columns for FULL replica identity
    let key_columns = event.get_key_columns().unwrap();

    // Should use all columns for FULL replica identity
    assert_eq!(key_columns.len(), 3);
    assert!(key_columns.iter().any(|k| k.as_ref() == "id"));
    assert!(key_columns.iter().any(|k| k.as_ref() == "name"));
    assert!(key_columns.iter().any(|k| k.as_ref() == "email"));
}

#[tokio::test]
async fn test_replica_identity_index() {
    let new_data = RowData::from_pairs(vec![
        ("id", Value::String("123".to_string())),
        ("name", Value::String("John".to_string())),
        ("email", Value::String("john@example.com".to_string())),
    ]);

    let old_data = RowData::from_pairs(vec![
        ("id", Value::String("123".to_string())),
        ("email", Value::String("jane@example.com".to_string())),
    ]);

    let event = ChangeEvent::update(
        "public",
        "users",
        12345, // relation_oid
        Some(old_data),
        new_data,
        ReplicaIdentity::Index,
        vec![Arc::from("id"), Arc::from("email")], // Index columns
        Lsn::from(300),
    );

    // Test the key columns for INDEX replica identity
    let key_columns = event.get_key_columns().unwrap();

    // Should use only index columns
    assert_eq!(key_columns.len(), 2);
    assert!(key_columns.iter().any(|k| k.as_ref() == "id"));
    assert!(key_columns.iter().any(|k| k.as_ref() == "email"));
    assert!(!key_columns.iter().any(|k| k.as_ref() == "name"));
}

#[tokio::test]
async fn test_replica_identity_nothing() {
    let new_data = RowData::from_pairs(vec![
        ("id", Value::String("123".to_string())),
        ("name", Value::String("John".to_string())),
    ]);

    let event = ChangeEvent::update(
        "public",
        "users",
        12345, // relation_oid
        None,  // No old data for NOTHING replica identity
        new_data,
        ReplicaIdentity::Nothing,
        vec![], // No key columns
        Lsn::from(300),
    );

    // Test that NOTHING replica identity has no key columns
    let key_columns = event.get_key_columns().unwrap();
    assert!(key_columns.is_empty());

    // And no old data
    assert!(get_old_data(&event).is_none());
}

#[tokio::test]
async fn test_delete_with_replica_identity_default() {
    let old_data = RowData::from_pairs(vec![
        ("id", Value::String("123".to_string())),
        ("name", Value::String("Jane".to_string())),
    ]);

    let event = ChangeEvent::delete(
        "public",
        "users",
        12345, // relation_oid
        old_data,
        ReplicaIdentity::Default,
        vec![Arc::from("id")], // Primary key
        Lsn::from(200),
    );

    // Test DELETE event structure
    let key_columns = event.get_key_columns().unwrap();
    assert_eq!(key_columns.len(), 1);
    assert_eq!(key_columns[0].as_ref(), "id");

    if let Some(old_data) = get_old_data(&event) {
        assert!(old_data.get("id").is_some());
        assert!(old_data.get("name").is_some());
    }
}

#[tokio::test]
async fn test_delete_with_replica_identity_full() {
    let old_data = RowData::from_pairs(vec![
        ("id", Value::String("123".to_string())),
        ("name", Value::String("Jane".to_string())),
        ("email", Value::String("jane@example.com".to_string())),
    ]);

    let event = ChangeEvent::delete(
        "public",
        "users",
        12345, // relation_oid
        old_data,
        ReplicaIdentity::Full,
        vec![Arc::from("id"), Arc::from("name"), Arc::from("email")], // All columns
        Lsn::from(200),
    );

    // Test DELETE with FULL replica identity
    let key_columns = event.get_key_columns().unwrap();
    assert_eq!(key_columns.len(), 3);
    assert!(key_columns.iter().any(|k| k.as_ref() == "id"));
    assert!(key_columns.iter().any(|k| k.as_ref() == "name"));
    assert!(key_columns.iter().any(|k| k.as_ref() == "email"));
}

fn get_old_data(event: &ChangeEvent) -> Option<RowData> {
    match &event.event_type {
        EventType::Update { old_data, .. } => old_data.clone(),
        EventType::Delete { old_data, .. } => Some(old_data.clone()),
        _ => None,
    }
}
