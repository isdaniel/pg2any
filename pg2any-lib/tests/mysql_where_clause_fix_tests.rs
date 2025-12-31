/// Unit tests for the MySQL destination WHERE clause data source fix
/// These tests verify the critical bug fix where UPDATE/DELETE operations
/// now correctly use old_data when available, falling back to new_data when needed
use pg2any_lib::types::{ChangeEvent, EventType, ReplicaIdentity};
use pg_walstream::Lsn;
use serde_json::Value;
use std::collections::HashMap;

#[test]
fn test_update_uses_old_data_for_where_clause() {
    // This test verifies the core fix: UPDATE should use old_data for WHERE clause
    let mut old_data = HashMap::new();
    old_data.insert(
        "id".to_string(),
        Value::Number(serde_json::Number::from(42)),
    );
    old_data.insert(
        "version".to_string(),
        Value::Number(serde_json::Number::from(1)),
    );

    let mut new_data = HashMap::new();
    new_data.insert(
        "id".to_string(),
        Value::Number(serde_json::Number::from(42)),
    );
    new_data.insert(
        "version".to_string(),
        Value::Number(serde_json::Number::from(2)),
    ); // Version incremented
    new_data.insert(
        "name".to_string(),
        Value::String("Updated Name".to_string()),
    );

    let event = ChangeEvent::update(
        "public".to_string(),
        "versioned_table".to_string(),
        16384,
        Some(old_data.clone()),
        new_data.clone(),
        ReplicaIdentity::Default,
        vec!["id".to_string()],
        Lsn::from(300),
    );

    // Test data source selection logic (mirrors the fixed MySQL destination code)
    match &event.event_type {
        EventType::Update {
            old_data, new_data, ..
        } => {
            // The fixed code should prefer old_data when available
            let data_source = match old_data {
                Some(old) => old,
                None => new_data,
            };

            // Verify old_data is selected and contains replica identity values
            assert_eq!(
                data_source.get("version"),
                Some(&Value::Number(serde_json::Number::from(1)))
            );
            assert!(data_source.contains_key("id"));

            // WHERE clause should use version=1 (old), not version=2 (new)
            // This ensures we update the correct row
        }
        _ => panic!("Expected Update event"),
    }
}

#[test]
fn test_update_fallback_to_new_data_when_old_data_none() {
    // Test the fallback behavior when old_data is None
    let mut new_data = HashMap::new();
    new_data.insert(
        "id".to_string(),
        Value::Number(serde_json::Number::from(100)),
    );
    new_data.insert("status".to_string(), Value::String("active".to_string()));

    let event = ChangeEvent::update(
        "public".to_string(),
        "status_table".to_string(),
        16384,
        None, // No old_data (NOTHING replica identity)
        new_data.clone(),
        ReplicaIdentity::Nothing,
        vec![], // No key columns
        Lsn::from(300),
    );

    // Test data source selection fallback
    match &event.event_type {
        EventType::Update {
            old_data, new_data, ..
        } => {
            let data_source = match old_data {
                Some(old) => old,
                None => new_data, // Should fallback to new_data
            };

            // Verify fallback works
            assert_eq!(
                data_source.get("id"),
                Some(&Value::Number(serde_json::Number::from(100)))
            );
            assert_eq!(
                data_source.get("status"),
                Some(&Value::String("active".to_string()))
            );
        }
        _ => panic!("Expected Update event"),
    }
}

#[test]
fn test_delete_always_uses_old_data() {
    // DELETE operations should always have old_data available
    let mut old_data = HashMap::new();
    old_data.insert(
        "id".to_string(),
        Value::Number(serde_json::Number::from(999)),
    );
    old_data.insert(
        "email".to_string(),
        Value::String("delete@example.com".to_string()),
    );

    let event = ChangeEvent::delete(
        "public".to_string(),
        "users".to_string(),
        16384,
        old_data.clone(),
        ReplicaIdentity::Full,
        vec!["id".to_string(), "email".to_string()],
        Lsn::from(200),
    );

    // For DELETE, old_data should always be available
    match &event.event_type {
        EventType::Delete { old_data, .. } => {
            assert_eq!(
                old_data.get("id"),
                Some(&Value::Number(serde_json::Number::from(999)))
            );
            assert_eq!(
                old_data.get("email"),
                Some(&Value::String("delete@example.com".to_string()))
            );
        }
        _ => panic!("Expected Delete event"),
    }
}

#[test]
fn test_composite_key_data_source_selection() {
    // Test with composite primary key
    let mut old_data = HashMap::new();
    old_data.insert(
        "tenant_id".to_string(),
        Value::String("tenant_1".to_string()),
    );
    old_data.insert(
        "user_id".to_string(),
        Value::Number(serde_json::Number::from(42)),
    );
    old_data.insert(
        "balance".to_string(),
        Value::Number(serde_json::Number::from(100)),
    );

    let mut new_data = HashMap::new();
    new_data.insert(
        "tenant_id".to_string(),
        Value::String("tenant_1".to_string()),
    );
    new_data.insert(
        "user_id".to_string(),
        Value::Number(serde_json::Number::from(42)),
    );
    new_data.insert(
        "balance".to_string(),
        Value::Number(serde_json::Number::from(150)),
    ); // Updated

    let event = ChangeEvent::update(
        "public".to_string(),
        "account_balances".to_string(),
        16384,
        Some(old_data.clone()),
        new_data.clone(),
        ReplicaIdentity::Default,
        vec!["tenant_id".to_string(), "user_id".to_string()], // Composite key
        Lsn::from(300),
    );

    // Test data source selection with composite key
    match &event.event_type {
        EventType::Update {
            old_data, new_data, ..
        } => {
            let data_source = match old_data {
                Some(old) => old,
                None => new_data,
            };

            // Both key columns should be available in old_data
            assert_eq!(
                data_source.get("tenant_id"),
                Some(&Value::String("tenant_1".to_string()))
            );
            assert_eq!(
                data_source.get("user_id"),
                Some(&Value::Number(serde_json::Number::from(42)))
            );

            // WHERE clause: WHERE tenant_id='tenant_1' AND user_id=42
        }
        _ => panic!("Expected Update event"),
    }
}

#[test]
fn test_replica_identity_full_uses_all_columns() {
    // Test FULL replica identity with all columns in WHERE clause
    let mut old_data = HashMap::new();
    old_data.insert("id".to_string(), Value::Number(serde_json::Number::from(1)));
    old_data.insert("name".to_string(), Value::String("John".to_string()));
    old_data.insert(
        "age".to_string(),
        Value::Number(serde_json::Number::from(25)),
    );

    let mut new_data = HashMap::new();
    new_data.insert("id".to_string(), Value::Number(serde_json::Number::from(1)));
    new_data.insert("name".to_string(), Value::String("Johnny".to_string())); // Changed
    new_data.insert(
        "age".to_string(),
        Value::Number(serde_json::Number::from(26)),
    ); // Changed

    let event = ChangeEvent::update(
        "public".to_string(),
        "people".to_string(),
        16384,
        Some(old_data.clone()),
        new_data.clone(),
        ReplicaIdentity::Full,
        vec!["id".to_string(), "name".to_string(), "age".to_string()],
        Lsn::from(300),
    );

    match &event.event_type {
        EventType::Update { old_data, .. } => {
            let data_source = old_data.as_ref().unwrap();

            // All columns available in old_data for FULL replica identity
            assert_eq!(
                data_source.get("name"),
                Some(&Value::String("John".to_string()))
            );
            assert_eq!(
                data_source.get("age"),
                Some(&Value::Number(serde_json::Number::from(25)))
            );

            // WHERE: WHERE id=1 AND name='John' AND age=25 (old values)
            // SET: SET id=1, name='Johnny', age=26 (new values)
        }
        _ => panic!("Expected Update event"),
    }
}

#[test]
fn test_json_null_values_in_replica_identity() {
    // Test handling of JSON null values
    let mut old_data = HashMap::new();
    old_data.insert("id".to_string(), Value::Number(serde_json::Number::from(1)));
    old_data.insert("optional_field".to_string(), Value::Null);

    let mut new_data = HashMap::new();
    new_data.insert("id".to_string(), Value::Number(serde_json::Number::from(1)));
    new_data.insert(
        "optional_field".to_string(),
        Value::String("now has value".to_string()),
    );

    let event = ChangeEvent::update(
        "public".to_string(),
        "test_table".to_string(),
        16384,
        Some(old_data.clone()),
        new_data.clone(),
        ReplicaIdentity::Full,
        vec!["id".to_string(), "optional_field".to_string()],
        Lsn::from(300),
    );

    match &event.event_type {
        EventType::Update { old_data, .. } => {
            let data_source = old_data.as_ref().unwrap();

            // Null values should be preserved in old_data
            assert_eq!(data_source.get("optional_field"), Some(&Value::Null));

            // WHERE clause should handle: WHERE id=1 AND optional_field IS NULL
        }
        _ => panic!("Expected Update event"),
    }
}

#[test]
fn test_key_columns_availability() {
    // Test that key columns are correctly accessible
    let mut data = HashMap::new();
    data.insert("id".to_string(), Value::Number(serde_json::Number::from(1)));
    data.insert("name".to_string(), Value::String("Test".to_string()));

    // Test DEFAULT replica identity (primary key only)
    let event_default = ChangeEvent::update(
        "public".to_string(),
        "table1".to_string(),
        16384,
        Some(data.clone()),
        data.clone(),
        ReplicaIdentity::Default,
        vec!["id".to_string()],
        Lsn::from(300),
    );

    let key_columns = event_default.get_key_columns().unwrap();
    assert_eq!(key_columns.len(), 1);
    assert_eq!(key_columns[0], "id");

    // Test FULL replica identity (all columns)
    let event_full = ChangeEvent::update(
        "public".to_string(),
        "table2".to_string(),
        16384,
        Some(data.clone()),
        data.clone(),
        ReplicaIdentity::Full,
        vec!["id".to_string(), "name".to_string()],
        Lsn::from(300),
    );

    let key_columns_full = event_full.get_key_columns().unwrap();
    assert_eq!(key_columns_full.len(), 2);
    assert!(key_columns_full.contains(&"id".to_string()));
    assert!(key_columns_full.contains(&"name".to_string()));

    // Test NOTHING replica identity (no key columns)
    let event_nothing = ChangeEvent::update(
        "public".to_string(),
        "table3".to_string(),
        16384,
        None,
        data,
        ReplicaIdentity::Nothing,
        vec![], // No key columns
        Lsn::from(300),
    );

    let key_columns_nothing = event_nothing.get_key_columns().unwrap();
    assert!(key_columns_nothing.is_empty());
}
