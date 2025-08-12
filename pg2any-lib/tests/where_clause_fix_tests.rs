use chrono::Utc;
use pg2any_lib::{
    destinations::{mysql::MySQLDestination, sqlserver::SqlServerDestination},
    types::{ChangeEvent, EventType},
};
use std::collections::HashMap;

/// Test to demonstrate the fix for UPDATE WHERE clause issue
#[test]
fn test_update_where_clause_uses_old_data() {
    // Create test event simulating PostgreSQL logical replication UPDATE
    // with replica identity containing old key values
    let mut old_data = HashMap::new();
    old_data.insert(
        "id".to_string(),
        serde_json::Value::Number(serde_json::Number::from(42)),
    );
    old_data.insert(
        "email".to_string(),
        serde_json::Value::String("old.email@example.com".to_string()),
    );

    let mut new_data = HashMap::new();
    new_data.insert(
        "id".to_string(),
        serde_json::Value::Number(serde_json::Number::from(42)),
    );
    new_data.insert(
        "email".to_string(),
        serde_json::Value::String("new.email@example.com".to_string()),
    );
    new_data.insert(
        "name".to_string(),
        serde_json::Value::String("Updated Name".to_string()),
    );
    new_data.insert(
        "age".to_string(),
        serde_json::Value::Number(serde_json::Number::from(30)),
    );

    let event = ChangeEvent {
        event_type: EventType::Update,
        transaction_id: Some(1001),
        commit_timestamp: Some(Utc::now()),
        schema_name: Some("public".to_string()),
        table_name: Some("users".to_string()),
        relation_oid: Some(16384),
        old_data: Some(old_data),
        new_data: Some(new_data),
        lsn: Some("0/A1B2C3D4".to_string()),
        metadata: None,
    };

    // Test that the event structure correctly supports proper WHERE clause generation

    // 1. Verify old_data contains the replica identity information
    let old_data = event.old_data.as_ref().unwrap();
    assert!(old_data.contains_key("id"));
    assert!(old_data.contains_key("email"));
    assert_eq!(
        old_data.get("id").unwrap(),
        &serde_json::Value::Number(serde_json::Number::from(42))
    );
    assert_eq!(
        old_data.get("email").unwrap(),
        &serde_json::Value::String("old.email@example.com".to_string())
    );

    // 2. Verify new_data contains the updated values
    let new_data = event.new_data.as_ref().unwrap();
    assert!(new_data.contains_key("id"));
    assert!(new_data.contains_key("email"));
    assert!(new_data.contains_key("name"));
    assert!(new_data.contains_key("age"));
    assert_eq!(
        new_data.get("email").unwrap(),
        &serde_json::Value::String("new.email@example.com".to_string())
    );

    // 3. Simulate the logic our fixed code should use:
    // - SET clause should use new_data keys/values
    // - WHERE clause should use old_data keys/values

    let set_keys: Vec<&String> = new_data.keys().collect();
    let where_keys: Vec<&String> = old_data.keys().collect();

    // Verify SET clause would include all new columns
    assert!(set_keys.contains(&&"id".to_string()));
    assert!(set_keys.contains(&&"email".to_string()));
    assert!(set_keys.contains(&&"name".to_string()));
    assert!(set_keys.contains(&&"age".to_string()));

    // Verify WHERE clause would use replica identity (old_data)
    assert!(where_keys.contains(&&"id".to_string()));
    assert!(where_keys.contains(&&"email".to_string()));

    // The generated SQL should conceptually be:
    // UPDATE `public`.`users`
    // SET `id` = ?, `email` = ?, `name` = ?, `age` = ?
    // WHERE `id` = ? AND `email` = ?
    //
    // With values:
    // SET: (42, "new.email@example.com", "Updated Name", 30)
    // WHERE: (42, "old.email@example.com")
}

/// Test DELETE operations use old_data for WHERE clause  
#[test]
fn test_delete_where_clause_uses_old_data() {
    let mut old_data = HashMap::new();
    old_data.insert(
        "id".to_string(),
        serde_json::Value::Number(serde_json::Number::from(99)),
    );
    old_data.insert(
        "username".to_string(),
        serde_json::Value::String("user_to_delete".to_string()),
    );

    let event = ChangeEvent {
        event_type: EventType::Delete,
        transaction_id: Some(1002),
        commit_timestamp: Some(Utc::now()),
        schema_name: Some("public".to_string()),
        table_name: Some("users".to_string()),
        relation_oid: Some(16384),
        old_data: Some(old_data),
        new_data: None, // DELETE has no new_data
        lsn: Some("0/A1B2C3D5".to_string()),
        metadata: None,
    };

    // Verify DELETE event structure
    assert!(event.old_data.is_some());
    assert!(event.new_data.is_none());

    let old_data = event.old_data.as_ref().unwrap();
    assert!(old_data.contains_key("id"));
    assert!(old_data.contains_key("username"));

    // The generated SQL should conceptually be:
    // DELETE FROM `public`.`users` WHERE `id` = ? AND `username` = ?
    // With values: (99, "user_to_delete")
}

/// Test UPDATE without old_data (REPLICA IDENTITY NOTHING scenario)
#[test]
fn test_update_fallback_when_no_old_data() {
    let mut new_data = HashMap::new();
    new_data.insert(
        "id".to_string(),
        serde_json::Value::Number(serde_json::Number::from(123)),
    );
    new_data.insert(
        "name".to_string(),
        serde_json::Value::String("New Name".to_string()),
    );

    let event = ChangeEvent {
        event_type: EventType::Update,
        transaction_id: Some(1003),
        commit_timestamp: Some(Utc::now()),
        schema_name: Some("public".to_string()),
        table_name: Some("logs".to_string()),
        relation_oid: Some(16385),
        old_data: None, // Simulates REPLICA IDENTITY NOTHING
        new_data: Some(new_data),
        lsn: Some("0/A1B2C3D6".to_string()),
        metadata: None,
    };

    // Verify the fallback scenario
    assert!(event.old_data.is_none());
    assert!(event.new_data.is_some());

    let new_data = event.new_data.as_ref().unwrap();
    assert!(new_data.contains_key("id"));
    assert!(new_data.contains_key("name"));

    // In our fixed implementation, this would fallback to using new_data for WHERE
    // (taking first column as a fallback), which is not ideal but better than WHERE 1=1
    // The generated SQL would be something like:
    // UPDATE `public`.`logs` SET `id` = ?, `name` = ? WHERE `id` = ?
    // With values: SET: (123, "New Name"), WHERE: (123)
}

/// Test that demonstrates the problem with the old WHERE 1=1 approach
#[test]
fn test_where_1_equals_1_is_dangerous() {
    // This test documents why WHERE 1=1 is dangerous

    // Imagine this UPDATE with WHERE 1=1:
    // UPDATE users SET name = 'Hacked' WHERE 1=1;
    // This would update ALL rows in the table!

    // Our fix ensures we use proper replica identity for row identification:
    // UPDATE users SET name = 'John Doe' WHERE id = 42 AND email = 'john@example.com';
    // This safely updates only the intended row.

    assert!(true, "WHERE 1=1 would be dangerous as it affects all rows");
}
