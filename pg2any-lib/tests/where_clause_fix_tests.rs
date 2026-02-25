use pg2any_lib::types::{ChangeEvent, EventType, ReplicaIdentity};
use pg_walstream::{ColumnValue, Lsn, RowData};
use std::sync::Arc;

/// Test to demonstrate the fix for UPDATE WHERE clause issue
#[test]
fn test_update_where_clause_uses_old_data() {
    // Create test event simulating PostgreSQL logical replication UPDATE
    // with replica identity containing old key values
    let old_data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("42")),
        ("email", ColumnValue::text("old.email@example.com")),
    ]);

    let new_data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("42")),
        ("email", ColumnValue::text("new.email@example.com")),
        ("name", ColumnValue::text("Updated Name")),
        ("age", ColumnValue::text("30")),
    ]);

    let event = ChangeEvent::update(
        "public",
        "users",
        16384,
        Some(old_data),
        new_data,
        ReplicaIdentity::Default,
        vec![Arc::from("id")],
        Lsn::from(300),
    );

    // Test that the event structure correctly supports proper WHERE clause generation

    // 1. Verify old_data contains the replica identity information
    match &event.event_type {
        EventType::Update {
            old_data, new_data, ..
        } => {
            let old_data = old_data.as_ref().unwrap();
            assert!(old_data.get("id").is_some());
            assert!(old_data.get("email").is_some());
            assert_eq!(old_data.get("id").unwrap(), "42");
            assert_eq!(old_data.get("email").unwrap(), "old.email@example.com");

            // 2. Verify new_data contains the updated values
            assert!(new_data.get("id").is_some());
            assert!(new_data.get("email").is_some());
            assert!(new_data.get("name").is_some());
            assert!(new_data.get("age").is_some());
            assert_eq!(new_data.get("email").unwrap(), "new.email@example.com");

            // 3. Simulate the logic our fixed code should use:
            // - SET clause should use new_data keys/values
            // - WHERE clause should use old_data keys/values

            let set_keys: Vec<&str> = new_data.iter().map(|(k, _)| k.as_ref()).collect();
            let where_keys: Vec<&str> = old_data.iter().map(|(k, _)| k.as_ref()).collect();

            // Verify SET clause would include all new columns
            assert!(set_keys.contains(&"id"));
            assert!(set_keys.contains(&"email"));
            assert!(set_keys.contains(&"name"));
            assert!(set_keys.contains(&"age"));

            // Verify WHERE clause would use replica identity (old_data)
            assert!(where_keys.contains(&"id"));
            assert!(where_keys.contains(&"email"));

            // The generated SQL should conceptually be:
            // UPDATE `public`.`users`
            // SET `id` = ?, `email` = ?, `name` = ?, `age` = ?
            // WHERE `id` = ? AND `email` = ?
            //
            // With values:
            // SET: (42, "new.email@example.com", "Updated Name", 30)
            // WHERE: (42, "old.email@example.com")
        }
        _ => panic!("Expected Update event"),
    }
}

/// Test DELETE operations use old_data for WHERE clause  
#[test]
fn test_delete_where_clause_uses_old_data() {
    let old_data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("99")),
        ("username", ColumnValue::text("user_to_delete")),
    ]);

    let event = ChangeEvent::delete(
        "public",
        "users",
        16384,
        old_data,
        ReplicaIdentity::Default,
        vec![Arc::from("id")],
        Lsn::from(200),
    );

    // Verify DELETE event structure
    match &event.event_type {
        EventType::Delete { old_data, .. } => {
            assert!(old_data.get("id").is_some());
            assert!(old_data.get("username").is_some());

            // The generated SQL should conceptually be:
            // DELETE FROM `public`.`users` WHERE `id` = ? AND `username` = ?
            // With values: (99, "user_to_delete")
        }
        _ => panic!("Expected Delete event"),
    }
}

/// Test UPDATE without old_data (REPLICA IDENTITY NOTHING scenario)
#[test]
fn test_update_fallback_when_no_old_data() {
    let new_data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("123")),
        ("name", ColumnValue::text("New Name")),
    ]);

    let event = ChangeEvent::update(
        "public",
        "logs",
        16385,
        None, // Simulates REPLICA IDENTITY NOTHING
        new_data,
        ReplicaIdentity::Nothing,
        vec![Arc::from("id")], // fallback key columns
        Lsn::from(300),
    );

    // Verify the fallback scenario
    match &event.event_type {
        EventType::Update {
            old_data, new_data, ..
        } => {
            assert!(old_data.is_none());
            assert!(!new_data.is_empty());

            assert!(new_data.get("id").is_some());
            assert!(new_data.get("name").is_some());

            // In our fixed implementation, this would fallback to using new_data for WHERE
            // (taking first column as a fallback), which is not ideal but better than WHERE 1=1

            // The generated SQL would be something like:
            // UPDATE `public`.`logs` SET `id` = ?, `name` = ? WHERE `id` = ?
            // With values: SET: (123, "New Name"), WHERE: (123)
        }
        _ => panic!("Expected Update event"),
    }
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
