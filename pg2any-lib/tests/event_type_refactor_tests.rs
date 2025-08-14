use chrono::Utc;
use pg2any_lib::types::{ChangeEvent, EventType};
use serde_json::json;
use std::collections::HashMap;

#[test]
fn test_new_event_type_insert() {
    let mut data = HashMap::new();
    data.insert("id".to_string(), json!(1));
    data.insert("name".to_string(), json!("test"));

    let event = ChangeEvent::insert(
        "public".to_string(),
        "users".to_string(),
        123,
        data.clone(),
    );

    match event.event_type {
        EventType::Insert {
            schema,
            table,
            relation_oid,
            data: event_data,
        } => {
            assert_eq!(schema, "public");
            assert_eq!(table, "users");
            assert_eq!(relation_oid, 123);
            assert_eq!(event_data, data);
        }
        _ => panic!("Expected Insert variant"),
    }
}

#[test]
fn test_new_event_type_update() {
    let mut old_data = HashMap::new();
    old_data.insert("id".to_string(), json!(1));
    old_data.insert("name".to_string(), json!("old_name"));

    let mut new_data = HashMap::new();
    new_data.insert("id".to_string(), json!(1));
    new_data.insert("name".to_string(), json!("new_name"));

    let event = ChangeEvent::update(
        "public".to_string(),
        "users".to_string(),
        123,
        Some(old_data.clone()),
        new_data.clone(),
    );

    match event.event_type {
        EventType::Update {
            schema,
            table,
            relation_oid,
            old_data: event_old_data,
            new_data: event_new_data,
        } => {
            assert_eq!(schema, "public");
            assert_eq!(table, "users");
            assert_eq!(relation_oid, 123);
            assert_eq!(event_old_data, Some(old_data));
            assert_eq!(event_new_data, new_data);
        }
        _ => panic!("Expected Update variant"),
    }
}

#[test]
fn test_new_event_type_delete() {
    let mut old_data = HashMap::new();
    old_data.insert("id".to_string(), json!(1));
    old_data.insert("name".to_string(), json!("deleted_name"));

    let event = ChangeEvent::delete(
        "public".to_string(),
        "users".to_string(),
        123,
        old_data.clone(),
    );

    match event.event_type {
        EventType::Delete {
            schema,
            table,
            relation_oid,
            old_data: event_old_data,
        } => {
            assert_eq!(schema, "public");
            assert_eq!(table, "users");
            assert_eq!(relation_oid, 123);
            assert_eq!(event_old_data, old_data);
        }
        _ => panic!("Expected Delete variant"),
    }
}

#[test]
fn test_new_event_type_begin_commit() {
    let timestamp = Utc::now();
    
    let begin_event = ChangeEvent::begin(12345, timestamp);
    match begin_event.event_type {
        EventType::Begin {
            transaction_id,
            commit_timestamp,
        } => {
            assert_eq!(transaction_id, 12345);
            assert_eq!(commit_timestamp, timestamp);
        }
        _ => panic!("Expected Begin variant"),
    }

    let commit_event = ChangeEvent::commit(12345, timestamp);
    match commit_event.event_type {
        EventType::Commit {
            transaction_id,
            commit_timestamp,
        } => {
            assert_eq!(transaction_id, 12345);
            assert_eq!(commit_timestamp, timestamp);
        }
        _ => panic!("Expected Commit variant"),
    }
}

#[test]
fn test_new_event_type_truncate() {
    let tables = vec![
        "public.users".to_string(),
        "public.orders".to_string(),
    ];
    
    let event = ChangeEvent::truncate(tables.clone());
    match event.event_type {
        EventType::Truncate(event_tables) => {
            assert_eq!(event_tables, tables);
        }
        _ => panic!("Expected Truncate variant"),
    }
}

#[test]
fn test_event_serialization() {
    let mut data = HashMap::new();
    data.insert("id".to_string(), json!(1));
    data.insert("name".to_string(), json!("test"));

    let event = ChangeEvent::insert(
        "public".to_string(),
        "users".to_string(),
        123,
        data,
    );

    // Test that the event can be serialized and deserialized
    let serialized = serde_json::to_string(&event).expect("Failed to serialize event");
    let deserialized: ChangeEvent = serde_json::from_str(&serialized).expect("Failed to deserialize event");

    // Verify they're equal
    match (&event.event_type, &deserialized.event_type) {
        (
            EventType::Insert { schema: s1, table: t1, relation_oid: r1, data: d1 },
            EventType::Insert { schema: s2, table: t2, relation_oid: r2, data: d2 },
        ) => {
            assert_eq!(s1, s2);
            assert_eq!(t1, t2);
            assert_eq!(r1, r2);
            assert_eq!(d1, d2);
        }
        _ => panic!("Serialization/deserialization failed"),
    }
}

#[test]
fn test_event_with_lsn_and_metadata() {
    let mut data = HashMap::new();
    data.insert("id".to_string(), json!(1));

    let mut metadata = HashMap::new();
    metadata.insert("source".to_string(), json!("postgresql"));

    let event = ChangeEvent::insert(
        "public".to_string(),
        "users".to_string(),
        123,
        data,
    )
    .with_lsn("0/12345678".to_string())
    .with_metadata(metadata.clone());

    assert_eq!(event.lsn, Some("0/12345678".to_string()));
    assert_eq!(event.metadata, Some(metadata));
}
