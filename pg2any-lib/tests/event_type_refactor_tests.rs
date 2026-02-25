use chrono::Utc;
use pg2any_lib::types::{ChangeEvent, EventType, ReplicaIdentity};
use pg_walstream::{ColumnValue, Lsn, RowData};
use std::sync::Arc;

#[test]
fn test_new_event_type_insert() {
    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("test")),
    ]);

    let event = ChangeEvent::insert("public", "users", 123, data.clone(), Lsn::from(100));

    match event.event_type {
        EventType::Insert {
            schema,
            table,
            relation_oid,
            data: event_data,
        } => {
            assert_eq!(schema.as_ref(), "public");
            assert_eq!(table.as_ref(), "users");
            assert_eq!(relation_oid, 123);
            assert_eq!(event_data, data);
        }
        _ => panic!("Expected Insert variant"),
    }
}

#[test]
fn test_new_event_type_update() {
    let old_data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("old_name")),
    ]);

    let new_data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("new_name")),
    ]);

    let event = ChangeEvent::update(
        "public",
        "users",
        123,
        Some(old_data.clone()),
        new_data.clone(),
        ReplicaIdentity::Default,
        vec![Arc::from("id")],
        Lsn::from(300),
    );

    match event.event_type {
        EventType::Update {
            schema,
            table,
            relation_oid,
            old_data: event_old_data,
            new_data: event_new_data,
            replica_identity,
            key_columns,
        } => {
            assert_eq!(schema.as_ref(), "public");
            assert_eq!(table.as_ref(), "users");
            assert_eq!(relation_oid, 123);
            assert_eq!(event_old_data, Some(old_data));
            assert_eq!(event_new_data, new_data);
            assert_eq!(replica_identity, ReplicaIdentity::Default);
            assert_eq!(key_columns.len(), 1);
            assert_eq!(key_columns[0].as_ref(), "id");
        }
        _ => panic!("Expected Update variant"),
    }
}

#[test]
fn test_new_event_type_delete() {
    let old_data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("deleted_name")),
    ]);

    let event = ChangeEvent::delete(
        "public",
        "users",
        123,
        old_data.clone(),
        ReplicaIdentity::Default,
        vec![Arc::from("id")],
        Lsn::from(200),
    );

    match event.event_type {
        EventType::Delete {
            schema,
            table,
            relation_oid,
            old_data: event_old_data,
            replica_identity,
            key_columns,
        } => {
            assert_eq!(schema.as_ref(), "public");
            assert_eq!(table.as_ref(), "users");
            assert_eq!(relation_oid, 123);
            assert_eq!(event_old_data, old_data);
            assert_eq!(replica_identity, ReplicaIdentity::Default);
            assert_eq!(key_columns.len(), 1);
            assert_eq!(key_columns[0].as_ref(), "id");
        }
        _ => panic!("Expected Delete variant"),
    }
}

#[test]
fn test_new_event_type_begin_commit() {
    let timestamp = Utc::now();

    let begin_event = ChangeEvent::begin(12345, Lsn::from(0), timestamp, Lsn::from(100));
    match begin_event.event_type {
        EventType::Begin {
            transaction_id,
            commit_timestamp,
            ..
        } => {
            assert_eq!(transaction_id, 12345);
            assert_eq!(commit_timestamp, timestamp);
        }
        _ => panic!("Expected Begin variant"),
    }

    let commit_event =
        ChangeEvent::commit(timestamp, Lsn::from(100), Lsn::from(110), Lsn::from(110));
    match commit_event.event_type {
        EventType::Commit {
            commit_timestamp, ..
        } => {
            assert_eq!(commit_timestamp, timestamp);
        }
        _ => panic!("Expected Commit variant"),
    }
}

#[test]
fn test_new_event_type_truncate() {
    let tables: Vec<Arc<str>> = vec![Arc::from("public.users"), Arc::from("public.orders")];

    let event = ChangeEvent::truncate(tables.clone(), Lsn::from(400));
    match event.event_type {
        EventType::Truncate(event_tables) => {
            assert_eq!(event_tables, tables);
        }
        _ => panic!("Expected Truncate variant"),
    }
}

#[test]
fn test_event_serialization() {
    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("test")),
    ]);

    let event = ChangeEvent::insert("public", "users", 123, data, Lsn::from(100));

    // Test that the event can be serialized and deserialized
    let serialized = serde_json::to_string(&event).expect("Failed to serialize event");
    let deserialized: ChangeEvent =
        serde_json::from_str(&serialized).expect("Failed to deserialize event");

    // Verify they're equal
    match (&event.event_type, &deserialized.event_type) {
        (
            EventType::Insert {
                schema: s1,
                table: t1,
                relation_oid: r1,
                data: d1,
            },
            EventType::Insert {
                schema: s2,
                table: t2,
                relation_oid: r2,
                data: d2,
            },
        ) => {
            assert_eq!(s1, s2);
            assert_eq!(t1, t2);
            assert_eq!(r1, r2);
            assert_eq!(d1, d2);
        }
        _ => panic!("Serialization/deserialization failed"),
    }
}
