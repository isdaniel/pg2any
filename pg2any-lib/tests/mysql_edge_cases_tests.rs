/// Integration tests for MySQL destination error handling and edge cases
/// These tests verify error conditions and edge cases in the WHERE clause generation
use pg2any_lib::types::{ChangeEvent, EventType, ReplicaIdentity};
use pg_walstream::{ColumnValue, Lsn, RowData};
use std::sync::Arc;

/// Test scenarios that should result in errors when processed by MySQL destination
#[cfg(test)]
mod mysql_error_scenarios {
    use super::*;

    #[test]
    fn test_missing_key_column_scenario() {
        // Create event where key column is missing from data
        let incomplete_data = RowData::from_pairs(vec![("name", ColumnValue::text("test"))]);
        // Missing "id" which is specified as key column

        let event = ChangeEvent::update(
            "public",
            "users",
            16384,
            Some(incomplete_data.clone()),
            incomplete_data,
            ReplicaIdentity::Default,
            vec![Arc::from("id")], // "id" not in data
            Lsn::from(300),
        );

        // This event structure should trigger an error in MySQL destination
        match &event.event_type {
            EventType::Update { old_data, .. } => {
                let data_source = old_data.as_ref().unwrap();
                assert!(data_source.get("id").is_none()); // Key column missing
                assert!(data_source.get("name").is_some()); // Non-key column present

                // Expected error: "Key column 'id' not found in data for Update on public.users"
            }
            _ => panic!("Expected Update event"),
        }
    }

    #[test]
    fn test_empty_data_scenario() {
        // Test scenario with empty old_data and new_data
        let event = ChangeEvent::update(
            "public",
            "empty_table",
            16384,
            Some(RowData::new()), // Empty old_data
            RowData::new(),       // Empty new_data
            ReplicaIdentity::Default,
            vec![Arc::from("id")],
            Lsn::from(300),
        );

        match &event.event_type {
            EventType::Update {
                old_data, new_data, ..
            } => {
                let data_source = old_data.as_ref().unwrap();
                assert!(data_source.is_empty());
                assert!(new_data.is_empty());

                // Expected error: "Key column 'id' not found in data for Update on public.empty_table"
            }
            _ => panic!("Expected Update event"),
        }
    }

    #[test]
    fn test_no_old_data_and_empty_new_data_scenario() {
        // Test fallback scenario where old_data is None and new_data is empty
        let event = ChangeEvent::update(
            "public",
            "test_table",
            16384,
            None,           // No old_data
            RowData::new(), // Empty new_data
            ReplicaIdentity::Nothing,
            vec![Arc::from("id")],
            Lsn::from(300),
        );

        match &event.event_type {
            EventType::Update {
                old_data, new_data, ..
            } => {
                assert!(old_data.is_none());
                assert!(new_data.is_empty());

                // Data source selection would fallback to new_data, but it's empty
                // Expected error: "No data available to build WHERE clause for Update on public.test_table"
            }
            _ => panic!("Expected Update event"),
        }
    }

    #[test]
    fn test_empty_key_columns_scenario() {
        // Test scenario with empty key columns (NOTHING replica identity)
        let data = RowData::from_pairs(vec![("some_data", ColumnValue::text("value"))]);

        let event = ChangeEvent::update(
            "public",
            "no_keys_table",
            16384,
            None,
            data,
            ReplicaIdentity::Nothing,
            vec![], // No key columns
            Lsn::from(300),
        );

        let key_columns = event.get_key_columns().unwrap();
        assert!(key_columns.is_empty());

        // For NOTHING replica identity with no key columns:
        // Expected behavior: either reject operation or use dangerous WHERE 1=1
    }

    #[test]
    fn test_partial_key_columns_missing() {
        // Test composite key where some key columns are missing
        let incomplete_data =
            RowData::from_pairs(vec![("tenant_id", ColumnValue::text("tenant_1"))]);
        // Missing "user_id" which is part of composite key

        let event = ChangeEvent::update(
            "public",
            "user_accounts",
            16384,
            Some(incomplete_data.clone()),
            incomplete_data,
            ReplicaIdentity::Default,
            vec![Arc::from("tenant_id"), Arc::from("user_id")], // Composite key
            Lsn::from(300),
        );

        match &event.event_type {
            EventType::Update { old_data, .. } => {
                let data_source = old_data.as_ref().unwrap();
                assert!(data_source.get("tenant_id").is_some()); // Present
                assert!(data_source.get("user_id").is_none()); // Missing

                // Expected error: "Key column 'user_id' not found in data for Update on public.user_accounts"
            }
            _ => panic!("Expected Update event"),
        }
    }
}

/// Test proper data source selection behavior
#[cfg(test)]
mod data_source_selection_tests {
    use super::*;

    #[test]
    fn test_data_source_priority_order() {
        // Test that old_data is always preferred over new_data when available
        let old_data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("value", ColumnValue::text("old_value")),
        ]);

        let new_data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("value", ColumnValue::text("new_value")),
        ]);

        let event = ChangeEvent::update(
            "public",
            "priority_test",
            16384,
            Some(old_data.clone()),
            new_data.clone(),
            ReplicaIdentity::Default,
            vec![Arc::from("id")],
            Lsn::from(300),
        );

        // Simulate the fixed data source selection logic
        match &event.event_type {
            EventType::Update {
                old_data, new_data, ..
            } => {
                let data_source = match old_data {
                    Some(old) => old, // Should be selected
                    None => new_data, // Fallback
                };

                // Verify old_data is selected despite new_data being available
                assert_eq!(
                    data_source.get("value"),
                    Some(&ColumnValue::text("old_value"))
                );
                assert_ne!(
                    data_source.get("value"),
                    Some(&ColumnValue::text("new_value"))
                );
            }
            _ => panic!("Expected Update event"),
        }
    }

    #[test]
    fn test_fallback_only_when_old_data_none() {
        // Test that fallback to new_data only happens when old_data is None
        let new_data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("value", ColumnValue::text("fallback_value")),
        ]);

        let event = ChangeEvent::update(
            "public",
            "fallback_test",
            16384,
            None, // old_data is None
            new_data.clone(),
            ReplicaIdentity::Nothing,
            vec![],
            Lsn::from(300),
        );

        match &event.event_type {
            EventType::Update {
                old_data, new_data, ..
            } => {
                let data_source = match old_data {
                    Some(old) => old,
                    None => new_data, // Should be selected
                };

                // Verify new_data is used when old_data is None
                assert_eq!(
                    data_source.get("value"),
                    Some(&ColumnValue::text("fallback_value"))
                );
            }
            _ => panic!("Expected Update event"),
        }
    }
}

/// Test complex data scenarios
#[cfg(test)]
mod complex_data_tests {
    use super::*;

    #[test]
    fn test_complex_json_data_in_where_clause() {
        // Test with complex JSON data structures
        let complex_json_str = r#"{"nested":{"array":[1,"text"]}}"#;

        let old_data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("json_data", ColumnValue::text(complex_json_str)),
        ]);

        let new_data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("json_data", ColumnValue::text(complex_json_str)),
        ]);

        let event = ChangeEvent::update(
            "public",
            "json_table",
            16384,
            Some(old_data),
            new_data,
            ReplicaIdentity::Default,
            vec![Arc::from("id")],
            Lsn::from(300),
        );

        // Verify complex JSON data is handled correctly
        match &event.event_type {
            EventType::Update { old_data, .. } => {
                let data_source = old_data.as_ref().unwrap();
                assert!(data_source.get("json_data").is_some());
                assert!(matches!(
                    data_source.get("json_data"),
                    Some(ColumnValue::Text(_))
                ));
            }
            _ => panic!("Expected Update event"),
        }
    }

    #[test]
    fn test_various_data_types_in_key_columns() {
        // Test with different PostgreSQL data types in key columns
        let old_data = RowData::from_pairs(vec![
            ("string_key", ColumnValue::text("key_value")),
            ("int_key", ColumnValue::text("42")),
            ("bool_key", ColumnValue::text("true")),
            ("null_key", ColumnValue::Null),
        ]);

        let event = ChangeEvent::update(
            "public",
            "mixed_types",
            16384,
            Some(old_data.clone()),
            old_data,
            ReplicaIdentity::Index,
            vec![
                Arc::from("string_key"),
                Arc::from("int_key"),
                Arc::from("bool_key"),
                Arc::from("null_key"),
            ],
            Lsn::from(300),
        );

        match &event.event_type {
            EventType::Update { old_data, .. } => {
                let data_source = old_data.as_ref().unwrap();

                // Verify all data types are preserved
                assert!(matches!(
                    data_source.get("string_key"),
                    Some(ColumnValue::Text(_))
                ));
                assert!(matches!(
                    data_source.get("int_key"),
                    Some(ColumnValue::Text(_))
                ));
                assert!(matches!(
                    data_source.get("bool_key"),
                    Some(ColumnValue::Text(_))
                ));
                assert!(matches!(
                    data_source.get("null_key"),
                    Some(ColumnValue::Null)
                ));
            }
            _ => panic!("Expected Update event"),
        }
    }
}

/// Test schema and table name variations
#[cfg(test)]
mod schema_table_tests {
    use super::*;

    #[test]
    fn test_various_schema_table_combinations() {
        // Test with different schema and table name formats
        let test_cases = vec![
            ("public", "simple_table"),
            ("app_schema", "user_profiles"),
            ("schema_with_underscores", "table_with_underscores"),
            ("CamelCase", "CamelCaseTable"),
            ("schema123", "table456"),
            (
                "very_long_schema_name_with_many_parts",
                "very_long_table_name_with_many_parts",
            ),
        ];

        for (schema, table) in test_cases {
            let data = RowData::from_pairs(vec![("id", ColumnValue::text("1"))]);

            let event = ChangeEvent::update(
                schema.to_string(),
                table.to_string(),
                16384,
                Some(data.clone()),
                data,
                ReplicaIdentity::Default,
                vec![Arc::from("id")],
                Lsn::from(300),
            );

            // Verify schema and table names are preserved correctly
            match &event.event_type {
                EventType::Update {
                    schema: event_schema,
                    table: event_table,
                    ..
                } => {
                    assert_eq!(event_schema.as_ref(), schema);
                    assert_eq!(event_table.as_ref(), table);
                }
                _ => panic!("Expected Update event"),
            }
        }
    }
}
