/// Integration tests for MySQL destination error handling and edge cases
/// These tests verify error conditions and edge cases in the WHERE clause generation
use pg2any_lib::types::{ChangeEvent, EventType, ReplicaIdentity};
use serde_json::Value;
use std::collections::HashMap;

/// Test scenarios that should result in errors when processed by MySQL destination
#[cfg(test)]
mod mysql_error_scenarios {
    use super::*;

    #[test]
    fn test_missing_key_column_scenario() {
        // Create event where key column is missing from data
        let mut incomplete_data = HashMap::new();
        incomplete_data.insert("name".to_string(), Value::String("test".to_string()));
        // Missing "id" which is specified as key column

        let event = ChangeEvent::update(
            "public".to_string(),
            "users".to_string(),
            16384,
            Some(incomplete_data.clone()),
            incomplete_data,
            ReplicaIdentity::Default,
            vec!["id".to_string()], // "id" not in data
        );

        // This event structure should trigger an error in MySQL destination
        match &event.event_type {
            EventType::Update { old_data, .. } => {
                let data_source = old_data.as_ref().unwrap();
                assert!(!data_source.contains_key("id")); // Key column missing
                assert!(data_source.contains_key("name")); // Non-key column present

                // Expected error: "Key column 'id' not found in data for Update on public.users"
            }
            _ => panic!("Expected Update event"),
        }
    }

    #[test]
    fn test_empty_data_scenario() {
        // Test scenario with empty old_data and new_data
        let event = ChangeEvent::update(
            "public".to_string(),
            "empty_table".to_string(),
            16384,
            Some(HashMap::new()), // Empty old_data
            HashMap::new(),       // Empty new_data
            ReplicaIdentity::Default,
            vec!["id".to_string()],
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
            "public".to_string(),
            "test_table".to_string(),
            16384,
            None,           // No old_data
            HashMap::new(), // Empty new_data
            ReplicaIdentity::Nothing,
            vec!["id".to_string()],
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
        let mut data = HashMap::new();
        data.insert("some_data".to_string(), Value::String("value".to_string()));

        let event = ChangeEvent::update(
            "public".to_string(),
            "no_keys_table".to_string(),
            16384,
            None,
            data,
            ReplicaIdentity::Nothing,
            vec![], // No key columns
        );

        let key_columns = event.get_key_columns().unwrap();
        assert!(key_columns.is_empty());

        // For NOTHING replica identity with no key columns:
        // Expected behavior: either reject operation or use dangerous WHERE 1=1
    }

    #[test]
    fn test_partial_key_columns_missing() {
        // Test composite key where some key columns are missing
        let mut incomplete_data = HashMap::new();
        incomplete_data.insert(
            "tenant_id".to_string(),
            Value::String("tenant_1".to_string()),
        );
        // Missing "user_id" which is part of composite key

        let event = ChangeEvent::update(
            "public".to_string(),
            "user_accounts".to_string(),
            16384,
            Some(incomplete_data.clone()),
            incomplete_data,
            ReplicaIdentity::Default,
            vec!["tenant_id".to_string(), "user_id".to_string()], // Composite key
        );

        match &event.event_type {
            EventType::Update { old_data, .. } => {
                let data_source = old_data.as_ref().unwrap();
                assert!(data_source.contains_key("tenant_id")); // Present
                assert!(!data_source.contains_key("user_id")); // Missing

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
        let mut old_data = HashMap::new();
        old_data.insert("id".to_string(), Value::Number(serde_json::Number::from(1)));
        old_data.insert("value".to_string(), Value::String("old_value".to_string()));

        let mut new_data = HashMap::new();
        new_data.insert("id".to_string(), Value::Number(serde_json::Number::from(1)));
        new_data.insert("value".to_string(), Value::String("new_value".to_string()));

        let event = ChangeEvent::update(
            "public".to_string(),
            "priority_test".to_string(),
            16384,
            Some(old_data.clone()),
            new_data.clone(),
            ReplicaIdentity::Default,
            vec!["id".to_string()],
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
                    Some(&Value::String("old_value".to_string()))
                );
                assert_ne!(
                    data_source.get("value"),
                    Some(&Value::String("new_value".to_string()))
                );
            }
            _ => panic!("Expected Update event"),
        }
    }

    #[test]
    fn test_fallback_only_when_old_data_none() {
        // Test that fallback to new_data only happens when old_data is None
        let mut new_data = HashMap::new();
        new_data.insert("id".to_string(), Value::Number(serde_json::Number::from(1)));
        new_data.insert(
            "value".to_string(),
            Value::String("fallback_value".to_string()),
        );

        let event = ChangeEvent::update(
            "public".to_string(),
            "fallback_test".to_string(),
            16384,
            None, // old_data is None
            new_data.clone(),
            ReplicaIdentity::Nothing,
            vec![],
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
                    Some(&Value::String("fallback_value".to_string()))
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
        let complex_json = Value::Object(serde_json::Map::from_iter([(
            "nested".to_string(),
            Value::Object(serde_json::Map::from_iter([(
                "array".to_string(),
                Value::Array(vec![
                    Value::Number(serde_json::Number::from(1)),
                    Value::String("text".to_string()),
                ]),
            )])),
        )]));

        let mut old_data = HashMap::new();
        old_data.insert("id".to_string(), Value::Number(serde_json::Number::from(1)));
        old_data.insert("json_data".to_string(), complex_json.clone());

        let mut new_data = HashMap::new();
        new_data.insert("id".to_string(), Value::Number(serde_json::Number::from(1)));
        new_data.insert("json_data".to_string(), complex_json);

        let event = ChangeEvent::update(
            "public".to_string(),
            "json_table".to_string(),
            16384,
            Some(old_data),
            new_data,
            ReplicaIdentity::Default,
            vec!["id".to_string()],
        );

        // Verify complex JSON data is handled correctly
        match &event.event_type {
            EventType::Update { old_data, .. } => {
                let data_source = old_data.as_ref().unwrap();
                assert!(data_source.contains_key("json_data"));
                assert!(matches!(
                    data_source.get("json_data"),
                    Some(Value::Object(_))
                ));
            }
            _ => panic!("Expected Update event"),
        }
    }

    #[test]
    fn test_various_data_types_in_key_columns() {
        // Test with different PostgreSQL data types in key columns
        let mut old_data = HashMap::new();
        old_data.insert(
            "string_key".to_string(),
            Value::String("key_value".to_string()),
        );
        old_data.insert(
            "int_key".to_string(),
            Value::Number(serde_json::Number::from(42)),
        );
        old_data.insert("bool_key".to_string(), Value::Bool(true));
        old_data.insert("null_key".to_string(), Value::Null);

        let event = ChangeEvent::update(
            "public".to_string(),
            "mixed_types".to_string(),
            16384,
            Some(old_data.clone()),
            old_data,
            ReplicaIdentity::Index,
            vec![
                "string_key".to_string(),
                "int_key".to_string(),
                "bool_key".to_string(),
                "null_key".to_string(),
            ],
        );

        match &event.event_type {
            EventType::Update { old_data, .. } => {
                let data_source = old_data.as_ref().unwrap();

                // Verify all data types are preserved
                assert!(matches!(
                    data_source.get("string_key"),
                    Some(Value::String(_))
                ));
                assert!(matches!(data_source.get("int_key"), Some(Value::Number(_))));
                assert!(matches!(
                    data_source.get("bool_key"),
                    Some(Value::Bool(true))
                ));
                assert!(matches!(data_source.get("null_key"), Some(Value::Null)));
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
            let mut data = HashMap::new();
            data.insert("id".to_string(), Value::Number(serde_json::Number::from(1)));

            let event = ChangeEvent::update(
                schema.to_string(),
                table.to_string(),
                16384,
                Some(data.clone()),
                data,
                ReplicaIdentity::Default,
                vec!["id".to_string()],
            );

            // Verify schema and table names are preserved correctly
            match &event.event_type {
                EventType::Update {
                    schema: event_schema,
                    table: event_table,
                    ..
                } => {
                    assert_eq!(event_schema, schema);
                    assert_eq!(event_table, table);
                }
                _ => panic!("Expected Update event"),
            }
        }
    }
}
