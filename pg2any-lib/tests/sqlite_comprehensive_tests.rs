use chrono::Utc;
use pg2any_lib::{
    destinations::{DestinationFactory, DestinationHandler},
    types::{ChangeEvent, DestinationType, ReplicaIdentity},
    Transaction,
};
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::fs;

/// Helper function to wrap a single event in a transaction for testing
fn wrap_in_transaction(event: ChangeEvent) -> Transaction {
    let mut tx = Transaction::new(1, Utc::now());
    tx.add_event(event);
    tx
}

/// Helper function to convert Transaction to SQL commands
fn transaction_to_sql_commands(tx: &Transaction) -> Vec<String> {
    tx.events.iter().filter_map(|e| event_to_sql(e)).collect()
}

fn event_to_sql(event: &ChangeEvent) -> Option<String> {
    use pg2any_lib::types::EventType;
    match &event.event_type {
        EventType::Insert { table, data, .. } => {
            let cols: Vec<_> = data.keys().cloned().collect();
            let vals: Vec<_> = cols.iter().map(|c| format_value(&data[c])).collect();
            Some(format!(
                "INSERT INTO \"{}\" ({}) VALUES ({});",
                table,
                cols.iter()
                    .map(|c| format!("\"{}\"", c))
                    .collect::<Vec<_>>()
                    .join(", "),
                vals.join(", ")
            ))
        }
        EventType::Update {
            table,
            new_data,
            old_data,
            key_columns,
            ..
        } => {
            let set: Vec<_> = new_data
                .iter()
                .map(|(k, v)| format!("\"{}\" = {}", k, format_value(v)))
                .collect();
            let cond: Vec<_> = key_columns
                .iter()
                .filter_map(|k| {
                    old_data
                        .as_ref()
                        .and_then(|d| d.get(k))
                        .or_else(|| new_data.get(k))
                        .map(|v| format!("\"{}\" = {}", k, format_value(v)))
                })
                .collect();
            if cond.is_empty() {
                return None;
            }
            Some(format!(
                "UPDATE \"{}\" SET {} WHERE {};",
                table,
                set.join(", "),
                cond.join(" AND ")
            ))
        }
        EventType::Delete {
            table,
            old_data,
            key_columns,
            ..
        } => {
            let cond: Vec<_> = key_columns
                .iter()
                .filter_map(|k| {
                    old_data
                        .get(k)
                        .map(|v| format!("\"{}\" = {}", k, format_value(v)))
                })
                .collect();
            if cond.is_empty() {
                return None;
            }
            Some(format!(
                "DELETE FROM \"{}\" WHERE {};",
                table,
                cond.join(" AND ")
            ))
        }
        _ => None,
    }
}

fn format_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{}'", s.replace("'", "''")),
        _ => format!("'{}'", v.to_string().replace("'", "''")),
    }
}

#[cfg(feature = "sqlite")]
use pg2any_lib::destinations::sqlite::SQLiteDestination;
#[cfg(feature = "sqlite")]
use sqlx::{Row, SqlitePool};

/// Helper struct to manage temporary SQLite database files
struct TempDatabase {
    path: PathBuf,
}

impl TempDatabase {
    fn new(test_name: &str) -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "pg2any_comprehensive_test_{}_{}.db",
            test_name,
            std::process::id()
        ));

        // Ensure the file doesn't exist before use
        let _ = fs::remove_file(&path);

        Self { path }
    }

    fn connection_string(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }
}

impl Drop for TempDatabase {
    fn drop(&mut self) {
        // Clean up the temporary database file
        let _ = fs::remove_file(&self.path);

        // Also clean up WAL and SHM files
        let wal_path = self.path.with_extension("db-wal");
        let shm_path = self.path.with_extension("db-shm");
        let _ = fs::remove_file(&wal_path);
        let _ = fs::remove_file(&shm_path);
    }
}

#[cfg(feature = "sqlite")]
async fn create_comprehensive_test_table(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS comprehensive_test (
            id INTEGER PRIMARY KEY,
            text_field TEXT,
            int_field INTEGER,
            real_field REAL,
            blob_field BLOB,
            bool_field BOOLEAN,
            date_field TEXT,
            nullable_field TEXT,
            json_field TEXT,
            unique_field TEXT UNIQUE,
            index_field TEXT
        )",
    )
    .execute(pool)
    .await?;

    // Create an index for testing
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_index_field ON comprehensive_test(index_field)")
        .execute(pool)
        .await?;

    Ok(())
}

#[cfg(feature = "sqlite")]
async fn create_users_table(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            email TEXT UNIQUE,
            active BOOLEAN DEFAULT 1,
            salary REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

// ================================
// EDGE CASE TESTS
// ================================

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_empty_string_handling() {
    let temp_db = TempDatabase::new("empty_strings");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();
    create_comprehensive_test_table(&pool).await.unwrap();

    // Test empty string insertion
    let mut data = HashMap::new();
    data.insert("id".to_string(), json!(1));
    data.insert("text_field".to_string(), json!(""));
    data.insert("nullable_field".to_string(), json!(""));

    let event = ChangeEvent::insert(
        "main".to_string(),
        "comprehensive_test".to_string(),
        123,
        data,
    );
    let result = destination
        .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(event)))
        .await;
    assert!(result.is_ok());

    // Verify empty strings are preserved
    let row = sqlx::query("SELECT text_field, nullable_field FROM comprehensive_test WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();

    let text_field: String = row.get("text_field");
    let nullable_field: String = row.get("nullable_field");
    assert_eq!(text_field, "");
    assert_eq!(nullable_field, "");

    pool.close().await;
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_null_value_handling() {
    let temp_db = TempDatabase::new("null_values");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();
    create_comprehensive_test_table(&pool).await.unwrap();

    // Test null value insertion
    let mut data = HashMap::new();
    data.insert("id".to_string(), json!(1));
    data.insert("text_field".to_string(), json!("test"));
    data.insert("nullable_field".to_string(), json!(null));
    data.insert("int_field".to_string(), json!(null));
    data.insert("real_field".to_string(), json!(null));

    let event = ChangeEvent::insert(
        "main".to_string(),
        "comprehensive_test".to_string(),
        123,
        data,
    );
    let result = destination
        .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(event)))
        .await;
    assert!(result.is_ok());

    // Verify null values are handled correctly
    let row = sqlx::query(
        "SELECT nullable_field, int_field, real_field FROM comprehensive_test WHERE id = 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let nullable_field: Option<String> = row.get("nullable_field");
    let int_field: Option<i32> = row.get("int_field");
    let real_field: Option<f64> = row.get("real_field");

    assert!(nullable_field.is_none());
    assert!(int_field.is_none());
    assert!(real_field.is_none());

    pool.close().await;
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_unicode_and_special_characters() {
    let temp_db = TempDatabase::new("unicode");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();
    create_comprehensive_test_table(&pool).await.unwrap();

    // Test unicode and special character insertion
    let mut data = HashMap::new();
    data.insert("id".to_string(), json!(1));
    data.insert(
        "text_field".to_string(),
        json!("🚀 Hello 世界! Special chars: áéíóú ñüç"),
    );
    data.insert(
        "json_field".to_string(),
        json!("{\"emoji\": \"😀\", \"chinese\": \"你好\"}"),
    );

    let event = ChangeEvent::insert(
        "main".to_string(),
        "comprehensive_test".to_string(),
        123,
        data,
    );
    let result = destination
        .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(event)))
        .await;
    assert!(result.is_ok());

    // Verify unicode characters are preserved
    let row = sqlx::query("SELECT text_field, json_field FROM comprehensive_test WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();

    let text_field: String = row.get("text_field");
    let json_field: String = row.get("json_field");

    assert!(text_field.contains("🚀"));
    assert!(text_field.contains("世界"));
    assert!(text_field.contains("áéíóú"));
    assert!(json_field.contains("😀"));
    assert!(json_field.contains("你好"));

    pool.close().await;
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_large_data_handling() {
    let temp_db = TempDatabase::new("large_data");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();
    create_comprehensive_test_table(&pool).await.unwrap();

    // Create large text data (1MB)
    let large_text = "A".repeat(1024 * 1024);
    let large_json = json!({
        "data": "B".repeat(500000),
        "nested": {
            "more_data": "C".repeat(500000)
        }
    });

    let mut data = HashMap::new();
    data.insert("id".to_string(), json!(1));
    data.insert("text_field".to_string(), json!(large_text));
    data.insert("json_field".to_string(), large_json);

    let event = ChangeEvent::insert(
        "main".to_string(),
        "comprehensive_test".to_string(),
        123,
        data,
    );
    let result = destination
        .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(event)))
        .await;
    assert!(result.is_ok());

    // Verify large data is stored correctly
    let row = sqlx::query("SELECT length(text_field) as text_len, length(json_field) as json_len FROM comprehensive_test WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();

    let text_len: i32 = row.get("text_len");
    let json_len: i32 = row.get("json_len");

    assert_eq!(text_len, 1024 * 1024);
    assert!(json_len > 1000000); // Should be large due to JSON structure

    pool.close().await;
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_numeric_precision() {
    let temp_db = TempDatabase::new("numeric_precision");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();
    create_comprehensive_test_table(&pool).await.unwrap();

    // Test various numeric types and precision
    let test_cases = vec![
        (1, json!(9223372036854775807i64)),  // i64 max
        (2, json!(-9223372036854775808i64)), // i64 min
        (3, json!(3.141592653589793)),       // High precision float
        (4, json!(1.7976931348623157e308)),  // Large float
        (5, json!(2.2250738585072014e-308)), // Small float
    ];

    for (id, value) in test_cases {
        let mut data = HashMap::new();
        data.insert("id".to_string(), json!(id));
        data.insert("real_field".to_string(), value.clone());

        let event = ChangeEvent::insert(
            "main".to_string(),
            "comprehensive_test".to_string(),
            123,
            data,
        );
        let result = destination
            .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(event)))
            .await;
        assert!(result.is_ok(), "Failed to insert value: {:?}", value);
    }

    // Verify precision is maintained
    let rows = sqlx::query("SELECT id, real_field FROM comprehensive_test ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();

    assert_eq!(rows.len(), 5);

    // Check specific values
    let pi_value: f64 = rows[2].get("real_field");
    assert!((pi_value - 3.141592653589793).abs() < 1e-10);

    pool.close().await;
    let _ = destination.close().await;
}

// ================================
// ERROR HANDLING TESTS
// ================================

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_invalid_connection_string() {
    let mut destination = SQLiteDestination::new();

    // Test various invalid connection strings
    let invalid_connections = vec!["/nonexistent/directory/that/cannot/be/created/test.db", ""];

    for conn_str in invalid_connections {
        let result = destination.connect(conn_str).await;
        // Some might fail, some might succeed (empty string creates in-memory DB)
        // Just ensure it doesn't panic
        let _ = result;
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_constraint_violations() {
    let temp_db = TempDatabase::new("constraints");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();
    create_comprehensive_test_table(&pool).await.unwrap();

    // Insert initial data
    let mut data1 = HashMap::new();
    data1.insert("id".to_string(), json!(1));
    data1.insert("unique_field".to_string(), json!("unique_value"));

    let event1 = ChangeEvent::insert(
        "main".to_string(),
        "comprehensive_test".to_string(),
        123,
        data1,
    );
    let result1 = destination
        .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(event1)))
        .await;
    assert!(result1.is_ok());

    // Try to insert duplicate unique value - should fail
    let mut data2 = HashMap::new();
    data2.insert("id".to_string(), json!(2));
    data2.insert("unique_field".to_string(), json!("unique_value"));

    let event2 = ChangeEvent::insert(
        "main".to_string(),
        "comprehensive_test".to_string(),
        124,
        data2,
    );
    let result2 = destination
        .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(event2)))
        .await;
    assert!(
        result2.is_err(),
        "Should fail due to unique constraint violation"
    );

    pool.close().await;
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_missing_key_columns_error() {
    let temp_db = TempDatabase::new("missing_keys");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();
    create_comprehensive_test_table(&pool).await.unwrap();

    // Insert initial data
    let mut data = HashMap::new();
    data.insert("id".to_string(), json!(1));
    data.insert("text_field".to_string(), json!("test"));

    let event = ChangeEvent::insert(
        "main".to_string(),
        "comprehensive_test".to_string(),
        123,
        data.clone(),
    );
    let result = destination
        .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(event)))
        .await;
    assert!(result.is_ok());

    // Try UPDATE with empty key columns - should fail
    let update_event = ChangeEvent::update(
        "main".to_string(),
        "comprehensive_test".to_string(),
        124,
        Some(data.clone()),
        data,
        ReplicaIdentity::Default,
        vec![], // Empty key columns
    );

    let result = destination
        .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(
            update_event,
        )))
        .await;
    // With the SQL-based workflow, events with missing key columns are skipped (generate no SQL)
    // so execute_sql_batch succeeds with empty batch
    assert!(result.is_ok(), "Empty SQL batch should succeed");

    pool.close().await;
    let _ = destination.close().await;
}

// ================================
// PERFORMANCE AND STRESS TESTS
// ================================

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_bulk_operations_performance() {
    let temp_db = TempDatabase::new("bulk_perf");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();
    create_users_table(&pool).await.unwrap();

    let start_time = Instant::now();
    let batch_size = 1000;

    // Bulk INSERT operations
    for i in 0..batch_size {
        let mut data = HashMap::new();
        data.insert("id".to_string(), json!(i));
        data.insert("name".to_string(), json!(format!("User {}", i)));
        data.insert("age".to_string(), json!(20 + (i % 50)));
        data.insert("email".to_string(), json!(format!("user{}@example.com", i)));
        data.insert("active".to_string(), json!(true));
        data.insert("salary".to_string(), json!(50000.0 + (i as f64 * 10.0)));

        let event = ChangeEvent::insert(
            "main".to_string(),
            "users".to_string(),
            123 + i as u32,
            data,
        );
        let result = destination
            .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(event)))
            .await;
        assert!(result.is_ok(), "Failed to insert record {}", i);
    }

    let insert_duration = start_time.elapsed();
    println!(
        "Bulk INSERT of {} records took: {:?}",
        batch_size, insert_duration
    );

    // Verify all records were inserted
    let count_row = sqlx::query("SELECT COUNT(*) as count FROM users")
        .fetch_one(&pool)
        .await
        .unwrap();
    let count: i32 = count_row.get("count");
    assert_eq!(count, batch_size as i32);

    // Bulk UPDATE operations
    let update_start = Instant::now();
    for i in 0..batch_size / 2 {
        let mut old_data = HashMap::new();
        old_data.insert("id".to_string(), json!(i));

        let mut new_data = HashMap::new();
        new_data.insert("id".to_string(), json!(i));
        new_data.insert("name".to_string(), json!(format!("Updated User {}", i)));
        new_data.insert("age".to_string(), json!(25 + (i % 50)));
        new_data.insert(
            "email".to_string(),
            json!(format!("updated_user{}@example.com", i)),
        );
        new_data.insert("active".to_string(), json!(true));
        new_data.insert("salary".to_string(), json!(60000.0 + (i as f64 * 15.0)));

        let event = ChangeEvent::update(
            "main".to_string(),
            "users".to_string(),
            200 + i as u32,
            Some(old_data),
            new_data,
            ReplicaIdentity::Default,
            vec!["id".to_string()],
        );

        let result = destination
            .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(event)))
            .await;
        assert!(result.is_ok(), "Failed to update record {}", i);
    }

    let update_duration = update_start.elapsed();
    println!(
        "Bulk UPDATE of {} records took: {:?}",
        batch_size / 2,
        update_duration
    );

    // Performance assertions - these are generous to account for CI environment
    assert!(
        insert_duration < Duration::from_secs(30),
        "INSERT operations too slow: {:?}",
        insert_duration
    );
    assert!(
        update_duration < Duration::from_secs(30),
        "UPDATE operations too slow: {:?}",
        update_duration
    );

    pool.close().await;
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_concurrent_operations() {
    let temp_db = TempDatabase::new("sequential");
    let connection_string = temp_db.connection_string();

    // Create destination and connect
    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    // Set up the database schema through the destination
    let pool = SqlitePool::connect(&format!("sqlite://{}", &connection_string))
        .await
        .unwrap();
    create_users_table(&pool).await.unwrap();
    pool.close().await;

    let mut total_records = 0;

    // Simulate multiple "sessions" operating sequentially
    // This tests that SQLite can handle repeated operations robustly
    for session_id in 0..3 {
        // Each session inserts 15 records
        for i in 0..15 {
            let record_id = session_id * 1000 + i;
            let mut data = HashMap::new();
            data.insert("id".to_string(), json!(record_id));
            data.insert(
                "name".to_string(),
                json!(format!("Session {} Record {}", session_id, i)),
            );
            data.insert("age".to_string(), json!(25 + i % 30));
            data.insert(
                "email".to_string(),
                json!(format!("session{}record{}@example.com", session_id, i)),
            );

            let event = ChangeEvent::insert(
                "main".to_string(),
                "users".to_string(),
                record_id as u32,
                data,
            );
            let result = destination
                .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(event)))
                .await;
            assert!(
                result.is_ok(),
                "Session {} failed to insert record {}: {:?}",
                session_id,
                i,
                result
            );
            total_records += 1;
        }
    }

    // Verify final record count
    let pool = SqlitePool::connect(&format!("sqlite://{}", &connection_string))
        .await
        .unwrap();

    let count_row = sqlx::query("SELECT COUNT(*) as count FROM users")
        .fetch_one(&pool)
        .await
        .unwrap();
    let count: i32 = count_row.get("count");

    assert_eq!(
        count, total_records,
        "Expected {} records, got {}",
        total_records, count
    );

    pool.close().await;
    let _ = destination.close().await;
}

// ================================
// TRANSACTION AND CONSISTENCY TESTS
// ================================

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_transaction_events() {
    let temp_db = TempDatabase::new("transactions");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();
    create_users_table(&pool).await.unwrap();

    // Test transaction BEGIN/COMMIT events
    let begin_event = ChangeEvent::begin(100, Utc::now());
    let commit_event = ChangeEvent::commit(110, Utc::now());

    // These should not fail (even though SQLite handles transactions automatically)
    let begin_result = destination
        .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(
            begin_event,
        )))
        .await;
    assert!(begin_result.is_ok());

    // Insert some data between transaction events
    let mut data = HashMap::new();
    data.insert("id".to_string(), json!(1));
    data.insert("name".to_string(), json!("Transaction Test"));
    data.insert("email".to_string(), json!("txn@example.com"));

    let insert_event = ChangeEvent::insert("main".to_string(), "users".to_string(), 105, data);
    let insert_result = destination
        .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(
            insert_event,
        )))
        .await;
    assert!(insert_result.is_ok());

    let commit_result = destination
        .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(
            commit_event,
        )))
        .await;
    assert!(commit_result.is_ok());

    // Verify data was inserted
    let row = sqlx::query("SELECT COUNT(*) as count FROM users WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    let count: i32 = row.get("count");
    assert_eq!(count, 1);

    pool.close().await;
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_metadata_events() {
    let temp_db = TempDatabase::new("metadata");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    // Test various metadata events that should be safely ignored
    let metadata_events = vec![
        ChangeEvent::relation(),
        ChangeEvent::type_event(),
        ChangeEvent::origin(),
        ChangeEvent::message(),
    ];

    for event in metadata_events {
        let tx = wrap_in_transaction(event.clone());
        let commands = transaction_to_sql_commands(&tx);
        let result = destination.execute_sql_batch(&commands).await;
        assert!(
            result.is_ok(),
            "Metadata event should not fail: {:?}",
            event.event_type
        );
    }

    let _ = destination.close().await;
}

// ================================
// RECOVERY AND ROBUSTNESS TESTS
// ================================

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_connection_recovery() {
    let temp_db = TempDatabase::new("recovery");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    // Verify connection works by creating a simple transaction
    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();
    create_users_table(&pool).await.unwrap();

    let mut values = HashMap::new();
    values.insert("id".to_string(), json!(1));
    values.insert("name".to_string(), json!("Alice"));
    let event = ChangeEvent::insert("public".to_string(), "users".to_string(), 1, values);
    let transaction = wrap_in_transaction(event);
    let process_result = destination
        .execute_sql_batch(&transaction_to_sql_commands(&transaction))
        .await;
    assert!(process_result.is_ok());

    // Close and reconnect
    let close_result = destination.close().await;
    assert!(close_result.is_ok());

    // Reconnect
    let reconnect_result = destination.connect(&connection_string).await;
    assert!(reconnect_result.is_ok());

    // Verify connection works again
    let mut values2 = HashMap::new();
    values2.insert("id".to_string(), json!(2));
    values2.insert("name".to_string(), json!("Bob"));
    let event2 = ChangeEvent::insert("public".to_string(), "users".to_string(), 2, values2);
    let transaction2 = wrap_in_transaction(event2);
    let process_result2 = destination
        .execute_sql_batch(&transaction_to_sql_commands(&transaction2))
        .await;
    assert!(process_result2.is_ok());

    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_file_permissions_and_paths() {
    // Test different path formats and edge cases
    let test_cases = vec![
        ("simple.db", true),
        ("./relative_path.db", true),
        ("nested/directory/test.db", true),
        ("file://explicit_file_protocol.db", true),
        ("sqlite://explicit_sqlite_protocol.db", true),
    ];

    for (path_format, should_succeed) in test_cases {
        println!("Testing path format: {}", path_format);

        let mut temp_path = std::env::temp_dir();
        temp_path.push(format!("pg2any_path_test_{}", std::process::id()));

        let full_path = if path_format.contains("/")
            && !path_format.starts_with("file://")
            && !path_format.starts_with("sqlite://")
        {
            temp_path.join(path_format).to_string_lossy().to_string()
        } else if path_format.starts_with("file://") {
            format!(
                "file://{}/{}",
                temp_path.to_string_lossy(),
                path_format.strip_prefix("file://").unwrap()
            )
        } else if path_format.starts_with("sqlite://") {
            format!(
                "sqlite://{}/{}",
                temp_path.to_string_lossy(),
                path_format.strip_prefix("sqlite://").unwrap()
            )
        } else {
            temp_path.join(path_format).to_string_lossy().to_string()
        };

        let mut destination = SQLiteDestination::new();
        let result = destination.connect(&full_path).await;

        if should_succeed {
            assert!(
                result.is_ok(),
                "Path format '{}' should succeed",
                path_format
            );
            let _ = destination.close().await;
        } else {
            assert!(result.is_err(), "Path format '{}' should fail", path_format);
        }

        // Clean up
        let _ = fs::remove_dir_all(&temp_path);
    }
}

// ================================
// INTEGRATION AND END-TO-END TESTS
// ================================

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_complete_crud_cycle() {
    let temp_db = TempDatabase::new("crud_cycle");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();
    create_users_table(&pool).await.unwrap();

    // 1. CREATE (INSERT)
    let mut create_data = HashMap::new();
    create_data.insert("id".to_string(), json!(1));
    create_data.insert("name".to_string(), json!("John Doe"));
    create_data.insert("age".to_string(), json!(30));
    create_data.insert("email".to_string(), json!("john@example.com"));
    create_data.insert("active".to_string(), json!(true));
    create_data.insert("salary".to_string(), json!(50000.0));

    let create_event = ChangeEvent::insert(
        "main".to_string(),
        "users".to_string(),
        1,
        create_data.clone(),
    );
    let create_result = destination
        .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(
            create_event,
        )))
        .await;
    assert!(create_result.is_ok());

    // Verify insertion
    let count_after_insert = sqlx::query("SELECT COUNT(*) as count FROM users WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get::<i32, _>("count");
    assert_eq!(count_after_insert, 1);

    // 2. READ (verify data integrity)
    let row = sqlx::query("SELECT * FROM users WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(row.get::<String, _>("name"), "John Doe");
    assert_eq!(row.get::<i32, _>("age"), 30);
    assert_eq!(row.get::<String, _>("email"), "john@example.com");

    // 3. UPDATE
    let mut update_data = HashMap::new();
    update_data.insert("id".to_string(), json!(1));
    update_data.insert("name".to_string(), json!("John Smith"));
    update_data.insert("age".to_string(), json!(31));
    update_data.insert("email".to_string(), json!("john.smith@example.com"));
    update_data.insert("active".to_string(), json!(true));
    update_data.insert("salary".to_string(), json!(55000.0));

    let update_event = ChangeEvent::update(
        "main".to_string(),
        "users".to_string(),
        2,
        Some(create_data),
        update_data,
        ReplicaIdentity::Default,
        vec!["id".to_string()],
    );

    let update_result = destination
        .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(
            update_event,
        )))
        .await;
    assert!(update_result.is_ok());

    // Verify update
    let updated_row = sqlx::query("SELECT name, age, email, salary FROM users WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(updated_row.get::<String, _>("name"), "John Smith");
    assert_eq!(updated_row.get::<i32, _>("age"), 31);
    assert_eq!(
        updated_row.get::<String, _>("email"),
        "john.smith@example.com"
    );
    assert_eq!(updated_row.get::<f64, _>("salary"), 55000.0);

    // 4. DELETE
    let mut delete_data = HashMap::new();
    delete_data.insert("id".to_string(), json!(1));

    let delete_event = ChangeEvent::delete(
        "main".to_string(),
        "users".to_string(),
        3,
        delete_data,
        ReplicaIdentity::Default,
        vec!["id".to_string()],
    );

    let delete_result = destination
        .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(
            delete_event,
        )))
        .await;
    assert!(delete_result.is_ok());

    // Verify deletion
    let count_after_delete = sqlx::query("SELECT COUNT(*) as count FROM users WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get::<i32, _>("count");
    assert_eq!(count_after_delete, 0);

    pool.close().await;
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_destination_factory_integration() {
    // Test that the factory creates a working SQLite destination
    let mut destination = DestinationFactory::create(&DestinationType::SQLite).unwrap();

    let temp_db = TempDatabase::new("factory_integration");
    let connection_string = temp_db.connection_string();

    // Test connection through factory-created destination
    let connect_result = destination.connect(&connection_string).await;
    assert!(connect_result.is_ok());

    // Test basic operation
    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();
    create_users_table(&pool).await.unwrap();

    let mut data = HashMap::new();
    data.insert("id".to_string(), json!(1));
    data.insert("name".to_string(), json!("Factory Test"));

    let event = ChangeEvent::insert("main".to_string(), "users".to_string(), 1, data);
    let process_result = destination
        .execute_sql_batch(&transaction_to_sql_commands(&wrap_in_transaction(event)))
        .await;
    assert!(process_result.is_ok());

    pool.close().await;
    let _ = destination.close().await;
}
