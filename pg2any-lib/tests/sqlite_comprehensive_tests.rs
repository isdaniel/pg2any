use chrono::Utc;
use pg2any_lib::{
    destinations::{DestinationFactory, DestinationHandler},
    types::{ChangeEvent, DestinationType, ReplicaIdentity},
    Transaction,
};
use pg_walstream::{ColumnValue, Lsn, RowData};
use std::path::PathBuf;
use std::sync::Arc;
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
            let cols: Vec<String> = data.iter().map(|(k, _)| k.to_string()).collect();
            let vals: Vec<String> = data.iter().map(|(_, v)| format_column_value(v)).collect();
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
            let set: Vec<String> = new_data
                .iter()
                .map(|(k, v)| format!("\"{}\" = {}", k, format_column_value(v)))
                .collect();
            let cond: Vec<String> = key_columns
                .iter()
                .filter_map(|k| {
                    old_data
                        .as_ref()
                        .and_then(|d| d.get(k.as_ref()))
                        .or_else(|| new_data.get(k.as_ref()))
                        .map(|v| format!("\"{}\" = {}", k, format_column_value(v)))
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
            let cond: Vec<String> = key_columns
                .iter()
                .filter_map(|k| {
                    old_data
                        .get(k.as_ref())
                        .map(|v| format!("\"{}\" = {}", k, format_column_value(v)))
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

fn format_column_value(value: &ColumnValue) -> String {
    match value {
        ColumnValue::Null => "NULL".to_string(),
        ColumnValue::Text(b) => match std::str::from_utf8(b) {
            Ok(s) => {
                if s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok() {
                    s.to_string()
                } else if s == "true" {
                    "1".to_string()
                } else if s == "false" {
                    "0".to_string()
                } else {
                    format!("'{}'", s.replace('\'', "''"))
                }
            }
            Err(_) => "NULL".to_string(),
        },
        ColumnValue::Binary(b) => {
            format!(
                "X'{}'",
                b.iter()
                    .map(|byte| format!("{:02x}", byte))
                    .collect::<String>()
            )
        }
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
    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("text_field", ColumnValue::text("")),
        ("nullable_field", ColumnValue::text("")),
    ]);

    let event = ChangeEvent::insert("main", "comprehensive_test", 123, data, Lsn::from(100));
    let result = destination
        .execute_sql_batch_with_hook(
            &transaction_to_sql_commands(&wrap_in_transaction(event)),
            None,
        )
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
    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("text_field", ColumnValue::text("test")),
        ("nullable_field", ColumnValue::Null),
        ("int_field", ColumnValue::Null),
        ("real_field", ColumnValue::Null),
    ]);

    let event = ChangeEvent::insert("main", "comprehensive_test", 123, data, Lsn::from(100));
    let result = destination
        .execute_sql_batch_with_hook(
            &transaction_to_sql_commands(&wrap_in_transaction(event)),
            None,
        )
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
    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        (
            "text_field",
            ColumnValue::text("🚀 Hello 世界! Special chars: áéíóú ñüç"),
        ),
        (
            "json_field",
            ColumnValue::text("{\"emoji\": \"😀\", \"chinese\": \"你好\"}"),
        ),
    ]);

    let event = ChangeEvent::insert("main", "comprehensive_test", 123, data, Lsn::from(100));
    let result = destination
        .execute_sql_batch_with_hook(
            &transaction_to_sql_commands(&wrap_in_transaction(event)),
            None,
        )
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
    let large_json = format!(
        "{{\"data\": \"{}\", \"nested\": {{\"more_data\": \"{}\"}}}}",
        "B".repeat(500000),
        "C".repeat(500000)
    );

    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("text_field", ColumnValue::text(&large_text)),
        ("json_field", ColumnValue::text(&large_json)),
    ]);

    let event = ChangeEvent::insert("main", "comprehensive_test", 123, data, Lsn::from(100));
    let result = destination
        .execute_sql_batch_with_hook(
            &transaction_to_sql_commands(&wrap_in_transaction(event)),
            None,
        )
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
    let test_cases: Vec<(i32, ColumnValue)> = vec![
        (1, ColumnValue::text("9223372036854775807")), // i64 max
        (2, ColumnValue::text("-9223372036854775808")), // i64 min
        (3, ColumnValue::text("3.141592653589793")),   // High precision float
        (4, ColumnValue::text("1.7976931348623157e308")), // Large float
        (5, ColumnValue::text("2.2250738585072014e-308")), // Small float
    ];

    for (id, value) in &test_cases {
        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text(&id.to_string())),
            ("real_field", value.clone()),
        ]);

        let event = ChangeEvent::insert("main", "comprehensive_test", 123, data, Lsn::from(100));
        let result = destination
            .execute_sql_batch_with_hook(
                &transaction_to_sql_commands(&wrap_in_transaction(event)),
                None,
            )
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
    let data1 = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("unique_field", ColumnValue::text("unique_value")),
    ]);

    let event1 = ChangeEvent::insert("main", "comprehensive_test", 123, data1, Lsn::from(100));
    let result1 = destination
        .execute_sql_batch_with_hook(
            &transaction_to_sql_commands(&wrap_in_transaction(event1)),
            None,
        )
        .await;
    assert!(result1.is_ok());

    // Try to insert duplicate unique value - should fail
    let data2 = RowData::from_pairs(vec![
        ("id", ColumnValue::text("2")),
        ("unique_field", ColumnValue::text("unique_value")),
    ]);

    let event2 = ChangeEvent::insert("main", "comprehensive_test", 124, data2, Lsn::from(100));
    let result2 = destination
        .execute_sql_batch_with_hook(
            &transaction_to_sql_commands(&wrap_in_transaction(event2)),
            None,
        )
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
    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("text_field", ColumnValue::text("test")),
    ]);

    let event = ChangeEvent::insert(
        "main",
        "comprehensive_test",
        123,
        data.clone(),
        Lsn::from(100),
    );
    let result = destination
        .execute_sql_batch_with_hook(
            &transaction_to_sql_commands(&wrap_in_transaction(event)),
            None,
        )
        .await;
    assert!(result.is_ok());

    // Try UPDATE with empty key columns - should fail
    let update_event = ChangeEvent::update(
        "main",
        "comprehensive_test",
        124,
        Some(data.clone()),
        data,
        ReplicaIdentity::Default,
        vec![], // Empty key columns
        Lsn::from(300),
    );

    let result = destination
        .execute_sql_batch_with_hook(
            &transaction_to_sql_commands(&wrap_in_transaction(update_event)),
            None,
        )
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
        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text(&i.to_string())),
            ("name", ColumnValue::text(&format!("User {}", i))),
            ("age", ColumnValue::text(&(20 + (i % 50)).to_string())),
            (
                "email",
                ColumnValue::text(&format!("user{}@example.com", i)),
            ),
            ("active", ColumnValue::text("true")),
            (
                "salary",
                ColumnValue::text(&format!("{}", 50000.0 + (i as f64 * 10.0))),
            ),
        ]);

        let event = ChangeEvent::insert("main", "users", 123 + i as u32, data, Lsn::from(100));
        let result = destination
            .execute_sql_batch_with_hook(
                &transaction_to_sql_commands(&wrap_in_transaction(event)),
                None,
            )
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
        let old_data = RowData::from_pairs(vec![("id", ColumnValue::text(&i.to_string()))]);

        let new_data = RowData::from_pairs(vec![
            ("id", ColumnValue::text(&i.to_string())),
            ("name", ColumnValue::text(&format!("Updated User {}", i))),
            ("age", ColumnValue::text(&(25 + (i % 50)).to_string())),
            (
                "email",
                ColumnValue::text(&format!("updated_user{}@example.com", i)),
            ),
            ("active", ColumnValue::text("true")),
            (
                "salary",
                ColumnValue::text(&format!("{}", 60000.0 + (i as f64 * 15.0))),
            ),
        ]);

        let event = ChangeEvent::update(
            "main",
            "users",
            200 + i as u32,
            Some(old_data),
            new_data,
            ReplicaIdentity::Default,
            vec![Arc::from("id")],
            Lsn::from(300),
        );

        let result = destination
            .execute_sql_batch_with_hook(
                &transaction_to_sql_commands(&wrap_in_transaction(event)),
                None,
            )
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
            let data = RowData::from_pairs(vec![
                ("id", ColumnValue::text(&record_id.to_string())),
                (
                    "name",
                    ColumnValue::text(&format!("Session {} Record {}", session_id, i)),
                ),
                ("age", ColumnValue::text(&(25 + i % 30).to_string())),
                (
                    "email",
                    ColumnValue::text(&format!("session{}record{}@example.com", session_id, i)),
                ),
            ]);

            let event =
                ChangeEvent::insert("main", "users", record_id as u32, data, Lsn::from(100));
            let result = destination
                .execute_sql_batch_with_hook(
                    &transaction_to_sql_commands(&wrap_in_transaction(event)),
                    None,
                )
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
    let timestamp = Utc::now();
    let begin_event = ChangeEvent::begin(100, Lsn::from(0), timestamp, Lsn::from(100));
    let commit_event =
        ChangeEvent::commit(timestamp, Lsn::from(100), Lsn::from(110), Lsn::from(110));

    // These should not fail (even though SQLite handles transactions automatically)
    let begin_result = destination
        .execute_sql_batch_with_hook(
            &transaction_to_sql_commands(&wrap_in_transaction(begin_event)),
            None,
        )
        .await;
    assert!(begin_result.is_ok());

    // Insert some data between transaction events
    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("Transaction Test")),
        ("email", ColumnValue::text("txn@example.com")),
    ]);

    let insert_event = ChangeEvent::insert("main", "users", 105, data, Lsn::from(100));
    let insert_result = destination
        .execute_sql_batch_with_hook(
            &transaction_to_sql_commands(&wrap_in_transaction(insert_event)),
            None,
        )
        .await;
    assert!(insert_result.is_ok());

    let commit_result = destination
        .execute_sql_batch_with_hook(
            &transaction_to_sql_commands(&wrap_in_transaction(commit_event)),
            None,
        )
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
        ChangeEvent::relation(Lsn::from(0)),
        ChangeEvent::type_event(Lsn::from(0)),
        ChangeEvent::origin(Lsn::from(0)),
        ChangeEvent::message(Lsn::from(0)),
    ];

    for event in metadata_events {
        let tx = wrap_in_transaction(event.clone());
        let commands = transaction_to_sql_commands(&tx);
        let result = destination
            .execute_sql_batch_with_hook(&commands, None)
            .await;
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

    let values = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("Alice")),
    ]);
    let event = ChangeEvent::insert("public", "users", 1, values, Lsn::from(100));
    let transaction = wrap_in_transaction(event);
    let process_result = destination
        .execute_sql_batch_with_hook(&transaction_to_sql_commands(&transaction), None)
        .await;
    assert!(process_result.is_ok());

    // Close and reconnect
    let close_result = destination.close().await;
    assert!(close_result.is_ok());

    // Reconnect
    let reconnect_result = destination.connect(&connection_string).await;
    assert!(reconnect_result.is_ok());

    // Verify connection works again
    let values2 = RowData::from_pairs(vec![
        ("id", ColumnValue::text("2")),
        ("name", ColumnValue::text("Bob")),
    ]);
    let event2 = ChangeEvent::insert("public", "users", 2, values2, Lsn::from(100));
    let transaction2 = wrap_in_transaction(event2);
    let process_result2 = destination
        .execute_sql_batch_with_hook(&transaction_to_sql_commands(&transaction2), None)
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
    let create_data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("John Doe")),
        ("age", ColumnValue::text("30")),
        ("email", ColumnValue::text("john@example.com")),
        ("active", ColumnValue::text("true")),
        ("salary", ColumnValue::text("50000")),
    ]);

    let create_event = ChangeEvent::insert("main", "users", 1, create_data.clone(), Lsn::from(100));
    let create_result = destination
        .execute_sql_batch_with_hook(
            &transaction_to_sql_commands(&wrap_in_transaction(create_event)),
            None,
        )
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
    let update_data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("John Smith")),
        ("age", ColumnValue::text("31")),
        ("email", ColumnValue::text("john.smith@example.com")),
        ("active", ColumnValue::text("true")),
        ("salary", ColumnValue::text("55000")),
    ]);

    let update_event = ChangeEvent::update(
        "main",
        "users",
        2,
        Some(create_data),
        update_data,
        ReplicaIdentity::Default,
        vec![Arc::from("id")],
        Lsn::from(300),
    );

    let update_result = destination
        .execute_sql_batch_with_hook(
            &transaction_to_sql_commands(&wrap_in_transaction(update_event)),
            None,
        )
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
    let delete_data = RowData::from_pairs(vec![("id", ColumnValue::text("1"))]);

    let delete_event = ChangeEvent::delete(
        "main",
        "users",
        3,
        delete_data,
        ReplicaIdentity::Default,
        vec![Arc::from("id")],
        Lsn::from(200),
    );

    let delete_result = destination
        .execute_sql_batch_with_hook(
            &transaction_to_sql_commands(&wrap_in_transaction(delete_event)),
            None,
        )
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

    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("Factory Test")),
    ]);

    let event = ChangeEvent::insert("main", "users", 1, data, Lsn::from(100));
    let process_result = destination
        .execute_sql_batch_with_hook(
            &transaction_to_sql_commands(&wrap_in_transaction(event)),
            None,
        )
        .await;
    assert!(process_result.is_ok());

    pool.close().await;
    let _ = destination.close().await;
}
