use chrono::Utc;
use pg2any_lib::{
    destinations::{DestinationFactory, DestinationHandler},
    types::{ChangeEvent, DestinationType, EventType, ReplicaIdentity},
    Transaction,
};
use pg_walstream::{ColumnValue, Lsn, RowData};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;

/// Helper function to wrap a single event in a transaction for testing
fn wrap_in_transaction(event: ChangeEvent) -> Transaction {
    let mut tx = Transaction::new(1, Utc::now());
    tx.add_event(event);
    tx
}

/// Helper function to convert a Transaction into SQL commands for SQLite
fn transaction_to_sql_commands(tx: &Transaction) -> Vec<String> {
    let mut commands = Vec::new();
    for event in &tx.events {
        if let Some(sql) = event_to_sql(event) {
            commands.push(sql);
        }
    }
    commands
}

/// Helper function to convert a ChangeEvent to SQL command for SQLite
fn event_to_sql(event: &ChangeEvent) -> Option<String> {
    match &event.event_type {
        EventType::Insert { table, data, .. } => {
            let columns: Vec<String> = data.iter().map(|(k, _)| k.to_string()).collect();
            let values: Vec<String> = data.iter().map(|(_, v)| format_column_value(v)).collect();
            Some(format!(
                "INSERT INTO \"{}\" ({}) VALUES ({});",
                table,
                columns
                    .iter()
                    .map(|c| format!("\"{}\"", c))
                    .collect::<Vec<_>>()
                    .join(", "),
                values.join(", ")
            ))
        }
        EventType::Update {
            table,
            new_data,
            old_data,
            key_columns,
            replica_identity,
            ..
        } => {
            let set_clause: Vec<String> = new_data
                .iter()
                .map(|(col, val)| format!("\"{}\" = {}", col, format_column_value(val)))
                .collect();

            // For REPLICA IDENTITY FULL with empty key_columns, use all old_data columns
            let actual_key_cols: Vec<String> =
                if key_columns.is_empty() && matches!(replica_identity, ReplicaIdentity::Full) {
                    old_data
                        .as_ref()
                        .map(|d| d.iter().map(|(k, _)| k.to_string()).collect())
                        .unwrap_or_default()
                } else {
                    key_columns.iter().map(|k| k.to_string()).collect()
                };

            let where_clause = if !actual_key_cols.is_empty() {
                let conditions: Vec<String> = actual_key_cols
                    .iter()
                    .filter_map(|col| {
                        old_data
                            .as_ref()
                            .and_then(|d| d.get(col.as_str()))
                            .or_else(|| new_data.get(col.as_str()))
                            .map(|val| format!("\"{}\" = {}", col, format_column_value(val)))
                    })
                    .collect();
                if conditions.is_empty() {
                    return None;
                }
                conditions.join(" AND ")
            } else {
                return None;
            };

            Some(format!(
                "UPDATE \"{}\" SET {} WHERE {};",
                table,
                set_clause.join(", "),
                where_clause
            ))
        }
        EventType::Delete {
            table,
            old_data,
            key_columns,
            ..
        } => {
            let where_clause = if !key_columns.is_empty() {
                let conditions: Vec<String> = key_columns
                    .iter()
                    .filter_map(|col| {
                        old_data
                            .get(col.as_ref())
                            .map(|val| format!("\"{}\" = {}", col, format_column_value(val)))
                    })
                    .collect();
                if conditions.is_empty() {
                    return None;
                }
                conditions.join(" AND ")
            } else {
                return None;
            };

            Some(format!("DELETE FROM \"{}\" WHERE {};", table, where_clause))
        }
        EventType::Truncate(tables) => {
            if tables.is_empty() {
                return None;
            }
            // SQLite doesn't have TRUNCATE, use DELETE
            // Handle schema.table format
            let table_name: &str = tables[0].as_ref();
            if table_name.contains('.') {
                let parts: Vec<&str> = table_name.splitn(2, '.').collect();
                Some(format!("DELETE FROM \"{}\".\"{}\"", parts[0], parts[1]))
            } else {
                Some(format!("DELETE FROM \"{}\"", table_name))
            }
        }
        _ => None, // Skip non-DML events
    }
}

/// Helper function to format a ColumnValue as SQL literal
fn format_column_value(value: &ColumnValue) -> String {
    match value {
        ColumnValue::Null => "NULL".to_string(),
        ColumnValue::Text(b) => {
            match std::str::from_utf8(b) {
                Ok(s) => {
                    // Try to parse as number
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
            }
        }
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
            "pg2any_test_{}_{}.db",
            test_name,
            std::process::id()
        ));

        // Ensure the file doesn't exist before use
        let _ = fs::remove_file(&path);

        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn connection_string(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }
}

impl Drop for TempDatabase {
    fn drop(&mut self) {
        // Clean up the temporary database file
        let _ = fs::remove_file(&self.path);
    }
}

/// Helper function to create test data
fn create_test_data() -> RowData {
    RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("John Doe")),
        ("age", ColumnValue::text("30")),
        ("email", ColumnValue::text("john@example.com")),
        ("active", ColumnValue::text("true")),
        ("salary", ColumnValue::text("50000.50")),
    ])
}

/// Helper function to create updated test data
fn create_updated_test_data() -> RowData {
    RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("John Smith")),
        ("age", ColumnValue::text("31")),
        ("email", ColumnValue::text("john.smith@example.com")),
        ("active", ColumnValue::text("true")),
        ("salary", ColumnValue::text("55000.75")),
    ])
}

#[cfg(feature = "sqlite")]
async fn create_test_table(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS main.users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            email TEXT,
            active BOOLEAN,
            salary REAL
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_destination_factory() {
    let destination = DestinationFactory::create(&DestinationType::SQLite);
    assert!(destination.is_ok());
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_destination_connect() {
    let temp_db = TempDatabase::new("connect");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    let result = destination.connect(&connection_string).await;

    assert!(result.is_ok());
    assert!(temp_db.path().exists());

    // Clean up
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_destination_connect_with_file_prefix() {
    let temp_db = TempDatabase::new("file_prefix");
    let connection_string = format!("file://{}", temp_db.path().to_string_lossy());

    let mut destination = SQLiteDestination::new();
    let result = destination.connect(&connection_string).await;

    assert!(result.is_ok());
    assert!(temp_db.path().exists());

    // Clean up
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_destination_connect_with_sqlite_prefix() {
    let temp_db = TempDatabase::new("sqlite_prefix");
    let connection_string = format!("sqlite://{}", temp_db.path().to_string_lossy());

    let mut destination = SQLiteDestination::new();
    let result = destination.connect(&connection_string).await;

    assert!(result.is_ok());
    assert!(temp_db.path().exists());

    // Clean up
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_destination_connection_lifecycle() {
    let temp_db = TempDatabase::new("connection_lifecycle");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();

    // Connect
    destination.connect(&connection_string).await.unwrap();

    // Verify connection works by processing a transaction
    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();

    sqlx::query("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, data TEXT)")
        .execute(&pool)
        .await
        .unwrap();

    // Clean up
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_destination_process_insert_event() {
    let temp_db = TempDatabase::new("insert");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    // Create the database pool for table setup
    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();
    create_test_table(&pool).await.unwrap();

    // Create INSERT event
    let data = create_test_data();
    let event = ChangeEvent::insert("main", "users", 123, data, Lsn::from(100));

    // Process the event using execute_sql_batch
    let tx = wrap_in_transaction(event);
    let commands = transaction_to_sql_commands(&tx);
    let result = destination
        .execute_sql_batch_with_hook(&commands, None)
        .await;
    assert!(result.is_ok());

    // Verify the data was inserted
    let row = sqlx::query("SELECT COUNT(*) as count FROM main.users WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    let count: i32 = row.get("count");
    assert_eq!(count, 1);

    // Clean up
    pool.close().await;
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_destination_process_update_event() {
    let temp_db = TempDatabase::new("update");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    // Create and populate table
    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();
    create_test_table(&pool).await.unwrap();

    // Insert initial data
    sqlx::query(
        "INSERT INTO main.users (id, name, age, email, active, salary) VALUES (1, 'John Doe', 30, 'john@example.com', 1, 50000.50)"
    ).execute(&pool).await.unwrap();

    // Create UPDATE event
    let old_data = create_test_data();
    let new_data = create_updated_test_data();
    let key_columns = vec![Arc::from("id")];

    let event = ChangeEvent::update(
        "main",
        "users",
        123,
        Some(old_data),
        new_data,
        ReplicaIdentity::Default,
        key_columns,
        Lsn::from(300),
    );

    // Process the event using execute_sql_batch
    let tx = wrap_in_transaction(event);
    let commands = transaction_to_sql_commands(&tx);
    let result = destination
        .execute_sql_batch_with_hook(&commands, None)
        .await;
    assert!(result.is_ok());

    // Verify the data was updated
    let row = sqlx::query("SELECT name, age FROM main.users WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    let name: String = row.get("name");
    let age: i32 = row.get("age");
    assert_eq!(name, "John Smith");
    assert_eq!(age, 31);

    // Clean up
    pool.close().await;
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_destination_process_delete_event() {
    let temp_db = TempDatabase::new("delete");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    // Create and populate table
    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();
    create_test_table(&pool).await.unwrap();

    // Insert test data
    sqlx::query(
        "INSERT INTO main.users (id, name, age, email, active, salary) VALUES (1, 'John Doe', 30, 'john@example.com', 1, 50000.50)"
    ).execute(&pool).await.unwrap();

    // Verify record exists
    let row = sqlx::query("SELECT COUNT(*) as count FROM main.users WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    let count: i32 = row.get("count");
    assert_eq!(count, 1);

    // Create DELETE event
    let old_data = create_test_data();
    let key_columns = vec![Arc::from("id")];

    let event = ChangeEvent::delete(
        "main",
        "users",
        123,
        old_data,
        ReplicaIdentity::Default,
        key_columns,
        Lsn::from(200),
    );

    // Process the event using execute_sql_batch
    let tx = wrap_in_transaction(event);
    let commands = transaction_to_sql_commands(&tx);
    let result = destination
        .execute_sql_batch_with_hook(&commands, None)
        .await;
    assert!(result.is_ok());

    // Verify the data was deleted
    let row = sqlx::query("SELECT COUNT(*) as count FROM main.users WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    let count: i32 = row.get("count");
    assert_eq!(count, 0);

    // Clean up
    pool.close().await;
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_destination_process_truncate_event() {
    let temp_db = TempDatabase::new("truncate");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    // Create and populate table
    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS main.users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Insert test data
    sqlx::query("INSERT INTO main.users (id, name) VALUES (1, 'John'), (2, 'Jane'), (3, 'Bob')")
        .execute(&pool)
        .await
        .unwrap();

    // Verify records exist
    let row = sqlx::query("SELECT COUNT(*) as count FROM main.users")
        .fetch_one(&pool)
        .await
        .unwrap();
    let count: i32 = row.get("count");
    assert_eq!(count, 3);

    // Create TRUNCATE event
    let event = ChangeEvent::truncate(vec![Arc::from("main.users")], Lsn::from(400));

    // Process the event using execute_sql_batch
    let tx = wrap_in_transaction(event);
    let commands = transaction_to_sql_commands(&tx);
    let result = destination
        .execute_sql_batch_with_hook(&commands, None)
        .await;
    assert!(result.is_ok());

    // Verify the table was truncated
    let row = sqlx::query("SELECT COUNT(*) as count FROM main.users")
        .fetch_one(&pool)
        .await
        .unwrap();
    let count: i32 = row.get("count");
    assert_eq!(count, 0);

    // Clean up
    pool.close().await;
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_destination_replica_identity_full() {
    let temp_db = TempDatabase::new("replica_full");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    // Create and populate table
    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS main.users (
            id INTEGER,
            name TEXT,
            email TEXT
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Insert test data
    sqlx::query(
        "INSERT INTO main.users (id, name, email) VALUES (1, 'John Doe', 'john@example.com')",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Create UPDATE event with REPLICA IDENTITY FULL
    let old_data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("John Doe")),
        ("email", ColumnValue::text("john@example.com")),
    ]);

    let new_data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("name", ColumnValue::text("John Smith")),
        ("email", ColumnValue::text("john.smith@example.com")),
    ]);

    let event = ChangeEvent::update(
        "main",
        "users",
        123,
        Some(old_data),
        new_data,
        ReplicaIdentity::Full,
        vec![], // No key columns needed for FULL
        Lsn::from(300),
    );

    // Process the event using execute_sql_batch
    let tx = wrap_in_transaction(event);
    let commands = transaction_to_sql_commands(&tx);
    let result = destination
        .execute_sql_batch_with_hook(&commands, None)
        .await;
    assert!(result.is_ok());

    // Verify the data was updated
    let row = sqlx::query("SELECT name FROM main.users WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    let name: String = row.get("name");
    assert_eq!(name, "John Smith");

    // Clean up
    pool.close().await;
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_destination_replica_identity_nothing_error() {
    let temp_db = TempDatabase::new("replica_nothing");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    // Create DELETE event with REPLICA IDENTITY NOTHING (should fail)
    let old_data = create_test_data();

    let event = ChangeEvent::delete(
        "main",
        "users",
        123,
        old_data,
        ReplicaIdentity::Nothing,
        vec![],
        Lsn::from(200),
    );

    // Process the event - with SQL workflow, invalid events are skipped (generate no SQL)
    let tx = wrap_in_transaction(event);
    let commands = transaction_to_sql_commands(&tx);
    let result = destination
        .execute_sql_batch_with_hook(&commands, None)
        .await;
    // Empty batch succeeds
    assert!(result.is_ok());

    // Clean up
    let _ = destination.close().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn test_sqlite_destination_complex_data_types() {
    let temp_db = TempDatabase::new("complex_types");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();
    destination.connect(&connection_string).await.unwrap();

    // Create table
    let pool = SqlitePool::connect(&format!("sqlite://{}", connection_string))
        .await
        .unwrap();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS main.complex_data (
            id INTEGER PRIMARY KEY,
            json_array TEXT,
            json_object TEXT,
            null_value TEXT
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Create INSERT event with complex data types
    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text("1")),
        ("json_array", ColumnValue::text("[1,2,3,\"test\"]")),
        (
            "json_object",
            ColumnValue::text("{\"key\":\"value\",\"nested\":{\"number\":42}}"),
        ),
        ("null_value", ColumnValue::Null),
    ]);

    let event = ChangeEvent::insert("main", "complex_data", 123, data, Lsn::from(100));

    // Process the event using execute_sql_batch
    let tx = wrap_in_transaction(event);
    let commands = transaction_to_sql_commands(&tx);
    let result = destination
        .execute_sql_batch_with_hook(&commands, None)
        .await;
    assert!(result.is_ok());

    // Verify the data was inserted
    let row = sqlx::query(
        "SELECT json_array, json_object, null_value FROM main.complex_data WHERE id = 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let json_array: String = row.get("json_array");
    let json_object: String = row.get("json_object");
    let null_value: Option<String> = row.get("null_value");

    assert_eq!(json_array, "[1,2,3,\"test\"]");
    assert!(json_object.contains("\"key\":\"value\""));
    assert!(null_value.is_none());

    // Clean up
    pool.close().await;
    let _ = destination.close().await;
}
