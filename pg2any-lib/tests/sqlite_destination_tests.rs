use pg2any_lib::{
    destinations::{DestinationFactory, DestinationHandler},
    types::{ChangeEvent, DestinationType, EventType, ReplicaIdentity},
};
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

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
fn create_test_data() -> HashMap<String, serde_json::Value> {
    let mut data = HashMap::new();
    data.insert("id".to_string(), json!(1));
    data.insert("name".to_string(), json!("John Doe"));
    data.insert("age".to_string(), json!(30));
    data.insert("email".to_string(), json!("john@example.com"));
    data.insert("active".to_string(), json!(true));
    data.insert("salary".to_string(), json!(50000.50));
    data
}

/// Helper function to create updated test data
fn create_updated_test_data() -> HashMap<String, serde_json::Value> {
    let mut data = HashMap::new();
    data.insert("id".to_string(), json!(1));
    data.insert("name".to_string(), json!("John Smith"));
    data.insert("age".to_string(), json!(31));
    data.insert("email".to_string(), json!("john.smith@example.com"));
    data.insert("active".to_string(), json!(true));
    data.insert("salary".to_string(), json!(55000.75));
    data
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
    let destination = DestinationFactory::create(DestinationType::SQLite);
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
async fn test_sqlite_destination_health_check() {
    let temp_db = TempDatabase::new("health");
    let connection_string = temp_db.connection_string();

    let mut destination = SQLiteDestination::new();

    // Health check should fail without connection
    let result = destination.health_check().await;
    assert!(result.is_err());

    // Connect and test health check
    destination.connect(&connection_string).await.unwrap();
    let health_result = destination.health_check().await;
    assert!(health_result.is_ok());
    assert!(health_result.unwrap());

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
    let event = ChangeEvent::insert("main".to_string(), "users".to_string(), 123, data);

    // Process the event
    let result = destination.process_event(&event).await;
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
    let key_columns = vec!["id".to_string()];

    let event = ChangeEvent::update(
        "main".to_string(),
        "users".to_string(),
        123,
        Some(old_data),
        new_data,
        ReplicaIdentity::Default,
        key_columns,
    );

    // Process the event
    let result = destination.process_event(&event).await;
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
    let key_columns = vec!["id".to_string()];

    let event = ChangeEvent::delete(
        "main".to_string(),
        "users".to_string(),
        123,
        old_data,
        ReplicaIdentity::Default,
        key_columns,
    );

    // Process the event
    let result = destination.process_event(&event).await;
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
    let event = ChangeEvent {
        event_type: EventType::Truncate(vec!["main.users".to_string()]),
        lsn: None,
        metadata: None,
    };

    // Process the event
    let result = destination.process_event(&event).await;
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
    let mut old_data = HashMap::new();
    old_data.insert("id".to_string(), json!(1));
    old_data.insert("name".to_string(), json!("John Doe"));
    old_data.insert("email".to_string(), json!("john@example.com"));

    let mut new_data = HashMap::new();
    new_data.insert("id".to_string(), json!(1));
    new_data.insert("name".to_string(), json!("John Smith"));
    new_data.insert("email".to_string(), json!("john.smith@example.com"));

    let event = ChangeEvent::update(
        "main".to_string(),
        "users".to_string(),
        123,
        Some(old_data),
        new_data,
        ReplicaIdentity::Full,
        vec![], // No key columns needed for FULL
    );

    // Process the event
    let result = destination.process_event(&event).await;
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
        "main".to_string(),
        "users".to_string(),
        123,
        old_data,
        ReplicaIdentity::Nothing,
        vec![],
    );

    // Process the event - should fail
    let result = destination.process_event(&event).await;
    assert!(result.is_err());

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
    let mut data = HashMap::new();
    data.insert("id".to_string(), json!(1));
    data.insert("json_array".to_string(), json!([1, 2, 3, "test"]));
    data.insert(
        "json_object".to_string(),
        json!({"key": "value", "nested": {"number": 42}}),
    );
    data.insert("null_value".to_string(), json!(null));

    let event = ChangeEvent::insert("main".to_string(), "complex_data".to_string(), 123, data);

    // Process the event
    let result = destination.process_event(&event).await;
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
