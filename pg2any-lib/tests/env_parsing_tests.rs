use std::env;
use std::sync::Mutex;

static ENV_LOCK: Mutex<()> = Mutex::new(());

/// A test that fails while holding the lock must not fail every later test with a poisoned lock.
fn lock_env() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn clear_env_vars() {
    for key in &[
        "CDC_SOURCE_CONNECTION_STRING",
        "CDC_DEST_TYPE",
        "CDC_DEST_URI",
        "CDC_SCHEMA_MAPPING",
        "CDC_REPLICATION_SLOT",
        "CDC_PUBLICATION",
        "CDC_PROTOCOL_VERSION",
        "CDC_BINARY_FORMAT",
        "CDC_STREAMING",
        "CDC_CONNECTION_TIMEOUT",
        "CDC_QUERY_TIMEOUT",
        "CDC_BUFFER_SIZE",
        "CDC_COMMIT_BATCH_SIZE",
        "CDC_CHANNEL_CAPACITY",
        "CDC_BATCH_SIZE",
        "CDC_TRANSACTION_SEGMENT_SIZE_MB",
        "CDC_BULK_INSERT_THRESHOLD",
        "CDC_TRANSACTION_FILE_BASE_PATH",
        "CDC_INITIAL_SNAPSHOT",
        "CDC_SNAPSHOT_TARGET_POLICY",
        "CDC_SNAPSHOT_WRITERS",
        "CDC_SNAPSHOT_BUFFER_MB",
        "CDC_SNAPSHOT_BATCH_ROWS",
        "CDC_SQLSERVER_LIVE_BULK_LOAD",
    ] {
        env::remove_var(key);
    }
}

fn set_required_env() {
    env::set_var(
        "CDC_SOURCE_CONNECTION_STRING",
        "postgresql://user:pass@localhost:5432/db?replication=database",
    );
    env::set_var("CDC_DEST_URI", "mysql://user:pass@localhost:3306/testdb");
}

#[test]
fn test_new_env_names_preferred() {
    let _lock = lock_env();
    clear_env_vars();
    set_required_env();
    env::set_var("CDC_CHANNEL_CAPACITY", "2000");
    env::set_var("CDC_BATCH_SIZE", "3000");

    let config = pg2any_lib::load_config_from_env().unwrap();
    assert_eq!(config.buffer_size, 2000);
    assert_eq!(config.batch_size, 3000);

    clear_env_vars();
}

#[test]
fn test_deprecated_env_names_still_work() {
    let _lock = lock_env();
    clear_env_vars();
    set_required_env();
    env::set_var("CDC_BUFFER_SIZE", "4000");
    env::set_var("CDC_COMMIT_BATCH_SIZE", "5000");

    let config = pg2any_lib::load_config_from_env().unwrap();
    assert_eq!(config.buffer_size, 4000);
    assert_eq!(config.batch_size, 5000);

    clear_env_vars();
}

#[test]
fn test_new_name_takes_precedence_over_deprecated() {
    let _lock = lock_env();
    clear_env_vars();
    set_required_env();
    env::set_var("CDC_CHANNEL_CAPACITY", "6000");
    env::set_var("CDC_BUFFER_SIZE", "9999");
    env::set_var("CDC_BATCH_SIZE", "7000");
    env::set_var("CDC_COMMIT_BATCH_SIZE", "9999");

    let config = pg2any_lib::load_config_from_env().unwrap();
    assert_eq!(config.buffer_size, 6000);
    assert_eq!(config.batch_size, 7000);

    clear_env_vars();
}

#[test]
fn test_defaults_when_no_env_set() {
    let _lock = lock_env();
    clear_env_vars();
    set_required_env();

    let config = pg2any_lib::load_config_from_env().unwrap();
    assert_eq!(config.buffer_size, 1000);
    assert_eq!(config.batch_size, 1000);
    assert_eq!(config.bulk_insert_threshold, 500);

    clear_env_vars();
}

#[test]
fn test_schema_mapping_parsing() {
    let _lock = lock_env();
    clear_env_vars();
    set_required_env();
    env::set_var("CDC_SCHEMA_MAPPING", "public:cdc_db,sales:sales_db");

    let config = pg2any_lib::load_config_from_env().unwrap();
    assert_eq!(config.schema_mappings.get("public").unwrap(), "cdc_db");
    assert_eq!(config.schema_mappings.get("sales").unwrap(), "sales_db");

    clear_env_vars();
}

#[test]
fn test_invalid_schema_mapping_returns_error() {
    let _lock = lock_env();
    clear_env_vars();
    set_required_env();
    env::set_var("CDC_SCHEMA_MAPPING", "invalid_no_colon");

    let result = pg2any_lib::load_config_from_env();
    assert!(result.is_err());

    clear_env_vars();
}

#[test]
fn test_snapshot_env_parsed() {
    let _lock = lock_env();
    clear_env_vars();
    set_required_env();
    env::set_var("CDC_INITIAL_SNAPSHOT", "true");
    env::set_var("CDC_SNAPSHOT_TARGET_POLICY", "truncate");
    env::set_var("CDC_SNAPSHOT_WRITERS", "8");
    env::set_var("CDC_SNAPSHOT_BUFFER_MB", "48");
    env::set_var("CDC_SNAPSHOT_BATCH_ROWS", "250");

    let config = pg2any_lib::load_config_from_env().unwrap();
    assert!(config.initial_snapshot);
    assert_eq!(
        config.snapshot_target_policy,
        pg2any_lib::SnapshotTargetPolicy::Truncate
    );
    assert_eq!(config.snapshot_writers, 8);
    assert_eq!(config.snapshot_buffer_bytes, 48 * 1024 * 1024);
    assert_eq!(config.snapshot_batch_rows, 250);

    clear_env_vars();
}

#[test]
fn test_snapshot_env_defaults() {
    let _lock = lock_env();
    clear_env_vars();
    set_required_env();

    let config = pg2any_lib::load_config_from_env().unwrap();
    assert!(!config.initial_snapshot);
    assert_eq!(
        config.snapshot_target_policy,
        pg2any_lib::SnapshotTargetPolicy::FailIfNotEmpty
    );
    assert_eq!(config.snapshot_writers, 4);
    assert_eq!(config.snapshot_buffer_bytes, 16 * 1024 * 1024);
    assert_eq!(config.snapshot_batch_rows, 20000);

    clear_env_vars();
}

#[test]
fn test_snapshot_invalid_policy_returns_error() {
    let _lock = lock_env();
    clear_env_vars();
    set_required_env();
    env::set_var("CDC_SNAPSHOT_TARGET_POLICY", "bogus");

    assert!(pg2any_lib::load_config_from_env().is_err());

    clear_env_vars();
}

#[test]
fn test_required_variables_missing_return_errors() {
    let _lock = lock_env();
    clear_env_vars();

    // Neither required variable set.
    assert!(matches!(
        pg2any_lib::load_config_from_env(),
        Err(pg2any_lib::CdcError::Config(_))
    ));

    // Only the source set: the destination is still required.
    env::set_var(
        "CDC_SOURCE_CONNECTION_STRING",
        "postgresql://user:pass@localhost:5432/db?replication=database",
    );
    assert!(matches!(
        pg2any_lib::load_config_from_env(),
        Err(pg2any_lib::CdcError::Config(_))
    ));

    clear_env_vars();
}

#[test]
fn test_unknown_destination_type_returns_error() {
    let _lock = lock_env();
    clear_env_vars();
    set_required_env();
    env::set_var("CDC_DEST_TYPE", "Kafak");

    let err = pg2any_lib::load_config_from_env().unwrap_err();
    assert!(err.to_string().contains("Kafak"), "got {err}");

    clear_env_vars();
}

#[test]
fn test_destination_type_names_are_case_insensitive() {
    let _lock = lock_env();
    clear_env_vars();
    set_required_env();

    for (name, expected) in [
        ("MySQL", pg2any_lib::DestinationType::MySQL),
        ("mysql", pg2any_lib::DestinationType::MySQL),
        ("SQLSERVER", pg2any_lib::DestinationType::SqlServer),
        ("SqlServer", pg2any_lib::DestinationType::SqlServer),
        ("sqlite", pg2any_lib::DestinationType::SQLite),
        ("Kafka", pg2any_lib::DestinationType::Kafka),
    ] {
        env::set_var("CDC_DEST_TYPE", name);
        let config = pg2any_lib::load_config_from_env().unwrap();
        assert_eq!(config.destination_type, expected, "{name}");
    }

    clear_env_vars();
}

#[test]
fn test_defaults_alone_are_a_valid_streaming_config() {
    let _lock = lock_env();
    clear_env_vars();
    set_required_env();

    let config = pg2any_lib::load_config_from_env().unwrap();
    assert!(config.streaming);

    clear_env_vars();
}

#[test]
fn test_removed_variables_are_ignored() {
    let _lock = lock_env();
    clear_env_vars();
    set_required_env();
    env::set_var("CDC_PROTOCOL_VERSION", "3");
    env::set_var("CDC_BINARY_FORMAT", "not-a-bool");
    env::set_var("CDC_QUERY_TIMEOUT", "not-a-number");
    env::set_var("CDC_SQLSERVER_LIVE_BULK_LOAD", "true");

    assert!(pg2any_lib::load_config_from_env().is_ok());

    env::remove_var("CDC_PROTOCOL_VERSION");
    env::remove_var("CDC_BINARY_FORMAT");
    env::remove_var("CDC_QUERY_TIMEOUT");
    env::remove_var("CDC_SQLSERVER_LIVE_BULK_LOAD");
    clear_env_vars();
}
