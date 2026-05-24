use std::env;
use std::sync::Mutex;

static ENV_LOCK: Mutex<()> = Mutex::new(());

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
    env::set_var("CDC_PROTOCOL_VERSION", "2");
}

#[test]
fn test_new_env_names_preferred() {
    let _lock = ENV_LOCK.lock().unwrap();
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
    let _lock = ENV_LOCK.lock().unwrap();
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
    let _lock = ENV_LOCK.lock().unwrap();
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
    let _lock = ENV_LOCK.lock().unwrap();
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
    let _lock = ENV_LOCK.lock().unwrap();
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
    let _lock = ENV_LOCK.lock().unwrap();
    clear_env_vars();
    set_required_env();
    env::set_var("CDC_SCHEMA_MAPPING", "invalid_no_colon");

    let result = pg2any_lib::load_config_from_env();
    assert!(result.is_err());

    clear_env_vars();
}
