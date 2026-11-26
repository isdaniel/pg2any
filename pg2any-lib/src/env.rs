//! Environment variable loading and configuration utilities
//!
//! This module provides functions to load configuration from environment variables
//! and setup various aspects of the CDC application.

use crate::{CdcError, Config, DestinationType, SnapshotTargetPolicy};
use std::collections::HashMap;
use std::time::Duration;

/// Variables that used to be read and no longer are, with why. Setting one only logs a warning,
/// so existing env files keep starting.
const REMOVED_ENV_VARS: &[(&str, &str)] = &[
    (
        "CDC_PROTOCOL_VERSION",
        "the protocol version follows CDC_STREAMING (2 when streaming, else 1)",
    ),
    (
        "CDC_BINARY_FORMAT",
        "binary pgoutput was never supported, so it had no effect",
    ),
    ("CDC_QUERY_TIMEOUT", "it had no effect"),
    (
        "CDC_SQLSERVER_LIVE_BULK_LOAD",
        "the SQL Server live bulk load is always on",
    ),
];

/// Load configuration from environment variables
///
/// This function reads all CDC-related environment variables and builds a Config instance.
/// It provides sensible defaults for all optional configuration values.
///
/// The defaults below are the ones of this function. `Config::builder()` has its own: the slot is
/// `pg2any_slot`, the publication `pg2any_pub` and `streaming` is `false`. Moving a deployment
/// between the two without naming the slot and publication changes the slot it uses.
///
/// # Environment Variables
///
/// ## Required
/// - `CDC_SOURCE_CONNECTION_STRING`: Complete PostgreSQL connection string with replication parameter
/// - `CDC_DEST_URI`: Destination connection string, broker list (Kafka) or file path (SQLite)
///
/// ## Destination Configuration
/// - `CDC_DEST_TYPE`: "MySQL", "SqlServer", "SQLite" or "Kafka", case-insensitive (default: "MySQL").
///   Any other value is an error.
///
/// ## Schema Mapping Configuration
/// - `CDC_SCHEMA_MAPPING`: Comma-separated list of schema mappings in format "source:dest"
///   Example: "public:cdc_db,myschema:mydb" maps PostgreSQL "public" schema to MySQL "cdc_db" database
///
/// ## CDC Configuration
/// - `CDC_REPLICATION_SLOT`: Replication slot name (default: "cdc_slot")
/// - `CDC_PUBLICATION`: Publication name (default: "cdc_pub")
/// - `CDC_STREAMING`: Stream in-progress transactions, needs PostgreSQL 14+ (default: "true")
///
/// ## Timeout Configuration
/// - `CDC_CONNECTION_TIMEOUT`: Connection timeout in seconds (default: "30")
///
/// ## Performance Configuration
/// - `CDC_CHANNEL_CAPACITY`: Capacity of the transaction channel between producer and consumer (default: "1000")
///   Controls how many complete transactions can be queued. Larger values handle burst traffic better but use more memory.
///   Recommended: 1000-5000 for typical workloads, 10000+ for high-throughput scenarios.
/// - `CDC_BATCH_SIZE`: Statements per destination batch (default: "1000")
///   Also the largest transaction the in-memory fast path keeps in memory and the statement budget
///   of one group commit. Higher values improve throughput for large transactions.
///   Recommended: 1000-5000 for typical workloads, adjust based on MySQL max_allowed_packet.
/// - `CDC_BULK_INSERT_THRESHOLD`: Minimum INSERT statements in a batch to use the bulk path (default: "500")
/// - `CDC_MAX_ROWS_PER_INSERT`: Maximum rows per multi-value INSERT statement (default: "0" = destination limit)
///   SQL Server caps it at 1000 and SQLite at 500; MySQL and Kafka ignore it.
/// - `CDC_TRANSACTION_SEGMENT_SIZE_MB`: Largest transaction segment file in megabytes (default: "64")
/// - `CDC_TRANSACTION_FILE_BASE_PATH`: Directory of the transaction files and the snapshot marker (default: ".")
///
/// ## Initial Snapshot Configuration
/// - `CDC_INITIAL_SNAPSHOT`: Run a one-time snapshot of existing table data before streaming (default: "false")
/// - `CDC_SNAPSHOT_TARGET_POLICY`: Handling of non-empty target tables: "fail_if_not_empty", "truncate",
///   or "append" (default: "fail_if_not_empty")
/// - `CDC_SNAPSHOT_WRITERS`: Number of concurrent snapshot writers, must be > 0 (default: "4")
/// - `CDC_SNAPSHOT_BUFFER_MB`: Snapshot buffer budget in megabytes, must be >= 1 (default: "16")
/// - `CDC_SNAPSHOT_BATCH_ROWS`: Rows per snapshot batch, must be > 0 (default: "20000")
///
/// ## Read elsewhere
/// `CDC_LAST_LSN_FILE` (LSN position file), `PG2ANY_ENABLE_COMPRESSION` (gzip transaction files)
/// and the `CDC_KAFKA_*` options are read by the components that use them, not by this function,
/// so they cannot be set through `ConfigBuilder`.
///
/// ## Removed
/// `CDC_PROTOCOL_VERSION`, `CDC_BINARY_FORMAT`, `CDC_QUERY_TIMEOUT` and
/// `CDC_SQLSERVER_LIVE_BULK_LOAD` are ignored with a warning.
///
/// # Errors
///
/// Returns `CdcError::Config` if a required variable is missing or any value is invalid.
pub fn load_config_from_env() -> Result<Config, CdcError> {
    tracing::info!("Loading configuration from environment variables");

    for (key, why) in REMOVED_ENV_VARS {
        if std::env::var_os(key).is_some() {
            tracing::warn!("Environment variable {key} is ignored: {why}");
        }
    }

    let source_connection_string = required_env(
        "CDC_SOURCE_CONNECTION_STRING",
        "postgresql://user:password@host:port/dbname?replication=database",
    )?;
    let destination_connection_string = required_env(
        "CDC_DEST_URI",
        "mysql://replicator:pass.123@127.0.0.1:3306/publif, ./cdc_target.db for SQLite or broker:9092 for Kafka",
    )?;

    let dest_type = parse_destination_type(
        &std::env::var("CDC_DEST_TYPE").unwrap_or_else(|_| "MySQL".to_string()),
    )?;

    // Schema mapping configuration
    let schema_mappings = parse_schema_mapping_env("CDC_SCHEMA_MAPPING")?;
    if !schema_mappings.is_empty() {
        tracing::info!("Schema mappings configured: {:?}", schema_mappings);
    }

    // CDC-specific configuration
    let replication_slot =
        std::env::var("CDC_REPLICATION_SLOT").unwrap_or_else(|_| "cdc_slot".to_string());
    let publication = std::env::var("CDC_PUBLICATION").unwrap_or_else(|_| "cdc_pub".to_string());
    let streaming = parse_bool_env("CDC_STREAMING", true)?;

    let connection_timeout_secs = parse_u64_env("CDC_CONNECTION_TIMEOUT", 30)?;
    let buffer_size =
        parse_usize_env_with_fallback("CDC_CHANNEL_CAPACITY", "CDC_BUFFER_SIZE", 1000)?;
    let batch_size =
        parse_usize_env_with_fallback("CDC_BATCH_SIZE", "CDC_COMMIT_BATCH_SIZE", 1000)?;
    let segment_size_mb = parse_usize_env("CDC_TRANSACTION_SEGMENT_SIZE_MB", 64)?;
    let segment_size_bytes = segment_size_mb.saturating_mul(1024 * 1024);

    let bulk_insert_threshold = parse_usize_env("CDC_BULK_INSERT_THRESHOLD", 500)?;
    let max_rows_per_insert = parse_usize_env("CDC_MAX_ROWS_PER_INSERT", 0)?;

    // Initial snapshot configuration
    let initial_snapshot = parse_bool_env("CDC_INITIAL_SNAPSHOT", false)?;
    let snapshot_target_policy = match std::env::var("CDC_SNAPSHOT_TARGET_POLICY") {
        Ok(value) => value.parse::<SnapshotTargetPolicy>()?,
        Err(_) => SnapshotTargetPolicy::default(),
    };
    let snapshot_writers = parse_usize_env("CDC_SNAPSHOT_WRITERS", 4)?;
    let snapshot_buffer_mb = parse_usize_env("CDC_SNAPSHOT_BUFFER_MB", 16)?;
    let snapshot_buffer_bytes = snapshot_buffer_mb.saturating_mul(1024 * 1024);
    let snapshot_batch_rows = parse_usize_env("CDC_SNAPSHOT_BATCH_ROWS", 20000)?;

    // Transaction file persistence configuration
    let transaction_file_base_path =
        std::env::var("CDC_TRANSACTION_FILE_BASE_PATH").unwrap_or_else(|_| ".".to_string());

    tracing::info!(
        "CDC Config - Slot: {}, Publication: {}, Streaming: {}, Connection timeout: {}s",
        replication_slot,
        publication,
        streaming,
        connection_timeout_secs
    );

    tracing::info!(
        "Performance - Transaction Channel Size: {}, Batch Size: {}",
        buffer_size,
        batch_size
    );

    tracing::info!(
        "Transaction file segment size: {} MB ({} bytes)",
        segment_size_mb,
        segment_size_bytes
    );

    tracing::info!(
        "Transaction file persistence at: {}",
        transaction_file_base_path
    );

    // Build the configuration
    let config = Config::builder()
        .source_connection_string(source_connection_string)
        .destination_type(dest_type)
        .destination_connection_string(destination_connection_string)
        .replication_slot_name(replication_slot)
        .publication_name(publication)
        .streaming(streaming)
        .connection_timeout(Duration::from_secs(connection_timeout_secs))
        .schema_mappings(schema_mappings)
        .buffer_size(buffer_size)
        .batch_size(batch_size)
        .transaction_file_base_path(transaction_file_base_path)
        .transaction_segment_size_bytes(segment_size_bytes)
        .bulk_insert_threshold(bulk_insert_threshold)
        .max_rows_per_insert(max_rows_per_insert)
        .initial_snapshot(initial_snapshot)
        .snapshot_target_policy(snapshot_target_policy)
        .snapshot_writers(snapshot_writers)
        .snapshot_buffer_bytes(snapshot_buffer_bytes)
        .snapshot_batch_rows(snapshot_batch_rows)
        .build()?;

    tracing::info!("Configuration loaded successfully");
    Ok(config)
}

fn required_env(key: &str, example: &str) -> Result<String, CdcError> {
    std::env::var(key).map_err(|_| {
        CdcError::config(format!(
            "{key} environment variable is required. Example: {example}"
        ))
    })
}

fn parse_destination_type(value: &str) -> Result<DestinationType, CdcError> {
    [
        ("mysql", DestinationType::MySQL),
        ("sqlserver", DestinationType::SqlServer),
        ("sqlite", DestinationType::SQLite),
        ("kafka", DestinationType::Kafka),
    ]
    .into_iter()
    .find(|(name, _)| value.eq_ignore_ascii_case(name))
    .map(|(_, destination)| destination)
    .ok_or_else(|| {
        CdcError::config(format!(
            "Unknown CDC_DEST_TYPE '{value}'. Expected one of: MySQL, SqlServer, SQLite, Kafka"
        ))
    })
}

// Helper functions for parsing environment variables

/// Parse schema mapping environment variable
/// Format: "source_schema:dest_schema,source_schema2:dest_schema2"
/// Example: "public:cdc_db,myschema:mydb"
fn parse_schema_mapping_env(key: &str) -> Result<HashMap<String, String>, CdcError> {
    match std::env::var(key) {
        Ok(value) if !value.is_empty() => {
            let mut mappings = HashMap::new();
            for pair in value.split(',') {
                let pair = pair.trim();
                if pair.is_empty() {
                    continue;
                }
                let parts: Vec<&str> = pair.splitn(2, ':').collect();
                if parts.len() != 2 {
                    return Err(CdcError::config(format!(
                        "Invalid schema mapping format '{pair}'. Expected 'source:dest' format."
                    )));
                }
                let source = parts[0].trim();
                let dest = parts[1].trim();
                if source.is_empty() || dest.is_empty() {
                    return Err(CdcError::config(format!(
                        "Invalid schema mapping '{pair}'. Both source and destination must be non-empty."
                    )));
                }
                mappings.insert(source.to_string(), dest.to_string());
            }
            Ok(mappings)
        }
        _ => Ok(HashMap::new()),
    }
}

/// Parse a boolean environment variable with a default value
fn parse_bool_env(key: &str, default: bool) -> Result<bool, CdcError> {
    match std::env::var(key) {
        Ok(value) => value.parse::<bool>().map_err(|e| {
            CdcError::config(format!("Invalid boolean value for {key}: {value} ({e})"))
        }),
        Err(_) => Ok(default),
    }
}

/// Parse a u64 environment variable with a default value
fn parse_u64_env(key: &str, default: u64) -> Result<u64, CdcError> {
    match std::env::var(key) {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|e| CdcError::config(format!("Invalid u64 value for {key}: {value} ({e})"))),
        Err(_) => Ok(default),
    }
}

/// Parse a usize environment variable with a default value
fn parse_usize_env(key: &str, default: usize) -> Result<usize, CdcError> {
    match std::env::var(key) {
        Ok(value) => value
            .parse::<usize>()
            .map_err(|e| CdcError::config(format!("Invalid usize value for {key}: {value} ({e})"))),
        Err(_) => Ok(default),
    }
}

/// Parse a usize env var with a deprecated fallback name.
/// Prefers `new_key`; if absent, checks `old_key` and logs a deprecation warning.
fn parse_usize_env_with_fallback(
    new_key: &str,
    old_key: &str,
    default: usize,
) -> Result<usize, CdcError> {
    if let Ok(value) = std::env::var(new_key) {
        return value.parse::<usize>().map_err(|e| {
            CdcError::config(format!("Invalid usize value for {new_key}: {value} ({e})"))
        });
    }
    if let Ok(value) = std::env::var(old_key) {
        tracing::warn!("Environment variable {old_key} is deprecated, use {new_key} instead");
        return value.parse::<usize>().map_err(|e| {
            CdcError::config(format!("Invalid usize value for {old_key}: {value} ({e})"))
        });
    }
    Ok(default)
}
