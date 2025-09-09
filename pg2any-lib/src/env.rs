//! Environment variable loading and configuration utilities
//!
//! This module provides functions to load configuration from environment variables
//! and setup various aspects of the CDC application.

use crate::{CdcError, Config, DestinationType};
use std::time::Duration;

/// Load configuration from environment variables
///
/// This function reads all CDC-related environment variables and builds a Config instance.
/// It provides sensible defaults for all optional configuration values.
///
/// # Environment Variables
///
/// ## Source PostgreSQL Configuration
/// - `CDC_SOURCE_CONNECTION_STRING`: Complete PostgreSQL connection string with replication parameter
///   (default: constructed from individual parameters below)
///
/// ## Destination Configuration
/// - `CDC_DEST_TYPE`: Destination type ("MySQL", "SqlServer", or "SQLite", default: "MySQL")
/// - `CDC_DEST_URI`: Destination URI/host/file path (default: "localhost" for databases, "./cdc_target.db" for SQLite)
/// - `CDC_DEST_PORT`: Destination port (default: "3306" for MySQL, "1433" for SqlServer) - Not used for SQLite
/// - `CDC_DEST_DB`: Destination database name (default: "cdc_target") - For SQLite, this is the database file path
/// - `CDC_DEST_USER`: Destination username (default: "cdc_user") - Not used for SQLite
/// - `CDC_DEST_PASSWORD`: Destination password (default: "cdc_password") - Not used for SQLite
///
/// ## CDC Configuration
/// - `CDC_REPLICATION_SLOT`: Replication slot name (default: "cdc_slot")
/// - `CDC_PUBLICATION`: Publication name (default: "cdc_pub")
/// - `CDC_PROTOCOL_VERSION`: Protocol version (default: "1")
/// - `CDC_BINARY_FORMAT`: Use binary format (default: "false")
/// - `CDC_STREAMING`: Enable streaming (default: "true")
///
/// ## Timeout Configuration
/// - `CDC_CONNECTION_TIMEOUT`: Connection timeout in seconds (default: "30")
/// - `CDC_QUERY_TIMEOUT`: Query timeout in seconds (default: "10")
/// - `CDC_HEARTBEAT_INTERVAL`: Heartbeat interval in seconds (default: "10")
///
/// # Errors
///
/// Returns `CdcError` if any required configuration is invalid or missing critical values.
pub fn load_config_from_env() -> Result<Config, CdcError> {
    tracing::info!("Loading configuration from environment variables");

    // Source PostgreSQL configuration
    let source_connection_string = std::env::var("CDC_SOURCE_CONNECTION_STRING").expect(
        "CDC_SOURCE_CONNECTION_STRING environment variable is required. Example: postgresql://user:password@host:port/dbname?replication=database",
    );

    // Destination configuration
    let dest_type_str = std::env::var("CDC_DEST_TYPE").unwrap_or_else(|_| "MySQL".to_string());
    let dest_type = match dest_type_str.as_str() {
        "MySQL" | "mysql" => DestinationType::MySQL,
        "SqlServer" | "sqlserver" => DestinationType::SqlServer,
        "SQLite" | "sqlite" => DestinationType::SQLite,
        _ => {
            tracing::warn!(
                "Unknown destination type '{}', defaulting to MySQL",
                dest_type_str
            );
            DestinationType::MySQL
        }
    };

    let destination_connection_string = std::env::var("CDC_DEST_URI").expect(
        "CDC_DEST_URI environment variable is required. Example for MySQL mysql://replicator:pass.123@127.0.0.1:3306/publif or ./cdc_target.db for SQLite ..etc",
    );

    // CDC-specific configuration
    let replication_slot =
        std::env::var("CDC_REPLICATION_SLOT").unwrap_or_else(|_| "cdc_slot".to_string());
    let publication = std::env::var("CDC_PUBLICATION").unwrap_or_else(|_| "cdc_pub".to_string());

    let protocol_version = parse_u32_env("CDC_PROTOCOL_VERSION", 1)?;
    let binary_format = parse_bool_env("CDC_BINARY_FORMAT", false)?;
    let streaming = parse_bool_env("CDC_STREAMING", true)?;

    // Timeout configurations
    let connection_timeout_secs = parse_u64_env("CDC_CONNECTION_TIMEOUT", 30)?;
    let query_timeout_secs = parse_u64_env("CDC_QUERY_TIMEOUT", 10)?;
    let heartbeat_interval_secs = parse_u64_env("CDC_HEARTBEAT_INTERVAL", 10)?;

    tracing::info!(
        "CDC Config - Slot: {}, Publication: {}, Protocol: {}, Streaming: {}, Binary: {}",
        replication_slot,
        publication,
        protocol_version,
        streaming,
        binary_format
    );

    tracing::info!(
        "Timeouts - Connection: {}s, Query: {}s, Heartbeat: {}s",
        connection_timeout_secs,
        query_timeout_secs,
        heartbeat_interval_secs
    );

    // Build the configuration
    let config = Config::builder()
        .source_connection_string(source_connection_string)
        .destination_type(dest_type)
        .destination_connection_string(destination_connection_string)
        .replication_slot_name(replication_slot)
        .publication_name(publication)
        .protocol_version(protocol_version)
        .binary_format(binary_format)
        .streaming(streaming)
        .connection_timeout(Duration::from_secs(connection_timeout_secs))
        .query_timeout(Duration::from_secs(query_timeout_secs))
        .heartbeat_interval(Duration::from_secs(heartbeat_interval_secs))
        .buffer_size(500)
        .build()?;

    tracing::info!("Configuration loaded successfully");
    Ok(config)
}

// Helper functions for parsing environment variables

/// Parse a boolean environment variable with a default value
fn parse_bool_env(key: &str, default: bool) -> Result<bool, CdcError> {
    match std::env::var(key) {
        Ok(value) => value.parse::<bool>().map_err(|e| {
            CdcError::config(format!(
                "Invalid boolean value for {}: {} ({})",
                key, value, e
            ))
        }),
        Err(_) => Ok(default),
    }
}

/// Parse a u32 environment variable with a default value
fn parse_u32_env(key: &str, default: u32) -> Result<u32, CdcError> {
    match std::env::var(key) {
        Ok(value) => value.parse::<u32>().map_err(|e| {
            CdcError::config(format!("Invalid u32 value for {}: {} ({})", key, value, e))
        }),
        Err(_) => Ok(default),
    }
}

/// Parse a u64 environment variable with a default value
fn parse_u64_env(key: &str, default: u64) -> Result<u64, CdcError> {
    match std::env::var(key) {
        Ok(value) => value.parse::<u64>().map_err(|e| {
            CdcError::config(format!("Invalid u64 value for {}: {} ({})", key, value, e))
        }),
        Err(_) => Ok(default),
    }
}
