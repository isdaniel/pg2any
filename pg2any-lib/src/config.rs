use pg_walstream::RetryConfig;

use crate::error::{CdcError, Result};
use crate::types::DestinationType;
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for the CDC client
#[derive(Debug, Clone)]
pub struct Config {
    /// Source PostgreSQL connection string
    pub source_connection_string: String,

    /// Destination database type
    pub destination_type: DestinationType,

    /// Destination database connection string
    pub destination_connection_string: String,

    /// Replication slot name to create/use
    pub replication_slot_name: String,

    /// Publication name to subscribe to
    pub publication_name: String,

    /// Protocol version to use (1-4)
    pub protocol_version: u32,

    /// Whether to use binary format
    pub binary_format: bool,

    /// Whether to stream in-progress transactions
    pub streaming: bool,

    /// Whether to include user messages
    pub include_messages: bool,

    /// Whether to include two-phase commit transactions
    pub two_phase: bool,

    /// Origin filtering
    pub origin: OriginFilter,

    /// Connection timeout
    pub connection_timeout: Duration,

    /// Query timeout
    pub query_timeout: Duration,

    /// Heartbeat interval for sending feedback to PostgreSQL
    pub heartbeat_interval: Duration,

    /// Maximum number of retry attempts for connection failures
    pub max_retry_attempts: u32,

    /// Initial retry delay (will be increased exponentially)
    pub initial_retry_delay: Duration,

    /// Maximum retry delay between attempts
    pub max_retry_delay: Duration,

    /// Retry multiplier for exponential backoff
    pub retry_multiplier: f64,

    /// Maximum total time to spend retrying before giving up
    pub max_retry_duration: Duration,

    /// Whether to add jitter to retry delays to prevent thundering herd
    pub retry_jitter: bool,

    /// Transaction channel capacity between producer and consumer
    pub buffer_size: usize,

    /// Table mapping for transformation between source and destination
    pub table_mappings: HashMap<String, TableMapping>,

    /// Schema mapping from PostgreSQL schema to destination database/schema
    /// Maps source schema (e.g., "public") to destination schema/database (e.g., "cdc_db")
    pub schema_mappings: HashMap<String, String>,

    /// Batch size for multi-value INSERT statements in destination database
    /// Higher values improve throughput for large streaming transactions
    /// but may increase memory usage. Default: 1000
    pub batch_size: usize,

    /// Base path for transaction files (default: current directory)
    /// Contains sql_received_tx/ and sql_pending_tx/ subdirectories
    /// Transaction file persistence is always enabled for data safety
    pub transaction_file_base_path: String,

    /// Maximum size in bytes for a single transaction segment file
    pub transaction_segment_size_bytes: usize,

    /// Additional configuration options
    pub extra_options: HashMap<String, String>,
}

/// Origin filtering options
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OriginFilter {
    /// Only changes with no origin
    None,
    /// Changes regardless of origin
    Any,
    /// Changes from specific origin
    Origin(String),
}

/// Table mapping configuration
#[derive(Debug, Clone)]
pub struct TableMapping {
    /// Destination schema name
    pub destination_schema: Option<String>,

    /// Destination table name
    pub destination_table: Option<String>,

    /// Column mappings (source_column -> destination_column)
    pub column_mappings: HashMap<String, String>,

    /// Columns to exclude from replication
    pub excluded_columns: Vec<String>,

    /// Custom transformation rules
    pub transformations: Vec<ColumnTransformation>,

    /// Whether to include this table in replication
    pub enabled: bool,
}

/// Column transformation rules
#[derive(Debug, Clone)]
pub struct ColumnTransformation {
    /// Source column name
    pub source_column: String,

    /// Destination column name
    pub destination_column: String,

    /// Transformation type
    pub transformation: TransformationType,
}

/// Types of column transformations
#[derive(Debug, Clone)]
pub enum TransformationType {
    /// Pass through without modification
    Identity,

    /// Convert to uppercase
    Uppercase,

    /// Convert to lowercase
    Lowercase,

    /// Custom function transformation
    Function(String),

    /// Static value replacement
    Static(serde_json::Value),

    /// Null replacement
    Null,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            source_connection_string: String::new(),
            destination_type: DestinationType::MySQL,
            destination_connection_string: String::new(),
            replication_slot_name: "pg2any_slot".to_string(),
            publication_name: "pg2any_pub".to_string(),
            protocol_version: 1,
            binary_format: false,
            streaming: false,
            include_messages: false,
            two_phase: false,
            origin: OriginFilter::None,
            connection_timeout: Duration::from_secs(30),
            query_timeout: Duration::from_secs(60),
            heartbeat_interval: Duration::from_secs(10), // Send feedback every 10 seconds to prevent 60s timeout
            max_retry_attempts: 5,
            initial_retry_delay: Duration::from_secs(1),
            max_retry_delay: Duration::from_secs(60),
            retry_multiplier: 2.0,
            max_retry_duration: Duration::from_secs(300), // 5 minutes total
            retry_jitter: true,
            buffer_size: 1000,
            table_mappings: HashMap::new(),
            schema_mappings: HashMap::new(),
            batch_size: 1000,
            transaction_file_base_path: ".".to_string(),
            transaction_segment_size_bytes: 64 * 1024 * 1024,
            extra_options: HashMap::new(),
        }
    }
}

/// Builder pattern for creating configuration
#[derive(Debug, Default)]
pub struct ConfigBuilder {
    config: Config,
}

impl ConfigBuilder {
    /// Create a new config builder
    pub fn new() -> Self {
        Self {
            config: Config::default(),
        }
    }

    /// Set the source PostgreSQL connection string
    pub fn source_connection_string<S: Into<String>>(mut self, connection_string: S) -> Self {
        self.config.source_connection_string = connection_string.into();
        self
    }

    /// Set the destination database type
    pub fn destination_type(mut self, destination_type: DestinationType) -> Self {
        self.config.destination_type = destination_type;
        self
    }

    /// Set the destination database connection string
    pub fn destination_connection_string<S: Into<String>>(mut self, connection_string: S) -> Self {
        self.config.destination_connection_string = connection_string.into();
        self
    }

    /// Set the replication slot name
    pub fn replication_slot_name<S: Into<String>>(mut self, slot_name: S) -> Self {
        self.config.replication_slot_name = slot_name.into();
        self
    }

    /// Set the publication name
    pub fn publication_name<S: Into<String>>(mut self, pub_name: S) -> Self {
        self.config.publication_name = pub_name.into();
        self
    }

    /// Set the protocol version (1-4)
    pub fn protocol_version(mut self, version: u32) -> Self {
        self.config.protocol_version = version;
        self
    }

    /// Enable/disable binary format
    pub fn binary_format(mut self, enabled: bool) -> Self {
        self.config.binary_format = enabled;
        self
    }

    /// Enable/disable streaming of in-progress transactions
    pub fn streaming(mut self, enabled: bool) -> Self {
        self.config.streaming = enabled;
        self
    }

    /// Enable/disable including user messages
    pub fn include_messages(mut self, enabled: bool) -> Self {
        self.config.include_messages = enabled;
        self
    }

    /// Enable/disable two-phase commit transactions
    pub fn two_phase(mut self, enabled: bool) -> Self {
        self.config.two_phase = enabled;
        self
    }

    /// Set origin filtering
    pub fn origin(mut self, origin: OriginFilter) -> Self {
        self.config.origin = origin;
        self
    }

    /// Set connection timeout
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.config.connection_timeout = timeout;
        self
    }

    /// Set query timeout
    pub fn query_timeout(mut self, timeout: Duration) -> Self {
        self.config.query_timeout = timeout;
        self
    }

    /// Set heartbeat interval
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    /// Set maximum number of retry attempts
    pub fn max_retry_attempts(mut self, attempts: u32) -> Self {
        self.config.max_retry_attempts = attempts;
        self
    }

    /// Set initial retry delay
    pub fn initial_retry_delay(mut self, delay: Duration) -> Self {
        self.config.initial_retry_delay = delay;
        self
    }

    /// Set maximum retry delay
    pub fn max_retry_delay(mut self, delay: Duration) -> Self {
        self.config.max_retry_delay = delay;
        self
    }

    /// Set retry multiplier for exponential backoff
    pub fn retry_multiplier(mut self, multiplier: f64) -> Self {
        self.config.retry_multiplier = multiplier;
        self
    }

    /// Set maximum retry duration
    pub fn max_retry_duration(mut self, duration: Duration) -> Self {
        self.config.max_retry_duration = duration;
        self
    }

    /// Enable/disable retry jitter
    pub fn retry_jitter(mut self, enabled: bool) -> Self {
        self.config.retry_jitter = enabled;
        self
    }

    /// Set buffer size
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }

    /// Higher values improve throughput for large streaming transactions
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size.max(1);
        self
    }

    /// Add a table mapping
    pub fn table_mapping<S: Into<String>>(mut self, table_name: S, mapping: TableMapping) -> Self {
        self.config
            .table_mappings
            .insert(table_name.into(), mapping);
        self
    }

    /// Add a schema mapping (maps source schema to destination schema/database)
    pub fn schema_mapping<S1, S2>(mut self, source_schema: S1, destination_schema: S2) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        self.config
            .schema_mappings
            .insert(source_schema.into(), destination_schema.into());
        self
    }

    /// Set all schema mappings at once
    pub fn schema_mappings(mut self, mappings: HashMap<String, String>) -> Self {
        self.config.schema_mappings = mappings;
        self
    }

    /// Set the base path for transaction files
    /// Transaction file persistence is always enabled for data safety
    pub fn transaction_file_base_path<S: Into<String>>(mut self, path: S) -> Self {
        self.config.transaction_file_base_path = path.into();
        self
    }

    /// Set the maximum size for a transaction segment file
    pub fn transaction_segment_size_bytes(mut self, size: usize) -> Self {
        self.config.transaction_segment_size_bytes = size;
        self
    }

    /// Add extra option
    pub fn extra_option<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.config.extra_options.insert(key.into(), value.into());
        self
    }

    /// Build the configuration
    pub fn build(self) -> Result<Config> {
        // Validate the configuration
        self.validate()?;
        Ok(self.config)
    }

    /// Validate the configuration
    fn validate(&self) -> Result<()> {
        if self.config.source_connection_string.is_empty() {
            return Err(CdcError::config("Source connection string is required"));
        }

        if self.config.destination_connection_string.is_empty() {
            return Err(CdcError::config(
                "Destination connection string is required",
            ));
        }

        if self.config.replication_slot_name.is_empty() {
            return Err(CdcError::config("Replication slot name is required"));
        }

        if self.config.publication_name.is_empty() {
            return Err(CdcError::config("Publication name is required"));
        }

        if !(1..=4).contains(&self.config.protocol_version) {
            return Err(CdcError::config("Protocol version must be between 1 and 4"));
        }

        if self.config.streaming && self.config.protocol_version < 2 {
            return Err(CdcError::config(
                "Streaming requires protocol version 2 or higher",
            ));
        }

        if self.config.two_phase && self.config.protocol_version < 3 {
            return Err(CdcError::config(
                "Two-phase commit requires protocol version 3 or higher",
            ));
        }

        if self.config.buffer_size == 0 {
            return Err(CdcError::config("Buffer size must be greater than 0"));
        }

        Ok(())
    }
}

impl Config {
    /// Create a new config builder
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::new()
    }
}

impl Default for TableMapping {
    fn default() -> Self {
        Self {
            destination_schema: None,
            destination_table: None,
            column_mappings: HashMap::new(),
            excluded_columns: Vec::new(),
            transformations: Vec::new(),
            enabled: true,
        }
    }
}

impl TableMapping {
    /// Create a new table mapping
    pub fn new() -> Self {
        Self::default()
    }

    /// Set destination schema
    pub fn destination_schema<S: Into<String>>(mut self, schema: S) -> Self {
        self.destination_schema = Some(schema.into());
        self
    }

    /// Set destination table
    pub fn destination_table<S: Into<String>>(mut self, table: S) -> Self {
        self.destination_table = Some(table.into());
        self
    }

    /// Add column mapping
    pub fn column_mapping<S1, S2>(mut self, source: S1, destination: S2) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        self.column_mappings
            .insert(source.into(), destination.into());
        self
    }

    /// Exclude column from replication
    pub fn exclude_column<S: Into<String>>(mut self, column: S) -> Self {
        self.excluded_columns.push(column.into());
        self
    }

    /// Add column transformation
    pub fn transformation(mut self, transformation: ColumnTransformation) -> Self {
        self.transformations.push(transformation);
        self
    }

    /// Enable/disable this mapping
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

/// Convert from CDC config to replication stream config
impl From<&Config> for pg_walstream::ReplicationStreamConfig {
    fn from(config: &Config) -> Self {
        let streaming_mode = if config.streaming {
            pg_walstream::StreamingMode::On
        } else {
            pg_walstream::StreamingMode::Off
        };

        pg_walstream::ReplicationStreamConfig::new(
            config.replication_slot_name.clone(),
            config.publication_name.clone(),
            config.protocol_version,
            streaming_mode,
            config.heartbeat_interval,
            config.connection_timeout,
            Duration::from_secs(30), // Health check interval
            RetryConfig::default(),
        )
    }
}
