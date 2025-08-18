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

    /// Buffer size for the event channel (kept for channel capacity)
    pub buffer_size: usize,

    /// Relay log directory for I/O thread to write events
    pub relay_log_directory: Option<String>,

    /// Whether to create missing destination tables automatically
    pub auto_create_tables: bool,

    /// Table mapping for transformation between source and destination
    pub table_mappings: HashMap<String, TableMapping>,

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
            buffer_size: 1000,
            relay_log_directory: None,
            auto_create_tables: true,
            table_mappings: HashMap::new(),
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

    /// Set buffer size
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }

    /// Set relay log directory
    pub fn relay_log_directory<S: Into<String>>(mut self, directory: Option<S>) -> Self {
        self.config.relay_log_directory = directory.map(|d| d.into());
        self
    }

    /// Enable/disable automatic table creation
    pub fn auto_create_tables(mut self, enabled: bool) -> Self {
        self.config.auto_create_tables = enabled;
        self
    }

    /// Add a table mapping
    pub fn table_mapping<S: Into<String>>(mut self, table_name: S, mapping: TableMapping) -> Self {
        self.config
            .table_mappings
            .insert(table_name.into(), mapping);
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
