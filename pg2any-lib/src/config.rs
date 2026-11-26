use pg_walstream::RetryConfig;

use crate::destinations::{DestinationFactoryFn, DestinationHandler};
use crate::error::{CdcError, Result};
use crate::types::DestinationType;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for the CDC client
///
/// `Config::builder()` and `load_config_from_env` have different defaults for the replication slot
/// (`pg2any_slot` / `cdc_slot`), the publication (`pg2any_pub` / `cdc_pub`) and `streaming`
/// (off / on). Name the slot and publication to get the same one from either.
#[derive(Clone)]
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

    /// Whether to stream in-progress transactions. Requests pgoutput protocol 2 (PostgreSQL 14+);
    /// without it protocol 1 is requested. Protocols 3 and 4 only add two-phase and parallel
    /// streaming, which pg2any does not support.
    pub streaming: bool,

    /// Connection timeout
    pub connection_timeout: Duration,

    /// Heartbeat interval for sending feedback to PostgreSQL
    pub heartbeat_interval: Duration,

    /// Transaction channel capacity between producer and consumer
    pub buffer_size: usize,

    /// Schema mapping from PostgreSQL schema to destination database/schema
    /// Maps source schema (e.g., "public") to destination schema/database (e.g., "cdc_db")
    pub schema_mappings: HashMap<String, String>,

    /// Statements per destination batch. It is also the largest transaction the in-memory fast
    /// path keeps in memory and the statement budget of one group commit. Higher values improve
    /// throughput for large transactions but use more memory. Default: 1000
    pub batch_size: usize,

    /// Base path for transaction files (default: current directory)
    /// Contains sql_received_tx/ and sql_pending_tx/ subdirectories
    /// Transaction file persistence is always enabled for data safety
    pub transaction_file_base_path: String,

    /// Maximum size in bytes for a single transaction segment file
    pub transaction_segment_size_bytes: usize,

    /// Minimum number of INSERT statements to trigger bulk insert mode
    pub bulk_insert_threshold: usize,

    /// Maximum number of rows per multi-value INSERT statement. 0 keeps the destination's own
    /// limit. SQL Server caps it at 1000 and SQLite at 500 (their hard limits); MySQL and Kafka
    /// ignore it.
    pub max_rows_per_insert: usize,

    /// Whether to run a one-time initial snapshot of existing table data before streaming
    pub initial_snapshot: bool,

    /// How the snapshot treats target tables that already contain data
    pub snapshot_target_policy: SnapshotTargetPolicy,

    /// Number of concurrent snapshot writer tasks
    pub snapshot_writers: usize,

    /// Snapshot in-memory buffer budget in bytes (minimum 1 MiB)
    pub snapshot_buffer_bytes: usize,

    /// Rows per snapshot batch written to the destination
    pub snapshot_batch_rows: usize,

    /// Per-Config registry of destination factories keyed by `DestinationType::registry_key()`.
    /// Built-ins self-register in `Config::default()`; external users add their own via
    /// `ConfigBuilder::custom_destination` / `use_destination`.
    pub(crate) destination_registry: HashMap<String, DestinationFactoryFn>,
}

/// Masks credentials in a connection string for safe logging.
///
/// Handles two formats:
/// - URI style: `scheme://user:password@host/...` → password replaced with `********`
/// - Key/value style (e.g. SQL Server ADO.NET): `key=value;...` → values for
///   `password`/`pwd` (case-insensitive) replaced with `********`
pub(crate) fn sanitize_connection_string(s: &str) -> String {
    if let Some(scheme_end) = s.find("://") {
        let after_scheme = scheme_end + 3;
        let authority_end = s[after_scheme..]
            .find(|c| c == '/' || c == '?' || c == '#')
            .map(|idx| after_scheme + idx)
            .unwrap_or(s.len());
        if let Some(rel_at) = s[after_scheme..authority_end].rfind('@') {
            let at_pos = after_scheme + rel_at;
            let userinfo = &s[after_scheme..at_pos];
            if let Some(colon) = userinfo.find(':') {
                let user = &userinfo[..colon];
                return format!(
                    "{}://{}:********@{}",
                    &s[..scheme_end],
                    user,
                    &s[at_pos + 1..]
                );
            }
        }
        return s.to_string();
    }
    // Walk the byte stream tracking quote state so that semicolons inside a
    // quoted value (e.g. ADO.NET `Password="my;pwd"`) are not treated as
    // delimiters — otherwise the second half of the password would leak.
    let bytes = s.as_bytes();
    let mut result = String::with_capacity(s.len());
    let mut start = 0;
    let mut in_quote: Option<u8> = None;
    for (i, &b) in bytes.iter().enumerate() {
        match in_quote {
            Some(q) if b == q => in_quote = None,
            Some(_) => {}
            None => match b {
                b'"' | b'\'' => in_quote = Some(b),
                b';' => {
                    append_sanitized_kv(&mut result, &s[start..i]);
                    result.push(';');
                    start = i + 1;
                }
                _ => {}
            },
        }
    }
    append_sanitized_kv(&mut result, &s[start..]);
    result
}

fn append_sanitized_kv(out: &mut String, part: &str) {
    if let Some(eq) = part.find('=') {
        let key = part[..eq].trim();
        if key.eq_ignore_ascii_case("password") || key.eq_ignore_ascii_case("pwd") {
            out.push_str(&part[..eq]);
            out.push_str("=********");
            return;
        }
    }
    out.push_str(part);
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sanitized_source = sanitize_connection_string(&self.source_connection_string);
        let sanitized_dest = sanitize_connection_string(&self.destination_connection_string);
        f.debug_struct("Config")
            .field("source_connection_string", &sanitized_source)
            .field("destination_type", &self.destination_type)
            .field("destination_connection_string", &sanitized_dest)
            .field("replication_slot_name", &self.replication_slot_name)
            .field("publication_name", &self.publication_name)
            .field("streaming", &self.streaming)
            .field("connection_timeout", &self.connection_timeout)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("buffer_size", &self.buffer_size)
            .field("schema_mappings", &self.schema_mappings)
            .field("batch_size", &self.batch_size)
            .field(
                "transaction_file_base_path",
                &self.transaction_file_base_path,
            )
            .field(
                "transaction_segment_size_bytes",
                &self.transaction_segment_size_bytes,
            )
            .field("bulk_insert_threshold", &self.bulk_insert_threshold)
            .field("max_rows_per_insert", &self.max_rows_per_insert)
            .field("initial_snapshot", &self.initial_snapshot)
            .field("snapshot_target_policy", &self.snapshot_target_policy)
            .field("snapshot_writers", &self.snapshot_writers)
            .field("snapshot_buffer_bytes", &self.snapshot_buffer_bytes)
            .field("snapshot_batch_rows", &self.snapshot_batch_rows)
            .field(
                "destination_registry",
                &format_args!("<{} entries>", self.destination_registry.len()),
            )
            .finish()
    }
}

/// How the initial snapshot treats target tables that already contain rows
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SnapshotTargetPolicy {
    /// Abort if any target table is non-empty
    #[default]
    FailIfNotEmpty,
    /// Truncate target tables before loading
    Truncate,
    /// Load on top of existing rows
    Append,
}

impl std::str::FromStr for SnapshotTargetPolicy {
    type Err = CdcError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "fail_if_not_empty" => Ok(Self::FailIfNotEmpty),
            "truncate" => Ok(Self::Truncate),
            "append" => Ok(Self::Append),
            other => Err(CdcError::config(format!(
                "Invalid snapshot target policy '{other}'. Expected one of: fail_if_not_empty, truncate, append"
            ))),
        }
    }
}

impl std::fmt::Display for SnapshotTargetPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::FailIfNotEmpty => "fail_if_not_empty",
            Self::Truncate => "truncate",
            Self::Append => "append",
        })
    }
}

impl Default for Config {
    fn default() -> Self {
        let mut destination_registry: HashMap<String, DestinationFactoryFn> = HashMap::new();

        #[cfg(feature = "mysql")]
        destination_registry.insert(
            "mysql".to_string(),
            Arc::new(|| {
                Ok(Box::new(crate::destinations::MySQLDestination::new())
                    as Box<dyn DestinationHandler>)
            }),
        );

        #[cfg(feature = "sqlserver")]
        destination_registry.insert(
            "sqlserver".to_string(),
            Arc::new(|| {
                Ok(Box::new(crate::destinations::SqlServerDestination::new())
                    as Box<dyn DestinationHandler>)
            }),
        );

        #[cfg(feature = "sqlite")]
        destination_registry.insert(
            "sqlite".to_string(),
            Arc::new(|| {
                Ok(Box::new(crate::destinations::SQLiteDestination::new())
                    as Box<dyn DestinationHandler>)
            }),
        );

        #[cfg(feature = "kafka")]
        destination_registry.insert(
            "kafka".to_string(),
            Arc::new(|| {
                Ok(Box::new(crate::destinations::KafkaDestination::new())
                    as Box<dyn DestinationHandler>)
            }),
        );

        Self {
            source_connection_string: String::new(),
            destination_type: DestinationType::MySQL,
            destination_connection_string: String::new(),
            replication_slot_name: "pg2any_slot".to_string(),
            publication_name: "pg2any_pub".to_string(),
            streaming: false,
            connection_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(10), // Send feedback every 10 seconds to prevent 60s timeout
            buffer_size: 1000,
            schema_mappings: HashMap::new(),
            batch_size: 1000,
            transaction_file_base_path: ".".to_string(),
            transaction_segment_size_bytes: 64 * 1024 * 1024,
            bulk_insert_threshold: 500,
            max_rows_per_insert: 0,
            initial_snapshot: false,
            snapshot_target_policy: SnapshotTargetPolicy::default(),
            snapshot_writers: 4,
            snapshot_buffer_bytes: 16 * 1024 * 1024,
            snapshot_batch_rows: 20000,
            destination_registry,
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

    /// Enable/disable streaming of in-progress transactions
    pub fn streaming(mut self, enabled: bool) -> Self {
        self.config.streaming = enabled;
        self
    }

    /// Set connection timeout
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.config.connection_timeout = timeout;
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

    /// Higher values improve throughput for large streaming transactions
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size.max(1);
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

    pub fn bulk_insert_threshold(mut self, threshold: usize) -> Self {
        self.config.bulk_insert_threshold = threshold;
        self
    }

    pub fn max_rows_per_insert(mut self, max: usize) -> Self {
        self.config.max_rows_per_insert = max;
        self
    }

    /// Enable/disable the one-time initial snapshot
    pub fn initial_snapshot(mut self, enabled: bool) -> Self {
        self.config.initial_snapshot = enabled;
        self
    }

    /// Set how the snapshot treats non-empty target tables
    pub fn snapshot_target_policy(mut self, policy: SnapshotTargetPolicy) -> Self {
        self.config.snapshot_target_policy = policy;
        self
    }

    /// Set the number of concurrent snapshot writers
    pub fn snapshot_writers(mut self, writers: usize) -> Self {
        self.config.snapshot_writers = writers;
        self
    }

    /// Set the snapshot buffer budget in bytes
    pub fn snapshot_buffer_bytes(mut self, bytes: usize) -> Self {
        self.config.snapshot_buffer_bytes = bytes;
        self
    }

    /// Set the number of rows per snapshot batch
    pub fn snapshot_batch_rows(mut self, rows: usize) -> Self {
        self.config.snapshot_batch_rows = rows;
        self
    }

    /// Sentinel registry key used by `custom_destination` / `use_destination`.
    const CUSTOM_DESTINATION_KEY: &'static str = "__custom__";

    /// Inject a custom `DestinationHandler` factory and switch `destination_type`
    /// to `DestinationType::Custom`. The closure must return a fresh handler each
    /// call (invoked once for the live pipeline and once per snapshot writer).
    ///
    /// ```ignore
    /// Config::builder()
    ///     .custom_destination(|| MyHandler::new())
    ///     .build()?;
    /// ```
    pub fn custom_destination<F, H>(mut self, factory: F) -> Self
    where
        F: Fn() -> H + Send + Sync + 'static,
        H: DestinationHandler + 'static,
    {
        let f: DestinationFactoryFn = Arc::new(move || Ok(Box::new(factory()) as _));
        self.config
            .destination_registry
            .insert(Self::CUSTOM_DESTINATION_KEY.to_string(), f);
        self.config.destination_type =
            DestinationType::Custom(Self::CUSTOM_DESTINATION_KEY.to_string());
        self
    }

    /// Zero-arg shortcut for any `DestinationHandler` that implements `Default`.
    /// Useful for swapping in a built-in or a custom type with no construction args.
    ///
    /// ```ignore
    /// Config::builder()
    ///     .use_destination::<MySQLDestination>()
    ///     .build()?;
    /// ```
    pub fn use_destination<H>(self) -> Self
    where
        H: DestinationHandler + Default + 'static,
    {
        self.custom_destination(H::default)
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

        crate::slot::validate_slot_name(&self.config.replication_slot_name)?;

        if self.config.publication_name.is_empty() {
            return Err(CdcError::config("Publication name is required"));
        }

        if self.config.buffer_size == 0 {
            return Err(CdcError::config("Buffer size must be greater than 0"));
        }

        if self.config.snapshot_writers == 0 {
            return Err(CdcError::config("Snapshot writers must be greater than 0"));
        }

        if self.config.snapshot_buffer_bytes < 1024 * 1024 {
            return Err(CdcError::config("Snapshot buffer must be at least 1 MiB"));
        }

        if self.config.snapshot_batch_rows == 0 {
            return Err(CdcError::config(
                "Snapshot batch rows must be greater than 0",
            ));
        }

        Ok(())
    }
}

impl Config {
    /// Create a new config builder
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::new()
    }

    /// Connect `handler` and apply this config's destination settings, the same for every handler
    /// the live path and the snapshot open.
    pub(crate) async fn connect_destination(
        &self,
        handler: &mut dyn DestinationHandler,
    ) -> Result<()> {
        handler.connect(&self.destination_connection_string).await?;
        if !self.schema_mappings.is_empty() {
            handler.set_schema_mappings(self.schema_mappings.clone());
        }
        if self.max_rows_per_insert > 0 {
            handler.set_max_rows_per_insert(self.max_rows_per_insert);
        }
        Ok(())
    }

    /// Construct a destination handler by looking up `self.destination_type.registry_key()`
    /// in the per-Config registry.
    pub fn create_destination(&self) -> Result<Box<dyn DestinationHandler>> {
        let key = self.destination_type.registry_key();
        match self.destination_registry.get(key) {
            Some(factory) => factory(),
            None => Err(CdcError::unsupported(format!(
                "No destination registered for key '{}'",
                key
            ))),
        }
    }
}

/// Convert from CDC config to replication stream config
impl From<&Config> for pg_walstream::ReplicationStreamConfig {
    fn from(config: &Config) -> Self {
        let (protocol_version, streaming_mode) = if config.streaming {
            (2, pg_walstream::StreamingMode::On)
        } else {
            (1, pg_walstream::StreamingMode::Off)
        };

        pg_walstream::ReplicationStreamConfig::new(
            config.replication_slot_name.clone(),
            config.publication_name.clone(),
            protocol_version,
            streaming_mode,
            config.heartbeat_interval,
            config.connection_timeout,
            Duration::from_secs(30), // Health check interval
            RetryConfig::default(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_builder() -> ConfigBuilder {
        Config::builder()
            .source_connection_string("postgresql://u:p@localhost/db?replication=database")
            .destination_connection_string("mysql://u:p@localhost/db")
    }

    #[test]
    fn test_snapshot_defaults() {
        let config = valid_builder().build().unwrap();
        assert!(!config.initial_snapshot);
        assert_eq!(
            config.snapshot_target_policy,
            SnapshotTargetPolicy::FailIfNotEmpty
        );
        assert_eq!(config.snapshot_writers, 4);
        assert_eq!(config.snapshot_buffer_bytes, 16 * 1024 * 1024);
        assert_eq!(config.snapshot_batch_rows, 20000);
    }

    #[test]
    fn test_snapshot_policy_from_str_rejects_unknown() {
        assert!(matches!(
            "bogus".parse::<SnapshotTargetPolicy>(),
            Err(CdcError::Config(_))
        ));
        assert_eq!(
            "fail_if_not_empty".parse::<SnapshotTargetPolicy>().unwrap(),
            SnapshotTargetPolicy::FailIfNotEmpty
        );
        assert_eq!(
            "truncate".parse::<SnapshotTargetPolicy>().unwrap(),
            SnapshotTargetPolicy::Truncate
        );
        assert_eq!(
            "append".parse::<SnapshotTargetPolicy>().unwrap(),
            SnapshotTargetPolicy::Append
        );
        assert_eq!(SnapshotTargetPolicy::Truncate.to_string(), "truncate");
    }

    #[test]
    fn test_validate_rejects_zero_writers() {
        assert!(valid_builder().snapshot_writers(0).build().is_err());
    }

    #[test]
    fn test_validate_rejects_buffer_below_1_mib() {
        assert!(valid_builder()
            .snapshot_buffer_bytes(1024 * 1024 - 1)
            .build()
            .is_err());
        assert!(valid_builder()
            .snapshot_buffer_bytes(1024 * 1024)
            .build()
            .is_ok());
    }

    #[test]
    fn test_validate_rejects_zero_batch_rows() {
        assert!(valid_builder().snapshot_batch_rows(0).build().is_err());
    }

    #[test]
    fn streaming_alone_builds_and_requests_protocol_2() {
        let config = valid_builder().streaming(true).build().unwrap();
        let stream = pg_walstream::ReplicationStreamConfig::from(&config);
        assert_eq!(stream.protocol_version, 2);
    }

    #[test]
    fn without_streaming_requests_protocol_1() {
        let config = valid_builder().build().unwrap();
        let stream = pg_walstream::ReplicationStreamConfig::from(&config);
        assert_eq!(stream.protocol_version, 1);
    }

    #[derive(Default)]
    struct Seen {
        connected: Option<String>,
        schema_mappings: Option<HashMap<String, String>>,
        max_rows: Option<usize>,
    }

    struct Recorder(std::sync::Arc<std::sync::Mutex<Seen>>);

    #[async_trait::async_trait]
    impl DestinationHandler for Recorder {
        async fn connect(&mut self, connection_string: &str) -> Result<()> {
            self.0.lock().unwrap().connected = Some(connection_string.to_string());
            Ok(())
        }
        fn set_schema_mappings(&mut self, mappings: HashMap<String, String>) {
            self.0.lock().unwrap().schema_mappings = Some(mappings);
        }
        fn set_max_rows_per_insert(&mut self, max_rows: usize) {
            self.0.lock().unwrap().max_rows = Some(max_rows);
        }
        async fn execute_sql_batch_with_hook(
            &mut self,
            _commands: &[String],
            _hook: Option<crate::destinations::PreCommitHook>,
        ) -> Result<()> {
            Ok(())
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn connect_destination_connects_and_applies_the_destination_settings() {
        let config = valid_builder()
            .schema_mapping("public", "cdc_db")
            .max_rows_per_insert(250)
            .build()
            .unwrap();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Seen::default()));
        config
            .connect_destination(&mut Recorder(seen.clone()))
            .await
            .unwrap();

        let seen = seen.lock().unwrap();
        assert_eq!(seen.connected.as_deref(), Some("mysql://u:p@localhost/db"));
        assert_eq!(
            seen.schema_mappings.as_ref().unwrap().get("public"),
            Some(&"cdc_db".to_string())
        );
        assert_eq!(seen.max_rows, Some(250));
    }

    #[tokio::test]
    async fn connect_destination_leaves_unset_options_to_the_destination() {
        let config = valid_builder().build().unwrap();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Seen::default()));
        config
            .connect_destination(&mut Recorder(seen.clone()))
            .await
            .unwrap();

        let seen = seen.lock().unwrap();
        assert!(seen.connected.is_some());
        assert!(seen.schema_mappings.is_none(), "no mappings configured");
        assert!(seen.max_rows.is_none(), "0 means the destination's limit");
    }
}
