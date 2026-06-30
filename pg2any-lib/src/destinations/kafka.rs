use super::destination_factory::{DestinationHandler, PreCommitHook};
use crate::destinations::dialect::SqlDialect;
use crate::error::{CdcError, Result};
use async_trait::async_trait;
use base64::prelude::*;
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use pg_walstream::{ChangeEvent, ColumnValue, Lsn};
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

const DEFAULT_TOPIC_PREFIX: &str = "pg2any";
const DEFAULT_FLUSH_TIMEOUT: Duration = Duration::from_secs(30);
const DELIVERY_FUTURE_TIMEOUT: Duration = Duration::from_secs(30);
const LIB_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Per-table cached names, built once per table and reused across all events
/// for that table. Keyed by the per-event `source_table_key` ("{schema}.{table}").
#[cfg_attr(test, derive(Clone))]
struct TableNames {
    topic: String,
    key_schema: String,
    envelope: String,
}

pub struct KafkaDestination {
    producer: Option<FutureProducer>,
    topic_prefix: String,
    source_db_name: String,
    schema_mappings: HashMap<String, String>,
    key_columns_config: HashMap<String, Vec<String>>,
    field_schema_cache: HashMap<String, Value>,
    /// Cached per-table names (topic, key schema, envelope schema) keyed by
    /// `source_table_key` ("{schema}.{table}"). Built once per table; zero key
    /// allocation on the cache-hit (hot) path.
    table_names_cache: HashMap<String, TableNames>,
    /// Cached key columns from Update/Delete events, used as fallback for Insert events
    /// that don't carry key_columns in the replication protocol.
    stream_key_columns: HashMap<String, Vec<Arc<str>>>,
    /// Cached static source schema fields (constant across all events)
    source_schema_fields: Value,
}

impl KafkaDestination {
    pub fn new() -> Self {
        Self {
            producer: None,
            topic_prefix: DEFAULT_TOPIC_PREFIX.to_string(),
            source_db_name: "postgres".to_string(),
            schema_mappings: HashMap::new(),
            key_columns_config: HashMap::new(),
            field_schema_cache: HashMap::new(),
            table_names_cache: HashMap::new(),
            stream_key_columns: HashMap::new(),
            source_schema_fields: Self::build_source_schema_fields(),
        }
    }

    fn build_source_schema_fields() -> Value {
        json!({"type": "struct", "fields": [
            {"type": "string", "optional": false, "field": "version"},
            {"type": "string", "optional": false, "field": "connector"},
            {"type": "string", "optional": false, "field": "name"},
            {"type": "int64", "optional": false, "field": "ts_ms"},
            {"type": "string", "optional": false, "field": "db"},
            {"type": "string", "optional": true, "field": "schema"},
            {"type": "string", "optional": false, "field": "table"},
            {"type": "int64", "optional": true, "field": "txId"},
            {"type": "int64", "optional": true, "field": "lsn"}
        ], "optional": false, "field": "source"})
    }

    /// Returns cached per-table names, building them once on a cache miss.
    /// On a cache hit this performs a single borrowed-key lookup with zero
    /// key allocation; on a miss it allocates one owned key (`source_table_key`)
    /// plus the three name strings.
    fn table_names(
        &mut self,
        source_table_key: &str,
        mapped_schema: &str,
        table: &str,
    ) -> &TableNames {
        if !self.table_names_cache.contains_key(source_table_key) {
            let prefix = &self.topic_prefix;
            let names = TableNames {
                topic: format!("{}.{}.{}", prefix, mapped_schema, table),
                key_schema: format!("{}.{}.{}.Key", prefix, mapped_schema, table),
                envelope: format!("{}.{}.{}.Envelope", prefix, mapped_schema, table),
            };
            self.table_names_cache
                .insert(source_table_key.to_owned(), names);
        }
        &self.table_names_cache[source_table_key]
    }

    fn map_schema<'a>(&'a self, source_schema: &'a str) -> &'a str {
        self.schema_mappings
            .get(source_schema)
            .map(|s| s.as_str())
            .unwrap_or(source_schema)
    }

    fn column_value_to_json(value: &ColumnValue) -> Value {
        match value {
            ColumnValue::Null => Value::Null,
            ColumnValue::Text(b) => match std::str::from_utf8(b) {
                Ok(s) => Value::String(s.to_string()),
                Err(_) => Value::String(BASE64_STANDARD.encode(b)),
            },
            ColumnValue::Binary(b) => Value::String(BASE64_STANDARD.encode(b)),
        }
    }

    fn row_data_to_json(data: &pg_walstream::RowData) -> Value {
        let mut map = serde_json::Map::new();
        for (col_name, col_value) in data.iter() {
            map.insert(col_name.to_string(), Self::column_value_to_json(col_value));
        }
        Value::Object(map)
    }

    fn build_field_schema(data: &pg_walstream::RowData) -> Value {
        let fields: Vec<Value> = data
            .iter()
            .map(|(col_name, col_value)| {
                let field_type = match col_value {
                    ColumnValue::Null => "string",
                    ColumnValue::Text(_) => "string",
                    ColumnValue::Binary(_) => "bytes",
                };
                json!({
                    "type": field_type,
                    "optional": true,
                    "field": col_name.as_ref()
                })
            })
            .collect();
        json!(fields)
    }

    fn get_or_build_field_schema(
        &mut self,
        table_key: &str,
        data: &pg_walstream::RowData,
    ) -> Value {
        let mut cache_key = String::with_capacity(table_key.len() + data.len() * 10);
        cache_key.push_str(table_key);
        for (col_name, _) in data.iter() {
            cache_key.push(':');
            cache_key.push_str(col_name);
        }
        if let Some(cached) = self.field_schema_cache.get(&cache_key) {
            return cached.clone();
        }
        let schema = Self::build_field_schema(data);
        self.field_schema_cache.insert(cache_key, schema.clone());
        schema
    }

    fn build_key_for_insert(
        &self,
        key_schema_name: &str,
        table_key: &str,
        data: &pg_walstream::RowData,
    ) -> Result<Option<String>> {
        let key_column_names: Vec<&str> = if let Some(cols) = self.key_columns_config.get(table_key)
        {
            cols.iter().map(|s| s.as_str()).collect()
        } else if let Some(stream_cols) = self.stream_key_columns.get(table_key) {
            stream_cols.iter().map(|c| c.as_ref()).collect()
        } else {
            return Ok(None);
        };

        let mut payload = serde_json::Map::new();
        let mut fields = Vec::new();

        for col_name in &key_column_names {
            if let Some(val) = data.get(col_name) {
                payload.insert(col_name.to_string(), Self::column_value_to_json(val));
                fields.push(json!({
                    "type": "string",
                    "optional": false,
                    "field": *col_name
                }));
            }
        }

        if payload.is_empty() {
            return Ok(None);
        }

        let key_schema_name = key_schema_name.to_owned();
        let key = json!({
            "schema": {
                "type": "struct",
                "fields": fields,
                "optional": false,
                "name": key_schema_name
            },
            "payload": Value::Object(payload)
        });

        serde_json::to_string(&key)
            .map_err(|e| CdcError::generic(format!("Key serialization failed: {e}")))
            .map(Some)
    }

    fn build_source_block(
        &self,
        schema: &str,
        table: &str,
        transaction_id: u32,
        commit_timestamp: DateTime<Utc>,
        lsn: Option<Lsn>,
    ) -> Value {
        json!({
            "version": LIB_VERSION,
            "connector": "pg2any",
            "name": self.topic_prefix,
            "ts_ms": commit_timestamp.timestamp_millis(),
            "db": self.source_db_name,
            "schema": schema,
            "table": table,
            "txId": transaction_id,
            "lsn": lsn.map(|l| l.0)
        })
    }

    fn build_change_envelope(
        &mut self,
        op: &str,
        schema: &str,
        table: &str,
        envelope_name: &str,
        before: Option<Value>,
        after: Option<Value>,
        before_fields: Option<Value>,
        after_fields: Option<Value>,
        transaction_id: u32,
        commit_timestamp: DateTime<Utc>,
        lsn: Option<Lsn>,
    ) -> Value {
        let source = self.build_source_block(schema, table, transaction_id, commit_timestamp, lsn);

        let unified_fields = after_fields
            .as_ref()
            .or(before_fields.as_ref())
            .cloned()
            .unwrap_or_else(|| json!([]));

        let before_schema = json!({
            "type": "struct",
            "fields": unified_fields,
            "optional": true,
            "field": "before"
        });
        let after_schema = json!({
            "type": "struct",
            "fields": unified_fields,
            "optional": true,
            "field": "after"
        });

        json!({
            "schema": {
                "type": "struct",
                "fields": [
                    before_schema,
                    after_schema,
                    self.source_schema_fields,
                    {"type": "string", "optional": false, "field": "op"},
                    {"type": "int64", "optional": true, "field": "ts_ms"}
                ],
                "optional": false,
                "name": envelope_name
            },
            "payload": {
                "before": before.unwrap_or(Value::Null),
                "after": after.unwrap_or(Value::Null),
                "source": source,
                "op": op,
                "ts_ms": commit_timestamp.timestamp_millis()
            }
        })
    }

    fn build_key_from_data(
        &self,
        key_schema_name: &str,
        source_table_key: &str,
        data: &pg_walstream::RowData,
        key_columns: &[Arc<str>],
    ) -> Result<Option<String>> {
        // Prefer manual key_columns_config over replication stream's key_columns
        // to ensure consistent keys across Insert/Update/Delete for the same row
        let effective_columns: Vec<&str> =
            if let Some(config_cols) = self.key_columns_config.get(source_table_key) {
                config_cols.iter().map(|s| s.as_str()).collect()
            } else if !key_columns.is_empty() {
                key_columns.iter().map(|c| c.as_ref()).collect()
            } else {
                return Ok(None);
            };

        let mut payload = serde_json::Map::new();
        let mut fields = Vec::new();

        for col in &effective_columns {
            if let Some(val) = data.get(*col) {
                payload.insert(col.to_string(), Self::column_value_to_json(val));
                fields.push(json!({
                    "type": "string",
                    "optional": false,
                    "field": *col
                }));
            }
        }

        if payload.is_empty() {
            return Ok(None);
        }

        let key_schema_name = key_schema_name.to_owned();
        let key = json!({
            "schema": {
                "type": "struct",
                "fields": fields,
                "optional": false,
                "name": key_schema_name
            },
            "payload": Value::Object(payload)
        });

        serde_json::to_string(&key)
            .map_err(|e| CdcError::generic(format!("Key serialization failed: {e}")))
            .map(Some)
    }

    async fn enqueue_event(
        &self,
        topic: &str,
        key: Option<&str>,
        value: &[u8],
    ) -> Result<DeliveryFuture> {
        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| CdcError::generic("Kafka producer not initialized"))?;

        const MAX_RETRIES: u32 = 5;
        const BASE_DELAY_MS: u64 = 100;
        const MAX_DELAY_MS: u64 = 3_000;

        for attempt in 0..MAX_RETRIES {
            let mut record = FutureRecord::to(topic).payload(value);
            if let Some(k) = key {
                record = record.key(k);
            }

            match producer.send_result(record) {
                Ok(future) => return Ok(future),
                Err((
                    KafkaError::MessageProduction(
                        RDKafkaErrorCode::UnknownTopic
                        | RDKafkaErrorCode::UnknownTopicOrPartition
                        | RDKafkaErrorCode::QueueFull,
                    ),
                    _,
                )) if attempt < MAX_RETRIES - 1 => {
                    let delay_ms = BASE_DELAY_MS
                        .saturating_mul(1u64 << attempt)
                        .min(MAX_DELAY_MS);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                Err((err, _)) => {
                    return Err(CdcError::Kafka(err));
                }
            }
        }

        Err(CdcError::generic("Kafka enqueue failed after max retries"))
    }

    async fn await_delivery_futures(&self, futures: Vec<DeliveryFuture>) -> Result<()> {
        let timed_futures: Vec<_> = futures
            .into_iter()
            .map(|f| tokio::time::timeout(DELIVERY_FUTURE_TIMEOUT, f))
            .collect();

        let results = join_all(timed_futures).await;

        for result in results {
            match result {
                Ok(Ok(Ok(_))) => {}
                Ok(Ok(Err((err, _)))) => {
                    return Err(CdcError::Kafka(err));
                }
                Ok(Err(_cancelled)) => {
                    return Err(CdcError::generic("Kafka delivery future cancelled"));
                }
                Err(_elapsed) => {
                    return Err(CdcError::generic(
                        "Kafka delivery future timed out waiting for broker acknowledgement",
                    ));
                }
            }
        }
        Ok(())
    }
}

impl Default for KafkaDestination {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DestinationHandler for KafkaDestination {
    async fn connect(&mut self, connection_string: &str) -> Result<()> {
        let topic_prefix = std::env::var("CDC_KAFKA_TOPIC_PREFIX")
            .unwrap_or_else(|_| DEFAULT_TOPIC_PREFIX.to_string());
        let source_db_name =
            std::env::var("CDC_KAFKA_SOURCE_DB_NAME").unwrap_or_else(|_| "postgres".to_string());
        let security_protocol = std::env::var("CDC_KAFKA_SECURITY_PROTOCOL")
            .unwrap_or_else(|_| "plaintext".to_string());
        let compression =
            std::env::var("CDC_KAFKA_COMPRESSION").unwrap_or_else(|_| "lz4".to_string());
        let batch_size =
            std::env::var("CDC_KAFKA_BATCH_SIZE").unwrap_or_else(|_| "16384".to_string());
        let linger_ms = std::env::var("CDC_KAFKA_LINGER_MS").unwrap_or_else(|_| "5".to_string());
        let acks = std::env::var("CDC_KAFKA_ACKS").unwrap_or_else(|_| "all".to_string());
        let message_max_bytes =
            std::env::var("CDC_KAFKA_MESSAGE_MAX_BYTES").unwrap_or_else(|_| "1048576".to_string());
        let retries = std::env::var("CDC_KAFKA_RETRIES").unwrap_or_else(|_| "3".to_string());

        let message_timeout_ms =
            std::env::var("CDC_KAFKA_MESSAGE_TIMEOUT_MS").unwrap_or_else(|_| "30000".to_string());
        let retry_backoff_ms =
            std::env::var("CDC_KAFKA_RETRY_BACKOFF_MS").unwrap_or_else(|_| "200".to_string());
        let metadata_refresh_ms = std::env::var("CDC_KAFKA_METADATA_REFRESH_INTERVAL_MS")
            .unwrap_or_else(|_| "5000".to_string());

        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", connection_string)
            .set("security.protocol", &security_protocol)
            .set("compression.type", &compression)
            .set("batch.size", &batch_size)
            .set("linger.ms", &linger_ms)
            .set("acks", &acks)
            .set("message.max.bytes", &message_max_bytes)
            .set("retries", &retries)
            .set("message.timeout.ms", &message_timeout_ms)
            .set("retry.backoff.ms", &retry_backoff_ms)
            .set("topic.metadata.refresh.interval.ms", &metadata_refresh_ms);

        if let Ok(mechanism) = std::env::var("CDC_KAFKA_SASL_MECHANISM") {
            config.set("sasl.mechanism", &mechanism);
        }
        if let Ok(username) = std::env::var("CDC_KAFKA_SASL_USERNAME") {
            config.set("sasl.username", &username);
        }
        if let Ok(password) = std::env::var("CDC_KAFKA_SASL_PASSWORD") {
            config.set("sasl.password", &password);
        }

        let producer: FutureProducer = config
            .create()
            .map_err(|e| CdcError::generic(format!("Failed to create Kafka producer: {e}")))?;

        self.producer = Some(producer);
        self.topic_prefix = topic_prefix;
        self.source_db_name = source_db_name;

        // Parse key columns config: CDC_KAFKA_KEY_COLUMNS="schema.table:col1,col2;schema2.table2:col3"
        if let Ok(key_cols_str) = std::env::var("CDC_KAFKA_KEY_COLUMNS") {
            for entry in key_cols_str.split(';') {
                let entry = entry.trim();
                if let Some((table_ref, cols)) = entry.split_once(':') {
                    let columns: Vec<String> =
                        cols.split(',').map(|c| c.trim().to_string()).collect();
                    if !columns.is_empty() {
                        self.key_columns_config
                            .insert(table_ref.trim().to_string(), columns);
                    }
                }
            }
            if !self.key_columns_config.is_empty() {
                debug!("Kafka key columns config: {:?}", self.key_columns_config);
            }
        }

        info!(
            "Kafka producer connected to {} (topic_prefix={})",
            connection_string, self.topic_prefix
        );
        Ok(())
    }

    fn set_schema_mappings(&mut self, mappings: HashMap<String, String>) {
        self.schema_mappings = mappings;
        if !self.schema_mappings.is_empty() {
            debug!(
                "Kafka destination schema mappings set: {:?}",
                self.schema_mappings
            );
        }
    }

    async fn execute_sql_batch_with_hook(
        &mut self,
        _commands: &[String],
        _pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        Err(CdcError::unsupported(
            "Kafka destination does not support SQL batch execution. Use event mode.",
        ))
    }

    fn supports_event_mode(&self) -> bool {
        true
    }

    fn dialect(&self) -> &'static dyn SqlDialect {
        use crate::destinations::dialects::KafkaDialect;
        static D: KafkaDialect = KafkaDialect;
        &D
    }

    async fn execute_events_batch_with_hook(
        &mut self,
        events: &[ChangeEvent],
        transaction_id: u32,
        commit_timestamp: DateTime<Utc>,
        commit_lsn: Option<Lsn>,
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        if events.is_empty() {
            if let Some(hook) = pre_commit_hook {
                hook().await?;
            }
            return Ok(());
        }

        let mut delivery_futures: Vec<DeliveryFuture> = Vec::with_capacity(events.len());

        for event in events {
            match &event.event_type {
                pg_walstream::EventType::Insert {
                    schema,
                    table,
                    data,
                    ..
                } => {
                    let mapped_schema = self.map_schema(schema).to_owned();
                    let source_table_key = format!("{}.{}", schema, table);
                    let names = self.table_names(&source_table_key, &mapped_schema, table);
                    let topic = names.topic.clone();
                    let key_schema = names.key_schema.clone();
                    let envelope_name = names.envelope.clone();
                    let after_fields =
                        Some(self.get_or_build_field_schema(&source_table_key, data));
                    let after = Some(Self::row_data_to_json(data));
                    let key = self.build_key_for_insert(&key_schema, &source_table_key, data)?;
                    let envelope = self.build_change_envelope(
                        "c",
                        &mapped_schema,
                        table,
                        &envelope_name,
                        None,
                        after,
                        None,
                        after_fields,
                        transaction_id,
                        commit_timestamp,
                        commit_lsn,
                    );
                    let value = serde_json::to_vec(&envelope).map_err(|e| {
                        CdcError::generic(format!("JSON serialization failed: {e}"))
                    })?;
                    let future = self.enqueue_event(&topic, key.as_deref(), &value).await?;
                    delivery_futures.push(future);
                }
                pg_walstream::EventType::Update {
                    schema,
                    table,
                    old_data,
                    new_data,
                    key_columns,
                    ..
                } => {
                    let mapped_schema = self.map_schema(schema).to_owned();
                    let source_table_key = format!("{}.{}", schema, table);
                    if !key_columns.is_empty() {
                        self.stream_key_columns
                            .entry(source_table_key.clone())
                            .or_insert_with(|| key_columns.clone());
                    }
                    let names = self.table_names(&source_table_key, &mapped_schema, table);
                    let topic = names.topic.clone();
                    let key_schema = names.key_schema.clone();
                    let envelope_name = names.envelope.clone();
                    let before_fields = old_data
                        .as_ref()
                        .map(|d| self.get_or_build_field_schema(&source_table_key, d));
                    let after_fields =
                        Some(self.get_or_build_field_schema(&source_table_key, new_data));
                    let before = old_data.as_ref().map(Self::row_data_to_json);
                    let after = Some(Self::row_data_to_json(new_data));
                    let key_data = old_data.as_ref().unwrap_or(new_data);
                    let key = self.build_key_from_data(
                        &key_schema,
                        &source_table_key,
                        key_data,
                        key_columns,
                    )?;
                    let envelope = self.build_change_envelope(
                        "u",
                        &mapped_schema,
                        table,
                        &envelope_name,
                        before,
                        after,
                        before_fields,
                        after_fields,
                        transaction_id,
                        commit_timestamp,
                        commit_lsn,
                    );
                    let value = serde_json::to_vec(&envelope).map_err(|e| {
                        CdcError::generic(format!("JSON serialization failed: {e}"))
                    })?;
                    let future = self.enqueue_event(&topic, key.as_deref(), &value).await?;
                    delivery_futures.push(future);
                }
                pg_walstream::EventType::Delete {
                    schema,
                    table,
                    old_data,
                    key_columns,
                    ..
                } => {
                    let mapped_schema = self.map_schema(schema).to_owned();
                    let source_table_key = format!("{}.{}", schema, table);
                    if !key_columns.is_empty() {
                        self.stream_key_columns
                            .entry(source_table_key.clone())
                            .or_insert_with(|| key_columns.clone());
                    }
                    let names = self.table_names(&source_table_key, &mapped_schema, table);
                    let topic = names.topic.clone();
                    let key_schema = names.key_schema.clone();
                    let envelope_name = names.envelope.clone();
                    let before_fields =
                        Some(self.get_or_build_field_schema(&source_table_key, old_data));
                    let before = Some(Self::row_data_to_json(old_data));
                    let key = self.build_key_from_data(
                        &key_schema,
                        &source_table_key,
                        old_data,
                        key_columns,
                    )?;
                    let envelope = self.build_change_envelope(
                        "d",
                        &mapped_schema,
                        table,
                        &envelope_name,
                        before,
                        None,
                        before_fields,
                        None,
                        transaction_id,
                        commit_timestamp,
                        commit_lsn,
                    );
                    let value = serde_json::to_vec(&envelope).map_err(|e| {
                        CdcError::generic(format!("JSON serialization failed: {e}"))
                    })?;
                    let future = self.enqueue_event(&topic, key.as_deref(), &value).await?;
                    delivery_futures.push(future);
                }
                pg_walstream::EventType::Truncate(tables) => {
                    for table_spec in tables.iter() {
                        let (schema, table) = match table_spec.split_once('.') {
                            Some((s, t)) if !t.contains('.') => (self.map_schema(s).to_owned(), t),
                            _ => (self.map_schema("public").to_owned(), table_spec.as_ref()),
                        };
                        let source_table_key = format!("{}.{}", schema, table);
                        let names = self.table_names(&source_table_key, &schema, table);
                        let topic = names.topic.clone();
                        let envelope_name = names.envelope.clone();
                        let envelope = self.build_change_envelope(
                            "t",
                            &schema,
                            table,
                            &envelope_name,
                            None,
                            None,
                            None,
                            None,
                            transaction_id,
                            commit_timestamp,
                            commit_lsn,
                        );
                        let value = serde_json::to_vec(&envelope).map_err(|e| {
                            CdcError::generic(format!("JSON serialization failed: {e}"))
                        })?;
                        let future = self.enqueue_event(&topic, None, &value).await?;
                        delivery_futures.push(future);
                    }
                }
                _ => {
                    debug!("Skipping non-DML event for Kafka");
                }
            }
        }

        self.await_delivery_futures(delivery_futures).await?;

        if let Some(hook) = pre_commit_hook {
            hook().await?;
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(producer) = self.producer.take() {
            info!(
                "Flushing Kafka producer (timeout {:?})...",
                DEFAULT_FLUSH_TIMEOUT
            );
            let result = tokio::task::spawn_blocking(move || producer.flush(DEFAULT_FLUSH_TIMEOUT))
                .await
                .map_err(|e| CdcError::generic(format!("Kafka flush task panicked: {e}")))?;
            match result {
                Ok(()) => info!("Kafka producer flushed and closed successfully"),
                Err(e) => warn!("Kafka producer flush timed out or failed: {e} — some messages may be re-delivered on restart"),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_walstream::RowData;

    fn test_destination() -> KafkaDestination {
        let mut dest = KafkaDestination::new();
        dest.topic_prefix = "test_prefix".to_string();
        dest.source_db_name = "testdb".to_string();
        dest
    }

    #[test]
    fn test_topic_name() {
        let mut dest = test_destination();
        assert_eq!(
            dest.table_names("public.users", "public", "users").topic,
            "test_prefix.public.users"
        );
        assert_eq!(
            dest.table_names("myschema.orders", "myschema", "orders")
                .topic,
            "test_prefix.myschema.orders"
        );
    }

    #[test]
    fn test_topic_and_schema_name_caches_are_stable() {
        let mut dest = test_destination();
        let names1 = dest.table_names("public.users", "public", "users").clone();
        let names2 = dest.table_names("public.users", "public", "users").clone();
        assert_eq!(names1.topic, names2.topic);
        assert_eq!(names1.key_schema, names2.key_schema);
        assert_eq!(names1.envelope, names2.envelope);
        assert_eq!(names1.topic, "test_prefix.public.users");
        assert_eq!(names1.key_schema, "test_prefix.public.users.Key");
        assert_eq!(names1.envelope, "test_prefix.public.users.Envelope");
    }

    #[test]
    fn test_column_value_to_json_null() {
        let result = KafkaDestination::column_value_to_json(&ColumnValue::Null);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_column_value_to_json_text() {
        let result = KafkaDestination::column_value_to_json(&ColumnValue::text("hello"));
        assert_eq!(result, Value::String("hello".to_string()));
    }

    #[test]
    fn test_column_value_to_json_binary() {
        let result = KafkaDestination::column_value_to_json(&ColumnValue::Binary(
            bytes::Bytes::from_static(&[0xde, 0xad]),
        ));
        assert_eq!(result, Value::String("3q0=".to_string()));
    }

    #[test]
    fn test_row_data_to_json() {
        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("Alice")),
            ("score", ColumnValue::Null),
        ]);
        let json = KafkaDestination::row_data_to_json(&data);
        assert_eq!(json["id"], Value::String("1".to_string()));
        assert_eq!(json["name"], Value::String("Alice".to_string()));
        assert_eq!(json["score"], Value::Null);
    }

    #[test]
    fn test_build_source_block() {
        let dest = test_destination();
        let ts = chrono::Utc::now();
        let source = dest.build_source_block("public", "users", 100, ts, Some(Lsn(12345)));

        assert_eq!(source["version"], LIB_VERSION);
        assert_eq!(source["connector"], "pg2any");
        assert_eq!(source["name"], "test_prefix");
        assert_eq!(source["db"], "testdb");
        assert_eq!(source["schema"], "public");
        assert_eq!(source["table"], "users");
        assert_eq!(source["txId"], 100);
        assert_eq!(source["lsn"], 12345);
    }

    #[test]
    fn test_build_change_envelope_insert() {
        let mut dest = test_destination();
        let ts = DateTime::parse_from_rfc3339("2024-01-15T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let after_data = json!({"id": "1", "name": "Alice"});
        let after_fields = json!([
            {"type": "string", "optional": true, "field": "id"},
            {"type": "string", "optional": true, "field": "name"}
        ]);

        let envelope = dest.build_change_envelope(
            "c",
            "public",
            "users",
            "test_prefix.public.users.Envelope",
            None,
            Some(after_data.clone()),
            None,
            Some(after_fields),
            100,
            ts,
            Some(Lsn(12345)),
        );

        assert_eq!(envelope["payload"]["op"], "c");
        assert_eq!(envelope["payload"]["before"], Value::Null);
        assert_eq!(envelope["payload"]["after"], after_data);
        assert_eq!(envelope["payload"]["source"]["table"], "users");
        assert_eq!(envelope["payload"]["source"]["txId"], 100);
        assert!(envelope["schema"]["name"]
            .as_str()
            .unwrap()
            .ends_with(".Envelope"));
        // Verify after field schema is populated
        let after_schema = &envelope["schema"]["fields"][1];
        assert_eq!(after_schema["field"], "after");
        assert!(!after_schema["fields"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_build_change_envelope_delete() {
        let mut dest = test_destination();
        let ts = Utc::now();
        let before_data = json!({"id": "1", "name": "Alice"});

        let envelope = dest.build_change_envelope(
            "d",
            "public",
            "users",
            "test_prefix.public.users.Envelope",
            Some(before_data.clone()),
            None,
            Some(json!([{"type": "string", "optional": true, "field": "id"}])),
            None,
            101,
            ts,
            None,
        );

        assert_eq!(envelope["payload"]["op"], "d");
        assert_eq!(envelope["payload"]["before"], before_data);
        assert_eq!(envelope["payload"]["after"], Value::Null);
        assert_eq!(envelope["payload"]["source"]["lsn"], Value::Null);
    }

    #[test]
    fn test_build_change_envelope_truncate() {
        let mut dest = test_destination();
        let ts = Utc::now();

        let envelope = dest.build_change_envelope(
            "t",
            "public",
            "users",
            "test_prefix.public.users.Envelope",
            None,
            None,
            None,
            None,
            102,
            ts,
            Some(Lsn(99999)),
        );

        assert_eq!(envelope["payload"]["op"], "t");
        assert_eq!(envelope["payload"]["before"], Value::Null);
        assert_eq!(envelope["payload"]["after"], Value::Null);
    }

    #[test]
    fn test_build_key_from_data_with_keys() {
        let dest = test_destination();
        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("42")),
            ("name", ColumnValue::text("Alice")),
        ]);
        let key_columns = vec![Arc::from("id")];

        let key = dest
            .build_key_from_data(
                "test_prefix.public.users.Key",
                "public.users",
                &data,
                &key_columns,
            )
            .unwrap();
        assert!(key.is_some());

        let key_json: Value = serde_json::from_str(key.as_ref().unwrap()).unwrap();
        assert_eq!(key_json["payload"]["id"], Value::String("42".to_string()));
        assert!(key_json["schema"]["name"]
            .as_str()
            .unwrap()
            .ends_with(".Key"));
    }

    #[test]
    fn test_build_key_from_data_no_keys() {
        let dest = test_destination();
        let data = RowData::from_pairs(vec![("id", ColumnValue::text("42"))]);
        let key_columns: Vec<Arc<str>> = vec![];

        let key = dest
            .build_key_from_data(
                "test_prefix.public.users.Key",
                "public.users",
                &data,
                &key_columns,
            )
            .unwrap();
        assert!(key.is_none());
    }

    #[test]
    fn test_build_key_composite() {
        let dest = test_destination();
        let data = RowData::from_pairs(vec![
            ("tenant_id", ColumnValue::text("t1")),
            ("user_id", ColumnValue::text("u1")),
            ("extra", ColumnValue::text("ignored")),
        ]);
        let key_columns = vec![Arc::from("tenant_id"), Arc::from("user_id")];

        let key = dest
            .build_key_from_data(
                "test_prefix.public.users.Key",
                "public.users",
                &data,
                &key_columns,
            )
            .unwrap();
        assert!(key.is_some());

        let key_json: Value = serde_json::from_str(key.as_ref().unwrap()).unwrap();
        assert_eq!(
            key_json["payload"]["tenant_id"],
            Value::String("t1".to_string())
        );
        assert_eq!(
            key_json["payload"]["user_id"],
            Value::String("u1".to_string())
        );
    }

    #[test]
    fn test_build_key_from_data_prefers_config_over_stream_keys() {
        let mut dest = test_destination();
        dest.key_columns_config
            .insert("public.users".to_string(), vec!["id".to_string()]);

        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("42")),
            ("name", ColumnValue::text("Alice")),
        ]);
        // Stream provides "name" as key column, but config says "id"
        let stream_key_columns = vec![Arc::from("name")];

        let key = dest
            .build_key_from_data(
                "test_prefix.public.users.Key",
                "public.users",
                &data,
                &stream_key_columns,
            )
            .unwrap();
        assert!(key.is_some());
        let key_json: Value = serde_json::from_str(key.as_ref().unwrap()).unwrap();
        // Config's "id" should win over stream's "name"
        assert_eq!(key_json["payload"]["id"], Value::String("42".to_string()));
        assert!(key_json["payload"].get("name").is_none());
    }

    #[test]
    fn test_map_schema_with_mappings() {
        let mut dest = test_destination();
        dest.schema_mappings
            .insert("public".to_string(), "mapped_db".to_string());

        assert_eq!(dest.map_schema("public"), "mapped_db");
        assert_eq!(dest.map_schema("other"), "other");
    }

    #[test]
    fn test_supports_event_mode() {
        let dest = KafkaDestination::new();
        assert!(dest.supports_event_mode());
    }

    #[test]
    fn test_default_creation() {
        let dest = KafkaDestination::new();
        assert!(dest.producer.is_none());
        assert_eq!(dest.topic_prefix, DEFAULT_TOPIC_PREFIX);
        assert_eq!(dest.source_db_name, "postgres");
    }

    #[test]
    fn test_base64_encode() {
        assert_eq!(BASE64_STANDARD.encode([0xde, 0xad, 0xbe, 0xef]), "3q2+7w==");
        assert_eq!(BASE64_STANDARD.encode([]), "");
        assert_eq!(BASE64_STANDARD.encode([0x00, 0xff]), "AP8=");
        assert_eq!(BASE64_STANDARD.encode(b"Hello"), "SGVsbG8=");
    }

    #[test]
    fn test_build_field_schema() {
        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("Alice")),
            (
                "avatar",
                ColumnValue::Binary(bytes::Bytes::from_static(&[0x01])),
            ),
            ("deleted", ColumnValue::Null),
        ]);
        let schema = KafkaDestination::build_field_schema(&data);
        let fields = schema.as_array().unwrap();
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[0]["field"], "id");
        assert_eq!(fields[0]["type"], "string");
        assert_eq!(fields[2]["field"], "avatar");
        assert_eq!(fields[2]["type"], "bytes");
        assert_eq!(fields[3]["field"], "deleted");
        assert_eq!(fields[3]["type"], "string");
    }

    #[test]
    fn test_build_key_for_insert_with_config() {
        let mut dest = test_destination();
        dest.key_columns_config
            .insert("public.users".to_string(), vec!["id".to_string()]);

        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("42")),
            ("name", ColumnValue::text("Alice")),
        ]);

        let key = dest
            .build_key_for_insert("test_prefix.public.users.Key", "public.users", &data)
            .unwrap();
        assert!(key.is_some());
        let key_json: Value = serde_json::from_str(key.as_ref().unwrap()).unwrap();
        assert_eq!(key_json["payload"]["id"], Value::String("42".to_string()));
    }

    #[test]
    fn test_build_key_for_insert_no_config() {
        let dest = test_destination();
        let data = RowData::from_pairs(vec![("id", ColumnValue::text("42"))]);

        let key = dest
            .build_key_for_insert("test_prefix.public.users.Key", "public.users", &data)
            .unwrap();
        assert!(key.is_none());
    }
}
