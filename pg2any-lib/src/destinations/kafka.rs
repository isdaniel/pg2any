use super::destination_factory::{DestinationHandler, EventTransaction, PreCommitHook};
use crate::destinations::dialect::SqlDialect;
use crate::error::{CdcError, Result};
use async_trait::async_trait;
use base64::prelude::*;
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use pg_walstream::{ChangeEvent, ColumnValue, Lsn};
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message as _;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

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
            // A key without all its columns would give distinct rows the same key.
            let Some(val) = data.get(col_name) else {
                return Ok(None);
            };
            payload.insert(col_name.to_string(), Self::column_value_to_json(val));
            fields.push(json!({
                "type": "string",
                "optional": false,
                "field": *col_name
            }));
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

    /// Builds the (topic, key, value) of one row-insert message. `op` is `"c"` for a live insert
    /// and `"r"` for a snapshot row; nothing else differs between the two.
    #[allow(clippy::too_many_arguments)]
    fn insert_message(
        &mut self,
        op: &str,
        schema: &str,
        table: &str,
        data: &pg_walstream::RowData,
        transaction_id: u32,
        commit_timestamp: DateTime<Utc>,
        lsn: Option<Lsn>,
    ) -> Result<(String, Option<String>, Vec<u8>)> {
        let mapped_schema = self.map_schema(schema).to_owned();
        let source_table_key = format!("{}.{}", schema, table);
        let names = self.table_names(&source_table_key, &mapped_schema, table);
        let topic = names.topic.clone();
        let key_schema = names.key_schema.clone();
        let envelope_name = names.envelope.clone();
        let after_fields = Some(self.get_or_build_field_schema(&source_table_key, data));
        let after = Some(Self::row_data_to_json(data));
        let key = self.build_key_for_insert(&key_schema, &source_table_key, data)?;
        let envelope = self.build_change_envelope(
            op,
            &mapped_schema,
            table,
            &envelope_name,
            None,
            after,
            None,
            after_fields,
            transaction_id,
            commit_timestamp,
            lsn,
        );
        let value = serde_json::to_vec(&envelope)
            .map_err(|e| CdcError::generic(format!("JSON serialization failed: {e}")))?;
        Ok((topic, key, value))
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
                    error!("{}", describe_send_failure(topic, value.len(), &err));
                    return Err(CdcError::Kafka(err));
                }
            }
        }

        Err(CdcError::generic("Kafka enqueue failed after max retries"))
    }

    /// Builds and enqueues the messages of one transaction's events (no delivery wait).
    async fn enqueue_events(
        &mut self,
        events: &[ChangeEvent],
        transaction_id: u32,
        commit_timestamp: DateTime<Utc>,
        commit_lsn: Option<Lsn>,
        delivery_futures: &mut Vec<DeliveryFuture>,
    ) -> Result<()> {
        for event in events {
            match &event.event_type {
                pg_walstream::EventType::Insert {
                    schema,
                    table,
                    data,
                    ..
                } => {
                    let (topic, key, value) = self.insert_message(
                        "c",
                        schema,
                        table,
                        data,
                        transaction_id,
                        commit_timestamp,
                        commit_lsn,
                    )?;
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
                pg_walstream::EventType::Truncate { tables, .. } => {
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
        Ok(())
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
                Ok(Ok(Err((err, msg)))) => {
                    let len = msg.payload().map_or(0, <[u8]>::len);
                    error!("{}", describe_send_failure(msg.topic(), len, &err));
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

/// Names the topic and size of a rejected message, and the settings to raise when it is too large:
/// the bare librdkafka error names neither.
fn describe_send_failure(topic: &str, len: usize, err: &KafkaError) -> String {
    let mut text = format!("Kafka rejected a {len}-byte message for topic '{topic}': {err}");
    if matches!(
        err,
        KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge)
    ) {
        text.push_str(
            "; raise CDC_KAFKA_MESSAGE_MAX_BYTES and the broker's message.max.bytes (or the \
             topic's max.message.bytes)",
        );
    }
    text
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
        self.enqueue_events(
            events,
            transaction_id,
            commit_timestamp,
            commit_lsn,
            &mut delivery_futures,
        )
        .await?;

        self.await_delivery_futures(delivery_futures).await?;

        if let Some(hook) = pre_commit_hook {
            hook().await?;
        }

        Ok(())
    }

    /// Every message of every transaction is enqueued before any delivery is awaited, so a group
    /// costs one broker round trip. Kafka cannot un-publish: when this fails some messages may
    /// have been delivered, and the caller's one-by-one retry sends them again (at-least-once, as
    /// for a failed batch, but over up to the whole group).
    async fn execute_events_group_with_hook(
        &mut self,
        group: &[EventTransaction<'_>],
    ) -> Result<()> {
        let mut delivery_futures: Vec<DeliveryFuture> = Vec::new();
        for tx in group {
            self.enqueue_events(
                tx.events,
                tx.transaction_id,
                tx.commit_timestamp,
                tx.commit_lsn,
                &mut delivery_futures,
            )
            .await?;
        }
        self.await_delivery_futures(delivery_futures).await
    }

    /// Kafka cannot un-publish, so the snapshot target policy has nothing to check or clear.
    async fn snapshot_table_is_empty(&mut self, _schema: &str, _table: &str) -> Result<bool> {
        Ok(true)
    }

    /// Snapshot rows are `op = "r"` messages stamped with the snapshot's consistent point. Every
    /// message of the batch is enqueued before any delivery is awaited, so one batch is one
    /// pipelined round trip; a failed delivery fails the batch.
    async fn execute_snapshot_events_batch(
        &mut self,
        events: &[ChangeEvent],
        consistent_point: Lsn,
    ) -> Result<()> {
        let copied_at = Utc::now();
        let mut delivery_futures: Vec<DeliveryFuture> = Vec::with_capacity(events.len());
        for event in events {
            let pg_walstream::EventType::Insert {
                schema,
                table,
                data,
                ..
            } = &event.event_type
            else {
                return Err(CdcError::generic(
                    "a Kafka snapshot batch carries only INSERT events",
                ));
            };
            let (topic, key, value) = self.insert_message(
                "r",
                schema,
                table,
                data,
                0,
                copied_at,
                Some(consistent_point),
            )?;
            let future = self.enqueue_event(&topic, key.as_deref(), &value).await?;
            delivery_futures.push(future);
        }
        self.await_delivery_futures(delivery_futures).await
    }

    /// Snapshot messages must carry the key a live Update/Delete of the same row will, so the
    /// coordinator hands over each table's replica-identity key columns (the snapshot's own
    /// relation descriptors carry none). An explicit `CDC_KAFKA_KEY_COLUMNS` entry still wins
    /// (`build_key_for_insert` consults it first).
    fn set_snapshot_key_columns(&mut self, source_table_key: &str, columns: Vec<Arc<str>>) {
        if !columns.is_empty() {
            self.stream_key_columns
                .insert(source_table_key.to_owned(), columns);
        }
    }

    /// One writer: the producer already pipelines a whole batch, and one handler keeps a single
    /// per-topic ordering.
    fn max_snapshot_writers(&self) -> usize {
        1
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
    fn size_too_large_failure_names_topic_size_and_settings() {
        let err = KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge);
        let text = describe_send_failure("pg2any.db.public.items", 2_097_152, &err);
        assert!(text.contains("pg2any.db.public.items"), "{text}");
        assert!(text.contains("2097152-byte"), "{text}");
        assert!(text.contains("MessageSizeTooLarge"), "{text}");
        assert!(text.contains("CDC_KAFKA_MESSAGE_MAX_BYTES"), "{text}");
        assert!(text.contains("message.max.bytes"), "{text}");
        assert!(text.contains("max.message.bytes"), "{text}");
    }

    #[test]
    fn generic_failure_names_topic_and_size_without_size_advice() {
        let err = KafkaError::MessageProduction(RDKafkaErrorCode::UnknownTopicOrPartition);
        let text = describe_send_failure("t1", 10, &err);
        assert!(text.contains("'t1'"), "{text}");
        assert!(text.contains("10-byte"), "{text}");
        assert!(!text.contains("CDC_KAFKA_MESSAGE_MAX_BYTES"), "{text}");
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

    /// An INSERT-only publication may publish a column list that leaves out part of a composite
    /// replica identity (PostgreSQL 15+), while the catalog lookup still reports the whole
    /// identity. A key with only the published part would give distinct rows the same key.
    #[test]
    fn insert_key_is_not_emitted_when_a_key_column_is_missing_from_the_row() {
        let mut dest = test_destination();
        dest.set_snapshot_key_columns("public.t", vec![Arc::from("a"), Arc::from("b")]);

        let published_only = RowData::from_pairs(vec![
            ("a", ColumnValue::text("1")),
            ("c", ColumnValue::text("x")),
        ]);
        let key = dest
            .build_key_for_insert("k", "public.t", &published_only)
            .unwrap();
        assert!(key.is_none(), "a partial key would collide: {key:?}");

        let whole = RowData::from_pairs(vec![
            ("a", ColumnValue::text("1")),
            ("b", ColumnValue::text("2")),
            ("c", ColumnValue::text("x")),
        ]);
        let key = dest.build_key_for_insert("k", "public.t", &whole).unwrap();
        let key: Value = serde_json::from_str(&key.expect("a complete key is emitted")).unwrap();
        assert_eq!(key["payload"], json!({"a": "1", "b": "2"}));
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

    fn user_row() -> RowData {
        RowData::from_pairs(vec![
            ("id", ColumnValue::text("42")),
            ("name", ColumnValue::text("Alice")),
        ])
    }

    fn message_json(dest: &mut KafkaDestination, op: &str, lsn: Option<Lsn>) -> Value {
        let (topic, _key, value) = dest
            .insert_message(op, "public", "users", &user_row(), 0, Utc::now(), lsn)
            .unwrap();
        assert_eq!(topic, "test_prefix.public.users");
        serde_json::from_slice(&value).unwrap()
    }

    #[test]
    fn snapshot_message_has_op_r_live_has_op_c() {
        let mut dest = test_destination();

        let snapshot = message_json(&mut dest, "r", Some(Lsn(0x1234)));
        assert_eq!(snapshot["payload"]["op"], "r");
        assert_eq!(snapshot["payload"]["before"], Value::Null);
        assert_eq!(snapshot["payload"]["after"]["id"], "42");
        assert_eq!(snapshot["payload"]["source"]["lsn"], 0x1234);
        assert_eq!(snapshot["payload"]["source"]["txId"], 0);

        let live = message_json(&mut dest, "c", Some(Lsn(0x5678)));
        assert_eq!(live["payload"]["op"], "c");
        assert_eq!(live["payload"]["after"], snapshot["payload"]["after"]);
        assert_eq!(live["payload"]["source"]["lsn"], 0x5678);
        assert_eq!(live["schema"], snapshot["schema"], "same envelope shape");
    }

    #[test]
    fn snapshot_key_equals_live_update_key_for_same_row() {
        let mut dest = test_destination();
        let key_columns: Vec<Arc<str>> = vec![Arc::from("id")];
        dest.set_snapshot_key_columns("public.users", key_columns.clone());

        let (_, snapshot_key, _) = dest
            .insert_message("r", "public", "users", &user_row(), 0, Utc::now(), None)
            .unwrap();

        // A live UPDATE/DELETE builds its key from the event's own key columns.
        let key_schema = dest
            .table_names("public.users", "public", "users")
            .key_schema
            .clone();
        let after_update = RowData::from_pairs(vec![
            ("id", ColumnValue::text("42")),
            ("name", ColumnValue::text("Alicia")),
        ]);
        let update_key = dest
            .build_key_from_data(&key_schema, "public.users", &after_update, &key_columns)
            .unwrap();
        let delete_key = dest
            .build_key_from_data(&key_schema, "public.users", &user_row(), &key_columns)
            .unwrap();

        assert!(snapshot_key.is_some(), "a keyed table gets a keyed message");
        assert_eq!(snapshot_key, update_key);
        assert_eq!(snapshot_key, delete_key);
    }

    #[test]
    fn snapshot_key_is_unset_for_a_table_without_key_columns() {
        let mut dest = test_destination();
        dest.set_snapshot_key_columns("public.users", Vec::new());
        let (_, key, _) = dest
            .insert_message("r", "public", "users", &user_row(), 0, Utc::now(), None)
            .unwrap();
        assert!(key.is_none());
    }

    #[test]
    fn explicit_key_columns_config_wins_over_snapshot_key_columns() {
        let mut dest = test_destination();
        dest.key_columns_config
            .insert("public.users".to_string(), vec!["name".to_string()]);
        dest.set_snapshot_key_columns("public.users", vec![Arc::from("id")]);

        let (_, key, _) = dest
            .insert_message("r", "public", "users", &user_row(), 0, Utc::now(), None)
            .unwrap();
        let key: Value = serde_json::from_str(key.as_deref().unwrap()).unwrap();
        assert_eq!(key["payload"]["name"], "Alice");
        assert!(key["payload"].get("id").is_none());
    }

    #[test]
    fn max_snapshot_writers_is_one() {
        assert_eq!(KafkaDestination::new().max_snapshot_writers(), 1);
    }

    #[tokio::test]
    async fn policy_probe_is_empty_true() {
        let mut dest = KafkaDestination::new();
        assert!(dest
            .snapshot_table_is_empty("public", "users")
            .await
            .unwrap());
        dest.snapshot_truncate_table("public", "users")
            .await
            .expect("Kafka has no truncate: a no-op");
    }

    #[tokio::test]
    async fn snapshot_batch_rejects_anything_but_inserts() {
        let mut dest = test_destination();
        let delete = ChangeEvent::delete(
            "public",
            "users",
            1,
            user_row(),
            pg_walstream::ReplicaIdentity::Default,
            vec![Arc::from("id")],
            Lsn(1),
        );
        let err = dest
            .execute_snapshot_events_batch(&[delete], Lsn(1))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("only INSERT"), "{err}");
    }

    #[tokio::test]
    async fn snapshot_batch_without_a_producer_fails_instead_of_dropping_rows() {
        let mut dest = test_destination();
        let insert = ChangeEvent::insert("public", "users", 1, user_row(), Lsn(1));
        let err = dest
            .execute_snapshot_events_batch(&[insert], Lsn(1))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not initialized"), "{err}");
        dest.execute_snapshot_events_batch(&[], Lsn(1))
            .await
            .expect("an empty batch has nothing to deliver");
    }
}
