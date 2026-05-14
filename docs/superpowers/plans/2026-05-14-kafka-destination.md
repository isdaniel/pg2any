# Kafka Destination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Kafka as a CDC destination publishing Debezium-compatible JSON, plus perf improvements, redundant code removal, and unit test coverage.

**Architecture:** Extend `DestinationHandler` trait with an event-mode path (`supports_event_mode()` + `execute_events_batch_with_hook()`). Kafka destination serializes `ChangeEvent` to Debezium JSON envelope and publishes via rdkafka. SQL destinations are unchanged. The `TransactionManager` skips SQL generation for event-mode destinations, storing serialized JSON events to transaction files instead.

**Tech Stack:** rdkafka (librdkafka wrapper), serde_json, tokio, async-trait

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `Cargo.toml` (workspace) | Modify | Add `rdkafka` workspace dependency |
| `pg2any-lib/Cargo.toml` | Modify | Add `rdkafka` dep + `kafka` feature flag |
| `pg2any-lib/src/types.rs` | Modify | Add `Kafka` variant to `DestinationType` |
| `pg2any-lib/src/error.rs` | Modify | Add `Kafka` error variant (feature-gated) |
| `pg2any-lib/src/destinations/destination_factory.rs` | Modify | Extend trait + factory |
| `pg2any-lib/src/destinations/kafka.rs` | Create | Kafka destination + Debezium serializer |
| `pg2any-lib/src/destinations/mod.rs` | Modify | Add `kafka` module |
| `pg2any-lib/src/lib.rs` | Modify | Re-export `KafkaDestination` |
| `pg2any-lib/src/env.rs` | Modify | Add Kafka env var parsing + `DestinationType::Kafka` |
| `pg2any-lib/src/transaction_manager.rs` | Modify | Handle `Kafka` variant in match arms, event-mode path, perf fix |
| `pg2any-lib/src/destinations/common.rs` | Modify | Remove unused `map_schema()` |
| `pg2any-lib/tests/kafka_destination_tests.rs` | Create | Unit tests for Kafka destination |

---

### Task 1: Add `Kafka` variant to `DestinationType` and handle it in match arms

**Files:**
- Modify: `pg2any-lib/src/types.rs:7-21`
- Modify: `pg2any-lib/src/transaction_manager.rs:1163-1176` (generate_truncate_sql)
- Modify: `pg2any-lib/src/transaction_manager.rs:1236-1246` (append_qualified_table)
- Modify: `pg2any-lib/src/transaction_manager.rs:1252-1256` (append_quoted_identifier)
- Modify: `pg2any-lib/src/transaction_manager.rs:1288-1291` (append_hex_literal)
- Modify: `pg2any-lib/src/transaction_manager.rs:1319` (append_value — escape_backslash)

- [ ] **Step 1: Add `Kafka` to `DestinationType` enum**

In `pg2any-lib/src/types.rs`, add `Kafka` to the enum and `Display` impl:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DestinationType {
    MySQL,
    SqlServer,
    SQLite,
    Kafka,
}

impl std::fmt::Display for DestinationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DestinationType::MySQL => write!(f, "mysql"),
            DestinationType::SqlServer => write!(f, "sqlserver"),
            DestinationType::SQLite => write!(f, "sqlite"),
            DestinationType::Kafka => write!(f, "kafka"),
        }
    }
}
```

- [ ] **Step 2: Fix all non-exhaustive match arms in `transaction_manager.rs`**

For Kafka, the `TransactionManager` will never call SQL-generation methods (event-mode bypasses them). But we still need the match arms to compile. Add `DestinationType::Kafka` arms that return empty string / unreachable:

In `generate_truncate_sql` (around line 1163), add after the `SQLite` arm:
```rust
DestinationType::Kafka => {
    // Kafka uses event-mode, SQL generation is never called
}
```

In `append_qualified_table` (around line 1236), add after the `SQLite` arm:
```rust
DestinationType::Kafka => {
    self.append_quoted_identifier(out, table);
}
```

In `append_quoted_identifier` (around line 1252), add `Kafka` to the existing match:
```rust
DestinationType::MySQL => ('`', '`', '`'),
DestinationType::SqlServer => ('[', ']', ']'),
DestinationType::SQLite | DestinationType::Kafka => ('"', '"', '"'),
```

In `append_hex_literal` (around line 1289), add `Kafka` alongside MySQL/SQLite:
```rust
DestinationType::MySQL | DestinationType::SQLite | DestinationType::Kafka => ("X'", "'"),
```

In `append_value` (around line 1319), the `escape_backslash` match — `Kafka` should NOT escape backslashes:
```rust
let escape_backslash = matches!(self.destination_type, DestinationType::MySQL);
```
This line already excludes Kafka, so no change needed here. Just verify the match is exhaustive.

- [ ] **Step 3: Build to verify all match arms compile**

Run: `cargo build --manifest-path /home/azureuser/pg2any/Cargo.toml --workspace 2>&1`
Expected: successful build with no match-arm warnings

- [ ] **Step 4: Run existing tests to confirm nothing broke**

Run: `cargo test --manifest-path /home/azureuser/pg2any/Cargo.toml --workspace 2>&1`
Expected: all tests pass

- [ ] **Step 5: Commit**

```bash
git add pg2any-lib/src/types.rs pg2any-lib/src/transaction_manager.rs
git commit -m "feat: add Kafka variant to DestinationType and handle in all match arms"
```

---

### Task 2: Add rdkafka dependency and `kafka` feature flag

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Modify: `pg2any-lib/Cargo.toml`

- [ ] **Step 1: Add rdkafka to workspace dependencies**

In the root `Cargo.toml`, add to `[workspace.dependencies]`:
```toml
rdkafka = { version = "0.37", features = ["cmake-build", "tokio"] }
```

- [ ] **Step 2: Add rdkafka to pg2any-lib and create `kafka` feature**

In `pg2any-lib/Cargo.toml`, add under `[dependencies]`:
```toml
rdkafka = { workspace = true, optional = true }
```

Add the `kafka` feature under `[features]`:
```toml
kafka = ["rdkafka"]
```

Do NOT add `kafka` to `default` features — it requires librdkafka build deps.

- [ ] **Step 3: Build with `--features kafka` to verify dependency resolves**

Run: `cargo build --manifest-path /home/azureuser/pg2any/Cargo.toml -p pg2any_lib --features kafka 2>&1`
Expected: successful build (rdkafka compiles librdkafka via cmake)

- [ ] **Step 4: Build without kafka feature to verify default features still work**

Run: `cargo build --manifest-path /home/azureuser/pg2any/Cargo.toml --workspace 2>&1`
Expected: successful build, no rdkafka compilation

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml pg2any-lib/Cargo.toml
git commit -m "feat: add rdkafka dependency and kafka feature flag"
```

---

### Task 3: Extend `DestinationHandler` trait with event-mode methods

**Files:**
- Modify: `pg2any-lib/src/destinations/destination_factory.rs`

- [ ] **Step 1: Add event-mode methods to `DestinationHandler` trait**

Add the following imports at the top of `destination_factory.rs`:
```rust
use chrono::{DateTime, Utc};
use pg_walstream::ChangeEvent;
use crate::types::Lsn;
```

Add these methods to the `DestinationHandler` trait (after `close()`):

```rust
    /// Whether this destination consumes structured events instead of SQL.
    /// When true, TransactionManager stores raw ChangeEvents (as JSON) instead of
    /// generated SQL, and calls execute_events_batch_with_hook() during consumption.
    fn supports_event_mode(&self) -> bool {
        false
    }

    /// Execute a batch of change events directly (for non-SQL destinations like Kafka).
    /// Default implementation returns Unsupported error.
    async fn execute_events_batch_with_hook(
        &mut self,
        _events: &[ChangeEvent],
        _transaction_id: u32,
        _commit_timestamp: DateTime<Utc>,
        _commit_lsn: Option<Lsn>,
        _pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        Err(CdcError::unsupported(
            "Event mode not supported by this destination",
        ))
    }
```

- [ ] **Step 2: Add Kafka to factory `create()` method**

In `DestinationFactory::create()`, add a new match arm:
```rust
#[cfg(feature = "kafka")]
DestinationType::Kafka => Ok(Box::new(super::kafka::KafkaDestination::new())),
```

- [ ] **Step 3: Build to verify trait compiles**

Run: `cargo build --manifest-path /home/azureuser/pg2any/Cargo.toml --workspace 2>&1`
Expected: successful build (Kafka arm only active with feature flag, factory creates it conditionally)

- [ ] **Step 4: Run tests**

Run: `cargo test --manifest-path /home/azureuser/pg2any/Cargo.toml --workspace 2>&1`
Expected: all existing tests pass

- [ ] **Step 5: Commit**

```bash
git add pg2any-lib/src/destinations/destination_factory.rs
git commit -m "feat: extend DestinationHandler trait with event-mode support"
```

---

### Task 4: Implement `KafkaDestination` with Debezium serializer

**Files:**
- Create: `pg2any-lib/src/destinations/kafka.rs`
- Modify: `pg2any-lib/src/destinations/mod.rs`
- Modify: `pg2any-lib/src/lib.rs`
- Modify: `pg2any-lib/src/error.rs`

This is the largest task. It creates the Kafka destination module with:
1. Debezium JSON envelope serialization
2. rdkafka producer integration
3. Graceful shutdown (flush on close)

- [ ] **Step 1: Add Kafka error variant**

In `pg2any-lib/src/error.rs`, add after the MySQL error variant:
```rust
    /// Kafka producer errors
    #[cfg(feature = "kafka")]
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
```

- [ ] **Step 2: Create `pg2any-lib/src/destinations/kafka.rs`**

```rust
use super::destination_factory::{DestinationHandler, PreCommitHook};
use crate::error::{CdcError, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pg_walstream::{ChangeEvent, ColumnValue, Lsn};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

const DEFAULT_TOPIC_PREFIX: &str = "pg2any";
const DEFAULT_FLUSH_TIMEOUT: Duration = Duration::from_secs(30);
const LIB_VERSION: &str = env!("CARGO_PKG_VERSION");

pub struct KafkaDestination {
    producer: Option<FutureProducer>,
    topic_prefix: String,
    source_db_name: String,
    schema_mappings: HashMap<String, String>,
}

impl KafkaDestination {
    pub fn new() -> Self {
        Self {
            producer: None,
            topic_prefix: DEFAULT_TOPIC_PREFIX.to_string(),
            source_db_name: "postgres".to_string(),
            schema_mappings: HashMap::new(),
        }
    }

    fn topic_name(&self, schema: &str, table: &str) -> String {
        format!("{}.{}.{}", self.topic_prefix, schema, table)
    }

    fn map_schema<'a>(&'a self, source_schema: &'a str) -> &'a str {
        self.schema_mappings
            .get(source_schema)
            .map(String::as_str)
            .unwrap_or(source_schema)
    }

    fn column_value_to_json(value: &ColumnValue) -> Value {
        match value {
            ColumnValue::Null => Value::Null,
            ColumnValue::Text(b) => match std::str::from_utf8(b) {
                Ok(s) => Value::String(s.to_string()),
                Err(_) => Value::String(format!("\\x{}", hex::encode(b))),
            },
            ColumnValue::Binary(b) => Value::String(format!("\\x{}", hex::encode(b))),
        }
    }

    fn row_data_to_json(data: &pg_walstream::RowData) -> Value {
        let mut map = serde_json::Map::new();
        for (col_name, col_value) in data.iter() {
            map.insert(col_name.to_string(), Self::column_value_to_json(col_value));
        }
        Value::Object(map)
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

    fn build_debezium_envelope(
        &self,
        op: &str,
        schema: &str,
        table: &str,
        before: Option<Value>,
        after: Option<Value>,
        transaction_id: u32,
        commit_timestamp: DateTime<Utc>,
        lsn: Option<Lsn>,
    ) -> Value {
        let source = self.build_source_block(
            schema,
            table,
            transaction_id,
            commit_timestamp,
            lsn,
        );

        json!({
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "struct", "fields": [], "optional": true, "field": "before"},
                    {"type": "struct", "fields": [], "optional": true, "field": "after"},
                    {"type": "struct", "fields": [], "optional": false, "field": "source"},
                    {"type": "string", "optional": false, "field": "op"},
                    {"type": "int64", "optional": true, "field": "ts_ms"}
                ],
                "optional": false,
                "name": format!("{}.{}.{}.Envelope", self.topic_prefix, schema, table)
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
        schema: &str,
        table: &str,
        data: &pg_walstream::RowData,
        key_columns: &[Arc<str>],
    ) -> Option<String> {
        if key_columns.is_empty() {
            return None;
        }

        let mut payload = serde_json::Map::new();
        let mut fields = Vec::new();

        for col in key_columns {
            if let Some(val) = data.get(col.as_ref()) {
                payload.insert(col.to_string(), Self::column_value_to_json(val));
                fields.push(json!({
                    "type": "string",
                    "optional": false,
                    "field": col.as_ref()
                }));
            }
        }

        let key = json!({
            "schema": {
                "type": "struct",
                "fields": fields,
                "optional": false,
                "name": format!("{}.{}.{}.Key", self.topic_prefix, schema, table)
            },
            "payload": Value::Object(payload)
        });

        serde_json::to_string(&key).ok()
    }

    async fn publish_event(
        &self,
        topic: &str,
        key: Option<&str>,
        value: &str,
    ) -> Result<()> {
        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| CdcError::generic("Kafka producer not initialized"))?;

        let mut record = FutureRecord::to(topic).payload(value);
        if let Some(k) = key {
            record = record.key(k);
        }

        producer
            .send(record, Duration::from_secs(10))
            .await
            .map_err(|(err, _)| CdcError::generic(format!("Kafka send failed: {err}")))?;

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
        // connection_string is the bootstrap servers
        let topic_prefix = std::env::var("CDC_KAFKA_TOPIC_PREFIX")
            .unwrap_or_else(|_| DEFAULT_TOPIC_PREFIX.to_string());
        let source_db_name = std::env::var("CDC_KAFKA_SOURCE_DB_NAME")
            .unwrap_or_else(|_| "postgres".to_string());
        let security_protocol = std::env::var("CDC_KAFKA_SECURITY_PROTOCOL")
            .unwrap_or_else(|_| "plaintext".to_string());
        let compression = std::env::var("CDC_KAFKA_COMPRESSION")
            .unwrap_or_else(|_| "lz4".to_string());
        let batch_size = std::env::var("CDC_KAFKA_BATCH_SIZE")
            .unwrap_or_else(|_| "16384".to_string());
        let linger_ms = std::env::var("CDC_KAFKA_LINGER_MS")
            .unwrap_or_else(|_| "5".to_string());
        let acks = std::env::var("CDC_KAFKA_ACKS")
            .unwrap_or_else(|_| "all".to_string());
        let message_max_bytes = std::env::var("CDC_KAFKA_MESSAGE_MAX_BYTES")
            .unwrap_or_else(|_| "1048576".to_string());
        let retries = std::env::var("CDC_KAFKA_RETRIES")
            .unwrap_or_else(|_| "3".to_string());

        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", connection_string)
            .set("security.protocol", &security_protocol)
            .set("compression.type", &compression)
            .set("batch.size", &batch_size)
            .set("linger.ms", &linger_ms)
            .set("acks", &acks)
            .set("message.max.bytes", &message_max_bytes)
            .set("retries", &retries);

        // SASL configuration
        if let Ok(mechanism) = std::env::var("CDC_KAFKA_SASL_MECHANISM") {
            config.set("sasl.mechanism", &mechanism);
        }
        if let Ok(username) = std::env::var("CDC_KAFKA_SASL_USERNAME") {
            config.set("sasl.username", &username);
        }
        if let Ok(password) = std::env::var("CDC_KAFKA_SASL_PASSWORD") {
            config.set("sasl.password", &password);
        }

        let producer: FutureProducer = config.create().map_err(|e| {
            CdcError::generic(format!("Failed to create Kafka producer: {e}"))
        })?;

        self.producer = Some(producer);
        self.topic_prefix = topic_prefix;
        self.source_db_name = source_db_name;

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

        for event in events {
            match &event.event_type {
                pg_walstream::EventType::Insert {
                    schema,
                    table,
                    data,
                    ..
                } => {
                    let schema = self.map_schema(schema);
                    let topic = self.topic_name(schema, table);
                    let after = Some(Self::row_data_to_json(data));
                    let envelope = self.build_debezium_envelope(
                        "c", schema, table, None, after,
                        transaction_id, commit_timestamp, commit_lsn,
                    );
                    let value = serde_json::to_string(&envelope)
                        .map_err(|e| CdcError::generic(format!("JSON serialization failed: {e}")))?;
                    self.publish_event(&topic, None, &value).await?;
                }
                pg_walstream::EventType::Update {
                    schema,
                    table,
                    old_data,
                    new_data,
                    key_columns,
                    ..
                } => {
                    let schema = self.map_schema(schema);
                    let topic = self.topic_name(schema, table);
                    let before = old_data.as_ref().map(Self::row_data_to_json);
                    let after = Some(Self::row_data_to_json(new_data));
                    let key_data = old_data.as_ref().unwrap_or(new_data);
                    let key = self.build_key_from_data(schema, table, key_data, key_columns);
                    let envelope = self.build_debezium_envelope(
                        "u", schema, table, before, after,
                        transaction_id, commit_timestamp, commit_lsn,
                    );
                    let value = serde_json::to_string(&envelope)
                        .map_err(|e| CdcError::generic(format!("JSON serialization failed: {e}")))?;
                    self.publish_event(&topic, key.as_deref(), &value).await?;
                }
                pg_walstream::EventType::Delete {
                    schema,
                    table,
                    old_data,
                    key_columns,
                    ..
                } => {
                    let schema = self.map_schema(schema);
                    let topic = self.topic_name(schema, table);
                    let before = Some(Self::row_data_to_json(old_data));
                    let key = self.build_key_from_data(schema, table, old_data, key_columns);
                    let envelope = self.build_debezium_envelope(
                        "d", schema, table, before, None,
                        transaction_id, commit_timestamp, commit_lsn,
                    );
                    let value = serde_json::to_string(&envelope)
                        .map_err(|e| CdcError::generic(format!("JSON serialization failed: {e}")))?;
                    self.publish_event(&topic, key.as_deref(), &value).await?;
                }
                pg_walstream::EventType::Truncate(tables) => {
                    for table_spec in tables.iter() {
                        let (schema, table) = match table_spec.split_once('.') {
                            Some((s, t)) if !t.contains('.') => (self.map_schema(s), t),
                            _ => (self.map_schema("public"), table_spec.as_ref()),
                        };
                        let topic = self.topic_name(schema, table);
                        let envelope = self.build_debezium_envelope(
                            "t", schema, table, None, None,
                            transaction_id, commit_timestamp, commit_lsn,
                        );
                        let value = serde_json::to_string(&envelope)
                            .map_err(|e| CdcError::generic(format!("JSON serialization failed: {e}")))?;
                        self.publish_event(&topic, None, &value).await?;
                    }
                }
                _ => {
                    // Skip non-DML events (Begin, Commit, StreamStart, etc.)
                    debug!("Skipping non-DML event for Kafka: {:?}", std::mem::discriminant(&event.event_type));
                }
            }
        }

        // Execute pre-commit hook AFTER all events published successfully
        if let Some(hook) = pre_commit_hook {
            hook().await?;
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(producer) = self.producer.take() {
            info!("Flushing Kafka producer (timeout {:?})...", DEFAULT_FLUSH_TIMEOUT);
            producer.flush(DEFAULT_FLUSH_TIMEOUT).map_err(|e| {
                CdcError::generic(format!("Kafka producer flush failed: {e}"))
            })?;
            info!("Kafka producer flushed and closed successfully");
        }
        Ok(())
    }
}

// hex module for binary value encoding — tiny inline impl to avoid an extra dependency
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        const LUT: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            out.push(LUT[(b >> 4) as usize] as char);
            out.push(LUT[(b & 0x0f) as usize] as char);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_walstream::{ColumnValue, RowData};
    use std::sync::Arc;

    fn test_destination() -> KafkaDestination {
        let mut dest = KafkaDestination::new();
        dest.topic_prefix = "test_prefix".to_string();
        dest.source_db_name = "testdb".to_string();
        dest
    }

    #[test]
    fn test_topic_name() {
        let dest = test_destination();
        assert_eq!(dest.topic_name("public", "users"), "test_prefix.public.users");
        assert_eq!(dest.topic_name("myschema", "orders"), "test_prefix.myschema.orders");
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
        let result = KafkaDestination::column_value_to_json(
            &ColumnValue::Binary(bytes::Bytes::from_static(&[0xde, 0xad])),
        );
        assert_eq!(result, Value::String("\\xdead".to_string()));
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
    fn test_build_debezium_envelope_insert() {
        let dest = test_destination();
        let ts = DateTime::parse_from_rfc3339("2024-01-15T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let after_data = json!({"id": "1", "name": "Alice"});

        let envelope = dest.build_debezium_envelope(
            "c", "public", "users",
            None, Some(after_data.clone()),
            100, ts, Some(Lsn(12345)),
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
    }

    #[test]
    fn test_build_debezium_envelope_delete() {
        let dest = test_destination();
        let ts = Utc::now();
        let before_data = json!({"id": "1", "name": "Alice"});

        let envelope = dest.build_debezium_envelope(
            "d", "public", "users",
            Some(before_data.clone()), None,
            101, ts, None,
        );

        assert_eq!(envelope["payload"]["op"], "d");
        assert_eq!(envelope["payload"]["before"], before_data);
        assert_eq!(envelope["payload"]["after"], Value::Null);
        assert_eq!(envelope["payload"]["source"]["lsn"], Value::Null);
    }

    #[test]
    fn test_build_debezium_envelope_truncate() {
        let dest = test_destination();
        let ts = Utc::now();

        let envelope = dest.build_debezium_envelope(
            "t", "public", "users",
            None, None,
            102, ts, Some(Lsn(99999)),
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

        let key = dest.build_key_from_data("public", "users", &data, &key_columns);
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

        let key = dest.build_key_from_data("public", "users", &data, &key_columns);
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

        let key = dest.build_key_from_data("public", "users", &data, &key_columns);
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
    fn test_hex_encode() {
        assert_eq!(hex::encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
        assert_eq!(hex::encode(&[]), "");
        assert_eq!(hex::encode(&[0x00, 0xff]), "00ff");
    }
}
```

- [ ] **Step 3: Add kafka module to `destinations/mod.rs`**

Add after the `sqlite` module declaration:
```rust
/// Kafka destination implementation
#[cfg(feature = "kafka")]
pub mod kafka;
```

Add re-export:
```rust
#[cfg(feature = "kafka")]
pub use kafka::KafkaDestination;
```

- [ ] **Step 4: Add Kafka re-export to `lib.rs`**

Add after the SQLite re-export:
```rust
#[cfg(feature = "kafka")]
pub use crate::destinations::KafkaDestination;
```

- [ ] **Step 5: Build with kafka feature**

Run: `cargo build --manifest-path /home/azureuser/pg2any/Cargo.toml -p pg2any_lib --features kafka 2>&1`
Expected: successful build

- [ ] **Step 6: Run tests with kafka feature**

Run: `cargo test --manifest-path /home/azureuser/pg2any/Cargo.toml -p pg2any_lib --features kafka 2>&1`
Expected: all tests pass including new kafka module tests

- [ ] **Step 7: Run tests without kafka feature**

Run: `cargo test --manifest-path /home/azureuser/pg2any/Cargo.toml --workspace 2>&1`
Expected: all existing tests pass, kafka tests skipped

- [ ] **Step 8: Commit**

```bash
git add pg2any-lib/src/destinations/kafka.rs pg2any-lib/src/destinations/mod.rs pg2any-lib/src/lib.rs pg2any-lib/src/error.rs
git commit -m "feat: implement KafkaDestination with Debezium-compatible JSON serializer"
```

---

### Task 5: Add Kafka to env config and `DestinationFactory`

**Files:**
- Modify: `pg2any-lib/src/env.rs`

- [ ] **Step 1: Add Kafka to destination type parsing in `load_config_from_env()`**

In `pg2any-lib/src/env.rs`, find the `dest_type` match block (around line 66) and add the Kafka case:

```rust
        "Kafka" | "kafka" => DestinationType::Kafka,
```

So the full match becomes:
```rust
    let dest_type = match dest_type_str.as_str() {
        "MySQL" | "mysql" => DestinationType::MySQL,
        "SqlServer" | "sqlserver" => DestinationType::SqlServer,
        "SQLite" | "sqlite" => DestinationType::SQLite,
        "Kafka" | "kafka" => DestinationType::Kafka,
        _ => {
            tracing::warn!(
                "Unknown destination type '{}', defaulting to MySQL",
                dest_type_str
            );
            DestinationType::MySQL
        }
    };
```

- [ ] **Step 2: Build and test**

Run: `cargo build --manifest-path /home/azureuser/pg2any/Cargo.toml --workspace 2>&1`
Expected: successful build

Run: `cargo test --manifest-path /home/azureuser/pg2any/Cargo.toml --workspace 2>&1`
Expected: all tests pass

- [ ] **Step 3: Commit**

```bash
git add pg2any-lib/src/env.rs
git commit -m "feat: add Kafka destination type to env config parsing"
```

---

### Task 6: Add event-mode path to `TransactionManager`

**Files:**
- Modify: `pg2any-lib/src/transaction_manager.rs`

The `TransactionManager` currently always generates SQL from events. For event-mode destinations, it should store raw `ChangeEvent`s (serialized as JSON lines) in the transaction files instead, and call `execute_events_batch_with_hook()` during consumption.

- [ ] **Step 1: Add `event_mode` flag to `TransactionManager`**

Add field to the struct:
```rust
pub struct TransactionManager {
    // ... existing fields ...
    /// When true, stores raw ChangeEvent JSON instead of generated SQL
    event_mode: bool,
}
```

Update `new()` to initialize it:
```rust
event_mode: false,
```

Add a setter:
```rust
pub fn set_event_mode(&mut self, enabled: bool) {
    self.event_mode = enabled;
}
```

- [ ] **Step 2: Modify `append_event` to store JSON in event-mode**

At the top of `append_event()`, before `generate_sql_for_event`, add:
```rust
    pub async fn append_event(&self, tx_id: u32, event: &ChangeEvent) -> Result<()> {
        let line = if self.event_mode {
            // Event-mode: store serialized ChangeEvent JSON instead of SQL
            match &event.event_type {
                EventType::Insert { .. }
                | EventType::Update { .. }
                | EventType::Delete { .. }
                | EventType::Truncate(_) => {
                    serde_json::to_string(event).map_err(|e| {
                        CdcError::generic(format!("Failed to serialize event: {e}"))
                    })?
                }
                _ => return Ok(()),
            }
        } else {
            let sql = self.generate_sql_for_event(event)?;
            if sql.is_empty() {
                return Ok(());
            }
            sql
        };

        let mut transactions = self.active_transactions.lock().await;
        // ... rest uses `line` instead of `sql` ...
```

Replace `sql` with `line` in the remaining body of the function (the `sql_bytes`, `tx_state.writer.append(&sql)` calls).

- [ ] **Step 3: Add event-mode batch execution method**

Add a new method `process_transaction_file_event_mode` (or integrate into the existing flow). This reads JSON lines from the segment files, deserializes them to `ChangeEvent`, and calls `execute_events_batch_with_hook()`:

```rust
    pub(crate) async fn process_transaction_file_event_mode(
        self: Arc<Self>,
        pending_tx: &PendingTransactionFile,
        destination_handler: &mut Box<dyn DestinationHandler>,
        cancellation_token: &CancellationToken,
        lsn_tracker: &Arc<LsnTracker>,
        metrics_collector: &Arc<MetricsCollector>,
        batch_size: usize,
        shared_lsn_feedback: &Arc<SharedLsnFeedback>,
    ) -> Result<()> {
        let tx_id = pending_tx.metadata.transaction_id;
        let start_time = std::time::Instant::now();

        let latest_metadata = self.read_metadata(&pending_tx.file_path).await?;
        let segments = if !latest_metadata.segments.is_empty() {
            latest_metadata.segments
        } else {
            pending_tx.metadata.segments.clone()
        };

        let mut all_events: Vec<ChangeEvent> = Vec::new();

        for segment in &segments {
            let content = tokio::fs::read_to_string(&segment.path).await.map_err(|e| {
                CdcError::generic(format!("Failed to read segment {:?}: {e}", segment.path))
            })?;

            for line in content.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                let event: ChangeEvent = serde_json::from_str(line).map_err(|e| {
                    CdcError::generic(format!("Failed to deserialize event: {e}"))
                })?;
                all_events.push(event);
            }
        }

        if cancellation_token.is_cancelled() {
            return Ok(());
        }

        // Process in batches
        let mut batch_count = 0usize;
        for chunk in all_events.chunks(batch_size) {
            if cancellation_token.is_cancelled() {
                warn!("Event-mode processing cancelled (tx_id: {})", tx_id);
                return Ok(());
            }

            batch_count += 1;
            let metadata_path = pending_tx.file_path.clone();
            let file_manager = self.clone();
            let staged_index = batch_count * batch_size;

            let pre_commit_hook: Option<PreCommitHook> = Some(Box::new(move || {
                let metadata_path = metadata_path.clone();
                let file_manager = file_manager.clone();
                Box::pin(async move {
                    file_manager
                        .stage_pending_metadata_progress(&metadata_path, staged_index)
                        .await?;
                    Ok(())
                })
            }));

            destination_handler
                .execute_events_batch_with_hook(
                    chunk,
                    tx_id,
                    pending_tx.metadata.commit_timestamp,
                    pending_tx.metadata.commit_lsn,
                    pre_commit_hook,
                )
                .await?;
        }

        let duration = start_time.elapsed();
        info!(
            "Event-mode: processed {} events in {} batches in {:?} (tx_id: {})",
            all_events.len(), batch_count, duration, tx_id
        );

        self.finalize_transaction_file(
            pending_tx,
            lsn_tracker,
            metrics_collector,
            all_events.len(),
            shared_lsn_feedback,
        )
        .await?;

        Ok(())
    }
```

- [ ] **Step 4: Build and test**

Run: `cargo build --manifest-path /home/azureuser/pg2any/Cargo.toml --workspace 2>&1`
Expected: successful build

Run: `cargo test --manifest-path /home/azureuser/pg2any/Cargo.toml --workspace 2>&1`
Expected: all existing tests pass

- [ ] **Step 5: Commit**

```bash
git add pg2any-lib/src/transaction_manager.rs
git commit -m "feat: add event-mode path to TransactionManager for non-SQL destinations"
```

---

### Task 7: Performance improvement — reduce lock contention in `append_event`

**Files:**
- Modify: `pg2any-lib/src/transaction_manager.rs`

- [ ] **Step 1: Move SQL/JSON generation outside the mutex lock**

Currently in `append_event()`, `generate_sql_for_event()` (or JSON serialization for event-mode) runs BEFORE the mutex lock. After the refactor in Task 6, the line generation already happens before the lock. Verify this is the case. If not, move the `let line = ...` block to before `let mut transactions = self.active_transactions.lock().await;`.

The key change: the `self.generate_sql_for_event(event)` call must NOT be inside the lock scope. It does string allocation and formatting which can take microseconds — holding the mutex during that time blocks all other producers.

- [ ] **Step 2: Verify the lock is only held for buffer operations**

After Task 6's refactor, the lock scope should look like:
```rust
let line = /* ... generate outside lock ... */;
let mut transactions = self.active_transactions.lock().await;
// Only buffer append + potential flush inside lock
```

- [ ] **Step 3: Run benchmarks / tests**

Run: `cargo test --manifest-path /home/azureuser/pg2any/Cargo.toml --workspace 2>&1`
Expected: all tests pass

- [ ] **Step 4: Commit**

```bash
git add pg2any-lib/src/transaction_manager.rs
git commit -m "perf: move SQL generation outside mutex lock in append_event"
```

---

### Task 8: Performance improvement — compact metadata serialization

**Files:**
- Modify: `pg2any-lib/src/transaction_manager.rs`

- [ ] **Step 1: Replace `to_string_pretty` with `to_string` for internal metadata**

Find all occurrences of `serde_json::to_string_pretty(&metadata)` in `transaction_manager.rs` and replace with `serde_json::to_string(&metadata)`. These are internal metadata files not meant for human consumption — compact JSON saves ~30% I/O.

Affected locations:
1. `begin_transaction()` (around line 415): `let metadata_json = serde_json::to_string_pretty(&metadata)?;` → `serde_json::to_string(&metadata)?`
2. `update_received_metadata_segments()` (around line 471): `serde_json::to_string_pretty(&metadata)` → `serde_json::to_string(&metadata)`
3. `write_pending_metadata_file()` (around line 646): `serde_json::to_string_pretty(metadata)` → `serde_json::to_string(metadata)`
4. `write_pending_metadata()` (around line 840): `serde_json::to_string_pretty(metadata)` → `serde_json::to_string(metadata)`

- [ ] **Step 2: Run tests**

Run: `cargo test --manifest-path /home/azureuser/pg2any/Cargo.toml --workspace 2>&1`
Expected: all tests pass (metadata format is internal, no test depends on pretty-printing)

- [ ] **Step 3: Commit**

```bash
git add pg2any-lib/src/transaction_manager.rs
git commit -m "perf: use compact JSON for internal transaction metadata files"
```

---

### Task 9: Remove redundant code

**Files:**
- Modify: `pg2any-lib/src/destinations/common.rs`

- [ ] **Step 1: Verify `map_schema` in `common.rs` is unused outside tests**

Run: `grep -rn "common::map_schema\|use.*common.*map_schema\|destinations::common::map_schema" pg2any-lib/src/ --include="*.rs"`
Expected: only references in `common.rs` itself (the definition and its test)

The `TransactionManager` has its own `map_schema()` method. The `common.rs` version is only used by its own `#[cfg(test)]` block. It can be removed.

- [ ] **Step 2: Remove `map_schema` function and its test from `common.rs`**

Remove the `map_schema` function (lines 11-16) and the entire `#[cfg(test)] mod tests` block (lines 89-101) from `common.rs`.

The file should only contain the `execute_sqlx_batch_with_hook` function.

- [ ] **Step 3: Build and test**

Run: `cargo test --manifest-path /home/azureuser/pg2any/Cargo.toml --workspace 2>&1`
Expected: all tests pass

- [ ] **Step 4: Commit**

```bash
git add pg2any-lib/src/destinations/common.rs
git commit -m "refactor: remove redundant map_schema from destinations/common.rs"
```

---

### Task 10: Add integration test file for Kafka destination

**Files:**
- Create: `pg2any-lib/tests/kafka_destination_tests.rs`

- [ ] **Step 1: Create `kafka_destination_tests.rs`**

```rust
//! Unit tests for Kafka destination serialization and configuration.
//! These tests do NOT require a running Kafka broker — they test
//! the Debezium JSON serialization, topic naming, and key extraction logic.

#[cfg(feature = "kafka")]
mod kafka_tests {
    use pg2any_lib::destinations::kafka::KafkaDestination;
    use pg2any_lib::destinations::destination_factory::DestinationHandler;
    use pg2any_lib::types::DestinationType;
    use pg2any_lib::DestinationFactory;
    use pg_walstream::{ChangeEvent, ColumnValue, Lsn, RowData};
    use std::sync::Arc;

    #[test]
    fn test_destination_factory_creates_kafka() {
        let result = DestinationFactory::create(&DestinationType::Kafka);
        assert!(result.is_ok());
    }

    #[test]
    fn test_kafka_supports_event_mode() {
        let dest = KafkaDestination::new();
        assert!(dest.supports_event_mode());
    }

    #[tokio::test]
    async fn test_kafka_rejects_sql_batch() {
        let mut dest = KafkaDestination::new();
        let commands = vec!["INSERT INTO t VALUES (1);".to_string()];
        let result = dest.execute_sql_batch_with_hook(&commands, None).await;
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("does not support SQL batch"));
    }

    #[tokio::test]
    async fn test_execute_events_empty_batch() {
        // An empty batch with no producer should succeed (no-op)
        // This verifies the early return path
        let mut dest = KafkaDestination::new();
        let events: Vec<ChangeEvent> = vec![];
        let ts = chrono::Utc::now();
        let result = dest
            .execute_events_batch_with_hook(&events, 1, ts, None, None)
            .await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_destination_type_kafka_display() {
        assert_eq!(DestinationType::Kafka.to_string(), "kafka");
    }

    #[test]
    fn test_destination_type_kafka_serialization() {
        let kafka_type = DestinationType::Kafka;
        let json = serde_json::to_string(&kafka_type).unwrap();
        assert_eq!(json, "\"Kafka\"");

        let deserialized: DestinationType = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, kafka_type);
    }
}
```

- [ ] **Step 2: Run kafka tests**

Run: `cargo test --manifest-path /home/azureuser/pg2any/Cargo.toml -p pg2any_lib --features kafka -- kafka 2>&1`
Expected: all kafka tests pass

- [ ] **Step 3: Run all tests (default features)**

Run: `cargo test --manifest-path /home/azureuser/pg2any/Cargo.toml --workspace 2>&1`
Expected: all tests pass (kafka tests skipped since kafka feature not in default)

- [ ] **Step 4: Commit**

```bash
git add pg2any-lib/tests/kafka_destination_tests.rs
git commit -m "test: add unit tests for Kafka destination serialization and factory"
```

---

### Task 11: Final verification — build and test all configurations

**Files:** None (verification only)

- [ ] **Step 1: Build with default features**

Run: `cargo build --manifest-path /home/azureuser/pg2any/Cargo.toml --workspace 2>&1`
Expected: successful build

- [ ] **Step 2: Test with default features**

Run: `cargo test --manifest-path /home/azureuser/pg2any/Cargo.toml --workspace 2>&1`
Expected: all tests pass

- [ ] **Step 3: Build with all features including kafka**

Run: `cargo build --manifest-path /home/azureuser/pg2any/Cargo.toml -p pg2any_lib --features "mysql,sqlserver,sqlite,kafka,metrics" 2>&1`
Expected: successful build

- [ ] **Step 4: Test with all features including kafka**

Run: `cargo test --manifest-path /home/azureuser/pg2any/Cargo.toml -p pg2any_lib --features "mysql,sqlserver,sqlite,kafka,metrics" 2>&1`
Expected: all tests pass

- [ ] **Step 5: Check for compiler warnings**

Run: `cargo build --manifest-path /home/azureuser/pg2any/Cargo.toml -p pg2any_lib --features "mysql,sqlserver,sqlite,kafka,metrics" 2>&1 | grep -i warning`
Expected: no warnings (or only upstream dependency warnings)

- [ ] **Step 6: Verify no regressions in existing test suites**

Run: `cargo test --manifest-path /home/azureuser/pg2any/Cargo.toml -p pg2any_lib -- --list 2>&1 | wc -l`
Compare with pre-change count to ensure no tests were accidentally removed.
