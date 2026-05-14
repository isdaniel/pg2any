# Kafka Destination for pg2any

## Summary

Add Apache Kafka as a new CDC destination for pg2any, publishing PostgreSQL WAL change events in Debezium-compatible JSON envelope format. This enables drop-in integration with existing Debezium consumers (Kafka Connect JDBC Sink, Elasticsearch Sink, S3 Sink, etc.).

## Architecture

### Data Flow

```
PostgreSQL WAL Stream
        |
        v
   CdcClient (Producer)
        |
        +-- ChangeEvent
        |
        v
   TransactionManager
        |
        +-- [SQL destinations: MySQL/SQLite/SqlServer]
        |     generate_sql_for_event() -> SQL files -> execute_sql_batch_with_hook()
        |
        +-- [Kafka destination]
              Bypass SQL generation -> serialize ChangeEvent -> Debezium JSON -> rdkafka producer
```

### Trait Extension

Extend `DestinationHandler` with an event-based method:

```rust
#[async_trait]
pub trait DestinationHandler: Send + Sync {
    // ... existing methods ...

    /// Whether this destination consumes structured events instead of SQL.
    fn supports_event_mode(&self) -> bool { false }

    /// Execute a batch of change events directly (for non-SQL destinations like Kafka).
    async fn execute_events_batch_with_hook(
        &mut self,
        events: &[ChangeEvent],
        transaction_id: u32,
        commit_timestamp: DateTime<Utc>,
        commit_lsn: Option<Lsn>,
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        // Default: unsupported
        Err(CdcError::unsupported("Event mode not supported by this destination"))
    }
}
```

SQL destinations inherit the default implementation (no change needed). Kafka overrides `supports_event_mode() -> true` and `execute_events_batch_with_hook()`.

`TransactionManager` checks `supports_event_mode()` and, when true, stores raw events (serialized as JSON) instead of SQL, and calls the event-based method during consumption.

## Debezium Message Format

### Topic Naming

`{topic_prefix}.{schema}.{table}` (e.g., `pg2any.public.users`)

Truncate events: `{topic_prefix}.{schema}.{table}` (one per table in the truncate list).

### Key Format

```json
{
  "schema": {
    "type": "struct",
    "fields": [{"type": "int32", "optional": false, "field": "id"}],
    "optional": false,
    "name": "pg2any.public.users.Key"
  },
  "payload": {"id": 1}
}
```

Tables without a primary key produce `null` keys.

### Value Format (Debezium Envelope)

```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {"type": "struct", "fields": [...], "optional": true, "field": "before"},
      {"type": "struct", "fields": [...], "optional": false, "field": "after"},
      {"type": "struct", "fields": [...], "optional": false, "field": "source"},
      {"type": "string", "optional": false, "field": "op"},
      {"type": "int64", "optional": true, "field": "ts_ms"}
    ],
    "optional": false,
    "name": "pg2any.public.users.Envelope"
  },
  "payload": {
    "before": null,
    "after": {"id": 1, "name": "Alice", "email": "alice@example.com"},
    "source": {
      "version": "0.9.0",
      "connector": "pg2any",
      "name": "pg2any",
      "ts_ms": 1715644800000,
      "db": "source_db",
      "schema": "public",
      "table": "users",
      "txId": 774,
      "lsn": 23852864
    },
    "op": "c",
    "ts_ms": 1715644800000
  }
}
```

### Operation Mapping

| PostgreSQL Event | Debezium op |
|------------------|-------------|
| INSERT           | `"c"`       |
| UPDATE           | `"u"`       |
| DELETE           | `"d"`       |
| TRUNCATE         | `"t"`       |

### PG Type to Debezium Type Mapping

| PostgreSQL OID Type | Debezium Schema Type |
|---------------------|---------------------|
| int2, int4          | int32               |
| int8                | int64               |
| float4              | float               |
| float8              | double              |
| bool                | boolean             |
| text, varchar, char | string              |
| bytea               | bytes               |
| numeric, decimal    | string (with scale) |
| uuid                | string (uuid)       |
| date                | int32 (days)        |
| timestamp           | int64 (micros)      |
| timestamptz         | string (iso8601)    |
| json, jsonb         | string              |
| All others          | string (fallback)   |

## Kafka Configuration

Environment variables following existing pg2any conventions:

| Variable | Default | Description |
|----------|---------|-------------|
| `CDC_DEST_TYPE` | - | Set to `"Kafka"` or `"kafka"` |
| `CDC_DEST_URI` | - | Bootstrap servers (e.g., `localhost:9092`) |
| `CDC_KAFKA_TOPIC_PREFIX` | `pg2any` | Debezium topic prefix |
| `CDC_KAFKA_SECURITY_PROTOCOL` | `plaintext` | `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl` |
| `CDC_KAFKA_SASL_MECHANISM` | - | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `CDC_KAFKA_SASL_USERNAME` | - | SASL username |
| `CDC_KAFKA_SASL_PASSWORD` | - | SASL password |
| `CDC_KAFKA_COMPRESSION` | `lz4` | `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `CDC_KAFKA_BATCH_SIZE` | `16384` | Producer batch size in bytes |
| `CDC_KAFKA_LINGER_MS` | `5` | Producer linger time in ms |
| `CDC_KAFKA_ACKS` | `all` | `0`, `1`, `all` |
| `CDC_KAFKA_MESSAGE_MAX_BYTES` | `1048576` | Max message size |
| `CDC_KAFKA_RETRIES` | `3` | Number of message send retries |
| `CDC_KAFKA_SOURCE_DB_NAME` | `postgres` | Source DB name for Debezium source block |

## Files to Modify

1. `Cargo.toml` (workspace) - Add `rdkafka` workspace dependency
2. `pg2any-lib/Cargo.toml` - Add `rdkafka` dep + `kafka` feature flag
3. `pg2any-lib/src/types.rs` - Add `Kafka` variant to `DestinationType`
4. `pg2any-lib/src/destinations/mod.rs` - Add `kafka` module
5. `pg2any-lib/src/destinations/destination_factory.rs` - Extend trait + factory
6. `pg2any-lib/src/lib.rs` - Re-export `KafkaDestination`
7. `pg2any-lib/src/env.rs` - Add Kafka env var parsing
8. `pg2any-lib/src/transaction_manager.rs` - Add event-mode storage path, extend `generate_truncate_sql` and `append_qualified_table` for Kafka

## New Files

1. `pg2any-lib/src/destinations/kafka.rs` - Kafka destination + Debezium serializer

## Performance Improvements

1. **`TransactionManager::append_event()` lock contention**: SQL generation happens inside the mutex lock. Move it outside.
2. **Metadata serialization**: Use `serde_json::to_string()` (compact) instead of `to_string_pretty()` for internal metadata files.
3. **Redundant code**: Remove unused `destinations/common.rs::map_schema()` (duplicated in `TransactionManager`).

## Test Coverage

### New Unit Tests
- Debezium JSON serialization for each operation type (INSERT/UPDATE/DELETE/TRUNCATE)
- Topic name generation from schema + table
- Key extraction from primary key columns
- PG type OID to Debezium type mapping
- Kafka config parsing from env vars
- `DestinationFactory::create` for `Kafka` type
- Event-mode trait dispatch

### Graceful Shutdown
- **Critical**: The Kafka destination must not break the existing graceful shutdown flow.
- On shutdown signal (SIGTERM/SIGINT), the rdkafka producer must flush all in-flight messages before dropping. `producer.flush(timeout)` must be called in `KafkaDestination::close()`.
- The `CdcClient` shutdown coordination (CancellationToken + oneshot + mpsc drain) must work identically for Kafka as it does for SQL destinations. Verify that:
  - All buffered events are published to Kafka before the producer is closed.
  - No messages are silently dropped during shutdown.
  - The pre-commit hook (LSN checkpoint) still executes atomically — if the Kafka publish fails, the checkpoint must not advance.
- Add a unit test that verifies `KafkaDestination::close()` calls flush and completes without error.

### Existing Tests
- All existing unit tests must continue to pass
- Build must succeed with `--all-features` and default features
