# AGENTS.md - AI Agent Context for pg2any

## What is pg2any?

pg2any is a Rust library and application for real-time PostgreSQL Change Data Capture (CDC). It reads from PostgreSQL's logical replication WAL stream and writes changes to destination databases (MySQL, SQL Server, SQLite) or message brokers (Kafka).

## Architecture Overview

```
PostgreSQL WAL --> Producer (reads WAL, writes files) --> File System --> Consumer (reads files, executes SQL) --> Destination
```

**Core pattern**: File-based producer-consumer with crash recovery. Transactions are persisted to disk before execution, enabling restart from exact position.

### Three-Directory Transaction Store

- `sql_data_tx/` - Actual SQL statements (append-only, never moved)
- `sql_received_tx/` - Metadata for in-progress transactions (not yet committed)
- `sql_pending_tx/` - Metadata for committed transactions awaiting execution

### Transaction Modes

- **Normal**: `BEGIN -> DML events -> COMMIT` (single active transaction)
- **Streaming**: `StreamStart -> DML events -> StreamStop -> StreamCommit` (multiple concurrent, protocol v2+)

## Workspace Layout

```
pg2any/                          # Cargo workspace root
  pg2any-lib/                    # Core library crate (published as pg2any_lib)
    src/
      lib.rs                     # Public API: re-exports CdcApp, CdcClient, Config, etc.
      app.rs                     # CdcApp - high-level runner with signal handling
      client.rs                  # CdcClient - producer/consumer orchestration (largest file, ~1500 lines)
      config.rs                  # Config struct + ConfigBuilder pattern
      env.rs                     # load_config_from_env() - maps env vars to Config
      error.rs                   # CdcError enum with thiserror
      types.rs                   # DestinationType, Transaction, EventType, ChangeEvent (re-exported from pg_walstream)
      lsn_tracker.rs             # LsnTracker - flush_lsn persistence to disk
      transaction_manager.rs     # TransactionManager - file lifecycle (begin/append/commit/abort/process)
      destinations/
        destination_factory.rs   # DestinationHandler trait (core interface) + DestinationFactory
        mysql.rs                 # MySQL impl via SQLx + mysql_async (LOAD DATA LOCAL INFILE)
        sqlserver.rs             # SQL Server impl via Tiberius
        sqlite.rs                # SQLite impl via SQLx
        kafka.rs                 # Kafka impl via rdkafka (event mode - sends JSON, not SQL)
        bulk_insert.rs           # TSV generation, INSERT batch detection, multi-value INSERT fallback
        coalescing.rs            # DML batch coalescing (multi-row INSERT, CASE-WHEN UPDATE)
        common.rs                # Shared transaction execution with session tuning
      storage/
        traits.rs                # TransactionStorage trait
        compressed.rs            # Gzip storage impl
        uncompressed.rs          # Plain file storage impl
        sql_parser.rs            # Streaming SQL statement parser
      monitoring/
        metrics.rs               # Prometheus metric definitions
        metrics_abstraction.rs   # MetricsCollector trait (allows no-op when metrics disabled)
        metrics_server.rs        # HTTP server for /metrics endpoint
    tests/                       # Integration tests (no live DB required for most)
  examples/                      # Example binary using the library
  tests/chaos/                   # Docker-based chaos and pgbench tests
```

## Key Interfaces

### DestinationHandler (trait) - `destinations/destination_factory.rs`

The central abstraction. Every destination implements this:

```rust
#[async_trait]
pub trait DestinationHandler: Send + Sync {
    async fn connect(&mut self, connection_string: &str) -> Result<()>;
    fn set_schema_mappings(&mut self, mappings: HashMap<String, String>);
    fn set_session_tuning(&mut self, _enabled: bool, _threshold: usize) {}
    async fn execute_sql_batch_with_hook(&mut self, commands: &[String], pre_commit_hook: Option<PreCommitHook>) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
    fn supports_event_mode(&self) -> bool { false }
    fn supports_bulk_insert(&self) -> bool { false }
    async fn execute_bulk_insert_with_hook(&mut self, table: &str, columns: &[String], rows: &[Vec<String>], pre_commit_hook: Option<PreCommitHook>) -> Result<()> { ... }
    async fn execute_events_batch_with_hook(&mut self, events: &[ChangeEvent], ...) -> Result<()> { ... }
}
```

- SQL destinations (MySQL, SQLite, SQL Server) use `execute_sql_batch_with_hook`
- MySQL additionally supports `execute_bulk_insert_with_hook` for LOAD DATA LOCAL INFILE
- SQL Server supports `execute_bulk_insert_with_hook` for TDS Bulk Load (with multi-value INSERT fallback)
- Kafka uses event mode (`supports_event_mode() = true`, `execute_events_batch_with_hook`)

### TransactionStorage (trait) - `storage/traits.rs`

Abstracts file I/O for compressed vs uncompressed transaction files.

### Config - `config.rs`

Builder pattern. All fields have sensible defaults. Required: `source_connection_string`, `destination_connection_string`.

## Feature Flags

```
default = ["mysql", "sqlserver", "sqlite"]
mysql   = ["sqlx/mysql", "mysql_async"]
kafka   = ["rdkafka", "futures-util", "base64"]
metrics = ["hyper", "hyper-util", "http-body-util", "prometheus"]
```

Destinations are conditionally compiled via `#[cfg(feature = "...")]`.

## Concurrency Model

- **Single producer task**: Reads WAL stream, writes transaction files, sends commit notifications via `mpsc` channel
- **Single consumer task**: Receives notifications, maintains a `BinaryHeap` priority queue ordered by `commit_lsn`, executes in order
- **Graceful shutdown**: `CancellationToken` + `oneshot` channel coordination between producer and consumer
- All async, Tokio-based. No blocking I/O on the hot path.

## Error Handling

`CdcError` in `error.rs` classifies errors as:
- Transient (retryable): `TransientConnection`, `Timeout`, `Io`, `ReplicationConnection`
- Permanent (non-retryable): `PermanentConnection`, `Authentication`, `Config`, `Unsupported`
- Cancelled: `Cancelled` (graceful shutdown)

Consumer uses exponential backoff (2^n seconds, capped at 30s) for transient failures.

## External Dependencies

- **pg_walstream** (by same author): Low-level PostgreSQL logical replication protocol implementation. Handles WAL parsing, replication state, connection management. [GitHub](https://github.com/isdaniel/pg-walstream)

System dependencies for building: `libpq-dev`, `pkg-config`, `libssl-dev`, `cmake` (for rdkafka).

## Performance Optimization (MySQL & SQL Server)

The consumer-side uses a tiered optimization strategy for batch INSERTs:

1. **DML Coalescing** (always active) - Consecutive INSERTs to same table are combined into multi-value INSERTs
2. **Session Tuning** (configurable) - `SET unique_checks=0, foreign_key_checks=0` during large batches (safe because PG already validated)
3. **Bulk Insert Detection** (configurable) - When a batch is entirely INSERTs to one table and exceeds threshold, routes to `execute_bulk_insert_with_hook`
4. **LOAD DATA LOCAL INFILE** (when server supports it) - MySQL's fastest bulk loading protocol via mysql_async. Falls back to multi-value INSERT if `local_infile` is disabled.
5. **TDS Bulk Load** (SQL Server, configurable) - Uses tiberius's native `bulk_insert()` API for streaming row insertion via TDS protocol. Falls back to multi-value INSERT if bulk load fails or table metadata retrieval fails.

Config env vars: `CDC_BULK_INSERT_ENABLED`, `CDC_BULK_INSERT_THRESHOLD`, `CDC_SESSION_TUNING_ENABLED`, `CDC_SESSION_TUNING_THRESHOLD`

## Common Patterns

- Environment config: all `CDC_*` prefixed env vars map to `Config` fields via `env.rs`
- Schema mapping: `CDC_SCHEMA_MAPPING=public:cdc_db` maps PG `public` schema to MySQL `cdc_db` database
- LSN tracking: `pg2any_last_lsn.metadata` file stores JSON with `flush_lsn` + consumer state
- Transaction files: `{txid}_{seq}.sql` in `sql_data_tx/`, `{txid}.meta` in received/pending dirs

## Adding a New Destination

1. Create `pg2any-lib/src/destinations/your_dest.rs`
2. Implement `DestinationHandler` trait
3. Add feature flag to `pg2any-lib/Cargo.toml`
4. Register in `destination_factory.rs` `DestinationFactory::create()` match arm
5. Add variant to `DestinationType` enum in `types.rs`
6. Add `#[cfg(feature = "your_dest")]` guards in `destinations/mod.rs` and `lib.rs`
