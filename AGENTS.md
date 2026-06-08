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
      client.rs                  # CdcClient - orchestration, startup, recovery, shutdown (~400 lines)
      producer.rs                # run_producer - WAL reader, transaction file writer
      consumer.rs                # run_consumer_loop - file executor, drain_and_shutdown, retry logic
      config.rs                  # Config struct + ConfigBuilder pattern
      env.rs                     # load_config_from_env() - maps env vars to Config
      error.rs                   # CdcError enum with thiserror
      types.rs                   # DestinationType, Transaction, EventType, ChangeEvent (re-exported from pg_walstream)
      lsn_tracker.rs             # LsnTracker - flush_lsn persistence to disk
      transaction_manager.rs     # TransactionManager - file lifecycle (begin/append/commit/abort/process)
      destinations/
        destination_factory.rs   # DestinationHandler trait + DestinationFactoryFn type alias
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
- Destinations are constructed via the per-Config registry (`Config::create_destination`). Built-ins self-register in `Config::default()`; external users add their own via `ConfigBuilder::custom_destination` (factory closure) or `ConfigBuilder::use_destination::<H>()` (for `H: Default`).

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

## Graceful Shutdown

pg2any implements coordinated producer-consumer shutdown with zero data loss:

### Sequence

1. **Signal** (SIGINT/SIGTERM) → `CancellationToken` cancelled
2. **Producer** exits WAL read loop → flushes buffers → drops mpsc sender → sends oneshot
3. **Consumer** receives oneshot → breaks main loop → calls `drain_and_shutdown()`
4. **Drain** uses a fresh (uncancellable) token → processes ALL remaining queued transactions
5. **Finalize** each transaction: commit to destination → update `flush_lsn` → persist to disk → delete file
6. **Client.stop()** joins handles → sends final ACK to PostgreSQL → closes destination → final persist

### Invariant

The on-disk LSN metadata represents the last transaction **fully applied** to the destination. On restart, any transaction with `commit_lsn <= flush_lsn` is skipped (position-tracking deduplication). Files in `sql_pending_tx/` with `commit_lsn > flush_lsn` are replayed.

### Why NOT duplicate-key ignore

Position-tracking (not `ON CONFLICT DO NOTHING` / `INSERT IGNORE`) is the correct approach because:
- It handles UPDATE and DELETE (not just INSERT)
- It avoids masking legitimate duplicate-key bugs in the source
- It works identically across all destination types (MySQL, SQL Server, SQLite, Kafka)

### Recovery scenarios

| Scenario | What's on disk | What happens on restart |
|----------|---------------|------------------------|
| Clean shutdown | No pending files, LSN persisted | Resume from `flush_lsn` |
| Crash mid-transaction | Files in `sql_received_tx/` | Incomplete transactions cleaned up |
| Crash after commit, before execute | Files in `sql_pending_tx/` | Replayed from start (or `last_executed_command_index`) |
| Crash after partial execution | File in `sql_pending_tx/` with progress | Resumed from checkpoint |

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

The consumer-side uses a tiered optimization strategy for batch INSERT operations:

1. **DML Coalescing** (always active) - Consecutive INSERT statements to same table are combined into multi-value INSERT
2. **Bulk Insert Detection** (configurable) - When a batch is entirely INSERT statements to one table and exceeds threshold, routes to `execute_bulk_insert_with_hook`
3. **LOAD DATA LOCAL INFILE** (when server supports it) - MySQL's fastest bulk loading protocol via mysql_async. Falls back to multi-value INSERT if `local_infile` is disabled.
4. **TDS Bulk Load** (SQL Server, configurable) - Uses tiberius's native `bulk_insert()` API for streaming row insertion via TDS protocol. Falls back to multi-value INSERT if bulk load fails or table metadata retrieval fails.

Config env vars: `CDC_BULK_INSERT_THRESHOLD`

## Common Patterns

- Environment config: all `CDC_*` prefixed env vars map to `Config` fields via `env.rs`
- Schema mapping: `CDC_SCHEMA_MAPPING=public:cdc_db` maps PG `public` schema to MySQL `cdc_db` database
- LSN tracking: `pg2any_last_lsn.metadata` file stores JSON with `flush_lsn` + consumer state
- Transaction files: `{txid}_{seq}.sql` in `sql_data_tx/`, `{txid}.meta` in received/pending dirs

## Adding a New Destination

1. Create `pg2any-lib/src/destinations/your_dest.rs`
2. Implement `DestinationHandler` trait
3. Add feature flag to `pg2any-lib/Cargo.toml`
4. Register in `Config::default()` (config.rs) under the appropriate registry key, gated by feature flag
5. Add variant to `DestinationType` enum in `types.rs`
6. Add `#[cfg(feature = "your_dest")]` guards in `destinations/mod.rs` and `lib.rs`
