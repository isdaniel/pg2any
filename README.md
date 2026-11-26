[![Crates.io Version](https://img.shields.io/crates/v/pg2any_lib)](https://crates.io/crates/pg2any_lib)
[![Crates.io Downloads (recent)](https://img.shields.io/crates/dr/pg2any_lib)](https://crates.io/crates/pg2any_lib)
[![Crates.io Total Downloads](https://img.shields.io/crates/d/pg2any_lib)](https://crates.io/crates/pg2any_lib)
[![docs.rs](https://img.shields.io/docsrs/pg2any_lib)](https://docs.rs/pg2any-lib)

# pg2any

A high-performance PostgreSQL Change Data Capture (CDC) tool that streams database changes in real time to multiple destination databases via logical replication.

## Supported Destinations

| Destination | Driver | Feature Flag |
|-------------|--------|--------------|
| MySQL | mysql_async | `mysql` (default) |
| SQL Server | Tiberius (TDS) | `sqlserver` (default) |
| SQLite | SQLx | `sqlite` (default) |
| Kafka | rdkafka (librdkafka) | `kafka` |

## Key Features

- **Crash-safe persistence** - File-based producer-consumer with automatic crash recovery; resumes from the exact position inside large transaction files
- **Streaming transactions** - Handles PostgreSQL protocol v2+ in-progress transaction streaming
- **Initial snapshot** - Opt-in copy of the existing table data before streaming, with a seamless handoff to the WAL stream (see [Initial Snapshot](#initial-snapshot))
- **Bulk insert optimization** - `LOAD DATA LOCAL INFILE` for MySQL, TDS Bulk Load for SQL Server
- **DML coalescing** - Multi-value INSERT, CASE-WHEN UPDATE, OR-combined DELETE batching
- **Schema mapping** - Configurable PostgreSQL schema to destination database/schema translation
- **SQL compression** - Optional gzip compression for transaction files
- **Prometheus metrics** - Built-in HTTP metrics endpoint (feature: `metrics`)
- **Graceful shutdown** - Coordinated producer-consumer shutdown with LSN persistence

## Quick Start

### Prerequisites

PostgreSQL with logical replication enabled:

```sql
ALTER SYSTEM SET wal_level = logical;
-- Restart PostgreSQL after this change

CREATE PUBLICATION my_publication FOR ALL TABLES;
CREATE USER replicator WITH REPLICATION LOGIN PASSWORD 'password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
```

### Docker

```bash
git clone https://github.com/isdaniel/pg2any
cd pg2any
docker-compose up -d
make build && RUST_LOG=info make run
```

### As a Library

```rust
use pg2any_lib::{load_config_from_env, run_cdc_app};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config_from_env()?;
    run_cdc_app(config, None).await?;
    Ok(())
}
```

### Programmatic Configuration

```rust
use pg2any_lib::{Config, DestinationType};

let config = Config::builder()
    .source_connection_string("postgresql://user:pass@localhost:5432/db?replication=database")
    .destination_type(DestinationType::MySQL)
    .destination_connection_string("mysql://root:pass@localhost:3306/replica_db")
    .replication_slot_name("cdc_slot")
    .publication_name("cdc_pub")
    .streaming(true) // needs PostgreSQL 14+
    .build()?;
```

## Upgrading from 0.11

This release changes the public API, so it needs a new minor version (0.12.0) before 1.0; the version in `Cargo.toml` is set when it is released.

- **Removed `Config` fields and `ConfigBuilder` methods** that nothing read: `protocol_version` (the protocol follows `streaming`: 2 when on, else 1), `binary_format`, `include_messages`, `two_phase`, `origin` (with `config::OriginFilter`), `query_timeout`, `max_retry_attempts`, `initial_retry_delay`, `max_retry_delay`, `retry_multiplier`, `max_retry_duration`, `retry_jitter`, `table_mappings` (with `config::{TableMapping, ColumnTransformation, TransformationType}`) and `extra_options`. `CDC_PROTOCOL_VERSION`, `CDC_BINARY_FORMAT` and `CDC_QUERY_TIMEOUT` are ignored with a warning.
- **Removed unused items**: `RenderedStatement` (crate root and `sql_renderer`) and `destinations::bulk_insert::{extract_insert_prefix, extract_values_from_suffix}`.
- **Changed**: `Config` has new public snapshot fields (a struct literal needs `..Config::default()`); `drain_and_shutdown` takes `&ConsumerSettings` instead of `batch_size`; `load_config_from_env` returns a configuration error instead of panicking when `CDC_SOURCE_CONNECTION_STRING` or `CDC_DEST_URI` is missing, and for an unknown `CDC_DEST_TYPE` instead of falling back to MySQL.

## Custom Destinations

Built-in destinations self-register in `Config::default()`. External users can plug in their own `DestinationHandler` through the per-`Config` registry, no fork required:

```rust,ignore
// 1) Type implements Default
Config::builder()
    .use_destination::<MyDest>()
    .destination_connection_string("...")
    .build()?;

// 2) Construction takes args: supply a factory closure
Config::builder()
    .custom_destination(|| MyDest::with_options(opts.clone()))
    .destination_connection_string("...")
    .build()?;
```

Minimal handler:

```rust,ignore
use async_trait::async_trait;
use pg2any_lib::destinations::{DestinationHandler, PreCommitHook};
use pg2any_lib::CdcResult;
use std::collections::HashMap;

#[derive(Default)]
struct MyDest;

#[async_trait]
impl DestinationHandler for MyDest {
    async fn connect(&mut self, _conn: &str) -> CdcResult<()> { Ok(()) }
    fn set_schema_mappings(&mut self, _m: HashMap<String, String>) {}
    async fn execute_sql_batch_with_hook(
        &mut self, _cmds: &[String], hook: Option<PreCommitHook>,
    ) -> CdcResult<()> {
        if let Some(h) = hook { h().await?; }
        Ok(())
    }
    async fn close(&mut self) -> CdcResult<()> { Ok(()) }
}
```

The factory must return a fresh handler on every call: it is invoked for the main pipeline, for the consumer and, with the initial snapshot on, once per snapshot writer. A custom destination that does not override `snapshot_table_is_empty` cannot use the `fail_if_not_empty` snapshot policy. See the [`DestinationHandler`](pg2any-lib/src/destinations/destination_factory.rs) trait for the full surface (event mode, bulk insert hooks, snapshot hooks, pre-commit checkpoint integration).

## Architecture

pg2any uses a **file-based producer-consumer pattern** for reliable, crash-safe transaction processing:

```
PostgreSQL WAL Stream
        |
        v
  +-----------+       +------------------+       +-----------+
  | Producer  | ----> | File System      | ----> | Consumer  |
  | (WAL      |       |                  |       | (SQL      |
  |  Reader)  |       | sql_data_tx/     |       |  Executor)|
  |           |       | sql_received_tx/ |       |           |
  +-----------+       | sql_pending_tx/  |       +-----------+
                      +------------------+             |
                                                       v
                                                 Destination DB
```

**Transaction lifecycle**

1. **BEGIN** - Create `.sql` in `sql_data_tx/` and `.meta` in `sql_received_tx/`
2. **Events** - Append SQL commands to the `.sql` file via buffered writes (8 MB buffer)
3. **COMMIT** - Move `.meta` to `sql_pending_tx/` and notify the consumer
4. **Execute** - Consumer reads the pending `.meta` and runs the SQL in batches
5. **Cleanup** - Delete both files on success and persist the flush LSN

Small transactions (at most one batch, while the in-memory budget lasts; see [Throughput options](#throughput-options)) skip steps 1-3 and 5 (no files); everything else is unchanged.

**Crash recovery.** On restart pg2any drops incomplete transactions from `sql_received_tx/`, replays committed ones from `sql_pending_tx/` (resuming at `last_executed_command_index` inside a partially executed file), and resumes replication from the persisted `flush_lsn`. The replication slot is created (when it does not exist) before the replay and the stream is started after it: the server terminates a started stream that nothing reads after `wal_sender_timeout`, and a slot created only after a long replay would miss everything committed meanwhile. The opt-in [initial snapshot](#initial-snapshot) is the one exception: it bypasses transaction files and recovers through its own marker file.

**Failures.** If the source stream fails for good (the retries of the replication stream are used up), the client applies what the producer committed and exits with an error, so a process manager with a restart policy restarts it. While the destination is down the consumer retries with backoff and stops reading its queue at 10,000 transactions: the channel then fills and the producer waits, so memory stays bounded and the WAL stays in the slot.

**Graceful shutdown.** On SIGINT/SIGTERM the producer flushes and stops; the consumer then applies every transaction the producer had committed, including any it commits while shutting down, `flush_lsn` is written atomically after each one (or each [group](#throughput-options)), and the final position is acknowledged to PostgreSQL. Data still inside PostgreSQL (not yet read from the slot) is not part of the guarantee; it stays in the slot. A failing destination is retried with backoff until everything is applied; there is no time limit of its own, the stop timeout of the process manager (Docker `stop_grace_period`, Kubernetes `terminationGracePeriodSeconds`, systemd `TimeoutStopSec`) decides how long that may take. When it kills the process, the unapplied transactions stay in `sql_pending_tx/` (replayed on the next start, where the batch the destination had already committed hits duplicate keys that are skipped, logged at WARN: only the first `GROUP_COMMIT_MAX_TXNS` (128) transactions, one group, and a genuine destination-side conflict inside that window is skipped too; in-memory ones are re-sent by PostgreSQL; nothing is acknowledged for any of them). Size the stop timeout for the slowest destination you expect. Shutdown during the initial snapshot aborts it and the next start copies again. On the next start any transaction with `commit_lsn <= flush_lsn` is skipped, so correctness rests on exact position tracking (no `ON CONFLICT DO NOTHING` / `INSERT IGNORE`). If the final ACK fails, PostgreSQL re-sends from its `confirmed_flush_lsn` and the local position skips what was already applied.

## Configuration

All configuration is via environment variables (ideal for containers) or the `ConfigBuilder` API.

### Required

| Variable | Description | Example |
|----------|-------------|---------|
| `CDC_SOURCE_CONNECTION_STRING` | PostgreSQL connection string | `postgresql://user:pass@host:5432/db?replication=database` |
| `CDC_DEST_TYPE` | Target database type | `MySQL`, `SqlServer`, `SQLite`, `Kafka` |
| `CDC_DEST_URI` | Destination connection string | See below |

| Database | `CDC_DEST_URI` format | Example |
|----------|-----------------------|---------|
| MySQL | `mysql://user:pass@host:port/db` | `mysql://root:pass@localhost:3306/mydb` |
| SQL Server | ADO.NET connection string | `Server=tcp:localhost,1433;Database=mydb;User Id=sa;Password=pass;TrustServerCertificate=true` |
| SQLite | File path | `./replica.db` |
| Kafka | Broker list | `broker1:9092,broker2:9092` |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `CDC_REPLICATION_SLOT` | `cdc_slot` | PostgreSQL replication slot name |
| `CDC_PUBLICATION` | `cdc_pub` | PostgreSQL publication name |
| `CDC_STREAMING` | `true` | Stream in-progress transactions (PostgreSQL 14+, requests replication protocol 2; `false` requests protocol 1) |
| `CDC_SCHEMA_MAPPING` | | Schema translation, e.g. `public:cdc_db,sales:sales_db` |
| `CDC_CHANNEL_CAPACITY` | `1000` | Transaction channel capacity between producer and consumer |
| `CDC_BATCH_SIZE` | `1000` | SQL commands per batch execution |
| `CDC_MAX_ROWS_PER_INSERT` | `0` | Rows per multi-value INSERT (0 = destination limit; SQL Server is capped at 1000 and SQLite at 500, MySQL and Kafka ignore it) |
| `CDC_BULK_INSERT_THRESHOLD` | `500` | Minimum INSERT statements to trigger the bulk path |
| `CDC_TRANSACTION_SEGMENT_SIZE_MB` | `64` | Max segment file size in MB |
| `CDC_CONNECTION_TIMEOUT` | `30` | Connection timeout (seconds) |
| `CDC_LAST_LSN_FILE` | `./pg2any_last_lsn.metadata` | LSN metadata file |
| `CDC_TRANSACTION_FILE_BASE_PATH` | `./` | Base directory for transaction files |
| `PG2ANY_ENABLE_COMPRESSION` | `false` | Gzip compression for SQL files |
| `RUST_LOG` | `pg2any=debug` | Log level |

> `CDC_PROTOCOL_VERSION`, `CDC_BINARY_FORMAT`, `CDC_QUERY_TIMEOUT` and `CDC_SQLSERVER_LIVE_BULK_LOAD` are no longer read: a value only logs a warning, so existing env files keep working. The protocol version follows `CDC_STREAMING` (2 when streaming, else 1; versions 3 and 4 only add two-phase and parallel streaming, which pg2any does not support). `CDC_DEST_TYPE` and the other values are validated: an unknown destination type or a missing required variable is a configuration error.
>
> These defaults are the ones of `load_config_from_env`. `Config::builder()` has its own: slot `pg2any_slot`, publication `pg2any_pub`, `streaming` off. A deployment that moves between the two without naming the slot and publication changes the slot it uses, so name them. `CDC_LAST_LSN_FILE`, `PG2ANY_ENABLE_COMPRESSION` and the `CDC_KAFKA_*` options below are read by the components that use them, so `ConfigBuilder` cannot set them: they are environment-only.

The initial snapshot has its own variables, see [Snapshot settings](#snapshot-settings).

### Kafka options

Topics are named `<CDC_KAFKA_TOPIC_PREFIX>.<schema>.<table>`; the topics must exist (or be auto-created by the broker).

| Variable | Default | Description |
|----------|---------|-------------|
| `CDC_KAFKA_TOPIC_PREFIX` | `pg2any` | Topic name prefix |
| `CDC_KAFKA_SOURCE_DB_NAME` | `postgres` | Source database name in the message envelope |
| `CDC_KAFKA_KEY_COLUMNS` | | Message key columns, `schema.table:col1,col2;schema2.table2:col3` |
| `CDC_KAFKA_COMPRESSION` | `lz4` | Producer compression |
| `CDC_KAFKA_ACKS` | `all` | Producer acks |
| `CDC_KAFKA_BATCH_SIZE` / `CDC_KAFKA_LINGER_MS` | `16384` / `5` | Producer batching |
| `CDC_KAFKA_MESSAGE_MAX_BYTES` | `1048576` | Largest message; a bigger row fails with `MessageSizeTooLarge` |
| `CDC_KAFKA_RETRIES` / `CDC_KAFKA_RETRY_BACKOFF_MS` | `3` / `200` | Producer retries |
| `CDC_KAFKA_MESSAGE_TIMEOUT_MS` | `30000` | Delivery timeout |
| `CDC_KAFKA_METADATA_REFRESH_INTERVAL_MS` | `5000` | Topic metadata refresh |
| `CDC_KAFKA_SECURITY_PROTOCOL` | `plaintext` | Security protocol |
| `CDC_KAFKA_SASL_MECHANISM`, `_USERNAME`, `_PASSWORD` | | SASL credentials (optional) |

## Throughput options

The live path has two built-in fast paths (group commit and the in-memory fast path, neither has a setting). Without them every source transaction costs one destination commit (an fsync on SQLite/MySQL/SQL Server, a delivery wait on Kafka) plus about eleven file operations, so small transactions top out at roughly 50-150 TPS whatever the hardware.

**Group commit** (always on, no setting; the cap is the constant `GROUP_COMMIT_MAX_TXNS` = 128 in `consumer.rs`, see [the sweep](#group-commit-cap) for why). The consumer merges the transactions that are *already queued* (it never waits for more, so there is no added latency: three transactions followed by silence are committed at once, whatever the cap or `CDC_BATCH_SIZE`) into one destination transaction, at most `GROUP_COMMIT_MAX_TXNS` transactions and `CDC_BATCH_SIZE` statements. Under light load nothing is queued and every transaction is applied on its own; the group grows by itself with the backlog. A group is all-or-nothing; if it fails, the same transactions are applied one by one, so an error is attributed to (and retried for) the transaction that causes it. On MySQL a group never contains a `TRUNCATE` (it commits implicitly, so a failed group would not be all-or-nothing): such transactions are applied one by one. Updates of the same row in successive transactions are never merged into one coalesced `UPDATE` (the later value would be lost), so a group of updates to one hot row costs one statement per update. MySQL assigns the SET columns left to right, so an UPDATE that changes a column of its own WHERE (a key change, or two changed columns under REPLICA IDENTITY FULL) is not coalesced there either. The position is persisted once per group, before its files are deleted: a crash before that replays the whole group, after it skips the whole group. A transaction with a resume checkpoint (partially applied before a restart) is never grouped. SQL destinations get one commit per group; Kafka enqueues a whole group and waits for delivery once (at-least-once as before, but a failed group is re-sent one by one, so up to a group of transactions can be delivered twice). On SQL Server and MySQL a group can reach `CDC_BULK_INSERT_THRESHOLD` statements and use the bulk path that a single small transaction would not (see the SQL Server note below; in the sweep MySQL switched to `LOAD DATA` at 10-row transactions from a cap of 50). On MySQL that path (`LOAD DATA LOCAL`) skips a row whose key collides at the destination (for example under a case-insensitive collation) with only a warning, where the same transaction applied alone fails with a duplicate-key error.

**In-memory fast path** (always on, no setting). Small ordinary transactions (at most `CDC_BATCH_SIZE` statements, never streaming ones) stay in memory instead of being written to, read from and deleted as transaction files, as long as the in-memory text of all such transactions stays below a fixed budget of 64 MiB (`MEMORY_FAST_PATH_BUDGET_BYTES` in `transaction_manager.rs`). Anything larger, and anything that arrives once the budget is used up, spills to files (the WAL reader never waits for the budget). This is safe because the slot only advances past a transaction after the destination applied it, so after a crash PostgreSQL sends the lost in-memory transactions again; transactions are applied atomically (one batch), so a crash re-applies at most the one the destination committed an instant before it. That one has no file to recover and arrives again as live work: MySQL and SQL Server reject its inserts as duplicates, so for the first `GROUP_COMMIT_MAX_TXNS + 1` batches after a restart a batch of only inserts skips duplicate-key rows (logged at WARN; not right after an initial snapshot, and not when the start found no replication slot, because a slot created then re-sends nothing). A clean restart looks the same as a crash, so a genuine duplicate in those first batches is skipped too; a duplicate later is a real error. Memory stays bounded by the budget. It composes with group commit and works for Kafka too.

**SQL Server bulk load** (always on, no setting). A batch of at least `CDC_BULK_INSERT_THRESHOLD` INSERT statements into one table goes through TDS Bulk Load. Every value arrives quoted, so the bulk load converts them to the column's integer type, names the INSERT's columns with `KEEP_NULLS` (a NULL stays NULL, a column the INSERT does not name gets its default) and asks for `FIRE_TRIGGERS` and `CHECK_CONSTRAINTS`, so triggers fire and constraints apply as they do for the INSERT it replaces (a statement-level trigger runs once per bulk-loaded or coalesced statement, not once per source transaction); a row that violates a constraint fails the batch. A table with other types (timestamps, decimals, uuid, bit) falls back to multi-value INSERT after its first failed bulk load, for as long as the process runs (a batch the server rejects for its rows, a duplicate key or a CHECK or FOREIGN KEY violation, does not count). The snapshot's own bulk load asks for `KEEP_NULLS` only.

### Measured

These numbers were taken while group commit and the in-memory fast path were switches: "files only" is the old default, "group commit" had only the first on, "group commit + memory" both. Both are always on now, which is the last column; the numbers were measured with a group cap of 100 (the shipped cap is 128, see [the sweep](#group-commit-cap)).

Pipeline capacity: a backlog of committed transactions applied by the client with no source writing (`bench_live_drain_*`, release build, one VM, one run per point; inserts of 44-byte rows). In the plain live benchmark (`bench_live_throughput_*`, one source connection committing 160-340 TPS) group commit lifts the apply rate of 1-10 row transactions from 47-140 TPS to the source's rate, so the queue is gone. Commit-to-visible latency was not re-measured.

| destination | rows per transaction | files only | group commit | group commit + memory |
|---|---|---|---|---|
| SQLite | 1 (TPS) | 92 | 1,844 | 12,589 |
| | 10 (rows/s) | 991 | 17,836 | 56,977 |
| | 100 (rows/s) | 9,386 | 61,207 | 68,198 |
| | 1,000 (rows/s) | 68,198 | 69,610 | 74,384 |
| MySQL | 1 (TPS) | 51 | 1,766 | 3,338 |
| | 10 (rows/s) | 523 | 13,770 | 24,165 |
| | 100 (rows/s) | 4,675 | 26,086 | 29,414 |
| | 1,000 (rows/s) | 26,854 | 26,569 | 30,720 |
| SQL Server * | 1 (TPS) | 100 | 1,916 | 9,041 |
| | 10 (rows/s) | 952 | 5,947 | 34,409 |
| | 100 (rows/s) | 7,972 | 7,993 | 37,591 |
| | 1,000 (rows/s) | 7,980 | 7,908 | 38,229 |
| Kafka | 1 (TPS) | 152 | 2,116 | 9,859 |
| | 10 (rows/s) | 1,419 | 12,833 | 23,476 |
| | 100 (rows/s) | 9,440 | 21,799 | 23,960 |
| | 1,000 (rows/s) | 23,432 | 22,936 | 23,528 |

\* the SQL Server "group commit + memory" column was measured with the typed live bulk load, which was an option then and is always on now; without it SQL Server stayed at about 8,000 rows/s from 100 rows per transaction up (multi-value INSERT is its ceiling), and `update` workloads stay at about 2,500 rows/s (CASE-WHEN coalescing). At 1,000 rows per transaction a transaction is already one full batch, so group commit changes nothing there; the destination's bulk path is the limit (SQLite/MySQL/Kafka within noise of each other). Differences below 15% are within the run-to-run spread of this VM.

### Group commit cap

`GROUP_COMMIT_MAX_TXNS` is 128. The caps below were measured with a sweep of the backlog-drain benchmark (`bench_live_drain_*`, release build, 8 vCPU AMD EPYC VM, 2,000 transactions per run, two runs averaged, one destination at a time: SQLite on the VM disk, MySQL 8.0, SQL Server 2022 and a single Kafka 3.7 broker in Docker, `CDC_BATCH_SIZE=1000`, `CDC_BULK_INSERT_THRESHOLD=500`). The cap 0 column is no grouping at all. 128 itself was not measured: it lies between the 100 and 200 columns, which bracket it. SQLite from 25 on ran 10,000 transactions, the 2,000-transaction drain ends in 0.1 s there.

One-row transactions, transactions per second applied:

| destination | 0 | 10 | 25 | 50 | 100 | 200 | 500 | 1,000 |
|---|---|---|---|---|---|---|---|---|
| SQLite | 134 | 1,375 | 3,320 | 6,623 | 12,802 | 21,273 | 21,416 | 23,473 |
| MySQL | 64 | 567 | 1,230 | 2,580 | 4,574 | 5,604 | 10,043 | 11,322 |
| SQL Server | 124 | 1,319 | 3,329 | 5,697 | 6,324 | 8,164 | 13,119 | 14,058 |
| Kafka | 163 | 1,472 | 3,358 | 5,895 | 9,066 | 11,963 | 16,035 | 17,356 |

Ten-row transactions, rows per second:

| destination | 0 | 10 | 25 | 50 | 100 | 200 | 500 | 1,000 |
|---|---|---|---|---|---|---|---|---|
| SQLite | 1,334 | 12,917 | 32,762 | 50,918 | 58,791 | 51,107 | 53,202 | 51,158 |
| MySQL | 612 | 4,498 | 9,032 | 12,744 | 17,424 | 16,290 | 16,407 | 16,736 |
| SQL Server | 1,316 | 11,393 | 16,480 | 10,332 | 7,696 | 9,064 | 8,946 | 8,876 |
| Kafka | 1,548 | 9,824 | 15,616 | 20,292 | 23,300 | 23,236 | 23,062 | 22,204 |

Reading the sweep:

- Every destination gains 50-80x on one-row transactions at 100 and the curve is flattening: SQLite stops at about 21,000 TPS from 200 on (the client is CPU bound there), the others still climb only because a group of 500 statements or more switches MySQL to `LOAD DATA` and SQL Server to its multi-value INSERT path.
- Ten-row transactions gain nothing from a cap above 100: a group is also limited to `CDC_BATCH_SIZE` (1,000) statements, so they are grouped 100 at a time whatever the cap. SQL Server is the exception that proves the bulk threshold: its ten-row figures are best at 25 (250 statements) and drop once a group reaches 500 statements, which is its known multi-value INSERT ceiling and not the cap.
- The bulk threshold (500 statements) is reached by a full group of 128 four-row transactions (128 x 4), not by one of three-row transactions.
- Every doubling doubles the transactions a crash can leave committed but unrecorded (startup recovery tolerates that many) and the transactions one failing group re-applies one by one.
- At a realistic arrival rate the cap hardly binds: in the plain live benchmark (one source connection, 5,000 one-row transactions at about 170 TPS) MySQL committed 5,000 source transactions in about 1,350 destination commits, an average group of 3.7. The cap only bounds the catch-up after a backlog.

At that arrival rate (apply TPS, and how far the destination lagged when the source finished): SQLite 108 TPS and 16.9 s without grouping, 173 TPS and 0.0 s with the cap of 100; MySQL 60 TPS and 53.7 s, 171 TPS and 0.1 s. An isolated transaction is not slowed down, nothing waits for a group: commit to visible p50 was 4.9 ms on SQLite and 10.3 ms vs 9.9 ms on MySQL without and with grouping.

### Testing the options

Group commit and the in-memory fast path are part of every end-to-end scenario and benchmark. `PG2ANY_BENCH_DRAIN_TXNS` sets how many transactions the backlog-drain benchmarks commit (default: the grid point's), which is how the cap sweep below was run. Scenario S15 commits a backlog of small transactions with no client running (inserts, repeated updates of the same rows, deletes), requires the destination to converge to PostgreSQL, counts the destination commits where the target can (MySQL: far fewer than the transactions), then commits three more transactions and goes silent: they must become visible within seconds. Scenario S13 starts the client as a child process and kills it with SIGKILL three times while four writers keep committing (it requires the destination to have been behind PostgreSQL at at least one kill), then restarts it and requires convergence; S14 stops the client gracefully mid-stream and requires that nothing committed is left unapplied and that the slot's confirmed position equals the persisted one.

## Initial Snapshot

Without a snapshot, pg2any replicates only changes made after its replication slot is created. With `CDC_INITIAL_SNAPSHOT=true` it first copies the rows that already exist, then hands over to the live stream with no gap and no duplicate window. It is off by default; existing deployments are unaffected.

- **When it runs.** Only on the first start with a fresh slot and no completed-snapshot marker. After it completes, restarts skip it. An existing slot with no marker is a startup error (the destination would have no baseline): drop the slot to run the snapshot, or set the flag to `false`.
- **What it copies.** Every table the publication resolves to, as of the slot's consistent point (PostgreSQL 14+; publication row filters and column lists need 15+). To copy fewer tables, narrow the publication. `CDC_SCHEMA_MAPPING` is honoured.
- **Destination tables must already exist.** pg2any creates no tables and propagates no DDL. A missing table fails the start before any row is copied or any table is truncated, and the error names it.
- **Not resumable.** A failed or interrupted snapshot starts over (see [Recovery](#recovery)).

### Snapshot settings

| Variable | Builder method | Default | Description |
|----------|----------------|---------|-------------|
| `CDC_INITIAL_SNAPSHOT` | `initial_snapshot(bool)` | `false` | Run the snapshot on the first start with a fresh slot |
| `CDC_SNAPSHOT_TARGET_POLICY` | `snapshot_target_policy(SnapshotTargetPolicy)` | `fail_if_not_empty` | What to do with non-empty destination tables (below) |
| `CDC_SNAPSHOT_WRITERS` | `snapshot_writers(usize)` | `4` | Concurrent writers, each with its own destination connection (> 0). SQLite and Kafka always use 1 |
| `CDC_SNAPSHOT_BUFFER_MB` | `snapshot_buffer_bytes(usize)` (bytes) | `16` | Byte budget for batches queued or being written (>= 1). Not a cap on process memory |
| `CDC_SNAPSHOT_BATCH_ROWS` | `snapshot_batch_rows(usize)` | `20000` | Maximum rows per batch (> 0); a batch also ends at a table boundary and at `max(1 MiB, buffer / writers / 2)` of row data |

An unknown policy name, or a zero for a count, is a configuration error. The size defaults were measured, not guessed: change them only with new measurements.

### Target table policy

| Policy | Behaviour |
|--------|-----------|
| `fail_if_not_empty` (default) | Every destination table is probed. A table that is non-empty, missing or unreadable fails the start before any row is copied; all such tables are listed in one error |
| `truncate` | Once every probe has passed, every destination table of the publication is emptied before the copy. **This destroys everything in those tables, including rows that did not come from the source** |
| `append` | Loads on top of the existing rows (tables must still exist). A snapshot row whose key collides with an existing row fails the copy, so use it only into tables that are empty or hold no colliding keys |

- **`truncate` is for a deliberate first load or re-sync.** With the flag on it also fires by itself when a slot is lost or invalidated, so do not leave it set in steady state.
- **Kafka ignores the policy.** Messages cannot be un-published and there is no emptiness probe, so a re-sync after a lost slot republishes the current rows but cannot retract the records of rows that were deleted while the slot was gone. The one exception is crash recovery under `append`, which refuses for Kafka too (nothing to clean: drop the slot, remove the marker file and expect republished rows).
- **Automatic cleanup is limited to our own leftovers.** After a crash only the tables listed in our `in_progress` marker are emptied, and never under `append`: an `append` run lists no tables and refuses to restart until you have cleaned the destination by hand.

### Recovery

The state lives in `snapshot_marker.json` in `CDC_TRANSACTION_FILE_BASE_PATH`: `in_progress` while a snapshot is running or was interrupted, `completed` once the baseline was taken. What happens at startup with the flag on:

| Found | Result |
|-------|--------|
| No slot, no marker | Run the snapshot |
| Slot present, no marker | Error |
| `completed` marker, slot present | Normal start |
| `completed` marker, slot gone or invalidated | Error, unless the policy is `truncate`: then an automatic re-sync that truncates every published table |
| `in_progress` marker | Clean up the tables listed in the marker (under `append` only when the run stopped before the first row was dispatched, since nothing was copied yet, the marker's `copy_started` is false and only the slot is dropped; once the copy began `append` refuses), then run again |
| Marker of another slot or (for an `in_progress` marker) of another destination, or an unreadable marker | Error: recovery empties the tables the marker lists, so it never runs against a destination other than the one written to the marker (compared without the password; a marker from an older version has none and is not checked) |

With the flag off, an `in_progress` marker is an error and anything else starts normally (a lost slot then only logs the usual warning, and WAL changes may be lost without an error).

- **Crash or shutdown mid-copy.** The next start cleans up and copies everything again. SIGINT/SIGTERM is honoured at every phase of the snapshot and ends the client with a warning, not an error.
- **Re-sync after slot loss.** Set `CDC_INITIAL_SNAPSHOT=true` and `CDC_SNAPSHOT_TARGET_POLICY=truncate`; pg2any drops the invalidated slot if it still exists, truncates every destination table of the publication and snapshots again. Set the policy back afterwards.
- **Unreadable marker.** The error names the file. Default recovery: remove it, clean the destination tables (or use `truncate`), drop the slot if it exists, and run again. Write a `completed` marker by hand only after you have confirmed the snapshot finished (the log line `initial snapshot complete`, or matching row counts): `{"version":1,"marker":{"state":"completed","slot_name":"<slot>"}}`. The slot must exist: the live stream resumes from the slot's position.
- **Publication with no tables.** `pg_walstream` refuses it: the start fails naming the publication and the marker stays `in_progress`. Add a table (and its destination table) and restart.
- **"On-disk LSN position is ahead of the new slot's consistent point".** The source was probably restored from a backup or failed over to a lagging replica. Nothing was copied. Delete the LSN file (`CDC_LAST_LSN_FILE`) and `snapshot_marker.json` and restart; the destination still holds the old data, so use `truncate` for that re-sync.

### Requirements and costs

- **Source.** The replication user needs `SELECT` on every published table, and `max_replication_slots` / `max_wal_senders` need room (Kafka also opens a second replication connection).
- **xmin pinning and WAL retention.** The copy holds a `REPEATABLE READ` transaction, so VACUUM cannot reclaim dead tuples database-wide until it ends, and the slot retains all WAL written meanwhile. Budget disk and bloat for the duration of the copy; the live changes that piled up are applied after the handoff at the live path's speed.
- **Foreign keys.** Tables are loaded one by one, not in FK order, and no FK or unique checks are toggled. Drop or defer such constraints on the destination for the copy. MySQL and SQL Server also refuse `TRUNCATE` on a table a foreign key references.
- **Metrics** (feature `metrics`): `pg2any_snapshot_rows_total`, `pg2any_snapshot_bytes_total`, `pg2any_snapshot_tables_done`, `pg2any_snapshot_in_progress`. A progress line is also logged about every 10 seconds.

### Destination notes

- **MySQL.** Uses `LOAD DATA LOCAL INFILE` when the server's `local_infile` is `ON`; otherwise (or if a load fails) it logs a `warn!` and falls back to the much slower multi-value `INSERT`. The server skips rows that violate a primary or unique key without an error, so snapshot writers compare the affected-row count with the rows sent and fail the snapshot on a mismatch or when the server reports warnings (a value that does not fit its column is stored truncated or changed without an error); the live path only logs a warning. Use a server whose default character set is `utf8mb4` (the MySQL 8 default): a pooled connection falls back to the server default after a reset, and on a `latin1` default non-ASCII text in an `INSERT` is stored as mojibake (`LOAD DATA` declares `CHARACTER SET utf8mb4` itself).
- **SQL Server.** Uses TDS Bulk Load. Integer columns are coerced, on the live path as well. A table with a `bit`, `float`, `decimal`, `money`, date/time, `uniqueidentifier` or `varbinary` column, or a cell over 65,535 bytes in a `(N)VARCHAR(MAX)`/`VARBINARY(MAX)` column, falls back to multi-value `INSERT` after one `warn!`. That fallback (and the live path) renders text without the `N` prefix, so non-ASCII text in an `NVARCHAR` column can become `?`; this predates the snapshot and is not fixed. The snapshot's bulk-loaded rows fire no triggers and are not constraint-checked; the live path's do (`FIRE_TRIGGERS`, `CHECK_CONSTRAINTS`). Both name the INSERT's columns with `KEEP_NULLS`, which fixes column permutation and keeps NULLs from becoming defaults.
- **SQLite.** One writer, one commit per batch.
- **Kafka.** Snapshot rows use the Debezium-style envelope with `op = "r"` on the live topics. The key is the table's replica-identity key columns, read from the source catalog, so a snapshot key equals the key of a later update or delete of the same row (`CDC_KAFKA_KEY_COLUMNS` still wins; tables without key columns are published unkeyed with a `warn!`). A row whose message exceeds `CDC_KAFKA_MESSAGE_MAX_BYTES` (a `bytea` over about 0.5 MiB at the default) fails the snapshot; the log names the topic, size and setting to raise. Delivery is at-least-once: a restart or re-sync republishes everything, so consumers must de-duplicate by key. Live `INSERT`s are keyed the same way: the consumer reads the replica-identity keys of every table of the publication when it starts (a table added to the publication later is keyed from its first `UPDATE`/`DELETE`).
- **`bytea` and booleans.** `bytea` arrives as the text `\x<hex>` (as on the live path), so use a text-typed destination column. A text value exactly `t` or `f` is rewritten to `1`/`0` by the MySQL, SQL Server and SQLite dialects (predates the snapshot, not fixed).

### Limits and performance

- **Memory does not grow with table size**, but the buffer is not a process memory cap: a single row larger than the buffer is admitted alone and held in several copies by the write path. Measured peak is about 14x the row's size on SQLite, 10x on MySQL and SQL Server, 8x on Kafka. For memory-constrained containers lower `CDC_SNAPSHOT_BUFFER_MB` and `CDC_SNAPSHOT_BATCH_ROWS` (Kafka with 8 KiB rows peaks around 0.6 GB at the defaults).
- **Maximum row size.** `pg_walstream` caps one `COPY` message at 128 MiB, so a `bytea` over about 64 MiB cannot be snapshotted.
- **Throughput** (release build, one 8 vCPU VM, narrow rows): roughly SQLite 280k rows/s, MySQL 64k, SQL Server 60-140k, Kafka 32k; 8 KiB rows are one to two orders of magnitude slower. Results depend on hardware and vary 10-20% run to run. Reproduce with `make snapshot-bench-<destination> SNAPSHOT_BENCH_CARGO_FLAGS=--release`.
- **Not included:** parallel or resumable snapshots (a failure restarts from the beginning), creating destination tables or propagating DDL, per-table filtering, exactly-once Kafka.

### Testing the snapshot

The end-to-end suites are `#[ignore]`d and run serially against real services from [`docker-compose.snapshot-test.yml`](docker-compose.snapshot-test.yml). They need docker compose v2 and the packages the CI job installs (`libpq-dev cmake libcurl4-openssl-dev pkg-config libssl-dev`; the first Kafka build compiles librdkafka). Use the compose services, not a shared database or broker: the harness creates and drops `snap_*` schemas, publications and slots, and the Kafka suite deletes every `snaptest*` topic.

```bash
make snapshot-unit-test        # no services: what the CI "unit" job runs
make snapshot-test-sqlite      # also -mysql, -sqlserver, -kafka
make snapshot-test             # all four destinations, then clean up
make snapshot-test-clean       # remove containers and volumes
make snapshot-bench-sqlite     # throughput benchmarks; add SNAPSHOT_BENCH_CARGO_FLAGS=--release
```

Every destination runs the same scenarios (handoff under concurrent writes, the three policies, existing slot, crash recovery, slot loss and re-sync, missing destination table, empty tables, special values, shutdown mid-copy, memory bounds, write failure mid-copy). The suites pin small settings (4 MiB buffer, 200-row batches), so no regular test runs the final default settings; only the benchmark with `PG2ANY_BENCH_BUFFERS=16` does. CI runs the same suites in [`snapshot_integration_test.yml`](.github/workflows/snapshot_integration_test.yml).

## Monitoring

Enable with the feature flag `metrics`. Prometheus-compatible metrics are served on port 8080:

```
pg2any_events_processed_total
pg2any_transactions_processed_total
pg2any_events_per_second
pg2any_errors_total
pg2any_uptime_seconds
pg2any_source_connection_status
pg2any_destination_connection_status
pg2any_snapshot_rows_total / _bytes_total / _tables_done / _in_progress
```

The Docker Compose setup includes Prometheus (`:9090`), Node Exporter, PostgreSQL Exporter and MySQL Exporter with predefined alert rules.

### Replication lag

There is no lag metric of its own: PostgreSQL measures it. Watch it on the source:

```sql
-- time lag as PostgreSQL measures it (PostgreSQL's own clock, no skew); pg2any reports progress
-- when PostgreSQL sends a keepalive, so it moves in steps of about 30 s
SELECT application_name, state, write_lag, flush_lag, replay_lag FROM pg_stat_replication;
-- WAL the slot has not confirmed. It counts the WAL of the whole cluster, also writes to tables that
-- are not published, so a growing number alone does not mean pg2any is behind
SELECT slot_name, active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
FROM pg_replication_slots;
```

## Development

```bash
make build              # Build the application
make test               # Run full test suite
make check              # Cargo check + validation
make format             # Format code with rustfmt
make before-git-push    # Pre-commit validation: what CI checks, then the snapshot tests (docker, ~25 min)

# Docker environment
make docker-start       # Start databases + monitoring
make docker-stop        # Stop all services

# Chaos & integration tests
make chaos-test-mysql-full
make chaos-test-sqlserver-full
make chaos-test-sqlite-full
make chaos-test-kafka-full
make pgbench-test-mysql-full
```

## Feature Flags

```toml
[features]
default = ["mysql", "sqlserver", "sqlite"]
mysql = ["mysql_async", "futures-util"]
sqlserver = ["tiberius"]
sqlite = ["sqlx/sqlite"]
kafka = ["rdkafka", "futures-util", "base64"]
metrics = ["hyper", "hyper-util", "http-body-util", "prometheus", "lazy_static"]
```

## Dependencies

| Crate | Purpose |
|-------|---------|
| [pg_walstream](https://crates.io/crates/pg_walstream) | PostgreSQL logical replication protocol |
| tokio | Async runtime |
| sqlx | SQLite async driver |
| mysql_async | MySQL driver, `LOAD DATA LOCAL INFILE` |
| tiberius | SQL Server TDS protocol |
| rdkafka | Kafka producer (librdkafka wrapper) |
| serde / serde_json | Serialization |
| prometheus | Metrics collection |
| thiserror | Error handling |
| `flate2` / `async-compression` | SQL file compression |

## License

Apache-2.0

## References

- [PostgreSQL Logical Replication Protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html)
- [PostgreSQL WAL Internals](https://www.postgresql.org/docs/current/wal-internals.html)
- [pg_walstream](https://github.com/isdaniel/pg-walstream) - Underlying replication library
