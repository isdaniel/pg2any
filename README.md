[![Crates.io Version](https://img.shields.io/crates/v/pg2any_lib)](https://crates.io/crates/pg2any_lib)
[![Crates.io Downloads (recent)](https://img.shields.io/crates/dr/pg2any_lib)](https://crates.io/crates/pg2any_lib)
[![Crates.io Total Downloads](https://img.shields.io/crates/d/pg2any_lib)](https://crates.io/crates/pg2any_lib)
[![docs.rs](https://img.shields.io/docsrs/pg2any_lib)](https://docs.rs/pg2any-lib)

# pg2any

A high-performance PostgreSQL Change Data Capture (CDC) tool that streams database changes in real-time to multiple destination databases via logical replication.

## Supported Destinations

| Destination | Driver | Feature Flag |
|-------------|--------|--------------|
| MySQL | SQLx + mysql_async | `mysql` (default) |
| SQL Server | Tiberius (TDS) | `sqlserver` (default) |
| SQLite | SQLx | `sqlite` (default) |
| Kafka | rdkafka (librdkafka) | `kafka` |

## Key Features

- **Crash-safe persistence** - File-based producer-consumer with automatic crash recovery
- **Resumable processing** - Restart from the exact position within large transaction files
- **Streaming transactions** - Handles PostgreSQL protocol v2+ in-progress transaction streaming
- **SQL compression** - Optional gzip compression for transaction files with streaming decompression
- **Schema mapping** - Configurable PostgreSQL schema to destination database/schema translation
- **Prometheus metrics** - Built-in HTTP metrics endpoint (feature: `metrics`)
- **Graceful shutdown** - Coordinated producer-consumer shutdown with LSN persistence
- **Bulk insert optimization** - LOAD DATA LOCAL INFILE for MySQL, TDS Bulk Load for SQL Server + session tuning for large batches
- **DML coalescing** - Multi-value INSERT, CASE-WHEN UPDATE, OR-combined DELETE batching
- **Smart batching** - Merges consecutive homogeneous INSERT-only transactions across boundaries for higher throughput

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
    .streaming(true)
    .build()?;
```

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

### Transaction Lifecycle

1. **BEGIN** - Create `.sql` file in `sql_data_tx/` and `.meta` in `sql_received_tx/`
2. **Events** - Append SQL commands to `.sql` file via buffered writes (8MB buffer)
3. **COMMIT** - Move `.meta` from `sql_received_tx/` to `sql_pending_tx/`; notify consumer
4. **Execute** - Consumer reads pending `.meta`, executes SQL from `.sql` in batches
5. **Cleanup** - Delete both `.meta` and `.sql` files on success; update flush LSN

### Crash Recovery

On restart, pg2any:
- Cleans up incomplete transactions from `sql_received_tx/` (uncommitted)
- Replays committed transactions from `sql_pending_tx/` (committed but not yet applied)
- Resumes from `last_executed_command_index` within partially-executed transaction files
- Starts replication from the persisted `flush_lsn`

### Graceful Shutdown

pg2any implements coordinated producer-consumer shutdown to prevent data loss or duplicate application:

1. **Signal received** (SIGINT/SIGTERM) — cancels the `CancellationToken`
2. **Producer exits** — flushes buffers, drops mpsc sender, sends oneshot signal to consumer
3. **Consumer drains** — processes all queued transactions within a 90-second deadline
4. **Position persisted** — `flush_lsn` written to disk via atomic temp-file rename
5. **Final ACK** — sends confirmed position to PostgreSQL so the WAL slot advances

On next startup, any transaction with `commit_lsn <= flush_lsn` is skipped automatically (position-tracking deduplication). No `ON CONFLICT DO NOTHING` or `INSERT IGNORE` is used — correctness relies on tracking the exact position.

### Workflow Diagrams

#### Transaction Processing Detail Flow

```mermaid
sequenceDiagram
    participant PG as PostgreSQL WAL
    participant Producer as Producer Task
    participant FS as File System
    participant Channel as Notification Channel
    participant Consumer as Consumer Task
    participant Dest as Destination DB
    participant LSN as LSN Tracker

    Note over PG,LSN: Transaction Processing Flow

    PG->>Producer: BEGIN (tx_id: 12345)
    Producer->>FS: Create 12345.sql in sql_data_tx/
    Producer->>FS: Create 12345.meta in sql_received_tx/

    PG->>Producer: INSERT event
    Producer->>Producer: Generate SQL
    Producer->>FS: Append to buffer (8MB)

    PG->>Producer: UPDATE event
    Producer->>Producer: Generate SQL
    Producer->>FS: Append to buffer

    PG->>Producer: DELETE event
    Producer->>Producer: Generate SQL
    Producer->>FS: Append to buffer

    Note over Producer,FS: Buffer reaches 8MB
    Producer->>FS: Flush buffer to 12345.sql

    PG->>Producer: COMMIT (LSN: 0/1A2B3C4D)
    Producer->>FS: Flush remaining buffer
    Producer->>FS: Move 12345.meta to sql_pending_tx/
    Producer->>Channel: Send notification

    Channel->>Consumer: Transaction ready
    Consumer->>FS: Read 12345.meta from sql_pending_tx/
    Consumer->>FS: Read SQL commands from 12345.sql

    loop For each SQL batch
        Consumer->>Dest: BEGIN TRANSACTION
        Consumer->>Dest: Execute SQL batch
        Consumer->>Dest: COMMIT TRANSACTION
        Consumer->>FS: Update pending .meta progress
    end

    Consumer->>FS: Delete 12345.meta and 12345.sql
    Consumer->>LSN: Update and persist flush_lsn
```

#### Crash Recovery Workflow

```mermaid
graph TB
    Crash([System Crash]) --> Restart[Restart pg2any]
    Restart --> LoadMeta[Load LSN Metadata]
    LoadMeta --> CheckMeta{Metadata Exists?}

    CheckMeta -->|No| StartFresh[Start from Latest]
    CheckMeta -->|Yes| GetLSN[Extract flush_lsn]

    GetLSN --> CleanReceived[Cleanup sql_received_tx/]
    CleanReceived --> ProcessPending[Process sql_pending_tx/]

    ProcessPending --> SortByTimestamp[Sort by commit_timestamp]
    SortByTimestamp --> ForEachPending{For Each .meta}

    ForEachPending -->|More files| CheckResume{Has resume index?}
    ForEachPending -->|Done| SetStartLSN[Set start_lsn = flush_lsn]

    CheckResume -->|Yes| ResumeFromIndex[Resume from last_executed_command_index + 1]
    CheckResume -->|No| ReadAll[Read All SQL Commands]

    ResumeFromIndex --> Execute[Execute Remaining Commands]
    ReadAll --> Execute

    Execute --> UpdateLSN[Update LSN Metadata]
    UpdateLSN --> DeleteFiles[Delete .meta and .sql]
    DeleteFiles --> ForEachPending

    SetStartLSN --> StartReplication[Start Replication]
    StartFresh --> StartReplication
    StartReplication --> NormalOperation([Normal Operation])

    style Crash fill:#ff6b6b
    style NormalOperation fill:#51cf66
    style ResumeFromIndex fill:#ffd43b
```

## Configuration

All configuration is via environment variables (ideal for containers) or the `ConfigBuilder` API.

### Required

| Variable | Description | Example |
|----------|-------------|---------|
| `CDC_SOURCE_CONNECTION_STRING` | PostgreSQL connection string | `postgresql://user:pass@host:5432/db?replication=database` |
| `CDC_DEST_TYPE` | Target database type | `MySQL`, `SqlServer`, `SQLite`, `Kafka` |
| `CDC_DEST_URI` | Destination connection string | See format table below |

### Destination URI Formats

| Database | Format | Example |
|----------|--------|---------|
| MySQL | `mysql://user:pass@host:port/db` | `mysql://root:pass@localhost:3306/mydb` |
| SQL Server | `sqlserver://user:pass@host:port/db` | `sqlserver://sa:pass@localhost:1433/master` |
| SQLite | File path | `./replica.db` or `/data/replica.db` |
| Kafka | Broker list | `broker1:9092,broker2:9092` |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `CDC_REPLICATION_SLOT` | `cdc_slot` | PostgreSQL replication slot name |
| `CDC_PUBLICATION` | `cdc_pub` | PostgreSQL publication name |
| `CDC_PROTOCOL_VERSION` | `1` | Replication protocol version (1-4) |
| `CDC_STREAMING` | `true` | Stream in-progress transactions (requires protocol v2+) |
| `CDC_SCHEMA_MAPPING` | | Schema translation, e.g. `public:cdc_db,sales:sales_db` |
| `CDC_CHANNEL_CAPACITY` | `1000` | Transaction channel capacity between producer and consumer |
| `CDC_BATCH_SIZE` | `1000` | SQL commands per batch execution |
| `CDC_TRANSACTION_SEGMENT_SIZE_MB` | `64` | Max segment file size in MB |
| `CDC_CONNECTION_TIMEOUT` | `30` | Connection timeout (seconds) |
| `CDC_QUERY_TIMEOUT` | `10` | Query timeout (seconds) |
| `CDC_LAST_LSN_FILE` | `./pg2any_last_lsn` | Base path for LSN metadata file |
| `CDC_TRANSACTION_FILE_BASE_PATH` | `./` | Base directory for transaction files |
| `PG2ANY_ENABLE_COMPRESSION` | `false` | Enable gzip compression for SQL files |
| `CDC_BULK_INSERT_THRESHOLD` | `500` | Minimum INSERT statements to trigger bulk path |
| `RUST_LOG` | `pg2any=debug` | Log level |


## Monitoring

Enable with feature flag `metrics`. Exposes Prometheus-compatible metrics on port 8080.

```bash
# Key metrics
pg2any_events_processed_total
pg2any_transactions_processed_total
pg2any_replication_lag_seconds
pg2any_events_per_second
pg2any_errors_total
pg2any_source_connection_status
pg2any_destination_connection_status
```

The Docker Compose setup includes a full observability stack: Prometheus (`:9090`), Node Exporter, PostgreSQL Exporter, and MySQL Exporter with predefined alert rules.

## Development

```bash
make build              # Build the application
make test               # Run full test suite
make check              # Cargo check + validation
make format             # Format code with rustfmt
make before-git-push    # Pre-commit validation

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
mysql = ["sqlx/mysql", "mysql_async"]
sqlserver = ["tiberius"]
sqlite = ["sqlx/sqlite"]
kafka = ["rdkafka", "futures-util", "base64"]
metrics = ["hyper", "hyper-util", "http-body-util", "prometheus"]
```

## Dependencies

| Crate | Purpose |
|-------|---------|
| [pg_walstream](https://crates.io/crates/pg_walstream) | PostgreSQL logical replication protocol |
| tokio | Async runtime |
| sqlx | MySQL + SQLite async driver |
| mysql_async | MySQL LOAD DATA LOCAL INFILE support |
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
