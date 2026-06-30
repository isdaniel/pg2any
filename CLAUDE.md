# CLAUDE.md - Project Instructions for Claude Code

## Project Overview

pg2any is a Rust CDC (Change Data Capture) library that streams PostgreSQL WAL changes to MySQL, SQL Server, SQLite, or Kafka. Workspace with two crates: `pg2any-lib` (core library) and `examples` (binary).

## Code Conventions

- **Async runtime**: Tokio with `#[tokio::main]` / `#[tokio::test]`
- **Error handling**: `CdcError` enum via `thiserror`, propagated with `?`. Use `CdcError::config()`, `CdcError::generic()`, etc. constructors
- **Traits**: `async-trait` for async trait methods
- **Logging**: `tracing` crate (`info!`, `warn!`, `error!`, `debug!`), no `println!`
- **Feature gates**: All destinations use `#[cfg(feature = "...")]` conditional compilation
- **Naming**: `DestinationType` enum variants are PascalCase (`MySQL`, `SqlServer`, `SQLite`, `Kafka`)
- **Config**: Builder pattern (`Config::builder().field(val).build()`)
- **Tests**: Integration tests in `pg2any-lib/tests/`, unit tests as `#[cfg(test)] mod tests` inline

## Architecture Rules

- Producer and consumer are separate Tokio tasks communicating via `mpsc` channel
- Transaction files are the source of truth for crash recovery - never skip file persistence
- LSN tracking: `flush_lsn` = last WAL position committed to destination. Updated by consumer only.
- Shutdown coordination: `CancellationToken` + `oneshot` channel (producer -> consumer)
- Destinations behind `DestinationHandler` trait; Kafka uses event mode, SQL destinations use SQL batch mode
- Destinations are constructed via the per-Config registry (`Config::create_destination`). Built-ins self-register in `Config::default()`; external users add their own via `ConfigBuilder::custom_destination` (factory closure) or `ConfigBuilder::use_destination::<H>()` (for `H: Default`).
- `TransactionStorage` trait abstracts compressed vs uncompressed file I/O. Finalize passes the producer-tracked statement count through the trait (`write_transaction_from_file(path, known_count)` / `write_raw_lines_from_file`); uncompressed finalize does NO file re-read (compressed still reads to compress)
- `commit_transaction` returns `(PathBuf, TransactionFileMetadata)`; the producer builds the consumer notification from that in-memory metadata, never re-reading the `.meta` file it just wrote
- Statement-count invariant: producer-tracked `segment_statement_counts` is authoritative for `TransactionSegment.statement_count` and MUST equal what the consumer's `SqlStreamParser` reads back (it drives crash-resume segment-skip arithmetic). Events that render multiple `;`-statements (e.g. multi-table `TRUNCATE`) are counted via `count_rendered_statements`, which mirrors the parser in-memory — never `+= 1` per event
- Pending-transaction count is an in-memory `AtomicUsize` (`pending_tx_count`, `Ordering::Relaxed`), monitoring-only: seeded once at startup from the pending-dir scan, `+1` on commit, `-1` on finalize. Never gate control flow / LSN persistence on it
- MySQL uses dual pools: sqlx for normal SQL batches, mysql_async for LOAD DATA LOCAL INFILE
- SQL Server uses TDS Bulk Load (tiberius `bulk_insert`) for homogeneous INSERT batches, falls back to multi-value INSERT
- Consumer detects homogeneous INSERT batches and routes to bulk insert path when threshold met
- Graceful shutdown invariant: `drain_and_shutdown` completes all queued transactions using an uncancellable token before persisting final LSN. The on-disk LSN represents the last FULLY APPLIED transaction. Recovery skips files with `commit_lsn <= flush_lsn`.
- Never pass the main `CancellationToken` to `drain_and_shutdown` — it must use its own fresh (never-cancelled) token

## Important Files

- `pg2any-lib/src/client.rs` - Core orchestration logic, startup, recovery, shutdown
- `pg2any-lib/src/producer.rs` - WAL reader, transaction file writer (extracted from client.rs)
- `pg2any-lib/src/consumer.rs` - File executor, drain_and_shutdown, retry logic (extracted from client.rs)
- `pg2any-lib/src/transaction_manager.rs` - File-based transaction lifecycle + bulk insert routing + `analyze_transaction_content`. Also: `count_rendered_statements` (producer statement counting that mirrors the consumer parser), `pending_tx_count` monitoring counter
- `pg2any-lib/src/destinations/destination_factory.rs` - `DestinationHandler` trait definition
- `pg2any-lib/src/destinations/mysql.rs` - MySQL destination (sqlx + mysql_async dual pool)
- `pg2any-lib/src/destinations/sqlserver.rs` - SQL Server destination (tiberius TDS Bulk Load)
- `pg2any-lib/src/destinations/common.rs` - Shared transaction helpers (`execute_with_hook_guard`, `commit_with_hook`)
- `pg2any-lib/src/destinations/bulk_insert.rs` - INSERT detection (backtick + bracket), TSV generation, multi-value INSERT fallback
- `pg2any-lib/src/destinations/coalescing.rs` - DML coalescing (multi-value INSERT, CASE-WHEN UPDATE)
- `pg2any-lib/src/config.rs` - All configuration fields and builder
- `pg2any-lib/src/env.rs` - Environment variable mapping (supports deprecated fallback names)
- `pg2any-lib/src/error.rs` - Error types (transient vs permanent classification matters for retry logic)

## Environment Variable Naming

- New canonical names: `CDC_CHANNEL_CAPACITY`, `CDC_BATCH_SIZE`
- Deprecated aliases (still work, emit warning): `CDC_BUFFER_SIZE`, `CDC_COMMIT_BATCH_SIZE`
- Use `parse_usize_env_with_fallback` pattern for any future renames

## What NOT to Do

- Don't add `println!` - use `tracing` macros
- Don't bypass feature gates - respect `#[cfg(feature = "...")]` boundaries
- Don't add blocking I/O on the hot path - everything is async
- Don't modify LSN tracking from the producer - only the consumer updates `flush_lsn`
- Don't skip the file persistence step - it's the crash recovery mechanism
- Don't increment `segment_statement_counts` by 1 per event - count via `count_rendered_statements` so it matches the consumer's `SqlStreamParser` (multi-statement events like multi-table TRUNCATE would otherwise corrupt crash-resume skip arithmetic)
- Don't re-read a transaction file at commit/finalize just to count or to fetch metadata you already hold in memory
