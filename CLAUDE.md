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
- `TransactionStorage` trait abstracts compressed vs uncompressed file I/O

## Important Files

- `pg2any-lib/src/client.rs` - Core orchestration logic (~1500 lines), producer/consumer tasks
- `pg2any-lib/src/transaction_manager.rs` - File-based transaction lifecycle
- `pg2any-lib/src/destinations/destination_factory.rs` - `DestinationHandler` trait definition
- `pg2any-lib/src/config.rs` - All configuration fields and builder
- `pg2any-lib/src/env.rs` - Environment variable mapping
- `pg2any-lib/src/error.rs` - Error types (transient vs permanent classification matters for retry logic)

## What NOT to Do

- Don't add `println!` - use `tracing` macros
- Don't bypass feature gates - respect `#[cfg(feature = "...")]` boundaries
- Don't add blocking I/O on the hot path - everything is async
- Don't modify LSN tracking from the producer - only the consumer updates `flush_lsn`
- Don't skip the file persistence step - it's the crash recovery mechanism
