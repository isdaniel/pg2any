[![Crates.io Version](https://img.shields.io/crates/v/pg2any_lib)](https://crates.io/crates/pg2any_lib)
[![Crates.io Downloads (recent)](https://img.shields.io/crates/dr/pg2any_lib)](https://crates.io/crates/pg2any_lib)
[![Crates.io Total Downloads](https://img.shields.io/crates/d/pg2any_lib)](https://crates.io/crates/pg2any_lib)
[![docs.rs](https://img.shields.io/docsrs/pg2any_lib)](https://docs.rs/pg2any-lib)

# PostgreSQL to Any Database Replication (pg2any) 

A high-performance, production-ready PostgreSQL to Any database replication tool using Change Data Capture (CDC) with logical replication. This tool streams database changes in real-time from PostgreSQL to target databases such as MySQL and SQL Server with comprehensive error handling and monitoring.

## Project Status

This is a **fully functional CDC implementation** providing enterprise-grade PostgreSQL to Any database replication using logical replication production-ready features.

**Current Status**: Production-ready CDC tool with complete PostgreSQL logical replication protocol implementation, and real-time change streaming capabilities with graceful shutdown and LSN persistence.

### What's Implemented

- **Production-Ready Architecture**: Async/await with Tokio, structured error handling, graceful shutdown
- **PostgreSQL Logical Replication**: Full protocol implementation with libpq-sys integration
- **Real-time CDC Pipeline**: Live streaming of INSERT, UPDATE, DELETE, TRUNCATE operations
- **Transaction Consistency**: Transaction-level atomicity with BEGIN/COMMIT boundary handling and LSN persistence
- **Database Destinations**: Complete MySQL, SQL Server, and SQLite implementations with shared utilities
- **Schema Mapping**: Configurable PostgreSQL schema to destination database name translation
- **Configuration Management**: Environment variables and builder pattern with validation
- **Docker Development**: Multi-service environment with PostgreSQL, MySQL setup
- **Development Tooling**: Makefile automation, formatting, linting, and quality checks
- **Production Logging**: Structured tracing with configurable levels and filtering

## Features

- **Async Runtime**: High-performance async/await with Tokio and proper cancellation
- **PostgreSQL Integration**: Native logical replication with libpq-sys bindings
- **Multiple Destinations**: MySQL (via SQLx), SQL Server (via Tiberius), and SQLite (via SQLx) support
- **Transaction-Level Processing**: Complete transactions processed atomically from BEGIN to COMMIT
- **Streaming Transaction Support**: Handles large transactions with mid-stream batching
- **Schema Mapping**: Configurable mapping from PostgreSQL schemas to destination database names
- **Transaction Safety**: ACID compliance with full transaction atomicity and ordering
- **Configuration**: Environment variables, builder pattern, and validation
- **Error Handling**: Comprehensive error types with `thiserror` and proper propagation
- **Real-time Streaming**: Live change capture for all DML operations
- **Production Ready**: Structured logging, graceful shutdown, and resource management
- **Monitoring & Metrics**: Comprehensive Prometheus metrics, and health monitoring
- **HTTP Metrics Endpoint**: Built-in metrics server on port 8080 with Prometheus format
- **Development Tools**: Docker environment, Makefile automation, extensive testing

### PostgreSQL Setup

1. Enable logical replication in your PostgreSQL configuration:
   ```sql
   ALTER SYSTEM SET wal_level = logical;
   -- Restart PostgreSQL server after this change
   ```

2. Create a publication for the tables you want to replicate:
   ```sql
   CREATE PUBLICATION my_publication FOR TABLE table1, table2;
   -- Or for all tables:
   CREATE PUBLICATION my_publication FOR ALL TABLES;
   ```

3. Create a user with replication privileges:
   ```sql
   CREATE USER replicator WITH REPLICATION LOGIN PASSWORD 'password';
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
   ```

### Basic Usage

```rust
use pg2any_lib::{load_config_from_env, run_cdc_app};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Main entry point for the CDC application
/// This function sets up a complete CDC pipeline from PostgreSQL to MySQL/SqlServer/SQLite
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize comprehensive logging
    init_logging();

    tracing::info!("Starting PostgreSQL CDC Application");

    // Load configuration from environment variables
    let config = load_config_from_env()?;

    // Run the CDC application with graceful shutdown handling
    run_cdc_app(config, None).await?;

    tracing::info!("CDC application stopped");
    Ok(())
}

/// Initialize comprehensive logging configuration
///
/// Sets up structured logging with filtering, thread IDs, and ANSI colors.
/// The log level can be controlled via the `RUST_LOG` environment variable.
///
/// # Default Log Level
///
/// If `RUST_LOG` is not set, defaults to:
/// - `pg2any=debug` - Debug level for our application
/// - `tokio_postgres=info` - Info level for PostgreSQL client
/// - `sqlx=info` - Info level for SQL execution
pub fn init_logging() {
    // Create a sophisticated logging setup
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("pg2any=debug,tokio_postgres=info,sqlx=info"));

    let fmt_layer = fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(true)
        .compact();

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();

    tracing::info!("Logging initialized with level filtering");
}
```

## Architecture

### Data Flow Architecture

```
                                          ┌──────────────────────────────────┐
                                          │  PostgreSQL Logical Replication  │
                                          │         (WAL Stream)             │
                                          └────────────┬─────────────────────┘
                                                       │
                                                       ▼
                                          ┌────────────────────────┐
                                          │   Protocol Parser     │
                                          │  (Message Decoding)   │
                                          └──────────┬─────────────┘
                                                     │
                                                     ▼
                                          ┌──────────────────────┐
                                          │   Transaction        │
                                          │   Assembler          │
                                          └──────────┬───────────┘
                                                     │
                                                     ▼
                                          ┌──────────────────────┐
                                          │   Transaction        │
                                          │   Channel (Bounded)  │
                                          └──────────┬───────────┘
                                                     │
                                                     ▼
                                          ┌──────────────────────┐
                                          │   Consumer Task      │
                                          │  (Single Thread)     │
                                          └──────────┬───────────┘
                                                     │
                                                     ▼
                                          ┌──────────────────────┐
                                          │  Target Database     │
                                          │ (MySQL/SQLServer/    │
                                          │      SQLite)         │
                                          └──────────────────────┘

Key: Single producer-single consumer with transaction-level atomicity and ordering
```

## Project Structure

This Cargo workspace provides a complete CDC implementation with clean separation of concerns:

```
pg2any/                          # Workspace root
├── Cargo.toml                   # Workspace configuration with shared dependencies
├── Cargo.lock                   # Dependency lock file
├── README.md                    # This documentation file
├── CHANGELOG.md                 # Release notes and version history
├── LICENSE                      # Project license
├── Makefile                     # Development automation (35+ commands)
├── Dockerfile                   # Application containerization
├── docker-compose.yml           # Multi-database development environment
├── .gitignore                   # Git ignore patterns
├── .cargo/                      # Cargo configuration
├── .github/                     # GitHub workflows and templates
├── .vscode/                     # VS Code workspace settings
├── docs/                        # Project documentation
│   └── DOCKER.md                # Docker setup and usage guide
├── env/                         # Environment configuration
│   ├── .env                     # Default environment variables
│   └── .env_local               # Local development overrides
├── examples/                    # Example applications and scripts
│   ├── Cargo.toml               # Examples workspace configuration
│   ├── pg2any_last_lsn          # LSN persistence file (runtime generated)
│   ├── src/
│   │   └── main.rs              # Example CLI application entry point
│   ├── scripts/                 # Database initialization scripts
│   │   ├── init_postgres.sql    # PostgreSQL setup with logical replication
│   │   └── init_mysql.sql       # MySQL destination database setup
│   └── monitoring/              # Monitoring and observability setup
│       ├── prometheus.yml       # Prometheus configuration
│       ├── prometheus-rules/    # Alert rules for monitoring
│       │   └── cdc-alerts.yml   # CDC-specific alerting rules
│       └── exporter/            # Database exporters
│           └── mysql/           # MySQL exporter configuration
├── pg2any-lib/                  # Core CDC library
│   ├── Cargo.toml               # Library dependencies with feature flags
│   ├── src/
│   │   ├── lib.rs               # Public API exports and documentation
│   │   ├── app.rs               # High-level CDC application orchestration
│   │   ├── client.rs            # Main CDC client implementation
│   │   ├── config.rs            # Configuration management and validation
│   │   ├── connection.rs        # PostgreSQL connection handling
│   │   ├── env.rs               # Environment variable loading
│   │   ├── error.rs             # Comprehensive error types
│   │   ├── logical_stream.rs    # Logical replication stream management
│   │   ├── pg_replication.rs    # Low-level PostgreSQL replication
│   │   ├── replication_protocol.rs # Message parsing and protocol handling
│   │   ├── buffer.rs            # Binary protocol buffer operations
│   │   ├── types.rs             # Core data types and enums
│   │   ├── destinations/        # Database destination implementations
│   │   │   ├── mod.rs           # Destination trait and factory pattern
│   │   │   ├── destination_factory.rs # Factory for creating destinations
│   │   │   ├── common.rs        # Common transaction handling utilities
│   │   │   ├── mysql.rs         # MySQL destination with SQLx
│   │   │   ├── sqlserver.rs     # SQL Server destination with Tiberius
│   │   │   └── sqlite.rs        # SQLite destination with SQLx
│   │   └── monitoring/          # Monitoring and metrics system
│   │       ├── mod.rs           # Monitoring module exports
│   │       ├── metrics.rs       # Core metrics definitions
│   │       ├── metrics_abstraction.rs # Metrics abstraction layer
│   │       └── metrics_server.rs # HTTP metrics server
│   └── tests/                   # Comprehensive test suite (10 test files, 100+ tests)
│       ├── integration_tests.rs       # End-to-end CDC testing
│       ├── destination_integration_tests.rs # Database destination testing
│       ├── event_type_refactor_tests.rs    # Event type handling tests
│       ├── mysql_edge_cases_tests.rs       # MySQL-specific edge cases
│       ├── mysql_error_handling_simple_tests.rs # Error handling tests
│       ├── mysql_where_clause_fix_tests.rs # WHERE clause generation tests
│       ├── replica_identity_tests.rs       # Replica identity handling
│       ├── sqlite_comprehensive_tests.rs   # SQLite comprehensive testing
│       ├── sqlite_destination_tests.rs     # SQLite destination tests
│       └── where_clause_fix_tests.rs       # WHERE clause bug fixes
```

## Supported Destination Databases

### Currently Implemented
- **MySQL**: Full implementation using SQLx with connection pooling, type mapping, and DML operations
- **SQL Server**: Native implementation using Tiberius TDS protocol with comprehensive type support
- **SQLite**: Complete implementation using SQLx with file-based storage and embedded scenarios

## Change Event Types

```rust
pub enum EventType {
    Insert,
    Update, 
    Delete,
    Truncate,
    Begin,       // Transaction begin
    Commit,      // Transaction commit
    Relation,    // Table schema information
    Type,        // Data type information
    Origin,      // Replication origin
    Message,     // Custom logical replication message
}
```

## Configuration

pg2any supports comprehensive configuration through environment variables or the `ConfigBuilder` pattern. All configuration can be managed through environment variables for containerized deployments or programmatically using the builder pattern.

### Environment Variables Mapping Table

| Category | Variable | Description | Default Value | Example | Notes |
|----------|----------|-------------|---------------|---------|-------|
| **Source PostgreSQL** | | | | | |
| | `CDC_SOURCE_CONNECTION_STRING` | Complete PostgreSQL connection string | | `postgresql://user:pass@host:port/db?replication=database` | Required for PostgreSQL logical replication |
| **Destination** | | | | | |
| | `CDC_DEST_TYPE` | Target database type | `MySQL` | `MySQL`, `SqlServer`, `SQLite` | Case-insensitive |
| | `CDC_DEST_URI` | **Complete destination connection string** | | See destination-specific examples below | **Primary connection method - replaces individual host/port/user/password variables** |
| **CDC Settings** | | | | | |
| | `CDC_REPLICATION_SLOT` | PostgreSQL replication slot | `cdc_slot` | `my_app_slot` | |
| | `CDC_PUBLICATION` | PostgreSQL publication name | `cdc_pub` | `my_app_publication` | |
| | `CDC_PROTOCOL_VERSION` | Replication protocol version | `1` | `1` | Integer value |
| | `CDC_BINARY_FORMAT` | Use binary message format | `false` | `true` | Boolean |
| | `CDC_STREAMING` | Enable streaming mode | `true` | `false` | Boolean |
| **Schema Mapping** | | | | | |
| | `CDC_SCHEMA_MAPPING` | Schema name translation for destination | | `public:cdc_db,myschema:mydb` | Maps PostgreSQL schemas to destination database names |
| **Timeouts** | | | | | |
| | `CDC_CONNECTION_TIMEOUT` | Connection timeout (seconds) | `30` | `60` | Integer |
| | `CDC_QUERY_TIMEOUT` | Query timeout (seconds) | `10` | `30` | Integer |
| **Performance** | | | | | |
| | `CDC_BUFFER_SIZE` | Size of the event channel buffer | `1000` | `5000`, `10000` | Integer. Larger buffers handle burst traffic better but use more memory |
| **System** | | | | | |
| | `CDC_LAST_LSN_FILE` | LSN persistence file | `./pg2any_last_lsn` | `/data/lsn_state` | |
| | `RUST_LOG` | Logging level | `pg2any=debug,tokio_postgres=info,sqlx=info` | `info` | Standard Rust logging |

### Key Configuration Approach

 pg2any uses `CDC_DEST_URI` as the primary method for destination database configuration. This uses standard database connection string formats instead of separate host, port, user, and password variables, making configuration simpler and more portable.

```bash
# Primary configuration method (recommended)
CDC_DEST_TYPE=MySQL
CDC_DEST_URI=mysql://user:password@host:port/database
```

### Destination Database Environment Configuration

pg2any uses the `CDC_DEST_URI` environment variable as the primary connection method for all destination databases. This simplifies configuration by using connection strings instead of separate host, port, user, and password variables.

#### MySQL Destination
```bash
# Source PostgreSQL
CDC_SOURCE_CONNECTION_STRING=postgresql://postgres:pass.123@127.0.0.1:5432/postgres?replication=database

# MySQL Destination - Complete connection string
CDC_DEST_TYPE=MySQL
CDC_DEST_URI=mysql://user:password@host:port/database

# Examples:
# Docker environment
CDC_DEST_URI=mysql://root:test.123@127.0.0.1:3306/mysql
# Production environment  
CDC_DEST_URI=mysql://cdc_user:secure_pass@mysql-prod.company.com:3306/replica_db

# Schema Mapping (maps PostgreSQL schema to MySQL database)
# Required when PostgreSQL uses "public" schema but MySQL uses a different database name
CDC_SCHEMA_MAPPING=public:cdc_db

# CDC Configuration
CDC_REPLICATION_SLOT=cdc_slot
CDC_PUBLICATION=cdc_pub
```

#### SQL Server Destination
```bash
# Source PostgreSQL
CDC_SOURCE_CONNECTION_STRING=postgresql://postgres:pass.123@127.0.0.1:5432/postgres?replication=database

# SQL Server Destination - Complete connection string
CDC_DEST_TYPE=SqlServer  
CDC_DEST_URI=sqlserver://user:password@host:port/database

# Examples:
# Local SQL Server
CDC_DEST_URI=sqlserver://sa:MyPass@123@localhost:1433/master
# Azure SQL Database
CDC_DEST_URI=sqlserver://user@server:password@server.database.windows.net:1433/mydb
# Production SQL Server
CDC_DEST_URI=sqlserver://cdc_user:secure_pass@sqlserver-prod:1433/replica_db

# CDC Configuration
CDC_REPLICATION_SLOT=cdc_slot
CDC_PUBLICATION=cdc_pub
```

#### SQLite Destination
```bash
# Source PostgreSQL
CDC_SOURCE_CONNECTION_STRING=postgresql://postgres:pass.123@127.0.0.1:5432/postgres?replication=database

# SQLite Destination - File path (no authentication needed)
CDC_DEST_TYPE=SQLite
CDC_DEST_URI=./path/to/database.db

# Examples:
# Local development
CDC_DEST_URI=./my_replica.db
# Absolute path
CDC_DEST_URI=/data/cdc/replica.db
# In-memory (for testing)
CDC_DEST_URI=:memory:

# CDC Configuration
CDC_REPLICATION_SLOT=cdc_slot
CDC_PUBLICATION=cdc_pub
CDC_STREAMING=true
```

### Connection String Format Summary

| Database | CDC_DEST_URI Format | Example |
|----------|---------------------|---------|
| **MySQL** | `mysql://user:password@host:port/database` | `mysql://root:pass123@localhost:3306/mydb` |
| **SQL Server** | `sqlserver://user:password@host:port/database` | `sqlserver://sa:pass123@localhost:1433/master` |
| **SQLite** | `./path/to/file.db` or `/absolute/path/file.db` | `./replica.db` or `/data/replica.db` |

### Schema Mapping Configuration

PostgreSQL uses schemas (e.g., `public`) to organize tables, while MySQL uses databases. When replicating from PostgreSQL to MySQL, you may need to map PostgreSQL schema names to MySQL database names to avoid errors like `Table 'public.t1' doesn't exist`.

The `CDC_SCHEMA_MAPPING` environment variable allows you to configure these mappings:

```bash
# Format: source_schema:dest_database,source_schema2:dest_database2
CDC_SCHEMA_MAPPING=public:cdc_db

# Multiple mappings
CDC_SCHEMA_MAPPING=public:cdc_db,sales:sales_db,hr:hr_db
```

**Note:** Schema mapping is primarily useful for MySQL and SQL Server destinations. SQLite doesn't use schema namespacing, so mappings are ignored for SQLite destinations.

### Performance Tuning

#### Producer-Consumer Architecture

pg2any uses a **single producer-single consumer architecture** optimized for transaction consistency:

- **Single Producer**: Reads from PostgreSQL logical replication stream and pushes events to channel
- **Single Consumer**: Processes events from channel and writes to destination database
- **Bounded Channel**: Acts as buffer between producer and consumer for burst traffic handling
- **Transaction Ordering**: Single consumer ensures strict transaction ordering and consistency

#### Performance Configuration Parameters

**CDC_BUFFER_SIZE** (Event Channel Buffer)

The buffer size determines how many events can be queued between the producer and consumer:
- **Smaller buffers** (100-1000): Lower memory usage, better for steady-state workloads
- **Larger buffers** (5000-10000): Better burst handling, more memory usage

#### Performance Examples

**Low Volume / Low Latency**
```bash
CDC_BUFFER_SIZE=100
```

**Burst Traffic Handling**
```bash
CDC_BUFFER_SIZE=10000
# Best for: Intermittent high-volume bursts
```

### Programmatic Configuration

You can also configure pg2any programmatically using the builder pattern with connection strings:

```rust
use pg2any_lib::{Config, DestinationType};
use std::time::Duration;

// SQLite example
let sqlite_config = Config::builder()
    .source_connection_string("postgresql://postgres:pass.123@localhost:5432/postgres?replication=database")
    .destination_type(DestinationType::SQLite)
    .destination_connection_string("./my_replica.db")
    .replication_slot_name("cdc_slot")
    .publication_name("cdc_pub")
    .protocol_version(2)
    .binary_format(false)
    .streaming(true)
    .build()?;

// MySQL example
let mysql_config = Config::builder()
    .source_connection_string("postgresql://postgres:pass.123@localhost:5432/postgres?replication=database")
    .destination_type(DestinationType::MySQL)
    .destination_connection_string("mysql://root:pass123@localhost:3306/replica_db")
    .replication_slot_name("cdc_slot")
    .publication_name("cdc_pub")
    .connection_timeout(Duration::from_secs(30))
    .query_timeout(Duration::from_secs(10))
    .heartbeat_interval(Duration::from_secs(10))
    .build()?;

// SQL Server example
let sqlserver_config = Config::builder()
    .source_connection_string("postgresql://postgres:pass.123@localhost:5432/postgres?replication=database")
    .destination_type(DestinationType::SqlServer)
    .destination_connection_string("sqlserver://sa:MyPass@123@localhost:1433/master")
    .replication_slot_name("cdc_slot")
    .publication_name("cdc_pub")
    .build()?;
```

### Configuration Validation

The configuration system provides comprehensive validation:
- **Connection Strings**: Automatically formatted and validated
- **Type Safety**: Proper enum handling for destination types
- **Default Values**: Sensible defaults for all optional parameters
- **Error Handling**: Clear error messages for invalid configurations

## Development Status

## Monitoring & Observability

pg2any includes comprehensive monitoring and observability features for production environments:

### Built-in Metrics System
- **HTTP Metrics Endpoint**: Prometheus-compatible metrics served on port 8080
- **Real-time Monitoring**: Replication lag, event processing rates, connection status
- **Resource Tracking**: Memory usage, network I/O, active connections, queue depth

### Key Metrics Available

```prometheus
# Core Replication Metrics
pg2any_events_processed_total          # Total CDC events processed
pg2any_events_by_type_total            # Events by type (insert/update/delete)
pg2any_replication_lag_seconds         # Current replication lag
pg2any_events_per_second               # Event processing rate
pg2any_last_processed_lsn              # Last processed LSN from PostgreSQL WAL

# Health & Error Metrics
pg2any_errors_total                    # Total errors by type and component
pg2any_source_connection_status        # PostgreSQL connection status
pg2any_destination_connection_status   # Destination database connection status

# Performance Metrics
pg2any_event_processing_duration_seconds # Event processing time
pg2any_queue_depth                     # Events waiting to be processed
pg2any_network_bytes_received_total    # Network I/O from PostgreSQL
pg2any_buffer_memory_usage_bytes       # Memory usage for event buffers
```

### Complete Monitoring Stack
The Docker environment includes a full observability stack:

- **Prometheus**: Metrics collection and storage (port 9090)
- **Node Exporter**: System metrics (port 9100)
- **PostgreSQL Exporter**: Database metrics (port 9187)
- **MySQL Exporter**: Destination database metrics (port 9104)
- **Alert Rules**: Predefined alerts for lag, errors, and connection issues

## Quick Start with Docker

Get up and running in minutes with the complete development environment including monitoring:

```bash
# Clone the repository
git clone https://github.com/isdaniel/pg2any
cd pg2any

# Start the complete environment (databases + monitoring)
docker-compose up -d

# Build the application
make build

# Run the CDC application with monitoring
RUST_LOG=info make run

# Access monitoring dashboards
open http://localhost:9090   # Prometheus metrics
open http://localhost:8080/metrics  # Application metrics

# In another terminal, test with sample data
make test-data     # Insert test data into PostgreSQL
make show-data     # Verify replication to destination databases
```

### Available Make Commands

**Development:**
```bash
make build         # Build the Rust application
make check         # Run cargo check and validation
make test          # Run the full test suite (104+ tests)
make format        # Format code with rustfmt
make run           # Run the CDC application locally
```

**Docker Management:**
```bash
make docker-start  # Start databases and monitoring stack
make docker-stop   # Stop all services
make docker-logs   # View application logs
make docker-status # Check service status
```

## Local Development

For development without Docker (requires manual database setup):

```bash
# Build and validate the project
make build             # Compile the application
make check             # Run code quality checks
make test              # Execute full test suite
make format            # Format code with rustfmt

# Run the application (requires PostgreSQL and destination DB)
RUST_LOG=info make run

# Development workflow
make dev-setup         # Complete development setup
make before-git-push   # Pre-commit validation
```

## Feature Configuration

pg2any supports feature flags to enable or disable optional functionality, allowing you to build a lighter binary when certain features aren't needed.

### Metrics Feature

The metrics collection and HTTP metrics server can be enabled/disabled using the `metrics` feature flag:

```bash
# Build with metrics (default)
cargo build

# Build with metrics explicitly
cargo build --features metrics

# Build without metrics (smaller binary, ~17% reduction)
cargo build --no-default-features --features mysql,sqlserver,sqlite

# Run tests with metrics enabled
cargo test --features metrics
```

## Example Application Output

When you run the application, you'll see structured logging output like this:

```
2025-08-15T10:30:00.123Z INFO  pg2any: Starting PostgreSQL CDC Application
2025-08-15T10:30:00.124Z INFO  pg2any: Loading configuration from environment variables
2025-08-15T10:30:00.125Z INFO  pg2any: Configuration loaded successfully
2025-08-15T10:30:00.126Z INFO  pg2any: Initializing CDC client
2025-08-15T10:30:00.127Z INFO  pg2any: Performing CDC client initialization
2025-08-15T10:30:00.128Z INFO  pg2any: CDC client initialized successfully
2025-08-15T10:30:00.129Z INFO  pg2any: Starting CDC replication pipeline
2025-08-15T10:30:00.130Z DEBUG pg2any_lib::logical_stream: Creating logical replication stream
2025-08-15T10:30:00.131Z DEBUG pg2any_lib::pg_replication: Connected to PostgreSQL server version: 150000
2025-08-15T10:30:00.132Z INFO  pg2any_lib::client: Processing BEGIN transaction (LSN: 0/1A2B3C4D)
2025-08-15T10:30:00.133Z INFO  pg2any_lib::client: Processing INSERT event on table 'users'
2025-08-15T10:30:00.134Z INFO  pg2any_lib::client: Processing COMMIT transaction (LSN: 0/1A2B3C5E)
2025-08-15T10:30:00.135Z INFO  pg2any: CDC replication running! Real-time change streaming active
```

**Note**: This shows the production-ready application with real PostgreSQL logical replication, integrated metrics collection, LSN tracking, and comprehensive monitoring capabilities.

## Dependencies

### Core Runtime
- **tokio** (1.47.1): Async runtime with full feature set
- **hyper** (1.x): HTTP server for metrics endpoint
- **prometheus** (0.13): Metrics collection and Prometheus integration
- **tokio-util** (0.7.16): Utilities for async operations and cancellation

### Database Clients
- **sqlx** (0.8.6): MySQL async client with runtime-tokio-rustls
- **tiberius** (0.12): Native SQL Server TDS protocol implementation
- **libpq-sys** (0.8): Low-level PostgreSQL C library bindings

### Serialization & Data
- **serde** (1.0.219): Serialization framework with derive support
- **serde_json** (1.0.142): JSON serialization
- **chrono** (0.4.41): Date/time handling with serde support
- **bytes** (1.10.1): Byte buffer manipulation

### Error Handling & Utilities
- **thiserror** (2.0.12): Ergonomic error handling and propagation
- **async-trait** (0.1.88): Async trait definitions
- **tracing** (0.1.41): Structured logging and instrumentation
- **tracing-subscriber** (0.3.20): Log filtering and formatting
- **prometheus** (0.13): Metrics collection library
- **lazy_static** (1.4): Global metrics registry initialization
- **libc** (0.2.174): C library bindings for system operations

### Running Tests
```bash
make test                    # Run all tests
cargo test --lib             # Library unit tests only
cargo test integration       # Integration tests only
cargo test mysql             # MySQL-specific tests
```

## Contributing

This project provides **production-ready PostgreSQL CDC replication** with a solid, well-tested foundation that makes contributing straightforward and impactful.

### Getting Started Contributing

```bash
# Fork and clone the repository
git clone https://github.com/YOUR_USERNAME/pg2any
cd pg2any

# Set up development environment
make dev-setup          # Runs formatting, tests, and builds Docker

# Start development databases
make docker-start

# Make your changes and validate
make check              # Code quality checks
make test               # Run full test suite
make format             # Format code

# Test end-to-end functionality
make run                # Test CDC pipeline locally
```

### Testing Your Changes

```bash
# Manual testing with real databases
make docker-start        # Start PostgreSQL and MySQL
cargo run               # Test end-to-end replication
make test-data          # Insert test data
make show-data          # Verify replication worked

set -a; source env/.env_local; set +a
```

---

## References

- [PostgreSQL Logical Replication Protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html)
- [PostgreSQL WAL Internals](https://www.postgresql.org/docs/current/wal-internals.html)
- [Logical Decoding Output Plugin](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html)
- [Rust Async Book](https://rust-lang.github.io/async-book/)
- [Tokio Runtime Documentation](https://docs.rs/tokio/latest/tokio/)
