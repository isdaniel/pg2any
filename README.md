[![Crates.io Version](https://img.shields.io/crates/v/pg2any_lib)](https://crates.io/crates/pg2any_lib)
[![Crates.io Downloads (recent)](https://img.shields.io/crates/dr/pg2any_lib)](https://crates.io/crates/pg2any_lib)

# PostgreSQL to Any Database Replication (pg2any) 

A high-performance, production-ready PostgreSQL to Any database replication tool using Change Data Capture (CDC) with logical replication. This tool streams database changes in real-time from PostgreSQL to target databases such as MySQL and SQL Server with comprehensive error handling and monitoring.

## Project Status

This is a **fully functional CDC implementation** providing enterprise-grade PostgreSQL to Any database replication using logical replication production-ready features.

**Current Status**: Production-ready CDC tool with complete PostgreSQL logical replication protocol implementation, and real-time change streaming capabilities with graceful shutdown and LSN persistence.

### What's Implemented ✅
- ✅ **Complete Rust Workspace**: Multi-crate project with `pg2any` binary and `pg2any-lib` library
- ✅ **Production-Ready Architecture**: Async/await with Tokio, structured error handling, graceful shutdown
- ✅ **PostgreSQL Logical Replication**: Full protocol implementation with libpq-sys integration
- ✅ **Real-time CDC Pipeline**: Live streaming of INSERT, UPDATE, DELETE, TRUNCATE operations
- ✅ **Transaction Consistency**: BEGIN/COMMIT boundary handling with LSN persistence
- ✅ **Database Destinations**: Complete MySQL, SQL Server, and SQLite implementations with type mapping
- ✅ **Configuration Management**: Environment variables and builder pattern with validation
- ✅ **Docker Development**: Multi-service environment with PostgreSQL, MySQL setup
- ✅ **Development Tooling**: Makefile automation, formatting, linting, and quality checks
- ✅ **Production Logging**: Structured tracing with configurable levels and filtering

### Production-Ready Features ✅
- ✅ **Monitoring & Observability**: Complete Prometheus metrics collection and alerting systems
- ✅ **Production Logging**: Structured tracing with configurable levels and HTTP metrics endpoint
- ✅ **Health Monitoring**: Database connection monitoring, replication lag tracking, and error rate alerts

### What Needs Enhancement 🚧
- 🚧 **Additional Destinations**: Oracle, ClickHouse, Elasticsearch support
- 🚧 **Schema Evolution**: DDL change handling and automatic schema migration
- 🚧 **Multi-table Replication**: Table filtering, routing, and transformation pipelines
- 🚧 **Performance Optimization**: High-throughput benchmarking and memory optimization

## Features

- ✅ **Async Runtime**: High-performance async/await with Tokio and proper cancellation
- ✅ **PostgreSQL Integration**: Native logical replication with libpq-sys bindings
- ✅ **Multiple Destinations**: MySQL (via SQLx), SQL Server (via Tiberius), and SQLite (via SQLx) support
- ✅ **Transaction Safety**: ACID compliance with BEGIN/COMMIT boundary handling
- ✅ **Configuration**: Environment variables, builder pattern, and validation
- ✅ **Error Handling**: Comprehensive error types with `thiserror` and proper propagation
- ✅ **Real-time Streaming**: Live change capture for all DML operations
- ✅ **Production Ready**: Structured logging, graceful shutdown, and resource management
- ✅ **Monitoring & Metrics**: Comprehensive Prometheus metrics, and health monitoring
- ✅ **HTTP Metrics Endpoint**: Built-in metrics server on port 8080 with Prometheus format
- ✅ **Development Tools**: Docker environment, Makefile automation, extensive testing

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

### Core Components

1. **CdcClient**: Main orchestrator managing the entire CDC pipeline
2. **Config/ConfigBuilder**: Comprehensive configuration management with environment variable support
3. **LogicalReplicationStream**: PostgreSQL logical replication lifecycle and protocol implementation
4. **LogicalReplicationParser**: Complete PostgreSQL replication protocol message parsing
5. **DestinationHandler**: Production-ready database destination handling (MySQL, SQL Server, SQLite)
6. **Error Types**: Comprehensive error handling with `CdcError` and proper error propagation
7. **Buffer Operations**: Efficient binary protocol handling with zero-copy optimizations

### Data Flow Architecture

```
PostgreSQL WAL → Logical Replication → Message Parser → Change Events → Destination Handler → Target DB
     ↓                    ↓                   ↓              ↓                    ↓              ↓
   Transactions      Protocol Messages    Parsed Events   Typed Changes    SQL Operations   Replicated Data
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
│   │   │   ├── operation.rs     # Operation types and handling
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

## Error Handling

The library provides comprehensive error types using `thiserror`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum CdcError {
    #[error("PostgreSQL connection error: {0}")]
    Connection(#[from] tokio_postgres::Error),
    
    #[error("MySQL destination error: {0}")]
    MySQL(String),
    
    #[error("SQL Server destination error: {0}")]
    SqlServer(String),
    
    #[error("Configuration error: {0}")]
    Configuration(String),
    
    #[error("Protocol parsing error: {0}")]
    Protocol(String),
    
    #[error("Generic CDC error: {0}")]
    Generic(String),
}
```

## Configuration

pg2any supports comprehensive configuration through environment variables or the `ConfigBuilder` pattern. All configuration can be managed through environment variables for containerized deployments or programmatically using the builder pattern.

### Environment Variables Mapping Table

| Category | Variable | Description | Default Value | Example | Notes |
|----------|----------|-------------|---------------|---------|-------|
| **Source PostgreSQL** | | | | | |
| | `CDC_SOURCE_HOST` | PostgreSQL hostname | `localhost` | `postgres` | |
| | `CDC_SOURCE_PORT` | PostgreSQL port | `5432` | `5432` | |
| | `CDC_SOURCE_DB` | PostgreSQL database name | `postgres` | `myapp_db` | |
| | `CDC_SOURCE_USER` | PostgreSQL username | `postgres` | `replication_user` | |
| | `CDC_SOURCE_PASSWORD` | PostgreSQL password | `postgres` | `securepassword` | |
| **Destination** | | | | | |
| | `CDC_DEST_TYPE` | Target database type | `MySQL` | `SQLite`, `SqlServer` | Case-insensitive |
| | `CDC_DEST_URI` | Destination URI/host/file path | `localhost` for databases, `./cdc_target.db` for SQLite | `mysql-server`, `/data/replica.db` | Host for databases, file path for SQLite |
| | `CDC_DEST_PORT` | Destination port | `3306` (MySQL), `1433` (SqlServer) | `3306` | Not used for SQLite |
| | `CDC_DEST_DB` | Destination database name | `cdc_target` | `replicated_db` | For SQLite: file path |
| | `CDC_DEST_USER` | Destination username | `cdc_user` | `replica_user` | Not used for SQLite |
| | `CDC_DEST_PASSWORD` | Destination password | `cdc_password` | `secure123` | Not used for SQLite |
| **CDC Settings** | | | | | |
| | `CDC_REPLICATION_SLOT` | PostgreSQL replication slot | `cdc_slot` | `my_app_slot` | |
| | `CDC_PUBLICATION` | PostgreSQL publication name | `cdc_pub` | `my_app_publication` | |
| | `CDC_PROTOCOL_VERSION` | Replication protocol version | `1` | `1` | Integer value |
| | `CDC_BINARY_FORMAT` | Use binary message format | `false` | `true` | Boolean |
| | `CDC_STREAMING` | Enable streaming mode | `true` | `false` | Boolean |
| **Timeouts** | | | | | |
| | `CDC_CONNECTION_TIMEOUT` | Connection timeout (seconds) | `30` | `60` | Integer |
| | `CDC_QUERY_TIMEOUT` | Query timeout (seconds) | `10` | `30` | Integer |
| | `CDC_HEARTBEAT_INTERVAL` | Heartbeat interval (seconds) | `10` | `15` | Integer |
| **System** | | | | | |
| | `CDC_LAST_LSN_FILE` | LSN persistence file | `./pg2any_last_lsn` | `/data/lsn_state` | |
| | `RUST_LOG` | Logging level | `pg2any=debug,tokio_postgres=info,sqlx=info` | `info` | Standard Rust logging |

### Configuration Examples

#### MySQL Destination (Docker Environment)
```bash
# Source PostgreSQL
CDC_SOURCE_HOST=postgres
CDC_SOURCE_PORT=5432
CDC_SOURCE_DB=postgres
CDC_SOURCE_USER=postgres
CDC_SOURCE_PASSWORD=test.123

# MySQL Destination
CDC_DEST_TYPE=MySQL
CDC_DEST_URI=mysql
CDC_DEST_PORT=3306
CDC_DEST_DB=cdc_db
CDC_DEST_USER=cdc_user
CDC_DEST_PASSWORD=test.123

# CDC Configuration
CDC_REPLICATION_SLOT=cdc_slot
CDC_PUBLICATION=cdc_pub
```

#### SQLite Destination (Local Development)
```bash
# Source PostgreSQL
CDC_SOURCE_HOST=localhost
CDC_SOURCE_PORT=5432
CDC_SOURCE_DB=postgres
CDC_SOURCE_USER=postgres
CDC_SOURCE_PASSWORD=test.123

# SQLite Destination
CDC_DEST_TYPE=SQLite
CDC_DEST_URI=./my_replica.db

# CDC Configuration
CDC_REPLICATION_SLOT=cdc_slot
CDC_PUBLICATION=cdc_pub
CDC_STREAMING=true
```

#### SQL Server Destination (Production)
```bash
# Source PostgreSQL
CDC_SOURCE_HOST=prod-postgres.example.com
CDC_SOURCE_PORT=5432
CDC_SOURCE_DB=application_db
CDC_SOURCE_USER=replication_user
CDC_SOURCE_PASSWORD=${POSTGRES_PASSWORD}

# SQL Server Destination
CDC_DEST_TYPE=SqlServer
CDC_DEST_URI=prod-sqlserver.example.com
CDC_DEST_PORT=1433
CDC_DEST_DB=replica_db
CDC_DEST_USER=sa
CDC_DEST_PASSWORD=${SQLSERVER_PASSWORD}

# Production Settings
CDC_CONNECTION_TIMEOUT=60
CDC_QUERY_TIMEOUT=30
CDC_HEARTBEAT_INTERVAL=30
```

### Programmatic Configuration

You can also configure pg2any programmatically using the builder pattern:

```rust
```rust
use pg2any_lib::{Config, DestinationType};
use std::time::Duration;

let config = Config::builder()
    .source_connection_string("postgresql://postgres:test.123@localhost:7777/postgres")
    .destination_type(DestinationType::SQLite)
    .destination_connection_string("./my_replica.db")
    .replication_slot_name("cdc_slot")
    .publication_name("cdc_pub")
    .protocol_version(1)
    .binary_format(false)
    .streaming(true)
    .auto_create_tables(true)
    .connection_timeout(Duration::from_secs(30))
    .query_timeout(Duration::from_secs(10))
    .heartbeat_interval(Duration::from_secs(10))
    .build()?;
```

### Configuration Validation

The configuration system provides comprehensive validation:
- **Connection Strings**: Automatically formatted and validated
- **Type Safety**: Proper enum handling for destination types
- **Default Values**: Sensible defaults for all optional parameters
- **Error Handling**: Clear error messages for invalid configurations

## Development Status

### ✅ Production-Ready Implementation 
This project provides **enterprise-grade PostgreSQL to Any database replication** with comprehensive monitoring:

- **🏗️ Core CDC Pipeline**: Complete end-to-end replication with transaction consistency
- **🔄 PostgreSQL Protocol**: Full logical replication implementation with binary message parsing
- **📊 Change Processing**: Real-time streaming of all DML operations with proper error handling
- **🎯 Destination Support**: Production-ready MySQL, SQL Server, and SQLite implementations
- **⚙️ Configuration**: Flexible environment-based configuration with validation
- **🐳 Docker Environment**: Complete development setup with multi-database support
- **📈 Monitoring**: Prometheus metrics collection and alerting systems
- **🛡️ Error Handling**: Comprehensive error types with proper propagation and recovery
- **🔄 Graceful Shutdown**: Proper resource cleanup and LSN persistence
- **📊 Health Monitoring**: HTTP metrics endpoint, connection status, and replication lag tracking

### � Enhancement Opportunities
- **️ Additional Databases**: Oracle, ClickHouse, Elasticsearch support  
- **🔄 Schema Evolution**: DDL change handling and migration automation
- **🎯 Advanced Features**: Table filtering, transformations, and routing
- **⚡ Performance**: High-throughput optimization and benchmarking

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

### Library Usage

When using pg2any as a library, you can selectively enable features:

```toml
[dependencies]
pg2any_lib = "0.2.0"

[dependencies]
pg2any_lib = { version = "0.2.0", default-features = false, features = ["mysql"] }

[dependencies]
pg2any_lib = { version = "0.2.0", features = ["metrics", "mysql", "sqlite"] }
```

**Simple usage (metrics abstracted away):**
```rust
use pg2any_lib::{load_config_from_env, CdcApp, init_metrics};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize metrics (no-op when disabled)
    init_metrics()?;
    
    let config = load_config_from_env()?;
    let mut app = CdcApp::new(config).await?;
    app.run(None).await?;
    Ok(())
}
```

**With metrics server (when metrics feature enabled):**
```rust
use pg2any_lib::{create_metrics_server, MetricsServerConfig, load_config_from_env, CdcApp};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config_from_env()?;
    let mut app = CdcApp::new(config).await?;
    
    // Conditionally start metrics server
    #[cfg(feature = "metrics")]
    let _server = {
        let server = create_metrics_server(8080);
        tokio::spawn(async move { server.start().await })
    };
    
    app.run(None).await?;
    Ok(())
}
```

## Example Application Output

When you run the application, you'll see structured logging output like this:

```
2025-08-15T10:30:00.123Z INFO  pg2any: 🚀 Starting PostgreSQL CDC Application
2025-08-15T10:30:00.124Z INFO  pg2any: 📋 Loading configuration from environment variables
2025-08-15T10:30:00.125Z INFO  pg2any: 🔗 Configuration loaded successfully
2025-08-15T10:30:00.126Z INFO  pg2any: ⚙️  Initializing CDC client
2025-08-15T10:30:00.127Z INFO  pg2any: 🔧 Performing CDC client initialization
2025-08-15T10:30:00.128Z INFO  pg2any: ✅ CDC client initialized successfully
2025-08-15T10:30:00.129Z INFO  pg2any: 🔄 Starting CDC replication pipeline
2025-08-15T10:30:00.130Z DEBUG pg2any_lib::logical_stream: Creating logical replication stream
2025-08-15T10:30:00.131Z DEBUG pg2any_lib::pg_replication: Connected to PostgreSQL server version: 150000
2025-08-15T10:30:00.132Z INFO  pg2any_lib::client: Processing BEGIN transaction (LSN: 0/1A2B3C4D)
2025-08-15T10:30:00.133Z INFO  pg2any_lib::client: Processing INSERT event on table 'users'
2025-08-15T10:30:00.134Z INFO  pg2any_lib::client: Processing COMMIT transaction (LSN: 0/1A2B3C5E)
2025-08-15T10:30:00.135Z INFO  pg2any: ✨ CDC replication running! Real-time change streaming active
```

**Note**: This shows the production-ready application with real PostgreSQL logical replication, integrated metrics collection, LSN tracking, and comprehensive monitoring capabilities.

## Dependencies

### Core Runtime
- **tokio** (1.47.1): Async runtime with full feature set
- **hyper** (1.x): HTTP server for metrics endpoint
- **prometheus** (0.13): Metrics collection and Prometheus integration
- **tokio-postgres** (0.7.13): PostgreSQL async client with logical replication support
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

set -a; source .env; set +a
```

### 📋 Contribution Guidelines

- **Code Quality**: Follow existing patterns, use `make before-git-push`
- **Testing**: Add tests for new functionality
- **Documentation**: Update README and inline docs
- **Error Handling**: Use the established `CdcError` pattern
- **Performance**: Consider async patterns and resource usage

---

## References

- [PostgreSQL Logical Replication Protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html)
- [PostgreSQL WAL Internals](https://www.postgresql.org/docs/current/wal-internals.html)
- [Logical Decoding Output Plugin](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html)
- [Rust Async Book](https://rust-lang.github.io/async-book/)
- [Tokio Runtime Documentation](https://docs.rs/tokio/latest/tokio/)
