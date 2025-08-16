# PostgreSQL to Any Database Replication (pg2any)

A comprehensive PostgreSQL to Any database replication tool using Change Data Capture (CDC) with logical replication. This tool allows you to stream database changes in real-time from PostgreSQL to other databases such as SQL Server, MySQL, and more.

## ⚠️ Project Status

This is a **working CDC implementation** that provides comprehensive PostgreSQL to Any database replication using logical replication. 

**Current Status**: This is a functional CDC tool with complete PostgreSQL logical replication protocol implementation, comprehensive test coverage, and real-time change streaming capabilities.

### What's Implemented ✅
- ✅ Complete project structure with Rust workspace configuration
- ✅ Comprehensive library (`pg2any-lib`) with modular architecture  
- ✅ Rust async architecture with Tokio runtime
- ✅ Configuration management with builder pattern and environment variable support
- ✅ Comprehensive error handling framework with typed errors (`thiserror`)
- ✅ **PostgreSQL logical replication protocol implementation** with full message parsing
- ✅ **WAL (Write-Ahead Log) record interpretation and processing** 
- ✅ **Binary protocol message handling** with efficient buffer operations
- ✅ **LSN (Log Sequence Number) tracking and feedback mechanisms**
- ✅ **Transaction boundary handling** (BEGIN, COMMIT) with consistency guarantees
- ✅ Complete destination handlers for MySQL and SQL Server
- ✅ Real-time change streaming (INSERT, UPDATE, DELETE, TRUNCATE)
- ✅ Graceful shutdown with CancellationToken and proper resource cleanup
- ✅ Docker containerization with multi-database development environment
- ✅ Development tooling (Makefile, formatting, testing, linting)
- ✅ Production-ready logging and structured error handling

### What Needs Enhancement 🚧
- 🚧 **Production-ready error recovery** and automatic reconnection strategies
- 🚧 **Performance optimizations** for high-throughput scenarios and benchmarking
- 🚧 **Advanced monitoring** with metrics collection and observability dashboards
- 🚧 **End-to-end integration testing** with real-world database scenarios
- 🚧 **Additional destination databases** (Oracle, SQLite, ClickHouse, etc.)
- 🚧 **Schema evolution handling** for DDL changes
- 🚧 **Multi-table replication** with table filtering and routing

## Features

- ✅ **Architecture**: Complete modular library structure with `pg2any-lib` core
- ✅ **Configuration**: Environment-based configuration with builder pattern
- ✅ **Async Runtime**: Full async/await support with Tokio
- ✅ **Error Handling**: Comprehensive error types with `thiserror`
- ✅ **Replication Protocol**: Complete PostgreSQL logical replication protocol implementation
- ✅ **WAL Processing**: Full Write-Ahead Log processing and interpretation
- ✅ **Real-time Streaming**: Live change streaming (INSERT, UPDATE, DELETE, TRUNCATE)
- ✅ **Destinations**: Working implementations for MySQL and SQL Server
- ✅ **Transaction Handling**: BEGIN/COMMIT transaction boundary processing
- ✅ **Docker Support**: Complete containerized development environment
- ✅ **Development Tools**: Makefile, formatting, testing, and linting setup


### Basic Usage

```rust
use pg2any_lib::{client::CdcClient, Config, DestinationType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::init();

    // Configure the CDC client
    let config = Config::builder()
        .source_connection_string("postgresql://postgres:test.123@localhost:7777/postgres")
        .destination_type(DestinationType::MySQL)
        .destination_connection_string("mysql://cdc_user:test.123@localhost:3306/cdc_db")
        .replication_slot_name("cdc_slot")
        .publication_name("cdc_pub")
        .build()?;
    
    // Create and initialize CDC client
    let mut client = CdcClient::new(config).await?;
    client.init().await?;
    client.start_replication().await?;
    
    Ok(())
}
```

### Configuration Options

```rust
use pg2any_lib::{Config, DestinationType};
use std::time::Duration;

let config = Config::builder()
    // Required configuration
    .source_connection_string("postgresql://postgres:test.123@localhost:7777/postgres")
    .destination_type(DestinationType::MySQL) // or SqlServer
    .destination_connection_string("mysql://cdc_user:test.123@localhost:3306/cdc_db")
    .replication_slot_name("cdc_slot")
    .publication_name("cdc_pub")
    
    // Optional configuration
    .protocol_version(1) // Logical replication protocol version
    .binary_format(false) // Use text format for debugging
    .streaming(true) // Stream in-progress transactions
    .auto_create_tables(true) // Auto-create destination tables
    .connection_timeout(Duration::from_secs(30))
    .query_timeout(Duration::from_secs(10))
    .heartbeat_interval(Duration::from_secs(10))
    .build()?;
```

## Architecture

### Core Components

1. **CdcClient**: Main orchestrator managing the entire CDC pipeline
2. **Config/ConfigBuilder**: Comprehensive configuration management with environment variable support
3. **LogicalReplicationStream**: PostgreSQL logical replication lifecycle and protocol implementation
4. **LogicalReplicationParser**: Complete PostgreSQL replication protocol message parsing
5. **DestinationHandler**: Production-ready database destination handling (MySQL, SQL Server)
6. **Error Types**: Comprehensive error handling with `CdcError` and proper error propagation
7. **Buffer Operations**: Efficient binary protocol handling with zero-copy optimizations

### Data Flow Architecture

```
PostgreSQL WAL → Logical Replication → Message Parser → Change Events → Destination Handler → Target DB
     ↓                    ↓                   ↓              ↓                    ↓              ↓
   Transactions      Protocol Messages    Parsed Events   Typed Changes    SQL Operations   Replicated Data
```

## Project Structure

This workspace uses Cargo's workspace feature for optimal organization of a working CDC implementation:

```
pg2any/
├── Cargo.toml              # Workspace configuration
├── src/main.rs             # Application entry point with full CDC pipeline
├── pg2any-lib/             # Core CDC library (fully implemented)
│   ├── Cargo.toml          # Library dependencies
│   ├── src/
│   │   ├── lib.rs          # Library public API
│   │   ├── config.rs       # Configuration management
│   │   ├── client.rs       # Main CDC client with producer/consumer
│   │   ├── error.rs        # Comprehensive error types
│   │   ├── destinations/   # Database destination implementations
│   │   │   ├── mod.rs      # Destination trait and factory
│   │   │   ├── mysql.rs    # MySQL destination handler
│   │   │   └── sqlserver.rs # SQL Server destination handler
│   │   ├── connection.rs   # PostgreSQL connection management
│   │   ├── logical_stream.rs # Logical replication stream handling
│   │   ├── pg_replication.rs # Low-level PostgreSQL replication
│   │   ├── replication_protocol.rs # Message parsing implementation
│   │   ├── buffer.rs       # Binary protocol buffer operations
│   │   └── types.rs        # Core data types and enums
│   └── tests/              # Comprehensive integration tests (24 tests)
│       ├── integration_tests.rs
│       ├── destination_integration_tests.rs
│       ├── event_type_refactor_tests.rs
│       └── where_clause_fix_tests.rs
├── docker-compose.yml      # Multi-database development setup
├── Dockerfile             # Application containerization
├── Makefile               # Development commands
└── scripts/               # Database initialization scripts
    ├── init_postgres.sql
    └── init_mysql.sql
```

## Supported Destination Databases

- **MySQL**: Complete implementation with type mapping, table creation, and DML operations
- **SQL Server**: Full implementation with type mapping, table creation, and DML operations
- **Extensible**: Architecture designed for easy addition of new destination types

### Destination Features
- ✅ Automatic table creation with proper schema mapping
- ✅ INSERT, UPDATE, DELETE, TRUNCATE operation support
- ✅ PostgreSQL to destination type conversion
- ✅ WHERE clause generation for UPDATE/DELETE operations
- ✅ Null value handling and data validation
- ✅ Connection pooling and error recovery

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

All configuration uses environment variables or the `ConfigBuilder` pattern:

```rust
// Environment variables (used in Docker setup)
std::env::set_var("CDC_SOURCE_HOST", "postgres");
std::env::set_var("CDC_SOURCE_PORT", "5432");
std::env::set_var("CDC_SOURCE_DB", "postgres");
std::env::set_var("CDC_SOURCE_USER", "postgres");
std::env::set_var("CDC_SOURCE_PASSWORD", "test.123");

std::env::set_var("CDC_DEST_TYPE", "MySQL");
std::env::set_var("CDC_DEST_HOST", "mysql");
std::env::set_var("CDC_DEST_PORT", "3306");
std::env::set_var("CDC_DEST_DB", "cdc_db");
std::env::set_var("CDC_DEST_USER", "cdc_user");
std::env::set_var("CDC_DEST_PASSWORD", "test.123");

// Or using the builder pattern
let config = Config::builder()
    .source_connection_string("postgresql://postgres:test.123@localhost:7777/postgres")
    .destination_type(DestinationType::MySQL)
    .destination_connection_string("mysql://cdc_user:test.123@localhost:3306/cdc_db")
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

## Development Status

This project provides **working PostgreSQL to Any database replication** with comprehensive functionality:

### ✅ Completed Implementation
- **Core CDC Pipeline**: Complete end-to-end replication from PostgreSQL to destination databases
- **PostgreSQL Protocol**: Full logical replication protocol implementation with message parsing
- **WAL Processing**: Complete Write-Ahead Log record parsing and interpretation
- **Transaction Processing**: BEGIN/COMMIT transaction boundary handling with consistency
- **Change Event Processing**: Real-time INSERT, UPDATE, DELETE, TRUNCATE operations
- **Binary Protocol**: Efficient binary message format support with buffer operations
- **LSN Management**: Log Sequence Number tracking and feedback mechanisms implemented
- **Error Handling**: Production-ready error handling with proper error propagation
- **Destination Adapters**: Working MySQL and SQL Server destination implementations
- **Configuration**: Environment-based configuration with validation and defaults
- **Docker Environment**: Working multi-database development environment
- **Async Architecture**: Full async/await support with graceful shutdown via CancellationToken

### 🚧 Enhancement Opportunities
- **Production Hardening**: Enhanced error recovery and automatic reconnection strategies
- **Performance Optimization**: High-throughput optimizations and comprehensive benchmarking
- **Advanced Monitoring**: Production metrics collection, dashboards, and alerting
- **Schema Evolution**: DDL change handling and schema migration support  
- **Multi-Database Support**: Additional destination databases (Oracle, SQLite, ClickHouse)
- **Advanced Features**: Table filtering, data transformations, and custom routing

## Quick Start with Docker

The easiest way to get started is using the provided Docker setup:

```bash
# Clone and navigate to the project
cd cdc_rs

# Start the multi-database development environment
make docker-build
make docker-start

# Check service status
make docker-status

# View application logs
make docker-logs

# Connect to databases for testing
make psql      # PostgreSQL source
make mysql     # MySQL destination

# Insert test data and watch CDC processing
make test-data
make show-data

set -a; source .env; set +a
```

## Local Development

For local development without Docker:

```bash
# Build the project
make build

# Run code quality checks
make check

# Run tests  
make test

# Format code
make format

# Run the application locally (requires databases)
make run
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

**Note**: This shows the complete working application with real PostgreSQL logical replication message processing, LSN tracking, and transaction handling.

## Dependencies

- `tokio`: Async runtime and ecosystem
- `tokio-postgres`: PostgreSQL async client with logical replication support
- `sqlx`: Multi-database async client (MySQL)
- `tiberius`: Native SQL Server async client
- `serde` & `serde_json`: Serialization framework
- `chrono`: Date and time handling with timezone support
- `tracing` & `tracing-subscriber`: Structured logging and observability
- `thiserror`: Ergonomic error handling and propagation
- `async-trait`: Async trait definitions
- `bytes`: Byte buffer manipulation
- `libpq-sys`: Low-level PostgreSQL C library bindings for replication

## Test Coverage

Key areas covered by tests:
- PostgreSQL logical replication protocol message parsing
- Buffer operations for binary protocol handling
- LSN (Log Sequence Number) operations and formatting
- Change event creation and processing
- Destination database handlers (MySQL, SQL Server)
- Configuration management and validation
- Error handling and recovery scenarios
- Graceful shutdown and cancellation handling

## Contributing

This project provides **working PostgreSQL to Any database replication** with a solid foundation for contributions. The core CDC functionality is implemented and tested, making it easy for contributors to focus on specific enhancements:

### 🎯 High Impact Areas
1. **Production Hardening**: Enhance error recovery, reconnection strategies, and resilience patterns
2. **Performance Optimization**: Implement high-throughput optimizations and comprehensive benchmarking
3. **Advanced Monitoring**: Add production metrics, dashboards, and observability features
4. **Additional Destinations**: Extend support to more databases (Oracle, SQLite, ClickHouse, etc.)
5. **Schema Evolution**: Implement DDL change handling and schema migration capabilities
6. **Advanced Features**: Add table filtering, data transformations, and routing capabilities

### 🏗️ Architecture Benefits for Contributors
- **Working Foundation**: Core CDC pipeline is functional with comprehensive test coverage
- **Modular Design**: Clear separation of concerns makes extending functionality straightforward
- **Type Safety**: Rust's type system prevents common replication errors and ensures reliability
- **Async Architecture**: Built for high-performance concurrent processing with Tokio
- **Documentation**: Well-documented APIs and architecture make contribution easier
- **Development Environment**: Complete Docker setup for immediate local development and testing

### 🚀 Getting Started Contributing

```bash
# Set up development environment
git clone https://github.com/isdaniel/pg2any
cd cdc_rs
make dev-setup      # Runs checks, formatting, tests, and builds Docker environment

# Start development databases
make docker-start

# Make changes and test
make check          # Run code quality checks
make test           # Run tests
make build          # Build project
cargo run           # Test locally

# Database access for testing
make psql           # Connect to PostgreSQL
make mysql          # Connect to MySQL
```

### 📚 Implementation Resources

For extending functionality, refer to:
- [PostgreSQL Logical Replication Protocol Documentation](https://www.postgresql.org/docs/current/protocol-logical-replication.html)
- [PostgreSQL WAL Internals](https://www.postgresql.org/docs/current/wal-internals.html)
- [Logical Decoding Output Plugin](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html)

### 🧪 Testing Your Changes

```bash
# Run the comprehensive test suite
cargo test --all              # All 57 tests should pass

# Test specific areas  
cargo test buffer             # Buffer operations tests
cargo test integration        # Integration tests
cargo test destinations       # Database destination tests

# Test with real databases
make docker-start            # Start PostgreSQL and MySQL
cargo run                    # Test end-to-end functionality
```

## License

MIT OR Apache-2.0

## References

- [PostgreSQL Logical Replication Protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html)
- [PostgreSQL Output Plugin](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html)
- [WAL Format](https://www.postgresql.org/docs/current/wal-internals.html)
