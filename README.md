# PostgreSQL to Any Database Replication (pg2any)

A comprehensive PostgreSQL to Any database replication tool using Change Data Capture (CDC) with logical replication. This tool allows you to stream database changes in real-time from PostgreSQL to other databases such as SQL Server, MySQL, and more.

## ⚠️ Project Status

This is a **framework implementation** that provides the complete architectural foundation for a PostgreSQL to Any database replication tool. It includes a working build system, comprehensive API design, Docker containerization, and structured placeholder implementations.

**Current Status**: This is a well-structured proof-of-concept that demonstrates the complete CDC architecture. The framework is ready for implementing the core PostgreSQL logical replication protocol.

### What's Implemented ✅
- ✅ Complete project structure with Rust workspace configuration
- ✅ Comprehensive library (`pg2any-lib`) with modular architecture
- ✅ Rust async architecture with Tokio runtime
- ✅ Configuration management with builder pattern and environment variable support
- ✅ Comprehensive error handling framework with typed errors (`thiserror`)
- ✅ Database destination framework for multiple targets (MySQL, SQL Server)
- ✅ PostgreSQL connection and basic logical replication setup
- ✅ Message parsing framework structure for WAL processing
- ✅ Docker containerization with multi-database development environment
- ✅ Complete build system with Cargo workspace and Docker builds
- ✅ Development tooling (Makefile, formatting, testing, linting)
- ✅ Comprehensive documentation and code examples
- ✅ Integration test framework structure

### What Needs Implementation 🚧
- 🚧 PostgreSQL logical replication protocol message parsing
- 🚧 WAL (Write-Ahead Log) record interpretation and processing
- 🚧 Binary protocol message handling for performance
- 🚧 LSN (Log Sequence Number) tracking and feedback mechanisms
- 🚧 Robust error recovery and reconnection strategies
- 🚧 Transaction boundary handling and consistency guarantees
- 🚧 Performance optimizations for high-throughput scenarios
- 🚧 Production monitoring, metrics, and observability

## Features

- ✅ **Architecture**: Complete modular library structure with `pg2any-lib` core
- ✅ **Configuration**: Environment-based configuration with builder pattern
- ✅ **Async Runtime**: Full async/await support with Tokio
- ✅ **Error Handling**: Comprehensive error types with `thiserror`
- ✅ **Destinations**: Framework for multiple database targets (MySQL, SQL Server)
- ✅ **Docker Support**: Complete containerized development environment
- ✅ **Development Tools**: Makefile, formatting, testing, and linting setup
- 🚧 **Replication Protocol**: PostgreSQL logical replication protocol (framework ready)
- 🚧 **WAL Processing**: Write-Ahead Log processing and interpretation
- 🚧 **Real-time Streaming**: Live change streaming (INSERT, UPDATE, DELETE, TRUNCATE)

## Quick Start

Add this to your `Cargo.toml`:

```toml
[dependencies]
pg2any = "0.1.0"
tokio = { version = "1.0", features = ["full"] }
anyhow = "1.0"
tracing-subscriber = "0.3"
```

### Basic Usage

```rust
use pg2any_lib::{client::CdcClient, Config, DestinationType};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
3. **ReplicationManager**: PostgreSQL logical replication lifecycle management (framework)
4. **MessageParser**: PostgreSQL replication protocol message parsing (framework)
5. **DestinationHandler**: Multi-database destination handling (MySQL, SQL Server)
6. **Error Types**: Comprehensive error handling with `CdcError` and proper error propagation

### Data Flow Architecture

```
PostgreSQL WAL → Logical Replication → Message Parser → Change Events → Destination Handler → Target DB
     ↓                    ↓                   ↓              ↓                    ↓              ↓
   Transactions      Protocol Messages    Parsed Events   Typed Changes    SQL Operations   Replicated Data
```

## Project Structure

This workspace uses Cargo's workspace feature for better organization:

```
cdc_rs/
├── Cargo.toml              # Workspace configuration
├── src/main.rs             # Application entry point  
├── pg2any-lib/             # Core CDC library
│   ├── Cargo.toml          # Library dependencies
│   ├── src/
│   │   ├── lib.rs          # Library public API
│   │   ├── config.rs       # Configuration management
│   │   ├── client.rs       # Main CDC client
│   │   ├── error.rs        # Error types and handling
│   │   ├── destinations.rs # Database destination handlers
│   │   ├── connection.rs   # PostgreSQL connections
│   │   ├── replication.rs  # Replication management
│   │   └── ...             # Additional modules
│   └── tests/              # Integration tests
├── docker-compose.yml      # Multi-database development setup
├── Dockerfile             # Application containerization
├── Makefile               # Development commands
└── scripts/               # Database initialization scripts
    ├── init_postgres.sql
    └── init_mysql.sql
```

## Supported Destination Databases

- **MySQL**: Framework support with type mapping and table creation structure
- **SQL Server**: Framework support with type mapping and table creation structure  
- **Extensible**: Architecture designed for easy addition of new destination types

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

This project provides a **complete architectural framework** for PostgreSQL to Any database replication:

### ✅ Completed Framework Components
- **Architecture**: Complete library structure and API design with Rust workspace
- **Configuration**: Comprehensive configuration management with environment variable support
- **Error Handling**: Robust error handling framework with `thiserror` integration
- **Database Adapters**: Destination database adapter framework for MySQL and SQL Server
- **Message Framework**: PostgreSQL message parsing and processing structure ready for implementation
- **Async Runtime**: Full async/await architecture with Tokio integration
- **Documentation**: Comprehensive documentation with practical examples
- **Testing**: Unit test and integration test framework structure
- **Docker Environment**: Complete multi-database development environment with PostgreSQL and MySQL
- **Development Tools**: Makefile, cargo configuration, formatting, and linting setup

### 🚧 Core Implementation Ready for Development
- **Replication Protocol**: PostgreSQL logical replication protocol handler (framework in place)
- **WAL Processing**: Write-Ahead Log record parsing and interpretation (structured for implementation)
- **Binary Protocol**: High-performance binary message format support
- **LSN Management**: Log Sequence Number tracking and feedback mechanisms
- **Error Recovery**: Production-ready error recovery and reconnection strategies
- **Performance**: High-throughput optimizations and benchmarking
- **Monitoring**: Production monitoring, metrics collection, and observability

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
2025-08-11T10:30:00.123Z INFO  pg2any: 🚀 Starting PostgreSQL CDC Application
2025-08-11T10:30:00.124Z INFO  pg2any: 📋 Loading configuration from environment variables
2025-08-11T10:30:00.125Z INFO  pg2any: 🔗 Configuration loaded successfully
2025-08-11T10:30:00.126Z INFO  pg2any: ⚙️  Initializing CDC client
2025-08-11T10:30:00.127Z INFO  pg2any: 🔧 Performing CDC client initialization
2025-08-11T10:30:00.128Z INFO  pg2any: ✅ CDC client initialized successfully
2025-08-11T10:30:00.129Z INFO  pg2any: 🔄 Starting CDC replication pipeline
2025-08-11T10:30:00.130Z INFO  pg2any: ✨ CDC replication started! (Framework ready for implementation)
```

**Note**: This demonstrates the complete application structure, configuration loading, and initialization flow. The replication pipeline is architecturally complete and ready for PostgreSQL protocol implementation.

## Dependencies

- `tokio`: Async runtime and ecosystem
- `tokio-postgres`: PostgreSQL async client with logical replication support
- `sqlx`: Multi-database async client (MySQL, SQL Server)
- `tiberius`: Native SQL Server async client
- `serde` & `serde_json`: Serialization framework
- `chrono`: Date and time handling with timezone support
- `uuid`: UUID generation and handling
- `tracing` & `tracing-subscriber`: Structured logging and observability
- `thiserror`: Ergonomic error handling and propagation
- `anyhow`: Flexible application-level error handling
- `async-trait`: Async trait definitions
- `bytes`: Byte buffer manipulation
- `futures-util`: Future utilities and combinators

## Contributing

This project provides a **complete architectural foundation** for PostgreSQL to Any database replication. Contributors can focus on specific areas:

### 🎯 High Impact Areas
1. **PostgreSQL Protocol Implementation**: Implement the logical replication protocol in `pg2any-lib/src/replication.rs`
2. **WAL Processing**: Add WAL record parsing and interpretation in message parsing modules
3. **Performance Optimization**: Benchmark and optimize for high-throughput scenarios
4. **Additional Destinations**: Add support for more destination databases (Oracle, SQLite, etc.)
5. **Monitoring & Metrics**: Implement production monitoring capabilities and observability

### 🏗️ Architecture Benefits for Contributors
- **Modular Design**: Clear separation of concerns makes implementing specific components straightforward
- **Type Safety**: Rust's type system prevents common replication errors and ensures reliability
- **Async Architecture**: Built for high-performance concurrent processing with Tokio
- **Comprehensive Testing**: Framework supports thorough testing strategies and CI/CD
- **Documentation**: Well-documented APIs and architecture make contribution easier
- **Development Environment**: Complete Docker setup for immediate local development

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
For core implementation, refer to:
- [PostgreSQL Logical Replication Protocol Documentation](https://www.postgresql.org/docs/current/protocol-logical-replication.html)
- [PostgreSQL WAL Internals](https://www.postgresql.org/docs/current/wal-internals.html)
- [Logical Decoding Output Plugin](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html)

## License

MIT OR Apache-2.0

## References

- [PostgreSQL Logical Replication Protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html)
- [PostgreSQL Output Plugin](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html)
- [WAL Format](https://www.postgresql.org/docs/current/wal-internals.html)
