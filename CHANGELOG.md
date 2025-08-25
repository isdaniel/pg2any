# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - 2025-08-25

### Added
- Initial release of pg2any CDC tool
- PostgreSQL logical replication protocol implementation
- MySQL destination handler with comprehensive SQL generation
- SQL Server destination handler with TDS protocol support
- Real-time change streaming (INSERT, UPDATE, DELETE, TRUNCATE)
- Configuration management with environment variable support
- Comprehensive error handling with structured error types
- Async/await architecture with Tokio runtime
- Transaction boundary handling (BEGIN, COMMIT)
- LSN (Log Sequence Number) tracking and persistence
- WHERE clause generation with replica identity support
- Docker containerization with development environment
- Extensive test suite with 80+ tests covering edge cases
- CI/CD pipelines for quality assurance
- Production-ready logging and observability

### Features
- **Core Library**: `pg2any_lib` with modular architecture
- **CLI Tool**: `pg2any` binary for running CDC operations
- **Database Support**: PostgreSQL source, MySQL and SQL Server destinations
- **Feature Flags**: Optional dependencies for destination databases
- **Development Tools**: Makefile, formatting, testing, and linting

### Technical Highlights
- Complete PostgreSQL logical replication protocol implementation
- WAL (Write-Ahead Log) record interpretation and processing
- Binary protocol message handling with efficient buffer operations
- Graceful shutdown with proper resource cleanup
- Real-time change streaming with bug fixes for production use
