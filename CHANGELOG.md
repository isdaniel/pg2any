# Changelog

All notable changes to this project will be documented in this file.

## [0.1.1] - 2025-08-26

### Updated
- **README.md**: Comprehensive update to accurately reflect current codebase
- **Documentation**: Enhanced project structure documentation with accurate file counts
- **Test Coverage**: Updated test statistics to reflect 104+ tests across 8 test files
- **Architecture**: Improved architecture documentation with detailed component breakdown
- **Dependencies**: Updated dependency list with accurate versions and descriptions
- **Contributing**: Enhanced contributing guidelines with better onboarding process
- **Make Commands**: Complete documentation of all available Makefile commands
- **Project Statistics**: Added accurate codebase metrics (67 Rust files, 113K+ lines of code)

### Fixed
- **Directory References**: Corrected repository directory name from `cdc_rs` to `pg2any`
- **Test Count**: Updated from 80+ to accurate 104+ test count
- **Feature Documentation**: Aligned feature descriptions with actual implementation
- **Development Setup**: Fixed Docker and local development instructions

### Enhanced
- **Client Implementation**: Improved error handling and logging in CDC client
- **Production Readiness**: Emphasized mature, production-ready status
- **Code Quality**: Better documentation of test coverage and quality metrics
- **User Experience**: Clearer quick start guide and development workflow

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
