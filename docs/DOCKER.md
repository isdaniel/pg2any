# PostgreSQL to Any Database (pg2any) Docker Setup

This directory contains a comprehensive Docker containerization setup for the PostgreSQL to Any database replication tool (pg2any) with complete monitoring infrastructure. The setup includes PostgreSQL as the source database, MySQL as the destination database, and a full observability stack with Prometheus, and multiple exporters for production monitoring.

## Current Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │   CDC App       │    │     MySQL       │
│   (Source)      │───▶│  + Monitoring   │───▶│ (Destination)   │
│                 │    │                 │    │                 │
│ • Port: 5432    │    │ • Rust/Cargo    │    │ • Port: 3306    │
│ • Logical Rep   │    │ • Metrics :8080 │    │ • Target DB     │
│ • WAL Streaming │    │ • Prometheus    │    │ • CDC Tables    │
│ • Publications  │    │ • Exporters     │    │ • Statistics    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                    ┌─────────────────┐
                    │  Monitoring     │
                    │  Infrastructure │
                    │                 │
                    │ • Prometheus    │
                    │ • Node Exporter │
                    │ • DB Exporters  │
                    │ • Alert Rules   │
                    └─────────────────┘
```

**Note**: The setup includes comprehensive monitoring with Prometheus and multiple exporters for production-ready observability.

**Status**: Production-ready CDC implementation with enterprise monitoring and observability stack.

## Quick Start

### Prerequisites
- Docker 20.10+
- Docker Compose 2.0+
- Make (optional, for convenience commands)

### 1. Clone and Setup
```bash
cd pg2any
set -a; source .env; set +a
# Edit .env if needed
```

### 2. Start the Complete Environment
```bash
# Start complete stack (databases + monitoring + application)
docker-compose up -d

# Or start databases and monitoring (recommended for local development)
docker-compose up -d postgres mysql prometheus

# Check service status
docker-compose ps
```

### 3. Build and Run the Application
```bash
# Local development (recommended)
make build
cargo run

# Access monitoring dashboards
open http://localhost:9090    # Prometheus 
open http://localhost:8080/metrics  # Application metrics endpoint

# Or build and run in Docker
docker-compose up --build -d cdc_app
```

## Services

### PostgreSQL (Source Database)
- **Port**: 5432
- **Database**: postgres
- **User**: postgres / test.123
- **Features**:
  - Logical replication enabled (`wal_level=logical`)
  - Max replication slots: 10
  - Max WAL senders: 10
  - Sample data initialization script included
  - Health checks enabled

### MySQL (Destination Database)  
- **Port**: 3306
- **Database**: cdc_db
- **User**: cdc_user / test.123 (root: test.123)
- **Features**:
  - Ready for CDC table creation
  - Health checks enabled
  - Initialization script included

### pg2any Application
- **Status**: Production-ready CDC replication
- **Runtime**: Rust with Tokio async runtime
- **Metrics Endpoint**: http://localhost:8080/metrics
- **Health Check**: http://localhost:8080/health
- **Features**:
  - Real-time PostgreSQL logical replication
  - Comprehensive metrics collection
  - Environment-based configuration
  - Graceful shutdown and LSN persistence
  - Built-in HTTP metrics server

### Monitoring Stack
- **Prometheus**: http://localhost:9090
  - Metrics collection and storage
  - Alert rule evaluation
  - Target health monitoring
- **Node Exporter**: Port 9100 (system metrics)
- **PostgreSQL Exporter**: Port 9187 (database metrics)
- **MySQL Exporter**: Port 9104 (destination database metrics)

### Admin Interfaces (Optional)
- **pgAdmin**: Can be added to docker-compose.yml if needed
- **phpMyAdmin**: Can be added to docker-compose.yml if needed
- **Direct Access**: Use `make psql` and `make mysql` commands

## Monitoring & Observability

### Metrics Collection
The application provides comprehensive metrics through multiple channels:

**Application Metrics (http://localhost:8080/metrics)**:
```prometheus
# Core CDC Metrics
pg2any_events_processed_total          # Total events processed
pg2any_events_by_type_total           # Events by type (insert/update/delete)
pg2any_replication_lag_seconds        # Current replication lag
pg2any_events_per_second              # Processing rate

# Health Metrics
pg2any_source_connection_status       # PostgreSQL connection (1=up, 0=down)
pg2any_destination_connection_status  # Destination connection status
pg2any_errors_total                   # Error count by type

# Performance Metrics  
pg2any_event_processing_duration_seconds  # Processing time distribution
pg2any_queue_depth                    # Events waiting to be processed
pg2any_buffer_memory_usage_bytes      # Memory usage
```

**Database Metrics**:
- PostgreSQL metrics via postgres-exporter (port 9187)
- MySQL metrics via mysql-exporter (port 9104) 
- System metrics via node-exporter (port 9100)

Once added, access http://localhost:3000 (admin/admin) for:
- **CDC Overview**: Replication status, lag, event rates
- **Error Analysis**: Error trends, types, and resolution  
- **Database Health**: Connection status, query performance
- **Resource Usage**: CPU, memory, network utilization
- **Alert Status**: Active alerts and system health

### Alert Rules
Predefined alerts in `monitoring/prometheus-rules/cdc-alerts.yml`:
- High replication lag (>30s warning, >120s critical)
- Connection failures (source/destination)
- High error rates (>0.1 errors/second)
- Low processing rates (<0.01 events/second)
- Queue depth issues and memory usage

## Current Implementation Status

This Docker setup provides a **production-ready CDC replication system** with comprehensive monitoring:

### Production-Ready Components
- **Complete CDC Pipeline**: PostgreSQL logical replication with real change streaming
- **Multi-Database Environment**: PostgreSQL source + MySQL destination with health checks
- **Monitoring Infrastructure**: Prometheus and multiple exporters for observability
- **Metrics Collection**: 20+ metrics covering performance, errors, and health
- **Alert System**: Predefined rules for lag, errors, and connection issues
- **Health Monitoring**: HTTP endpoints for application and database health
- **Network Configuration**: Docker network with proper service discovery
- **Initialization Scripts**: Complete database setup with sample data
- **Container Orchestration**: Production-ready Docker compose with dependencies
- **Build System**: Multi-stage Docker builds with optimization

### Monitoring & Observability
- **Prometheus Integration**: Complete metrics collection and alerting
- **Performance Tracking**: Event processing rates, lag monitoring, error analysis
- **Resource Monitoring**: Memory usage, network I/O, connection pooling
- **Database Metrics**: PostgreSQL and MySQL performance monitoring
- **System Metrics**: Host-level resource utilization

### Production Features
- **High Availability**: Health checks, restart policies, graceful shutdown
- **Scalability**: Configurable connection pools, queue management
- **Reliability**: Error handling, retry logic, LSN persistence
- **Security**: Database isolation, credential management
- **Performance**: Optimized Docker builds, resource allocation

## Configuration

Configuration uses environment variables, with defaults for the Docker setup:

### Source Database (PostgreSQL)
```bash
CDC_SOURCE_HOST=postgres         # Docker service name
CDC_SOURCE_PORT=5432            # Internal container port  
CDC_SOURCE_DB=postgres          # Database name
CDC_SOURCE_USER=postgres        # Username
CDC_SOURCE_PASSWORD=test.123    # Password (from docker-compose)
```

### Destination Database (MySQL)
```bash
CDC_DEST_TYPE=MySQL
CDC_DEST_HOST=mysql             # Docker service name
CDC_DEST_PORT=3306              # Internal container port
CDC_DEST_DB=cdc_db              # Target database name
CDC_DEST_USER=cdc_user          # MySQL user for CDC
CDC_DEST_PASSWORD=test.123      # Password (from docker-compose)
```

### CDC Settings
```bash
CDC_REPLICATION_SLOT=cdc_slot           # PostgreSQL replication slot name
CDC_PUBLICATION=cdc_pub                 # PostgreSQL publication name  
CDC_PROTOCOL_VERSION=1                  # Logical replication protocol version
CDC_CONNECTION_TIMEOUT=30               # Connection timeout in seconds
CDC_QUERY_TIMEOUT=10                   # Query timeout in seconds  
CDC_HEARTBEAT_INTERVAL=10              # Heartbeat interval in seconds
```

### Application Settings
```bash
RUST_LOG=debug                          # Rust logging level
RUST_BACKTRACE=1                        # Enable backtraces for debugging
METRICS_PORT=8080                       # HTTP metrics server port
```

## Development Commands

### Using Make (Recommended)
```bash
# Development workflow
make build              # Build Rust application
make run               # Run application locally
make test              # Run tests
make check             # Code quality checks (clippy, format)
make format            # Format code with rustfmt

# Docker database management
make docker-build      # Build Docker services
make docker-start      # Start Docker databases
make docker-stop       # Stop Docker services
make docker-restart    # Restart Docker services
make docker-status     # Check service status
make docker-logs       # View all service logs
make docker-clean      # Clean Docker resources

# Database connections
make psql              # Connect to PostgreSQL
make mysql             # Connect to MySQL
make test-data         # Insert test data
make show-data         # Show data in both databases
```


### Direct Docker Compose  
```bash
# Start databases only
docker-compose up -d postgres mysql

# View logs
docker-compose logs -f postgres
docker-compose logs -f mysql

# Connect to databases
docker-compose exec postgres psql -U postgres -d postgres
docker-compose exec mysql mysql -u cdc_user -ptest.123 cdc_db

# Clean up
docker-compose down -v  # Remove containers and volumes
```

## Testing the Development Environment

### 1. Start the Environment
```bash
# Start databases
make docker-start
# or: docker-compose up -d postgres mysql

# Check that all services are running
make docker-status

# Access monitoring dashboards
open http://localhost:9090    # Prometheus
```

### 2. Run the Application Locally
```bash
# Set up environment variables (optional - defaults will work)
export CDC_SOURCE_HOST=localhost
export CDC_SOURCE_PORT=5432  # PostgreSQL on standard port
export CDC_SOURCE_DB=postgres
export CDC_SOURCE_USER=postgres
export CDC_SOURCE_PASSWORD=test.123

export CDC_DEST_TYPE=MySQL
export CDC_DEST_HOST=localhost
export CDC_DEST_PORT=3306
export CDC_DEST_DB=cdc_db  
export CDC_DEST_USER=cdc_user
export CDC_DEST_PASSWORD=test.123

# Build and run
make build
make run

# Monitor application metrics
curl http://localhost:8080/metrics
```

### 3. Test Database Connectivity
```bash
# Connect to PostgreSQL (port 5432)
make psql
# or: docker-compose exec postgres psql -U postgres -d postgres

# Connect to MySQL
make mysql
# or: docker-compose exec mysql mysql -u cdc_user -ptest.123 cdc_db

# Check PostgreSQL logical replication setup
SELECT * FROM pg_replication_slots;
SELECT * FROM pg_publication;

# View application metrics
curl http://localhost:8080/metrics | grep pg2any
```

### 4. Development Workflow
```bash
# Make code changes, then:
make check         # Verify code quality
make test          # Run tests
make build         # Rebuild
cargo run          # Test changes

# View application logs
# (Application runs locally, database logs in Docker)
make docker-logs   # Database logs only
```

## Monitoring and Debugging

### View Logs
```bash
# Application logs (when running locally)
# Displayed directly in terminal from `cargo run`

# Container logs  
make docker-logs   # All services
docker-compose logs -f postgres
docker-compose logs -f mysql
docker-compose logs -f prometheus
# Application logs (when running in Docker)
docker-compose logs -f cdc_app
```

### Check Service Health
```bash
make docker-status
docker-compose ps
docker-compose exec postgres pg_isready -U postgres  
docker-compose exec mysql mysqladmin ping -u cdc_user -ptest.123
```

### Database Development Status
```bash
# PostgreSQL setup verification
make psql
\l                                    -- List databases
\dt                                   -- List tables
SELECT * FROM pg_replication_slots;   -- Check replication slots
SELECT * FROM pg_publication;         -- Check publications

# MySQL setup verification  
make mysql
SHOW DATABASES;                       -- List databases
USE cdc_db;                          -- Switch to CDC database
SHOW TABLES;                         -- List tables
```

### Performance Monitoring
```bash
# Resource usage
docker stats

# Container resource monitoring
make docker-stats

# Application performance metrics
curl http://localhost:8080/metrics | grep -E "(processing_duration|queue_depth|events_per_second)"

# Database-specific monitoring
docker-compose exec postgres psql -U postgres -c "SELECT * FROM pg_stat_activity;"
docker-compose exec mysql mysql -u root -ptest.123 -e "SHOW PROCESSLIST;"

# Prometheus query interface  
open http://localhost:9090/graph       # Custom metric queries
```
docker-compose logs -f postgres
docker-compose logs -f mysql

# Application logs (when running locally)
# Will be displayed directly in terminal from `cargo run`
```

### Check Service Health
```bash
make docker-status
docker-compose ps
docker-compose exec postgres pg_isready -U postgres  
docker-compose exec mysql mysqladmin ping -u cdc_user -ptest.123
```

### Database Development Status
```bash
# PostgreSQL setup verification
make psql
\l                                    -- List databases
\dt                                   -- List tables
SELECT * FROM pg_replication_slots;   -- Check replication slots
SELECT * FROM pg_publication;         -- Check publications

# MySQL setup verification  
make mysql
SHOW DATABASES;                       -- List databases
USE cdc_db;                          -- Switch to CDC database
SHOW TABLES;                         -- List tables
```
docker-compose logs -f mysql
```

### Check Service Health
```bash
make docker-status
docker-compose ps
```

### Database Status
```bash
# PostgreSQL replication status
make psql
SELECT * FROM pg_replication_slots;
SELECT * FROM pg_publication_tables;

# MySQL CDC statistics
make mysql  
SELECT * FROM cdc_statistics;
```

### Performance Monitoring
```bash
# Resource usage
docker stats

# Container logs with timestamps
docker-compose logs -t pg2any_app
```

## Troubleshooting

### Common Issues

**1. Application Connection Issues**
```bash
# Check database connectivity from host
telnet localhost 5432  # PostgreSQL
telnet localhost 3306  # MySQL

# Check Docker network
docker network ls
docker network inspect cdc_rs_cdc_network

# Verify environment variables
env | grep CDC_

# Check application health
curl http://localhost:8080/health
curl http://localhost:8080/metrics | head -20
```

**2. PostgreSQL Not Ready for Replication**
```bash
# Check PostgreSQL configuration
make psql
SHOW wal_level;                    -- Should be 'logical'
SHOW max_replication_slots;        -- Should be > 0
SHOW max_wal_senders;             -- Should be > 0

# Check if extension is available
SELECT * FROM pg_available_extensions WHERE name = 'uuid-ossp';
```

**3. MySQL Connection Issues**
```bash
# Test MySQL connectivity
docker-compose exec mysql mysql -u cdc_user -ptest.123 -e "SELECT 1;"

# Check MySQL user privileges
docker-compose exec mysql mysql -u root -ptest.123 -e "SELECT User, Host, Grant_priv FROM mysql.user WHERE User='cdc_user';"
```

**4. Build or Runtime Issues**
```bash
# Clean and rebuild
make clean
cargo clean
make build

# Check Rust environment
rustc --version
cargo --version

# Check for port conflicts
netstat -ln | grep -E ':(5432|3306|8080|9090|3000)'
```

## File Structure

```
cdc_rs/
├── docker-compose.yml              # Multi-database development environment
├── Dockerfile                      # Application containerization (ready for deployment)  
├── Makefile                       # Development commands and shortcuts
├── Cargo.toml                     # Workspace configuration
├── src/main.rs                    # Application entry point
├── pg2any-lib/                    # Core CDC library
│   ├── Cargo.toml                # Library dependencies and features
│   ├── src/                      # Library implementation
│   └── tests/                    # Integration tests
├── scripts/
│   ├── init_postgres.sql         # PostgreSQL initialization and sample data
│   └── init_mysql.sql            # MySQL initialization and schema setup
└── docs/
    └── DOCKER.md                 # This documentation file
```

### Key Configuration Files

**docker-compose.yml**: Defines PostgreSQL (port 5432) and MySQL (port 3306) services with complete monitoring stack
**Dockerfile**: Multi-stage Rust build for containerized deployment
**Cargo.toml**: Workspace configuration with pg2any-lib dependency  
**init_postgres.sql**: Sample tables (users, orders, order_items) with UUID support
**init_mysql.sql**: Destination database setup with CDC metadata tables

## Security Considerations

### Development Environment
- **Passwords**: Uses simple passwords (`test.123`) for development convenience
- **Network**: Uses Docker internal networking with minimal external exposure  
- **Ports**: PostgreSQL exposed on 5432, MySQL on 3306 for development access
- **Users**: Dedicated MySQL user (`cdc_user`) with limited privileges

### Production Recommendations
- **Strong Passwords**: Use secure, randomly generated passwords
- **SSL/TLS**: Enable encrypted connections for all database communications
- **Network Security**: Use VPNs, firewalls, and restrict database access
- **Docker Secrets**: Use Docker secrets or external secret management
- **User Permissions**: Follow principle of least privilege for database users
- **Monitoring**: Implement comprehensive security monitoring and alerting

## Production Deployment Considerations

### Container Deployment  
```bash
# Build for production
docker build -t pg2any:latest .

# Run with production environment
docker run -d \
  --name pg2any \
  --env-file .env.production \
  --restart unless-stopped \
  pg2any:latest
```

### Production Checklist
1. **Security**: 
   - Change all default passwords
   - Enable PostgreSQL and MySQL SSL/TLS
   - Use Docker secrets or external secret management
   - Configure firewall rules and network security

2. **High Availability**:
   - PostgreSQL primary/replica setup with logical replication
   - MySQL clustering or replication for destination
   - Load balancing and failover strategies
   - Container orchestration (Docker Swarm/Kubernetes)

3. **Monitoring & Observability**:
   - Application metrics and health checks
   - Database performance monitoring  
   - Alerting for failures and performance issues

4. **Data Management**:
   - Database backup and recovery procedures
   - Volume persistence and backup strategies
   - Data retention and archival policies
   - Disaster recovery planning

5. **Performance**:
   - Database configuration tuning
   - Connection pooling optimization  
   - Resource allocation (CPU, memory, storage)
   - Network optimization for high throughput

## Environment Variables Reference

### PostgreSQL Source Database
| Variable | Default | Description |
|----------|---------|-------------|
| `CDC_SOURCE_HOST` | `postgres` | PostgreSQL hostname (Docker service name) |
| `CDC_SOURCE_PORT` | `5432` | PostgreSQL port (internal container port) |  
| `CDC_SOURCE_DB` | `postgres` | Source database name |
| `CDC_SOURCE_USER` | `postgres` | PostgreSQL username |
| `CDC_SOURCE_PASSWORD` | `test.123` | PostgreSQL password |

### MySQL Destination Database  
| Variable | Default | Description |
|----------|---------|-------------|
| `CDC_DEST_TYPE` | `MySQL` | Destination database type |
| `CDC_DEST_HOST` | `mysql` | MySQL hostname (Docker service name) |
| `CDC_DEST_PORT` | `3306` | MySQL port |
| `CDC_DEST_DB` | `cdc_db` | Destination database name |
| `CDC_DEST_USER` | `cdc_user` | MySQL username for CDC operations |
| `CDC_DEST_PASSWORD` | `test.123` | MySQL password |

### CDC Configuration
| Variable | Default | Description |
|----------|---------|-------------|
| `CDC_REPLICATION_SLOT` | `cdc_slot` | PostgreSQL replication slot name |
| `CDC_PUBLICATION` | `cdc_pub` | PostgreSQL publication name |
| `CDC_PROTOCOL_VERSION` | `1` | Logical replication protocol version |
| `CDC_CONNECTION_TIMEOUT` | `30` | Database connection timeout (seconds) |
| `CDC_QUERY_TIMEOUT` | `10` | Database query timeout (seconds) |
| `CDC_HEARTBEAT_INTERVAL` | `10` | Replication heartbeat interval (seconds) |

### Application Settings
| Variable | Default | Description |
|----------|---------|-------------|
| `RUST_LOG` | `info` | Rust logging level (trace, debug, info, warn, error) |
| `RUST_BACKTRACE` | `0` | Enable Rust backtraces (0=off, 1=on, full=detailed) |

### Docker Port Mapping
- **PostgreSQL**: Host port `5432` → Container port `5432`
- **MySQL**: Host port `3306` → Container port `3306`
- **Application Metrics**: Host port `8080` → Container port `8080`
- **Prometheus**: Host port `9090` → Container port `9090`
- **Node Exporter**: Host port `9100` → Container port `9100`
- **PostgreSQL Exporter**: Host port `9187` → Container port `9187`
- **MySQL Exporter**: Host port `9104` → Container port `9104`

## Next Steps for Development

### 1. Core Implementation
- **PostgreSQL Protocol**: Implement logical replication message parsing in `pg2any-lib/src/replication.rs`
- **WAL Processing**: Add WAL record interpretation and change event generation  
- **Transaction Handling**: Implement transaction boundary detection and consistency

### 2. Testing and Validation
```bash
# Set up development environment
make docker-start
make build

# Test database connectivity
make psql
make mysql

# Implement and test core features
cargo test
make check
```

### 3. Feature Extensions
- **Additional Destinations**: Add PostgreSQL-to-PostgreSQL, Oracle, etc.
- **Schema Evolution**: Handle DDL changes and schema migration
- **Filtering**: Implement table/column filtering and transformation rules
- **Monitoring**: Add metrics collection and health endpoints

### 4. Production Readiness  
- **Performance Testing**: Benchmark with high-volume data changes
- **Error Recovery**: Implement robust reconnection and state recovery
- **Deployment**: Create Kubernetes manifests and CI/CD pipelines
- **Documentation**: Add operational guides and troubleshooting playbooks

---

For more implementation details, see the main [README.md](../README.md) file.
