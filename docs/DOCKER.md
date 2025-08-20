# 🐳 PostgreSQL to Any Database (pg2any) Docker Setup

This directory contains a complete Docker containerization setup for the PostgreSQL to Any database replication tool (pg2any). The setup includes PostgreSQL as the source database, MySQL as the destination database, and provides a complete development environment for implementing and testing CDC functionality.

## 🏗️ Current Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │   Development   │    │     MySQL       │
│   (Source)      │───▶│   Environment   │───▶│ (Destination)   │
│                 │    │                 │    │                 │
│ • Port: 5432    │    │ • Rust/Cargo    │    │ • Port: 3306    │
│ • Logical Rep   │    │ • pg2any App    │    │ • Target DB     │
│ • WAL Streaming │    │ • Hot Reload    │    │ • CDC Tables    │
│ • Publications  │    │ • Testing       │    │ • Statistics    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

**Note**: The pg2any application container is currently commented out in docker-compose.yml to allow for iterative development. You can build and run the application locally while using the containerized databases.

## 🚀 Quick Start

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

### 2. Start the Development Environment
```bash
# Start databases only (recommended for development)
docker-compose up -d postgres mysql

# Or start all services including optional admin tools
docker-compose up -d

# Check service status
docker-compose ps
```

### 3. Build and Run the Application
```bash
# Local development (recommended)
make build
cargo run

# Or build and run in Docker (uncomment cdc_app service first)
# docker-compose up --build -d cdc_app
```

## 📊 Services

### PostgreSQL (Source Database)
- **Port**: 7777 (mapped from container's 5432)
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
- **Status**: Framework ready for development
- **Runtime**: Rust with Tokio async runtime
- **Current State**: Complete architecture with placeholder implementations
- **Development**: Run locally with `cargo run` while using containerized databases
- **Features**:
  - Environment-based configuration
  - Comprehensive error handling and logging
  - Graceful shutdown handling
  - Ready for PostgreSQL protocol implementation

### Admin Interfaces (Optional)
```bash
# Start admin tools (pgAdmin, phpMyAdmin)
docker-compose up -d pgadmin phpmyadmin
```
- **pgAdmin**: http://localhost:8081 (if configured)
- **phpMyAdmin**: http://localhost:8082 (if configured)

**Note**: Admin interface services may need to be added to docker-compose.yml if desired.

## 📋 Current Implementation Status

This Docker setup provides a **complete development environment** for the pg2any framework:

### ✅ Working Components
- **Multi-Database Environment**: PostgreSQL source (port 7777) + MySQL destination (port 3306)  
- **Database Configuration**: Both databases properly configured for CDC with health checks
- **Network Setup**: Docker network allowing seamless communication between services
- **Initialization Scripts**: PostgreSQL and MySQL setup scripts with sample data
- **Development Workflow**: Local application development with containerized databases
- **Configuration Management**: Environment-based configuration ready for CDC settings
- **Build System**: Docker build configuration for application containerization
- **Development Tools**: Makefile integration for easy database management

### 🚧 Application Integration
- **pg2any Application**: Framework architecture complete, PostgreSQL protocol implementation in progress  
- **Database Connections**: Connection handling and configuration management implemented
- **Error Handling**: Comprehensive error management and logging infrastructure ready
- **Configuration Loading**: Environment variable configuration system fully functional
- **Docker Integration**: Application containerization ready (currently commented for development)

### 🎯 Development Ready
This setup provides everything needed to implement and test CDC functionality:
- Pre-configured PostgreSQL with logical replication enabled
- MySQL destination database ready for CDC tables  
- Sample schemas and data for testing
- Logging and monitoring infrastructure
- Hot-reload development workflow with `cargo run`

## 🎛️ Configuration

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
```

## 🔧 Development Commands

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

## 🧪 Testing the Development Environment

### 1. Start the Environment
```bash
# Start databases
make docker-start
# or: docker-compose up -d postgres mysql

# Check that databases are running
make docker-status
```

### 2. Run the Application Locally
```bash
# Set up environment variables (optional - defaults will work)
export CDC_SOURCE_HOST=localhost
export CDC_SOURCE_PORT=7777  # PostgreSQL mapped port
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
```

### 3. Test Database Connectivity
```bash
# Connect to PostgreSQL (port 7777)
make psql
# or: docker-compose exec postgres psql -U postgres -d postgres

# Connect to MySQL
make mysql
# or: docker-compose exec mysql mysql -u cdc_user -ptest.123 cdc_db

# Check PostgreSQL logical replication setup
SELECT * FROM pg_replication_slots;
SELECT * FROM pg_publication;
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

## 🔍 Monitoring and Debugging

### View Logs
```bash
# Database logs
make docker-logs
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

### Performance Monitoring
```bash
# Resource usage
docker stats postgres mysql

# Database-specific monitoring
docker-compose exec postgres psql -U postgres -c "SELECT * FROM pg_stat_activity;"
docker-compose exec mysql mysql -u root -ptest.123 -e "SHOW PROCESSLIST;"
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

## 🐛 Troubleshooting

### Common Issues

**1. Application Connection Issues**
```bash
# Check database connectivity from host
telnet localhost 7777  # PostgreSQL
telnet localhost 3306  # MySQL

# Check Docker network
docker network ls
docker network inspect cdc_rs_cdc_network

# Verify environment variables
env | grep CDC_
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
netstat -ln | grep -E ':(7777|3306)'
```

## 📁 File Structure

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

**docker-compose.yml**: Defines PostgreSQL (port 7777) and MySQL (port 3306) services
**Dockerfile**: Multi-stage Rust build for containerized deployment
**Cargo.toml**: Workspace configuration with pg2any-lib dependency  
**init_postgres.sql**: Sample tables (users, orders, order_items) with UUID support
**init_mysql.sql**: Destination database setup with CDC metadata tables

## 🔒 Security Considerations

### Development Environment
- **Passwords**: Uses simple passwords (`test.123`) for development convenience
- **Network**: Uses Docker internal networking with minimal external exposure  
- **Ports**: PostgreSQL exposed on 7777, MySQL on 3306 for development access
- **Users**: Dedicated MySQL user (`cdc_user`) with limited privileges

### Production Recommendations
- **Strong Passwords**: Use secure, randomly generated passwords
- **SSL/TLS**: Enable encrypted connections for all database communications
- **Network Security**: Use VPNs, firewalls, and restrict database access
- **Docker Secrets**: Use Docker secrets or external secret management
- **User Permissions**: Follow principle of least privilege for database users
- **Monitoring**: Implement comprehensive security monitoring and alerting

## 🚀 Production Deployment Considerations

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
   - Log aggregation and analysis (ELK/Grafana)
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

## 📝 Environment Variables Reference

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
- **PostgreSQL**: Host port `7777` → Container port `5432`
- **MySQL**: Host port `3306` → Container port `3306`

## 🎯 Next Steps for Development

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
