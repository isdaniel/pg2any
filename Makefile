# PostgreSQL CDC Makefile
# Provides convenient commands for development and deployment

.PHONY: help build start stop restart clean logs test check format docker-build docker-start docker-stop docker-clean \
	chaos-test-mysql-setup chaos-test-mysql chaos-test-mysql-clean chaos-test-mysql-full \
	pgbench-test-mysql-setup pgbench-test-mysql pgbench-test-mysql-clean pgbench-test-mysql-full \
	chaos-test-sqlserver-setup chaos-test-sqlserver chaos-test-sqlserver-clean chaos-test-sqlserver-full \
	pgbench-test-sqlserver-setup pgbench-test-sqlserver pgbench-test-sqlserver-clean pgbench-test-sqlserver-full \
	chaos-test-sqlite-setup chaos-test-sqlite chaos-test-sqlite-clean chaos-test-sqlite-full \
	pgbench-test-sqlite-setup pgbench-test-sqlite pgbench-test-sqlite-clean pgbench-test-sqlite-full

# Default target
help:
	@echo "PostgreSQL CDC Development Commands"
	@echo ""
	@echo "Development:"
	@echo "  build          Build the Rust application"
	@echo "  check          Check code with cargo check"
	@echo "  test           Run tests"
	@echo "  format         Format code with rustfmt"
	@echo "  run            Run the application locally"
	@echo ""
	@echo "Docker:"
	@echo "  docker-build   Build Docker services"
	@echo "  docker-start   Start Docker services"
	@echo "  docker-stop    Stop Docker services" 
	@echo "  docker-restart Restart Docker services"
	@echo "  docker-clean   Clean Docker resources"
	@echo "  docker-logs    Show Docker logs"
	@echo "  docker-status  Show Docker service status"
	@echo ""
	@echo "Database:"
	@echo "  psql           Connect to PostgreSQL"
	@echo "  mysql          Connect to MySQL"
	@echo "  test-data      Insert test data"
	@echo "  show-data      Show data in both databases"
	@echo ""
	@echo "MySQL Chaos Testing:"
	@echo "  chaos-test-mysql-setup   Set up and start services for MySQL chaos testing"
	@echo "  chaos-test-mysql         Run chaos integration tests against MySQL"
	@echo "  chaos-test-mysql-clean   Clean up after MySQL chaos testing"
	@echo "  chaos-test-mysql-full    Full MySQL chaos test cycle"
	@echo ""
	@echo "MySQL PGBench Testing:"
	@echo "  pgbench-test-mysql-setup Set up and start services for MySQL pgbench testing"
	@echo "  pgbench-test-mysql       Run pgbench chaos integration tests against MySQL"
	@echo "  pgbench-test-mysql-clean Clean up after MySQL pgbench testing"
	@echo "  pgbench-test-mysql-full  Full MySQL pgbench test cycle"
	@echo ""
	@echo "SQL Server Chaos Testing:"
	@echo "  chaos-test-sqlserver-setup   Set up SQL Server chaos testing environment"
	@echo "  chaos-test-sqlserver         Run chaos tests against SQL Server"
	@echo "  chaos-test-sqlserver-clean   Clean up SQL Server chaos testing"
	@echo "  chaos-test-sqlserver-full    Full SQL Server chaos test cycle"
	@echo ""
	@echo "SQL Server PGBench Testing:"
	@echo "  pgbench-test-sqlserver-setup Set up SQL Server pgbench testing environment"
	@echo "  pgbench-test-sqlserver       Run pgbench tests against SQL Server"
	@echo "  pgbench-test-sqlserver-clean Clean up SQL Server pgbench testing"
	@echo "  pgbench-test-sqlserver-full  Full SQL Server pgbench test cycle"
	@echo ""
	@echo "SQLite Chaos Testing:"
	@echo "  chaos-test-sqlite-setup      Set up SQLite chaos testing environment"
	@echo "  chaos-test-sqlite            Run chaos tests against SQLite"
	@echo "  chaos-test-sqlite-clean      Clean up SQLite chaos testing"
	@echo "  chaos-test-sqlite-full       Full SQLite chaos test cycle"
	@echo ""
	@echo "SQLite PGBench Testing:"
	@echo "  pgbench-test-sqlite-setup    Set up SQLite pgbench testing environment"
	@echo "  pgbench-test-sqlite          Run pgbench tests against SQLite"
	@echo "  pgbench-test-sqlite-clean    Clean up SQLite pgbench testing"
	@echo "  pgbench-test-sqlite-full     Full SQLite pgbench test cycle"
	@echo ""

# Development commands
build:
	cargo build 

check:
	cargo check
# 	cargo clippy -- -D warnings

test:
	cd ./pg2any-lib && cargo test

audit:
	cargo audit

format:
	cargo fmt

run:
	cargo run

doc-check:
	cargo doc --no-deps --all-features

before-git-push: check build format audit test doc-check

# Docker commands
docker-build:
	docker compose build

docker-start:
	docker compose up -d --remove-orphans postgres mysql

docker-stop:
	docker compose down

docker-restart: docker-stop docker-start

docker-clean:
	docker compose down -v
	docker system prune -f

docker-logs:
	docker compose logs -f

docker-status:
	docker compose ps

psql:
	psql -h 127.0.0.1 -U postgres -p 5432 postgres

mysql:
	mysql -h 127.0.0.1 -P 3306 -u root -ptest.123 mysql

clean:
	cargo clean

# MySQL Chaos Testing commands
chaos-test-mysql-setup:
	@echo "Setting up MySQL chaos testing environment..."
	@chmod +x tests/chaos/scripts/*.sh
	@echo "Cleaning up previous state..."
	@docker compose -f docker-compose.chaos-test.yml down -v 2>/dev/null || true
	@docker compose -f docker-compose.chaos-test.yml up --build --remove-orphans -d
	@echo "Waiting for services to be healthy..."
	@docker compose -f docker-compose.chaos-test.yml ps

chaos-test-mysql:
	@echo "Running chaos integration tests against MySQL..."
	@echo "This will randomly restart the CDC application to test graceful shutdown"
	@cd tests/chaos/scripts && ./run_chaos_tests.sh

chaos-test-mysql-clean:
	@echo "Cleaning up MySQL chaos testing environment..."
	@docker compose -f docker-compose.chaos-test.yml down -v
	@docker volume rm chaos_test_lsn_data 2>/dev/null || true
	@docker network rm chaos_test_network 2>/dev/null || true
	@echo "Cleanup complete."

chaos-test-mysql-full: chaos-test-mysql-setup chaos-test-mysql chaos-test-mysql-clean
	@echo "Full MySQL chaos test cycle complete!"

chaos-test-mysql-logs:
	@echo "Showing CDC application logs..."
	@docker logs -f cdc_application

# MySQL PGBench Testing commands
pgbench-test-mysql-setup:
	@echo "Setting up MySQL pgbench testing environment..."
	@chmod +x tests/chaos/scripts/*.sh
	@echo "Cleaning up previous state..."
	@docker compose -f docker-compose.chaos-test.yml down -v 2>/dev/null || true
	@docker compose -f docker-compose.chaos-test.yml up --build --remove-orphans -d
	@echo "Waiting for services to be healthy..."
	@docker compose -f docker-compose.chaos-test.yml ps
	@sleep 10 # Wait for the CDC application to fully initialize

pgbench-test-mysql:
	@echo "Running pgbench chaos integration test against MySQL..."
	@echo "This will run pgbench while randomly restarting the CDC application"
	@cd tests/chaos/scripts && ./run_pgbench_chaos_test.sh

pgbench-test-mysql-clean:
	@echo "Cleaning up MySQL pgbench testing environment..."
	@docker compose -f docker-compose.chaos-test.yml down -v
	@docker volume rm chaos_test_lsn_data 2>/dev/null || true
	@docker network rm chaos_test_network 2>/dev/null || true
	@echo "Cleanup complete."

pgbench-test-mysql-full: pgbench-test-mysql-setup pgbench-test-mysql pgbench-test-mysql-clean
	@echo "Full MySQL pgbench test cycle complete!"

pgbench-test-mysql-logs:
	@echo "Showing CDC application logs..."
	@docker logs -f cdc_application

# Helper: find sqlcmd path inside SQL Server container
SQLSERVER_SA_PASSWORD ?= Test.123!
SQLCMD_EXEC = docker exec cdc_sqlserver bash -c 'if [ -x /opt/mssql-tools18/bin/sqlcmd ]; then /opt/mssql-tools18/bin/sqlcmd "$$@"; elif [ -x /opt/mssql-tools/bin/sqlcmd ]; then /opt/mssql-tools/bin/sqlcmd "$$@"; else echo "sqlcmd not found" >&2; exit 1; fi' --

# SQL Server Chaos Testing commands
chaos-test-sqlserver-setup:
	@echo "Setting up SQL Server chaos testing environment..."
	@chmod +x tests/chaos/scripts/*.sh
	@echo "Cleaning up previous state..."
	@docker compose -f docker-compose.chaos-test-sqlserver.yml down -v 2>/dev/null || true
	@echo "Building CDC application image..."
	@docker compose -f docker-compose.chaos-test-sqlserver.yml build cdc_app
	@echo "Starting PostgreSQL and SQL Server..."
	@docker compose -f docker-compose.chaos-test-sqlserver.yml up -d --remove-orphans postgres sqlserver
	@echo "Waiting for PostgreSQL to be ready..."
	@timeout 60 bash -c 'until docker exec cdc_postgres pg_isready -U postgres 2>/dev/null; do sleep 2; done'
	@docker exec cdc_postgres psql -U postgres -c "SELECT pg_drop_replication_slot('cdc_slot') FROM pg_replication_slots WHERE slot_name = 'cdc_slot';" 2>/dev/null || true
	@echo "Waiting for SQL Server to be ready..."
	@timeout 120 bash -c 'until docker ps --filter name=cdc_sqlserver --format "{{.Status}}" | grep -q healthy; do sleep 5; done'
	@echo "Initializing SQL Server database..."
	@$(SQLCMD_EXEC) -S localhost -U sa -P '$(SQLSERVER_SA_PASSWORD)' -C -i /init/init_sqlserver.sql
	@echo "Starting CDC application..."
	@docker compose -f docker-compose.chaos-test-sqlserver.yml up -d cdc_app
	@echo "Waiting for CDC application to initialize..."
	@timeout 60 bash -c 'until docker ps --filter name=cdc_application --format "{{.Status}}" | grep -q healthy; do sleep 5; done'
	@docker compose -f docker-compose.chaos-test-sqlserver.yml ps

chaos-test-sqlserver:
	@echo "Running chaos integration tests against SQL Server..."
	@cd tests/chaos/scripts && ./run_chaos_tests_sqlserver.sh

chaos-test-sqlserver-clean:
	@echo "Cleaning up SQL Server chaos testing environment..."
	@docker compose -f docker-compose.chaos-test-sqlserver.yml down -v
	@docker network rm chaos_test_network 2>/dev/null || true
	@echo "Cleanup complete."

chaos-test-sqlserver-full: chaos-test-sqlserver-setup chaos-test-sqlserver chaos-test-sqlserver-clean
	@echo "Full SQL Server chaos test cycle complete!"

chaos-test-sqlserver-logs:
	@echo "Showing CDC application logs..."
	@docker logs -f cdc_application

# SQL Server PGBench Testing commands
pgbench-test-sqlserver-setup:
	@echo "Setting up SQL Server pgbench testing environment..."
	@chmod +x tests/chaos/scripts/*.sh
	@echo "Cleaning up previous state..."
	@docker compose -f docker-compose.chaos-test-sqlserver.yml down -v 2>/dev/null || true
	@echo "Building CDC application image..."
	@docker compose -f docker-compose.chaos-test-sqlserver.yml build cdc_app
	@echo "Starting PostgreSQL and SQL Server..."
	@docker compose -f docker-compose.chaos-test-sqlserver.yml up -d --remove-orphans postgres sqlserver
	@echo "Waiting for PostgreSQL to be ready..."
	@timeout 60 bash -c 'until docker exec cdc_postgres pg_isready -U postgres 2>/dev/null; do sleep 2; done'
	@docker exec cdc_postgres psql -U postgres -c "SELECT pg_drop_replication_slot('cdc_slot') FROM pg_replication_slots WHERE slot_name = 'cdc_slot';" 2>/dev/null || true
	@echo "Waiting for SQL Server to be ready..."
	@timeout 120 bash -c 'until docker ps --filter name=cdc_sqlserver --format "{{.Status}}" | grep -q healthy; do sleep 5; done'
	@echo "Initializing SQL Server database..."
	@$(SQLCMD_EXEC) -S localhost -U sa -P '$(SQLSERVER_SA_PASSWORD)' -C -i /init/init_sqlserver.sql
	@echo "Starting CDC application..."
	@docker compose -f docker-compose.chaos-test-sqlserver.yml up -d cdc_app
	@echo "Waiting for CDC application to initialize..."
	@timeout 60 bash -c 'until docker ps --filter name=cdc_application --format "{{.Status}}" | grep -q healthy; do sleep 5; done'
	@docker compose -f docker-compose.chaos-test-sqlserver.yml ps

pgbench-test-sqlserver:
	@echo "Running pgbench chaos integration test against SQL Server..."
	@cd tests/chaos/scripts && ./run_pgbench_chaos_test_sqlserver.sh

pgbench-test-sqlserver-clean:
	@echo "Cleaning up SQL Server pgbench testing environment..."
	@docker compose -f docker-compose.chaos-test-sqlserver.yml down -v
	@docker network rm chaos_test_network 2>/dev/null || true
	@echo "Cleanup complete."

pgbench-test-sqlserver-full: pgbench-test-sqlserver-setup pgbench-test-sqlserver pgbench-test-sqlserver-clean
	@echo "Full SQL Server pgbench test cycle complete!"

# SQLite Chaos Testing commands
chaos-test-sqlite-setup:
	@echo "Setting up SQLite chaos testing environment..."
	@chmod +x tests/chaos/scripts/*.sh
	@echo "Cleaning up previous state..."
	@docker compose -f docker-compose.chaos-test-sqlite.yml down -v 2>/dev/null || true
	@echo "Building CDC application image..."
	@docker compose -f docker-compose.chaos-test-sqlite.yml build cdc_app
	@echo "Starting PostgreSQL..."
	@docker compose -f docker-compose.chaos-test-sqlite.yml up -d --remove-orphans postgres
	@echo "Waiting for PostgreSQL to be ready..."
	@timeout 60 bash -c 'until docker exec cdc_postgres pg_isready -U postgres 2>/dev/null; do sleep 2; done'
	@docker exec cdc_postgres psql -U postgres -c "SELECT pg_drop_replication_slot('cdc_slot') FROM pg_replication_slots WHERE slot_name = 'cdc_slot';" 2>/dev/null || true
	@echo "Starting CDC application..."
	@docker compose -f docker-compose.chaos-test-sqlite.yml up -d cdc_app
	@echo "Waiting for CDC application to initialize..."
	@timeout 60 bash -c 'until docker ps --filter name=cdc_application --format "{{.Status}}" | grep -q healthy; do sleep 5; done'
	@echo "Initializing SQLite database schema..."
	@docker exec cdc_application sqlite3 /app/data/cdc_target.db ".read /init/init_sqlite.sql"
	@docker compose -f docker-compose.chaos-test-sqlite.yml ps

chaos-test-sqlite:
	@echo "Running chaos integration tests against SQLite..."
	@cd tests/chaos/scripts && ./run_chaos_tests_sqlite.sh

chaos-test-sqlite-clean:
	@echo "Cleaning up SQLite chaos testing environment..."
	@docker compose -f docker-compose.chaos-test-sqlite.yml down -v
	@docker volume rm chaos_test_sqlite_data 2>/dev/null || true
	@docker network rm chaos_test_network 2>/dev/null || true
	@echo "Cleanup complete."

chaos-test-sqlite-full: chaos-test-sqlite-setup chaos-test-sqlite chaos-test-sqlite-clean
	@echo "Full SQLite chaos test cycle complete!"

chaos-test-sqlite-logs:
	@echo "Showing CDC application logs..."
	@docker logs -f cdc_application

# SQLite PGBench Testing commands
pgbench-test-sqlite-setup:
	@echo "Setting up SQLite pgbench testing environment..."
	@chmod +x tests/chaos/scripts/*.sh
	@echo "Cleaning up previous state..."
	@docker compose -f docker-compose.chaos-test-sqlite.yml down -v 2>/dev/null || true
	@echo "Building CDC application image..."
	@docker compose -f docker-compose.chaos-test-sqlite.yml build cdc_app
	@echo "Starting PostgreSQL..."
	@docker compose -f docker-compose.chaos-test-sqlite.yml up -d --remove-orphans postgres
	@echo "Waiting for PostgreSQL to be ready..."
	@timeout 60 bash -c 'until docker exec cdc_postgres pg_isready -U postgres 2>/dev/null; do sleep 2; done'
	@docker exec cdc_postgres psql -U postgres -c "SELECT pg_drop_replication_slot('cdc_slot') FROM pg_replication_slots WHERE slot_name = 'cdc_slot';" 2>/dev/null || true
	@echo "Starting CDC application..."
	@docker compose -f docker-compose.chaos-test-sqlite.yml up -d cdc_app
	@echo "Waiting for CDC application to initialize..."
	@timeout 60 bash -c 'until docker ps --filter name=cdc_application --format "{{.Status}}" | grep -q healthy; do sleep 5; done'
	@echo "Initializing SQLite database schema..."
	@docker exec cdc_application sqlite3 /app/data/cdc_target.db ".read /init/init_sqlite.sql"
	@docker compose -f docker-compose.chaos-test-sqlite.yml ps

pgbench-test-sqlite:
	@echo "Running pgbench chaos integration test against SQLite..."
	@cd tests/chaos/scripts && ./run_pgbench_chaos_test_sqlite.sh

pgbench-test-sqlite-clean:
	@echo "Cleaning up SQLite pgbench testing environment..."
	@docker compose -f docker-compose.chaos-test-sqlite.yml down -v
	@docker volume rm chaos_test_sqlite_data 2>/dev/null || true
	@docker network rm chaos_test_network 2>/dev/null || true
	@echo "Cleanup complete."

pgbench-test-sqlite-full: pgbench-test-sqlite-setup pgbench-test-sqlite pgbench-test-sqlite-clean
	@echo "Full SQLite pgbench test cycle complete!"
