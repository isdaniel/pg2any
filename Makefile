# PostgreSQL CDC Makefile
# Provides convenient commands for development and deployment

.PHONY: help build start stop restart clean logs test check format docker-build docker-start docker-stop docker-clean chaos-test chaos-test-setup chaos-test-clean

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
	@echo "Chaos Testing:"
	@echo "  chaos-test-setup   Set up and start services for chaos testing"
	@echo "  chaos-test         Run chaos integration tests"
	@echo "  chaos-test-clean   Clean up after chaos testing"
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

before-git-push: check build format test doc-check

# Docker commands
docker-build:
	docker-compose build

docker-start:
	docker-compose up -d postgres mysql

docker-stop:
	docker-compose down

docker-restart: docker-stop docker-start

docker-clean:
	docker-compose down -v
	docker system prune -f

docker-logs:
	docker-compose logs -f

docker-status:
	docker-compose ps

psql:
	psql -h 127.0.0.1 -U postgres -p 5432 postgres

mysql:
	mysql -h 127.0.0.1 -P 3306 -u root -ptest.123 mysql

clean:
	cargo clean

# Chaos Testing commands
chaos-test-setup:
	@echo "Setting up chaos testing environment..."
	@chmod +x tests/chaos/scripts/*.sh
	@docker-compose -f docker-compose.chaos-test.yml up --build -d
	@echo "Waiting for services to be healthy..."
	@docker-compose -f docker-compose.chaos-test.yml ps

chaos-test:
	@echo "Running chaos integration tests..."
	@echo "This will randomly restart the CDC application to test graceful shutdown"
	@cd tests/chaos/scripts && ./run_chaos_tests.sh

chaos-test-clean:
	@echo "Cleaning up chaos testing environment..."
	@docker-compose -f docker-compose.chaos-test.yml down -v
	@docker volume rm chaos_test_lsn_data 2>/dev/null || true
	@docker network rm chaos_test_network 2>/dev/null || true
	@echo "Cleanup complete."

chaos-test-full: chaos-test-setup chaos-test chaos-test-clean
	@echo "Full chaos test cycle complete!"

chaos-test-logs:
	@echo "Showing CDC application logs..."
	@docker logs -f cdc_application
