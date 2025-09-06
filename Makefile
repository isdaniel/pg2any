# PostgreSQL CDC Makefile
# Provides convenient commands for development and deployment

.PHONY: help build start stop restart clean logs test check format docker-build docker-start docker-stop docker-clean

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

doc-chcek:
	cargo doc --no-deps --all-features

before-git-push: check build format test audit doc-chcek

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
