#!/bin/bash
#
# Chaos Integration Test Runner - SQL Server Destination
# This script runs all test scenarios with chaos testing (random container restarts)
# and verifies that CDC replication works correctly under adverse conditions.
#

set -e

# Load safe environment variables from .env file if it exists
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
ENV_FILE="$PROJECT_ROOT/env/.env.sqlserver"

load_env_file() {
    local env_file="$1"
    while IFS= read -r line || [[ -n "$line" ]]; do
        [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
        [[ "$line" != *=* ]] && continue
        export "$line"
    done < "$env_file"
}

if [ -f "$ENV_FILE" ]; then
    echo "Loading environment from: $ENV_FILE"
    load_env_file "$ENV_FILE"
else
    echo "Warning: .env.sqlserver file not found at $ENV_FILE, using defaults"
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
POSTGRES_HOST="${CDC_POSTGRES_HOST:-127.0.0.1}"
POSTGRES_PORT="${CDC_POSTGRES_PORT:-5432}"
POSTGRES_USER="${CDC_POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${CDC_POSTGRES_PASSWORD:-test.123}"
POSTGRES_DB="${CDC_POSTGRES_DB:-postgres}"

SQLSERVER_CONTAINER="${CDC_SQLSERVER_CONTAINER:-cdc_sqlserver}"
SQLSERVER_PASSWORD="${CDC_SQLSERVER_PASSWORD:-Test.123!}"
SQLSERVER_DB="${CDC_SQLSERVER_DB:-cdc_db}"

CONTAINER_NAME="${CDC_CONTAINER_NAME:-cdc_application}"
MAX_RETRIES=40
RETRY_INTERVAL=45
CHAOS_SCRIPT_PID=""

# Script paths
SCENARIOS_DIR="$SCRIPT_DIR/../scenarios"
CHAOS_SCRIPT="$SCRIPT_DIR/chaos_script.sh"

# Detect sqlcmd path inside SQL Server container
detect_sqlcmd() {
    if docker exec "$SQLSERVER_CONTAINER" test -x /opt/mssql-tools18/bin/sqlcmd 2>/dev/null; then
        echo "/opt/mssql-tools18/bin/sqlcmd"
    elif docker exec "$SQLSERVER_CONTAINER" test -x /opt/mssql-tools/bin/sqlcmd 2>/dev/null; then
        echo "/opt/mssql-tools/bin/sqlcmd"
    else
        echo ""
    fi
}

SQLCMD_PATH=""

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*" >&2
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

# Cleanup function
cleanup() {
    log_info "Cleaning up..."

    if [ -n "$CHAOS_SCRIPT_PID" ] && kill -0 "$CHAOS_SCRIPT_PID" 2>/dev/null; then
        log_info "Stopping chaos script (PID: $CHAOS_SCRIPT_PID)..."
        kill "$CHAOS_SCRIPT_PID" 2>/dev/null || true
        wait "$CHAOS_SCRIPT_PID" 2>/dev/null || true
    fi

    log_info "Cleanup complete."
}

# Set up trap for cleanup
trap cleanup EXIT INT TERM

# Function to wait for container to be running
wait_for_container_running() {
    local container="$1"
    local max_wait="${2:-120}"
    local waited=0
    while [ $waited -lt $max_wait ]; do
        if docker ps --filter "name=^${container}$" --format "{{.Status}}" | grep -q "^Up"; then
            return 0
        fi
        log_warning "Container '$container' is not running. Attempting to start... (${waited}s/${max_wait}s)"
        docker start "$container" 2>/dev/null || true
        sleep 5
        waited=$((waited + 5))
    done
    log_error "Container '$container' did not start within ${max_wait}s"
    return 1
}

# Function to execute PostgreSQL SQL
execute_postgres_sql() {
    local sql_file="$1"
    log_info "Executing PostgreSQL SQL: $sql_file"

    PGPASSWORD="$POSTGRES_PASSWORD" psql \
        -h "$POSTGRES_HOST" \
        -p "$POSTGRES_PORT" \
        -U "$POSTGRES_USER" \
        -d "$POSTGRES_DB" \
        -f "$sql_file" \
        2>&1

    return ${PIPESTATUS[0]}
}

# Function to verify test result via mounted verify scripts
verify_scenario() {
    local verify_file="$1"
    local verify_filename
    verify_filename=$(basename "$verify_file")

    if ! wait_for_container_running "$SQLSERVER_CONTAINER" 120; then
        log_error "SQL Server container not running, cannot verify scenario"
        return 1
    fi

    local result
    result=$(docker exec "$SQLSERVER_CONTAINER" $SQLCMD_PATH \
        -S localhost -U sa -P "$SQLSERVER_PASSWORD" -C \
        -d "$SQLSERVER_DB" \
        -i "/verify/$verify_filename" \
        -h -1 -W 2>&1)

    if echo "$result" | grep -q "PASS"; then
        return 0
    else
        log_warning "Verification result: $result"
        return 1
    fi
}

# Function to clean up test data
cleanup_test_data() {
    log_info "Cleaning up test data from both databases..."

    # Clean PostgreSQL
    PGPASSWORD="$POSTGRES_PASSWORD" psql \
        -h "$POSTGRES_HOST" \
        -p "$POSTGRES_PORT" \
        -U "$POSTGRES_USER" \
        -d "$POSTGRES_DB" \
        -c "TRUNCATE TABLE public.t1;" > /dev/null 2>&1 || true

    # Clean SQL Server
    docker exec "$SQLSERVER_CONTAINER" $SQLCMD_PATH \
        -S localhost -U sa -P "$SQLSERVER_PASSWORD" -C \
        -d "$SQLSERVER_DB" \
        -Q "TRUNCATE TABLE dbo.t1;" \
        2>&1 || true

    # Wait for truncate to replicate
    sleep 5
}

# Function to run a single scenario
run_scenario() {
    local scenario_num="$1"
    local input_file="$SCENARIOS_DIR/input/scenario${scenario_num}_input.sql"
    local verify_file="$SCENARIOS_DIR/verify_sqlserver/scenario${scenario_num}_verify.sql"

    if [ ! -f "$input_file" ]; then
        log_error "Input file not found: $input_file"
        return 1
    fi

    if [ ! -f "$verify_file" ]; then
        log_error "Verify file not found: $verify_file"
        return 1
    fi

    log_info "========================================"
    log_info "Running Scenario $scenario_num (SQL Server)"
    log_info "========================================"

    # Execute input SQL on PostgreSQL
    if ! execute_postgres_sql "$input_file"; then
        log_error "Failed to execute input SQL for scenario $scenario_num"
        return 1
    fi

    log_info "Input SQL executed successfully. Waiting for replication..."

    # Retry verification until it passes
    local retry_count=0
    local verification_passed=false

    while [ $retry_count -lt $MAX_RETRIES ]; do
        retry_count=$((retry_count + 1))
        log_info "Verification attempt $retry_count/$MAX_RETRIES for scenario $scenario_num..."

        if verify_scenario "$verify_file"; then
            log_success "Scenario $scenario_num PASSED on attempt $retry_count"
            verification_passed=true
            break
        else
            log_warning "Scenario $scenario_num verification failed (attempt $retry_count/$MAX_RETRIES)"

            if [ $retry_count -lt $MAX_RETRIES ]; then
                log_info "Waiting $RETRY_INTERVAL seconds before retry..."
                sleep "$RETRY_INTERVAL"
            fi
        fi
    done

    if [ "$verification_passed" = false ]; then
        log_error "Scenario $scenario_num FAILED after $MAX_RETRIES attempts"
        return 1
    fi

    return 0
}

# Main execution
main() {
    log_info "=========================================="
    log_info "CDC Chaos Integration Test Suite (SQL Server)"
    log_info "=========================================="

    # Detect sqlcmd path
    SQLCMD_PATH=$(detect_sqlcmd)
    if [ -z "$SQLCMD_PATH" ]; then
        log_error "sqlcmd not found in SQL Server container"
        exit 1
    fi
    log_info "Using sqlcmd: $SQLCMD_PATH"

    log_info "Max retries per scenario: $MAX_RETRIES"
    log_info "Retry interval: $RETRY_INTERVAL seconds"
    log_info "Container under test: $CONTAINER_NAME"
    log_info "SQL Server container: $SQLSERVER_CONTAINER"
    log_info ""

    # Check if chaos script exists
    if [ ! -f "$CHAOS_SCRIPT" ]; then
        log_error "Chaos script not found: $CHAOS_SCRIPT"
        exit 1
    fi

    # Make chaos script executable
    chmod +x "$CHAOS_SCRIPT"

    # Check if container exists
    if ! docker ps -a --filter "name=^${CONTAINER_NAME}$" --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
        log_error "Container '$CONTAINER_NAME' not found. Please start docker-compose first."
        exit 1
    fi

    # Wait for services to be ready
    log_info "Waiting for services to be ready..."
    sleep 10

    # Start chaos script in background
    log_info "Starting chaos script for container: $CONTAINER_NAME"
    rm -f "$SCRIPT_DIR/chaos_script.log"
    "$CHAOS_SCRIPT" "$CONTAINER_NAME" > "$SCRIPT_DIR/chaos_script.log" 2>&1 &
    CHAOS_SCRIPT_PID=$!
    log_info "Chaos script started with PID: $CHAOS_SCRIPT_PID"

    # Wait a moment for chaos to begin
    sleep 5

    # Find all scenario input files
    local scenario_files=($(ls "$SCENARIOS_DIR"/input/scenario*_input.sql 2>/dev/null | sort))
    local total_scenarios=${#scenario_files[@]}

    if [ $total_scenarios -eq 0 ]; then
        log_error "No scenario files found in $SCENARIOS_DIR/input"
        exit 1
    fi

    log_info "Found $total_scenarios scenarios to run"
    log_info ""

    # Run each scenario
    local passed=0
    local failed=0

    for scenario_file in "${scenario_files[@]}"; do
        # Extract scenario number from filename
        local scenario_num
        scenario_num=$(basename "$scenario_file" | sed 's/scenario\([0-9]*\)_input.sql/\1/')

        # Clean up before each scenario
        cleanup_test_data

        if run_scenario "$scenario_num"; then
            passed=$((passed + 1))
        else
            failed=$((failed + 1))
            log_error "Scenario $scenario_num failed. Continuing to next scenario..."
        fi

        log_info ""
        sleep 5
    done

    # Summary
    log_info "=========================================="
    log_info "Test Suite Complete (SQL Server)"
    log_info "=========================================="
    log_info "Total scenarios: $total_scenarios"
    log_success "Passed: $passed"

    if [ $failed -gt 0 ]; then
        log_error "Failed: $failed"
        exit 1
    else
        log_success "All scenarios passed!"
        exit 0
    fi
}

# Run main function
main "$@"
