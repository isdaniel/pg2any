#!/bin/bash
#
# PGBench Chaos Test Runner - SQL Server Destination
# This script runs pgbench performance testing while randomly restarting the CDC application
# to test graceful shutdown and recovery under load.
#

set -e

# Load environment variables from .env file if it exists
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
ENV_FILE="$PROJECT_ROOT/env/.env.sqlserver"

if [ -f "$ENV_FILE" ]; then
    echo "Loading environment from: $ENV_FILE"
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "Warning: .env.sqlserver file not found at $ENV_FILE, using defaults"
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
POSTGRES_HOST="${CDC_POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${CDC_POSTGRES_PORT:-5432}"
POSTGRES_USER="${CDC_POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${CDC_POSTGRES_PASSWORD:-test.123}"
POSTGRES_DB="${CDC_POSTGRES_DB:-postgres}"
CONTAINER_NAME="${CDC_CONTAINER_NAME:-cdc_application}"

SQLSERVER_CONTAINER="${CDC_SQLSERVER_CONTAINER:-cdc_sqlserver}"
SQLSERVER_PASSWORD="${CDC_SQLSERVER_PASSWORD:-Test.123!}"
SQLSERVER_DB="${CDC_SQLSERVER_DB:-cdc_db}"

# PGBench configuration
PGBENCH_SCALE="${PGBENCH_SCALE:-32}"
PGBENCH_CLIENTS="${PGBENCH_CLIENTS:-100}"
PGBENCH_THREADS="${PGBENCH_THREADS:-8}"
PGBENCH_TRANSACTIONS="${PGBENCH_TRANSACTIONS:-12}"
PGBENCH_SCRIPT="${PGBENCH_SCRIPT:-$PROJECT_ROOT/examples/scripts/pgbench_testing.sql}"

# Verification configuration
MAX_RETRIES=60
RETRY_INTERVAL=45
EXPECTED_ROW_COUNT=$((3000 * PGBENCH_CLIENTS * PGBENCH_TRANSACTIONS))

# Connection string
POSTGRES_CONNSTRING="postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}?user=${POSTGRES_USER}&password=${POSTGRES_PASSWORD}"

# Script paths
CHAOS_SCRIPT="$SCRIPT_DIR/chaos_script.sh"
CHAOS_SCRIPT_PID=""

# Results file
RESULTS_FILE="$SCRIPT_DIR/pgbench_chaos_sqlserver_results_$(date +%Y%m%d_%H%M%S).log"

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $*" | tee -a "$RESULTS_FILE"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*" | tee -a "$RESULTS_FILE"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*" | tee -a "$RESULTS_FILE" >&2
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" | tee -a "$RESULTS_FILE" >&2
}

log_section() {
    echo -e "${CYAN}========================================${NC}" | tee -a "$RESULTS_FILE"
    echo -e "${CYAN}$*${NC}" | tee -a "$RESULTS_FILE"
    echo -e "${CYAN}========================================${NC}" | tee -a "$RESULTS_FILE"
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
    log_info "Results saved to: $RESULTS_FILE"
}

# Set up trap for cleanup
trap cleanup EXIT INT TERM

# Function to check if container exists
check_container() {
    if ! docker ps -a --filter "name=^${CONTAINER_NAME}$" --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
        log_error "Container '$CONTAINER_NAME' not found. Please start docker-compose first."
        exit 1
    fi

    log_success "Container '$CONTAINER_NAME' found."
}

# Function to check if pgbench is installed
check_pgbench() {
    if ! command -v pgbench &> /dev/null; then
        log_error "pgbench is not installed. Please install PostgreSQL client tools."
        exit 1
    fi

    log_success "pgbench is installed."
}

# Function to test PostgreSQL connection
test_postgres_connection() {
    log_info "Testing PostgreSQL connection..."

    if PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT 1" > /dev/null 2>&1; then
        log_success "PostgreSQL connection successful."
        return 0
    else
        log_error "Cannot connect to PostgreSQL."
        return 1
    fi
}

# Function to test SQL Server connection
test_sqlserver_connection() {
    log_info "Testing SQL Server connection..."

    if docker exec "$SQLSERVER_CONTAINER" /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P "$SQLSERVER_PASSWORD" -C \
        -Q "SELECT 1" -b > /dev/null 2>&1; then
        log_success "SQL Server connection successful."
        return 0
    else
        log_error "Cannot connect to SQL Server."
        return 1
    fi
}

# Function to get row count from SQL Server t1 table
get_sqlserver_row_count() {
    local count=$(docker exec "$SQLSERVER_CONTAINER" /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P "$SQLSERVER_PASSWORD" -C \
        -d "$SQLSERVER_DB" \
        -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM dbo.t1;" \
        -h -1 -W 2>/dev/null | tr -d '[:space:]')

    echo "$count"
}

# Function to verify replication completed
verify_replication() {
    local current_count=$(get_sqlserver_row_count)

    if [ -z "$current_count" ]; then
        log_warning "Failed to get row count from SQL Server"
        return 1
    fi

    log_info "Current SQL Server row count: $current_count / Expected: $EXPECTED_ROW_COUNT"

    if [ "$current_count" -eq "$EXPECTED_ROW_COUNT" ] 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Function to initialize pgbench
initialize_pgbench() {
    log_section "Initializing PGBench (Scale: $PGBENCH_SCALE)"

    local start_time=$(date +%s)

    if pgbench -i -s "$PGBENCH_SCALE" --unlogged-tables --foreign-keys "$POSTGRES_CONNSTRING" 2>&1 | tee -a "$RESULTS_FILE"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        log_success "PGBench initialization completed in ${duration}s"
        return 0
    else
        log_error "PGBench initialization failed"
        return 1
    fi
}

# Function to run pgbench benchmark
run_pgbench_benchmark() {
    log_section "Running PGBench Benchmark"
    log_info "Configuration:"
    log_info "  - Clients: $PGBENCH_CLIENTS"
    log_info "  - Threads: $PGBENCH_THREADS"
    log_info "  - Transactions per client: $PGBENCH_TRANSACTIONS"
    log_info "  - Script: $PGBENCH_SCRIPT"
    echo "" | tee -a "$RESULTS_FILE"

    local start_time=$(date +%s)

    # Check if custom script exists
    local pgbench_cmd="pgbench -c $PGBENCH_CLIENTS -j $PGBENCH_THREADS -t $PGBENCH_TRANSACTIONS"

    if [ -f "$PGBENCH_SCRIPT" ]; then
        log_info "Using custom script: $PGBENCH_SCRIPT"
        pgbench_cmd="$pgbench_cmd -f $PGBENCH_SCRIPT"
    else
        log_warning "Custom script not found: $PGBENCH_SCRIPT"
    fi

    pgbench_cmd="$pgbench_cmd $POSTGRES_CONNSTRING"

    echo "" | tee -a "$RESULTS_FILE"

    if eval "$pgbench_cmd" 2>&1 | tee -a "$RESULTS_FILE"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        log_success "PGBench benchmark completed in ${duration}s"
        return 0
    else
        log_error "PGBench benchmark failed"
        return 1
    fi
}

# Function to start chaos testing
start_chaos_testing() {
    log_section "Starting Chaos Testing"

    if [ ! -f "$CHAOS_SCRIPT" ]; then
        log_error "Chaos script not found: $CHAOS_SCRIPT"
        exit 1
    fi

    chmod +x "$CHAOS_SCRIPT"

    log_info "Starting chaos script for container: $CONTAINER_NAME"
    "$CHAOS_SCRIPT" "$CONTAINER_NAME" > "$SCRIPT_DIR/pgbench_chaos_script.log" 2>&1 &
    CHAOS_SCRIPT_PID=$!

    log_success "Chaos script started with PID: $CHAOS_SCRIPT_PID"
    log_info "Chaos script logs: $SCRIPT_DIR/pgbench_chaos_script.log"

    sleep 5
}

# Main execution
main() {
    export PGPASSWORD=$POSTGRES_PASSWORD
    log_section "PGBench Chaos Integration Test (SQL Server)"
    log_info "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
    log_info "Container under test: $CONTAINER_NAME"
    log_info "SQL Server container: $SQLSERVER_CONTAINER"
    echo "" | tee -a "$RESULTS_FILE"

    # Pre-flight checks
    log_info "Running pre-flight checks..."
    check_pgbench
    test_postgres_connection || exit 1
    test_sqlserver_connection || exit 1
    check_container
    echo "" | tee -a "$RESULTS_FILE"

    # Initialize pgbench
    if ! initialize_pgbench; then
        log_error "Failed to initialize pgbench. Exiting."
        exit 1
    fi
    echo "" | tee -a "$RESULTS_FILE"

    # Wait for initialization to settle
    log_info "Waiting for initialization to settle..."
    sleep 10

    # Start chaos testing
    start_chaos_testing
    echo "" | tee -a "$RESULTS_FILE"

    # Wait for chaos to take effect
    log_info "Allowing chaos script to run for a few cycles..."
    sleep 15

    # Run benchmark
    local benchmark_result=0
    if ! run_pgbench_benchmark; then
        log_error "Benchmark failed."
        benchmark_result=1
    fi

    echo "" | tee -a "$RESULTS_FILE"

    # Verify replication with retry loop
    log_section "Verifying Replication"
    log_info "Expected row count: $EXPECTED_ROW_COUNT"
    log_info "Max retries: $MAX_RETRIES"
    log_info "Retry interval: $RETRY_INTERVAL seconds"
    echo "" | tee -a "$RESULTS_FILE"

    local retry_count=0
    local verification_passed=false

    while [ $retry_count -lt $MAX_RETRIES ]; do
        retry_count=$((retry_count + 1))
        log_info "Verification attempt $retry_count/$MAX_RETRIES..."

        if verify_replication; then
            log_success "Replication verification PASSED on attempt $retry_count"
            verification_passed=true
            break
        else
            log_warning "Replication verification failed (attempt $retry_count/$MAX_RETRIES)"

            if [ $retry_count -lt $MAX_RETRIES ]; then
                log_info "Waiting $RETRY_INTERVAL seconds before retry..."
                sleep "$RETRY_INTERVAL"
            fi
        fi
    done

    echo "" | tee -a "$RESULTS_FILE"

    # Summary
    log_section "Test Complete"
    log_info "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"

    if [ $benchmark_result -eq 0 ] && [ "$verification_passed" = true ]; then
        log_success "PGBench chaos test completed successfully! (SQL Server)"
        log_success "All $EXPECTED_ROW_COUNT rows replicated to SQL Server"
        log_info "Check the results file for detailed metrics: $RESULTS_FILE"
        exit 0
    else
        if [ $benchmark_result -ne 0 ]; then
            log_error "Benchmark failed!"
        fi
        if [ "$verification_passed" = false ]; then
            log_error "Replication verification failed after $MAX_RETRIES attempts!"
        fi
        exit 1
    fi
}

# Run main function
main "$@"
