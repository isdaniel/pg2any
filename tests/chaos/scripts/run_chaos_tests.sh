#!/bin/bash
#
# Chaos Integration Test Runner
# This script runs all test scenarios with chaos testing (random container restarts)
# and verifies that CDC replication works correctly under adverse conditions.
#
# The script will:
# 1. Start the chaos script in background to randomly restart cdc_application
# 2. For each scenario:
#    - Clean up previous test data
#    - Execute input SQL on PostgreSQL
#    - Wait and retry verification on MySQL until it passes (max 80 attempts, 45s intervals)
#    - Move to next scenario
# 3. Stop chaos script when all scenarios complete
#

set -e

# Load environment variables from .env file if it exists
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
ENV_FILE="$PROJECT_ROOT/env/.env"

if [ -f "$ENV_FILE" ]; then
    echo "Loading environment from: $ENV_FILE"
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "Warning: .env file not found at $ENV_FILE, using defaults"
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

MYSQL_HOST="${CDC_MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${CDC_MYSQL_PORT:-3306}"
MYSQL_USER="${CDC_MYSQL_USER:-root}"
MYSQL_PASSWORD="${CDC_MYSQL_PASSWORD:-test.123}"
MYSQL_DB="${CDC_MYSQL_DB:-cdc_db}"

CONTAINER_NAME="${CDC_CONTAINER_NAME:-cdc_application}"
MAX_RETRIES=40
RETRY_INTERVAL=45
CHAOS_SCRIPT_PID=""

# Script paths
SCENARIOS_DIR="$SCRIPT_DIR/../scenarios"
CHAOS_SCRIPT="$SCRIPT_DIR/chaos_script.sh"

# Logging functions (output to stdout/stderr)
log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message"
}

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

# Function to execute MySQL SQL and get result
execute_mysql_sql() {
    local sql_file="$1"
    log_info "Executing MySQL SQL: $sql_file"
    
    mysql \
        -h "$MYSQL_HOST" \
        -P "$MYSQL_PORT" \
        -u "$MYSQL_USER" \
        -p"$MYSQL_PASSWORD" \
        "$MYSQL_DB" \
        < "$sql_file" \
        2>&1
    
    return ${PIPESTATUS[0]}
}

# Function to verify test result
verify_scenario() {
    local verify_file="$1"
    
    local result=$(mysql \
        -h "$MYSQL_HOST" \
        -P "$MYSQL_PORT" \
        -u "$MYSQL_USER" \
        -p"$MYSQL_PASSWORD" \
        "$MYSQL_DB" \
        -N -B \
        < "$verify_file" 2>&1)
    
    # Check if result contains "PASS"
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
    
    # Clean MySQL
    mysql \
        -h "$MYSQL_HOST" \
        -P "$MYSQL_PORT" \
        -u "$MYSQL_USER" \
        -p"$MYSQL_PASSWORD" \
        "$MYSQL_DB" \
        -e "TRUNCATE TABLE t1;" \
        2>&1 || true
    
    # Wait for truncate to replicate
    sleep 5
}

# Function to run a single scenario
run_scenario() {
    local scenario_num="$1"
    local input_file="$SCENARIOS_DIR/input/scenario${scenario_num}_input.sql"
    local verify_file="$SCENARIOS_DIR/verify/scenario${scenario_num}_verify.sql"
    
    if [ ! -f "$input_file" ]; then
        log_error "Input file not found: $input_file"
        return 1
    fi
    
    if [ ! -f "$verify_file" ]; then
        log_error "Verify file not found: $verify_file"
        return 1
    fi
    
    log_info "========================================"
    log_info "Running Scenario $scenario_num"
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
            log_success "✓ Scenario $scenario_num PASSED on attempt $retry_count"
            verification_passed=true
            break
        else
            log_warning "✗ Scenario $scenario_num verification failed (attempt $retry_count/$MAX_RETRIES)"
            
            if [ $retry_count -lt $MAX_RETRIES ]; then
                log_info "Waiting $RETRY_INTERVAL seconds before retry..."
                sleep "$RETRY_INTERVAL"
            fi
        fi
    done
    
    if [ "$verification_passed" = false ]; then
        log_error "✗ Scenario $scenario_num FAILED after $MAX_RETRIES attempts"
        return 1
    fi
    
    return 0
}

# Main execution
main() {
    log_info "=========================================="
    log_info "CDC Chaos Integration Test Suite"
    log_info "=========================================="
    log_info "Max retries per scenario: $MAX_RETRIES"
    log_info "Retry interval: $RETRY_INTERVAL seconds"
    log_info "Container under test: $CONTAINER_NAME"
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
        # Extract scenario number from filename (e.g., scenario1_input.sql -> 1)
        local scenario_num=$(basename "$scenario_file" | sed 's/scenario\([0-9]*\)_input.sql/\1/')
        
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
    log_info "Test Suite Complete"
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
