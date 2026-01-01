#!/bin/bash
#
# Quick Chaos Test - A simplified script for rapid local testing
# This script runs a single scenario or all scenarios with reduced retry counts
#


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

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

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
SCENARIO="$1"
MAX_RETRIES=20  # Reduced for quick testing
RETRY_INTERVAL=10  # Reduced for quick testing
CHAOS_SCRIPT_PID=""

SCENARIOS_DIR="$SCRIPT_DIR/../scenarios"
CHAOS_SCRIPT="$SCRIPT_DIR/chaos_script.sh"

log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

cleanup() {
    if [ -n "$CHAOS_SCRIPT_PID" ] && kill -0 "$CHAOS_SCRIPT_PID" 2>/dev/null; then
        log_info "Stopping chaos script..."
        kill "$CHAOS_SCRIPT_PID" 2>/dev/null || true
        wait "$CHAOS_SCRIPT_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT INT TERM

execute_postgres_sql() {
    local sql_file="$1"
    PGPASSWORD="$POSTGRES_PASSWORD" psql \
        -h "$POSTGRES_HOST" \
        -p "$POSTGRES_PORT" \
        -U "$POSTGRES_USER" \
        -d "$POSTGRES_DB" \
        -f "$sql_file" \
        2>&1
}

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
    
    if echo "$result" | grep -q "PASS"; then
        return 0
    else
        log_warning "Verification: $result"
        return 1
    fi
}

cleanup_test_data() {
    log_info "Cleaning test data..."
    
    PGPASSWORD="$POSTGRES_PASSWORD" psql \
        -h "$POSTGRES_HOST" \
        -p "$POSTGRES_PORT" \
        -U "$POSTGRES_USER" \
        -d "$POSTGRES_DB" \
        -c "TRUNCATE TABLE public.t1;" 2>&1 > /dev/null || true
    
    mysql \
        -h "$MYSQL_HOST" \
        -P "$MYSQL_PORT" \
        -u "$MYSQL_USER" \
        -p"$MYSQL_PASSWORD" \
        "$MYSQL_DB" \
        -e "TRUNCATE TABLE t1;" 2>&1 > /dev/null || true
    
    sleep 3
}

run_scenario() {
    local scenario_num="$1"
    local input_file="$SCENARIOS_DIR/input/scenario${scenario_num}_input.sql"
    local verify_file="$SCENARIOS_DIR/verify/scenario${scenario_num}_verify.sql"
    
    if [ ! -f "$input_file" ] || [ ! -f "$verify_file" ]; then
        log_error "Scenario $scenario_num files not found"
        return 1
    fi
    
    log_info "=========================================="
    log_info "Quick Test - Scenario $scenario_num"
    log_info "=========================================="
    
    cleanup_test_data
    
    if ! execute_postgres_sql "$input_file" > /dev/null; then
        log_error "Failed to execute input SQL"
        return 1
    fi
    
    log_info "Input executed. Verifying (max $MAX_RETRIES attempts)..."
    
    local retry_count=0
    while [ $retry_count -lt $MAX_RETRIES ]; do
        retry_count=$((retry_count + 1))
        
        if verify_scenario "$verify_file"; then
            log_success "✓ Scenario $scenario_num PASSED (attempt $retry_count)"
            return 0
        fi
        
        if [ $retry_count -lt $MAX_RETRIES ]; then
            echo -n "."
            sleep "$RETRY_INTERVAL"
        fi
    done
    
    echo ""
    log_error "✗ Scenario $scenario_num FAILED after $MAX_RETRIES attempts"
    return 1
}

show_usage() {
    echo "Usage: $0 [scenario_number|all]"
    echo ""
    echo "Quick chaos testing with reduced retry counts for faster feedback"
    echo ""
    echo "Examples:"
    echo "  $0 1        # Run scenario 1 only"
    echo "  $0 all      # Run all scenarios"
    echo ""
    echo "Available scenarios:"
    ls "$SCENARIOS_DIR"/input/scenario*_input.sql 2>/dev/null | while read -r file; do
        local num=$(basename "$file" | sed 's/scenario\([0-9]*\)_input.sql/\1/')
        local desc=$(head -n 2 "$file" | tail -n 1 | sed 's/^-- //')
        echo "  $num: $desc"
    done
}

main() {
    if [ -z "$SCENARIO" ]; then
        show_usage
        exit 1
    fi
    
    # Check prerequisites
    if ! docker ps -a --filter "name=^${CONTAINER_NAME}$" --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
        log_error "Container '$CONTAINER_NAME' not found. Run 'make chaos-test-setup' first."
        exit 1
    fi
    
    if [ ! -f "$CHAOS_SCRIPT" ]; then
        log_error "Chaos script not found: $CHAOS_SCRIPT"
        exit 1
    fi
    
    chmod +x "$CHAOS_SCRIPT"
    
    log_info "Starting chaos script..."
    "$CHAOS_SCRIPT" "$CONTAINER_NAME" > /dev/null 2>&1 &
    CHAOS_SCRIPT_PID=$!
    log_info "Chaos script running (PID: $CHAOS_SCRIPT_PID)"
    sleep 3
    
    if [ "$SCENARIO" = "all" ]; then
        log_info "Running all scenarios..."
        local passed=0
        local failed=0
        
        for file in "$SCENARIOS_DIR"/input/scenario*_input.sql; do
            local num=$(basename "$file" | sed 's/scenario\([0-9]*\)_input.sql/\1/')
            if run_scenario "$num"; then
                passed=$((passed + 1))
            else
                failed=$((failed + 1))
            fi
            echo ""
        done
        
        log_info "=========================================="
        log_info "Results: $passed passed, $failed failed"
        log_info "=========================================="
        
        [ $failed -eq 0 ] && exit 0 || exit 1
    else
        run_scenario "$SCENARIO"
    fi
}

main "$@"
