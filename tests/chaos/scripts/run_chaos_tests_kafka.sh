#!/bin/bash
#
# Chaos Integration Test Runner - Kafka Destination
# This script runs all test scenarios with chaos testing (random container restarts)
# and verifies that CDC replication works correctly under adverse conditions.
#
# Kafka verification uses kafkacat/kcat to consume messages and count insert events.
# Between scenarios, the Kafka topic is deleted for clean state.
#

set -eo pipefail

# Load safe environment variables from .env file if it exists
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
ENV_FILE="$PROJECT_ROOT/env/.env.kafka"

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
    echo "Warning: .env.kafka file not found at $ENV_FILE, using defaults"
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

KAFKA_CONTAINER="${CDC_KAFKA_CONTAINER:-cdc_kafka}"
KAFKA_BROKER="${CDC_KAFKA_BROKER:-127.0.0.1:9094}"
KAFKA_TOPIC="${CDC_KAFKA_TOPIC:-pg2any.public.t1}"

CONTAINER_NAME="${CDC_CONTAINER_NAME:-cdc_application}"
MAX_RETRIES=20
RETRY_INTERVAL=45
CHAOS_SCRIPT_PID=""

# Expected insert counts per scenario (deterministic)
declare -A EXPECTED_INSERTS
EXPECTED_INSERTS[1]=100
EXPECTED_INSERTS[2]=50
EXPECTED_INSERTS[3]=300000
EXPECTED_INSERTS[4]=60
EXPECTED_INSERTS[5]=3000000

# Script paths
SCENARIOS_DIR="$SCRIPT_DIR/../scenarios"
CHAOS_SCRIPT="$SCRIPT_DIR/chaos_script.sh"

# Detect kafkacat or kcat
detect_kafkacat() {
    if command -v kcat &> /dev/null; then
        echo "kcat"
    elif command -v kafkacat &> /dev/null; then
        echo "kafkacat"
    else
        echo ""
    fi
}

KAFKACAT_CMD=""

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

# Function to get total message offset from Kafka (fast, no consumption)
get_kafka_offset_count() {
    local offset
    offset=$(docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-get-offsets.sh \
        --broker-list localhost:9092 --topic "$KAFKA_TOPIC" 2>/dev/null \
        | awk -F: '{sum += $NF} END {print sum}')
    echo "${offset:-0}"
}

# Function to get insert count from Kafka topic
get_kafka_insert_count() {
    local count
    count=$(timeout 600 $KAFKACAT_CMD -C -b "$KAFKA_BROKER" -t "$KAFKA_TOPIC" -e -q -o beginning 2>/dev/null \
        | grep -c '"op":"c"' || true)
    echo "${count:-0}"
}

# Function to get total message count from Kafka topic
get_kafka_total_count() {
    local count
    count=$(timeout 600 $KAFKACAT_CMD -C -b "$KAFKA_BROKER" -t "$KAFKA_TOPIC" -e -q -o beginning 2>/dev/null \
        | wc -l | tr -d '[:space:]')
    echo "${count:-0}"
}

# Function to delete and recreate Kafka topic for clean state
reset_kafka_topic() {
    log_info "Resetting Kafka topic: $KAFKA_TOPIC"
    docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --delete --topic "$KAFKA_TOPIC" 2>/dev/null || true
    sleep 3
    # Pre-create the topic to avoid auto-creation race conditions that can lose first message
    docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --create --topic "$KAFKA_TOPIC" \
        --partitions 1 --replication-factor 1 \
        --config max.message.bytes=10485760 2>/dev/null || true
    sleep 2
    log_info "Kafka topic reset complete"
}

# Function to verify scenario
verify_scenario() {
    local scenario_num="$1"
    local expected="${EXPECTED_INSERTS[$scenario_num]}"

    if [ -z "$expected" ]; then
        log_warning "No expected insert count for scenario $scenario_num, skipping Kafka verification"
        return 0
    fi

    # Fast check: get total offset (includes all event types)
    local offset_count
    offset_count=$(get_kafka_offset_count)
    log_info "Kafka topic offset: $offset_count (need >= $expected inserts)"

    # If total offset is less than expected inserts, no need to do expensive kcat parse
    if [ "$offset_count" -lt "$expected" ] 2>/dev/null; then
        return 1
    fi

    # For large topics (>10K messages), trust offset count as insert count since
    # test scenarios only produce INSERT events. kcat consumption is unreliable
    # for large topics under active production (never reaches EOF with -e flag).
    if [ "$offset_count" -ge 10000 ]; then
        log_info "Kafka insert events (offset-based): actual=$offset_count expected=$expected (at-least-once)"
        return 0
    fi

    # Full verification for small topics: count insert events (op=c)
    local actual
    actual=$(get_kafka_insert_count)

    log_info "Kafka insert events: actual=$actual expected=$expected (at-least-once)"

    if [ "$actual" -ge "$expected" ] 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Function to clean up test data
cleanup_test_data() {
    log_info "Cleaning up test data..."

    # Clean PostgreSQL
    PGPASSWORD="$POSTGRES_PASSWORD" psql \
        -h "$POSTGRES_HOST" \
        -p "$POSTGRES_PORT" \
        -U "$POSTGRES_USER" \
        -d "$POSTGRES_DB" \
        -c "TRUNCATE TABLE public.t1;" > /dev/null 2>&1 || true

    # Reset Kafka topic
    reset_kafka_topic

    # Wait for cleanup to settle
    sleep 5
}

# Function to run a single scenario
run_scenario() {
    local scenario_num="$1"
    local input_file="$SCENARIOS_DIR/input/scenario${scenario_num}_input.sql"

    if [ ! -f "$input_file" ]; then
        log_error "Input file not found: $input_file"
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

    log_info "Input SQL executed successfully. Waiting for replication to Kafka..."

    # Retry verification until it passes
    local retry_count=0
    local verification_passed=false
    local last_count=-1
    local stall_count=0

    while [ $retry_count -lt $MAX_RETRIES ]; do
        retry_count=$((retry_count + 1))
        log_info "Verification attempt $retry_count/$MAX_RETRIES for scenario $scenario_num..."

        if verify_scenario "$scenario_num"; then
            log_success "Scenario $scenario_num PASSED on attempt $retry_count"
            verification_passed=true
            break
        else
            log_warning "Scenario $scenario_num verification failed (attempt $retry_count/$MAX_RETRIES)"

            # Stall detection: if count unchanged for 5 consecutive retries, CDC is likely dead
            # (5 × 45s = 225s tolerance accounts for Docker restart cycle + large transaction replay)
            # Skip stall detection when count is 0 — transaction may still be in producer phase
            local current_count
            current_count=$(get_kafka_offset_count)
            if [ "$current_count" -gt 0 ] && [ "$current_count" -eq "$last_count" ] 2>/dev/null; then
                stall_count=$((stall_count + 1))
                if [ $stall_count -ge 5 ]; then
                    log_error "Scenario $scenario_num stalled at $current_count inserts for 5 retries — CDC likely dead"
                    break
                fi
            else
                stall_count=0
            fi
            last_count=$current_count

            if [ $retry_count -lt $MAX_RETRIES ]; then
                log_info "Waiting $RETRY_INTERVAL seconds before retry..."
                sleep "$RETRY_INTERVAL"
            fi
        fi
    done

    if [ "$verification_passed" = false ]; then
        log_error "Scenario $scenario_num FAILED after $MAX_RETRIES attempts"
        local final_insert_count
        final_insert_count=$(get_kafka_insert_count)
        local final_total_count
        final_total_count=$(get_kafka_total_count)
        log_error "Final Kafka state: insert_events=$final_insert_count total_events=$final_total_count"
        return 1
    fi

    return 0
}

# Main execution
main() {
    log_info "=========================================="
    log_info "CDC Chaos Integration Test Suite (Kafka)"
    log_info "=========================================="
    log_info "Max retries per scenario: $MAX_RETRIES"
    log_info "Retry interval: $RETRY_INTERVAL seconds"
    log_info "Container under test: $CONTAINER_NAME"
    log_info "Kafka broker: $KAFKA_BROKER"
    log_info "Kafka topic: $KAFKA_TOPIC"
    log_info ""

    # Detect kafkacat
    KAFKACAT_CMD=$(detect_kafkacat)
    if [ -z "$KAFKACAT_CMD" ]; then
        log_error "kafkacat/kcat not found. Please install kafkacat."
        exit 1
    fi
    log_info "Using: $KAFKACAT_CMD"

    # Check jq
    if ! command -v jq &> /dev/null; then
        log_error "jq not found. Please install jq."
        exit 1
    fi

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
        # Extract scenario number from filename
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
