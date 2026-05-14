#!/bin/bash
#
# Kafka Verification Script
# Counts Kafka messages matching a specific operation type and compares with expected count.
#
# Usage: ./verify_kafka_count.sh <expected_count> [op_filter]
#   expected_count: The number of messages expected
#   op_filter: Operation type to filter (c=insert, u=update, d=delete, t=truncate)
#              Default: count all messages
#
# Returns: Prints "PASS" or "FAIL" with counts
#
# Requires: kafkacat (or kcat) and jq installed on the host
#

set -e

EXPECTED_COUNT="${1:-0}"
OP_FILTER="${2:-}"
KAFKA_BROKER="${KAFKA_BROKER:-127.0.0.1:9094}"
KAFKA_TOPIC="${KAFKA_TOPIC:-pg2any.public.t1}"
TIMEOUT="${KAFKA_CONSUME_TIMEOUT:-10}"

# Detect kafkacat or kcat
if command -v kcat &> /dev/null; then
    KAFKACAT_CMD="kcat"
elif command -v kafkacat &> /dev/null; then
    KAFKACAT_CMD="kafkacat"
else
    echo "FAIL - kafkacat/kcat not found"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "FAIL - jq not found"
    exit 1
fi

# Consume all messages from topic
# -C: consumer mode, -e: exit at end, -q: quiet (no stats), -u: unbuffered
MESSAGES=$($KAFKACAT_CMD -C -b "$KAFKA_BROKER" -t "$KAFKA_TOPIC" -e -q -o beginning 2>/dev/null || true)

if [ -z "$MESSAGES" ]; then
    ACTUAL_COUNT=0
else
    if [ -n "$OP_FILTER" ]; then
        ACTUAL_COUNT=$(echo "$MESSAGES" | jq -r '.payload.op // empty' 2>/dev/null | grep -c "^${OP_FILTER}$" || true)
    else
        ACTUAL_COUNT=$(echo "$MESSAGES" | wc -l | tr -d '[:space:]')
    fi
fi

if [ "$ACTUAL_COUNT" -eq "$EXPECTED_COUNT" ] 2>/dev/null; then
    echo "PASS"
    echo "actual_count=$ACTUAL_COUNT expected_count=$EXPECTED_COUNT"
else
    echo "FAIL"
    echo "actual_count=$ACTUAL_COUNT expected_count=$EXPECTED_COUNT"
fi
