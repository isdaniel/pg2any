#!/bin/bash
#
# Chaos Script - Randomly restart CDC application container
# This script continuously restarts the specified container at random intervals
# to test the graceful shutdown and recovery capabilities of the CDC application.
#
# Usage: ./chaos_script.sh <container_name>
#

set -e

# Function to sleep for a random duration within a range
sleep_in_range() {
    local min=$1
    local max=$2
    local range=$((max - min))
    local random_sleep=$((RANDOM % range + min))
    echo "Sleeping for $random_sleep seconds..."
    sleep "$random_sleep"
}

# Function to restart container
restart_container() {
    local container_id="$1"
    local container_name="$2"
    
    echo "========================================"
    echo "Restarting container: $container_name (ID: $container_id)"
    echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "========================================"
    
    # Gracefully stop the container (respects stop_grace_period in docker-compose)
    if docker stop "$container_id"; then
        echo "✓ Container stopped successfully."
    else
        echo "✗ Failed to stop container: $container_id"
        return 1
    fi
    
    # Wait a moment before starting
    sleep 5
    
    # Start the container
    if docker start "$container_id"; then
        echo "✓ Container started successfully."
    else
        echo "✗ Failed to start container: $container_id"
        return 1
    fi
    
    # Wait for container to be healthy before next restart
    echo "Waiting for container to become healthy..."
    local wait_count=0
    local max_wait=60  # Wait up to 60 seconds for health check
    
    while [ $wait_count -lt $max_wait ]; do
        local health_status=$(docker inspect --format='{{.State.Health.Status}}' "$container_id" 2>/dev/null || echo "unknown")
        
        if [ "$health_status" = "healthy" ]; then
            echo "✓ Container is healthy."
            break
        elif [ "$health_status" = "unhealthy" ]; then
            echo "⚠ Container is unhealthy, but continuing..."
            break
        fi
        
        sleep 2
        wait_count=$((wait_count + 2))
    done
    
    if [ $wait_count -ge $max_wait ]; then
        echo "⚠ Health check timeout reached, continuing anyway..."
    fi
}

# Main function to manage container chaos
manage_container() {
    local container_name="$1"
    
    echo "=========================================="
    echo "Starting Chaos Script for: $container_name"
    echo "Container will be restarted every 45-90 seconds"
    echo "=========================================="
    
    while true; do
        # Find container by name
        local container_info
        container_info=$(docker ps -a --filter "name=^${container_name}$" --format "{{.ID}} {{.Status}} {{.Names}}")
        
        if [ -n "$container_info" ]; then
            local container_id status container_found_name
            container_id=$(echo "$container_info" | awk '{print $1}')
            status=$(echo "$container_info" | awk '{print $2}')
            container_found_name=$(echo "$container_info" | awk '{print $NF}')
            
            echo ""
            echo "Container Info:"
            echo "  Name: $container_found_name"
            echo "  ID: $container_id"
            echo "  Status: $status"
            
            if [ -n "$status" ]; then
                # Only restart if container is running
                if [[ "$status" == Up* ]] || [[ "$status" == "running" ]]; then
                    restart_container "$container_id" "$container_name"
                    # Random sleep between 45-90 seconds before next restart
                    sleep_in_range 45 90
                else
                    echo "Container is not running (status: $status). Attempting to start..."
                    if docker start "$container_id"; then
                        echo "✓ Container started successfully."
                        sleep_in_range 30 45
                    else
                        echo "✗ Failed to start container."
                        sleep 10
                    fi
                fi
            fi
        else
            echo "⚠ Container '$container_name' not found. Retrying in 5 seconds..."
            sleep 5
        fi
    done
}

# Main script execution
container_name="$1"

if [ -z "$container_name" ]; then
    echo "Error: Container name not provided"
    echo "Usage: $0 <container_name>"
    echo "Example: $0 cdc_application"
    exit 1
fi

# Verify container exists before starting chaos
if ! docker ps -a --filter "name=^${container_name}$" --format "{{.Names}}" | grep -q "^${container_name}$"; then
    echo "Error: Container '$container_name' does not exist"
    echo "Available containers:"
    docker ps -a --format "{{.Names}}"
    exit 1
fi

echo "✓ Container '$container_name' found. Starting chaos testing..."
echo ""

manage_container "$container_name"
