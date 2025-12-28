#!/bin/bash
set -e

ROLE=$1

# Read REST API configuration from environment or use defaults
REST_HOST=${REST_HOST:-localhost}
REST_PORT=${REST_PORT:-8081}

if [ "$ROLE" = "jobmanager" ]; then
    echo "Starting JobManager..."
    /opt/flink/bin/jobmanager.sh start-foreground &
    JM_PID=$!
    
    # Wait for REST API to be ready
    echo "Waiting for JobManager REST API on ${REST_HOST}:${REST_PORT}..."
    for i in {1..60}; do
        if curl -s http://${REST_HOST}:${REST_PORT}/v1/overview > /dev/null 2>&1; then
            echo "JobManager is ready. Waiting for TaskManager to register slots..."
            sleep 20  # Wait for TaskManager to register and producer to stabilize
            echo "Submitting consumer job..."
            /opt/flink/bin/flink run -m ${REST_HOST}:${REST_PORT} /opt/flink/usrlib/procstat-flink-consumer-0.1.0.jar
            echo "Consumer job submitted."
            break
        fi
        echo "Waiting for JobManager... ($i/60)"
        sleep 1
    done
    
    # Keep the JobManager running in foreground
    wait $JM_PID

elif [ "$ROLE" = "taskmanager" ]; then
    echo "Starting TaskManager..."
    exec /opt/flink/bin/taskmanager.sh start-foreground

else
    echo "Unknown role: $ROLE"
    exit 1
fi
