#!/bin/bash
set -e

AIRFLOW_HOME="${AIRFLOW_HOME:-$PWD/airflow}"

# Look for Airflow PIDs and stop the services
for service in api-server dag-processor scheduler triggerer; do
    pid_file="$AIRFLOW_HOME/$service.pid"
    # Ensure the PID file is present
    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        # Ensure the process is running
        if ps -p $pid > /dev/null 2>&1; then
            # Send a SIGTERM signal to the process
            echo "Stopping $service (PID $pid)..."
            kill $pid
        else
            echo "$service not running (stale PID file)."
        fi
        rm -f "$pid_file"
    else
        echo "No PID file for $service, skipping."
    fi
done

echo "All Airflow services stopped (if running)."
