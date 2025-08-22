#!/bin/bash
# ------------------------------------------------------------------------------
# Script Name: kill_airflow.sh
# Purpose: Terminates all running Airflow processes.
# Usage: bash kill_airflow.sh
# ------------------------------------------------------------------------------
set -e

# =========================
# Configuration
# =========================
AIRFLOW_HOME="${AIRFLOW_HOME:-$PWD/airflow}"

# =========================
# Stop Airflow services
# =========================
for service in api-server dag-processor scheduler triggerer; do
    pid_file="$AIRFLOW_HOME/$service.pid"
    # Check if PID file exists
    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        # Check if process is running
        if ps -p $pid > /dev/null 2>&1; then
            # Terminate the process
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
