#!/bin/bash
# ------------------------------------------------------------------------------
# Script Name: run_docker_airflow.sh
# Purpose: Starts Airflow services using Docker Compose.
# Usage: bash run_docker_airflow.sh
# ------------------------------------------------------------------------------
set -e

# =========================
# Configuration
# =========================
COMPOSE_FILE="docker-compose.yaml"
AIRFLOW_WEB_PORT=8080

# =========================
# Stop any running containers
# =========================
echo "Stopping any running Airflow containers..."
docker compose -f $COMPOSE_FILE down

# =========================
# Initialize DB + create admin user
# =========================
echo "Initializing Airflow DB and creating admin user..."
docker compose -f $COMPOSE_FILE run --rm airflow-init

# =========================
# Start services in detached mode
# =========================
echo "Starting Airflow webserver and scheduler in detached mode..."
docker compose -f $COMPOSE_FILE up -d

# =========================
# Wait a few seconds for containers to start
# =========================
echo "Waiting 10 seconds for containers to stabilize..."
sleep 10

# =========================
# Check container status
# =========================
echo "Airflow containers status:"
docker compose -f $COMPOSE_FILE ps

# =========================
# Instructions for browser
# =========================
echo ""
echo "Airflow should now be running."
echo "Open your browser and go to: http://localhost:$AIRFLOW_WEB_PORT"
echo "Login with:"
echo "  Username: admin"
echo "  Password: admin"
echo ""
echo "To stop Airflow later, run:"
echo "  docker compose -f $COMPOSE_FILE down"
