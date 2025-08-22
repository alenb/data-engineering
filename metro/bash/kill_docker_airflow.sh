#!/bin/bash
# ------------------------------------------------------------------------------
# Script Name: kill_docker_airflow.sh
# Purpose: Stops and removes all Docker containers related to Airflow.
# Usage: bash kill_docker_airflow.sh
# ------------------------------------------------------------------------------
set -e

# =========================
# Configuration
# =========================
COMPOSE_FILE="docker-compose.yaml"
PROJECT_NAME="metro-airflow"

echo "Stopping and removing all Airflow containers..."
# Stop all running containers for this project
docker compose -f $COMPOSE_FILE down

# =========================
# Remove remaining containers
# =========================
CONTAINERS=$(docker ps -a -q --filter "name=$PROJECT_NAME")
if [ -n "$CONTAINERS" ]; then
    echo "Forcefully removing remaining Airflow containers..."
    docker rm -f $CONTAINERS
else
    echo "No remaining Airflow containers found."
fi

# =========================
# Remove Docker network
# =========================
NETWORK=$(docker network ls --filter "name=${PROJECT_NAME}_default" -q)
if [ -n "$NETWORK" ]; then
    echo "Removing Docker network used by Airflow..."
    docker network rm $NETWORK
else
    echo "No Airflow network found."
fi

# =========================
# Summary
# =========================
echo ""
echo "Airflow Docker environment is now completely stopped."
echo "All active sessions and containers have been terminated."
echo "If you want to remove volumes (DB/logs) for a full reset, run:"
echo "  docker compose -f $COMPOSE_FILE down -v"
