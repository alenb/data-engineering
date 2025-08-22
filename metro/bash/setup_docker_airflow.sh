#!/bin/bash
# ------------------------------------------------------------------------------
# Script Name: setup_docker_airflow.sh
# Purpose: Builds and initializes Airflow environment using Docker Compose.
# Usage: bash setup_docker_airflow.sh
# ------------------------------------------------------------------------------
set -e

# =========================
# Build Docker image
# =========================
echo "=== Build updated Airflow Docker image ==="
docker build -t metro-airflow:latest .