#!/bin/bash
set -e

echo "=== Build updated Airflow Docker image ==="
docker build -t metro-airflow:latest .