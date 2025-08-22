#!/bin/bash
# ------------------------------------------------------------------------------
# Script Name: setup_airflow.sh
# Purpose: Installs dependencies and initializes Airflow locally.
# Usage: bash setup_airflow.sh
# ------------------------------------------------------------------------------
set -e

# =========================
# Configuration
# =========================
VENV_DIR="venv-airflow"
PROJECT_DIR="$(pwd)"
# Change to the correct path
PYTHONPATH="/mnt/c/Wamp.NET/sites/data-engineering/metro:$PYTHONPATH"
AIRFLOW_HOME="${PROJECT_DIR}/airflow"
DAGS_DIR="${PROJECT_DIR}/dags"
AIRFLOW_YAML="${PROJECT_DIR}/airflow.yaml"
AIRFLOW_VERSION="3.0.4"

# =========================
# Create virtual environment
# =========================
if [ ! -d "$VENV_DIR" ]; then
    python -m venv "$VENV_DIR"
    echo "Created virtual environment at $VENV_DIR"
else
    echo "Virtual environment already exists at $VENV_DIR"
fi

# =========================
# Activate virtual environment
# =========================
source "${VENV_DIR}/bin/activate"

# =========================
# Upgrade pip
# =========================
python.exe -m pip install --upgrade pip

# =========================
# Install Apache Airflow
# =========================
INSTALLED_AIRFLOW=$(pip show apache-airflow 2>/dev/null | grep Version | awk '{print $2}' || echo "")
if [ "$INSTALLED_AIRFLOW" != "$AIRFLOW_VERSION" ]; then
    PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    echo "Installed Apache Airflow $AIRFLOW_VERSION"
else
    echo "Apache Airflow $AIRFLOW_VERSION is already installed"
fi

# =========================
# Configure environment variables
# =========================
ACTIVATE_FILE="${VENV_DIR}/bin/activate"
if ! grep -Fxq "export PYTHONPATH=${PYTHONPATH}" "$ACTIVATE_FILE"; then
    echo "export PYTHONPATH=${PYTHONPATH}" >> "$ACTIVATE_FILE"
    echo "Added PYTHONPATH export to $ACTIVATE_FILE"
fi

if ! grep -Fxq "export AIRFLOW_HOME=${AIRFLOW_HOME}" "$ACTIVATE_FILE"; then
    echo "export AIRFLOW_HOME=${AIRFLOW_HOME}" >> "$ACTIVATE_FILE"
    echo "Added AIRFLOW_HOME export to $ACTIVATE_FILE"
fi

if ! grep -Fxq "export AIRFLOW__CORE__LOAD_EXAMPLES=False" "$ACTIVATE_FILE"; then
    echo "export AIRFLOW__CORE__LOAD_EXAMPLES=False" >> "$ACTIVATE_FILE"
    echo "Added AIRFLOW__CORE__LOAD_EXAMPLES export to $ACTIVATE_FILE"
fi

if ! grep -Fxq "export AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0" "$ACTIVATE_FILE"; then
    echo "export AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0" >> "$ACTIVATE_FILE"
    echo "Added AIRFLOW__WEBSERVER__WEB_SERVER_HOST export to $ACTIVATE_FILE"
fi

if ! grep -Fxq "export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8888" "$ACTIVATE_FILE"; then
    echo "export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8888" >> "$ACTIVATE_FILE"
    echo "Added AIRFLOW__WEBSERVER__WEB_SERVER_PORT export to $ACTIVATE_FILE"
fi

# =========================
# Initialize Airflow database
# =========================
AIRFLOW_DB_FILE="${AIRFLOW_HOME}/airflow.db"
if [ ! -f "$AIRFLOW_DB_FILE" ]; then
    mkdir -p "$AIRFLOW_HOME"
    airflow db migrate
    echo "Initialized Airflow database at $AIRFLOW_DB_FILE"
else
    echo "Airflow database already exists at $AIRFLOW_DB_FILE"
fi

# =========================
# Setup complete
# =========================
echo "Setup complete! Activate your venv with:"
echo "source ${VENV_DIR}/bin/activate"
echo "Then run Airflow commands like 'airflow api-server --host 0.0.0.0 --port 8888'"
