#!/bin/bash
set -euo pipefail

# --- Activate venv ---
if [[ -d "venv-airflow" ]]; then
  echo "Activating virtualenv..."
  source venv-airflow/bin/activate
else
  echo "venv-airflow not found"; exit 1
fi

# --- Paths ---
AIRFLOW_HOME="${AIRFLOW_HOME:-$PWD/airflow}"
LOG_DIR="$AIRFLOW_HOME/logs/daemon"
mkdir -p "$LOG_DIR"

# --- Create default admin user if it doesn't exist ---
# echo "Checking if admin user exists..."
# if ! airflow users list | grep -q "admin"; then
#     echo "Creating default admin user..."
#     airflow users create \
#         --username admin \
#         --firstname Admin \
#         --lastname User \
#         --role Admin \
#         --email admin@example.com \
#         --password admin
#     echo "Default user created: username='admin', password='admin'"
# else
#     echo "Admin user already exists"
# fi

# --- Start services ---
echo "Starting API server (with single worker)..."
nohup airflow api-server --host 0.0.0.0 --port 8888 --workers 1 \
  >"$LOG_DIR/api-server.out" 2>"$LOG_DIR/api-server.err" & echo $! > "$AIRFLOW_HOME/api-server.pid"

sleep 2

echo "Starting DAG processor..."
nohup airflow dag-processor \
  >"$LOG_DIR/dag-processor.out" 2>"$LOG_DIR/dag-processor.err" & echo $! > "$AIRFLOW_HOME/dag-processor.pid"

sleep 2

echo "Starting Scheduler..."
nohup airflow scheduler \
  >"$LOG_DIR/scheduler.out" 2>"$LOG_DIR/scheduler.err" & echo $! > "$AIRFLOW_HOME/scheduler.pid"

sleep 2

echo "Starting Triggerer..."
nohup airflow triggerer \
  >"$LOG_DIR/triggerer.out" 2>"$LOG_DIR/triggerer.err" & echo $! > "$AIRFLOW_HOME/triggerer.pid"

# --- Report status ---
sleep 3
echo "PIDs:"
echo "  api-server:     $(cat "$AIRFLOW_HOME/api-server.pid" 2>/dev/null || echo 'n/a')"
echo "  dag-processor:  $(cat "$AIRFLOW_HOME/dag-processor.pid" 2>/dev/null || echo 'n/a')"
echo "  scheduler:      $(cat "$AIRFLOW_HOME/scheduler.pid" 2>/dev/null || echo 'n/a')"
echo "  triggerer:      $(cat "$AIRFLOW_HOME/triggerer.pid" 2>/dev/null || echo 'n/a')"

echo -e "\n=== Airflow Setup Complete ==="
# echo "Login credentials:"
echo "  URL:      http://127.0.0.1:8888"
# echo "  Username: admin"
# echo "  Password: admin"
echo ""
echo "Tail logs, e.g.: tail -f $LOG_DIR/api-server.out"