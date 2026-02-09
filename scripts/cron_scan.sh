#!/bin/bash
# Daily NBA calibration scan + auto-settle — called by crontab
# Logs to data/logs/scan-YYYY-MM-DD.log

set -euo pipefail

PROJECT_DIR="/Users/taro/dev/nbabot"
PYTHON="${PROJECT_DIR}/.venv/bin/python"
LOG_DIR="${PROJECT_DIR}/data/logs"

mkdir -p "$LOG_DIR"

DATE=$(date +%Y-%m-%d)
LOG_FILE="${LOG_DIR}/scan-${DATE}.log"

echo "=== Scan started at $(date) ===" >> "$LOG_FILE"
"$PYTHON" "${PROJECT_DIR}/scripts/scan.py" --mode calibration >> "$LOG_FILE" 2>&1
echo "=== Scan finished at $(date) ===" >> "$LOG_FILE"

echo "=== Auto-settle started at $(date) ===" >> "$LOG_FILE"
"$PYTHON" "${PROJECT_DIR}/scripts/settle.py" --auto >> "$LOG_FILE" 2>&1
echo "=== Auto-settle finished at $(date) ===" >> "$LOG_FILE"
