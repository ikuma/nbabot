#!/bin/bash
# Daily NBA calibration scan + auto-settle — called by crontab
# Logs to data/logs/scan-YYYY-MM-DD.log

set -euo pipefail

LOCKDIR="/tmp/nbabot-scan.lock"

# 多重起動防止 (mkdir はアトミック — macOS/Linux 両対応)
if ! mkdir "$LOCKDIR" 2>/dev/null; then
    echo "Already running"
    exit 1
fi
trap 'rmdir "$LOCKDIR"' EXIT

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${PROJECT_DIR}/.venv/bin/python"
LOG_DIR="${PROJECT_DIR}/data/logs"

cd "$PROJECT_DIR"

mkdir -p "$LOG_DIR"

DATE=$(date +%Y-%m-%d)
LOG_FILE="${LOG_DIR}/scan-${DATE}.log"

echo "=== Scan started at $(date) ===" >> "$LOG_FILE"
"$PYTHON" "${PROJECT_DIR}/scripts/scan.py" --mode calibration >> "$LOG_FILE" 2>&1
echo "=== Scan finished at $(date) ===" >> "$LOG_FILE"

echo "=== Auto-settle started at $(date) ===" >> "$LOG_FILE"
"$PYTHON" "${PROJECT_DIR}/scripts/settle.py" --auto >> "$LOG_FILE" 2>&1
echo "=== Auto-settle finished at $(date) ===" >> "$LOG_FILE"
