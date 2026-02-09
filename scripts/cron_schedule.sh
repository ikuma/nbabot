#!/bin/bash
# Per-game trade scheduler — called by crontab every 5 minutes
# Logs to data/logs/scheduler-YYYY-MM-DD.log

set -euo pipefail

# 多重起動防止 (別ロックファイルで cron_scan.sh と共存可能)
exec 200>/tmp/nbabot-scheduler.lock
flock -n 200 || exit 0

PROJECT_DIR="/Users/taro/dev/nbabot"
PYTHON="${PROJECT_DIR}/.venv/bin/python"
LOG_DIR="${PROJECT_DIR}/data/logs"

mkdir -p "$LOG_DIR"

DATE=$(date +%Y-%m-%d)
LOG_FILE="${LOG_DIR}/scheduler-${DATE}.log"

"$PYTHON" "${PROJECT_DIR}/scripts/schedule_trades.py" >> "$LOG_FILE" 2>&1
