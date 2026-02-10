#!/bin/bash
# Per-game trade scheduler — called by crontab every 2 minutes (DCA 対応)
# Logs to data/logs/scheduler-YYYY-MM-DD.log

set -euo pipefail

LOCKDIR="/tmp/nbabot-scheduler.lock"

# 多重起動防止 (mkdir はアトミック — macOS/Linux 両対応)
if ! mkdir "$LOCKDIR" 2>/dev/null; then
    exit 0
fi
trap 'rmdir "$LOCKDIR"' EXIT

PROJECT_DIR="/Users/taro/dev/nbabot"
PYTHON="${PROJECT_DIR}/.venv/bin/python"
LOG_DIR="${PROJECT_DIR}/data/logs"

mkdir -p "$LOG_DIR"

DATE=$(date +%Y-%m-%d)
LOG_FILE="${LOG_DIR}/scheduler-${DATE}.log"

# ログローテーション (30日超ファイル削除)
find "$LOG_DIR" -name "scheduler-*.log" -mtime +30 -delete 2>/dev/null || true

"$PYTHON" "${PROJECT_DIR}/scripts/schedule_trades.py" >> "$LOG_FILE" 2>&1
