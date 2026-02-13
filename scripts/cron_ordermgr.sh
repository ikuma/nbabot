#!/bin/bash
# Order lifecycle manager — called by launchd every 2 minutes.
#
# Monitors placed orders for fills, manages TTL-based cancel/re-place.
# Only active in live mode (paper/dry-run exits immediately).
#
# Logs to data/logs/ordermgr-YYYY-MM-DD.log

set -euo pipefail

LOCKDIR="/tmp/nbabot-ordermgr.lock"

# 多重起動防止 (mkdir はアトミック)
if ! mkdir "$LOCKDIR" 2>/dev/null; then
    exit 0
fi
trap 'rmdir "$LOCKDIR"' EXIT

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${PROJECT_DIR}/.venv/bin/python"
LOG_DIR="${PROJECT_DIR}/data/logs"

cd "$PROJECT_DIR"

mkdir -p "$LOG_DIR"

DATE=$(date +%Y-%m-%d)
LOG_FILE="${LOG_DIR}/ordermgr-${DATE}.log"

# ログローテーション (30日超ファイル削除)
find "$LOG_DIR" -name "ordermgr-*.log" -mtime +30 -delete 2>/dev/null || true

caffeinate -i "$PYTHON" "${PROJECT_DIR}/scripts/order_tick.py" >> "$LOG_FILE" 2>&1
