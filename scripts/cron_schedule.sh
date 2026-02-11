#!/bin/bash
# Per-game trade scheduler — called by launchd every 15 minutes, 24/7.
#
# Best practice: "dumb scheduler, smart worker"
# - cron は単純なハートビートとして常時実行
# - スクリプト内で today + tomorrow (ET) のゲームを探索
# - 実行窓 (execute_after <= now < execute_before) 内のジョブのみ処理
# - 実行窓外: NBA API + DB チェック (~3秒) で早期終了
#
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

# Pydantic Settings が .env を CWD から探すため、プロジェクトルートに移動必須
cd "$PROJECT_DIR"

mkdir -p "$LOG_DIR"

DATE=$(date +%Y-%m-%d)
LOG_FILE="${LOG_DIR}/scheduler-${DATE}.log"

# ログローテーション (30日超ファイル削除)
find "$LOG_DIR" -name "scheduler-*.log" -mtime +30 -delete 2>/dev/null || true

# caffeinate -i: スクリプト実行中の macOS idle sleep を防止
# (プロセス終了時に自動解除 — バッテリ影響は最小)
caffeinate -i "$PYTHON" "${PROJECT_DIR}/scripts/schedule_trades.py" >> "$LOG_FILE" 2>&1
