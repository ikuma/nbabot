#!/bin/bash
# Install nbabot launchd jobs (idempotent).
#
# Usage: bash scripts/install_launchd.sh
#
# Actions:
#   1. Unload existing jobs (if any)
#   2. Copy plist files to ~/Library/LaunchAgents/
#   3. Load jobs via launchctl bootstrap
#   4. Remove old crontab entries (if any)

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
DOMAIN="gui/$(id -u)"

PLISTS=(
    "com.nbabot.scheduler"
    "com.nbabot.watchdog"
)

echo "=== nbabot launchd installer ==="
echo "Project: $PROJECT_DIR"
echo ""

mkdir -p "$LAUNCH_AGENTS_DIR"

for label in "${PLISTS[@]}"; do
    plist_src="${PROJECT_DIR}/launchd/${label}.plist"
    plist_dst="${LAUNCH_AGENTS_DIR}/${label}.plist"

    if [ ! -f "$plist_src" ]; then
        echo "ERROR: $plist_src not found"
        exit 1
    fi

    # 1. 既存ジョブをアンロード (エラーは無視)
    echo "Unloading ${label} (if exists)..."
    launchctl bootout "${DOMAIN}/${label}" 2>/dev/null || true

    # 2. plist コピー
    echo "Copying ${label}.plist → ${LAUNCH_AGENTS_DIR}/"
    cp "$plist_src" "$plist_dst"

    # 3. ロード
    echo "Loading ${label}..."
    launchctl bootstrap "$DOMAIN" "$plist_dst"

    echo "  ✓ ${label} loaded"
    echo ""
done

# 4. crontab から旧エントリを削除
if crontab -l 2>/dev/null | grep -q "cron_schedule.sh"; then
    echo "Removing old crontab entries..."
    crontab -l 2>/dev/null | grep -v "cron_schedule.sh" | crontab -
    echo "  ✓ crontab cleaned"
else
    echo "No crontab entries to clean"
fi

echo ""
echo "=== Done ==="
echo "Verify with: launchctl list | grep nbabot"
echo "Kick start:  launchctl kickstart ${DOMAIN}/com.nbabot.scheduler"
