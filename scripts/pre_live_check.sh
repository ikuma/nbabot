#!/bin/bash
# Pre-live safety checks for nbabot.
#
# Runs:
#   1) Focused regression tests
#   2) DB invariants (no phantom settle)
#   3) Launchd/heartbeat health checks
#   4) Optional scheduler shadow run (dry-run/paper)
#
# Usage:
#   bash scripts/pre_live_check.sh
#   bash scripts/pre_live_check.sh --with-shadow
#   bash scripts/pre_live_check.sh --with-shadow --shadow-mode paper
#   bash scripts/pre_live_check.sh --with-shadow --shadow-mode dry-run --shadow-date 2026-02-14

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${PROJECT_DIR}/.venv/bin/python"
DB_PATH="${PROJECT_DIR}/data/paper_trades.db"

WITH_SHADOW=0
SHADOW_MODE="dry-run" # dry-run is default to reduce side effects
SHADOW_DATE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --with-shadow)
            WITH_SHADOW=1
            shift
            ;;
        --shadow-mode)
            SHADOW_MODE="${2:-}"
            if [[ "${SHADOW_MODE}" != "dry-run" && "${SHADOW_MODE}" != "paper" ]]; then
                echo "ERROR: --shadow-mode must be 'dry-run' or 'paper'"
                exit 2
            fi
            shift 2
            ;;
        --shadow-date)
            SHADOW_DATE="${2:-}"
            shift 2
            ;;
        *)
            echo "ERROR: unknown option: $1"
            exit 2
            ;;
    esac
done

if [[ ! -x "${PYTHON}" ]]; then
    echo "ERROR: python not found: ${PYTHON}"
    exit 2
fi

if [[ ! -f "${DB_PATH}" ]]; then
    echo "ERROR: DB not found: ${DB_PATH}"
    exit 2
fi

if ! command -v sqlite3 >/dev/null 2>&1; then
    echo "ERROR: sqlite3 not found"
    exit 2
fi

FAILURES=0

section() {
    echo
    echo "== $1 =="
}

record_failure() {
    FAILURES=$((FAILURES + 1))
    echo "FAIL: $1"
}

section "Focused Tests"
if ! "${PYTHON}" -m pytest -q \
    "${PROJECT_DIR}/tests/test_settle.py" \
    "${PROJECT_DIR}/tests/test_order_manager.py" \
    "${PROJECT_DIR}/tests/test_merge_executor_phase45.py" \
    "${PROJECT_DIR}/tests/test_hedge_executor.py" \
    "${PROJECT_DIR}/tests/test_nba_schedule.py"; then
    record_failure "pytest failed"
fi

if [[ ${WITH_SHADOW} -eq 1 ]]; then
    section "Scheduler Shadow Run (${SHADOW_MODE})"
    SHADOW_CMD=("${PYTHON}" "${PROJECT_DIR}/scripts/schedule_trades.py" "--execution" "${SHADOW_MODE}" "--no-settle")
    if [[ -n "${SHADOW_DATE}" ]]; then
        SHADOW_CMD+=("--date" "${SHADOW_DATE}")
    fi
    echo "Running: ${SHADOW_CMD[*]}"
    if ! "${SHADOW_CMD[@]}"; then
        record_failure "scheduler shadow run failed"
    fi
fi

section "DB Invariants"
BAD_SETTLE="$(sqlite3 "${DB_PATH}" "
SELECT COUNT(*)
FROM results r
JOIN signals s ON s.id = r.signal_id
WHERE s.order_status NOT IN ('paper', 'filled');
")"
echo "bad_settle=${BAD_SETTLE}"
if [[ "${BAD_SETTLE}" != "0" ]]; then
    record_failure "found settled results from non-settleable order_status"
fi

echo "trade_jobs status:"
sqlite3 -header -column "${DB_PATH}" \
    "SELECT status, job_side, COUNT(*) AS n FROM trade_jobs GROUP BY status, job_side ORDER BY n DESC, status ASC;"

echo "signals order_status:"
sqlite3 -header -column "${DB_PATH}" \
    "SELECT order_status, signal_role, COUNT(*) AS n FROM signals GROUP BY order_status, signal_role ORDER BY n DESC, order_status ASC;"

section "Runtime Health"
if command -v launchctl >/dev/null 2>&1; then
    JOBS="$(launchctl list | grep nbabot || true)"
    if [[ -z "${JOBS}" ]]; then
        record_failure "launchctl has no nbabot jobs"
    else
        echo "${JOBS}"
    fi
else
    echo "WARN: launchctl not available on this host, skipping launchd checks"
fi

HEARTBEAT_OUTPUT="$("${PYTHON}" - <<PY
from datetime import datetime, timezone
from pathlib import Path

from src.config import settings

project = Path("${PROJECT_DIR}")
paths = [project / "data" / "heartbeat"]
if settings.execution_mode == "live" and settings.order_manager_enabled:
    paths.append(project / "data" / "heartbeat_ordermgr")

now = datetime.now(timezone.utc)
for p in paths:
    if not p.exists():
        print(f"MISSING {p}")
        continue
    mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    age_min = (now - mtime).total_seconds() / 60.0
    status = "OK" if age_min <= 35 else "STALE"
    print(f"{status} {p} age_min={age_min:.1f}")
PY
)"
echo "${HEARTBEAT_OUTPUT}"
if grep -q "^MISSING " <<<"${HEARTBEAT_OUTPUT}"; then
    record_failure "heartbeat file missing"
fi
if grep -q "^STALE " <<<"${HEARTBEAT_OUTPUT}"; then
    record_failure "heartbeat stale (>35 min)"
fi

section "Result"
if [[ ${FAILURES} -gt 0 ]]; then
    echo "Pre-live check failed: ${FAILURES} issue(s)"
    exit 1
fi
echo "Pre-live check passed"
