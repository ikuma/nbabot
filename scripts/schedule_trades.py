#!/usr/bin/env python3
"""Per-game trade scheduler: cron-driven execution at optimal timing.

Usage:
    # Dry-run (no orders, no DB signals)
    python scripts/schedule_trades.py --execution dry-run

    # Paper mode (log signals, no real orders)
    python scripts/schedule_trades.py --execution paper

    # Live mode (real orders)
    python scripts/schedule_trades.py --execution live

    # Override date (for testing)
    python scripts/schedule_trades.py --date 2026-02-10 --execution dry-run
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")


def main() -> None:
    from src.config import settings
    from src.scheduler.trade_scheduler import (
        format_tick_summary,
        process_dca_active_jobs,
        process_eligible_jobs,
        process_merge_eligible,
        refresh_schedule,
    )
    from src.store.db import cancel_expired_jobs

    parser = argparse.ArgumentParser(description="Per-game trade scheduler")
    parser.add_argument(
        "--execution",
        choices=["paper", "live", "dry-run"],
        default=None,
        help="Execution mode (default: from settings.execution_mode)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Game date YYYY-MM-DD (default: today ET)",
    )
    parser.add_argument(
        "--no-settle",
        action="store_true",
        help="Skip auto-settlement",
    )
    args = parser.parse_args()

    execution_mode = args.execution or settings.execution_mode
    game_date = args.date or datetime.now(timezone.utc).astimezone(ET).strftime("%Y-%m-%d")
    now_utc = datetime.now(timezone.utc).isoformat()

    log.info(
        "=== Scheduler tick (date=%s, execution=%s) ===",
        game_date,
        execution_mode,
    )

    # 1. スケジュール更新
    new_jobs = refresh_schedule(game_date)
    log.info("Schedule refresh: %d new job(s)", new_jobs)

    # 2. 期限切れ処理
    expired = cancel_expired_jobs(now_utc)
    if expired:
        log.info("Expired %d job(s)", expired)

    # 3. 窓内ジョブ実行 (初回エントリー)
    results = process_eligible_jobs(execution_mode)

    executed = [r for r in results if r.status == "executed"]
    skipped = [r for r in results if r.status == "skipped"]
    failed = [r for r in results if r.status == "failed"]

    log.info(
        "Tick results: executed=%d skipped=%d failed=%d",
        len(executed),
        len(skipped),
        len(failed),
    )

    # 3b. DCA アクティブジョブ処理
    dca_results = process_dca_active_jobs(execution_mode)
    dca_executed = [r for r in dca_results if r.status == "executed"]
    dca_failed = [r for r in dca_results if r.status == "failed"]

    if dca_results:
        log.info(
            "DCA results: executed=%d failed=%d",
            len(dca_executed),
            len(dca_failed),
        )

    # 3c. MERGE 処理 (bothside DCA 完了後)
    merge_results = process_merge_eligible(execution_mode)
    merge_executed = [r for r in merge_results if r.status == "executed"]
    merge_failed = [r for r in merge_results if r.status == "failed"]

    if merge_results:
        log.info(
            "MERGE results: executed=%d failed=%d",
            len(merge_executed),
            len(merge_failed),
        )

    # 4. 決済 (オプション)
    if not args.no_settle:
        try:
            from scripts.settle import auto_settle

            settle_summary = auto_settle()
            if settle_summary.settled:
                log.info("Auto-settle: %s", settle_summary.format_summary())
        except Exception:
            log.exception("Auto-settle failed (continuing)")

    # 5. Telegram 通知
    try:
        summary_text = format_tick_summary(
            results,
            game_date,
            expired,
            dca_results=dca_results,
            merge_results=merge_results,
        )
        if summary_text:
            from src.notifications.telegram import send_message

            send_message(summary_text)
    except Exception:
        log.exception("Telegram notification failed")

    # サマリー出力
    print(f"\n{'=' * 50}")
    print(f"  Scheduler tick: {game_date} [{execution_mode}]")
    print(f"  New jobs: {new_jobs} | Expired: {expired}")
    print(f"  Executed: {len(executed)} | Skipped: {len(skipped)} | Failed: {len(failed)}")
    if dca_executed:
        print(f"  DCA entries: {len(dca_executed)}")
    if merge_executed:
        print(f"  MERGE: {len(merge_executed)}")
    for r in executed:
        print(f"    OK: {r.event_slug} → signal #{r.signal_id}")
    for r in dca_executed:
        print(f"    DCA: {r.event_slug} → signal #{r.signal_id}")
    for r in merge_executed:
        print(f"    MERGE: {r.event_slug}")
    for r in failed + dca_failed + merge_failed:
        print(f"    FAIL: {r.event_slug} → {r.error}")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
