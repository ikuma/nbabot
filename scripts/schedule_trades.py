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
from datetime import datetime, timedelta, timezone
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
        process_position_groups_tick,
        refresh_schedule,
    )
    from src.store.db import cancel_expired_jobs
    from src.store.db_path import resolve_db_path

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
    now_et = datetime.now(timezone.utc).astimezone(ET)
    now_utc = datetime.now(timezone.utc).isoformat()
    db_path = resolve_db_path(execution_mode=execution_mode)

    # ゲーム日付: today + tomorrow (ET) の両日を探索 — タイムゾーン境界対策
    # NBA.com のゲーム日付は ET ベースだが、境界付近で日付がずれるケースがある。
    # "dumb scheduler, smart worker" パターン: cron は 24/7 ハートビート、
    # スクリプト内で実行窓 (execute_after/execute_before) 判定するため安全。
    if args.date:
        dates_to_refresh = [args.date]
        game_date = args.date  # 表示用
    else:
        today_et = now_et.strftime("%Y-%m-%d")
        tomorrow_et = (now_et + timedelta(days=1)).strftime("%Y-%m-%d")
        dates_to_refresh = [today_et, tomorrow_et]
        game_date = today_et  # 表示用

    log.info(
        "=== Scheduler tick (dates=%s, execution=%s) ===",
        "+".join(dates_to_refresh),
        execution_mode,
    )
    log.info("DB path: %s", db_path)

    # 0. リスクチェック
    risk_level_name = "GREEN"
    sizing_multiplier = 1.0
    try:
        from src.risk.models import CircuitBreakerLevel
        from src.risk.risk_engine import load_or_compute_risk_state

        risk_state = load_or_compute_risk_state(db_path)
        risk_level_name = risk_state.circuit_breaker_level.name
        sizing_multiplier = risk_state.sizing_multiplier
        log.info(
            "Risk state: level=%s multiplier=%.2f daily_pnl=$%.2f flags=%s",
            risk_level_name,
            sizing_multiplier,
            risk_state.daily_pnl,
            risk_state.flags,
        )

        if risk_state.circuit_breaker_level >= CircuitBreakerLevel.RED:
            log.warning("Circuit breaker RED — skipping to settle-only mode")
            # RED: settle のみ実行して通知して終了
            if not args.no_settle:
                try:
                    from src.settlement.settler import auto_settle

                    settle_summary = auto_settle()
                    if settle_summary.settled:
                        log.info("Auto-settle: %s", settle_summary.format_summary())
                except Exception:
                    log.exception("Auto-settle failed")

            try:
                from src.notifications.telegram import send_message

                send_message(
                    f"*Circuit Breaker RED*\nTrading halted. "
                    f"Daily PnL: ${risk_state.daily_pnl:+.2f}\n"
                    f"Flags: {', '.join(risk_state.flags) or 'none'}"
                )
            except Exception:
                log.exception("Telegram notification failed")

            # DCA 強制停止
            from src.store.db import force_stop_dca_jobs

            stopped = force_stop_dca_jobs(db_path=db_path)
            if stopped:
                log.info("Force-stopped %d DCA jobs (RED)", stopped)

            # snapshot 保存
            from src.store.db import save_risk_snapshot

            save_risk_snapshot(risk_state, db_path=db_path)
            return

        # YELLOW 以上: DCA 新規エントリー停止
        if risk_state.circuit_breaker_level >= CircuitBreakerLevel.YELLOW:
            from src.store.db import force_stop_dca_jobs

            stopped = force_stop_dca_jobs(db_path=db_path)
            if stopped:
                log.info("Force-stopped %d DCA jobs (YELLOW+)", stopped)

    except Exception:
        log.exception("Risk check failed — continuing in degraded mode")
        sizing_multiplier = 0.5

    # 1. スケジュール更新 — today + tomorrow (ET) を探索
    new_jobs = 0
    for d in dates_to_refresh:
        new_jobs += refresh_schedule(d, db_path=db_path)
    log.info("Schedule refresh (%s): %d new job(s)", "+".join(dates_to_refresh), new_jobs)

    # 2. 期限切れ処理
    expired = cancel_expired_jobs(now_utc, db_path=db_path)
    if expired:
        log.info("Expired %d job(s)", expired)

    # 3. 窓内ジョブ実行 (初回エントリー)
    results = process_eligible_jobs(
        execution_mode,
        db_path=db_path,
        sizing_multiplier=sizing_multiplier,
    )

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
    dca_results = process_dca_active_jobs(execution_mode, db_path=db_path)
    dca_executed = [r for r in dca_results if r.status == "executed"]
    dca_failed = [r for r in dca_results if r.status == "failed"]

    if dca_results:
        log.info(
            "DCA results: executed=%d failed=%d",
            len(dca_executed),
            len(dca_failed),
        )

    # 3c. MERGE 処理 (bothside DCA 完了後)
    merge_results = process_merge_eligible(execution_mode, db_path=db_path)
    merge_executed = [r for r in merge_results if r.status == "executed"]
    merge_failed = [r for r in merge_results if r.status == "failed"]

    if merge_results:
        log.info(
            "MERGE results: executed=%d failed=%d",
            len(merge_executed),
            len(merge_failed),
        )

    # 3d. PositionGroup 状態機械更新 (Track B)
    position_group_transitions = process_position_groups_tick(db_path=db_path)
    if position_group_transitions:
        log.info("PositionGroup transitions: %d", position_group_transitions)

    # 4. 決済 (オプション)
    if not args.no_settle:
        try:
            from src.settlement.settler import auto_settle

            settle_summary = auto_settle(db_path=db_path)
            if settle_summary.settled:
                log.info("Auto-settle: %s", settle_summary.format_summary())
        except Exception:
            log.exception("Auto-settle failed (continuing)")

    # 4b. リスク snapshot 保存
    try:
        from src.risk.risk_engine import invalidate_cache, load_or_compute_risk_state
        from src.store.db import save_risk_snapshot

        invalidate_cache()
        risk_state = load_or_compute_risk_state(db_path)
        save_risk_snapshot(risk_state, db_path=db_path)

        # レベル変更があった場合に通知
        if risk_state.circuit_breaker_level.name != risk_level_name:
            try:
                from src.notifications.telegram import send_message

                new_level = risk_state.circuit_breaker_level.name
                send_message(
                    f"*Risk Level Changed: {risk_level_name} → "
                    f"{new_level}*\n"
                    f"Daily PnL: ${risk_state.daily_pnl:+.2f} | "
                    f"Multiplier: {risk_state.sizing_multiplier:.2f}"
                )
            except Exception:
                log.exception("Risk alert notification failed")
    except Exception:
        log.exception("Risk snapshot save failed")

    # 5. Telegram 通知
    try:
        summary_text = format_tick_summary(
            results,
            game_date,
            expired,
            dca_results=dca_results,
            merge_results=merge_results,
            execution_mode=execution_mode,
            db_path=db_path,
        )
        if summary_text:
            from src.notifications.telegram import send_message

            send_message(summary_text)
    except Exception:
        log.exception("Telegram notification failed")

    # Heartbeat (watchdog 死活監視用)
    heartbeat = Path(__file__).resolve().parent.parent / "data" / "heartbeat"
    heartbeat.write_text(datetime.now(timezone.utc).isoformat() + "\n")

    # サマリー出力
    print(f"\n{'=' * 50}")
    print(f"  Scheduler tick: {game_date} [{execution_mode}]")
    print(f"  Risk: {risk_level_name} | Sizing: {sizing_multiplier:.2f}x")
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
