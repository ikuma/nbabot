"""Per-game trade scheduler: cron-driven state machine with SQLite job queue.

Each NBA game gets a trade_job with an execution window (tipoff - 2h to tipoff).
A 5-minute cron tick calls refresh → expire → process, executing orders only
for games whose window is currently open.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from src.config import settings
from src.scheduler.dca_executor import process_dca_active_jobs  # noqa: F401
from src.scheduler.hedge_executor import _schedule_hedge_job
from src.scheduler.job_executor import JobResult  # noqa: F401
from src.scheduler.merge_executor import process_merge_eligible  # noqa: F401
from src.store.db import (
    DEFAULT_DB_PATH,
    get_eligible_jobs,
    get_executing_jobs,
    get_job_summary,
    has_signal_for_slug,
    update_job_status,
    upsert_trade_job,
)

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# 1. refresh_schedule — NBA.com → trade_jobs
# ---------------------------------------------------------------------------


def refresh_schedule(
    game_date: str,
    db_path: str | None = None,
) -> int:
    """Fetch games for game_date and upsert into trade_jobs.

    Returns the number of newly inserted jobs.
    """
    from src.connectors.nba_schedule import fetch_games_for_date, fetch_todays_games
    from src.connectors.team_mapping import build_event_slug

    path = db_path or DEFAULT_DB_PATH

    # 今日の日付判定
    today_str = datetime.now(timezone.utc).astimezone(ET).strftime("%Y-%m-%d")
    if game_date == today_str:
        games = fetch_todays_games()
    else:
        games = fetch_games_for_date(game_date)

    if not games:
        logger.info("No games found for %s", game_date)
        return 0

    window_hours = settings.schedule_window_hours
    inserted = 0

    for game in games:
        # 終了済み試合はスキップ
        if game.game_status == 3:
            continue

        slug = build_event_slug(game.away_team, game.home_team, game_date)
        if not slug:
            logger.warning(
                "Cannot build slug for %s @ %s",
                game.away_team,
                game.home_team,
            )
            continue

        # game_time_utc のパース
        game_time_utc = game.game_time_utc
        if not game_time_utc:
            logger.warning("No game_time_utc for %s", slug)
            continue

        try:
            gt = datetime.fromisoformat(game_time_utc.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            logger.warning("Bad game_time_utc '%s' for %s", game_time_utc, slug)
            continue

        execute_after = (gt - timedelta(hours=window_hours)).isoformat()
        execute_before = gt.isoformat()

        was_inserted = upsert_trade_job(
            game_date=game_date,
            event_slug=slug,
            home_team=game.home_team,
            away_team=game.away_team,
            game_time_utc=game_time_utc,
            execute_after=execute_after,
            execute_before=execute_before,
            db_path=path,
        )
        if was_inserted:
            inserted += 1
            logger.info(
                "Job created: %s window=[%s, %s)",
                slug,
                execute_after,
                execute_before,
            )

    summary = get_job_summary(game_date, db_path=path)
    logger.info(
        "Schedule refresh for %s: +%d new | pending=%d executing=%d executed=%d "
        "skipped=%d failed=%d expired=%d",
        game_date,
        inserted,
        summary.pending,
        summary.executing,
        summary.executed,
        summary.skipped,
        summary.failed,
        summary.expired,
    )
    return inserted


# ---------------------------------------------------------------------------
# 2. recover_executing_jobs — crash recovery
# ---------------------------------------------------------------------------


def recover_executing_jobs(db_path: str | None = None) -> int:
    """Recover jobs stuck in 'executing' state after a crash.

    If signals table has a placed/filled record for the event_slug,
    mark the job as 'executed' (order already went through).
    Otherwise, reset to 'pending' for retry.
    """
    path = db_path or DEFAULT_DB_PATH
    stuck = get_executing_jobs(db_path=path)
    if not stuck:
        return 0

    recovered = 0
    for job in stuck:
        if has_signal_for_slug(job.event_slug, db_path=path):
            # 発注済み — executed に
            update_job_status(job.id, "executed", db_path=path)
            logger.info(
                "Recovered job %d (%s): executing → executed (signal found)",
                job.id,
                job.event_slug,
            )
        else:
            # 発注未完了 — pending に戻す
            update_job_status(job.id, "pending", db_path=path)
            logger.info(
                "Recovered job %d (%s): executing → pending (no signal)",
                job.id,
                job.event_slug,
            )
        recovered += 1

    return recovered


# ---------------------------------------------------------------------------
# 3. process_eligible_jobs — scan + place orders (dispatcher)
# ---------------------------------------------------------------------------


def process_eligible_jobs(
    execution_mode: str = "paper",
    db_path: str | None = None,
) -> list[JobResult]:
    """Process trade jobs whose execution window is currently open.

    Args:
        execution_mode: "paper" (log signal only), "live" (real orders),
                       or "dry-run" (log output only).
    """
    from src.connectors.polymarket import fetch_moneyline_for_game, place_limit_buy
    from src.scheduler.hedge_executor import process_hedge_job
    from src.scheduler.job_executor import process_single_job
    from src.store.db import log_signal, update_order_status
    from src.strategy.calibration_scanner import scan_calibration

    path = db_path or DEFAULT_DB_PATH
    now_utc = datetime.now(timezone.utc).isoformat()

    # クラッシュ回復
    recovered = recover_executing_jobs(db_path=path)
    if recovered:
        logger.info("Recovered %d executing jobs", recovered)

    eligible = get_eligible_jobs(now_utc, db_path=path)
    if not eligible:
        logger.info("No eligible jobs in execution window")
        return []

    logger.info("Found %d eligible job(s)", len(eligible))

    # 暴走防止: 1 tick あたりの最大発注数
    max_per_tick = settings.max_orders_per_tick
    results: list[JobResult] = []

    orders_this_tick = 0
    for job in eligible:
        if orders_this_tick >= max_per_tick:
            logger.warning(
                "max_orders_per_tick (%d) reached, deferring remaining jobs",
                max_per_tick,
            )
            break

        # Hedge ジョブは専用処理
        if job.job_side == "hedge":
            result = process_hedge_job(
                job,
                execution_mode,
                path,
                fetch_moneyline_for_game,
                log_signal,
                place_limit_buy,
                update_order_status,
            )
        else:
            jr, bothside_opp = process_single_job(
                job,
                execution_mode,
                path,
                fetch_moneyline_for_game,
                scan_calibration,
                log_signal,
                place_limit_buy,
                update_order_status,
            )
            result = jr

            # bothside: hedge ジョブをスケジュール
            if bothside_opp and bothside_opp.hedge is not None:
                _schedule_hedge_job(job, bothside_opp, "", path)

        results.append(result)
        if result.status == "executed":
            orders_this_tick += 1

    return results


# ---------------------------------------------------------------------------
# 4. format_tick_summary — Telegram 通知用
# ---------------------------------------------------------------------------


def format_tick_summary(
    results: list[JobResult],
    game_date: str,
    expired_count: int = 0,
    recovered_count: int = 0,
    dca_results: list[JobResult] | None = None,
    merge_results: list[JobResult] | None = None,
    db_path: str | None = None,
) -> str | None:
    """Format a tick summary for Telegram. Returns None if nothing happened."""
    dca_results = dca_results or []
    merge_results = merge_results or []
    all_results = results + dca_results + merge_results

    if not all_results and expired_count == 0 and recovered_count == 0:
        return None

    path = db_path or DEFAULT_DB_PATH
    summary = get_job_summary(game_date, db_path=path)

    executed = [r for r in results if r.status == "executed"]
    skipped = [r for r in results if r.status == "skipped"]
    failed = [r for r in results if r.status == "failed"]
    dca_executed = [r for r in dca_results if r.status == "executed"]
    dca_failed = [r for r in dca_results if r.status == "failed"]
    merge_executed = [r for r in merge_results if r.status == "executed"]
    merge_failed = [r for r in merge_results if r.status == "failed"]

    lines = [f"*Scheduler Tick* ({game_date})"]

    if executed:
        lines.append(f"Executed: {len(executed)}")
        for r in executed:
            lines.append(f"  #{r.signal_id} {r.event_slug}")
    if dca_executed:
        lines.append(f"DCA entries: {len(dca_executed)}")
        for r in dca_executed:
            lines.append(f"  DCA #{r.signal_id} {r.event_slug}")
    if merge_executed:
        lines.append(f"MERGE: {len(merge_executed)}")
        for r in merge_executed:
            lines.append(f"  MERGE {r.event_slug}")
    if skipped:
        lines.append(f"Skipped: {len(skipped)}")
    if failed or dca_failed or merge_failed:
        total_failed = failed + dca_failed + merge_failed
        lines.append(f"Failed: {len(total_failed)}")
        for r in total_failed:
            lines.append(f"  {r.event_slug}: {r.error}")
    if expired_count:
        lines.append(f"Expired: {expired_count}")

    lines.append(
        f"\nJobs: P={summary.pending} X={summary.executing} "
        f"OK={summary.executed} S={summary.skipped} "
        f"F={summary.failed} E={summary.expired} "
        f"DCA={summary.dca_active}"
    )

    # 何も発注していなければ通知しない (ノイズ削減)
    if (
        not executed
        and not dca_executed
        and not merge_executed
        and not failed
        and not dca_failed
        and not merge_failed
    ):
        return None

    return "\n".join(lines)
