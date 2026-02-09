"""Per-game trade scheduler: cron-driven state machine with SQLite job queue.

Each NBA game gets a trade_job with an execution window (tipoff - 2h to tipoff).
A 5-minute cron tick calls refresh → expire → process, executing orders only
for games whose window is currently open.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from src.config import settings
from src.store.db import (
    DEFAULT_DB_PATH,
    TradeJob,
    get_eligible_jobs,
    get_executing_jobs,
    get_job_summary,
    has_signal_for_slug,
    update_job_status,
    upsert_trade_job,
)

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")


@dataclass
class JobResult:
    """Outcome of processing a single trade job."""

    job_id: int
    event_slug: str
    status: str  # executed, skipped, failed
    signal_id: int | None = None
    error: str | None = None


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
# 3. process_eligible_jobs — scan + place orders
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

        result = _process_single_job(
            job,
            execution_mode,
            path,
            fetch_moneyline_for_game,
            scan_calibration,
            log_signal,
            place_limit_buy,
            update_order_status,
        )
        results.append(result)
        if result.status == "executed":
            orders_this_tick += 1

    return results


def _process_single_job(
    job: TradeJob,
    execution_mode: str,
    db_path: str,
    fetch_moneyline_for_game,
    scan_calibration,
    log_signal,
    place_limit_buy,
    update_order_status,
) -> JobResult:
    """Process a single trade job through the state machine."""
    # L2: executing ロック (発注前にステータス遷移)
    update_job_status(job.id, "executing", db_path=db_path)

    try:
        # 最新価格を取得
        ml = fetch_moneyline_for_game(job.away_team, job.home_team, job.game_date)
        if not ml:
            update_job_status(
                job.id,
                "skipped",
                error_message="No moneyline market found",
                db_path=db_path,
            )
            logger.info("Job %d (%s): no moneyline → skipped", job.id, job.event_slug)
            return JobResult(job.id, job.event_slug, "skipped")

        # EV 判定
        opps = scan_calibration([ml])
        if not opps:
            update_job_status(
                job.id,
                "skipped",
                error_message="No positive EV",
                db_path=db_path,
            )
            logger.info("Job %d (%s): no positive EV → skipped", job.id, job.event_slug)
            return JobResult(job.id, job.event_slug, "skipped")

        opp = opps[0]

        # dry-run: ログ出力のみ
        if execution_mode == "dry-run":
            update_job_status(
                job.id,
                "skipped",
                error_message="dry-run mode",
                db_path=db_path,
            )
            logger.info(
                "[dry-run] Job %d: BUY %s @ %.3f $%.0f edge=%.1f%%",
                job.id,
                opp.outcome_name,
                opp.poly_price,
                opp.position_usd,
                opp.calibration_edge_pct,
            )
            return JobResult(job.id, job.event_slug, "skipped")

        # paper or live: シグナルを DB に記録
        signal_id = log_signal(
            game_title=opp.event_title,
            event_slug=opp.event_slug,
            team=opp.outcome_name,
            side=opp.side,
            poly_price=opp.poly_price,
            book_prob=opp.book_prob or 0.0,
            edge_pct=opp.calibration_edge_pct,
            kelly_size=opp.position_usd,
            token_id=opp.token_id,
            market_type=opp.market_type,
            calibration_edge_pct=opp.calibration_edge_pct,
            expected_win_rate=opp.expected_win_rate,
            price_band=opp.price_band,
            in_sweet_spot=opp.in_sweet_spot,
            band_confidence=opp.band_confidence,
            strategy_mode="calibration",
            db_path=db_path,
        )

        if execution_mode == "live":
            # preflight チェック
            if not _preflight_check():
                update_job_status(
                    job.id,
                    "failed",
                    signal_id=signal_id,
                    error_message="Preflight check failed",
                    increment_retry=True,
                    db_path=db_path,
                )
                return JobResult(job.id, job.event_slug, "failed", signal_id, "preflight failed")

            size_usd = min(opp.position_usd, settings.max_position_usd)
            try:
                resp = place_limit_buy(opp.token_id, opp.poly_price, size_usd)
                order_id = resp.get("orderID") or resp.get("id", "")
                update_order_status(signal_id, order_id, "placed", db_path=db_path)
                logger.info(
                    "[live] Job %d: BUY %s @ %.3f $%.0f order=%s",
                    job.id,
                    opp.outcome_name,
                    opp.poly_price,
                    size_usd,
                    order_id,
                )
            except Exception as e:
                update_order_status(signal_id, None, "failed", db_path=db_path)
                update_job_status(
                    job.id,
                    "failed",
                    signal_id=signal_id,
                    error_message=str(e),
                    increment_retry=True,
                    db_path=db_path,
                )
                logger.exception("Job %d: order failed", job.id)
                return JobResult(job.id, job.event_slug, "failed", signal_id, str(e))

        # 成功
        update_job_status(
            job.id,
            "executed",
            signal_id=signal_id,
            db_path=db_path,
        )
        logger.info(
            "Job %d (%s): executed → signal #%d [%s]",
            job.id,
            job.event_slug,
            signal_id,
            execution_mode,
        )
        return JobResult(job.id, job.event_slug, "executed", signal_id)

    except Exception as e:
        update_job_status(
            job.id,
            "failed",
            error_message=str(e),
            increment_retry=True,
            db_path=db_path,
        )
        logger.exception("Job %d (%s): unexpected error", job.id, job.event_slug)
        return JobResult(job.id, job.event_slug, "failed", error=str(e))


def _preflight_check() -> bool:
    """Run pre-trade checks for live execution."""
    from datetime import date

    from src.connectors.polymarket import get_usdc_balance
    from src.store.db import get_todays_exposure, get_todays_live_orders

    try:
        if not settings.polymarket_private_key:
            logger.error("[preflight] POLYMARKET_PRIVATE_KEY not set")
            return False

        balance = get_usdc_balance()
        if balance < settings.min_balance_usd:
            logger.error(
                "[preflight] Balance $%.2f < minimum $%.2f",
                balance,
                settings.min_balance_usd,
            )
            return False

        today_str = date.today().strftime("%Y-%m-%d")
        order_count = get_todays_live_orders(today_str)
        if order_count >= settings.max_daily_positions:
            logger.error(
                "[preflight] Daily order limit reached: %d/%d",
                order_count,
                settings.max_daily_positions,
            )
            return False

        exposure = get_todays_exposure(today_str)
        if exposure >= settings.max_daily_exposure_usd:
            logger.error(
                "[preflight] Daily exposure limit: $%.0f/$%.0f",
                exposure,
                settings.max_daily_exposure_usd,
            )
            return False

        return True
    except Exception:
        logger.exception("[preflight] Check failed")
        return False


# ---------------------------------------------------------------------------
# 4. format_tick_summary — Telegram 通知用
# ---------------------------------------------------------------------------


def format_tick_summary(
    results: list[JobResult],
    game_date: str,
    expired_count: int = 0,
    recovered_count: int = 0,
    db_path: str | None = None,
) -> str | None:
    """Format a tick summary for Telegram. Returns None if nothing happened."""
    if not results and expired_count == 0 and recovered_count == 0:
        return None

    path = db_path or DEFAULT_DB_PATH
    summary = get_job_summary(game_date, db_path=path)

    executed = [r for r in results if r.status == "executed"]
    skipped = [r for r in results if r.status == "skipped"]
    failed = [r for r in results if r.status == "failed"]

    lines = [f"*Scheduler Tick* ({game_date})"]

    if executed:
        lines.append(f"Executed: {len(executed)}")
        for r in executed:
            lines.append(f"  #{r.signal_id} {r.event_slug}")
    if skipped:
        lines.append(f"Skipped: {len(skipped)}")
    if failed:
        lines.append(f"Failed: {len(failed)}")
        for r in failed:
            lines.append(f"  {r.event_slug}: {r.error}")
    if expired_count:
        lines.append(f"Expired: {expired_count}")

    lines.append(
        f"\nJobs: P={summary.pending} X={summary.executing} "
        f"OK={summary.executed} S={summary.skipped} "
        f"F={summary.failed} E={summary.expired}"
    )

    # 何も発注していなければ通知しない (ノイズ削減)
    if not executed and not failed:
        return None

    return "\n".join(lines)
