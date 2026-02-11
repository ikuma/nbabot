"""Both-side hedge job scheduling and execution.

Extracted from src/scheduler/trade_scheduler.py — _schedule_hedge_job,
_process_hedge_job, and _get_directional_dca_group.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone

from src.config import settings
from src.scheduler.job_executor import JobResult, _preflight_check
from src.store.db import (
    TradeJob,
    get_dca_group_signals,
    update_dca_job,
    update_job_bothside,
    update_job_status,
    upsert_hedge_job,
)

logger = logging.getLogger(__name__)


def _schedule_hedge_job(
    directional_job: TradeJob,
    bothside_opp,
    db_path: str,
) -> None:
    """Create a pending hedge job for the opposite outcome."""
    bothside_group_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    hedge_after = now + timedelta(minutes=settings.bothside_hedge_delay_min)

    hedge_job_id = upsert_hedge_job(
        directional_job_id=directional_job.id,
        event_slug=directional_job.event_slug,
        game_date=directional_job.game_date,
        home_team=directional_job.home_team,
        away_team=directional_job.away_team,
        game_time_utc=directional_job.game_time_utc,
        execute_after=hedge_after.isoformat(),
        execute_before=directional_job.execute_before,
        bothside_group_id=bothside_group_id,
        db_path=db_path,
    )

    if hedge_job_id:
        # directional ジョブにも bothside_group_id をセット
        update_job_bothside(
            directional_job.id,
            bothside_group_id=bothside_group_id,
            paired_job_id=hedge_job_id,
            db_path=db_path,
        )
        logger.info(
            "Hedge job %d scheduled for %s (after=%s, bs_group=%s, hedge=%s @ %.3f)",
            hedge_job_id,
            directional_job.event_slug,
            hedge_after.isoformat(),
            bothside_group_id,
            bothside_opp.hedge.outcome_name,
            bothside_opp.hedge.poly_price,
        )


def _get_directional_dca_group(directional_job_id: int, db_path: str) -> str:
    """Get the dca_group_id from the directional job."""
    from src.store.db import _connect

    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT dca_group_id FROM trade_jobs WHERE id = ?", (directional_job_id,)
        ).fetchone()
        return row[0] if row and row[0] else ""
    finally:
        conn.close()


def process_hedge_job(
    job: TradeJob,
    execution_mode: str,
    db_path: str,
    fetch_moneyline_for_game,
    log_signal,
    place_limit_buy,
    update_order_status,
) -> JobResult:
    """Process a hedge job: re-check combined VWAP, then place order."""
    from src.sizing.position_sizer import calculate_dca_budget
    from src.strategy.calibration_scanner import (
        _calibration_kelly,
        _ev_per_dollar,
    )

    update_job_status(job.id, "executing", db_path=db_path)

    try:
        # directional ジョブのシグナルから VWAP を取得
        dir_signals = []
        if job.paired_job_id:
            dir_signals = get_dca_group_signals(
                _get_directional_dca_group(job.paired_job_id, db_path), db_path=db_path
            )

        # 最新価格を取得
        ml = fetch_moneyline_for_game(job.away_team, job.home_team, job.game_date)
        if not ml:
            update_job_status(
                job.id, "skipped", error_message="No moneyline market", db_path=db_path
            )
            return JobResult(job.id, job.event_slug, "skipped")

        # directional のアウトカムを特定 (シグナルのチーム名で)
        dir_team = dir_signals[0].team if dir_signals else None
        dir_price = None
        hedge_price = None
        hedge_outcome = None
        hedge_token_id = None

        for i, outcome in enumerate(ml.outcomes):
            if i >= len(ml.prices):
                continue
            if outcome == dir_team:
                dir_price = ml.prices[i]
            else:
                hedge_price = ml.prices[i]
                hedge_outcome = outcome
                hedge_token_id = ml.token_ids[i] if i < len(ml.token_ids) else None

        if hedge_price is None or hedge_outcome is None or hedge_token_id is None:
            update_job_status(
                job.id, "skipped", error_message="Cannot find hedge outcome", db_path=db_path
            )
            return JobResult(job.id, job.event_slug, "skipped")

        # Combined VWAP 再チェック
        dir_vwap = dir_price or 0.0
        if dir_signals:
            from src.strategy.dca_strategy import calculate_vwap_from_pairs

            dir_vwap = calculate_vwap_from_pairs(
                [s.kelly_size for s in dir_signals],
                [s.fill_price or s.poly_price for s in dir_signals],
            )

        combined = dir_vwap + hedge_price
        if combined >= settings.bothside_max_combined_vwap:
            update_job_status(
                job.id,
                "skipped",
                error_message=f"Combined VWAP {combined:.4f} >= "
                f"{settings.bothside_max_combined_vwap}",
                db_path=db_path,
            )
            logger.info(
                "Hedge job %d: combined VWAP %.4f >= %.3f → skipped",
                job.id,
                combined,
                settings.bothside_max_combined_vwap,
            )
            return JobResult(job.id, job.event_slug, "skipped")

        # Hedge 価格ガード
        if hedge_price > settings.bothside_hedge_max_price:
            update_job_status(
                job.id,
                "skipped",
                error_message=f"Hedge price {hedge_price:.3f} > "
                f"max {settings.bothside_hedge_max_price}",
                db_path=db_path,
            )
            return JobResult(job.id, job.event_slug, "skipped")

        # Calibration EV 再検証
        from src.strategy.calibration import lookup_band

        band = lookup_band(hedge_price)
        if band is None:
            update_job_status(
                job.id, "skipped", error_message="No calibration band for hedge", db_path=db_path
            )
            return JobResult(job.id, job.event_slug, "skipped")

        ev = _ev_per_dollar(band.expected_win_rate, hedge_price)
        if ev <= 0:
            update_job_status(
                job.id, "skipped", error_message="Hedge EV non-positive", db_path=db_path
            )
            return JobResult(job.id, job.event_slug, "skipped")

        # サイジング (LLM hedge_ratio 適用: Phase L)
        kelly = _calibration_kelly(band.expected_win_rate, hedge_price)
        hedge_mult = settings.bothside_hedge_kelly_mult
        if settings.llm_analysis_enabled:
            from src.strategy.llm_cache import get_cached_analysis

            _llm = get_cached_analysis(job.event_slug, db_path=db_path)
            if _llm:
                hedge_mult = max(0.3, min(0.8, _llm.hedge_ratio))
                logger.info(
                    "Hedge job %d: LLM hedge_ratio=%.2f (was %.2f)",
                    job.id,
                    hedge_mult,
                    settings.bothside_hedge_kelly_mult,
                )
        kelly *= hedge_mult
        kelly_usd = min(kelly * settings.max_position_usd * 10, settings.max_position_usd)

        dca_max = settings.dca_max_entries
        budget = calculate_dca_budget(
            kelly_usd=kelly_usd,
            num_entries=dca_max,
            max_position_usd=settings.max_position_usd,
            capital_risk_pct=settings.capital_risk_pct,
        )

        if budget.slice_size_usd <= 0:
            update_job_status(
                job.id, "skipped", error_message="Hedge DCA budget is zero", db_path=db_path
            )
            return JobResult(job.id, job.event_slug, "skipped")

        # dry-run
        if execution_mode == "dry-run":
            update_job_status(job.id, "skipped", error_message="dry-run mode", db_path=db_path)
            logger.info(
                "[dry-run] Hedge job %d: BUY %s @ %.3f $%.0f combined=%.4f",
                job.id,
                hedge_outcome,
                hedge_price,
                budget.slice_size_usd,
                combined,
            )
            return JobResult(job.id, job.event_slug, "skipped")

        # DCA グループ ID (hedge 独立)
        dca_group_id = str(uuid.uuid4())

        from src.strategy.calibration import is_in_sweet_spot

        sweet = is_in_sweet_spot(hedge_price, settings.sweet_spot_lo, settings.sweet_spot_hi)
        band_label = f"{band.price_lo:.2f}-{band.price_hi:.2f}"
        edge_pct = (band.expected_win_rate - hedge_price) * 100

        signal_id = log_signal(
            game_title=ml.event_title,
            event_slug=job.event_slug,
            team=hedge_outcome,
            side="BUY",
            poly_price=hedge_price,
            book_prob=0.0,
            edge_pct=edge_pct,
            kelly_size=budget.slice_size_usd,
            token_id=hedge_token_id,
            market_type="moneyline",
            calibration_edge_pct=edge_pct,
            expected_win_rate=band.expected_win_rate,
            price_band=band_label,
            in_sweet_spot=sweet,
            band_confidence=band.confidence,
            strategy_mode="calibration",
            dca_group_id=dca_group_id,
            dca_sequence=1,
            bothside_group_id=job.bothside_group_id,
            signal_role="hedge",
            condition_id=ml.condition_id,
            db_path=db_path,
        )

        # live モード
        if execution_mode == "live":
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

            try:
                resp = place_limit_buy(hedge_token_id, hedge_price, budget.slice_size_usd)
                order_id = resp.get("orderID") or resp.get("id", "")
                update_order_status(signal_id, order_id, "placed", db_path=db_path)
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
                logger.exception("Hedge job %d: order failed", job.id)
                return JobResult(job.id, job.event_slug, "failed", signal_id, str(e))

        # DCA 有効なら dca_active に遷移
        if dca_max > 1:
            update_job_status(job.id, "dca_active", signal_id=signal_id, db_path=db_path)
            update_dca_job(
                job.id,
                dca_entries_count=1,
                dca_max_entries=dca_max,
                dca_group_id=dca_group_id,
                dca_total_budget=budget.total_budget_usd,
                dca_slice_size=budget.slice_size_usd,
                db_path=db_path,
            )
        else:
            update_job_status(job.id, "executed", signal_id=signal_id, db_path=db_path)

        logger.info(
            "Hedge job %d (%s): BUY %s @ %.3f $%.0f combined=%.4f signal #%d [%s]",
            job.id,
            job.event_slug,
            hedge_outcome,
            hedge_price,
            budget.slice_size_usd,
            combined,
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
        logger.exception("Hedge job %d (%s): unexpected error", job.id, job.event_slug)
        return JobResult(job.id, job.event_slug, "failed", error=str(e))
