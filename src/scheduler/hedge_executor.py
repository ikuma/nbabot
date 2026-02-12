"""Both-side hedge job scheduling and execution.

Extracted from src/scheduler/trade_scheduler.py — _schedule_hedge_job,
_process_hedge_job, and _get_directional_dca_group.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone

from src.config import settings
from src.scheduler.job_executor import JobResult
from src.scheduler.preflight import preflight_check as _preflight_check
from src.scheduler.pricing import apply_price_ceiling, below_market_price
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
        if bothside_opp.hedge:
            logger.info(
                "Hedge job %d scheduled for %s (after=%s, bs_group=%s, hedge=%s @ %.3f)",
                hedge_job_id,
                directional_job.event_slug,
                hedge_after.isoformat(),
                bothside_group_id,
                bothside_opp.hedge.outcome_name,
                bothside_opp.hedge.poly_price,
            )
        else:
            logger.info(
                "Hedge job %d scheduled for %s (after=%s, bs_group=%s, hedge=pending)",
                hedge_job_id,
                directional_job.event_slug,
                hedge_after.isoformat(),
                bothside_group_id,
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


def _select_hedge_market(ml, dir_team: str | None):
    """Pick directional and hedge outcomes from market outcomes."""
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
    return dir_price, hedge_price, hedge_outcome, hedge_token_id


def _compute_directional_vwap(dir_signals: list, dir_price: float | None) -> float:
    """Compute directional VWAP from DCA signals, fallback to current directional price."""
    if not dir_signals:
        return dir_price or 0.0

    from src.strategy.dca_strategy import calculate_vwap_from_pairs

    return calculate_vwap_from_pairs(
        [s.kelly_size for s in dir_signals],
        [s.fill_price or s.poly_price for s in dir_signals],
    )


def _compute_hedge_order_price(
    hedge_token_id: str,
    hedge_price: float,
    max_hedge_price: float,
    event_slug: str,
) -> tuple[float, float]:
    """Fetch best ask and compute constrained hedge order price."""
    from src.connectors.polymarket import fetch_order_books_batch as _fetch_obs
    from src.sizing.liquidity import extract_liquidity as _extract

    best_ask = hedge_price  # fallback
    try:
        obs = _fetch_obs([hedge_token_id])
        if obs and hedge_token_id in obs:
            snap = _extract(obs[hedge_token_id], hedge_token_id)
            if snap and snap.best_ask > 0:
                best_ask = snap.best_ask
    except Exception:
        logger.warning("Order book fetch failed for hedge %s", event_slug)

    order_price = below_market_price(best_ask)
    order_price = apply_price_ceiling(order_price, max_hedge_price)
    return best_ask, order_price


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
    from src.strategy.bothside_target import (
        estimate_shares_from_pairs,
        resolve_target_combined,
    )
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
        dir_price, hedge_price, hedge_outcome, hedge_token_id = _select_hedge_market(
            ml, dir_team
        )

        if hedge_price is None or hedge_outcome is None or hedge_token_id is None:
            update_job_status(
                job.id, "skipped", error_message="Cannot find hedge outcome", db_path=db_path
            )
            return JobResult(job.id, job.event_slug, "skipped")

        # Directional VWAP 算出
        dir_vwap = _compute_directional_vwap(dir_signals, dir_price)
        dir_shares = estimate_shares_from_pairs(
            [s.kelly_size for s in dir_signals],
            [s.fill_price or s.poly_price for s in dir_signals],
        )

        max_target = min(
            settings.bothside_target_combined_max,
            settings.bothside_max_combined_vwap - 1e-6,
        )
        target_decision = resolve_target_combined(
            static_target=settings.bothside_target_combined,
            mode=settings.bothside_target_mode,
            mergeable_shares_est=dir_shares,
            estimated_fee_usd=settings.bothside_dynamic_estimated_fee_usd,
            min_profit_usd=settings.merge_min_profit_usd,
            min_target=settings.bothside_target_combined_min,
            max_target=max_target,
        )
        target_combined = target_decision.target_combined

        # MERGE 経済性から動的に限界価格を算出 (Phase H)
        _gas_plus_profit = settings.merge_est_gas_usd + settings.merge_min_profit_usd
        _min_margin = _gas_plus_profit / settings.merge_min_shares_floor
        max_hedge_price = 1.0 - dir_vwap - _min_margin
        max_hedge_price = min(max_hedge_price, settings.bothside_max_combined_vwap - dir_vwap)

        if max_hedge_price < 0.01:
            update_job_status(
                job.id,
                "skipped",
                error_message=f"max_hedge {max_hedge_price:.3f} < 0.01 (dir_vwap={dir_vwap:.3f})",
                db_path=db_path,
            )
            logger.info(
                "Hedge job %d: max_hedge %.3f < 0.01 (dir_vwap=%.3f) → skipped",
                job.id,
                max_hedge_price,
                dir_vwap,
            )
            return JobResult(job.id, job.event_slug, "skipped")

        # 注文板取得 → below-market pricing
        best_ask, order_price = _compute_hedge_order_price(
            hedge_token_id=hedge_token_id,
            hedge_price=hedge_price,
            max_hedge_price=max_hedge_price,
            event_slug=job.event_slug,
        )

        # Combined VWAP 最終チェック (MERGE 上限)
        combined = dir_vwap + order_price
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

        # Calibration EV 再検証 (order_price で — 連続カーブ)
        from src.strategy.calibration_curve import (
            _confidence_from_sample_size,
            get_default_curve,
        )

        _curve = get_default_curve()
        est = _curve.estimate(order_price)
        if est is None:
            update_job_status(
                job.id, "skipped", error_message="No calibration band for hedge", db_path=db_path
            )
            return JobResult(job.id, job.event_slug, "skipped")

        ev = _ev_per_dollar(est.lower_bound, order_price)
        if ev <= 0:
            logger.info(
                "Hedge job %d: EV non-positive (%.3f) — proceeding for MERGE (combined=%.4f)",
                job.id,
                ev,
                combined,
            )

        # サイジング: 正 EV → Kelly ベース、非正 EV → MERGE-only (cost-based)
        kelly = _calibration_kelly(est.lower_bound, order_price)
        if kelly > 0:
            # 正 EV パス: Kelly ベース (既存)
            from src.strategy.hedge_ratio_runtime import resolve_hedge_kelly_mult

            hedge_mult = resolve_hedge_kelly_mult(settings.bothside_hedge_kelly_mult)
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
        else:
            # MERGE-only パス: directional コストベースのサイジング
            from src.strategy.calibration_scanner import _hedge_margin_multiplier

            dir_total_cost = sum(s.kelly_size for s in dir_signals) if dir_signals else 0
            merge_margin = 1.0 - combined
            effective_mult = _hedge_margin_multiplier(merge_margin)
            kelly_usd = min(dir_total_cost * effective_mult, settings.max_position_usd)

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
                "[dry-run] Hedge job %d: BUY %s @ %.3f (best_ask=%.3f) $%.0f combined=%.4f",
                job.id,
                hedge_outcome,
                order_price,
                best_ask,
                budget.slice_size_usd,
                combined,
            )
            return JobResult(job.id, job.event_slug, "skipped")

        # DCA グループ ID (hedge 独立)
        dca_group_id = str(uuid.uuid4())

        from src.strategy.calibration import is_in_sweet_spot, lookup_band

        sweet = is_in_sweet_spot(order_price, settings.sweet_spot_lo, settings.sweet_spot_hi)
        _band = lookup_band(order_price)
        band_label = f"{_band.price_lo:.2f}-{_band.price_hi:.2f}" if _band else f"{order_price:.2f}"
        band_confidence = _confidence_from_sample_size(est.effective_sample_size)
        edge_pct = (est.lower_bound - order_price) * 100

        signal_id = log_signal(
            game_title=ml.event_title,
            event_slug=job.event_slug,
            team=hedge_outcome,
            side="BUY",
            poly_price=order_price,
            book_prob=0.0,
            edge_pct=edge_pct,
            kelly_size=budget.slice_size_usd,
            token_id=hedge_token_id,
            market_type="moneyline",
            calibration_edge_pct=edge_pct,
            expected_win_rate=est.lower_bound,
            price_band=band_label,
            in_sweet_spot=sweet,
            band_confidence=band_confidence,
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
                resp = place_limit_buy(hedge_token_id, order_price, budget.slice_size_usd)
                order_id = resp.get("orderID") or resp.get("id", "")
                update_order_status(signal_id, order_id, "placed", db_path=db_path)
                # Order lifecycle 記録 (Phase O)
                from src.store.db import log_order_event, update_order_lifecycle

                _now_iso = datetime.now(timezone.utc).isoformat()
                update_order_lifecycle(
                    signal_id,
                    order_placed_at=_now_iso,
                    order_original_price=order_price,
                    db_path=db_path,
                )
                log_order_event(
                    signal_id=signal_id,
                    event_type="placed",
                    order_id=order_id,
                    price=order_price,
                    best_ask_at_event=best_ask,
                    db_path=db_path,
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
                logger.exception("Hedge job %d: order failed", job.id)
                return JobResult(job.id, job.event_slug, "failed", signal_id, str(e))

        # Fee 記録 (Phase M3 — 監査証跡)
        try:
            from src.store.db import update_signal_fee

            update_signal_fee(signal_id, fee_rate_bps=0.0, fee_usd=0.0, db_path=db_path)
        except Exception:
            logger.debug("Fee recording failed for signal #%d", signal_id, exc_info=True)

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

        # 即時通知 (Phase N)
        try:
            from src.notifications.telegram import notify_hedge

            notify_hedge(
                outcome_name=hedge_outcome,
                event_slug=job.event_slug,
                order_price=order_price,
                best_ask=best_ask,
                size_usd=budget.slice_size_usd,
                dir_vwap=dir_vwap,
                combined_vwap=combined,
                target_combined=target_combined,
                dca_seq=1,
                dca_max=dca_max,
                edge_pct=edge_pct,
                signal_id=signal_id,
            )
        except Exception:
            logger.debug("Hedge notification failed", exc_info=True)

        logger.info(
            "Hedge job %d (%s): BUY %s @ %.3f (best_ask=%.3f) $%.0f combined=%.4f signal #%d [%s]",
            job.id,
            job.event_slug,
            hedge_outcome,
            order_price,
            best_ask,
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
