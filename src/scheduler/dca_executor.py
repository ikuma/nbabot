"""DCA (Dollar Cost Averaging) additional entry execution.

Extracted from src/scheduler/trade_scheduler.py — process_dca_active_jobs.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from src.config import settings
from src.scheduler.job_executor import JobResult
from src.scheduler.pricing import apply_price_ceiling, below_market_price
from src.store.db import (
    DEFAULT_DB_PATH,
    get_dca_active_jobs,
    get_dca_group_signals,
    update_dca_job,
)

logger = logging.getLogger(__name__)


def _get_directional_stats(paired_job_id: int | None, db_path: str) -> tuple[float, float]:
    """Get directional DCA group stats: (vwap, shares)."""
    if not paired_job_id:
        return 0.0, 0.0
    try:
        from src.scheduler.hedge_executor import _get_directional_dca_group
        from src.strategy.bothside_target import estimate_shares_from_pairs
        from src.strategy.dca_strategy import calculate_vwap_from_pairs

        group_id = _get_directional_dca_group(paired_job_id, db_path)
        if not group_id:
            return 0.0, 0.0
        signals = get_dca_group_signals(group_id, db_path=db_path)
        if not signals:
            return 0.0, 0.0
        prices = [s.fill_price or s.poly_price for s in signals]
        costs = [s.kelly_size for s in signals]
        return calculate_vwap_from_pairs(
            costs,
            prices,
        ), estimate_shares_from_pairs(
            costs,
            prices,
        )
    except Exception:
        logger.warning("Failed to get directional stats for paired_job_id=%d", paired_job_id)
        return 0.0, 0.0


def _resolve_hedge_target_combined(dir_shares: float) -> float:
    """Resolve hedge target_combined (static/dynamic)."""
    from src.strategy.bothside_target import resolve_target_combined

    max_target = min(
        settings.bothside_target_combined_max,
        settings.bothside_max_combined_vwap - 1e-6,
    )
    return resolve_target_combined(
        static_target=settings.bothside_target_combined,
        mode=settings.bothside_target_mode,
        mergeable_shares_est=dir_shares,
        estimated_fee_usd=settings.bothside_dynamic_estimated_fee_usd,
        min_profit_usd=settings.merge_min_profit_usd,
        min_target=settings.bothside_target_combined_min,
        max_target=max_target,
    ).target_combined


def _get_directional_vwap(paired_job_id: int | None, db_path: str) -> float:
    """Backward-compatible helper returning directional VWAP only."""
    vwap, _ = _get_directional_stats(paired_job_id, db_path)
    return vwap


def _compute_directional_vwap_and_target(
    paired_job_id: int | None,
    db_path: str,
) -> tuple[float, float]:
    """Return (dir_vwap, target_combined) for hedge-side checks."""
    dir_vwap, dir_shares = _get_directional_stats(paired_job_id, db_path)
    target_combined = _resolve_hedge_target_combined(dir_shares)
    return dir_vwap, target_combined


def _legacy_vwap_for_log(signals):
    """Deprecated helper kept to minimize diff size."""
    from src.strategy.dca_strategy import calculate_vwap_from_pairs

    return calculate_vwap_from_pairs(
            [s.kelly_size for s in signals],
            [s.fill_price or s.poly_price for s in signals],
        )


def _compute_live_dca_order_price(
    job,
    target_token_id: str,
    current_price: float,
    db_path: str,
) -> float:
    """Compute DCA live order price from order book and hedge constraints."""
    dca_order_price = current_price  # fallback
    try:
        from src.connectors.polymarket import fetch_order_books_batch as _fetch_obs_dca
        from src.sizing.liquidity import extract_liquidity as _extract_dca

        obs = _fetch_obs_dca([target_token_id])
        if obs and target_token_id in obs:
            snap = _extract_dca(obs[target_token_id], target_token_id)
            if snap and snap.best_ask > 0:
                dca_order_price = below_market_price(snap.best_ask)
    except Exception:
        logger.warning("DCA order book fetch failed for job %d", job.id)

    # hedge DCA: target combined で上限制限
    if job.job_side == "hedge" and settings.bothside_enabled:
        dir_vwap, target_combined = _compute_directional_vwap_and_target(
            job.paired_job_id, db_path
        )
        if dir_vwap > 0:
            max_hedge = target_combined - dir_vwap
            dca_order_price = apply_price_ceiling(dca_order_price, max_hedge)

    return dca_order_price


def process_dca_active_jobs(
    execution_mode: str = "paper",
    db_path: str | None = None,
) -> list[JobResult]:
    """Process DCA-active jobs: add entries based on time/price triggers.

    Runs after process_eligible_jobs() in each tick.
    """
    from src.connectors.polymarket import fetch_moneyline_for_game, place_limit_buy
    from src.store.db import log_signal, update_order_status
    from src.strategy.dca_strategy import DCAConfig, DCAEntry, should_add_dca_entry

    path = db_path or DEFAULT_DB_PATH
    now = datetime.now(timezone.utc)
    now_utc = now.isoformat()

    dca_jobs = get_dca_active_jobs(now_utc, db_path=path)
    if not dca_jobs:
        return []

    logger.info("Found %d DCA-active job(s)", len(dca_jobs))

    dca_config = DCAConfig(
        max_entries=settings.dca_max_entries,
        min_interval_min=settings.dca_min_interval_min,
        max_price_spread=settings.dca_max_price_spread,
        favorable_price_pct=settings.dca_favorable_price_pct,
        unfavorable_price_pct=settings.dca_unfavorable_price_pct,
        cutoff_before_tipoff_min=settings.dca_cutoff_before_tipoff_min,
    )

    max_per_tick = settings.max_orders_per_tick
    results: list[JobResult] = []
    orders_this_tick = 0

    for job in dca_jobs:
        if orders_this_tick >= max_per_tick:
            logger.warning("max_orders_per_tick reached during DCA processing")
            break

        if not job.dca_group_id:
            logger.warning("Job %d has no dca_group_id, skipping", job.id)
            continue

        # 既存の DCA エントリーを取得
        signals = get_dca_group_signals(job.dca_group_id, db_path=path)
        if not signals:
            logger.warning("No signals found for DCA group %s", job.dca_group_id)
            continue

        first_signal = signals[0]

        # 最新価格を取得
        try:
            ml = fetch_moneyline_for_game(job.away_team, job.home_team, job.game_date)
        except Exception:
            logger.warning("Price fetch failed for DCA job %d", job.id)
            continue

        if not ml:
            continue

        # 対象アウトカムの現在価格を取得
        current_price = None
        target_token_id = first_signal.token_id
        target_team = first_signal.team
        for i, tid in enumerate(ml.token_ids):
            if tid == target_token_id:
                current_price = ml.prices[i]
                break

        if current_price is None:
            # token_id が変わった場合はチーム名で fallback
            for i, outcome in enumerate(ml.outcomes):
                if outcome == target_team:
                    current_price = ml.prices[i]
                    target_token_id = ml.token_ids[i]
                    break

        if current_price is None:
            logger.warning("Cannot find price for %s in job %d", target_team, job.id)
            continue

        # Hedge DCA: combined target フィルター
        if job.job_side == "hedge" and settings.bothside_enabled:
            _dir_vwap, _target_combined = _compute_directional_vwap_and_target(
                job.paired_job_id, path
            )
            if _dir_vwap > 0:
                _max_hedge = _target_combined - _dir_vwap
                if current_price > _max_hedge:
                    logger.debug(
                        "Hedge DCA skip: price %.3f > max_hedge %.3f (dir_vwap=%.3f target=%.3f)",
                        current_price,
                        _max_hedge,
                        _dir_vwap,
                        _target_combined,
                    )
                    continue

        # DCA エントリーを構築
        entries = []
        for sig in signals:
            try:
                created = datetime.fromisoformat(sig.created_at.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                created = now
            entries.append(
                DCAEntry(
                    price=sig.poly_price,
                    size_usd=sig.kelly_size,
                    created_at=created,
                )
            )

        # ティップオフ時刻をパース
        try:
            tipoff = datetime.fromisoformat(job.game_time_utc.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            logger.warning("Bad game_time_utc for job %d", job.id)
            continue

        # DCA 判定
        decision = should_add_dca_entry(current_price, entries, tipoff, now, dca_config)

        if not decision.should_buy:
            logger.debug(
                "DCA job %d (%s): no buy — %s (price=%.3f vwap=%.3f)",
                job.id,
                job.event_slug,
                decision.reason,
                current_price,
                decision.vwap,
            )
            # max_reached なら dca_active → executed に遷移
            if decision.reason == "max_reached":
                update_dca_job(job.id, status="executed", db_path=path)
                logger.info("Job %d: DCA max entries reached → executed", job.id)
            continue

        # DCA エントリーを発注
        logger.info(
            "DCA job %d (%s): %s @ %.3f (vwap=%.3f, seq=%d/%d)",
            job.id,
            job.event_slug,
            decision.reason,
            current_price,
            decision.vwap,
            decision.sequence,
            dca_config.max_entries,
        )

        # サイジング: target-holding 方式 (Phase DCA2)
        total_budget = job.dca_total_budget
        target_result = None
        if total_budget and total_budget > 0:
            from src.sizing.position_sizer import calculate_target_order_size

            target_result = calculate_target_order_size(
                total_budget=total_budget,
                costs=[s.kelly_size for s in signals],
                prices=[s.fill_price or s.poly_price for s in signals],
                current_price=current_price,
                max_entries=job.dca_max_entries,
                entries_done=len(signals),
                cap_mult=settings.dca_per_entry_cap_mult,
                min_order_usd=settings.dca_min_order_usd,
            )
            dca_size = target_result.order_size_usd
            if dca_size <= 0:
                if target_result.completion_reason:
                    update_dca_job(job.id, status="executed", db_path=path)
                    logger.info(
                        "Job %d: DCA %s → executed", job.id, target_result.completion_reason
                    )
                continue
        else:
            # dca_total_budget が NULL の旧データ: equal split フォールバック
            dca_size = job.dca_slice_size if job.dca_slice_size else first_signal.kelly_size

        if execution_mode == "dry-run":
            logger.info(
                "[dry-run] DCA #%d: BUY %s @ %.3f $%.0f",
                decision.sequence,
                target_team,
                current_price,
                dca_size,
            )
            results.append(JobResult(job.id, job.event_slug, "skipped"))
            continue

        # シグナル記録
        new_signal_id = log_signal(
            game_title=first_signal.game_title,
            event_slug=first_signal.event_slug,
            team=target_team,
            side="BUY",
            poly_price=current_price,
            book_prob=first_signal.book_prob,
            edge_pct=first_signal.edge_pct,
            kelly_size=dca_size,
            token_id=target_token_id,
            market_type=first_signal.market_type,
            calibration_edge_pct=first_signal.calibration_edge_pct,
            expected_win_rate=first_signal.expected_win_rate,
            price_band=first_signal.price_band,
            in_sweet_spot=bool(first_signal.in_sweet_spot),
            band_confidence=first_signal.band_confidence,
            strategy_mode="calibration",
            dca_group_id=job.dca_group_id,
            dca_sequence=decision.sequence,
            bothside_group_id=job.bothside_group_id,
            signal_role=job.job_side,
            condition_id=first_signal.condition_id,
            db_path=path,
        )

        # live モード: below-market 指値で発注
        if execution_mode == "live":
            try:
                dca_order_price = _compute_live_dca_order_price(
                    job=job,
                    target_token_id=target_token_id,
                    current_price=current_price,
                    db_path=path,
                )

                resp = place_limit_buy(target_token_id, dca_order_price, dca_size)
                order_id = resp.get("orderID") or resp.get("id", "")
                update_order_status(new_signal_id, order_id, "placed", db_path=path)
                # Order lifecycle 記録 (Phase O)
                from src.store.db import log_order_event, update_order_lifecycle

                _now_iso = datetime.now(timezone.utc).isoformat()
                update_order_lifecycle(
                    new_signal_id,
                    order_placed_at=_now_iso,
                    order_original_price=dca_order_price,
                    db_path=path,
                )
                log_order_event(
                    signal_id=new_signal_id,
                    event_type="placed",
                    order_id=order_id,
                    price=dca_order_price,
                    db_path=path,
                )
            except Exception as e:
                update_order_status(new_signal_id, None, "failed", db_path=path)
                logger.exception("DCA order failed for job %d", job.id)
                results.append(JobResult(job.id, job.event_slug, "failed", new_signal_id, str(e)))
                continue

        # Fee 記録 (Phase M3 — 監査証跡)
        try:
            from src.store.db import update_signal_fee

            update_signal_fee(new_signal_id, fee_rate_bps=0.0, fee_usd=0.0, db_path=path)
        except Exception:
            logger.debug("Fee recording failed for signal #%d", new_signal_id, exc_info=True)

        # 即時通知 (Phase N)
        try:
            from src.notifications.telegram import notify_dca
            from src.strategy.dca_strategy import calculate_vwap_from_pairs

            _old_vwap = decision.vwap
            _stub = type("_S", (), {"kelly_size": dca_size, "poly_price": current_price})
            _new_signals = signals + [_stub]
            _new_vwap = calculate_vwap_from_pairs(
                [s.kelly_size for s in _new_signals],
                [getattr(s, "fill_price", None) or s.poly_price for s in _new_signals],
            )
            notify_dca(
                outcome_name=target_team,
                event_slug=job.event_slug,
                order_price=current_price,
                size_usd=dca_size,
                old_vwap=_old_vwap,
                new_vwap=_new_vwap,
                dca_seq=decision.sequence,
                dca_max=dca_config.max_entries,
                trigger_reason=decision.reason,
            )
        except Exception:
            logger.debug("DCA notification failed", exc_info=True)

        # DCA カウント更新
        new_count = job.dca_entries_count + 1
        # Target-holding: 3 条件で完了判定
        if new_count >= job.dca_max_entries:
            new_status = "executed"
        elif target_result and target_result.completion_reason:
            new_status = "executed"
        else:
            new_status = "dca_active"
        update_dca_job(
            job.id,
            dca_entries_count=new_count,
            status=new_status,
            signal_id=new_signal_id,
            db_path=path,
        )

        orders_this_tick += 1
        results.append(JobResult(job.id, job.event_slug, "executed", new_signal_id))
        logger.info(
            "DCA job %d: entry %d/%d → signal #%d [%s]",
            job.id,
            new_count,
            job.dca_max_entries,
            new_signal_id,
            execution_mode,
        )

    return results
