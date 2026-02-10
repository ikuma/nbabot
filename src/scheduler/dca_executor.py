"""DCA (Dollar Cost Averaging) additional entry execution.

Extracted from src/scheduler/trade_scheduler.py — process_dca_active_jobs.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from src.config import settings
from src.scheduler.job_executor import JobResult
from src.store.db import (
    DEFAULT_DB_PATH,
    get_dca_active_jobs,
    get_dca_group_signals,
    update_dca_job,
)

logger = logging.getLogger(__name__)


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

        # サイジング: 事前計算済みスライスサイズを使用 (フォールバック: 初回の kelly_size)
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

        # live モード: 実発注
        if execution_mode == "live":
            try:
                resp = place_limit_buy(target_token_id, current_price, dca_size)
                order_id = resp.get("orderID") or resp.get("id", "")
                update_order_status(new_signal_id, order_id, "placed", db_path=path)
            except Exception as e:
                update_order_status(new_signal_id, None, "failed", db_path=path)
                logger.exception("DCA order failed for job %d", job.id)
                results.append(JobResult(job.id, job.event_slug, "failed", new_signal_id, str(e)))
                continue

        # DCA カウント更新
        new_count = job.dca_entries_count + 1
        new_status = "dca_active" if new_count < job.dca_max_entries else "executed"
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
