"""Initial order execution for directional trade jobs.

Extracted from src/scheduler/trade_scheduler.py — _process_single_job and helpers.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass

from src.config import settings
from src.scheduler.preflight import preflight_check as _preflight_check  # noqa: F401
from src.scheduler.pricing import below_market_price
from src.store.db import (
    TradeJob,
    update_dca_job,
    update_job_status,
)

logger = logging.getLogger(__name__)


@dataclass
class JobResult:
    """Outcome of processing a single trade job."""

    job_id: int
    event_slug: str
    status: str  # executed, skipped, failed
    signal_id: int | None = None
    error: str | None = None


def _build_liquidity_map(token_ids: list[str], event_slug: str):
    """Build token_id -> liquidity snapshot map when liquidity checks are enabled."""
    if not settings.check_liquidity or not token_ids:
        return None
    try:
        from src.connectors.polymarket import fetch_order_books_batch
        from src.sizing.liquidity import extract_liquidity

        order_books = fetch_order_books_batch(token_ids)
        if not order_books:
            return None

        liquidity_map = {}
        for token_id, book in order_books.items():
            snap = extract_liquidity(book, token_id)
            if snap:
                liquidity_map[token_id] = snap
        return liquidity_map or None
    except Exception:
        logger.warning("Order book fetch failed for %s, proceeding without", event_slug)
        return None


def _fetch_live_balance(execution_mode: str, event_slug: str) -> float | None:
    """Fetch USDC balance only for live execution mode."""
    if execution_mode != "live":
        return None
    try:
        from src.connectors.polymarket import get_usdc_balance

        return get_usdc_balance()
    except Exception:
        logger.warning("Balance fetch failed for %s", event_slug)
        return None


def _resolve_home_away_outcomes(outcomes: list[str], home_short: str) -> tuple[str, str]:
    """Resolve home/away outcome names from market outcomes."""
    home_outcome = ""
    away_outcome = ""
    for outcome in outcomes:
        if outcome == home_short:
            home_outcome = outcome
        else:
            away_outcome = outcome
    return home_outcome, away_outcome


def _apply_llm_directional_override(
    ml,
    bothside_opp,
    opp,
    llm_analysis,
    home_short: str,
    liquidity_map,
    balance_usd: float | None,
    effective_hedge_mult: float,
):
    """Apply LLM-first directional override (Case A/B) and return updated opp/bothside."""
    from src.strategy.calibration_scanner import (
        BothsideOpportunity,
        evaluate_single_outcome,
    )
    from src.strategy.llm_analyzer import determine_directional

    home_outcome, away_outcome = _resolve_home_away_outcomes(ml.outcomes, home_short)
    dir_name, _ = determine_directional(llm_analysis, home_outcome, away_outcome)

    if opp.outcome_name == dir_name:
        return opp, bothside_opp

    # Case A: hedge 存在 → swap
    if bothside_opp.hedge and bothside_opp.hedge.outcome_name == dir_name:
        logger.info(
            "LLM override (swap): %s -> %s (LLM favored=%s)",
            opp.outcome_name,
            dir_name,
            llm_analysis.favored_team,
        )
        opp = bothside_opp.hedge
        bothside_opp = BothsideOpportunity(
            directional=opp,
            hedge=bothside_opp.directional,
            combined_price=bothside_opp.combined_price,
            hedge_position_usd=bothside_opp.hedge_position_usd,
        )
        return opp, bothside_opp

    # Case B: hedge=None → LLM 側を独立評価
    llm_price = None
    llm_token_id = None
    for i, outcome in enumerate(ml.outcomes):
        if outcome == dir_name and i < len(ml.prices):
            llm_price = ml.prices[i]
            llm_token_id = ml.token_ids[i] if i < len(ml.token_ids) else None
            break

    if llm_price and llm_token_id:
        liquidity = liquidity_map.get(llm_token_id) if liquidity_map else None
        llm_opp = evaluate_single_outcome(
            price=llm_price,
            outcome_name=dir_name,
            token_id=llm_token_id,
            event_slug=ml.event_slug,
            event_title=ml.event_title,
            balance_usd=balance_usd,
            liquidity=liquidity,
        )
        if llm_opp:
            logger.info(
                "LLM override (Case B): %s -> %s @ %.3f ev=%.3f",
                opp.outcome_name,
                dir_name,
                llm_price,
                llm_opp.ev_per_dollar,
            )
            old_dir = opp
            opp = llm_opp
            bothside_opp = BothsideOpportunity(
                directional=opp,
                hedge=old_dir,
                combined_price=opp.poly_price + old_dir.poly_price,
                hedge_position_usd=old_dir.position_usd * effective_hedge_mult,
            )
        else:
            logger.info(
                "LLM Case B: %s @ %.3f no EV band, keeping %s",
                dir_name,
                llm_price,
                opp.outcome_name,
            )
    return opp, bothside_opp


def process_single_job(
    job: TradeJob,
    execution_mode: str,
    db_path: str,
    fetch_moneyline_for_game,
    scan_calibration,
    log_signal,
    place_limit_buy,
    update_order_status,
    sizing_multiplier: float = 1.0,
) -> tuple[JobResult, object | None]:
    """Process a single directional trade job through the state machine.

    Args:
        sizing_multiplier: Risk-adjusted multiplier (1.0 = normal, <1.0 = reduced).

    Returns (JobResult, bothside_opp_or_None).
    """
    # L2: executing ロック (発注前にステータス遷移)
    update_job_status(job.id, "executing", db_path=db_path)

    try:
        bothside_opp = None

        # 最新価格を取得
        ml = fetch_moneyline_for_game(job.away_team, job.home_team, job.game_date)
        if not ml:
            update_job_status(
                job.id,
                "skipped",
                error_message="No moneyline market found",
                db_path=db_path,
            )
            logger.info("Job %d (%s): no moneyline -> skipped", job.id, job.event_slug)
            return JobResult(job.id, job.event_slug, "skipped"), None

        # 注文板取得 + 流動性抽出 (check_liquidity=True の場合)
        liquidity_map = _build_liquidity_map(ml.token_ids, job.event_slug)

        # 残高取得 (live モードのみ)
        balance_usd = _fetch_live_balance(execution_mode, job.event_slug)

        # --- LLM 分析 (Phase L): 校正スキャンの前に実行 ---
        llm_analysis = None
        home_short = ""
        effective_sizing = sizing_multiplier
        effective_hedge_mult = settings.bothside_hedge_kelly_mult

        if settings.llm_analysis_enabled and job.job_side == "directional":
            try:
                from src.connectors.nba_data import build_game_context
                from src.connectors.team_mapping import get_team_short_name
                from src.strategy.llm_cache import get_or_analyze

                # ml.outcomes の順序は API 依存 — チーム名マッチで home/away を特定
                home_short = get_team_short_name(job.home_team) or ""
                poly_home_price = 0.0
                poly_away_price = 0.0
                for i, outcome in enumerate(ml.outcomes):
                    if i >= len(ml.prices):
                        break
                    if outcome == home_short:
                        poly_home_price = ml.prices[i]
                    else:
                        poly_away_price = ml.prices[i]

                ctx = build_game_context(
                    home_team=job.home_team,
                    away_team=job.away_team,
                    game_date=job.game_date,
                    game_time_utc=job.game_time_utc,
                    poly_home_price=poly_home_price,
                    poly_away_price=poly_away_price,
                )
                llm_analysis = get_or_analyze(
                    job.event_slug, job.game_date, ctx, db_path=db_path
                )
                if llm_analysis:
                    effective_sizing = sizing_multiplier * max(
                        settings.llm_min_sizing_modifier,
                        min(settings.llm_max_sizing_modifier, llm_analysis.sizing_modifier),
                    )
                    effective_hedge_mult = max(0.3, min(0.8, llm_analysis.hedge_ratio))
                    logger.info(
                        "LLM analysis for %s: favored=%s conf=%.2f sizing=%.2f hedge=%.2f",
                        job.event_slug,
                        llm_analysis.favored_team,
                        llm_analysis.confidence,
                        effective_sizing,
                        effective_hedge_mult,
                    )
            except Exception:
                logger.warning("LLM analysis failed for %s, falling back", job.event_slug)

        # EV 判定 (bothside 有効時は両サイド同時評価)
        if settings.bothside_enabled and job.job_side == "directional":
            from src.strategy.calibration_scanner import scan_calibration_bothside

            bothside_results = scan_calibration_bothside(
                [ml],
                balance_usd=balance_usd,
                liquidity_map=liquidity_map,
                max_combined_vwap=settings.bothside_max_combined_vwap,
                hedge_kelly_mult=settings.bothside_hedge_kelly_mult,
                hedge_max_price=settings.bothside_hedge_max_price,
            )
            if bothside_results:
                bothside_opp = bothside_results[0]
                opp = bothside_opp.directional
            else:
                opp = None

            # --- LLM-First: directional を LLM が決定 ---
            if llm_analysis and bothside_opp and opp:
                opp, bothside_opp = _apply_llm_directional_override(
                    ml=ml,
                    bothside_opp=bothside_opp,
                    opp=opp,
                    llm_analysis=llm_analysis,
                    home_short=home_short,
                    liquidity_map=liquidity_map,
                    balance_usd=balance_usd,
                    effective_hedge_mult=effective_hedge_mult,
                )
        else:
            opps = scan_calibration([ml], balance_usd=balance_usd, liquidity_map=liquidity_map)
            opp = opps[0] if opps else None

        if not opp:
            update_job_status(
                job.id,
                "skipped",
                error_message="No positive EV",
                db_path=db_path,
            )
            logger.info("Job %d (%s): no positive EV -> skipped", job.id, job.event_slug)
            return JobResult(job.id, job.event_slug, "skipped"), None

        # DCA 予算計算
        from src.sizing.position_sizer import calculate_dca_budget

        dca_max = settings.dca_max_entries
        _liq_snap_for_budget = liquidity_map.get(opp.token_id) if liquidity_map else None
        budget = calculate_dca_budget(
            kelly_usd=opp.position_usd,
            num_entries=dca_max,
            balance_usd=balance_usd,
            liquidity=_liq_snap_for_budget,
            max_position_usd=settings.max_position_usd,
            capital_risk_pct=settings.capital_risk_pct,
            liquidity_fill_pct=settings.liquidity_fill_pct,
            max_spread_pct=settings.max_spread_pct,
            sizing_multiplier=effective_sizing,
        )

        if budget.slice_size_usd <= 0:
            update_job_status(
                job.id,
                "skipped",
                error_message="DCA budget is zero",
                db_path=db_path,
            )
            logger.info("Job %d (%s): DCA budget=0 -> skipped", job.id, job.event_slug)
            return JobResult(job.id, job.event_slug, "skipped"), None

        # dry-run
        if execution_mode == "dry-run":
            update_job_status(
                job.id,
                "skipped",
                error_message="dry-run mode",
                db_path=db_path,
            )
            logger.info(
                "[dry-run] Job %d: BUY %s @ %.3f $%.0f (slice=$%.0f total=$%.0f) "
                "edge=%.1f%% liq=%s bind=%s",
                job.id,
                opp.outcome_name,
                opp.poly_price,
                budget.slice_size_usd,
                budget.slice_size_usd,
                budget.total_budget_usd,
                opp.calibration_edge_pct,
                opp.liquidity_score,
                budget.constraint_binding,
            )
            return JobResult(job.id, job.event_slug, "skipped"), None

        # 流動性メタデータを抽出
        _liq_snap = liquidity_map.get(opp.token_id) if liquidity_map else None
        _ask_depth = _liq_snap.ask_depth_5c if _liq_snap else None
        _spread = _liq_snap.spread_pct if _liq_snap else None

        # DCA グループ ID を生成 (初回エントリー)
        dca_group_id = str(uuid.uuid4())

        # paper or live: シグナルを DB に記録
        signal_id = log_signal(
            game_title=opp.event_title,
            event_slug=opp.event_slug,
            team=opp.outcome_name,
            side=opp.side,
            poly_price=opp.poly_price,
            book_prob=opp.book_prob or 0.0,
            edge_pct=opp.calibration_edge_pct,
            kelly_size=budget.slice_size_usd,
            token_id=opp.token_id,
            market_type=opp.market_type,
            calibration_edge_pct=opp.calibration_edge_pct,
            expected_win_rate=opp.expected_win_rate,
            price_band=opp.price_band,
            in_sweet_spot=opp.in_sweet_spot,
            band_confidence=opp.band_confidence,
            strategy_mode="calibration",
            liquidity_score=opp.liquidity_score,
            ask_depth_5c=_ask_depth,
            spread_pct=_spread,
            balance_usd_at_trade=balance_usd,
            constraint_binding=budget.constraint_binding,
            dca_group_id=dca_group_id,
            dca_sequence=1,
            signal_role="directional",
            condition_id=ml.condition_id,
            db_path=db_path,
        )

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
                return (
                    JobResult(job.id, job.event_slug, "failed", signal_id, "preflight failed"),
                    None,
                )

            size_usd = budget.slice_size_usd
            order_price = opp.poly_price
            if _liq_snap and _liq_snap.best_ask > 0:
                order_price = below_market_price(_liq_snap.best_ask)  # below-market maker order
            try:
                resp = place_limit_buy(opp.token_id, order_price, size_usd)
                order_id = resp.get("orderID") or resp.get("id", "")
                update_order_status(signal_id, order_id, "placed", db_path=db_path)
                # Order lifecycle 記録 (Phase O)
                from datetime import datetime, timezone

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
                    best_ask_at_event=(
                        _liq_snap.best_ask if _liq_snap and _liq_snap.best_ask > 0 else None
                    ),
                    db_path=db_path,
                )
                logger.info(
                    "[live] Job %d: BUY %s @ %.3f $%.0f order=%s (liq=%s, bind=%s)",
                    job.id,
                    opp.outcome_name,
                    order_price,
                    size_usd,
                    order_id,
                    opp.liquidity_score,
                    opp.constraint_binding,
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
                return JobResult(job.id, job.event_slug, "failed", signal_id, str(e)), None

        # Fee 記録 (Phase M3 — 監査証跡)
        try:
            from src.store.db import update_signal_fee

            update_signal_fee(signal_id, fee_rate_bps=0.0, fee_usd=0.0, db_path=db_path)
        except Exception:
            logger.debug("Fee recording failed for signal #%d", signal_id, exc_info=True)

        # 即時通知 (Phase N)
        try:
            from src.notifications.telegram import notify_trade

            _has_ask = _liq_snap and _liq_snap.best_ask > 0
            _notif_ask = _liq_snap.best_ask if _has_ask else opp.poly_price
            _notif_price = opp.poly_price  # paper mode default
            if execution_mode == "live":
                _notif_price = order_price  # set in live block above
            notify_trade(
                outcome_name=opp.outcome_name,
                event_slug=opp.event_slug,
                order_price=_notif_price,
                best_ask=_notif_ask,
                size_usd=budget.slice_size_usd,
                edge_pct=opp.calibration_edge_pct,
                price_band=opp.price_band,
                in_sweet_spot=opp.in_sweet_spot,
                expected_win_rate=opp.expected_win_rate,
                dca_seq=1,
                dca_max=dca_max,
                llm_favored=llm_analysis.favored_team if llm_analysis else None,
                llm_confidence=llm_analysis.confidence if llm_analysis else None,
                llm_sizing=llm_analysis.sizing_modifier if llm_analysis else None,
            )
        except Exception:
            logger.debug("Trade notification failed", exc_info=True)

        # DCA 有効なら dca_active に遷移
        if dca_max > 1:
            next_status = "dca_active"
            update_job_status(job.id, next_status, signal_id=signal_id, db_path=db_path)
            update_dca_job(
                job.id,
                dca_entries_count=1,
                dca_max_entries=dca_max,
                dca_group_id=dca_group_id,
                dca_total_budget=budget.total_budget_usd,
                dca_slice_size=budget.slice_size_usd,
                db_path=db_path,
            )
            logger.info(
                "Job %d (%s): -> dca_active (1/%d) budget=$%.0f slice=$%.0f signal #%d [%s]",
                job.id,
                job.event_slug,
                dca_max,
                budget.total_budget_usd,
                budget.slice_size_usd,
                signal_id,
                execution_mode,
            )
        else:
            update_job_status(job.id, "executed", signal_id=signal_id, db_path=db_path)
            logger.info(
                "Job %d (%s): executed -> signal #%d [%s]",
                job.id,
                job.event_slug,
                signal_id,
                execution_mode,
            )

        return JobResult(job.id, job.event_slug, "executed", signal_id), bothside_opp

    except Exception as e:
        update_job_status(
            job.id,
            "failed",
            error_message=str(e),
            increment_retry=True,
            db_path=db_path,
        )
        logger.exception("Job %d (%s): unexpected error", job.id, job.event_slug)
        return JobResult(job.id, job.event_slug, "failed", error=str(e)), None
