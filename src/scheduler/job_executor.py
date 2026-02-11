"""Initial order execution for directional trade jobs.

Extracted from src/scheduler/trade_scheduler.py — _process_single_job and helpers.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass

from src.config import settings
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
        from src.connectors.polymarket import (
            fetch_order_books_batch as _fetch_obs,
        )
        from src.sizing.liquidity import LiquiditySnapshot
        from src.sizing.liquidity import extract_liquidity as _extract

        liquidity_map: dict[str, LiquiditySnapshot] | None = None
        balance_usd: float | None = None

        if settings.check_liquidity and ml.token_ids:
            try:
                order_books = _fetch_obs(ml.token_ids)
                if order_books:
                    liquidity_map = {}
                    for tid, book in order_books.items():
                        snap = _extract(book, tid)
                        if snap:
                            liquidity_map[tid] = snap
                    if not liquidity_map:
                        liquidity_map = None
            except Exception:
                logger.warning("Order book fetch failed for %s, proceeding without", job.event_slug)

        # 残高取得 (live モードのみ)
        if execution_mode == "live":
            try:
                from src.connectors.polymarket import get_usdc_balance

                balance_usd = get_usdc_balance()
            except Exception:
                logger.warning("Balance fetch failed for %s", job.event_slug)

        # --- LLM 分析 (Phase L): 校正スキャンの前に実行 ---
        llm_analysis = None
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
                from src.strategy.calibration_scanner import (
                    BothsideOpportunity,
                    evaluate_single_outcome,
                )
                from src.strategy.llm_analyzer import determine_directional

                # LLM の favored_team と校正スキャナーの directional を比較
                # home_short は上の LLM ブロックで算出済み
                _home_outcome = ""
                _away_outcome = ""
                for _oc in ml.outcomes:
                    if _oc == home_short:
                        _home_outcome = _oc
                    else:
                        _away_outcome = _oc

                dir_name, hedge_name = determine_directional(
                    llm_analysis,
                    _home_outcome,
                    _away_outcome,
                )
                # LLM が校正スキャナーと違う側を推奨した場合
                if opp.outcome_name != dir_name:
                    if (
                        bothside_opp.hedge
                        and bothside_opp.hedge.outcome_name == dir_name
                    ):
                        # Case A: hedge 存在 → swap
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
                    else:
                        # Case B: hedge=None → LLM 側を独立評価
                        _llm_price = None
                        _llm_token_id = None
                        for _i, _oc in enumerate(ml.outcomes):
                            if _oc == dir_name and _i < len(ml.prices):
                                _llm_price = ml.prices[_i]
                                _llm_token_id = ml.token_ids[_i] if _i < len(ml.token_ids) else None
                                break
                        if _llm_price and _llm_token_id:
                            _liq = liquidity_map.get(_llm_token_id) if liquidity_map else None
                            _llm_opp = evaluate_single_outcome(
                                price=_llm_price,
                                outcome_name=dir_name,
                                token_id=_llm_token_id,
                                event_slug=ml.event_slug,
                                event_title=ml.event_title,
                                balance_usd=balance_usd,
                                liquidity=_liq,
                            )
                            if _llm_opp:
                                logger.info(
                                    "LLM override (Case B): %s -> %s @ %.3f ev=%.3f",
                                    opp.outcome_name,
                                    dir_name,
                                    _llm_price,
                                    _llm_opp.ev_per_dollar,
                                )
                                old_dir = opp
                                opp = _llm_opp
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
                                    _llm_price or 0,
                                    opp.outcome_name,
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
                order_price = max(_liq_snap.best_ask - 0.01, 0.01)  # below-market maker order
            try:
                resp = place_limit_buy(opp.token_id, order_price, size_usd)
                order_id = resp.get("orderID") or resp.get("id", "")
                update_order_status(signal_id, order_id, "placed", db_path=db_path)
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
