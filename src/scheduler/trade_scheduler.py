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
    get_dca_active_jobs,
    get_dca_group_signals,
    get_eligible_jobs,
    get_executing_jobs,
    get_job_summary,
    has_signal_for_slug,
    update_dca_job,
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

        # 残高取得 (live モードのみ — preflight より前倒し)
        if execution_mode == "live":
            try:
                from src.connectors.polymarket import get_usdc_balance

                balance_usd = get_usdc_balance()
            except Exception:
                logger.warning("Balance fetch failed for %s", job.event_slug)

        # EV 判定 (3層制約付き)
        opps = scan_calibration([ml], balance_usd=balance_usd, liquidity_map=liquidity_map)
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

        # DCA 予算計算 (事前トータルサイジング)
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
        )

        # スライスサイズが 0 なら skip
        if budget.slice_size_usd <= 0:
            update_job_status(
                job.id,
                "skipped",
                error_message="DCA budget is zero",
                db_path=db_path,
            )
            logger.info("Job %d (%s): DCA budget=0 → skipped", job.id, job.event_slug)
            return JobResult(job.id, job.event_slug, "skipped")

        # dry-run: ログ出力のみ
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
            return JobResult(job.id, job.event_slug, "skipped")

        # 流動性メタデータを抽出
        _liq_snap = liquidity_map.get(opp.token_id) if liquidity_map else None
        _ask_depth = _liq_snap.ask_depth_5c if _liq_snap else None
        _spread = _liq_snap.spread_pct if _liq_snap else None

        # DCA グループ ID を生成 (初回エントリー)
        import uuid

        dca_group_id = str(uuid.uuid4())

        # paper or live: シグナルを DB に記録 (kelly_size はスライスサイズ)
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

            size_usd = budget.slice_size_usd
            # best_ask 価格で発注 (注文板データがあれば)、なければ midpoint フォールバック
            order_price = opp.poly_price
            if _liq_snap and _liq_snap.best_ask > 0:
                order_price = _liq_snap.best_ask
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
                return JobResult(job.id, job.event_slug, "failed", signal_id, str(e))

        # 成功: DCA 有効なら dca_active に遷移、そうでなければ executed
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
                "Job %d (%s): → dca_active (1/%d) budget=$%.0f slice=$%.0f signal #%d [%s]",
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


# ---------------------------------------------------------------------------
# 3b. process_dca_active_jobs — DCA 追加購入
# ---------------------------------------------------------------------------


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
            from src.connectors.polymarket import fetch_moneyline_for_game

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
    dca_results: list[JobResult] | None = None,
    db_path: str | None = None,
) -> str | None:
    """Format a tick summary for Telegram. Returns None if nothing happened."""
    dca_results = dca_results or []
    all_results = results + dca_results

    if not all_results and expired_count == 0 and recovered_count == 0:
        return None

    path = db_path or DEFAULT_DB_PATH
    summary = get_job_summary(game_date, db_path=path)

    executed = [r for r in results if r.status == "executed"]
    skipped = [r for r in results if r.status == "skipped"]
    failed = [r for r in results if r.status == "failed"]
    dca_executed = [r for r in dca_results if r.status == "executed"]
    dca_failed = [r for r in dca_results if r.status == "failed"]

    lines = [f"*Scheduler Tick* ({game_date})"]

    if executed:
        lines.append(f"Executed: {len(executed)}")
        for r in executed:
            lines.append(f"  #{r.signal_id} {r.event_slug}")
    if dca_executed:
        lines.append(f"DCA entries: {len(dca_executed)}")
        for r in dca_executed:
            lines.append(f"  DCA #{r.signal_id} {r.event_slug}")
    if skipped:
        lines.append(f"Skipped: {len(skipped)}")
    if failed or dca_failed:
        total_failed = failed + dca_failed
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
    if not executed and not dca_executed and not failed and not dca_failed:
        return None

    return "\n".join(lines)
