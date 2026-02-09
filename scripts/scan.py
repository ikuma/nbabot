#!/usr/bin/env python3
"""Daily NBA edge scanner: find Polymarket mispricing opportunities."""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def _run_bookmaker(args, games, moneylines):
    """Run bookmaker-consensus divergence scan (legacy path)."""
    from src.notifications.telegram import format_opportunities, send_message
    from src.store.db import log_signal
    from src.strategy.scanner import scan

    opportunities = scan(moneylines, games)
    log.info("[bookmaker] Found %d opportunities above edge threshold", len(opportunities))

    if not args.dry_run and opportunities:
        for opp in opportunities:
            signal_id = log_signal(
                game_title=opp.game_title,
                event_slug=opp.event_slug,
                team=opp.team,
                side=opp.side,
                poly_price=opp.poly_price,
                book_prob=opp.book_prob,
                edge_pct=opp.edge_pct,
                kelly_size=opp.kelly_size,
                token_id=opp.token_id,
                bookmakers_count=opp.bookmakers_count,
                consensus_std=opp.consensus_std,
                strategy_mode="bookmaker",
            )
            log.info(
                "Signal #%d logged: %s %s edge=%.1f%%",
                signal_id,
                opp.side,
                opp.team,
                opp.edge_pct,
            )

    msg = format_opportunities(opportunities)
    send_message(msg)
    return opportunities


def _run_calibration(args, moneylines):
    """Run calibration-based scan."""
    from src.config import settings
    from src.notifications.telegram import format_opportunities, send_message
    from src.store.db import log_signal
    from src.strategy.calibration_scanner import scan_calibration

    opportunities = scan_calibration(moneylines)
    log.info("[calibration] Found %d opportunities above edge threshold", len(opportunities))

    # 実行モード判定
    execution_mode = getattr(args, "execution", None) or settings.execution_mode

    # live モードでは _execute_live_orders が DB 記録 + 発注を一括で行う
    if execution_mode == "live" and opportunities and not args.dry_run:
        _execute_live_orders(opportunities)
    elif execution_mode == "dry-run" and opportunities:
        _execute_dry_run(opportunities)
    elif not args.dry_run and opportunities:
        # paper モード: シグナルのみ DB 記録
        for opp in opportunities:
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
            )
            log.info(
                "Signal #%d logged: %s %s cal_edge=%.1f%% band=%s",
                signal_id,
                opp.side,
                opp.outcome_name,
                opp.calibration_edge_pct,
                opp.price_band,
            )

    msg = format_opportunities(opportunities)
    send_message(msg)
    return opportunities


def _preflight_checks() -> bool:
    """Run pre-trade checks. Returns True if all pass."""
    from datetime import date

    from src.config import settings
    from src.connectors.polymarket import get_usdc_balance
    from src.store.db import get_todays_exposure, get_todays_live_orders

    # 1. 秘密鍵チェック
    if not settings.polymarket_private_key:
        log.error("[preflight] POLYMARKET_PRIVATE_KEY not set")
        return False

    # 2. USDC 残高チェック
    try:
        balance = get_usdc_balance()
        log.info("[preflight] USDC balance: $%.2f", balance)
        if balance < settings.min_balance_usd:
            log.error(
                "[preflight] Balance $%.2f < minimum $%.2f",
                balance, settings.min_balance_usd,
            )
            return False
    except Exception:
        log.exception("[preflight] Failed to check balance")
        return False

    # 3. 日次発注数チェック
    today_str = date.today().strftime("%Y-%m-%d")
    order_count = get_todays_live_orders(today_str)
    if order_count >= settings.max_daily_positions:
        log.error(
            "[preflight] Daily order limit reached: %d/%d",
            order_count, settings.max_daily_positions,
        )
        return False

    # 4. 日次エクスポージャーチェック
    exposure = get_todays_exposure(today_str)
    if exposure >= settings.max_daily_exposure_usd:
        log.error(
            "[preflight] Daily exposure limit reached: $%.0f/$%.0f",
            exposure, settings.max_daily_exposure_usd,
        )
        return False

    log.info(
        "[preflight] OK — balance=$%.2f, orders=%d/%d, exposure=$%.0f/$%.0f",
        balance, order_count, settings.max_daily_positions,
        exposure, settings.max_daily_exposure_usd,
    )
    return True


def _execute_live_orders(opportunities: list) -> None:
    """Place real orders for calibration opportunities."""
    from datetime import date

    from src.config import settings
    from src.connectors.polymarket import place_limit_buy
    from src.notifications.telegram import send_message
    from src.store.db import get_todays_live_orders, update_order_status

    if not _preflight_checks():
        log.warning("[live] Preflight failed, skipping all orders")
        send_message("[LIVE] Preflight checks failed — no orders placed")
        return

    today_str = date.today().strftime("%Y-%m-%d")
    placed = 0

    for opp in opportunities:
        # 日次上限チェック (ループ中もチェック)
        if get_todays_live_orders(today_str) >= settings.max_daily_positions:
            log.warning("[live] Daily position limit reached, stopping")
            break

        # 重複チェック: 同じ event_slug + 今日で発注済みならスキップ
        from src.store.db import _connect

        conn = _connect()
        try:
            dup = conn.execute(
                """SELECT COUNT(*) FROM signals
                   WHERE event_slug = ? AND order_status NOT IN ('paper', 'failed')
                   AND created_at LIKE ?""",
                (opp.event_slug, f"{today_str}%"),
            ).fetchone()[0]
        finally:
            conn.close()

        if dup > 0:
            log.info("[live] Skipping duplicate: %s", opp.event_slug)
            continue

        # シグナルを DB に記録 (order_status は後で更新)
        from src.store.db import log_signal

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
        )

        size_usd = min(opp.position_usd, settings.max_position_usd)

        try:
            result = place_limit_buy(opp.token_id, opp.poly_price, size_usd)
            order_id = result.get("orderID") or result.get("id", "")
            update_order_status(signal_id, order_id, "placed")
            placed += 1
            log.info(
                "[live] Order placed #%d: BUY %s @ %.3f $%.0f order_id=%s",
                signal_id, opp.outcome_name, opp.poly_price, size_usd, order_id,
            )
            send_message(
                f"[LIVE] BUY {opp.outcome_name} @ {opp.poly_price:.3f}"
                f" ${size_usd:.0f} | {opp.event_title}"
                f" | edge={opp.calibration_edge_pct:.1f}%"
            )
        except Exception:
            update_order_status(signal_id, None, "failed")
            log.exception("[live] Order failed for %s", opp.outcome_name)
            send_message(f"[LIVE] ORDER FAILED: {opp.outcome_name} @ {opp.poly_price:.3f}")

    log.info("[live] Placed %d/%d orders", placed, len(opportunities))


def _execute_dry_run(opportunities: list) -> None:
    """Log what would be placed without submitting orders."""
    from src.config import settings

    log.info("[dry-run] Would place %d orders:", len(opportunities))
    total = 0.0
    for opp in opportunities:
        size = min(opp.position_usd, settings.max_position_usd)
        total += size
        log.info(
            "[dry-run]   BUY %s @ %.3f $%.0f | %s | edge=%.1f%%",
            opp.outcome_name, opp.poly_price, size,
            opp.event_title, opp.calibration_edge_pct,
        )
    log.info("[dry-run] Total exposure: $%.0f", total)

    if _preflight_checks():
        log.info("[dry-run] Preflight checks: PASS")
    else:
        log.warning("[dry-run] Preflight checks: FAIL")


def _format_report_calibration(opps: list) -> list[str]:
    """Format calibration opportunities for the markdown report."""
    lines: list[str] = []
    sweet = [o for o in opps if o.in_sweet_spot]
    other = [o for o in opps if not o.in_sweet_spot]

    def _fmt(opps_list: list, lines: list) -> None:
        for i, opp in enumerate(opps_list, 1):
            spot = " [SWEET]" if opp.in_sweet_spot else ""
            lines.append(f"### {i}. {opp.event_title} — {opp.outcome_name}{spot}")
            lines.append(f"- **Side**: {opp.side} @ {opp.poly_price:.3f}")
            lines.append(
                f"- **Calibration edge**: {opp.calibration_edge_pct:.1f}%"
                f" | Expected WR: {opp.expected_win_rate:.1%}"
            )
            lines.append(
                f"- **EV/dollar**: {opp.ev_per_dollar:.2f}"
                f" | Band: {opp.price_band} ({opp.band_confidence})"
            )
            lines.append(f"- **Position size**: ${opp.position_usd:.0f}")
            lines.append(f"- **Token ID**: `{opp.token_id}`")
            lines.append(f"- **Polymarket**: https://polymarket.com/event/{opp.event_slug}")
            lines.append("")

    if sweet:
        lines.append(f"## Sweet Spot (0.25-0.55) — {len(sweet)} signals\n")
        _fmt(sweet, lines)
    if other:
        lines.append(f"## Outside Sweet Spot — {len(other)} signals\n")
        _fmt(other, lines)

    return lines


def _format_report_bookmaker(opps: list) -> list[str]:
    """Format bookmaker opportunities for the markdown report."""
    lines: list[str] = []
    high_edge = [o for o in opps if o.edge_pct >= 5.0]
    low_edge = [o for o in opps if o.edge_pct < 5.0]

    def _fmt(opps_list: list, lines: list) -> None:
        for i, opp in enumerate(opps_list, 1):
            lines.append(f"### {i}. {opp.game_title} — {opp.team}")
            lines.append(f"- **Side**: {opp.side} @ {opp.poly_price:.3f}")
            lines.append(f"- **Book consensus**: {opp.book_prob:.3f}")
            lines.append(
                f"- **Edge**: {opp.edge_pct:.1f}%"
                f" | Consensus std: {opp.consensus_std:.4f}"
                f" | Bookmakers: {opp.bookmakers_count}"
            )
            lines.append(f"- **Kelly size**: ${opp.kelly_size:.0f}")
            lines.append(f"- **Token ID**: `{opp.token_id}`")
            lines.append(f"- **Polymarket**: https://polymarket.com/event/{opp.event_slug}")
            lines.append("")

    if high_edge:
        lines.append(f"## High Edge >= 5% ({len(high_edge)})\n")
        _fmt(high_edge, lines)
    if low_edge:
        lines.append(f"## Low Edge 1-5% — Monitor ({len(low_edge)})\n")
        _fmt(low_edge, lines)

    return lines


def main():
    from src.config import settings

    parser = argparse.ArgumentParser(description="NBA edge scanner")
    parser.add_argument("--dry-run", action="store_true", help="Skip DB signal logging")
    parser.add_argument(
        "--mode",
        choices=["calibration", "bookmaker", "both"],
        default=None,
        help="Scan mode (default: from settings.strategy_mode)",
    )
    parser.add_argument(
        "--execution",
        choices=["paper", "live", "dry-run"],
        default=None,
        help="Execution mode (default: from settings.execution_mode)",
    )
    parser.add_argument(
        "--scan-date",
        type=str,
        default=None,
        help="Scan date YYYY-MM-DD (default: today)",
    )
    args = parser.parse_args()

    mode = args.mode or settings.strategy_mode
    exec_mode = args.execution or settings.execution_mode
    scan_date = args.scan_date
    log.info(
        "=== NBA Edge Scanner (mode=%s, execution=%s, date=%s) ===",
        mode, exec_mode, scan_date or "today",
    )

    if args.dry_run:
        log.info("--dry-run: skipping DB signal logging")

    # calibration モード → NBA.com 駆動 (Odds API 不使用)
    # bookmaker / both モード → Odds API 引き続き使用
    games = []
    nba_games = []
    moneylines: list = []

    if mode in ("bookmaker", "both"):
        from src.connectors.odds_api import fetch_nba_odds
        from src.connectors.polymarket import fetch_all_moneylines

        log.info("Fetching sportsbook odds...")
        games = fetch_nba_odds()
        log.info("Found %d games with odds", len(games))

        if not games:
            log.warning("No NBA games found on Odds API")
            return

        log.info("Fetching Polymarket moneyline markets...")
        moneylines = fetch_all_moneylines(games)
    else:
        from src.connectors.nba_schedule import fetch_games_for_date, fetch_todays_games
        from src.connectors.polymarket_discovery import fetch_all_nba_moneylines

        log.info("Fetching NBA schedule from NBA.com...")
        if scan_date:
            nba_games = fetch_games_for_date(scan_date)
        else:
            nba_games = fetch_todays_games()
        log.info("NBA.com: %d games for %s", len(nba_games), scan_date or "today")
        moneylines = fetch_all_nba_moneylines(target_date=scan_date)

    log.info("Moneyline events found: %d", len(moneylines))

    if not moneylines:
        log.warning("No moneyline markets found on Polymarket")
        return

    # Run scan(s)
    cal_opps: list = []
    book_opps: list = []

    if mode in ("calibration", "both"):
        cal_opps = _run_calibration(args, moneylines)
    if mode in ("bookmaker", "both"):
        book_opps = _run_bookmaker(args, games, moneylines)

    # Generate report
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    report_date = scan_date or today
    report_dir = Path(__file__).resolve().parent.parent / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{report_date}.md"

    date_label = f"{report_date} (scanned {today})" if scan_date else today
    lines = [f"# NBA Edge Report {date_label} (mode={mode})\n"]
    if games:
        lines.append(f"Games with odds: {len(games)} | Moneyline events found: {len(moneylines)}\n")
    elif nba_games:
        lines.append(
            f"NBA.com games: {len(nba_games)} | Moneyline events found: {len(moneylines)}\n"
        )
    else:
        lines.append(f"Moneyline events found: {len(moneylines)}\n")

    if mode in ("calibration", "both") and cal_opps:
        lines.append("# Calibration Signals\n")
        lines.extend(_format_report_calibration(cal_opps))
    if mode in ("bookmaker", "both") and book_opps:
        lines.append("# Bookmaker Signals\n")
        lines.extend(_format_report_bookmaker(book_opps))
    if not cal_opps and not book_opps:
        lines.append("No opportunities above minimum edge threshold.\n")

    # both モードの比較サマリー
    if mode == "both" and cal_opps and book_opps:
        cal_slugs = {o.event_slug for o in cal_opps}
        book_slugs = {o.event_slug for o in book_opps}
        overlap = cal_slugs & book_slugs
        lines.append("# Mode Comparison\n")
        lines.append(f"- Calibration signals: {len(cal_opps)}")
        lines.append(f"- Bookmaker signals: {len(book_opps)}")
        lines.append(f"- Overlapping games: {len(overlap)}")
        lines.append("")

    report_path.write_text("\n".join(lines))
    log.info("Report saved: %s", report_path)

    # Print summary
    print(f"\n{'=' * 60}")
    if cal_opps:
        print(f"  [CAL] {len(cal_opps)} calibration signals")
        for opp in cal_opps[:5]:
            spot = "SWEET" if opp.in_sweet_spot else "OTHER"
            print(
                f"  [{spot}] {opp.side} {opp.outcome_name} ({opp.event_title})"
                f" cal_edge={opp.calibration_edge_pct:.1f}%"
                f" band={opp.price_band}"
            )
    if book_opps:
        print(f"  [BOOK] {len(book_opps)} bookmaker signals")
        for opp in book_opps[:5]:
            tag = "HIGH" if opp.edge_pct >= 5.0 else "LOW"
            print(
                f"  [{tag}] {opp.side} {opp.team} ({opp.game_title})"
                f" edge={opp.edge_pct:.1f}% std={opp.consensus_std:.4f}"
            )
    if not cal_opps and not book_opps:
        print("  No opportunities found.")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
