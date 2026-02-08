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
                signal_id, opp.side, opp.team, opp.edge_pct,
            )

    msg = format_opportunities(opportunities)
    send_message(msg)
    return opportunities


def _run_calibration(args, moneylines):
    """Run calibration-based scan."""
    from src.notifications.telegram import format_opportunities, send_message
    from src.store.db import log_signal
    from src.strategy.calibration_scanner import scan_calibration

    opportunities = scan_calibration(moneylines)
    log.info("[calibration] Found %d opportunities above edge threshold", len(opportunities))

    if not args.dry_run and opportunities:
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
                signal_id, opp.side, opp.outcome_name,
                opp.calibration_edge_pct, opp.price_band,
            )

    msg = format_opportunities(opportunities)
    send_message(msg)
    return opportunities


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
    args = parser.parse_args()

    mode = args.mode or settings.strategy_mode
    log.info("=== NBA Edge Scanner (mode=%s) ===", mode)

    if args.dry_run:
        log.info("--dry-run: skipping DB signal logging")

    # Odds API は bookmaker モードと both モードで必要
    # calibration モードでもゲームリスト取得に使用 (Phase 2 で Polymarket 駆動に切替)
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
    report_dir = Path(__file__).resolve().parent.parent / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{today}.md"

    lines = [f"# NBA Edge Report {today} (mode={mode})\n"]
    lines.append(
        f"Games with odds: {len(games)} | Moneyline events found: {len(moneylines)}\n"
    )

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
    print(f"\n{'='*60}")
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
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
