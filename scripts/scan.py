#!/usr/bin/env python3
"""Daily NBA edge scanner: find Polymarket vs sportsbook divergences."""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="NBA edge scanner")
    parser.add_argument("--dry-run", action="store_true", help="Skip DB signal logging")
    args = parser.parse_args()

    from src.connectors.odds_api import fetch_nba_odds
    from src.connectors.polymarket import fetch_all_moneylines
    from src.notifications.telegram import format_opportunities, send_message
    from src.store.db import log_signal
    from src.strategy.scanner import scan

    log.info("=== NBA Edge Scanner ===")

    # 1. Fetch sportsbook odds first (drives the game list)
    log.info("Fetching sportsbook odds...")
    games = fetch_nba_odds()
    log.info("Found %d games with odds", len(games))

    if not games:
        log.warning("No NBA games found on Odds API")
        return

    # 2. Fetch Polymarket moneyline markets for each game
    log.info("Fetching Polymarket moneyline markets...")
    moneylines = fetch_all_moneylines(games)
    log.info("Moneyline events found: %d", len(moneylines))

    if not moneylines:
        log.warning("No moneyline markets found on Polymarket")
        return

    # 3. Scan for divergences
    opportunities = scan(moneylines, games)
    log.info("Found %d opportunities above edge threshold", len(opportunities))

    # 4. Log signals to DB (unless --dry-run)
    if not args.dry_run and opportunities:
        # ゲームの commence_time をルックアップ用に構築
        game_by_teams: dict[tuple[str, str], str] = {}
        for g in games:
            game_by_teams[(g.home_team, g.away_team)] = g.commence_time

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
            )
            log.info(
                "Signal #%d logged: %s %s edge=%.1f%%",
                signal_id, opp.side, opp.team, opp.edge_pct,
            )
    elif args.dry_run:
        log.info("--dry-run: skipping DB signal logging")

    # 5. Generate report
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    report_dir = Path(__file__).resolve().parent.parent / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{today}.md"

    lines = [f"# NBA Edge Report {today}\n"]
    lines.append(f"Games with odds: {len(games)} | Moneyline events found: {len(moneylines)}\n")

    if not opportunities:
        lines.append("No opportunities above minimum edge threshold.\n")
    else:
        lines.append(f"## Opportunities ({len(opportunities)})\n")
        for i, opp in enumerate(opportunities, 1):
            lines.append(f"### {i}. {opp.game_title} — {opp.team}")
            lines.append(f"- **Side**: {opp.side} @ {opp.poly_price:.3f}")
            lines.append(f"- **Book consensus**: {opp.book_prob:.3f}")
            lines.append(f"- **Edge**: {opp.edge_pct:.1f}%")
            lines.append(f"- **Kelly size**: ${opp.kelly_size:.0f}")
            lines.append(f"- **Bookmakers**: {opp.bookmakers_count}")
            lines.append(f"- **Token ID**: `{opp.token_id}`")
            lines.append(f"- **Polymarket**: https://polymarket.com/event/{opp.event_slug}")
            lines.append("")

    report_path.write_text("\n".join(lines))
    log.info("Report saved: %s", report_path)

    # 6. Send Telegram notification
    msg = format_opportunities(opportunities)
    send_message(msg)

    # 7. Print summary
    print(f"\n{'='*60}")
    print(f"  Games: {len(games)} | Moneylines: {len(moneylines)} | Opps: {len(opportunities)}")
    for opp in opportunities[:5]:
        print(f"  {opp.side} {opp.team} ({opp.game_title}) edge={opp.edge_pct:.1f}%")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
