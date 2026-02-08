#!/usr/bin/env python3
"""Daily NBA edge scanner: find Polymarket vs sportsbook divergences."""

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def main():
    from src.connectors.odds_api import fetch_nba_odds
    from src.connectors.polymarket import fetch_all_moneylines
    from src.notifications.telegram import format_opportunities, send_message
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

    # 4. Generate report
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

    # 5. Send Telegram notification
    msg = format_opportunities(opportunities)
    send_message(msg)

    # 6. Print summary
    print(f"\n{'='*60}")
    print(f"  Games: {len(games)} | Moneylines: {len(moneylines)} | Opps: {len(opportunities)}")
    for opp in opportunities[:5]:
        print(f"  {opp.side} {opp.team} ({opp.game_title}) edge={opp.edge_pct:.1f}%")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
