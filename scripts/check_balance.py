#!/usr/bin/env python3
"""Verify API connections: Polymarket, Odds API, and Telegram."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging

from src.config import settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def check_polymarket():
    log.info("--- Polymarket ---")
    if settings.http_proxy:
        log.info("Using proxy: %s", settings.http_proxy)

    from src.connectors.polymarket import fetch_nba_markets

    try:
        markets = fetch_nba_markets()
        log.info("OK: found %d NBA markets", len(markets))
        for m in markets[:3]:
            log.info("  - %s (YES=%.2f)", m.question, m.yes_price or 0)
    except Exception as e:
        log.error("FAIL: %s", e)
        log.error("If geo-blocked, set HTTP_PROXY in .env (e.g. socks5://127.0.0.1:1080)")

    if settings.polymarket_private_key:
        from src.connectors.polymarket import get_balance

        try:
            balance = get_balance()
            log.info("OK: balance = %s", balance)
        except Exception as e:
            log.error("FAIL (auth): %s", e)
    else:
        log.warning("POLYMARKET_PRIVATE_KEY not set, skipping auth check")


def check_odds_api():
    log.info("--- The Odds API ---")
    if not settings.odds_api_key:
        log.warning("ODDS_API_KEY not set, skipping")
        return

    from src.connectors.odds_api import fetch_nba_odds

    try:
        games = fetch_nba_odds()
        log.info("OK: %d games with odds", len(games))
        for g in games[:3]:
            probs = g.consensus_probs
            home_p = probs.get(g.home_team, 0)
            away_p = probs.get(g.away_team, 0)
            log.info("  - %s vs %s (%.0f%% / %.0f%%)", g.home_team, g.away_team,
                     home_p * 100, away_p * 100)
    except Exception as e:
        log.error("FAIL: %s", e)


def check_telegram():
    log.info("--- Telegram ---")
    if not settings.telegram_bot_token:
        log.warning("TELEGRAM_BOT_TOKEN not set, skipping")
        return

    from src.notifications.telegram import send_message

    ok = send_message("nbabot check: connection test")
    log.info("OK: message sent" if ok else "FAIL: could not send message")


if __name__ == "__main__":
    check_polymarket()
    print()
    check_odds_api()
    print()
    check_telegram()
    print()
    log.info("All checks complete.")
