"""Polymarket discovery via NBA.com schedule (no Odds API dependency)."""

from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from src.connectors.nba_schedule import fetch_todays_games
from src.connectors.polymarket import MoneylineMarket, fetch_moneyline_for_game
from src.connectors.team_mapping import build_event_slug

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")


def fetch_all_nba_moneylines() -> list[MoneylineMarket]:
    """Fetch all NBA moneyline markets using NBA.com schedule + Gamma Events API.

    No Odds API dependency — uses NBA.com scoreboard for game discovery,
    then fetches Polymarket moneyline markets via the Gamma Events API.
    """
    games = fetch_todays_games()
    if not games:
        logger.warning("No games found from NBA.com scoreboard")
        return []

    moneylines: list[MoneylineMarket] = []

    for game in games:
        # Convert UTC game time to Eastern date for slug
        try:
            utc_dt = datetime.fromisoformat(
                game.game_time_utc.replace("Z", "+00:00")
            )
            et_dt = utc_dt.astimezone(ET)
            game_date = et_dt.strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            logger.warning(
                "Bad game_time_utc for %s @ %s: %s",
                game.away_team, game.home_team, game.game_time_utc,
            )
            continue

        slug = build_event_slug(game.away_team, game.home_team, game_date)
        if not slug:
            logger.warning(
                "Cannot build slug for %s @ %s", game.away_team, game.home_team,
            )
            continue

        ml = fetch_moneyline_for_game(game.away_team, game.home_team, game_date)
        if ml:
            moneylines.append(ml)
        else:
            logger.info(
                "No moneyline found: %s @ %s (slug=%s)",
                game.away_team, game.home_team, slug,
            )

    logger.info(
        "Fetched %d moneyline markets for %d games (NBA.com discovery)",
        len(moneylines), len(games),
    )
    return moneylines
