"""NBA.com Scoreboard API connector for today's game discovery."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

from src.config import settings
from src.connectors.team_mapping import normalize_team_name

logger = logging.getLogger(__name__)


@dataclass
class NBAGame:
    game_id: str
    home_team: str  # "Boston Celtics" (teamCity + " " + teamName)
    away_team: str  # "New York Knicks"
    game_time_utc: str  # ISO 8601 e.g. "2026-02-08T17:30:00Z"
    game_status: int  # 1=scheduled, 2=in-progress, 3=final
    home_score: int = 0
    away_score: int = 0


def fetch_todays_games() -> list[NBAGame]:
    """Fetch today's NBA games from NBA.com scoreboard JSON.

    Returns all games regardless of status (scheduled, in-progress, final).
    """
    url = settings.nba_scoreboard_url
    logger.info("Fetching NBA schedule from %s", url)

    try:
        resp = httpx.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPError:
        logger.exception("NBA.com scoreboard request failed")
        return []

    scoreboard = data.get("scoreboard", {})
    raw_games = scoreboard.get("games", [])

    games: list[NBAGame] = []
    for g in raw_games:
        home = g.get("homeTeam", {})
        away = g.get("awayTeam", {})
        home_name = f"{home.get('teamCity', '')} {home.get('teamName', '')}".strip()
        away_name = f"{away.get('teamCity', '')} {away.get('teamName', '')}".strip()
        home_name = normalize_team_name(home_name)
        away_name = normalize_team_name(away_name)

        if not home_name or not away_name:
            logger.warning("Skipping game with missing team info: %s", g.get("gameId"))
            continue

        games.append(
            NBAGame(
                game_id=g.get("gameId", ""),
                home_team=home_name,
                away_team=away_name,
                game_time_utc=g.get("gameTimeUTC", ""),
                game_status=g.get("gameStatus", 0),
                home_score=home.get("score", 0),
                away_score=away.get("score", 0),
            )
        )

    logger.info("Found %d games from NBA.com scoreboard", len(games))
    return games
