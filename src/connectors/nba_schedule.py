"""NBA.com API connectors for game discovery (today + future dates)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx

from src.config import settings
from src.connectors.team_mapping import normalize_team_name

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")

NBA_SCHEDULE_URL = (
    "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"
)


@dataclass
class NBAGame:
    game_id: str
    home_team: str  # "Boston Celtics" (teamCity + " " + teamName)
    away_team: str  # "New York Knicks"
    game_time_utc: str  # ISO 8601 e.g. "2026-02-08T17:30:00Z"
    game_status: int  # 1=scheduled, 2=in-progress, 3=final
    home_score: int = 0
    away_score: int = 0
    period: int = 0  # 1-4 = regulation, 5+ = OT
    game_status_text: str = ""  # "Final", "Final/OT", "Postponed" etc.


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
    except httpx.TimeoutException:
        logger.error("NBA.com scoreboard request timed out")
        return []
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
                period=g.get("period", 0),
                game_status_text=g.get("gameStatusText", ""),
            )
        )

    logger.info("Found %d games from NBA.com scoreboard", len(games))
    return games


# ---------------------------------------------------------------------------
# Season schedule API — for future date lookups
# ---------------------------------------------------------------------------

_schedule_cache: list[dict] | None = None


def _fetch_season_schedule() -> list[dict]:
    """Fetch full season schedule from NBA.com CDN. Cached in-process."""
    global _schedule_cache
    if _schedule_cache is not None:
        return _schedule_cache

    logger.info("Fetching NBA season schedule from %s", NBA_SCHEDULE_URL)
    try:
        resp = httpx.get(NBA_SCHEDULE_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except httpx.TimeoutException:
        logger.error("NBA.com season schedule request timed out")
        return []
    except httpx.HTTPError:
        logger.exception("NBA.com season schedule request failed")
        return []

    # leagueSchedule.gameDates[] — each has { gameDate, games[] }
    game_dates = (
        data.get("leagueSchedule", {}).get("gameDates", [])
    )
    _schedule_cache = game_dates
    logger.info("Loaded schedule: %d game-dates", len(game_dates))
    return game_dates


def fetch_games_for_date(date_str: str) -> list[NBAGame]:
    """Fetch NBA games for a specific date (YYYY-MM-DD format).

    Uses the full-season schedule API (scheduleLeagueV2.json).
    Games are returned with game_status=1 (scheduled) and scores=0
    since future games have no score data.
    """
    game_dates = _fetch_season_schedule()
    if not game_dates:
        return []

    # NBA.com format: "MM/DD/YYYY 00:00:00"
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        nba_date_str = dt.strftime("%m/%d/%Y 00:00:00")
    except ValueError:
        logger.error("Invalid date format: %s (expected YYYY-MM-DD)", date_str)
        return []

    # Find matching gameDate entry
    target_games: list[dict] = []
    for gd in game_dates:
        if gd.get("gameDate") == nba_date_str:
            target_games = gd.get("games", [])
            break

    if not target_games:
        logger.info("No games found in schedule for %s", date_str)
        return []

    games: list[NBAGame] = []
    for g in target_games:
        home = g.get("homeTeam", {})
        away = g.get("awayTeam", {})
        home_name = f"{home.get('teamCity', '')} {home.get('teamName', '')}".strip()
        away_name = f"{away.get('teamCity', '')} {away.get('teamName', '')}".strip()
        home_name = normalize_team_name(home_name)
        away_name = normalize_team_name(away_name)

        if not home_name or not away_name:
            logger.warning(
                "Skipping game with missing team info: %s", g.get("gameId"),
            )
            continue

        # gameDateTimeUTC from schedule API
        game_time_utc = g.get("gameDateTimeUTC", "")

        games.append(
            NBAGame(
                game_id=g.get("gameId", ""),
                home_team=home_name,
                away_team=away_name,
                game_time_utc=game_time_utc,
                game_status=g.get("gameStatus", 1),
                home_score=0,
                away_score=0,
            )
        )

    logger.info("Found %d games for %s from season schedule", len(games), date_str)
    return games
