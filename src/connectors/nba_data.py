"""NBA contextual data collection for LLM game analysis (Phase L).

Fetches standings, injuries, rest days, and head-to-head records from
public ESPN / NBA.com APIs. All data is used to build GameContext for
LLM-based directional analysis.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

import httpx

from src.connectors.nba_schedule import _fetch_season_schedule
from src.connectors.team_mapping import NBA_TEAMS, normalize_team_name

logger = logging.getLogger(__name__)

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
ESPN_STANDINGS_URL = "https://site.api.espn.com/apis/v2/sports/basketball/nba/standings"
_CACHE_TTL_STANDINGS = 6 * 3600  # 6h
_CACHE_TTL_INJURIES = 3 * 3600  # 3h

# In-process caches: (data, fetched_at_epoch)
_standings_cache: tuple[dict, float] | None = None
_injuries_cache: tuple[dict, float] | None = None


@dataclass
class TeamContext:
    """Contextual data for one team in a game."""

    name: str  # "Boston Celtics"
    record: str  # "42-15"
    win_pct: float  # 0.737
    home_record: str | None  # "24-5"
    away_record: str | None  # "18-10"
    last_10: str  # "7-3"
    streak: str  # "W3"
    conference_rank: int  # 1
    rest_days: int  # 前試合からの休息日数 (0=B2B)
    is_back_to_back: bool
    injuries: list[str] = field(default_factory=list)  # ["Jaylen Brown (OUT - knee)"]


@dataclass
class GameContext:
    """Full context for a single game, fed to LLM analysis."""

    home: TeamContext
    away: TeamContext
    game_time_utc: str
    poly_home_price: float
    poly_away_price: float
    h2h_season: str | None = None  # "Home leads 2-1"


def _now_epoch() -> float:
    return datetime.now().timestamp()


def _is_cache_valid(cache: tuple | None, ttl: int) -> bool:
    if cache is None:
        return False
    _, fetched_at = cache
    return (_now_epoch() - fetched_at) < ttl


# ---------------------------------------------------------------------------
# ESPN Standings
# ---------------------------------------------------------------------------


def _fetch_standings() -> dict[str, dict]:
    """Fetch NBA standings from ESPN. Returns {team_display_name: stats_dict}."""
    global _standings_cache
    if _is_cache_valid(_standings_cache, _CACHE_TTL_STANDINGS):
        return _standings_cache[0]  # type: ignore[index]

    url = ESPN_STANDINGS_URL
    result: dict[str, dict] = {}
    try:
        resp = httpx.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        for child in data.get("children", []):
            for entry in child.get("standings", {}).get("entries", []):
                team_info = entry.get("team", {})
                display = team_info.get("displayName", "")
                display = normalize_team_name(display)

                stats_map: dict[str, str] = {}
                for stat in entry.get("stats", []):
                    stats_map[stat.get("name", "")] = stat.get("displayValue", "")

                wins = int(stats_map.get("wins", "0"))
                losses = int(stats_map.get("losses", "0"))
                total = wins + losses
                result[display] = {
                    "record": f"{wins}-{losses}",
                    "win_pct": wins / total if total > 0 else 0.0,
                    "home_record": stats_map.get("Home", None),
                    "away_record": stats_map.get("Road", None),
                    "last_10": stats_map.get("Last Ten Games", ""),
                    "streak": stats_map.get("streak", ""),
                    "conference_rank": int(stats_map.get("playoffSeed", "0")),
                }
        logger.info("Fetched standings for %d teams from ESPN", len(result))
    except Exception:
        logger.warning("ESPN standings fetch failed, using empty data")

    _standings_cache = (result, _now_epoch())
    return result


# ---------------------------------------------------------------------------
# ESPN Injuries
# ---------------------------------------------------------------------------


def _fetch_injuries() -> dict[str, list[str]]:
    """Fetch NBA injuries from ESPN. Returns {team_display_name: [injury_strings]}."""
    global _injuries_cache
    if _is_cache_valid(_injuries_cache, _CACHE_TTL_INJURIES):
        return _injuries_cache[0]  # type: ignore[index]

    result: dict[str, list[str]] = {}
    # ESPN injuries endpoint: per-team
    # We iterate through known teams and fetch injuries
    # Alternatively use the league-wide injuries endpoint
    url = f"{ESPN_BASE}/injuries"
    try:
        resp = httpx.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        for team_entry in data.get("injuries", []):
            # ESPN API 形式: team_entry.displayName (新) or team_entry.team.displayName (旧)
            team_name = team_entry.get("displayName", "")
            if not team_name:
                team_info = team_entry.get("team", {})
                team_name = team_info.get("displayName", "")
            team_name = normalize_team_name(team_name)
            injuries_list: list[str] = []

            for item in team_entry.get("injuries", []):
                player = item.get("athlete", {}).get("displayName", "Unknown")
                status = item.get("status", "")
                detail = item.get("longComment", "") or item.get("shortComment", "")
                label = f"{player} ({status})"
                if detail:
                    label = f"{player} ({status} - {detail[:50]})"
                injuries_list.append(label)

            if injuries_list:
                result[team_name] = injuries_list

        logger.info("Fetched injuries for %d teams from ESPN", len(result))
    except Exception:
        logger.warning("ESPN injuries fetch failed, using empty data")

    _injuries_cache = (result, _now_epoch())
    return result


# ---------------------------------------------------------------------------
# Rest days / B2B detection
# ---------------------------------------------------------------------------


def _calculate_rest_days(team_name: str, game_date: str) -> int:
    """Calculate days since last game for a team. Returns -1 if unknown."""
    try:
        target = datetime.strptime(game_date, "%Y-%m-%d")
    except ValueError:
        return -1

    game_dates = _fetch_season_schedule()
    if not game_dates:
        return -1

    # 全チーム名のマッピング
    team_info = NBA_TEAMS.get(team_name)
    if not team_info:
        return -1

    # 最新の過去試合日を探索
    last_game_date: datetime | None = None
    for gd in game_dates:
        # NBA.com format: "MM/DD/YYYY 00:00:00"
        raw_date = gd.get("gameDate", "")
        try:
            gd_dt = datetime.strptime(raw_date, "%m/%d/%Y %H:%M:%S")
        except ValueError:
            continue

        if gd_dt.date() >= target.date():
            continue

        for game in gd.get("games", []):
            home = game.get("homeTeam", {})
            away = game.get("awayTeam", {})
            home_name = normalize_team_name(
                f"{home.get('teamCity', '')} {home.get('teamName', '')}".strip()
            )
            away_name = normalize_team_name(
                f"{away.get('teamCity', '')} {away.get('teamName', '')}".strip()
            )
            if team_name in (home_name, away_name):
                if last_game_date is None or gd_dt > last_game_date:
                    last_game_date = gd_dt

    if last_game_date is None:
        return -1

    return (target - last_game_date).days


# ---------------------------------------------------------------------------
# Build GameContext
# ---------------------------------------------------------------------------


def build_game_context(
    *,
    home_team: str,
    away_team: str,
    game_date: str,
    game_time_utc: str = "",
    poly_home_price: float = 0.0,
    poly_away_price: float = 0.0,
) -> GameContext:
    """Build a GameContext from all available data sources.

    Gracefully handles API failures — returns partial context with defaults.
    """
    standings = _fetch_standings()
    injuries = _fetch_injuries()

    def _build_team(name: str, is_home: bool) -> TeamContext:
        stats = standings.get(name, {})
        team_injuries = injuries.get(name, [])

        rest = _calculate_rest_days(name, game_date)

        return TeamContext(
            name=name,
            record=stats.get("record", "0-0"),
            win_pct=stats.get("win_pct", 0.0),
            home_record=stats.get("home_record") if is_home else None,
            away_record=stats.get("away_record") if not is_home else None,
            last_10=stats.get("last_10", ""),
            streak=stats.get("streak", ""),
            conference_rank=stats.get("conference_rank", 0),
            rest_days=max(rest, 0),
            is_back_to_back=(rest == 1),
            injuries=team_injuries,
        )

    home_ctx = _build_team(home_team, is_home=True)
    away_ctx = _build_team(away_team, is_home=False)

    return GameContext(
        home=home_ctx,
        away=away_ctx,
        game_time_utc=game_time_utc,
        poly_home_price=poly_home_price,
        poly_away_price=poly_away_price,
    )
