"""The Odds API connector for fetching NBA sportsbook odds."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

from src.config import settings

logger = logging.getLogger(__name__)

BASE_URL = "https://api.the-odds-api.com/v4"
SPORT = "basketball_nba"


@dataclass
class TeamOdds:
    team: str
    american_odds: int
    implied_prob: float


@dataclass
class BookmakerOdds:
    bookmaker: str
    last_update: str
    outcomes: list[TeamOdds]


@dataclass
class GameOdds:
    game_id: str
    home_team: str
    away_team: str
    commence_time: str
    bookmakers: list[BookmakerOdds]

    @property
    def consensus_probs(self) -> dict[str, float]:
        """Average implied probability across all bookmakers (vig-removed)."""
        team_probs: dict[str, list[float]] = {}
        for bm in self.bookmakers:
            total = sum(o.implied_prob for o in bm.outcomes)
            for o in bm.outcomes:
                # Remove vig by normalizing
                fair_prob = o.implied_prob / total if total > 0 else 0
                team_probs.setdefault(o.team, []).append(fair_prob)

        return {team: sum(ps) / len(ps) for team, ps in team_probs.items()}


def american_to_prob(odds: int) -> float:
    """Convert American odds to implied probability (0-1)."""
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    return 100 / (odds + 100)


def fetch_nba_odds(
    regions: list[str] | None = None,
    markets: list[str] | None = None,
) -> list[GameOdds]:
    """Fetch NBA moneyline odds from The Odds API."""
    if not settings.odds_api_key:
        raise ValueError("ODDS_API_KEY not set in .env")

    regions = regions or ["us"]
    markets = markets or ["h2h"]

    resp = httpx.get(
        f"{BASE_URL}/sports/{SPORT}/odds",
        params={
            "apiKey": settings.odds_api_key,
            "regions": ",".join(regions),
            "markets": ",".join(markets),
            "oddsFormat": "american",
        },
        timeout=30,
    )
    resp.raise_for_status()

    remaining = resp.headers.get("x-requests-remaining", "?")
    logger.info("Odds API requests remaining: %s", remaining)

    games: list[GameOdds] = []
    for raw in resp.json():
        bookmakers: list[BookmakerOdds] = []
        for bm in raw.get("bookmakers", []):
            h2h = next((m for m in bm["markets"] if m["key"] == "h2h"), None)
            if not h2h:
                continue
            outcomes = []
            for o in h2h["outcomes"]:
                odds_val = int(o["price"])
                outcomes.append(TeamOdds(
                    team=o["name"],
                    american_odds=odds_val,
                    implied_prob=american_to_prob(odds_val),
                ))
            bookmakers.append(BookmakerOdds(
                bookmaker=bm["key"],
                last_update=bm.get("last_update", ""),
                outcomes=outcomes,
            ))

        games.append(GameOdds(
            game_id=raw["id"],
            home_team=raw["home_team"],
            away_team=raw["away_team"],
            commence_time=raw["commence_time"],
            bookmakers=bookmakers,
        ))

    logger.info("Fetched odds for %d NBA games", len(games))
    return games
