"""Polymarket CLOB API connector for fetching NBA markets and prices."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from src.config import settings
from src.connectors.team_mapping import build_event_slug, full_name_from_short

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")

NBA_KEYWORDS = [
    "nba",
    "basketball",
    "lakers",
    "celtics",
    "warriors",
    "nuggets",
    "bucks",
    "76ers",
    "suns",
    "heat",
    "knicks",
    "nets",
    "bulls",
    "cavaliers",
    "mavericks",
    "rockets",
    "clippers",
    "grizzlies",
    "timberwolves",
    "thunder",
    "kings",
    "hawks",
    "raptors",
    "pacers",
    "magic",
    "hornets",
    "wizards",
    "pistons",
    "spurs",
    "blazers",
    "jazz",
    "pelicans",
]


@dataclass
class MarketToken:
    token_id: str
    outcome: str
    price: float


@dataclass
class NBAMarket:
    condition_id: str
    question: str
    tokens: list[MarketToken]
    end_date: str
    active: bool
    slug: str = ""

    @property
    def yes_price(self) -> float | None:
        for t in self.tokens:
            if t.outcome.upper() == "YES":
                return t.price
        return None

    @property
    def no_price(self) -> float | None:
        for t in self.tokens:
            if t.outcome.upper() == "NO":
                return t.price
        return None


def _apply_proxy() -> None:
    """Set env-level proxy so py-clob-client's internal httpx picks it up."""
    if settings.http_proxy:
        os.environ.setdefault("HTTPS_PROXY", settings.http_proxy)
        os.environ.setdefault("HTTP_PROXY", settings.http_proxy)


def _get_httpx_client() -> httpx.Client:
    proxy = settings.http_proxy or None
    return httpx.Client(proxy=proxy, timeout=30)


def _create_client(authenticated: bool = False):
    from py_clob_client.client import ClobClient

    _apply_proxy()
    if authenticated and settings.polymarket_private_key:
        client = ClobClient(
            host=settings.polymarket_host,
            key=settings.polymarket_private_key,
            chain_id=settings.polymarket_chain_id,
        )
        client.set_api_creds(client.create_or_derive_api_creds())
        return client
    return ClobClient(host=settings.polymarket_host)


def _is_nba_market(market: dict[str, Any]) -> bool:
    question = market.get("question", "")
    # Strict check: question starts with "NBA:" or contains "NBA" as a word
    if question.startswith("NBA:") or question.startswith("NBA "):
        return True
    text = (question + " " + market.get("description", "")).lower()
    # Require "nba" as a whole word to avoid false positives
    if " nba " in f" {text} ":
        return True
    return False


def _parse_market(raw: dict[str, Any]) -> NBAMarket:
    tokens = []
    for t in raw.get("tokens", []):
        tokens.append(
            MarketToken(
                token_id=t["token_id"],
                outcome=t.get("outcome", ""),
                price=float(t.get("price", 0)),
            )
        )
    return NBAMarket(
        condition_id=raw.get("condition_id", ""),
        question=raw.get("question", ""),
        tokens=tokens,
        end_date=raw.get("end_date_iso", ""),
        active=raw.get("active", False),
        slug=raw.get("slug", ""),
    )


def _parse_json_or_csv(value: str | list) -> list[str]:
    """Parse a value that may be a JSON array string or comma-separated string."""
    if isinstance(value, list):
        return [str(v) for v in value]
    if not isinstance(value, str) or not value:
        return []
    value = value.strip()
    if value.startswith("["):
        import json

        try:
            return [str(v) for v in json.loads(value)]
        except (json.JSONDecodeError, ValueError):
            pass
    return [x.strip() for x in value.split(",") if x.strip()]


def fetch_nba_markets_gamma() -> list[NBAMarket]:
    """Fetch NBA markets via the Gamma Markets API."""
    client = _get_httpx_client()
    markets: list[NBAMarket] = []
    offset = 0
    limit = 100

    for _ in range(10):  # safety limit
        resp = client.get(
            f"{settings.gamma_api_url}/markets",
            params={
                "closed": "false",
                "active": "true",
                "limit": limit,
                "offset": offset,
            },
        )
        resp.raise_for_status()
        data = resp.json()

        if not data:
            break

        for raw in data:
            question = raw.get("question", "")
            if not _is_nba_market({"question": question, "description": ""}):
                continue

            clob_ids = _parse_json_or_csv(raw.get("clobTokenIds", ""))
            outcomes = _parse_json_or_csv(raw.get("outcomes", ""))
            prices = _parse_json_or_csv(raw.get("outcomePrices", ""))

            tokens = []
            for i, tid in enumerate(clob_ids):
                tokens.append(
                    MarketToken(
                        token_id=tid,
                        outcome=outcomes[i] if i < len(outcomes) else f"Outcome {i}",
                        price=float(prices[i]) if i < len(prices) else 0.0,
                    )
                )

            markets.append(
                NBAMarket(
                    condition_id=raw.get("conditionId", raw.get("condition_id", "")),
                    question=question,
                    tokens=tokens,
                    end_date=raw.get("endDate", raw.get("end_date_iso", "")),
                    active=raw.get("active", True),
                    slug=raw.get("slug", ""),
                )
            )

        if len(data) < limit:
            break
        offset += limit

    logger.info("Found %d NBA markets via Gamma API", len(markets))
    return markets


def fetch_nba_markets() -> list[NBAMarket]:
    """Fetch all active NBA markets. Tries Gamma API first, falls back to CLOB."""
    try:
        markets = fetch_nba_markets_gamma()
        if markets:
            return markets
    except Exception as e:
        logger.warning("Gamma API failed (%s), falling back to CLOB API", e)

    # Fallback: CLOB API with keyword filtering
    client = _create_client()
    markets = []
    cursor = None

    for _ in range(20):
        kwargs: dict[str, Any] = {}
        if cursor:
            kwargs["next_cursor"] = cursor
        resp = client.get_markets(**kwargs)

        for raw in resp.get("data", []):
            if raw.get("active") and _is_nba_market(raw):
                markets.append(_parse_market(raw))

        cursor = resp.get("next_cursor")
        if not cursor or cursor == "LTE=":
            break

    logger.info("Found %d active NBA markets via CLOB", len(markets))
    return markets


def get_midpoint(token_id: str) -> float:
    """Get midpoint price for a token."""
    client = _create_client()
    return float(client.get_midpoint(token_id))


def get_order_book(token_id: str) -> dict[str, Any]:
    """Get order book for a specific token."""
    client = _create_client()
    return client.get_order_book(token_id)


def get_balance() -> dict[str, Any]:
    """Get account USDC balance (requires authentication)."""
    client = _create_client(authenticated=True)
    return client.get_balance_allowance(asset_type=0)  # COLLATERAL


# ---------------------------------------------------------------------------
# Events API – per-game moneyline markets
# ---------------------------------------------------------------------------


@dataclass
class MoneylineMarket:
    condition_id: str
    event_slug: str
    event_title: str  # "Knicks vs. Celtics"
    home_team: str  # full name, e.g. "Boston Celtics"
    away_team: str  # full name, e.g. "New York Knicks"
    outcomes: list[str]  # ["Knicks", "Celtics"]
    prices: list[float]  # [0.405, 0.595]
    token_ids: list[str]
    sports_market_type: str
    active: bool


def fetch_moneyline_for_game(
    away_team: str,
    home_team: str,
    game_date: str,
) -> MoneylineMarket | None:
    """Fetch the moneyline market for a single game via the Gamma Events API.

    Args:
        away_team: Full team name from Odds API (e.g. "New York Knicks").
        home_team: Full team name from Odds API (e.g. "Boston Celtics").
        game_date: Date string "YYYY-MM-DD" in US Eastern time.
    """
    slug = build_event_slug(away_team, home_team, game_date)
    if not slug:
        logger.warning("Cannot build slug for %s @ %s", away_team, home_team)
        return None

    client = _get_httpx_client()
    try:
        resp = client.get(
            f"{settings.gamma_api_url}/events",
            params={"slug": slug},
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        logger.exception("Events API request failed for slug=%s", slug)
        return None

    # The API returns a list; find our event
    events = data if isinstance(data, list) else [data]
    if not events:
        logger.debug("No event found for slug=%s", slug)
        return None

    event = events[0]
    event_title = event.get("title", f"{away_team} vs {home_team}")

    # Search nested markets for the moneyline
    moneyline_count = 0
    for mkt in event.get("markets", []):
        if mkt.get("sportsMarketType") != "moneyline":
            continue

        moneyline_count += 1
        if moneyline_count > 1:
            logger.warning("Multiple moneyline markets for slug=%s, using first", slug)
            break

        outcomes = _parse_json_or_csv(mkt.get("outcomes", ""))
        prices_raw = _parse_json_or_csv(mkt.get("outcomePrices", ""))
        token_ids = _parse_json_or_csv(mkt.get("clobTokenIds", ""))
        prices = [float(p) for p in prices_raw] if prices_raw else []

        if not outcomes:
            logger.warning("Empty outcomes for slug=%s", slug)
            return None
        if not prices:
            logger.warning("Empty prices for slug=%s", slug)
            return None
        if len(outcomes) != 2:
            logger.warning(
                "Expected 2 outcomes, got %d for slug=%s",
                len(outcomes),
                slug,
            )
        if len(token_ids) != len(outcomes):
            logger.warning(
                "Token count mismatch: %d tokens vs %d outcomes for slug=%s",
                len(token_ids),
                len(outcomes),
                slug,
            )

        return MoneylineMarket(
            condition_id=mkt.get("conditionId", mkt.get("condition_id", "")),
            event_slug=slug,
            event_title=event_title,
            home_team=home_team,
            away_team=away_team,
            outcomes=outcomes,
            prices=prices,
            token_ids=token_ids,
            sports_market_type="moneyline",
            active=mkt.get("active", True),
        )

    logger.debug("No moneyline market in event slug=%s", slug)
    return None


def fetch_all_moneylines(games: list) -> list[MoneylineMarket]:
    """Fetch moneyline markets for all today's games.

    Args:
        games: list of GameOdds from the Odds API.
    """
    moneylines: list[MoneylineMarket] = []

    for game in games:
        # Convert UTC commence_time to Eastern date
        try:
            utc_dt = datetime.fromisoformat(game.commence_time.replace("Z", "+00:00"))
            et_dt = utc_dt.astimezone(ET)
            game_date = et_dt.strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            logger.warning("Bad commence_time for %s @ %s", game.away_team, game.home_team)
            continue

        ml = fetch_moneyline_for_game(game.away_team, game.home_team, game_date)
        if ml:
            moneylines.append(ml)
        else:
            logger.info(
                "No moneyline found: %s @ %s (%s)",
                game.away_team,
                game.home_team,
                game_date,
            )

    logger.info("Fetched %d moneyline markets for %d games", len(moneylines), len(games))
    return moneylines
