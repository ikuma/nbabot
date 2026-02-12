"""Shared test helpers — import in test files: from tests.helpers import insert_signal."""

from __future__ import annotations

from pathlib import Path

from src.connectors.polymarket import MoneylineMarket
from src.store.db import log_signal, upsert_trade_job


def insert_signal(db_path: Path, **overrides) -> int:
    """Insert a signal with sensible defaults. Override any field via kwargs."""
    defaults = {
        "game_title": "Knicks vs Celtics",
        "event_slug": "nba-nyk-bos-2026-02-08",
        "team": "Celtics",
        "side": "BUY",
        "poly_price": 0.40,
        "book_prob": 0.6,
        "edge_pct": 5.0,
        "kelly_size": 25.0,
        "token_id": "tok123",
        "db_path": db_path,
    }
    defaults.update(overrides)
    return log_signal(**defaults)


def insert_trade_job(db_path: Path, **overrides) -> None:
    """Insert a trade job with sensible defaults."""
    defaults = {
        "game_date": "2026-02-10",
        "event_slug": "nba-nyk-bos-2026-02-10",
        "home_team": "Boston Celtics",
        "away_team": "New York Knicks",
        "game_time_utc": "2026-02-11T01:00:00+00:00",
        "execute_after": "2026-02-10T17:00:00+00:00",
        "execute_before": "2026-02-11T01:00:00+00:00",
        "db_path": db_path,
    }
    defaults.update(overrides)
    upsert_trade_job(**defaults)


def make_moneyline(
    outcomes: list[str],
    prices: list[float],
    active: bool = True,
    slug: str = "nba-nyk-bos-2026-02-08",
    title: str = "Knicks vs Celtics",
) -> MoneylineMarket:
    """Build a MoneylineMarket for testing."""
    return MoneylineMarket(
        condition_id="cond1",
        event_slug=slug,
        event_title=title,
        home_team="Boston Celtics",
        away_team="New York Knicks",
        outcomes=outcomes,
        prices=prices,
        token_ids=[f"token_{i}" for i in range(len(outcomes))],
        sports_market_type="moneyline",
        active=active,
    )
