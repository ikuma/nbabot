"""Shared fixtures for nbabot tests.

Helper functions (insert_signal, make_moneyline, etc.) are in tests/helpers.py.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.connectors.odds_api import BookmakerOdds, GameOdds, TeamOdds
from src.connectors.polymarket import MoneylineMarket


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    """Temporary database path — each test gets an isolated SQLite file."""
    return tmp_path / "test.db"


@pytest.fixture()
def sample_game_odds() -> GameOdds:
    """BOS vs NYK game with two bookmakers."""
    return GameOdds(
        game_id="abc123",
        home_team="Boston Celtics",
        away_team="New York Knicks",
        commence_time="2026-02-08T00:00:00Z",
        bookmakers=[
            BookmakerOdds(
                bookmaker="fanduel",
                last_update="2026-02-07T23:00:00Z",
                outcomes=[
                    TeamOdds("Boston Celtics", -200, 0.6667),
                    TeamOdds("New York Knicks", +150, 0.4),
                ],
            ),
            BookmakerOdds(
                bookmaker="draftkings",
                last_update="2026-02-07T23:00:00Z",
                outcomes=[
                    TeamOdds("Boston Celtics", -180, 0.6429),
                    TeamOdds("New York Knicks", +160, 0.3846),
                ],
            ),
        ],
    )


@pytest.fixture()
def sample_moneyline() -> MoneylineMarket:
    """Moneyline market matching sample_game_odds."""
    return MoneylineMarket(
        condition_id="cond1",
        event_slug="nba-nyk-bos-2026-02-08",
        event_title="Knicks vs Celtics",
        home_team="Boston Celtics",
        away_team="New York Knicks",
        outcomes=["Knicks", "Celtics"],
        prices=[0.35, 0.55],
        token_ids=["token_knicks", "token_celtics"],
        sports_market_type="moneyline",
        active=True,
    )
