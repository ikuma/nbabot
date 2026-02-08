"""Tests for Polymarket NBA.com-based discovery."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.connectors.nba_schedule import NBAGame
from src.connectors.polymarket import MoneylineMarket
from src.connectors.polymarket_discovery import fetch_all_nba_moneylines


def _make_game(
    game_id: str = "001",
    home: str = "Boston Celtics",
    away: str = "New York Knicks",
    time_utc: str = "2026-02-09T00:30:00Z",
    status: int = 1,
) -> NBAGame:
    return NBAGame(game_id, home, away, time_utc, status)


def _make_moneyline(
    slug: str = "nba-nyk-bos-2026-02-08",
    home: str = "Boston Celtics",
    away: str = "New York Knicks",
) -> MoneylineMarket:
    return MoneylineMarket(
        condition_id="cond1",
        event_slug=slug,
        event_title=f"{away.split()[-1]} vs {home.split()[-1]}",
        home_team=home,
        away_team=away,
        outcomes=[away.split()[-1], home.split()[-1]],
        prices=[0.40, 0.60],
        token_ids=["tok_a", "tok_b"],
        sports_market_type="moneyline",
        active=True,
    )


class TestFetchAllNbaMoneylines:
    @patch("src.connectors.polymarket_discovery.fetch_moneyline_for_game")
    @patch("src.connectors.polymarket_discovery.fetch_todays_games")
    def test_basic_flow(self, mock_games, mock_ml):
        mock_games.return_value = [_make_game()]
        mock_ml.return_value = _make_moneyline()

        result = fetch_all_nba_moneylines()

        assert len(result) == 1
        assert result[0].event_slug == "nba-nyk-bos-2026-02-08"
        mock_ml.assert_called_once_with(
            "New York Knicks", "Boston Celtics", "2026-02-08",
        )

    @patch("src.connectors.polymarket_discovery.fetch_moneyline_for_game")
    @patch("src.connectors.polymarket_discovery.fetch_todays_games")
    def test_utc_to_eastern_date_conversion(self, mock_games, mock_ml):
        """Game at 2026-02-09T03:00:00Z = 2026-02-08 22:00 ET → date=2026-02-08."""
        mock_games.return_value = [
            _make_game(time_utc="2026-02-09T03:00:00Z"),
        ]
        mock_ml.return_value = _make_moneyline()

        fetch_all_nba_moneylines()

        mock_ml.assert_called_once_with(
            "New York Knicks", "Boston Celtics", "2026-02-08",
        )

    @patch("src.connectors.polymarket_discovery.fetch_moneyline_for_game")
    @patch("src.connectors.polymarket_discovery.fetch_todays_games")
    def test_utc_to_eastern_same_day(self, mock_games, mock_ml):
        """Game at 2026-02-08T20:00:00Z = 2026-02-08 15:00 ET → date=2026-02-08."""
        mock_games.return_value = [
            _make_game(time_utc="2026-02-08T20:00:00Z"),
        ]
        mock_ml.return_value = _make_moneyline()

        fetch_all_nba_moneylines()

        mock_ml.assert_called_once_with(
            "New York Knicks", "Boston Celtics", "2026-02-08",
        )

    @patch("src.connectors.polymarket_discovery.fetch_moneyline_for_game")
    @patch("src.connectors.polymarket_discovery.fetch_todays_games")
    def test_no_games(self, mock_games, mock_ml):
        mock_games.return_value = []

        result = fetch_all_nba_moneylines()

        assert result == []
        mock_ml.assert_not_called()

    @patch("src.connectors.polymarket_discovery.fetch_moneyline_for_game")
    @patch("src.connectors.polymarket_discovery.fetch_todays_games")
    def test_moneyline_not_found(self, mock_games, mock_ml):
        mock_games.return_value = [_make_game()]
        mock_ml.return_value = None

        result = fetch_all_nba_moneylines()

        assert result == []

    @patch("src.connectors.polymarket_discovery.fetch_moneyline_for_game")
    @patch("src.connectors.polymarket_discovery.fetch_todays_games")
    def test_bad_game_time_skipped(self, mock_games, mock_ml):
        mock_games.return_value = [
            _make_game(time_utc="not-a-date"),
            _make_game(game_id="002", time_utc="2026-02-08T20:00:00Z"),
        ]
        mock_ml.return_value = _make_moneyline()

        result = fetch_all_nba_moneylines()

        # First game skipped, second succeeds
        assert len(result) == 1
        assert mock_ml.call_count == 1

    @patch("src.connectors.polymarket_discovery.fetch_moneyline_for_game")
    @patch("src.connectors.polymarket_discovery.fetch_todays_games")
    def test_unknown_team_skipped(self, mock_games, mock_ml):
        mock_games.return_value = [
            _make_game(home="Unknown Team", away="Another Team"),
        ]
        mock_ml.return_value = None

        result = fetch_all_nba_moneylines()

        # build_event_slug returns None for unknown teams → skipped
        assert result == []
        mock_ml.assert_not_called()

    @patch("src.connectors.polymarket_discovery.fetch_moneyline_for_game")
    @patch("src.connectors.polymarket_discovery.fetch_todays_games")
    def test_multiple_games(self, mock_games, mock_ml):
        games = [
            _make_game(game_id="001", home="Boston Celtics", away="New York Knicks",
                       time_utc="2026-02-08T20:00:00Z"),
            _make_game(game_id="002", home="Miami Heat", away="Washington Wizards",
                       time_utc="2026-02-08T22:00:00Z"),
        ]
        mock_games.return_value = games

        ml1 = _make_moneyline(slug="nba-nyk-bos-2026-02-08")
        ml2 = _make_moneyline(slug="nba-was-mia-2026-02-08", home="Miami Heat", away="Washington Wizards")
        mock_ml.side_effect = [ml1, ml2]

        result = fetch_all_nba_moneylines()

        assert len(result) == 2
        assert mock_ml.call_count == 2
