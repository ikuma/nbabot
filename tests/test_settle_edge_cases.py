"""Tests for settlement edge cases: OT and postponed games (Phase D2)."""

from __future__ import annotations

from src.connectors.nba_schedule import NBAGame
from src.settlement.settler import _determine_winner


class TestDetermineWinnerOT:
    def test_regulation_game(self):
        game = NBAGame(
            game_id="1",
            home_team="Boston Celtics",
            away_team="New York Knicks",
            game_time_utc="2026-02-10T20:00:00Z",
            game_status=3,
            home_score=110,
            away_score=105,
            period=4,
            game_status_text="Final",
        )
        winner = _determine_winner(game)
        assert winner == "Boston Celtics"

    def test_overtime_game(self):
        game = NBAGame(
            game_id="2",
            home_team="Boston Celtics",
            away_team="New York Knicks",
            game_time_utc="2026-02-10T20:00:00Z",
            game_status=3,
            home_score=120,
            away_score=115,
            period=5,
            game_status_text="Final/OT",
        )
        winner = _determine_winner(game)
        assert winner == "Boston Celtics"

    def test_double_overtime(self):
        game = NBAGame(
            game_id="3",
            home_team="Boston Celtics",
            away_team="New York Knicks",
            game_time_utc="2026-02-10T20:00:00Z",
            game_status=3,
            home_score=130,
            away_score=125,
            period=6,
            game_status_text="Final/2OT",
        )
        winner = _determine_winner(game)
        assert winner == "Boston Celtics"

    def test_away_wins_ot(self):
        game = NBAGame(
            game_id="4",
            home_team="Boston Celtics",
            away_team="New York Knicks",
            game_time_utc="2026-02-10T20:00:00Z",
            game_status=3,
            home_score=115,
            away_score=120,
            period=5,
            game_status_text="Final/OT",
        )
        winner = _determine_winner(game)
        assert winner == "New York Knicks"


class TestDetermineWinnerPostponed:
    def test_postponed_game(self):
        game = NBAGame(
            game_id="5",
            home_team="Boston Celtics",
            away_team="New York Knicks",
            game_time_utc="2026-02-10T20:00:00Z",
            game_status=1,
            home_score=0,
            away_score=0,
            period=0,
            game_status_text="Postponed",
        )
        winner = _determine_winner(game)
        assert winner is None

    def test_cancelled_game(self):
        game = NBAGame(
            game_id="6",
            home_team="Boston Celtics",
            away_team="New York Knicks",
            game_time_utc="2026-02-10T20:00:00Z",
            game_status=1,
            home_score=0,
            away_score=0,
            period=0,
            game_status_text="Cancelled",
        )
        winner = _determine_winner(game)
        assert winner is None

    def test_normal_scheduled_game(self):
        """Scheduled game without 'postponed' text → returns None (scores 0-0)."""
        game = NBAGame(
            game_id="7",
            home_team="Boston Celtics",
            away_team="New York Knicks",
            game_time_utc="2026-02-10T20:00:00Z",
            game_status=1,
            home_score=0,
            away_score=0,
            period=0,
            game_status_text="",
        )
        winner = _determine_winner(game)
        assert winner is None  # tie / no scores

    def test_game_without_new_fields(self):
        """NBAGame without period/game_status_text (backward compat)."""
        game = NBAGame(
            game_id="8",
            home_team="Boston Celtics",
            away_team="New York Knicks",
            game_time_utc="2026-02-10T20:00:00Z",
            game_status=3,
            home_score=110,
            away_score=105,
        )
        winner = _determine_winner(game)
        assert winner == "Boston Celtics"
