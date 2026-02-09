"""Tests for auto-settle logic in scripts/settle.py."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Ensure scripts/ is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.settle import (
    AutoSettleSummary,
    SettleResult,
    _calc_pnl,
    _determine_winner,
    _parse_slug,
    auto_settle,
)
from src.connectors.nba_schedule import NBAGame
from src.store.db import SignalRecord


def _make_signal(
    id: int = 1,
    event_slug: str = "nba-nyk-bos-2026-02-09",
    team: str = "Celtics",
    poly_price: float = 0.40,
    kelly_size: float = 50.0,
    **kwargs,
) -> SignalRecord:
    defaults = dict(
        game_title="Knicks vs Celtics",
        side="BUY",
        book_prob=0.6,
        edge_pct=5.0,
        token_id="tok123",
        bookmakers_count=0,
        consensus_std=0.0,
        commence_time="",
        created_at="2026-02-09T00:00:00+00:00",
    )
    defaults.update(kwargs)
    return SignalRecord(
        id=id,
        event_slug=event_slug,
        team=team,
        poly_price=poly_price,
        kelly_size=kelly_size,
        **defaults,
    )


def _make_game(
    home_team: str = "Boston Celtics",
    away_team: str = "New York Knicks",
    home_score: int = 112,
    away_score: int = 105,
    game_status: int = 3,
) -> NBAGame:
    return NBAGame(
        game_id="001",
        home_team=home_team,
        away_team=away_team,
        game_time_utc="2026-02-09T17:30:00Z",
        game_status=game_status,
        home_score=home_score,
        away_score=away_score,
    )


class TestParseSlug:
    def test_valid_slug(self):
        result = _parse_slug("nba-nyk-bos-2026-02-09")
        assert result == ("nyk", "bos", "2026-02-09")

    def test_invalid_slug(self):
        assert _parse_slug("invalid") is None
        assert _parse_slug("nba-xx-bos-2026-02-09") is None  # 2-char abbr
        assert _parse_slug("") is None

    def test_non_nba_prefix(self):
        assert _parse_slug("mlb-nyk-bos-2026-02-09") is None


class TestDetermineWinner:
    def test_home_wins(self):
        game = _make_game(home_score=112, away_score=105)
        assert _determine_winner(game) == "Boston Celtics"

    def test_away_wins(self):
        game = _make_game(home_score=100, away_score=110)
        assert _determine_winner(game) == "New York Knicks"

    def test_tie(self):
        game = _make_game(home_score=100, away_score=100)
        assert _determine_winner(game) is None


class TestCalcPnl:
    def test_win(self):
        pnl = _calc_pnl(True, 50.0, 0.40)
        # 50 * (1/0.40 - 1) = 50 * 1.5 = 75
        assert pnl == pytest.approx(75.0)

    def test_loss(self):
        pnl = _calc_pnl(False, 50.0, 0.40)
        assert pnl == -50.0


class TestAutoSettle:
    @patch("src.connectors.nba_schedule.fetch_todays_games")
    @patch("src.store.db.get_unsettled")
    @patch("src.store.db.log_result")
    def test_settles_winning_signal(self, mock_log_result, mock_unsettled, mock_games):
        """Signal on winning team → WIN settlement."""
        signal = _make_signal(team="Celtics", event_slug="nba-nyk-bos-2026-02-09")
        mock_unsettled.return_value = [signal]
        mock_games.return_value = [_make_game(home_score=112, away_score=105)]

        summary = auto_settle(today="2026-02-09")

        assert len(summary.settled) == 1
        assert summary.settled[0].won is True
        assert summary.settled[0].method == "nba_scores"
        assert summary.wins == 1
        assert summary.losses == 0
        mock_log_result.assert_called_once()

    @patch("src.connectors.nba_schedule.fetch_todays_games")
    @patch("src.store.db.get_unsettled")
    @patch("src.store.db.log_result")
    def test_settles_losing_signal(self, mock_log_result, mock_unsettled, mock_games):
        """Signal on losing team → LOSS settlement."""
        signal = _make_signal(team="Knicks", event_slug="nba-nyk-bos-2026-02-09")
        mock_unsettled.return_value = [signal]
        mock_games.return_value = [_make_game(home_score=112, away_score=105)]

        summary = auto_settle(today="2026-02-09")

        assert len(summary.settled) == 1
        assert summary.settled[0].won is False
        assert summary.losses == 1

    @patch("src.connectors.nba_schedule.fetch_todays_games")
    @patch("src.store.db.get_unsettled")
    @patch("src.store.db.log_result")
    def test_dry_run_no_db_write(self, mock_log_result, mock_unsettled, mock_games):
        """Dry run should not call log_result."""
        signal = _make_signal(team="Celtics", event_slug="nba-nyk-bos-2026-02-09")
        mock_unsettled.return_value = [signal]
        mock_games.return_value = [_make_game(home_score=112, away_score=105)]

        summary = auto_settle(dry_run=True, today="2026-02-09")

        assert len(summary.settled) == 1
        mock_log_result.assert_not_called()

    @patch("src.connectors.nba_schedule.fetch_todays_games")
    @patch("src.store.db.get_unsettled")
    def test_skips_non_final_games(self, mock_unsettled, mock_games):
        """In-progress games should be skipped."""
        signal = _make_signal(event_slug="nba-nyk-bos-2026-02-09")
        mock_unsettled.return_value = [signal]
        # ゲームはまだ進行中 (status=2)
        mock_games.return_value = [_make_game(game_status=2)]

        summary = auto_settle(today="2026-02-09")

        assert len(summary.settled) == 0
        assert summary.skipped == 1

    @patch("src.connectors.nba_schedule.fetch_todays_games")
    @patch("src.store.db.get_unsettled")
    def test_skips_invalid_slug(self, mock_unsettled, mock_games):
        signal = _make_signal(event_slug="invalid-slug")
        mock_unsettled.return_value = [signal]
        mock_games.return_value = []

        summary = auto_settle(today="2026-02-09")

        assert len(summary.settled) == 0
        assert summary.skipped == 1

    @patch("src.connectors.nba_schedule.fetch_todays_games")
    @patch("src.store.db.get_unsettled")
    def test_no_unsettled(self, mock_unsettled, mock_games):
        mock_unsettled.return_value = []
        summary = auto_settle(today="2026-02-09")
        assert len(summary.settled) == 0
        assert summary.skipped == 0
        mock_games.assert_not_called()

    @patch("scripts.settle._try_polymarket_fallback")
    @patch("src.connectors.nba_schedule.fetch_todays_games")
    @patch("src.store.db.get_unsettled")
    @patch("src.store.db.log_result")
    def test_polymarket_fallback(
        self, mock_log_result, mock_unsettled, mock_games, mock_fallback,
    ):
        """Past-date signals use Polymarket fallback."""
        signal = _make_signal(team="Celtics", event_slug="nba-nyk-bos-2026-02-08")
        mock_unsettled.return_value = [signal]
        mock_games.return_value = []
        mock_fallback.return_value = ("Celtics", "polymarket")

        summary = auto_settle(today="2026-02-09")

        assert len(summary.settled) == 1
        assert summary.settled[0].method == "polymarket"
        assert summary.settled[0].won is True


class TestAutoSettleSummary:
    def test_empty_summary(self):
        s = AutoSettleSummary()
        assert s.wins == 0
        assert s.losses == 0
        assert s.total_pnl == 0.0
        assert "no signals settled" in s.format_summary()

    def test_summary_with_results(self):
        s = AutoSettleSummary(
            settled=[
                SettleResult(1, "Celtics", True, 75.0, "nba_scores"),
                SettleResult(2, "Knicks", False, -50.0, "nba_scores"),
            ],
            skipped=1,
        )
        assert s.wins == 1
        assert s.losses == 1
        assert s.total_pnl == pytest.approx(25.0)
        text = s.format_summary()
        assert "Settled: 2" in text
        assert "Skipped: 1" in text
        assert "WIN" in text
        assert "LOSS" in text
