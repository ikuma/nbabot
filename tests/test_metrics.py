"""Tests for decomposed metrics (Phase M1)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.analysis.metrics import (
    DecomposedMetrics,
    compute_decomposed_metrics,
    format_decomposed_summary,
)
from src.store.models import ResultRecord, SignalRecord


def _make_signal(**overrides) -> SignalRecord:
    """Create a minimal SignalRecord with defaults."""
    defaults = dict(
        id=1,
        game_title="Test Game",
        event_slug="nba-nyk-bos-2026-02-01",
        team="Knicks",
        side="BUY",
        poly_price=0.40,
        book_prob=0.0,
        edge_pct=10.0,
        kelly_size=25.0,
        token_id="tok1",
        bookmakers_count=0,
        consensus_std=0.0,
        commence_time="",
        created_at="2026-02-01T12:00:00Z",
        shares_merged=0.0,
        merge_recovery_usd=0.0,
    )
    defaults.update(overrides)
    return SignalRecord(**defaults)


def _make_result(**overrides) -> ResultRecord:
    """Create a minimal ResultRecord with defaults."""
    defaults = dict(
        id=1,
        signal_id=1,
        outcome="Knicks",
        won=True,
        settlement_price=1.0,
        pnl=10.0,
        settled_at="2026-02-01T22:00:00Z",
    )
    defaults.update(overrides)
    return ResultRecord(**defaults)


class TestComputeDecomposedMetrics:
    """Test compute_decomposed_metrics with various scenarios."""

    def test_empty_input(self):
        m = compute_decomposed_metrics([])
        assert m.total_settled == 0
        assert m.game_correct_rate == 0.0
        assert m.trade_profit_rate == 0.0
        assert m.merge_rate == 0.0

    def test_all_wins_no_merge(self):
        """All games correct, all profitable, no merges."""
        pairs = [
            (_make_result(id=i, signal_id=i, won=True, pnl=10.0),
             _make_signal(id=i))
            for i in range(1, 6)
        ]
        m = compute_decomposed_metrics(pairs)
        assert m.total_settled == 5
        assert m.game_correct_rate == 1.0
        assert m.game_correct_count == 5
        assert m.game_incorrect_count == 0
        assert m.trade_profit_rate == 1.0
        assert m.merge_rate == 0.0

    def test_all_losses_no_merge(self):
        """All games incorrect, all unprofitable, no merges."""
        pairs = [
            (_make_result(id=i, signal_id=i, won=False, pnl=-25.0),
             _make_signal(id=i))
            for i in range(1, 4)
        ]
        m = compute_decomposed_metrics(pairs)
        assert m.total_settled == 3
        assert m.game_correct_rate == 0.0
        assert m.trade_profit_rate == 0.0
        assert m.merge_rate == 0.0

    def test_game_loss_but_profitable_via_merge(self):
        """Game lost but MERGE recovery makes the trade profitable."""
        sig = _make_signal(id=1, shares_merged=50.0, merge_recovery_usd=20.0)
        res = _make_result(id=1, signal_id=1, won=False, pnl=2.0)  # profitable despite loss
        m = compute_decomposed_metrics([(res, sig)])

        assert m.game_correct_count == 0  # game was lost
        assert m.game_incorrect_count == 1
        assert m.trade_profitable_count == 1  # but trade is profitable
        assert m.merge_settled_count == 1
        assert m.game_correct_rate == 0.0
        assert m.trade_profit_rate == 1.0
        assert m.merge_rate == 1.0

    def test_mixed_scenario(self):
        """Mix of wins, losses, merges, profitable and unprofitable."""
        pairs = [
            # Win, profitable, no merge
            (_make_result(id=1, signal_id=1, won=True, pnl=15.0),
             _make_signal(id=1)),
            # Win, profitable, with merge
            (_make_result(id=2, signal_id=2, won=True, pnl=5.0),
             _make_signal(id=2, shares_merged=10.0, merge_recovery_usd=8.0)),
            # Loss, unprofitable, with merge (merge didn't fully recover)
            (_make_result(id=3, signal_id=3, won=False, pnl=-3.0),
             _make_signal(id=3, shares_merged=20.0, merge_recovery_usd=15.0)),
            # Loss, unprofitable, no merge
            (_make_result(id=4, signal_id=4, won=False, pnl=-25.0),
             _make_signal(id=4)),
        ]
        m = compute_decomposed_metrics(pairs)
        assert m.total_settled == 4
        assert m.game_correct_count == 2
        assert m.game_incorrect_count == 2
        assert m.game_correct_rate == pytest.approx(0.5)
        assert m.trade_profitable_count == 2
        assert m.trade_unprofitable_count == 2
        assert m.trade_profit_rate == pytest.approx(0.5)
        assert m.merge_settled_count == 2
        assert m.merge_rate == pytest.approx(0.5)

    def test_dca_signals_with_merge(self):
        """Multiple DCA entries where some are merged."""
        pairs = [
            # DCA entry 1: merged, lost game, but merge recovery gives small profit
            (_make_result(id=1, signal_id=1, won=False, pnl=0.50),
             _make_signal(id=1, dca_sequence=1, shares_merged=30.0, merge_recovery_usd=18.0)),
            # DCA entry 2: merged, lost game, small loss after merge
            (_make_result(id=2, signal_id=2, won=False, pnl=-1.20),
             _make_signal(id=2, dca_sequence=2, shares_merged=25.0, merge_recovery_usd=14.0)),
            # Hedge: won game, merged, profitable
            (_make_result(id=3, signal_id=3, won=True, pnl=3.0),
             _make_signal(id=3, signal_role="hedge", shares_merged=40.0, merge_recovery_usd=22.0)),
        ]
        m = compute_decomposed_metrics(pairs)
        assert m.total_settled == 3
        assert m.game_correct_count == 1
        assert m.game_incorrect_count == 2
        assert m.trade_profitable_count == 2  # pnl=0.50 and pnl=3.0
        assert m.trade_unprofitable_count == 1  # pnl=-1.20
        assert m.merge_settled_count == 3  # all had shares_merged > 0

    def test_zero_pnl_is_unprofitable(self):
        """P&L exactly 0 counts as unprofitable (not > 0)."""
        pairs = [
            (_make_result(id=1, signal_id=1, won=True, pnl=0.0),
             _make_signal(id=1)),
        ]
        m = compute_decomposed_metrics(pairs)
        assert m.trade_profitable_count == 0
        assert m.trade_unprofitable_count == 1


class TestFormatDecomposedSummary:

    def test_format(self):
        m = DecomposedMetrics(
            game_correct_rate=0.6,
            game_correct_count=3,
            game_incorrect_count=2,
            trade_profit_rate=0.8,
            trade_profitable_count=4,
            trade_unprofitable_count=1,
            merge_rate=0.4,
            merge_settled_count=2,
            total_settled=5,
        )
        s = format_decomposed_summary(m)
        assert "Game W/L: 3/2" in s
        assert "Profit W/L: 4/1" in s
        assert "Merged: 2" in s
