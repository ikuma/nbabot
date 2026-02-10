"""Tests for both-side settlement (Phase B)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.settle import _calc_bothside_pnl
from src.store.db import SignalRecord


def _make_signal(
    id: int = 1,
    team: str = "Celtics",
    poly_price: float = 0.40,
    kelly_size: float = 25.0,
    fill_price: float | None = None,
    dca_group_id: str | None = None,
    dca_sequence: int = 1,
    bothside_group_id: str | None = None,
    signal_role: str = "directional",
    **kwargs,
) -> SignalRecord:
    defaults = dict(
        game_title="Knicks vs Celtics",
        event_slug="nba-nyk-bos-2026-02-10",
        side="BUY",
        book_prob=0.6,
        edge_pct=5.0,
        token_id="tok123",
        bookmakers_count=0,
        consensus_std=0.0,
        commence_time="",
        created_at="2026-02-10T00:00:00+00:00",
    )
    defaults.update(kwargs)
    return SignalRecord(
        id=id,
        team=team,
        poly_price=poly_price,
        kelly_size=kelly_size,
        fill_price=fill_price,
        dca_group_id=dca_group_id,
        dca_sequence=dca_sequence,
        bothside_group_id=bothside_group_id,
        signal_role=signal_role,
        **defaults,
    )


class TestCalcBothsidePnl:
    def test_combined_pnl_dir_wins(self):
        """Directional wins → dir positive, hedge negative."""
        dir_signals = [_make_signal(id=1, team="Celtics", poly_price=0.35, kelly_size=25.0)]
        hedge_signals = [_make_signal(id=2, team="Knicks", poly_price=0.50, kelly_size=12.0)]

        dir_pnl, hedge_pnl, combined = _calc_bothside_pnl("Celtics", dir_signals, hedge_signals)

        # Celtics wins: dir wins ($25 * (1/0.35 - 1)), hedge loses (-$12)
        expected_dir = 25.0 * (1.0 / 0.35 - 1.0)
        assert dir_pnl == pytest.approx(expected_dir)
        assert hedge_pnl == pytest.approx(-12.0)
        assert combined == pytest.approx(expected_dir - 12.0)

    def test_combined_pnl_hedge_wins(self):
        """Hedge team wins → dir negative, hedge positive."""
        dir_signals = [_make_signal(id=1, team="Celtics", poly_price=0.35, kelly_size=25.0)]
        hedge_signals = [_make_signal(id=2, team="Knicks", poly_price=0.50, kelly_size=12.0)]

        dir_pnl, hedge_pnl, combined = _calc_bothside_pnl("Knicks", dir_signals, hedge_signals)

        # Knicks wins: dir loses (-$25), hedge wins ($12 * (1/0.50 - 1) = $12)
        expected_hedge = 12.0 * (1.0 / 0.50 - 1.0)
        assert dir_pnl == pytest.approx(-25.0)
        assert hedge_pnl == pytest.approx(expected_hedge)
        assert combined == pytest.approx(-25.0 + expected_hedge)

    def test_combined_pnl_with_dca(self):
        """DCA groups on both sides should use VWAP-based PnL."""
        dir_signals = [
            _make_signal(id=1, team="Celtics", poly_price=0.35, kelly_size=25.0, dca_sequence=1),
            _make_signal(id=2, team="Celtics", poly_price=0.33, kelly_size=25.0, dca_sequence=2),
        ]
        hedge_signals = [
            _make_signal(id=3, team="Knicks", poly_price=0.50, kelly_size=12.0, dca_sequence=1),
            _make_signal(id=4, team="Knicks", poly_price=0.48, kelly_size=12.0, dca_sequence=2),
        ]

        dir_pnl, hedge_pnl, combined = _calc_bothside_pnl("Celtics", dir_signals, hedge_signals)

        # Dir wins: DCA group PnL
        dir_total_cost = 50.0
        dir_total_shares = 25.0 / 0.35 + 25.0 / 0.33
        expected_dir = dir_total_shares - dir_total_cost

        # Hedge loses: DCA group PnL
        hedge_total_cost = 24.0
        expected_hedge = -hedge_total_cost

        assert dir_pnl == pytest.approx(expected_dir)
        assert hedge_pnl == pytest.approx(expected_hedge)
        assert combined == pytest.approx(expected_dir + expected_hedge)

    def test_empty_hedge_signals(self):
        """When hedge has no signals, hedge PnL should be 0."""
        dir_signals = [_make_signal(id=1, team="Celtics", poly_price=0.35, kelly_size=25.0)]

        dir_pnl, hedge_pnl, combined = _calc_bothside_pnl("Celtics", dir_signals, [])

        expected_dir = 25.0 * (1.0 / 0.35 - 1.0)
        assert dir_pnl == pytest.approx(expected_dir)
        assert hedge_pnl == 0.0
        assert combined == pytest.approx(expected_dir)

    def test_both_lose_possible(self):
        """Edge case: both teams can't win at once, but one side always loses."""
        dir_signals = [_make_signal(id=1, team="Celtics", poly_price=0.35, kelly_size=25.0)]
        hedge_signals = [_make_signal(id=2, team="Knicks", poly_price=0.50, kelly_size=12.0)]

        # Celtics wins → hedge loses
        _, hedge_pnl, _ = _calc_bothside_pnl("Celtics", dir_signals, hedge_signals)
        assert hedge_pnl < 0

        # Knicks wins → directional loses
        dir_pnl, _, _ = _calc_bothside_pnl("Knicks", dir_signals, hedge_signals)
        assert dir_pnl < 0

    def test_low_combined_price_guarantees_profit(self):
        """When combined price < 1.0, one outcome always profits enough."""
        # Combined = 0.35 + 0.50 = 0.85 < 1.0
        dir_signals = [_make_signal(id=1, team="Celtics", poly_price=0.35, kelly_size=25.0)]
        hedge_signals = [_make_signal(id=2, team="Knicks", poly_price=0.50, kelly_size=25.0)]

        # Check Celtics wins
        dir_pnl, hedge_pnl, combined_cel = _calc_bothside_pnl("Celtics", dir_signals, hedge_signals)
        # Check Knicks wins
        dir_pnl2, hedge_pnl2, combined_nyk = _calc_bothside_pnl(
            "Knicks", dir_signals, hedge_signals
        )

        # At least one scenario should be positive
        # (with these prices, Celtics winning gives huge profit)
        assert combined_cel > 0 or combined_nyk > 0
