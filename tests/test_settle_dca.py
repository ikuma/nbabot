"""Tests for DCA group settlement in scripts/settle.py."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.settle import _calc_dca_group_pnl, _calc_pnl
from src.store.db import SignalRecord


def _make_signal(
    id: int = 1,
    poly_price: float = 0.40,
    kelly_size: float = 25.0,
    fill_price: float | None = None,
    dca_group_id: str | None = None,
    dca_sequence: int = 1,
    **kwargs,
) -> SignalRecord:
    defaults = dict(
        game_title="Knicks vs Celtics",
        event_slug="nba-nyk-bos-2026-02-10",
        team="Celtics",
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
        poly_price=poly_price,
        kelly_size=kelly_size,
        fill_price=fill_price,
        dca_group_id=dca_group_id,
        dca_sequence=dca_sequence,
        **defaults,
    )


class TestCalcDcaGroupPnl:
    def test_win_single_entry(self):
        """Single entry DCA group should match _calc_pnl."""
        signals = [_make_signal(poly_price=0.40, kelly_size=50.0)]
        pnl = _calc_dca_group_pnl(True, signals)
        expected = _calc_pnl(True, 50.0, 0.40)
        assert pnl == pytest.approx(expected)

    def test_loss_single_entry(self):
        signals = [_make_signal(poly_price=0.40, kelly_size=50.0)]
        pnl = _calc_dca_group_pnl(False, signals)
        assert pnl == pytest.approx(-50.0)

    def test_win_multiple_entries(self):
        """DCA group with multiple entries: VWAP-based PnL."""
        signals = [
            _make_signal(id=1, poly_price=0.40, kelly_size=25.0, dca_sequence=1),
            _make_signal(id=2, poly_price=0.38, kelly_size=25.0, dca_sequence=2),
            _make_signal(id=3, poly_price=0.35, kelly_size=25.0, dca_sequence=3),
        ]
        pnl = _calc_dca_group_pnl(True, signals)
        # total_cost = 75
        # total_shares = 25/0.40 + 25/0.38 + 25/0.35 = 62.5 + 65.789 + 71.429 = 199.718
        # pnl = 199.718 * 1.0 - 75 = 124.718
        total_shares = 25 / 0.40 + 25 / 0.38 + 25 / 0.35
        assert pnl == pytest.approx(total_shares - 75.0)

    def test_loss_multiple_entries(self):
        signals = [
            _make_signal(id=1, poly_price=0.40, kelly_size=25.0),
            _make_signal(id=2, poly_price=0.38, kelly_size=25.0),
        ]
        pnl = _calc_dca_group_pnl(False, signals)
        assert pnl == pytest.approx(-50.0)

    def test_uses_fill_price_when_available(self):
        """Fill price should be used over poly_price."""
        signals = [
            _make_signal(id=1, poly_price=0.40, kelly_size=25.0, fill_price=0.39),
            _make_signal(id=2, poly_price=0.38, kelly_size=25.0, fill_price=0.37),
        ]
        pnl = _calc_dca_group_pnl(True, signals)
        total_shares = 25 / 0.39 + 25 / 0.37
        assert pnl == pytest.approx(total_shares - 50.0)

    def test_dca_improves_pnl(self):
        """DCA at lower prices should improve PnL vs single entry."""
        # Single entry at 0.40
        single_pnl = _calc_pnl(True, 75.0, 0.40)

        # DCA: 3 entries at progressively lower prices
        dca_signals = [
            _make_signal(id=1, poly_price=0.40, kelly_size=25.0),
            _make_signal(id=2, poly_price=0.38, kelly_size=25.0),
            _make_signal(id=3, poly_price=0.35, kelly_size=25.0),
        ]
        dca_pnl = _calc_dca_group_pnl(True, dca_signals)

        # DCA should yield better PnL when averaging down
        assert dca_pnl > single_pnl
