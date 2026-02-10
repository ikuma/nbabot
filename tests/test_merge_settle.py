"""Tests for MERGE settlement (Phase B2)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.settle import _calc_merge_pnl
from src.store.db import MergeOperation, SignalRecord


def _make_signal(
    id: int = 1,
    team: str = "Celtics",
    poly_price: float = 0.40,
    kelly_size: float = 25.0,
    fill_price: float | None = None,
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
        signal_role=signal_role,
        **defaults,
    )


def _make_merge_op(
    merge_amount: float = 50.0,
    combined_vwap: float = 0.85,
    remainder_shares: float = 20.0,
    remainder_side: str = "directional",
    gas_cost_usd: float = 0.01,
    dir_shares: float = 70.0,
    hedge_shares: float = 50.0,
) -> MergeOperation:
    return MergeOperation(
        id=1,
        bothside_group_id="bs-123",
        condition_id="0xcond1",
        event_slug="nba-nyk-bos-2026-02-10",
        dir_shares=dir_shares,
        hedge_shares=hedge_shares,
        merge_amount=merge_amount,
        remainder_shares=remainder_shares,
        remainder_side=remainder_side,
        dir_vwap=0.35,
        hedge_vwap=0.50,
        combined_vwap=combined_vwap,
        gross_profit_usd=merge_amount * (1.0 - combined_vwap),
        gas_cost_usd=gas_cost_usd,
        net_profit_usd=merge_amount * (1.0 - combined_vwap) - gas_cost_usd,
        status="executed",
        tx_hash="0xdeadbeef",
        error_message=None,
        created_at="2026-02-10T00:00:00+00:00",
        executed_at="2026-02-10T00:01:00+00:00",
    )


class TestCalcMergePnl:
    def test_no_remainder(self):
        """When no remainder, PnL is purely from MERGE."""
        merge_op = _make_merge_op(
            merge_amount=50.0,
            combined_vwap=0.85,
            remainder_shares=0.0,
            remainder_side=None,
        )
        dir_sigs = [_make_signal(id=1, poly_price=0.35, kelly_size=17.5)]  # 50 shares
        hedge_sigs = [_make_signal(id=2, poly_price=0.50, kelly_size=25.0, team="Knicks")]  # 50

        merge_pnl, rem_pnl, total = _calc_merge_pnl(merge_op, "Celtics", dir_sigs, hedge_sigs)

        expected_merge = 50.0 * (1.0 - 0.85) - 0.01
        assert merge_pnl == pytest.approx(expected_merge)
        assert rem_pnl == pytest.approx(0.0)
        assert total == pytest.approx(expected_merge)

    def test_dir_remainder_wins(self):
        """Directional remainder, winner is directional team."""
        merge_op = _make_merge_op(
            merge_amount=50.0,
            combined_vwap=0.85,
            remainder_shares=20.0,
            remainder_side="directional",
            dir_shares=70.0,
            hedge_shares=50.0,
        )
        # Dir: 25/0.35 = ~71.43 shares, cost=25
        dir_sigs = [_make_signal(id=1, poly_price=0.35, kelly_size=25.0)]
        hedge_sigs = [_make_signal(id=2, poly_price=0.50, kelly_size=25.0, team="Knicks")]

        merge_pnl, rem_pnl, total = _calc_merge_pnl(merge_op, "Celtics", dir_sigs, hedge_sigs)

        expected_merge = 50.0 * 0.15 - 0.01
        assert merge_pnl == pytest.approx(expected_merge)

        # Remainder: 20 shares win ($1 each), cost = 25 * (20 / 71.43)
        total_dir_shares = 25.0 / 0.35
        rem_cost = 25.0 * (20.0 / total_dir_shares)
        expected_rem = 20.0 * 1.0 - rem_cost
        assert rem_pnl == pytest.approx(expected_rem)
        assert total == pytest.approx(expected_merge + expected_rem)

    def test_dir_remainder_loses(self):
        """Directional remainder, winner is hedge team (remainder loses)."""
        merge_op = _make_merge_op(
            merge_amount=50.0,
            combined_vwap=0.85,
            remainder_shares=20.0,
            remainder_side="directional",
            dir_shares=70.0,
            hedge_shares=50.0,
        )
        dir_sigs = [_make_signal(id=1, poly_price=0.35, kelly_size=25.0)]
        hedge_sigs = [_make_signal(id=2, poly_price=0.50, kelly_size=25.0, team="Knicks")]

        merge_pnl, rem_pnl, total = _calc_merge_pnl(merge_op, "Knicks", dir_sigs, hedge_sigs)

        expected_merge = 50.0 * 0.15 - 0.01
        assert merge_pnl == pytest.approx(expected_merge)

        # Remainder loses: cost = 25 * (20 / (25/0.35))
        total_dir_shares = 25.0 / 0.35
        rem_cost = 25.0 * (20.0 / total_dir_shares)
        expected_rem = -rem_cost
        assert rem_pnl == pytest.approx(expected_rem)

    def test_hedge_remainder(self):
        """Hedge has remainder."""
        merge_op = _make_merge_op(
            merge_amount=50.0,
            combined_vwap=0.85,
            remainder_shares=30.0,
            remainder_side="hedge",
            dir_shares=50.0,
            hedge_shares=80.0,
        )
        dir_sigs = [_make_signal(id=1, poly_price=0.50, kelly_size=25.0)]
        hedge_sigs = [_make_signal(id=2, poly_price=0.35, kelly_size=28.0, team="Knicks")]

        merge_pnl, rem_pnl, total = _calc_merge_pnl(merge_op, "Knicks", dir_sigs, hedge_sigs)

        expected_merge = 50.0 * 0.15 - 0.01
        assert merge_pnl == pytest.approx(expected_merge)

        # Hedge remainder wins: 30 shares * $1 - cost
        total_hedge_shares = 28.0 / 0.35
        rem_cost = 28.0 * (30.0 / total_hedge_shares)
        expected_rem = 30.0 - rem_cost
        assert rem_pnl == pytest.approx(expected_rem)

    def test_gas_deducted(self):
        """Gas cost should be deducted from merge profit."""
        merge_op = _make_merge_op(
            merge_amount=100.0,
            combined_vwap=0.85,
            remainder_shares=0.0,
            remainder_side=None,
            gas_cost_usd=0.50,
        )
        dir_sigs = [_make_signal(id=1, poly_price=0.35, kelly_size=35.0)]
        hedge_sigs = [_make_signal(id=2, poly_price=0.50, kelly_size=50.0, team="Knicks")]

        merge_pnl, _, _ = _calc_merge_pnl(merge_op, "Celtics", dir_sigs, hedge_sigs)

        expected = 100.0 * 0.15 - 0.50
        assert merge_pnl == pytest.approx(expected)

    def test_dca_remainder(self):
        """DCA entries with remainder should handle share accounting correctly."""
        merge_op = _make_merge_op(
            merge_amount=50.0,
            combined_vwap=0.85,
            remainder_shares=20.0,
            remainder_side="directional",
            dir_shares=70.0,
            hedge_shares=50.0,
        )
        dir_sigs = [
            _make_signal(id=1, poly_price=0.35, kelly_size=12.5),
            _make_signal(id=2, poly_price=0.33, kelly_size=12.5),
        ]
        hedge_sigs = [_make_signal(id=3, poly_price=0.50, kelly_size=25.0, team="Knicks")]

        merge_pnl, rem_pnl, total = _calc_merge_pnl(merge_op, "Celtics", dir_sigs, hedge_sigs)

        expected_merge = 50.0 * 0.15 - 0.01
        assert merge_pnl == pytest.approx(expected_merge)

        # Dir total shares = 12.5/0.35 + 12.5/0.33
        total_shares = 12.5 / 0.35 + 12.5 / 0.33
        total_cost = 25.0
        rem_cost = total_cost * (20.0 / total_shares)
        expected_rem = 20.0 - rem_cost  # wins
        assert rem_pnl == pytest.approx(expected_rem)
