"""Tests for merge_strategy.py pure functions."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.store.db import SignalRecord
from src.strategy.merge_strategy import (
    calculate_combined_vwap,
    calculate_mergeable_shares,
    should_merge,
)


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


class TestCalculateMergeableShares:
    def test_equal_shares(self):
        """Equal investment at equal price → equal shares, no remainder."""
        dir_sigs = [_make_signal(id=1, poly_price=0.40, kelly_size=20.0)]
        hedge_sigs = [_make_signal(id=2, poly_price=0.40, kelly_size=20.0, team="Knicks")]

        d, h, merge, rem, side = calculate_mergeable_shares(dir_sigs, hedge_sigs)
        assert d == pytest.approx(50.0)
        assert h == pytest.approx(50.0)
        assert merge == pytest.approx(50.0)
        assert rem == pytest.approx(0.0)
        assert side is None

    def test_dir_more_shares(self):
        """Dir has more shares → remainder is directional."""
        dir_sigs = [_make_signal(id=1, poly_price=0.30, kelly_size=30.0)]  # 100 shares
        hedge_sigs = [_make_signal(id=2, poly_price=0.50, kelly_size=25.0, team="Knicks")]  # 50

        d, h, merge, rem, side = calculate_mergeable_shares(dir_sigs, hedge_sigs)
        assert d == pytest.approx(100.0)
        assert h == pytest.approx(50.0)
        assert merge == pytest.approx(50.0)
        assert rem == pytest.approx(50.0)
        assert side == "directional"

    def test_hedge_more_shares(self):
        """Hedge has more shares → remainder is hedge."""
        dir_sigs = [_make_signal(id=1, poly_price=0.50, kelly_size=25.0)]  # 50 shares
        hedge_sigs = [_make_signal(id=2, poly_price=0.30, kelly_size=30.0, team="Knicks")]  # 100

        d, h, merge, rem, side = calculate_mergeable_shares(dir_sigs, hedge_sigs)
        assert d == pytest.approx(50.0)
        assert h == pytest.approx(100.0)
        assert merge == pytest.approx(50.0)
        assert rem == pytest.approx(50.0)
        assert side == "hedge"

    def test_dca_groups(self):
        """Multiple DCA entries sum correctly."""
        dir_sigs = [
            _make_signal(id=1, poly_price=0.35, kelly_size=25.0),
            _make_signal(id=2, poly_price=0.33, kelly_size=25.0),
        ]
        hedge_sigs = [
            _make_signal(id=3, poly_price=0.50, kelly_size=12.0, team="Knicks"),
            _make_signal(id=4, poly_price=0.48, kelly_size=12.0, team="Knicks"),
        ]

        d, h, merge, rem, side = calculate_mergeable_shares(dir_sigs, hedge_sigs)
        expected_d = 25.0 / 0.35 + 25.0 / 0.33
        expected_h = 12.0 / 0.50 + 12.0 / 0.48
        assert d == pytest.approx(expected_d)
        assert h == pytest.approx(expected_h)
        assert merge == pytest.approx(min(expected_d, expected_h))

    def test_empty_signals(self):
        """Empty signal lists → 0 shares."""
        d, h, merge, rem, side = calculate_mergeable_shares([], [])
        assert d == 0.0
        assert h == 0.0
        assert merge == 0.0

    def test_fill_price_preferred(self):
        """fill_price should be used when available."""
        dir_sigs = [_make_signal(id=1, poly_price=0.40, kelly_size=20.0, fill_price=0.38)]

        d, _, _, _, _ = calculate_mergeable_shares(dir_sigs, [])
        assert d == pytest.approx(20.0 / 0.38)


class TestCalculateCombinedVwap:
    def test_simple_vwap(self):
        """Simple case: single entry each side."""
        dir_sigs = [_make_signal(id=1, poly_price=0.35, kelly_size=25.0)]
        hedge_sigs = [_make_signal(id=2, poly_price=0.50, kelly_size=12.0, team="Knicks")]

        dir_vwap, hedge_vwap, combined = calculate_combined_vwap(dir_sigs, hedge_sigs)
        assert dir_vwap == pytest.approx(0.35)
        assert hedge_vwap == pytest.approx(0.50)
        assert combined == pytest.approx(0.85)

    def test_dca_vwap(self):
        """DCA entries should produce weighted average."""
        dir_sigs = [
            _make_signal(id=1, poly_price=0.35, kelly_size=25.0),
            _make_signal(id=2, poly_price=0.33, kelly_size=25.0),
        ]
        hedge_sigs = [_make_signal(id=3, poly_price=0.50, kelly_size=12.0, team="Knicks")]

        dir_vwap, hedge_vwap, combined = calculate_combined_vwap(dir_sigs, hedge_sigs)

        # Dir VWAP = total_cost / total_shares = 50 / (25/0.35 + 25/0.33)
        expected_dir = 50.0 / (25.0 / 0.35 + 25.0 / 0.33)
        assert dir_vwap == pytest.approx(expected_dir)
        assert combined == pytest.approx(expected_dir + 0.50)


class TestShouldMerge:
    def _settings(self, **overrides):
        s = MagicMock()
        s.merge_enabled = True
        s.merge_max_combined_vwap = 0.998
        s.merge_min_profit_usd = 0.10
        return s

    def test_ok(self):
        s = self._settings()
        ok, reason = should_merge(0.85, 100.0, s, gas_cost_usd=0.01)
        assert ok is True
        assert reason == "ok"

    def test_disabled(self):
        s = self._settings()
        s.merge_enabled = False
        ok, reason = should_merge(0.85, 100.0, s)
        assert ok is False
        assert reason == "merge_disabled"

    def test_not_eoa(self):
        s = self._settings()
        ok, reason = should_merge(0.85, 100.0, s, is_eoa=False)
        assert ok is False
        assert reason == "unsupported_wallet"

    def test_poly_proxy_supported(self):
        """is_supported_wallet=True → POLY_PROXY でも MERGE OK。"""
        s = self._settings()
        ok, reason = should_merge(
            0.85, 100.0, s, gas_cost_usd=0.01,
            is_eoa=False, is_supported_wallet=True,
        )
        assert ok is True
        assert reason == "ok"

    def test_backward_compat(self):
        """is_supported_wallet 未指定 → is_eoa にフォールバック。"""
        s = self._settings()
        # is_eoa=True, is_supported_wallet=None → wallet_ok=True
        ok, _ = should_merge(0.85, 100.0, s, gas_cost_usd=0.01, is_eoa=True)
        assert ok is True

        # is_eoa=False, is_supported_wallet=None → wallet_ok=False
        ok, reason = should_merge(0.85, 100.0, s, is_eoa=False)
        assert ok is False
        assert reason == "unsupported_wallet"

    def test_high_combined_vwap(self):
        s = self._settings()
        ok, reason = should_merge(0.999, 100.0, s)
        assert ok is False
        assert "combined_vwap" in reason

    def test_no_shares(self):
        s = self._settings()
        ok, reason = should_merge(0.85, 0.0, s)
        assert ok is False
        assert "no_mergeable_shares" in reason

    def test_profit_below_min(self):
        s = self._settings()
        # combined=0.998 → gross = 100 * 0.002 = 0.20, gas=0.15 → net=0.05 < 0.10
        s.merge_max_combined_vwap = 0.999
        ok, reason = should_merge(0.998, 100.0, s, gas_cost_usd=0.15)
        assert ok is False
        assert "net_profit" in reason

    def test_early_partial_benefit_guard_blocks(self):
        s = self._settings()
        ok, reason = should_merge(
            0.90,
            100.0,
            s,
            gas_cost_usd=0.01,
            capital_release_benefit_usd=0.04,
            additional_fee_usd=0.05,
        )
        assert ok is False
        assert "capital_release_benefit" in reason

    def test_early_partial_benefit_guard_allows(self):
        s = self._settings()
        ok, reason = should_merge(
            0.90,
            100.0,
            s,
            gas_cost_usd=0.01,
            capital_release_benefit_usd=0.10,
            additional_fee_usd=0.05,
        )
        assert ok is True
        assert reason == "ok"
