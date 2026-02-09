"""Tests for 3-layer position sizing."""

from __future__ import annotations

from src.sizing.liquidity import LiquiditySnapshot
from src.sizing.position_sizer import SizingResult, calculate_position_size


def _make_snap(
    ask_depth_5c: float = 3000.0,
    spread_pct: float = 2.0,
    best_ask: float = 0.45,
    best_bid: float = 0.44,
) -> LiquiditySnapshot:
    return LiquiditySnapshot(
        token_id="t",
        timestamp="",
        best_ask=best_ask,
        best_bid=best_bid,
        ask_depth_5c=ask_depth_5c,
        ask_depth_10c=ask_depth_5c * 2,
        bid_depth_5c=ask_depth_5c,
        spread=best_ask - best_bid,
        spread_pct=spread_pct,
        midpoint=(best_ask + best_bid) / 2,
        impact_estimate=0.0,
        ask_levels=5,
        bid_levels=5,
    )


class TestCalculatePositionSize:
    def test_kelly_binding(self):
        """When Kelly is smallest, it binds."""
        result = calculate_position_size(
            kelly_usd=78.0,
            balance_usd=5000.0,
            liquidity=_make_snap(ask_depth_5c=3000),
            max_position_usd=100.0,
        )
        assert result.final_size_usd == 78.0
        assert result.constraint_binding == "kelly"

    def test_capital_binding(self):
        """When capital is smallest (balance low), it binds."""
        result = calculate_position_size(
            kelly_usd=78.0,
            balance_usd=500.0,
            liquidity=_make_snap(ask_depth_5c=3000),
            max_position_usd=100.0,
            capital_risk_pct=2.0,
        )
        # capital_cap = 500 * 2% = $10
        assert result.final_size_usd == 10.0
        assert result.constraint_binding == "capital"

    def test_liquidity_binding(self):
        """When liquidity is smallest (thin market), it binds."""
        result = calculate_position_size(
            kelly_usd=78.0,
            balance_usd=5000.0,
            liquidity=_make_snap(ask_depth_5c=150),
            max_position_usd=100.0,
            liquidity_fill_pct=10.0,
        )
        # liquidity_cap = 150 * 10% = $15
        assert result.final_size_usd == 15.0
        assert result.constraint_binding == "liquidity"

    def test_max_position_binding(self):
        """When max_position_usd is smallest, it binds."""
        result = calculate_position_size(
            kelly_usd=120.0,
            balance_usd=10000.0,
            liquidity=_make_snap(ask_depth_5c=5000),
            max_position_usd=100.0,
        )
        assert result.final_size_usd == 100.0
        assert result.constraint_binding == "max_position"

    def test_spread_too_wide_skips(self):
        """Spread > max_spread_pct → skip."""
        result = calculate_position_size(
            kelly_usd=78.0,
            balance_usd=5000.0,
            liquidity=_make_snap(spread_pct=15.0),
            max_position_usd=100.0,
            max_spread_pct=10.0,
        )
        assert result.final_size_usd == 0.0
        assert result.recommended_execution == "skip"
        assert result.liquidity_score == "insufficient"

    def test_no_constraints(self):
        """balance=None, liquidity=None → pure Kelly + max_position."""
        result = calculate_position_size(
            kelly_usd=78.0,
            balance_usd=None,
            liquidity=None,
            max_position_usd=100.0,
        )
        assert result.final_size_usd == 78.0
        assert result.constraint_binding == "kelly"
        assert result.liquidity_score == "unknown"

    def test_no_balance_constraint(self):
        """balance=None → capital cap is infinite, does not bind."""
        result = calculate_position_size(
            kelly_usd=50.0,
            balance_usd=None,
            liquidity=_make_snap(ask_depth_5c=3000),
            max_position_usd=100.0,
        )
        assert result.final_size_usd == 50.0
        assert result.constraint_binding == "kelly"

    def test_no_liquidity_constraint(self):
        """liquidity=None → liquidity cap is infinite."""
        result = calculate_position_size(
            kelly_usd=50.0,
            balance_usd=500.0,
            liquidity=None,
            max_position_usd=100.0,
            capital_risk_pct=2.0,
        )
        # capital_cap = $10
        assert result.final_size_usd == 10.0
        assert result.constraint_binding == "capital"

    def test_negative_kelly_clamped(self):
        """Negative Kelly → final is 0."""
        result = calculate_position_size(
            kelly_usd=-5.0,
            balance_usd=5000.0,
            liquidity=_make_snap(),
            max_position_usd=100.0,
        )
        assert result.final_size_usd == 0.0

    def test_spread_warning_halves_cap(self):
        """Spread between 75% and 100% of max → liquidity cap halved."""
        # max_spread_pct=10 → warn threshold=7.5
        result = calculate_position_size(
            kelly_usd=200.0,
            balance_usd=50000.0,
            liquidity=_make_snap(ask_depth_5c=1000, spread_pct=8.0),
            max_position_usd=200.0,
            max_spread_pct=10.0,
            liquidity_fill_pct=10.0,
        )
        # normal liquidity_cap = 1000 * 10% = 100
        # halved due to spread warning = 50
        assert result.final_size_usd == 50.0
        assert result.constraint_binding == "liquidity"

    def test_result_fields(self):
        """Verify all SizingResult fields are populated."""
        result = calculate_position_size(
            kelly_usd=78.0,
            balance_usd=5000.0,
            liquidity=_make_snap(),
            max_position_usd=100.0,
        )
        assert isinstance(result, SizingResult)
        assert result.raw_kelly_usd == 78.0
        assert result.recommended_execution in ("immediate", "wait", "skip")
        assert result.liquidity_score in ("high", "medium", "low", "insufficient", "unknown")
