"""Tests for 3-layer position sizing."""

from __future__ import annotations

from src.sizing.liquidity import LiquiditySnapshot
from src.sizing.position_sizer import (
    SizingResult,
    TargetOrderResult,
    calculate_position_size,
    calculate_target_order_size,
)


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


class TestCalculateTargetOrderSize:
    """Tests for target-holding DCA order sizing (Phase DCA2)."""

    def test_price_drop_buys_more(self):
        """Price drops → gap increases → larger order."""
        result = calculate_target_order_size(
            total_budget=50.0,
            costs=[10.0],
            prices=[0.40],
            current_price=0.35,
            max_entries=5,
            entries_done=1,
            cap_mult=2.0,
        )
        # 25 shares @ 0.35 = $8.75 value. gap = 50 - 8.75 = $41.25
        # remaining = $40.0. cap = (40/4)*2 = $20
        assert isinstance(result, TargetOrderResult)
        assert result.order_size_usd == 20.0  # capped by per_entry_cap
        assert result.completion_reason is None

    def test_price_rise_buys_less(self):
        """Price rises → gap shrinks → smaller order."""
        result = calculate_target_order_size(
            total_budget=50.0,
            costs=[10.0],
            prices=[0.40],
            current_price=0.50,
            max_entries=5,
            entries_done=1,
            cap_mult=2.0,
        )
        # 25 shares @ 0.50 = $12.50 value. gap = 50 - 12.50 = $37.50
        # remaining = $40. cap = $20. → order = min(37.50, 40, 20) = $20
        assert result.order_size_usd == 20.0

    def test_budget_exhausted(self):
        """Remaining budget < min_order → budget_exhausted."""
        result = calculate_target_order_size(
            total_budget=50.0,
            costs=[25.0, 24.5],
            prices=[0.40, 0.38],
            current_price=0.40,
            max_entries=5,
            entries_done=2,
            min_order_usd=1.0,
        )
        # total_cost = 49.5, remaining = 0.5 < 1.0
        assert result.order_size_usd == 0.0
        assert result.completion_reason == "budget_exhausted"

    def test_target_reached(self):
        """Current value ≈ total budget → target_reached."""
        result = calculate_target_order_size(
            total_budget=50.0,
            costs=[20.0],
            prices=[0.40],
            current_price=1.00,
            max_entries=5,
            entries_done=1,
            min_order_usd=1.0,
        )
        # 50 shares @ 1.00 = $50 value. gap = 50 - 50 = 0
        assert result.order_size_usd == 0.0
        assert result.completion_reason == "target_reached"

    def test_remaining_budget_caps_order(self):
        """Order cannot exceed remaining budget."""
        result = calculate_target_order_size(
            total_budget=50.0,
            costs=[10.0, 20.0, 15.0],
            prices=[0.40, 0.35, 0.33],
            current_price=0.30,
            max_entries=5,
            entries_done=3,
            cap_mult=5.0,  # high cap_mult so per_entry_cap doesn't bind
        )
        # total_cost = $45. remaining = $5
        # shares = 25 + 57.14 + 45.45 = 127.6. value = 127.6 * 0.30 = $38.27
        # gap = 50 - 38.27 = $11.73. cap = (5/2)*5 = $12.50
        # order = min(11.73, 5, 12.5) = $5
        assert result.order_size_usd == 5.0
        assert result.completion_reason is None

    def test_per_entry_cap_prevents_all_in(self):
        """cap_mult limits single entry from consuming entire remaining budget."""
        result = calculate_target_order_size(
            total_budget=100.0,
            costs=[10.0],
            prices=[0.50],
            current_price=0.20,
            max_entries=5,
            entries_done=1,
            cap_mult=1.5,
        )
        # remaining = $90. remaining_entries = 4
        # cap = (90/4)*1.5 = $33.75
        # shares = 20. value = 20*0.20 = $4. gap = $96
        # order = min(96, 90, 33.75) = $33.75
        assert result.order_size_usd == 33.75

    def test_empty_entries(self):
        """No existing entries → gap = total_budget, capped by per_entry_cap."""
        result = calculate_target_order_size(
            total_budget=50.0,
            costs=[],
            prices=[],
            current_price=0.40,
            max_entries=5,
            entries_done=0,
            cap_mult=2.0,
        )
        # remaining = $50. remaining_entries = 5. cap = (50/5)*2 = $20
        # gap = 50 (no shares). order = min(50, 50, 20) = $20
        assert result.order_size_usd == 20.0

    def test_min_order_usd_threshold(self):
        """Order below min_order_usd → 0 with budget_exhausted."""
        result = calculate_target_order_size(
            total_budget=50.0,
            costs=[49.2],
            prices=[0.40],
            current_price=0.40,
            max_entries=5,
            entries_done=1,
            min_order_usd=1.0,
        )
        # remaining = $0.80 < $1.0
        assert result.order_size_usd == 0.0
        assert result.completion_reason == "budget_exhausted"

    def test_numerical_example_from_plan(self):
        """Verify the exact numerical example from the plan doc."""
        # Entry 1: $10 @ 0.40 → 25 shares
        # Entry 2 @ 0.35: value=25×0.35=$8.75. gap=50-8.75=$41.25
        #   remaining=$40. cap=(40/4)*2=$20. order=min(41.25,40,20)=$20
        result = calculate_target_order_size(
            total_budget=50.0,
            costs=[10.0],
            prices=[0.40],
            current_price=0.35,
            max_entries=5,
            entries_done=1,
            cap_mult=2.0,
        )
        assert result.order_size_usd == 20.0
        assert result.raw_gap == 41.25
        assert result.remaining_budget == 40.0
        assert result.per_entry_cap == 20.0
