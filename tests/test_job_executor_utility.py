"""Tests for utility-based first-leg ordering (Track B4)."""

from __future__ import annotations

from types import SimpleNamespace

from src.scheduler.job_executor import _apply_utility_leg_order
from src.strategy.calibration_scanner import BothsideOpportunity, CalibrationOpportunity


def _opp(
    *,
    name: str,
    token_id: str,
    price: float,
    ev: float,
    position_usd: float = 25.0,
) -> CalibrationOpportunity:
    return CalibrationOpportunity(
        event_slug="nba-nyk-bos-2026-02-10",
        event_title="Knicks vs Celtics",
        market_type="moneyline",
        outcome_name=name,
        token_id=token_id,
        poly_price=price,
        calibration_edge_pct=5.0,
        expected_win_rate=0.60,
        ev_per_dollar=ev,
        price_band="0.40-0.45",
        in_sweet_spot=True,
        band_confidence="high",
        position_usd=position_usd,
    )


def test_apply_utility_leg_order_swaps_to_better_fill_side(monkeypatch):
    monkeypatch.setattr("src.scheduler.job_executor.settings.game_position_group_enabled", True)
    monkeypatch.setattr("src.scheduler.job_executor.settings.position_group_utility_enabled", True)
    monkeypatch.setattr("src.scheduler.job_executor.settings.max_spread_pct", 10.0)
    monkeypatch.setattr(
        "src.scheduler.job_executor.settings.position_group_utility_alpha_weight",
        1.0,
    )
    monkeypatch.setattr(
        "src.scheduler.job_executor.settings.position_group_utility_merge_weight",
        1.0,
    )
    monkeypatch.setattr(
        "src.scheduler.job_executor.settings.position_group_utility_slippage_weight",
        1.0,
    )

    directional = _opp(name="Celtics", token_id="tok-dir", price=0.47, ev=0.20)
    opposite = _opp(name="Knicks", token_id="tok-opp", price=0.42, ev=0.12)
    bothside = BothsideOpportunity(
        directional=directional,
        hedge=opposite,
        combined_price=0.89,
        hedge_position_usd=8.0,
    )
    liquidity_map = {
        "tok-dir": SimpleNamespace(spread_pct=8.0),
        "tok-opp": SimpleNamespace(spread_pct=1.0),
    }

    selected, reordered = _apply_utility_leg_order(
        bothside_opp=bothside,
        liquidity_map=liquidity_map,
        effective_hedge_mult=0.5,
    )

    assert selected.outcome_name == "Knicks"
    assert reordered.directional.outcome_name == "Knicks"
    assert reordered.hedge.outcome_name == "Celtics"


def test_apply_utility_leg_order_noop_when_disabled(monkeypatch):
    monkeypatch.setattr("src.scheduler.job_executor.settings.game_position_group_enabled", True)
    monkeypatch.setattr("src.scheduler.job_executor.settings.position_group_utility_enabled", False)

    directional = _opp(name="Celtics", token_id="tok-dir", price=0.47, ev=0.20)
    opposite = _opp(name="Knicks", token_id="tok-opp", price=0.42, ev=0.12)
    bothside = BothsideOpportunity(
        directional=directional,
        hedge=opposite,
        combined_price=0.89,
        hedge_position_usd=8.0,
    )

    selected, reordered = _apply_utility_leg_order(
        bothside_opp=bothside,
        liquidity_map={},
        effective_hedge_mult=0.5,
    )
    assert selected.outcome_name == "Celtics"
    assert reordered.directional.outcome_name == "Celtics"
