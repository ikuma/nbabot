"""Tests for position group sizing (Track B3)."""

from __future__ import annotations

import pytest

from src.strategy.position_group_sizing import (
    PositionGroupSizingInputs,
    compute_position_group_targets,
)


def test_compute_targets_positive_directional_and_merge():
    inputs = PositionGroupSizingInputs(
        directional_price=0.40,
        opposite_price=0.55,
        directional_expected_win_rate=0.62,
        directional_band_confidence="high",
        directional_vwap=0.40,
        opposite_vwap=0.55,
    )
    t = compute_position_group_targets(inputs=inputs, balance_usd=5000.0, u_regime=1.0)
    assert t.d_target > 0
    assert t.m_target > 0
    assert t.q_dir_target == pytest.approx(t.m_target + t.d_target)
    assert t.q_opp_target == pytest.approx(t.m_target)
    assert t.merge_edge > 0


def test_merge_target_zero_when_edge_not_positive():
    inputs = PositionGroupSizingInputs(
        directional_price=0.51,
        opposite_price=0.49,
        directional_expected_win_rate=0.60,
        directional_band_confidence="high",
        directional_vwap=0.51,
        opposite_vwap=0.49,
    )
    t = compute_position_group_targets(inputs=inputs, balance_usd=5000.0, u_regime=1.0)
    assert t.merge_edge < 0
    assert t.m_target == 0.0
    assert t.q_opp_target == 0.0


def test_low_confidence_reduces_directional_target(monkeypatch):
    monkeypatch.setattr("src.strategy.position_group_sizing.settings.max_position_usd", 1000.0)
    common = dict(
        directional_price=0.40,
        opposite_price=0.55,
        directional_expected_win_rate=0.62,
        directional_vwap=0.40,
        opposite_vwap=0.55,
    )
    high = compute_position_group_targets(
        inputs=PositionGroupSizingInputs(**common, directional_band_confidence="high"),
        balance_usd=500.0,
        u_regime=1.0,
    )
    low = compute_position_group_targets(
        inputs=PositionGroupSizingInputs(**common, directional_band_confidence="low"),
        balance_usd=500.0,
        u_regime=1.0,
    )
    assert low.d_target < high.d_target
    assert low.u_conf < high.u_conf


def test_regime_multiplier_zero_blocks_new_targets():
    inputs = PositionGroupSizingInputs(
        directional_price=0.40,
        opposite_price=0.55,
        directional_expected_win_rate=0.62,
        directional_band_confidence="high",
    )
    t = compute_position_group_targets(inputs=inputs, balance_usd=5000.0, u_regime=0.0)
    assert t.d_target == 0.0
    assert t.m_target == 0.0
