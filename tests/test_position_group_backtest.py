"""Tests for first-principles position-group strategy comparison."""

from __future__ import annotations

import pytest

from src.analysis.position_group_backtest import (
    PositionGroupGameInput,
    compare_position_group_strategies,
)


def test_composite_superior_when_merge_edge_and_directional_alpha_exist():
    games = [
        PositionGroupGameInput(
            "g1", directional_price=0.45, opposite_price=0.50, directional_won=True
        ),
        PositionGroupGameInput(
            "g2", directional_price=0.44, opposite_price=0.50, directional_won=True
        ),
        PositionGroupGameInput(
            "g3", directional_price=0.46, opposite_price=0.49, directional_won=False
        ),
        PositionGroupGameInput(
            "g4", directional_price=0.43, opposite_price=0.51, directional_won=True
        ),
    ]
    out = compare_position_group_strategies(
        games,
        merge_shares=100.0,
        directional_shares=30.0,
        fee_per_share=0.0,
        gas_per_game=0.0,
    )
    assert out.merge_only.total_pnl > 0
    assert out.directional_only.total_pnl > 0
    assert out.composite.total_pnl > out.merge_only.total_pnl
    assert out.composite.total_pnl > out.directional_only.total_pnl
    assert out.composite_superior is True


def test_composite_not_superior_when_directional_alpha_is_negative():
    games = [
        PositionGroupGameInput(
            "g1", directional_price=0.48, opposite_price=0.50, directional_won=False
        ),
        PositionGroupGameInput(
            "g2", directional_price=0.47, opposite_price=0.51, directional_won=False
        ),
    ]
    out = compare_position_group_strategies(
        games,
        merge_shares=100.0,
        directional_shares=50.0,
        fee_per_share=0.0,
        gas_per_game=0.0,
    )
    assert out.merge_only.total_pnl > out.composite.total_pnl
    assert out.composite_superior is False


def test_negative_share_inputs_raise():
    with pytest.raises(ValueError):
        compare_position_group_strategies([], merge_shares=-1.0, directional_shares=10.0)
