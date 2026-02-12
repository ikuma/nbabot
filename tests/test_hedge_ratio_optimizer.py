"""Tests for hedge ratio optimizer (Phase 2 external search)."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.analysis.hedge_ratio_optimizer import (
    HedgeRatioGroupSample,
    build_group_samples,
    evaluate_hedge_ratio,
    optimize_hedge_ratio,
)


@dataclass
class _Result:
    pnl: float
    settled_at: str


@dataclass
class _Signal:
    bothside_group_id: str | None
    signal_role: str
    kelly_size: float


def _pair(
    gid: str,
    role: str,
    cost: float,
    pnl: float,
    settled_at: str,
) -> tuple[_Result, _Signal]:
    return (
        _Result(pnl=pnl, settled_at=settled_at),
        _Signal(bothside_group_id=gid, signal_role=role, kelly_size=cost),
    )


class TestBuildGroupSamples:
    def test_keeps_only_complete_bothside_groups(self):
        pairs = [
            _pair("bs-1", "directional", 100.0, 12.0, "2026-02-01T00:00:00+00:00"),
            _pair("bs-1", "hedge", 50.0, 4.0, "2026-02-01T01:00:00+00:00"),
            _pair("bs-2", "directional", 80.0, -3.0, "2026-02-02T00:00:00+00:00"),
        ]

        samples = build_group_samples(pairs)  # bs-2 has no hedge -> excluded
        assert len(samples) == 1
        s = samples[0]
        assert s.bothside_group_id == "bs-1"
        assert s.directional_cost_usd == pytest.approx(100.0)
        assert s.hedge_cost_usd == pytest.approx(50.0)
        assert s.directional_pnl_usd == pytest.approx(12.0)
        assert s.hedge_pnl_usd == pytest.approx(4.0)
        assert s.base_hedge_ratio == pytest.approx(0.5)

    def test_date_filter(self):
        pairs = [
            _pair("bs-1", "directional", 100.0, 12.0, "2026-02-01T00:00:00+00:00"),
            _pair("bs-1", "hedge", 50.0, 4.0, "2026-02-01T01:00:00+00:00"),
            _pair("bs-2", "directional", 100.0, 5.0, "2026-02-10T00:00:00+00:00"),
            _pair("bs-2", "hedge", 50.0, 2.0, "2026-02-10T01:00:00+00:00"),
        ]

        samples = build_group_samples(pairs, start_date="2026-02-05", end_date="2026-02-12")
        assert len(samples) == 1
        assert samples[0].bothside_group_id == "bs-2"


class TestEvaluateAndOptimize:
    def _samples(self) -> list[HedgeRatioGroupSample]:
        return [
            HedgeRatioGroupSample(
                bothside_group_id="g1",
                settled_at="2026-02-01T00:00:00+00:00",
                directional_cost_usd=100.0,
                hedge_cost_usd=50.0,  # base_ratio=0.5
                directional_pnl_usd=10.0,
                hedge_pnl_usd=-4.0,
            ),
            HedgeRatioGroupSample(
                bothside_group_id="g2",
                settled_at="2026-02-02T00:00:00+00:00",
                directional_cost_usd=100.0,
                hedge_cost_usd=50.0,  # base_ratio=0.5
                directional_pnl_usd=-8.0,
                hedge_pnl_usd=4.0,
            ),
        ]

    def test_evaluate_ratio(self):
        ev = evaluate_hedge_ratio(self._samples(), hedge_ratio=0.5, dd_penalty=1.0)
        # Group pnl at base ratio:
        # g1 = 10 + (-4)*1 = 6
        # g2 = -8 + 4*1 = -4
        # total=2, max_dd=4, objective=-2
        assert ev.total_pnl_usd == pytest.approx(2.0)
        assert ev.max_drawdown_usd == pytest.approx(4.0)
        assert ev.objective_score == pytest.approx(-2.0)

    def test_optimize_prefers_higher_ratio_under_dd_penalty(self):
        result = optimize_hedge_ratio(
            self._samples(),
            min_ratio=0.3,
            max_ratio=0.8,
            step=0.1,
            dd_penalty=1.0,
        )
        # With this sample set, ratio=0.8 gives lower drawdown while preserving total pnl.
        assert result.best_ratio == pytest.approx(0.8)
        assert result.best_evaluation.max_drawdown_usd < 4.0
        assert result.best_evaluation.objective_score > -2.0

    def test_invalid_grid_raises(self):
        with pytest.raises(ValueError):
            optimize_hedge_ratio(self._samples(), step=0.0)
        with pytest.raises(ValueError):
            optimize_hedge_ratio(self._samples(), min_ratio=0.9, max_ratio=0.3)
