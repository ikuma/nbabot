"""Tests for dynamic bothside target_combined calculation."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.strategy.bothside_target import (
    estimate_shares_from_pairs,
    resolve_target_combined,
)


def test_estimate_shares_from_pairs():
    shares = estimate_shares_from_pairs([25.0, 25.0], [0.50, 0.25])
    assert shares == pytest.approx(150.0)  # 50 + 100


def test_static_mode_uses_static_target():
    d = resolve_target_combined(
        static_target=0.97,
        mode="static",
        mergeable_shares_est=100.0,
        estimated_fee_usd=0.2,
        min_profit_usd=0.1,
        min_target=0.90,
        max_target=0.994,
    )
    assert d.target_combined == pytest.approx(0.97)
    assert d.reason == "static_mode"


def test_dynamic_mode_large_shares_relaxes_target():
    d = resolve_target_combined(
        static_target=0.97,
        mode="dynamic",
        mergeable_shares_est=200.0,
        estimated_fee_usd=0.2,
        min_profit_usd=0.1,
        min_target=0.90,
        max_target=0.994,
    )
    # required per share = (0.2+0.1)/200 = 0.0015 -> target ~0.9985 -> clamped to 0.994
    assert d.target_combined == pytest.approx(0.994)
    assert d.required_profit_per_share == pytest.approx(0.0015)
    assert d.reason == "dynamic_formula"


def test_dynamic_mode_small_shares_tightens_target():
    d = resolve_target_combined(
        static_target=0.97,
        mode="dynamic",
        mergeable_shares_est=3.0,
        estimated_fee_usd=0.2,
        min_profit_usd=0.1,
        min_target=0.90,
        max_target=0.994,
    )
    # required per share = 0.1 -> target=0.9
    assert d.target_combined == pytest.approx(0.90)
    assert d.required_profit_per_share == pytest.approx(0.1)


def test_dynamic_fallback_when_no_shares():
    d = resolve_target_combined(
        static_target=0.97,
        mode="dynamic",
        mergeable_shares_est=0.0,
        estimated_fee_usd=0.2,
        min_profit_usd=0.1,
        min_target=0.90,
        max_target=0.994,
    )
    assert d.target_combined == pytest.approx(0.97)
    assert d.reason == "fallback_no_shares"
