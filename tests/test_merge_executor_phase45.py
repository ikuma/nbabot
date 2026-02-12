"""Tests for Phase 4/5 merge executor helper behavior."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.scheduler.merge_executor import (
    _estimate_capital_release_benefit_usd,
    _in_rollout_cohort,
)


def test_rollout_cohort_bounds():
    gid = "bs-rollout-test"
    assert _in_rollout_cohort(gid, 0) is False
    assert _in_rollout_cohort(gid, 100) is True


def test_rollout_cohort_deterministic():
    gid = "bs-rollout-deterministic"
    a = _in_rollout_cohort(gid, 25)
    b = _in_rollout_cohort(gid, 25)
    assert a == b


def test_capital_release_benefit_positive_for_future_tipoff():
    # Far-future execute_before guarantees positive horizon even in CI runtime.
    benefit = _estimate_capital_release_benefit_usd(
        merge_amount=100.0,
        combined_vwap=0.95,
        execute_before="2099-01-01T00:00:00+00:00",
    )
    assert benefit > 0


def test_capital_release_benefit_zero_for_zero_principal():
    benefit = _estimate_capital_release_benefit_usd(
        merge_amount=0.0,
        combined_vwap=0.95,
        execute_before="2099-01-01T00:00:00+00:00",
    )
    assert benefit == 0.0
