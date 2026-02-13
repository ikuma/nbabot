"""Tests for PnL divergence + structural-change drift monitors."""

from __future__ import annotations

from src.risk.calibration_monitor import (
    PnLGapHealthMetrics,
    StructuralChangeHealthMetrics,
    _cusum_score,
    evaluate_pnl_divergence_flags,
    evaluate_structural_change_flags,
)


def _gap(
    scope: str,
    days: int,
    sample_size: int,
    gap_pct: float,
    gap_usd: float,
) -> PnLGapHealthMetrics:
    expected = 100.0
    return PnLGapHealthMetrics(
        scope=scope,
        days=days,
        expected_pnl=expected,
        realized_pnl=expected + gap_usd,
        gap_usd=gap_usd,
        gap_pct=gap_pct,
        sample_size=sample_size,
    )


def test_pnl_divergence_respects_min_sample() -> None:
    health = {
        "total": {
            "short": _gap("total", 7, 20, -40.0, -200.0),  # below min_total_short=30
            "long": _gap("total", 28, 60, -20.0, -300.0),  # below min_total_long=80
        },
        "bands": {
            "short": {
                "0.30-0.35": _gap("0.30-0.35", 7, 9, -35.0, -80.0),  # below min_band_short=10
            },
            "long": {},
        },
    }
    flags = evaluate_pnl_divergence_flags(health)
    assert flags == set()


def test_pnl_divergence_yellow_by_band_hits() -> None:
    health = {
        "total": {
            "short": _gap("total", 7, 20, -5.0, -10.0),  # below total min sample
            "long": _gap("total", 28, 50, -3.0, -12.0),
        },
        "bands": {
            "short": {
                "0.25-0.30": _gap("0.25-0.30", 7, 10, -24.0, -30.0),
                "0.30-0.35": _gap("0.30-0.35", 7, 12, -23.0, -35.0),
            },
            "long": {},
        },
    }
    flags = evaluate_pnl_divergence_flags(health)
    assert "pnl_divergence_yellow" in flags
    assert "pnl_divergence_orange" not in flags


def test_pnl_divergence_orange_on_severe_total_gap() -> None:
    health = {
        "total": {
            "short": _gap("total", 7, 40, -30.0, -180.0),
            "long": _gap("total", 28, 90, -12.0, -260.0),
        },
        "bands": {"short": {}, "long": {}},
    }
    flags = evaluate_pnl_divergence_flags(health)
    assert "pnl_divergence_orange" in flags


def test_structural_change_yellow_from_total_score() -> None:
    health = {
        "total": StructuralChangeHealthMetrics(
            scope="total",
            days=28,
            sample_points=12,
            cusum_score=5.0,
            yellow_triggered=True,
            orange_triggered=False,
        ),
        "bands": {},
    }
    flags = evaluate_structural_change_flags(health)
    assert "structural_change_yellow" in flags
    assert "structural_change_orange" not in flags


def test_structural_change_orange_from_band_count() -> None:
    health = {
        "total": StructuralChangeHealthMetrics(
            scope="total",
            days=28,
            sample_points=12,
            cusum_score=3.0,
            yellow_triggered=False,
            orange_triggered=False,
        ),
        "bands": {
            "0.30-0.35": StructuralChangeHealthMetrics(
                scope="0.30-0.35",
                days=28,
                sample_points=10,
                cusum_score=6.1,
                yellow_triggered=True,
                orange_triggered=True,
            ),
            "0.35-0.40": StructuralChangeHealthMetrics(
                scope="0.35-0.40",
                days=28,
                sample_points=9,
                cusum_score=6.4,
                yellow_triggered=True,
                orange_triggered=True,
            ),
        },
    }
    flags = evaluate_structural_change_flags(health)
    assert "structural_change_orange" in flags


def test_cusum_score_detects_distribution_shift() -> None:
    flat = [0.0] * 20
    shifted = [0.0] * 10 + [4.0] * 10

    flat_score = _cusum_score(flat)
    shifted_score = _cusum_score(shifted)

    assert flat_score == 0.0
    assert shifted_score > flat_score
    assert shifted_score > 1.0
