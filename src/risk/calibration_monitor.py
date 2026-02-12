"""Calibration drift detection.

Monitors per-band rolling win rates against the calibration table
and flags bands where observed performance diverges significantly.
"""

from __future__ import annotations

import logging
import math
from pathlib import Path

from src.risk.models import CalibrationHealthMetrics
from src.strategy.calibration import NBA_ML_CALIBRATION, CalibrationBand

logger = logging.getLogger(__name__)


def compute_calibration_health(
    db_path: Path | str,
    min_sample: int = 20,
    drift_threshold_sigma: float = 2.0,
) -> list[CalibrationHealthMetrics]:
    """Compute rolling win rate and trade profit rate per band and detect drift.

    Uses both game_correct z-score and trade_profit z-score for drift detection.
    Trade profit rate directly reflects realized revenue.

    Args:
        db_path: Path to SQLite database.
        min_sample: Minimum settled conditions per band to evaluate.
        drift_threshold_sigma: Z-score threshold for flagging drift.

    Returns:
        List of CalibrationHealthMetrics, one per band with enough data.
    """
    from src.store.db import get_band_decomposed_stats, get_band_win_rates

    band_stats = get_band_win_rates(db_path=db_path)
    decomposed_stats = get_band_decomposed_stats(db_path=db_path)
    results: list[CalibrationHealthMetrics] = []

    for band in NBA_ML_CALIBRATION:
        label = f"{band.price_lo:.2f}-{band.price_hi:.2f}"
        stats = band_stats.get(label)
        if not stats or stats["total"] < min_sample:
            continue

        rolling_wr = stats["wins"] / stats["total"]
        z_game = _z_score(rolling_wr, band.expected_win_rate, stats["total"])

        # Trade profit z-score (trade_profitable / total)
        decomp = decomposed_stats.get(label)
        z_profit = 0.0
        if decomp and decomp["total"] >= min_sample:
            trade_profit_rate = decomp["trade_profitable"] / decomp["total"]
            z_profit = _z_score(
                trade_profit_rate, band.expected_win_rate, decomp["total"]
            )

        # Drift if either game_correct OR trade_profit is significantly below expected
        drifted = z_game < -drift_threshold_sigma or z_profit < -drift_threshold_sigma

        results.append(
            CalibrationHealthMetrics(
                band_label=label,
                expected_win_rate=band.expected_win_rate,
                rolling_win_rate=rolling_wr,
                sample_size=stats["total"],
                z_score=z_game,
                drifted=drifted,
            )
        )

        if drifted:
            logger.warning(
                "Calibration drift: band %s expected=%.1f%% "
                "game_correct=%.1f%% (z=%.2f) trade_profit z=%.2f (n=%d)",
                label,
                band.expected_win_rate * 100,
                rolling_wr * 100,
                z_game,
                z_profit,
                stats["total"],
            )

    return results


def should_pause_band(
    band: CalibrationBand,
    rolling_win_rate: float,
    sample_size: int,
    drift_threshold_sigma: float = 2.0,
    min_sample: int = 20,
) -> bool:
    """Check if a band should be paused due to drift.

    Returns True if sample is sufficient and win rate is significantly below expected.
    """
    if sample_size < min_sample:
        return False
    z = _z_score(rolling_win_rate, band.expected_win_rate, sample_size)
    return z < -drift_threshold_sigma


def compute_continuous_drift(
    db_path: Path | str,
    min_sample: int = 20,
    drift_threshold_sigma: float = 2.0,
) -> list[CalibrationHealthMetrics]:
    """Compute drift using the continuous calibration curve.

    Uses the continuous curve's point estimate as the expected win rate
    (instead of the discrete band value) for a more accurate comparison.
    """
    from src.store.db import get_band_win_rates
    from src.strategy.calibration_curve import get_default_curve

    curve = get_default_curve()
    band_stats = get_band_win_rates(db_path=db_path)
    results: list[CalibrationHealthMetrics] = []

    for band in NBA_ML_CALIBRATION:
        label = f"{band.price_lo:.2f}-{band.price_hi:.2f}"
        stats = band_stats.get(label)
        if not stats or stats["total"] < min_sample:
            continue

        midpoint = (band.price_lo + band.price_hi) / 2
        est = curve.estimate(midpoint)
        if est is None:
            continue

        rolling_wr = stats["wins"] / stats["total"]
        # 連続カーブの点推定に対する z-score
        z = _z_score(rolling_wr, est.point_estimate, stats["total"])
        drifted = z < -drift_threshold_sigma

        results.append(
            CalibrationHealthMetrics(
                band_label=label,
                expected_win_rate=est.point_estimate,
                rolling_win_rate=rolling_wr,
                sample_size=stats["total"],
                z_score=z,
                drifted=drifted,
            )
        )

        if drifted:
            logger.warning(
                "Continuous drift: band %s curve_est=%.1f%% "
                "observed=%.1f%% (z=%.2f, n=%d)",
                label,
                est.point_estimate * 100,
                rolling_wr * 100,
                z,
                stats["total"],
            )

    return results


def _z_score(observed: float, expected: float, n: int) -> float:
    """Compute z-score for a binomial proportion test."""
    if n <= 0 or expected <= 0 or expected >= 1:
        return 0.0
    se = math.sqrt(expected * (1 - expected) / n)
    if se == 0:
        return 0.0
    return (observed - expected) / se
