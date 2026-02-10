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
    """Compute rolling win rate per band and detect drift.

    Args:
        db_path: Path to SQLite database.
        min_sample: Minimum settled conditions per band to evaluate.
        drift_threshold_sigma: Z-score threshold for flagging drift.

    Returns:
        List of CalibrationHealthMetrics, one per band with enough data.
    """
    from src.store.db import get_band_win_rates

    band_stats = get_band_win_rates(db_path=db_path)
    results: list[CalibrationHealthMetrics] = []

    for band in NBA_ML_CALIBRATION:
        label = f"{band.price_lo:.2f}-{band.price_hi:.2f}"
        stats = band_stats.get(label)
        if not stats or stats["total"] < min_sample:
            continue

        rolling_wr = stats["wins"] / stats["total"]
        z = _z_score(rolling_wr, band.expected_win_rate, stats["total"])
        drifted = z < -drift_threshold_sigma  # 下振れのみ検出

        results.append(
            CalibrationHealthMetrics(
                band_label=label,
                expected_win_rate=band.expected_win_rate,
                rolling_win_rate=rolling_wr,
                sample_size=stats["total"],
                z_score=z,
                drifted=drifted,
            )
        )

        if drifted:
            logger.warning(
                "Calibration drift: band %s expected=%.1f%% actual=%.1f%% z=%.2f (n=%d)",
                label,
                band.expected_win_rate * 100,
                rolling_wr * 100,
                z,
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


def _z_score(observed: float, expected: float, n: int) -> float:
    """Compute z-score for a binomial proportion test."""
    if n <= 0 or expected <= 0 or expected >= 1:
        return 0.0
    se = math.sqrt(expected * (1 - expected) / n)
    if se == 0:
        return 0.0
    return (observed - expected) / se
