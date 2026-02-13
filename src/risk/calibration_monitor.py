"""Calibration drift detection.

Monitors per-band rolling win rates against the calibration table
and flags bands where observed performance diverges significantly.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, pstdev

from src.risk.models import CalibrationHealthMetrics
from src.strategy.calibration import NBA_ML_CALIBRATION, CalibrationBand

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PnLGapHealthMetrics:
    """Expected vs realized PnL health for a scope/window."""

    scope: str  # "total" or band label
    days: int
    expected_pnl: float
    realized_pnl: float
    gap_usd: float
    gap_pct: float
    sample_size: int


@dataclass(frozen=True)
class StructuralChangeHealthMetrics:
    """CUSUM-based structural change health."""

    scope: str  # "total" or band label
    days: int
    sample_points: int
    cusum_score: float
    yellow_triggered: bool
    orange_triggered: bool


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


def compute_pnl_divergence_health(
    db_path: Path | str,
    as_of_date: str | None = None,
    short_days: int = 7,
    long_days: int = 28,
) -> dict[str, dict[str, PnLGapHealthMetrics]]:
    """Compute expected-vs-realized PnL divergence for total and per-band scopes."""
    from src.store.db import (
        get_expected_realized_gap_by_band,
        get_expected_realized_gap_summary,
    )

    total_short_raw = get_expected_realized_gap_summary(
        days=short_days, as_of_date=as_of_date, db_path=db_path
    )
    total_long_raw = get_expected_realized_gap_summary(
        days=long_days, as_of_date=as_of_date, db_path=db_path
    )
    band_short_raw = get_expected_realized_gap_by_band(
        days=short_days, as_of_date=as_of_date, db_path=db_path
    )
    band_long_raw = get_expected_realized_gap_by_band(
        days=long_days, as_of_date=as_of_date, db_path=db_path
    )

    return {
        "total": {
            "short": PnLGapHealthMetrics(
                scope="total",
                days=short_days,
                expected_pnl=total_short_raw["expected_pnl"],
                realized_pnl=total_short_raw["realized_pnl"],
                gap_usd=total_short_raw["gap_usd"],
                gap_pct=total_short_raw["gap_pct"],
                sample_size=total_short_raw["total"],
            ),
            "long": PnLGapHealthMetrics(
                scope="total",
                days=long_days,
                expected_pnl=total_long_raw["expected_pnl"],
                realized_pnl=total_long_raw["realized_pnl"],
                gap_usd=total_long_raw["gap_usd"],
                gap_pct=total_long_raw["gap_pct"],
                sample_size=total_long_raw["total"],
            ),
        },
        "bands": {
            "short": {
                label: PnLGapHealthMetrics(
                    scope=label,
                    days=short_days,
                    expected_pnl=stats["expected_pnl"],
                    realized_pnl=stats["realized_pnl"],
                    gap_usd=stats["gap_usd"],
                    gap_pct=stats["gap_pct"],
                    sample_size=stats["total"],
                )
                for label, stats in band_short_raw.items()
            },
            "long": {
                label: PnLGapHealthMetrics(
                    scope=label,
                    days=long_days,
                    expected_pnl=stats["expected_pnl"],
                    realized_pnl=stats["realized_pnl"],
                    gap_usd=stats["gap_usd"],
                    gap_pct=stats["gap_pct"],
                    sample_size=stats["total"],
                )
                for label, stats in band_long_raw.items()
            },
        },
    }


def evaluate_pnl_divergence_flags(
    health: dict[str, dict[str, PnLGapHealthMetrics]],
    *,
    min_total_short: int = 30,
    min_total_long: int = 80,
    min_band_short: int = 10,
    yellow_total_gap_pct: float = -15.0,
    yellow_total_gap_usd: float = -50.0,
    yellow_band_gap_pct: float = -20.0,
    yellow_band_gap_usd: float = -20.0,
    yellow_band_count: int = 2,
    orange_short_gap_pct: float = -25.0,
    orange_short_gap_usd: float = -100.0,
    orange_long_gap_pct: float = -10.0,
) -> set[str]:
    """Evaluate PnL divergence health and emit CB flags."""
    flags: set[str] = set()

    total_short = health["total"]["short"]
    total_long = health["total"]["long"]

    yellow_total = (
        total_short.sample_size >= min_total_short
        and total_short.gap_pct <= yellow_total_gap_pct
        and total_short.gap_usd <= yellow_total_gap_usd
    )

    short_bands = health["bands"]["short"].values()
    yellow_band_hits = [
        b for b in short_bands
        if b.sample_size >= min_band_short
        and b.gap_pct <= yellow_band_gap_pct
        and b.gap_usd <= yellow_band_gap_usd
    ]
    yellow_band = len(yellow_band_hits) >= yellow_band_count

    if yellow_total or yellow_band:
        flags.add("pnl_divergence_yellow")

    orange_total = (
        total_short.sample_size >= min_total_short
        and total_long.sample_size >= min_total_long
        and total_short.gap_pct <= orange_short_gap_pct
        and total_short.gap_usd <= orange_short_gap_usd
        and total_long.gap_pct <= orange_long_gap_pct
    )
    if orange_total:
        flags.add("pnl_divergence_orange")

    if "pnl_divergence_orange" in flags:
        logger.warning(
            "PnL divergence ORANGE: short(%dd) gap=%+.1f%%/$%+.2f n=%d, "
            "long(%dd) gap=%+.1f%%/$%+.2f n=%d",
            total_short.days,
            total_short.gap_pct,
            total_short.gap_usd,
            total_short.sample_size,
            total_long.days,
            total_long.gap_pct,
            total_long.gap_usd,
            total_long.sample_size,
        )
    elif "pnl_divergence_yellow" in flags:
        logger.warning(
            "PnL divergence YELLOW: short(%dd) gap=%+.1f%%/$%+.2f n=%d, "
            "band_hits=%d",
            total_short.days,
            total_short.gap_pct,
            total_short.gap_usd,
            total_short.sample_size,
            len(yellow_band_hits),
        )

    return flags


def compute_structural_change_health(
    db_path: Path | str,
    as_of_date: str | None = None,
    window_days: int = 28,
    cusum_k: float = 0.5,
    cusum_h_yellow: float = 4.5,
    cusum_h_orange: float = 6.0,
) -> dict[str, object]:
    """Compute CUSUM structural-change health for total and per-band series."""
    from src.store.db import get_daily_gap_series, get_expected_realized_gap_by_band

    total_series = get_daily_gap_series(
        days=window_days, as_of_date=as_of_date, db_path=db_path
    )
    total_score = _cusum_score(total_series, k=cusum_k)
    total = StructuralChangeHealthMetrics(
        scope="total",
        days=window_days,
        sample_points=len(total_series),
        cusum_score=total_score,
        yellow_triggered=total_score >= cusum_h_yellow,
        orange_triggered=total_score >= cusum_h_orange,
    )

    band_raw = get_expected_realized_gap_by_band(
        days=window_days, as_of_date=as_of_date, db_path=db_path
    )
    bands: dict[str, StructuralChangeHealthMetrics] = {}
    for label in sorted(band_raw):
        series = get_daily_gap_series(
            days=window_days,
            as_of_date=as_of_date,
            band_label=label,
            db_path=db_path,
        )
        score = _cusum_score(series, k=cusum_k)
        bands[label] = StructuralChangeHealthMetrics(
            scope=label,
            days=window_days,
            sample_points=len(series),
            cusum_score=score,
            yellow_triggered=score >= cusum_h_yellow,
            orange_triggered=score >= cusum_h_orange,
        )

    return {"total": total, "bands": bands}


def evaluate_structural_change_flags(
    health: dict[str, object],
    *,
    min_points: int = 8,
    yellow_band_count: int = 1,
    orange_band_count: int = 2,
) -> set[str]:
    """Evaluate structural-change health and emit CB flags."""
    flags: set[str] = set()
    total = health["total"]
    assert isinstance(total, StructuralChangeHealthMetrics)
    bands = health["bands"]
    assert isinstance(bands, dict)

    yellow_band_hits = [
        m for m in bands.values()
        if isinstance(m, StructuralChangeHealthMetrics)
        and m.sample_points >= min_points
        and m.yellow_triggered
    ]
    orange_band_hits = [
        m for m in bands.values()
        if isinstance(m, StructuralChangeHealthMetrics)
        and m.sample_points >= min_points
        and m.orange_triggered
    ]

    yellow_total = total.sample_points >= min_points and total.yellow_triggered
    orange_total = total.sample_points >= min_points and total.orange_triggered

    if yellow_total or len(yellow_band_hits) >= yellow_band_count:
        flags.add("structural_change_yellow")
    if orange_total or len(orange_band_hits) >= orange_band_count:
        flags.add("structural_change_orange")

    if "structural_change_orange" in flags:
        logger.warning(
            "Structural change ORANGE: total_score=%.2f (n=%d), band_hits=%d",
            total.cusum_score,
            total.sample_points,
            len(orange_band_hits),
        )
    elif "structural_change_yellow" in flags:
        logger.warning(
            "Structural change YELLOW: total_score=%.2f (n=%d), band_hits=%d",
            total.cusum_score,
            total.sample_points,
            len(yellow_band_hits),
        )

    return flags


def _cusum_score(series: list[float], k: float = 0.5) -> float:
    """Return max two-sided CUSUM score on standardized residuals."""
    if len(series) < 2:
        return 0.0

    mu = mean(series)
    sigma = pstdev(series)
    if sigma <= 1e-9:
        return 0.0

    pos = 0.0
    neg = 0.0
    max_score = 0.0
    for value in series:
        z = (value - mu) / sigma
        pos = max(0.0, pos + z - k)
        neg = max(0.0, neg - z - k)
        max_score = max(max_score, pos, neg)
    return max_score
