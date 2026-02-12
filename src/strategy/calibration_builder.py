"""Calibration table builder with walk-forward time-series separation.

Provides pure functions for:
- Building calibration tables from condition-level P&L data
- Walk-forward train/test splits to detect in-sample bias
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from src.strategy.calibration import CalibrationBand


@dataclass(frozen=True)
class CalibrationBuildResult:
    """Result of building a calibration table from data."""

    bands: list[CalibrationBand]
    train_start: str
    train_end: str
    total_conditions: int


def build_calibration_from_conditions(
    conditions: list[dict],
    train_start: str | None = None,
    train_end: str | None = None,
    price_key: str = "avg_buy_price",
) -> CalibrationBuildResult:
    """Build calibration table from conditions within date range.

    Args:
        conditions: List of condition dicts with at minimum:
            - date (str YYYY-MM-DD)
            - avg_buy_price (float)
            - status ("WIN" | "LOSS_OR_OPEN" | "MERGED")
            - net_cost (float)
            - pnl (float)
        train_start: Start date (inclusive), None = earliest.
        train_end: End date (exclusive), None = latest+1.
        price_key: Key to use for price band assignment.

    Returns:
        CalibrationBuildResult with bands and metadata.
    """
    # Filter by date range
    filtered = []
    for c in conditions:
        d = c.get("date", "")
        if not d:
            continue
        if train_start and d < train_start:
            continue
        if train_end and d >= train_end:
            continue
        filtered.append(c)

    # Only use WIN/LOSS_OR_OPEN (exclude MERGED — same as lhtsports methodology)
    eligible = [c for c in filtered if c.get("status") in ("WIN", "LOSS_OR_OPEN")]

    bands: list[CalibrationBand] = []
    for lo_int in range(20, 95, 5):  # 0.20-0.95 range (5-cent bands)
        lo = lo_int / 100
        hi = (lo_int + 5) / 100

        band_conds = [
            c for c in eligible
            if lo <= c.get(price_key, 0) < hi
        ]

        if not band_conds:
            continue

        wins = sum(1 for c in band_conds if c["status"] == "WIN")
        n = len(band_conds)
        win_rate = wins / n

        total_cost = sum(c.get("net_cost", 0) for c in band_conds)
        total_pnl = sum(c.get("pnl", 0) for c in band_conds)
        roi = total_pnl / total_cost * 100 if total_cost > 0 else 0.0

        if n >= 100:
            conf = "high"
        elif n >= 40:
            conf = "medium"
        else:
            conf = "low"

        bands.append(CalibrationBand(
            price_lo=lo,
            price_hi=hi,
            expected_win_rate=round(win_rate, 3),
            historical_roi_pct=round(roi, 1),
            sample_size=n,
            confidence=conf,
        ))

    dates = [c["date"] for c in eligible if c.get("date")]
    actual_start = min(dates) if dates else (train_start or "")
    actual_end = max(dates) if dates else (train_end or "")

    return CalibrationBuildResult(
        bands=bands,
        train_start=actual_start,
        train_end=actual_end,
        total_conditions=len(eligible),
    )


def walk_forward_split(
    conditions: list[dict],
    train_months: int = 6,
    test_months: int = 2,
    step_months: int = 1,
) -> list[tuple[CalibrationBuildResult, list[dict]]]:
    """Generate (train_table, test_conditions) pairs for walk-forward validation.

    Args:
        conditions: All conditions with "date" key.
        train_months: Training window in months.
        test_months: Test window in months.
        step_months: Step size for sliding window.

    Returns:
        List of (CalibrationBuildResult, test_conditions) tuples.
    """
    dates = sorted({c["date"] for c in conditions if c.get("date")})
    if not dates:
        return []

    # Get unique months
    months = sorted({d[:7] for d in dates})
    if len(months) < train_months + test_months:
        return []

    results: list[tuple[CalibrationBuildResult, list[dict]]] = []

    for i in range(0, len(months) - train_months - test_months + 1, step_months):
        train_start_month = months[i]
        train_end_month = months[i + train_months]
        test_end_idx = i + train_months + test_months
        test_end_month = months[test_end_idx] if test_end_idx < len(months) else None

        train_start = f"{train_start_month}-01"
        train_end = f"{train_end_month}-01"
        test_start = train_end
        test_end = f"{test_end_month}-01" if test_end_month else None

        # Build calibration table from training data
        build_result = build_calibration_from_conditions(
            conditions,
            train_start=train_start,
            train_end=train_end,
        )

        if not build_result.bands:
            continue

        # Filter test conditions
        test_conds = []
        for c in conditions:
            d = c.get("date", "")
            if not d:
                continue
            if d < test_start:
                continue
            if test_end and d >= test_end:
                continue
            if c.get("status") in ("WIN", "LOSS_OR_OPEN"):
                test_conds.append(c)

        if test_conds:
            results.append((build_result, test_conds))

    return results


def evaluate_split(
    train_result: CalibrationBuildResult,
    test_conditions: list[dict],
    price_key: str = "avg_buy_price",
) -> dict:
    """Evaluate a train/test split: expected vs realized P&L.

    Returns dict with:
        - period: test date range
        - expected_pnl: sum of EV * cost for each signal
        - realized_pnl: sum of actual P&L
        - n_signals: number of test signals that matched a band
        - gap_usd: realized - expected
        - gap_pct: gap / |expected| * 100
    """
    from src.strategy.calibration import lookup_band

    expected_pnl = 0.0
    realized_pnl = 0.0
    n_signals = 0

    for c in test_conditions:
        price = c.get(price_key, 0)
        if price <= 0 or price >= 1:
            continue

        band = lookup_band(price, train_result.bands)
        if band is None:
            continue

        # EV = expected_win_rate / price - 1
        ev_per_dollar = band.expected_win_rate / price - 1
        if ev_per_dollar <= 0:
            continue

        cost = c.get("net_cost", 0)
        if cost <= 0:
            continue

        expected_pnl += ev_per_dollar * cost
        realized_pnl += c.get("pnl", 0)
        n_signals += 1

    dates = [c["date"] for c in test_conditions if c.get("date")]
    period = f"{min(dates)} to {max(dates)}" if dates else "unknown"
    gap = realized_pnl - expected_pnl

    return {
        "period": period,
        "expected_pnl": round(expected_pnl, 2),
        "realized_pnl": round(realized_pnl, 2),
        "gap_usd": round(gap, 2),
        "gap_pct": round(gap / abs(expected_pnl) * 100, 1) if expected_pnl != 0 else 0.0,
        "n_signals": n_signals,
        "train_start": train_result.train_start,
        "train_end": train_result.train_end,
        "train_conditions": train_result.total_conditions,
        "train_bands": len(train_result.bands),
    }


def build_continuous_from_conditions(
    conditions: list[dict],
    confidence_level: float = 0.90,
    train_start: str | None = None,
    train_end: str | None = None,
    price_key: str = "avg_buy_price",
):
    """Build continuous calibration curve from condition data.

    Returns a ContinuousCalibration instance fitted on the given conditions.
    """
    from src.strategy.calibration_curve import ContinuousCalibration

    return ContinuousCalibration.from_conditions(
        conditions,
        confidence_level=confidence_level,
        price_key=price_key,
        train_start=train_start,
        train_end=train_end,
    )


def evaluate_split_continuous(
    train_result: CalibrationBuildResult,
    test_conditions: list[dict],
    confidence_level: float = 0.90,
    price_key: str = "avg_buy_price",
) -> dict:
    """Evaluate train/test split using continuous curve (conservative lower bound).

    Same return format as evaluate_split but uses continuous calibration.
    """
    from src.strategy.calibration_curve import ContinuousCalibration

    if not train_result.bands or len(train_result.bands) < 2:
        return {
            "period": "unknown",
            "expected_pnl": 0.0,
            "realized_pnl": 0.0,
            "gap_usd": 0.0,
            "gap_pct": 0.0,
            "n_signals": 0,
            "train_start": train_result.train_start,
            "train_end": train_result.train_end,
            "train_conditions": train_result.total_conditions,
            "train_bands": len(train_result.bands),
        }

    curve = ContinuousCalibration.from_bands(train_result.bands, confidence_level)

    expected_pnl = 0.0
    realized_pnl = 0.0
    n_signals = 0

    for c in test_conditions:
        price = c.get(price_key, 0)
        if price <= 0 or price >= 1:
            continue

        est = curve.estimate(price)
        if est is None:
            continue

        # 保守的推定 (下限)
        ev_per_dollar = est.lower_bound / price - 1
        if ev_per_dollar <= 0:
            continue

        cost = c.get("net_cost", 0)
        if cost <= 0:
            continue

        expected_pnl += ev_per_dollar * cost
        realized_pnl += c.get("pnl", 0)
        n_signals += 1

    dates = [c["date"] for c in test_conditions if c.get("date")]
    period = f"{min(dates)} to {max(dates)}" if dates else "unknown"
    gap = realized_pnl - expected_pnl

    return {
        "period": period,
        "expected_pnl": round(expected_pnl, 2),
        "realized_pnl": round(realized_pnl, 2),
        "gap_usd": round(gap, 2),
        "gap_pct": round(gap / abs(expected_pnl) * 100, 1) if expected_pnl != 0 else 0.0,
        "n_signals": n_signals,
        "train_start": train_result.train_start,
        "train_end": train_result.train_end,
        "train_conditions": train_result.total_conditions,
        "train_bands": len(train_result.bands),
    }


def _add_months(date_str: str, months: int) -> str:
    """Add months to a YYYY-MM-DD date string."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    month = dt.month + months
    year = dt.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    return f"{year:04d}-{month:02d}-01"
