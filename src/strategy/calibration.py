"""Calibration table and lookup functions for Polymarket mispricing detection.

Based on lhtsports NBA ML analysis (2024-12 to 2026-02, 1,395 settled conditions).
Uniform 5-cent bands. Prices below 0.20 are excluded (< 0.20 bands have
win rate 12-36% and cumulative P&L deeply negative).
MERGED conditions are excluded from win rate calculation.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CalibrationBand:
    """A price band with historical win rate from lhtsports data."""

    price_lo: float  # lower bound (inclusive)
    price_hi: float  # upper bound (exclusive)
    expected_win_rate: float  # historical win rate in this band
    historical_roi_pct: float  # historical ROI %
    sample_size: int  # number of trades in lhtsports data
    confidence: str  # "high" | "medium" | "low"


# lhtsports NBA ML 全データ (2024-12 〜 2026-02, 1,395 settled conditions)
# 5-cent uniform bands, 0.20-0.95 range
# MERGED 除外、WIN/LOSS_OR_OPEN のみで勝率算出
NBA_ML_CALIBRATION: list[CalibrationBand] = [
    # === Sweet spot (0.20-0.55): フル Kelly ===
    CalibrationBand(0.20, 0.25, 0.711, 30.3, 45, "medium"),
    CalibrationBand(0.25, 0.30, 0.852, 54.5, 54, "medium"),
    CalibrationBand(0.30, 0.35, 0.822, 20.0, 73, "medium"),
    CalibrationBand(0.35, 0.40, 0.904, 26.1, 104, "high"),
    CalibrationBand(0.40, 0.45, 0.917, 7.4, 121, "high"),
    CalibrationBand(0.45, 0.50, 0.938, 5.9, 162, "high"),
    CalibrationBand(0.50, 0.55, 0.947, 6.2, 169, "high"),
    # === Outside sweet spot: 0.5x Kelly ===
    CalibrationBand(0.55, 0.60, 0.957, 4.0, 141, "high"),
    CalibrationBand(0.60, 0.65, 0.974, 16.0, 78, "medium"),
    CalibrationBand(0.65, 0.70, 0.931, 2.1, 58, "medium"),
    CalibrationBand(0.70, 0.75, 0.933, 15.5, 45, "medium"),
    CalibrationBand(0.75, 0.80, 0.973, 15.9, 37, "low"),
    CalibrationBand(0.80, 0.85, 1.000, 17.4, 33, "low"),
    CalibrationBand(0.85, 0.90, 1.000, 14.1, 30, "low"),
    CalibrationBand(0.90, 0.95, 1.000, 8.8, 22, "low"),
]


def load_calibration_table(path: str | None = None) -> list[CalibrationBand]:
    """Load calibration table from JSON file, or fall back to hardcoded NBA_ML_CALIBRATION.

    Args:
        path: Path to a JSON file with calibration band data.
              If None, returns the hardcoded default.
    """
    if path is None:
        return NBA_ML_CALIBRATION

    import json
    from pathlib import Path as _P

    p = _P(path)
    if not p.exists():
        return NBA_ML_CALIBRATION

    try:
        with open(p) as f:
            data = json.load(f)
        bands = []
        for b in data:
            bands.append(CalibrationBand(
                price_lo=float(b["price_lo"]),
                price_hi=float(b["price_hi"]),
                expected_win_rate=float(b["expected_win_rate"]),
                historical_roi_pct=float(b.get("historical_roi_pct", 0)),
                sample_size=int(b.get("sample_size", 0)),
                confidence=b.get("confidence", "low"),
            ))
        return bands if bands else NBA_ML_CALIBRATION
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return NBA_ML_CALIBRATION


def lookup_band(
    price: float, table: list[CalibrationBand] = NBA_ML_CALIBRATION
) -> CalibrationBand | None:
    """Find the calibration band for a given price.

    Returns None if the price falls outside all bands.
    """
    for band in table:
        if band.price_lo <= price < band.price_hi:
            return band
    return None


def is_in_sweet_spot(price: float, lo: float = 0.20, hi: float = 0.55) -> bool:
    """Check if a price falls within the high-edge sweet spot."""
    return lo <= price <= hi
