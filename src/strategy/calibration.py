"""Calibration table and lookup functions for Polymarket mispricing detection.

Based on lhtsports NBA ML analysis: Polymarket systematically underprices
outcomes in the 0.25-0.55 range (implied prob 25-40% vs actual win rate 72-80%).
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


# lhtsports NBA ML 分析から導出した初期テーブル (agent2-summary.json)
NBA_ML_CALIBRATION: list[CalibrationBand] = [
    CalibrationBand(0.05, 0.15, 0.270, -15.6, 50, "low"),
    CalibrationBand(0.15, 0.25, 0.430, -5.0, 200, "low"),
    CalibrationBand(0.25, 0.30, 0.715, 30.0, 92, "high"),
    CalibrationBand(0.30, 0.35, 0.650, 12.0, 150, "high"),
    CalibrationBand(0.35, 0.40, 0.795, 8.0, 172, "high"),
    CalibrationBand(0.40, 0.45, 0.750, 5.0, 200, "high"),
    CalibrationBand(0.45, 0.50, 0.800, 3.0, 300, "high"),
    CalibrationBand(0.50, 0.55, 0.849, 2.0, 849, "high"),
    CalibrationBand(0.55, 0.65, 0.880, -1.0, 360, "medium"),
    CalibrationBand(0.65, 0.75, 0.920, 2.0, 200, "medium"),
    CalibrationBand(0.75, 0.85, 0.960, 1.0, 100, "medium"),
]


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


def is_in_sweet_spot(price: float, lo: float = 0.25, hi: float = 0.55) -> bool:
    """Check if a price falls within the high-edge sweet spot."""
    return lo <= price <= hi
