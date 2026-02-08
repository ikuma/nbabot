"""Tests for calibration table and lookup functions."""

from __future__ import annotations

import pytest

from src.strategy.calibration import (
    NBA_ML_CALIBRATION,
    CalibrationBand,
    is_in_sweet_spot,
    lookup_band,
)


class TestLookupBand:
    def test_sweet_spot_price(self):
        """Price 0.35 → band [0.35, 0.40)."""
        band = lookup_band(0.35)
        assert band is not None
        assert band.price_lo == 0.35
        assert band.price_hi == 0.40
        assert band.confidence == "high"

    def test_lower_boundary_inclusive(self):
        """Lower bound is inclusive."""
        band = lookup_band(0.25)
        assert band is not None
        assert band.price_lo == 0.25

    def test_upper_boundary_exclusive(self):
        """Upper bound is exclusive — 0.30 falls into [0.30, 0.35)."""
        band = lookup_band(0.30)
        assert band is not None
        assert band.price_lo == 0.30
        assert band.price_hi == 0.35

    def test_below_all_bands(self):
        """Price below minimum → None."""
        assert lookup_band(0.01) is None

    def test_above_all_bands(self):
        """Price above maximum → None."""
        assert lookup_band(0.90) is None

    def test_at_exact_upper_bound_of_last_band(self):
        """Price at 0.85 (upper bound of last band) → None."""
        assert lookup_band(0.85) is None

    def test_mid_band_lookup(self):
        """Price 0.42 → band [0.40, 0.45)."""
        band = lookup_band(0.42)
        assert band is not None
        assert band.price_lo == 0.40
        assert band.price_hi == 0.45

    def test_custom_table(self):
        """Custom table works."""
        custom = [CalibrationBand(0.10, 0.50, 0.60, 5.0, 100, "high")]
        band = lookup_band(0.30, table=custom)
        assert band is not None
        assert band.expected_win_rate == 0.60

    def test_all_bands_contiguous(self):
        """Each band's hi == next band's lo (no gaps in sweet spot)."""
        for i in range(len(NBA_ML_CALIBRATION) - 1):
            curr = NBA_ML_CALIBRATION[i]
            nxt = NBA_ML_CALIBRATION[i + 1]
            assert curr.price_hi == pytest.approx(nxt.price_lo, abs=0.001), (
                f"Gap between band {i} hi={curr.price_hi} and band {i+1} lo={nxt.price_lo}"
            )


class TestIsInSweetSpot:
    def test_in_sweet_spot(self):
        assert is_in_sweet_spot(0.35) is True

    def test_at_lower_boundary(self):
        assert is_in_sweet_spot(0.25) is True

    def test_at_upper_boundary(self):
        assert is_in_sweet_spot(0.55) is True

    def test_below_sweet_spot(self):
        assert is_in_sweet_spot(0.20) is False

    def test_above_sweet_spot(self):
        assert is_in_sweet_spot(0.60) is False

    def test_custom_bounds(self):
        assert is_in_sweet_spot(0.30, lo=0.30, hi=0.40) is True
        assert is_in_sweet_spot(0.29, lo=0.30, hi=0.40) is False
