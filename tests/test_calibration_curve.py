"""Tests for continuous calibration curve (Phase Q)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.strategy.calibration import NBA_ML_CALIBRATION, CalibrationBand
from src.strategy.calibration_curve import (
    ContinuousCalibration,
    _confidence_from_sample_size,
    get_default_curve,
)

# --- Helper fixtures ---


def _make_simple_bands() -> list[CalibrationBand]:
    """Minimal set of bands for testing."""
    return [
        CalibrationBand(0.20, 0.25, 0.70, 30.0, 50, "medium"),
        CalibrationBand(0.25, 0.30, 0.80, 40.0, 60, "medium"),
        CalibrationBand(0.30, 0.35, 0.85, 20.0, 70, "medium"),
        CalibrationBand(0.35, 0.40, 0.90, 25.0, 100, "high"),
        CalibrationBand(0.40, 0.45, 0.95, 10.0, 120, "high"),
    ]


def _make_nonmonotone_bands() -> list[CalibrationBand]:
    """Bands with monotonicity violations."""
    return [
        CalibrationBand(0.20, 0.25, 0.70, 10.0, 50, "medium"),
        CalibrationBand(0.25, 0.30, 0.85, 20.0, 60, "medium"),  # higher than next
        CalibrationBand(0.30, 0.35, 0.80, 15.0, 70, "medium"),  # lower than previous
        CalibrationBand(0.35, 0.40, 0.90, 25.0, 80, "medium"),
        CalibrationBand(0.40, 0.45, 0.92, 10.0, 90, "high"),
    ]


def _make_perfect_bands() -> list[CalibrationBand]:
    """Bands with 100% win rate (small sample)."""
    return [
        CalibrationBand(0.30, 0.35, 0.80, 20.0, 50, "medium"),
        CalibrationBand(0.35, 0.40, 0.90, 25.0, 60, "medium"),
        CalibrationBand(0.40, 0.45, 1.00, 10.0, 20, "low"),  # 100%, small N
        CalibrationBand(0.45, 0.50, 1.00, 15.0, 30, "low"),  # 100%, small N
    ]


# --- Tests ---


class TestPAVAMonotonicity:

    def test_monotone_input_unchanged(self):
        """Already monotone data should pass through PAVA unchanged."""
        bands = _make_simple_bands()
        curve = ContinuousCalibration.from_bands(bands, confidence_level=0.90)
        # point estimates should be non-decreasing
        for i in range(len(curve.knot_point_estimates) - 1):
            assert curve.knot_point_estimates[i] <= curve.knot_point_estimates[i + 1] + 1e-9

    def test_nonmonotone_corrected(self):
        """PAVA should fix monotonicity violations."""
        bands = _make_nonmonotone_bands()
        curve = ContinuousCalibration.from_bands(bands, confidence_level=0.90)
        for i in range(len(curve.knot_point_estimates) - 1):
            assert curve.knot_point_estimates[i] <= curve.knot_point_estimates[i + 1] + 1e-9

    def test_lower_bounds_monotone(self):
        """Lower bounds should also be non-decreasing after PAVA."""
        bands = _make_nonmonotone_bands()
        curve = ContinuousCalibration.from_bands(bands, confidence_level=0.90)
        for i in range(len(curve.knot_lower_bounds) - 1):
            assert curve.knot_lower_bounds[i] <= curve.knot_lower_bounds[i + 1] + 1e-9

    def test_pava_with_real_table(self):
        """Real NBA calibration table: verify PAVA fixes known inversions."""
        curve = ContinuousCalibration.from_bands(NBA_ML_CALIBRATION, 0.90)
        for i in range(len(curve.knot_point_estimates) - 1):
            assert curve.knot_point_estimates[i] <= curve.knot_point_estimates[i + 1] + 1e-9


class TestPCHIPInterpolation:

    def test_continuous_between_knots(self):
        """Values between knots should be interpolated smoothly."""
        bands = _make_simple_bands()
        curve = ContinuousCalibration.from_bands(bands, confidence_level=0.90)
        # Check intermediate points
        est1 = curve.estimate(0.27)
        est2 = curve.estimate(0.28)
        assert est1 is not None and est2 is not None
        # Should be close but not identical
        assert abs(est1.point_estimate - est2.point_estimate) < 0.05

    def test_monotonicity_preserved_by_pchip(self):
        """PCHIP should maintain monotonicity between knots."""
        bands = _make_simple_bands()
        curve = ContinuousCalibration.from_bands(bands, confidence_level=0.90)
        prices = [0.20 + i * 0.01 for i in range(26)]  # 0.20 to 0.45
        estimates = [curve.estimate(p) for p in prices]
        valid = [e for e in estimates if e is not None]
        for i in range(len(valid) - 1):
            # PCHIP preserves monotonicity
            assert valid[i].point_estimate <= valid[i + 1].point_estimate + 1e-9

    def test_values_in_valid_range(self):
        """All estimates should be in [0, 1]."""
        curve = ContinuousCalibration.from_bands(NBA_ML_CALIBRATION, 0.90)
        for p_int in range(20, 96):
            p = p_int / 100
            est = curve.estimate(p)
            if est is not None:
                assert 0.0 <= est.point_estimate <= 1.0
                assert 0.0 <= est.lower_bound <= 1.0
                assert 0.0 <= est.upper_bound <= 1.0


class TestBetaPosterior:

    def test_small_sample_conservative(self):
        """Small sample 100% win rate should have lower bound well below 1.0."""
        bands = _make_perfect_bands()
        curve = ContinuousCalibration.from_bands(bands, confidence_level=0.90)
        # 0.40-0.45 band: 100% win rate, N=20
        est = curve.estimate(0.425)
        assert est is not None
        assert est.point_estimate > 0.95  # PAVA maintains high point estimate
        assert est.lower_bound < 0.95  # But lower bound should be notably lower

    def test_large_sample_tight_bounds(self):
        """Large sample should have tight confidence interval."""
        bands = [
            CalibrationBand(0.30, 0.35, 0.85, 20.0, 200, "high"),
            CalibrationBand(0.35, 0.40, 0.90, 25.0, 300, "high"),
            CalibrationBand(0.40, 0.45, 0.92, 10.0, 500, "high"),
        ]
        curve = ContinuousCalibration.from_bands(bands, confidence_level=0.90)
        est = curve.estimate(0.425)
        assert est is not None
        # With N=500, bounds should be tight
        assert est.upper_bound - est.lower_bound < 0.10

    def test_lower_below_point(self):
        """Lower bound should always be <= point estimate."""
        curve = ContinuousCalibration.from_bands(NBA_ML_CALIBRATION, 0.90)
        for p_int in range(20, 95):
            p = p_int / 100
            est = curve.estimate(p)
            if est is not None:
                assert est.lower_bound <= est.point_estimate + 1e-9

    def test_upper_above_lower(self):
        """Upper bound should always be >= lower bound."""
        curve = ContinuousCalibration.from_bands(NBA_ML_CALIBRATION, 0.90)
        for p_int in range(20, 95):
            p = p_int / 100
            est = curve.estimate(p)
            if est is not None:
                assert est.upper_bound >= est.lower_bound - 1e-9

    def test_higher_confidence_wider_bounds(self):
        """Higher confidence level should produce wider bounds (lower lower_bound)."""
        bands = _make_simple_bands()
        curve_90 = ContinuousCalibration.from_bands(bands, confidence_level=0.90)
        curve_95 = ContinuousCalibration.from_bands(bands, confidence_level=0.95)
        est_90 = curve_90.estimate(0.325)
        est_95 = curve_95.estimate(0.325)
        assert est_90 is not None and est_95 is not None
        # 95% CI should have lower lower_bound than 90% CI
        assert est_95.lower_bound <= est_90.lower_bound + 1e-9


class TestPriceRangeHandling:

    def test_out_of_range_returns_none(self):
        """Prices outside the curve range should return None."""
        bands = _make_simple_bands()
        curve = ContinuousCalibration.from_bands(bands, confidence_level=0.90)
        assert curve.estimate(0.10) is None
        assert curve.estimate(0.99) is None

    def test_band_edges_covered(self):
        """Band edges (not just midpoints) should be within range."""
        curve = ContinuousCalibration.from_bands(NBA_ML_CALIBRATION, 0.90)
        # First band is 0.20-0.25, so price 0.20 should be valid
        assert curve.estimate(0.20) is not None
        # Last band is 0.90-0.95, so price 0.94 should be valid
        assert curve.estimate(0.94) is not None

    def test_exact_boundary(self):
        """Exact boundary prices should work."""
        curve = ContinuousCalibration.from_bands(NBA_ML_CALIBRATION, 0.90)
        # 0.95 is upper bound of last band
        assert curve.estimate(0.95) is not None
        # 0.20 is lower bound of first band
        assert curve.estimate(0.20) is not None


class TestJSONRoundTrip:

    def test_roundtrip(self):
        """to_dict -> from_dict should produce equivalent curve."""
        bands = _make_simple_bands()
        original = ContinuousCalibration.from_bands(bands, confidence_level=0.90)
        data = original.to_dict()
        restored = ContinuousCalibration.from_dict(data)

        # Check metadata
        assert restored.confidence_level == original.confidence_level
        assert restored.knot_prices == original.knot_prices
        assert restored.knot_point_estimates == pytest.approx(original.knot_point_estimates)
        assert restored.knot_lower_bounds == pytest.approx(original.knot_lower_bounds)

        # Check estimates match
        for p in [0.22, 0.30, 0.40]:
            e_orig = original.estimate(p)
            e_rest = restored.estimate(p)
            assert e_orig is not None and e_rest is not None
            assert e_rest.point_estimate == pytest.approx(e_orig.point_estimate, abs=1e-6)
            assert e_rest.lower_bound == pytest.approx(e_orig.lower_bound, abs=1e-6)

    def test_json_serializable(self):
        """to_dict should produce JSON-serializable output."""
        curve = ContinuousCalibration.from_bands(NBA_ML_CALIBRATION, 0.90)
        data = curve.to_dict()
        # Should not raise
        json_str = json.dumps(data)
        assert len(json_str) > 0

    def test_roundtrip_with_price_range(self):
        """Price range should survive roundtrip."""
        curve = ContinuousCalibration.from_bands(NBA_ML_CALIBRATION, 0.90)
        data = curve.to_dict()
        restored = ContinuousCalibration.from_dict(data)
        assert restored._price_lo == pytest.approx(curve._price_lo)
        assert restored._price_hi == pytest.approx(curve._price_hi)


class TestFromConditions:

    def _make_conditions(self, n: int = 200) -> list[dict]:
        """Generate synthetic conditions."""
        conditions = []
        for i in range(n):
            month = 1 + (i // 30)
            year = 2025 + (month - 1) // 12
            month = (month - 1) % 12 + 1
            day = (i % 28) + 1
            price = 0.25 + (i % 10) * 0.05  # 0.25-0.70 range
            status = "WIN" if (i % 4 != 0) else "LOSS_OR_OPEN"
            conditions.append({
                "date": f"{year:04d}-{month:02d}-{day:02d}",
                "avg_buy_price": price,
                "status": status,
                "net_cost": 25.0,
                "pnl": 25.0 * (1.0 / price - 1.0) if status == "WIN" else -25.0,
            })
        return conditions

    def test_from_conditions_basic(self):
        """Should build curve from conditions."""
        conds = self._make_conditions()
        curve = ContinuousCalibration.from_conditions(conds, confidence_level=0.90)
        assert len(curve.knot_prices) >= 2
        est = curve.estimate(0.40)
        assert est is not None
        assert 0.0 < est.point_estimate < 1.0

    def test_from_conditions_matches_bands(self):
        """from_conditions and from_bands should give similar results for same data."""
        conds = self._make_conditions(500)
        curve_cond = ContinuousCalibration.from_conditions(conds, confidence_level=0.90)
        # Also build via bands
        from src.strategy.calibration_builder import build_calibration_from_conditions
        result = build_calibration_from_conditions(conds)
        if len(result.bands) >= 2:
            curve_band = ContinuousCalibration.from_bands(result.bands, confidence_level=0.90)
            # Point estimates at midpoints should be identical
            for mp in curve_cond.knot_prices:
                e1 = curve_cond.estimate(mp)
                e2 = curve_band.estimate(mp)
                if e1 and e2:
                    assert e1.point_estimate == pytest.approx(e2.point_estimate, abs=0.01)

    def test_from_conditions_empty_raises(self):
        """Empty conditions should raise ValueError."""
        with pytest.raises(ValueError):
            ContinuousCalibration.from_conditions([], confidence_level=0.90)


class TestGetDefaultCurve:

    def test_returns_curve(self):
        """Should return a ContinuousCalibration instance."""
        curve = get_default_curve(confidence_level=0.90)
        assert isinstance(curve, ContinuousCalibration)

    def test_cached(self):
        """Same confidence_level should return same object."""
        c1 = get_default_curve(confidence_level=0.90)
        c2 = get_default_curve(confidence_level=0.90)
        assert c1 is c2

    def test_different_confidence_different_cache(self):
        """Different confidence levels should return different objects."""
        c1 = get_default_curve(confidence_level=0.90)
        c2 = get_default_curve(confidence_level=0.95)
        assert c1 is not c2


class TestEffectiveSampleSize:

    def test_nearest_knot_ess(self):
        """ESS should reflect the nearest knot's sample size."""
        curve = ContinuousCalibration.from_bands(NBA_ML_CALIBRATION, 0.90)
        # 0.40-0.45 band has N=121, midpoint=0.425
        est = curve.estimate(0.42)
        assert est is not None
        assert est.effective_sample_size == 121.0


class TestConfidenceFromSampleSize:

    def test_high(self):
        assert _confidence_from_sample_size(150) == "high"

    def test_medium(self):
        assert _confidence_from_sample_size(50) == "medium"

    def test_low(self):
        assert _confidence_from_sample_size(20) == "low"


class TestConstructorValidation:

    def test_too_few_knots(self):
        """Should raise with fewer than 2 knots."""
        with pytest.raises(ValueError, match="at least 2 knots"):
            ContinuousCalibration(
                knot_prices=[0.3],
                knot_point_estimates=[0.8],
                knot_lower_bounds=[0.7],
                knot_upper_bounds=[0.9],
                knot_sample_sizes=[50],
            )

    def test_too_few_bands(self):
        """from_bands should raise with fewer than 2 bands."""
        with pytest.raises(ValueError, match="at least 2 bands"):
            ContinuousCalibration.from_bands(
                [CalibrationBand(0.30, 0.35, 0.80, 10.0, 50, "medium")],
            )
