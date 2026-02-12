"""Tests for calibration_builder (Phase M2)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.strategy.calibration import load_calibration_table
from src.strategy.calibration_builder import (
    CalibrationBuildResult,
    build_calibration_from_conditions,
    evaluate_split,
    walk_forward_split,
)


def _make_conditions(n: int, start_month: str = "2025-01") -> list[dict]:
    """Generate synthetic conditions for testing."""
    conditions = []
    year, month = map(int, start_month.split("-"))

    for i in range(n):
        m = month + (i // 30)
        y = year + (m - 1) // 12
        m = (m - 1) % 12 + 1
        day = (i % 28) + 1
        date = f"{y:04d}-{m:02d}-{day:02d}"

        # Alternate prices in sweet spot
        price = 0.30 + (i % 6) * 0.05  # 0.30-0.55 range

        # 80% win rate
        status = "WIN" if (i % 5 != 0) else "LOSS_OR_OPEN"
        net_cost = 25.0
        pnl = net_cost * (1.0 / price - 1.0) if status == "WIN" else -net_cost

        conditions.append({
            "date": date,
            "avg_buy_price": price,
            "status": status,
            "net_cost": net_cost,
            "pnl": pnl,
        })

    return conditions


class TestBuildCalibrationFromConditions:

    def test_empty_conditions(self):
        result = build_calibration_from_conditions([])
        assert result.bands == []
        assert result.total_conditions == 0

    def test_basic_build(self):
        conds = _make_conditions(100)
        result = build_calibration_from_conditions(conds)
        assert isinstance(result, CalibrationBuildResult)
        assert len(result.bands) > 0
        assert result.total_conditions > 0
        assert result.train_start <= result.train_end

    def test_date_filtering(self):
        conds = _make_conditions(200, start_month="2025-01")
        # Only use first 2 months
        result = build_calibration_from_conditions(
            conds, train_start="2025-01-01", train_end="2025-03-01"
        )
        assert result.total_conditions > 0
        assert result.total_conditions < 200

    def test_merged_excluded(self):
        """MERGED conditions should not appear in the calibration table."""
        conds = [
            {"date": "2025-01-01", "avg_buy_price": 0.40, "status": "WIN",
             "net_cost": 25.0, "pnl": 37.5},
            {"date": "2025-01-02", "avg_buy_price": 0.40, "status": "MERGED",
             "net_cost": 25.0, "pnl": 1.0},
            {"date": "2025-01-03", "avg_buy_price": 0.40, "status": "LOSS_OR_OPEN",
             "net_cost": 25.0, "pnl": -25.0},
        ]
        result = build_calibration_from_conditions(conds)
        # Only WIN and LOSS_OR_OPEN should count
        assert result.total_conditions == 2

    def test_bands_are_5_cent(self):
        conds = _make_conditions(200)
        result = build_calibration_from_conditions(conds)
        for band in result.bands:
            assert band.price_hi - band.price_lo == pytest.approx(0.05)

    def test_confidence_assignment(self):
        # 100+ conditions per band should be high
        conds = [
            {"date": f"2025-01-{(i%28)+1:02d}", "avg_buy_price": 0.40,
             "status": "WIN" if i % 3 != 0 else "LOSS_OR_OPEN",
             "net_cost": 10.0, "pnl": 15.0 if i % 3 != 0 else -10.0}
            for i in range(120)
        ]
        result = build_calibration_from_conditions(conds)
        band_40 = next((b for b in result.bands if b.price_lo == 0.40), None)
        assert band_40 is not None
        assert band_40.confidence == "high"


class TestWalkForwardSplit:

    def test_empty_conditions(self):
        splits = walk_forward_split([])
        assert splits == []

    def test_insufficient_data(self):
        """Not enough months for even one split."""
        conds = _make_conditions(30, start_month="2025-01")
        splits = walk_forward_split(conds, train_months=6, test_months=2)
        assert splits == []

    def test_generates_splits(self):
        """With enough months, should generate at least one split."""
        conds = _make_conditions(300, start_month="2025-01")
        splits = walk_forward_split(
            conds, train_months=3, test_months=1, step_months=1
        )
        assert len(splits) > 0

        for train_result, test_conds in splits:
            assert isinstance(train_result, CalibrationBuildResult)
            assert len(test_conds) > 0
            assert train_result.train_start < train_result.train_end

    def test_no_overlap(self):
        """Test conditions should not overlap with training period."""
        conds = _make_conditions(300, start_month="2025-01")
        splits = walk_forward_split(
            conds, train_months=3, test_months=1, step_months=1
        )
        for train_result, test_conds in splits:
            for c in test_conds:
                assert c["date"] >= train_result.train_end


class TestEvaluateSplit:

    def test_evaluate_basic(self):
        conds = _make_conditions(200, start_month="2025-01")
        splits = walk_forward_split(
            conds, train_months=3, test_months=1, step_months=1
        )
        if not splits:
            pytest.skip("Not enough data for splits")

        train_result, test_conds = splits[0]
        ev = evaluate_split(train_result, test_conds)

        assert "expected_pnl" in ev
        assert "realized_pnl" in ev
        assert "gap_usd" in ev
        assert "gap_pct" in ev
        assert "n_signals" in ev
        assert ev["n_signals"] >= 0

    def test_evaluate_empty_test(self):
        conds = _make_conditions(100)
        result = build_calibration_from_conditions(conds)
        ev = evaluate_split(result, [])
        assert ev["n_signals"] == 0
        assert ev["expected_pnl"] == 0.0
        assert ev["realized_pnl"] == 0.0


class TestLoadCalibrationTable:

    def test_default_returns_hardcoded(self):
        table = load_calibration_table()
        assert len(table) > 0
        assert table[0].price_lo == 0.20

    def test_nonexistent_file_returns_default(self):
        table = load_calibration_table("/nonexistent/path.json")
        assert len(table) > 0

    def test_load_from_json(self, tmp_path):
        import json

        data = [
            {
                "price_lo": 0.30,
                "price_hi": 0.35,
                "expected_win_rate": 0.85,
                "historical_roi_pct": 20.0,
                "sample_size": 50,
                "confidence": "medium",
            }
        ]
        p = tmp_path / "custom_cal.json"
        p.write_text(json.dumps(data))

        table = load_calibration_table(str(p))
        assert len(table) == 1
        assert table[0].price_lo == 0.30
        assert table[0].expected_win_rate == 0.85

    def test_invalid_json_returns_default(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text("not valid json {")
        table = load_calibration_table(str(p))
        assert len(table) > 0  # falls back to hardcoded
