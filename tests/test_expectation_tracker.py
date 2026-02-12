"""Tests for expectation_tracker (Phase S)."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.analysis.expectation_tracker import (
    ExpectationGap,
    compute_expectation_gaps,
    format_expectation_report,
)


# Lightweight stand-ins matching ResultRecord / SignalRecord fields
@dataclass
class _Result:
    pnl: float
    settled_at: str


@dataclass
class _Signal:
    expected_win_rate: float | None
    poly_price: float
    kelly_size: float


def _pair(
    pnl: float,
    settled: str,
    wr: float | None,
    price: float,
    size: float,
) -> tuple:
    return (_Result(pnl=pnl, settled_at=settled), _Signal(wr, price, size))


class TestComputeExpectationGaps:

    def test_empty_input(self):
        gaps = compute_expectation_gaps([])
        assert gaps == []

    def test_single_signal_monthly(self):
        # wr=0.90, price=0.40 → ev_per_dollar = 0.90/0.40 - 1 = 1.25
        # expected_pnl = 1.25 * 50 = 62.5
        pairs = [_pair(pnl=75.0, settled="2026-01-15 12:00:00", wr=0.90, price=0.40, size=50.0)]
        gaps = compute_expectation_gaps(pairs, period="monthly")
        assert len(gaps) == 1
        g = gaps[0]
        assert g.period == "2026-01"
        assert g.expected_pnl == 62.5
        assert g.realized_pnl == 75.0
        assert g.gap_usd == 12.5
        assert g.gap_pct == pytest.approx(20.0, abs=0.1)
        assert g.n_signals == 1

    def test_multiple_months(self):
        pairs = [
            _pair(pnl=10.0, settled="2026-01-05 10:00:00", wr=0.80, price=0.40, size=25.0),
            _pair(pnl=-5.0, settled="2026-01-20 10:00:00", wr=0.80, price=0.40, size=25.0),
            _pair(pnl=20.0, settled="2026-02-10 10:00:00", wr=0.80, price=0.40, size=25.0),
        ]
        gaps = compute_expectation_gaps(pairs, period="monthly")
        assert len(gaps) == 2
        assert gaps[0].period == "2026-01"
        assert gaps[0].n_signals == 2
        assert gaps[1].period == "2026-02"
        assert gaps[1].n_signals == 1

    def test_weekly_mode(self):
        pairs = [
            _pair(pnl=10.0, settled="2026-01-05 10:00:00", wr=0.80, price=0.40, size=25.0),
            _pair(pnl=20.0, settled="2026-01-12 10:00:00", wr=0.80, price=0.40, size=25.0),
        ]
        gaps = compute_expectation_gaps(pairs, period="weekly")
        assert len(gaps) >= 1
        # 別の週に分かれるはず
        assert all(g.period.startswith("2026-W") for g in gaps)

    def test_skips_zero_price(self):
        pairs = [_pair(pnl=10.0, settled="2026-01-05 10:00:00", wr=0.80, price=0.0, size=25.0)]
        gaps = compute_expectation_gaps(pairs)
        assert gaps == []

    def test_skips_none_win_rate(self):
        pairs = [_pair(pnl=10.0, settled="2026-01-05 10:00:00", wr=None, price=0.40, size=25.0)]
        gaps = compute_expectation_gaps(pairs)
        assert gaps == []

    def test_skips_negative_ev(self):
        # wr=0.30, price=0.50 → ev = 0.30/0.50 - 1 = -0.4 → skip
        pairs = [_pair(pnl=-10.0, settled="2026-01-05 10:00:00", wr=0.30, price=0.50, size=25.0)]
        gaps = compute_expectation_gaps(pairs)
        assert gaps == []

    def test_aggregation_accuracy(self):
        # 2 signals in same month, both with ev > 0
        # wr=0.80, price=0.40 → ev/$ = 1.0; expected_pnl = 1.0 * 25 = 25 each
        pairs = [
            _pair(pnl=30.0, settled="2026-01-10 10:00:00", wr=0.80, price=0.40, size=25.0),
            _pair(pnl=15.0, settled="2026-01-20 10:00:00", wr=0.80, price=0.40, size=25.0),
        ]
        gaps = compute_expectation_gaps(pairs)
        assert len(gaps) == 1
        g = gaps[0]
        assert g.expected_pnl == 50.0  # 25 + 25
        assert g.realized_pnl == 45.0  # 30 + 15
        assert g.gap_usd == -5.0
        assert g.gap_pct == pytest.approx(-10.0, abs=0.1)


class TestFormatExpectationReport:

    def test_empty_gaps(self):
        lines = format_expectation_report([])
        assert lines == []

    def test_basic_format(self):
        gaps = [
            ExpectationGap("2026-01", 100.0, 120.0, 20.0, 20.0, 10),
            ExpectationGap("2026-02", 80.0, 60.0, -20.0, -25.0, 8),
        ]
        lines = format_expectation_report(gaps)
        text = "\n".join(lines)
        assert "Expected vs Realized PnL" in text
        assert "2026-01" in text
        assert "2026-02" in text
        assert "WARNING" not in text  # only 2 periods, no warning

    def test_decay_warning(self):
        # 3 consecutive periods with worsening gap_pct, latest < -10%
        gaps = [
            ExpectationGap("2026-01", 100.0, 95.0, -5.0, -5.0, 10),
            ExpectationGap("2026-02", 100.0, 80.0, -20.0, -20.0, 10),
            ExpectationGap("2026-03", 100.0, 60.0, -40.0, -40.0, 10),
        ]
        lines = format_expectation_report(gaps)
        text = "\n".join(lines)
        assert "WARNING" in text
        assert "decaying" in text

    def test_no_warning_when_improving(self):
        # 3 periods but gap is improving
        gaps = [
            ExpectationGap("2026-01", 100.0, 60.0, -40.0, -40.0, 10),
            ExpectationGap("2026-02", 100.0, 80.0, -20.0, -20.0, 10),
            ExpectationGap("2026-03", 100.0, 95.0, -5.0, -5.0, 10),
        ]
        lines = format_expectation_report(gaps)
        text = "\n".join(lines)
        assert "WARNING" not in text

    def test_no_warning_small_gap(self):
        # Worsening but latest gap > -10%, no warning
        gaps = [
            ExpectationGap("2026-01", 100.0, 99.0, -1.0, -1.0, 10),
            ExpectationGap("2026-02", 100.0, 97.0, -3.0, -3.0, 10),
            ExpectationGap("2026-03", 100.0, 95.0, -5.0, -5.0, 10),
        ]
        lines = format_expectation_report(gaps)
        text = "\n".join(lines)
        assert "WARNING" not in text


class TestReportGeneratorIntegration:
    """Test that report_generator accepts expectation_gaps parameter."""

    def test_report_with_gaps(self):
        from src.analysis.report_generator import generate_report

        gaps = [
            ExpectationGap("2026-01", 100.0, 120.0, 20.0, 20.0, 10),
        ]
        # Minimal conditions/games to avoid errors
        report = generate_report(
            conditions={},
            games=[],
            trader_name="Test",
            expectation_gaps=gaps,
        )
        assert "Expected vs Realized PnL" in report
        assert "2026-01" in report

    def test_report_without_gaps(self):
        from src.analysis.report_generator import generate_report

        report = generate_report(
            conditions={},
            games=[],
            trader_name="Test",
        )
        assert "Expected vs Realized PnL" not in report
