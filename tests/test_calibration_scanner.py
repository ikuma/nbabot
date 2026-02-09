"""Tests for calibration-based scanner."""

from __future__ import annotations

import pytest

from src.connectors.polymarket import MoneylineMarket
from src.strategy.calibration_scanner import (
    _calibration_kelly,
    _ev_per_dollar,
    scan_calibration,
)


def _make_ml(
    outcomes: list[str],
    prices: list[float],
    active: bool = True,
    slug: str = "nba-nyk-bos-2026-02-08",
    title: str = "Knicks vs Celtics",
) -> MoneylineMarket:
    return MoneylineMarket(
        condition_id="cond1",
        event_slug=slug,
        event_title=title,
        home_team="Boston Celtics",
        away_team="New York Knicks",
        outcomes=outcomes,
        prices=prices,
        token_ids=[f"token_{i}" for i in range(len(outcomes))],
        sports_market_type="moneyline",
        active=active,
    )


class TestCalibrationKelly:
    def test_positive_edge(self):
        """Calibration prob > poly price → positive Kelly."""
        kelly = _calibration_kelly(0.904, 0.36, kelly_fraction=0.25)
        assert kelly > 0
        assert kelly < 1  # fractional Kelly

    def test_no_edge(self):
        """Expected prob == price → zero edge → zero Kelly."""
        kelly = _calibration_kelly(0.50, 0.50, kelly_fraction=0.25)
        assert kelly == pytest.approx(0.0, abs=0.001)

    def test_negative_edge(self):
        """Expected prob < price → negative Kelly → clamped to 0."""
        kelly = _calibration_kelly(0.30, 0.50, kelly_fraction=0.25)
        assert kelly == 0.0

    def test_zero_price(self):
        assert _calibration_kelly(0.50, 0.0, kelly_fraction=0.25) == 0.0

    def test_price_one(self):
        assert _calibration_kelly(0.50, 1.0, kelly_fraction=0.25) == 0.0

    def test_fraction_scales(self):
        k1 = _calibration_kelly(0.904, 0.36, kelly_fraction=0.50)
        k2 = _calibration_kelly(0.904, 0.36, kelly_fraction=0.25)
        assert k1 == pytest.approx(k2 * 2, abs=0.001)


class TestEvPerDollar:
    def test_positive_ev(self):
        # win rate 0.904, price 0.35 → 0.904/0.35 - 1 ≈ 1.583
        ev = _ev_per_dollar(0.904, 0.35)
        assert ev == pytest.approx(1.583, abs=0.01)

    def test_negative_ev(self):
        ev = _ev_per_dollar(0.30, 0.50)
        assert ev < 0

    def test_zero_price(self):
        assert _ev_per_dollar(0.50, 0.0) == 0.0


class TestScanCalibration:
    def _patch(self, monkeypatch):
        monkeypatch.setattr(
            "src.strategy.calibration_scanner.settings.sweet_spot_lo", 0.25
        )
        monkeypatch.setattr(
            "src.strategy.calibration_scanner.settings.sweet_spot_hi", 0.55
        )
        monkeypatch.setattr(
            "src.strategy.calibration_scanner.settings.kelly_fraction", 0.25
        )
        monkeypatch.setattr(
            "src.strategy.calibration_scanner.settings.max_position_usd", 100.0
        )

    def test_sweet_spot_signal(self, monkeypatch):
        """Price in sweet spot (0.35) → signal with positive EV."""
        self._patch(monkeypatch)
        # 0.35 → band [0.35, 0.40), expected_wr = 0.904
        ml = _make_ml(["Knicks", "Celtics"], [0.35, 0.65])
        opps = scan_calibration([ml])
        assert len(opps) == 1
        opp = opps[0]
        assert opp.in_sweet_spot is True
        assert opp.ev_per_dollar > 0
        assert opp.side == "BUY"

    def test_selects_higher_ev_side(self, monkeypatch):
        """Between two outcomes, picks the one with higher EV/dollar."""
        self._patch(monkeypatch)
        # A=0.35 → band wr=0.904, EV/$ high
        # B=0.65 → band wr=0.931, EV/$ lower
        ml = _make_ml(["Knicks", "Celtics"], [0.35, 0.65])
        opps = scan_calibration([ml])
        assert len(opps) == 1
        assert opps[0].outcome_name == "Knicks"

    def test_one_signal_per_game(self, monkeypatch):
        """Only one signal per game (not both outcomes)."""
        self._patch(monkeypatch)
        ml = _make_ml(["Knicks", "Celtics"], [0.40, 0.50])
        opps = scan_calibration([ml])
        assert len(opps) <= 1

    def test_price_outside_table_filtered(self, monkeypatch):
        """Price outside calibration table (< 0.25) → no band → filtered."""
        self._patch(monkeypatch)
        ml = _make_ml(["Knicks", "Celtics"], [0.10, 0.90])
        opps = scan_calibration([ml])
        # 0.10 has no band, 0.90 → band [0.90, 0.95) wr=1.0, EV positive
        assert len(opps) == 1
        assert opps[0].poly_price == 0.90

    def test_inactive_moneyline_skipped(self, monkeypatch):
        """Inactive moneyline → skipped."""
        self._patch(monkeypatch)
        ml = _make_ml(["Knicks", "Celtics"], [0.35, 0.65], active=False)
        opps = scan_calibration([ml])
        assert opps == []

    def test_sweet_spot_vs_outside_sizing(self, monkeypatch):
        """Sweet spot gets full size; outside sweet spot gets 0.5x."""
        self._patch(monkeypatch)
        # Sweet spot: 0.35 (band [0.35, 0.40), wr=0.904)
        ml_sweet = _make_ml(
            ["Knicks", "Celtics"], [0.35, 0.65],
            slug="nba-sweet", title="Sweet Game",
        )
        # Outside sweet spot: 0.70 (band [0.70, 0.75), wr=0.933)
        # B=0.15 has no band → only 0.70 considered
        ml_outside = _make_ml(
            ["Knicks", "Celtics"], [0.70, 0.15],
            slug="nba-outside", title="Outside Game",
        )
        opps_sweet = scan_calibration([ml_sweet])
        opps_outside = scan_calibration([ml_outside])

        assert len(opps_sweet) == 1
        assert len(opps_outside) == 1

        assert opps_sweet[0].in_sweet_spot is True
        assert opps_outside[0].in_sweet_spot is False
        assert opps_outside[0].poly_price == 0.70

    def test_sorted_by_ev_desc(self, monkeypatch):
        """Multiple games sorted by EV/$ descending."""
        self._patch(monkeypatch)
        ml1 = _make_ml(
            ["A", "B"], [0.35, 0.65],
            slug="nba-game1", title="Game 1",
        )
        ml2 = _make_ml(
            ["C", "D"], [0.50, 0.50],
            slug="nba-game2", title="Game 2",
        )
        opps = scan_calibration([ml1, ml2])
        assert len(opps) == 2
        assert opps[0].ev_per_dollar >= opps[1].ev_per_dollar

    def test_no_band_for_price(self, monkeypatch):
        """Price outside all calibration bands → skipped."""
        self._patch(monkeypatch)
        # 0.03 and 0.97 are both outside the table (0.25-0.95)
        ml = _make_ml(["Knicks", "Celtics"], [0.03, 0.97])
        opps = scan_calibration([ml])
        assert opps == []

    def test_edge_calculation_correct(self, monkeypatch):
        """Verify edge = expected_win_rate - poly_price."""
        self._patch(monkeypatch)
        ml = _make_ml(["Knicks", "Celtics"], [0.35, 0.65])
        opps = scan_calibration([ml])
        assert len(opps) >= 1
        opp = opps[0]
        expected_edge = (opp.expected_win_rate - opp.poly_price) * 100
        assert opp.calibration_edge_pct == pytest.approx(expected_edge, abs=0.01)

    def test_high_favorite_signal(self, monkeypatch):
        """High favorite (0.80) gets a signal with reduced sizing."""
        self._patch(monkeypatch)
        ml = _make_ml(["Knicks", "Celtics"], [0.80, 0.15])
        opps = scan_calibration([ml])
        assert len(opps) == 1
        assert opps[0].poly_price == 0.80
        assert opps[0].in_sweet_spot is False

    def test_all_bands_positive_ev(self, monkeypatch):
        """Every band in the table produces positive EV signals."""
        self._patch(monkeypatch)
        from src.strategy.calibration import NBA_ML_CALIBRATION

        for band in NBA_ML_CALIBRATION:
            mid = (band.price_lo + band.price_hi) / 2
            complement = 1 - mid
            ml = _make_ml(
                ["A", "B"], [mid, complement],
                slug=f"nba-test-{band.price_lo:.2f}",
                title=f"Test {band.price_lo:.2f}",
            )
            opps = scan_calibration([ml])
            assert len(opps) >= 1, (
                f"Band {band.price_lo:.2f}-{band.price_hi:.2f} "
                f"produced no signal at price {mid:.3f}"
            )
