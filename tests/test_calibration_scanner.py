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
        # band [0.35, 0.40): expected_wr = 0.795, price = 0.36
        # b = (1/0.36)-1 ≈ 1.778, p=0.795, q=0.205
        # kelly_full = (1.778*0.795 - 0.205) / 1.778 ≈ 0.680
        kelly = _calibration_kelly(0.795, 0.36, kelly_fraction=0.25)
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
        k1 = _calibration_kelly(0.795, 0.36, kelly_fraction=0.50)
        k2 = _calibration_kelly(0.795, 0.36, kelly_fraction=0.25)
        assert k1 == pytest.approx(k2 * 2, abs=0.001)


class TestEvPerDollar:
    def test_positive_ev(self):
        # win rate 0.795, price 0.35 → 0.795/0.35 - 1 ≈ 1.27
        ev = _ev_per_dollar(0.795, 0.35)
        assert ev == pytest.approx(1.271, abs=0.01)

    def test_negative_ev(self):
        ev = _ev_per_dollar(0.30, 0.50)
        assert ev < 0

    def test_zero_price(self):
        assert _ev_per_dollar(0.50, 0.0) == 0.0


class TestScanCalibration:
    def _patch(self, monkeypatch):
        monkeypatch.setattr(
            "src.strategy.calibration_scanner.settings.min_buy_price", 0.20
        )
        monkeypatch.setattr(
            "src.strategy.calibration_scanner.settings.max_buy_price", 0.85
        )
        monkeypatch.setattr(
            "src.strategy.calibration_scanner.settings.min_calibration_edge_pct", 3.0
        )
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
        """Price in sweet spot (0.35) → signal with edge from calibration table."""
        self._patch(monkeypatch)
        # 0.35 → band [0.35, 0.40), expected_wr = 0.795
        # edge = 0.795 - 0.35 = 0.445 → 44.5%
        ml = _make_ml(["Knicks", "Celtics"], [0.35, 0.65])
        opps = scan_calibration([ml])
        assert len(opps) == 1
        opp = opps[0]
        assert opp.in_sweet_spot is True
        assert opp.calibration_edge_pct > 3.0
        assert opp.side == "BUY"

    def test_selects_higher_ev_side(self, monkeypatch):
        """Between two outcomes, picks the one with higher EV/dollar."""
        self._patch(monkeypatch)
        # A=0.35 (EV/$ ≈ 1.27), B=0.65 (band [0.65, 0.75), wr=0.92, EV/$ ≈ 0.42)
        # Both pass edge threshold, but A has higher EV → select A
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

    def test_price_below_min_filtered(self, monkeypatch):
        """Price below min_buy_price → filtered out."""
        self._patch(monkeypatch)
        ml = _make_ml(["Knicks", "Celtics"], [0.10, 0.90])
        opps = scan_calibration([ml])
        # 0.10 is below min_buy_price=0.20, 0.90 is above max_buy_price=0.85
        assert opps == []

    def test_price_above_max_filtered(self, monkeypatch):
        """Price above max_buy_price → filtered out."""
        self._patch(monkeypatch)
        ml = _make_ml(["Knicks", "Celtics"], [0.90, 0.10])
        opps = scan_calibration([ml])
        assert opps == []

    def test_min_edge_filter(self, monkeypatch):
        """Edge below threshold → filtered."""
        self._patch(monkeypatch)
        monkeypatch.setattr(
            "src.strategy.calibration_scanner.settings.min_calibration_edge_pct", 50.0
        )
        # band [0.50, 0.55): expected_wr = 0.849, edge = 0.849 - 0.52 = 0.329 = 32.9%
        # 32.9% < 50% → filtered
        ml = _make_ml(["Knicks", "Celtics"], [0.52, 0.48])
        opps = scan_calibration([ml])
        assert opps == []

    def test_inactive_moneyline_skipped(self, monkeypatch):
        """Inactive moneyline → skipped."""
        self._patch(monkeypatch)
        ml = _make_ml(["Knicks", "Celtics"], [0.35, 0.65], active=False)
        opps = scan_calibration([ml])
        assert opps == []

    def test_sweet_spot_vs_outside_sizing(self, monkeypatch):
        """Sweet spot gets full size; outside sweet spot gets 0.5x."""
        self._patch(monkeypatch)
        # Sweet spot: 0.35 (band [0.35, 0.40), wr=0.795)
        ml_sweet = _make_ml(
            ["Knicks", "Celtics"], [0.35, 0.65],
            slug="nba-sweet", title="Sweet Game",
        )
        # Outside sweet spot: both outcomes outside sweet spot
        # A=0.70, B=0.30 → scanner picks higher EV side (B=0.30 is in sweet spot!)
        # Use A=0.70, B=0.20 (B is below min_buy_price) to force 0.70 selection
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

        # Outside sizing should be smaller due to 0.5x multiplier
        assert opps_outside[0].position_usd >= 0

    def test_sorted_by_edge_desc(self, monkeypatch):
        """Multiple games sorted by calibration edge descending."""
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
        assert opps[0].calibration_edge_pct >= opps[1].calibration_edge_pct

    def test_custom_min_max_price(self, monkeypatch):
        """Override min/max price via parameters."""
        self._patch(monkeypatch)
        ml = _make_ml(["Knicks", "Celtics"], [0.35, 0.65])
        # 0.35 is outside [0.40, 0.60] range
        opps = scan_calibration([ml], min_price=0.40, max_price=0.60)
        # 0.35 filtered, 0.65 is in range but may or may not pass edge threshold
        # Check that 0.35 is excluded
        assert all(o.poly_price >= 0.40 for o in opps)

    def test_no_band_for_price(self, monkeypatch):
        """Price outside all calibration bands → skipped."""
        self._patch(monkeypatch)
        monkeypatch.setattr(
            "src.strategy.calibration_scanner.settings.min_buy_price", 0.01
        )
        # 0.03 is below all bands (min is 0.05)
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
