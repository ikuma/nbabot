"""Tests for scan_calibration_bothside (Phase B / Phase H: MERGE-first)."""

from __future__ import annotations

import pytest

from src.connectors.polymarket import MoneylineMarket
from src.strategy.calibration_scanner import scan_calibration_bothside


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


def _patch_settings(monkeypatch):
    monkeypatch.setattr("src.strategy.calibration_scanner.settings.sweet_spot_lo", 0.25)
    monkeypatch.setattr("src.strategy.calibration_scanner.settings.sweet_spot_hi", 0.55)
    monkeypatch.setattr("src.strategy.calibration_scanner.settings.kelly_fraction", 0.25)
    monkeypatch.setattr("src.strategy.calibration_scanner.settings.max_position_usd", 100.0)


class TestScanCalibrationBothside:
    def test_returns_both_sides_when_profitable(self, monkeypatch):
        """Both outcomes have positive EV → hedge is set."""
        _patch_settings(monkeypatch)
        # 0.35 → wr≈0.904, 0.50 → wr≈0.828 — both positive EV
        ml = _make_ml(["Knicks", "Celtics"], [0.35, 0.50])
        results = scan_calibration_bothside([ml])
        assert len(results) == 1
        opp = results[0]
        assert opp.directional is not None
        assert opp.hedge is not None

    def test_hedge_none_when_combined_too_high(self, monkeypatch):
        """Combined price > max_combined_vwap → hedge is None."""
        _patch_settings(monkeypatch)
        # 0.50 + 0.50 = 1.00 > 0.995
        ml = _make_ml(["Knicks", "Celtics"], [0.50, 0.50])
        results = scan_calibration_bothside([ml], max_combined_vwap=0.995)
        assert len(results) == 1
        assert results[0].hedge is None

    def test_hedge_none_when_no_ev(self, monkeypatch):
        """Only one outcome has positive EV → hedge is None."""
        _patch_settings(monkeypatch)
        # 0.35 → positive EV, 0.03 → no calibration band → no EV
        ml = _make_ml(["Knicks", "Celtics"], [0.35, 0.03])
        results = scan_calibration_bothside([ml])
        assert len(results) == 1
        assert results[0].hedge is None

    def test_directional_is_higher_ev(self, monkeypatch):
        """Directional should always be the higher EV outcome."""
        _patch_settings(monkeypatch)
        ml = _make_ml(["Knicks", "Celtics"], [0.35, 0.50])
        results = scan_calibration_bothside([ml])
        assert len(results) == 1
        opp = results[0]
        if opp.hedge is not None:
            assert opp.directional.ev_per_dollar >= opp.hedge.ev_per_dollar

    def test_inactive_skipped(self, monkeypatch):
        """Inactive market → no results."""
        _patch_settings(monkeypatch)
        ml = _make_ml(["Knicks", "Celtics"], [0.35, 0.50], active=False)
        results = scan_calibration_bothside([ml])
        assert results == []

    def test_combined_price_correct(self, monkeypatch):
        """combined_price = directional.price + hedge.price when both present."""
        _patch_settings(monkeypatch)
        ml = _make_ml(["Knicks", "Celtics"], [0.35, 0.50])
        results = scan_calibration_bothside([ml], max_combined_vwap=1.0)
        assert len(results) == 1
        opp = results[0]
        if opp.hedge is not None:
            expected_combined = opp.directional.poly_price + opp.hedge.poly_price
            assert opp.combined_price == pytest.approx(expected_combined, abs=0.001)

    def test_multiple_games(self, monkeypatch):
        """Multiple games sorted by directional EV descending."""
        _patch_settings(monkeypatch)
        ml1 = _make_ml(["A", "B"], [0.35, 0.50], slug="nba-game1", title="Game 1")
        ml2 = _make_ml(["C", "D"], [0.40, 0.45], slug="nba-game2", title="Game 2")
        results = scan_calibration_bothside([ml1, ml2])
        assert len(results) == 2
        assert results[0].directional.ev_per_dollar >= results[1].directional.ev_per_dollar

    # --- Phase H: MERGE-first tests ---

    def test_hedge_set_when_above_old_max_price(self, monkeypatch):
        """Hedge price > old 0.55 cap → still set (MERGE-first)."""
        _patch_settings(monkeypatch)
        # dir=0.35, hedge=0.60, combined=0.95 < 0.995 → hedge set
        ml = _make_ml(["Knicks", "Celtics"], [0.35, 0.60])
        results = scan_calibration_bothside([ml])
        assert results[0].hedge is not None

    def test_hedge_sizing_dynamic_margin(self, monkeypatch):
        """High margin → high multiplier."""
        _patch_settings(monkeypatch)
        # 0.35+0.50=0.85, margin=0.15 → mult=min(0.9,0.15*15)=0.9
        ml = _make_ml(["Knicks", "Celtics"], [0.35, 0.50])
        results = scan_calibration_bothside([ml])
        opp = results[0]
        assert opp.hedge is not None
        from src.strategy.calibration_scanner import _hedge_margin_multiplier

        expected = opp.hedge.position_usd * _hedge_margin_multiplier(1.0 - opp.combined_price)
        assert opp.hedge_position_usd == pytest.approx(expected, rel=0.01)

    def test_deprecated_hedge_max_price_ignored(self, monkeypatch):
        """Old hedge_max_price param is ignored."""
        _patch_settings(monkeypatch)
        ml = _make_ml(["Knicks", "Celtics"], [0.35, 0.60])
        results = scan_calibration_bothside([ml], hedge_max_price=0.30)
        assert results[0].hedge is not None  # 旧ロジックなら None

    def test_hedge_margin_multiplier_low(self, monkeypatch):
        """Low margin → low multiplier (0.3 floor)."""
        _patch_settings(monkeypatch)
        # 0.49+0.50=0.99, margin=0.01 → mult=max(0.3,0.01*15)=0.3
        ml = _make_ml(["Knicks", "Celtics"], [0.49, 0.50])
        results = scan_calibration_bothside([ml], max_combined_vwap=1.0)
        opp = results[0]
        assert opp.hedge is not None
        from src.strategy.calibration_scanner import _hedge_margin_multiplier

        assert _hedge_margin_multiplier(0.01) == pytest.approx(0.3)
        expected = opp.hedge.position_usd * 0.3
        assert opp.hedge_position_usd == pytest.approx(expected, rel=0.01)
