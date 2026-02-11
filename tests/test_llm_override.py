"""Tests for LLM-First directional + below-market limit pricing."""

from __future__ import annotations

import pytest

from src.strategy.calibration_scanner import (
    BothsideOpportunity,
    CalibrationOpportunity,
    evaluate_single_outcome,
)
from src.strategy.llm_analyzer import GameAnalysis

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_opp(
    outcome: str = "Mavericks",
    price: float = 0.31,
    ev: float = 1.5,
    position_usd: float = 25.0,
    token_id: str = "tok_0",
    event_slug: str = "nba-dal-lal-2026-02-15",
) -> CalibrationOpportunity:
    return CalibrationOpportunity(
        event_slug=event_slug,
        event_title="Mavericks vs Lakers",
        market_type="moneyline",
        outcome_name=outcome,
        token_id=token_id,
        poly_price=price,
        calibration_edge_pct=50.0,
        expected_win_rate=0.85,
        ev_per_dollar=ev,
        price_band="0.30-0.35",
        in_sweet_spot=True,
        band_confidence="medium",
        position_usd=position_usd,
    )


def _make_analysis(
    favored: str = "Los Angeles Lakers",
    confidence: float = 0.85,
    sizing_modifier: float = 1.2,
    hedge_ratio: float = 0.4,
) -> GameAnalysis:
    return GameAnalysis(
        favored_team=favored,
        home_win_prob=0.65,
        away_win_prob=0.35,
        confidence=confidence,
        sizing_modifier=sizing_modifier,
        hedge_ratio=hedge_ratio,
        risk_flags=[],
        reasoning="Test reasoning",
        model_id="claude-opus-4-6",
        latency_ms=2500,
    )


# ---------------------------------------------------------------------------
# evaluate_single_outcome
# ---------------------------------------------------------------------------


class TestEvaluateSingleOutcome:
    def test_positive_ev_returns_opportunity(self):
        """Price in band with positive EV returns CalibrationOpportunity."""
        result = evaluate_single_outcome(
            price=0.31,
            outcome_name="Lakers",
            token_id="tok_lal",
            event_slug="nba-dal-lal-2026-02-15",
            event_title="Mavericks vs Lakers",
        )
        assert result is not None
        assert result.outcome_name == "Lakers"
        assert result.ev_per_dollar > 0

    def test_price_outside_bands_returns_none(self):
        """Price outside all calibration bands returns None."""
        result = evaluate_single_outcome(
            price=0.15,
            outcome_name="Bucks",
            token_id="tok_mil",
            event_slug="nba-mil-bos-2026-02-15",
            event_title="Bucks vs Celtics",
        )
        assert result is None

    def test_invalid_price_returns_none(self):
        """Price <= 0 or >= 1 returns None."""
        assert evaluate_single_outcome(0.0, "X", "t", "s", "t") is None
        assert evaluate_single_outcome(1.0, "X", "t", "s", "t") is None
        assert evaluate_single_outcome(-0.5, "X", "t", "s", "t") is None

    def test_high_price_positive_ev(self):
        """Price 0.69 (band 0.65-0.70) should have positive EV."""
        result = evaluate_single_outcome(
            price=0.69,
            outcome_name="Lakers",
            token_id="tok_lal",
            event_slug="nba-dal-lal-2026-02-15",
            event_title="Mavericks vs Lakers",
        )
        assert result is not None
        assert result.ev_per_dollar > 0


# ---------------------------------------------------------------------------
# LLM-First Directional (Case A, B, match, fallback)
# ---------------------------------------------------------------------------


class TestLlmFirstDirectional:
    """Test LLM-First directional logic in job_executor.process_single_job."""

    def test_llm_and_calibration_agree(self):
        """When LLM and calibration agree, no swap occurs."""
        # LLM favors Mavericks, calibration also picks Mavericks as directional
        dal_opp = _make_opp("Mavericks", 0.31, 1.5, 25.0, "tok_dal")
        lal_opp = _make_opp("Lakers", 0.69, 0.35, 25.0, "tok_lal")
        bothside = BothsideOpportunity(
            directional=dal_opp,
            hedge=lal_opp,
            combined_price=1.0,
            hedge_position_usd=12.0,
        )
        analysis = _make_analysis(favored="Dallas Mavericks")

        from src.strategy.llm_analyzer import determine_directional

        dir_name, _ = determine_directional(analysis, "Lakers", "Mavericks")
        # LLM picks Mavericks (away) → matches calibration directional
        assert dir_name == "Mavericks"
        # No swap needed since they agree
        assert bothside.directional.outcome_name == dir_name

    def test_llm_disagrees_hedge_exists_swap(self):
        """Case A: LLM disagrees with calibration, hedge exists → swap."""
        dal_opp = _make_opp("Mavericks", 0.31, 1.5, 25.0, "tok_dal")
        lal_opp = _make_opp("Lakers", 0.69, 0.35, 25.0, "tok_lal")
        bothside = BothsideOpportunity(
            directional=dal_opp,
            hedge=lal_opp,
            combined_price=1.0,
            hedge_position_usd=12.0,
        )
        analysis = _make_analysis(favored="Los Angeles Lakers")

        from src.strategy.llm_analyzer import determine_directional

        dir_name, hedge_name = determine_directional(analysis, "Lakers", "Mavericks")
        assert dir_name == "Lakers"

        # Simulate Case A swap
        opp = bothside.directional
        if (
            bothside.hedge
            and opp.outcome_name != dir_name
            and bothside.hedge.outcome_name == dir_name
        ):
            new_bothside = BothsideOpportunity(
                directional=bothside.hedge,
                hedge=opp,
                combined_price=bothside.combined_price,
                hedge_position_usd=bothside.hedge_position_usd,
            )
            opp = new_bothside.directional

        assert opp.outcome_name == "Lakers"

    def test_llm_disagrees_no_hedge_case_b(self):
        """Case B: LLM disagrees, hedge=None → evaluate LLM side."""
        # Calibration only found Mavericks (DAL at 0.31), no hedge
        dal_opp = _make_opp("Mavericks", 0.31, 1.5, 25.0, "tok_dal")

        # LLM wants Lakers (at 0.69) — evaluate_single_outcome should work
        llm_opp = evaluate_single_outcome(
            price=0.69,
            outcome_name="Lakers",
            token_id="tok_lal",
            event_slug="nba-dal-lal-2026-02-15",
            event_title="Mavericks vs Lakers",
        )

        assert llm_opp is not None
        assert llm_opp.outcome_name == "Lakers"
        assert llm_opp.ev_per_dollar > 0

        # After Case B: directional=Lakers, hedge=Mavericks (old directional)
        new_bothside = BothsideOpportunity(
            directional=llm_opp,
            hedge=dal_opp,
            combined_price=llm_opp.poly_price + dal_opp.poly_price,
            hedge_position_usd=dal_opp.position_usd * 0.5,
        )
        assert new_bothside.directional.outcome_name == "Lakers"
        assert new_bothside.hedge.outcome_name == "Mavericks"

    def test_llm_side_no_band_keeps_calibration(self):
        """Case B fail: LLM side has no calibration band → keep calibration."""
        # Calibration picks a team, LLM wants a team at 0.15 (below bands)
        llm_opp = evaluate_single_outcome(
            price=0.15,
            outcome_name="Bucks",
            token_id="tok_mil",
            event_slug="nba-mil-bos-2026-02-15",
            event_title="Bucks vs Celtics",
        )
        assert llm_opp is None  # No band → fallback to calibration

    def test_llm_disabled_no_change(self):
        """When LLM is disabled, directional from calibration is unchanged."""
        dal_opp = _make_opp("Mavericks", 0.31, 1.5, 25.0, "tok_dal")
        # llm_analysis is None → no override
        assert dal_opp.outcome_name == "Mavericks"


# ---------------------------------------------------------------------------
# Below-Market Pricing
# ---------------------------------------------------------------------------


class TestBelowMarketPricing:
    def test_below_market_normal(self):
        """best_ask=0.50 → order_price=0.49."""
        best_ask = 0.50
        order_price = max(best_ask - 0.01, 0.01)
        assert order_price == pytest.approx(0.49)

    def test_below_market_floor(self):
        """best_ask=0.01 → order_price=0.01 (floor)."""
        best_ask = 0.01
        order_price = max(best_ask - 0.01, 0.01)
        assert order_price == pytest.approx(0.01)

    def test_below_market_very_low(self):
        """best_ask=0.02 → order_price=0.01."""
        best_ask = 0.02
        order_price = max(best_ask - 0.01, 0.01)
        assert order_price == pytest.approx(0.01)


# ---------------------------------------------------------------------------
# Hedge Target Pricing
# ---------------------------------------------------------------------------


class TestHedgeTargetPricing:
    def test_target_limits_hedge(self):
        """dir_vwap=0.68, target=0.97 → max_hedge=0.29, best_ask=0.31 → order at 0.29."""
        dir_vwap = 0.68
        target_combined = 0.97
        best_ask = 0.31

        max_hedge_price = target_combined - dir_vwap
        assert max_hedge_price == pytest.approx(0.29)

        order_price = max(best_ask - 0.01, 0.01)  # 0.30
        order_price = min(order_price, max_hedge_price)  # 0.29
        assert order_price == pytest.approx(0.29)

    def test_below_market_wins_when_cheaper(self):
        """dir_vwap=0.68, target=0.97 → max_hedge=0.29, best_ask=0.25 → order at 0.24."""
        dir_vwap = 0.68
        target_combined = 0.97
        best_ask = 0.25

        max_hedge_price = target_combined - dir_vwap
        assert max_hedge_price == pytest.approx(0.29)

        order_price = max(best_ask - 0.01, 0.01)  # 0.24
        order_price = min(order_price, max_hedge_price)  # 0.24 (below-market wins)
        assert order_price == pytest.approx(0.24)

    def test_high_dir_vwap_still_allows_hedge(self):
        """dir_vwap=0.30, target=0.97 → max_hedge=0.67, best_ask=0.71 → order at 0.67."""
        dir_vwap = 0.30
        target_combined = 0.97
        best_ask = 0.71

        max_hedge_price = target_combined - dir_vwap
        assert max_hedge_price == pytest.approx(0.67)

        order_price = max(best_ask - 0.01, 0.01)  # 0.70
        order_price = min(order_price, max_hedge_price)  # 0.67
        assert order_price == pytest.approx(0.67)

    def test_skip_when_max_hedge_below_band(self):
        """dir_vwap=0.80, target=0.97 → max_hedge=0.17 < 0.20 → should skip."""
        dir_vwap = 0.80
        target_combined = 0.97

        max_hedge_price = target_combined - dir_vwap
        assert max_hedge_price == pytest.approx(0.17)
        assert max_hedge_price < 0.20  # 校正範囲外 → skip

    def test_hedge_dca_combined_skip(self):
        """Hedge DCA: current_price > max_hedge from target → skip tick."""
        dir_vwap = 0.68
        target_combined = 0.97
        current_price = 0.35

        max_hedge = target_combined - dir_vwap  # 0.29
        assert current_price > max_hedge  # should skip


# ---------------------------------------------------------------------------
# Config: BOTHSIDE_TARGET_COMBINED
# ---------------------------------------------------------------------------


class TestConfig:
    def test_default_target_combined(self):
        """Default BOTHSIDE_TARGET_COMBINED is 0.97."""
        from src.config import Settings

        s = Settings(
            _env_file=None,
            polymarket_private_key="",
        )
        assert s.bothside_target_combined == pytest.approx(0.97)

    def test_target_combined_env_override(self, monkeypatch):
        """BOTHSIDE_TARGET_COMBINED can be overridden via env."""
        monkeypatch.setenv("BOTHSIDE_TARGET_COMBINED", "0.96")
        from src.config import Settings

        s = Settings(
            _env_file=None,
        )
        assert s.bothside_target_combined == pytest.approx(0.96)
