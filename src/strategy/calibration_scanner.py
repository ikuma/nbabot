"""Calibration-based scanner: exploits Polymarket's systematic mispricing.

Instead of comparing Polymarket vs bookmaker consensus, this scanner uses
historical calibration data (from lhtsports analysis) to identify outcomes
where Polymarket underprices the true win probability.

Side selection: for each game, compute EV per dollar for both outcomes
and select the one with higher EV. This naturally favours underdogs due
to the concave shape of the calibration curve.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from src.config import settings
from src.connectors.polymarket import MoneylineMarket
from src.sizing.liquidity import LiquiditySnapshot
from src.sizing.position_sizer import calculate_position_size
from src.strategy.calibration import is_in_sweet_spot, lookup_band
from src.strategy.calibration_curve import (
    WinRateEstimate,
    _confidence_from_sample_size,
    get_default_curve,
)

logger = logging.getLogger(__name__)


@dataclass
class CalibrationOpportunity:
    """A calibration-based trading signal."""

    event_slug: str
    event_title: str
    market_type: str  # "moneyline" | "total"
    outcome_name: str  # "Celtics" or "Over 220.5"
    token_id: str
    poly_price: float
    calibration_edge_pct: float  # (expected_win_rate - poly_price) * 100
    expected_win_rate: float
    ev_per_dollar: float  # w(p) / p - 1
    price_band: str  # "0.25-0.30"
    in_sweet_spot: bool
    band_confidence: str
    position_usd: float
    side: str = "BUY"
    book_prob: float | None = None  # optional bookmaker validation
    # Continuous curve diagnostic (Phase Q)
    point_estimate_wr: float | None = None
    # Liquidity-aware sizing fields
    liquidity_score: str = "unknown"
    constraint_binding: str = "kelly"
    recommended_execution: str = "immediate"


def _calibration_kelly(
    expected_prob: float,
    poly_price: float,
    kelly_fraction: float | None = None,
) -> float:
    """Kelly sizing using calibration expected_prob as true probability.

    b = decimal odds from Polymarket price = (1/poly_price) - 1
    p = calibration expected win rate
    """
    if poly_price <= 0 or poly_price >= 1:
        return 0.0
    b = (1 / poly_price) - 1
    if b <= 0:
        return 0.0
    p = expected_prob
    q = 1 - p
    kelly_full = (b * p - q) / b
    frac = kelly_fraction if kelly_fraction is not None else settings.kelly_fraction
    return max(0.0, kelly_full) * frac


def _confidence_multiplier(est: WinRateEstimate) -> float:
    """Continuous confidence scaling based on CI width.

    Replaces the hard sweet-spot boundary (0.5x at price > 0.55).
    Uses lower_bound / point_estimate ratio: tighter CI → higher multiplier.
    """
    if est.point_estimate <= 0:
        return 0.5
    ratio = est.lower_bound / est.point_estimate
    # Clip to [0.5, 1.0] — 0.5 matches old minimum
    return max(0.5, min(1.0, ratio))


def _hedge_margin_multiplier(merge_margin: float) -> float:
    """Scale hedge multiplier based on MERGE margin.

    Higher margin → more aggressive hedge (maximize MERGE profit).
    Linear: margin * 15, clamped to [0.3, 0.9].
    Examples: margin=0.02→0.3, 0.03→0.45, 0.05→0.75, 0.06+→0.9
    """
    return min(0.9, max(0.3, merge_margin * 15))


def _ev_per_dollar(expected_win_rate: float, price: float) -> float:
    """Expected value per dollar: w(p) / p - 1."""
    if price <= 0:
        return 0.0
    return expected_win_rate / price - 1


def scan_calibration(
    moneylines: list[MoneylineMarket],
    balance_usd: float | None = None,
    liquidity_map: dict[str, LiquiditySnapshot] | None = None,
) -> list[CalibrationOpportunity]:
    """Calibration-based scan. No bookmaker odds required.

    For each game:
      1. Compute calibration EV for both outcomes
      2. Require calibration band exists (table coverage: 0.25-0.95)
      3. Require positive EV (expected_win_rate > poly_price)
      4. Select the outcome with higher EV (one signal per game)
      5. Kelly sizing with continuous confidence multiplier (CI-based)
      6. Apply 3-layer constraints (kelly, capital, liquidity) if provided
    """
    _curve = get_default_curve()
    opportunities: list[CalibrationOpportunity] = []

    for ml in moneylines:
        if not ml.active:
            continue

        # 各アウトカムの候補を評価
        best: CalibrationOpportunity | None = None
        best_ev: float = -999.0

        for i, outcome_name in enumerate(ml.outcomes):
            if i >= len(ml.prices) or i >= len(ml.token_ids):
                continue

            price = ml.prices[i]

            if price <= 0 or price >= 1:
                logger.debug("Skipping %s: invalid price %.3f", outcome_name, price)
                continue

            est = _curve.estimate(price)
            if est is None:
                logger.debug("No calibration band for %s @ %.3f", outcome_name, price)
                continue

            # 保守的推定 (Beta 下限) を使用
            expected_wr = est.lower_bound
            point_wr = est.point_estimate
            edge = expected_wr - price  # raw edge (0-1 scale)
            edge_pct = edge * 100

            ev = _ev_per_dollar(expected_wr, price)

            # 正の EV のみ
            if ev <= 0:
                logger.debug(
                    "Non-positive EV for %s: %.3f (wr=%.3f, price=%.3f)",
                    outcome_name,
                    ev,
                    expected_wr,
                    price,
                )
                continue

            # Kelly sizing with continuous confidence scaling
            kelly = _calibration_kelly(expected_wr, price)
            confidence_mult = _confidence_multiplier(est)
            kelly *= confidence_mult

            # メタデータ用 (DB 互換 — Kelly サイジングからは分離)
            sweet = is_in_sweet_spot(price, settings.sweet_spot_lo, settings.sweet_spot_hi)

            kelly_usd = min(kelly * settings.max_position_usd * 10, settings.max_position_usd)

            # メタデータは既存バンドから (DB 互換)
            band = lookup_band(price)
            band_label = f"{band.price_lo:.2f}-{band.price_hi:.2f}" if band else f"{price:.2f}"
            band_confidence = _confidence_from_sample_size(est.effective_sample_size)

            # 3層制約の適用
            liq_score = "unknown"
            binding = "kelly"
            rec_exec = "immediate"
            position_usd = kelly_usd

            token_id = ml.token_ids[i]
            liq = liquidity_map.get(token_id) if liquidity_map else None

            if balance_usd is not None or liq is not None:
                sizing = calculate_position_size(
                    kelly_usd=kelly_usd,
                    balance_usd=balance_usd,
                    liquidity=liq,
                    max_position_usd=settings.max_position_usd,
                    capital_risk_pct=settings.capital_risk_pct,
                    liquidity_fill_pct=settings.liquidity_fill_pct,
                    max_spread_pct=settings.max_spread_pct,
                )
                position_usd = sizing.final_size_usd
                liq_score = sizing.liquidity_score
                binding = sizing.constraint_binding
                rec_exec = sizing.recommended_execution

                if rec_exec == "skip":
                    logger.info(
                        "Skipping %s: %s (spread=%.1f%%)",
                        outcome_name,
                        liq_score,
                        liq.spread_pct if liq else 0,
                    )
                    continue

            candidate = CalibrationOpportunity(
                event_slug=ml.event_slug,
                event_title=ml.event_title,
                market_type="moneyline",
                outcome_name=outcome_name,
                token_id=token_id,
                poly_price=price,
                calibration_edge_pct=edge_pct,
                expected_win_rate=expected_wr,
                ev_per_dollar=ev,
                price_band=band_label,
                in_sweet_spot=sweet,
                band_confidence=band_confidence,
                position_usd=position_usd,
                point_estimate_wr=point_wr,
                liquidity_score=liq_score,
                constraint_binding=binding,
                recommended_execution=rec_exec,
            )

            # 1 試合 1 シグナル: EV が最も高いアウトカムを選択
            if ev > best_ev:
                best_ev = ev
                best = candidate

        if best is not None:
            opportunities.append(best)

    opportunities.sort(key=lambda o: o.ev_per_dollar, reverse=True)
    return opportunities


# ---------------------------------------------------------------------------
# Single-outcome evaluation (LLM-First directional Case B)
# ---------------------------------------------------------------------------


def evaluate_single_outcome(
    price: float,
    outcome_name: str,
    token_id: str,
    event_slug: str,
    event_title: str,
    balance_usd: float | None = None,
    liquidity: LiquiditySnapshot | None = None,
) -> CalibrationOpportunity | None:
    """Evaluate a single outcome at given price.

    Returns CalibrationOpportunity if the outcome has a positive EV band,
    or None if no band or non-positive EV.
    Used by LLM-First directional (Case B) when LLM recommends a side
    that the bothside scanner didn't return as hedge.
    """
    if price <= 0 or price >= 1:
        return None

    _curve = get_default_curve()
    est = _curve.estimate(price)
    if est is None:
        return None

    expected_wr = est.lower_bound
    point_wr = est.point_estimate
    ev = _ev_per_dollar(expected_wr, price)
    if ev <= 0:
        return None

    kelly = _calibration_kelly(expected_wr, price)
    confidence_mult = _confidence_multiplier(est)
    kelly *= confidence_mult

    sweet = is_in_sweet_spot(price, settings.sweet_spot_lo, settings.sweet_spot_hi)

    kelly_usd = min(kelly * settings.max_position_usd * 10, settings.max_position_usd)
    edge_pct = (expected_wr - price) * 100

    band = lookup_band(price)
    band_label = f"{band.price_lo:.2f}-{band.price_hi:.2f}" if band else f"{price:.2f}"
    band_confidence = _confidence_from_sample_size(est.effective_sample_size)

    # 3層制約
    liq_score = "unknown"
    binding = "kelly"
    rec_exec = "immediate"
    position_usd = kelly_usd

    if balance_usd is not None or liquidity is not None:
        sizing = calculate_position_size(
            kelly_usd=kelly_usd,
            balance_usd=balance_usd,
            liquidity=liquidity,
            max_position_usd=settings.max_position_usd,
            capital_risk_pct=settings.capital_risk_pct,
            liquidity_fill_pct=settings.liquidity_fill_pct,
            max_spread_pct=settings.max_spread_pct,
        )
        position_usd = sizing.final_size_usd
        liq_score = sizing.liquidity_score
        binding = sizing.constraint_binding
        rec_exec = sizing.recommended_execution
        if rec_exec == "skip":
            return None

    return CalibrationOpportunity(
        event_slug=event_slug,
        event_title=event_title,
        market_type="moneyline",
        outcome_name=outcome_name,
        token_id=token_id,
        poly_price=price,
        calibration_edge_pct=edge_pct,
        expected_win_rate=expected_wr,
        ev_per_dollar=ev,
        price_band=band_label,
        in_sweet_spot=sweet,
        band_confidence=band_confidence,
        position_usd=position_usd,
        point_estimate_wr=point_wr,
        liquidity_score=liq_score,
        constraint_binding=binding,
        recommended_execution=rec_exec,
    )


# ---------------------------------------------------------------------------
# Both-side betting (Phase B)
# ---------------------------------------------------------------------------


@dataclass
class BothsideOpportunity:
    """A both-side betting opportunity for a single game."""

    directional: CalibrationOpportunity  # 高 EV 側
    hedge: CalibrationOpportunity | None  # 低 EV 側 (条件不通過なら None)
    combined_price: float  # directional.price + hedge.price (簡易 combined VWAP)
    hedge_position_usd: float  # hedge サイジング (kelly * hedge_mult)


def scan_calibration_bothside(
    moneylines: list[MoneylineMarket],
    balance_usd: float | None = None,
    liquidity_map: dict[str, LiquiditySnapshot] | None = None,
    max_combined_vwap: float = 0.995,
    hedge_kelly_mult: float = 0.5,  # base fallback (動的乗数のフォールバック)
    hedge_max_price: float | None = None,  # DEPRECATED: ignored
) -> list[BothsideOpportunity]:
    """Calibration scan returning both-side opportunities (MERGE-first).

    For each game:
      1. Evaluate both outcomes (same logic as scan_calibration)
      2. Collect all positive-EV candidates
      3. Sort by EV → [0]=directional, [1]=hedge candidate
      4. Hedge guard: combined < max_combined_vwap only (MERGE-first)
      5. Dynamic sizing based on MERGE margin
    """
    _curve = get_default_curve()
    results: list[BothsideOpportunity] = []

    for ml in moneylines:
        if not ml.active:
            continue

        candidates: list[CalibrationOpportunity] = []

        for i, outcome_name in enumerate(ml.outcomes):
            if i >= len(ml.prices) or i >= len(ml.token_ids):
                continue

            price = ml.prices[i]
            if price <= 0 or price >= 1:
                continue

            est = _curve.estimate(price)
            if est is None:
                continue

            expected_wr = est.lower_bound
            point_wr = est.point_estimate
            ev = _ev_per_dollar(expected_wr, price)
            if ev <= 0:
                continue

            kelly = _calibration_kelly(expected_wr, price)
            confidence_mult = _confidence_multiplier(est)
            kelly *= confidence_mult

            sweet = is_in_sweet_spot(price, settings.sweet_spot_lo, settings.sweet_spot_hi)

            kelly_usd = min(kelly * settings.max_position_usd * 10, settings.max_position_usd)
            edge_pct = (expected_wr - price) * 100

            band = lookup_band(price)
            band_label = f"{band.price_lo:.2f}-{band.price_hi:.2f}" if band else f"{price:.2f}"
            band_confidence = _confidence_from_sample_size(est.effective_sample_size)

            # 3層制約
            liq_score = "unknown"
            binding = "kelly"
            rec_exec = "immediate"
            position_usd = kelly_usd
            token_id = ml.token_ids[i]
            liq = liquidity_map.get(token_id) if liquidity_map else None

            if balance_usd is not None or liq is not None:
                sizing = calculate_position_size(
                    kelly_usd=kelly_usd,
                    balance_usd=balance_usd,
                    liquidity=liq,
                    max_position_usd=settings.max_position_usd,
                    capital_risk_pct=settings.capital_risk_pct,
                    liquidity_fill_pct=settings.liquidity_fill_pct,
                    max_spread_pct=settings.max_spread_pct,
                )
                position_usd = sizing.final_size_usd
                liq_score = sizing.liquidity_score
                binding = sizing.constraint_binding
                rec_exec = sizing.recommended_execution
                if rec_exec == "skip":
                    continue

            candidates.append(
                CalibrationOpportunity(
                    event_slug=ml.event_slug,
                    event_title=ml.event_title,
                    market_type="moneyline",
                    outcome_name=outcome_name,
                    token_id=token_id,
                    poly_price=price,
                    calibration_edge_pct=edge_pct,
                    expected_win_rate=expected_wr,
                    ev_per_dollar=ev,
                    price_band=band_label,
                    in_sweet_spot=sweet,
                    band_confidence=band_confidence,
                    position_usd=position_usd,
                    point_estimate_wr=point_wr,
                    liquidity_score=liq_score,
                    constraint_binding=binding,
                    recommended_execution=rec_exec,
                )
            )

        if not candidates:
            continue

        # EV でソート (高い方 = directional)
        candidates.sort(key=lambda c: c.ev_per_dollar, reverse=True)
        directional = candidates[0]

        hedge: CalibrationOpportunity | None = None
        hedge_pos_usd = 0.0
        combined = directional.poly_price

        if len(candidates) >= 2:
            hedge_candidate = candidates[1]
            combined = directional.poly_price + hedge_candidate.poly_price

            # MERGE-first: 安全弁 (combined < max_combined_vwap) のみ。
            # 実際の価格判定は executor が注文板ベースで行う。
            if combined < max_combined_vwap:
                hedge = hedge_candidate
                merge_margin = 1.0 - combined
                effective_mult = _hedge_margin_multiplier(merge_margin)
                hedge_pos_usd = hedge.position_usd * effective_mult

        results.append(
            BothsideOpportunity(
                directional=directional,
                hedge=hedge,
                combined_price=combined,
                hedge_position_usd=hedge_pos_usd,
            )
        )

    results.sort(key=lambda r: r.directional.ev_per_dollar, reverse=True)
    return results
