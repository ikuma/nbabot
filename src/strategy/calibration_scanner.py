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
      5. Kelly sizing (sweet spot = full, outside = 0.5x)
      6. Apply 3-layer constraints (kelly, capital, liquidity) if provided
    """
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

            band = lookup_band(price)
            if band is None:
                logger.debug("No calibration band for %s @ %.3f", outcome_name, price)
                continue

            expected_wr = band.expected_win_rate
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

            # Kelly sizing
            kelly = _calibration_kelly(expected_wr, price)
            sweet = is_in_sweet_spot(price, settings.sweet_spot_lo, settings.sweet_spot_hi)

            # sweet spot 外はサイズ 0.5x
            if not sweet:
                kelly *= 0.5

            kelly_usd = min(kelly * settings.max_position_usd * 10, settings.max_position_usd)

            band_label = f"{band.price_lo:.2f}-{band.price_hi:.2f}"

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
                band_confidence=band.confidence,
                position_usd=position_usd,
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
    hedge_kelly_mult: float = 0.5,
    hedge_max_price: float = 0.55,
) -> list[BothsideOpportunity]:
    """Calibration scan returning both-side opportunities.

    For each game:
      1. Evaluate both outcomes (same logic as scan_calibration)
      2. Collect all positive-EV candidates
      3. Sort by EV → [0]=directional, [1]=hedge candidate
      4. Hedge guard: positive EV, price <= max, combined < threshold
      5. Return BothsideOpportunity with hedge=None if guard fails
    """
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

            band = lookup_band(price)
            if band is None:
                continue

            expected_wr = band.expected_win_rate
            ev = _ev_per_dollar(expected_wr, price)
            if ev <= 0:
                continue

            kelly = _calibration_kelly(expected_wr, price)
            sweet = is_in_sweet_spot(price, settings.sweet_spot_lo, settings.sweet_spot_hi)
            if not sweet:
                kelly *= 0.5

            kelly_usd = min(kelly * settings.max_position_usd * 10, settings.max_position_usd)
            edge_pct = (expected_wr - price) * 100
            band_label = f"{band.price_lo:.2f}-{band.price_hi:.2f}"

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
                    band_confidence=band.confidence,
                    position_usd=position_usd,
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

            # Hedge ガード
            if (
                hedge_candidate.ev_per_dollar > 0
                and hedge_candidate.poly_price <= hedge_max_price
                and combined < max_combined_vwap
            ):
                hedge = hedge_candidate
                hedge_pos_usd = hedge.position_usd * hedge_kelly_mult

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
