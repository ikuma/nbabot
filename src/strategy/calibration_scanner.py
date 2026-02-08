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
    *,
    min_price: float | None = None,
    max_price: float | None = None,
    min_edge_pct: float | None = None,
) -> list[CalibrationOpportunity]:
    """Calibration-based scan. No bookmaker odds required.

    For each game:
      1. Compute calibration EV for both outcomes
      2. Filter by price band (min_price <= poly_price <= max_price)
      3. Select the outcome with higher EV (one signal per game)
      4. Require calibration_edge >= min_edge_pct
      5. Kelly sizing (sweet spot = normal, outside = 0.5x)
    """
    lo = min_price if min_price is not None else settings.min_buy_price
    hi = max_price if max_price is not None else settings.max_buy_price
    threshold = min_edge_pct if min_edge_pct is not None else settings.min_calibration_edge_pct

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

            # 価格帯フィルター
            if price < lo or price > hi:
                continue

            band = lookup_band(price)
            if band is None:
                continue

            expected_wr = band.expected_win_rate
            edge = expected_wr - price  # raw edge (0-1 scale)
            edge_pct = edge * 100

            # 最低エッジ閾値
            if edge_pct < threshold:
                continue

            ev = _ev_per_dollar(expected_wr, price)

            # Kelly sizing
            kelly = _calibration_kelly(expected_wr, price)
            sweet = is_in_sweet_spot(price, settings.sweet_spot_lo, settings.sweet_spot_hi)

            # sweet spot 外はサイズ 0.5x
            if not sweet:
                kelly *= 0.5

            position_usd = min(kelly * settings.max_position_usd * 10, settings.max_position_usd)

            band_label = f"{band.price_lo:.2f}-{band.price_hi:.2f}"

            candidate = CalibrationOpportunity(
                event_slug=ml.event_slug,
                event_title=ml.event_title,
                market_type="moneyline",
                outcome_name=outcome_name,
                token_id=ml.token_ids[i],
                poly_price=price,
                calibration_edge_pct=edge_pct,
                expected_win_rate=expected_wr,
                ev_per_dollar=ev,
                price_band=band_label,
                in_sweet_spot=sweet,
                band_confidence=band.confidence,
                position_usd=position_usd,
            )

            # 1 試合 1 シグナル: EV が最も高いアウトカムを選択
            if ev > best_ev:
                best_ev = ev
                best = candidate

        if best is not None:
            opportunities.append(best)

    opportunities.sort(key=lambda o: o.calibration_edge_pct, reverse=True)
    return opportunities
