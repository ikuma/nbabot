"""Order book liquidity extraction and scoring.

Parses py-clob-client OrderBookSummary into a frozen dataclass with
depth, spread, and market-impact estimates.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LiquiditySnapshot:
    """Snapshot of order book liquidity for a single token."""

    token_id: str
    timestamp: str
    best_ask: float
    best_bid: float
    ask_depth_5c: float  # USD available within best_ask + 0.05
    ask_depth_10c: float  # USD available within best_ask + 0.10
    bid_depth_5c: float  # USD available within best_bid - 0.05
    spread: float  # best_ask - best_bid
    spread_pct: float  # spread / midpoint * 100
    midpoint: float
    impact_estimate: float  # estimated slippage in cents for given order size
    ask_levels: int  # number of ask price levels
    bid_levels: int  # number of bid price levels


def extract_liquidity(
    order_book: dict[str, Any],
    token_id: str,
    order_size_usd: float = 100.0,
) -> LiquiditySnapshot | None:
    """Extract liquidity metrics from a py-clob-client order book.

    Args:
        order_book: Raw order book dict with "asks" and "bids" lists.
            Each entry has "price" (str) and "size" (str).
        token_id: The token this order book belongs to.
        order_size_usd: Hypothetical order size for impact estimation.

    Returns:
        LiquiditySnapshot or None if the book is empty.
    """
    asks_raw = order_book.get("asks", [])
    bids_raw = order_book.get("bids", [])

    if not asks_raw and not bids_raw:
        return None

    # Parse and sort: asks ascending, bids descending
    asks = sorted(
        [(float(a["price"]), float(a["size"])) for a in asks_raw],
        key=lambda x: x[0],
    )
    bids = sorted(
        [(float(b["price"]), float(b["size"])) for b in bids_raw],
        key=lambda x: x[0],
        reverse=True,
    )

    best_ask = asks[0][0] if asks else 1.0
    best_bid = bids[0][0] if bids else 0.0
    spread = best_ask - best_bid
    midpoint = (best_ask + best_bid) / 2 if (best_ask + best_bid) > 0 else 0.5
    spread_pct = (spread / midpoint * 100) if midpoint > 0 else 0.0

    # Ask depth within thresholds
    ask_depth_5c = _sum_depth(asks, best_ask, 0.05)
    ask_depth_10c = _sum_depth(asks, best_ask, 0.10)

    # Bid depth within thresholds
    bid_depth_5c = _sum_depth_bid(bids, best_bid, 0.05)

    # Market impact estimate
    impact_estimate = _estimate_impact(asks, order_size_usd)

    return LiquiditySnapshot(
        token_id=token_id,
        timestamp=datetime.now(timezone.utc).isoformat(),
        best_ask=best_ask,
        best_bid=best_bid,
        ask_depth_5c=ask_depth_5c,
        ask_depth_10c=ask_depth_10c,
        bid_depth_5c=bid_depth_5c,
        spread=spread,
        spread_pct=spread_pct,
        midpoint=midpoint,
        impact_estimate=impact_estimate,
        ask_levels=len(asks),
        bid_levels=len(bids),
    )


def _sum_depth(
    levels: list[tuple[float, float]], best_price: float, threshold: float
) -> float:
    """Sum USD depth for ask levels within best_price + threshold.

    Each level: (price, size_in_shares). USD = price * size.
    """
    total = 0.0
    ceiling = best_price + threshold
    for price, size in levels:
        if price > ceiling:
            break
        total += price * size
    return total


def _sum_depth_bid(
    levels: list[tuple[float, float]], best_price: float, threshold: float
) -> float:
    """Sum USD depth for bid levels within best_bid - threshold.

    Bids are sorted descending.
    """
    total = 0.0
    floor = best_price - threshold
    for price, size in levels:
        if price < floor:
            break
        total += price * size
    return total


def _estimate_impact(
    asks: list[tuple[float, float]], order_size_usd: float
) -> float:
    """Estimate price impact (cents) for a market buy of order_size_usd.

    Walks up the ask side, consuming liquidity until the order is filled.
    Returns the difference between VWAP and best_ask in cents.
    """
    if not asks or order_size_usd <= 0:
        return 0.0

    best_ask = asks[0][0]
    remaining_usd = order_size_usd
    total_shares = 0.0
    total_cost = 0.0

    for price, size in asks:
        level_usd = price * size
        if level_usd >= remaining_usd:
            # 部分約定
            shares_here = remaining_usd / price
            total_shares += shares_here
            total_cost += remaining_usd
            remaining_usd = 0
            break
        else:
            total_shares += size
            total_cost += level_usd
            remaining_usd -= level_usd

    if total_shares == 0:
        return 0.0

    vwap = total_cost / total_shares
    # Impact in cents (multiply by 100 to convert from price units)
    return (vwap - best_ask) * 100


def score_liquidity(snapshot: LiquiditySnapshot, size_usd: float) -> str:
    """Score liquidity relative to intended order size.

    Returns:
        "high": order < 5% of ask_depth_5c and spread < 3%
        "medium": order < 15% of ask_depth_5c and spread < 8%
        "low": order is fillable but tight
        "insufficient": spread > 15% or depth too thin
    """
    if snapshot.ask_depth_5c <= 0:
        return "insufficient"

    size_pct = (size_usd / snapshot.ask_depth_5c) * 100
    spread_pct = snapshot.spread_pct

    if spread_pct > 15.0:
        return "insufficient"

    if size_pct < 5.0 and spread_pct < 3.0:
        return "high"

    if size_pct < 15.0 and spread_pct < 8.0:
        return "medium"

    if spread_pct < 15.0:
        return "low"

    return "insufficient"
