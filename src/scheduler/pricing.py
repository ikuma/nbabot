"""Shared pricing helpers for scheduler executors."""

from __future__ import annotations


def below_market_price(best_ask: float, tick: float = 0.01, floor: float = 0.01) -> float:
    """Compute maker-style order price from best ask."""
    return max(best_ask - tick, floor)


def apply_price_ceiling(price: float, ceiling: float, floor: float = 0.01) -> float:
    """Clamp price to [floor, ceiling]."""
    return max(min(price, ceiling), floor)

