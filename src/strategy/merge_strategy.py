"""MERGE strategy: pure functions for mergePositions decision-making.

Calculates mergeable shares, combined VWAP, and guards for the
CTF mergePositions operation (1 YES + 1 NO → 1 USDC).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.config import Settings
    from src.store.db import SignalRecord


def calculate_mergeable_shares(
    dir_signals: list[SignalRecord],
    hedge_signals: list[SignalRecord],
) -> tuple[float, float, float, float, str | None]:
    """Calculate how many shares can be merged.

    Returns:
        (dir_shares, hedge_shares, merge_amount, remainder_shares, remainder_side)
        - merge_amount = min(dir_shares, hedge_shares)
        - remainder_side: "directional" | "hedge" | None (if equal)
    """
    dir_shares = 0.0
    for sig in dir_signals:
        price = sig.fill_price if sig.fill_price is not None else sig.poly_price
        if price > 0:
            dir_shares += sig.kelly_size / price

    hedge_shares = 0.0
    for sig in hedge_signals:
        price = sig.fill_price if sig.fill_price is not None else sig.poly_price
        if price > 0:
            hedge_shares += sig.kelly_size / price

    merge_amount = min(dir_shares, hedge_shares)
    remainder = abs(dir_shares - hedge_shares)

    if dir_shares > hedge_shares:
        remainder_side = "directional"
    elif hedge_shares > dir_shares:
        remainder_side = "hedge"
    else:
        remainder_side = None

    return dir_shares, hedge_shares, merge_amount, remainder, remainder_side


def calculate_combined_vwap(
    dir_signals: list[SignalRecord],
    hedge_signals: list[SignalRecord],
) -> tuple[float, float, float]:
    """Calculate VWAP for directional, hedge, and combined.

    Returns:
        (dir_vwap, hedge_vwap, combined_vwap)
    """
    dir_cost = sum(s.kelly_size for s in dir_signals)
    dir_shares = 0.0
    for s in dir_signals:
        price = s.fill_price if s.fill_price is not None else s.poly_price
        if price > 0:
            dir_shares += s.kelly_size / price
    dir_vwap = dir_cost / dir_shares if dir_shares > 0 else 0.0

    hedge_cost = sum(s.kelly_size for s in hedge_signals)
    hedge_shares = 0.0
    for s in hedge_signals:
        price = s.fill_price if s.fill_price is not None else s.poly_price
        if price > 0:
            hedge_shares += s.kelly_size / price
    hedge_vwap = hedge_cost / hedge_shares if hedge_shares > 0 else 0.0

    combined_vwap = dir_vwap + hedge_vwap
    return dir_vwap, hedge_vwap, combined_vwap


def should_merge(
    combined_vwap: float,
    merge_amount: float,
    s: Settings,
    gas_cost_usd: float = 0.0,
    is_eoa: bool = True,
) -> tuple[bool, str]:
    """Determine whether to merge.

    Returns:
        (should_merge, reason)
    """
    if not s.merge_enabled:
        return False, "merge_disabled"

    if not is_eoa:
        return False, "not_eoa"

    if combined_vwap >= s.merge_max_combined_vwap:
        return False, f"combined_vwap={combined_vwap:.4f}>={s.merge_max_combined_vwap}"

    if merge_amount <= 0:
        return False, "no_mergeable_shares"

    gross_profit = merge_amount * (1.0 - combined_vwap)
    net_profit = gross_profit - gas_cost_usd

    if net_profit < s.merge_min_profit_usd:
        return False, f"net_profit=${net_profit:.4f}<min=${s.merge_min_profit_usd}"

    return True, "ok"
