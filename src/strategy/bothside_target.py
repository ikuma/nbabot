"""Dynamic target_combined calculation for bothside hedge pricing."""

from __future__ import annotations

from dataclasses import dataclass


def estimate_shares_from_pairs(costs: list[float], prices: list[float]) -> float:
    """Estimate total shares from cost/price pairs."""
    shares = 0.0
    for cost, price in zip(costs, prices):
        if price > 0 and cost > 0:
            shares += cost / price
    return shares


@dataclass(frozen=True)
class TargetCombinedDecision:
    """Decision output for target_combined resolution."""

    target_combined: float
    mode: str
    mergeable_shares_est: float
    required_profit_per_share: float
    reason: str


def resolve_target_combined(
    *,
    static_target: float,
    mode: str,
    mergeable_shares_est: float,
    estimated_fee_usd: float,
    min_profit_usd: float,
    min_target: float,
    max_target: float,
) -> TargetCombinedDecision:
    """Resolve target_combined from mode + estimated mergeable shares."""
    lo = min(min_target, max_target)
    hi = max(min_target, max_target)
    static_clamped = max(lo, min(hi, static_target))

    if mode != "dynamic":
        return TargetCombinedDecision(
            target_combined=round(static_clamped, 4),
            mode="static",
            mergeable_shares_est=max(mergeable_shares_est, 0.0),
            required_profit_per_share=max(1.0 - static_clamped, 0.0),
            reason="static_mode",
        )

    shares = max(mergeable_shares_est, 0.0)
    if shares <= 0:
        return TargetCombinedDecision(
            target_combined=round(static_clamped, 4),
            mode="dynamic",
            mergeable_shares_est=0.0,
            required_profit_per_share=max(1.0 - static_clamped, 0.0),
            reason="fallback_no_shares",
        )

    required_profit_total = max(estimated_fee_usd, 0.0) + max(min_profit_usd, 0.0)
    required_profit_per_share = required_profit_total / shares
    dynamic_target = 1.0 - required_profit_per_share
    target = max(lo, min(hi, dynamic_target))

    return TargetCombinedDecision(
        target_combined=round(target, 4),
        mode="dynamic",
        mergeable_shares_est=round(shares, 4),
        required_profit_per_share=round(required_profit_per_share, 6),
        reason="dynamic_formula",
    )
