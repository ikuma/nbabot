"""Three-layer position sizing: Kelly, capital, and liquidity constraints.

final_size = min(kelly, capital_cap, liquidity_cap, max_position_usd)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from src.sizing.liquidity import LiquiditySnapshot, score_liquidity

logger = logging.getLogger(__name__)


@dataclass
class DCABudget:
    """Pre-calculated total budget for a DCA position."""

    total_budget_usd: float  # DCA 全体の総予算
    num_slices: int  # 分割回数
    slice_size_usd: float  # 1 スライス (= total / num_slices)
    constraint_binding: str  # 最終的にどの制約が効いたか


def calculate_dca_budget(
    kelly_usd: float,
    num_entries: int,
    balance_usd: float | None = None,
    liquidity: LiquiditySnapshot | None = None,
    max_position_usd: float = 100.0,
    capital_risk_pct: float = 2.0,
    liquidity_fill_pct: float = 10.0,
    max_spread_pct: float = 10.0,
) -> DCABudget:
    """Calculate total DCA budget upfront, then divide into equal slices.

    Args:
        kelly_usd: Raw Kelly criterion size for a single entry.
        num_entries: Number of DCA slices (config.dca_max_entries).
        balance_usd: Current wallet balance. None = no capital constraint.
        liquidity: Order book snapshot. None = no liquidity constraint.
        max_position_usd: Hard cap per single position.
        capital_risk_pct: Max % of balance per position.
        liquidity_fill_pct: Max % of ask_depth_5c to consume.
        max_spread_pct: Skip if spread exceeds this %.
    """
    num_entries = max(num_entries, 1)
    raw_total = max(kelly_usd, 0.0) * num_entries

    # 残高制約 (DCA 全体で)
    if balance_usd is not None and balance_usd > 0:
        capital_cap = balance_usd * capital_risk_pct / 100.0 * num_entries
    else:
        capital_cap = float("inf")

    # 流動性制約 (全体で)
    liquidity_cap = float("inf")
    if liquidity is not None:
        if liquidity.spread_pct > max_spread_pct:
            return DCABudget(
                total_budget_usd=0.0,
                num_slices=num_entries,
                slice_size_usd=0.0,
                constraint_binding="liquidity",
            )
        liquidity_cap = liquidity.ask_depth_5c * liquidity_fill_pct / 100.0 * num_entries

    # ハードキャップ (per-position cap × N)
    max_total = max_position_usd * num_entries

    # 最小制約を適用
    candidates = {
        "kelly": raw_total,
        "capital": capital_cap,
        "liquidity": liquidity_cap,
        "max_position": max_total,
    }
    total_budget = min(candidates.values())
    total_budget = max(total_budget, 0.0)

    # binding 制約を特定
    binding = "kelly"
    min_val = float("inf")
    for name, val in candidates.items():
        if val < min_val:
            min_val = val
            binding = name

    slice_size = round(total_budget / num_entries, 2)
    total_budget = round(total_budget, 2)

    logger.info(
        "DCA budget: total=$%.2f slices=%d slice=$%.2f bind=%s",
        total_budget,
        num_entries,
        slice_size,
        binding,
    )

    return DCABudget(
        total_budget_usd=total_budget,
        num_slices=num_entries,
        slice_size_usd=slice_size,
        constraint_binding=binding,
    )


@dataclass
class SizingResult:
    """Result of position sizing with constraint attribution."""

    raw_kelly_usd: float
    capital_cap_usd: float
    liquidity_cap_usd: float
    final_size_usd: float
    constraint_binding: str  # "kelly" | "liquidity" | "capital" | "max_position"
    liquidity_score: str  # "high" | "medium" | "low" | "insufficient" | "unknown"
    recommended_execution: str  # "immediate" | "wait" | "skip"


def calculate_position_size(
    kelly_usd: float,
    balance_usd: float | None = None,
    liquidity: LiquiditySnapshot | None = None,
    max_position_usd: float = 100.0,
    capital_risk_pct: float = 2.0,
    liquidity_fill_pct: float = 10.0,
    max_spread_pct: float = 10.0,
) -> SizingResult:
    """Calculate position size with 3-layer constraints.

    Args:
        kelly_usd: Raw Kelly criterion size.
        balance_usd: Current wallet balance. None = no capital constraint.
        liquidity: Order book snapshot. None = no liquidity constraint.
        max_position_usd: Hard cap per position.
        capital_risk_pct: Max % of balance per position.
        liquidity_fill_pct: Max % of ask_depth_5c to consume.
        max_spread_pct: Skip if spread exceeds this %.

    Returns:
        SizingResult with final size and constraint attribution.
    """
    # Layer 1: Kelly (input)
    raw_kelly = max(kelly_usd, 0.0)

    # Layer 2: Capital constraint
    if balance_usd is not None and balance_usd > 0:
        capital_cap = balance_usd * capital_risk_pct / 100.0
    else:
        capital_cap = float("inf")

    # Layer 3: Liquidity constraint
    liquidity_cap = float("inf")
    liq_score = "unknown"
    execution = "immediate"

    if liquidity is not None:
        liq_score = score_liquidity(liquidity, raw_kelly)

        # スプレッドが閾値を超えたら skip
        if liquidity.spread_pct > max_spread_pct:
            return SizingResult(
                raw_kelly_usd=raw_kelly,
                capital_cap_usd=capital_cap if capital_cap != float("inf") else -1,
                liquidity_cap_usd=0.0,
                final_size_usd=0.0,
                constraint_binding="liquidity",
                liquidity_score="insufficient",
                recommended_execution="skip",
            )

        # スプレッドが閾値の 75% を超えたら流動性キャップ半減
        spread_warn_threshold = max_spread_pct * 0.75
        depth_cap = liquidity.ask_depth_5c * liquidity_fill_pct / 100.0
        if liquidity.spread_pct > spread_warn_threshold:
            depth_cap *= 0.5
            execution = "wait"

        liquidity_cap = depth_cap

        if liq_score == "insufficient":
            execution = "skip"
        elif liq_score == "low":
            execution = "wait"

    # 最終サイズ = 4つの制約の最小値
    candidates = {
        "kelly": raw_kelly,
        "capital": capital_cap,
        "liquidity": liquidity_cap,
        "max_position": max_position_usd,
    }
    final = min(candidates.values())
    final = max(final, 0.0)

    # binding 制約を特定
    binding = "kelly"
    min_val = float("inf")
    for name, val in candidates.items():
        if val < min_val:
            min_val = val
            binding = name

    # 流動性 skip の場合はサイズ 0
    if execution == "skip":
        final = 0.0

    return SizingResult(
        raw_kelly_usd=raw_kelly,
        capital_cap_usd=capital_cap if capital_cap != float("inf") else -1,
        liquidity_cap_usd=liquidity_cap if liquidity_cap != float("inf") else -1,
        final_size_usd=round(final, 2),
        constraint_binding=binding,
        liquidity_score=liq_score,
        recommended_execution=execution,
    )
