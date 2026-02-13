"""Decomposed performance metrics and capital turnover tracking.

Provides clear separation of:
- Game correct rate: did we pick the right team?
- Trade profit rate: did the trade make money (P&L > 0)?
- Merge rate: was the position (partially) recovered via MERGE?
- Capital turnover: how quickly merged capital is released and reusable.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.store.models import ResultRecord, SignalRecord


@dataclass(frozen=True)
class DecomposedMetrics:
    """Three independent performance metrics."""

    game_correct_rate: float
    game_correct_count: int
    game_incorrect_count: int
    trade_profit_rate: float
    trade_profitable_count: int
    trade_unprofitable_count: int
    merge_rate: float
    merge_settled_count: int
    total_settled: int


@dataclass(frozen=True)
class CapitalTurnoverInput:
    """Single MERGE event input for capital turnover calculation."""

    bothside_group_id: str
    merge_amount: float
    combined_vwap: float
    gas_cost_usd: float
    net_profit_usd: float
    first_entry_at: str
    released_at: str


@dataclass(frozen=True)
class CapitalTurnoverMetrics:
    """Capital efficiency metrics around MERGE-based capital release."""

    groups_count: int
    total_merge_net_pnl_usd: float
    total_released_usd: float
    total_released_principal_usd: float
    capital_time_usd_hours: float
    avg_lock_hours_weighted: float
    analysis_period_hours: float
    avg_locked_capital_usd: float
    capital_turnover_ratio: float
    profit_opportunity_cycles: float


def compute_decomposed_metrics(
    results_with_signals: list[tuple[ResultRecord, SignalRecord]],
) -> DecomposedMetrics:
    """Compute 3 decomposed metrics from (ResultRecord, SignalRecord) pairs.

    Args:
        results_with_signals: List of (result, signal) tuples for settled trades.

    Returns:
        DecomposedMetrics with game_correct, trade_profit, and merge rates.
    """
    if not results_with_signals:
        return DecomposedMetrics(
            game_correct_rate=0.0,
            game_correct_count=0,
            game_incorrect_count=0,
            trade_profit_rate=0.0,
            trade_profitable_count=0,
            trade_unprofitable_count=0,
            merge_rate=0.0,
            merge_settled_count=0,
            total_settled=0,
        )

    total = len(results_with_signals)
    game_correct = sum(1 for r, _ in results_with_signals if r.won)
    game_incorrect = total - game_correct
    trade_profitable = sum(1 for r, _ in results_with_signals if r.pnl > 0)
    trade_unprofitable = total - trade_profitable
    merge_settled = sum(1 for _, s in results_with_signals if s.shares_merged > 0)

    return DecomposedMetrics(
        game_correct_rate=game_correct / total if total > 0 else 0.0,
        game_correct_count=game_correct,
        game_incorrect_count=game_incorrect,
        trade_profit_rate=trade_profitable / total if total > 0 else 0.0,
        trade_profitable_count=trade_profitable,
        trade_unprofitable_count=trade_unprofitable,
        merge_rate=merge_settled / total if total > 0 else 0.0,
        merge_settled_count=merge_settled,
        total_settled=total,
    )


def _parse_iso8601(ts: str) -> datetime | None:
    """Parse ISO8601 timestamp with fallback for trailing Z."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def compute_capital_turnover_metrics(
    merge_inputs: list[CapitalTurnoverInput],
) -> CapitalTurnoverMetrics:
    """Compute capital release and turnover metrics from MERGE events."""
    if not merge_inputs:
        return CapitalTurnoverMetrics(
            groups_count=0,
            total_merge_net_pnl_usd=0.0,
            total_released_usd=0.0,
            total_released_principal_usd=0.0,
            capital_time_usd_hours=0.0,
            avg_lock_hours_weighted=0.0,
            analysis_period_hours=0.0,
            avg_locked_capital_usd=0.0,
            capital_turnover_ratio=0.0,
            profit_opportunity_cycles=0.0,
        )

    total_net = 0.0
    total_released = 0.0
    total_principal = 0.0
    capital_time = 0.0
    weighted_lock_sum = 0.0
    weighted_lock_weight = 0.0

    start_ts: datetime | None = None
    end_ts: datetime | None = None
    valid_groups = 0

    for item in merge_inputs:
        first_dt = _parse_iso8601(item.first_entry_at)
        released_dt = _parse_iso8601(item.released_at)
        if first_dt is None or released_dt is None:
            continue

        lock_hours = max((released_dt - first_dt).total_seconds() / 3600.0, 0.0)
        merged_principal = max(item.merge_amount * item.combined_vwap, 0.0)
        released_usd = max(item.merge_amount - item.gas_cost_usd, 0.0)

        total_net += item.net_profit_usd
        total_released += released_usd
        total_principal += merged_principal
        capital_time += merged_principal * lock_hours
        weighted_lock_sum += merged_principal * lock_hours
        weighted_lock_weight += merged_principal
        valid_groups += 1

        if start_ts is None or first_dt < start_ts:
            start_ts = first_dt
        if end_ts is None or released_dt > end_ts:
            end_ts = released_dt

    if valid_groups == 0:
        return CapitalTurnoverMetrics(
            groups_count=0,
            total_merge_net_pnl_usd=0.0,
            total_released_usd=0.0,
            total_released_principal_usd=0.0,
            capital_time_usd_hours=0.0,
            avg_lock_hours_weighted=0.0,
            analysis_period_hours=0.0,
            avg_locked_capital_usd=0.0,
            capital_turnover_ratio=0.0,
            profit_opportunity_cycles=0.0,
        )

    period_hours = 0.0
    if start_ts and end_ts:
        period_hours = max((end_ts - start_ts).total_seconds() / 3600.0, 0.0)

    avg_lock_hours = (weighted_lock_sum / weighted_lock_weight) if weighted_lock_weight > 0 else 0.0
    avg_locked_capital = (capital_time / period_hours) if period_hours > 0 else 0.0
    turnover_ratio = (total_released / avg_locked_capital) if avg_locked_capital > 0 else 0.0

    return CapitalTurnoverMetrics(
        groups_count=valid_groups,
        total_merge_net_pnl_usd=round(total_net, 2),
        total_released_usd=round(total_released, 2),
        total_released_principal_usd=round(total_principal, 2),
        capital_time_usd_hours=round(capital_time, 2),
        avg_lock_hours_weighted=round(avg_lock_hours, 2),
        analysis_period_hours=round(period_hours, 2),
        avg_locked_capital_usd=round(avg_locked_capital, 2),
        capital_turnover_ratio=round(turnover_ratio, 3),
        profit_opportunity_cycles=round(turnover_ratio, 3),
    )


@dataclass(frozen=True)
class MergeProfileMetrics:
    """MERGE profiling metrics computed from condition dicts."""

    merge_count: int
    merge_recovery_usd: float
    merge_pnl: float
    merge_recovery_pct: float
    avg_merge_profit: float


def compute_decomposed_from_conditions(
    conditions: dict[str, dict],
) -> DecomposedMetrics:
    """Compute 3 decomposed metrics from condition dicts (DB-free).

    Args:
        conditions: Output of build_condition_pnl(). Keys are conditionIds.

    Returns:
        DecomposedMetrics with game_correct, trade_profit, and merge rates.
    """
    if not conditions:
        return DecomposedMetrics(
            game_correct_rate=0.0,
            game_correct_count=0,
            game_incorrect_count=0,
            trade_profit_rate=0.0,
            trade_profitable_count=0,
            trade_unprofitable_count=0,
            merge_rate=0.0,
            merge_settled_count=0,
            total_settled=0,
        )

    conds = list(conditions.values())
    # Game correct: WIN status (MERGED excluded — no game outcome)
    game_correct = sum(1 for c in conds if c["status"] == "WIN")
    game_incorrect = sum(1 for c in conds if c["status"] == "LOSS_OR_OPEN")
    game_total = game_correct + game_incorrect

    # Trade profitable: P&L > 0
    total_settled = sum(1 for c in conds if c["status"] in ("WIN", "LOSS_OR_OPEN", "MERGED"))
    trade_profitable = sum(1 for c in conds if c["pnl"] > 0)
    trade_unprofitable = total_settled - trade_profitable

    # Merge rate: conditions settled via MERGE
    merge_settled = sum(1 for c in conds if c["merge_usdc"] > 0)

    return DecomposedMetrics(
        game_correct_rate=game_correct / game_total if game_total > 0 else 0.0,
        game_correct_count=game_correct,
        game_incorrect_count=game_incorrect,
        trade_profit_rate=trade_profitable / total_settled if total_settled > 0 else 0.0,
        trade_profitable_count=trade_profitable,
        trade_unprofitable_count=trade_unprofitable,
        merge_rate=merge_settled / total_settled if total_settled > 0 else 0.0,
        merge_settled_count=merge_settled,
        total_settled=total_settled,
    )


def compute_merge_metrics(
    conditions: dict[str, dict],
) -> MergeProfileMetrics:
    """Compute MERGE profiling metrics from condition dicts (DB-free).

    Args:
        conditions: Output of build_condition_pnl(). Keys are conditionIds.

    Returns:
        MergeProfileMetrics with merge count, recovery, and PnL.
    """
    merged = [c for c in conditions.values() if c.get("merge_usdc", 0) > 0]
    if not merged:
        return MergeProfileMetrics(
            merge_count=0,
            merge_recovery_usd=0.0,
            merge_pnl=0.0,
            merge_recovery_pct=0.0,
            avg_merge_profit=0.0,
        )

    merge_count = len(merged)
    merge_recovery = sum(c["merge_usdc"] for c in merged)
    merge_cost = sum(c["net_cost"] for c in merged)
    merge_pnl = merge_recovery - merge_cost
    merge_recovery_pct = merge_recovery / merge_cost * 100 if merge_cost > 0 else 0.0
    avg_profit = merge_pnl / merge_count

    return MergeProfileMetrics(
        merge_count=merge_count,
        merge_recovery_usd=round(merge_recovery, 2),
        merge_pnl=round(merge_pnl, 2),
        merge_recovery_pct=round(merge_recovery_pct, 2),
        avg_merge_profit=round(avg_profit, 2),
    )


def format_decomposed_summary(m: DecomposedMetrics) -> str:
    """Format decomposed metrics for Telegram / log summary."""
    return (
        f"Game W/L: {m.game_correct_count}/{m.game_incorrect_count} "
        f"| Profit W/L: {m.trade_profitable_count}/{m.trade_unprofitable_count} "
        f"| Merged: {m.merge_settled_count}"
    )


def format_capital_turnover_summary(m: CapitalTurnoverMetrics) -> str:
    """Format capital turnover metrics for log/summary output."""
    return (
        f"MERGE net=${m.total_merge_net_pnl_usd:+.2f} "
        f"| Released=${m.total_released_usd:.2f} "
        f"| Avg lock={m.avg_lock_hours_weighted:.1f}h "
        f"| Turnover={m.capital_turnover_ratio:.3f}x"
    )
