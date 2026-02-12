"""Decomposed performance metrics: game_correct, trade_profit, merge_rate.

Provides clear separation of:
- Game correct rate: did we pick the right team?
- Trade profit rate: did the trade make money (P&L > 0)?
- Merge rate: was the position (partially) recovered via MERGE?
"""

from __future__ import annotations

from dataclasses import dataclass
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


def format_decomposed_summary(m: DecomposedMetrics) -> str:
    """Format decomposed metrics for Telegram / log summary."""
    return (
        f"Game W/L: {m.game_correct_count}/{m.game_incorrect_count} "
        f"| Profit W/L: {m.trade_profitable_count}/{m.trade_unprofitable_count} "
        f"| Merged: {m.merge_settled_count}"
    )
