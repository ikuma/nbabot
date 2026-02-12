"""Hedge ratio optimizer for out-of-sample exploration.

This module is intentionally side-effect free. It consumes settled
(result, signal) pairs and evaluates candidate hedge ratios using:

    objective = total_pnl - dd_penalty * max_drawdown

The hedge leg PnL is scaled linearly by ratio/base_ratio, where:
base_ratio = hedge_cost / directional_cost (observed in history).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.store.models import ResultRecord, SignalRecord


@dataclass(frozen=True)
class HedgeRatioGroupSample:
    """Per bothside-group realized sample used for ratio optimization."""

    bothside_group_id: str
    settled_at: str
    directional_cost_usd: float
    hedge_cost_usd: float
    directional_pnl_usd: float
    hedge_pnl_usd: float

    @property
    def base_hedge_ratio(self) -> float:
        return (
            self.hedge_cost_usd / self.directional_cost_usd
            if self.directional_cost_usd > 0
            else 0.0
        )


@dataclass(frozen=True)
class HedgeRatioEvaluation:
    """Single hedge ratio evaluation result."""

    hedge_ratio: float
    total_pnl_usd: float
    avg_pnl_per_group_usd: float
    max_drawdown_usd: float
    objective_score: float


@dataclass(frozen=True)
class HedgeRatioOptimizationResult:
    """Optimization output over a ratio grid."""

    sample_count: int
    best_ratio: float
    best_evaluation: HedgeRatioEvaluation
    evaluations: list[HedgeRatioEvaluation]


def _parse_iso8601(ts: str) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def _date_in_range(settled_at: str, start_date: str | None, end_date: str | None) -> bool:
    date_str = settled_at[:10] if settled_at else ""
    if not date_str:
        return False
    if start_date and date_str < start_date:
        return False
    if end_date and date_str > end_date:
        return False
    return True


def build_group_samples(
    results_with_signals: list[tuple[ResultRecord, SignalRecord]],
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[HedgeRatioGroupSample]:
    """Build optimization samples from settled signal-level records.

    Only bothside groups that include both directional and hedge signals
    are kept.
    """
    grouped: dict[str, dict] = {}

    for result, signal in results_with_signals:
        gid = signal.bothside_group_id
        if not gid:
            continue

        if not _date_in_range(result.settled_at, start_date, end_date):
            continue

        g = grouped.setdefault(
            gid,
            {
                "dir_cost": 0.0,
                "hedge_cost": 0.0,
                "dir_pnl": 0.0,
                "hedge_pnl": 0.0,
                "settled_at": "",
                "has_dir": False,
                "has_hedge": False,
            },
        )

        if signal.signal_role == "directional":
            g["dir_cost"] += float(signal.kelly_size)
            g["dir_pnl"] += float(result.pnl)
            g["has_dir"] = True
        elif signal.signal_role == "hedge":
            g["hedge_cost"] += float(signal.kelly_size)
            g["hedge_pnl"] += float(result.pnl)
            g["has_hedge"] = True
        else:
            continue

        # Keep the latest settle time per group for ordering.
        if not g["settled_at"] or result.settled_at > g["settled_at"]:
            g["settled_at"] = result.settled_at

    samples: list[HedgeRatioGroupSample] = []
    for gid, g in grouped.items():
        if not g["has_dir"] or not g["has_hedge"]:
            continue
        if g["dir_cost"] <= 0 or g["hedge_cost"] <= 0:
            continue
        samples.append(
            HedgeRatioGroupSample(
                bothside_group_id=gid,
                settled_at=g["settled_at"],
                directional_cost_usd=float(g["dir_cost"]),
                hedge_cost_usd=float(g["hedge_cost"]),
                directional_pnl_usd=float(g["dir_pnl"]),
                hedge_pnl_usd=float(g["hedge_pnl"]),
            )
        )

    samples.sort(key=lambda s: s.settled_at)
    return samples


def evaluate_hedge_ratio(
    samples: list[HedgeRatioGroupSample],
    hedge_ratio: float,
    dd_penalty: float = 1.0,
) -> HedgeRatioEvaluation:
    """Evaluate one hedge ratio candidate on group samples."""
    if not samples:
        return HedgeRatioEvaluation(
            hedge_ratio=hedge_ratio,
            total_pnl_usd=0.0,
            avg_pnl_per_group_usd=0.0,
            max_drawdown_usd=0.0,
            objective_score=0.0,
        )

    group_pnls: list[float] = []
    for sample in samples:
        base_ratio = sample.base_hedge_ratio
        if base_ratio <= 0:
            continue
        scale = hedge_ratio / base_ratio
        pnl = sample.directional_pnl_usd + sample.hedge_pnl_usd * scale
        group_pnls.append(pnl)

    if not group_pnls:
        return HedgeRatioEvaluation(
            hedge_ratio=hedge_ratio,
            total_pnl_usd=0.0,
            avg_pnl_per_group_usd=0.0,
            max_drawdown_usd=0.0,
            objective_score=0.0,
        )

    total = sum(group_pnls)
    avg = total / len(group_pnls)

    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in group_pnls:
        cumulative += pnl
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd

    objective = total - dd_penalty * max_dd
    return HedgeRatioEvaluation(
        hedge_ratio=round(hedge_ratio, 4),
        total_pnl_usd=round(total, 2),
        avg_pnl_per_group_usd=round(avg, 2),
        max_drawdown_usd=round(max_dd, 2),
        objective_score=round(objective, 2),
    )


def optimize_hedge_ratio(
    samples: list[HedgeRatioGroupSample],
    min_ratio: float = 0.30,
    max_ratio: float = 0.80,
    step: float = 0.05,
    dd_penalty: float = 1.0,
) -> HedgeRatioOptimizationResult:
    """Grid-search hedge ratio with objective: pnl - dd_penalty * max_dd."""
    if step <= 0:
        raise ValueError("step must be > 0")
    if min_ratio <= 0 or max_ratio <= 0:
        raise ValueError("ratio bounds must be > 0")
    if min_ratio > max_ratio:
        raise ValueError("min_ratio must be <= max_ratio")

    evaluations: list[HedgeRatioEvaluation] = []
    ratio = min_ratio
    while ratio <= max_ratio + 1e-9:
        evaluations.append(evaluate_hedge_ratio(samples, ratio, dd_penalty=dd_penalty))
        ratio += step

    if not evaluations:
        zero = evaluate_hedge_ratio(samples, min_ratio, dd_penalty=dd_penalty)
        return HedgeRatioOptimizationResult(
            sample_count=len(samples),
            best_ratio=min_ratio,
            best_evaluation=zero,
            evaluations=[zero],
        )

    best = max(evaluations, key=lambda e: (e.objective_score, e.total_pnl_usd))
    return HedgeRatioOptimizationResult(
        sample_count=len(samples),
        best_ratio=best.hedge_ratio,
        best_evaluation=best,
        evaluations=evaluations,
    )
