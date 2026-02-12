#!/usr/bin/env python3
"""Grid-search optimizer for bothside hedge ratio.

Example:
    ./.venv/bin/python scripts/optimize_hedge_ratio.py \
      --start-date 2026-01-01 --end-date 2026-02-12 \
      --min-ratio 0.30 --max-ratio 0.80 --step 0.05 --dd-penalty 1.0
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Optimize hedge ratio from settled bothside groups"
    )
    parser.add_argument("--start-date", type=str, default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument("--min-ratio", type=float, default=0.30, help="Minimum hedge ratio")
    parser.add_argument("--max-ratio", type=float, default=0.80, help="Maximum hedge ratio")
    parser.add_argument("--step", type=float, default=0.05, help="Grid step")
    parser.add_argument(
        "--dd-penalty",
        type=float,
        default=1.0,
        help="Drawdown penalty coefficient in objective",
    )
    parser.add_argument("--top", type=int, default=5, help="Show top N ratios")
    return parser


def main() -> None:
    from src.analysis.hedge_ratio_optimizer import (
        build_group_samples,
        optimize_hedge_ratio,
    )
    from src.store.db import get_results_with_signals

    args = _build_parser().parse_args()

    pairs = get_results_with_signals()
    samples = build_group_samples(
        pairs,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    if not samples:
        print("No eligible bothside settled samples found for the specified period.")
        return

    result = optimize_hedge_ratio(
        samples,
        min_ratio=args.min_ratio,
        max_ratio=args.max_ratio,
        step=args.step,
        dd_penalty=args.dd_penalty,
    )

    print("Hedge Ratio Optimization")
    print("========================")
    print(f"Period      : {args.start_date or 'ALL'} -> {args.end_date or 'ALL'}")
    print(f"Samples     : {result.sample_count}")
    print(f"Grid        : {args.min_ratio:.3f} .. {args.max_ratio:.3f} (step {args.step:.3f})")
    print(f"DD Penalty  : {args.dd_penalty:.3f}")
    print("")
    print(
        "Best ratio  : "
        f"{result.best_ratio:.3f} "
        f"(objective={result.best_evaluation.objective_score:+.2f}, "
        f"total_pnl={result.best_evaluation.total_pnl_usd:+.2f}, "
        f"max_dd={result.best_evaluation.max_drawdown_usd:.2f})"
    )
    print("")

    ranked = sorted(
        result.evaluations,
        key=lambda e: (e.objective_score, e.total_pnl_usd),
        reverse=True,
    )
    top_n = max(1, args.top)
    print(f"Top {top_n} candidates")
    print("----------------")
    print("ratio\tobjective\ttotal_pnl\tmax_dd\tavg/group")
    for ev in ranked[:top_n]:
        print(
            f"{ev.hedge_ratio:.3f}\t"
            f"{ev.objective_score:+.2f}\t"
            f"{ev.total_pnl_usd:+.2f}\t"
            f"{ev.max_drawdown_usd:.2f}\t"
            f"{ev.avg_pnl_per_group_usd:+.2f}"
        )


if __name__ == "__main__":
    main()
