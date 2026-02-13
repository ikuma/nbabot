"""Run first-principles strategy comparison from JSON inputs.

Input JSON format (list of objects):
[
  {
    "event_slug": "nba-nyk-bos-2026-02-10",
    "directional_price": 0.46,
    "opposite_price": 0.50,
    "directional_won": true
  }
]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.analysis.position_group_backtest import (  # noqa: E402
    PositionGroupGameInput,
    compare_position_group_strategies,
)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Compare MERGE-only / Directional-only / Composite")
    p.add_argument("--input", required=True, help="Path to JSON dataset")
    p.add_argument("--merge-shares", type=float, default=100.0, help="M shares per game")
    p.add_argument("--directional-shares", type=float, default=30.0, help="D shares per game")
    p.add_argument("--fee-per-share", type=float, default=0.0, help="Fee per share (USD)")
    p.add_argument("--gas-per-game", type=float, default=0.0, help="Gas cost per merged game (USD)")
    p.add_argument(
        "--fail-if-not-superior",
        action="store_true",
        help="Exit 1 when composite is not superior to both baselines",
    )
    return p


def _load_inputs(path: str) -> list[PositionGroupGameInput]:
    raw = json.loads(Path(path).read_text())
    if not isinstance(raw, list):
        raise ValueError("Input JSON must be a list")
    games: list[PositionGroupGameInput] = []
    for row in raw:
        games.append(
            PositionGroupGameInput(
                event_slug=str(row["event_slug"]),
                directional_price=float(row["directional_price"]),
                opposite_price=float(row["opposite_price"]),
                directional_won=bool(row["directional_won"]),
            )
        )
    return games


def main() -> int:
    args = _build_parser().parse_args()
    games = _load_inputs(args.input)
    out = compare_position_group_strategies(
        games,
        merge_shares=args.merge_shares,
        directional_shares=args.directional_shares,
        fee_per_share=args.fee_per_share,
        gas_per_game=args.gas_per_game,
    )

    print("=== PositionGroup Strategy Comparison ===")
    print(f"games={out.composite.games}")
    print(
        f"merge_only: total={out.merge_only.total_pnl:.2f}, avg={out.merge_only.avg_pnl:.4f}, "
        f"win_rate={out.merge_only.win_rate*100:.1f}%"
    )
    print(
        f"directional_only: total={out.directional_only.total_pnl:.2f}, "
        f"avg={out.directional_only.avg_pnl:.4f}, "
        f"win_rate={out.directional_only.win_rate*100:.1f}%"
    )
    print(
        f"composite: total={out.composite.total_pnl:.2f}, avg={out.composite.avg_pnl:.4f}, "
        f"win_rate={out.composite.win_rate*100:.1f}%"
    )
    print(f"composite_superior={out.composite_superior}")

    if args.fail_if_not_superior and not out.composite_superior:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
