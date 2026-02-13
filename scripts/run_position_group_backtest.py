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

Or DB source:
  ./.venv/bin/python scripts/run_position_group_backtest.py --db data/paper_trades.db
  ./.venv/bin/python scripts/run_position_group_backtest.py --execution live
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
from src.store.db import get_position_group_backtest_games  # noqa: E402
from src.store.db_path import resolve_db_path  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Compare MERGE-only / Directional-only / Composite")
    p.add_argument("--input", default="", help="Path to JSON dataset")
    p.add_argument("--db", default="", help="SQLite DB path (optional override)")
    p.add_argument(
        "--execution",
        choices=["paper", "live", "dry-run"],
        default="paper",
        help="DB mode when --db is omitted",
    )
    p.add_argument("--start-at", default="", help="ISO8601 start (inclusive)")
    p.add_argument("--end-at", default="", help="ISO8601 end (exclusive)")
    p.add_argument(
        "--fill-opposite-from-complement",
        action="store_true",
        help="Use 1-directional_price when opposite_price is missing",
    )
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


def _load_inputs_from_db(
    *,
    db_path: str,
    start_at: str | None,
    end_at: str | None,
    fill_opposite_from_complement: bool,
) -> tuple[list[PositionGroupGameInput], int]:
    rows = get_position_group_backtest_games(
        db_path=db_path,
        start_at=start_at,
        end_at=end_at,
    )
    games: list[PositionGroupGameInput] = []
    skipped_missing_opp = 0
    for row in rows:
        opp = row["opposite_price"]
        if opp is None and fill_opposite_from_complement:
            opp = 1.0 - float(row["directional_price"])
        if opp is None:
            skipped_missing_opp += 1
            continue
        games.append(
            PositionGroupGameInput(
                event_slug=str(row["event_slug"]),
                directional_price=float(row["directional_price"]),
                opposite_price=float(opp),
                directional_won=bool(row["directional_won"]),
            )
        )
    return games, skipped_missing_opp


def main() -> int:
    args = _build_parser().parse_args()
    resolved_db = ""
    if args.input:
        games = _load_inputs(args.input)
        skipped_missing_opp = 0
    else:
        resolved_db = resolve_db_path(
            execution_mode=args.execution,
            explicit_db_path=args.db or None,
        )
        games, skipped_missing_opp = _load_inputs_from_db(
            db_path=resolved_db,
            start_at=args.start_at or None,
            end_at=args.end_at or None,
            fill_opposite_from_complement=args.fill_opposite_from_complement,
        )
    if not games:
        print("No backtest games found")
        return 1
    out = compare_position_group_strategies(
        games,
        merge_shares=args.merge_shares,
        directional_shares=args.directional_shares,
        fee_per_share=args.fee_per_share,
        gas_per_game=args.gas_per_game,
    )

    print("=== PositionGroup Strategy Comparison ===")
    print(f"games={out.composite.games}")
    if not args.input:
        print(f"source=db:{resolved_db}")
        print(f"skipped_missing_opposite={skipped_missing_opp}")
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
