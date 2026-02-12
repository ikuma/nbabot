#!/usr/bin/env python3
"""Settle paper-trade signals with game outcomes.

Usage:
    # Auto-settle using NBA.com scores (final games only)
    python scripts/settle.py --auto

    # Auto-settle dry run (no DB writes)
    python scripts/settle.py --auto --dry-run

    # Interactive: settle each unsettled signal one by one
    python scripts/settle.py

    # Settle a specific signal by ID
    python scripts/settle.py --signal-id 3 --winner "Boston Celtics"

    # List unsettled signals
    python scripts/settle.py --list
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

# Re-export for backward compatibility (tests import from scripts.settle)
from src.settlement.pnl_calc import (  # noqa: E402, F401
    _calc_bothside_pnl,
    _calc_dca_group_pnl,
    _calc_merge_pnl,
    _calc_pnl,
    calc_signal_pnl,
)
from src.settlement.settler import (  # noqa: E402, F401
    AutoSettleSummary,
    SettleResult,
    _determine_winner,
    _parse_slug,
    _try_polymarket_fallback,
    auto_settle,
    settle_signal,
)


def list_unsettled(db_path: Path | str | None = None) -> None:
    """Print unsettled signals."""
    from src.store.db import DEFAULT_DB_PATH, get_unsettled

    path = db_path or DEFAULT_DB_PATH
    unsettled = get_unsettled(db_path=path)

    if not unsettled:
        print("No unsettled signals.")
        return

    print(f"\nUnsettled signals: {len(unsettled)}\n")
    print(
        f"{'ID':>4}  {'Date':10}  {'Game':40}  {'Team':25}"
        f"  {'Edge%':>6}  {'Size$':>6}  {'Status':10}"
    )
    print("-" * 112)
    for s in unsettled:
        dt = s.created_at[:10]
        status = getattr(s, "order_status", "paper") or "paper"
        print(
            f"{s.id:>4}  {dt:10}  {s.game_title:40}  {s.team:25}  "
            f"{s.edge_pct:>5.1f}%  ${s.kelly_size:>5.0f}  {status:10}"
        )


def interactive_settle(db_path: Path | str | None = None) -> None:
    """Interactively settle each unsettled signal."""
    from src.store.db import DEFAULT_DB_PATH, get_unsettled

    path = db_path or DEFAULT_DB_PATH
    unsettled = get_unsettled(db_path=path)

    if not unsettled:
        print("No unsettled signals.")
        return

    print(f"\n{len(unsettled)} unsettled signal(s). Enter winner team name or 'skip'.\n")
    for s in unsettled:
        print(f"  Signal #{s.id}: {s.game_title}")
        print(
            f"    BUY {s.team} @ {s.poly_price:.3f} "
            f"(book: {s.book_prob:.3f}, edge: {s.edge_pct:.1f}%)"
        )
        winner = input("    Winner (or 'skip'/'quit'): ").strip()
        if winner.lower() == "quit":
            break
        if winner.lower() == "skip":
            continue
        settle_signal(s.id, winner, db_path=path)
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Settle paper-trade signals")
    parser.add_argument("--list", action="store_true", help="List unsettled signals")
    parser.add_argument("--auto", action="store_true", help="Auto-settle via NBA.com scores")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run (no DB writes)",
    )
    parser.add_argument("--signal-id", type=int, help="Settle a specific signal by ID")
    parser.add_argument("--winner", type=str, help="Winner team name (with --signal-id)")
    args = parser.parse_args()

    if args.list:
        list_unsettled()
    elif args.auto:
        summary = auto_settle(dry_run=args.dry_run)
        print(summary.format_summary())
        # Telegram 通知 (dry-run 時はスキップ)
        if summary.settled and not args.dry_run:
            try:
                from src.notifications.telegram import send_message

                send_message(summary.format_summary())
            except Exception:
                log.exception("Failed to send Telegram notification")
    elif args.signal_id and args.winner:
        settle_signal(args.signal_id, args.winner)
    elif args.signal_id and not args.winner:
        parser.error("--winner is required with --signal-id")
    else:
        interactive_settle()


if __name__ == "__main__":
    main()
