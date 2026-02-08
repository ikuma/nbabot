#!/usr/bin/env python3
"""Settle paper-trade signals with game outcomes.

Usage:
    # Interactive: settle each unsettled signal one by one
    python scripts/settle.py

    # Settle a specific signal by ID
    python scripts/settle.py --signal-id 3 --winner "Boston Celtics"

    # List unsettled signals
    python scripts/settle.py --list
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def _calc_pnl(won: bool, kelly_size: float, poly_price: float) -> float:
    """Calculate PnL for a paper trade.

    BUY at poly_price, risk kelly_size USD.
    Win: profit = kelly_size * (1/poly_price - 1)  (shares pay $1 each)
    Lose: loss = -kelly_size
    """
    if won:
        return kelly_size * (1.0 / poly_price - 1.0)
    return -kelly_size


def settle_signal(signal_id: int, winner: str, db_path: Path | str | None = None) -> None:
    """Settle a single signal given the game winner."""
    from src.store.db import DEFAULT_DB_PATH, get_unsettled, log_result

    path = db_path or DEFAULT_DB_PATH
    unsettled = get_unsettled(db_path=path)
    signal = next((s for s in unsettled if s.id == signal_id), None)
    if signal is None:
        log.error("Signal #%d not found or already settled", signal_id)
        return

    won = signal.team == winner
    pnl = _calc_pnl(won, signal.kelly_size, signal.poly_price)

    log_result(
        signal_id=signal.id,
        outcome=winner,
        won=won,
        pnl=pnl,
        settlement_price=1.0 if won else 0.0,
        db_path=path,
    )

    status = "WIN" if won else "LOSS"
    log.info(
        "Settled signal #%d: %s %s → %s (PnL: $%.2f)",
        signal.id, signal.side, signal.team, status, pnl,
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
    print(f"{'ID':>4}  {'Date':10}  {'Game':40}  {'Team':25}  {'Edge%':>6}  {'Size$':>6}")
    print("-" * 100)
    for s in unsettled:
        date = s.created_at[:10]
        print(
            f"{s.id:>4}  {date:10}  {s.game_title:40}  {s.team:25}  "
            f"{s.edge_pct:>5.1f}%  ${s.kelly_size:>5.0f}"
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
    parser.add_argument("--signal-id", type=int, help="Settle a specific signal by ID")
    parser.add_argument("--winner", type=str, help="Winner team name (with --signal-id)")
    args = parser.parse_args()

    if args.list:
        list_unsettled()
    elif args.signal_id and args.winner:
        settle_signal(args.signal_id, args.winner)
    elif args.signal_id and not args.winner:
        parser.error("--winner is required with --signal-id")
    else:
        interactive_settle()


if __name__ == "__main__":
    main()
