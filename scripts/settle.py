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
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging

if TYPE_CHECKING:
    from src.connectors.nba_schedule import NBAGame
    from src.store.db import SignalRecord

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


def _parse_slug(slug: str) -> tuple[str, str, str] | None:
    """Parse event_slug 'nba-{away}-{home}-YYYY-MM-DD' → (away_abbr, home_abbr, date)."""
    m = re.match(r"^nba-([a-z]{3})-([a-z]{3})-(\d{4}-\d{2}-\d{2})$", slug)
    if not m:
        return None
    return m.group(1), m.group(2), m.group(3)


@dataclass
class SettleResult:
    """Result of settling a single signal."""

    signal_id: int
    team: str
    won: bool
    pnl: float
    method: str  # "nba_scores" or "polymarket"


@dataclass
class AutoSettleSummary:
    """Summary of an auto-settle run."""

    settled: list[SettleResult] = field(default_factory=list)
    skipped: int = 0
    errors: int = 0

    @property
    def wins(self) -> int:
        return sum(1 for r in self.settled if r.won)

    @property
    def losses(self) -> int:
        return sum(1 for r in self.settled if not r.won)

    @property
    def total_pnl(self) -> float:
        return sum(r.pnl for r in self.settled)

    def format_summary(self) -> str:
        if not self.settled:
            return "Auto-settle: no signals settled."
        lines = [
            "*Auto-Settle Summary*",
            f"Settled: {len(self.settled)} | Skipped: {self.skipped}",
            f"W/L: {self.wins}/{self.losses} | PnL: ${self.total_pnl:+.2f}",
            "",
        ]
        for r in self.settled:
            status = "WIN" if r.won else "LOSS"
            lines.append(f"  #{r.signal_id} {r.team}: {status} ${r.pnl:+.2f} ({r.method})")
        return "\n".join(lines)


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


def auto_settle(
    dry_run: bool = False,
    db_path: Path | str | None = None,
    today: str | None = None,
) -> AutoSettleSummary:
    """Auto-settle unsettled signals using NBA.com scores + Polymarket fallback.

    Args:
        today: Override today's date (YYYY-MM-DD) for testing. Defaults to actual today.
    """
    from datetime import date

    from src.connectors.nba_schedule import fetch_todays_games
    from src.connectors.team_mapping import full_name_from_abbr, get_team_short_name
    from src.store.db import DEFAULT_DB_PATH, get_unsettled, log_result

    path = db_path or DEFAULT_DB_PATH
    unsettled = get_unsettled(db_path=path)
    summary = AutoSettleSummary()

    if not unsettled:
        log.info("No unsettled signals")
        return summary

    log.info("Found %d unsettled signal(s)", len(unsettled))

    # NBA.com スコアボードから final ゲームを取得
    all_games = fetch_todays_games()
    final_games = [g for g in all_games if g.game_status == 3]
    log.info("Found %d final games from NBA.com", len(final_games))

    # final ゲームを (home_team, away_team) → NBAGame でインデックス
    game_index: dict[tuple[str, str], NBAGame] = {}
    for g in final_games:
        game_index[(g.home_team, g.away_team)] = g

    today_str = today or date.today().strftime("%Y-%m-%d")

    for signal in unsettled:
        parsed = _parse_slug(signal.event_slug)
        if not parsed:
            log.warning("Cannot parse slug '%s' for signal #%d", signal.event_slug, signal.id)
            summary.skipped += 1
            continue

        away_abbr, home_abbr, slug_date = parsed
        away_full = full_name_from_abbr(away_abbr)
        home_full = full_name_from_abbr(home_abbr)
        if not away_full or not home_full:
            log.warning(
                "Unknown team abbr in slug '%s' for signal #%d",
                signal.event_slug, signal.id,
            )
            summary.skipped += 1
            continue

        # NBA.com スコアで決済 (slug 日付が今日の場合)
        game = game_index.get((home_full, away_full))
        if game and slug_date == today_str:
            winner_full = _determine_winner(game)
            if not winner_full:
                log.warning("Tie or zero scores for signal #%d, skipping", signal.id)
                summary.skipped += 1
                continue

            winner_short = get_team_short_name(winner_full)
            if not winner_short:
                log.warning("Cannot get short name for '%s'", winner_full)
                summary.skipped += 1
                continue

            won = signal.team == winner_short
            pnl = _calc_pnl(won, signal.kelly_size, signal.poly_price)

            if not dry_run:
                log_result(
                    signal_id=signal.id,
                    outcome=winner_short,
                    won=won,
                    pnl=pnl,
                    settlement_price=1.0 if won else 0.0,
                    db_path=path,
                )

            result = SettleResult(
                signal_id=signal.id, team=signal.team,
                won=won, pnl=pnl, method="nba_scores",
            )
            summary.settled.append(result)
            status = "WIN" if won else "LOSS"
            prefix = "[DRY-RUN] " if dry_run else ""
            log.info(
                "%sSettled #%d: %s → %s (PnL: $%.2f) via NBA scores",
                prefix, signal.id, signal.team, status, pnl,
            )
            continue

        # Polymarket フォールバック: slug 日付≠今日 → Gamma Events API で確認
        if slug_date != today_str:
            poly_result = _try_polymarket_fallback(signal, away_full, home_full, slug_date)
            if poly_result:
                winner_short, method = poly_result
                won = signal.team == winner_short
                pnl = _calc_pnl(won, signal.kelly_size, signal.poly_price)

                if not dry_run:
                    log_result(
                        signal_id=signal.id,
                        outcome=winner_short,
                        won=won,
                        pnl=pnl,
                        settlement_price=1.0 if won else 0.0,
                        db_path=path,
                    )

                result = SettleResult(
                    signal_id=signal.id, team=signal.team,
                    won=won, pnl=pnl, method=method,
                )
                summary.settled.append(result)
                status = "WIN" if won else "LOSS"
                prefix = "[DRY-RUN] " if dry_run else ""
                log.info(
                    "%sSettled #%d: %s → %s (PnL: $%.2f) via %s",
                    prefix, signal.id, signal.team, status, pnl, method,
                )
                continue

        log.debug("Signal #%d: game not yet final or not found, skipping", signal.id)
        summary.skipped += 1

    return summary


def _determine_winner(game: "NBAGame") -> str | None:
    """Determine winner from final scores. Returns full team name or None."""
    if game.home_score > game.away_score:
        return game.home_team
    elif game.away_score > game.home_score:
        return game.away_team
    return None  # tie (shouldn't happen in NBA)


def _try_polymarket_fallback(
    signal: "SignalRecord",
    away_full: str,
    home_full: str,
    slug_date: str,
) -> tuple[str, str] | None:
    """Try to settle via Polymarket Gamma Events API.

    Returns (winner_short_name, "polymarket") or None.
    """
    from src.connectors.polymarket import fetch_moneyline_for_game

    try:
        ml = fetch_moneyline_for_game(away_full, home_full, slug_date)
    except Exception:
        log.exception("Polymarket fallback failed for signal #%d", signal.id)
        return None

    if not ml:
        return None

    # マーケットが非アクティブで、一方の価格が 0.95 以上なら決済済みとみなす
    if ml.active:
        return None

    for i, price in enumerate(ml.prices):
        if price >= 0.95 and i < len(ml.outcomes):
            winner_short = ml.outcomes[i]
            return winner_short, "polymarket"

    return None


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
    parser.add_argument("--auto", action="store_true", help="Auto-settle via NBA.com scores")
    parser.add_argument(
        "--dry-run", action="store_true", help="Dry run (no DB writes)",
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
