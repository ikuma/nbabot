"""Core settlement logic: auto-settle and helpers.

Extracted from scripts/settle.py — handles NBA score + Polymarket fallback settlement.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from src.settlement.pnl_calc import (
    _calc_bothside_pnl,
    _calc_dca_group_pnl,
    _calc_merge_pnl,
    _calc_pnl,
)

if TYPE_CHECKING:
    from src.connectors.nba_schedule import NBAGame
    from src.store.db import SignalRecord

log = logging.getLogger(__name__)


def _parse_slug(slug: str) -> tuple[str, str, str] | None:
    """Parse event_slug 'nba-{away}-{home}-YYYY-MM-DD' -> (away_abbr, home_abbr, date)."""
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
    # Bothside fields (optional)
    is_bothside: bool = False
    dir_pnl: float = 0.0
    hedge_pnl: float = 0.0
    # MERGE fields (optional)
    is_merged: bool = False
    merge_pnl: float = 0.0
    remainder_pnl: float = 0.0


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
            # Telegram Markdown V1: _ をエスケープ (italic 誤解釈防止)
            method = r.method.replace("_", "\\_")
            if r.is_merged:
                lines.append(
                    f"  #{r.signal_id} \\[MERGE] {r.team}: "
                    f"MERGE=${r.merge_pnl:+.2f} REM=${r.remainder_pnl:+.2f} "
                    f"NET=${r.pnl:+.2f} ({method})"
                )
            elif r.is_bothside:
                lines.append(
                    f"  #{r.signal_id} \\[BOTHSIDE] {r.team}: "
                    f"DIR=${r.dir_pnl:+.2f} HEDGE=${r.hedge_pnl:+.2f} "
                    f"NET=${r.pnl:+.2f} ({method})"
                )
            else:
                lines.append(f"  #{r.signal_id} {r.team}: {status} ${r.pnl:+.2f} ({method})")
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
    fill_px = getattr(signal, "fill_price", None)
    pnl = _calc_pnl(won, signal.kelly_size, signal.poly_price, fill_px)

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
        "Settled signal #%d: %s %s -> %s (PnL: $%.2f)",
        signal.id,
        signal.side,
        signal.team,
        status,
        pnl,
    )


def _refresh_order_statuses(db_path: Path | str | None = None) -> None:
    """Check placed orders for fills; cancel orders older than 24h."""
    from datetime import datetime, timedelta, timezone

    from src.store.db import DEFAULT_DB_PATH, get_placed_orders, update_order_status

    path = db_path or DEFAULT_DB_PATH
    placed = get_placed_orders(db_path=path)
    if not placed:
        return

    log.info("Checking %d placed order(s) for fill status", len(placed))

    from src.connectors.polymarket import cancel_order, get_order_status

    now = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(hours=24)

    for signal in placed:
        if not signal.order_id:
            continue

        try:
            status = get_order_status(signal.order_id)
        except Exception:
            log.exception("Failed to get status for order %s", signal.order_id)
            continue

        order_status = status.get("status", "").lower()

        if order_status in ("matched", "filled"):
            avg_price = None
            try:
                avg_price = float(status.get("associate_trades", [{}])[0].get("price", 0))
            except (IndexError, KeyError, TypeError, ValueError):
                pass
            if not avg_price:
                try:
                    avg_price = float(status.get("price", 0))
                except (ValueError, TypeError):
                    avg_price = signal.poly_price
            update_order_status(signal.id, signal.order_id, "filled", avg_price, db_path=path)
            log.info(
                "Order %s filled @ %.3f (signal #%d)",
                signal.order_id,
                avg_price or 0,
                signal.id,
            )
        elif order_status in ("cancelled", "expired"):
            update_order_status(signal.id, signal.order_id, "cancelled", db_path=path)
            log.info("Order %s already %s (signal #%d)", signal.order_id, order_status, signal.id)
        else:
            try:
                created = datetime.fromisoformat(signal.created_at.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue
            if created < stale_cutoff:
                log.info(
                    "Cancelling stale order %s (signal #%d, age > 24h)",
                    signal.order_id,
                    signal.id,
                )
                if cancel_order(signal.order_id):
                    update_order_status(signal.id, signal.order_id, "cancelled", db_path=path)


def _determine_winner(game: NBAGame) -> str | None:
    """Determine winner from final scores. Returns full team name or None.

    Returns None for postponed games (settle skipped with warning).
    """
    # 延期試合チェック
    status_text = getattr(game, "game_status_text", "").lower()
    if "postpone" in status_text or "cancel" in status_text:
        log.warning(
            "Game %s vs %s is postponed/cancelled — skipping settle",
            game.away_team, game.home_team,
        )
        return None

    if game.home_score > game.away_score:
        # OT 検出 (ログ注記)
        period = getattr(game, "period", 0)
        if period > 4:
            ot_count = period - 4
            log.info(
                "Game %s vs %s went to %dOT (final: %d-%d)",
                game.away_team, game.home_team, ot_count, game.away_score, game.home_score,
            )
        return game.home_team
    elif game.away_score > game.home_score:
        period = getattr(game, "period", 0)
        if period > 4:
            ot_count = period - 4
            log.info(
                "Game %s vs %s went to %dOT (final: %d-%d)",
                game.away_team, game.home_team, ot_count, game.away_score, game.home_score,
            )
        return game.away_team
    return None  # tie (shouldn't happen in NBA)


def _try_polymarket_fallback(
    signal: SignalRecord,
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

    if ml.active:
        return None

    for i, price in enumerate(ml.prices):
        if price >= 0.95 and i < len(ml.outcomes):
            winner_short = ml.outcomes[i]
            return winner_short, "polymarket"

    return None


def auto_settle(
    dry_run: bool = False,
    db_path: Path | str | None = None,
    today: str | None = None,
) -> AutoSettleSummary:
    """Auto-settle unsettled signals using NBA.com scores + Polymarket fallback.

    Args:
        today: Override today's date (YYYY-MM-DD) for testing. Defaults to today in ET.
    """
    from collections import defaultdict
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo

    from src.connectors.nba_schedule import fetch_todays_games
    from src.connectors.team_mapping import full_name_from_abbr, get_team_short_name
    from src.store.db import DEFAULT_DB_PATH, get_unsettled, log_result

    path = db_path or DEFAULT_DB_PATH

    # ライブ注文のステータスを更新 (約定チェック + 24h 超キャンセル)
    if not dry_run:
        try:
            _refresh_order_statuses(db_path=path)
        except Exception:
            log.exception("Failed to refresh order statuses (continuing with settle)")

    unsettled = get_unsettled(db_path=path)
    summary = AutoSettleSummary()

    if not unsettled:
        log.info("No unsettled signals")
        return summary

    # DCA グループをまとめる (同一 dca_group_id のシグナルは一括決済)
    dca_groups: dict[str, list[SignalRecord]] = defaultdict(list)
    standalone: list[SignalRecord] = []
    for sig in unsettled:
        gid = getattr(sig, "dca_group_id", None)
        if gid:
            dca_groups[gid].append(sig)
        else:
            standalone.append(sig)

    # Bothside グループをまとめる (同一 bothside_group_id の DCA グループは一括決済)
    bothside_groups: dict[str, dict[str, list[SignalRecord]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for sig in unsettled:
        bs_gid = getattr(sig, "bothside_group_id", None)
        role = getattr(sig, "signal_role", "directional")
        if bs_gid:
            bothside_groups[bs_gid][role].append(sig)

    # DCA グループ代表 + スタンドアロンを統合してイテレーション
    signals_to_process: list[tuple[SignalRecord, list[SignalRecord] | None]] = []
    seen_groups: set[str] = set()
    seen_bothside: set[str] = set()
    for sig in unsettled:
        bs_gid = getattr(sig, "bothside_group_id", None)
        if bs_gid:
            if bs_gid in seen_bothside:
                continue
            seen_bothside.add(bs_gid)
            continue
        gid = getattr(sig, "dca_group_id", None)
        if gid:
            if gid in seen_groups:
                continue
            seen_groups.add(gid)
            signals_to_process.append((sig, dca_groups[gid]))
        else:
            signals_to_process.append((sig, None))

    log.info(
        "Found %d unsettled signal(s) (%d DCA groups, %d standalone, %d bothside groups)",
        len(unsettled),
        len(dca_groups),
        len(standalone),
        len(bothside_groups),
    )

    # NBA.com スコアボードから final ゲームを取得
    all_games = fetch_todays_games()
    final_games = [g for g in all_games if g.game_status == 3]
    log.info("Found %d final games from NBA.com", len(final_games))

    # final ゲームを (home_team, away_team) -> NBAGame でインデックス
    game_index: dict[tuple[str, str], NBAGame] = {}
    for g in final_games:
        game_index[(g.home_team, g.away_team)] = g

    # NBA.com スコアボードは ET ベース — ローカル TZ ではなく ET の日付を使用
    today_str = today or datetime.now(timezone.utc).astimezone(
        ZoneInfo("America/New_York")
    ).strftime("%Y-%m-%d")

    for signal, dca_group in signals_to_process:
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
                signal.event_slug,
                signal.id,
            )
            summary.skipped += 1
            continue

        # 勝者判定 (NBA.com or Polymarket)
        winner_short: str | None = None
        method: str = ""

        game = game_index.get((home_full, away_full))
        if game and slug_date == today_str:
            winner_full = _determine_winner(game)
            if not winner_full:
                log.warning("Tie or zero scores for signal #%d, skipping", signal.id)
                summary.skipped += 1
                continue
            winner_short = get_team_short_name(winner_full)
            method = "nba_scores"
        elif slug_date != today_str:
            poly_result = _try_polymarket_fallback(signal, away_full, home_full, slug_date)
            if poly_result:
                winner_short, method = poly_result

        if not winner_short:
            log.debug("Signal #%d: game not yet final or not found, skipping", signal.id)
            summary.skipped += 1
            continue

        won = signal.team == winner_short

        # PnL 計算: DCA グループ or 単一シグナル
        if dca_group and len(dca_group) > 1:
            group_pnl = _calc_dca_group_pnl(won, dca_group)
            total_cost = sum(s.kelly_size for s in dca_group)
            for sig in dca_group:
                sig_pnl = group_pnl * (sig.kelly_size / total_cost) if total_cost > 0 else 0.0
                if not dry_run:
                    log_result(
                        signal_id=sig.id,
                        outcome=winner_short,
                        won=won,
                        pnl=sig_pnl,
                        settlement_price=1.0 if won else 0.0,
                        db_path=path,
                    )
            result = SettleResult(
                signal_id=signal.id,
                team=signal.team,
                won=won,
                pnl=group_pnl,
                method=method,
            )
            summary.settled.append(result)
            status = "WIN" if won else "LOSS"
            prefix = "[DRY-RUN] " if dry_run else ""
            log.info(
                "%sSettled DCA group (%d entries) #%d: %s -> %s (PnL: $%.2f) via %s",
                prefix,
                len(dca_group),
                signal.id,
                signal.team,
                status,
                group_pnl,
                method,
            )
        else:
            fill_px = getattr(signal, "fill_price", None)
            pnl = _calc_pnl(won, signal.kelly_size, signal.poly_price, fill_px)
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
                signal_id=signal.id,
                team=signal.team,
                won=won,
                pnl=pnl,
                method=method,
            )
            summary.settled.append(result)
            status = "WIN" if won else "LOSS"
            prefix = "[DRY-RUN] " if dry_run else ""
            log.info(
                "%sSettled #%d: %s -> %s (PnL: $%.2f) via %s",
                prefix,
                signal.id,
                signal.team,
                status,
                pnl,
                method,
            )

    # Bothside グループ決済
    from src.store.db import get_merge_operation

    for bs_gid, roles in bothside_groups.items():
        dir_signals = roles.get("directional", [])
        hedge_signals = roles.get("hedge", [])
        all_bs_signals = dir_signals + hedge_signals
        if not all_bs_signals:
            continue

        representative = dir_signals[0] if dir_signals else hedge_signals[0]
        parsed = _parse_slug(representative.event_slug)
        if not parsed:
            summary.skipped += len(all_bs_signals)
            continue

        away_abbr, home_abbr, slug_date = parsed
        away_full = full_name_from_abbr(away_abbr)
        home_full = full_name_from_abbr(home_abbr)
        if not away_full or not home_full:
            summary.skipped += len(all_bs_signals)
            continue

        winner_short = None
        method = ""
        game = game_index.get((home_full, away_full))
        if game and slug_date == today_str:
            winner_full = _determine_winner(game)
            if winner_full:
                winner_short = get_team_short_name(winner_full)
                method = "nba_scores"
        elif slug_date != today_str:
            poly_result = _try_polymarket_fallback(representative, away_full, home_full, slug_date)
            if poly_result:
                winner_short, method = poly_result

        if not winner_short:
            summary.skipped += len(all_bs_signals)
            continue

        # MERGE 済みかどうかで分岐
        merge_op = get_merge_operation(bs_gid, db_path=path)
        if merge_op and merge_op.status in ("executed", "simulated"):
            merge_pnl_val, remainder_pnl, total_pnl = _calc_merge_pnl(
                merge_op, winner_short, dir_signals, hedge_signals
            )

            if not dry_run:
                total_cost = sum(s.kelly_size for s in all_bs_signals)
                for sig in all_bs_signals:
                    sig_pnl = total_pnl * (sig.kelly_size / total_cost) if total_cost > 0 else 0.0
                    sig_won = total_pnl > 0
                    log_result(
                        signal_id=sig.id,
                        outcome=winner_short,
                        won=sig_won,
                        pnl=sig_pnl,
                        settlement_price=1.0 if sig_won else 0.0,
                        db_path=path,
                    )

            result = SettleResult(
                signal_id=representative.id,
                team=representative.team,
                won=total_pnl > 0,
                pnl=total_pnl,
                method=method,
                is_bothside=True,
                is_merged=True,
                merge_pnl=merge_pnl_val,
                remainder_pnl=remainder_pnl,
            )
            summary.settled.append(result)

            prefix = "[DRY-RUN] " if dry_run else ""
            log.info(
                "%sSettled MERGE group %s: MERGE=$%.2f REM=$%.2f NET=$%.2f via %s",
                prefix,
                bs_gid[:8],
                merge_pnl_val,
                remainder_pnl,
                total_pnl,
                method,
            )
        else:
            dir_pnl, hedge_pnl, combined_pnl = _calc_bothside_pnl(
                winner_short, dir_signals, hedge_signals
            )

            if not dry_run:
                if dir_signals:
                    dir_won = dir_signals[0].team == winner_short
                    total_dir_cost = sum(s.kelly_size for s in dir_signals)
                    for sig in dir_signals:
                        sig_pnl = (
                            dir_pnl * (sig.kelly_size / total_dir_cost)
                            if total_dir_cost > 0
                            else 0.0
                        )
                        log_result(
                            signal_id=sig.id,
                            outcome=winner_short,
                            won=dir_won,
                            pnl=sig_pnl,
                            settlement_price=1.0 if dir_won else 0.0,
                            db_path=path,
                        )
                if hedge_signals:
                    hedge_won = hedge_signals[0].team == winner_short
                    total_hedge_cost = sum(s.kelly_size for s in hedge_signals)
                    for sig in hedge_signals:
                        sig_pnl = (
                            hedge_pnl * (sig.kelly_size / total_hedge_cost)
                            if total_hedge_cost > 0
                            else 0.0
                        )
                        log_result(
                            signal_id=sig.id,
                            outcome=winner_short,
                            won=hedge_won,
                            pnl=sig_pnl,
                            settlement_price=1.0 if hedge_won else 0.0,
                            db_path=path,
                        )

            combined_won = combined_pnl > 0
            result = SettleResult(
                signal_id=representative.id,
                team=representative.team,
                won=combined_won,
                pnl=combined_pnl,
                method=method,
                is_bothside=True,
                dir_pnl=dir_pnl,
                hedge_pnl=hedge_pnl,
            )
            summary.settled.append(result)

            prefix = "[DRY-RUN] " if dry_run else ""
            log.info(
                "%sSettled BOTHSIDE group %s: DIR=$%.2f HEDGE=$%.2f NET=$%.2f via %s",
                prefix,
                bs_gid[:8],
                dir_pnl,
                hedge_pnl,
                combined_pnl,
                method,
            )

    return summary
