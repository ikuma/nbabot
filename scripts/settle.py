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
    from src.store.db import MergeOperation, SignalRecord

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def _calc_pnl(
    won: bool,
    kelly_size: float,
    poly_price: float,
    fill_price: float | None = None,
) -> float:
    """Calculate PnL for a single trade.

    BUY at price, risk kelly_size USD.
    Win: profit = kelly_size * (1/price - 1)  (shares pay $1 each)
    Lose: loss = -kelly_size

    Uses fill_price if available (live trade), otherwise poly_price (paper).
    """
    price = fill_price if fill_price is not None else poly_price
    if price <= 0:
        return -kelly_size
    if won:
        return kelly_size * (1.0 / price - 1.0)
    return -kelly_size


def _calc_dca_group_pnl(
    won: bool,
    signals: list["SignalRecord"],
) -> float:
    """Calculate PnL for a DCA group (multiple entries on same outcome).

    total_cost = sum(kelly_size)
    total_shares = sum(kelly_size / price)
    win: pnl = total_shares * $1.00 - total_cost
    lose: pnl = -total_cost
    """
    total_cost = 0.0
    total_shares = 0.0
    for sig in signals:
        price = sig.fill_price if sig.fill_price is not None else sig.poly_price
        if price <= 0:
            total_cost += sig.kelly_size
            continue
        total_cost += sig.kelly_size
        total_shares += sig.kelly_size / price
    if won:
        return total_shares * 1.0 - total_cost
    return -total_cost


def _calc_bothside_pnl(
    winner_short: str,
    dir_signals: list["SignalRecord"],
    hedge_signals: list["SignalRecord"],
) -> tuple[float, float, float]:
    """Calculate combined PnL for a bothside game.

    Returns (dir_pnl, hedge_pnl, combined_pnl).
    """
    dir_won = dir_signals[0].team == winner_short if dir_signals else False
    hedge_won = hedge_signals[0].team == winner_short if hedge_signals else False

    if len(dir_signals) > 1:
        dir_pnl = _calc_dca_group_pnl(dir_won, dir_signals)
    elif dir_signals:
        fill_px = dir_signals[0].fill_price
        dir_pnl = _calc_pnl(dir_won, dir_signals[0].kelly_size, dir_signals[0].poly_price, fill_px)
    else:
        dir_pnl = 0.0

    if len(hedge_signals) > 1:
        hedge_pnl = _calc_dca_group_pnl(hedge_won, hedge_signals)
    elif hedge_signals:
        fill_px = hedge_signals[0].fill_price
        hedge_pnl = _calc_pnl(
            hedge_won, hedge_signals[0].kelly_size, hedge_signals[0].poly_price, fill_px
        )
    else:
        hedge_pnl = 0.0

    return dir_pnl, hedge_pnl, dir_pnl + hedge_pnl


def _calc_merge_pnl(
    merge_op: "MergeOperation",
    winner_short: str,
    dir_signals: list["SignalRecord"],
    hedge_signals: list["SignalRecord"],
) -> tuple[float, float, float]:
    """Calculate PnL for a MERGE-settled bothside group.

    Returns (merge_pnl, remainder_pnl, total_pnl).
    - merge_pnl: net profit from mergePositions (gross - gas)
    - remainder_pnl: PnL from unmerged shares (normal win/loss)
    - total_pnl: merge_pnl + remainder_pnl
    """
    # MERGE 分: gross = merge_amount * (1 - combined_vwap), net = gross - gas
    gross = merge_op.merge_amount * (1.0 - merge_op.combined_vwap)
    gas = merge_op.gas_cost_usd or 0.0
    merge_pnl = gross - gas

    # 残余分: remainder_shares を remainder_side で判定
    remainder_pnl = 0.0
    if merge_op.remainder_shares > 0 and merge_op.remainder_side:
        if merge_op.remainder_side == "directional":
            rem_signals = dir_signals
        else:
            rem_signals = hedge_signals

        if rem_signals:
            rem_team = rem_signals[0].team
            rem_won = rem_team == winner_short

            # 残余シェアのコスト按分計算
            total_cost = sum(s.kelly_size for s in rem_signals)
            total_shares = 0.0
            for s in rem_signals:
                px = s.fill_price if s.fill_price is not None else s.poly_price
                if px > 0:
                    total_shares += s.kelly_size / px

            if total_shares > 0:
                rem_shares = merge_op.remainder_shares
                # 残余コスト = total_cost * (rem_shares / total_shares)
                rem_cost = total_cost * (rem_shares / total_shares)

                if rem_won:
                    remainder_pnl = rem_shares * 1.0 - rem_cost
                else:
                    remainder_pnl = -rem_cost

    total_pnl = merge_pnl + remainder_pnl
    return merge_pnl, remainder_pnl, total_pnl


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
            if r.is_merged:
                lines.append(
                    f"  #{r.signal_id} [MERGE] {r.team}: "
                    f"MERGE=${r.merge_pnl:+.2f} REM=${r.remainder_pnl:+.2f} "
                    f"NET=${r.pnl:+.2f} ({r.method})"
                )
            elif r.is_bothside:
                lines.append(
                    f"  #{r.signal_id} [BOTHSIDE] {r.team}: "
                    f"DIR=${r.dir_pnl:+.2f} HEDGE=${r.hedge_pnl:+.2f} "
                    f"NET=${r.pnl:+.2f} ({r.method})"
                )
            else:
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
        "Settled signal #%d: %s %s → %s (PnL: $%.2f)",
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
            # 約定済み — fill_price を記録
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
            # まだオープン — 24h 超ならキャンセル
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


def auto_settle(
    dry_run: bool = False,
    db_path: Path | str | None = None,
    today: str | None = None,
) -> AutoSettleSummary:
    """Auto-settle unsettled signals using NBA.com scores + Polymarket fallback.

    Args:
        today: Override today's date (YYYY-MM-DD) for testing. Defaults to actual today.
    """
    from collections import defaultdict
    from datetime import date

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
    # DCA グループは代表 (最初のシグナル) で判定し、結果を全メンバーに適用
    # bothside_group_id がある DCA グループは bothside として処理
    signals_to_process: list[tuple[SignalRecord, list[SignalRecord] | None]] = []
    seen_groups: set[str] = set()
    seen_bothside: set[str] = set()
    for sig in unsettled:
        bs_gid = getattr(sig, "bothside_group_id", None)
        if bs_gid:
            if bs_gid in seen_bothside:
                continue
            seen_bothside.add(bs_gid)
            # bothside は専用処理するのでここでは skip
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

    # final ゲームを (home_team, away_team) → NBAGame でインデックス
    game_index: dict[tuple[str, str], NBAGame] = {}
    for g in final_games:
        game_index[(g.home_team, g.away_team)] = g

    today_str = today or date.today().strftime("%Y-%m-%d")

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
            # DCA グループ: VWAP ベース PnL
            group_pnl = _calc_dca_group_pnl(won, dca_group)
            # 各シグナルに按分 (total_cost 比率)
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
                "%sSettled DCA group (%d entries) #%d: %s → %s (PnL: $%.2f) via %s",
                prefix,
                len(dca_group),
                signal.id,
                signal.team,
                status,
                group_pnl,
                method,
            )
        else:
            # 単一シグナル: 既存ロジック
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
                "%sSettled #%d: %s → %s (PnL: $%.2f) via %s",
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
            # MERGE PnL 計算
            merge_pnl_val, remainder_pnl, total_pnl = _calc_merge_pnl(
                merge_op, winner_short, dir_signals, hedge_signals
            )

            # 各シグナルに PnL を記録 (按分)
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
            # 既存: 通常 bothside PnL 計算
            dir_pnl, hedge_pnl, combined_pnl = _calc_bothside_pnl(
                winner_short, dir_signals, hedge_signals
            )

            # 各シグナルに PnL を記録
            if not dry_run:
                # Directional 側
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
                # Hedge 側
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

            # どちらか一方でも勝てば combined は WIN 扱い
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
