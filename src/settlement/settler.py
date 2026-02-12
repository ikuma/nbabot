"""Core settlement logic: auto-settle and helpers.

Extracted from scripts/settle.py — handles NBA score + Polymarket fallback settlement.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from src.settlement.pnl_calc import (  # noqa: F401
    _calc_bothside_pnl,
    _calc_dca_group_pnl,
    _calc_merge_pnl,
    _calc_pnl,
    calc_signal_pnl,
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
    # Enrichment fields (Phase N)
    total_cost: float = 0.0
    away_score: int | None = None
    home_score: int | None = None


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
            roi_str = ""
            if r.total_cost > 0:
                roi_pct = (r.pnl / r.total_cost) * 100
                roi_str = f" (ROI {roi_pct:+.1f}%)"
            if r.is_merged:
                lines.append(
                    f"  #{r.signal_id} \\[MERGE] {r.team}: "
                    f"MERGE=${r.merge_pnl:+.2f} REM=${r.remainder_pnl:+.2f} "
                    f"NET=${r.pnl:+.2f}{roi_str} ({method})"
                )
            elif r.is_bothside:
                lines.append(
                    f"  #{r.signal_id} \\[BOTHSIDE] {r.team}: "
                    f"DIR=${r.dir_pnl:+.2f} HEDGE=${r.hedge_pnl:+.2f} "
                    f"NET=${r.pnl:+.2f}{roi_str} ({method})"
                )
            else:
                lines.append(
                    f"  #{r.signal_id} {r.team}: {status}"
                    f" ${r.pnl:+.2f}{roi_str} ({method})"
                )
            # スコア追記
            if r.away_score is not None and r.home_score is not None:
                lines.append(f"    Score: {r.away_score}-{r.home_score}")
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


def _resolve_winner(
    signal: SignalRecord,
    cache: dict[str, tuple[str, str] | None],
    game_index: dict[tuple[str, str], NBAGame],
    today_str: str,
) -> tuple[str, str] | None:
    """Determine winner for a signal's game (cached per event_slug).

    Returns (winner_short, method) or None.
    """
    from src.connectors.team_mapping import full_name_from_abbr, get_team_short_name

    slug = signal.event_slug
    if slug in cache:
        return cache[slug]

    parsed = _parse_slug(slug)
    if not parsed:
        log.warning("Cannot parse slug '%s' for signal #%d", slug, signal.id)
        cache[slug] = None
        return None

    away_abbr, home_abbr, slug_date = parsed
    away_full = full_name_from_abbr(away_abbr)
    home_full = full_name_from_abbr(home_abbr)
    if not away_full or not home_full:
        log.warning("Unknown team abbr in slug '%s' for signal #%d", slug, signal.id)
        cache[slug] = None
        return None

    winner_short: str | None = None
    method: str = ""

    # game_index は Final (status==3) のみ → 日付ガード不要
    # 深夜 0-4 AM ET にスコアボードが前日分を返すケースを正しく処理
    game = game_index.get((home_full, away_full))
    if game:
        winner_full = _determine_winner(game)
        if winner_full:
            winner_short = get_team_short_name(winner_full)
            method = "nba_scores"
    if not winner_short and slug_date != today_str:
        poly_result = _try_polymarket_fallback(
            signal, away_full, home_full, slug_date,
        )
        if poly_result:
            winner_short, method = poly_result

    if not winner_short:
        cache[slug] = None
        return None

    cache[slug] = (winner_short, method)
    return (winner_short, method)


def auto_settle(
    dry_run: bool = False,
    db_path: Path | str | None = None,
    today: str | None = None,
) -> AutoSettleSummary:
    """Auto-settle unsettled signals using NBA.com scores + Polymarket fallback.

    Per-signal settlement: each signal calculates its own PnL using
    calc_signal_pnl() which accounts for merge recovery.

    Args:
        today: Override today's date (YYYY-MM-DD) for testing. Defaults to today in ET.
    """
    from collections import defaultdict
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo

    from src.connectors.nba_schedule import fetch_todays_games
    from src.connectors.team_mapping import full_name_from_abbr
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

    log.info("Found %d unsettled signal(s)", len(unsettled))

    # NBA.com スコアボードから final ゲームを取得
    all_games = fetch_todays_games()
    final_games = [g for g in all_games if g.game_status == 3]
    log.info("Found %d final games from NBA.com", len(final_games))

    # final ゲームを (home_team, away_team) -> NBAGame でインデックス
    game_index: dict[tuple[str, str], NBAGame] = {}
    for g in final_games:
        game_index[(g.home_team, g.away_team)] = g

    # NBA.com スコアボードは ET ベース
    today_str = today or datetime.now(timezone.utc).astimezone(
        ZoneInfo("America/New_York")
    ).strftime("%Y-%m-%d")

    # winner キャッシュ (slug → result)
    winner_cache: dict[str, tuple[str, str] | None] = {}

    # 各シグナルを個別に settle
    settled_signals: list[tuple[SignalRecord, float, bool, str]] = []

    for signal in unsettled:
        result = _resolve_winner(signal, winner_cache, game_index, today_str)
        if result is None:
            summary.skipped += 1
            continue

        winner_short, method = result
        won = signal.team == winner_short

        pnl = calc_signal_pnl(
            won=won,
            kelly_size=signal.kelly_size,
            poly_price=signal.poly_price,
            fill_price=signal.fill_price,
            shares_merged=signal.shares_merged,
            merge_recovery_usd=signal.merge_recovery_usd,
        )

        if not dry_run:
            log_result(
                signal_id=signal.id,
                outcome=winner_short,
                won=won,
                pnl=pnl,
                settlement_price=1.0 if won else 0.0,
                db_path=path,
            )

        settled_signals.append((signal, pnl, won, method))
        prefix = "[DRY-RUN] " if dry_run else ""
        status = "WIN" if won else "LOSS"
        merge_tag = " [MERGE]" if signal.shares_merged > 0 else ""
        log.info(
            "%sSettled #%d: %s%s -> %s (PnL: $%.2f) via %s",
            prefix, signal.id, signal.team, merge_tag, status, pnl, method,
        )

    # 通知用: event_slug 単位で集約して SettleResult を構築
    game_settled: dict[str, list[tuple[SignalRecord, float, bool, str]]] = defaultdict(list)
    for item in settled_signals:
        game_settled[item[0].event_slug].append(item)

    for slug, signals_data in game_settled.items():
        total_pnl = sum(d[1] for d in signals_data)
        total_cost = sum(d[0].kelly_size for d in signals_data)
        has_merge = any(d[0].shares_merged > 0 for d in signals_data)

        dir_pnl = sum(d[1] for d in signals_data if d[0].signal_role == "directional")
        hedge_pnl = sum(d[1] for d in signals_data if d[0].signal_role == "hedge")

        representative = signals_data[0][0]
        method = signals_data[0][3]

        # スコア取得
        parsed = _parse_slug(slug)
        away_score = None
        home_score = None
        if parsed:
            away_full = full_name_from_abbr(parsed[0])
            home_full = full_name_from_abbr(parsed[1])
            if away_full and home_full:
                _game_ref = game_index.get((home_full, away_full))
                if _game_ref:
                    away_score = _game_ref.away_score
                    home_score = _game_ref.home_score

        is_bothside = any(d[0].signal_role == "hedge" for d in signals_data)

        if has_merge:
            merge_recovery_total = sum(d[0].merge_recovery_usd for d in signals_data)
            merge_cost_total = sum(
                d[0].shares_merged * (d[0].fill_price or d[0].poly_price)
                for d in signals_data
                if (d[0].fill_price or d[0].poly_price) > 0
            )
            merge_pnl_val = merge_recovery_total - merge_cost_total
            remainder_pnl = total_pnl - merge_pnl_val

            settle_result = SettleResult(
                signal_id=representative.id,
                team=representative.team,
                won=total_pnl > 0,
                pnl=total_pnl,
                method=method,
                is_bothside=is_bothside,
                is_merged=True,
                merge_pnl=merge_pnl_val,
                remainder_pnl=remainder_pnl,
                total_cost=total_cost,
                away_score=away_score,
                home_score=home_score,
            )
        elif is_bothside:
            settle_result = SettleResult(
                signal_id=representative.id,
                team=representative.team,
                won=total_pnl > 0,
                pnl=total_pnl,
                method=method,
                is_bothside=True,
                dir_pnl=dir_pnl,
                hedge_pnl=hedge_pnl,
                total_cost=total_cost,
                away_score=away_score,
                home_score=home_score,
            )
        else:
            settle_result = SettleResult(
                signal_id=representative.id,
                team=representative.team,
                won=total_pnl > 0,
                pnl=total_pnl,
                method=method,
                total_cost=total_cost,
                away_score=away_score,
                home_score=home_score,
            )
        summary.settled.append(settle_result)

    return summary
