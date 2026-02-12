"""Order lifecycle manager: monitor placed orders, detect fills, cancel/re-place stale orders.

Phase O — runs every 2 minutes via launchd, independent of the 15-minute strategy scheduler.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.config import settings
from src.store.db import (
    DEFAULT_DB_PATH,
    SignalRecord,
    get_active_placed_orders,
    log_order_event,
    update_order_lifecycle,
    update_order_status,
)

logger = logging.getLogger(__name__)


@dataclass
class OrderCheckResult:
    """Result of checking a single order."""

    signal_id: int
    action: str  # 'filled'|'cancelled'|'replaced'|'expired'|'kept'|'error'
    old_order_id: str | None = None
    new_order_id: str | None = None
    fill_price: float | None = None
    new_price: float | None = None
    best_ask: float | None = None


@dataclass
class OrderTickSummary:
    """Summary of an order manager tick."""

    checked: int = 0
    filled: int = 0
    replaced: int = 0
    expired: int = 0
    cancelled: int = 0
    kept: int = 0
    errors: int = 0
    results: list[OrderCheckResult] = field(default_factory=list)


def _extract_fill_price(status: dict, fallback_price: float) -> float:
    """Extract average fill price from CLOB order status."""
    try:
        avg_price = float(status.get("associate_trades", [{}])[0].get("price", 0))
        if avg_price > 0:
            return avg_price
    except (IndexError, KeyError, TypeError, ValueError):
        pass
    try:
        price = float(status.get("price", 0))
        if price > 0:
            return price
    except (ValueError, TypeError):
        pass
    return fallback_price


def _get_best_ask(token_id: str) -> float | None:
    """Fetch current best ask price from the order book."""
    try:
        from src.connectors.polymarket import fetch_order_book_safe
        from src.sizing.liquidity import extract_liquidity

        book = fetch_order_book_safe(token_id)
        if book:
            snap = extract_liquidity(book, token_id)
            if snap and snap.best_ask > 0:
                return snap.best_ask
    except Exception:
        logger.debug("Failed to get best_ask for %s", token_id, exc_info=True)
    return None


def _is_hedge_signal(signal: SignalRecord) -> bool:
    """Check if this signal is a hedge (needs target_combined re-check)."""
    return signal.signal_role == "hedge"


def _check_hedge_target(signal: SignalRecord, new_price: float, db_path: str) -> bool:
    """Re-check combined VWAP constraint for hedge re-place.

    Returns True if new_price passes the target_combined guard.
    """
    if not signal.bothside_group_id:
        return True

    try:
        from src.store.db import get_bothside_signals
        from src.strategy.dca_strategy import calculate_vwap_from_pairs

        all_sigs = get_bothside_signals(signal.bothside_group_id, db_path=db_path)
        dir_sigs = [s for s in all_sigs if s.signal_role == "directional"]
        if not dir_sigs:
            return True

        dir_vwap = calculate_vwap_from_pairs(
            [s.kelly_size for s in dir_sigs],
            [s.fill_price or s.poly_price for s in dir_sigs],
        )
        combined = dir_vwap + new_price
        if combined >= settings.bothside_max_combined_vwap:
            logger.info(
                "Hedge re-place blocked: combined %.4f >= %.3f (signal #%d)",
                combined,
                settings.bothside_max_combined_vwap,
                signal.id,
            )
            return False
        return True
    except Exception:
        logger.warning("Hedge target check failed for signal #%d", signal.id, exc_info=True)
        return True  # fail-open: allow re-place


def check_single_order(
    signal: SignalRecord,
    db_path: str,
) -> OrderCheckResult:
    """Check a single placed order: detect fill, TTL expiry, or re-place."""
    from src.connectors.polymarket import cancel_order, get_order_status

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    order_id = signal.order_id or ""

    # a. CLOB ステータスチェック
    try:
        status = get_order_status(order_id)
    except Exception:
        logger.warning("Failed to get status for order %s (signal #%d)", order_id, signal.id)
        update_order_lifecycle(signal.id, order_last_checked_at=now_iso, db_path=db_path)
        return OrderCheckResult(signal.id, "error", old_order_id=order_id)

    order_status = status.get("status", "").lower()

    # Filled
    if order_status in ("matched", "filled"):
        fill_price = _extract_fill_price(status, signal.poly_price)
        update_order_status(signal.id, order_id, "filled", fill_price, db_path=db_path)
        update_order_lifecycle(signal.id, order_last_checked_at=now_iso, db_path=db_path)
        log_order_event(
            signal_id=signal.id,
            event_type="filled",
            order_id=order_id,
            price=fill_price,
            db_path=db_path,
        )
        logger.info("Order %s filled @ %.3f (signal #%d)", order_id, fill_price, signal.id)

        # 約定通知
        try:
            from src.notifications.telegram import notify_order_filled_early

            notify_order_filled_early(
                event_slug=signal.event_slug,
                outcome_name=signal.team,
                fill_price=fill_price,
                signal_id=signal.id,
            )
        except Exception:
            pass

        return OrderCheckResult(
            signal.id, "filled", old_order_id=order_id, fill_price=fill_price,
        )

    # Already cancelled
    if order_status in ("cancelled", "expired"):
        update_order_status(signal.id, order_id, "cancelled", db_path=db_path)
        update_order_lifecycle(signal.id, order_last_checked_at=now_iso, db_path=db_path)
        log_order_event(
            signal_id=signal.id,
            event_type="cancelled",
            order_id=order_id,
            db_path=db_path,
        )
        logger.info("Order %s already %s (signal #%d)", order_id, order_status, signal.id)
        return OrderCheckResult(signal.id, "cancelled", old_order_id=order_id)

    # b. TTL チェック
    order_age_ok = True
    if signal.order_placed_at:
        try:
            placed_at = datetime.fromisoformat(signal.order_placed_at.replace("Z", "+00:00"))
            age_min = (now - placed_at).total_seconds() / 60
            if age_min < settings.order_ttl_min:
                order_age_ok = False
        except (ValueError, AttributeError):
            pass

    update_order_lifecycle(signal.id, order_last_checked_at=now_iso, db_path=db_path)

    if not order_age_ok:
        return OrderCheckResult(signal.id, "kept", old_order_id=order_id)

    # c. 再発注判定
    replace_count = signal.order_replace_count or 0

    # 最大再発注回数超過 → cancel + expired
    if replace_count >= settings.order_max_replaces:
        if cancel_order(order_id):
            update_order_status(signal.id, order_id, "cancelled", db_path=db_path)
            log_order_event(
                signal_id=signal.id,
                event_type="expired",
                order_id=order_id,
                db_path=db_path,
            )
        logger.info(
            "Order %s expired: max replaces %d (signal #%d)", order_id, replace_count, signal.id,
        )
        return OrderCheckResult(signal.id, "expired", old_order_id=order_id)

    # ティップオフ過ぎ → cancel + expired
    try:
        from src.store.db import _connect

        conn = _connect(db_path)
        try:
            row = conn.execute(
                """SELECT game_time_utc FROM trade_jobs
                   WHERE event_slug = ? AND job_side = ?
                   LIMIT 1""",
                (signal.event_slug, signal.signal_role),
            ).fetchone()
        finally:
            conn.close()

        if row and row[0]:
            tipoff = datetime.fromisoformat(row[0].replace("Z", "+00:00"))
            if now >= tipoff:
                if cancel_order(order_id):
                    update_order_status(signal.id, order_id, "cancelled", db_path=db_path)
                    log_order_event(
                        signal_id=signal.id,
                        event_type="expired",
                        order_id=order_id,
                        db_path=db_path,
                    )
                logger.info(
                    "Order %s expired: past tipoff (signal #%d)", order_id, signal.id,
                )
                return OrderCheckResult(signal.id, "expired", old_order_id=order_id)
    except Exception:
        logger.debug("Tipoff check failed for signal #%d", signal.id, exc_info=True)

    # best_ask 取得 → re-place 判定
    best_ask = _get_best_ask(signal.token_id)
    if best_ask is None:
        return OrderCheckResult(signal.id, "kept", old_order_id=order_id)

    # 現在の注文価格を推定 (order_original_price は初回価格、実際の注文価格は再発注後は変わる)
    # order_placed_at が更新されるたびに poly_price は変わらないが、
    # 注文価格は best_ask - 0.01 で再計算される
    current_order_price = signal.order_original_price or signal.poly_price
    if replace_count > 0 and signal.order_placed_at:
        # 再発注済みの場合、前回の best_ask - 0.01 だが正確には追跡できない
        # order_events から最後の placed イベントの price を取得
        try:
            from src.store.db import get_order_events

            events = get_order_events(signal.id, db_path=db_path)
            placed_events = [e for e in events if e.event_type == "placed"]
            if placed_events:
                last_placed = placed_events[-1]
                if last_placed.price and last_placed.price > 0:
                    current_order_price = last_placed.price
        except Exception:
            pass

    new_price = max(best_ask - 0.01, 0.01)

    # 価格移動が小さい → keep
    if abs(new_price - current_order_price) < settings.order_min_price_move:
        return OrderCheckResult(signal.id, "kept", old_order_id=order_id, best_ask=best_ask)

    # Hedge の場合: target_combined 再チェック
    if _is_hedge_signal(signal) and not _check_hedge_target(signal, new_price, db_path):
        if cancel_order(order_id):
            update_order_status(signal.id, order_id, "cancelled", db_path=db_path)
            log_order_event(
                signal_id=signal.id,
                event_type="expired",
                order_id=order_id,
                db_path=db_path,
            )
        return OrderCheckResult(signal.id, "expired", old_order_id=order_id)

    # Cancel + Re-place
    try:
        from src.connectors.polymarket import cancel_and_replace_order

        resp = cancel_and_replace_order(order_id, signal.token_id, new_price, signal.kelly_size)
        new_order_id = resp.get("orderID") or resp.get("id", "")

        # DB 更新
        update_order_lifecycle(
            signal.id,
            order_id=new_order_id,
            order_status="placed",
            order_placed_at=now_iso,
            order_replace_count=replace_count + 1,
            order_last_checked_at=now_iso,
            db_path=db_path,
        )

        # order_events 記録 (cancel + placed)
        log_order_event(
            signal_id=signal.id,
            event_type="cancelled",
            order_id=order_id,
            best_ask_at_event=best_ask,
            db_path=db_path,
        )
        log_order_event(
            signal_id=signal.id,
            event_type="placed",
            order_id=new_order_id,
            price=new_price,
            best_ask_at_event=best_ask,
            db_path=db_path,
        )

        logger.info(
            "Order replaced: %s -> %s @ %.3f (ask %.3f, signal #%d, count %d/%d)",
            order_id,
            new_order_id,
            new_price,
            best_ask,
            signal.id,
            replace_count + 1,
            settings.order_max_replaces,
        )

        # 通知
        try:
            from src.notifications.telegram import notify_order_replaced

            notify_order_replaced(
                event_slug=signal.event_slug,
                outcome_name=signal.team,
                old_price=current_order_price,
                new_price=new_price,
                best_ask=best_ask,
                replace_count=replace_count + 1,
                max_replaces=settings.order_max_replaces,
            )
        except Exception:
            pass

        return OrderCheckResult(
            signal.id,
            "replaced",
            old_order_id=order_id,
            new_order_id=new_order_id,
            new_price=new_price,
            best_ask=best_ask,
        )

    except Exception as e:
        logger.exception("Re-place failed for signal #%d (order %s)", signal.id, order_id)
        return OrderCheckResult(signal.id, "error", old_order_id=order_id)


def check_and_manage_orders(
    execution_mode: str = "live",
    db_path: str | None = None,
) -> OrderTickSummary:
    """Main entry point: check all active placed orders and manage their lifecycle.

    Called by order_tick.py (2-min launchd) and as fallback by settler.py.
    """
    path = db_path or str(DEFAULT_DB_PATH)
    summary = OrderTickSummary()

    if execution_mode != "live":
        logger.debug("Order manager skipped: execution_mode=%s", execution_mode)
        return summary

    if not settings.order_manager_enabled:
        logger.debug("Order manager disabled")
        return summary

    placed_orders = get_active_placed_orders(db_path=path)
    if not placed_orders:
        return summary

    batch_size = settings.order_check_batch_size
    orders_to_check = placed_orders[:batch_size]

    logger.info(
        "Order manager: checking %d/%d placed orders",
        len(orders_to_check),
        len(placed_orders),
    )

    for signal in orders_to_check:
        result = check_single_order(signal, path)
        summary.results.append(result)
        summary.checked += 1

        if result.action == "filled":
            summary.filled += 1
        elif result.action == "replaced":
            summary.replaced += 1
        elif result.action == "expired":
            summary.expired += 1
        elif result.action == "cancelled":
            summary.cancelled += 1
        elif result.action == "kept":
            summary.kept += 1
        else:
            summary.errors += 1

        # レート制限
        time.sleep(settings.order_rate_limit_sleep)

    if summary.filled or summary.replaced or summary.expired:
        logger.info(
            "Order manager tick: checked=%d filled=%d replaced=%d expired=%d kept=%d errors=%d",
            summary.checked,
            summary.filled,
            summary.replaced,
            summary.expired,
            summary.kept,
            summary.errors,
        )

    return summary
