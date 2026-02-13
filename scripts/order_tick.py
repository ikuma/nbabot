#!/usr/bin/env python3
"""Order lifecycle manager: 2-minute tick for monitoring and managing placed orders.

Usage:
    python scripts/order_tick.py

Intended to run via launchd every 120 seconds. Checks placed orders for:
- Fill detection
- TTL expiry → cancel/re-place at new best_ask
- Max replace count → final cancel

Only active in live mode. Paper/dry-run mode exits immediately.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


def main() -> None:
    from src.config import settings
    from src.scheduler.order_manager import check_and_manage_orders
    from src.store.db_path import resolve_db_path

    execution_mode = settings.execution_mode
    db_path = resolve_db_path(execution_mode=execution_mode)

    # paper/dry-run は即座に終了
    if execution_mode != "live":
        log.debug("Order manager: skipped (execution_mode=%s)", execution_mode)
        return

    if not settings.order_manager_enabled:
        log.debug("Order manager: disabled")
        return

    log.info("=== Order manager tick ===")

    try:
        summary = check_and_manage_orders(execution_mode=execution_mode, db_path=db_path)
    except Exception:
        log.exception("Order manager tick failed")
        return

    # Telegram サマリー (fill/replace があった場合のみ)
    if summary.filled or summary.replaced or summary.expired:
        try:
            from src.notifications.telegram import send_message

            lines = [
                "*Order Manager Tick*",
                f"Checked: {summary.checked} | Filled: {summary.filled} "
                f"| Replaced: {summary.replaced} | Expired: {summary.expired}",
            ]
            send_message("\n".join(lines))
        except Exception:
            log.debug("Telegram notification failed", exc_info=True)

    # Heartbeat (watchdog 死活監視用)
    heartbeat = Path(__file__).resolve().parent.parent / "data" / "heartbeat_ordermgr"
    heartbeat.parent.mkdir(parents=True, exist_ok=True)
    heartbeat.write_text(datetime.now(timezone.utc).isoformat() + "\n")


if __name__ == "__main__":
    main()
