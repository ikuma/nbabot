"""Pre-trade checks for live execution.

Extracted from job_executor.py to keep file sizes under 500 lines.
"""

from __future__ import annotations

import logging

from src.config import settings

logger = logging.getLogger(__name__)


def preflight_check() -> bool:
    """Run pre-trade checks for live execution."""
    from datetime import date

    from src.connectors.polymarket import get_usdc_balance
    from src.store.db import get_pending_dca_exposure, get_todays_exposure, get_todays_live_orders

    try:
        if not settings.polymarket_private_key:
            logger.error("[preflight] POLYMARKET_PRIVATE_KEY not set")
            return False

        balance = get_usdc_balance()
        if balance < settings.min_balance_usd:
            logger.error(
                "[preflight] Balance $%.2f < minimum $%.2f",
                balance,
                settings.min_balance_usd,
            )
            return False

        today_str = date.today().strftime("%Y-%m-%d")
        order_count = get_todays_live_orders(today_str)
        if order_count >= settings.max_daily_positions:
            logger.error(
                "[preflight] Daily order limit reached: %d/%d",
                order_count,
                settings.max_daily_positions,
            )
            return False

        exposure = get_todays_exposure(today_str)
        pending_dca = get_pending_dca_exposure()
        total_potential = exposure + pending_dca
        if total_potential >= settings.max_daily_exposure_usd:
            logger.error(
                "[preflight] Potential exposure limit: $%.0f ($%.0f placed + $%.0f pending DCA) / $%.0f",
                total_potential,
                exposure,
                pending_dca,
                settings.max_daily_exposure_usd,
            )
            return False

        return True
    except Exception:
        logger.exception("[preflight] Check failed")
        return False
