"""Pre-trade checks for live execution.

Extracted from job_executor.py to keep file sizes under 500 lines.
"""

from __future__ import annotations

import logging

from src.config import settings

logger = logging.getLogger(__name__)


def _get_today_risk_stats(today_str: str) -> tuple[int, float, float]:
    """Load today's order/exposure stats used by preflight guards."""
    from src.store.db import get_pending_dca_exposure, get_todays_exposure, get_todays_live_orders

    order_count = get_todays_live_orders(today_str)
    exposure = get_todays_exposure(today_str)
    pending_dca = get_pending_dca_exposure()
    return order_count, exposure, pending_dca


def _is_exposure_limit_exceeded(exposure: float, pending_dca: float) -> bool:
    """Return True when potential exposure breaches configured daily limit."""
    total_potential = exposure + pending_dca
    if total_potential < settings.max_daily_exposure_usd:
        return False
    logger.error(
        "[preflight] Potential exposure limit: $%.0f "
        "($%.0f placed + $%.0f pending DCA) / $%.0f",
        total_potential,
        exposure,
        pending_dca,
        settings.max_daily_exposure_usd,
    )
    return True


def preflight_check() -> bool:
    """Run pre-trade checks for live execution."""
    from datetime import date

    from src.connectors.polymarket import get_usdc_balance
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
        order_count, exposure, pending_dca = _get_today_risk_stats(today_str)
        if order_count >= settings.max_daily_positions:
            logger.error(
                "[preflight] Daily order limit reached: %d/%d",
                order_count,
                settings.max_daily_positions,
            )
            return False

        if _is_exposure_limit_exceeded(exposure, pending_dca):
            return False

        return True
    except Exception:
        logger.exception("[preflight] Check failed")
        return False
