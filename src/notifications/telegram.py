"""Telegram notification sender."""

from __future__ import annotations

import logging

import httpx

from src.config import settings

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def send_message(text: str, parse_mode: str = "Markdown") -> bool:
    """Send a message via Telegram bot API. Returns True on success."""
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        logger.warning("Telegram not configured, skipping notification")
        return False

    try:
        resp = httpx.post(
            TELEGRAM_API.format(token=settings.telegram_bot_token),
            json={
                "chat_id": settings.telegram_chat_id,
                "text": text,
                "parse_mode": parse_mode,
            },
            timeout=10,
        )
        resp.raise_for_status()
        return True
    except Exception:
        logger.exception("Failed to send Telegram message")
        return False


def format_opportunities(opportunities: list) -> str:
    """Format opportunities list into a Telegram message.

    Handles both Opportunity (bookmaker) and CalibrationOpportunity objects.
    """
    if not opportunities:
        return "No NBA edge opportunities found today."

    lines = ["*NBA Edge Report*\n"]
    for i, opp in enumerate(opportunities, 1):
        # CalibrationOpportunity has calibration_edge_pct attribute
        if hasattr(opp, "calibration_edge_pct"):
            spot = " \\[SWEET]" if opp.in_sweet_spot else ""
            lines.append(
                f"{i}. *{opp.event_title}*{spot}\n"
                f"   {opp.side} {opp.outcome_name} @ {opp.poly_price:.2f}\n"
                f"   Cal edge: {opp.calibration_edge_pct:.1f}%"
                f" | WR: {opp.expected_win_rate:.0%}\n"
                f"   EV/$: {opp.ev_per_dollar:.2f}"
                f" | Band: {opp.price_band}"
                f" | Size: ${opp.position_usd:.0f}"
            )
        else:
            lines.append(
                f"{i}. *{opp.game_title}*\n"
                f"   {opp.side} {opp.team} @ {opp.poly_price:.2f}"
                f" | Book: {opp.book_prob:.2f}\n"
                f"   Edge: {opp.edge_pct:.1f}%"
                f" | Kelly size: ${opp.kelly_size:.0f}\n"
                f"   Books: {opp.bookmakers_count}"
            )
    return "\n".join(lines)
