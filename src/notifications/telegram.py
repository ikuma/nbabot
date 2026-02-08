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
    """Format opportunities list into a Telegram message."""
    if not opportunities:
        return "No NBA edge opportunities found today."

    lines = ["*NBA Edge Report*\n"]
    for i, opp in enumerate(opportunities, 1):
        lines.append(
            f"{i}. *{opp.game_title}*\n"
            f"   {opp.side} {opp.team} @ {opp.poly_price:.2f} | Book: {opp.book_prob:.2f}\n"
            f"   Edge: {opp.edge_pct:.1f}% | Kelly size: ${opp.kelly_size:.0f}\n"
            f"   Books: {opp.bookmakers_count}"
        )
    return "\n".join(lines)
