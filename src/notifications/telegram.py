"""Telegram notification sender."""

from __future__ import annotations

import logging

import httpx

from src.config import settings

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def send_message(text: str, parse_mode: str = "Markdown") -> bool:
    """Send a message via Telegram bot API. Returns True on success.

    Falls back to plain text if Markdown parsing fails (HTTP 400).
    """
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        logger.warning("Telegram not configured, skipping notification")
        return False

    url = TELEGRAM_API.format(token=settings.telegram_bot_token)

    try:
        resp = httpx.post(
            url,
            json={
                "chat_id": settings.telegram_chat_id,
                "text": text,
                "parse_mode": parse_mode,
            },
            timeout=10,
        )
        resp.raise_for_status()
        return True
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 400 and parse_mode:
            # Markdown パースエラー → plain text でリトライ
            logger.warning("Telegram Markdown parse failed, retrying as plain text")
            try:
                resp2 = httpx.post(
                    url,
                    json={
                        "chat_id": settings.telegram_chat_id,
                        "text": text,
                    },
                    timeout=10,
                )
                resp2.raise_for_status()
                return True
            except Exception:
                logger.exception("Telegram plain text fallback also failed")
                return False
        logger.error("Telegram HTTP error %d: %s", e.response.status_code, e)
        return False
    except httpx.TimeoutException:
        logger.warning("Telegram request timed out")
        return False
    except Exception:
        logger.exception("Failed to send Telegram message")
        return False


def format_order_notification(
    team: str,
    price: float,
    size: float,
    order_id: str,
    event_title: str = "",
    edge_pct: float = 0.0,
) -> str:
    """Format a live order notification for Telegram."""
    lines = [
        "*Order Placed*",
        f"BUY {team} @ {price:.3f} | ${size:.0f}",
    ]
    if event_title:
        lines.append(f"Game: {event_title}")
    if edge_pct:
        lines.append(f"Edge: {edge_pct:.1f}%")
    lines.append(f"Order: `{order_id}`")
    return "\n".join(lines)


def send_risk_alert(level: str, trigger: str, daily_pnl: float = 0.0) -> bool:
    """Send circuit breaker level change alert."""
    text = (
        f"*Risk Alert: {level}*\n"
        f"Trigger: {trigger}\n"
        f"Daily PnL: ${daily_pnl:+.2f}"
    )
    return send_message(text)


def send_health_alert(messages: list[str]) -> bool:
    """Send health check failure notification."""
    text = "*Health Check Alert*\n" + "\n".join(f"- {m}" for m in messages)
    return send_message(text)


def send_error_alert(error_type: str, message: str) -> bool:
    """Send error notification (order failure, etc.)."""
    text = f"*Error: {error_type}*\n{message}"
    return send_message(text)


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
