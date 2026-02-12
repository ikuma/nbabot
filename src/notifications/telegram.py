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


# ---------------------------------------------------------------------------
# Markdown V1 escape helper
# ---------------------------------------------------------------------------

# Telegram Markdown V1 special characters that can cause parse errors
_MD_V1_SPECIAL = str.maketrans(
    {"_": "\\_", "[": "\\[", "]": "\\]", "(": "\\(", ")": "\\)"}
)


def escape_md(text: str) -> str:
    """Escape Telegram Markdown V1 special characters in data strings."""
    return str(text).translate(_MD_V1_SPECIAL)


# ---------------------------------------------------------------------------
# Structured game info helper
# ---------------------------------------------------------------------------


def _format_game(event_slug: str) -> str:
    """Convert 'nba-mil-okc-2026-02-12' → 'MIL @ OKC | 2026-02-12'."""
    import re

    m = re.match(r"^nba-([a-z]{3})-([a-z]{3})-(\d{4}-\d{2}-\d{2})$", event_slug)
    if m:
        return f"{m.group(1).upper()} @ {m.group(2).upper()} | {m.group(3)}"
    return escape_md(event_slug)


_SEP = "\u2500" * 13  # ─────────────


# ---------------------------------------------------------------------------
# Instant trade notifications (Phase N)
# ---------------------------------------------------------------------------


def notify_trade(
    *,
    outcome_name: str,
    event_slug: str,
    order_price: float,
    best_ask: float,
    size_usd: float,
    edge_pct: float,
    price_band: str,
    in_sweet_spot: bool,
    expected_win_rate: float,
    dca_seq: int,
    dca_max: int,
    signal_id: int | None = None,
    llm_favored: str | None = None,
    llm_confidence: float | None = None,
    llm_sizing: float | None = None,
) -> bool:
    """Send instant notification for a directional trade."""
    try:
        sid = f" #{signal_id}" if signal_id else ""
        lines = [
            f"*BUY {escape_md(outcome_name)}*{sid}",
            _format_game(event_slug),
            _SEP,
            f"Price: `{order_price:.3f}` (ask {best_ask:.3f})",
            f"Size: `${size_usd:.0f}` | Edge: `{edge_pct:.1f}%`",
            f"WR: {expected_win_rate:.0%} | DCA {dca_seq}/{dca_max}",
        ]
        if llm_favored:
            conf = f" ({llm_confidence:.2f})" if llm_confidence is not None else ""
            sizing = f" x{llm_sizing:.2f}" if llm_sizing is not None else ""
            lines.append(f"LLM: {escape_md(llm_favored)}{conf}{sizing}")
        return send_message("\n".join(lines))
    except Exception:
        logger.debug("notify_trade failed", exc_info=True)
        return False


def notify_hedge(
    *,
    outcome_name: str,
    event_slug: str,
    order_price: float,
    best_ask: float,
    size_usd: float,
    dir_vwap: float,
    combined_vwap: float,
    target_combined: float,
    dca_seq: int,
    dca_max: int,
    edge_pct: float,
    signal_id: int | None = None,
) -> bool:
    """Send instant notification for a hedge trade."""
    try:
        sid = f" #{signal_id}" if signal_id else ""
        lines = [
            f"*HEDGE {escape_md(outcome_name)}*{sid}",
            _format_game(event_slug),
            _SEP,
            f"Price: `{order_price:.3f}` (ask {best_ask:.3f})",
            f"Size: `${size_usd:.0f}` | Edge: `{edge_pct:.1f}%`",
            f"Dir VWAP: {dir_vwap:.3f} | Combined: `{combined_vwap:.3f}`",
            f"DCA {dca_seq}/{dca_max}",
        ]
        return send_message("\n".join(lines))
    except Exception:
        logger.debug("notify_hedge failed", exc_info=True)
        return False


def notify_dca(
    *,
    outcome_name: str,
    event_slug: str,
    order_price: float,
    size_usd: float,
    old_vwap: float,
    new_vwap: float,
    dca_seq: int,
    dca_max: int,
    trigger_reason: str,
    signal_id: int | None = None,
) -> bool:
    """Send instant notification for a DCA entry."""
    try:
        sid = f" #{signal_id}" if signal_id else ""
        lines = [
            f"*DCA {dca_seq}/{dca_max} {escape_md(outcome_name)}*{sid}",
            _format_game(event_slug),
            _SEP,
            f"Price: `{order_price:.3f}` | Size: `${size_usd:.0f}`",
            f"VWAP: {old_vwap:.3f} \u2192 `{new_vwap:.3f}`",
            f"Trigger: {escape_md(trigger_reason)}",
        ]
        return send_message("\n".join(lines))
    except Exception:
        logger.debug("notify_dca failed", exc_info=True)
        return False


def notify_merge(
    *,
    event_slug: str,
    merge_shares: float,
    combined_vwap: float,
    gross_profit: float,
    gas_cost: float,
    net_profit: float,
    remainder_shares: float,
    remainder_side: str | None,
) -> bool:
    """Send instant notification for a MERGE result."""
    try:
        lines = [
            f"*MERGE* {_format_game(event_slug)}",
            _SEP,
            f"Shares: `{merge_shares:.0f}` | VWAP: `{combined_vwap:.4f}`",
            f"Profit: `+${net_profit:.2f}` (gross ${gross_profit:.2f} \\- gas ${gas_cost:.2f})",
        ]
        if remainder_shares > 0 and remainder_side:
            lines.append(f"Remainder: {remainder_shares:.0f} {escape_md(remainder_side)} shares")
        return send_message("\n".join(lines))
    except Exception:
        logger.debug("notify_merge failed", exc_info=True)
        return False


def notify_order_replaced(
    *,
    event_slug: str,
    outcome_name: str,
    old_price: float,
    new_price: float,
    best_ask: float,
    replace_count: int,
    max_replaces: int,
    signal_id: int | None = None,
) -> bool:
    """Send notification when an order is replaced at a new price."""
    try:
        sid = f" #{signal_id}" if signal_id else ""
        lines = [
            f"*REPLACE {replace_count}/{max_replaces}* {escape_md(outcome_name)}{sid}",
            _format_game(event_slug),
            f"{old_price:.3f} \u2192 `{new_price:.3f}` (ask {best_ask:.3f})",
        ]
        return send_message("\n".join(lines))
    except Exception:
        logger.debug("notify_order_replaced failed", exc_info=True)
        return False


def notify_order_filled_early(
    *,
    event_slug: str,
    outcome_name: str,
    fill_price: float,
    signal_id: int,
    size_usd: float | None = None,
) -> bool:
    """Send notification when order manager detects a fill."""
    try:
        size_part = f" | `${size_usd:.0f}`" if size_usd else ""
        lines = [
            f"*FILLED* {escape_md(outcome_name)} @ `{fill_price:.3f}` #{signal_id}",
            f"{_format_game(event_slug)}{size_part}",
        ]
        return send_message("\n".join(lines))
    except Exception:
        logger.debug("notify_order_filled_early failed", exc_info=True)
        return False


def notify_tick_header(
    game_date: str,
    found: int,
    window: int,
    pending: int,
    execution_mode: str = "",
) -> str:
    """Format tick summary header line. Returns formatted string (does not send)."""
    mode_label = f" | {execution_mode}" if execution_mode else ""
    return (
        f"*Tick* {game_date}{mode_label}\n"
        f"{_SEP}\n"
        f"Games: {found} | Window: {window} | Pending: {pending}"
    )
