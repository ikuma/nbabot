"""Tests for Telegram notification module."""

from __future__ import annotations

import re

import httpx
import pytest

from src.notifications.telegram import format_opportunities, send_message
from src.settlement.settler import AutoSettleSummary, SettleResult
from src.strategy.scanner import Opportunity


# ---------------------------------------------------------------------------
# Telegram Markdown V1 safety helper
# ---------------------------------------------------------------------------

# Telegram Markdown V1 の特殊文字: _ * [ ` (bold, italic, link, code)
# エスケープ済み (\_ \[ 等) は許可、未エスケープの _ [ は不許可
_UNESCAPED_UNDERSCORE = re.compile(r"(?<!\\)_")
_UNESCAPED_BRACKET = re.compile(r"(?<!\\)\[")


def assert_telegram_markdown_safe(text: str) -> None:
    """Assert text has no unescaped _ or [ outside of intentional Markdown.

    Telegram Markdown V1 treats _ as italic, [ as link start.
    Unescaped occurrences in data strings (e.g. nba_scores, [MERGE])
    cause HTTP 400 parse errors.
    """
    # *bold* は意図的 → 許可。ただし * が奇数だとエラー
    asterisks = text.count("*")
    assert asterisks % 2 == 0, f"Unbalanced * in Telegram message: {asterisks} asterisks"

    # _ のチェック: \_ 以外の _ は危険
    unescaped = _UNESCAPED_UNDERSCORE.findall(text)
    assert not unescaped, (
        f"Unescaped underscore(s) in Telegram message (causes italic parse error): "
        f"found {len(unescaped)} in: {text!r}"
    )

    # [ のチェック: \[ 以外の [ はリンクパースエラーの可能性
    unescaped_brackets = _UNESCAPED_BRACKET.findall(text)
    assert not unescaped_brackets, (
        f"Unescaped bracket(s) in Telegram message (causes link parse error): "
        f"found {len(unescaped_brackets)} in: {text!r}"
    )


# ---------------------------------------------------------------------------
# TestFormatOpportunities
# ---------------------------------------------------------------------------


class TestFormatOpportunities:
    def test_empty_list(self):
        result = format_opportunities([])
        assert "No" in result
        assert "found" in result

    def test_with_opportunities(self):
        opp = Opportunity(
            game_title="Knicks vs Celtics",
            event_slug="nba-nyk-bos-2026-02-08",
            team="Boston Celtics",
            poly_price=0.55,
            book_prob=0.65,
            edge_pct=10.0,
            side="BUY",
            token_id="token1",
            kelly_size=25.0,
            bookmakers_count=3,
        )
        result = format_opportunities([opp])
        assert "Knicks vs Celtics" in result
        assert "BUY" in result
        assert "Boston Celtics" in result
        assert "10.0%" in result
        assert "$25" in result

    def test_opportunities_markdown_safe(self):
        """format_opportunities output must be valid Telegram Markdown."""
        opp = Opportunity(
            game_title="Knicks vs Celtics",
            event_slug="nba-nyk-bos-2026-02-08",
            team="Boston Celtics",
            poly_price=0.55,
            book_prob=0.65,
            edge_pct=10.0,
            side="BUY",
            token_id="token1",
            kelly_size=25.0,
            bookmakers_count=3,
        )
        result = format_opportunities([opp])
        assert_telegram_markdown_safe(result)


# ---------------------------------------------------------------------------
# TestSendMessage
# ---------------------------------------------------------------------------


class TestSendMessage:
    def test_not_configured(self, monkeypatch):
        monkeypatch.setattr("src.notifications.telegram.settings.telegram_bot_token", "")
        monkeypatch.setattr("src.notifications.telegram.settings.telegram_chat_id", "")
        assert send_message("test") is False

    def test_success(self, monkeypatch):
        monkeypatch.setattr("src.notifications.telegram.settings.telegram_bot_token", "fake-token")
        monkeypatch.setattr("src.notifications.telegram.settings.telegram_chat_id", "12345")

        class MockResponse:
            def raise_for_status(self):
                pass

        monkeypatch.setattr(
            "src.notifications.telegram.httpx.post",
            lambda *a, **kw: MockResponse(),
        )
        assert send_message("hello") is True

    def test_failure(self, monkeypatch):
        monkeypatch.setattr("src.notifications.telegram.settings.telegram_bot_token", "fake-token")
        monkeypatch.setattr("src.notifications.telegram.settings.telegram_chat_id", "12345")

        def raise_error(*a, **kw):
            raise Exception("network error")

        monkeypatch.setattr("src.notifications.telegram.httpx.post", raise_error)
        assert send_message("hello") is False

    def test_markdown_400_falls_back_to_plain_text(self, monkeypatch):
        """HTTP 400 (Markdown parse error) → retry without parse_mode → success."""
        monkeypatch.setattr("src.notifications.telegram.settings.telegram_bot_token", "fake-token")
        monkeypatch.setattr("src.notifications.telegram.settings.telegram_chat_id", "12345")

        call_log: list[dict] = []

        def mock_post(url, *, json, timeout=10):
            call_log.append(json)
            if "parse_mode" in json:
                # Markdown パースエラーをシミュレート
                resp = httpx.Response(400, request=httpx.Request("POST", url))
                raise httpx.HTTPStatusError("Bad Request", request=resp.request, response=resp)

            class OkResponse:
                def raise_for_status(self):
                    pass

            return OkResponse()

        monkeypatch.setattr("src.notifications.telegram.httpx.post", mock_post)

        result = send_message("text with _underscore_")
        assert result is True
        assert len(call_log) == 2
        # 1回目: Markdown 付き
        assert "parse_mode" in call_log[0]
        # 2回目: parse_mode なし (plain text)
        assert "parse_mode" not in call_log[1]

    def test_non_400_error_no_fallback(self, monkeypatch):
        """HTTP 500 → no fallback, just fail."""
        monkeypatch.setattr("src.notifications.telegram.settings.telegram_bot_token", "fake-token")
        monkeypatch.setattr("src.notifications.telegram.settings.telegram_chat_id", "12345")

        def mock_post(url, *, json, timeout=10):
            resp = httpx.Response(500, request=httpx.Request("POST", url))
            raise httpx.HTTPStatusError("Server Error", request=resp.request, response=resp)

        monkeypatch.setattr("src.notifications.telegram.httpx.post", mock_post)

        result = send_message("hello")
        assert result is False


# ---------------------------------------------------------------------------
# TestFormatSummaryMarkdownSafety — format_summary の Telegram 互換性
# ---------------------------------------------------------------------------


class TestFormatSummaryMarkdownSafety:
    """Verify format_summary() output is valid Telegram Markdown V1.

    This class would have caught the nba_scores underscore bug:
    method='nba_scores' → unescaped _ → Telegram 400.
    """

    def test_regular_settle_markdown_safe(self):
        """Regular WIN/LOSS with nba_scores method."""
        summary = AutoSettleSummary(
            settled=[
                SettleResult(1, "Celtics", True, 75.0, "nba_scores"),
                SettleResult(2, "Knicks", False, -50.0, "nba_scores"),
            ],
            skipped=1,
        )
        text = summary.format_summary()
        assert_telegram_markdown_safe(text)
        # 内容も確認
        assert "nba\\_scores" in text
        assert "WIN" in text
        assert "LOSS" in text

    def test_polymarket_method_no_underscore(self):
        """Polymarket method has no underscore — still safe."""
        summary = AutoSettleSummary(
            settled=[SettleResult(1, "Celtics", True, 10.0, "polymarket")],
        )
        text = summary.format_summary()
        assert_telegram_markdown_safe(text)

    def test_bothside_format_markdown_safe(self):
        """BOTHSIDE format with [BOTHSIDE] bracket must be escaped."""
        summary = AutoSettleSummary(
            settled=[
                SettleResult(
                    signal_id=10,
                    team="Celtics",
                    won=True,
                    pnl=8.0,
                    method="nba_scores",
                    is_bothside=True,
                    dir_pnl=5.0,
                    hedge_pnl=3.0,
                ),
            ],
        )
        text = summary.format_summary()
        assert_telegram_markdown_safe(text)
        assert "BOTHSIDE" in text

    def test_merge_format_markdown_safe(self):
        """MERGE format with [MERGE] bracket must be escaped."""
        summary = AutoSettleSummary(
            settled=[
                SettleResult(
                    signal_id=20,
                    team="Lakers",
                    won=True,
                    pnl=15.0,
                    method="nba_scores",
                    is_bothside=True,
                    is_merged=True,
                    merge_pnl=12.0,
                    remainder_pnl=3.0,
                ),
            ],
        )
        text = summary.format_summary()
        assert_telegram_markdown_safe(text)
        assert "MERGE" in text

    def test_all_variants_combined(self):
        """Mixed settle results: regular + bothside + merge."""
        summary = AutoSettleSummary(
            settled=[
                SettleResult(1, "Celtics", True, 75.0, "nba_scores"),
                SettleResult(2, "Knicks", False, -50.0, "polymarket"),
                SettleResult(
                    signal_id=3, team="Lakers", won=True, pnl=8.0,
                    method="nba_scores", is_bothside=True,
                    dir_pnl=5.0, hedge_pnl=3.0,
                ),
                SettleResult(
                    signal_id=4, team="Warriors", won=True, pnl=15.0,
                    method="nba_scores", is_bothside=True, is_merged=True,
                    merge_pnl=12.0, remainder_pnl=3.0,
                ),
            ],
            skipped=2,
        )
        text = summary.format_summary()
        assert_telegram_markdown_safe(text)

    def test_empty_summary_markdown_safe(self):
        """Empty summary should also be safe."""
        summary = AutoSettleSummary()
        text = summary.format_summary()
        assert_telegram_markdown_safe(text)

    def test_hypothetical_underscore_method(self):
        """Any method name with underscores must be escaped."""
        summary = AutoSettleSummary(
            settled=[
                SettleResult(1, "Celtics", True, 10.0, "some_custom_method"),
            ],
        )
        text = summary.format_summary()
        assert_telegram_markdown_safe(text)
        assert "some\\_custom\\_method" in text
