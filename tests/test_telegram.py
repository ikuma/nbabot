"""Tests for Telegram notification module."""

from __future__ import annotations

from src.notifications.telegram import format_opportunities, send_message
from src.strategy.scanner import Opportunity


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
