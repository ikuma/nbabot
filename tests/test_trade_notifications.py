"""Tests for Phase N trade notification functions."""

from __future__ import annotations

import re

from src.notifications.telegram import (
    escape_md,
    notify_dca,
    notify_hedge,
    notify_merge,
    notify_tick_header,
    notify_trade,
)
from src.settlement.settler import AutoSettleSummary, SettleResult

# Telegram Markdown V1 safety — reuse pattern from test_telegram.py
_UNESCAPED_UNDERSCORE = re.compile(r"(?<!\\)_")
_UNESCAPED_BRACKET = re.compile(r"(?<!\\)\[")


def assert_telegram_markdown_safe(text: str) -> None:
    asterisks = text.count("*")
    assert asterisks % 2 == 0, f"Unbalanced * in: {text!r}"
    assert not _UNESCAPED_UNDERSCORE.findall(text), f"Unescaped _ in: {text!r}"
    assert not _UNESCAPED_BRACKET.findall(text), f"Unescaped [ in: {text!r}"


# ---------------------------------------------------------------------------
# escape_md
# ---------------------------------------------------------------------------


class TestEscapeMd:
    def test_underscores(self):
        assert escape_md("nba_scores") == "nba\\_scores"

    def test_brackets(self):
        assert escape_md("[SWEET]") == "\\[SWEET\\]"

    def test_parens(self):
        assert escape_md("(test)") == "\\(test\\)"

    def test_no_special(self):
        assert escape_md("hello world") == "hello world"

    def test_mixed(self):
        result = escape_md("a_b[c](d)")
        assert "\\_" in result
        assert "\\[" in result
        assert "\\(" in result


# ---------------------------------------------------------------------------
# notify_trade
# ---------------------------------------------------------------------------


class TestNotifyTrade:
    def _capture(self, monkeypatch):
        messages = []

        def fake_send(text, parse_mode="Markdown"):
            messages.append(text)
            return True

        monkeypatch.setattr("src.notifications.telegram.send_message", fake_send)
        return messages

    def test_basic_format(self, monkeypatch):
        msgs = self._capture(monkeypatch)
        result = notify_trade(
            outcome_name="New York Knicks",
            event_slug="nba-nyk-bos-2026-02-11",
            order_price=0.370,
            best_ask=0.380,
            size_usd=42,
            edge_pct=26.1,
            price_band="0.35-0.40",
            in_sweet_spot=True,
            expected_win_rate=0.904,
            dca_seq=1,
            dca_max=5,
            signal_id=39,
        )
        assert result is True
        assert len(msgs) == 1
        text = msgs[0]
        assert "*BUY New York Knicks* #39" in text
        assert "NYK @ BOS | 2026-02-11" in text
        assert "Price: `0.370`" in text
        assert "Edge: `26.1%`" in text
        assert "DCA 1/5" in text
        assert "LLM" not in text
        assert_telegram_markdown_safe(text)

    def test_with_llm(self, monkeypatch):
        msgs = self._capture(monkeypatch)
        notify_trade(
            outcome_name="NYK",
            event_slug="nba-nyk-bos-2026-02-11",
            order_price=0.370,
            best_ask=0.380,
            size_usd=42,
            edge_pct=26.1,
            price_band="0.35-0.40",
            in_sweet_spot=False,
            expected_win_rate=0.904,
            dca_seq=1,
            dca_max=5,
            llm_favored="NYK",
            llm_confidence=0.72,
            llm_sizing=1.2,
        )
        text = msgs[0]
        assert "LLM: NYK" in text
        assert "(0.72)" in text
        assert "x1.20" in text
        assert_telegram_markdown_safe(text)

    def test_slug_underscores_escaped(self, monkeypatch):
        """Event slug with underscores must be safe."""
        msgs = self._capture(monkeypatch)
        notify_trade(
            outcome_name="Test",
            event_slug="nba-nyk-bos-2026-02-11",
            order_price=0.3,
            best_ask=0.31,
            size_usd=10,
            edge_pct=5.0,
            price_band="0.30-0.35",
            in_sweet_spot=False,
            expected_win_rate=0.8,
            dca_seq=1,
            dca_max=3,
        )
        assert_telegram_markdown_safe(msgs[0])


# ---------------------------------------------------------------------------
# notify_hedge
# ---------------------------------------------------------------------------


class TestNotifyHedge:
    def _capture(self, monkeypatch):
        messages = []

        def fake_send(text, parse_mode="Markdown"):
            messages.append(text)
            return True

        monkeypatch.setattr("src.notifications.telegram.send_message", fake_send)
        return messages

    def test_basic_format(self, monkeypatch):
        msgs = self._capture(monkeypatch)
        result = notify_hedge(
            outcome_name="Boston Celtics",
            event_slug="nba-nyk-bos-2026-02-11",
            order_price=0.580,
            best_ask=0.590,
            size_usd=21,
            dir_vwap=0.374,
            combined_vwap=0.954,
            target_combined=0.970,
            dca_seq=1,
            dca_max=5,
            edge_pct=4.0,
            signal_id=41,
        )
        assert result is True
        text = msgs[0]
        assert "*HEDGE Boston Celtics* #41" in text
        assert "NYK @ BOS | 2026-02-11" in text
        assert "Dir VWAP: 0.374" in text
        assert "Combined: `0.954`" in text
        assert_telegram_markdown_safe(text)


# ---------------------------------------------------------------------------
# notify_dca
# ---------------------------------------------------------------------------


class TestNotifyDca:
    def _capture(self, monkeypatch):
        messages = []

        def fake_send(text, parse_mode="Markdown"):
            messages.append(text)
            return True

        monkeypatch.setattr("src.notifications.telegram.send_message", fake_send)
        return messages

    def test_basic_format(self, monkeypatch):
        msgs = self._capture(monkeypatch)
        result = notify_dca(
            outcome_name="New York Knicks",
            event_slug="nba-nyk-bos-2026-02-11",
            order_price=0.350,
            size_usd=42,
            old_vwap=0.371,
            new_vwap=0.365,
            dca_seq=3,
            dca_max=5,
            trigger_reason="price_dip (-2.1%)",
            signal_id=40,
        )
        assert result is True
        text = msgs[0]
        assert "*DCA 3/5 New York Knicks* #40" in text
        assert "NYK @ BOS | 2026-02-11" in text
        assert "VWAP: 0.371" in text
        assert "`0.365`" in text
        assert "price\\_dip" in text
        assert_telegram_markdown_safe(text)


# ---------------------------------------------------------------------------
# notify_merge
# ---------------------------------------------------------------------------


class TestNotifyMerge:
    def _capture(self, monkeypatch):
        messages = []

        def fake_send(text, parse_mode="Markdown"):
            messages.append(text)
            return True

        monkeypatch.setattr("src.notifications.telegram.send_message", fake_send)
        return messages

    def test_basic_format(self, monkeypatch):
        msgs = self._capture(monkeypatch)
        result = notify_merge(
            event_slug="nba-nyk-bos-2026-02-11",
            merge_shares=150,
            combined_vwap=0.9730,
            gross_profit=4.50,
            gas_cost=0.45,
            net_profit=4.05,
            remainder_shares=30,
            remainder_side="directional",
        )
        assert result is True
        text = msgs[0]
        assert "*MERGE* NYK @ BOS | 2026-02-11" in text
        assert "`150`" in text
        assert "`0.9730`" in text
        assert "+$4.05" in text
        assert "30 directional shares" in text
        assert_telegram_markdown_safe(text)

    def test_no_remainder(self, monkeypatch):
        msgs = self._capture(monkeypatch)
        notify_merge(
            event_slug="nba-nyk-bos-2026-02-11",
            merge_shares=100,
            combined_vwap=0.9500,
            gross_profit=5.0,
            gas_cost=0.10,
            net_profit=4.90,
            remainder_shares=0,
            remainder_side=None,
        )
        text = msgs[0]
        assert "Remainder" not in text
        assert_telegram_markdown_safe(text)


# ---------------------------------------------------------------------------
# notify_tick_header
# ---------------------------------------------------------------------------


class TestNotifyTickHeader:
    def test_format(self):
        result = notify_tick_header("2026-02-11", found=6, window=3, pending=2)
        assert "*Tick*" in result
        assert "2026-02-11" in result
        assert "Games: 6" in result
        assert "Window: 3" in result
        assert "Pending: 2" in result
        assert_telegram_markdown_safe(result)

    def test_with_execution_mode(self):
        result = notify_tick_header(
            "2026-02-11", found=6, window=3, pending=2, execution_mode="live"
        )
        assert "| live" in result
        assert_telegram_markdown_safe(result)


# ---------------------------------------------------------------------------
# Enriched settle summary (Phase N)
# ---------------------------------------------------------------------------


class TestEnrichedSettleSummary:
    def test_score_displayed(self):
        summary = AutoSettleSummary(
            settled=[
                SettleResult(
                    signal_id=45,
                    team="NYK",
                    won=True,
                    pnl=15.20,
                    method="nba_scores",
                    total_cost=35.0,
                    away_score=112,
                    home_score=105,
                ),
            ],
        )
        text = summary.format_summary()
        assert "Score: 112-105" in text
        assert "ROI" in text
        assert_telegram_markdown_safe(text)

    def test_roi_calculation(self):
        summary = AutoSettleSummary(
            settled=[
                SettleResult(
                    signal_id=1,
                    team="BOS",
                    won=True,
                    pnl=50.0,
                    method="nba_scores",
                    total_cost=100.0,
                ),
            ],
        )
        text = summary.format_summary()
        assert "ROI +50.0%" in text

    def test_no_score_no_line(self):
        summary = AutoSettleSummary(
            settled=[
                SettleResult(
                    signal_id=1,
                    team="BOS",
                    won=True,
                    pnl=10.0,
                    method="polymarket",
                    total_cost=25.0,
                ),
            ],
        )
        text = summary.format_summary()
        assert "Score:" not in text
        assert "ROI" in text

    def test_zero_cost_no_roi(self):
        summary = AutoSettleSummary(
            settled=[
                SettleResult(
                    signal_id=1,
                    team="BOS",
                    won=False,
                    pnl=-10.0,
                    method="nba_scores",
                    total_cost=0.0,
                ),
            ],
        )
        text = summary.format_summary()
        assert "ROI" not in text

    def test_merge_with_score_and_roi(self):
        summary = AutoSettleSummary(
            settled=[
                SettleResult(
                    signal_id=20,
                    team="LAL",
                    won=True,
                    pnl=4.55,
                    method="nba_scores",
                    is_bothside=True,
                    is_merged=True,
                    merge_pnl=4.05,
                    remainder_pnl=0.50,
                    total_cost=80.0,
                    away_score=118,
                    home_score=110,
                ),
            ],
        )
        text = summary.format_summary()
        assert "MERGE" in text
        assert "Score: 118-110" in text
        assert "ROI" in text
        assert_telegram_markdown_safe(text)
