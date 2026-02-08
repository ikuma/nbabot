"""Tests for Polymarket connector parser utilities."""

from __future__ import annotations

from src.connectors.polymarket import (
    MarketToken,
    NBAMarket,
    _is_nba_market,
    _parse_json_or_csv,
    _parse_market,
)


class TestIsNbaMarket:
    def test_nba_prefix(self):
        assert _is_nba_market({"question": "NBA: Will the Celtics win?", "description": ""})
        assert _is_nba_market({"question": "NBA game tonight", "description": ""})

    def test_nba_in_description(self):
        mkt = {"question": "Who wins tonight?", "description": "NBA regular season"}
        assert _is_nba_market(mkt)

    def test_not_nba(self):
        assert not _is_nba_market({"question": "Will Bitcoin hit 100k?", "description": ""})
        assert not _is_nba_market({"question": "NFL game tonight", "description": "football"})

    def test_no_false_positive_substring(self):
        # "nba" embedded in another word should not match
        assert not _is_nba_market({"question": "Barnabas wins prize", "description": ""})


class TestParseJsonOrCsv:
    def test_json_array_string(self):
        result = _parse_json_or_csv('["Yes", "No"]')
        assert result == ["Yes", "No"]

    def test_csv_string(self):
        result = _parse_json_or_csv("Knicks, Celtics")
        assert result == ["Knicks", "Celtics"]

    def test_empty_string(self):
        assert _parse_json_or_csv("") == []

    def test_list_input(self):
        assert _parse_json_or_csv(["a", "b"]) == ["a", "b"]

    def test_list_with_numbers(self):
        assert _parse_json_or_csv([1, 2]) == ["1", "2"]

    def test_json_with_numbers(self):
        result = _parse_json_or_csv("[0.45, 0.55]")
        assert result == ["0.45", "0.55"]


class TestParseMarket:
    def test_basic_parsing(self):
        raw = {
            "condition_id": "cond123",
            "question": "NBA: Celtics vs Knicks?",
            "tokens": [
                {"token_id": "t1", "outcome": "YES", "price": 0.6},
                {"token_id": "t2", "outcome": "NO", "price": 0.4},
            ],
            "end_date_iso": "2026-02-09T00:00:00Z",
            "active": True,
            "slug": "nba-test",
        }
        market = _parse_market(raw)
        assert market.condition_id == "cond123"
        assert market.question == "NBA: Celtics vs Knicks?"
        assert len(market.tokens) == 2
        assert market.active is True
        assert market.slug == "nba-test"

    def test_missing_fields(self):
        market = _parse_market({})
        assert market.condition_id == ""
        assert market.question == ""
        assert market.tokens == []
        assert market.active is False


class TestNBAMarketProperties:
    def test_yes_price(self):
        market = NBAMarket(
            condition_id="c1",
            question="Test",
            tokens=[
                MarketToken("t1", "Yes", 0.6),
                MarketToken("t2", "No", 0.4),
            ],
            end_date="",
            active=True,
        )
        assert market.yes_price == 0.6
        assert market.no_price == 0.4

    def test_no_matching_token(self):
        market = NBAMarket(
            condition_id="c1",
            question="Test",
            tokens=[MarketToken("t1", "Celtics", 0.6)],
            end_date="",
            active=True,
        )
        assert market.yes_price is None
        assert market.no_price is None
