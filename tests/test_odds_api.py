"""Tests for Odds API connector."""

from __future__ import annotations

import pytest

from src.connectors.odds_api import (
    BookmakerOdds,
    GameOdds,
    TeamOdds,
    american_to_prob,
    fetch_nba_odds,
)


class TestAmericanToProb:
    def test_negative_odds(self):
        assert american_to_prob(-200) == pytest.approx(0.6667, abs=0.001)

    def test_positive_odds(self):
        assert american_to_prob(150) == pytest.approx(0.4, abs=0.001)

    def test_even_odds(self):
        assert american_to_prob(100) == pytest.approx(0.5, abs=0.001)

    def test_heavy_favorite(self):
        assert american_to_prob(-500) == pytest.approx(0.8333, abs=0.001)

    def test_big_underdog(self):
        assert american_to_prob(500) == pytest.approx(0.1667, abs=0.001)


class TestConsensusProbs:
    def test_single_bookmaker(self):
        game = GameOdds(
            game_id="g1",
            home_team="Boston Celtics",
            away_team="New York Knicks",
            commence_time="2026-02-08T00:00:00Z",
            bookmakers=[
                BookmakerOdds("bk1", "", [
                    TeamOdds("Boston Celtics", -200, 0.6667),
                    TeamOdds("New York Knicks", +150, 0.4),
                ]),
            ],
        )
        probs = game.consensus_probs
        # vig removed: probs should sum to ~1.0
        assert probs["Boston Celtics"] + probs["New York Knicks"] == pytest.approx(1.0, abs=0.01)
        assert probs["Boston Celtics"] > probs["New York Knicks"]

    def test_multiple_bookmakers_average(self, sample_game_odds: GameOdds):
        probs = sample_game_odds.consensus_probs
        assert probs["Boston Celtics"] + probs["New York Knicks"] == pytest.approx(1.0, abs=0.01)
        # Consensus should be between the two bookmakers' individual vig-removed probs
        assert 0.6 < probs["Boston Celtics"] < 0.7

    def test_no_bookmakers(self):
        game = GameOdds("g2", "A", "B", "", bookmakers=[])
        assert game.consensus_probs == {}


class TestFetchNbaOdds:
    def test_api_key_not_set(self, monkeypatch):
        monkeypatch.setattr("src.connectors.odds_api.settings.odds_api_key", "")
        with pytest.raises(ValueError, match="ODDS_API_KEY"):
            fetch_nba_odds()

    def test_successful_fetch(self, monkeypatch):
        api_response = [
            {
                "id": "game1",
                "home_team": "Boston Celtics",
                "away_team": "New York Knicks",
                "commence_time": "2026-02-08T00:00:00Z",
                "bookmakers": [
                    {
                        "key": "fanduel",
                        "last_update": "2026-02-07T23:00:00Z",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Boston Celtics", "price": -200},
                                    {"name": "New York Knicks", "price": 150},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]

        class MockResponse:
            headers = {"x-requests-remaining": "499"}

            def raise_for_status(self):
                pass

            def json(self):
                return api_response

        monkeypatch.setattr("src.connectors.odds_api.settings.odds_api_key", "test-key")
        monkeypatch.setattr("src.connectors.odds_api.httpx.get", lambda *a, **kw: MockResponse())

        games = fetch_nba_odds()
        assert len(games) == 1
        assert games[0].home_team == "Boston Celtics"
        assert len(games[0].bookmakers) == 1
        assert len(games[0].bookmakers[0].outcomes) == 2

    def test_empty_response(self, monkeypatch):
        class MockResponse:
            headers = {"x-requests-remaining": "498"}

            def raise_for_status(self):
                pass

            def json(self):
                return []

        monkeypatch.setattr("src.connectors.odds_api.settings.odds_api_key", "test-key")
        monkeypatch.setattr("src.connectors.odds_api.httpx.get", lambda *a, **kw: MockResponse())

        games = fetch_nba_odds()
        assert games == []
