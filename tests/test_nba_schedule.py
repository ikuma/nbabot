"""Tests for NBA.com schedule connector."""

from __future__ import annotations

from unittest.mock import patch

import httpx
import pytest

from src.connectors.nba_schedule import NBAGame, fetch_todays_games

_DUMMY_REQUEST = httpx.Request("GET", "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json")

SAMPLE_SCOREBOARD = {
    "scoreboard": {
        "games": [
            {
                "gameId": "0022500757",
                "gameTimeUTC": "2026-02-08T17:30:00Z",
                "gameStatus": 1,
                "homeTeam": {"teamCity": "Boston", "teamName": "Celtics"},
                "awayTeam": {"teamCity": "New York", "teamName": "Knicks"},
            },
            {
                "gameId": "0022500758",
                "gameTimeUTC": "2026-02-08T20:00:00Z",
                "gameStatus": 2,
                "homeTeam": {"teamCity": "Miami", "teamName": "Heat"},
                "awayTeam": {"teamCity": "Washington", "teamName": "Wizards"},
            },
        ]
    }
}


class TestFetchTodaysGames:
    @patch("src.connectors.nba_schedule.httpx.get")
    def test_parses_games(self, mock_get):
        mock_resp = httpx.Response(200, request=_DUMMY_REQUEST, json=SAMPLE_SCOREBOARD)
        mock_get.return_value = mock_resp

        games = fetch_todays_games()

        assert len(games) == 2
        assert games[0].game_id == "0022500757"
        assert games[0].home_team == "Boston Celtics"
        assert games[0].away_team == "New York Knicks"
        assert games[0].game_time_utc == "2026-02-08T17:30:00Z"
        assert games[0].game_status == 1

    @patch("src.connectors.nba_schedule.httpx.get")
    def test_team_name_concatenation(self, mock_get):
        """teamCity + " " + teamName forms the full name."""
        mock_resp = httpx.Response(200, request=_DUMMY_REQUEST, json=SAMPLE_SCOREBOARD)
        mock_get.return_value = mock_resp

        games = fetch_todays_games()

        assert games[1].home_team == "Miami Heat"
        assert games[1].away_team == "Washington Wizards"

    @patch("src.connectors.nba_schedule.httpx.get")
    def test_empty_games(self, mock_get):
        mock_resp = httpx.Response(200, request=_DUMMY_REQUEST, json={"scoreboard": {"games": []}})
        mock_get.return_value = mock_resp

        games = fetch_todays_games()
        assert games == []

    @patch("src.connectors.nba_schedule.httpx.get")
    def test_empty_scoreboard(self, mock_get):
        mock_resp = httpx.Response(200, request=_DUMMY_REQUEST, json={})
        mock_get.return_value = mock_resp

        games = fetch_todays_games()
        assert games == []

    @patch("src.connectors.nba_schedule.httpx.get")
    def test_network_error(self, mock_get):
        mock_get.side_effect = httpx.ConnectError("Connection refused")

        games = fetch_todays_games()
        assert games == []

    @patch("src.connectors.nba_schedule.httpx.get")
    def test_http_error(self, mock_get):
        mock_resp = httpx.Response(500, request=_DUMMY_REQUEST)
        mock_get.return_value = mock_resp

        games = fetch_todays_games()
        assert games == []

    @patch("src.connectors.nba_schedule.httpx.get")
    def test_skips_missing_team_info(self, mock_get):
        data = {
            "scoreboard": {
                "games": [
                    {
                        "gameId": "001",
                        "gameTimeUTC": "2026-02-08T17:30:00Z",
                        "gameStatus": 1,
                        "homeTeam": {"teamCity": "", "teamName": ""},
                        "awayTeam": {"teamCity": "New York", "teamName": "Knicks"},
                    },
                    {
                        "gameId": "002",
                        "gameTimeUTC": "2026-02-08T20:00:00Z",
                        "gameStatus": 1,
                        "homeTeam": {"teamCity": "Boston", "teamName": "Celtics"},
                        "awayTeam": {"teamCity": "New York", "teamName": "Knicks"},
                    },
                ]
            }
        }
        mock_resp = httpx.Response(200, request=_DUMMY_REQUEST, json=data)
        mock_get.return_value = mock_resp

        games = fetch_todays_games()
        assert len(games) == 1
        assert games[0].game_id == "002"

    @patch("src.connectors.nba_schedule.httpx.get")
    def test_all_game_statuses_returned(self, mock_get):
        """All statuses (scheduled, in-progress, final) are returned."""
        data = {
            "scoreboard": {
                "games": [
                    {
                        "gameId": "001",
                        "gameTimeUTC": "2026-02-08T17:30:00Z",
                        "gameStatus": 1,
                        "homeTeam": {"teamCity": "Boston", "teamName": "Celtics"},
                        "awayTeam": {"teamCity": "New York", "teamName": "Knicks"},
                    },
                    {
                        "gameId": "002",
                        "gameTimeUTC": "2026-02-08T20:00:00Z",
                        "gameStatus": 3,
                        "homeTeam": {"teamCity": "Miami", "teamName": "Heat"},
                        "awayTeam": {"teamCity": "Chicago", "teamName": "Bulls"},
                    },
                ]
            }
        }
        mock_resp = httpx.Response(200, request=_DUMMY_REQUEST, json=data)
        mock_get.return_value = mock_resp

        games = fetch_todays_games()
        assert len(games) == 2
        assert games[0].game_status == 1
        assert games[1].game_status == 3


    @patch("src.connectors.nba_schedule.httpx.get")
    def test_normalizes_la_clippers(self, mock_get):
        """NBA.com 'LA Clippers' should be normalized to 'Los Angeles Clippers'."""
        data = {
            "scoreboard": {
                "games": [
                    {
                        "gameId": "001",
                        "gameTimeUTC": "2026-02-08T20:00:00Z",
                        "gameStatus": 1,
                        "homeTeam": {"teamCity": "LA", "teamName": "Clippers"},
                        "awayTeam": {"teamCity": "Boston", "teamName": "Celtics"},
                    },
                ]
            }
        }
        mock_resp = httpx.Response(200, request=_DUMMY_REQUEST, json=data)
        mock_get.return_value = mock_resp

        games = fetch_todays_games()
        assert len(games) == 1
        assert games[0].home_team == "Los Angeles Clippers"


    @patch("src.connectors.nba_schedule.httpx.get")
    def test_parses_scores(self, mock_get):
        """Scores are parsed from homeTeam/awayTeam score fields."""
        data = {
            "scoreboard": {
                "games": [
                    {
                        "gameId": "003",
                        "gameTimeUTC": "2026-02-08T17:30:00Z",
                        "gameStatus": 3,
                        "homeTeam": {
                            "teamCity": "Boston",
                            "teamName": "Celtics",
                            "score": 112,
                        },
                        "awayTeam": {
                            "teamCity": "New York",
                            "teamName": "Knicks",
                            "score": 105,
                        },
                    }
                ]
            }
        }
        mock_resp = httpx.Response(200, request=_DUMMY_REQUEST, json=data)
        mock_get.return_value = mock_resp

        games = fetch_todays_games()
        assert len(games) == 1
        assert games[0].home_score == 112
        assert games[0].away_score == 105

    @patch("src.connectors.nba_schedule.httpx.get")
    def test_scores_default_zero(self, mock_get):
        """Games without scores (scheduled) default to 0."""
        mock_resp = httpx.Response(200, request=_DUMMY_REQUEST, json=SAMPLE_SCOREBOARD)
        mock_get.return_value = mock_resp

        games = fetch_todays_games()
        assert games[0].home_score == 0
        assert games[0].away_score == 0


class TestNBAGame:
    def test_dataclass(self):
        game = NBAGame(
            game_id="001",
            home_team="Boston Celtics",
            away_team="New York Knicks",
            game_time_utc="2026-02-08T17:30:00Z",
            game_status=1,
        )
        assert game.game_id == "001"
        assert game.home_team == "Boston Celtics"
