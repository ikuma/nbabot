"""Tests for team name mapping utilities."""

from src.connectors.team_mapping import (
    build_event_slug,
    full_name_from_short,
    get_team_abbr,
    get_team_short_name,
    normalize_team_name,
)


class TestGetTeamAbbr:
    def test_known_teams(self):
        assert get_team_abbr("Boston Celtics") == "bos"
        assert get_team_abbr("New York Knicks") == "nyk"
        assert get_team_abbr("Golden State Warriors") == "gsw"
        assert get_team_abbr("Los Angeles Lakers") == "lal"
        assert get_team_abbr("Oklahoma City Thunder") == "okc"

    def test_unknown_team(self):
        assert get_team_abbr("Seattle SuperSonics") is None
        assert get_team_abbr("") is None


class TestGetTeamShortName:
    def test_known_teams(self):
        assert get_team_short_name("Boston Celtics") == "Celtics"
        assert get_team_short_name("Portland Trail Blazers") == "Trail Blazers"
        assert get_team_short_name("Philadelphia 76ers") == "76ers"

    def test_unknown_team(self):
        assert get_team_short_name("Nonexistent Team") is None


class TestFullNameFromShort:
    def test_short_name(self):
        assert full_name_from_short("Celtics") == "Boston Celtics"
        assert full_name_from_short("Knicks") == "New York Knicks"

    def test_alias(self):
        assert full_name_from_short("Cavs") == "Cleveland Cavaliers"
        assert full_name_from_short("Mavs") == "Dallas Mavericks"
        assert full_name_from_short("Sixers") == "Philadelphia 76ers"
        assert full_name_from_short("Blazers") == "Portland Trail Blazers"
        assert full_name_from_short("Wolves") == "Minnesota Timberwolves"

    def test_case_insensitive(self):
        assert full_name_from_short("celtics") == "Boston Celtics"
        assert full_name_from_short("CELTICS") == "Boston Celtics"
        assert full_name_from_short("cavs") == "Cleveland Cavaliers"

    def test_unknown(self):
        assert full_name_from_short("Unknown") is None


class TestNormalizeTeamName:
    def test_la_clippers_alias(self):
        assert normalize_team_name("LA Clippers") == "Los Angeles Clippers"

    def test_la_lakers_alias(self):
        assert normalize_team_name("LA Lakers") == "Los Angeles Lakers"

    def test_already_canonical(self):
        assert normalize_team_name("Boston Celtics") == "Boston Celtics"
        assert normalize_team_name("Los Angeles Clippers") == "Los Angeles Clippers"

    def test_unknown_name_passthrough(self):
        assert normalize_team_name("Unknown Team") == "Unknown Team"


class TestBuildEventSlug:
    def test_valid_teams(self):
        slug = build_event_slug("New York Knicks", "Boston Celtics", "2026-02-08")
        assert slug == "nba-nyk-bos-2026-02-08"

    def test_unknown_away(self):
        assert build_event_slug("Unknown", "Boston Celtics", "2026-02-08") is None

    def test_unknown_home(self):
        assert build_event_slug("New York Knicks", "Unknown", "2026-02-08") is None
