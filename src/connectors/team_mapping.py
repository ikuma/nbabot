"""NBA team name mapping for Odds API <-> Polymarket cross-referencing."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class TeamInfo:
    full_name: str       # "Boston Celtics" (Odds API)
    abbr: str            # "bos" (Polymarket slug)
    short_name: str      # "Celtics" (Polymarket outcome)
    city: str            # "Boston"
    aliases: list[str] = field(default_factory=list)


NBA_TEAMS: dict[str, TeamInfo] = {
    "Atlanta Hawks": TeamInfo("Atlanta Hawks", "atl", "Hawks", "Atlanta"),
    "Boston Celtics": TeamInfo("Boston Celtics", "bos", "Celtics", "Boston"),
    "Brooklyn Nets": TeamInfo("Brooklyn Nets", "bkn", "Nets", "Brooklyn"),
    "Charlotte Hornets": TeamInfo("Charlotte Hornets", "cha", "Hornets", "Charlotte"),
    "Chicago Bulls": TeamInfo("Chicago Bulls", "chi", "Bulls", "Chicago"),
    "Cleveland Cavaliers": TeamInfo("Cleveland Cavaliers", "cle", "Cavaliers", "Cleveland", ["Cavs"]),
    "Dallas Mavericks": TeamInfo("Dallas Mavericks", "dal", "Mavericks", "Dallas", ["Mavs"]),
    "Denver Nuggets": TeamInfo("Denver Nuggets", "den", "Nuggets", "Denver"),
    "Detroit Pistons": TeamInfo("Detroit Pistons", "det", "Pistons", "Detroit"),
    "Golden State Warriors": TeamInfo("Golden State Warriors", "gsw", "Warriors", "Golden State"),
    "Houston Rockets": TeamInfo("Houston Rockets", "hou", "Rockets", "Houston"),
    "Indiana Pacers": TeamInfo("Indiana Pacers", "ind", "Pacers", "Indiana"),
    "Los Angeles Clippers": TeamInfo("Los Angeles Clippers", "lac", "Clippers", "Los Angeles"),
    "Los Angeles Lakers": TeamInfo("Los Angeles Lakers", "lal", "Lakers", "Los Angeles"),
    "Memphis Grizzlies": TeamInfo("Memphis Grizzlies", "mem", "Grizzlies", "Memphis"),
    "Miami Heat": TeamInfo("Miami Heat", "mia", "Heat", "Miami"),
    "Milwaukee Bucks": TeamInfo("Milwaukee Bucks", "mil", "Bucks", "Milwaukee"),
    "Minnesota Timberwolves": TeamInfo("Minnesota Timberwolves", "min", "Timberwolves", "Minnesota", ["Wolves"]),
    "New Orleans Pelicans": TeamInfo("New Orleans Pelicans", "nop", "Pelicans", "New Orleans"),
    "New York Knicks": TeamInfo("New York Knicks", "nyk", "Knicks", "New York"),
    "Oklahoma City Thunder": TeamInfo("Oklahoma City Thunder", "okc", "Thunder", "Oklahoma City"),
    "Orlando Magic": TeamInfo("Orlando Magic", "orl", "Magic", "Orlando"),
    "Philadelphia 76ers": TeamInfo("Philadelphia 76ers", "phi", "76ers", "Philadelphia", ["Sixers"]),
    "Phoenix Suns": TeamInfo("Phoenix Suns", "phx", "Suns", "Phoenix"),
    "Portland Trail Blazers": TeamInfo("Portland Trail Blazers", "por", "Trail Blazers", "Portland", ["Blazers"]),
    "Sacramento Kings": TeamInfo("Sacramento Kings", "sac", "Kings", "Sacramento"),
    "San Antonio Spurs": TeamInfo("San Antonio Spurs", "sas", "Spurs", "San Antonio"),
    "Toronto Raptors": TeamInfo("Toronto Raptors", "tor", "Raptors", "Toronto"),
    "Utah Jazz": TeamInfo("Utah Jazz", "uta", "Jazz", "Utah"),
    "Washington Wizards": TeamInfo("Washington Wizards", "was", "Wizards", "Washington"),
}

# Reverse lookup: short_name / alias -> full_name
_SHORT_TO_FULL: dict[str, str] = {}
for _full, _info in NBA_TEAMS.items():
    _SHORT_TO_FULL[_info.short_name.lower()] = _full
    for _alias in _info.aliases:
        _SHORT_TO_FULL[_alias.lower()] = _full


def get_team_abbr(full_name: str) -> str | None:
    info = NBA_TEAMS.get(full_name)
    return info.abbr if info else None


def get_team_short_name(full_name: str) -> str | None:
    info = NBA_TEAMS.get(full_name)
    return info.short_name if info else None


def full_name_from_short(short_name: str) -> str | None:
    return _SHORT_TO_FULL.get(short_name.lower())


def build_event_slug(away_full: str, home_full: str, date_str: str) -> str | None:
    """Build a Polymarket event slug like 'nba-nyk-bos-2026-02-08'."""
    away_abbr = get_team_abbr(away_full)
    home_abbr = get_team_abbr(home_full)
    if not away_abbr or not home_abbr:
        return None
    return f"nba-{away_abbr}-{home_abbr}-{date_str}"
