#!/usr/bin/env python3
"""Step-by-step connection tester: NBA.com → slug → Gamma Events API."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


def step1_nba_schedule():
    """Step 1: Fetch today's games from NBA.com."""
    from src.connectors.nba_schedule import fetch_todays_games

    print("\n=== Step 1: NBA.com Scoreboard ===")
    games = fetch_todays_games()
    if not games:
        print("  FAIL: No games returned from NBA.com")
        return []

    print(f"  OK: {len(games)} games found")
    for g in games:
        status_map = {1: "Scheduled", 2: "In Progress", 3: "Final"}
        status = status_map.get(g.game_status, f"Unknown({g.game_status})")
        print(f"    {g.away_team} @ {g.home_team} [{status}] {g.game_time_utc}")
    return games


def step2_slug_generation(games):
    """Step 2: Generate Polymarket slugs for each game."""
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from src.connectors.team_mapping import build_event_slug

    et = ZoneInfo("America/New_York")

    print("\n=== Step 2: Slug Generation ===")
    slugs = []
    for g in games:
        try:
            utc_dt = datetime.fromisoformat(g.game_time_utc.replace("Z", "+00:00"))
            et_dt = utc_dt.astimezone(et)
            game_date = et_dt.strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            print(f"  FAIL: Bad time for {g.away_team} @ {g.home_team}: {g.game_time_utc}")
            continue

        slug = build_event_slug(g.away_team, g.home_team, game_date)
        if slug:
            print(f"  OK: {slug}")
            slugs.append((g, slug, game_date))
        else:
            print(f"  FAIL: Cannot build slug for {g.away_team} @ {g.home_team}")

    print(f"  {len(slugs)}/{len(games)} slugs generated")
    return slugs


def step3_gamma_api(slugs):
    """Step 3: Fetch moneyline market from Gamma Events API."""
    from src.connectors.polymarket import fetch_moneyline_for_game

    print("\n=== Step 3: Gamma Events API ===")
    found = 0
    for game, slug, game_date in slugs:
        ml = fetch_moneyline_for_game(game.away_team, game.home_team, game_date)
        if ml:
            found += 1
            prices = " | ".join(f"{o} @ {p:.3f}" for o, p in zip(ml.outcomes, ml.prices))
            print(f"  OK: {slug} → {prices}")
        else:
            print(f"  MISS: {slug} (no moneyline market)")

    print(f"  {found}/{len(slugs)} moneyline markets found")
    return found


def main():
    print("=" * 60)
    print("  NBA Polymarket Connection Test")
    print("=" * 60)

    results = {}

    # Step 1
    games = step1_nba_schedule()
    results["NBA.com"] = "OK" if games else "FAIL"

    if not games:
        print("\nStopping: no games to test further")
        _print_summary(results)
        return

    # Step 2
    slugs = step2_slug_generation(games)
    results["Slug generation"] = "OK" if slugs else "FAIL"

    if not slugs:
        print("\nStopping: no slugs generated")
        _print_summary(results)
        return

    # Step 3
    found = step3_gamma_api(slugs)
    results["Gamma API"] = "OK" if found > 0 else "MISS"

    _print_summary(results)


def _print_summary(results):
    print("\n" + "=" * 60)
    print("  Summary")
    print("=" * 60)
    for step, status in results.items():
        icon = "+" if status == "OK" else "-"
        print(f"  [{icon}] {step}: {status}")
    print("=" * 60)


if __name__ == "__main__":
    main()
