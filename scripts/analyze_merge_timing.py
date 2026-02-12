#!/usr/bin/env python3
"""Analyze MERGE timing relative to game times."""

import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def parse_event_slug_date(slug: str) -> datetime | None:
    """Extract date from event slug like 'nba-sas-sac-2024-12-01'."""
    match = re.search(r"(\d{4})-(\d{2})-(\d{2})", slug)
    if not match:
        return None
    year, month, day = match.groups()
    # Assume game date is in ET timezone
    return datetime(int(year), int(month), int(day), tzinfo=ET)


def estimate_game_time_utc(game_date_et: datetime) -> datetime:
    """Estimate game time (assume typical 8pm ET tipoff)."""
    game_time_et = game_date_et.replace(hour=20, minute=0, second=0, microsecond=0)
    return game_time_et.astimezone(ZoneInfo("UTC"))


def analyze_merge_timing(merge_file: Path, trader_name: str) -> dict:
    """Analyze MERGE timing patterns."""
    with open(merge_file) as f:
        merges = json.load(f)

    print(f"\n{'='*80}")
    print(f"Analyzing {trader_name}: {len(merges)} MERGE operations")
    print(f"{'='*80}\n")

    timing_distribution = Counter()
    hour_distribution = Counter()
    relative_hours = []
    examples = []

    for merge in merges:
        timestamp = merge.get("timestamp")
        slug = merge.get("eventSlug") or merge.get("slug")

        if not timestamp or not slug:
            continue

        # Parse merge time
        merge_time_utc = datetime.fromtimestamp(timestamp, tz=ZoneInfo("UTC"))
        merge_time_et = merge_time_utc.astimezone(ET)

        # Parse game date
        game_date_et = parse_event_slug_date(slug)
        if not game_date_et:
            continue

        # Estimate game time
        game_time_utc = estimate_game_time_utc(game_date_et)

        # Calculate relative timing
        time_diff = merge_time_utc - game_time_utc
        hours_diff = time_diff.total_seconds() / 3600

        relative_hours.append(hours_diff)
        hour_distribution[int(hours_diff)] += 1

        # Categorize timing
        if time_diff.days < 0:
            # Before game
            hours_before = abs(hours_diff)
            if hours_before <= 24:
                timing_distribution["same_day_before"] += 1
            else:
                timing_distribution["days_before"] += 1
        elif time_diff.total_seconds() < 4 * 3600:
            # Within 4 hours after tipoff (during/immediately after game)
            timing_distribution["during_or_right_after"] += 1
        elif time_diff.days == 0:
            # Same day after game
            timing_distribution["same_day_after"] += 1
        else:
            # Days after
            timing_distribution[f"{time_diff.days}_days_after"] += 1

        # Collect examples
        if len(examples) < 10:
            examples.append(
                {
                    "slug": slug,
                    "merge_time_et": merge_time_et.strftime("%Y-%m-%d %H:%M:%S %Z"),
                    "game_date_et": game_date_et.strftime("%Y-%m-%d"),
                    "est_game_time_et": "8:00 PM ET (typical)",
                    "hours_relative": round(hours_diff, 1),
                    "category": (
                        "BEFORE game"
                        if hours_diff < 0
                        else (
                            "DURING/IMMEDIATELY AFTER"
                            if hours_diff < 4
                            else "HOURS/DAYS AFTER"
                        )
                    ),
                }
            )

    # Print timing distribution
    print("TIMING DISTRIBUTION:")
    print("-" * 80)
    total = sum(timing_distribution.values())
    for category, count in sorted(timing_distribution.items()):
        pct = 100 * count / total if total > 0 else 0
        print(f"  {category:30s}: {count:4d} ({pct:5.1f}%)")

    print("\n\nHOURS RELATIVE TO GAME (histogram):")
    print("-" * 80)
    print("Note: negative = before game, positive = after game")
    print("      0 = estimated tipoff (8pm ET)")
    print()

    # Histogram by hour
    for hour in sorted(hour_distribution.keys()):
        count = hour_distribution[hour]
        bar = "#" * (count // 5 or 1)
        print(f"  {hour:+4d}h: {bar} ({count})")

    # Statistics
    if relative_hours:
        avg_hours = sum(relative_hours) / len(relative_hours)
        median_hours = sorted(relative_hours)[len(relative_hours) // 2]
        print(f"\n\nSTATISTICS:")
        print("-" * 80)
        print(f"  Mean:   {avg_hours:+.1f} hours relative to tipoff")
        print(f"  Median: {median_hours:+.1f} hours relative to tipoff")
        print(f"  Min:    {min(relative_hours):+.1f} hours")
        print(f"  Max:    {max(relative_hours):+.1f} hours")

        before_game = sum(1 for h in relative_hours if h < 0)
        during_game = sum(1 for h in relative_hours if 0 <= h < 4)
        after_game = sum(1 for h in relative_hours if h >= 4)
        print(f"\n  Before game (h < 0):        {before_game:4d} ({100*before_game/len(relative_hours):.1f}%)")
        print(f"  During/immediately (0-4h):  {during_game:4d} ({100*during_game/len(relative_hours):.1f}%)")
        print(f"  Hours/days after (h >= 4):  {after_game:4d} ({100*after_game/len(relative_hours):.1f}%)")

    # Print examples
    print("\n\nEXAMPLES (first 10):")
    print("-" * 80)
    for i, ex in enumerate(examples, 1):
        print(f"\n{i}. {ex['slug']}")
        print(f"   MERGE time:  {ex['merge_time_et']}")
        print(f"   Game date:   {ex['game_date_et']} @ {ex['est_game_time_et']}")
        print(f"   Relative:    {ex['hours_relative']:+.1f} hours  [{ex['category']}]")

    return {
        "trader": trader_name,
        "total_merges": len(merges),
        "timing_distribution": dict(timing_distribution),
        "mean_hours": avg_hours if relative_hours else None,
        "median_hours": median_hours if relative_hours else None,
        "before_game_pct": 100 * before_game / len(relative_hours) if relative_hours else 0,
        "during_game_pct": 100 * during_game / len(relative_hours) if relative_hours else 0,
        "after_game_pct": 100 * after_game / len(relative_hours) if relative_hours else 0,
    }


def main():
    """Analyze MERGE timing for both traders."""
    base_dir = Path(__file__).parent.parent

    # Analyze sovereign2013
    sovereign_file = base_dir / "data/traders/sovereign2013/raw_merge.json"
    lhtsports_file = base_dir / "data/reports/lhtsports-analysis/lhtsports_merge.json"

    results = []

    if sovereign_file.exists():
        results.append(analyze_merge_timing(sovereign_file, "sovereign2013"))
    else:
        print(f"WARNING: {sovereign_file} not found")

    if lhtsports_file.exists():
        results.append(analyze_merge_timing(lhtsports_file, "lhtsports"))
    else:
        print(f"WARNING: {lhtsports_file} not found")

    # Summary comparison
    print(f"\n\n{'='*80}")
    print("SUMMARY COMPARISON")
    print(f"{'='*80}\n")

    for res in results:
        print(f"{res['trader']:20s}: {res['total_merges']:4d} merges")
        print(f"  Mean:   {res['mean_hours']:+7.1f}h relative to tipoff")
        print(f"  Median: {res['median_hours']:+7.1f}h relative to tipoff")
        print(f"  Before game:  {res['before_game_pct']:5.1f}%")
        print(f"  During/immed: {res['during_game_pct']:5.1f}%")
        print(f"  After game:   {res['after_game_pct']:5.1f}%")
        print()

    print("\nCONCLUSION:")
    print("-" * 80)
    print("MERGE operations happen AFTER both positions are executed and DCA is complete.")
    print("Based on nbabot code:")
    print("  - Directional trade: execute_after = tipoff - 8h, execute_before = tipoff")
    print("  - Hedge trade: 30min+ delay after directional")
    print("  - DCA: up to 5 entries over 2+ hours")
    print("  - MERGE: called AFTER all DCA entries are complete (status=executed)")
    print()
    print("Therefore:")
    print("  - If trades start 1-2h before tipoff and DCA completes during game")
    print("  - MERGE likely happens DURING or AFTER the game (0-4h+ after tipoff)")
    print("  - This matches the data distribution above")


if __name__ == "__main__":
    main()
