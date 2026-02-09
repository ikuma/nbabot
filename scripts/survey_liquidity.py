#!/usr/bin/env python3
"""Survey NBA moneyline market liquidity on Polymarket.

Fetches order books for all active NBA ML markets and produces a report
with spread, depth, and market impact metrics.

Usage:
    python scripts/survey_liquidity.py
    python scripts/survey_liquidity.py --date 2026-02-10

Output:
    data/reports/liquidity-survey/YYYY-MM-DD.md
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")


def main() -> None:
    from src.connectors.nba_schedule import fetch_games_for_date, fetch_todays_games
    from src.connectors.polymarket import fetch_moneyline_for_game, fetch_order_books_batch
    from src.connectors.team_mapping import build_event_slug
    from src.sizing.liquidity import extract_liquidity, score_liquidity

    parser = argparse.ArgumentParser(description="Survey NBA market liquidity")
    parser.add_argument("--date", type=str, default=None, help="Game date YYYY-MM-DD")
    args = parser.parse_args()

    game_date = args.date or datetime.now(timezone.utc).astimezone(ET).strftime("%Y-%m-%d")
    today_str = datetime.now(timezone.utc).astimezone(ET).strftime("%Y-%m-%d")

    log.info("Surveying liquidity for %s", game_date)

    if game_date == today_str:
        games = fetch_todays_games()
    else:
        games = fetch_games_for_date(game_date)

    if not games:
        log.info("No games found for %s", game_date)
        return

    log.info("Found %d games", len(games))

    rows: list[dict] = []

    for game in games:
        if game.game_status == 3:
            continue

        slug = build_event_slug(game.away_team, game.home_team, game_date)
        if not slug:
            continue

        ml = fetch_moneyline_for_game(game.away_team, game.home_team, game_date)
        if not ml:
            log.info("No moneyline for %s", slug)
            continue

        # 試合開始までの残り時間
        hours_to_tipoff = None
        if game.game_time_utc:
            try:
                gt = datetime.fromisoformat(game.game_time_utc.replace("Z", "+00:00"))
                delta = gt - datetime.now(timezone.utc)
                hours_to_tipoff = delta.total_seconds() / 3600
            except (ValueError, AttributeError):
                pass

        order_books = fetch_order_books_batch(ml.token_ids)

        for i, outcome in enumerate(ml.outcomes):
            if i >= len(ml.token_ids) or i >= len(ml.prices):
                continue

            tid = ml.token_ids[i]
            price = ml.prices[i]
            book = order_books.get(tid)

            if not book:
                rows.append({
                    "game": slug,
                    "outcome": outcome,
                    "price": price,
                    "best_bid": None,
                    "best_ask": None,
                    "spread_pct": None,
                    "ask_depth_5c": None,
                    "ask_depth_10c": None,
                    "bid_depth_5c": None,
                    "ask_levels": None,
                    "bid_levels": None,
                    "impact_100": None,
                    "impact_500": None,
                    "impact_2000": None,
                    "liq_score_100": None,
                    "hours_to_tipoff": hours_to_tipoff,
                })
                continue

            snap = extract_liquidity(book, tid, order_size_usd=100.0)
            if not snap:
                continue

            snap_500 = extract_liquidity(book, tid, order_size_usd=500.0)
            snap_2000 = extract_liquidity(book, tid, order_size_usd=2000.0)

            rows.append({
                "game": slug,
                "outcome": outcome,
                "price": price,
                "best_bid": snap.best_bid,
                "best_ask": snap.best_ask,
                "spread_pct": snap.spread_pct,
                "ask_depth_5c": snap.ask_depth_5c,
                "ask_depth_10c": snap.ask_depth_10c,
                "bid_depth_5c": snap.bid_depth_5c,
                "ask_levels": snap.ask_levels,
                "bid_levels": snap.bid_levels,
                "impact_100": snap.impact_estimate,
                "impact_500": snap_500.impact_estimate if snap_500 else None,
                "impact_2000": snap_2000.impact_estimate if snap_2000 else None,
                "liq_score_100": score_liquidity(snap, 100.0),
                "hours_to_tipoff": hours_to_tipoff,
            })

    if not rows:
        log.info("No liquidity data collected")
        return

    # レポート生成
    report_dir = Path("data/reports/liquidity-survey")
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{game_date}.md"

    lines = [
        f"# NBA Moneyline Liquidity Survey — {game_date}",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        f"Markets surveyed: {len(rows)}",
        "",
        "## Summary",
        "",
    ]

    # 集計
    valid_spreads = [r["spread_pct"] for r in rows if r["spread_pct"] is not None]
    valid_depths = [r["ask_depth_5c"] for r in rows if r["ask_depth_5c"] is not None]

    if valid_spreads:
        valid_spreads_sorted = sorted(valid_spreads)
        lines.append(f"- Spread: min={min(valid_spreads):.1f}% "
                      f"median={valid_spreads_sorted[len(valid_spreads_sorted)//2]:.1f}% "
                      f"max={max(valid_spreads):.1f}%")
    if valid_depths:
        valid_depths_sorted = sorted(valid_depths)
        lines.append(f"- Ask depth (5c): min=${min(valid_depths):.0f} "
                      f"median=${valid_depths_sorted[len(valid_depths_sorted)//2]:.0f} "
                      f"max=${max(valid_depths):.0f}")

    lines.extend(["", "## Detail", ""])

    # テーブルヘッダー
    lines.append(
        "| Game | Outcome | Price | Bid | Ask | Spread% | "
        "Depth5c | Depth10c | BidD5c | "
        "Imp$100 | Imp$500 | Imp$2K | Score | Tipoff(h) |"
    )
    lines.append(
        "|------|---------|-------|-----|-----|---------|"
        "---------|----------|--------|"
        "---------|---------|--------|-------|-----------|"
    )

    for r in rows:
        def _f(v, fmt=".2f"):
            return f"{v:{fmt}}" if v is not None else "—"

        def _fc(v):
            return f"{v:.1f}c" if v is not None else "—"

        lines.append(
            f"| {r['game']} | {r['outcome']} | {_f(r['price'], '.3f')} | "
            f"{_f(r['best_bid'], '.3f')} | {_f(r['best_ask'], '.3f')} | "
            f"{_f(r['spread_pct'], '.1f')} | "
            f"${_f(r['ask_depth_5c'], '.0f')} | ${_f(r['ask_depth_10c'], '.0f')} | "
            f"${_f(r['bid_depth_5c'], '.0f')} | "
            f"{_fc(r['impact_100'])} | {_fc(r['impact_500'])} | {_fc(r['impact_2000'])} | "
            f"{r['liq_score_100'] or '—'} | {_f(r['hours_to_tipoff'], '.1f')} |"
        )

    report_text = "\n".join(lines) + "\n"
    report_path.write_text(report_text)
    log.info("Report written to %s", report_path)
    print(f"\nReport: {report_path}")
    print(f"Markets: {len(rows)}")


if __name__ == "__main__":
    main()
