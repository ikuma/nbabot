"""
Agent 1: Position Architecture Analysis

Analyzes how lhtsports constructs positions within individual games/events.
Focuses on trade sequences, DCA patterns, position building types, and timing.
"""

import json
import os
import re
import statistics
from collections import defaultdict
from datetime import datetime, timezone

BASE_DIR = "/Users/taro/dev/nbabot"
TRADES_PATH = os.path.join(BASE_DIR, "data/reports/lhtsports-analysis/lhtsports_all_trades.json")
CONDITION_PNL_PATH = os.path.join(BASE_DIR, "data/reports/lhtsports-pnl/condition_pnl.json")
GAME_PNL_PATH = os.path.join(BASE_DIR, "data/reports/lhtsports-pnl/game_pnl.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "data/reports/lhtsports-pnl/deep-analysis")


def load_data():
    """Load all input data files."""
    print("Loading trades...")
    with open(TRADES_PATH) as f:
        trades = json.load(f)
    print(f"  Loaded {len(trades):,} trades")

    print("Loading condition P&L...")
    with open(CONDITION_PNL_PATH) as f:
        condition_pnl = json.load(f)
    print(f"  Loaded {len(condition_pnl):,} conditions")

    print("Loading game P&L...")
    with open(GAME_PNL_PATH) as f:
        game_pnl = json.load(f)
    print(f"  Loaded {len(game_pnl):,} games")

    return trades, condition_pnl, game_pnl


def group_trades_by_condition(trades):
    """Group trades by conditionId, sorted by timestamp."""
    by_condition = defaultdict(list)
    for t in trades:
        by_condition[t["conditionId"]].append(t)
    # Sort each group by timestamp
    for cid in by_condition:
        by_condition[cid].sort(key=lambda x: x["timestamp"])
    return by_condition


def group_trades_by_game(trades):
    """Group trades by eventSlug (game-level)."""
    by_game = defaultdict(list)
    for t in trades:
        by_game[t["eventSlug"]].append(t)
    for gk in by_game:
        by_game[gk].sort(key=lambda x: x["timestamp"])
    return by_game


def analyze_trade_sequences(trades_by_condition, condition_pnl_map):
    """Analyze trade sequences within each condition."""
    results = []
    for cid, trades in trades_by_condition.items():
        buys = [t for t in trades if t["side"] == "BUY"]
        sells = [t for t in trades if t["side"] == "SELL"]

        if not buys:
            continue

        buy_prices = [t["price"] for t in buys]
        buy_sizes_usd = [t["usdcSize"] for t in buys]
        buy_timestamps = [t["timestamp"] for t in buys]

        time_span_seconds = buy_timestamps[-1] - buy_timestamps[0] if len(buy_timestamps) > 1 else 0
        time_span_hours = time_span_seconds / 3600.0

        # Price evolution
        first_price = buy_prices[0]
        last_price = buy_prices[-1]
        avg_price = statistics.mean(buy_prices)
        price_std = statistics.stdev(buy_prices) if len(buy_prices) > 1 else 0.0
        price_cv = price_std / avg_price if avg_price > 0 else 0.0

        # Did they average down?
        averaging_down = last_price < first_price if len(buy_prices) > 1 else False

        # VWAP: size * price ~= usdcSize, so VWAP = total_cost / total_shares
        total_cost = sum(buy_sizes_usd)
        total_shares = sum(t["size"] for t in buys)
        vwap = total_cost / total_shares if total_shares > 0 else avg_price

        cpnl = condition_pnl_map.get(cid, {})

        results.append({
            "conditionId": cid,
            "eventSlug": trades[0].get("eventSlug", ""),
            "slug": trades[0].get("slug", ""),
            "n_buys": len(buys),
            "n_sells": len(sells),
            "n_total_trades": len(trades),
            "buy_prices": buy_prices,
            "buy_sizes_usd": buy_sizes_usd,
            "first_price": first_price,
            "last_price": last_price,
            "avg_price": avg_price,
            "vwap": vwap,
            "price_std": price_std,
            "price_cv": price_cv,
            "averaging_down": averaging_down,
            "time_span_hours": time_span_hours,
            "total_buy_cost": total_cost,
            "first_buy_ts": buy_timestamps[0],
            "last_buy_ts": buy_timestamps[-1],
            "pnl": cpnl.get("pnl", 0),
            "roi_pct": cpnl.get("roi_pct", 0),
            "status": cpnl.get("status", "UNKNOWN"),
            "sport": cpnl.get("sport", "Unknown"),
            "market_type": cpnl.get("market_type", "Unknown"),
        })
    return results


def analyze_dca_patterns(sequence_results):
    """Analyze DCA patterns across conditions."""
    # Distribution of buys per condition
    buy_counts = [r["n_buys"] for r in sequence_results]

    single_buy = [r for r in sequence_results if r["n_buys"] == 1]
    multi_buy = [r for r in sequence_results if r["n_buys"] > 1]

    buy_count_dist = defaultdict(int)
    for c in buy_counts:
        if c == 1:
            buy_count_dist["1"] += 1
        elif c <= 3:
            buy_count_dist["2-3"] += 1
        elif c <= 5:
            buy_count_dist["4-5"] += 1
        elif c <= 10:
            buy_count_dist["6-10"] += 1
        elif c <= 20:
            buy_count_dist["11-20"] += 1
        elif c <= 50:
            buy_count_dist["21-50"] += 1
        else:
            buy_count_dist["50+"] += 1

    # Price variance for multi-buy conditions
    price_cvs = [r["price_cv"] for r in multi_buy if r["price_cv"] > 0]
    avg_price_cv = statistics.mean(price_cvs) if price_cvs else 0

    # Time spans for multi-buy
    time_spans = [r["time_span_hours"] for r in multi_buy]
    avg_time_span = statistics.mean(time_spans) if time_spans else 0
    median_time_span = statistics.median(time_spans) if time_spans else 0

    # Averaging down frequency
    avg_down_count = sum(1 for r in multi_buy if r["averaging_down"])
    avg_down_pct = avg_down_count / len(multi_buy) * 100 if multi_buy else 0

    # Price improvement for DCA (compare VWAP to first price)
    price_improvements = []
    for r in multi_buy:
        if r["first_price"] > 0:
            improvement = (r["first_price"] - r["vwap"]) / r["first_price"] * 100
            price_improvements.append(improvement)

    avg_price_improvement = statistics.mean(price_improvements) if price_improvements else 0

    # P&L comparison: single vs multi buy
    single_pnl = [r["pnl"] for r in single_buy if r["status"] in ("WIN", "LOSS", "LOSS_OR_OPEN")]
    multi_pnl = [r["pnl"] for r in multi_buy if r["status"] in ("WIN", "LOSS", "LOSS_OR_OPEN")]
    single_roi = [r["roi_pct"] for r in single_buy if r["status"] in ("WIN", "LOSS", "LOSS_OR_OPEN")]
    multi_roi = [r["roi_pct"] for r in multi_buy if r["status"] in ("WIN", "LOSS", "LOSS_OR_OPEN")]

    return {
        "buy_count_distribution": dict(buy_count_dist),
        "total_conditions": len(sequence_results),
        "single_buy_count": len(single_buy),
        "multi_buy_count": len(multi_buy),
        "avg_buys_per_condition": statistics.mean(buy_counts) if buy_counts else 0,
        "median_buys_per_condition": statistics.median(buy_counts) if buy_counts else 0,
        "max_buys_per_condition": max(buy_counts) if buy_counts else 0,
        "avg_price_cv": avg_price_cv,
        "avg_time_span_hours": avg_time_span,
        "median_time_span_hours": median_time_span,
        "avg_down_pct": avg_down_pct,
        "avg_price_improvement_pct": avg_price_improvement,
        "single_buy_avg_pnl": statistics.mean(single_pnl) if single_pnl else 0,
        "multi_buy_avg_pnl": statistics.mean(multi_pnl) if multi_pnl else 0,
        "single_buy_avg_roi": statistics.mean(single_roi) if single_roi else 0,
        "multi_buy_avg_roi": statistics.mean(multi_roi) if multi_roi else 0,
        "single_buy_total_pnl": sum(single_pnl),
        "multi_buy_total_pnl": sum(multi_pnl),
    }


def classify_game_position_type(game_key, trades_by_game, game_pnl_map, condition_pnl_map):
    """Classify a game's position building type."""
    game_info = game_pnl_map.get(game_key, {})
    market_types = game_info.get("market_types", [])
    n_conditions = game_info.get("n_conditions", 0)

    game_trades = trades_by_game.get(game_key, [])
    # Conditions involved in this game
    condition_ids = set(t["conditionId"] for t in game_trades)

    # Count buys per condition
    buys_per_condition = defaultdict(int)
    for t in game_trades:
        if t["side"] == "BUY":
            buys_per_condition[t["conditionId"]] += 1

    has_multi_buy = any(c > 1 for c in buys_per_condition.values())
    has_multi_market = len(market_types) > 1

    if has_multi_market and has_multi_buy:
        return "complex"
    elif has_multi_market:
        return "multi_market"
    elif has_multi_buy:
        return "dca"
    else:
        return "single_buy"


def classify_all_games(game_pnl, trades_by_game, game_pnl_map, condition_pnl_map):
    """Classify all games by position type and compute stats."""
    classifications = {}
    for game in game_pnl:
        gk = game["game_key"]
        ptype = classify_game_position_type(gk, trades_by_game, game_pnl_map, condition_pnl_map)
        classifications[gk] = {
            "type": ptype,
            "pnl": game.get("total_pnl", 0),
            "roi_pct": game.get("roi_pct", 0),
            "buy_cost": game.get("total_buy_cost", 0),
            "trade_count": game.get("trade_count", 0),
            "sport": game.get("sport", "Unknown"),
            "n_conditions": game.get("n_conditions", 0),
            "market_types": game.get("market_types", []),
        }

    # Aggregate stats per type
    type_stats = {}
    for ptype in ["single_buy", "dca", "multi_market", "complex"]:
        games_of_type = [v for v in classifications.values() if v["type"] == ptype]
        if games_of_type:
            pnls = [g["pnl"] for g in games_of_type]
            rois = [g["roi_pct"] for g in games_of_type]
            costs = [g["buy_cost"] for g in games_of_type]
            type_stats[ptype] = {
                "count": len(games_of_type),
                "pct": len(games_of_type) / len(classifications) * 100,
                "total_pnl": sum(pnls),
                "avg_pnl": statistics.mean(pnls),
                "median_pnl": statistics.median(pnls),
                "avg_roi": statistics.mean(rois),
                "median_roi": statistics.median(rois),
                "total_cost": sum(costs),
                "avg_cost": statistics.mean(costs),
                "win_rate": sum(1 for g in games_of_type if g["pnl"] > 0) / len(games_of_type) * 100,
                "avg_trade_count": statistics.mean([g["trade_count"] for g in games_of_type]),
            }
        else:
            type_stats[ptype] = {
                "count": 0, "pct": 0, "total_pnl": 0, "avg_pnl": 0,
                "median_pnl": 0, "avg_roi": 0, "median_roi": 0,
                "total_cost": 0, "avg_cost": 0, "win_rate": 0, "avg_trade_count": 0,
            }

    return classifications, type_stats


def extract_game_date_from_slug(slug):
    """Extract game date from slug like 'nba-nyk-bos-2026-02-08'."""
    match = re.search(r'(\d{4}-\d{2}-\d{2})$', slug)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def analyze_entry_timing(sequence_results):
    """Analyze entry timing relative to game start."""
    timing_data = []
    for r in sequence_results:
        slug = r["slug"] or r["eventSlug"]
        game_date = extract_game_date_from_slug(slug)
        if game_date is None:
            continue

        # Approximate game start: date at 00:00 UTC (games typically 23:00-03:00 UTC)
        # We'll use the date in slug as an approximation of game day
        # Most US sports games start in the evening ET = ~23:00-02:00 UTC
        # Use noon UTC of that day as a rough midpoint estimate
        game_start_approx = game_date.timestamp() + 12 * 3600  # noon UTC

        first_buy_ts = r["first_buy_ts"]
        last_buy_ts = r["last_buy_ts"]

        hours_before_first = (game_start_approx - first_buy_ts) / 3600
        hours_before_last = (game_start_approx - last_buy_ts) / 3600

        # Only include if reasonable (within 7 days before to 12 hours after)
        if -12 <= hours_before_first <= 168:
            timing_data.append({
                "conditionId": r["conditionId"],
                "slug": slug,
                "sport": r["sport"],
                "hours_before_first_buy": hours_before_first,
                "hours_before_last_buy": hours_before_last,
                "pnl": r["pnl"],
                "roi_pct": r["roi_pct"],
                "status": r["status"],
                "n_buys": r["n_buys"],
                "total_buy_cost": r["total_buy_cost"],
            })

    return timing_data


def compute_timing_correlation(timing_data):
    """Compute correlation between entry timing and P&L."""
    settled = [t for t in timing_data if t["status"] in ("WIN", "LOSS", "LOSS_OR_OPEN")]
    if len(settled) < 10:
        return 0.0, {}

    hours = [t["hours_before_first_buy"] for t in settled]
    rois = [t["roi_pct"] for t in settled]

    # Pearson correlation
    n = len(hours)
    mean_h = statistics.mean(hours)
    mean_r = statistics.mean(rois)
    cov = sum((h - mean_h) * (r - mean_r) for h, r in zip(hours, rois)) / n
    std_h = statistics.stdev(hours) if n > 1 else 1
    std_r = statistics.stdev(rois) if n > 1 else 1
    correlation = cov / (std_h * std_r) if std_h > 0 and std_r > 0 else 0

    # Bucket analysis
    buckets = {
        "0-2h (in-game/near start)": [],
        "2-6h before": [],
        "6-24h before": [],
        "1-3 days before": [],
        "3+ days before": [],
    }
    for t in settled:
        h = t["hours_before_first_buy"]
        if h < 2:
            buckets["0-2h (in-game/near start)"].append(t)
        elif h < 6:
            buckets["2-6h before"].append(t)
        elif h < 24:
            buckets["6-24h before"].append(t)
        elif h < 72:
            buckets["1-3 days before"].append(t)
        else:
            buckets["3+ days before"].append(t)

    bucket_stats = {}
    for name, items in buckets.items():
        if items:
            bucket_stats[name] = {
                "count": len(items),
                "avg_roi": statistics.mean([t["roi_pct"] for t in items]),
                "median_roi": statistics.median([t["roi_pct"] for t in items]),
                "total_pnl": sum(t["pnl"] for t in items),
                "win_rate": sum(1 for t in items if t["pnl"] > 0) / len(items) * 100,
                "avg_cost": statistics.mean([t["total_buy_cost"] for t in items]),
            }
        else:
            bucket_stats[name] = {"count": 0, "avg_roi": 0, "median_roi": 0,
                                   "total_pnl": 0, "win_rate": 0, "avg_cost": 0}

    return correlation, bucket_stats


def analyze_dca_effectiveness(sequence_results):
    """Compare single purchase vs DCA conditions in same price bands."""
    # Group by price bands
    price_bands = {
        "deep_longshot (0-0.10)": (0, 0.10),
        "longshot (0.10-0.25)": (0.10, 0.25),
        "underdog (0.25-0.40)": (0.25, 0.40),
        "slight_underdog (0.40-0.50)": (0.40, 0.50),
        "coin_flip (0.50-0.60)": (0.50, 0.60),
        "favorite (0.60-0.80)": (0.60, 0.80),
        "heavy_favorite (0.80+)": (0.80, 1.01),
    }

    results = {}
    settled = [r for r in sequence_results if r["status"] in ("WIN", "LOSS", "LOSS_OR_OPEN")]

    for band_name, (low, high) in price_bands.items():
        in_band = [r for r in settled if low <= r["avg_price"] < high]
        single = [r for r in in_band if r["n_buys"] == 1]
        dca = [r for r in in_band if r["n_buys"] > 1]

        band_result = {"total": len(in_band)}
        for label, group in [("single", single), ("dca", dca)]:
            if group:
                band_result[label] = {
                    "count": len(group),
                    "avg_roi": statistics.mean([r["roi_pct"] for r in group]),
                    "total_pnl": sum(r["pnl"] for r in group),
                    "win_rate": sum(1 for r in group if r["pnl"] > 0) / len(group) * 100,
                    "avg_cost": statistics.mean([r["total_buy_cost"] for r in group]),
                }
            else:
                band_result[label] = {"count": 0, "avg_roi": 0, "total_pnl": 0,
                                       "win_rate": 0, "avg_cost": 0}
        results[band_name] = band_result

    return results


def generate_summary_json(dca_stats, type_stats, timing_correlation, timing_bucket_stats,
                          dca_effectiveness, sequence_results):
    """Generate the summary JSON output."""
    settled = [r for r in sequence_results if r["status"] in ("WIN", "LOSS", "LOSS_OR_OPEN")]
    total_pnl = sum(r["pnl"] for r in settled)

    # Key findings
    key_findings = []

    # Best pattern type
    best_type = max(type_stats.items(), key=lambda x: x[1]["total_pnl"] if x[1]["count"] > 0 else float('-inf'))
    key_findings.append(
        f"'{best_type[0]}' pattern generates the most total P&L: ${best_type[1]['total_pnl']:,.0f} "
        f"across {best_type[1]['count']:,} games ({best_type[1]['avg_roi']:.1f}% avg ROI)"
    )

    # DCA effectiveness
    if dca_stats["multi_buy_count"] > 0:
        if dca_stats["avg_price_improvement_pct"] > 0:
            key_findings.append(
                f"DCA provides {dca_stats['avg_price_improvement_pct']:.1f}% average price improvement "
                f"over initial entry across {dca_stats['multi_buy_count']:,} multi-buy conditions"
            )
        else:
            key_findings.append(
                f"DCA does NOT provide price improvement on average "
                f"({dca_stats['avg_price_improvement_pct']:.1f}% change)"
            )

    # Timing insight
    best_bucket = max(timing_bucket_stats.items(),
                      key=lambda x: x[1]["total_pnl"] if x[1]["count"] > 10 else float('-inf'))
    if best_bucket[1]["count"] > 10:
        key_findings.append(
            f"Best entry timing: '{best_bucket[0]}' with {best_bucket[1]['avg_roi']:.1f}% avg ROI "
            f"and ${best_bucket[1]['total_pnl']:,.0f} total P&L ({best_bucket[1]['count']} conditions)"
        )

    # Win rate by type
    highest_wr_type = max(type_stats.items(),
                          key=lambda x: x[1]["win_rate"] if x[1]["count"] > 10 else 0)
    if highest_wr_type[1]["count"] > 10:
        key_findings.append(
            f"Highest win rate: '{highest_wr_type[0]}' at {highest_wr_type[1]['win_rate']:.1f}%"
        )

    # DCA vs single on same price bands
    dca_advantage_bands = []
    for band, data in dca_effectiveness.items():
        if data["dca"]["count"] > 5 and data["single"]["count"] > 5:
            if data["dca"]["avg_roi"] > data["single"]["avg_roi"]:
                dca_advantage_bands.append(band)
    if dca_advantage_bands:
        key_findings.append(
            f"DCA outperforms single-buy in {len(dca_advantage_bands)} price bands: "
            + ", ".join(dca_advantage_bands)
        )

    # Recommendations for nbabot
    recommendations = [
        f"Primary pattern: Use '{best_type[0]}' approach - it accounts for "
        f"${best_type[1]['total_pnl']:,.0f} of total P&L",
    ]

    if dca_stats["avg_price_improvement_pct"] > 1:
        recommendations.append(
            f"Implement DCA with ~{dca_stats['median_buys_per_condition']:.0f} tranches "
            f"for {dca_stats['avg_price_improvement_pct']:.1f}% price improvement"
        )
    else:
        recommendations.append(
            "DCA shows limited price improvement; consider single-entry for simplicity "
            "unless market is volatile"
        )

    if best_bucket[1]["count"] > 10:
        recommendations.append(
            f"Target entry timing: {best_bucket[0]} before game start for optimal ROI"
        )

    # Size recommendations
    for ptype, stats in type_stats.items():
        if stats["count"] > 10 and stats["avg_roi"] > 0:
            recommendations.append(
                f"For '{ptype}' pattern, avg position size is ${stats['avg_cost']:,.0f} "
                f"with {stats['avg_trade_count']:.0f} trades per game"
            )

    summary = {
        "agent": "position-architect",
        "total_conditions_analyzed": len(sequence_results),
        "total_trades_analyzed": sum(r["n_total_trades"] for r in sequence_results),
        "pattern_distribution": {
            ptype: {
                "count": stats["count"],
                "pct": round(stats["pct"], 1),
                "avg_roi": round(stats["avg_roi"], 2),
                "total_pnl": round(stats["total_pnl"], 2),
            }
            for ptype, stats in type_stats.items()
        },
        "dca_stats": {
            "avg_buys_per_condition": round(dca_stats["avg_buys_per_condition"], 2),
            "median_buys_per_condition": round(dca_stats["median_buys_per_condition"], 1),
            "avg_price_improvement_pct": round(dca_stats["avg_price_improvement_pct"], 2),
            "avg_time_span_hours": round(dca_stats["avg_time_span_hours"], 2),
        },
        "timing_correlation": round(timing_correlation, 4),
        "key_findings": key_findings,
        "nbabot_recommendations": recommendations,
    }
    return summary


def generate_report_md(sequence_results, dca_stats, type_stats, timing_correlation,
                       timing_bucket_stats, dca_effectiveness, classifications):
    """Generate the detailed markdown report."""
    lines = []
    lines.append("# Agent 1: Position Architecture Analysis")
    lines.append("")
    lines.append("## Overview")
    lines.append("")
    lines.append("This report analyzes how lhtsports constructs positions within individual")
    lines.append("games/events. It covers trade sequences, DCA patterns, position building types,")
    lines.append("entry timing, and the effectiveness of different construction approaches.")
    lines.append("")

    total_conditions = len(sequence_results)
    total_trades = sum(r["n_total_trades"] for r in sequence_results)
    total_games = len(classifications)
    lines.append(f"- **Conditions analyzed**: {total_conditions:,}")
    lines.append(f"- **Trades analyzed**: {total_trades:,}")
    lines.append(f"- **Games analyzed**: {total_games:,}")
    lines.append("")

    # Section 1: Trade Sequence Analysis
    lines.append("---")
    lines.append("")
    lines.append("## 1. Trade Sequence Within Conditions")
    lines.append("")
    lines.append("### Buy Count Distribution")
    lines.append("")
    lines.append("| Buys per Condition | Count | % |")
    lines.append("|---|---|---|")
    ordered_keys = ["1", "2-3", "4-5", "6-10", "11-20", "21-50", "50+"]
    for key in ordered_keys:
        count = dca_stats["buy_count_distribution"].get(key, 0)
        pct = count / total_conditions * 100 if total_conditions > 0 else 0
        lines.append(f"| {key} | {count:,} | {pct:.1f}% |")
    lines.append("")

    lines.append(f"- **Average buys per condition**: {dca_stats['avg_buys_per_condition']:.1f}")
    lines.append(f"- **Median buys per condition**: {dca_stats['median_buys_per_condition']:.0f}")
    lines.append(f"- **Max buys in a single condition**: {dca_stats['max_buys_per_condition']}")
    lines.append("")

    # Top 10 most-traded conditions
    lines.append("### Top 10 Most-Traded Conditions")
    lines.append("")
    lines.append("| Slug | Buys | Sells | Total Cost | Avg Price | P&L | Status |")
    lines.append("|---|---|---|---|---|---|---|")
    top_traded = sorted(sequence_results, key=lambda x: x["n_total_trades"], reverse=True)[:10]
    for r in top_traded:
        slug_short = (r["slug"] or r["eventSlug"])[:50]
        lines.append(
            f"| {slug_short} | {r['n_buys']} | {r['n_sells']} | "
            f"${r['total_buy_cost']:,.0f} | {r['avg_price']:.3f} | "
            f"${r['pnl']:,.0f} | {r['status']} |"
        )
    lines.append("")

    # Section 2: DCA Pattern Detection
    lines.append("---")
    lines.append("")
    lines.append("## 2. DCA Pattern Detection")
    lines.append("")
    lines.append(f"- **Single-buy conditions**: {dca_stats['single_buy_count']:,} "
                 f"({dca_stats['single_buy_count']/total_conditions*100:.1f}%)")
    lines.append(f"- **Multi-buy (DCA) conditions**: {dca_stats['multi_buy_count']:,} "
                 f"({dca_stats['multi_buy_count']/total_conditions*100:.1f}%)")
    lines.append("")
    lines.append("### DCA Characteristics")
    lines.append("")
    lines.append(f"- **Average price coefficient of variation**: {dca_stats['avg_price_cv']:.3f}")
    lines.append(f"- **Average time span of purchases**: {dca_stats['avg_time_span_hours']:.1f} hours")
    lines.append(f"- **Median time span**: {dca_stats['median_time_span_hours']:.1f} hours")
    lines.append(f"- **Conditions where trader averaged down**: {dca_stats['avg_down_pct']:.1f}%")
    lines.append(f"- **Average VWAP improvement over first price**: "
                 f"{dca_stats['avg_price_improvement_pct']:.2f}%")
    lines.append("")

    lines.append("### Single-Buy vs DCA P&L Comparison")
    lines.append("")
    lines.append("| Metric | Single Buy | DCA (Multi-Buy) |")
    lines.append("|---|---|---|")
    lines.append(f"| Count | {dca_stats['single_buy_count']:,} | {dca_stats['multi_buy_count']:,} |")
    lines.append(f"| Avg P&L | ${dca_stats['single_buy_avg_pnl']:,.0f} | "
                 f"${dca_stats['multi_buy_avg_pnl']:,.0f} |")
    lines.append(f"| Total P&L | ${dca_stats['single_buy_total_pnl']:,.0f} | "
                 f"${dca_stats['multi_buy_total_pnl']:,.0f} |")
    lines.append(f"| Avg ROI | {dca_stats['single_buy_avg_roi']:.1f}% | "
                 f"{dca_stats['multi_buy_avg_roi']:.1f}% |")
    lines.append("")

    # Section 3: Position Building Type Classification
    lines.append("---")
    lines.append("")
    lines.append("## 3. Position Building Type Classification")
    lines.append("")
    lines.append("Games are classified into four position construction patterns:")
    lines.append("")
    lines.append("- **Type A (single_buy)**: Single purchase on one condition")
    lines.append("- **Type B (dca)**: Multiple BUYs on same condition(s), single market type")
    lines.append("- **Type C (multi_market)**: Multiple market types (ML + Spread + Total), single buys")
    lines.append("- **Type D (complex)**: Multiple market types + DCA")
    lines.append("")

    lines.append("| Type | Count | % | Total P&L | Avg P&L | Avg ROI | Med ROI | Win Rate | Avg Cost |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for ptype in ["single_buy", "dca", "multi_market", "complex"]:
        s = type_stats[ptype]
        lines.append(
            f"| {ptype} | {s['count']:,} | {s['pct']:.1f}% | "
            f"${s['total_pnl']:,.0f} | ${s['avg_pnl']:,.0f} | "
            f"{s['avg_roi']:.1f}% | {s['median_roi']:.1f}% | "
            f"{s['win_rate']:.1f}% | ${s['avg_cost']:,.0f} |"
        )
    lines.append("")

    # Sport breakdown per type
    lines.append("### Position Type by Sport")
    lines.append("")
    sport_type_counts = defaultdict(lambda: defaultdict(int))
    sport_type_pnl = defaultdict(lambda: defaultdict(float))
    for gk, info in classifications.items():
        sport_type_counts[info["sport"]][info["type"]] += 1
        sport_type_pnl[info["sport"]][info["type"]] += info["pnl"]

    all_sports = sorted(sport_type_counts.keys())
    lines.append("| Sport | single_buy | dca | multi_market | complex | Total |")
    lines.append("|---|---|---|---|---|---|")
    for sport in all_sports:
        counts = sport_type_counts[sport]
        total = sum(counts.values())
        lines.append(
            f"| {sport} | {counts.get('single_buy', 0)} | {counts.get('dca', 0)} | "
            f"{counts.get('multi_market', 0)} | {counts.get('complex', 0)} | {total} |"
        )
    lines.append("")

    # Section 4: Entry Timing Analysis
    lines.append("---")
    lines.append("")
    lines.append("## 4. Entry Timing Analysis")
    lines.append("")
    lines.append(f"**Timing-ROI correlation**: {timing_correlation:.4f}")
    lines.append("")
    lines.append("### P&L by Entry Timing Bucket")
    lines.append("")
    lines.append("| Timing Bucket | Count | Avg ROI | Med ROI | Total P&L | Win Rate | Avg Cost |")
    lines.append("|---|---|---|---|---|---|---|")
    bucket_order = [
        "0-2h (in-game/near start)", "2-6h before",
        "6-24h before", "1-3 days before", "3+ days before"
    ]
    for bucket in bucket_order:
        s = timing_bucket_stats.get(bucket, {})
        if s.get("count", 0) > 0:
            lines.append(
                f"| {bucket} | {s['count']:,} | {s['avg_roi']:.1f}% | "
                f"{s['median_roi']:.1f}% | ${s['total_pnl']:,.0f} | "
                f"{s['win_rate']:.1f}% | ${s['avg_cost']:,.0f} |"
            )
        else:
            lines.append(f"| {bucket} | 0 | - | - | - | - | - |")
    lines.append("")

    # Section 5: Type-by-Type P&L Comparison (detailed)
    lines.append("---")
    lines.append("")
    lines.append("## 5. Type-by-Type P&L Deep Dive")
    lines.append("")

    for ptype in ["single_buy", "dca", "multi_market", "complex"]:
        s = type_stats[ptype]
        lines.append(f"### {ptype}")
        lines.append("")
        if s["count"] == 0:
            lines.append("No games of this type.")
            lines.append("")
            continue

        lines.append(f"- **Games**: {s['count']:,}")
        lines.append(f"- **Total invested**: ${s['total_cost']:,.0f}")
        lines.append(f"- **Total P&L**: ${s['total_pnl']:,.0f}")
        lines.append(f"- **Average P&L per game**: ${s['avg_pnl']:,.0f}")
        lines.append(f"- **Median P&L per game**: ${s['median_pnl']:,.0f}")
        lines.append(f"- **Average ROI**: {s['avg_roi']:.1f}%")
        lines.append(f"- **Win rate**: {s['win_rate']:.1f}%")
        lines.append(f"- **Average trades per game**: {s['avg_trade_count']:.1f}")
        lines.append("")

        # Top 5 winners and losers of this type
        type_games = sorted(
            [(gk, info) for gk, info in classifications.items() if info["type"] == ptype],
            key=lambda x: x[1]["pnl"], reverse=True
        )
        if type_games:
            lines.append("**Top 5 Winners:**")
            lines.append("")
            lines.append("| Game | P&L | ROI | Cost | Trades |")
            lines.append("|---|---|---|---|---|")
            for gk, info in type_games[:5]:
                lines.append(
                    f"| {gk[:50]} | ${info['pnl']:,.0f} | {info['roi_pct']:.0f}% | "
                    f"${info['buy_cost']:,.0f} | {info['trade_count']} |"
                )
            lines.append("")

            lines.append("**Top 5 Losers:**")
            lines.append("")
            lines.append("| Game | P&L | ROI | Cost | Trades |")
            lines.append("|---|---|---|---|---|")
            for gk, info in type_games[-5:]:
                lines.append(
                    f"| {gk[:50]} | ${info['pnl']:,.0f} | {info['roi_pct']:.0f}% | "
                    f"${info['buy_cost']:,.0f} | {info['trade_count']} |"
                )
            lines.append("")

    # Section 6: DCA Effectiveness by Price Band
    lines.append("---")
    lines.append("")
    lines.append("## 6. DCA Effectiveness by Price Band")
    lines.append("")
    lines.append("Comparing single-purchase vs DCA conditions at the same price levels.")
    lines.append("")
    lines.append("| Price Band | Single Count | Single ROI | Single WR | DCA Count | DCA ROI | DCA WR |")
    lines.append("|---|---|---|---|---|---|---|")
    for band_name in [
        "deep_longshot (0-0.10)", "longshot (0.10-0.25)", "underdog (0.25-0.40)",
        "slight_underdog (0.40-0.50)", "coin_flip (0.50-0.60)",
        "favorite (0.60-0.80)", "heavy_favorite (0.80+)"
    ]:
        d = dca_effectiveness.get(band_name, {})
        s = d.get("single", {})
        m = d.get("dca", {})
        lines.append(
            f"| {band_name} | {s.get('count', 0)} | "
            f"{s.get('avg_roi', 0):.1f}% | {s.get('win_rate', 0):.1f}% | "
            f"{m.get('count', 0)} | {m.get('avg_roi', 0):.1f}% | "
            f"{m.get('win_rate', 0):.1f}% |"
        )
    lines.append("")

    # Conclusions
    lines.append("---")
    lines.append("")
    lines.append("## Conclusions and Recommendations for nbabot")
    lines.append("")

    # Compute overall summary stats
    settled = [r for r in sequence_results if r["status"] in ("WIN", "LOSS", "LOSS_OR_OPEN")]
    total_pnl = sum(r["pnl"] for r in settled)
    lines.append(f"**Overall settled P&L**: ${total_pnl:,.0f}")
    lines.append("")

    best_type = max(type_stats.items(), key=lambda x: x[1]["total_pnl"] if x[1]["count"] > 0 else float('-inf'))
    lines.append(f"1. **Dominant pattern**: '{best_type[0]}' accounts for "
                 f"${best_type[1]['total_pnl']:,.0f} total P&L with "
                 f"{best_type[1]['win_rate']:.1f}% win rate.")
    lines.append("")

    if dca_stats["avg_price_improvement_pct"] > 0:
        lines.append(f"2. **DCA value**: Provides {dca_stats['avg_price_improvement_pct']:.1f}% "
                     f"average price improvement. Median DCA uses "
                     f"{dca_stats['median_buys_per_condition']:.0f} buys over "
                     f"{dca_stats['median_time_span_hours']:.1f} hours.")
    else:
        lines.append(f"2. **DCA value**: Does not provide price improvement on average "
                     f"({dca_stats['avg_price_improvement_pct']:.1f}%). "
                     f"Consider whether the execution complexity is justified.")
    lines.append("")

    lines.append(f"3. **Entry timing**: Correlation between timing and ROI is "
                 f"{timing_correlation:.3f}.")
    best_bucket = max(timing_bucket_stats.items(),
                      key=lambda x: x[1]["total_pnl"] if x[1]["count"] > 10 else float('-inf'))
    if best_bucket[1]["count"] > 10:
        lines.append(f"   Best timing bucket: '{best_bucket[0]}' with "
                     f"{best_bucket[1]['avg_roi']:.1f}% avg ROI.")
    lines.append("")

    lines.append("4. **nbabot implementation**: Based on lhtsports's most profitable patterns:")
    for ptype, stats in type_stats.items():
        if stats["count"] > 10 and stats["total_pnl"] > 0:
            lines.append(f"   - {ptype}: ${stats['avg_cost']:,.0f} avg position, "
                         f"{stats['avg_trade_count']:.0f} trades, {stats['avg_roi']:.1f}% avg ROI")
    lines.append("")

    return "\n".join(lines)


def main():
    print("=" * 60)
    print("Agent 1: Position Architecture Analysis")
    print("=" * 60)
    print()

    # Load data
    trades, condition_pnl, game_pnl = load_data()

    # Build lookup maps
    condition_pnl_map = {c["conditionId"]: c for c in condition_pnl}
    game_pnl_map = {g["game_key"]: g for g in game_pnl}

    # Group trades
    print("\nGrouping trades by condition...")
    trades_by_condition = group_trades_by_condition(trades)
    print(f"  {len(trades_by_condition):,} unique conditions with trades")

    print("Grouping trades by game...")
    trades_by_game = group_trades_by_game(trades)
    print(f"  {len(trades_by_game):,} unique games with trades")

    # 1. Trade sequence analysis
    print("\n--- Analysis 1: Trade Sequences ---")
    sequence_results = analyze_trade_sequences(trades_by_condition, condition_pnl_map)
    print(f"  Analyzed {len(sequence_results):,} conditions with buy trades")

    # 2. DCA pattern detection
    print("\n--- Analysis 2: DCA Patterns ---")
    dca_stats = analyze_dca_patterns(sequence_results)
    print(f"  Single-buy: {dca_stats['single_buy_count']:,}")
    print(f"  Multi-buy (DCA): {dca_stats['multi_buy_count']:,}")
    print(f"  Avg buys/condition: {dca_stats['avg_buys_per_condition']:.1f}")
    print(f"  Avg price improvement: {dca_stats['avg_price_improvement_pct']:.2f}%")

    # 3. Position type classification
    print("\n--- Analysis 3: Position Type Classification ---")
    classifications, type_stats = classify_all_games(
        game_pnl, trades_by_game, game_pnl_map, condition_pnl_map
    )
    for ptype in ["single_buy", "dca", "multi_market", "complex"]:
        s = type_stats[ptype]
        print(f"  {ptype}: {s['count']:,} games, ${s['total_pnl']:,.0f} P&L, "
              f"{s['avg_roi']:.1f}% avg ROI")

    # 4. Entry timing analysis
    print("\n--- Analysis 4: Entry Timing ---")
    timing_data = analyze_entry_timing(sequence_results)
    timing_correlation, timing_bucket_stats = compute_timing_correlation(timing_data)
    print(f"  Conditions with timing data: {len(timing_data):,}")
    print(f"  Timing-ROI correlation: {timing_correlation:.4f}")
    for bucket, stats in timing_bucket_stats.items():
        if stats["count"] > 0:
            print(f"    {bucket}: {stats['count']:,} conditions, "
                  f"{stats['avg_roi']:.1f}% avg ROI, ${stats['total_pnl']:,.0f} P&L")

    # 6. DCA effectiveness by price band
    print("\n--- Analysis 6: DCA Effectiveness by Price Band ---")
    dca_effectiveness = analyze_dca_effectiveness(sequence_results)
    for band, data in dca_effectiveness.items():
        s = data.get("single", {})
        d = data.get("dca", {})
        if s.get("count", 0) > 0 or d.get("count", 0) > 0:
            print(f"  {band}: single={s.get('count', 0)} ({s.get('avg_roi', 0):.1f}% ROI), "
                  f"dca={d.get('count', 0)} ({d.get('avg_roi', 0):.1f}% ROI)")

    # Generate outputs
    print("\n--- Generating outputs ---")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Summary JSON
    summary = generate_summary_json(
        dca_stats, type_stats, timing_correlation, timing_bucket_stats,
        dca_effectiveness, sequence_results
    )
    summary_path = os.path.join(OUTPUT_DIR, "agent1-summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"  Wrote {summary_path}")

    # Detailed report
    report = generate_report_md(
        sequence_results, dca_stats, type_stats, timing_correlation,
        timing_bucket_stats, dca_effectiveness, classifications
    )
    report_path = os.path.join(OUTPUT_DIR, "agent1-position-architecture.md")
    with open(report_path, "w") as f:
        f.write(report)
    print(f"  Wrote {report_path}")

    print("\n" + "=" * 60)
    print("Analysis complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
