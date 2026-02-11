#!/usr/bin/env python3
"""Agent 3: Multi-market analyst.

Analyzes synergies and anti-synergies between Moneyline, Spread, and Total
markets in lhtsports trading data.
"""

import json
import os
from collections import Counter, defaultdict
from datetime import datetime

BASE = "/Users/taro/dev/nbabot"
CONDITION_PNL = os.path.join(BASE, "data/reports/lhtsports-pnl/condition_pnl.json")
GAME_PNL = os.path.join(BASE, "data/reports/lhtsports-pnl/game_pnl.json")
TRADES = os.path.join(BASE, "data/reports/lhtsports-analysis/lhtsports_all_trades.json")
MERGES = os.path.join(BASE, "data/reports/lhtsports-analysis/lhtsports_merge.json")
OUT_DIR = os.path.join(BASE, "data/reports/lhtsports-pnl/deep-analysis")

MAJOR_SPORTS = {"NBA", "NFL", "MLB", "NHL", "CFB"}


def load_json(path: str) -> list:
    with open(path) as f:
        return json.load(f)


def combo_label(market_types: list[str]) -> str:
    """Canonical label for a market_types combination."""
    s = sorted(set(market_types))
    mapping = {
        ("Moneyline",): "ML_only",
        ("Moneyline", "Spread"): "ML+Spread",
        ("Moneyline", "Total"): "ML+Total",
        ("Moneyline", "Spread", "Total"): "ML+Spread+Total",
        ("Spread",): "Spread_only",
        ("Total",): "Total_only",
        ("Spread", "Total"): "Spread+Total",
    }
    return mapping.get(tuple(s), "Other:" + "+".join(s))


def game_won(game: dict) -> bool:
    """Game is profitable (positive P&L)."""
    return game["total_pnl"] > 0


def safe_div(a: float, b: float) -> float:
    return a / b if b != 0 else 0.0


def pct(a: float, b: float) -> float:
    return round(safe_div(a, b) * 100, 2)


# ---------------------------------------------------------------------------
# 1. Market combination breakdown
# ---------------------------------------------------------------------------
def market_combo_breakdown(games: list[dict]) -> dict:
    """Group games by market_types combo and compute stats."""
    groups: dict[str, list[dict]] = defaultdict(list)
    for g in games:
        label = combo_label(g["market_types"])
        groups[label].append(g)

    result = {}
    for label in sorted(groups):
        gg = groups[label]
        total_cost = sum(g["net_cost"] for g in gg)
        total_pnl = sum(g["total_pnl"] for g in gg)
        wins = sum(1 for g in gg if game_won(g))
        settled = [g for g in gg if g.get("fully_settled", True)]
        settled_wins = sum(1 for g in settled if game_won(g))
        total_payout = sum(g.get("total_payout", 0) for g in gg)
        avg_payoff = safe_div(total_payout, total_cost) if total_cost > 0 else 0
        result[label] = {
            "n_games": len(gg),
            "total_cost": round(total_cost, 2),
            "total_pnl": round(total_pnl, 2),
            "roi_pct": pct(total_pnl, total_cost),
            "win_rate_all": pct(wins, len(gg)),
            "win_rate_settled": pct(settled_wins, len(settled)) if settled else 0,
            "avg_cost_per_game": round(safe_div(total_cost, len(gg)), 2),
            "avg_payoff_ratio": round(avg_payoff, 3),
        }
    return result


# ---------------------------------------------------------------------------
# 2. ML + Total synergy deep-dive
# ---------------------------------------------------------------------------
def ml_total_synergy(games: list[dict], conds: list[dict]) -> dict:
    """Analyze ML vs Total components within ML+Total games."""
    # Build condition lookup by eventSlug
    cond_by_event: dict[str, list[dict]] = defaultdict(list)
    for c in conds:
        cond_by_event[c["eventSlug"]].append(c)

    ml_total_games = [
        g for g in games if combo_label(g["market_types"]) == "ML+Total"
    ]

    ml_pnl_total = 0.0
    ml_cost_total = 0.0
    total_pnl_total = 0.0
    total_cost_total = 0.0

    both_win = 0
    both_lose = 0
    ml_win_total_lose = 0
    ml_lose_total_win = 0
    counted = 0

    for g in ml_total_games:
        event_conds = cond_by_event.get(g["game_key"], [])
        ml_conds = [c for c in event_conds if c["market_type"] == "Moneyline"]
        tot_conds = [c for c in event_conds if c["market_type"] == "Total"]

        if not ml_conds or not tot_conds:
            continue

        ml_pnl = sum(c["pnl"] for c in ml_conds)
        ml_cost = sum(c["net_cost"] for c in ml_conds)
        tot_pnl = sum(c["pnl"] for c in tot_conds)
        tot_cost = sum(c["net_cost"] for c in tot_conds)

        ml_pnl_total += ml_pnl
        ml_cost_total += ml_cost
        total_pnl_total += tot_pnl
        total_cost_total += tot_cost

        ml_won = ml_pnl > 0
        tot_won = tot_pnl > 0

        counted += 1
        if ml_won and tot_won:
            both_win += 1
        elif not ml_won and not tot_won:
            both_lose += 1
        elif ml_won:
            ml_win_total_lose += 1
        else:
            ml_lose_total_win += 1

    # Phi coefficient as proxy for correlation
    # Using 2x2: ML_win/lose x Total_win/lose
    a, b, c_val, d = both_win, ml_win_total_lose, ml_lose_total_win, both_lose
    n = a + b + c_val + d
    denom = ((a + b) * (c_val + d) * (a + c_val) * (b + d)) ** 0.5
    phi = safe_div((a * d - b * c_val), denom) if denom > 0 else 0

    # Determine synergy type
    if phi > 0.2:
        synergy_type = "correlated_wins"
    elif phi < -0.2:
        synergy_type = "hedge"
    else:
        synergy_type = "independent_edge"

    return {
        "n_games_analyzed": counted,
        "ml_component_cost": round(ml_cost_total, 2),
        "ml_component_pnl": round(ml_pnl_total, 2),
        "ml_component_roi": pct(ml_pnl_total, ml_cost_total),
        "total_component_cost": round(total_cost_total, 2),
        "total_component_pnl": round(total_pnl_total, 2),
        "total_component_roi": pct(total_pnl_total, total_cost_total),
        "outcome_matrix": {
            "both_win": both_win,
            "ml_win_total_lose": ml_win_total_lose,
            "ml_lose_total_win": ml_lose_total_win,
            "both_lose": both_lose,
        },
        "phi_correlation": round(phi, 4),
        "synergy_type": synergy_type,
    }


# ---------------------------------------------------------------------------
# 3. Spread toxicity analysis
# ---------------------------------------------------------------------------
def spread_analysis(games: list[dict], conds: list[dict]) -> dict:
    """Analyze Spread performance and its drag effect."""
    cond_by_event: dict[str, list[dict]] = defaultdict(list)
    for c in conds:
        cond_by_event[c["eventSlug"]].append(c)

    # Spread-only conditions
    spread_conds = [c for c in conds if c["market_type"] == "Spread"]
    spread_wins = sum(1 for c in spread_conds if c["status"] == "WIN")
    spread_cost = sum(c["net_cost"] for c in spread_conds)
    spread_pnl = sum(c["pnl"] for c in spread_conds)
    spread_merged = sum(1 for c in spread_conds if c["status"] == "MERGED")

    # Compare ML-only vs ML+Spread games
    ml_only_games = [g for g in games if combo_label(g["market_types"]) == "ML_only"]
    ml_spread_games = [g for g in games if combo_label(g["market_types"]) == "ML+Spread"]

    ml_only_roi = pct(
        sum(g["total_pnl"] for g in ml_only_games),
        sum(g["net_cost"] for g in ml_only_games),
    )
    ml_spread_roi = pct(
        sum(g["total_pnl"] for g in ml_spread_games),
        sum(g["net_cost"] for g in ml_spread_games),
    )

    # Spread component within ML+Spread games
    sp_component_pnl = 0.0
    sp_component_cost = 0.0
    ml_in_ms_pnl = 0.0
    ml_in_ms_cost = 0.0
    for g in ml_spread_games:
        ec = cond_by_event.get(g["game_key"], [])
        for c in ec:
            if c["market_type"] == "Spread":
                sp_component_pnl += c["pnl"]
                sp_component_cost += c["net_cost"]
            elif c["market_type"] == "Moneyline":
                ml_in_ms_pnl += c["pnl"]
                ml_in_ms_cost += c["net_cost"]

    # Price band analysis for spread losses
    spread_losses = [c for c in spread_conds if c["pnl"] < 0]
    spread_wins_list = [c for c in spread_conds if c["pnl"] > 0]
    avg_price_loss = safe_div(
        sum(c["avg_buy_price"] for c in spread_losses), len(spread_losses)
    )
    avg_price_win = safe_div(
        sum(c["avg_buy_price"] for c in spread_wins_list), len(spread_wins_list)
    )

    # Sports breakdown for spread
    spread_by_sport: dict[str, list[dict]] = defaultdict(list)
    for c in spread_conds:
        spread_by_sport[c["sport"]].append(c)

    sport_spread_roi = {}
    for sport, sc in sorted(spread_by_sport.items()):
        cost = sum(c["net_cost"] for c in sc)
        pnl = sum(c["pnl"] for c in sc)
        sport_spread_roi[sport] = {
            "n": len(sc),
            "cost": round(cost, 2),
            "pnl": round(pnl, 2),
            "roi_pct": pct(pnl, cost),
            "win_rate": pct(sum(1 for c in sc if c["status"] == "WIN"), len(sc)),
        }

    return {
        "spread_conditions_total": len(spread_conds),
        "spread_win_rate": pct(spread_wins, len(spread_conds)),
        "spread_cost": round(spread_cost, 2),
        "spread_pnl": round(spread_pnl, 2),
        "spread_roi_pct": pct(spread_pnl, spread_cost),
        "spread_merge_rate": pct(spread_merged, len(spread_conds)),
        "ml_only_game_roi": ml_only_roi,
        "ml_spread_game_roi": ml_spread_roi,
        "spread_drag": round(ml_only_roi - ml_spread_roi, 2),
        "spread_component_in_ml_spread": {
            "cost": round(sp_component_cost, 2),
            "pnl": round(sp_component_pnl, 2),
            "roi_pct": pct(sp_component_pnl, sp_component_cost),
        },
        "ml_component_in_ml_spread": {
            "cost": round(ml_in_ms_cost, 2),
            "pnl": round(ml_in_ms_pnl, 2),
            "roi_pct": pct(ml_in_ms_pnl, ml_in_ms_cost),
        },
        "avg_buy_price_losses": round(avg_price_loss, 4),
        "avg_buy_price_wins": round(avg_price_win, 4),
        "sport_breakdown": sport_spread_roi,
    }


# ---------------------------------------------------------------------------
# 4. Directional analysis within ML+Total games
# ---------------------------------------------------------------------------
def directional_analysis(games: list[dict], conds: list[dict]) -> dict:
    """Analyze ML side + Total side combinations."""
    cond_by_event: dict[str, list[dict]] = defaultdict(list)
    for c in conds:
        cond_by_event[c["eventSlug"]].append(c)

    ml_total_games = [
        g for g in games if combo_label(g["market_types"]) == "ML+Total"
    ]

    combos: dict[str, dict] = defaultdict(lambda: {"n": 0, "pnl": 0.0, "cost": 0.0, "wins": 0})

    for g in ml_total_games:
        ec = cond_by_event.get(g["game_key"], [])
        ml_conds = [c for c in ec if c["market_type"] == "Moneyline"]
        tot_conds = [c for c in ec if c["market_type"] == "Total"]

        if not ml_conds or not tot_conds:
            continue

        # Determine ML direction - pick the main bought side
        # We take the side with highest net_cost
        ml_main = max(ml_conds, key=lambda c: c["net_cost"])
        tot_main = max(tot_conds, key=lambda c: c["net_cost"])

        # Categorize Total outcome
        total_side = tot_main["outcome_bought"]
        if total_side not in ("Over", "Under"):
            # Try to infer from slug
            slug = tot_main.get("slug", "")
            if "over" in slug.lower():
                total_side = "Over"
            elif "under" in slug.lower():
                total_side = "Under"
            else:
                total_side = "Unknown"

        # Categorize ML as Favorite/Underdog using price
        ml_price = ml_main["avg_buy_price"]
        if ml_price >= 0.5:
            ml_dir = "Favorite"
        else:
            ml_dir = "Underdog"

        combo_key = f"{ml_dir}+{total_side}"
        combos[combo_key]["n"] += 1
        combos[combo_key]["pnl"] += g["total_pnl"]
        combos[combo_key]["cost"] += g["net_cost"]
        combos[combo_key]["wins"] += 1 if game_won(g) else 0

    result = {}
    for key in sorted(combos):
        d = combos[key]
        result[key] = {
            "n_games": d["n"],
            "total_cost": round(d["cost"], 2),
            "total_pnl": round(d["pnl"], 2),
            "roi_pct": pct(d["pnl"], d["cost"]),
            "win_rate": pct(d["wins"], d["n"]),
        }
    return result


# ---------------------------------------------------------------------------
# 5. MERGE analysis by market type
# ---------------------------------------------------------------------------
def merge_analysis(conds: list[dict], merges: list[dict]) -> dict:
    """Analyze MERGE frequency by market type."""
    # Build conditionId -> market_type from conditions
    cid_to_mt: dict[str, str] = {}
    for c in conds:
        cid_to_mt[c["conditionId"]] = c["market_type"]

    # Count merges by market type
    merge_by_mt: dict[str, int] = Counter()
    merge_usdc_by_mt: dict[str, float] = defaultdict(float)
    unmatched = 0

    for m in merges:
        mt = cid_to_mt.get(m["conditionId"])
        if mt:
            merge_by_mt[mt] += 1
            merge_usdc_by_mt[mt] += m.get("usdcSize", 0)
        else:
            unmatched += 1

    # Merge rate by market type
    cond_count_by_mt = Counter(c["market_type"] for c in conds)
    merged_cond_by_mt = Counter(
        c["market_type"] for c in conds if c["status"] == "MERGED"
    )

    result = {}
    for mt in ["Moneyline", "Spread", "Total"]:
        result[mt] = {
            "merge_txns": merge_by_mt.get(mt, 0),
            "merge_usdc": round(merge_usdc_by_mt.get(mt, 0), 2),
            "conditions_merged": merged_cond_by_mt.get(mt, 0),
            "total_conditions": cond_count_by_mt.get(mt, 0),
            "merge_rate_pct": pct(
                merged_cond_by_mt.get(mt, 0), cond_count_by_mt.get(mt, 0)
            ),
        }

    # Merge timing analysis: average time from first trade to merge
    cond_first_trade: dict[str, int] = {}
    for c in conds:
        cond_first_trade[c["conditionId"]] = c.get("first_trade_ts", 0)

    timing_by_mt: dict[str, list[float]] = defaultdict(list)
    for m in merges:
        mt = cid_to_mt.get(m["conditionId"])
        ft = cond_first_trade.get(m["conditionId"], 0)
        if mt and ft > 0:
            hours_since_first = (m["timestamp"] - ft) / 3600
            timing_by_mt[mt].append(hours_since_first)

    timing_stats = {}
    for mt in ["Moneyline", "Spread", "Total"]:
        times = timing_by_mt.get(mt, [])
        if times:
            timing_stats[mt] = {
                "avg_hours": round(sum(times) / len(times), 1),
                "median_hours": round(sorted(times)[len(times) // 2], 1),
                "n_samples": len(times),
            }
        else:
            timing_stats[mt] = {"avg_hours": 0, "median_hours": 0, "n_samples": 0}

    return {
        "merge_counts": result,
        "merge_timing": timing_stats,
        "unmatched_merges": unmatched,
    }


# ---------------------------------------------------------------------------
# 6. Sport-specific multi-market patterns
# ---------------------------------------------------------------------------
def sport_multi_market(games: list[dict]) -> dict:
    """Analyze which sports benefit most from multi-market approach."""
    result = {}

    for sport in MAJOR_SPORTS:
        sport_games = [g for g in games if g["sport"] == sport]
        if not sport_games:
            continue

        combos: dict[str, list[dict]] = defaultdict(list)
        for g in sport_games:
            combos[combo_label(g["market_types"])].append(g)

        sport_result = {}
        best_combo = None
        best_roi = -999

        for label, gg in sorted(combos.items()):
            if len(gg) < 5:  # minimum sample size
                continue
            cost = sum(g["net_cost"] for g in gg)
            pnl = sum(g["total_pnl"] for g in gg)
            roi = pct(pnl, cost)
            wr = pct(sum(1 for g in gg if game_won(g)), len(gg))
            sport_result[label] = {
                "n_games": len(gg),
                "total_cost": round(cost, 2),
                "total_pnl": round(pnl, 2),
                "roi_pct": roi,
                "win_rate": wr,
            }
            if roi > best_roi:
                best_roi = roi
                best_combo = label

        result[sport] = {
            "total_games": len(sport_games),
            "combos": sport_result,
            "best_combo": best_combo,
            "best_combo_roi": best_roi,
        }

    return result


# ---------------------------------------------------------------------------
# 7. 3-layer stacking effect
# ---------------------------------------------------------------------------
def three_layer_decomp(games: list[dict], conds: list[dict]) -> dict:
    """Decompose ML+Spread+Total by component."""
    cond_by_event: dict[str, list[dict]] = defaultdict(list)
    for c in conds:
        cond_by_event[c["eventSlug"]].append(c)

    triple_games = [
        g for g in games if combo_label(g["market_types"]) == "ML+Spread+Total"
    ]

    ml_pnl = 0.0
    ml_cost = 0.0
    sp_pnl = 0.0
    sp_cost = 0.0
    tot_pnl = 0.0
    tot_cost = 0.0

    ml_wins = 0
    sp_wins = 0
    tot_wins = 0
    n = 0

    for g in triple_games:
        ec = cond_by_event.get(g["game_key"], [])
        ml_c = [c for c in ec if c["market_type"] == "Moneyline"]
        sp_c = [c for c in ec if c["market_type"] == "Spread"]
        tot_c = [c for c in ec if c["market_type"] == "Total"]

        if not (ml_c and sp_c and tot_c):
            continue
        n += 1

        ml_g_pnl = sum(c["pnl"] for c in ml_c)
        sp_g_pnl = sum(c["pnl"] for c in sp_c)
        tot_g_pnl = sum(c["pnl"] for c in tot_c)

        ml_pnl += ml_g_pnl
        ml_cost += sum(c["net_cost"] for c in ml_c)
        sp_pnl += sp_g_pnl
        sp_cost += sum(c["net_cost"] for c in sp_c)
        tot_pnl += tot_g_pnl
        tot_cost += sum(c["net_cost"] for c in tot_c)

        if ml_g_pnl > 0:
            ml_wins += 1
        if sp_g_pnl > 0:
            sp_wins += 1
        if tot_g_pnl > 0:
            tot_wins += 1

    total_cost = ml_cost + sp_cost + tot_cost
    return {
        "n_games": n,
        "total_cost": round(total_cost, 2),
        "total_pnl": round(ml_pnl + sp_pnl + tot_pnl, 2),
        "overall_roi": pct(ml_pnl + sp_pnl + tot_pnl, total_cost),
        "moneyline": {
            "cost": round(ml_cost, 2),
            "pnl": round(ml_pnl, 2),
            "roi_pct": pct(ml_pnl, ml_cost),
            "win_rate": pct(ml_wins, n),
            "cost_share_pct": pct(ml_cost, total_cost),
        },
        "spread": {
            "cost": round(sp_cost, 2),
            "pnl": round(sp_pnl, 2),
            "roi_pct": pct(sp_pnl, sp_cost),
            "win_rate": pct(sp_wins, n),
            "cost_share_pct": pct(sp_cost, total_cost),
        },
        "total": {
            "cost": round(tot_cost, 2),
            "pnl": round(tot_pnl, 2),
            "roi_pct": pct(tot_pnl, tot_cost),
            "win_rate": pct(tot_wins, n),
            "cost_share_pct": pct(tot_cost, total_cost),
        },
        "spread_is_drag": sp_pnl < 0
        and ml_pnl > 0
        and tot_pnl > 0,
    }


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------
def generate_markdown(
    combo_stats: dict,
    synergy: dict,
    spread: dict,
    directional: dict,
    merges: dict,
    sport_mm: dict,
    three_layer: dict,
) -> str:
    lines = [
        "# Agent 3: Multi-Market Analysis Report",
        "",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "---",
        "",
        "## 1. Market Combination Breakdown",
        "",
        "| Combo | Games | Total Cost | Total P&L | ROI % | Win Rate (settled) | Avg Cost/Game |",
        "|-------|------:|----------:|---------:|------:|---------:|---------:|",
    ]
    for label, s in sorted(combo_stats.items(), key=lambda x: -x[1]["n_games"]):
        lines.append(
            f"| {label} | {s['n_games']:,} | ${s['total_cost']:,.0f} | "
            f"${s['total_pnl']:,.0f} | {s['roi_pct']}% | {s['win_rate_settled']}% | "
            f"${s['avg_cost_per_game']:,.0f} |"
        )

    lines += [
        "",
        "**Key observations:**",
        "",
    ]
    # Sort combos by ROI for commentary
    by_roi = sorted(combo_stats.items(), key=lambda x: x[1]["roi_pct"], reverse=True)
    for label, s in by_roi:
        if s["n_games"] >= 30:
            lines.append(
                f"- **{label}**: {s['roi_pct']}% ROI on ${s['total_cost']:,.0f} "
                f"({s['n_games']} games, {s['win_rate_settled']}% win rate)"
            )

    # Section 2: ML + Total Synergy
    lines += [
        "",
        "---",
        "",
        "## 2. Moneyline + Total Synergy Deep-Dive",
        "",
        f"Analyzed {synergy['n_games_analyzed']} ML+Total games.",
        "",
        "### Component Performance",
        "",
        "| Component | Cost | P&L | ROI % |",
        "|-----------|-----:|----:|------:|",
        f"| Moneyline | ${synergy['ml_component_cost']:,.0f} | "
        f"${synergy['ml_component_pnl']:,.0f} | {synergy['ml_component_roi']}% |",
        f"| Total | ${synergy['total_component_cost']:,.0f} | "
        f"${synergy['total_component_pnl']:,.0f} | {synergy['total_component_roi']}% |",
        "",
        "### Outcome Correlation Matrix",
        "",
        "| | Total Win | Total Lose |",
        "|---|---:|---:|",
        f"| **ML Win** | {synergy['outcome_matrix']['both_win']} | "
        f"{synergy['outcome_matrix']['ml_win_total_lose']} |",
        f"| **ML Lose** | {synergy['outcome_matrix']['ml_lose_total_win']} | "
        f"{synergy['outcome_matrix']['both_lose']} |",
        "",
        f"- **Phi correlation**: {synergy['phi_correlation']}",
        f"- **Synergy type**: {synergy['synergy_type']}",
        "",
    ]

    if synergy["synergy_type"] == "independent_edge":
        lines.append(
            "ML and Total outcomes are largely independent, meaning Total adds "
            "diversification value without hedging ML. Both generate positive alpha "
            "independently."
        )
    elif synergy["synergy_type"] == "hedge":
        lines.append(
            "Total acts as a hedge against ML, tending to win when ML loses. "
            "This reduces volatility but may dilute overall returns."
        )
    else:
        lines.append(
            "ML and Total outcomes are positively correlated - they tend to win "
            "and lose together, which increases variance."
        )

    # Section 3: Spread Toxicity
    lines += [
        "",
        "---",
        "",
        "## 3. Spread Toxicity Analysis",
        "",
        f"Total Spread conditions: {spread['spread_conditions_total']}",
        "",
        "### Overall Spread Performance",
        "",
        f"- **Spread ROI**: {spread['spread_roi_pct']}% "
        f"(cost: ${spread['spread_cost']:,.0f}, P&L: ${spread['spread_pnl']:,.0f})",
        f"- **Win rate**: {spread['spread_win_rate']}%",
        f"- **Merge rate**: {spread['spread_merge_rate']}%",
        "",
        "### Spread Drag Effect",
        "",
        f"- ML-only game ROI: **{spread['ml_only_game_roi']}%**",
        f"- ML+Spread game ROI: **{spread['ml_spread_game_roi']}%**",
        f"- **Spread drag**: {spread['spread_drag']} percentage points",
        "",
        "### Component Decomposition (ML+Spread games)",
        "",
        "| Component | Cost | P&L | ROI % |",
        "|-----------|-----:|----:|------:|",
        f"| Moneyline | ${spread['ml_component_in_ml_spread']['cost']:,.0f} | "
        f"${spread['ml_component_in_ml_spread']['pnl']:,.0f} | "
        f"{spread['ml_component_in_ml_spread']['roi_pct']}% |",
        f"| Spread | ${spread['spread_component_in_ml_spread']['cost']:,.0f} | "
        f"${spread['spread_component_in_ml_spread']['pnl']:,.0f} | "
        f"{spread['spread_component_in_ml_spread']['roi_pct']}% |",
        "",
        "### Spread Price Analysis",
        "",
        f"- Avg buy price (losing spreads): {spread['avg_buy_price_losses']}",
        f"- Avg buy price (winning spreads): {spread['avg_buy_price_wins']}",
        "",
        "### Spread Performance by Sport",
        "",
        "| Sport | N | Cost | P&L | ROI % | Win Rate |",
        "|-------|--:|-----:|----:|------:|---------:|",
    ]
    for sport, ss in sorted(
        spread["sport_breakdown"].items(), key=lambda x: -x[1]["n"]
    ):
        lines.append(
            f"| {sport} | {ss['n']} | ${ss['cost']:,.0f} | "
            f"${ss['pnl']:,.0f} | {ss['roi_pct']}% | {ss['win_rate']}% |"
        )

    # Section 4: Directional Analysis
    lines += [
        "",
        "---",
        "",
        "## 4. Directional Analysis (ML+Total Games)",
        "",
        "ML direction classified by avg_buy_price: >= 0.50 = Favorite, < 0.50 = Underdog.",
        "",
        "| Combination | Games | Cost | P&L | ROI % | Win Rate |",
        "|-------------|------:|-----:|----:|------:|---------:|",
    ]
    for combo_key, d in sorted(directional.items()):
        lines.append(
            f"| {combo_key} | {d['n_games']} | ${d['total_cost']:,.0f} | "
            f"${d['total_pnl']:,.0f} | {d['roi_pct']}% | {d['win_rate']}% |"
        )

    # Section 5: MERGE Analysis
    lines += [
        "",
        "---",
        "",
        "## 5. MERGE Analysis by Market Type",
        "",
        "| Market Type | Merge Txns | Merge USDC | Conditions Merged | Total Conditions | Merge Rate |",
        "|-------------|----------:|-----------:|------------------:|----------------:|-----------:|",
    ]
    for mt in ["Moneyline", "Spread", "Total"]:
        mc = merges["merge_counts"][mt]
        lines.append(
            f"| {mt} | {mc['merge_txns']} | ${mc['merge_usdc']:,.0f} | "
            f"{mc['conditions_merged']} | {mc['total_conditions']} | {mc['merge_rate_pct']}% |"
        )

    lines += [
        "",
        "### Merge Timing (hours from first trade to merge)",
        "",
        "| Market Type | Avg Hours | Median Hours | Samples |",
        "|-------------|----------:|-------------:|--------:|",
    ]
    for mt in ["Moneyline", "Spread", "Total"]:
        mt_t = merges["merge_timing"][mt]
        lines.append(
            f"| {mt} | {mt_t['avg_hours']} | {mt_t['median_hours']} | {mt_t['n_samples']} |"
        )

    # Section 6: Sport-specific
    lines += [
        "",
        "---",
        "",
        "## 6. Sport-Specific Multi-Market Patterns",
        "",
    ]
    for sport in sorted(sport_mm.keys()):
        sd = sport_mm[sport]
        lines += [
            f"### {sport} ({sd['total_games']} games)",
            f"Best combo: **{sd['best_combo']}** ({sd['best_combo_roi']}% ROI)",
            "",
            "| Combo | Games | Cost | P&L | ROI % | Win Rate |",
            "|-------|------:|-----:|----:|------:|---------:|",
        ]
        for label, cs in sorted(
            sd["combos"].items(), key=lambda x: -x[1]["roi_pct"]
        ):
            lines.append(
                f"| {label} | {cs['n_games']} | ${cs['total_cost']:,.0f} | "
                f"${cs['total_pnl']:,.0f} | {cs['roi_pct']}% | {cs['win_rate']}% |"
            )
        lines.append("")

    # Section 7: 3-Layer Decomposition
    tl = three_layer
    lines += [
        "---",
        "",
        "## 7. Three-Layer Stacking Decomposition (ML+Spread+Total)",
        "",
        f"Analyzed {tl['n_games']} games with all 3 market types.",
        "",
        f"- **Overall ROI**: {tl['overall_roi']}%",
        f"- **Spread is drag?** {'Yes' if tl['spread_is_drag'] else 'No'}",
        "",
        "| Component | Cost | Cost Share | P&L | ROI % | Win Rate |",
        "|-----------|-----:|-----------:|----:|------:|---------:|",
        f"| Moneyline | ${tl['moneyline']['cost']:,.0f} | "
        f"{tl['moneyline']['cost_share_pct']}% | "
        f"${tl['moneyline']['pnl']:,.0f} | {tl['moneyline']['roi_pct']}% | "
        f"{tl['moneyline']['win_rate']}% |",
        f"| Spread | ${tl['spread']['cost']:,.0f} | "
        f"{tl['spread']['cost_share_pct']}% | "
        f"${tl['spread']['pnl']:,.0f} | {tl['spread']['roi_pct']}% | "
        f"{tl['spread']['win_rate']}% |",
        f"| Total | ${tl['total']['cost']:,.0f} | "
        f"{tl['total']['cost_share_pct']}% | "
        f"${tl['total']['pnl']:,.0f} | {tl['total']['roi_pct']}% | "
        f"{tl['total']['win_rate']}% |",
    ]

    # Conclusions
    lines += [
        "",
        "---",
        "",
        "## Key Findings & Recommendations for nbabot",
        "",
    ]

    findings = derive_findings(
        combo_stats, synergy, spread, directional, merges, sport_mm, three_layer
    )
    for i, f in enumerate(findings["key_findings"], 1):
        lines.append(f"{i}. {f}")
    lines.append("")
    lines.append("### Recommendations for nbabot")
    lines.append("")
    for i, r in enumerate(findings["nbabot_recommendations"], 1):
        lines.append(f"{i}. {r}")

    return "\n".join(lines)


def derive_findings(combo_stats, synergy, spread, directional, merges, sport_mm, three_layer):
    """Derive key findings and recommendations from all analyses."""
    findings = []
    recommendations = []

    # Combo insights
    ml_only = combo_stats.get("ML_only", {})
    ml_total = combo_stats.get("ML+Total", {})
    ml_spread = combo_stats.get("ML+Spread", {})
    ml_sp_tot = combo_stats.get("ML+Spread+Total", {})

    if ml_total.get("roi_pct", 0) > ml_only.get("roi_pct", 0):
        findings.append(
            f"ML+Total ({ml_total['roi_pct']}% ROI) outperforms ML-only "
            f"({ml_only['roi_pct']}% ROI). Adding Total improves returns."
        )
    else:
        findings.append(
            f"ML+Total ({ml_total.get('roi_pct', 'N/A')}% ROI) does not outperform "
            f"ML-only ({ml_only.get('roi_pct', 'N/A')}% ROI)."
        )

    # Spread drag
    if spread["spread_drag"] > 0:
        findings.append(
            f"Spread acts as a drag: adding Spread to ML reduces ROI by "
            f"{spread['spread_drag']} percentage points."
        )
        recommendations.append(
            "Avoid Spread markets unless edge detection is significantly improved. "
            f"Spread ROI is {spread['spread_roi_pct']}% vs ML's positive ROI."
        )

    # Synergy type
    findings.append(
        f"ML and Total have {synergy['synergy_type'].replace('_', ' ')} relationship "
        f"(phi={synergy['phi_correlation']}). "
        f"ML component ROI: {synergy['ml_component_roi']}%, "
        f"Total component ROI: {synergy['total_component_roi']}%."
    )

    if synergy["synergy_type"] == "independent_edge":
        recommendations.append(
            "Consider adding Total market scanning to nbabot. The independent edge "
            "from Totals diversifies without hedging ML profits."
        )

    # Merge analysis
    mc = merges["merge_counts"]
    max_merge_mt = max(mc.items(), key=lambda x: x[1]["merge_rate_pct"])
    findings.append(
        f"{max_merge_mt[0]} has the highest merge rate at "
        f"{max_merge_mt[1]['merge_rate_pct']}%. "
        "MERGE operations indicate position exits before settlement."
    )

    # 3-layer
    if three_layer["spread_is_drag"]:
        findings.append(
            f"In 3-layer stacking, Spread is confirmed drag: "
            f"ML ROI {three_layer['moneyline']['roi_pct']}%, "
            f"Total ROI {three_layer['total']['roi_pct']}%, "
            f"Spread ROI {three_layer['spread']['roi_pct']}%."
        )

    # Sport-specific
    nba_data = sport_mm.get("NBA")
    if nba_data:
        findings.append(
            f"For NBA specifically, best combo is {nba_data['best_combo']} "
            f"with {nba_data['best_combo_roi']}% ROI."
        )
        recommendations.append(
            f"nbabot should prioritize {nba_data['best_combo']} strategy for NBA. "
            f"Cross-reference with lhtsports results to validate edge detection."
        )

    # Directional
    if directional:
        best_dir = max(directional.items(), key=lambda x: x[1]["roi_pct"])
        if best_dir[1]["n_games"] >= 10:
            findings.append(
                f"Best directional combo in ML+Total: {best_dir[0]} with "
                f"{best_dir[1]['roi_pct']}% ROI ({best_dir[1]['n_games']} games)."
            )

    recommendations.append(
        "Focus on ML+Total combination. Spread markets show negative edge and "
        "add complexity without improving returns."
    )
    recommendations.append(
        "If implementing Total market scanning, start with Over/Under on games "
        "where ML edge is already detected for diversification."
    )

    return {
        "key_findings": findings,
        "nbabot_recommendations": recommendations,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("Loading data...")
    conds = load_json(CONDITION_PNL)
    games = load_json(GAME_PNL)
    merges_data = load_json(MERGES)

    print(f"  Conditions: {len(conds)}")
    print(f"  Games: {len(games)}")
    print(f"  Merges: {len(merges_data)}")

    print("\n1. Market combination breakdown...")
    combo_stats = market_combo_breakdown(games)
    for label, s in sorted(combo_stats.items(), key=lambda x: -x[1]["n_games"]):
        print(f"   {label}: {s['n_games']} games, ROI {s['roi_pct']}%")

    print("\n2. ML + Total synergy...")
    synergy = ml_total_synergy(games, conds)
    print(f"   ML ROI: {synergy['ml_component_roi']}%")
    print(f"   Total ROI: {synergy['total_component_roi']}%")
    print(f"   Correlation (phi): {synergy['phi_correlation']}")
    print(f"   Synergy type: {synergy['synergy_type']}")

    print("\n3. Spread toxicity...")
    spread = spread_analysis(games, conds)
    print(f"   Spread ROI: {spread['spread_roi_pct']}%")
    print(f"   ML-only game ROI: {spread['ml_only_game_roi']}%")
    print(f"   ML+Spread game ROI: {spread['ml_spread_game_roi']}%")
    print(f"   Spread drag: {spread['spread_drag']} pp")

    print("\n4. Directional analysis...")
    directional = directional_analysis(games, conds)
    for combo_key, d in sorted(directional.items()):
        print(f"   {combo_key}: {d['n_games']} games, ROI {d['roi_pct']}%")

    print("\n5. MERGE analysis...")
    merge_result = merge_analysis(conds, merges_data)
    for mt, mc in merge_result["merge_counts"].items():
        print(f"   {mt}: {mc['merge_rate_pct']}% merge rate ({mc['conditions_merged']}/{mc['total_conditions']})")

    print("\n6. Sport-specific multi-market patterns...")
    sport_mm = sport_multi_market(games)
    for sport in sorted(sport_mm):
        sd = sport_mm[sport]
        print(f"   {sport}: best={sd['best_combo']} ({sd['best_combo_roi']}% ROI)")

    print("\n7. Three-layer decomposition...")
    three_layer = three_layer_decomp(games, conds)
    print(f"   Overall ROI: {three_layer['overall_roi']}%")
    print(f"   ML: {three_layer['moneyline']['roi_pct']}%")
    print(f"   Spread: {three_layer['spread']['roi_pct']}%")
    print(f"   Total: {three_layer['total']['roi_pct']}%")
    print(f"   Spread is drag: {three_layer['spread_is_drag']}")

    # Build findings
    findings = derive_findings(
        combo_stats, synergy, spread, directional, merge_result, sport_mm, three_layer
    )

    # Write summary JSON
    summary = {
        "agent": "multi-market-analyst",
        "generated": datetime.now().isoformat(),
        "market_combo_stats": combo_stats,
        "ml_total_synergy": synergy,
        "spread_analysis": {
            "spread_only_roi": spread["spread_roi_pct"],
            "spread_drag_on_ml": spread["spread_drag"],
            "spread_merge_rate": spread["spread_merge_rate"],
            "spread_conditions_total": spread["spread_conditions_total"],
            "spread_win_rate": spread["spread_win_rate"],
            "component_in_ml_spread": spread["spread_component_in_ml_spread"],
            "sport_breakdown": spread["sport_breakdown"],
        },
        "directional_analysis": directional,
        "merge_by_market_type": merge_result,
        "sport_multi_market": sport_mm,
        "three_layer_decomposition": three_layer,
        "key_findings": findings["key_findings"],
        "nbabot_recommendations": findings["nbabot_recommendations"],
    }

    json_path = os.path.join(OUT_DIR, "agent3-summary.json")
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nWrote {json_path}")

    # Write markdown report
    md = generate_markdown(
        combo_stats, synergy, spread, directional, merge_result, sport_mm, three_layer
    )
    md_path = os.path.join(OUT_DIR, "agent3-multi-market.md")
    with open(md_path, "w") as f:
        f.write(md)
    print(f"Wrote {md_path}")


if __name__ == "__main__":
    main()
