"""Agent 2: Payoff structure and calibration analysis for lhtsports."""

import json
import math
import os
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "reports", "lhtsports-pnl")
OUTPUT_DIR = os.path.join(DATA_DIR, "deep-analysis")

PRICE_BANDS = [(i * 0.05, (i + 1) * 0.05) for i in range(20)]
MAJOR_SPORTS = ["NBA", "NFL", "MLB", "NHL", "CFB"]


def load_data():
    with open(os.path.join(DATA_DIR, "condition_pnl.json")) as f:
        conditions = json.load(f)
    with open(os.path.join(DATA_DIR, "game_pnl.json")) as f:
        games = json.load(f)
    return conditions, games


def band_label(lo, hi):
    return f"{lo:.2f}-{hi:.2f}"


def get_band(price):
    idx = min(int(price / 0.05), 19)
    return PRICE_BANDS[idx]


def is_settled(c):
    """Only include WIN or LOSS_OR_OPEN (treated as LOSS) for win rate calcs."""
    return c["status"] in ("WIN", "LOSS_OR_OPEN")


def is_win(c):
    return c["status"] == "WIN"


def compute_price_band_analysis(conditions):
    """Compute stats per price band."""
    bands = defaultdict(list)
    for c in conditions:
        lo, hi = get_band(c["avg_buy_price"])
        bands[(lo, hi)].append(c)

    results = []
    for lo, hi in PRICE_BANDS:
        conds = bands[(lo, hi)]
        settled = [c for c in conds if is_settled(c)]
        wins = [c for c in settled if is_win(c)]
        losses = [c for c in settled if not is_win(c)]

        n = len(conds)
        n_settled = len(settled)
        win_rate = len(wins) / n_settled if n_settled > 0 else 0.0

        avg_win_pnl = sum(c["pnl"] for c in wins) / len(wins) if wins else 0.0
        avg_loss_pnl = sum(c["pnl"] for c in losses) / len(losses) if losses else 0.0
        avg_win_roi = sum(c["roi_pct"] for c in wins) / len(wins) if wins else 0.0
        avg_loss_roi = sum(c["roi_pct"] for c in losses) / len(losses) if losses else 0.0

        payoff_ratio = abs(avg_win_pnl / avg_loss_pnl) if avg_loss_pnl != 0 else float("inf")

        # Expected value per dollar bet
        avg_price = (lo + hi) / 2
        if n_settled > 0:
            # EV = win_rate * (1/avg_price - 1) - (1 - win_rate) * 1
            # Simplified: per dollar, if you buy at avg_price, you get 1/avg_price shares
            # Win: payout = shares, Loss: payout = 0
            avg_cost = sum(c["net_cost"] for c in settled) / n_settled if n_settled else 1
            avg_payout = sum(c["total_payout"] for c in settled) / n_settled if n_settled else 0
            ev_per_dollar = (avg_payout - avg_cost) / avg_cost if avg_cost > 0 else 0
        else:
            ev_per_dollar = 0.0

        # Kelly fraction: f* = (bp - q) / b where b = payoff ratio, p = win_rate, q = 1-p
        if payoff_ratio > 0 and payoff_ratio != float("inf") and n_settled > 0:
            b = payoff_ratio
            p = win_rate
            q = 1 - p
            kelly = (b * p - q) / b if b > 0 else 0
            kelly = max(0, kelly)
        else:
            kelly = 0.0

        total_pnl = sum(c["pnl"] for c in conds)
        total_cost = sum(c["net_cost"] for c in conds)
        roi = (total_pnl / total_cost * 100) if total_cost > 0 else 0.0

        results.append({
            "band": band_label(lo, hi),
            "n_conditions": n,
            "n_settled": n_settled,
            "n_wins": len(wins),
            "n_losses": len(losses),
            "win_rate": round(win_rate, 4),
            "avg_win_pnl": round(avg_win_pnl, 2),
            "avg_loss_pnl": round(avg_loss_pnl, 2),
            "avg_win_roi_pct": round(avg_win_roi, 2),
            "avg_loss_roi_pct": round(avg_loss_roi, 2),
            "payoff_ratio": round(payoff_ratio, 3) if payoff_ratio != float("inf") else None,
            "expected_value_per_dollar": round(ev_per_dollar, 4),
            "kelly_fraction": round(kelly, 4),
            "total_pnl": round(total_pnl, 2),
            "total_cost": round(total_cost, 2),
            "roi_pct": round(roi, 2),
        })

    return results


def compute_calibration(conditions, label="all"):
    """Compute calibration: implied prob vs actual prob per price band."""
    bands = defaultdict(list)
    for c in conditions:
        if not is_settled(c):
            continue
        lo, hi = get_band(c["avg_buy_price"])
        bands[(lo, hi)].append(c)

    rows = []
    for lo, hi in PRICE_BANDS:
        conds = bands.get((lo, hi), [])
        if not conds:
            rows.append({
                "band": band_label(lo, hi),
                "n": 0,
                "implied_prob": round((lo + hi) / 2, 4),
                "actual_prob": None,
                "edge": None,
            })
            continue
        implied = sum(c["avg_buy_price"] for c in conds) / len(conds)
        actual = sum(1 for c in conds if is_win(c)) / len(conds)
        rows.append({
            "band": band_label(lo, hi),
            "n": len(conds),
            "implied_prob": round(implied, 4),
            "actual_prob": round(actual, 4),
            "edge": round(actual - implied, 4),
        })
    return rows


def compute_sport_calibration(conditions):
    """Compute calibration per sport."""
    by_sport = defaultdict(list)
    for c in conditions:
        by_sport[c["sport"]].append(c)

    result = {}
    for sport in sorted(by_sport.keys()):
        conds = by_sport[sport]
        cal = compute_calibration(conds, sport)
        settled = [c for c in conds if is_settled(c)]
        total_pnl = sum(c["pnl"] for c in conds)
        total_cost = sum(c["net_cost"] for c in conds)
        roi = (total_pnl / total_cost * 100) if total_cost > 0 else 0

        edges = [r["edge"] for r in cal if r["edge"] is not None and r["n"] >= 5]
        avg_edge = sum(edges) / len(edges) if edges else 0

        result[sport] = {
            "n": len(conds),
            "n_settled": len(settled),
            "avg_edge": round(avg_edge, 4),
            "total_pnl": round(total_pnl, 2),
            "total_cost": round(total_cost, 2),
            "roi": round(roi, 2),
            "calibration": cal,
        }
    return result


def compute_fav_vs_dog(conditions):
    """Favorite (>=0.50) vs Underdog (<0.50) analysis."""
    groups = {"favorite": [], "underdog": []}
    for c in conditions:
        if c["avg_buy_price"] >= 0.50:
            groups["favorite"].append(c)
        else:
            groups["underdog"].append(c)

    result = {}
    for label, conds in groups.items():
        settled = [c for c in conds if is_settled(c)]
        wins = [c for c in settled if is_win(c)]
        win_rate = len(wins) / len(settled) if settled else 0
        total_pnl = sum(c["pnl"] for c in conds)
        total_cost = sum(c["net_cost"] for c in conds)
        roi = (total_pnl / total_cost * 100) if total_cost > 0 else 0

        result[label] = {
            "n": len(conds),
            "n_settled": len(settled),
            "win_rate": round(win_rate, 4),
            "roi": round(roi, 2),
            "total_pnl": round(total_pnl, 2),
            "total_cost": round(total_cost, 2),
        }

    # By sport breakdown
    by_sport = defaultdict(lambda: {"favorite": [], "underdog": []})
    for c in conditions:
        grp = "favorite" if c["avg_buy_price"] >= 0.50 else "underdog"
        by_sport[c["sport"]][grp].append(c)

    sport_breakdown = {}
    for sport in sorted(by_sport.keys()):
        sport_breakdown[sport] = {}
        for label in ["favorite", "underdog"]:
            conds = by_sport[sport][label]
            settled = [c for c in conds if is_settled(c)]
            wins = [c for c in settled if is_win(c)]
            win_rate = len(wins) / len(settled) if settled else 0
            total_pnl = sum(c["pnl"] for c in conds)
            total_cost = sum(c["net_cost"] for c in conds)
            roi = (total_pnl / total_cost * 100) if total_cost > 0 else 0
            sport_breakdown[sport][label] = {
                "n": len(conds),
                "win_rate": round(win_rate, 4),
                "roi": round(roi, 2),
                "total_pnl": round(total_pnl, 2),
            }

    return result, sport_breakdown


def compute_sport_price_cross(conditions):
    """2D table: ROI by sport x price band."""
    grid = defaultdict(lambda: defaultdict(list))
    for c in conditions:
        lo, hi = get_band(c["avg_buy_price"])
        grid[c["sport"]][(lo, hi)].append(c)

    result = {}
    for sport in sorted(grid.keys()):
        result[sport] = {}
        for lo, hi in PRICE_BANDS:
            conds = grid[sport].get((lo, hi), [])
            if not conds:
                result[sport][band_label(lo, hi)] = {"n": 0, "roi": None, "pnl": 0}
                continue
            total_pnl = sum(c["pnl"] for c in conds)
            total_cost = sum(c["net_cost"] for c in conds)
            roi = (total_pnl / total_cost * 100) if total_cost > 0 else 0
            result[sport][band_label(lo, hi)] = {
                "n": len(conds),
                "roi": round(roi, 2),
                "pnl": round(total_pnl, 2),
            }
    return result


def compute_temporal_calibration(conditions):
    """Calibration by month/quarter."""
    from datetime import datetime

    by_quarter = defaultdict(list)
    for c in conditions:
        ts = c.get("first_trade_ts")
        if ts:
            dt = datetime.utcfromtimestamp(ts)
            q = (dt.month - 1) // 3 + 1
            key = f"{dt.year}-Q{q}"
            by_quarter[key].append(c)

    result = {}
    for period in sorted(by_quarter.keys()):
        conds = by_quarter[period]
        settled = [c2 for c2 in conds if is_settled(c2)]
        wins = [c2 for c2 in settled if is_win(c2)]
        total_pnl = sum(c2["pnl"] for c2 in conds)
        total_cost = sum(c2["net_cost"] for c2 in conds)
        roi = (total_pnl / total_cost * 100) if total_cost > 0 else 0
        win_rate = len(wins) / len(settled) if settled else 0

        # Calibration per band for this period
        cal = compute_calibration(conds, period)
        edges = [r["edge"] for r in cal if r["edge"] is not None and r["n"] >= 5]
        avg_edge = sum(edges) / len(edges) if edges else 0

        result[period] = {
            "n": len(conds),
            "n_settled": len(settled),
            "win_rate": round(win_rate, 4),
            "avg_edge": round(avg_edge, 4),
            "total_pnl": round(total_pnl, 2),
            "roi": round(roi, 2),
        }
    return result


def find_sweet_spots(band_analysis):
    """Identify bands with highest EV and sufficient sample size."""
    candidates = [
        b for b in band_analysis
        if b["n_settled"] >= 20 and b["expected_value_per_dollar"] > 0
    ]
    candidates.sort(key=lambda x: x["expected_value_per_dollar"], reverse=True)
    return [c["band"] for c in candidates[:5]]


def generate_key_findings(band_analysis, sport_cal, fav_dog, temporal, cross):
    """Generate key findings list."""
    findings = []

    # Best price band
    best_band = max(
        [b for b in band_analysis if b["n_settled"] >= 20],
        key=lambda x: x["expected_value_per_dollar"],
        default=None,
    )
    if best_band:
        findings.append(
            f"Best EV price band: {best_band['band']} with "
            f"EV/$ = {best_band['expected_value_per_dollar']:.4f}, "
            f"win rate = {best_band['win_rate']:.1%}, "
            f"n = {best_band['n_settled']}"
        )

    # Best sport by ROI
    sports_sorted = sorted(
        [(s, d) for s, d in sport_cal.items() if d["n_settled"] >= 50],
        key=lambda x: x[1]["roi"],
        reverse=True,
    )
    if sports_sorted:
        best_sport, best_data = sports_sorted[0]
        findings.append(
            f"Most profitable sport: {best_sport} with ROI = {best_data['roi']:.1f}%, "
            f"P&L = ${best_data['total_pnl']:,.0f} over {best_data['n']} conditions"
        )

    # Favorite vs underdog
    findings.append(
        f"Underdogs: win rate = {fav_dog['underdog']['win_rate']:.1%}, "
        f"ROI = {fav_dog['underdog']['roi']:.1f}%, "
        f"P&L = ${fav_dog['underdog']['total_pnl']:,.0f}"
    )
    findings.append(
        f"Favorites: win rate = {fav_dog['favorite']['win_rate']:.1%}, "
        f"ROI = {fav_dog['favorite']['roi']:.1f}%, "
        f"P&L = ${fav_dog['favorite']['total_pnl']:,.0f}"
    )

    # Overall calibration edge
    all_bands_with_edge = [
        b for b in band_analysis if b["n_settled"] >= 20
    ]
    if all_bands_with_edge:
        positive_ev = sum(1 for b in all_bands_with_edge if b["expected_value_per_dollar"] > 0)
        findings.append(
            f"{positive_ev}/{len(all_bands_with_edge)} price bands with sufficient data show positive EV"
        )

    # Temporal trend
    periods_sorted = sorted(temporal.items())
    if len(periods_sorted) >= 2:
        first_half = periods_sorted[:len(periods_sorted)//2]
        second_half = periods_sorted[len(periods_sorted)//2:]
        roi_early = sum(p[1]["total_pnl"] for p in first_half) / max(1, sum(abs(p[1]["total_pnl"]) for p in first_half)) * 100
        roi_late = sum(p[1]["total_pnl"] for p in second_half) / max(1, sum(abs(p[1]["total_pnl"]) for p in second_half)) * 100
        edge_early = sum(p[1]["avg_edge"] for p in first_half) / len(first_half) if first_half else 0
        edge_late = sum(p[1]["avg_edge"] for p in second_half) / len(second_half) if second_half else 0
        findings.append(
            f"Temporal trend: avg edge early periods = {edge_early:.4f}, "
            f"later periods = {edge_late:.4f}"
        )

    return findings


def generate_recommendations(band_analysis, sport_cal, fav_dog, sweet_spots):
    """Generate nbabot-specific recommendations."""
    recs = []

    # NBA-specific findings
    nba = sport_cal.get("NBA", {})
    if nba:
        recs.append(
            f"NBA shows ROI of {nba['roi']:.1f}% with avg edge {nba['avg_edge']:.4f}. "
            f"This is the primary sport for nbabot -- sufficient edge exists."
        )

    # Best NBA price bands
    nba_cal = nba.get("calibration", [])
    nba_best = [
        r for r in nba_cal
        if r["edge"] is not None and r["n"] >= 10 and r["edge"] > 0
    ]
    nba_best.sort(key=lambda x: x["edge"], reverse=True)
    if nba_best:
        top_bands = nba_best[:3]
        bands_str = ", ".join(f"{b['band']} (edge={b['edge']:.3f}, n={b['n']})" for b in top_bands)
        recs.append(f"Focus NBA bets on these price bands: {bands_str}")

    # Underdog vs favorite for NBA
    recs.append(
        f"Overall underdog strategy yields ROI={fav_dog['underdog']['roi']:.1f}% vs "
        f"favorites ROI={fav_dog['favorite']['roi']:.1f}%. "
        "Prioritize underdog bets where calibration edge is strongest."
    )

    # Sweet spots
    if sweet_spots:
        recs.append(
            f"Sweet spot price bands (highest EV): {', '.join(sweet_spots)}. "
            "Concentrate capital in these ranges."
        )

    # Kelly sizing
    good_kelly = [
        b for b in band_analysis
        if b["kelly_fraction"] > 0.05 and b["n_settled"] >= 50
    ]
    if good_kelly:
        avg_kelly = sum(b["kelly_fraction"] for b in good_kelly) / len(good_kelly)
        recs.append(
            f"Average Kelly fraction across profitable bands with n>=50: {avg_kelly:.3f}. "
            f"Use quarter-Kelly ({avg_kelly/4:.4f}) for conservative sizing."
        )

    return recs


def write_markdown_report(
    band_analysis, calibration, sport_cal, fav_dog, fav_dog_sport,
    cross, temporal, sweet_spots, findings, recs, total_conditions, total_pnl,
):
    """Write detailed markdown report."""
    lines = []
    lines.append("# Agent 2: Payoff Structure & Calibration Analysis")
    lines.append("")
    lines.append(f"**Total conditions analyzed**: {total_conditions:,}")
    lines.append(f"**Total P&L**: ${total_pnl:,.2f}")
    lines.append("")

    # Key findings
    lines.append("## Key Findings")
    lines.append("")
    for i, f in enumerate(findings, 1):
        lines.append(f"{i}. {f}")
    lines.append("")

    # Price band analysis
    lines.append("## 1. Price Band Analysis")
    lines.append("")
    lines.append("| Band | N | Settled | Wins | Losses | Win Rate | Avg Win P&L | Avg Loss P&L | Payoff Ratio | EV/$ | Kelly f* | Total P&L | ROI% |")
    lines.append("|------|---|---------|------|--------|----------|-------------|--------------|-------------|------|---------|-----------|------|")
    for b in band_analysis:
        pr = f"{b['payoff_ratio']:.2f}" if b["payoff_ratio"] is not None else "N/A"
        lines.append(
            f"| {b['band']} | {b['n_conditions']} | {b['n_settled']} | {b['n_wins']} | {b['n_losses']} | "
            f"{b['win_rate']:.1%} | ${b['avg_win_pnl']:,.0f} | ${b['avg_loss_pnl']:,.0f} | "
            f"{pr} | {b['expected_value_per_dollar']:.4f} | {b['kelly_fraction']:.4f} | "
            f"${b['total_pnl']:,.0f} | {b['roi_pct']:.1f}% |"
        )
    lines.append("")

    # Calibration curve
    lines.append("## 2. Calibration Curve (All Sports)")
    lines.append("")
    lines.append("| Band | N | Implied Prob | Actual Prob | Edge (Actual - Implied) |")
    lines.append("|------|---|-------------|-------------|------------------------|")
    for r in calibration:
        if r["n"] == 0:
            lines.append(f"| {r['band']} | 0 | {r['implied_prob']:.4f} | - | - |")
        else:
            lines.append(
                f"| {r['band']} | {r['n']} | {r['implied_prob']:.4f} | "
                f"{r['actual_prob']:.4f} | {r['edge']:+.4f} |"
            )
    lines.append("")

    # Sport calibration
    lines.append("## 3. Sport-Specific Calibration")
    lines.append("")
    lines.append("### Overview")
    lines.append("")
    lines.append("| Sport | N | Settled | Avg Edge | Total P&L | ROI% |")
    lines.append("|-------|---|---------|----------|-----------|------|")
    for sport in sorted(sport_cal.keys(), key=lambda s: sport_cal[s]["total_pnl"], reverse=True):
        d = sport_cal[sport]
        lines.append(
            f"| {sport} | {d['n']} | {d['n_settled']} | {d['avg_edge']:+.4f} | "
            f"${d['total_pnl']:,.0f} | {d['roi']:.1f}% |"
        )
    lines.append("")

    # Per-sport calibration detail for major sports
    for sport in MAJOR_SPORTS:
        if sport not in sport_cal:
            continue
        d = sport_cal[sport]
        lines.append(f"### {sport} Calibration")
        lines.append("")
        lines.append("| Band | N | Implied | Actual | Edge |")
        lines.append("|------|---|---------|--------|------|")
        for r in d["calibration"]:
            if r["n"] == 0:
                continue
            edge_str = f"{r['edge']:+.4f}" if r["edge"] is not None else "-"
            actual_str = f"{r['actual_prob']:.4f}" if r["actual_prob"] is not None else "-"
            lines.append(
                f"| {r['band']} | {r['n']} | {r['implied_prob']:.4f} | {actual_str} | {edge_str} |"
            )
        lines.append("")

    # Favorite vs underdog
    lines.append("## 4. Favorite vs Underdog Analysis")
    lines.append("")
    lines.append("### Overall")
    lines.append("")
    lines.append("| Group | N | Settled | Win Rate | ROI% | Total P&L | Total Cost |")
    lines.append("|-------|---|---------|----------|------|-----------|------------|")
    for label in ["favorite", "underdog"]:
        d = fav_dog[label]
        lines.append(
            f"| {label.title()} | {d['n']} | {d['n_settled']} | {d['win_rate']:.1%} | "
            f"{d['roi']:.1f}% | ${d['total_pnl']:,.0f} | ${d['total_cost']:,.0f} |"
        )
    lines.append("")

    lines.append("### By Sport")
    lines.append("")
    lines.append("| Sport | Fav N | Fav WR | Fav ROI | Fav P&L | Dog N | Dog WR | Dog ROI | Dog P&L |")
    lines.append("|-------|-------|--------|---------|---------|-------|--------|---------|---------|")
    for sport in sorted(fav_dog_sport.keys(), key=lambda s: sum(
        fav_dog_sport[s][g].get("total_pnl", 0) for g in ["favorite", "underdog"]
    ), reverse=True):
        f_d = fav_dog_sport[sport]["favorite"]
        u_d = fav_dog_sport[sport]["underdog"]
        lines.append(
            f"| {sport} | {f_d['n']} | {f_d['win_rate']:.1%} | {f_d['roi']:.1f}% | "
            f"${f_d['total_pnl']:,.0f} | {u_d['n']} | {u_d['win_rate']:.1%} | "
            f"{u_d['roi']:.1f}% | ${u_d['total_pnl']:,.0f} |"
        )
    lines.append("")

    # Sport x Price Band cross table
    lines.append("## 5. Sport x Price Band ROI Cross-Analysis")
    lines.append("")
    major_sports_present = [s for s in MAJOR_SPORTS if s in cross]
    header = "| Band | " + " | ".join(f"{s} ROI% (n)" for s in major_sports_present) + " |"
    sep = "|------|" + "|".join(["-------" for _ in major_sports_present]) + "|"
    lines.append(header)
    lines.append(sep)
    for lo, hi in PRICE_BANDS:
        bl = band_label(lo, hi)
        cells = []
        for s in major_sports_present:
            d = cross[s].get(bl, {"n": 0, "roi": None})
            if d["n"] == 0 or d["roi"] is None:
                cells.append("-")
            else:
                cells.append(f"{d['roi']:.0f}% ({d['n']})")
        lines.append(f"| {bl} | " + " | ".join(cells) + " |")
    lines.append("")

    # Temporal calibration
    lines.append("## 6. Temporal Calibration")
    lines.append("")
    lines.append("| Period | N | Settled | Win Rate | Avg Edge | Total P&L | ROI% |")
    lines.append("|--------|---|---------|----------|----------|-----------|------|")
    for period in sorted(temporal.keys()):
        d = temporal[period]
        lines.append(
            f"| {period} | {d['n']} | {d['n_settled']} | {d['win_rate']:.1%} | "
            f"{d['avg_edge']:+.4f} | ${d['total_pnl']:,.0f} | {d['roi']:.1f}% |"
        )
    lines.append("")

    # Optimal allocation
    lines.append("## 7. Optimal Capital Allocation (Kelly-based)")
    lines.append("")
    kelly_bands = [b for b in band_analysis if b["kelly_fraction"] > 0 and b["n_settled"] >= 20]
    total_kelly = sum(b["kelly_fraction"] for b in kelly_bands) or 1
    lines.append("| Band | Kelly f* | Allocation Weight | Suggested % (Quarter-Kelly) |")
    lines.append("|------|---------|-------------------|----------------------------|")
    for b in kelly_bands:
        weight = b["kelly_fraction"] / total_kelly
        qk = weight * 0.25 * 100
        lines.append(
            f"| {b['band']} | {b['kelly_fraction']:.4f} | {weight:.3f} | {qk:.1f}% |"
        )
    lines.append("")

    # Recommendations
    lines.append("## 8. Recommendations for nbabot")
    lines.append("")
    for i, r in enumerate(recs, 1):
        lines.append(f"{i}. {r}")
    lines.append("")

    return "\n".join(lines)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Loading data...")
    conditions, games = load_data()
    total_conditions = len(conditions)
    total_pnl = sum(c["pnl"] for c in conditions)
    print(f"  {total_conditions} conditions, total P&L = ${total_pnl:,.2f}")

    print("Computing price band analysis...")
    band_analysis = compute_price_band_analysis(conditions)

    print("Computing calibration curve...")
    calibration = compute_calibration(conditions)

    print("Computing sport-specific calibration...")
    sport_cal = compute_sport_calibration(conditions)

    print("Computing favorite vs underdog analysis...")
    fav_dog, fav_dog_sport = compute_fav_vs_dog(conditions)

    print("Computing sport x price band cross-analysis...")
    cross = compute_sport_price_cross(conditions)

    print("Computing temporal calibration...")
    temporal = compute_temporal_calibration(conditions)

    print("Identifying sweet spots...")
    sweet_spots = find_sweet_spots(band_analysis)
    print(f"  Sweet spots: {sweet_spots}")

    print("Generating findings and recommendations...")
    findings = generate_key_findings(band_analysis, sport_cal, fav_dog, temporal, cross)
    recs = generate_recommendations(band_analysis, sport_cal, fav_dog, sweet_spots)

    # Write JSON summary
    summary = {
        "agent": "payoff-structure-analyst",
        "total_conditions": total_conditions,
        "total_pnl": round(total_pnl, 2),
        "price_band_analysis": band_analysis,
        "calibration_by_sport": {
            sport: {
                "n": d["n"],
                "n_settled": d["n_settled"],
                "avg_edge": d["avg_edge"],
                "total_pnl": d["total_pnl"],
                "roi": d["roi"],
            }
            for sport, d in sport_cal.items()
        },
        "favorite_vs_underdog": fav_dog,
        "sweet_spot_bands": sweet_spots,
        "temporal_calibration": temporal,
        "key_findings": findings,
        "nbabot_recommendations": recs,
    }

    json_path = os.path.join(OUTPUT_DIR, "agent2-summary.json")
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Wrote {json_path}")

    # Write markdown report
    md = write_markdown_report(
        band_analysis, calibration, sport_cal, fav_dog, fav_dog_sport,
        cross, temporal, sweet_spots, findings, recs, total_conditions, total_pnl,
    )
    md_path = os.path.join(OUTPUT_DIR, "agent2-payoff-structure.md")
    with open(md_path, "w") as f:
        f.write(md)
    print(f"Wrote {md_path}")

    # Print summary
    print("\n=== KEY FINDINGS ===")
    for f_item in findings:
        print(f"  - {f_item}")
    print("\n=== RECOMMENDATIONS ===")
    for r in recs:
        print(f"  - {r}")


if __name__ == "__main__":
    main()
