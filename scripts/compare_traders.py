"""Compare strategy profiles across multiple traders.

Reads strategy_profile.json from each trader directory and generates
a comparison report with risk-adjusted rankings.

Usage:
  python scripts/compare_traders.py                              # All analyzed traders
  python scripts/compare_traders.py --sort-by sharpe             # Sort by Sharpe
  python scripts/compare_traders.py --sort-by consistency         # Sort by consistency
  python scripts/compare_traders.py --min-months 6               # 6+ months only
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TRADERS_DIR = PROJECT_ROOT / "data" / "traders"
COMPARISON_DIR = TRADERS_DIR / "_comparison"


def load_profiles(min_months: int = 0) -> list[dict]:
    """Load all strategy_profile.json files."""
    profiles = []
    if not TRADERS_DIR.exists():
        return profiles

    for trader_dir in sorted(TRADERS_DIR.iterdir()):
        if not trader_dir.is_dir() or trader_dir.name.startswith("_"):
            continue
        profile_path = trader_dir / "strategy_profile.json"
        if not profile_path.exists():
            continue
        with open(profile_path) as f:
            p = json.load(f)
        if min_months and p.get("active_months", 0) < min_months:
            continue
        profiles.append(p)

    return profiles


def sort_profiles(profiles: list[dict], sort_by: str) -> list[dict]:
    """Sort profiles by given metric."""
    key_map = {
        "pnl": lambda p: p.get("leaderboard_pnl", 0) or p.get("total_pnl", 0),
        "roi": lambda p: p.get("roi_pct", 0),
        "sharpe": lambda p: p.get("daily_sharpe", 0),
        "consistency": lambda p: p.get("consistency_score", 0),
        "drawdown": lambda p: -p.get("max_drawdown_pct", 100),  # lower is better
        "profit_factor": lambda p: p.get("profit_factor", 0),
        "volume": lambda p: p.get("total_volume", 0),
        "win_rate": lambda p: p.get("win_rate", 0),
    }
    key_fn = key_map.get(sort_by, key_map["sharpe"])
    return sorted(profiles, key=key_fn, reverse=True)


def generate_comparison_report(profiles: list[dict], sort_by: str) -> str:
    """Generate markdown comparison report."""
    out: list[str] = []

    out.append("# Polymarket Trader Comparison Report")
    out.append("")
    out.append(f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    out.append(f"**Traders**: {len(profiles)}")
    out.append(f"**Sorted by**: {sort_by}")
    out.append("")

    # --- 1. Risk-Adjusted Leaderboard ---
    out.append("---")
    out.append("## 1. Risk-Adjusted Leaderboard")
    out.append("")
    out.append(
        "| # | Trader | PnL | LB PnL | ROI | Sharpe(d) | Sharpe(w)"
        " | MaxDD% | Consistency | Profit Factor | Months | Quality |"
    )
    out.append(
        "|---|--------|-----|--------|-----|-----------|-----------|"
        "--------|-------------|---------------|--------|---------|"
    )

    for i, p in enumerate(profiles, 1):
        name = p.get("username", "?")[:15]
        pnl = p.get("total_pnl", 0)
        lb_pnl = p.get("leaderboard_pnl", 0)
        roi = p.get("roi_pct", 0)
        ds = p.get("daily_sharpe", 0)
        ws = p.get("weekly_sharpe", 0)
        dd = p.get("max_drawdown_pct", 0)
        cons = p.get("consistency_score", 0)
        pf = p.get("profit_factor", 0)
        months = p.get("active_months", 0)
        quality = p.get("data_quality", "unknown")

        pf_str = f"{pf:.2f}" if pf < 100 else "inf"
        lb_str = f"${lb_pnl:,.0f}" if lb_pnl else "-"
        q_icon = "✓" if quality == "complete" else "⚠" if quality == "incomplete" else "?"
        out.append(
            f"| {i} | {name} | ${pnl:,.0f} | {lb_str} | {roi:.1f}% | {ds:.2f} | {ws:.2f} | "
            f"{dd:.1f}% | {cons:.0%} | {pf_str} | {months} | {q_icon} |"
        )
    out.append("")

    # --- 2. Category Specialization Matrix ---
    out.append("---")
    out.append("## 2. Category Specialization")
    out.append("")

    # カテゴリを収集
    all_cats: set[str] = set()
    for p in profiles:
        all_cats.update(p.get("category_pnl", {}).keys())
    cats = sorted(all_cats)

    if cats:
        header = "| Trader | " + " | ".join(cats) + " | Primary |"
        sep = "|--------|" + "|".join(["-------"] * len(cats)) + "|---------|"
        out.append(header)
        out.append(sep)

        for p in profiles:
            name = p.get("username", "?")[:15]
            cat_pnl = p.get("category_pnl", {})
            cells = []
            for cat in cats:
                val = cat_pnl.get(cat, 0)
                cells.append(f"${val:,.0f}" if val != 0 else "-")
            primary = p.get("primary_category", "?")
            out.append(f"| {name} | " + " | ".join(cells) + f" | {primary} |")
        out.append("")

    # --- 3. Sport Breakdown (for Sports traders) ---
    out.append("---")
    out.append("## 3. Sport Breakdown (Sports category traders)")
    out.append("")

    all_sports: set[str] = set()
    for p in profiles:
        all_sports.update(p.get("sport_pnl", {}).keys())
    sports = sorted(all_sports - {"Other"})

    if sports:
        header = "| Trader | " + " | ".join(sports) + " |"
        sep = "|--------|" + "|".join(["-------"] * len(sports)) + "|"
        out.append(header)
        out.append(sep)

        for p in profiles:
            sp = p.get("sport_pnl", {})
            if not sp:
                continue
            name = p.get("username", "?")[:15]
            cells = []
            for sport in sports:
                val = sp.get(sport, 0)
                cells.append(f"${val:,.0f}" if val != 0 else "-")
            out.append(f"| {name} | " + " | ".join(cells) + " |")
        out.append("")

    # --- 4. Risk Profile Comparison ---
    out.append("---")
    out.append("## 4. Risk Profile Comparison")
    out.append("")
    out.append(
        "| Trader | Daily Sharpe | Weekly Sharpe | MaxDD%"
        " | DD Days | Daily W% | Weekly W% | Profit Factor |"
    )
    out.append(
        "|--------|-------------|---------------|--------|"
        "---------|----------|-----------|---------------|"
    )

    for p in profiles:
        name = p.get("username", "?")[:15]
        ds = p.get("daily_sharpe", 0)
        ws = p.get("weekly_sharpe", 0)
        dd = p.get("max_drawdown_pct", 0)
        dd_days = p.get("max_drawdown_days", 0)
        dw = p.get("daily_win_rate", 0)
        ww = p.get("weekly_win_rate", 0)
        pf = p.get("profit_factor", 0)
        pf_str = f"{pf:.2f}" if pf < 100 else "inf"
        out.append(
            f"| {name} | {ds:.3f} | {ws:.3f} | {dd:.1f}% | {dd_days} | "
            f"{dw:.1%} | {ww:.1%} | {pf_str} |"
        )
    out.append("")

    # --- 5. Calibration Curve Overlay ---
    out.append("---")
    out.append("## 5. Calibration Curve Overlay (win rate by price band)")
    out.append("")

    # 共通バンドを収集
    all_bands: list[str] = []
    band_set: set[str] = set()
    for p in profiles:
        for bs in p.get("price_band_stats", []):
            b = bs["band"]
            if b not in band_set:
                band_set.add(b)
                all_bands.append(b)
    all_bands.sort()

    if all_bands:
        # 主要バンドのみ表示 (0.05-0.95)
        key_bands = [b for b in all_bands if 0.05 <= float(b.split("-")[0]) <= 0.90]
        if key_bands:
            header = "| Band | " + " | ".join(p.get("username", "?")[:10] for p in profiles) + " |"
            sep = "|------|" + "|".join(["------"] * len(profiles)) + "|"
            out.append(header)
            out.append(sep)

            for band in key_bands:
                cells = []
                for p in profiles:
                    band_data = {bs["band"]: bs for bs in p.get("price_band_stats", [])}
                    if band in band_data:
                        wr = band_data[band].get("win_rate", 0)
                        n = band_data[band].get("count", 0)
                        cells.append(f"{wr:.0%} (n={n})")
                    else:
                        cells.append("-")
                out.append(f"| {band} | " + " | ".join(cells) + " |")
            out.append("")

    # --- 6. Consistency (Monthly PnL Trends) ---
    out.append("---")
    out.append("## 6. Monthly Consistency")
    out.append("")
    out.append(
        "| Trader | Active Months | Positive Months | Consistency"
        " | Avg Month PnL | Best Month | Worst Month |"
    )
    out.append(
        "|--------|--------------|-----------------|-------------|"
        "---------------|------------|-------------|"
    )

    for p in profiles:
        name = p.get("username", "?")[:15]
        monthly = p.get("monthly_pnl", {})
        months = len(monthly)
        if months == 0:
            continue
        values = list(monthly.values())
        pos = sum(1 for v in values if v > 0)
        cons = pos / months if months > 0 else 0
        avg_m = sum(values) / months
        best = max(values)
        worst = min(values)
        out.append(
            f"| {name} | {months} | {pos} | {cons:.0%} | "
            f"${avg_m:,.0f} | ${best:,.0f} | ${worst:,.0f} |"
        )
    out.append("")

    # --- 7. Execution Style ---
    out.append("---")
    out.append("## 7. Execution Style")
    out.append("")
    out.append(
        "| Trader | DCA% | Avg Trades/Cond | Avg Position | Median Position | Sweet Spot % |"
    )
    out.append("|--------|------|-----------------|-------------|-----------------|--------------|")

    for p in profiles:
        name = p.get("username", "?")[:15]
        dca = p.get("dca_fraction", 0)
        atc = p.get("avg_trades_per_condition", 0)
        avg_pos = p.get("avg_position_size", 0)
        med_pos = p.get("median_position_size", 0)
        ss = p.get("sweet_spot_concentration", 0)
        out.append(
            f"| {name} | {dca:.0%} | {atc:.1f} | ${avg_pos:,.0f} | ${med_pos:,.0f} | {ss:.0%} |"
        )
    out.append("")

    return "\n".join(out)


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare Polymarket trader profiles")
    ap.add_argument(
        "--sort-by",
        type=str,
        default="sharpe",
        choices=[
            "pnl",
            "roi",
            "sharpe",
            "consistency",
            "drawdown",
            "profit_factor",
            "volume",
            "win_rate",
        ],
        help="Sort metric (default: sharpe)",
    )
    ap.add_argument("--min-months", type=int, default=0, help="Minimum active months filter")
    args = ap.parse_args()

    profiles = load_profiles(min_months=args.min_months)
    if not profiles:
        print("No analyzed traders found. Run analyze_trader.py first.", file=sys.stderr)
        sys.exit(1)

    profiles = sort_profiles(profiles, args.sort_by)

    print(f"Comparing {len(profiles)} traders (sorted by {args.sort_by})...")

    report = generate_comparison_report(profiles, args.sort_by)

    COMPARISON_DIR.mkdir(parents=True, exist_ok=True)
    report_path = COMPARISON_DIR / "comparison_report.md"
    with open(report_path, "w") as f:
        f.write(report)
    print(f"Report saved to {report_path}")

    # Quick console summary
    print(f"\n{'=' * 90}")
    hdr = (
        f"{'#':>3} {'Trader':<18} {'PnL':>12} {'LB PnL':>12}"
        f" {'Sharpe(d)':>10} {'Consist':>8} {'MaxDD':>7} {'Months':>7} {'Q':>3}"
    )
    print(hdr)
    print("-" * 90)
    for i, p in enumerate(profiles, 1):
        name = p.get("username", "?")[:18]
        pnl = p.get("total_pnl", 0)
        lb_pnl = p.get("leaderboard_pnl", 0)
        ds = p.get("daily_sharpe", 0)
        cons = p.get("consistency_score", 0)
        dd = p.get("max_drawdown_pct", 0)
        months = p.get("active_months", 0)
        quality = p.get("data_quality", "unknown")
        q_icon = "✓" if quality == "complete" else "⚠" if quality == "incomplete" else "?"
        lb_str = f"${lb_pnl:>10,.0f}" if lb_pnl else f"{'-':>11}"
        print(
            f"{i:>3} {name:<18} ${pnl:>10,.0f} {lb_str}"
            f" {ds:>10.3f} {cons:>7.0%} {dd:>6.1f}% {months:>7} {q_icon:>3}"
        )
    print(f"{'=' * 90}")


if __name__ == "__main__":
    main()
