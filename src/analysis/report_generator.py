"""Markdown report generation for trader P&L analysis.

Extracted from src/analysis/pnl.py — generate_report() and its helpers.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from statistics import mean, median


def generate_report(
    conditions: dict[str, dict],
    games: list[dict],
    trader_name: str = "Trader",
) -> str:
    """Generate a full P&L report as Markdown."""
    out: list[str] = []

    total_buy = sum(c["buy_cost"] for c in conditions.values())
    total_sell = sum(c["sell_proceeds"] for c in conditions.values())
    total_redeem = sum(c["redeem_usdc"] for c in conditions.values())
    total_merge = sum(c["merge_usdc"] for c in conditions.values())
    net_cost = total_buy - total_sell
    total_payout = total_redeem + total_merge
    total_pnl = total_payout - net_cost

    wins = [c for c in conditions.values() if c["status"] == "WIN"]
    losses = [c for c in conditions.values() if c["status"] == "LOSS_OR_OPEN"]
    merged = [c for c in conditions.values() if c["status"] == "MERGED"]

    out.append(f"# @{trader_name} P&L Analysis Report (TRADE + REDEEM + MERGE)")
    out.append("")
    out.append(f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    out.append("")

    # データ品質警告
    missing_conds = [c for c in conditions.values() if c.get("data_quality") == "missing_trades"]
    if missing_conds:
        missing_pnl = sum(c["pnl"] for c in missing_conds)
        out.append("> **⚠ DATA QUALITY WARNING**")
        out.append(f"> {len(missing_conds)} conditions have REDEEM/MERGE but no TRADE data.")
        out.append(f"> Phantom PnL from missing trades: **${missing_pnl:,.2f}**")
        out.append("> These conditions inflate the total PnL. Re-fetch TRADE data to fix.")
        out.append("")

    out.append("---")
    out.append("## 1. Overall Summary")
    out.append("")
    out.append("| Item | Amount |")
    out.append("|------|--------|")
    out.append(f"| BUY Total Cost | ${total_buy:,.2f} |")
    out.append(f"| SELL Revenue | ${total_sell:,.2f} |")
    out.append(f"| **Net Cost (BUY - SELL)** | **${net_cost:,.2f}** |")
    out.append(f"| REDEEM Payout | ${total_redeem:,.2f} |")
    out.append(f"| MERGE Payout | ${total_merge:,.2f} |")
    out.append(f"| **Total Payout** | **${total_payout:,.2f}** |")
    out.append(f"| **P&L** | **${total_pnl:,.2f}** |")
    if net_cost > 0:
        out.append(f"| **ROI** | **{total_pnl / net_cost * 100:.2f}%** |")
    out.append("")

    out.append("### Condition Win/Loss")
    out.append("")
    out.append(f"- **WIN (REDEEM)**: {len(wins):,} conditions")
    out.append(f"- **LOSS or OPEN**: {len(losses):,} conditions")
    out.append(f"- **MERGED**: {len(merged):,} conditions")
    out.append(f"- **Total**: {len(conditions):,} conditions")
    out.append("")

    # WIN
    win_cost = sum(c["net_cost"] for c in wins)
    win_payout = sum(c["total_payout"] for c in wins)
    win_pnl = win_payout - win_cost
    if win_cost > 0:
        out.append("### WIN conditions only")
        win_roi = win_pnl / win_cost * 100
        out.append(
            f"- Cost: ${win_cost:,.2f} -> Payout: ${win_payout:,.2f}"
            f" -> P&L: ${win_pnl:,.2f} (ROI: {win_roi:.1f}%)"
        )
        out.append("")

    # LOSS
    loss_cost = sum(c["net_cost"] for c in losses)
    out.append("### LOSS/OPEN conditions")
    out.append(f"- Net cost (lost or unsettled): ${loss_cost:,.2f}")
    out.append("")

    # MERGE
    merge_cost = sum(c["net_cost"] for c in merged)
    merge_payout = sum(c["total_payout"] for c in merged)
    merge_pnl = merge_payout - merge_cost
    out.append("### MERGED conditions")
    out.append(
        f"- Cost: ${merge_cost:,.2f} -> Payout: ${merge_payout:,.2f} -> P&L: ${merge_pnl:,.2f}"
    )
    out.append("")

    # -- 2. Category / Sport P&L --
    out.append("---")
    out.append("## 2. Category / Sport P&L")
    out.append("")

    sport_pnl: dict[str, dict] = defaultdict(
        lambda: {
            "buy": 0.0,
            "sell": 0.0,
            "redeem": 0.0,
            "merge": 0.0,
            "games": 0,
            "wins": 0,
            "losses": 0,
        }
    )
    for g in games:
        label = g["sport"] if g.get("category") == "Sports" else g.get("category", "Other")
        sp = sport_pnl[label]
        sp["buy"] += g["total_buy_cost"]
        sp["sell"] += g["total_sell_proceeds"]
        sp["redeem"] += g["total_redeem"]
        sp["merge"] += g["total_merge"]
        sp["games"] += 1
        if g["total_pnl"] > 0:
            sp["wins"] += 1
        elif g["total_pnl"] < 0:
            sp["losses"] += 1

    out.append("| Category | Games | W | L | Win% | Net Cost | Payout | P&L | ROI |")
    out.append("|----------|-------|---|---|------|----------|--------|-----|-----|")
    for sport in sorted(sport_pnl, key=lambda s: sport_pnl[s]["buy"], reverse=True):
        sp = sport_pnl[sport]
        net = sp["buy"] - sp["sell"]
        payout = sp["redeem"] + sp["merge"]
        pnl = payout - net
        wl = sp["wins"] + sp["losses"]
        wr = sp["wins"] / wl * 100 if wl > 0 else 0
        roi = pnl / net * 100 if net > 0 else 0
        out.append(
            f"| {sport} | {sp['games']:,} | {sp['wins']:,} | {sp['losses']:,} | "
            f"{wr:.1f}% | ${net:,.0f} | ${payout:,.0f} | ${pnl:,.0f} | {roi:.1f}% |"
        )
    out.append("")

    # -- 3. Market Type P&L --
    out.append("---")
    out.append("## 3. Market Type P&L (condition level)")
    out.append("")

    mt_pnl: dict[str, dict] = defaultdict(
        lambda: {
            "cost": 0.0,
            "payout": 0.0,
            "wins": 0,
            "losses": 0,
            "count": 0,
        }
    )
    for c in conditions.values():
        mt = mt_pnl[c["market_type"]]
        mt["cost"] += c["net_cost"]
        mt["payout"] += c["total_payout"]
        mt["count"] += 1
        if c["status"] == "WIN":
            mt["wins"] += 1
        elif c["status"] == "LOSS_OR_OPEN":
            mt["losses"] += 1

    out.append("| Type | Conditions | W | L/Open | Win% | Net Cost | Payout | P&L | ROI |")
    out.append("|------|-----------|---|--------|------|----------|--------|-----|-----|")
    for mt_name in sorted(mt_pnl, key=lambda k: mt_pnl[k]["cost"], reverse=True):
        mt = mt_pnl[mt_name]
        pnl = mt["payout"] - mt["cost"]
        wl = mt["wins"] + mt["losses"]
        wr = mt["wins"] / wl * 100 if wl > 0 else 0
        roi = pnl / mt["cost"] * 100 if mt["cost"] > 0 else 0
        out.append(
            f"| {mt_name} | {mt['count']:,} | {mt['wins']:,} | {mt['losses']:,} | "
            f"{wr:.1f}% | ${mt['cost']:,.0f} | ${mt['payout']:,.0f} | ${pnl:,.0f} | {roi:.1f}% |"
        )
    out.append("")

    # -- 4. Monthly P&L --
    out.append("---")
    out.append("## 4. Monthly P&L Trend")
    out.append("")

    monthly: dict[str, dict] = defaultdict(
        lambda: {
            "buy": 0.0,
            "sell": 0.0,
            "redeem": 0.0,
            "merge": 0.0,
            "games": 0,
            "wins": 0,
            "losses": 0,
        }
    )
    for g in games:
        m = monthly[g["month"]]
        m["buy"] += g["total_buy_cost"]
        m["sell"] += g["total_sell_proceeds"]
        m["redeem"] += g["total_redeem"]
        m["merge"] += g["total_merge"]
        m["games"] += 1
        if g["total_pnl"] > 0:
            m["wins"] += 1
        elif g["total_pnl"] < 0:
            m["losses"] += 1

    out.append("| Month | Games | W | L | Win% | Net Cost | Payout | P&L | ROI | Cumulative |")
    out.append("|-------|-------|---|---|------|----------|--------|-----|-----|------------|")
    cumulative = 0.0
    for month in sorted(monthly):
        m = monthly[month]
        net = m["buy"] - m["sell"]
        payout = m["redeem"] + m["merge"]
        pnl = payout - net
        cumulative += pnl
        wl = m["wins"] + m["losses"]
        wr = m["wins"] / wl * 100 if wl > 0 else 0
        roi = pnl / net * 100 if net > 0 else 0
        out.append(
            f"| {month} | {m['games']:,} | {m['wins']} | {m['losses']} | "
            f"{wr:.1f}% | ${net:,.0f} | ${payout:,.0f} | "
            f"${pnl:,.0f} | {roi:.1f}% | ${cumulative:,.0f} |"
        )
    out.append("")

    # -- 5. Price band P&L --
    out.append("---")
    out.append("## 5. Price Band P&L (avg buy price)")
    out.append("")

    price_buckets = [
        ("0.01-0.20", 0.01, 0.20),
        ("0.20-0.40", 0.20, 0.40),
        ("0.40-0.60", 0.40, 0.60),
        ("0.60-0.80", 0.60, 0.80),
        ("0.80-1.00", 0.80, 1.00),
    ]
    pb_data: dict[str, dict] = {
        name: {"cost": 0.0, "payout": 0.0, "wins": 0, "losses": 0, "count": 0}
        for name, _, _ in price_buckets
    }
    for c in conditions.values():
        avg_p = c["avg_buy_price"]
        if avg_p <= 0:
            continue
        for name, lo, hi in price_buckets:
            if lo <= avg_p < hi or (hi == 1.0 and avg_p == 1.0):
                pb = pb_data[name]
                pb["cost"] += c["net_cost"]
                pb["payout"] += c["total_payout"]
                pb["count"] += 1
                if c["status"] == "WIN":
                    pb["wins"] += 1
                elif c["status"] == "LOSS_OR_OPEN":
                    pb["losses"] += 1
                break

    out.append("| Band | Conditions | W | L/Open | Win% | Net Cost | Payout | P&L | ROI |")
    out.append("|------|-----------|---|--------|------|----------|--------|-----|-----|")
    for name, _, _ in price_buckets:
        pb = pb_data[name]
        pnl = pb["payout"] - pb["cost"]
        wl = pb["wins"] + pb["losses"]
        wr = pb["wins"] / wl * 100 if wl > 0 else 0
        roi = pnl / pb["cost"] * 100 if pb["cost"] > 0 else 0
        out.append(
            f"| {name} | {pb['count']:,} | {pb['wins']:,} | {pb['losses']:,} | "
            f"{wr:.1f}% | ${pb['cost']:,.0f} | ${pb['payout']:,.0f} | ${pnl:,.0f} | {roi:.1f}% |"
        )
    out.append("")

    # -- 6. Top / Bottom games --
    out.append("---")
    out.append("## 6. Game P&L Ranking")
    out.append("")

    settled_games = [g for g in games if g["total_redeem"] > 0 or g["total_merge"] > 0]
    sorted_by_pnl = sorted(settled_games, key=lambda x: x["total_pnl"], reverse=True)

    out.append("### Top 20 (profit)")
    out.append("")
    out.append("| # | Game | Date | Category | MT | Cost | Payout | P&L | ROI |")
    out.append("|---|------|------|----------|-----|------|--------|-----|-----|")
    for i, g in enumerate(sorted_by_pnl[:20], 1):
        mts = "/".join(g["market_types"])
        cat = g.get("sport", "Other")
        out.append(
            f"| {i} | {g['game_key']} | {g['date']} | {cat} | {mts} | "
            f"${g['net_cost']:,.0f} | ${g['total_payout']:,.0f} | "
            f"${g['total_pnl']:,.0f} | {g['roi_pct']:.0f}% |"
        )
    out.append("")

    out.append("### Bottom 20 (loss)")
    out.append("")
    out.append("| # | Game | Date | Category | MT | Cost | Payout | P&L | ROI |")
    out.append("|---|------|------|----------|-----|------|--------|-----|-----|")
    all_by_pnl = sorted(games, key=lambda x: x["total_pnl"])
    for i, g in enumerate(all_by_pnl[:20], 1):
        mts = "/".join(g["market_types"])
        cat = g.get("sport", "Other")
        out.append(
            f"| {i} | {g['game_key']} | {g['date']} | {cat} | {mts} | "
            f"${g['net_cost']:,.0f} | ${g['total_payout']:,.0f} | "
            f"${g['total_pnl']:,.0f} | {g['roi_pct']:.0f}% |"
        )
    out.append("")

    # -- 7. P&L distribution --
    out.append("---")
    out.append("## 7. Game P&L Distribution")
    out.append("")

    game_pnls = [g["total_pnl"] for g in games if g["net_cost"] > 0]
    game_rois = [g["roi_pct"] for g in games if g["net_cost"] > 0]
    if game_pnls:
        out.append(f"- **Mean P&L / game**: ${mean(game_pnls):,.2f}")
        out.append(f"- **Median P&L / game**: ${median(game_pnls):,.2f}")
        out.append(f"- **Max profit**: ${max(game_pnls):,.2f}")
        out.append(f"- **Max loss**: ${min(game_pnls):,.2f}")
        out.append(f"- **Mean ROI / game**: {mean(game_rois):.2f}%")
        out.append(f"- **Median ROI / game**: {median(game_rois):.2f}%")
        out.append("")

        buckets = [
            ("<-$500", lambda p: p < -500),
            ("-$500~-$200", lambda p: -500 <= p < -200),
            ("-$200~-$50", lambda p: -200 <= p < -50),
            ("-$50~$0", lambda p: -50 <= p < 0),
            ("$0~$50", lambda p: 0 <= p < 50),
            ("$50~$200", lambda p: 50 <= p < 200),
            ("$200~$500", lambda p: 200 <= p < 500),
            (">$500", lambda p: p >= 500),
        ]
        out.append("### Distribution")
        out.append("")
        out.append("| Range | Games | % |")
        out.append("|-------|-------|---|")
        for label, fn in buckets:
            cnt = sum(1 for p in game_pnls if fn(p))
            pct = cnt / len(game_pnls) * 100
            bar = "#" * int(pct / 2)
            out.append(f"| {label:>15} | {cnt:,} | {pct:.1f}% {bar} |")
        out.append("")

    # -- 8. Streaks --
    out.append("---")
    out.append("## 8. Win/Loss Streak Analysis")
    out.append("")

    sorted_games = sorted(games, key=lambda x: x["date"])
    streaks_w: list[int] = []
    streaks_l: list[int] = []
    current = 0
    current_type = ""
    for g in sorted_games:
        if g["total_pnl"] > 0:
            if current_type == "W":
                current += 1
            else:
                if current_type == "L":
                    streaks_l.append(current)
                current = 1
                current_type = "W"
        elif g["total_pnl"] < 0:
            if current_type == "L":
                current += 1
            else:
                if current_type == "W":
                    streaks_w.append(current)
                current = 1
                current_type = "L"
    if current_type == "W":
        streaks_w.append(current)
    elif current_type == "L":
        streaks_l.append(current)

    if streaks_w:
        out.append(f"- **Longest win streak**: {max(streaks_w)} games")
        out.append(f"- **Average win streak**: {mean(streaks_w):.1f} games")
    if streaks_l:
        out.append(f"- **Longest loss streak**: {max(streaks_l)} games")
        out.append(f"- **Average loss streak**: {mean(streaks_l):.1f} games")
    out.append("")

    return "\n".join(out)
