"""Analyze sovereign2013's market selection, timing, and temporal patterns."""

import json
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta

sys.path.insert(0, "/Users/taro/dev/nbabot")
from src.analysis.pnl import build_condition_pnl, classify_sport, classify_market_type, classify_category

# US Eastern timezone offset helper
US_EASTERN_OFFSET = timedelta(hours=-5)  # EST (simplified, ignoring DST)

DATA_DIR = "/Users/taro/dev/nbabot/data/traders/sovereign2013"
OUT_DIR = "/Users/taro/dev/nbabot/data/reports/sovereign2013-analysis"


def load_json(path: str) -> list[dict]:
    with open(path) as f:
        return json.load(f)


def ts_to_eastern(ts: int) -> datetime:
    """Convert unix timestamp to US Eastern datetime (simplified EST)."""
    utc_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    # Simple EST offset; for full accuracy would need pytz/zoneinfo
    return utc_dt + US_EASTERN_OFFSET


def main():
    print("Loading data...")
    trades = load_json(f"{DATA_DIR}/raw_trade.json")
    redeems = load_json(f"{DATA_DIR}/raw_redeem.json")
    merges = load_json(f"{DATA_DIR}/raw_merge.json")

    print(f"  TRADE: {len(trades):,} records")
    print(f"  REDEEM: {len(redeems):,} records")
    print(f"  MERGE: {len(merges):,} records")

    print("Building condition PnL...")
    conditions = build_condition_pnl(trades, redeems, merges)
    print(f"  {len(conditions):,} conditions")

    # =========================================================================
    # 1. Sport-level PnL
    # =========================================================================
    print("Analyzing by sport...")
    sport_stats: dict[str, dict] = defaultdict(
        lambda: {"conditions": 0, "wins": 0, "losses": 0, "buy": 0.0, "sell": 0.0,
                 "redeem": 0.0, "merge": 0.0, "trade_count": 0}
    )
    for c in conditions.values():
        sport = c["sport"]
        s = sport_stats[sport]
        s["conditions"] += 1
        s["buy"] += c["buy_cost"]
        s["sell"] += c["sell_proceeds"]
        s["redeem"] += c["redeem_usdc"]
        s["merge"] += c["merge_usdc"]
        s["trade_count"] += c["trade_count"]
        if c["status"] == "WIN":
            s["wins"] += 1
        elif c["status"] == "LOSS_OR_OPEN":
            s["losses"] += 1

    # =========================================================================
    # 2. Market type PnL
    # =========================================================================
    print("Analyzing by market type...")
    mt_stats: dict[str, dict] = defaultdict(
        lambda: {"conditions": 0, "wins": 0, "losses": 0, "buy": 0.0, "sell": 0.0,
                 "redeem": 0.0, "merge": 0.0}
    )
    for c in conditions.values():
        mt = c["market_type"]
        m = mt_stats[mt]
        m["conditions"] += 1
        m["buy"] += c["buy_cost"]
        m["sell"] += c["sell_proceeds"]
        m["redeem"] += c["redeem_usdc"]
        m["merge"] += c["merge_usdc"]
        if c["status"] == "WIN":
            m["wins"] += 1
        elif c["status"] == "LOSS_OR_OPEN":
            m["losses"] += 1

    # =========================================================================
    # 3. Category PnL
    # =========================================================================
    print("Analyzing by category...")
    cat_stats: dict[str, dict] = defaultdict(
        lambda: {"conditions": 0, "wins": 0, "losses": 0, "buy": 0.0, "sell": 0.0,
                 "redeem": 0.0, "merge": 0.0}
    )
    for c in conditions.values():
        cat = c["category"]
        cs = cat_stats[cat]
        cs["conditions"] += 1
        cs["buy"] += c["buy_cost"]
        cs["sell"] += c["sell_proceeds"]
        cs["redeem"] += c["redeem_usdc"]
        cs["merge"] += c["merge_usdc"]
        if c["status"] == "WIN":
            cs["wins"] += 1
        elif c["status"] == "LOSS_OR_OPEN":
            cs["losses"] += 1

    # =========================================================================
    # 4. Hour-of-day analysis (BUY trades only)
    # =========================================================================
    print("Analyzing by hour of day...")
    hour_stats: dict[int, dict] = {
        h: {"count": 0, "volume": 0.0} for h in range(24)
    }
    for t in trades:
        if t.get("side") == "BUY":
            et = ts_to_eastern(t["timestamp"])
            h = et.hour
            hour_stats[h]["count"] += 1
            hour_stats[h]["volume"] += float(t.get("usdcSize", 0))

    # =========================================================================
    # 5. Day-of-week analysis
    # =========================================================================
    print("Analyzing by day of week...")
    dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    dow_stats: dict[int, dict] = {
        d: {"buy_count": 0, "volume": 0.0, "conditions": set()} for d in range(7)
    }
    for t in trades:
        if t.get("side") == "BUY":
            et = ts_to_eastern(t["timestamp"])
            dow = et.weekday()
            dow_stats[dow]["buy_count"] += 1
            dow_stats[dow]["volume"] += float(t.get("usdcSize", 0))
            dow_stats[dow]["conditions"].add(t.get("conditionId", ""))

    # PnL by day of week: use condition's first trade timestamp
    dow_pnl: dict[int, dict] = {
        d: {"pnl": 0.0, "cost": 0.0, "conditions": 0} for d in range(7)
    }
    for c in conditions.values():
        if c["first_trade_ts"] == float("inf"):
            continue
        et = ts_to_eastern(int(c["first_trade_ts"]))
        dow = et.weekday()
        dow_pnl[dow]["pnl"] += c["pnl"]
        dow_pnl[dow]["cost"] += c["net_cost"]
        dow_pnl[dow]["conditions"] += 1

    # =========================================================================
    # 6. Monthly trend
    # =========================================================================
    print("Analyzing monthly trend...")
    monthly_stats: dict[str, dict] = defaultdict(
        lambda: {"conditions": 0, "wins": 0, "losses": 0, "buy": 0.0, "sell": 0.0,
                 "redeem": 0.0, "merge": 0.0, "trade_count": 0}
    )
    for c in conditions.values():
        if c["first_trade_ts"] == float("inf"):
            continue
        month = datetime.fromtimestamp(int(c["first_trade_ts"]), tz=timezone.utc).strftime("%Y-%m")
        ms = monthly_stats[month]
        ms["conditions"] += 1
        ms["buy"] += c["buy_cost"]
        ms["sell"] += c["sell_proceeds"]
        ms["redeem"] += c["redeem_usdc"]
        ms["merge"] += c["merge_usdc"]
        ms["trade_count"] += c["trade_count"]
        if c["status"] == "WIN":
            ms["wins"] += 1
        elif c["status"] == "LOSS_OR_OPEN":
            ms["losses"] += 1

    # =========================================================================
    # 7. Sport x Market Type cross-tab
    # =========================================================================
    print("Analyzing sport x market type...")
    sport_mt_stats: dict[str, dict] = defaultdict(
        lambda: {"conditions": 0, "wins": 0, "losses": 0, "buy": 0.0, "sell": 0.0,
                 "redeem": 0.0, "merge": 0.0}
    )
    for c in conditions.values():
        key = f"{c['sport']}|{c['market_type']}"
        smt = sport_mt_stats[key]
        smt["conditions"] += 1
        smt["buy"] += c["buy_cost"]
        smt["sell"] += c["sell_proceeds"]
        smt["redeem"] += c["redeem_usdc"]
        smt["merge"] += c["merge_usdc"]
        if c["status"] == "WIN":
            smt["wins"] += 1
        elif c["status"] == "LOSS_OR_OPEN":
            smt["losses"] += 1

    # =========================================================================
    # Generate report
    # =========================================================================
    print("Generating report...")
    out = []
    out.append("# sovereign2013 マーケット・タイミング分析")
    out.append("")
    out.append(f"**分析日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    out.append(f"**データ**: TRADE {len(trades):,} / REDEEM {len(redeems):,} / MERGE {len(merges):,}")
    out.append(f"**条件数**: {len(conditions):,}")
    out.append("")

    # --- 1. Sport PnL ---
    out.append("---")
    out.append("## 1. スポーツ別 PnL")
    out.append("")
    out.append("| スポーツ | 条件数 | 勝 | 負/Open | 勝率 | Volume (Buy) | Net Cost | Payout | PnL | ROI |")
    out.append("|----------|--------|-----|---------|------|-------------|----------|--------|-----|-----|")
    for sport in sorted(sport_stats, key=lambda s: sport_stats[s]["buy"], reverse=True):
        s = sport_stats[sport]
        net = s["buy"] - s["sell"]
        payout = s["redeem"] + s["merge"]
        pnl = payout - net
        wl = s["wins"] + s["losses"]
        wr = s["wins"] / wl * 100 if wl > 0 else 0
        roi = pnl / net * 100 if net > 0 else 0
        out.append(
            f"| {sport} | {s['conditions']:,} | {s['wins']:,} | {s['losses']:,} | "
            f"{wr:.1f}% | ${s['buy']:,.0f} | ${net:,.0f} | ${payout:,.0f} | "
            f"${pnl:,.0f} | {roi:.1f}% |"
        )
    out.append("")

    # --- 2. Market Type PnL ---
    out.append("---")
    out.append("## 2. マーケットタイプ別 PnL")
    out.append("")
    out.append("| タイプ | 条件数 | 勝 | 負/Open | 勝率 | Net Cost | Payout | PnL | ROI |")
    out.append("|--------|--------|-----|---------|------|----------|--------|-----|-----|")
    for mt_name in sorted(mt_stats, key=lambda k: mt_stats[k]["buy"], reverse=True):
        m = mt_stats[mt_name]
        net = m["buy"] - m["sell"]
        payout = m["redeem"] + m["merge"]
        pnl = payout - net
        wl = m["wins"] + m["losses"]
        wr = m["wins"] / wl * 100 if wl > 0 else 0
        roi = pnl / net * 100 if net > 0 else 0
        out.append(
            f"| {mt_name} | {m['conditions']:,} | {m['wins']:,} | {m['losses']:,} | "
            f"{wr:.1f}% | ${net:,.0f} | ${payout:,.0f} | ${pnl:,.0f} | {roi:.1f}% |"
        )
    out.append("")

    # --- 3. Category PnL ---
    out.append("---")
    out.append("## 3. カテゴリ別 PnL")
    out.append("")
    out.append("| カテゴリ | 条件数 | 勝 | 負/Open | 勝率 | Net Cost | Payout | PnL | ROI |")
    out.append("|----------|--------|-----|---------|------|----------|--------|-----|-----|")
    for cat in sorted(cat_stats, key=lambda k: cat_stats[k]["buy"], reverse=True):
        cs = cat_stats[cat]
        net = cs["buy"] - cs["sell"]
        payout = cs["redeem"] + cs["merge"]
        pnl = payout - net
        wl = cs["wins"] + cs["losses"]
        wr = cs["wins"] / wl * 100 if wl > 0 else 0
        roi = pnl / net * 100 if net > 0 else 0
        out.append(
            f"| {cat} | {cs['conditions']:,} | {cs['wins']:,} | {cs['losses']:,} | "
            f"{wr:.1f}% | ${net:,.0f} | ${payout:,.0f} | ${pnl:,.0f} | {roi:.1f}% |"
        )
    out.append("")

    # --- 4. Hour-of-day ---
    out.append("---")
    out.append("## 4. 時間帯別取引量 (US Eastern, BUY のみ)")
    out.append("")
    out.append("| 時間 (ET) | 取引数 | Volume ($) | 割合 |")
    out.append("|-----------|--------|------------|------|")
    total_buy_count = sum(h["count"] for h in hour_stats.values())
    total_buy_vol = sum(h["volume"] for h in hour_stats.values())
    for h in range(24):
        hs = hour_stats[h]
        pct = hs["count"] / total_buy_count * 100 if total_buy_count > 0 else 0
        bar = "#" * int(pct)
        out.append(
            f"| {h:02d}:00-{h:02d}:59 | {hs['count']:,} | ${hs['volume']:,.0f} | {pct:.1f}% {bar} |"
        )
    out.append("")
    out.append(f"**合計 BUY 取引**: {total_buy_count:,} (Volume: ${total_buy_vol:,.0f})")
    out.append("")

    # Peak hours summary
    sorted_hours = sorted(range(24), key=lambda h: hour_stats[h]["count"], reverse=True)
    out.append("### ピーク時間帯 (取引数 Top 5)")
    out.append("")
    for rank, h in enumerate(sorted_hours[:5], 1):
        hs = hour_stats[h]
        pct = hs["count"] / total_buy_count * 100
        out.append(f"{rank}. **{h:02d}:00 ET** — {hs['count']:,} trades ({pct:.1f}%), Volume ${hs['volume']:,.0f}")
    out.append("")

    # --- 5. Day of week ---
    out.append("---")
    out.append("## 5. 曜日別パターン")
    out.append("")
    out.append("### 取引量")
    out.append("")
    out.append("| 曜日 | BUY 取引数 | Volume ($) | ユニーク条件数 |")
    out.append("|------|------------|------------|----------------|")
    for d in range(7):
        ds = dow_stats[d]
        out.append(
            f"| {dow_names[d]} | {ds['buy_count']:,} | ${ds['volume']:,.0f} | {len(ds['conditions']):,} |"
        )
    out.append("")

    out.append("### PnL (条件の初回取引日ベース)")
    out.append("")
    out.append("| 曜日 | 条件数 | Net Cost | PnL | ROI |")
    out.append("|------|--------|----------|-----|-----|")
    for d in range(7):
        dp = dow_pnl[d]
        roi = dp["pnl"] / dp["cost"] * 100 if dp["cost"] > 0 else 0
        out.append(
            f"| {dow_names[d]} | {dp['conditions']:,} | ${dp['cost']:,.0f} | "
            f"${dp['pnl']:,.0f} | {roi:.1f}% |"
        )
    out.append("")

    # --- 6. Monthly trend ---
    out.append("---")
    out.append("## 6. 月次トレンド")
    out.append("")
    out.append("| 月 | 条件数 | 勝 | 負 | 勝率 | Volume (Buy) | Net Cost | Payout | PnL | ROI | 累計PnL |")
    out.append("|-----|--------|-----|-----|------|-------------|----------|--------|-----|-----|---------|")
    cumulative = 0.0
    for month in sorted(monthly_stats):
        ms = monthly_stats[month]
        net = ms["buy"] - ms["sell"]
        payout = ms["redeem"] + ms["merge"]
        pnl = payout - net
        cumulative += pnl
        wl = ms["wins"] + ms["losses"]
        wr = ms["wins"] / wl * 100 if wl > 0 else 0
        roi = pnl / net * 100 if net > 0 else 0
        out.append(
            f"| {month} | {ms['conditions']:,} | {ms['wins']} | {ms['losses']} | "
            f"{wr:.1f}% | ${ms['buy']:,.0f} | ${net:,.0f} | ${payout:,.0f} | "
            f"${pnl:,.0f} | {roi:.1f}% | ${cumulative:,.0f} |"
        )
    out.append("")

    # Identify trend (last 3 months vs earlier)
    sorted_months = sorted(monthly_stats.keys())
    if len(sorted_months) >= 4:
        recent_3 = sorted_months[-3:]
        earlier = sorted_months[:-3]
        recent_pnl = sum(
            (monthly_stats[m]["redeem"] + monthly_stats[m]["merge"]) - (monthly_stats[m]["buy"] - monthly_stats[m]["sell"])
            for m in recent_3
        )
        recent_cost = sum(monthly_stats[m]["buy"] - monthly_stats[m]["sell"] for m in recent_3)
        earlier_pnl = sum(
            (monthly_stats[m]["redeem"] + monthly_stats[m]["merge"]) - (monthly_stats[m]["buy"] - monthly_stats[m]["sell"])
            for m in earlier
        )
        earlier_cost = sum(monthly_stats[m]["buy"] - monthly_stats[m]["sell"] for m in earlier)
        recent_roi = recent_pnl / recent_cost * 100 if recent_cost > 0 else 0
        earlier_roi = earlier_pnl / earlier_cost * 100 if earlier_cost > 0 else 0
        out.append("### トレンド比較")
        out.append("")
        out.append(f"- **直近3ヶ月** ({', '.join(recent_3)}): PnL ${recent_pnl:,.0f}, ROI {recent_roi:.1f}%")
        out.append(f"- **それ以前** ({earlier[0]}~{earlier[-1]}): PnL ${earlier_pnl:,.0f}, ROI {earlier_roi:.1f}%")
        if recent_roi > earlier_roi:
            out.append(f"- 直近3ヶ月は ROI が **改善** している (+{recent_roi - earlier_roi:.1f}pp)")
        else:
            out.append(f"- 直近3ヶ月は ROI が **悪化** している ({recent_roi - earlier_roi:.1f}pp)")
        out.append("")

    # --- 7. Sport x Market Type ---
    out.append("---")
    out.append("## 7. スポーツ x マーケットタイプ クロス集計 (上位20)")
    out.append("")
    out.append("| スポーツ | タイプ | 条件数 | 勝率 | Net Cost | PnL | ROI |")
    out.append("|----------|--------|--------|------|----------|-----|-----|")
    sorted_smt = sorted(sport_mt_stats.items(), key=lambda x: x[1]["buy"], reverse=True)
    for key, smt in sorted_smt[:20]:
        sport, mt = key.split("|")
        net = smt["buy"] - smt["sell"]
        payout = smt["redeem"] + smt["merge"]
        pnl = payout - net
        wl = smt["wins"] + smt["losses"]
        wr = smt["wins"] / wl * 100 if wl > 0 else 0
        roi = pnl / net * 100 if net > 0 else 0
        out.append(
            f"| {sport} | {mt} | {smt['conditions']:,} | {wr:.1f}% | "
            f"${net:,.0f} | ${pnl:,.0f} | {roi:.1f}% |"
        )
    out.append("")

    # --- 8. Implications for nbabot ---
    out.append("---")
    out.append("## 8. nbabot への示唆")
    out.append("")

    # Find NBA specific stats
    nba = sport_stats.get("NBA", None)
    if nba:
        nba_net = nba["buy"] - nba["sell"]
        nba_payout = nba["redeem"] + nba["merge"]
        nba_pnl = nba_payout - nba_net
        nba_roi = nba_pnl / nba_net * 100 if nba_net > 0 else 0
        nba_wr = nba["wins"] / (nba["wins"] + nba["losses"]) * 100 if (nba["wins"] + nba["losses"]) > 0 else 0
        out.append(f"- **NBA 成績**: {nba['conditions']:,} 条件, 勝率 {nba_wr:.1f}%, PnL ${nba_pnl:,.0f}, ROI {nba_roi:.1f}%")

    # NBA Moneyline specific
    nba_ml = sport_mt_stats.get("NBA|Moneyline", None)
    if nba_ml:
        net = nba_ml["buy"] - nba_ml["sell"]
        payout = nba_ml["redeem"] + nba_ml["merge"]
        pnl = payout - net
        roi = pnl / net * 100 if net > 0 else 0
        wl = nba_ml["wins"] + nba_ml["losses"]
        wr = nba_ml["wins"] / wl * 100 if wl > 0 else 0
        out.append(f"- **NBA Moneyline**: {nba_ml['conditions']:,} 条件, 勝率 {wr:.1f}%, PnL ${pnl:,.0f}, ROI {roi:.1f}%")

    # Peak trading hours for NBA
    nba_hour: dict[int, int] = defaultdict(int)
    for t in trades:
        if t.get("side") == "BUY" and classify_sport(t.get("slug", "")) == "NBA":
            et = ts_to_eastern(t["timestamp"])
            nba_hour[et.hour] += 1
    if nba_hour:
        top_nba_hours = sorted(nba_hour.items(), key=lambda x: x[1], reverse=True)[:3]
        hours_str = ", ".join(f"{h:02d}:00 ET ({c:,})" for h, c in top_nba_hours)
        out.append(f"- **NBA ピーク取引時間**: {hours_str}")

    out.append("")

    # Write report
    import os
    os.makedirs(OUT_DIR, exist_ok=True)
    report_path = f"{OUT_DIR}/market_timing.md"
    with open(report_path, "w") as f:
        f.write("\n".join(out))
    print(f"\nReport written to {report_path}")


if __name__ == "__main__":
    main()
