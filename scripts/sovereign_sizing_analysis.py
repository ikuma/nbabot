"""Sovereign2013 position sizing & risk management analysis (stdlib only)."""

import json
import math
import os
import statistics
from datetime import datetime, timezone
from collections import defaultdict

DATA_DIR = "/Users/taro/dev/nbabot/data/traders/sovereign2013"
OUT_DIR = "/Users/taro/dev/nbabot/data/reports/sovereign2013-analysis"


def load_json(name):
    with open(f"{DATA_DIR}/{name}") as f:
        return json.load(f)


def ts_to_date(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def percentile(sorted_arr, p):
    """Linear interpolation percentile on a pre-sorted list."""
    if not sorted_arr:
        return 0.0
    k = (len(sorted_arr) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_arr[int(k)]
    return sorted_arr[f] * (c - k) + sorted_arr[c] * (k - f)


def pct_table(values, label=""):
    arr = sorted(values)
    n = len(arr)
    total = sum(arr)
    mean = total / n if n else 0
    pcts = [10, 25, 50, 75, 90, 95, 99]
    lines = [
        "| 統計量 | 値 |",
        "|--------|------|",
        f"| Count | {n:,} |",
        f"| Mean | ${mean:,.2f} |",
    ]
    for p in pcts:
        lines.append(f"| P{p} | ${percentile(arr, p):,.2f} |")
    lines.append(f"| Max | ${arr[-1]:,.2f} |")
    lines.append(f"| Sum | ${total:,.2f} |")
    return "\n".join(lines)


def top_table(items, headers, n=10):
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["------"] * len(headers)) + " |",
    ]
    for row in items[:n]:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(lines)


def main():
    trades = load_json("raw_trade.json")
    redeems = load_json("raw_redeem.json")
    merges = load_json("raw_merge.json")

    buys = [t for t in trades if t["side"] == "BUY"]
    sells = [t for t in trades if t["side"] == "SELL"]

    report = []
    report.append("# Sovereign2013 ポジションサイジング & リスク管理分析\n")
    report.append(f"**分析日**: {datetime.now().strftime('%Y-%m-%d')}")
    report.append(f"**総TRADE数**: {len(trades):,} (BUY: {len(buys):,}, SELL: {len(sells):,})")
    report.append(f"**REDEEM数**: {len(redeems):,}")
    report.append(f"**MERGE数**: {len(merges):,}\n")

    # === 1. Per-trade size distribution ===
    report.append("## 1. 1取引あたりのサイズ分布 (BUY)\n")
    buy_sizes = sorted([t["usdcSize"] for t in buys])
    report.append(pct_table(buy_sizes))

    # Price distribution
    report.append("\n### BUY 価格帯分布\n")
    price_bins = [
        (0, 0.10), (0.10, 0.20), (0.20, 0.30), (0.30, 0.40),
        (0.40, 0.50), (0.50, 0.60), (0.60, 0.70), (0.70, 0.80),
        (0.80, 0.90), (0.90, 1.00),
    ]
    report.append("| 価格帯 | 取引数 | 割合 | 平均サイズ($) | 合計BUYコスト($) |")
    report.append("|--------|--------|------|------------|--------------|")
    for lo, hi in price_bins:
        matching = [t for t in buys if lo <= t["price"] < hi]
        if matching:
            count = len(matching)
            pct = count / len(buys) * 100
            sizes = [t["usdcSize"] for t in matching]
            avg_size = sum(sizes) / len(sizes)
            total = sum(sizes)
            report.append(
                f"| {lo:.2f}-{hi:.2f} | {count:,} | {pct:.1f}% | "
                f"${avg_size:.2f} | ${total:,.0f} |"
            )

    # === 2. Per-condition total BUY cost ===
    report.append("\n## 2. 1条件(conditionId)あたりの合計BUYコスト分布\n")
    cond_cost = defaultdict(float)
    cond_count = defaultdict(int)
    cond_meta = {}
    for t in buys:
        cid = t["conditionId"]
        cond_cost[cid] += t["usdcSize"]
        cond_count[cid] += 1
        if cid not in cond_meta:
            cond_meta[cid] = {
                "title": t.get("title", ""),
                "slug": t.get("slug", ""),
                "outcome": t.get("outcome", ""),
            }

    cond_costs = sorted(cond_cost.values())
    report.append(pct_table(cond_costs))
    report.append(f"\n**ユニーク条件数**: {len(cond_cost):,}\n")

    # Per-condition BUY count
    report.append("### 1条件あたりのBUY取引回数\n")
    cond_counts_sorted = sorted(cond_count.values())
    n_cc = len(cond_counts_sorted)
    report.append("| 統計量 | 値 |")
    report.append("|--------|------|")
    report.append(f"| Mean | {sum(cond_counts_sorted)/n_cc:.1f} |")
    report.append(f"| Median | {percentile(cond_counts_sorted, 50):.0f} |")
    report.append(f"| P90 | {percentile(cond_counts_sorted, 90):.0f} |")
    report.append(f"| P99 | {percentile(cond_counts_sorted, 99):.0f} |")
    report.append(f"| Max | {cond_counts_sorted[-1]:,} |")

    # === 3. Daily total BUY cost ===
    report.append("\n## 3. 1日あたりの総BUYコスト\n")
    daily_cost = defaultdict(float)
    daily_conds = defaultdict(set)
    daily_buy_count = defaultdict(int)
    for t in buys:
        d = ts_to_date(t["timestamp"])
        daily_cost[d] += t["usdcSize"]
        daily_conds[d].add(t["conditionId"])
        daily_buy_count[d] += 1

    daily_costs = sorted(daily_cost.values())
    report.append(pct_table(daily_costs))
    all_dates = sorted(daily_cost.keys())
    report.append(f"\n**アクティブ日数**: {len(daily_cost):,}")
    report.append(f"**期間**: {all_dates[0]} ~ {all_dates[-1]}\n")

    # === 4. Daily unique conditions ===
    report.append("## 4. 1日あたりの取引条件数 (ユニーク conditionId)\n")
    daily_cond_counts = sorted([len(v) for v in daily_conds.values()])
    n_dc = len(daily_cond_counts)
    report.append("| 統計量 | 値 |")
    report.append("|--------|------|")
    report.append(f"| Mean | {sum(daily_cond_counts)/n_dc:.1f} |")
    report.append(f"| Median | {percentile(daily_cond_counts, 50):.0f} |")
    report.append(f"| P75 | {percentile(daily_cond_counts, 75):.0f} |")
    report.append(f"| P90 | {percentile(daily_cond_counts, 90):.0f} |")
    report.append(f"| P95 | {percentile(daily_cond_counts, 95):.0f} |")
    report.append(f"| Max | {daily_cond_counts[-1]:,} |")

    # === 5. Top 20 largest single positions ===
    report.append("\n## 5. 最大単一ポジション (conditionId 別 BUY cost) Top 20\n")
    sorted_conds = sorted(cond_cost.items(), key=lambda x: -x[1])
    rows = []
    for cid, cost in sorted_conds[:20]:
        meta = cond_meta.get(cid, {})
        rows.append([
            meta.get("title", "")[:40],
            meta.get("outcome", ""),
            f"${cost:,.0f}",
            f"{cond_count[cid]:,}",
            meta.get("slug", "")[:30],
        ])
    report.append(top_table(rows, ["イベント", "アウトカム", "BUYコスト", "取引回数", "slug"], n=20))

    # === 6. Top 20 daily exposure ===
    report.append("\n## 6. 1日の最大エクスポージャー Top 20\n")
    sorted_daily = sorted(daily_cost.items(), key=lambda x: -x[1])
    rows = []
    for d, cost in sorted_daily[:20]:
        rows.append([
            d,
            f"${cost:,.0f}",
            f"{len(daily_conds[d]):,}",
            f"{daily_buy_count[d]:,}",
        ])
    report.append(top_table(rows, ["日付", "BUYコスト", "条件数", "取引回数"], n=20))

    # === 7. Sport breakdown ===
    report.append("\n## 7. スポーツ別内訳\n")
    sport_cost = defaultdict(float)
    sport_count = defaultdict(int)
    for t in buys:
        slug = t.get("slug", "") or t.get("eventSlug", "")
        if slug.startswith("nba-"):
            sport = "NBA"
        elif slug.startswith("mlb-"):
            sport = "MLB"
        elif slug.startswith("nfl-"):
            sport = "NFL"
        elif slug.startswith("nhl-"):
            sport = "NHL"
        elif slug.startswith("ncaa"):
            sport = "NCAA"
        elif slug.startswith("soccer") or slug.startswith("epl") or slug.startswith("ucl"):
            sport = "Soccer"
        elif slug.startswith("wnba"):
            sport = "WNBA"
        elif slug.startswith("mls"):
            sport = "MLS"
        else:
            sport = f"Other"
        sport_cost[sport] += t["usdcSize"]
        sport_count[sport] += 1

    sorted_sports = sorted(sport_cost.items(), key=lambda x: -x[1])
    total_buy_cost = sum(sport_cost.values())
    report.append("| スポーツ | BUYコスト | 取引数 | 割合 |")
    report.append("|---------|----------|--------|------|")
    for sport, cost in sorted_sports:
        pct = cost / total_buy_cost * 100
        report.append(f"| {sport} | ${cost:,.0f} | {sport_count[sport]:,} | {pct:.1f}% |")

    # === 8. SELL analysis ===
    report.append("\n## 8. SELL 取引分析\n")
    if sells:
        sell_sizes = sorted([t["usdcSize"] for t in sells])
        report.append(f"**SELL取引数**: {len(sells):,}")
        report.append(f"**SELL総額**: ${sum(sell_sizes):,.0f}")
        report.append(f"**BUY/SELL比率**: {len(buys)/len(sells):.1f}x\n")
        report.append(pct_table(sell_sizes))
    else:
        report.append("SELL取引なし\n")

    # === 9. REDEEM & MERGE ===
    report.append("\n## 9. REDEEM & MERGE 分析\n")
    if redeems:
        redeem_sizes = [r["usdcSize"] for r in redeems]
        report.append(f"**REDEEM数**: {len(redeems):,}")
        report.append(f"**REDEEM総額**: ${sum(redeem_sizes):,.0f}")
        report.append(f"**REDEEM平均**: ${sum(redeem_sizes)/len(redeem_sizes):,.2f}\n")
    if merges:
        merge_sizes = [m["usdcSize"] for m in merges]
        report.append(f"**MERGE数**: {len(merges):,}")
        report.append(f"**MERGE総額**: ${sum(merge_sizes):,.0f}")
        report.append(f"**MERGE平均**: ${sum(merge_sizes)/len(merge_sizes):,.2f}\n")

    # === 10. Time-of-day ===
    report.append("## 10. 時間帯別取引パターン (UTC)\n")
    hour_cost = defaultdict(float)
    hour_count = defaultdict(int)
    for t in buys:
        h = datetime.fromtimestamp(t["timestamp"], tz=timezone.utc).hour
        hour_cost[h] += t["usdcSize"]
        hour_count[h] += 1
    report.append("| 時間 (UTC) | BUYコスト | 取引数 |")
    report.append("|-----------|----------|--------|")
    for h in range(24):
        if hour_count[h] > 0:
            report.append(f"| {h:02d}:00 | ${hour_cost[h]:,.0f} | {hour_count[h]:,} |")

    # === 11. Condition avg buy price distribution ===
    report.append("\n## 11. 条件別の平均購入価格分布\n")
    cond_prices = defaultdict(list)
    for t in buys:
        cond_prices[t["conditionId"]].append(t["price"])
    avg_prices = sorted([sum(v)/len(v) for v in cond_prices.values()])
    report.append(pct_table(avg_prices))

    # === 12. Sizing by price bucket ===
    report.append("\n## 12. ポジションサイジングパターン: 価格帯別の1条件あたりBUYコスト\n")
    # Use VWAP price per condition
    cond_vwap = {}
    for cid, prices in cond_prices.items():
        cond_vwap[cid] = sum(prices) / len(prices)

    bucket_cond_costs = defaultdict(list)
    for cid, cost in cond_cost.items():
        p = cond_vwap.get(cid, 0)
        for lo, hi in price_bins:
            if lo <= p < hi:
                bucket_cond_costs[(lo, hi)].append(cost)
                break

    report.append("| 価格帯 | 条件数 | 平均BUYコスト | 中央値 | P90 | P99 |")
    report.append("|--------|--------|------------|--------|-----|-----|")
    for lo, hi in price_bins:
        key = (lo, hi)
        if key in bucket_cond_costs:
            arr = sorted(bucket_cond_costs[key])
            n = len(arr)
            mean = sum(arr) / n
            med = percentile(arr, 50)
            p90 = percentile(arr, 90)
            p99 = percentile(arr, 99)
            report.append(
                f"| {lo:.2f}-{hi:.2f} | {n:,} | ${mean:,.0f} | "
                f"${med:,.0f} | ${p90:,.0f} | ${p99:,.0f} |"
            )

    # === nbabot implications ===
    report.append("\n---\n")
    report.append("## nbabot への示唆\n")

    med_per_trade = percentile(buy_sizes, 50)
    med_per_cond = percentile(cond_costs, 50)
    med_daily = percentile(daily_costs, 50)
    p90_daily_cond = percentile(daily_cond_counts, 90)
    sell_ratio = len(buys) / max(len(sells), 1)

    report.append(f"1. **1取引中央値 ${med_per_trade:.2f}** — sovereign は極めて小ロット (< $10) を多頻度で発注する分散戦略")
    report.append(f"2. **1条件中央値 ${med_per_cond:.0f}** — 1 条件あたりの集積はこの水準。nbabot の MAX_POSITION_USD パラメータの参考値")
    report.append(f"3. **日次BUYコスト中央値 ${med_daily:,.0f}** — nbabot の MAX_DAILY_EXPOSURE_USD は $2,000 だが、sovereign は桁違いのスケール")
    report.append(f"4. **P90 日次条件数 {p90_daily_cond:.0f}** — 極めて広範に分散。nbabot の MAX_DAILY_POSITIONS=20 は保守的で適切")
    report.append(f"5. **BUY/SELL比 {sell_ratio:.1f}x** — ほぼ SELL しない hold-to-expiry 戦略。nbabot の BUY-only + auto-settle 方針と一致")
    report.append(f"6. **価格帯別サイジング** — セクション12 を参照。低価格帯でもサイジングを抑制していない点が特徴的")

    # Write
    os.makedirs(OUT_DIR, exist_ok=True)
    outpath = f"{OUT_DIR}/sizing_risk.md"
    with open(outpath, "w") as f:
        f.write("\n".join(report))
    print(f"Report written to {outpath}")
    print(f"Total lines: {len(report)}")


if __name__ == "__main__":
    main()
