"""Sovereign2013 DCA patterns, trade frequency, and edge source analysis (stdlib only)."""

import json
import math
import os
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
    if not sorted_arr:
        return 0.0
    k = (len(sorted_arr) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_arr[int(k)]
    return sorted_arr[f] * (c - k) + sorted_arr[c] * (k - f)


def main():
    trades = load_json("raw_trade.json")
    redeems = load_json("raw_redeem.json")
    merges = load_json("raw_merge.json")

    buys = [t for t in trades if t["side"] == "BUY"]

    # Build per-condition data
    cond_buy_cost = defaultdict(float)       # total USDC spent buying
    cond_buy_shares = defaultdict(float)     # total shares bought
    cond_buy_count = defaultdict(int)        # number of BUY trades
    cond_buy_timestamps = defaultdict(list)  # timestamps of buys
    cond_buy_prices = defaultdict(list)      # prices of buys
    cond_meta = {}

    for t in buys:
        cid = t["conditionId"]
        cond_buy_cost[cid] += t["usdcSize"]
        cond_buy_shares[cid] += t["size"]
        cond_buy_count[cid] += 1
        cond_buy_timestamps[cid].append(t["timestamp"])
        cond_buy_prices[cid].append(t["price"])
        if cid not in cond_meta:
            cond_meta[cid] = {
                "title": t.get("title", ""),
                "slug": t.get("slug", "") or t.get("eventSlug", ""),
                "outcome": t.get("outcome", ""),
            }

    # REDEEM = winning conditions (payout = shares redeemed)
    cond_redeem = defaultdict(float)
    for r in redeems:
        cond_redeem[r["conditionId"]] += r["usdcSize"]

    # MERGE = losing conditions (get back merge amount, typically losing)
    cond_merge = defaultdict(float)
    for m in merges:
        cond_merge[m["conditionId"]] += m["usdcSize"]

    # Calculate P&L per condition
    # P&L = redeem_amount + merge_amount - buy_cost
    # Redeemed conditions: won (payout = shares, redeem usdcSize = payout)
    # Merged conditions: lost (merge = partial recovery)
    # Neither: still open or lost with no recovery
    cond_pnl = {}
    all_cids = set(cond_buy_cost.keys())

    for cid in all_cids:
        buy_cost = cond_buy_cost[cid]
        redeem = cond_redeem.get(cid, 0)
        merge = cond_merge.get(cid, 0)
        pnl = redeem + merge - buy_cost
        cond_pnl[cid] = {
            "buy_cost": buy_cost,
            "redeem": redeem,
            "merge": merge,
            "pnl": pnl,
            "resolved": cid in cond_redeem or cid in cond_merge,
            "won": cid in cond_redeem and redeem > 0,
        }

    report = []
    report.append("# Sovereign2013 DCA・取引頻度・エッジ源泉分析\n")
    report.append(f"**分析日**: {datetime.now().strftime('%Y-%m-%d')}\n")

    # === 1. DCA パターン ===
    report.append("## 1. DCA (分割購入) パターン\n")

    # Trades per condition
    counts = sorted(cond_buy_count.values())
    report.append("### 1条件あたりのBUY取引回数分布\n")
    report.append("| 統計量 | 値 |")
    report.append("|--------|------|")
    report.append(f"| Count | {len(counts):,} |")
    report.append(f"| Mean | {sum(counts)/len(counts):.1f} |")
    for p in [10, 25, 50, 75, 90, 95, 99]:
        report.append(f"| P{p} | {percentile(counts, p):.0f} |")
    report.append(f"| Max | {counts[-1]:,} |")

    # Time span of DCA per condition (for conditions with >1 trade)
    report.append("\n### DCA期間 (最初のBUYから最後のBUYまでの時間, 分)\n")
    dca_spans = []
    for cid, ts_list in cond_buy_timestamps.items():
        if len(ts_list) > 1:
            span_min = (max(ts_list) - min(ts_list)) / 60.0
            dca_spans.append(span_min)
    dca_spans.sort()
    report.append("| 統計量 | 値 |")
    report.append("|--------|------|")
    report.append(f"| 条件数 (2回以上BUY) | {len(dca_spans):,} |")
    for p in [10, 25, 50, 75, 90, 95, 99]:
        v = percentile(dca_spans, p)
        if v < 60:
            report.append(f"| P{p} | {v:.0f} 分 |")
        elif v < 1440:
            report.append(f"| P{p} | {v/60:.1f} 時間 |")
        else:
            report.append(f"| P{p} | {v/1440:.1f} 日 |")
    report.append(f"| Max | {dca_spans[-1]/1440:.1f} 日 |")

    # DCA count buckets: how many conditions fall into each bucket
    report.append("\n### DCA回数別の条件数分布\n")
    dca_buckets = [(1, 1), (2, 5), (6, 10), (11, 20), (21, 50), (51, 100), (101, 500), (501, 5000)]
    report.append("| DCA回数 | 条件数 | 割合 | 平均BUYコスト | 平均P&L |")
    report.append("|---------|--------|------|-------------|---------|")
    for lo, hi in dca_buckets:
        matching_cids = [cid for cid, cnt in cond_buy_count.items() if lo <= cnt <= hi]
        if matching_cids:
            n = len(matching_cids)
            pct = n / len(cond_buy_count) * 100
            avg_cost = sum(cond_buy_cost[c] for c in matching_cids) / n
            resolved = [c for c in matching_cids if cond_pnl[c]["resolved"]]
            if resolved:
                avg_pnl = sum(cond_pnl[c]["pnl"] for c in resolved) / len(resolved)
            else:
                avg_pnl = 0
            label = f"{lo}" if lo == hi else f"{lo}-{hi}"
            report.append(f"| {label} | {n:,} | {pct:.1f}% | ${avg_cost:,.0f} | ${avg_pnl:,.0f} |")

    # === 2. P&L by outcome ===
    report.append("\n## 2. 条件別P&L分布 (解決済みのみ)\n")

    resolved_cids = [cid for cid in all_cids if cond_pnl[cid]["resolved"]]
    open_cids = [cid for cid in all_cids if not cond_pnl[cid]["resolved"]]
    won_cids = [cid for cid in resolved_cids if cond_pnl[cid]["won"]]
    lost_cids = [cid for cid in resolved_cids if not cond_pnl[cid]["won"]]

    report.append(f"**解決済み条件数**: {len(resolved_cids):,} (勝ち: {len(won_cids):,}, 負け: {len(lost_cids):,})")
    report.append(f"**未解決条件数**: {len(open_cids):,}")

    if resolved_cids:
        total_pnl = sum(cond_pnl[c]["pnl"] for c in resolved_cids)
        total_cost = sum(cond_buy_cost[c] for c in resolved_cids)
        win_rate = len(won_cids) / len(resolved_cids) * 100
        report.append(f"**勝率**: {win_rate:.1f}%")
        report.append(f"**総P&L**: ${total_pnl:,.0f}")
        report.append(f"**総BUYコスト (解決済み)**: ${total_cost:,.0f}")
        report.append(f"**ROI**: {total_pnl/total_cost*100:.2f}%\n")

        # Win P&L
        if won_cids:
            won_pnls = sorted([cond_pnl[c]["pnl"] for c in won_cids])
            report.append("### 勝ち条件のP&L分布\n")
            report.append("| 統計量 | 値 |")
            report.append("|--------|------|")
            report.append(f"| Count | {len(won_pnls):,} |")
            report.append(f"| Mean | ${sum(won_pnls)/len(won_pnls):,.0f} |")
            for p in [10, 25, 50, 75, 90, 95]:
                report.append(f"| P{p} | ${percentile(won_pnls, p):,.0f} |")
            report.append(f"| Max | ${won_pnls[-1]:,.0f} |")
            report.append(f"| Total | ${sum(won_pnls):,.0f} |")

        # Loss P&L
        if lost_cids:
            lost_pnls = sorted([cond_pnl[c]["pnl"] for c in lost_cids])
            report.append("\n### 負け条件のP&L分布\n")
            report.append("| 統計量 | 値 |")
            report.append("|--------|------|")
            report.append(f"| Count | {len(lost_pnls):,} |")
            report.append(f"| Mean | ${sum(lost_pnls)/len(lost_pnls):,.0f} |")
            for p in [5, 10, 25, 50, 75, 90]:
                report.append(f"| P{p} | ${percentile(lost_pnls, p):,.0f} |")
            report.append(f"| Min | ${lost_pnls[0]:,.0f} |")
            report.append(f"| Total | ${sum(lost_pnls):,.0f} |")

    # === 3. Edge by price bucket ===
    report.append("\n## 3. 価格帯別のエッジ (解決済み条件)\n")
    price_bins = [
        (0, 0.10), (0.10, 0.20), (0.20, 0.30), (0.30, 0.40),
        (0.40, 0.50), (0.50, 0.60), (0.60, 0.70), (0.70, 0.80),
        (0.80, 0.90), (0.90, 1.00),
    ]

    # VWAP per condition
    cond_vwap = {}
    for cid in all_cids:
        prices = cond_buy_prices[cid]
        cond_vwap[cid] = sum(prices) / len(prices)

    report.append("| 価格帯 | 条件数 | 勝率 | 総BUYコスト | 総P&L | ROI | 平均P&L/条件 |")
    report.append("|--------|--------|------|-----------|-------|-----|------------|")
    for lo, hi in price_bins:
        bucket_cids = [c for c in resolved_cids if lo <= cond_vwap.get(c, 0) < hi]
        if bucket_cids:
            n = len(bucket_cids)
            wins = sum(1 for c in bucket_cids if cond_pnl[c]["won"])
            wr = wins / n * 100
            total_cost = sum(cond_buy_cost[c] for c in bucket_cids)
            total_pnl = sum(cond_pnl[c]["pnl"] for c in bucket_cids)
            roi = total_pnl / total_cost * 100 if total_cost > 0 else 0
            avg_pnl = total_pnl / n
            report.append(
                f"| {lo:.2f}-{hi:.2f} | {n:,} | {wr:.1f}% | ${total_cost:,.0f} | "
                f"${total_pnl:,.0f} | {roi:.2f}% | ${avg_pnl:,.0f} |"
            )

    # === 4. Edge by sport ===
    report.append("\n## 4. スポーツ別のエッジ (解決済み条件)\n")

    def get_sport(slug):
        if slug.startswith("nba-"):
            return "NBA"
        elif slug.startswith("mlb-"):
            return "MLB"
        elif slug.startswith("nfl-"):
            return "NFL"
        elif slug.startswith("nhl-"):
            return "NHL"
        elif slug.startswith("ncaa") or slug.startswith("cfb-") or slug.startswith("cbb-"):
            return "NCAA"
        elif slug.startswith("wnba"):
            return "WNBA"
        elif slug.startswith("wta-") or slug.startswith("atp-"):
            return "Tennis"
        elif slug.startswith("mma-") or slug.startswith("ufc-"):
            return "MMA/UFC"
        else:
            return "Other"

    sport_stats = defaultdict(lambda: {"cids": [], "cost": 0, "pnl": 0, "wins": 0})
    for cid in resolved_cids:
        slug = cond_meta.get(cid, {}).get("slug", "")
        sport = get_sport(slug)
        sport_stats[sport]["cids"].append(cid)
        sport_stats[sport]["cost"] += cond_buy_cost[cid]
        sport_stats[sport]["pnl"] += cond_pnl[cid]["pnl"]
        if cond_pnl[cid]["won"]:
            sport_stats[sport]["wins"] += 1

    sorted_sports = sorted(sport_stats.items(), key=lambda x: -abs(x[1]["pnl"]))
    report.append("| スポーツ | 条件数 | 勝率 | BUYコスト | P&L | ROI |")
    report.append("|---------|--------|------|----------|-----|-----|")
    for sport, stats in sorted_sports:
        n = len(stats["cids"])
        wr = stats["wins"] / n * 100 if n > 0 else 0
        roi = stats["pnl"] / stats["cost"] * 100 if stats["cost"] > 0 else 0
        report.append(f"| {sport} | {n:,} | {wr:.1f}% | ${stats['cost']:,.0f} | ${stats['pnl']:,.0f} | {roi:.2f}% |")

    # === 5. Top winning and losing conditions ===
    report.append("\n## 5. 最大利益条件 Top 10\n")
    sorted_by_pnl = sorted(
        [(cid, cond_pnl[cid]) for cid in resolved_cids],
        key=lambda x: -x[1]["pnl"]
    )
    report.append("| イベント | アウトカム | BUYコスト | P&L | ROI | DCA回数 |")
    report.append("|---------|----------|----------|-----|-----|---------|")
    for cid, pnl_data in sorted_by_pnl[:10]:
        meta = cond_meta.get(cid, {})
        roi = pnl_data["pnl"] / pnl_data["buy_cost"] * 100 if pnl_data["buy_cost"] > 0 else 0
        report.append(
            f"| {meta.get('title', '')[:35]} | {meta.get('outcome', '')[:15]} | "
            f"${pnl_data['buy_cost']:,.0f} | ${pnl_data['pnl']:,.0f} | {roi:.0f}% | "
            f"{cond_buy_count[cid]:,} |"
        )

    report.append("\n## 6. 最大損失条件 Top 10\n")
    report.append("| イベント | アウトカム | BUYコスト | P&L | DCA回数 |")
    report.append("|---------|----------|----------|-----|---------|")
    for cid, pnl_data in sorted_by_pnl[-10:][::-1]:  # worst 10
        meta = cond_meta.get(cid, {})
        report.append(
            f"| {meta.get('title', '')[:35]} | {meta.get('outcome', '')[:15]} | "
            f"${pnl_data['buy_cost']:,.0f} | ${pnl_data['pnl']:,.0f} | "
            f"{cond_buy_count[cid]:,} |"
        )

    # === 6. Market type analysis (moneyline vs spread vs total) ===
    report.append("\n## 7. マーケットタイプ別エッジ\n")

    def classify_market(slug, title):
        title_lower = (title or "").lower()
        slug_lower = (slug or "").lower()
        if "spread" in slug_lower or "spread" in title_lower:
            return "Spread"
        elif "total" in slug_lower or "o/u" in title_lower:
            return "Total (O/U)"
        elif "will" in title_lower or "mvp" in title_lower or "award" in title_lower:
            return "Prop/Special"
        else:
            return "Moneyline"

    market_stats = defaultdict(lambda: {"count": 0, "cost": 0, "pnl": 0, "wins": 0})
    for cid in resolved_cids:
        meta = cond_meta.get(cid, {})
        mtype = classify_market(meta.get("slug", ""), meta.get("title", ""))
        market_stats[mtype]["count"] += 1
        market_stats[mtype]["cost"] += cond_buy_cost[cid]
        market_stats[mtype]["pnl"] += cond_pnl[cid]["pnl"]
        if cond_pnl[cid]["won"]:
            market_stats[mtype]["wins"] += 1

    sorted_markets = sorted(market_stats.items(), key=lambda x: -x[1]["cost"])
    report.append("| タイプ | 条件数 | 勝率 | BUYコスト | P&L | ROI |")
    report.append("|--------|--------|------|----------|-----|-----|")
    for mtype, stats in sorted_markets:
        wr = stats["wins"] / stats["count"] * 100 if stats["count"] > 0 else 0
        roi = stats["pnl"] / stats["cost"] * 100 if stats["cost"] > 0 else 0
        report.append(
            f"| {mtype} | {stats['count']:,} | {wr:.1f}% | "
            f"${stats['cost']:,.0f} | ${stats['pnl']:,.0f} | {roi:.2f}% |"
        )

    # === 7. Edge concentration ===
    report.append("\n## 8. エッジの集中度\n")
    report.append("利益の何%がトップN条件から来ているか:\n")

    total_profit = sum(cond_pnl[c]["pnl"] for c in resolved_cids if cond_pnl[c]["pnl"] > 0)
    profitable_sorted = sorted(
        [(cid, cond_pnl[cid]["pnl"]) for cid in resolved_cids if cond_pnl[cid]["pnl"] > 0],
        key=lambda x: -x[1]
    )
    report.append("| トップN | 累積利益 | 全利益に占める割合 |")
    report.append("|---------|---------|-----------------|")
    for n in [10, 50, 100, 500, 1000]:
        if n <= len(profitable_sorted):
            cum = sum(x[1] for x in profitable_sorted[:n])
            pct = cum / total_profit * 100
            report.append(f"| Top {n} | ${cum:,.0f} | {pct:.1f}% |")

    total_loss = sum(abs(cond_pnl[c]["pnl"]) for c in resolved_cids if cond_pnl[c]["pnl"] < 0)
    report.append(f"\n**総利益 (勝ち条件合計)**: ${total_profit:,.0f}")
    report.append(f"**総損失 (負け条件合計)**: -${total_loss:,.0f}")
    report.append(f"**純利益**: ${total_profit - total_loss:,.0f}")
    report.append(f"**利益/損失比**: {total_profit/total_loss:.2f}x" if total_loss > 0 else "")

    # === nbabot implications ===
    report.append("\n---\n")
    report.append("## nbabot への示唆\n")
    report.append("1. **DCA中央値8回** — sovereign は1条件あたり複数回に分けて購入。nbabot の1回発注は流動性制約を考慮すると小資金では妥当だが、スケールする場合は分割発注を検討")
    report.append("2. **DCA期間** — 上記の分布を参照。sovereign がどの程度の時間幅で分割しているかが分かる")
    report.append("3. **価格帯別ROI** — セクション3 で最もエッジの大きい価格帯を特定。nbabot の SWEET_SPOT パラメータ調整の根拠")
    report.append("4. **スポーツ別ROI** — NBA以外にもエッジがあるか。nbabot の対象拡大の判断材料")
    report.append("5. **マーケットタイプ別ROI** — Moneyline vs Spread vs Total。nbabot は現在 Moneyline のみだが、拡張の余地")
    report.append("6. **エッジ集中度** — 利益がロングテールか集中型か。分散度合いの判断材料")

    # Write
    os.makedirs(OUT_DIR, exist_ok=True)
    outpath = f"{OUT_DIR}/edge_dca.md"
    with open(outpath, "w") as f:
        f.write("\n".join(report))
    print(f"Report written to {outpath}")


if __name__ == "__main__":
    main()
