"""Sovereign2013 DCA pattern & edge source analysis."""
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean, median

sys.path.insert(0, "/Users/taro/dev/nbabot")
from src.analysis.pnl import build_condition_pnl, classify_sport

DATA_DIR = "/Users/taro/dev/nbabot/data/traders/sovereign2013"
OUT = "/Users/taro/dev/nbabot/data/reports/sovereign2013-analysis/edge_dca.md"


def load_json(name):
    with open(f"{DATA_DIR}/{name}") as f:
        return json.load(f)


def ts_to_dt(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def main():
    print("Loading data...")
    trades = load_json("raw_trade.json")
    redeems = load_json("raw_redeem.json")
    merges = load_json("raw_merge.json")

    print(f"Trades: {len(trades)}, Redeems: {len(redeems)}, Merges: {len(merges)}")

    # Build condition PnL
    conditions = build_condition_pnl(trades, redeems, merges)
    print(f"Conditions: {len(conditions)}")

    # --- Pre-compute per-condition trade details ---
    # Group trades by conditionId for DCA/SELL analysis
    cond_trades = defaultdict(list)
    for t in trades:
        cid = t.get("conditionId", "")
        if cid:
            cond_trades[cid].append(t)

    # Sort each condition's trades by timestamp
    for cid in cond_trades:
        cond_trades[cid].sort(key=lambda x: x["timestamp"])

    out = []
    out.append("# sovereign2013 DCA・エッジ源泉分析")
    out.append("")
    out.append(f"**分析日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    out.append(f"**データ**: TRADE {len(trades):,} / REDEEM {len(redeems):,} / MERGE {len(merges):,}")
    out.append(f"**条件数**: {len(conditions):,}")
    out.append("")

    # ===== 1. DCA Analysis =====
    out.append("---")
    out.append("## 1. DCA (分割購入) 分析")
    out.append("")

    # Count BUYs per condition
    buy_counts = {}
    for cid, tlist in cond_trades.items():
        buys = [t for t in tlist if t.get("side") == "BUY"]
        buy_counts[cid] = len(buys)

    # Distribution
    buckets = [
        ("1回 (単発)", 1, 1),
        ("2-5回", 2, 5),
        ("6-10回", 6, 10),
        ("11-50回", 11, 50),
        ("51-100回", 51, 100),
        ("101回+", 101, 999999),
    ]

    out.append("### 1.1 1条件あたりの BUY 回数分布")
    out.append("")
    out.append("| BUY回数 | 条件数 | 割合 | 平均PnL | 勝率 | 平均コスト |")
    out.append("|---------|--------|------|---------|------|-----------|")

    for label, lo, hi in buckets:
        cids_in_bucket = [cid for cid, cnt in buy_counts.items() if lo <= cnt <= hi and cid in conditions]
        if not cids_in_bucket:
            out.append(f"| {label} | 0 | 0% | - | - | - |")
            continue
        n = len(cids_in_bucket)
        pct = n / len(conditions) * 100
        pnls = [conditions[cid]["pnl"] for cid in cids_in_bucket]
        avg_pnl = mean(pnls)
        wins = sum(1 for cid in cids_in_bucket if conditions[cid]["status"] == "WIN")
        losses = sum(1 for cid in cids_in_bucket if conditions[cid]["status"] == "LOSS_OR_OPEN")
        wl = wins + losses
        wr = wins / wl * 100 if wl > 0 else 0
        avg_cost = mean([conditions[cid]["net_cost"] for cid in cids_in_bucket])
        out.append(f"| {label} | {n:,} | {pct:.1f}% | ${avg_pnl:,.2f} | {wr:.1f}% | ${avg_cost:,.2f} |")

    out.append("")

    # DCA vs single: comparison
    out.append("### 1.2 DCA条件 vs 単発条件の比較")
    out.append("")

    single_cids = [cid for cid, cnt in buy_counts.items() if cnt == 1 and cid in conditions]
    dca_cids = [cid for cid, cnt in buy_counts.items() if cnt >= 2 and cid in conditions]

    for label, cids in [("単発 (1回)", single_cids), ("DCA (2回+)", dca_cids)]:
        if not cids:
            continue
        total_cost = sum(conditions[cid]["net_cost"] for cid in cids)
        total_payout = sum(conditions[cid]["total_payout"] for cid in cids)
        total_pnl = total_payout - total_cost
        wins = sum(1 for cid in cids if conditions[cid]["status"] == "WIN")
        losses = sum(1 for cid in cids if conditions[cid]["status"] == "LOSS_OR_OPEN")
        wl = wins + losses
        wr = wins / wl * 100 if wl > 0 else 0
        roi = total_pnl / total_cost * 100 if total_cost > 0 else 0
        out.append(f"**{label}**: {len(cids):,} 条件")
        out.append(f"- コスト: ${total_cost:,.2f} / PnL: ${total_pnl:,.2f} / ROI: {roi:.2f}%")
        out.append(f"- 勝率: {wr:.1f}% ({wins:,}W / {losses:,}L)")
        out.append("")

    # DCA price drift
    out.append("### 1.3 DCA時の価格推移パターン")
    out.append("")

    price_drift_up = 0
    price_drift_down = 0
    price_drift_flat = 0
    drifts = []

    for cid in dca_cids:
        tlist = cond_trades.get(cid, [])
        buys = [t for t in tlist if t.get("side") == "BUY"]
        if len(buys) < 2:
            continue
        first_price = float(buys[0].get("price", 0))
        last_price = float(buys[-1].get("price", 0))
        if first_price <= 0:
            continue
        drift = (last_price - first_price) / first_price * 100
        drifts.append(drift)
        if drift > 1:
            price_drift_up += 1
        elif drift < -1:
            price_drift_down += 1
        else:
            price_drift_flat += 1

    total_drift = price_drift_up + price_drift_down + price_drift_flat
    if total_drift > 0:
        out.append(f"- 価格上昇 (>1%): {price_drift_up:,} ({price_drift_up/total_drift*100:.1f}%)")
        out.append(f"- 価格横ばい (±1%): {price_drift_flat:,} ({price_drift_flat/total_drift*100:.1f}%)")
        out.append(f"- 価格下落 (<-1%): {price_drift_down:,} ({price_drift_down/total_drift*100:.1f}%)")
        out.append(f"- 平均ドリフト: {mean(drifts):.2f}%")
        out.append(f"- 中央値ドリフト: {median(drifts):.2f}%")
        out.append("")

        # ナンピン vs 追い上げ
        # Check PnL for dip-buying vs chasing
        dip_cids = [cid for cid in dca_cids if cid in conditions]
        dip_buy = []
        chase_buy = []
        for cid in dip_cids:
            tlist = cond_trades.get(cid, [])
            buys = [t for t in tlist if t.get("side") == "BUY"]
            if len(buys) < 2:
                continue
            first_p = float(buys[0].get("price", 0))
            last_p = float(buys[-1].get("price", 0))
            if first_p <= 0:
                continue
            drift_val = (last_p - first_p) / first_p * 100
            if drift_val < -5:
                dip_buy.append(cid)
            elif drift_val > 5:
                chase_buy.append(cid)

        out.append("**ナンピン (価格5%以上下落) vs 追い上げ (価格5%以上上昇)**")
        out.append("")
        for label, cids_sub in [("ナンピン (<-5%)", dip_buy), ("追い上げ (>+5%)", chase_buy)]:
            if not cids_sub:
                out.append(f"- {label}: 0 条件")
                continue
            pnl_total = sum(conditions[cid]["pnl"] for cid in cids_sub)
            cost_total = sum(conditions[cid]["net_cost"] for cid in cids_sub)
            wins = sum(1 for cid in cids_sub if conditions[cid]["status"] == "WIN")
            losses = sum(1 for cid in cids_sub if conditions[cid]["status"] == "LOSS_OR_OPEN")
            wl = wins + losses
            wr = wins / wl * 100 if wl > 0 else 0
            roi = pnl_total / cost_total * 100 if cost_total > 0 else 0
            out.append(f"- {label}: {len(cids_sub):,} 条件 / PnL ${pnl_total:,.2f} / ROI {roi:.1f}% / 勝率 {wr:.1f}%")
        out.append("")

    # ===== 2. SELL Analysis =====
    out.append("---")
    out.append("## 2. SELL (途中売却) 分析")
    out.append("")

    # Conditions with SELLs
    conds_with_sell = []
    conds_without_sell = []
    for cid in conditions:
        tlist = cond_trades.get(cid, [])
        sells = [t for t in tlist if t.get("side") == "SELL"]
        if sells:
            conds_with_sell.append(cid)
        else:
            conds_without_sell.append(cid)

    out.append(f"### 2.1 SELL がある条件の割合")
    out.append("")
    out.append(f"- SELL あり: {len(conds_with_sell):,} ({len(conds_with_sell)/len(conditions)*100:.1f}%)")
    out.append(f"- SELL なし: {len(conds_without_sell):,} ({len(conds_without_sell)/len(conditions)*100:.1f}%)")
    out.append("")

    # SELL timing: hours after first BUY
    out.append("### 2.2 SELL タイミング (最初のBUY後の経過時間)")
    out.append("")
    sell_delays = []
    for cid in conds_with_sell:
        tlist = cond_trades.get(cid, [])
        buys = [t for t in tlist if t.get("side") == "BUY"]
        sells = [t for t in tlist if t.get("side") == "SELL"]
        if buys and sells:
            first_buy_ts = buys[0]["timestamp"]
            first_sell_ts = sells[0]["timestamp"]
            delay_hours = (first_sell_ts - first_buy_ts) / 3600
            if delay_hours >= 0:
                sell_delays.append(delay_hours)

    if sell_delays:
        delay_buckets = [
            ("<1h", 0, 1),
            ("1-6h", 1, 6),
            ("6-24h", 6, 24),
            ("1-3日", 24, 72),
            ("3-7日", 72, 168),
            ("7日+", 168, 999999),
        ]
        out.append("| 経過時間 | 件数 | 割合 |")
        out.append("|----------|------|------|")
        for label, lo, hi in delay_buckets:
            cnt = sum(1 for d in sell_delays if lo <= d < hi)
            pct = cnt / len(sell_delays) * 100
            out.append(f"| {label} | {cnt:,} | {pct:.1f}% |")
        out.append("")
        out.append(f"- 平均: {mean(sell_delays):.1f}h / 中央値: {median(sell_delays):.1f}h")
        out.append("")

    # SELL vs no-SELL PnL comparison
    out.append("### 2.3 SELL あり vs なし の PnL 比較")
    out.append("")
    for label, cids in [("SELL あり", conds_with_sell), ("SELL なし", conds_without_sell)]:
        if not cids:
            continue
        total_cost = sum(conditions[cid]["net_cost"] for cid in cids)
        total_payout = sum(conditions[cid]["total_payout"] for cid in cids)
        total_pnl = total_payout - total_cost
        wins = sum(1 for cid in cids if conditions[cid]["status"] == "WIN")
        losses = sum(1 for cid in cids if conditions[cid]["status"] == "LOSS_OR_OPEN")
        wl = wins + losses
        wr = wins / wl * 100 if wl > 0 else 0
        roi = total_pnl / total_cost * 100 if total_cost > 0 else 0
        sell_rev = sum(conditions[cid]["sell_proceeds"] for cid in cids)
        out.append(f"**{label}**: {len(cids):,} 条件")
        out.append(f"- BUYコスト: ${sum(conditions[cid]['buy_cost'] for cid in cids):,.2f}")
        out.append(f"- SELL収入: ${sell_rev:,.2f}")
        out.append(f"- 純コスト: ${total_cost:,.2f} / PnL: ${total_pnl:,.2f} / ROI: {roi:.2f}%")
        out.append(f"- 勝率: {wr:.1f}% ({wins:,}W / {losses:,}L)")
        out.append("")

    # SELL ratio analysis: how much of position is sold
    out.append("### 2.4 SELL比率分析 (売却量/購入量)")
    out.append("")
    sell_ratios = []
    for cid in conds_with_sell:
        c = conditions[cid]
        if c["buy_shares"] > 0:
            ratio = c["sell_shares"] / c["buy_shares"]
            sell_ratios.append((cid, ratio))

    if sell_ratios:
        # Partial vs full sell
        partial = [(cid, r) for cid, r in sell_ratios if r < 0.9]
        full = [(cid, r) for cid, r in sell_ratios if r >= 0.9]
        out.append(f"- 部分売却 (<90%): {len(partial):,} 条件")
        out.append(f"- 全量売却 (>=90%): {len(full):,} 条件")
        out.append(f"- 平均売却比率: {mean([r for _, r in sell_ratios])*100:.1f}%")
        out.append("")

        # PnL by sell type
        for label, cid_list in [("部分売却", [cid for cid, _ in partial]), ("全量売却", [cid for cid, _ in full])]:
            if not cid_list:
                continue
            pnl_total = sum(conditions[cid]["pnl"] for cid in cid_list)
            cost_total = sum(conditions[cid]["net_cost"] for cid in cid_list)
            roi = pnl_total / cost_total * 100 if cost_total > 0 else 0
            out.append(f"  - {label}: PnL ${pnl_total:,.2f} / ROI {roi:.1f}%")
        out.append("")

    # ===== 3. Edge Concentration =====
    out.append("---")
    out.append("## 3. エッジの集中度")
    out.append("")

    # Sort conditions by PnL
    sorted_conds = sorted(conditions.values(), key=lambda c: c["pnl"], reverse=True)
    total_pnl = sum(c["pnl"] for c in sorted_conds)

    n_conds = len(sorted_conds)
    top_10pct = sorted_conds[:max(1, n_conds // 10)]
    top_10pct_pnl = sum(c["pnl"] for c in top_10pct)
    bottom_10pct = sorted_conds[-(n_conds // 10):]
    bottom_10pct_pnl = sum(c["pnl"] for c in bottom_10pct)

    out.append(f"### 3.1 PnL集中度")
    out.append("")
    out.append(f"- 全条件: {n_conds:,} / 合計PnL: ${total_pnl:,.2f}")
    out.append(f"- **上位10% ({len(top_10pct):,}条件) のPnL: ${top_10pct_pnl:,.2f}** ({top_10pct_pnl/total_pnl*100:.1f}% of total)" if total_pnl != 0 else f"- 上位10%: ${top_10pct_pnl:,.2f}")
    out.append(f"- **下位10% ({len(bottom_10pct):,}条件) のPnL: ${bottom_10pct_pnl:,.2f}**")
    out.append("")

    # Percentile breakdown
    pcts = [5, 10, 20, 50]
    out.append("| 上位N% | 条件数 | PnL合計 | 全体PnL比 |")
    out.append("|--------|--------|---------|-----------|")
    for p in pcts:
        n = max(1, n_conds * p // 100)
        pnl_sum = sum(c["pnl"] for c in sorted_conds[:n])
        ratio = pnl_sum / total_pnl * 100 if total_pnl != 0 else 0
        out.append(f"| {p}% | {n:,} | ${pnl_sum:,.2f} | {ratio:.1f}% |")
    out.append("")

    # Top 10 conditions
    out.append("### 3.2 PnL上位10条件")
    out.append("")
    out.append("| # | Slug | Sport | 価格帯 | コスト | Payout | PnL | ROI |")
    out.append("|---|------|-------|--------|--------|--------|-----|-----|")
    for i, c in enumerate(sorted_conds[:10], 1):
        sport = c["sport"]
        avg_p = c["avg_buy_price"]
        band = f"{avg_p:.2f}"
        out.append(
            f"| {i} | {c['slug'][:50]} | {sport} | {band} | "
            f"${c['net_cost']:,.2f} | ${c['total_payout']:,.2f} | "
            f"${c['pnl']:,.2f} | {c['roi_pct']:.0f}% |"
        )
    out.append("")

    # Bottom 10 conditions
    out.append("### 3.3 PnL下位10条件 (最大損失)")
    out.append("")
    out.append("| # | Slug | Sport | 価格帯 | コスト | Payout | PnL | ROI |")
    out.append("|---|------|-------|--------|--------|--------|-----|-----|")
    for i, c in enumerate(sorted_conds[-10:], 1):
        sport = c["sport"]
        avg_p = c["avg_buy_price"]
        band = f"{avg_p:.2f}"
        out.append(
            f"| {i} | {c['slug'][:50]} | {sport} | {band} | "
            f"${c['net_cost']:,.2f} | ${c['total_payout']:,.2f} | "
            f"${c['pnl']:,.2f} | {c['roi_pct']:.0f}% |"
        )
    out.append("")

    # ===== 4. Both-Side Bets =====
    out.append("---")
    out.append("## 4. 両サイドベット分析")
    out.append("")

    # Group conditions by eventSlug
    event_conditions = defaultdict(list)
    for cid, c in conditions.items():
        es = c.get("eventSlug", "")
        if es:
            event_conditions[es].append(cid)

    multi_cond_events = {es: cids for es, cids in event_conditions.items() if len(cids) >= 2}
    single_cond_events = {es: cids for es, cids in event_conditions.items() if len(cids) == 1}

    out.append(f"### 4.1 概要")
    out.append("")
    out.append(f"- 単一条件イベント: {len(single_cond_events):,}")
    out.append(f"- 複数条件イベント (2+): {len(multi_cond_events):,}")
    out.append("")

    # Analyze multi-condition events
    if multi_cond_events:
        # Check if they bet on both sides (different outcomes in same event)
        both_side_count = 0
        both_side_events = []
        for es, cids in multi_cond_events.items():
            outcomes = set()
            for cid in cids:
                c = conditions[cid]
                outcomes.add(c.get("outcome_bought", ""))
            if len(outcomes) >= 2 and "" not in outcomes:
                both_side_count += 1
                total_pnl_ev = sum(conditions[cid]["pnl"] for cid in cids)
                total_cost_ev = sum(conditions[cid]["net_cost"] for cid in cids)
                both_side_events.append((es, cids, total_pnl_ev, total_cost_ev))

        out.append(f"### 4.2 両サイドベット (同一イベントで異なるアウトカム)")
        out.append("")
        out.append(f"- 両サイドベットイベント数: {both_side_count:,}")
        out.append(f"- 全複数条件イベント中の割合: {both_side_count/len(multi_cond_events)*100:.1f}%")
        out.append("")

        if both_side_events:
            both_pnl_total = sum(p for _, _, p, _ in both_side_events)
            both_cost_total = sum(c for _, _, _, c in both_side_events)
            both_roi = both_pnl_total / both_cost_total * 100 if both_cost_total > 0 else 0
            out.append(f"**両サイドベット合計**:")
            out.append(f"- コスト: ${both_cost_total:,.2f} / PnL: ${both_pnl_total:,.2f} / ROI: {both_roi:.1f}%")
            out.append("")

            # Classify: hedging vs speculating
            profitable = [(es, cids, p, c) for es, cids, p, c in both_side_events if p > 0]
            unprofitable = [(es, cids, p, c) for es, cids, p, c in both_side_events if p <= 0]
            out.append(f"- 利益: {len(profitable):,} イベント (${sum(p for _, _, p, _ in profitable):,.2f})")
            out.append(f"- 損失: {len(unprofitable):,} イベント (${sum(p for _, _, p, _ in unprofitable):,.2f})")
            out.append("")

            # Top 5 both-side events by PnL
            both_side_events.sort(key=lambda x: x[2], reverse=True)
            out.append("**両サイドベット上位5件**:")
            out.append("")
            out.append("| # | Event | 条件数 | コスト | PnL |")
            out.append("|---|-------|--------|--------|-----|")
            for i, (es, cids, pnl, cost) in enumerate(both_side_events[:5], 1):
                out.append(f"| {i} | {es[:50]} | {len(cids)} | ${cost:,.2f} | ${pnl:,.2f} |")
            out.append("")

        # Multi-condition same side (e.g., spread + moneyline)
        same_side_multi = len(multi_cond_events) - both_side_count
        out.append(f"### 4.3 同一イベント・同一サイド複数条件 (スプレッド+マネーライン等)")
        out.append("")
        out.append(f"- 件数: {same_side_multi:,}")

        # Breakdown by market type combo
        mt_combos = defaultdict(int)
        for es, cids in multi_cond_events.items():
            outcomes = set()
            for cid in cids:
                outcomes.add(conditions[cid].get("outcome_bought", ""))
            if len(outcomes) < 2 or "" in outcomes:
                mts = sorted(set(conditions[cid]["market_type"] for cid in cids))
                mt_combos[" + ".join(mts)] += 1

        if mt_combos:
            out.append("")
            out.append("| マーケットタイプ組合せ | 件数 |")
            out.append("|----------------------|------|")
            for combo, cnt in sorted(mt_combos.items(), key=lambda x: -x[1])[:10]:
                out.append(f"| {combo} | {cnt:,} |")
        out.append("")

    # ===== 5. Price Band Deep Dive =====
    out.append("---")
    out.append("## 5. 価格帯別エッジ分析 (DCA加味)")
    out.append("")

    price_bands = [
        ("0.01-0.10", 0.01, 0.10),
        ("0.10-0.20", 0.10, 0.20),
        ("0.20-0.30", 0.20, 0.30),
        ("0.30-0.40", 0.30, 0.40),
        ("0.40-0.50", 0.40, 0.50),
        ("0.50-0.60", 0.50, 0.60),
        ("0.60-0.70", 0.60, 0.70),
        ("0.70-0.80", 0.70, 0.80),
        ("0.80-0.90", 0.80, 0.90),
        ("0.90-1.00", 0.90, 1.00),
    ]

    out.append("| 価格帯 | 条件数 | 勝率 | 平均BUY回数 | 純コスト | PnL | ROI |")
    out.append("|--------|--------|------|------------|----------|-----|-----|")

    for label, lo, hi in price_bands:
        cids_in_band = []
        for cid, c in conditions.items():
            avg_p = c["avg_buy_price"]
            if lo <= avg_p < hi or (hi == 1.0 and avg_p == 1.0):
                cids_in_band.append(cid)
        if not cids_in_band:
            continue
        n = len(cids_in_band)
        wins = sum(1 for cid in cids_in_band if conditions[cid]["status"] == "WIN")
        losses = sum(1 for cid in cids_in_band if conditions[cid]["status"] == "LOSS_OR_OPEN")
        wl = wins + losses
        wr = wins / wl * 100 if wl > 0 else 0
        avg_buys = mean([buy_counts.get(cid, 0) for cid in cids_in_band])
        total_cost = sum(conditions[cid]["net_cost"] for cid in cids_in_band)
        total_pnl_band = sum(conditions[cid]["pnl"] for cid in cids_in_band)
        roi = total_pnl_band / total_cost * 100 if total_cost > 0 else 0
        out.append(f"| {label} | {n:,} | {wr:.1f}% | {avg_buys:.1f} | ${total_cost:,.0f} | ${total_pnl_band:,.0f} | {roi:.1f}% |")
    out.append("")

    # ===== 6. Summary for nbabot strategy =====
    out.append("---")
    out.append("## 6. nbabot 戦略への示唆")
    out.append("")

    # Calculate key metrics for summary
    dca_pnl = sum(conditions[cid]["pnl"] for cid in dca_cids) if dca_cids else 0
    single_pnl = sum(conditions[cid]["pnl"] for cid in single_cids) if single_cids else 0
    dca_cost = sum(conditions[cid]["net_cost"] for cid in dca_cids) if dca_cids else 0
    single_cost = sum(conditions[cid]["net_cost"] for cid in single_cids) if single_cids else 0
    dca_roi = dca_pnl / dca_cost * 100 if dca_cost > 0 else 0
    single_roi = single_pnl / single_cost * 100 if single_cost > 0 else 0

    out.append(f"1. **DCA効果**: DCA (ROI {dca_roi:.1f}%) vs 単発 (ROI {single_roi:.1f}%)")
    out.append(f"2. **SELL活用**: SELL がある条件は {len(conds_with_sell)/len(conditions)*100:.1f}% — 損切り/利確の判断材料")
    out.append(f"3. **エッジ集中度**: 上位10%の条件が全体PnLの {top_10pct_pnl/total_pnl*100:.0f}% を占める — テール利益が重要" if total_pnl != 0 else "3. エッジ集中度: データ不十分")
    out.append(f"4. **両サイドベット**: {both_side_count:,} イベントで両サイドに賭けている — ヘッジまたはアービトラージの可能性")
    out.append("")

    # Write report
    report_text = "\n".join(out)
    with open(OUT, "w") as f:
        f.write(report_text)
    print(f"\nReport written to: {OUT}")
    print(f"Report length: {len(report_text)} chars")


if __name__ == "__main__":
    main()
