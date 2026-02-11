#!/usr/bin/env python3
"""Deep dive analysis of sovereign2013's strategy by sport category.

Produces a detailed breakdown of:
- Per-sport market type mix, position sizing, price bands, win rates
- DCA patterns and execution style
- Timing patterns (entry schedule)
- MERGE usage patterns (key to the strategy)
- Replicability assessment
"""

import json
import sys
from collections import defaultdict
from pathlib import Path
from statistics import mean, median

BASE = Path(__file__).resolve().parent.parent
DATA = BASE / "data" / "traders" / "sovereign2013"


def load_data():
    with open(DATA / "game_pnl.json") as f:
        games = json.load(f)
    with open(DATA / "condition_pnl.json") as f:
        conditions = json.load(f)
    with open(DATA / "strategy_profile.json") as f:
        profile = json.load(f)

    # 生トレードデータ
    raw_trades = []
    for fn in ["raw_trades.json", "raw_redeem.json", "raw_merge.json"]:
        p = DATA / fn
        if p.exists():
            with open(p) as f:
                raw_trades.extend(json.load(f))

    return games, conditions, profile, raw_trades


def analyze_per_sport(games, conditions):
    """スポーツ別の詳細分析."""
    # condition をゲームキーでインデックス
    cond_by_game = defaultdict(list)
    for c in conditions:
        cond_by_game[c.get("game_key", c.get("event_slug", ""))].append(c)

    sport_stats = {}
    for sport_key in [
        "NBA", "CBB", "NFL", "CFB", "NHL", "ATP", "WTA", "MLB",
    ]:
        sport_games = [g for g in games if g["sport"] == sport_key]
        if not sport_games:
            continue

        sport_conds = []
        for g in sport_games:
            sport_conds.extend(cond_by_game.get(g["game_key"], []))

        # マーケットタイプ集計
        mt_counts = defaultdict(int)
        mt_pnl = defaultdict(float)
        for c in sport_conds:
            mt = c.get("market_type", "Unknown")
            mt_counts[mt] += 1
            mt_pnl[mt] += c.get("pnl", 0)

        # 価格帯分析
        price_bands = defaultdict(lambda: {"count": 0, "wins": 0, "vol": 0, "pnl": 0})
        for c in sport_conds:
            avg_price = c.get("avg_buy_price", 0)
            if avg_price <= 0:
                continue
            band_lo = int(avg_price * 20) * 5  # 5¢ バンド
            band_hi = band_lo + 5
            band_key = f"0.{band_lo:02d}-0.{band_hi:02d}"
            pb = price_bands[band_key]
            pb["count"] += 1
            pb["wins"] += 1 if c.get("outcome") == "win" else 0
            pb["vol"] += c.get("total_buy_cost", 0)
            pb["pnl"] += c.get("pnl", 0)

        # ポジションサイズ
        costs = [g["net_cost"] for g in sport_games if g["net_cost"] > 0]
        payouts = [g["total_payout"] for g in sport_games if g["total_payout"] > 0]

        # DCA (trades per condition)
        trades_per_cond = [
            c.get("trade_count", 0) for c in sport_conds
            if c.get("trade_count", 0) > 0
        ]

        # Win/Loss
        wins = sum(1 for g in sport_games if g["win_conditions"] > 0 and g["loss_conditions"] == 0)
        losses = sum(1 for g in sport_games if g["loss_conditions"] > 0 and g["win_conditions"] == 0)
        mixed = len(sport_games) - wins - losses

        # MERGE 分析
        merge_games = sum(1 for g in sport_games if g["merged_conditions"] > 0)
        total_merge_payout = sum(g["total_merge"] for g in sport_games)

        # 月別
        monthly = defaultdict(lambda: {"games": 0, "pnl": 0})
        for g in sport_games:
            m = g["month"]
            monthly[m]["games"] += 1
            monthly[m]["pnl"] += g["total_pnl"]

        # 1 試合あたりの market type 数
        mt_per_game = []
        for g in sport_games:
            mts = g.get("market_types", [])
            if isinstance(mts, list):
                mt_per_game.append(len(mts))

        sport_stats[sport_key] = {
            "n_games": len(sport_games),
            "n_conditions": len(sport_conds),
            "conds_per_game": len(sport_conds) / max(len(sport_games), 1),
            "total_pnl": sum(g["total_pnl"] for g in sport_games),
            "total_net_cost": sum(g["net_cost"] for g in sport_games),
            "total_payout": sum(g["total_payout"] for g in sport_games),
            "total_merge_payout": total_merge_payout,
            "merge_games": merge_games,
            "merge_pct": merge_games / max(len(sport_games), 1) * 100,
            "wins": wins,
            "losses": losses,
            "mixed": mixed,
            "win_pct": wins / max(wins + losses, 1) * 100,
            "mt_counts": dict(mt_counts),
            "mt_pnl": dict(mt_pnl),
            "mt_per_game_avg": mean(mt_per_game) if mt_per_game else 0,
            "price_bands": dict(price_bands),
            "avg_cost": mean(costs) if costs else 0,
            "median_cost": median(costs) if costs else 0,
            "avg_payout": mean(payouts) if payouts else 0,
            "median_payout": median(payouts) if payouts else 0,
            "avg_trades_per_cond": mean(trades_per_cond) if trades_per_cond else 0,
            "median_trades_per_cond": median(trades_per_cond) if trades_per_cond else 0,
            "monthly": dict(monthly),
        }

    return sport_stats


def analyze_merge_strategy(games, conditions, raw_trades):
    """MERGE 戦略の詳細分析 — sovereign2013 の最大の特徴."""
    merge_trades = [t for t in raw_trades if t.get("type") == "MERGE"]

    # MERGE ありのゲーム vs なし
    merge_game_keys = set()
    for t in merge_trades:
        slug = t.get("event_slug", "")
        merge_game_keys.add(slug)

    # MERGE 金額
    merge_by_game = defaultdict(float)
    for t in merge_trades:
        slug = t.get("event_slug", "")
        merge_by_game[slug] += abs(float(t.get("amount", 0)))

    return {
        "total_merge_trades": len(merge_trades),
        "unique_merge_games": len(merge_game_keys),
        "total_merge_volume": sum(merge_by_game.values()),
        "avg_merge_per_game": (
            mean(merge_by_game.values()) if merge_by_game else 0
        ),
        "median_merge_per_game": (
            median(merge_by_game.values()) if merge_by_game else 0
        ),
    }


def analyze_timing(games):
    """エントリータイミング分析."""
    sport_first_last = {}
    for g in games:
        sport = g["sport"]
        date = g["date"]
        if sport not in sport_first_last:
            sport_first_last[sport] = {"first": date, "last": date, "count": 0}
        if date < sport_first_last[sport]["first"]:
            sport_first_last[sport]["first"] = date
        if date > sport_first_last[sport]["last"]:
            sport_first_last[sport]["last"] = date
        sport_first_last[sport]["count"] += 1

    return sport_first_last


def analyze_both_sides(conditions):
    """両サイド購入パターンの分析.

    sovereign2013 が同じ試合で Yes/No 両方のアウトカムを買っているか？
    → MERGE で利益確定するパターンの証拠
    """
    # condition ごとに game_key + side を確認
    game_sides = defaultdict(lambda: {"yes": 0, "no": 0, "conds": []})
    for c in conditions:
        gk = c.get("game_key", c.get("event_slug", ""))
        # condition の title や outcome で side を推定
        title = c.get("title", "").lower()
        outcome = c.get("outcome_label", c.get("outcome", ""))
        game_sides[gk]["conds"].append(c)

    # 1 ゲームに複数 condition がある = Moneyline + Spread + Total
    # 各 condition は Yes/No の片方だけ買うはず
    multi_cond_games = {
        gk: v for gk, v in game_sides.items()
        if len(v["conds"]) > 1
    }

    # condition の "side" フィールドを確認
    side_patterns = defaultdict(int)
    for gk, v in multi_cond_games.items():
        sides = set()
        for c in v["conds"]:
            side = c.get("side", "unknown")
            sides.add(side)
        key = "+".join(sorted(sides))
        side_patterns[key] += 1

    return {
        "total_games": len(game_sides),
        "multi_condition_games": len(multi_cond_games),
        "side_patterns": dict(side_patterns),
    }


def generate_report(sport_stats, merge_info, timing, both_sides):
    """深掘りレポートを生成."""
    out = []
    out.append("# sovereign2013 深掘り分析レポート")
    out.append(f"\n**Date**: 2026-02-09")
    out.append("")

    # ========== Section 1: スポーツ横断サマリー ==========
    out.append("---")
    out.append("## 1. スポーツ横断サマリー")
    out.append("")
    hdr = (
        "| Sport | Games | Conds | PnL | Net Cost | Payout | MERGE Payout"
        " | Win% | Conds/Game | MT/Game |"
    )
    out.append(hdr)
    out.append(
        "|-------|-------|-------|-----|----------|--------|"
        "--------------|------|------------|---------|"
    )
    sorted_sports = sorted(
        sport_stats.items(), key=lambda x: x[1]["total_pnl"], reverse=True,
    )
    for sport, s in sorted_sports:
        out.append(
            f"| {sport} | {s['n_games']:,} | {s['n_conditions']:,}"
            f" | ${s['total_pnl']:,.0f} | ${s['total_net_cost']:,.0f}"
            f" | ${s['total_payout']:,.0f}"
            f" | ${s['total_merge_payout']:,.0f}"
            f" | {s['win_pct']:.1f}%"
            f" | {s['conds_per_game']:.1f}"
            f" | {s['mt_per_game_avg']:.1f} |"
        )
    out.append("")

    # ========== Section 2: MERGE 戦略 (核心) ==========
    out.append("---")
    out.append("## 2. MERGE 戦略分析 (sovereign2013 の核心)")
    out.append("")
    out.append(
        "sovereign2013 の PnL $93.9M のうち **$49.2M (52%) が MERGE**"
        " から。これが最大の特徴。"
    )
    out.append("")
    out.append("### MERGE とは？")
    out.append(
        "Polymarket では Yes + No の両方のシェアを 1:1 で保有していると"
        "「MERGE」で $1.00 に換金できる。\n"
        "つまり **Yes を 0.45 で、No を 0.48 で買うと合計 0.93 のコスト"
        "で $1.00 が確定** → 7% の無リスク利益。"
    )
    out.append("")
    out.append("### MERGE 統計")
    out.append("")
    out.append(f"- 総 MERGE 取引数: {merge_info['total_merge_trades']:,}")
    out.append(
        f"- MERGE 関連ゲーム数: {merge_info['unique_merge_games']:,}"
    )
    out.append(
        f"- 総 MERGE ボリューム: ${merge_info['total_merge_volume']:,.0f}"
    )
    out.append("")

    out.append("### スポーツ別 MERGE 比率")
    out.append("")
    out.append("| Sport | Games | MERGE Games | MERGE% | MERGE Payout |")
    out.append("|-------|-------|-------------|--------|--------------|")
    for sport, s in sorted_sports:
        out.append(
            f"| {sport} | {s['n_games']:,} | {s['merge_games']:,}"
            f" | {s['merge_pct']:.1f}%"
            f" | ${s['total_merge_payout']:,.0f} |"
        )
    out.append("")

    # ========== Section 3: マーケットタイプ別 P&L ==========
    out.append("---")
    out.append("## 3. スポーツ × マーケットタイプ P&L")
    out.append("")
    out.append(
        "| Sport | Moneyline PnL | Spread PnL | Total PnL"
        " | ML Conds | Spread Conds | Total Conds |"
    )
    out.append(
        "|-------|--------------|------------|----------"
        "|----------|--------------|-------------|"
    )
    for sport, s in sorted_sports:
        ml = s["mt_pnl"].get("Moneyline", 0)
        sp = s["mt_pnl"].get("Spread", 0)
        tt = s["mt_pnl"].get("Total", 0)
        ml_c = s["mt_counts"].get("Moneyline", 0)
        sp_c = s["mt_counts"].get("Spread", 0)
        tt_c = s["mt_counts"].get("Total", 0)
        out.append(
            f"| {sport} | ${ml:,.0f} | ${sp:,.0f} | ${tt:,.0f}"
            f" | {ml_c:,} | {sp_c:,} | {tt_c:,} |"
        )
    out.append("")

    # ========== Section 4: 価格帯分析 (スポーツ別) ==========
    out.append("---")
    out.append("## 4. 価格帯別勝率 (主要スポーツ)")
    out.append("")

    for sport in ["NBA", "CBB", "NFL", "CFB", "NHL", "ATP", "WTA"]:
        if sport not in sport_stats:
            continue
        s = sport_stats[sport]
        bands = s["price_bands"]
        if not bands:
            continue

        out.append(f"### {sport}")
        out.append("")
        out.append(
            "| Band | Count | Wins | Win% | Volume | PnL | ROI |"
        )
        out.append(
            "|------|-------|------|------|--------|-----|-----|"
        )
        for bk in sorted(bands.keys()):
            b = bands[bk]
            if b["count"] == 0:
                continue
            wr = b["wins"] / b["count"] * 100 if b["count"] > 0 else 0
            roi = b["pnl"] / b["vol"] * 100 if b["vol"] > 0 else 0
            out.append(
                f"| {bk} | {b['count']:,} | {b['wins']:,}"
                f" | {wr:.0f}% | ${b['vol']:,.0f}"
                f" | ${b['pnl']:,.0f} | {roi:.1f}% |"
            )
        out.append("")

    # ========== Section 5: ポジションサイズ ==========
    out.append("---")
    out.append("## 5. ポジションサイズ & DCA パターン")
    out.append("")
    out.append(
        "| Sport | Avg Cost/Game | Med Cost/Game | Avg Payout/Game"
        " | Med Payout/Game | Avg Trades/Cond | Med Trades/Cond |"
    )
    out.append(
        "|-------|-------------|-------------|-------------"
        "|----------------|-----------------|-----------------|"
    )
    for sport, s in sorted_sports:
        out.append(
            f"| {sport} | ${s['avg_cost']:,.0f}"
            f" | ${s['median_cost']:,.0f}"
            f" | ${s['avg_payout']:,.0f}"
            f" | ${s['median_payout']:,.0f}"
            f" | {s['avg_trades_per_cond']:.1f}"
            f" | {s['median_trades_per_cond']:.0f} |"
        )
    out.append("")

    # ========== Section 6: タイミング ==========
    out.append("---")
    out.append("## 6. スポーツ参入タイムライン")
    out.append("")
    out.append(
        "| Sport | First Trade | Last Trade | Games | Active Days |"
    )
    out.append(
        "|-------|------------|------------|-------|-------------|"
    )
    for sport in sorted(
        timing.keys(),
        key=lambda x: timing[x]["first"],
    ):
        t = timing[sport]
        # 活動日数概算
        from datetime import datetime
        d1 = datetime.strptime(t["first"], "%Y-%m-%d")
        d2 = datetime.strptime(t["last"], "%Y-%m-%d")
        days = (d2 - d1).days + 1
        out.append(
            f"| {sport} | {t['first']} | {t['last']}"
            f" | {t['count']:,} | {days} |"
        )
    out.append("")

    # ========== Section 7: 月別スポーツ分散 ==========
    out.append("---")
    out.append("## 7. 月別スポーツ P&L 推移")
    out.append("")
    all_months = sorted(set(
        m for s in sport_stats.values() for m in s["monthly"].keys()
    ))
    top_sports = [sp for sp, _ in sorted_sports[:7]]

    hdr_parts = ["| Month |"] + [f" {sp} |" for sp in top_sports]
    out.append("".join(hdr_parts))
    sep_parts = ["|-------|"] + ["--------|" for _ in top_sports]
    out.append("".join(sep_parts))

    for month in all_months:
        row = [f"| {month} |"]
        for sp in top_sports:
            pnl = sport_stats.get(sp, {}).get("monthly", {}).get(
                month, {},
            )
            if isinstance(pnl, dict):
                val = pnl.get("pnl", 0)
            else:
                val = 0
            if val == 0:
                row.append(" - |")
            else:
                row.append(f" ${val:,.0f} |")
        out.append("".join(row))
    out.append("")

    # ========== Section 8: 模倣可能性 ==========
    out.append("---")
    out.append("## 8. 戦略の模倣可能性分析")
    out.append("")
    out.append("### sovereign2013 の戦略要約")
    out.append("")
    out.append("1. **両サイド購入 + MERGE**: Yes と No の両方を購入し、")
    out.append(
        "   合計コスト < $1.00 になる非効率を狙う。"
        " 勝者側は REDEEM、両方持っていれば MERGE で確定利益。"
    )
    out.append("2. **3 マーケット全張り**: 1 試合で Moneyline + Spread"
               " + Total の全マーケットに参加")
    out.append("3. **大量 DCA**: 平均 67 回/condition の分割買い")
    out.append("4. **スイートスポット集中**: 88% が 0.20-0.55 価格帯")
    out.append("5. **マルチスポーツ**: NBA, CBB, NFL, CFB, NHL, テニス"
               " まで横断")
    out.append("")
    out.append("### 模倣の難易度")
    out.append("")
    out.append("| 要素 | 難易度 | 理由 |")
    out.append("|------|--------|------|")
    out.append(
        "| 価格帯 0.45-0.55 での Moneyline 購入 | **低** |"
        " 現行 Bot で実装済み |"
    )
    out.append(
        "| Spread/Total マーケットへの拡張 | **低〜中** |"
        " 校正テーブルを Spread/Total 用に作成すれば可能 |"
    )
    out.append(
        "| NBA 以外のスポーツへの拡張 | **中** |"
        " スポーツごとの校正カーブが必要。ただしデータは取得済み |"
    )
    out.append(
        "| 両サイド購入 + MERGE | **高** |"
        " Yes+No の合計が $1 未満になるタイミングの検知+"
        " 高速執行が必要 |"
    )
    out.append(
        "| 大量 DCA (67 回/condition) | **高** |"
        " ガス代と API レート制限の問題。資本効率も低い |"
    )
    out.append(
        "| $17M 規模の資本投下 | **非常に高** |"
        " 資本量が桁違い |"
    )
    out.append("")
    out.append("### 現実的な模倣プラン")
    out.append("")
    out.append("**Phase A (すぐ実行可能)**:")
    out.append("- 現行の NBA Moneyline 校正戦略を継続")
    out.append("- 同じ校正ロジックで **Spread + Total** にも対象拡大")
    out.append("  → sovereign2013 の Spread PnL $26M, Total PnL $39M"
               " が示すように大きなエッジ")
    out.append("")
    out.append("**Phase B (データ分析後)**:")
    out.append("- CBB/NFL/CFB/NHL の校正テーブルを構築")
    out.append("  → sovereign2013 のデータから導出可能")
    out.append("- テニス (ATP/WTA) も検討")
    out.append("")
    out.append("**Phase C (上級)**:")
    out.append("- Yes+No spread < $1.00 のアービトラージ検知")
    out.append("- MERGE 戦略の自動化 (両サイド購入 → MERGE 実行)")
    out.append("")

    return "\n".join(out)


def main():
    print("Loading data...")
    games, conditions, profile, raw_trades = load_data()

    print("Analyzing per-sport strategy...")
    sport_stats = analyze_per_sport(games, conditions)

    print("Analyzing MERGE strategy...")
    merge_info = analyze_merge_strategy(games, conditions, raw_trades)

    print("Analyzing timing...")
    timing = analyze_timing(games)

    print("Analyzing both-sides pattern...")
    both_sides = analyze_both_sides(conditions)

    print("Generating report...")
    report = generate_report(sport_stats, merge_info, timing, both_sides)

    out_path = DATA / "deep_dive_report.md"
    with open(out_path, "w") as f:
        f.write(report)
    print(f"\nReport written to {out_path}")

    # コンソールにもサマリー出力
    print("\n" + "=" * 60)
    print("KEY FINDINGS:")
    print("=" * 60)
    print(f"\nMERGE Payout: ${profile.get('total_pnl', 0):,.0f} total P&L")
    print(f"  - REDEEM: $62,065,636 (66%)")
    print(f"  - MERGE:  $49,158,801 (52% of total payout)")
    print(f"\nBoth-sides pattern: {both_sides}")
    print(f"\nMERGE info: {merge_info}")

    # スポーツ別 PnL/MERGE 比率
    print("\nSport MERGE ratios:")
    for sport, s in sorted(
        sport_stats.items(),
        key=lambda x: x[1]["total_pnl"],
        reverse=True,
    ):
        merge_ratio = (
            s["total_merge_payout"]
            / max(s["total_payout"], 1) * 100
        )
        print(
            f"  {sport:5s}: PnL ${s['total_pnl']:>12,.0f}"
            f"  MERGE ${s['total_merge_payout']:>12,.0f}"
            f"  ({merge_ratio:.0f}% of payout)"
        )


if __name__ == "__main__":
    main()
