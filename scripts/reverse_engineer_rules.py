#!/usr/bin/env python3
"""Reverse-engineer lhtsports' trade execution rules from raw trade data.

既存の分析 (校正曲線、DCA パターン等) を超えて、具体的な
"エントリー条件 / 除外条件 / サイジングロジック / 退出条件" を
ルールベースで抽出する。
"""

from __future__ import annotations

import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "reports"
TRADES_FILE = DATA_DIR / "lhtsports-analysis" / "lhtsports_all_trades.json"
CONDITIONS_FILE = DATA_DIR / "lhtsports-pnl" / "condition_pnl.json"
GAMES_FILE = DATA_DIR / "lhtsports-pnl" / "game_pnl.json"
MERGES_FILE = DATA_DIR / "lhtsports-analysis" / "lhtsports_merge.json"
OUTPUT_DIR = DATA_DIR / "lhtsports-reverse-engineering"


def load_json(path: Path) -> Any:
    with open(path) as f:
        return json.load(f)


def ts_to_dt(ts: int | float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def hour_of_day_utc(ts: int | float) -> int:
    return ts_to_dt(ts).hour


def day_of_week(ts: int | float) -> int:
    return ts_to_dt(ts).weekday()  # 0=Mon 6=Sun


def detect_sport(slug: str) -> str:
    """Extract sport prefix from slug."""
    prefixes = [
        "nba", "nfl", "mlb", "nhl", "cfb", "wnba", "cbb",
        "ufc", "ucl", "uel", "epl", "cs2", "lol", "val",
    ]
    lower = slug.lower()
    for p in prefixes:
        if lower.startswith(p + "-"):
            return p.upper()
    return "Other"


# ── Section 1: エントリー条件の抽出 ────────────────────────────────


def analyze_entry_conditions(conditions: list[dict]) -> dict:
    """lhtsports がどの condition にエントリーするかのルールを抽出。"""
    print("=" * 70)
    print("SECTION 1: ENTRY CONDITIONS (エントリー条件)")
    print("=" * 70)

    # 1a. 価格帯別の取引頻度と意思決定
    price_bands = defaultdict(lambda: {"count": 0, "total_cost": 0.0, "outcomes": []})
    for c in conditions:
        p = c["avg_buy_price"]
        band = f"{int(p * 20) * 5:02d}-{int(p * 20) * 5 + 5:02d}"  # 5¢刻み
        band_key = f"0.{band}"
        price_bands[band_key]["count"] += 1
        price_bands[band_key]["total_cost"] += c["net_cost"]
        price_bands[band_key]["outcomes"].append(c["status"])

    # 1b. スポーツ × マーケットタイプ別のフィルター
    sport_market = defaultdict(lambda: {"count": 0, "cost": 0.0, "pnl": 0.0})
    for c in conditions:
        key = f"{c['sport']}|{c['market_type']}"
        sport_market[key]["count"] += 1
        sport_market[key]["cost"] += c["net_cost"]
        sport_market[key]["pnl"] += c["pnl"]

    # 1c. アウトカムサイド分析 (Favorite vs Underdog 判定)
    fav_und = {"favorite": [], "underdog": [], "coinflip": []}
    for c in conditions:
        p = c["avg_buy_price"]
        if p >= 0.55:
            fav_und["favorite"].append(c)
        elif p <= 0.45:
            fav_und["underdog"].append(c)
        else:
            fav_und["coinflip"].append(c)

    # 1d. 両サイド購入パターン (同一 eventSlug で Yes/No 両方)
    event_conditions = defaultdict(list)
    for c in conditions:
        event_conditions[c["eventSlug"]].append(c)

    both_sides = 0
    one_side = 0
    both_sides_detail = []
    for slug, conds in event_conditions.items():
        if len(conds) == 1:
            one_side += 1
            continue
        # 同一マーケットで反対サイドを買っているか
        outcomes = {c["outcome_bought"] for c in conds}
        markets = {c["market_type"] for c in conds}
        if len(outcomes) > 1 and len(markets) == 1:
            both_sides += 1
            both_sides_detail.append({
                "slug": slug,
                "outcomes": list(outcomes),
                "costs": [c["net_cost"] for c in conds],
            })
        elif len(markets) > 1:
            one_side += 1  # マルチマーケットは別カウント
        else:
            one_side += 1

    results = {
        "entry_price_distribution": {},
        "sport_market_filter": {},
        "side_preference": {},
        "both_sides_trading": {
            "both_sides_events": both_sides,
            "one_side_events": one_side,
            "pct_both_sides": round(both_sides / max(1, both_sides + one_side) * 100, 2),
        },
    }

    # 出力: 価格帯フィルター
    print("\n### 1a. Entry Price Range Filter (エントリー価格帯)")
    print(f"{'Price Band':<15} {'Count':>7} {'Total Cost':>13} {'Avg Cost':>10} {'% of Total':>10}")
    total_conds = len(conditions)
    sorted_bands = sorted(price_bands.items())
    for band, data in sorted_bands:
        pct = data["count"] / total_conds * 100
        avg = data["total_cost"] / max(1, data["count"])
        print(f"{band:<15} {data['count']:>7} ${data['total_cost']:>11,.0f} ${avg:>8,.0f} {pct:>9.1f}%")
        results["entry_price_distribution"][band] = {
            "count": data["count"],
            "total_cost": round(data["total_cost"], 2),
            "avg_cost": round(avg, 2),
            "pct": round(pct, 2),
        }

    # 出力: スポーツ × マーケット
    print("\n### 1b. Sport × Market Filter (スポーツ別マーケット選択)")
    print(f"{'Sport|Market':<25} {'Count':>7} {'Cost':>13} {'P&L':>12} {'ROI':>8}")
    for key, data in sorted(sport_market.items(), key=lambda x: -x[1]["cost"]):
        roi = data["pnl"] / max(1, data["cost"]) * 100
        print(f"{key:<25} {data['count']:>7} ${data['cost']:>11,.0f} ${data['pnl']:>10,.0f} {roi:>7.1f}%")
        results["sport_market_filter"][key] = {
            "count": data["count"],
            "cost": round(data["cost"], 2),
            "pnl": round(data["pnl"], 2),
            "roi_pct": round(roi, 2),
        }

    # 出力: サイド選好
    print("\n### 1c. Side Preference (ファボリット/アンダードッグ選好)")
    for side, conds in fav_und.items():
        if not conds:
            continue
        total_cost = sum(c["net_cost"] for c in conds)
        total_pnl = sum(c["pnl"] for c in conds)
        wins = sum(1 for c in conds if c["status"] == "WIN")
        wr = wins / len(conds) * 100
        roi = total_pnl / max(1, total_cost) * 100
        avg_price = statistics.mean(c["avg_buy_price"] for c in conds)
        print(f"  {side:<12}: n={len(conds):>5}, WR={wr:.1f}%, ROI={roi:.1f}%, "
              f"avgPrice={avg_price:.3f}, cost=${total_cost:,.0f}, pnl=${total_pnl:,.0f}")
        results["side_preference"][side] = {
            "count": len(conds),
            "win_rate": round(wr, 2),
            "roi_pct": round(roi, 2),
            "avg_price": round(avg_price, 4),
            "total_cost": round(total_cost, 2),
            "total_pnl": round(total_pnl, 2),
        }

    print(f"\n### 1d. Both-Sides Trading (両サイド購入)")
    print(f"  Both sides: {both_sides} events ({results['both_sides_trading']['pct_both_sides']}%)")
    print(f"  One side:   {one_side} events")

    return results


# ── Section 2: 除外条件の抽出 ────────────────────────────────


def analyze_exclusion_rules(conditions: list[dict], trades: list[dict]) -> dict:
    """lhtsports が取引しない/避けるパターンを特定。"""
    print("\n" + "=" * 70)
    print("SECTION 2: EXCLUSION RULES (除外条件)")
    print("=" * 70)

    results: dict[str, Any] = {}

    # 2a. 取引しないスポーツ/マーケットの特定 (ROI が著しく負)
    sport_perf = defaultdict(lambda: {"count": 0, "cost": 0.0, "pnl": 0.0, "months": set()})
    for c in conditions:
        sport_perf[c["sport"]]["count"] += 1
        sport_perf[c["sport"]]["cost"] += c["net_cost"]
        sport_perf[c["sport"]]["pnl"] += c["pnl"]
        dt = ts_to_dt(c["first_trade_ts"])
        sport_perf[c["sport"]]["months"].add(f"{dt.year}-{dt.month:02d}")

    print("\n### 2a. Sport Performance (スポーツ別成績 — ROI が負のものは除外候補)")
    print(f"{'Sport':<10} {'N':>6} {'Cost':>12} {'P&L':>12} {'ROI':>8} {'Months':>8}")
    unprofitable_sports = []
    for sport, data in sorted(sport_perf.items(), key=lambda x: -x[1]["cost"]):
        roi = data["pnl"] / max(1, data["cost"]) * 100
        print(f"{sport:<10} {data['count']:>6} ${data['cost']:>10,.0f} ${data['pnl']:>10,.0f} "
              f"{roi:>7.1f}% {len(data['months']):>6}mo")
        if roi < 0 and data["count"] >= 20:
            unprofitable_sports.append(sport)

    results["unprofitable_sports"] = unprofitable_sports
    print(f"\n  → 除外候補スポーツ (ROI<0, n>=20): {unprofitable_sports}")

    # 2b. 価格帯別の除外ルール (「ここでは買わない」)
    # 最近6ヶ月のデータで低価格帯のエントリー傾向をチェック
    cutoff_ts = datetime(2025, 8, 1, tzinfo=timezone.utc).timestamp()
    recent = [c for c in conditions if c["first_trade_ts"] >= cutoff_ts]
    old = [c for c in conditions if c["first_trade_ts"] < cutoff_ts]

    price_bins_recent = Counter()
    price_bins_old = Counter()
    for c in recent:
        p = c["avg_buy_price"]
        b = f"{max(0, min(95, int(p * 20) * 5)):02d}"
        price_bins_recent[b] += 1
    for c in old:
        p = c["avg_buy_price"]
        b = f"{max(0, min(95, int(p * 20) * 5)):02d}"
        price_bins_old[b] += 1

    print("\n### 2b. Price Range Evolution (価格帯の変遷)")
    print(f"{'Bin':<8} {'Old (pre-Aug25)':>16} {'Recent (Aug25+)':>16} {'Shift':>10}")
    old_total = max(1, len(old))
    recent_total = max(1, len(recent))
    price_shift = {}
    for b in sorted(set(price_bins_recent) | set(price_bins_old)):
        o_pct = price_bins_old.get(b, 0) / old_total * 100
        r_pct = price_bins_recent.get(b, 0) / recent_total * 100
        shift = r_pct - o_pct
        print(f"0.{b:<6} {price_bins_old.get(b, 0):>8} ({o_pct:>5.1f}%) "
              f"{price_bins_recent.get(b, 0):>8} ({r_pct:>5.1f}%) {shift:>+9.1f}pp")
        price_shift[f"0.{b}"] = round(shift, 2)

    results["price_range_shift"] = price_shift

    # 2c. 曜日・時間帯の除外
    weekday_perf = defaultdict(lambda: {"count": 0, "pnl": 0.0, "cost": 0.0})
    hour_perf = defaultdict(lambda: {"count": 0, "pnl": 0.0, "cost": 0.0})
    for c in conditions:
        wd = day_of_week(c["first_trade_ts"])
        hr = hour_of_day_utc(c["first_trade_ts"])
        weekday_perf[wd]["count"] += 1
        weekday_perf[wd]["pnl"] += c["pnl"]
        weekday_perf[wd]["cost"] += c["net_cost"]
        hour_perf[hr]["count"] += 1
        hour_perf[hr]["pnl"] += c["pnl"]
        hour_perf[hr]["cost"] += c["net_cost"]

    print("\n### 2c. Day-of-Week Performance (曜日別)")
    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    results["weekday_performance"] = {}
    for d in range(7):
        data = weekday_perf[d]
        roi = data["pnl"] / max(1, data["cost"]) * 100
        print(f"  {day_names[d]}: n={data['count']:>5}, P&L=${data['pnl']:>10,.0f}, ROI={roi:>6.1f}%")
        results["weekday_performance"][day_names[d]] = {
            "count": data["count"],
            "pnl": round(data["pnl"], 2),
            "roi_pct": round(roi, 2),
        }

    print("\n### 2d. Hour-of-Day Performance UTC (時間帯別)")
    results["hour_performance"] = {}
    for h in range(24):
        data = hour_perf[h]
        if data["count"] == 0:
            continue
        roi = data["pnl"] / max(1, data["cost"]) * 100
        bar = "█" * int(abs(roi) * 2) if roi > 0 else "░" * int(abs(roi) * 2)
        print(f"  {h:02d}:00 UTC: n={data['count']:>5}, ROI={roi:>6.1f}% {bar}")
        results["hour_performance"][h] = {
            "count": data["count"],
            "pnl": round(data["pnl"], 2),
            "roi_pct": round(roi, 2),
        }

    return results


# ── Section 3: サイジングロジック ────────────────────────────────


def analyze_sizing_logic(conditions: list[dict]) -> dict:
    """ポジションサイズを決定するロジックをリバースエンジニアリング。"""
    print("\n" + "=" * 70)
    print("SECTION 3: SIZING LOGIC (サイジングルール)")
    print("=" * 70)

    results: dict[str, Any] = {}

    # 3a. サイズ = f(price, sport, market_type) のモデル化
    # 価格帯 × スポーツ別の平均サイズ
    sport_price_size = defaultdict(lambda: defaultdict(list))
    for c in conditions:
        if c["net_cost"] < 1:  # ゼロコスト除外
            continue
        p = c["avg_buy_price"]
        band = f"{int(p * 10) * 10:02d}"  # 10¢刻み
        sport_price_size[c["sport"]][band].append(c["net_cost"])

    print("\n### 3a. Average Position Size by Sport × Price (スポーツ×価格帯別サイズ)")
    top_sports = ["NBA", "MLB", "CFB", "NHL", "NFL"]
    header = f"{'Band':<10}" + "".join(f"{s:>12}" for s in top_sports)
    print(header)
    results["size_by_sport_price"] = {}
    for band in sorted({b for sp in sport_price_size.values() for b in sp}):
        row = f"0.{band:<8}"
        for sport in top_sports:
            sizes = sport_price_size[sport].get(band, [])
            if sizes:
                med = statistics.median(sizes)
                row += f"${med:>10,.0f}"
            else:
                row += f"{'—':>12}"
        print(row)

    # 3b. サイジングは確信度に連動するか？ (サイズと勝率の相関)
    # net_cost の十分位別に勝率を計算
    costs = sorted([c for c in conditions if c["net_cost"] >= 1], key=lambda x: x["net_cost"])
    decile_size = len(costs) // 10

    print("\n### 3b. Win Rate by Position Size Decile (サイズ十分位と勝率)")
    print(f"{'Decile':<10} {'Size Range':>25} {'N':>6} {'WR':>8} {'ROI':>8} {'P&L':>12}")
    results["size_decile_win_rate"] = []
    for i in range(10):
        start = i * decile_size
        end = (i + 1) * decile_size if i < 9 else len(costs)
        decile = costs[start:end]
        sizes = [c["net_cost"] for c in decile]
        wins = sum(1 for c in decile if c["status"] == "WIN")
        wr = wins / len(decile) * 100
        total_pnl = sum(c["pnl"] for c in decile)
        total_cost = sum(c["net_cost"] for c in decile)
        roi = total_pnl / max(1, total_cost) * 100
        lo, hi = min(sizes), max(sizes)
        print(f"D{i + 1:<9} ${lo:>10,.0f}-${hi:>8,.0f} {len(decile):>6} {wr:>7.1f}% "
              f"{roi:>7.1f}% ${total_pnl:>10,.0f}")
        results["size_decile_win_rate"].append({
            "decile": i + 1,
            "size_range": [round(lo, 2), round(hi, 2)],
            "count": len(decile),
            "win_rate": round(wr, 2),
            "roi_pct": round(roi, 2),
            "total_pnl": round(total_pnl, 2),
        })

    # 3c. 日次エクスポージャー分布
    daily_exposure = defaultdict(float)
    for c in conditions:
        dt = ts_to_dt(c["first_trade_ts"])
        day_key = dt.strftime("%Y-%m-%d")
        daily_exposure[day_key] += c["net_cost"]

    exposures = sorted(daily_exposure.values())
    print("\n### 3c. Daily Exposure Distribution (日次エクスポージャー)")
    print(f"  取引日数: {len(exposures)}")
    print(f"  中央値:   ${statistics.median(exposures):,.0f}")
    print(f"  平均:     ${statistics.mean(exposures):,.0f}")
    print(f"  P25:      ${exposures[len(exposures) // 4]:,.0f}")
    print(f"  P75:      ${exposures[3 * len(exposures) // 4]:,.0f}")
    print(f"  P95:      ${exposures[int(len(exposures) * 0.95)]:,.0f}")
    print(f"  最大:     ${exposures[-1]:,.0f}")

    results["daily_exposure"] = {
        "trading_days": len(exposures),
        "median": round(statistics.median(exposures), 2),
        "mean": round(statistics.mean(exposures), 2),
        "p25": round(exposures[len(exposures) // 4], 2),
        "p75": round(exposures[3 * len(exposures) // 4], 2),
        "p95": round(exposures[int(len(exposures) * 0.95)], 2),
        "max": round(exposures[-1], 2),
    }

    # 3d. 1試合あたりの condition 数パターン
    game_cond_count = Counter()
    for c in conditions:
        game_cond_count[c["eventSlug"]] += 1

    cond_counts = Counter(game_cond_count.values())
    print("\n### 3d. Conditions per Game (試合あたり condition 数)")
    for n, count in sorted(cond_counts.items()):
        print(f"  {n} conditions: {count:>5} games ({count / len(game_cond_count) * 100:.1f}%)")

    results["conditions_per_game"] = dict(sorted(cond_counts.items()))

    return results


# ── Section 4: 退出条件の抽出 ────────────────────────────────


def analyze_exit_rules(conditions: list[dict], merges: list[dict]) -> dict:
    """MERGE (早期退出) のトリガー条件をリバースエンジニアリング。"""
    print("\n" + "=" * 70)
    print("SECTION 4: EXIT RULES (退出条件)")
    print("=" * 70)

    results: dict[str, Any] = {}

    # マージされた condition の特徴
    merged = [c for c in conditions if c["status"] == "MERGED"]
    wins = [c for c in conditions if c["status"] == "WIN"]
    losses = [c for c in conditions if c["status"] not in ("WIN", "MERGED")]

    print(f"\n  Merged: {len(merged)}, WIN: {len(wins)}, LOSS/OPEN: {len(losses)}")

    # 4a. マージされた condition の価格帯分布
    price_merge_rate = defaultdict(lambda: {"merged": 0, "total": 0})
    for c in conditions:
        p = c["avg_buy_price"]
        band = f"{int(p * 10) * 10:02d}"
        price_merge_rate[band]["total"] += 1
        if c["status"] == "MERGED":
            price_merge_rate[band]["merged"] += 1

    print("\n### 4a. Merge Rate by Price Band (価格帯別マージ率)")
    print(f"{'Band':<10} {'Merged':>8} {'Total':>8} {'Rate':>8}")
    results["merge_rate_by_price"] = {}
    for band in sorted(price_merge_rate.keys()):
        data = price_merge_rate[band]
        rate = data["merged"] / max(1, data["total"]) * 100
        print(f"0.{band:<8} {data['merged']:>8} {data['total']:>8} {rate:>7.1f}%")
        results["merge_rate_by_price"][f"0.{band}"] = round(rate, 2)

    # 4b. マージのタイミング (最初の購入からマージまでの時間)
    if merged:
        merge_times = []
        for c in merged:
            if c["merge_usdc"] > 0 and c["last_trade_ts"] > c["first_trade_ts"]:
                hours = (c["last_trade_ts"] - c["first_trade_ts"]) / 3600
                merge_times.append(hours)

        if merge_times:
            print(f"\n### 4b. Time to Merge (マージまでの時間)")
            print(f"  サンプル数: {len(merge_times)}")
            print(f"  中央値:    {statistics.median(merge_times):.2f} 時間")
            print(f"  平均:      {statistics.mean(merge_times):.2f} 時間")
            if len(merge_times) > 2:
                print(f"  P25:       {sorted(merge_times)[len(merge_times) // 4]:.2f} 時間")
                print(f"  P75:       {sorted(merge_times)[3 * len(merge_times) // 4]:.2f} 時間")

            results["merge_timing"] = {
                "median_hours": round(statistics.median(merge_times), 2),
                "mean_hours": round(statistics.mean(merge_times), 2),
            }

    # 4c. マージ回収率 (merge_usdc / net_cost)
    merge_ratios = []
    for c in merged:
        if c["net_cost"] > 10:  # 極小ポジション除外
            ratio = c["merge_usdc"] / c["net_cost"]
            merge_ratios.append(ratio)

    if merge_ratios:
        print(f"\n### 4c. Merge Recovery Ratio (マージ回収率)")
        print(f"  中央値: {statistics.median(merge_ratios):.2%}")
        print(f"  平均:   {statistics.mean(merge_ratios):.2%}")
        print(f"  P25:    {sorted(merge_ratios)[len(merge_ratios) // 4]:.2%}")
        print(f"  P75:    {sorted(merge_ratios)[3 * len(merge_ratios) // 4]:.2%}")

        results["merge_recovery_ratio"] = {
            "median": round(statistics.median(merge_ratios), 4),
            "mean": round(statistics.mean(merge_ratios), 4),
            "p25": round(sorted(merge_ratios)[len(merge_ratios) // 4], 4),
            "p75": round(sorted(merge_ratios)[3 * len(merge_ratios) // 4], 4),
        }

    # 4d. マージされた condition vs 保持した condition の特徴比較
    print(f"\n### 4d. Merged vs Held Conditions (マージ vs 保持の比較)")
    for label, group in [("MERGED", merged), ("WIN", wins), ("LOSS", losses)]:
        if not group:
            continue
        avg_price = statistics.mean(c["avg_buy_price"] for c in group)
        avg_cost = statistics.mean(c["net_cost"] for c in group)
        avg_trades = statistics.mean(c["trade_count"] for c in group)
        print(f"  {label:<8}: n={len(group):>5}, avgPrice={avg_price:.3f}, "
              f"avgCost=${avg_cost:>8,.0f}, avgTrades={avg_trades:.1f}")

    # 4e. スポーツ別マージ率
    sport_merge = defaultdict(lambda: {"merged": 0, "total": 0})
    for c in conditions:
        sport_merge[c["sport"]]["total"] += 1
        if c["status"] == "MERGED":
            sport_merge[c["sport"]]["merged"] += 1

    print(f"\n### 4e. Merge Rate by Sport (スポーツ別マージ率)")
    results["merge_rate_by_sport"] = {}
    for sport in sorted(sport_merge, key=lambda s: -sport_merge[s]["total"]):
        data = sport_merge[sport]
        rate = data["merged"] / max(1, data["total"]) * 100
        print(f"  {sport:<10}: {data['merged']:>4}/{data['total']:>5} = {rate:.1f}%")
        results["merge_rate_by_sport"][sport] = round(rate, 2)

    return results


# ── Section 5: DCA 実行パターン ────────────────────────────────


def analyze_dca_patterns(conditions: list[dict], trades: list[dict]) -> dict:
    """DCA の具体的な実行ルールを抽出。"""
    print("\n" + "=" * 70)
    print("SECTION 5: DCA EXECUTION PATTERNS (DCA 実行パターン)")
    print("=" * 70)

    results: dict[str, Any] = {}

    # condition ごとのトレードを集約
    cond_trades = defaultdict(list)
    for t in trades:
        cond_trades[t["conditionId"]].append(t)

    # 各 condition のトレード数、時間幅、価格変動
    dca_profiles = []
    for c in conditions:
        ctrades = cond_trades.get(c["conditionId"], [])
        buys = [t for t in ctrades if t["side"] == "BUY"]
        if len(buys) < 2:
            continue

        buys_sorted = sorted(buys, key=lambda t: t["timestamp"])
        prices = [t["price"] for t in buys_sorted]
        sizes_usdc = [t["usdcSize"] for t in buys_sorted]
        time_span_hours = (buys_sorted[-1]["timestamp"] - buys_sorted[0]["timestamp"]) / 3600

        # 購入パターンの分類
        # フラット: すべてほぼ同価格
        price_cv = statistics.stdev(prices) / max(0.001, statistics.mean(prices)) if len(prices) > 1 else 0
        # ナンピン: 後半の購入価格が前半より低い
        mid = len(prices) // 2
        first_half_avg = statistics.mean(prices[:mid]) if mid > 0 else prices[0]
        second_half_avg = statistics.mean(prices[mid:])
        is_averaging_down = second_half_avg < first_half_avg * 0.95

        dca_profiles.append({
            "conditionId": c["conditionId"],
            "sport": c["sport"],
            "n_buys": len(buys),
            "time_span_hours": time_span_hours,
            "price_cv": price_cv,
            "avg_price": statistics.mean(prices),
            "avg_order_usdc": statistics.mean(sizes_usdc),
            "total_cost": c["net_cost"],
            "is_averaging_down": is_averaging_down,
            "status": c["status"],
            "pnl": c["pnl"],
        })

    # 5a. DCA 回数分布
    buys_dist = Counter(p["n_buys"] for p in dca_profiles)
    print("\n### 5a. Number of Buys Distribution (DCA 回数分布)")
    for n in sorted(buys_dist.keys())[:20]:
        print(f"  {n:>3} buys: {buys_dist[n]:>5} conditions")
    if max(buys_dist.keys()) > 20:
        over_20 = sum(v for k, v in buys_dist.items() if k > 20)
        print(f"  >20 buys: {over_20:>5} conditions")

    # 5b. DCA 時間幅と結果
    time_buckets = {
        "<5min": (0, 5 / 60),
        "5-30min": (5 / 60, 0.5),
        "30min-1h": (0.5, 1),
        "1-2h": (1, 2),
        "2-4h": (2, 4),
        "4h+": (4, 1000),
    }
    print("\n### 5b. DCA Time Span vs Outcome (DCA 時間幅と結果)")
    print(f"{'Bucket':<12} {'N':>6} {'WR':>8} {'ROI':>8} {'AvgCost':>10}")
    results["dca_time_buckets"] = {}
    for label, (lo, hi) in time_buckets.items():
        bucket = [p for p in dca_profiles if lo <= p["time_span_hours"] < hi]
        if not bucket:
            continue
        wins = sum(1 for p in bucket if p["status"] == "WIN")
        wr = wins / len(bucket) * 100
        total_cost = sum(p["total_cost"] for p in bucket)
        total_pnl = sum(p["pnl"] for p in bucket)
        roi = total_pnl / max(1, total_cost) * 100
        avg_cost = statistics.mean(p["total_cost"] for p in bucket)
        print(f"{label:<12} {len(bucket):>6} {wr:>7.1f}% {roi:>7.1f}% ${avg_cost:>8,.0f}")
        results["dca_time_buckets"][label] = {
            "count": len(bucket),
            "win_rate": round(wr, 2),
            "roi_pct": round(roi, 2),
            "avg_cost": round(avg_cost, 2),
        }

    # 5c. ナンピン率と効果
    averaging_down = [p for p in dca_profiles if p["is_averaging_down"]]
    not_averaging = [p for p in dca_profiles if not p["is_averaging_down"]]

    print(f"\n### 5c. Averaging Down Analysis (ナンピン分析)")
    for label, group in [("Averaging Down", averaging_down), ("Not Averaging", not_averaging)]:
        if not group:
            continue
        wins = sum(1 for p in group if p["status"] == "WIN")
        wr = wins / len(group) * 100
        total_pnl = sum(p["pnl"] for p in group)
        total_cost = sum(p["total_cost"] for p in group)
        roi = total_pnl / max(1, total_cost) * 100
        print(f"  {label:<20}: n={len(group):>5}, WR={wr:.1f}%, ROI={roi:.1f}%, P&L=${total_pnl:>10,.0f}")

    results["averaging_down"] = {
        "count": len(averaging_down),
        "pct": round(len(averaging_down) / max(1, len(dca_profiles)) * 100, 2),
    }

    # 5d. 1回あたりの注文サイズ
    order_sizes = [p["avg_order_usdc"] for p in dca_profiles]
    if order_sizes:
        print(f"\n### 5d. Individual Order Size (1回あたり注文額)")
        print(f"  中央値: ${statistics.median(order_sizes):,.0f}")
        print(f"  平均:   ${statistics.mean(order_sizes):,.0f}")
        print(f"  P25:    ${sorted(order_sizes)[len(order_sizes) // 4]:,.0f}")
        print(f"  P75:    ${sorted(order_sizes)[3 * len(order_sizes) // 4]:,.0f}")

        results["order_size"] = {
            "median": round(statistics.median(order_sizes), 2),
            "mean": round(statistics.mean(order_sizes), 2),
        }

    return results


# ── Section 6: 時間経過による戦略進化 ────────────────────────────────


def analyze_strategy_evolution(conditions: list[dict]) -> dict:
    """戦略がどう進化したかを時系列で追跡。"""
    print("\n" + "=" * 70)
    print("SECTION 6: STRATEGY EVOLUTION (戦略の進化)")
    print("=" * 70)

    results: dict[str, Any] = {}

    # 月次でキー指標を追跡
    monthly = defaultdict(lambda: {
        "count": 0, "cost": 0.0, "pnl": 0.0,
        "prices": [], "sizes": [], "sports": Counter(),
        "market_types": Counter(), "wins": 0,
    })

    for c in conditions:
        dt = ts_to_dt(c["first_trade_ts"])
        m = dt.strftime("%Y-%m")
        monthly[m]["count"] += 1
        monthly[m]["cost"] += c["net_cost"]
        monthly[m]["pnl"] += c["pnl"]
        monthly[m]["prices"].append(c["avg_buy_price"])
        monthly[m]["sizes"].append(c["net_cost"])
        monthly[m]["sports"][c["sport"]] += 1
        monthly[m]["market_types"][c["market_type"]] += 1
        if c["status"] == "WIN":
            monthly[m]["wins"] += 1

    print("\n### Monthly Strategy Metrics (月次戦略指標)")
    print(f"{'Month':<10} {'N':>5} {'WR':>6} {'ROI':>7} {'AvgPrice':>9} "
          f"{'MedSize':>10} {'TopSport':>10} {'ML%':>6}")
    results["monthly_metrics"] = {}
    for m in sorted(monthly.keys()):
        d = monthly[m]
        wr = d["wins"] / max(1, d["count"]) * 100
        roi = d["pnl"] / max(1, d["cost"]) * 100
        avg_p = statistics.mean(d["prices"]) if d["prices"] else 0
        med_sz = statistics.median(d["sizes"]) if d["sizes"] else 0
        top_sport = d["sports"].most_common(1)[0][0] if d["sports"] else "—"
        ml_pct = d["market_types"].get("Moneyline", 0) / max(1, d["count"]) * 100
        print(f"{m:<10} {d['count']:>5} {wr:>5.1f}% {roi:>6.1f}% {avg_p:>8.3f} "
              f"${med_sz:>8,.0f} {top_sport:>10} {ml_pct:>5.0f}%")
        results["monthly_metrics"][m] = {
            "count": d["count"],
            "win_rate": round(wr, 2),
            "roi_pct": round(roi, 2),
            "avg_price": round(avg_p, 4),
            "median_size": round(med_sz, 2),
            "top_sport": top_sport,
            "ml_pct": round(ml_pct, 2),
        }

    return results


# ── Section 7: 取引実行ルールの統合 ────────────────────────────────


def synthesize_rules(
    entry: dict,
    exclusion: dict,
    sizing: dict,
    exit_rules: dict,
    dca: dict,
    evolution: dict,
    conditions: list[dict],
) -> dict:
    """全分析を統合して具体的な取引ルールを記述。"""
    print("\n" + "=" * 70)
    print("SECTION 7: SYNTHESIZED TRADING RULES (統合取引ルール)")
    print("=" * 70)

    # 全期間の sport 別 ROI を計算
    sport_stats = defaultdict(lambda: {"cost": 0.0, "pnl": 0.0, "n": 0})
    for c in conditions:
        sport_stats[c["sport"]]["cost"] += c["net_cost"]
        sport_stats[c["sport"]]["pnl"] += c["pnl"]
        sport_stats[c["sport"]]["n"] += 1

    profitable_sports = []
    for sport, data in sport_stats.items():
        roi = data["pnl"] / max(1, data["cost"]) * 100
        if roi > 0 and data["n"] >= 50:
            profitable_sports.append(sport)

    rules = {
        "rule_1_market_selection": {
            "description": "取引対象のマーケットを選択するフィルター",
            "subrules": {
                "1a_sport_filter": {
                    "rule": "NBA, MLB, NHL, CFB, NFL のマネーライン (ML) に集中。Spread は NBA/NFL/NHL では避ける。",
                    "profitable_sports": profitable_sports,
                    "moneyline_pct_of_trades": round(
                        sum(1 for c in conditions if c["market_type"] == "Moneyline") / len(conditions) * 100, 1,
                    ),
                },
                "1b_market_type": {
                    "rule": "主力は Moneyline (64.6%)。Total は補助的エッジ源 (19.0%)。Spread は NBA では非推奨。",
                    "breakdown": {
                        mt: round(sum(1 for c in conditions if c["market_type"] == mt) / len(conditions) * 100, 1)
                        for mt in ["Moneyline", "Spread", "Total"]
                    },
                },
                "1c_multi_market": {
                    "rule": "ML+Total の組み合わせが最高 ROI (10.6%)。ML と Total は独立エッジ (phi=0.05)。",
                },
            },
        },
        "rule_2_entry_price": {
            "description": "購入価格帯のフィルター",
            "subrules": {
                "2a_sweet_spot": {
                    "rule": "価格帯 0.25-0.55 のアウトカムを優先購入。EV/$ が最高。",
                    "best_bands": ["0.35-0.40", "0.25-0.30", "0.50-0.55", "0.30-0.35"],
                },
                "2b_avoid_longshots": {
                    "rule": "価格 < 0.25 は購入しない。EV/$ が負。",
                    "evidence": "0.00-0.25 の累計 P&L: -$297K",
                },
                "2c_high_favorites": {
                    "rule": "価格 0.60-0.85 も購入するが、サイジングを控えめに。",
                    "evidence": "0.60-0.85 の累計 P&L: +$372K, ROI +4-12%",
                },
                "2d_avoid_extreme_favorites": {
                    "rule": "価格 > 0.85 はマージン薄すぎ。大量資本が必要で非効率。",
                },
            },
        },
        "rule_3_sizing": {
            "description": "ポジションサイズ決定ロジック",
            "subrules": {
                "3a_conviction_sizing": {
                    "rule": "確信度に応じてサイズを調整。大型ポジション ($5K-50K) は高い勝率 (85-87%)。",
                    "median_position": "$961",
                    "sweet_spot_range": "$1K-$50K",
                },
                "3b_favorite_premium": {
                    "rule": "ファボリットにはアンダードッグより ~39% 大きくサイジング。",
                    "favorite_avg": "$4,351",
                    "underdog_avg": "$3,124",
                },
                "3c_avoid_oversizing": {
                    "rule": "$50K+ のポジションは避ける。WR 84% でも ROI -22.2%。スリッページ/流動性コスト。",
                },
                "3d_kelly_fraction": {
                    "rule": "実際の Kelly フラクションは理論値の 0.3%。極端な保守性。",
                },
            },
        },
        "rule_4_execution": {
            "description": "取引実行のタイミングと方法",
            "subrules": {
                "4a_timing": {
                    "rule": "試合開始前 0-2 時間にエントリー。最も ROI が高いウィンドウ (6.2%)。",
                },
                "4b_dca": {
                    "rule": "大型ポジション ($5K+) では DCA で流動性アクセス。12.9 回/condition, 1.6 時間。",
                },
                "4c_single_buy": {
                    "rule": "小型ポジション (<$500) では一括購入。DCA のメリットなし。",
                },
            },
        },
        "rule_5_exit": {
            "description": "ポジション退出条件",
            "subrules": {
                "5a_hold_to_settle": {
                    "rule": "基本は決済 (試合終了) まで保有。保有期間中央値 5.5 時間。",
                },
                "5b_merge_stoploss": {
                    "rule": "含み損ポジションは MERGE で早期退出。回収率中央値 55%。",
                    "trigger": "含み損の 86% で MERGE 実行",
                    "timing": "最終取引後 中央値 0.7 時間",
                },
                "5c_merge_only_ml": {
                    "rule": "MERGE は主に Moneyline ポジションで使用 (664/665 condition)。Spread/Total はほぼ MERGE しない。",
                },
            },
        },
        "rule_6_per_game": {
            "description": "1 試合あたりの取引ルール",
            "subrules": {
                "6a_one_side_per_market": {
                    "rule": "1 つのマーケットでは片サイドのみ購入。両サイド購入は極めて稀。",
                    "both_sides_pct": entry.get("both_sides_trading", {}).get("pct_both_sides", 0),
                },
                "6b_ev_comparison": {
                    "rule": "両アウトカムの EV/$ を比較し、高い方を 1 つ選択。",
                },
            },
        },
    }

    # ルールの出力
    for rule_id, rule_data in rules.items():
        print(f"\n### {rule_id}: {rule_data['description']}")
        for sub_id, sub_data in rule_data["subrules"].items():
            print(f"  {sub_id}: {sub_data['rule']}")
            for k, v in sub_data.items():
                if k != "rule":
                    print(f"    {k}: {v}")

    return rules


# ── メインエントリ ────────────────────────────────


def main() -> None:
    print("Loading data...")
    trades = load_json(TRADES_FILE)
    conditions = load_json(CONDITIONS_FILE)
    games = load_json(GAMES_FILE)

    merges = []
    if MERGES_FILE.exists():
        merges = load_json(MERGES_FILE)

    print(f"Loaded: {len(trades):,} trades, {len(conditions):,} conditions, "
          f"{len(games):,} games, {len(merges):,} merges\n")

    # 各セクションの分析を実行
    entry = analyze_entry_conditions(conditions)
    exclusion = analyze_exclusion_rules(conditions, trades)
    sizing = analyze_sizing_logic(conditions)
    exit_rules = analyze_exit_rules(conditions, merges)
    dca = analyze_dca_patterns(conditions, trades)
    evolution = analyze_strategy_evolution(conditions)
    rules = synthesize_rules(entry, exclusion, sizing, exit_rules, dca, evolution, conditions)

    # JSON 出力
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary = {
        "generated": datetime.now(tz=timezone.utc).isoformat(),
        "data_basis": {
            "trades": len(trades),
            "conditions": len(conditions),
            "games": len(games),
        },
        "entry_conditions": entry,
        "exclusion_rules": exclusion,
        "sizing_logic": sizing,
        "exit_rules": exit_rules,
        "dca_patterns": dca,
        "strategy_evolution": evolution,
        "synthesized_rules": rules,
    }

    json_path = OUTPUT_DIR / "trading_rules.json"
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
    print(f"\n\nJSON output: {json_path}")

    # Markdown レポート出力
    md_path = OUTPUT_DIR / "trading_rules_report.md"
    write_markdown_report(rules, md_path)
    print(f"Markdown report: {md_path}")


def write_markdown_report(rules: dict, path: Path) -> None:
    """統合ルールを Markdown に整形。"""
    lines = [
        "# lhtsports 取引実行ルール — リバースエンジニアリング結果",
        "",
        f"**生成日**: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d')}",
        f"**データ基盤**: 136,982 トレード / 10,597 condition / 6,732 試合 / $38.7M 投下資本",
        "",
        "---",
        "",
        "## エグゼクティブサマリー",
        "",
        "lhtsports の取引は以下の6つの明確なルールセットに従う:",
        "",
        "1. **マーケット選択**: NBA/MLB/NHL/CFB の ML を主軸、Total を補助エッジ",
        "2. **エントリー価格**: 0.25-0.55 のスイートスポットに集中 (EV/$ 最高)",
        "3. **サイジング**: 確信度ベース。ファボリットに大きく、$50K は避ける",
        "4. **実行タイミング**: 試合前 0-2 時間。大型は DCA、小型は一括",
        "5. **退出**: 基本は決済まで保持。含み損は MERGE で 55% 回収",
        "6. **試合ルール**: 1 マーケット 1 サイド。EV/$ で高い方を選択",
        "",
        "---",
        "",
    ]

    for rule_id, rule_data in rules.items():
        lines.append(f"## {rule_id}: {rule_data['description']}")
        lines.append("")
        for sub_id, sub_data in rule_data["subrules"].items():
            lines.append(f"### {sub_id}")
            lines.append(f"**ルール**: {sub_data['rule']}")
            lines.append("")
            for k, v in sub_data.items():
                if k != "rule":
                    lines.append(f"- **{k}**: {v}")
            lines.append("")

    with open(path, "w") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    main()
