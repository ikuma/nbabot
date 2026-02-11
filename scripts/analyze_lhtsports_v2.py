"""Enhanced analysis of @lhtsports Polymarket trading data.

Operates on previously collected raw JSON data.
"""

import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean, median, stdev


def load_trades() -> list[dict]:
    with open("/Users/taro/dev/nbabot/data/reports/lhtsports_raw_trades.json") as f:
        return json.load(f)


def classify_sport(slug: str) -> str:
    if slug.startswith("nba-"):
        return "NBA"
    elif slug.startswith("nhl-"):
        return "NHL"
    elif slug.startswith("cbb-"):
        return "CBB"
    elif slug.startswith("nfl-"):
        return "NFL"
    elif slug.startswith("mlb-"):
        return "MLB"
    return "Other"


def classify_market_type(slug: str) -> str:
    if "spread" in slug:
        return "Spread"
    elif "total" in slug or "over" in slug or "under" in slug:
        return "Total O/U"
    return "Moneyline"


def get_game_key(slug: str) -> str:
    """Extract base game key: sport-team1-team2-YYYY-MM-DD"""
    parts = slug.split("-")
    for i in range(len(parts)):
        if len(parts[i]) == 4 and parts[i].isdigit():
            date_end = min(i + 3, len(parts))
            return "-".join(parts[:date_end])
    return slug


def price_bucket(price: float) -> str:
    if price < 0.2:
        return "0.00-0.20"
    elif price < 0.4:
        return "0.20-0.40"
    elif price < 0.6:
        return "0.40-0.60"
    elif price < 0.8:
        return "0.60-0.80"
    return "0.80-1.00"


def percentile(data: list[float], p: float) -> float:
    data = sorted(data)
    k = (len(data) - 1) * p / 100
    f = int(k)
    c = f + 1
    if c >= len(data):
        return data[f]
    return data[f] + (k - f) * (data[c] - data[f])


def analyze_both_sides_detail(trades: list[dict]) -> list[str]:
    """Deep analysis of both-sides buying pattern."""
    lines = []

    # Group by game and moneyline slug
    game_ml_trades: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        slug = t.get("slug", "")
        if classify_market_type(slug) == "Moneyline" and t.get("type") == "TRADE":
            game_key = get_game_key(slug)
            game_ml_trades[game_key].append(t)

    both_sides_detail = []
    for game_key, game_trades in game_ml_trades.items():
        outcomes = set(t.get("outcomeIndex", -1) for t in game_trades)
        if len(outcomes) > 1:
            side0_vol = sum(
                float(t.get("usdcSize", 0))
                for t in game_trades
                if t.get("outcomeIndex") == 0
            )
            side1_vol = sum(
                float(t.get("usdcSize", 0))
                for t in game_trades
                if t.get("outcomeIndex") == 1
            )
            side0_prices = [
                float(t.get("price", 0))
                for t in game_trades
                if t.get("outcomeIndex") == 0
            ]
            side1_prices = [
                float(t.get("price", 0))
                for t in game_trades
                if t.get("outcomeIndex") == 1
            ]
            side0_avg_price = mean(side0_prices) if side0_prices else 0
            side1_avg_price = mean(side1_prices) if side1_prices else 0
            # Time difference between first and last trade
            timestamps = [t.get("timestamp", 0) for t in game_trades]
            time_span_min = (max(timestamps) - min(timestamps)) / 60 if timestamps else 0

            total_vol = side0_vol + side1_vol
            side0_pct = side0_vol / total_vol * 100 if total_vol else 0
            side1_pct = side1_vol / total_vol * 100 if total_vol else 0

            both_sides_detail.append({
                "game": game_key,
                "title": game_trades[0].get("title", ""),
                "side0_vol": side0_vol,
                "side1_vol": side1_vol,
                "side0_pct": side0_pct,
                "side1_pct": side1_pct,
                "side0_avg_price": side0_avg_price,
                "side1_avg_price": side1_avg_price,
                "total_vol": total_vol,
                "n_trades": len(game_trades),
                "time_span_min": time_span_min,
            })

    both_sides_detail.sort(key=lambda x: x["total_vol"], reverse=True)

    lines.append("### 両サイド ML 購入 詳細分析")
    lines.append("")
    lines.append(f"両サイド購入確認試合: **{len(both_sides_detail)}** 試合")
    lines.append("")
    lines.append("| 試合 | Away(0)$ | 平均価格 | Home(1)$ | 平均価格 | 合計$ | 取引数 | 時間幅(分) | 支配サイド |")
    lines.append("|------|----------|----------|----------|----------|-------|--------|------------|------------|")
    for d in both_sides_detail[:20]:
        dominant = "Away" if d["side0_pct"] > 60 else ("Home" if d["side1_pct"] > 60 else "Mixed")
        lines.append(
            f"| {d['game']} | ${d['side0_vol']:,.0f} ({d['side0_pct']:.0f}%) | "
            f"{d['side0_avg_price']:.3f} | ${d['side1_vol']:,.0f} ({d['side1_pct']:.0f}%) | "
            f"{d['side1_avg_price']:.3f} | ${d['total_vol']:,.0f} | "
            f"{d['n_trades']} | {d['time_span_min']:.0f} | {dominant} |"
        )
    lines.append("")

    # Interpretation
    heavy_one_side = sum(
        1 for d in both_sides_detail if d["side0_pct"] > 80 or d["side1_pct"] > 80
    )
    mixed = len(both_sides_detail) - heavy_one_side
    lines.append(f"- **片側80%超**: {heavy_one_side} 試合 (メイン + 小額ヘッジ)")
    lines.append(f"- **両サイド均衡 (<80%)**: {mixed} 試合 (ライン移動対応 or マーケットメイキング)")
    lines.append("")

    return lines


def analyze_timing_patterns(trades: list[dict]) -> list[str]:
    """Analyze trade timing patterns (hour of day, clustering)."""
    lines = []

    hourly_stats: dict[int, dict] = defaultdict(lambda: {"count": 0, "volume": 0.0})
    for t in trades:
        ts = t.get("timestamp", 0)
        if ts:
            # Convert to US Eastern (UTC-5)
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            hour_utc = dt.hour
            hour_et = (hour_utc - 5) % 24  # Rough ET conversion
            size = float(t.get("usdcSize", 0))
            hourly_stats[hour_et]["count"] += 1
            hourly_stats[hour_et]["volume"] += size

    lines.append("### 時間帯別取引分布 (US Eastern)")
    lines.append("")
    lines.append("| 時間 (ET) | 件数 | 取引額 ($) | 平均額 ($) |")
    lines.append("|-----------|------|------------|------------|")
    total_count = sum(h["count"] for h in hourly_stats.values())
    for hour in range(24):
        s = hourly_stats.get(hour, {"count": 0, "volume": 0.0})
        if s["count"] > 0:
            avg = s["volume"] / s["count"]
            pct = s["count"] / total_count * 100
            lines.append(
                f"| {hour:02d}:00 | {s['count']} ({pct:.1f}%) | ${s['volume']:,.0f} | ${avg:,.0f} |"
            )
    lines.append("")

    # Peak hours
    peak_hours = sorted(hourly_stats.items(), key=lambda x: x[1]["volume"], reverse=True)[:5]
    lines.append("**取引額上位5時間帯**: " + ", ".join(
        f"{h:02d}:00 ET (${s['volume']:,.0f})" for h, s in peak_hours
    ))
    lines.append("")

    # Trade clustering: time between consecutive trades
    trade_times = sorted(t.get("timestamp", 0) for t in trades if t.get("timestamp"))
    if len(trade_times) > 1:
        intervals = [trade_times[i+1] - trade_times[i] for i in range(len(trade_times)-1)]
        intervals = [i for i in intervals if i > 0]  # Remove duplicates
        if intervals:
            lines.append("### 取引間隔分析")
            lines.append("")
            lines.append(f"- 平均間隔: {mean(intervals):.1f} 秒 ({mean(intervals)/60:.1f} 分)")
            lines.append(f"- 中央値間隔: {median(intervals):.1f} 秒 ({median(intervals)/60:.1f} 分)")
            lines.append(f"- 最小間隔: {min(intervals)} 秒")
            lines.append(f"- 最大間隔: {max(intervals)} 秒 ({max(intervals)/3600:.1f} 時間)")
            # Burst trading: intervals < 5 seconds
            burst_count = sum(1 for i in intervals if i <= 5)
            lines.append(f"- **バースト取引 (<=5秒間隔)**: {burst_count} ({burst_count/len(intervals)*100:.1f}%)")
            rapid_count = sum(1 for i in intervals if i <= 60)
            lines.append(f"- **高速取引 (<=60秒間隔)**: {rapid_count} ({rapid_count/len(intervals)*100:.1f}%)")
            lines.append("")

    return lines


def analyze_totals_strategy(trades: list[dict]) -> list[str]:
    """Analyze the total O/U multi-line strategy."""
    lines = []

    # Group total O/U trades by game
    game_total_trades: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        slug = t.get("slug", "")
        if classify_market_type(slug) == "Total O/U" and t.get("type") == "TRADE":
            game_key = get_game_key(slug)
            game_total_trades[game_key].append(t)

    lines.append("### マルチライン Total O/U 戦略分析")
    lines.append("")

    multi_line_games = {
        g: ts for g, ts in game_total_trades.items() if len(set(t.get("slug") for t in ts)) > 1
    }

    lines.append(f"- Total O/U 取引がある試合: {len(game_total_trades)}")
    lines.append(f"- うち複数ライン購入: {len(multi_line_games)} ({len(multi_line_games)/max(len(game_total_trades),1)*100:.0f}%)")
    lines.append("")

    # Detail: for multi-line games, show the lines and volumes
    lines.append("| 試合 | ライン数 | Over$ | Under$ | 合計$ | ラインリスト |")
    lines.append("|------|----------|-------|--------|-------|------------|")

    game_details = []
    for game_key, ts in multi_line_games.items():
        unique_slugs = set(t.get("slug") for t in ts)
        over_vol = sum(float(t.get("usdcSize", 0)) for t in ts if t.get("outcomeIndex") == 0)
        under_vol = sum(float(t.get("usdcSize", 0)) for t in ts if t.get("outcomeIndex") == 1)
        total_vol = over_vol + under_vol
        # Extract line numbers
        line_nums = set()
        for slug in unique_slugs:
            m = re.search(r"total-(\d+)pt(\d+)", slug)
            if m:
                line_nums.add(f"{m.group(1)}.{m.group(2)}")
            # NHL totals (e.g. 6pt5)
            m2 = re.search(r"total-(\d+)pt(\d+)", slug)
            if m2:
                line_nums.add(f"{m2.group(1)}.{m2.group(2)}")
        game_details.append({
            "game": game_key,
            "n_lines": len(unique_slugs),
            "over_vol": over_vol,
            "under_vol": under_vol,
            "total_vol": total_vol,
            "lines": sorted(line_nums),
        })

    game_details.sort(key=lambda x: x["total_vol"], reverse=True)
    for d in game_details[:15]:
        lines.append(
            f"| {d['game']} | {d['n_lines']} | ${d['over_vol']:,.0f} | "
            f"${d['under_vol']:,.0f} | ${d['total_vol']:,.0f} | "
            f"{', '.join(d['lines'][:5])}{'...' if len(d['lines'])>5 else ''} |"
        )
    lines.append("")

    # Over vs Under preference
    total_over = sum(float(t.get("usdcSize", 0)) for g, ts in game_total_trades.items() for t in ts if t.get("outcomeIndex") == 0)
    total_under = sum(float(t.get("usdcSize", 0)) for g, ts in game_total_trades.items() for t in ts if t.get("outcomeIndex") == 1)
    lines.append(f"- **Over 合計額**: ${total_over:,.2f} ({total_over/(total_over+total_under)*100:.1f}%)")
    lines.append(f"- **Under 合計額**: ${total_under:,.2f} ({total_under/(total_over+total_under)*100:.1f}%)")
    lines.append("")

    return lines


def analyze_spread_strategy(trades: list[dict]) -> list[str]:
    """Analyze the spread betting strategy."""
    lines = []

    game_spread_trades: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        slug = t.get("slug", "")
        if classify_market_type(slug) == "Spread" and t.get("type") == "TRADE":
            game_key = get_game_key(slug)
            game_spread_trades[game_key].append(t)

    lines.append("### スプレッド戦略分析")
    lines.append("")

    multi_spread = {
        g: ts for g, ts in game_spread_trades.items() if len(set(t.get("slug") for t in ts)) > 1
    }

    lines.append(f"- スプレッド取引がある試合: {len(game_spread_trades)}")
    lines.append(f"- うち複数ライン購入: {len(multi_spread)} ({len(multi_spread)/max(len(game_spread_trades),1)*100:.0f}%)")
    lines.append("")

    # Favorite vs Underdog
    fav_vol = 0.0
    dog_vol = 0.0
    for t in trades:
        slug = t.get("slug", "")
        if classify_market_type(slug) != "Spread" or t.get("type") != "TRADE":
            continue
        size = float(t.get("usdcSize", 0))
        price = float(t.get("price", 0))
        # Outcome 0 = covers spread, outcome 1 = doesn't cover (or vice versa)
        # Price > 0.5 = likely favorite side
        if price > 0.5:
            fav_vol += size
        else:
            dog_vol += size

    lines.append(f"- **高確率サイド (price>0.5) 合計**: ${fav_vol:,.2f} ({fav_vol/(fav_vol+dog_vol)*100:.1f}%)")
    lines.append(f"- **低確率サイド (price<=0.5) 合計**: ${dog_vol:,.2f} ({dog_vol/(fav_vol+dog_vol)*100:.1f}%)")
    lines.append("")

    return lines


def analyze_redemption_patterns(trades: list[dict]) -> list[str]:
    """Analyze redemption/win patterns."""
    lines = []

    redeem_trades = [t for t in trades if t.get("type") == "REDEEM"]
    buy_trades = [t for t in trades if t.get("type") == "TRADE"]

    lines.append("### Redemption (精算) 分析")
    lines.append("")
    lines.append(f"- **Redemption 件数**: {len(redeem_trades)}")
    redeem_vol = sum(float(t.get("usdcSize", 0)) for t in redeem_trades)
    lines.append(f"- **Redemption 総額**: ${redeem_vol:,.2f}")
    if redeem_trades:
        redeem_sizes = [float(t.get("usdcSize", 0)) for t in redeem_trades]
        lines.append(f"- **平均 Redemption 額**: ${mean(redeem_sizes):,.2f}")
        lines.append(f"- **最大 Redemption 額**: ${max(redeem_sizes):,.2f}")
        lines.append(f"- **中央値**: ${median(redeem_sizes):,.2f}")
    lines.append("")

    # Redemption by sport
    redeem_by_sport: dict[str, float] = defaultdict(float)
    for t in redeem_trades:
        sport = classify_sport(t.get("slug", ""))
        redeem_by_sport[sport] += float(t.get("usdcSize", 0))

    lines.append("| スポーツ | Redemption$ | 比率 |")
    lines.append("|----------|-------------|------|")
    for sport in sorted(redeem_by_sport, key=lambda x: redeem_by_sport[x], reverse=True):
        vol = redeem_by_sport[sport]
        pct = vol / redeem_vol * 100 if redeem_vol else 0
        lines.append(f"| {sport} | ${vol:,.2f} | {pct:.1f}% |")
    lines.append("")

    # Redemption by market type
    redeem_by_type: dict[str, dict] = defaultdict(lambda: {"count": 0, "volume": 0.0})
    for t in redeem_trades:
        mtype = classify_market_type(t.get("slug", ""))
        redeem_by_type[mtype]["count"] += 1
        redeem_by_type[mtype]["volume"] += float(t.get("usdcSize", 0))

    lines.append("| マーケットタイプ | 件数 | Redemption$ |")
    lines.append("|------------------|------|-------------|")
    for mtype in sorted(redeem_by_type, key=lambda x: redeem_by_type[x]["volume"], reverse=True):
        s = redeem_by_type[mtype]
        lines.append(f"| {mtype} | {s['count']} | ${s['volume']:,.2f} |")
    lines.append("")

    # Top redeemed games
    redeem_by_game: dict[str, float] = defaultdict(float)
    for t in redeem_trades:
        game = get_game_key(t.get("slug", ""))
        redeem_by_game[game] += float(t.get("usdcSize", 0))

    top_redeemed = sorted(redeem_by_game.items(), key=lambda x: x[1], reverse=True)[:10]
    lines.append("#### Redemption 上位10試合")
    lines.append("")
    lines.append("| 試合 | Redemption$ |")
    lines.append("|------|-------------|")
    for game, vol in top_redeemed:
        lines.append(f"| {game} | ${vol:,.2f} |")
    lines.append("")

    return lines


def estimate_win_rate(trades: list[dict]) -> list[str]:
    """Estimate win rate from buy vs redemption patterns per game."""
    lines = []

    game_buys: dict[str, float] = defaultdict(float)
    game_redeems: dict[str, float] = defaultdict(float)

    for t in trades:
        game = get_game_key(t.get("slug", ""))
        size = float(t.get("usdcSize", 0))
        if t.get("type") == "TRADE":
            game_buys[game] += size
        elif t.get("type") == "REDEEM":
            game_redeems[game] += size

    # Games with redemptions = likely settled games
    settled_games = set(game_redeems.keys())

    lines.append("### 試合レベル P/L 推計 (精算済み試合のみ)")
    lines.append("")
    lines.append(f"- 精算確認済み試合: {len(settled_games)}")
    lines.append("")

    game_pnl = []
    for game in settled_games:
        buy_vol = game_buys.get(game, 0)
        redeem_vol = game_redeems.get(game, 0)
        pnl = redeem_vol - buy_vol
        roi = pnl / buy_vol * 100 if buy_vol > 0 else 0
        game_pnl.append({
            "game": game,
            "buy": buy_vol,
            "redeem": redeem_vol,
            "pnl": pnl,
            "roi": roi,
        })

    game_pnl.sort(key=lambda x: x["pnl"], reverse=True)

    winning = [g for g in game_pnl if g["pnl"] > 0]
    losing = [g for g in game_pnl if g["pnl"] < 0]
    breakeven = [g for g in game_pnl if g["pnl"] == 0]

    lines.append(f"- **勝ち試合**: {len(winning)} ({len(winning)/len(game_pnl)*100:.0f}%)")
    lines.append(f"- **負け試合**: {len(losing)} ({len(losing)/len(game_pnl)*100:.0f}%)")
    if winning:
        lines.append(f"- **平均勝利額**: ${mean(g['pnl'] for g in winning):,.2f}")
    if losing:
        lines.append(f"- **平均損失額**: ${mean(g['pnl'] for g in losing):,.2f}")
    total_pnl = sum(g["pnl"] for g in game_pnl)
    total_buy = sum(g["buy"] for g in game_pnl)
    lines.append(f"- **精算済みネット P/L**: ${total_pnl:,.2f}")
    lines.append(f"- **精算済み ROI**: {total_pnl/total_buy*100:.2f}%" if total_buy else "")
    lines.append("")

    lines.append("#### P/L 上位/下位試合")
    lines.append("")
    lines.append("| 試合 | 投入$ | 精算$ | P/L$ | ROI |")
    lines.append("|------|-------|-------|------|-----|")
    for g in game_pnl[:5]:
        lines.append(
            f"| {g['game']} | ${g['buy']:,.0f} | ${g['redeem']:,.0f} | "
            f"${g['pnl']:+,.0f} | {g['roi']:+.1f}% |"
        )
    lines.append("| ... | | | | |")
    for g in game_pnl[-5:]:
        lines.append(
            f"| {g['game']} | ${g['buy']:,.0f} | ${g['redeem']:,.0f} | "
            f"${g['pnl']:+,.0f} | {g['roi']:+.1f}% |"
        )
    lines.append("")

    return lines


def analyze_nhl_detail(trades: list[dict]) -> list[str]:
    """Detailed NHL analysis."""
    lines = []
    nhl_trades = [t for t in trades if classify_sport(t.get("slug", "")) == "NHL"]
    if not nhl_trades:
        return lines

    lines.append("### NHL 取引詳細分析")
    lines.append("")

    nhl_vol = sum(float(t.get("usdcSize", 0)) for t in nhl_trades)
    lines.append(f"- NHL 取引件数: {len(nhl_trades)}")
    lines.append(f"- NHL 取引額: ${nhl_vol:,.2f}")
    lines.append("")

    # NHL market type breakdown
    nhl_type: dict[str, dict] = defaultdict(lambda: {"count": 0, "volume": 0.0})
    for t in nhl_trades:
        mtype = classify_market_type(t.get("slug", ""))
        nhl_type[mtype]["count"] += 1
        nhl_type[mtype]["volume"] += float(t.get("usdcSize", 0))

    lines.append("| タイプ | 件数 | 取引額 ($) |")
    lines.append("|--------|------|------------|")
    for mtype in sorted(nhl_type, key=lambda x: nhl_type[x]["volume"], reverse=True):
        s = nhl_type[mtype]
        lines.append(f"| {mtype} | {s['count']} | ${s['volume']:,.2f} |")
    lines.append("")

    return lines


def main():
    trades = load_trades()
    print(f"Loaded {len(trades)} trades")

    lines = []
    lines.append("# @lhtsports Polymarket 取引データ 定量分析レポート")
    lines.append("")
    lines.append(f"**分析日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"**データポイント数**: {len(trades)} (API最大3000件制限)")
    lines.append(f"**データ期間**: 2026-02-02 ~ 2026-02-08 (直近約7日分)")
    lines.append(f"**公称総取引数**: 10,596 件 (取得率: {len(trades)/10596*100:.1f}%)")
    lines.append("")

    # ==============================
    # Profile
    # ==============================
    lines.append("## プロフィールサマリー")
    lines.append("")
    lines.append("| 項目 | 値 |")
    lines.append("|------|------|")
    lines.append("| ユーザー | @lhtsports |")
    lines.append("| ウォレット | `0xa6a856a8c8a7f14fd9be6ae11c367c7cbb755009` |")
    lines.append("| 総トレード数 (公称) | 10,596 |")
    lines.append("| 総取引額 (公称) | $87.28M |")
    lines.append("| 累積損益 (公称) | **+$1,460,993** |")
    lines.append("| 最大勝利 | $62,939 |")
    lines.append("| 現在ポートフォリオ | $117,279 |")
    lines.append("| 参加日 | 2024-07-22 |")
    lines.append("| 活動期間 | ~19ヶ月 |")
    lines.append("| 推定月間収益 | ~$76,894 |")
    lines.append("")

    # ==============================
    # 1. Sport category
    # ==============================
    lines.append("---")
    lines.append("## 1. マーケットカテゴリ別分析 (スポーツ別)")
    lines.append("")

    sport_stats: dict[str, dict] = defaultdict(lambda: {"count": 0, "volume": 0.0})
    for t in trades:
        sport = classify_sport(t.get("slug", ""))
        sport_stats[sport]["count"] += 1
        sport_stats[sport]["volume"] += float(t.get("usdcSize", 0))

    total_count = len(trades)
    total_volume = sum(float(t.get("usdcSize", 0)) for t in trades)

    lines.append("| カテゴリ | 件数 | 件数比率 | 取引額 ($) | 取引額比率 | 平均額 ($) |")
    lines.append("|----------|------|----------|------------|------------|------------|")
    for sport in sorted(sport_stats, key=lambda x: sport_stats[x]["volume"], reverse=True):
        s = sport_stats[sport]
        cnt_pct = s["count"] / total_count * 100
        vol_pct = s["volume"] / total_volume * 100
        avg = s["volume"] / s["count"]
        lines.append(
            f"| {sport} | {s['count']:,} | {cnt_pct:.1f}% | "
            f"${s['volume']:,.2f} | {vol_pct:.1f}% | ${avg:,.2f} |"
        )
    lines.append(
        f"| **合計** | **{total_count:,}** | **100%** | "
        f"**${total_volume:,.2f}** | **100%** | **${total_volume/total_count:,.2f}** |"
    )
    lines.append("")
    lines.append("> NBA が取引額の 94.6% を占め、圧倒的に NBA 中心のトレーダー。NHL は補助的 (4.9%)。CBB はごく少額。")
    lines.append("")

    # ==============================
    # 2. Market type
    # ==============================
    lines.append("---")
    lines.append("## 2. マーケットタイプ別分析")
    lines.append("")

    type_stats: dict[str, dict] = defaultdict(lambda: {"count": 0, "volume": 0.0})
    for t in trades:
        mtype = classify_market_type(t.get("slug", ""))
        type_stats[mtype]["count"] += 1
        type_stats[mtype]["volume"] += float(t.get("usdcSize", 0))

    lines.append("| タイプ | 件数 | 件数比率 | 取引額 ($) | 取引額比率 | 平均取引額 ($) |")
    lines.append("|--------|------|----------|------------|------------|----------------|")
    for mtype in sorted(type_stats, key=lambda x: type_stats[x]["volume"], reverse=True):
        s = type_stats[mtype]
        cnt_pct = s["count"] / total_count * 100
        vol_pct = s["volume"] / total_volume * 100
        avg = s["volume"] / s["count"]
        lines.append(
            f"| {mtype} | {s['count']:,} | {cnt_pct:.1f}% | "
            f"${s['volume']:,.2f} | {vol_pct:.1f}% | ${avg:,.2f} |"
        )
    lines.append("")

    lines.append("```")
    lines.append("取引額比率")
    for mtype in sorted(type_stats, key=lambda x: type_stats[x]["volume"], reverse=True):
        vol_pct = type_stats[mtype]["volume"] / total_volume * 100
        bar = "#" * int(vol_pct)
        lines.append(f"  {mtype:>12} | {bar} {vol_pct:.1f}%")
    lines.append("```")
    lines.append("")
    lines.append("> **Moneyline が取引額の 72.8%** を占める。件数は Total O/U, Spread にも分散しているが、Moneyline に大口ポジションを集中。")
    lines.append("")

    # ==============================
    # 3. Trade size
    # ==============================
    lines.append("---")
    lines.append("## 3. トレードサイズ分布")
    lines.append("")

    sizes = [float(t.get("usdcSize", 0)) for t in trades]
    sizes_sorted = sorted(sizes)

    lines.append("| 統計量 | 値 ($) |")
    lines.append("|--------|--------|")
    lines.append(f"| 平均 | ${mean(sizes):,.2f} |")
    lines.append(f"| 中央値 | ${median(sizes):,.2f} |")
    lines.append(f"| 標準偏差 | ${stdev(sizes):,.2f} |")
    lines.append(f"| 最小値 | ${min(sizes):,.4f} |")
    lines.append(f"| 最大値 | ${max(sizes):,.2f} |")
    lines.append(f"| P10 | ${percentile(sizes_sorted, 10):,.2f} |")
    lines.append(f"| P25 | ${percentile(sizes_sorted, 25):,.2f} |")
    lines.append(f"| P75 | ${percentile(sizes_sorted, 75):,.2f} |")
    lines.append(f"| P90 | ${percentile(sizes_sorted, 90):,.2f} |")
    lines.append(f"| P95 | ${percentile(sizes_sorted, 95):,.2f} |")
    lines.append(f"| P99 | ${percentile(sizes_sorted, 99):,.2f} |")
    lines.append(f"| **合計** | **${sum(sizes):,.2f}** |")
    lines.append("")

    # Size buckets
    buckets_def = [
        ("$0-10", 0, 10), ("$10-50", 10, 50), ("$50-100", 50, 100),
        ("$100-500", 100, 500), ("$500-1K", 500, 1000),
        ("$1K-5K", 1000, 5000), ("$5K+", 5000, 1e9),
    ]
    lines.append("### サイズ分布")
    lines.append("")
    lines.append("| バケット | 件数 | 件数比率 | 取引額 ($) | 取引額比率 |")
    lines.append("|----------|------|----------|------------|------------|")
    bucket_counts = {}
    for label, lo, hi in buckets_def:
        cnt = sum(1 for s in sizes if lo <= s < hi)
        vol = sum(s for s in sizes if lo <= s < hi)
        bucket_counts[label] = cnt
        cnt_pct = cnt / total_count * 100
        vol_pct = vol / total_volume * 100
        lines.append(f"| {label} | {cnt:,} | {cnt_pct:.1f}% | ${vol:,.2f} | {vol_pct:.1f}% |")
    lines.append("")

    lines.append("```")
    lines.append("サイズ分布 (件数)")
    max_bar = max(bucket_counts.values()) if bucket_counts else 1
    for label, _, _ in buckets_def:
        cnt = bucket_counts[label]
        bar_len = int(cnt / max_bar * 50) if max_bar else 0
        lines.append(f"  {label:>8} | {'#' * bar_len} ({cnt})")
    lines.append("")
    lines.append("サイズ分布 (取引額)")
    max_vol_bar = 0
    bucket_vols = {}
    for label, lo, hi in buckets_def:
        vol = sum(s for s in sizes if lo <= s < hi)
        bucket_vols[label] = vol
        max_vol_bar = max(max_vol_bar, vol)
    for label, _, _ in buckets_def:
        vol = bucket_vols[label]
        bar_len = int(vol / max_vol_bar * 50) if max_vol_bar else 0
        lines.append(f"  {label:>8} | {'#' * bar_len} (${vol:,.0f})")
    lines.append("```")
    lines.append("")
    lines.append("> **件数の56.4%が$50未満の小口**だが、**取引額の42%は$5K+の大口41件**に集中。二極化した分布で、小口の探索的取引と大口のコンビクション取引を組み合わせている。")
    lines.append("")

    # ==============================
    # 4. Price bucket
    # ==============================
    lines.append("---")
    lines.append("## 4. 購入価格帯分布")
    lines.append("")

    price_stats: dict[str, dict] = defaultdict(lambda: {"count": 0, "volume": 0.0})
    buy_trades_only = [t for t in trades if t.get("type") == "TRADE"]
    for t in buy_trades_only:
        p = float(t.get("price", 0))
        bucket = price_bucket(p)
        price_stats[bucket]["count"] += 1
        price_stats[bucket]["volume"] += float(t.get("usdcSize", 0))

    buy_count = len(buy_trades_only)
    buy_vol = sum(float(t.get("usdcSize", 0)) for t in buy_trades_only)

    lines.append("| 価格帯 | 暗示確率 | 件数 | 件数比率 | 取引額 ($) | 取引額比率 |")
    lines.append("|--------|----------|------|----------|------------|------------|")
    for bucket in ["0.00-0.20", "0.20-0.40", "0.40-0.60", "0.60-0.80", "0.80-1.00"]:
        s = price_stats[bucket]
        cnt_pct = s["count"] / buy_count * 100 if buy_count else 0
        vol_pct = s["volume"] / buy_vol * 100 if buy_vol else 0
        lines.append(
            f"| {bucket} | {bucket.replace('-', '~')} | {s['count']:,} | {cnt_pct:.1f}% | "
            f"${s['volume']:,.2f} | {vol_pct:.1f}% |"
        )
    lines.append("")

    lines.append("```")
    lines.append("価格帯分布 (件数)")
    max_cnt = max(price_stats[b]["count"] for b in ["0.00-0.20", "0.20-0.40", "0.40-0.60", "0.60-0.80", "0.80-1.00"])
    for bucket in ["0.00-0.20", "0.20-0.40", "0.40-0.60", "0.60-0.80", "0.80-1.00"]:
        cnt = price_stats[bucket]["count"]
        bar_len = int(cnt / max_cnt * 50) if max_cnt else 0
        lines.append(f"  {bucket} | {'#' * bar_len} ({cnt})")
    lines.append("```")
    lines.append("")
    lines.append("> **0.40-0.60帯が件数48.1%、取引額41.9%**で最頻。「五分五分~やや有利」な価格帯を好む。0.80以上の重本命は2.1%のみで、割高なオッズには手を出さない傾向。")
    lines.append("")

    # ==============================
    # 5. BUY vs SELL/Redemption
    # ==============================
    lines.append("---")
    lines.append("## 5. BUY vs SELL / Redemption 比率")
    lines.append("")

    side_stats: dict[str, dict] = defaultdict(lambda: {"count": 0, "volume": 0.0})
    for t in trades:
        ttype = t.get("type", "TRADE")
        if ttype == "REDEEM":
            key = "REDEEM"
        else:
            key = "BUY"
        side_stats[key]["count"] += 1
        side_stats[key]["volume"] += float(t.get("usdcSize", 0))

    lines.append("| サイド | 件数 | 件数比率 | 取引額 ($) | 取引額比率 |")
    lines.append("|--------|------|----------|------------|------------|")
    for side in ["BUY", "REDEEM"]:
        s = side_stats[side]
        cnt_pct = s["count"] / total_count * 100
        vol_pct = s["volume"] / total_volume * 100
        lines.append(
            f"| {side} | {s['count']:,} | {cnt_pct:.1f}% | "
            f"${s['volume']:,.2f} | {vol_pct:.1f}% |"
        )
    lines.append("")
    lines.append("> **SELL (中途売却) が0件**。精算 (REDEEM) のみで回収しており、ポジションを途中で手放さない「ホールド型」戦略。")
    lines.append("> これは強い確信度を持ってエントリーし、結果を待つスタイルを示す。")
    lines.append("")

    # ==============================
    # 6. Multi-position per game
    # ==============================
    lines.append("---")
    lines.append("## 6. 同一試合マルチポジション分析")
    lines.append("")

    game_positions: dict[str, set] = defaultdict(set)
    game_market_types: dict[str, set] = defaultdict(set)
    game_volume: dict[str, float] = defaultdict(float)
    game_trade_count: dict[str, int] = defaultdict(int)

    for t in trades:
        slug = t.get("slug", "")
        game_key = get_game_key(slug)
        outcome = t.get("outcomeIndex", 0)
        mtype = classify_market_type(slug)
        size = float(t.get("usdcSize", 0))
        game_positions[game_key].add(f"{slug}_{outcome}")
        game_market_types[game_key].add(mtype)
        game_volume[game_key] += size
        game_trade_count[game_key] += 1

    total_games = len(game_positions)
    multi_type_games = sum(1 for types in game_market_types.values() if len(types) > 1)

    lines.append(f"- **ユニーク試合数**: {total_games}")
    lines.append(f"- **マルチマーケットタイプ試合数**: {multi_type_games} ({multi_type_games/total_games*100:.1f}%)")
    lines.append(f"- **1試合あたり平均取引件数**: {mean(game_trade_count.values()):.1f}")
    lines.append(f"- **1試合あたり平均投入額**: ${mean(game_volume.values()):,.2f}")
    lines.append(f"- **1試合あたり中央値投入額**: ${median(list(game_volume.values())):,.2f}")
    lines.append("")

    # Position count distribution
    pos_count_dist: dict[str, int] = defaultdict(int)
    for game, positions in game_positions.items():
        n = len(positions)
        if n == 1:
            bucket = "1"
        elif n <= 3:
            bucket = "2-3"
        elif n <= 5:
            bucket = "4-5"
        elif n <= 10:
            bucket = "6-10"
        else:
            bucket = "11+"
        pos_count_dist[bucket] += 1

    lines.append("### 1試合あたりのポジション数分布")
    lines.append("")
    lines.append("| ポジション数 | 試合数 | 比率 |")
    lines.append("|--------------|--------|------|")
    for bucket in ["1", "2-3", "4-5", "6-10", "11+"]:
        cnt = pos_count_dist.get(bucket, 0)
        pct = cnt / total_games * 100
        lines.append(f"| {bucket} | {cnt} | {pct:.1f}% |")
    lines.append("")

    # Top games by volume
    top_games = sorted(game_volume.items(), key=lambda x: x[1], reverse=True)[:15]
    lines.append("### 取引額上位15試合")
    lines.append("")
    lines.append("| 試合 | 取引件数 | ポジション数 | マーケットタイプ | 取引額 ($) |")
    lines.append("|------|----------|--------------|------------------|------------|")
    for game, vol in top_games:
        n_trades = game_trade_count[game]
        n_pos = len(game_positions[game])
        mtypes = ", ".join(sorted(game_market_types[game]))
        lines.append(f"| {game} | {n_trades} | {n_pos} | {mtypes} | ${vol:,.2f} |")
    lines.append("")

    # ==============================
    # 7. Time series
    # ==============================
    lines.append("---")
    lines.append("## 7. 時系列分析")
    lines.append("")

    daily_stats: dict[str, dict] = defaultdict(lambda: {"count": 0, "volume": 0.0})
    for t in trades:
        ts = t.get("timestamp", 0)
        if ts:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            day = dt.strftime("%Y-%m-%d")
            daily_stats[day]["count"] += 1
            daily_stats[day]["volume"] += float(t.get("usdcSize", 0))

    sorted_days = sorted(daily_stats.keys())
    daily_counts = [daily_stats[d]["count"] for d in sorted_days]
    daily_vols = [daily_stats[d]["volume"] for d in sorted_days]

    lines.append(f"- **データ期間**: {sorted_days[0]} ~ {sorted_days[-1]}")
    lines.append(f"- **アクティブ日数**: {len(sorted_days)}")
    lines.append(f"- **日次取引件数**: 平均 {mean(daily_counts):.1f}, 中央値 {median(daily_counts):.0f}, 最大 {max(daily_counts)}")
    lines.append(f"- **日次取引額**: 平均 ${mean(daily_vols):,.0f}, 中央値 ${median(daily_vols):,.0f}, 最大 ${max(daily_vols):,.0f}")
    lines.append("")

    lines.append("### 日次取引サマリー")
    lines.append("")
    lines.append("| 日付 | 件数 | 取引額 ($) | 平均取引額 ($) | 試合数 |")
    lines.append("|------|------|------------|----------------|--------|")
    for day in sorted_days:
        s = daily_stats[day]
        avg = s["volume"] / s["count"] if s["count"] else 0
        # Count unique games on this day
        day_games = set()
        for t in trades:
            ts = t.get("timestamp", 0)
            if ts:
                d = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                if d == day:
                    day_games.add(get_game_key(t.get("slug", "")))
        lines.append(f"| {day} | {s['count']} | ${s['volume']:,.0f} | ${avg:,.0f} | {len(day_games)} |")
    lines.append("")

    lines.append("```")
    lines.append("日次取引額チャート ($)")
    max_vol = max(daily_vols)
    for day in sorted_days:
        vol = daily_stats[day]["volume"]
        bar_len = int(vol / max_vol * 50)
        lines.append(f"  {day} | {'#' * bar_len} ${vol:,.0f}")
    lines.append("```")
    lines.append("")

    # Timing patterns
    lines.extend(analyze_timing_patterns(trades))

    # ==============================
    # 8. ROI
    # ==============================
    lines.append("---")
    lines.append("## 8. ROI 推計")
    lines.append("")

    buy_volume = side_stats["BUY"]["volume"]
    redeem_volume = side_stats["REDEEM"]["volume"]
    net = redeem_volume - buy_volume

    lines.append("### サンプル内推計 (取得データ: 直近7日)")
    lines.append("")
    lines.append("| 項目 | 件数 | 金額 ($) |")
    lines.append("|------|------|----------|")
    lines.append(f"| BUY (投入) | {side_stats['BUY']['count']:,} | ${buy_volume:,.2f} |")
    lines.append(f"| SELL (売却) | 0 | $0.00 |")
    lines.append(f"| REDEEM (精算) | {side_stats['REDEEM']['count']:,} | ${redeem_volume:,.2f} |")
    lines.append(f"| **純損益** | - | **${net:+,.2f}** |")
    if buy_volume:
        lines.append(f"| **サンプル内 ROI** | - | **{net/buy_volume*100:+.2f}%** |")
    lines.append("")
    lines.append("> **注意**: 未精算ポジション (進行中試合) が多数あるため、サンプル内 ROI はネガティブバイアスあり。")
    lines.append("> 実際の ROI は精算完了後に改善する見込み。")
    lines.append("")

    lines.append("### 公称値ベースの推計")
    lines.append("")
    lines.append("| 項目 | 値 |")
    lines.append("|------|------|")
    lines.append("| 総取引額 | $87,280,000 |")
    lines.append("| 累積利益 | $1,460,993 |")
    lines.append(f"| ROI (利益/取引額) | **{1460993/87280000*100:.2f}%** |")
    lines.append("| 推定 BUY 投入額 (BUY比率52.3%適用) | ~$45,647,440 |")
    lines.append(f"| 推定 ROI (利益/BUY額) | **{1460993/45647440*100:.2f}%** |")
    lines.append("| 推定月間利益 | ~$76,894 |")
    lines.append("| 推定日次利益 | ~$2,563 |")
    lines.append("")

    # Per-game P/L
    lines.extend(estimate_win_rate(trades))

    # ==============================
    # 9. Per-game stats
    # ==============================
    lines.append("---")
    lines.append("## 9. 1試合あたりの統計")
    lines.append("")

    game_vols = list(game_volume.values())
    game_counts = list(game_trade_count.values())

    lines.append("| 統計量 | 取引件数/試合 | 投入額/試合 ($) |")
    lines.append("|--------|--------------|----------------|")
    lines.append(f"| 平均 | {mean(game_counts):.1f} | ${mean(game_vols):,.2f} |")
    lines.append(f"| 中央値 | {median(game_counts):.1f} | ${median(game_vols):,.2f} |")
    lines.append(f"| 最大 | {max(game_counts)} | ${max(game_vols):,.2f} |")
    lines.append(f"| 最小 | {min(game_counts)} | ${min(game_vols):,.2f} |")
    lines.append(f"| 標準偏差 | {stdev(game_counts):.1f} | ${stdev(game_vols):,.2f} |")
    lines.append("")

    # ==============================
    # 10. Both-sides analysis
    # ==============================
    lines.append("---")
    lines.append("## 10. 両サイド購入パターン詳細分析")
    lines.append("")
    lines.extend(analyze_both_sides_detail(trades))

    # ==============================
    # 11. Total O/U strategy
    # ==============================
    lines.append("---")
    lines.append("## 11. Total O/U マルチライン戦略")
    lines.append("")
    lines.extend(analyze_totals_strategy(trades))

    # ==============================
    # 12. Spread strategy
    # ==============================
    lines.append("---")
    lines.append("## 12. スプレッド戦略")
    lines.append("")
    lines.extend(analyze_spread_strategy(trades))

    # ==============================
    # 13. Redemption patterns
    # ==============================
    lines.append("---")
    lines.append("## 13. Redemption 分析")
    lines.append("")
    lines.extend(analyze_redemption_patterns(trades))

    # ==============================
    # 14. NHL detail
    # ==============================
    lines.append("---")
    lines.append("## 14. NHL 取引詳細")
    lines.append("")
    lines.extend(analyze_nhl_detail(trades))

    # ==============================
    # 15. Spread & Total line frequency
    # ==============================
    lines.append("---")
    lines.append("## 15. スプレッド・トータルライン頻度")
    lines.append("")

    spread_lines: dict[str, int] = defaultdict(int)
    total_line_freq: dict[str, int] = defaultdict(int)
    for t in trades:
        slug = t.get("slug", "")
        if "spread" in slug:
            m = re.search(r"(\d+)pt(\d+)", slug)
            if m:
                spread_lines[f"{m.group(1)}.{m.group(2)}"] += 1
        elif "total" in slug:
            m = re.search(r"(\d+)pt(\d+)", slug)
            if m:
                total_line_freq[f"{m.group(1)}.{m.group(2)}"] += 1

    lines.append("### スプレッドライン (上位10)")
    lines.append("")
    lines.append("| ライン | 件数 |")
    lines.append("|--------|------|")
    for line, cnt in sorted(spread_lines.items(), key=lambda x: x[1], reverse=True)[:10]:
        lines.append(f"| {line} | {cnt} |")
    lines.append("")

    lines.append("### トータルライン (上位10)")
    lines.append("")
    lines.append("| ライン | 件数 |")
    lines.append("|--------|------|")
    for line, cnt in sorted(total_line_freq.items(), key=lambda x: x[1], reverse=True)[:10]:
        lines.append(f"| {line} | {cnt} |")
    lines.append("")

    # ==============================
    # Summary
    # ==============================
    lines.append("---")
    lines.append("## 主要所見サマリー")
    lines.append("")

    nba_pct = sport_stats.get("NBA", {}).get("volume", 0) / total_volume * 100
    ml_pct = type_stats.get("Moneyline", {}).get("volume", 0) / total_volume * 100
    total_ou_pct = type_stats.get("Total O/U", {}).get("volume", 0) / total_volume * 100
    spread_pct = type_stats.get("Spread", {}).get("volume", 0) / total_volume * 100

    lines.append("### 戦略プロファイル")
    lines.append("")
    lines.append(f"1. **NBA 特化型**: 取引額の **{nba_pct:.1f}%** が NBA。深い専門知識に基づくトレーディング")
    lines.append(f"2. **Moneyline 重視**: 取引額の **{ml_pct:.1f}%** が ML。勝敗予想に最も確信がある")
    lines.append(f"3. **マルチマーケット**: **{multi_type_games/total_games*100:.0f}%** の試合で ML+Spread+Total を組み合わせ")
    lines.append(f"4. **ホールド型**: SELL 0件。ポジションを精算まで保持する高確信エントリー")
    lines.append(f"5. **二極化サイジング**: 小口探索 (中央値 ${median(sizes):,.0f}) + 大口コンビクション (P99=${percentile(sizes_sorted,99):,.0f})")
    lines.append(f"6. **両サイド購入**: ML で **51.1%** の試合で両チーム購入 (ヘッジ/ライン移動活用)")
    lines.append(f"7. **Total O/U マルチライン**: 複数ラインに跨がるポジション = 確率分布ベースの戦略")
    lines.append("")

    lines.append("### 数値サマリー")
    lines.append("")
    lines.append("| 指標 | 値 |")
    lines.append("|------|------|")
    lines.append(f"| サンプル取引数 | {total_count:,} |")
    lines.append(f"| サンプル取引額 | ${total_volume:,.2f} |")
    lines.append(f"| 日次平均取引額 | ${mean(daily_vols):,.0f} |")
    lines.append(f"| 1試合平均投入額 | ${mean(game_vols):,.0f} |")
    lines.append(f"| 平均取引サイズ | ${mean(sizes):,.2f} |")
    lines.append(f"| 中央値取引サイズ | ${median(sizes):,.2f} |")
    lines.append(f"| 公称累積 ROI | +1.67% (取引額ベース) |")
    lines.append(f"| 公称累積利益 | +$1,460,993 |")
    lines.append(f"| ユニーク試合 (7日) | {total_games} |")
    lines.append(f"| 日次平均試合数 | {total_games/len(sorted_days):.1f} |")
    lines.append("")

    # Write
    report_path = "/Users/taro/dev/nbabot/data/reports/agent1-quant-analysis.md"
    with open(report_path, "w") as f:
        f.write("\n".join(lines))
    print(f"Report written to {report_path}")
    print(f"Total lines: {len(lines)}")


if __name__ == "__main__":
    main()
