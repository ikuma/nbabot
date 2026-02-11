"""Analyze @lhtsports Polymarket trading data."""

import json
import time
import urllib.request
import urllib.error
from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean, median

BASE_URL = "https://data-api.polymarket.com/activity"
USER = "0xa6a856a8c8a7f14fd9be6ae11c367c7cbb755009"
LIMIT = 50


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Accept": "application/json",
}


def fetch_trades(offset: int) -> list[dict]:
    """Fetch trades at given offset."""
    url = f"{BASE_URL}?user={USER}&limit={LIMIT}&offset={offset}"
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            return data if isinstance(data, list) else []
    except Exception as e:
        print(f"  Error at offset {offset}: {e}")
        return []


def collect_all_trades() -> list[dict]:
    """Collect all trades by paginating through the API."""
    all_trades = []
    offset = 0
    empty_count = 0

    while empty_count < 3:
        print(f"Fetching offset={offset}...")
        trades = fetch_trades(offset)
        if not trades:
            empty_count += 1
            offset += LIMIT
            continue
        empty_count = 0
        all_trades.extend(trades)
        offset += LIMIT
        time.sleep(0.3)  # Rate limiting

        # Progress
        if offset % 500 == 0:
            print(f"  Collected {len(all_trades)} trades so far...")

    print(f"\nTotal trades collected: {len(all_trades)}")
    return all_trades


def classify_sport(slug: str) -> str:
    """Classify sport from slug prefix."""
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
    elif slug.startswith("soccer-") or slug.startswith("epl-") or slug.startswith("ucl-"):
        return "Soccer"
    else:
        return "Other"


def classify_market_type(slug: str) -> str:
    """Classify market type from slug."""
    if "spread" in slug:
        return "Spread"
    elif "total" in slug or "over" in slug or "under" in slug:
        return "Total O/U"
    else:
        return "Moneyline"


def get_game_key(slug: str) -> str:
    """Extract game key (team matchup + date) from slug."""
    # Remove spread/total suffixes to get base game
    parts = slug.split("-")
    # Find the date portion (YYYY-MM-DD pattern)
    for i in range(len(parts)):
        if len(parts[i]) == 4 and parts[i].isdigit():
            # Found year, date is parts[i:i+3]
            date_end = min(i + 3, len(parts))
            return "-".join(parts[:date_end])
    return slug


def price_bucket(price: float) -> str:
    """Assign price to bucket."""
    if price < 0.2:
        return "0.00-0.20"
    elif price < 0.4:
        return "0.20-0.40"
    elif price < 0.6:
        return "0.40-0.60"
    elif price < 0.8:
        return "0.60-0.80"
    else:
        return "0.80-1.00"


def analyze(trades: list[dict]) -> str:
    """Run full analysis and return markdown report."""
    lines = []
    lines.append("# @lhtsports Polymarket 取引データ 定量分析レポート")
    lines.append("")
    lines.append(f"**分析日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"**データポイント数**: {len(trades)}")
    lines.append("")

    # Profile summary
    lines.append("## プロフィールサマリー")
    lines.append("")
    lines.append("| 項目 | 値 |")
    lines.append("|------|------|")
    lines.append("| ユーザー | @lhtsports |")
    lines.append("| ウォレット | 0xa6a856...755009 |")
    lines.append("| 総トレード数 (公称) | 10,596 |")
    lines.append("| 総取引額 (公称) | $87.28M |")
    lines.append("| 累積損益 (公称) | +$1,460,993 |")
    lines.append("| 最大勝利 | $62,939 |")
    lines.append("| 現在ポートフォリオ | $117,279 |")
    lines.append("| 参加日 | 2024-07-22 |")
    lines.append("")

    # ======================
    # 1. Sport category analysis
    # ======================
    lines.append("## 1. マーケットカテゴリ別分析 (スポーツ別)")
    lines.append("")

    sport_stats: dict[str, dict] = defaultdict(lambda: {"count": 0, "volume": 0.0})
    for t in trades:
        slug = t.get("slug", "")
        sport = classify_sport(slug)
        size = float(t.get("usdcSize", 0))
        sport_stats[sport]["count"] += 1
        sport_stats[sport]["volume"] += size

    total_count = sum(s["count"] for s in sport_stats.values())
    total_volume = sum(s["volume"] for s in sport_stats.values())

    lines.append("| カテゴリ | 件数 | 件数比率 | 取引額 ($) | 取引額比率 |")
    lines.append("|----------|------|----------|------------|------------|")
    for sport in sorted(sport_stats, key=lambda x: sport_stats[x]["volume"], reverse=True):
        s = sport_stats[sport]
        cnt_pct = s["count"] / total_count * 100 if total_count else 0
        vol_pct = s["volume"] / total_volume * 100 if total_volume else 0
        lines.append(
            f"| {sport} | {s['count']:,} | {cnt_pct:.1f}% | "
            f"${s['volume']:,.2f} | {vol_pct:.1f}% |"
        )
    lines.append(
        f"| **合計** | **{total_count:,}** | **100%** | "
        f"**${total_volume:,.2f}** | **100%** |"
    )
    lines.append("")

    # ======================
    # 2. Market type analysis
    # ======================
    lines.append("## 2. マーケットタイプ別分析")
    lines.append("")

    type_stats: dict[str, dict] = defaultdict(lambda: {"count": 0, "volume": 0.0})
    for t in trades:
        slug = t.get("slug", "")
        mtype = classify_market_type(slug)
        size = float(t.get("usdcSize", 0))
        type_stats[mtype]["count"] += 1
        type_stats[mtype]["volume"] += size

    lines.append("| タイプ | 件数 | 件数比率 | 取引額 ($) | 取引額比率 | 平均取引額 ($) |")
    lines.append("|--------|------|----------|------------|------------|----------------|")
    for mtype in sorted(type_stats, key=lambda x: type_stats[x]["volume"], reverse=True):
        s = type_stats[mtype]
        cnt_pct = s["count"] / total_count * 100 if total_count else 0
        vol_pct = s["volume"] / total_volume * 100 if total_volume else 0
        avg = s["volume"] / s["count"] if s["count"] else 0
        lines.append(
            f"| {mtype} | {s['count']:,} | {cnt_pct:.1f}% | "
            f"${s['volume']:,.2f} | {vol_pct:.1f}% | ${avg:,.2f} |"
        )
    lines.append("")

    # ======================
    # 3. Trade size distribution
    # ======================
    lines.append("## 3. トレードサイズ分布")
    lines.append("")

    sizes = [float(t.get("usdcSize", 0)) for t in trades]
    sizes_sorted = sorted(sizes)

    def percentile(data, p):
        k = (len(data) - 1) * p / 100
        f = int(k)
        c = f + 1
        if c >= len(data):
            return data[f]
        return data[f] + (k - f) * (data[c] - data[f])

    lines.append("| 統計量 | 値 ($) |")
    lines.append("|--------|--------|")
    lines.append(f"| 平均 | ${mean(sizes):,.2f} |")
    lines.append(f"| 中央値 | ${median(sizes):,.2f} |")
    lines.append(f"| 最小値 | ${min(sizes):,.2f} |")
    lines.append(f"| 最大値 | ${max(sizes):,.2f} |")
    lines.append(f"| P10 | ${percentile(sizes_sorted, 10):,.2f} |")
    lines.append(f"| P25 | ${percentile(sizes_sorted, 25):,.2f} |")
    lines.append(f"| P75 | ${percentile(sizes_sorted, 75):,.2f} |")
    lines.append(f"| P90 | ${percentile(sizes_sorted, 90):,.2f} |")
    lines.append(f"| P95 | ${percentile(sizes_sorted, 95):,.2f} |")
    lines.append(f"| P99 | ${percentile(sizes_sorted, 99):,.2f} |")
    lines.append(f"| 合計 | ${sum(sizes):,.2f} |")
    lines.append("")

    # Size buckets
    size_buckets = {"$0-10": 0, "$10-50": 0, "$50-100": 0, "$100-500": 0,
                    "$500-1K": 0, "$1K-5K": 0, "$5K+": 0}
    size_bucket_vol = {"$0-10": 0.0, "$10-50": 0.0, "$50-100": 0.0, "$100-500": 0.0,
                       "$500-1K": 0.0, "$1K-5K": 0.0, "$5K+": 0.0}
    for s in sizes:
        if s < 10:
            size_buckets["$0-10"] += 1
            size_bucket_vol["$0-10"] += s
        elif s < 50:
            size_buckets["$10-50"] += 1
            size_bucket_vol["$10-50"] += s
        elif s < 100:
            size_buckets["$50-100"] += 1
            size_bucket_vol["$50-100"] += s
        elif s < 500:
            size_buckets["$100-500"] += 1
            size_bucket_vol["$100-500"] += s
        elif s < 1000:
            size_buckets["$500-1K"] += 1
            size_bucket_vol["$500-1K"] += s
        elif s < 5000:
            size_buckets["$1K-5K"] += 1
            size_bucket_vol["$1K-5K"] += s
        else:
            size_buckets["$5K+"] += 1
            size_bucket_vol["$5K+"] += s

    lines.append("### サイズ分布ヒストグラム")
    lines.append("")
    lines.append("| バケット | 件数 | 件数比率 | 取引額 ($) | 取引額比率 |")
    lines.append("|----------|------|----------|------------|------------|")
    for bucket in ["$0-10", "$10-50", "$50-100", "$100-500", "$500-1K", "$1K-5K", "$5K+"]:
        cnt = size_buckets[bucket]
        vol = size_bucket_vol[bucket]
        cnt_pct = cnt / total_count * 100 if total_count else 0
        vol_pct = vol / total_volume * 100 if total_volume else 0
        lines.append(
            f"| {bucket} | {cnt:,} | {cnt_pct:.1f}% | ${vol:,.2f} | {vol_pct:.1f}% |"
        )
    lines.append("")

    # ASCII histogram
    lines.append("```")
    lines.append("サイズ分布 (件数)")
    max_bar = max(size_buckets.values()) if size_buckets.values() else 1
    for bucket in ["$0-10", "$10-50", "$50-100", "$100-500", "$500-1K", "$1K-5K", "$5K+"]:
        cnt = size_buckets[bucket]
        bar_len = int(cnt / max_bar * 50) if max_bar else 0
        lines.append(f"  {bucket:>8} | {'#' * bar_len} ({cnt})")
    lines.append("```")
    lines.append("")

    # ======================
    # 4. Price bucket distribution
    # ======================
    lines.append("## 4. 購入価格帯分布")
    lines.append("")

    price_buckets: dict[str, dict] = defaultdict(lambda: {"count": 0, "volume": 0.0})
    for t in trades:
        if t.get("side") == "BUY":
            p = float(t.get("price", 0))
            bucket = price_bucket(p)
            size = float(t.get("usdcSize", 0))
            price_buckets[bucket]["count"] += 1
            price_buckets[bucket]["volume"] += size

    buy_total_count = sum(b["count"] for b in price_buckets.values())
    buy_total_vol = sum(b["volume"] for b in price_buckets.values())

    lines.append("| 価格帯 | 件数 | 件数比率 | 取引額 ($) | 取引額比率 | 暗示確率 |")
    lines.append("|--------|------|----------|------------|------------|----------|")
    for bucket in ["0.00-0.20", "0.20-0.40", "0.40-0.60", "0.60-0.80", "0.80-1.00"]:
        b = price_buckets[bucket]
        cnt_pct = b["count"] / buy_total_count * 100 if buy_total_count else 0
        vol_pct = b["volume"] / buy_total_vol * 100 if buy_total_vol else 0
        lines.append(
            f"| {bucket} | {b['count']:,} | {cnt_pct:.1f}% | "
            f"${b['volume']:,.2f} | {vol_pct:.1f}% | {bucket} |"
        )
    lines.append("")

    # ASCII histogram
    lines.append("```")
    lines.append("価格帯分布 (件数)")
    max_bar = max((price_buckets[b]["count"] for b in ["0.00-0.20", "0.20-0.40", "0.40-0.60", "0.60-0.80", "0.80-1.00"]), default=1)
    for bucket in ["0.00-0.20", "0.20-0.40", "0.40-0.60", "0.60-0.80", "0.80-1.00"]:
        cnt = price_buckets[bucket]["count"]
        bar_len = int(cnt / max_bar * 50) if max_bar else 0
        lines.append(f"  {bucket} | {'#' * bar_len} ({cnt})")
    lines.append("```")
    lines.append("")

    # ======================
    # 5. BUY vs SELL/Redemption
    # ======================
    lines.append("## 5. BUY vs SELL / Redemption 比率")
    lines.append("")

    side_stats: dict[str, dict] = defaultdict(lambda: {"count": 0, "volume": 0.0})
    for t in trades:
        side = t.get("side", "UNKNOWN")
        ttype = t.get("type", "TRADE")
        if ttype == "REDEEM" or side == "REDEEM":
            key = "REDEEM"
        else:
            key = side
        size = float(t.get("usdcSize", 0))
        side_stats[key]["count"] += 1
        side_stats[key]["volume"] += size

    lines.append("| サイド | 件数 | 件数比率 | 取引額 ($) | 取引額比率 |")
    lines.append("|--------|------|----------|------------|------------|")
    for side in sorted(side_stats, key=lambda x: side_stats[x]["count"], reverse=True):
        s = side_stats[side]
        cnt_pct = s["count"] / total_count * 100 if total_count else 0
        vol_pct = s["volume"] / total_volume * 100 if total_volume else 0
        lines.append(
            f"| {side} | {s['count']:,} | {cnt_pct:.1f}% | "
            f"${s['volume']:,.2f} | {vol_pct:.1f}% |"
        )
    lines.append("")

    # ======================
    # 6. Multi-position per game
    # ======================
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

    # Count games with multiple market types
    multi_type_games = {g for g, types in game_market_types.items() if len(types) > 1}
    single_type_games = {g for g, types in game_market_types.items() if len(types) == 1}

    total_games = len(game_positions)
    lines.append(f"- **ユニーク試合数**: {total_games}")
    lines.append(f"- **マルチマーケットタイプ試合数**: {len(multi_type_games)} ({len(multi_type_games)/total_games*100:.1f}%)")
    lines.append(f"- **単一マーケットタイプ試合数**: {len(single_type_games)} ({len(single_type_games)/total_games*100:.1f}%)")
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
        pct = cnt / total_games * 100 if total_games else 0
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

    # ======================
    # 7. Time series analysis
    # ======================
    lines.append("## 7. 時系列分析")
    lines.append("")

    daily_stats: dict[str, dict] = defaultdict(lambda: {"count": 0, "volume": 0.0})
    for t in trades:
        ts = t.get("timestamp", 0)
        if ts:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            day = dt.strftime("%Y-%m-%d")
            size = float(t.get("usdcSize", 0))
            daily_stats[day]["count"] += 1
            daily_stats[day]["volume"] += size

    sorted_days = sorted(daily_stats.keys())

    # Summary
    if sorted_days:
        lines.append(f"- **データ期間**: {sorted_days[0]} ~ {sorted_days[-1]}")
        lines.append(f"- **アクティブ日数**: {len(sorted_days)}")
        daily_counts = [daily_stats[d]["count"] for d in sorted_days]
        daily_vols = [daily_stats[d]["volume"] for d in sorted_days]
        lines.append(f"- **日次取引件数**: 平均 {mean(daily_counts):.1f}, 中央値 {median(daily_counts):.1f}, 最大 {max(daily_counts)}")
        lines.append(f"- **日次取引額**: 平均 ${mean(daily_vols):,.2f}, 中央値 ${median(daily_vols):,.2f}, 最大 ${max(daily_vols):,.2f}")
        lines.append("")

    # Recent daily table (last 20 days)
    recent_days = sorted_days[-20:] if len(sorted_days) > 20 else sorted_days
    lines.append("### 直近の日次取引 (最新20日)")
    lines.append("")
    lines.append("| 日付 | 件数 | 取引額 ($) | 平均取引額 ($) |")
    lines.append("|------|------|------------|----------------|")
    for day in recent_days:
        s = daily_stats[day]
        avg = s["volume"] / s["count"] if s["count"] else 0
        lines.append(f"| {day} | {s['count']} | ${s['volume']:,.2f} | ${avg:,.2f} |")
    lines.append("")

    # ASCII volume chart (last 20 days)
    lines.append("```")
    lines.append("日次取引額チャート (直近20日, $)")
    max_vol = max(daily_stats[d]["volume"] for d in recent_days) if recent_days else 1
    for day in recent_days:
        vol = daily_stats[day]["volume"]
        bar_len = int(vol / max_vol * 40) if max_vol else 0
        lines.append(f"  {day} | {'#' * bar_len} ${vol:,.0f}")
    lines.append("```")
    lines.append("")

    # Weekly aggregation
    weekly_stats: dict[str, dict] = defaultdict(lambda: {"count": 0, "volume": 0.0})
    for day in sorted_days:
        dt = datetime.strptime(day, "%Y-%m-%d")
        week = dt.strftime("%Y-W%W")
        weekly_stats[week]["count"] += daily_stats[day]["count"]
        weekly_stats[week]["volume"] += daily_stats[day]["volume"]

    sorted_weeks = sorted(weekly_stats.keys())[-12:]
    lines.append("### 週次取引推移 (直近12週)")
    lines.append("")
    lines.append("| 週 | 件数 | 取引額 ($) |")
    lines.append("|----|------|------------|")
    for week in sorted_weeks:
        s = weekly_stats[week]
        lines.append(f"| {week} | {s['count']} | ${s['volume']:,.2f} |")
    lines.append("")

    # ======================
    # 8. ROI estimation
    # ======================
    lines.append("## 8. ROI 推計")
    lines.append("")

    buy_volume = 0.0
    sell_volume = 0.0
    redeem_volume = 0.0
    buy_count = 0
    sell_count = 0
    redeem_count = 0

    for t in trades:
        side = t.get("side", "")
        ttype = t.get("type", "TRADE")
        size = float(t.get("usdcSize", 0))

        if ttype == "REDEEM" or side == "REDEEM":
            redeem_volume += size
            redeem_count += 1
        elif side == "BUY":
            buy_volume += size
            buy_count += 1
        elif side == "SELL":
            sell_volume += size
            sell_count += 1

    lines.append("### サンプル内推計 (取得データのみ)")
    lines.append("")
    lines.append("| 項目 | 件数 | 金額 ($) |")
    lines.append("|------|------|----------|")
    lines.append(f"| BUY (投入) | {buy_count:,} | ${buy_volume:,.2f} |")
    lines.append(f"| SELL (売却) | {sell_count:,} | ${sell_volume:,.2f} |")
    lines.append(f"| REDEEM (精算) | {redeem_count:,} | ${redeem_volume:,.2f} |")
    lines.append(f"| 回収合計 (SELL+REDEEM) | {sell_count + redeem_count:,} | ${sell_volume + redeem_volume:,.2f} |")
    net = (sell_volume + redeem_volume) - buy_volume
    lines.append(f"| **純損益** | - | **${net:,.2f}** |")
    if buy_volume > 0:
        roi = net / buy_volume * 100
        lines.append(f"| **ROI** | - | **{roi:+.2f}%** |")
    lines.append("")

    lines.append("### 公称値ベースの推計")
    lines.append("")
    lines.append("| 項目 | 値 |")
    lines.append("|------|------|")
    lines.append("| 総取引額 | $87,280,000 |")
    lines.append("| 累積利益 | $1,460,993 |")
    lines.append(f"| ROI (利益/取引額) | {1460993/87280000*100:.2f}% |")
    lines.append("| 推定投入額 (取引額の約半分) | ~$43,640,000 |")
    lines.append(f"| 推定 ROI (利益/投入額) | {1460993/43640000*100:.2f}% |")
    lines.append("")

    # ======================
    # 9. Per-game stats
    # ======================
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
    lines.append("")

    # ======================
    # 10. Outcome Index analysis (both sides of market)
    # ======================
    lines.append("## 10. アウトカム分析 (両サイド購入パターン)")
    lines.append("")

    # For each game, check if both outcomeIndex 0 and 1 are present on moneyline
    both_sides_games = []
    for game_key in game_positions:
        outcomes_by_slug: dict[str, set] = defaultdict(set)
        for t in trades:
            slug = t.get("slug", "")
            if get_game_key(slug) == game_key and classify_market_type(slug) == "Moneyline":
                outcomes_by_slug[slug].add(t.get("outcomeIndex", 0))
        for slug, outcomes in outcomes_by_slug.items():
            if len(outcomes) > 1:
                both_sides_games.append(game_key)
                break

    lines.append(f"- **両サイド購入 (ML) 試合数**: {len(both_sides_games)} / {total_games} ({len(both_sides_games)/total_games*100:.1f}%)")
    lines.append("")
    if both_sides_games:
        lines.append("両サイド購入の例:")
        lines.append("")
        for g in both_sides_games[:10]:
            lines.append(f"  - {g}")
    lines.append("")

    # ======================
    # 11. Spread line clustering
    # ======================
    lines.append("## 11. スプレッド・トータルライン分析")
    lines.append("")

    spread_lines: dict[str, int] = defaultdict(int)
    total_lines: dict[str, int] = defaultdict(int)

    for t in trades:
        slug = t.get("slug", "")
        if "spread" in slug:
            # Extract spread number
            parts = slug.split("-")
            for p in parts:
                if "pt" in p:
                    spread_lines[p.replace("pt", ".")] += 1
        elif "total" in slug:
            parts = slug.split("-")
            for p in parts:
                if "pt" in p:
                    total_lines[p.replace("pt", ".")] += 1

    lines.append("### スプレッドライン頻度 (上位10)")
    lines.append("")
    lines.append("| ライン | 件数 |")
    lines.append("|--------|------|")
    for line, cnt in sorted(spread_lines.items(), key=lambda x: x[1], reverse=True)[:10]:
        lines.append(f"| {line} | {cnt} |")
    lines.append("")

    lines.append("### トータルライン頻度 (上位10)")
    lines.append("")
    lines.append("| ライン | 件数 |")
    lines.append("|--------|------|")
    for line, cnt in sorted(total_lines.items(), key=lambda x: x[1], reverse=True)[:10]:
        lines.append(f"| {line} | {cnt} |")
    lines.append("")

    # ======================
    # Summary and key findings
    # ======================
    lines.append("## 主要所見サマリー")
    lines.append("")

    # Calculate key ratios
    nba_pct = sport_stats.get("NBA", {}).get("volume", 0) / total_volume * 100 if total_volume else 0
    ml_pct = type_stats.get("Moneyline", {}).get("volume", 0) / total_volume * 100 if total_volume else 0
    total_pct = type_stats.get("Total O/U", {}).get("volume", 0) / total_volume * 100 if total_volume else 0
    spread_pct = type_stats.get("Spread", {}).get("volume", 0) / total_volume * 100 if total_volume else 0
    avg_trade = total_volume / total_count if total_count else 0
    multi_pct = len(multi_type_games) / total_games * 100 if total_games else 0

    lines.append(f"1. **NBA 集中**: 取引額の {nba_pct:.1f}% が NBA マーケット")
    lines.append(f"2. **マーケットタイプ**: ML {ml_pct:.1f}%, Total O/U {total_pct:.1f}%, Spread {spread_pct:.1f}%")
    lines.append(f"3. **平均取引額**: ${avg_trade:,.2f} (大口トレーダー)")
    lines.append(f"4. **マルチマーケット戦略**: {multi_pct:.1f}% の試合で複数マーケットタイプに参加")
    lines.append(f"5. **両サイド購入**: {len(both_sides_games)} 試合で ML 両サイド購入 = ヘッジまたはライン移動利用")
    lines.append(f"6. **公称 ROI**: {1460993/87280000*100:.2f}% (取引額ベース)")
    lines.append("")

    return "\n".join(lines)


def main():
    print("=" * 60)
    print("@lhtsports Polymarket Trading Data Analysis")
    print("=" * 60)
    print()

    # Collect data
    trades = collect_all_trades()

    if not trades:
        print("No trades collected. Exiting.")
        return

    # Save raw data
    raw_path = "/Users/taro/dev/nbabot/data/reports/lhtsports_raw_trades.json"
    with open(raw_path, "w") as f:
        json.dump(trades, f, indent=2)
    print(f"\nRaw data saved to {raw_path}")

    # Run analysis
    report = analyze(trades)

    # Write report
    report_path = "/Users/taro/dev/nbabot/data/reports/agent1-quant-analysis.md"
    with open(report_path, "w") as f:
        f.write(report)
    print(f"Report saved to {report_path}")


if __name__ == "__main__":
    main()
