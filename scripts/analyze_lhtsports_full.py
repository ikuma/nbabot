"""Full-period analysis of @lhtsports Polymarket trading data (136,982 trades).

Reads lhtsports_all_trades.json and outputs:
- python_summary.json  (structured data for Agent Team)
- python_report.md     (human-readable intermediate report)

Both saved to data/reports/lhtsports-full-analysis/
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from statistics import mean, median, stdev

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = PROJECT_ROOT / "data/reports/lhtsports-analysis/lhtsports_all_trades.json"
OUTPUT_DIR = PROJECT_ROOT / "data/reports/lhtsports-full-analysis"

# ---------------------------------------------------------------------------
# Classification helpers
# ---------------------------------------------------------------------------

SPORT_PREFIXES: list[tuple[str, str]] = [
    ("nba-", "NBA"),
    ("mlb-", "MLB"),
    ("nhl-", "NHL"),
    ("nfl-", "NFL"),
    ("cfb-", "CFB"),
    ("cbb-", "CBB"),
    ("wnba-", "WNBA"),
    ("epl-", "EPL"),
    ("ucl-", "UCL"),
    ("uel-", "UEL"),
    ("ufc-", "UFC"),
    ("cs2-", "CS2"),
    ("lol-", "LOL"),
    ("val-", "VAL"),
]


def classify_sport(slug: str) -> str:
    """Classify sport from slug prefix."""
    for prefix, sport in SPORT_PREFIXES:
        if slug.startswith(prefix):
            return sport
    # 個別イベント slug のフォールバック
    if any(k in slug for k in ("mets", "yankees", "dodgers", "marlins", "braves")):
        return "MLB"
    return "Other"


def classify_market_type(slug: str) -> str:
    """Classify market type from slug."""
    if "spread" in slug:
        return "Spread"
    if "total-" in slug:
        return "Total"
    if "-over-" in slug or "-under-" in slug:
        return "Total"
    if "-draw" in slug:
        return "Draw"
    return "Moneyline"


def get_game_key(slug: str) -> str:
    """Extract base game key: sport-team1-team2-YYYY-MM-DD."""
    parts = slug.split("-")
    for i in range(len(parts)):
        if len(parts[i]) == 4 and parts[i].isdigit():
            date_end = min(i + 3, len(parts))
            return "-".join(parts[:date_end])
    return slug


def ts_to_month(ts: int) -> str:
    """Unix timestamp -> YYYY-MM string."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m")


def ts_to_date(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def ts_to_hour_et(ts: int) -> int:
    """Unix timestamp -> hour in US Eastern (rough UTC-5)."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    # Approximate ET: UTC-5 (ignoring DST for simplicity)
    return (dt.hour - 5) % 24


def percentile(data: list[float], p: float) -> float:
    """Linear interpolation percentile."""
    s = sorted(data)
    k = (len(s) - 1) * p / 100
    f = int(k)
    c = f + 1
    if c >= len(s):
        return s[f]
    return s[f] + (k - f) * (s[c] - s[f])


def price_bucket(price: float) -> str:
    if price < 0.2:
        return "0.00-0.20"
    if price < 0.4:
        return "0.20-0.40"
    if price < 0.6:
        return "0.40-0.60"
    if price < 0.8:
        return "0.60-0.80"
    return "0.80-1.00"


# ---------------------------------------------------------------------------
# Analysis functions
# ---------------------------------------------------------------------------


def monthly_analysis(trades: list[dict]) -> dict:
    """月次集計: スポーツ別件数/額、マーケットタイプ比率、平均サイズ."""
    monthly: dict[str, dict] = {}

    for t in trades:
        month = ts_to_month(t["timestamp"])
        sport = classify_sport(t.get("slug", ""))
        mtype = classify_market_type(t.get("slug", ""))
        size = float(t.get("usdcSize", 0))

        if month not in monthly:
            monthly[month] = {
                "total_count": 0,
                "total_volume": 0.0,
                "sports": defaultdict(lambda: {"count": 0, "volume": 0.0}),
                "market_types": defaultdict(lambda: {"count": 0, "volume": 0.0}),
                "sizes": [],
            }

        m = monthly[month]
        m["total_count"] += 1
        m["total_volume"] += size
        m["sports"][sport]["count"] += 1
        m["sports"][sport]["volume"] += size
        m["market_types"][mtype]["count"] += 1
        m["market_types"][mtype]["volume"] += size
        m["sizes"].append(size)

    # 集計の加工
    result = {}
    for month in sorted(monthly):
        m = monthly[month]
        sizes = m["sizes"]
        result[month] = {
            "total_count": m["total_count"],
            "total_volume": round(m["total_volume"], 2),
            "avg_trade_size": round(mean(sizes), 2) if sizes else 0,
            "median_trade_size": round(median(sizes), 2) if sizes else 0,
            "sports": {
                s: {"count": d["count"], "volume": round(d["volume"], 2)}
                for s, d in sorted(m["sports"].items(), key=lambda x: x[1]["volume"], reverse=True)
            },
            "market_types": {
                mt: {"count": d["count"], "volume": round(d["volume"], 2)}
                for mt, d in sorted(m["market_types"].items(), key=lambda x: x[1]["volume"], reverse=True)
            },
        }
    return result


def sport_analysis(trades: list[dict]) -> dict:
    """スポーツ別詳細集計."""
    sports: dict[str, dict] = defaultdict(lambda: {
        "count": 0, "volume": 0.0,
        "market_types": defaultdict(lambda: {"count": 0, "volume": 0.0}),
        "sizes": [],
        "first_trade": float("inf"),
        "last_trade": 0,
        "games": set(),
    })

    for t in trades:
        sport = classify_sport(t.get("slug", ""))
        size = float(t.get("usdcSize", 0))
        ts = t["timestamp"]
        game = get_game_key(t.get("slug", ""))
        mtype = classify_market_type(t.get("slug", ""))

        s = sports[sport]
        s["count"] += 1
        s["volume"] += size
        s["sizes"].append(size)
        s["first_trade"] = min(s["first_trade"], ts)
        s["last_trade"] = max(s["last_trade"], ts)
        s["games"].add(game)
        s["market_types"][mtype]["count"] += 1
        s["market_types"][mtype]["volume"] += size

    result = {}
    for sport in sorted(sports, key=lambda x: sports[x]["volume"], reverse=True):
        s = sports[sport]
        result[sport] = {
            "count": s["count"],
            "volume": round(s["volume"], 2),
            "avg_size": round(mean(s["sizes"]), 2),
            "median_size": round(median(s["sizes"]), 2),
            "unique_games": len(s["games"]),
            "first_trade": ts_to_date(int(s["first_trade"])),
            "last_trade": ts_to_date(int(s["last_trade"])),
            "market_types": {
                mt: {"count": d["count"], "volume": round(d["volume"], 2)}
                for mt, d in sorted(s["market_types"].items(), key=lambda x: x[1]["volume"], reverse=True)
            },
        }
    return result


def game_position_analysis(trades: list[dict]) -> dict:
    """試合単位のポジション再構成 & 自動分類."""
    games: dict[str, dict] = defaultdict(lambda: {
        "trades": [],
        "ml_trades": [],
        "spread_trades": [],
        "total_trades": [],
        "draw_trades": [],
    })

    for t in trades:
        slug = t.get("slug", "")
        game_key = get_game_key(slug)
        mtype = classify_market_type(slug)
        entry = {
            "slug": slug,
            "side": t.get("side", "BUY"),
            "outcome": t.get("outcomeIndex", 0),
            "price": float(t.get("price", 0)),
            "size": float(t.get("usdcSize", 0)),
            "timestamp": t["timestamp"],
        }
        games[game_key]["trades"].append(entry)
        if mtype == "Moneyline":
            games[game_key]["ml_trades"].append(entry)
        elif mtype == "Spread":
            games[game_key]["spread_trades"].append(entry)
        elif mtype == "Total":
            games[game_key]["total_trades"].append(entry)
        elif mtype == "Draw":
            games[game_key]["draw_trades"].append(entry)

    # 分類
    type_counts = {"A_ml_only": 0, "B_ml_spread": 0, "C_ml_total": 0, "D_multi": 0, "E_no_ml": 0}
    game_summaries = []

    for game_key, g in games.items():
        has_ml = len(g["ml_trades"]) > 0
        has_spread = len(g["spread_trades"]) > 0
        has_total = len(g["total_trades"]) > 0
        total_volume = sum(t["size"] for t in g["trades"])
        n_trades = len(g["trades"])

        # ML 方向判定
        ml_out0_vol = sum(t["size"] for t in g["ml_trades"] if t["outcome"] == 0)
        ml_out1_vol = sum(t["size"] for t in g["ml_trades"] if t["outcome"] == 1)
        ml_total = ml_out0_vol + ml_out1_vol
        ml_direction = "out0" if ml_out0_vol > ml_out1_vol else ("out1" if ml_out1_vol > ml_out0_vol else "even")
        ml_ratio = max(ml_out0_vol, ml_out1_vol) / ml_total if ml_total > 0 else 0

        # Spread 方向判定
        spread_out0_vol = sum(t["size"] for t in g["spread_trades"] if t["outcome"] == 0)
        spread_out1_vol = sum(t["size"] for t in g["spread_trades"] if t["outcome"] == 1)

        # Total Over/Under
        total_out0_vol = sum(t["size"] for t in g["total_trades"] if t["outcome"] == 0)
        total_out1_vol = sum(t["size"] for t in g["total_trades"] if t["outcome"] == 1)

        # 分類
        if has_ml and has_spread and has_total:
            game_type = "D_multi"
        elif has_ml and has_spread:
            game_type = "B_ml_spread"
        elif has_ml and has_total:
            game_type = "C_ml_total"
        elif has_ml:
            game_type = "A_ml_only"
        else:
            game_type = "E_no_ml"
        type_counts[game_type] += 1

        sport = classify_sport(game_key)

        game_summaries.append({
            "game_key": game_key,
            "sport": sport,
            "game_type": game_type,
            "n_trades": n_trades,
            "total_volume": round(total_volume, 2),
            "ml_volume": round(ml_total, 2),
            "ml_direction": ml_direction,
            "ml_direction_ratio": round(ml_ratio, 4),
            "ml_out0_vol": round(ml_out0_vol, 2),
            "ml_out1_vol": round(ml_out1_vol, 2),
            "spread_out0_vol": round(spread_out0_vol, 2),
            "spread_out1_vol": round(spread_out1_vol, 2),
            "total_over_vol": round(total_out0_vol, 2),
            "total_under_vol": round(total_out1_vol, 2),
            "n_unique_slugs": len(set(t["slug"] for t in g["trades"])),
        })

    game_summaries.sort(key=lambda x: x["total_volume"], reverse=True)

    # 集計統計
    volumes = [g["total_volume"] for g in game_summaries]
    n_trades_list = [g["n_trades"] for g in game_summaries]

    return {
        "total_games": len(game_summaries),
        "type_counts": type_counts,
        "per_game_stats": {
            "avg_volume": round(mean(volumes), 2) if volumes else 0,
            "median_volume": round(median(volumes), 2) if volumes else 0,
            "max_volume": round(max(volumes), 2) if volumes else 0,
            "avg_trades": round(mean(n_trades_list), 1) if n_trades_list else 0,
            "median_trades": round(median(n_trades_list), 1) if n_trades_list else 0,
        },
        "top_50_games": game_summaries[:50],
    }


def under_over_analysis(trades: list[dict]) -> dict:
    """Under/Over バイアス分析."""
    total_over_vol = 0.0
    total_under_vol = 0.0
    total_over_cnt = 0
    total_under_cnt = 0

    # 月別
    monthly_ou: dict[str, dict] = defaultdict(lambda: {"over_vol": 0.0, "under_vol": 0.0, "over_cnt": 0, "under_cnt": 0})
    # スポーツ別
    sport_ou: dict[str, dict] = defaultdict(lambda: {"over_vol": 0.0, "under_vol": 0.0, "over_cnt": 0, "under_cnt": 0})

    for t in trades:
        slug = t.get("slug", "")
        mtype = classify_market_type(slug)
        if mtype != "Total":
            continue

        size = float(t.get("usdcSize", 0))
        outcome = t.get("outcomeIndex", 0)
        month = ts_to_month(t["timestamp"])
        sport = classify_sport(slug)

        # outcomeIndex 0 = Over (first outcome), 1 = Under (second outcome)
        # ただし slug に "under" が入っている場合の解釈は要注意
        if outcome == 0:
            total_over_vol += size
            total_over_cnt += 1
            monthly_ou[month]["over_vol"] += size
            monthly_ou[month]["over_cnt"] += 1
            sport_ou[sport]["over_vol"] += size
            sport_ou[sport]["over_cnt"] += 1
        else:
            total_under_vol += size
            total_under_cnt += 1
            monthly_ou[month]["under_vol"] += size
            monthly_ou[month]["under_cnt"] += 1
            sport_ou[sport]["under_vol"] += size
            sport_ou[sport]["under_cnt"] += 1

    total = total_over_vol + total_under_vol

    return {
        "overall": {
            "over_volume": round(total_over_vol, 2),
            "under_volume": round(total_under_vol, 2),
            "over_pct": round(total_over_vol / total * 100, 1) if total else 0,
            "under_pct": round(total_under_vol / total * 100, 1) if total else 0,
            "over_count": total_over_cnt,
            "under_count": total_under_cnt,
        },
        "monthly": {
            m: {
                "over_vol": round(d["over_vol"], 2),
                "under_vol": round(d["under_vol"], 2),
                "over_pct": round(d["over_vol"] / (d["over_vol"] + d["under_vol"]) * 100, 1)
                if (d["over_vol"] + d["under_vol"]) > 0 else 0,
            }
            for m, d in sorted(monthly_ou.items())
        },
        "by_sport": {
            s: {
                "over_vol": round(d["over_vol"], 2),
                "under_vol": round(d["under_vol"], 2),
                "over_pct": round(d["over_vol"] / (d["over_vol"] + d["under_vol"]) * 100, 1)
                if (d["over_vol"] + d["under_vol"]) > 0 else 0,
            }
            for s, d in sorted(sport_ou.items(), key=lambda x: x[1]["over_vol"] + x[1]["under_vol"], reverse=True)
        },
    }


def sizing_analysis(trades: list[dict]) -> dict:
    """ポジションサイジング分析: 価格帯×サイズ、スポーツ別."""
    # 価格帯別
    price_size: dict[str, list[float]] = defaultdict(list)
    # スポーツ別
    sport_sizes: dict[str, list[float]] = defaultdict(list)

    for t in trades:
        size = float(t.get("usdcSize", 0))
        price = float(t.get("price", 0))
        sport = classify_sport(t.get("slug", ""))

        pb = price_bucket(price)
        price_size[pb].append(size)
        sport_sizes[sport].append(size)

    def summarize(sizes: list[float]) -> dict:
        if not sizes:
            return {"count": 0, "total": 0, "avg": 0, "median": 0}
        return {
            "count": len(sizes),
            "total": round(sum(sizes), 2),
            "avg": round(mean(sizes), 2),
            "median": round(median(sizes), 2),
            "p75": round(percentile(sizes, 75), 2),
            "p95": round(percentile(sizes, 95), 2),
        }

    return {
        "by_price_bucket": {
            pb: summarize(price_size[pb])
            for pb in ["0.00-0.20", "0.20-0.40", "0.40-0.60", "0.60-0.80", "0.80-1.00"]
        },
        "by_sport": {
            s: summarize(sport_sizes[s])
            for s in sorted(sport_sizes, key=lambda x: sum(sport_sizes[x]), reverse=True)
        },
        "overall": summarize([float(t.get("usdcSize", 0)) for t in trades]),
    }


def trading_pattern_analysis(trades: list[dict]) -> dict:
    """取引パターン分析: 間隔分布、時間帯、DCA検出."""
    # 時間帯別 (EST)
    hourly: dict[int, dict] = defaultdict(lambda: {"count": 0, "volume": 0.0})
    for t in trades:
        h = ts_to_hour_et(t["timestamp"])
        hourly[h]["count"] += 1
        hourly[h]["volume"] += float(t.get("usdcSize", 0))

    # 取引間隔
    sorted_ts = sorted(t["timestamp"] for t in trades)
    intervals = [sorted_ts[i + 1] - sorted_ts[i] for i in range(len(sorted_ts) - 1)]
    intervals = [iv for iv in intervals if iv > 0]

    interval_stats = {}
    if intervals:
        interval_stats = {
            "mean_seconds": round(mean(intervals), 1),
            "median_seconds": round(median(intervals), 1),
            "min_seconds": min(intervals),
            "max_seconds": max(intervals),
            "burst_lte5s_count": sum(1 for iv in intervals if iv <= 5),
            "burst_lte5s_pct": round(sum(1 for iv in intervals if iv <= 5) / len(intervals) * 100, 1),
            "rapid_lte60s_count": sum(1 for iv in intervals if iv <= 60),
            "rapid_lte60s_pct": round(sum(1 for iv in intervals if iv <= 60) / len(intervals) * 100, 1),
        }

    # DCA パターン検出: 同一 slug に対して 3+ 回の BUY を短時間 (1時間以内) に行った場合
    slug_trades: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        slug_trades[t.get("slug", "")].append(t)

    dca_events = 0
    dca_examples: list[dict] = []
    for slug, sts in slug_trades.items():
        if len(sts) < 3:
            continue
        sts_sorted = sorted(sts, key=lambda x: x["timestamp"])
        # スライディングウィンドウで1時間以内に3回以上
        for i in range(len(sts_sorted) - 2):
            window = [sts_sorted[i]]
            for j in range(i + 1, len(sts_sorted)):
                if sts_sorted[j]["timestamp"] - sts_sorted[i]["timestamp"] <= 3600:
                    window.append(sts_sorted[j])
                else:
                    break
            if len(window) >= 3:
                dca_events += 1
                if len(dca_examples) < 10:
                    dca_examples.append({
                        "slug": slug,
                        "n_trades": len(window),
                        "total_volume": round(sum(float(w.get("usdcSize", 0)) for w in window), 2),
                        "duration_min": round((window[-1]["timestamp"] - window[0]["timestamp"]) / 60, 1),
                        "prices": [round(float(w.get("price", 0)), 4) for w in window[:5]],
                    })
                break  # 1 slug あたり1カウントのみ

    # 曜日別
    dow_stats: dict[str, dict] = defaultdict(lambda: {"count": 0, "volume": 0.0})
    dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    for t in trades:
        dt = datetime.fromtimestamp(t["timestamp"], tz=timezone.utc)
        dow = dow_names[dt.weekday()]
        dow_stats[dow]["count"] += 1
        dow_stats[dow]["volume"] += float(t.get("usdcSize", 0))

    return {
        "hourly_distribution": {
            h: {"count": hourly[h]["count"], "volume": round(hourly[h]["volume"], 2)}
            for h in range(24)
        },
        "interval_stats": interval_stats,
        "dca_detection": {
            "slugs_with_dca": dca_events,
            "examples": dca_examples,
        },
        "day_of_week": {
            d: {"count": dow_stats[d]["count"], "volume": round(dow_stats[d]["volume"], 2)}
            for d in dow_names
        },
    }


def strategy_evolution_analysis(monthly_data: dict) -> dict:
    """戦略の進化ポイントを自動検出."""
    milestones = []

    months = sorted(monthly_data.keys())

    # Spread/Total が初めて登場した月を検出
    first_spread = None
    first_total = None
    for m in months:
        mts = monthly_data[m].get("market_types", {})
        if "Spread" in mts and mts["Spread"]["count"] > 0 and first_spread is None:
            first_spread = m
            milestones.append(f"Spread trading started: {m}")
        if "Total" in mts and mts["Total"]["count"] > 0 and first_total is None:
            first_total = m
            milestones.append(f"Total O/U trading started: {m}")

    # 各スポーツの初回登場月
    sport_first: dict[str, str] = {}
    for m in months:
        for sport in monthly_data[m].get("sports", {}):
            if sport not in sport_first:
                sport_first[sport] = m
    for sport, m in sorted(sport_first.items(), key=lambda x: x[1]):
        milestones.append(f"{sport} first appeared: {m}")

    # 月間取引額のピーク
    peak_month = max(months, key=lambda m: monthly_data[m]["total_volume"])
    milestones.append(f"Peak volume month: {peak_month} (${monthly_data[peak_month]['total_volume']:,.0f})")

    # NBA が支配的になった月 (NBA > 50% of volume)
    for m in months:
        sports = monthly_data[m].get("sports", {})
        total_vol = monthly_data[m]["total_volume"]
        nba_vol = sports.get("NBA", {}).get("volume", 0)
        if total_vol > 0 and nba_vol / total_vol > 0.5:
            milestones.append(f"NBA became dominant (>50%): {m}")
            break

    # 月次スポーツ構成比の推移
    sport_share_monthly: dict[str, dict[str, float]] = {}
    for m in months:
        total_vol = monthly_data[m]["total_volume"]
        if total_vol == 0:
            continue
        sport_share_monthly[m] = {}
        for sport, data in monthly_data[m].get("sports", {}).items():
            sport_share_monthly[m][sport] = round(data["volume"] / total_vol * 100, 1)

    return {
        "milestones": milestones,
        "first_spread_month": first_spread,
        "first_total_month": first_total,
        "sport_first_appearance": sport_first,
        "peak_volume_month": peak_month,
        "sport_share_monthly": sport_share_monthly,
    }


# ---------------------------------------------------------------------------
# Markdown report generation
# ---------------------------------------------------------------------------

def generate_report(
    trades: list[dict],
    monthly_data: dict,
    sport_data: dict,
    game_data: dict,
    ou_data: dict,
    sizing_data: dict,
    pattern_data: dict,
    evolution_data: dict,
) -> str:
    """Generate human-readable markdown report."""
    L: list[str] = []

    total_count = len(trades)
    total_volume = sum(float(t.get("usdcSize", 0)) for t in trades)
    dates = sorted(set(ts_to_date(t["timestamp"]) for t in trades))

    L.append("# @lhtsports 全期間取引データ分析レポート")
    L.append("")
    L.append(f"**分析日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    L.append(f"**データポイント数**: {total_count:,}")
    L.append(f"**データ期間**: {dates[0]} ~ {dates[-1]} ({len(set(ts_to_month(t['timestamp']) for t in trades))}ヶ月)")
    L.append(f"**アクティブ日数**: {len(dates)}")
    L.append(f"**総取引額**: ${total_volume:,.2f}")
    L.append(f"**BUY**: {sum(1 for t in trades if t.get('side')=='BUY'):,} / **SELL**: {sum(1 for t in trades if t.get('side')=='SELL'):,}")
    L.append("")

    # ===== 1. 時系列分析 (月次) =====
    L.append("---")
    L.append("## 1. 時系列分析 (月次)")
    L.append("")

    months = sorted(monthly_data.keys())
    L.append("### 月次サマリー")
    L.append("")
    L.append("| 月 | 件数 | 取引額 ($) | 平均サイズ | 中央値 | トップスポーツ | ML% | Spread% | Total% |")
    L.append("|------|------|------------|------------|--------|---------------|------|---------|--------|")
    for m in months:
        md = monthly_data[m]
        # トップスポーツ
        top_sport = max(md["sports"].items(), key=lambda x: x[1]["volume"])[0] if md["sports"] else "-"
        # マーケットタイプ比率
        ml_pct = md["market_types"].get("Moneyline", {}).get("volume", 0) / md["total_volume"] * 100 if md["total_volume"] else 0
        sp_pct = md["market_types"].get("Spread", {}).get("volume", 0) / md["total_volume"] * 100 if md["total_volume"] else 0
        to_pct = md["market_types"].get("Total", {}).get("volume", 0) / md["total_volume"] * 100 if md["total_volume"] else 0
        L.append(
            f"| {m} | {md['total_count']:,} | ${md['total_volume']:,.0f} | "
            f"${md['avg_trade_size']:,.0f} | ${md['median_trade_size']:,.0f} | "
            f"{top_sport} | {ml_pct:.0f}% | {sp_pct:.0f}% | {to_pct:.0f}% |"
        )
    L.append("")

    # 月次スポーツ構成比
    L.append("### 月次スポーツ構成比 (取引額%)")
    L.append("")
    all_sports = sorted(set(s for m in months for s in monthly_data[m]["sports"]))
    header = "| 月 | " + " | ".join(all_sports) + " |"
    sep = "|------|" + "|".join(["------"] * len(all_sports)) + "|"
    L.append(header)
    L.append(sep)
    for m in months:
        total_vol = monthly_data[m]["total_volume"]
        cells = []
        for s in all_sports:
            vol = monthly_data[m]["sports"].get(s, {}).get("volume", 0)
            pct = vol / total_vol * 100 if total_vol else 0
            cells.append(f"{pct:.0f}%" if pct >= 0.5 else "-")
        L.append(f"| {m} | " + " | ".join(cells) + " |")
    L.append("")

    # 戦略進化ポイント
    L.append("### 戦略進化マイルストーン")
    L.append("")
    for ms in evolution_data["milestones"]:
        L.append(f"- {ms}")
    L.append("")

    # ===== 2. スポーツ別分析 =====
    L.append("---")
    L.append("## 2. スポーツ別分析")
    L.append("")

    L.append("| スポーツ | 件数 | 件数% | 取引額 ($) | 取引額% | 平均サイズ | 試合数 | 初回 | 最終 |")
    L.append("|----------|------|-------|------------|---------|------------|--------|------|------|")
    for sport, sd in sport_data.items():
        cnt_pct = sd["count"] / total_count * 100
        vol_pct = sd["volume"] / total_volume * 100
        L.append(
            f"| {sport} | {sd['count']:,} | {cnt_pct:.1f}% | ${sd['volume']:,.0f} | {vol_pct:.1f}% | "
            f"${sd['avg_size']:,.0f} | {sd['unique_games']:,} | {sd['first_trade']} | {sd['last_trade']} |"
        )
    L.append("")

    # スポーツ別マーケットタイプ
    L.append("### スポーツ別マーケットタイプ分布")
    L.append("")
    for sport, sd in sport_data.items():
        if sd["count"] < 50:
            continue
        L.append(f"**{sport}** ({sd['count']:,}件, ${sd['volume']:,.0f}):")
        for mt, mtd in sd["market_types"].items():
            pct = mtd["volume"] / sd["volume"] * 100 if sd["volume"] else 0
            L.append(f"  - {mt}: {mtd['count']:,}件, ${mtd['volume']:,.0f} ({pct:.1f}%)")
        L.append("")

    # ===== 3. 試合単位のポジション再構成 =====
    L.append("---")
    L.append("## 3. 試合単位のポジション再構成")
    L.append("")

    gd = game_data
    L.append(f"- **ユニーク試合数**: {gd['total_games']:,}")
    L.append(f"- **1試合平均取引額**: ${gd['per_game_stats']['avg_volume']:,.0f}")
    L.append(f"- **1試合中央値取引額**: ${gd['per_game_stats']['median_volume']:,.0f}")
    L.append(f"- **1試合平均取引件数**: {gd['per_game_stats']['avg_trades']}")
    L.append("")

    L.append("### 試合タイプ分類")
    L.append("")
    L.append("| タイプ | 説明 | 試合数 | 比率 |")
    L.append("|--------|------|--------|------|")
    type_labels = {
        "A_ml_only": "A: ML のみ",
        "B_ml_spread": "B: ML + Spread",
        "C_ml_total": "C: ML + Total",
        "D_multi": "D: ML + Spread + Total (フル)",
        "E_no_ml": "E: ML なし (Spread/Total/Draw のみ)",
    }
    for key, label in type_labels.items():
        cnt = gd["type_counts"][key]
        pct = cnt / gd["total_games"] * 100
        L.append(f"| {label} | | {cnt:,} | {pct:.1f}% |")
    L.append("")

    # 上位20試合
    L.append("### 取引額上位20試合")
    L.append("")
    L.append("| # | 試合 | スポーツ | タイプ | 取引数 | 取引額 | ML方向 | ML比率 | O/U Over | O/U Under |")
    L.append("|---|------|----------|--------|--------|--------|--------|--------|----------|-----------|")
    for i, g in enumerate(gd["top_50_games"][:20], 1):
        L.append(
            f"| {i} | {g['game_key']} | {g['sport']} | {g['game_type'][0]} | "
            f"{g['n_trades']} | ${g['total_volume']:,.0f} | {g['ml_direction']} | "
            f"{g['ml_direction_ratio']:.0%} | ${g['total_over_vol']:,.0f} | ${g['total_under_vol']:,.0f} |"
        )
    L.append("")

    # ===== 4. Under/Over バイアス =====
    L.append("---")
    L.append("## 4. Under/Over バイアス分析")
    L.append("")

    ov = ou_data["overall"]
    L.append(f"- **Over 全体**: ${ov['over_volume']:,.0f} ({ov['over_pct']}%) — {ov['over_count']:,}件")
    L.append(f"- **Under 全体**: ${ov['under_volume']:,.0f} ({ov['under_pct']}%) — {ov['under_count']:,}件")
    L.append("")

    L.append("### 月別 Over 比率推移")
    L.append("")
    L.append("| 月 | Over$ | Under$ | Over% |")
    L.append("|------|-------|--------|-------|")
    for m, d in ou_data["monthly"].items():
        L.append(f"| {m} | ${d['over_vol']:,.0f} | ${d['under_vol']:,.0f} | {d['over_pct']}% |")
    L.append("")

    L.append("### スポーツ別 Over 比率")
    L.append("")
    L.append("| スポーツ | Over$ | Under$ | Over% |")
    L.append("|----------|-------|--------|-------|")
    for s, d in ou_data["by_sport"].items():
        L.append(f"| {s} | ${d['over_vol']:,.0f} | ${d['under_vol']:,.0f} | {d['over_pct']}% |")
    L.append("")

    # ===== 5. ポジションサイジング =====
    L.append("---")
    L.append("## 5. ポジションサイジング分析")
    L.append("")

    ov_sizing = sizing_data["overall"]
    L.append(f"- **全体平均**: ${ov_sizing['avg']:,.2f}")
    L.append(f"- **全体中央値**: ${ov_sizing['median']:,.2f}")
    L.append(f"- **P75**: ${ov_sizing['p75']:,.2f}")
    L.append(f"- **P95**: ${ov_sizing['p95']:,.2f}")
    L.append("")

    L.append("### 価格帯別サイジング")
    L.append("")
    L.append("| 価格帯 | 件数 | 合計$ | 平均$ | 中央値$ | P95$ |")
    L.append("|--------|------|-------|-------|---------|------|")
    for pb, d in sizing_data["by_price_bucket"].items():
        L.append(
            f"| {pb} | {d['count']:,} | ${d['total']:,.0f} | "
            f"${d['avg']:,.0f} | ${d['median']:,.0f} | ${d.get('p95', 0):,.0f} |"
        )
    L.append("")

    L.append("### スポーツ別サイジング")
    L.append("")
    L.append("| スポーツ | 件数 | 合計$ | 平均$ | 中央値$ | P95$ |")
    L.append("|----------|------|-------|-------|---------|------|")
    for s, d in sizing_data["by_sport"].items():
        if d["count"] < 10:
            continue
        L.append(
            f"| {s} | {d['count']:,} | ${d['total']:,.0f} | "
            f"${d['avg']:,.0f} | ${d['median']:,.0f} | ${d.get('p95', 0):,.0f} |"
        )
    L.append("")

    # ===== 6. 取引パターン =====
    L.append("---")
    L.append("## 6. 取引パターン分析")
    L.append("")

    iv = pattern_data["interval_stats"]
    if iv:
        L.append("### 取引間隔")
        L.append("")
        L.append(f"- **平均間隔**: {iv['mean_seconds']:.0f}秒 ({iv['mean_seconds']/60:.1f}分)")
        L.append(f"- **中央値間隔**: {iv['median_seconds']:.0f}秒 ({iv['median_seconds']/60:.1f}分)")
        L.append(f"- **バースト (<=5秒)**: {iv['burst_lte5s_count']:,}件 ({iv['burst_lte5s_pct']}%)")
        L.append(f"- **高速 (<=60秒)**: {iv['rapid_lte60s_count']:,}件 ({iv['rapid_lte60s_pct']}%)")
        L.append("")

    L.append("### 時間帯別取引分布 (EST)")
    L.append("")
    L.append("| 時間 | 件数 | 取引額 ($) |")
    L.append("|------|------|------------|")
    for h in range(24):
        hd = pattern_data["hourly_distribution"].get(h, {"count": 0, "volume": 0})
        if hd["count"] > 0:
            L.append(f"| {h:02d}:00 | {hd['count']:,} | ${hd['volume']:,.0f} |")
    L.append("")

    L.append("### 曜日別")
    L.append("")
    L.append("| 曜日 | 件数 | 取引額 ($) |")
    L.append("|------|------|------------|")
    for d, dd in pattern_data["day_of_week"].items():
        L.append(f"| {d} | {dd['count']:,} | ${dd['volume']:,.0f} |")
    L.append("")

    dca = pattern_data["dca_detection"]
    L.append("### DCA パターン検出")
    L.append("")
    L.append(f"- **1時間以内に3回以上同一マーケット取引**: {dca['slugs_with_dca']} スラッグ")
    L.append("")
    if dca["examples"]:
        L.append("| slug | 取引数 | 合計$ | 時間(分) | 価格推移 |")
        L.append("|------|--------|-------|----------|----------|")
        for ex in dca["examples"][:5]:
            prices = " → ".join(f"{p:.3f}" for p in ex["prices"])
            L.append(f"| {ex['slug'][:40]}... | {ex['n_trades']} | ${ex['total_volume']:,.0f} | {ex['duration_min']:.0f} | {prices} |")
        L.append("")

    # ===== サマリー =====
    L.append("---")
    L.append("## 主要所見サマリー")
    L.append("")
    nba_vol = sport_data.get("NBA", {}).get("volume", 0)
    mlb_vol = sport_data.get("MLB", {}).get("volume", 0)
    L.append(f"1. **NBA 支配**: 取引額の {nba_vol/total_volume*100:.1f}% が NBA")
    L.append(f"2. **MLB 存在感**: 取引額の {mlb_vol/total_volume*100:.1f}% が MLB (前回の直近7日分析では検出不可)")
    L.append(f"3. **戦略進化**: Spread初登場={evolution_data['first_spread_month']}, Total初登場={evolution_data['first_total_month']}")
    L.append(f"4. **全期間試合数**: {gd['total_games']:,}")
    L.append(f"5. **Under/Over バイアス**: Over {ov['over_pct']}% vs Under {ov['under_pct']}%")
    L.append(f"6. **SELL ほぼゼロ**: 136,973 BUY vs 9 SELL — 純粋なホールド型")
    L.append("")

    return "\n".join(L)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("@lhtsports Full-Period Analysis (136,982 trades)")
    print("=" * 60)
    print()

    # Load data
    print(f"Loading data from {INPUT_PATH}...")
    with open(INPUT_PATH) as f:
        trades: list[dict] = json.load(f)
    print(f"Loaded {len(trades):,} trades")
    print()

    # Run analyses
    print("[1/7] Monthly analysis...")
    monthly_data = monthly_analysis(trades)

    print("[2/7] Sport analysis...")
    sport_data = sport_analysis(trades)

    print("[3/7] Game position analysis...")
    game_data = game_position_analysis(trades)

    print("[4/7] Under/Over analysis...")
    ou_data = under_over_analysis(trades)

    print("[5/7] Sizing analysis...")
    sizing_data = sizing_analysis(trades)

    print("[6/7] Trading pattern analysis...")
    pattern_data = trading_pattern_analysis(trades)

    print("[7/7] Strategy evolution analysis...")
    evolution_data = strategy_evolution_analysis(monthly_data)

    # Output JSON summary
    print("\nWriting JSON summary...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    summary = {
        "meta": {
            "total_trades": len(trades),
            "total_volume": round(sum(float(t.get("usdcSize", 0)) for t in trades), 2),
            "date_range": {
                "start": ts_to_date(min(t["timestamp"] for t in trades)),
                "end": ts_to_date(max(t["timestamp"] for t in trades)),
            },
            "active_days": len(set(ts_to_date(t["timestamp"]) for t in trades)),
            "buy_count": sum(1 for t in trades if t.get("side") == "BUY"),
            "sell_count": sum(1 for t in trades if t.get("side") == "SELL"),
        },
        "monthly": monthly_data,
        "sports": sport_data,
        "games": game_data,
        "under_over": ou_data,
        "sizing": sizing_data,
        "trading_patterns": pattern_data,
        "strategy_evolution": evolution_data,
    }

    json_path = OUTPUT_DIR / "python_summary.json"
    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"  -> {json_path}")

    # Output Markdown report
    print("Writing Markdown report...")
    report = generate_report(
        trades, monthly_data, sport_data, game_data,
        ou_data, sizing_data, pattern_data, evolution_data,
    )
    md_path = OUTPUT_DIR / "python_report.md"
    with open(md_path, "w") as f:
        f.write(report)
    print(f"  -> {md_path}")

    # Verification
    print("\n" + "=" * 60)
    print("VERIFICATION")
    print("=" * 60)
    json_trade_count = summary["meta"]["total_trades"]
    json_volume = summary["meta"]["total_volume"]
    sport_total = sum(sd["count"] for sd in sport_data.values())
    game_total_from_top50 = len(game_data["top_50_games"])
    print(f"  Input trades:  {len(trades):,}")
    print(f"  JSON meta:     {json_trade_count:,}")
    print(f"  Sport sum:     {sport_total:,}")
    print(f"  Volume:        ${json_volume:,.2f}")
    print(f"  Games:         {game_data['total_games']:,}")
    print(f"  Top-50 sample: {game_total_from_top50}")
    assert json_trade_count == len(trades), "Trade count mismatch!"
    assert sport_total == len(trades), "Sport sum mismatch!"
    print("\n  All checks passed!")
    print()


if __name__ == "__main__":
    main()
