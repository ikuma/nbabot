"""TRADE + REDEEM + MERGE を結合した試合単位 P&L 分析.

conditionId ベースで取引コストと払戻を突合し、
試合 (eventSlug) 単位で P&L を算出する。

出力: data/reports/lhtsports-pnl/
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data/reports/lhtsports-analysis"
OUTPUT_DIR = PROJECT_ROOT / "data/reports/lhtsports-pnl"

SPORT_PREFIXES: list[tuple[str, str]] = [
    ("nba-", "NBA"), ("mlb-", "MLB"), ("nhl-", "NHL"), ("nfl-", "NFL"),
    ("cfb-", "CFB"), ("cbb-", "CBB"), ("wnba-", "WNBA"),
    ("epl-", "EPL"), ("ucl-", "UCL"), ("uel-", "UEL"),
    ("ufc-", "UFC"), ("cs2-", "CS2"), ("lol-", "LOL"), ("val-", "VAL"),
]


def classify_sport(slug: str) -> str:
    for prefix, sport in SPORT_PREFIXES:
        if slug.startswith(prefix):
            return sport
    return "Other"


def classify_market_type(slug: str) -> str:
    if "spread" in slug:
        return "Spread"
    if "total-" in slug or "-over-" in slug or "-under-" in slug:
        return "Total"
    return "Moneyline"


def ts_to_date(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def ts_to_month(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m")


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
def load_data() -> tuple[list[dict], list[dict], list[dict]]:
    with open(DATA_DIR / "lhtsports_all_trades.json") as f:
        trades = json.load(f)
    with open(DATA_DIR / "lhtsports_redeem.json") as f:
        redeems = json.load(f)
    with open(DATA_DIR / "lhtsports_merge.json") as f:
        merges = json.load(f)
    return trades, redeems, merges


# ---------------------------------------------------------------------------
# Core P&L calculation
# ---------------------------------------------------------------------------
def build_condition_pnl(
    trades: list[dict], redeems: list[dict], merges: list[dict]
) -> dict[str, dict]:
    """conditionId 単位で P&L を計算."""
    conditions: dict[str, dict] = {}

    # TRADE を集計
    for t in trades:
        cid = t.get("conditionId", "")
        if not cid:
            continue
        if cid not in conditions:
            conditions[cid] = {
                "conditionId": cid,
                "slug": t.get("slug", ""),
                "eventSlug": t.get("eventSlug", ""),
                "title": t.get("title", ""),
                "sport": classify_sport(t.get("slug", "")),
                "market_type": classify_market_type(t.get("slug", "")),
                "buy_cost": 0.0,       # BUY に支払った USDC
                "buy_shares": 0.0,     # BUY で取得したシェア数
                "sell_proceeds": 0.0,  # SELL で得た USDC
                "sell_shares": 0.0,
                "trade_count": 0,
                "first_trade_ts": float("inf"),
                "last_trade_ts": 0,
                "redeem_usdc": 0.0,    # REDEEM 払戻
                "redeem_shares": 0.0,
                "merge_usdc": 0.0,     # MERGE 払戻
                "merge_shares": 0.0,
                "outcome_bought": "",
                "avg_buy_price": 0.0,
                "prices": [],
            }

        c = conditions[cid]
        size = float(t.get("usdcSize", 0))
        shares = float(t.get("size", 0))
        c["trade_count"] += 1
        c["first_trade_ts"] = min(c["first_trade_ts"], t["timestamp"])
        c["last_trade_ts"] = max(c["last_trade_ts"], t["timestamp"])
        c["prices"].append(float(t.get("price", 0)))

        if t.get("side") == "BUY":
            c["buy_cost"] += size
            c["buy_shares"] += shares
            if not c["outcome_bought"]:
                c["outcome_bought"] = t.get("outcome", "")
        elif t.get("side") == "SELL":
            c["sell_proceeds"] += size
            c["sell_shares"] += shares

    # REDEEM を結合
    for r in redeems:
        cid = r.get("conditionId", "")
        if cid in conditions:
            conditions[cid]["redeem_usdc"] += float(r.get("usdcSize", 0))
            conditions[cid]["redeem_shares"] += float(r.get("size", 0))
        else:
            # TRADE なしの REDEEM (稀)
            conditions[cid] = {
                "conditionId": cid,
                "slug": r.get("slug", ""),
                "eventSlug": r.get("eventSlug", ""),
                "title": r.get("title", ""),
                "sport": classify_sport(r.get("slug", "")),
                "market_type": classify_market_type(r.get("slug", "")),
                "buy_cost": 0.0, "buy_shares": 0.0,
                "sell_proceeds": 0.0, "sell_shares": 0.0,
                "trade_count": 0,
                "first_trade_ts": r["timestamp"],
                "last_trade_ts": r["timestamp"],
                "redeem_usdc": float(r.get("usdcSize", 0)),
                "redeem_shares": float(r.get("size", 0)),
                "merge_usdc": 0.0, "merge_shares": 0.0,
                "outcome_bought": "", "avg_buy_price": 0.0, "prices": [],
            }

    # MERGE を結合
    for m in merges:
        cid = m.get("conditionId", "")
        if cid in conditions:
            conditions[cid]["merge_usdc"] += float(m.get("usdcSize", 0))
            conditions[cid]["merge_shares"] += float(m.get("size", 0))

    # P&L 算出
    for cid, c in conditions.items():
        c["net_cost"] = c["buy_cost"] - c["sell_proceeds"]
        c["total_payout"] = c["redeem_usdc"] + c["merge_usdc"]
        c["pnl"] = c["total_payout"] - c["net_cost"]
        c["roi_pct"] = (c["pnl"] / c["net_cost"] * 100) if c["net_cost"] > 0 else 0.0
        c["avg_buy_price"] = (
            c["buy_cost"] / c["buy_shares"] if c["buy_shares"] > 0 else 0.0
        )

        # ステータス判定
        if c["redeem_usdc"] > 0:
            c["status"] = "WIN"
        elif c["merge_usdc"] > 0:
            c["status"] = "MERGED"
        elif c["redeem_usdc"] == 0 and c["merge_usdc"] == 0 and c["buy_cost"] > 0:
            c["status"] = "LOSS_OR_OPEN"
        else:
            c["status"] = "UNKNOWN"

        # prices 消す (出力軽量化)
        del c["prices"]

    return conditions


# ---------------------------------------------------------------------------
# Game-level aggregation
# ---------------------------------------------------------------------------
def aggregate_by_game(conditions: dict[str, dict]) -> list[dict]:
    """eventSlug 単位で集約."""
    games: dict[str, dict] = {}

    for cid, c in conditions.items():
        game_key = c["eventSlug"] or c["slug"]
        if not game_key:
            continue

        if game_key not in games:
            games[game_key] = {
                "game_key": game_key,
                "sport": c["sport"],
                "title": "",
                "conditions": [],
                "total_buy_cost": 0.0,
                "total_sell_proceeds": 0.0,
                "total_redeem": 0.0,
                "total_merge": 0.0,
                "total_pnl": 0.0,
                "trade_count": 0,
                "first_trade_ts": float("inf"),
                "market_types": set(),
                "win_conditions": 0,
                "loss_conditions": 0,
                "merged_conditions": 0,
                "open_conditions": 0,
            }

        g = games[game_key]
        g["conditions"].append(cid)
        g["total_buy_cost"] += c["buy_cost"]
        g["total_sell_proceeds"] += c["sell_proceeds"]
        g["total_redeem"] += c["redeem_usdc"]
        g["total_merge"] += c["merge_usdc"]
        g["total_pnl"] += c["pnl"]
        g["trade_count"] += c["trade_count"]
        g["first_trade_ts"] = min(g["first_trade_ts"], c["first_trade_ts"])
        g["market_types"].add(c["market_type"])
        if not g["title"]:
            g["title"] = c["title"]

        if c["status"] == "WIN":
            g["win_conditions"] += 1
        elif c["status"] == "LOSS_OR_OPEN":
            g["loss_conditions"] += 1
        elif c["status"] == "MERGED":
            g["merged_conditions"] += 1

    # 加工
    result = []
    for gk, g in games.items():
        net_cost = g["total_buy_cost"] - g["total_sell_proceeds"]
        total_payout = g["total_redeem"] + g["total_merge"]
        g["net_cost"] = round(net_cost, 2)
        g["total_payout"] = round(total_payout, 2)
        g["total_pnl"] = round(g["total_pnl"], 2)
        g["total_buy_cost"] = round(g["total_buy_cost"], 2)
        g["total_sell_proceeds"] = round(g["total_sell_proceeds"], 2)
        g["total_redeem"] = round(g["total_redeem"], 2)
        g["total_merge"] = round(g["total_merge"], 2)
        g["roi_pct"] = round(g["total_pnl"] / net_cost * 100, 2) if net_cost > 0 else 0
        g["n_conditions"] = len(g["conditions"])
        g["market_types"] = sorted(g["market_types"])
        g["date"] = ts_to_date(int(g["first_trade_ts"]))
        g["month"] = ts_to_month(int(g["first_trade_ts"]))

        # 全 condition が決済済みか
        n_total = g["win_conditions"] + g["loss_conditions"] + g["merged_conditions"]
        g["fully_settled"] = g["loss_conditions"] == 0 or (
            g["win_conditions"] + g["merged_conditions"] > 0
        )

        del g["conditions"]
        del g["first_trade_ts"]
        result.append(g)

    result.sort(key=lambda x: x["date"])
    return result


# ---------------------------------------------------------------------------
# Analysis & Report
# ---------------------------------------------------------------------------
def generate_report(
    conditions: dict[str, dict], games: list[dict]
) -> str:
    L: list[str] = []

    # -- 全体 P&L --
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

    L.append("# @lhtsports P&L 分析レポート (TRADE + REDEEM + MERGE)")
    L.append("")
    L.append(f"**分析日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    L.append("")
    L.append("---")
    L.append("## 1. 全体サマリー")
    L.append("")
    L.append("| 項目 | 金額 |")
    L.append("|------|------|")
    L.append(f"| BUY 総コスト | ${total_buy:,.2f} |")
    L.append(f"| SELL 収入 | ${total_sell:,.2f} |")
    L.append(f"| **純コスト (BUY - SELL)** | **${net_cost:,.2f}** |")
    L.append(f"| REDEEM 払戻 | ${total_redeem:,.2f} |")
    L.append(f"| MERGE 払戻 | ${total_merge:,.2f} |")
    L.append(f"| **総払戻** | **${total_payout:,.2f}** |")
    L.append(f"| **P&L** | **${total_pnl:,.2f}** |")
    L.append(f"| **ROI** | **{total_pnl / net_cost * 100:.2f}%** |")
    L.append("")

    L.append("### Condition 単位の勝敗")
    L.append("")
    L.append(f"- **WIN (REDEEM あり)**: {len(wins):,} conditions")
    L.append(f"- **LOSS or OPEN**: {len(losses):,} conditions")
    L.append(f"- **MERGED (早期退出)**: {len(merged):,} conditions")
    L.append(f"- **合計**: {len(conditions):,} conditions")
    L.append("")

    # WIN のみの P&L
    win_cost = sum(c["net_cost"] for c in wins)
    win_payout = sum(c["total_payout"] for c in wins)
    win_pnl = win_payout - win_cost
    L.append("### WIN condition のみ")
    L.append(f"- コスト: ${win_cost:,.2f} → 払戻: ${win_payout:,.2f} → P&L: ${win_pnl:,.2f} (ROI: {win_pnl/win_cost*100:.1f}%)")
    L.append("")

    # LOSS のコスト
    loss_cost = sum(c["net_cost"] for c in losses)
    L.append("### LOSS/OPEN condition")
    L.append(f"- 純コスト (失った額 or 未決済): ${loss_cost:,.2f}")
    L.append("")

    # MERGE P&L
    merge_cost = sum(c["net_cost"] for c in merged)
    merge_payout = sum(c["total_payout"] for c in merged)
    merge_pnl = merge_payout - merge_cost
    L.append("### MERGED condition")
    L.append(f"- コスト: ${merge_cost:,.2f} → 払戻: ${merge_payout:,.2f} → P&L: ${merge_pnl:,.2f}")
    L.append("")

    # -- 2. スポーツ別 P&L --
    L.append("---")
    L.append("## 2. スポーツ別 P&L")
    L.append("")

    sport_pnl: dict[str, dict] = defaultdict(lambda: {
        "buy": 0.0, "sell": 0.0, "redeem": 0.0, "merge": 0.0,
        "games": 0, "wins": 0, "losses": 0,
    })
    for g in games:
        sp = sport_pnl[g["sport"]]
        sp["buy"] += g["total_buy_cost"]
        sp["sell"] += g["total_sell_proceeds"]
        sp["redeem"] += g["total_redeem"]
        sp["merge"] += g["total_merge"]
        sp["games"] += 1
        if g["total_pnl"] > 0:
            sp["wins"] += 1
        elif g["total_pnl"] < 0:
            sp["losses"] += 1

    L.append("| スポーツ | 試合数 | 勝ち | 負け | 勝率 | 純コスト | 払戻 | P&L | ROI |")
    L.append("|----------|--------|------|------|------|----------|------|-----|-----|")
    for sport in sorted(sport_pnl, key=lambda s: sport_pnl[s]["buy"], reverse=True):
        sp = sport_pnl[sport]
        net = sp["buy"] - sp["sell"]
        payout = sp["redeem"] + sp["merge"]
        pnl = payout - net
        wr = sp["wins"] / (sp["wins"] + sp["losses"]) * 100 if (sp["wins"] + sp["losses"]) > 0 else 0
        roi = pnl / net * 100 if net > 0 else 0
        L.append(
            f"| {sport} | {sp['games']:,} | {sp['wins']:,} | {sp['losses']:,} | "
            f"{wr:.1f}% | ${net:,.0f} | ${payout:,.0f} | ${pnl:,.0f} | {roi:.1f}% |"
        )
    L.append("")

    # -- 3. マーケットタイプ別 P&L --
    L.append("---")
    L.append("## 3. マーケットタイプ別 P&L (condition 単位)")
    L.append("")

    mt_pnl: dict[str, dict] = defaultdict(lambda: {
        "cost": 0.0, "payout": 0.0, "wins": 0, "losses": 0, "count": 0,
    })
    for c in conditions.values():
        mt = mt_pnl[c["market_type"]]
        mt["cost"] += c["net_cost"]
        mt["payout"] += c["total_payout"]
        mt["count"] += 1
        if c["status"] == "WIN":
            mt["wins"] += 1
        elif c["status"] == "LOSS_OR_OPEN":
            mt["losses"] += 1

    L.append("| タイプ | Conditions | 勝ち | 負け/Open | 勝率 | 純コスト | 払戻 | P&L | ROI |")
    L.append("|--------|-----------|------|----------|------|----------|------|-----|-----|")
    for mt_name in ["Moneyline", "Spread", "Total"]:
        mt = mt_pnl.get(mt_name, {"cost": 0, "payout": 0, "wins": 0, "losses": 0, "count": 0})
        pnl = mt["payout"] - mt["cost"]
        wr = mt["wins"] / (mt["wins"] + mt["losses"]) * 100 if (mt["wins"] + mt["losses"]) > 0 else 0
        roi = pnl / mt["cost"] * 100 if mt["cost"] > 0 else 0
        L.append(
            f"| {mt_name} | {mt['count']:,} | {mt['wins']:,} | {mt['losses']:,} | "
            f"{wr:.1f}% | ${mt['cost']:,.0f} | ${mt['payout']:,.0f} | ${pnl:,.0f} | {roi:.1f}% |"
        )
    L.append("")

    # -- 4. 月次 P&L 推移 --
    L.append("---")
    L.append("## 4. 月次 P&L 推移")
    L.append("")

    monthly: dict[str, dict] = defaultdict(lambda: {
        "buy": 0.0, "sell": 0.0, "redeem": 0.0, "merge": 0.0,
        "games": 0, "wins": 0, "losses": 0,
    })
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

    L.append("| 月 | 試合数 | W | L | 勝率 | 純コスト | 払戻 | P&L | ROI | 累積P&L |")
    L.append("|------|--------|---|---|------|----------|------|-----|-----|---------|")
    cumulative = 0.0
    for month in sorted(monthly):
        m = monthly[month]
        net = m["buy"] - m["sell"]
        payout = m["redeem"] + m["merge"]
        pnl = payout - net
        cumulative += pnl
        wr = m["wins"] / (m["wins"] + m["losses"]) * 100 if (m["wins"] + m["losses"]) > 0 else 0
        roi = pnl / net * 100 if net > 0 else 0
        L.append(
            f"| {month} | {m['games']:,} | {m['wins']} | {m['losses']} | "
            f"{wr:.1f}% | ${net:,.0f} | ${payout:,.0f} | ${pnl:,.0f} | {roi:.1f}% | ${cumulative:,.0f} |"
        )
    L.append("")

    # -- 5. 価格帯別 P&L (avg_buy_price) --
    L.append("---")
    L.append("## 5. 平均購入価格帯別 P&L")
    L.append("")

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

    L.append("| 価格帯 | Conditions | 勝ち | 負け/Open | 勝率 | 純コスト | 払戻 | P&L | ROI |")
    L.append("|--------|-----------|------|----------|------|----------|------|-----|-----|")
    for name, _, _ in price_buckets:
        pb = pb_data[name]
        pnl = pb["payout"] - pb["cost"]
        wr = pb["wins"] / (pb["wins"] + pb["losses"]) * 100 if (pb["wins"] + pb["losses"]) > 0 else 0
        roi = pnl / pb["cost"] * 100 if pb["cost"] > 0 else 0
        L.append(
            f"| {name} | {pb['count']:,} | {pb['wins']:,} | {pb['losses']:,} | "
            f"{wr:.1f}% | ${pb['cost']:,.0f} | ${pb['payout']:,.0f} | ${pnl:,.0f} | {roi:.1f}% |"
        )
    L.append("")

    # -- 6. P&L 上位/下位の試合 --
    L.append("---")
    L.append("## 6. 試合別 P&L ランキング")
    L.append("")

    settled_games = [g for g in games if g["total_redeem"] > 0 or g["total_merge"] > 0]
    sorted_by_pnl = sorted(settled_games, key=lambda x: x["total_pnl"], reverse=True)

    L.append("### Top 20 (利益)")
    L.append("")
    L.append("| # | 試合 | 日付 | スポーツ | MT | コスト | 払戻 | P&L | ROI |")
    L.append("|---|------|------|----------|-------|--------|------|-----|-----|")
    for i, g in enumerate(sorted_by_pnl[:20], 1):
        mts = "/".join(g["market_types"])
        L.append(
            f"| {i} | {g['game_key']} | {g['date']} | {g['sport']} | {mts} | "
            f"${g['net_cost']:,.0f} | ${g['total_payout']:,.0f} | "
            f"${g['total_pnl']:,.0f} | {g['roi_pct']:.0f}% |"
        )
    L.append("")

    L.append("### Bottom 20 (損失)")
    L.append("")
    L.append("| # | 試合 | 日付 | スポーツ | MT | コスト | 払戻 | P&L | ROI |")
    L.append("|---|------|------|----------|-------|--------|------|-----|-----|")
    worst = sorted_by_pnl[-20:][::-1]
    # ゼロ払戻(= LOSS_OR_OPEN)の中でコストが大きいものを拾う
    all_by_pnl = sorted(games, key=lambda x: x["total_pnl"])
    for i, g in enumerate(all_by_pnl[:20], 1):
        mts = "/".join(g["market_types"])
        L.append(
            f"| {i} | {g['game_key']} | {g['date']} | {g['sport']} | {mts} | "
            f"${g['net_cost']:,.0f} | ${g['total_payout']:,.0f} | "
            f"${g['total_pnl']:,.0f} | {g['roi_pct']:.0f}% |"
        )
    L.append("")

    # -- 7. P&L 分布統計 --
    L.append("---")
    L.append("## 7. 試合 P&L 分布")
    L.append("")

    game_pnls = [g["total_pnl"] for g in games if g["net_cost"] > 0]
    game_rois = [g["roi_pct"] for g in games if g["net_cost"] > 0]
    if game_pnls:
        L.append(f"- **平均 P&L / 試合**: ${mean(game_pnls):,.2f}")
        L.append(f"- **中央値 P&L / 試合**: ${median(game_pnls):,.2f}")
        L.append(f"- **最大利益**: ${max(game_pnls):,.2f}")
        L.append(f"- **最大損失**: ${min(game_pnls):,.2f}")
        L.append(f"- **平均 ROI / 試合**: {mean(game_rois):.2f}%")
        L.append(f"- **中央値 ROI / 試合**: {median(game_rois):.2f}%")
        L.append("")

        # P&L 分布ヒストグラム (テキスト)
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
        L.append("### P&L 分布")
        L.append("")
        L.append("| 範囲 | 試合数 | 比率 |")
        L.append("|------|--------|------|")
        for label, fn in buckets:
            cnt = sum(1 for p in game_pnls if fn(p))
            pct = cnt / len(game_pnls) * 100
            bar = "#" * int(pct / 2)
            L.append(f"| {label:>15} | {cnt:,} | {pct:.1f}% {bar} |")
        L.append("")

    # -- 8. 勝敗の連続性 --
    L.append("---")
    L.append("## 8. 連勝・連敗分析")
    L.append("")

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
        L.append(f"- **最長連勝**: {max(streaks_w)} 試合")
        L.append(f"- **平均連勝**: {mean(streaks_w):.1f} 試合")
    if streaks_l:
        L.append(f"- **最長連敗**: {max(streaks_l)} 試合")
        L.append(f"- **平均連敗**: {mean(streaks_l):.1f} 試合")
    L.append("")

    return "\n".join(L)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    print("Loading data...")
    trades, redeems, merges = load_data()
    print(f"  TRADE: {len(trades):,} | REDEEM: {len(redeems):,} | MERGE: {len(merges):,}")

    print("Building condition-level P&L...")
    conditions = build_condition_pnl(trades, redeems, merges)
    print(f"  {len(conditions):,} conditions")

    print("Aggregating by game...")
    games = aggregate_by_game(conditions)
    print(f"  {len(games):,} games")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # JSON 出力
    print("Writing JSON...")
    json_path = OUTPUT_DIR / "game_pnl.json"
    with open(json_path, "w") as f:
        json.dump(games, f, indent=2, ensure_ascii=False)
    print(f"  -> {json_path}")

    condition_list = sorted(conditions.values(), key=lambda x: x["pnl"], reverse=True)
    cond_path = OUTPUT_DIR / "condition_pnl.json"
    with open(cond_path, "w") as f:
        json.dump(condition_list, f, indent=2, ensure_ascii=False)
    print(f"  -> {cond_path}")

    # レポート出力
    print("Generating report...")
    report = generate_report(conditions, games)
    md_path = OUTPUT_DIR / "pnl_report.md"
    with open(md_path, "w") as f:
        f.write(report)
    print(f"  -> {md_path}")

    # クイックサマリー
    total_buy = sum(c["buy_cost"] for c in conditions.values())
    total_sell = sum(c["sell_proceeds"] for c in conditions.values())
    total_redeem = sum(c["redeem_usdc"] for c in conditions.values())
    total_merge = sum(c["merge_usdc"] for c in conditions.values())
    net_cost = total_buy - total_sell
    pnl = (total_redeem + total_merge) - net_cost
    print(f"\n{'='*50}")
    print(f"  Net Cost:    ${net_cost:,.2f}")
    print(f"  Total Payout: ${total_redeem + total_merge:,.2f}")
    print(f"  P&L:         ${pnl:,.2f}")
    print(f"  ROI:         {pnl / net_cost * 100:.2f}%")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
