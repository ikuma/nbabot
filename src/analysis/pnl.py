"""Pure P&L computation functions extracted from scripts/analyze_pnl.py.

All functions are stateless (no I/O) — callers handle file loading and saving.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Sport / Market / Category classifiers
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
    ("mma-", "MMA"),
    ("f1-", "F1"),
    ("atp-", "ATP"),
    ("wta-", "WTA"),
    ("liga-", "LaLiga"),
    ("serie-a-", "SerieA"),
    ("bund-", "Bundesliga"),
    ("ligue1-", "Ligue1"),
]

# 非スポーツカテゴリ推定キーワード (title ベース)
CATEGORY_KEYWORDS: list[tuple[str, str]] = [
    ("president", "Politics"),
    ("election", "Politics"),
    ("trump", "Politics"),
    ("biden", "Politics"),
    ("congress", "Politics"),
    ("senate", "Politics"),
    ("vote", "Politics"),
    ("governor", "Politics"),
    ("democrat", "Politics"),
    ("republican", "Politics"),
    ("gop", "Politics"),
    ("primary", "Politics"),
    ("bitcoin", "Crypto"),
    ("btc", "Crypto"),
    ("ethereum", "Crypto"),
    ("eth", "Crypto"),
    ("crypto", "Crypto"),
    ("token", "Crypto"),
    ("solana", "Crypto"),
    ("sol", "Crypto"),
    ("defi", "Crypto"),
    ("oscar", "Culture"),
    ("grammy", "Culture"),
    ("emmy", "Culture"),
    ("movie", "Culture"),
    ("box office", "Culture"),
    ("album", "Culture"),
    ("spotify", "Culture"),
    ("twitter", "Culture"),
    ("fed", "Economics"),
    ("gdp", "Economics"),
    ("inflation", "Economics"),
    ("rate cut", "Economics"),
    ("unemployment", "Economics"),
    ("cpi", "Economics"),
    ("interest rate", "Economics"),
]


def classify_sport(slug: str) -> str:
    """Classify a market slug into a sport name."""
    for prefix, sport in SPORT_PREFIXES:
        if slug.startswith(prefix):
            return sport
    return "Other"


def classify_market_type(slug: str) -> str:
    """Classify a market slug into Moneyline/Spread/Total."""
    if "spread" in slug:
        return "Spread"
    if "total-" in slug or "-over-" in slug or "-under-" in slug:
        return "Total"
    return "Moneyline"


def classify_category(slug: str, title: str = "") -> str:
    """Classify into broad category: Sports, Politics, Crypto, Culture, etc.

    Checks sport prefix first; falls back to title keyword matching.
    """
    sport = classify_sport(slug)
    if sport != "Other":
        return "Sports"

    title_lower = title.lower()
    for keyword, category in CATEGORY_KEYWORDS:
        if keyword in title_lower:
            return category

    # slug にスポーツっぽいパターンがあればスポーツ
    sport_indicators = [
        "game",
        "match",
        "win",
        "vs-",
        "series",
        "championship",
        "playoffs",
        "finals",
        "bowl",
        "cup",
    ]
    slug_lower = slug.lower()
    for indicator in sport_indicators:
        if indicator in slug_lower:
            return "Sports"

    return "Other"


def ts_to_date(ts: int) -> str:
    """Convert unix timestamp to YYYY-MM-DD string (UTC)."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def ts_to_month(ts: int) -> str:
    """Convert unix timestamp to YYYY-MM string (UTC)."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m")


# ---------------------------------------------------------------------------
# Core P&L: condition-level
# ---------------------------------------------------------------------------
def _make_empty_condition(
    cid: str,
    slug: str,
    event_slug: str,
    title: str,
    first_ts: float,
    last_ts: float,
) -> dict:
    """Create an empty condition dict with default values."""
    return {
        "conditionId": cid,
        "slug": slug,
        "eventSlug": event_slug,
        "title": title,
        "sport": classify_sport(slug),
        "market_type": classify_market_type(slug),
        "category": classify_category(slug, title),
        "buy_cost": 0.0,
        "buy_shares": 0.0,
        "sell_proceeds": 0.0,
        "sell_shares": 0.0,
        "trade_count": 0,
        "first_trade_ts": first_ts,
        "last_trade_ts": last_ts,
        "settlement_ts": 0,
        "redeem_usdc": 0.0,
        "redeem_shares": 0.0,
        "merge_usdc": 0.0,
        "merge_shares": 0.0,
        "outcome_bought": "",
        "avg_buy_price": 0.0,
        "prices": [],
    }


def build_condition_pnl(
    trades: list[dict],
    redeems: list[dict],
    merges: list[dict],
) -> dict[str, dict]:
    """Compute P&L per conditionId from trades, redeems, and merges."""
    conditions: dict[str, dict] = {}

    # TRADE
    for t in trades:
        cid = t.get("conditionId", "")
        if not cid:
            continue
        if cid not in conditions:
            conditions[cid] = _make_empty_condition(
                cid,
                t.get("slug", ""),
                t.get("eventSlug", ""),
                t.get("title", ""),
                float("inf"),
                0,
            )

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

    # REDEEM
    for r in redeems:
        cid = r.get("conditionId", "")
        r_ts = r.get("timestamp", 0)
        if cid in conditions:
            conditions[cid]["redeem_usdc"] += float(r.get("usdcSize", 0))
            conditions[cid]["redeem_shares"] += float(r.get("size", 0))
            conditions[cid]["settlement_ts"] = max(conditions[cid]["settlement_ts"], r_ts)
        else:
            conditions[cid] = _make_empty_condition(
                cid,
                r.get("slug", ""),
                r.get("eventSlug", ""),
                r.get("title", ""),
                r["timestamp"],
                r["timestamp"],
            )
            conditions[cid]["redeem_usdc"] = float(r.get("usdcSize", 0))
            conditions[cid]["redeem_shares"] = float(r.get("size", 0))
            conditions[cid]["settlement_ts"] = r_ts

    # MERGE
    for m in merges:
        cid = m.get("conditionId", "")
        m_ts = m.get("timestamp", 0)
        if cid in conditions:
            conditions[cid]["merge_usdc"] += float(m.get("usdcSize", 0))
            conditions[cid]["merge_shares"] += float(m.get("size", 0))
            conditions[cid]["settlement_ts"] = max(conditions[cid]["settlement_ts"], m_ts)

    # P&L
    for cid, c in conditions.items():
        c["net_cost"] = c["buy_cost"] - c["sell_proceeds"]
        c["total_payout"] = c["redeem_usdc"] + c["merge_usdc"]
        c["pnl"] = c["total_payout"] - c["net_cost"]
        c["roi_pct"] = (c["pnl"] / c["net_cost"] * 100) if c["net_cost"] > 0 else 0.0
        c["avg_buy_price"] = c["buy_cost"] / c["buy_shares"] if c["buy_shares"] > 0 else 0.0

        if c["redeem_usdc"] > 0:
            c["status"] = "WIN"
        elif c["merge_usdc"] > 0:
            c["status"] = "MERGED"
        elif c["redeem_usdc"] == 0 and c["merge_usdc"] == 0 and c["buy_cost"] > 0:
            c["status"] = "LOSS_OR_OPEN"
        else:
            c["status"] = "UNKNOWN"

        # 保有期間 (hours)
        end_ts = c["settlement_ts"] if c["settlement_ts"] > 0 else c["last_trade_ts"]
        first_ts = c["first_trade_ts"]
        if first_ts < float("inf") and end_ts > 0 and end_ts >= first_ts:
            c["holding_hours"] = (end_ts - first_ts) / 3600.0
        else:
            c["holding_hours"] = 0.0

        # データ品質判定: BUY データ欠落の検知
        if c["buy_cost"] == 0 and c["total_payout"] > 0 and c["trade_count"] == 0:
            c["data_quality"] = "missing_trades"
        else:
            c["data_quality"] = "complete"

        del c["prices"]

    return conditions


# ---------------------------------------------------------------------------
# Game-level aggregation
# ---------------------------------------------------------------------------
def aggregate_by_game(conditions: dict[str, dict]) -> list[dict]:
    """Aggregate condition-level P&L to eventSlug (game) level."""
    games: dict[str, dict] = {}

    for cid, c in conditions.items():
        game_key = c["eventSlug"] or c["slug"]
        if not game_key:
            continue

        if game_key not in games:
            games[game_key] = {
                "game_key": game_key,
                "sport": c["sport"],
                "category": c.get("category", "Other"),
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

        g["fully_settled"] = g["loss_conditions"] == 0 or (
            g["win_conditions"] + g["merged_conditions"] > 0
        )

        del g["conditions"]
        del g["first_trade_ts"]
        result.append(g)

    result.sort(key=lambda x: x["date"])
    return result


# ---------------------------------------------------------------------------
# Data quality detection
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DataQualityReport:
    """Summary of data quality issues for a trader's condition set."""

    total_conditions: int
    missing_trade_conditions: int
    phantom_pnl: float
    activity_gaps_days: list[int]  # gaps > 30 days between consecutive conditions
    quality_score: str  # "complete" / "incomplete" / "suspect"


def detect_data_quality_issues(conditions: dict[str, dict]) -> DataQualityReport:
    """Detect data quality issues from condition-level P&L data.

    Args:
        conditions: Output of build_condition_pnl().

    Returns:
        DataQualityReport summarising issues found.
    """
    if not conditions:
        return DataQualityReport(
            total_conditions=0,
            missing_trade_conditions=0,
            phantom_pnl=0.0,
            activity_gaps_days=[],
            quality_score="complete",
        )

    missing = [c for c in conditions.values() if c.get("data_quality") == "missing_trades"]
    missing_count = len(missing)
    phantom_pnl = sum(c["pnl"] for c in missing)

    # Activity gaps: sort conditions by first_trade_ts, find >30-day gaps
    valid_ts = sorted(
        c["first_trade_ts"]
        for c in conditions.values()
        if c["first_trade_ts"] < float("inf") and c["first_trade_ts"] > 0
    )
    gaps: list[int] = []
    for i in range(1, len(valid_ts)):
        gap_days = int((valid_ts[i] - valid_ts[i - 1]) / 86400)
        if gap_days > 30:
            gaps.append(gap_days)

    # Quality score
    total = len(conditions)
    if missing_count == 0 and not gaps:
        quality = "complete"
    elif missing_count > total * 0.1 or phantom_pnl > 1000:
        quality = "suspect"
    else:
        quality = "incomplete"

    return DataQualityReport(
        total_conditions=total,
        missing_trade_conditions=missing_count,
        phantom_pnl=round(phantom_pnl, 2),
        activity_gaps_days=gaps,
        quality_score=quality,
    )


# Re-export generate_report for backward compatibility
from src.analysis.report_generator import generate_report  # noqa: E402, F401
