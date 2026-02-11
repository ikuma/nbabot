"""Lightweight strategy profiling with risk management metrics.

Computes a StrategyProfile from condition-level and game-level P&L data
in a single pass. Prioritizes risk-adjusted metrics (Sharpe, drawdown,
consistency) over raw PnL.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import mean, median, stdev

from src.analysis.pnl import classify_category, classify_market_type, classify_sport


@dataclass
class PriceBandStat:
    """Stats for a 5-cent price band."""

    band: str  # e.g. "0.20-0.25"
    lo: float
    hi: float
    count: int = 0
    wins: int = 0
    losses: int = 0
    volume: float = 0.0
    pnl: float = 0.0

    @property
    def win_rate(self) -> float:
        total = self.wins + self.losses
        return self.wins / total if total > 0 else 0.0

    @property
    def roi(self) -> float:
        return self.pnl / self.volume * 100 if self.volume > 0 else 0.0

    def to_dict(self) -> dict:
        return {
            "band": self.band,
            "count": self.count,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate": round(self.win_rate, 4),
            "volume": round(self.volume, 2),
            "pnl": round(self.pnl, 2),
            "roi": round(self.roi, 2),
        }


@dataclass
class StrategyProfile:
    """Strategy fingerprint for a trader."""

    username: str
    address: str

    # 基本指標
    total_volume: float = 0.0
    net_cost: float = 0.0
    total_pnl: float = 0.0
    roi_pct: float = 0.0
    total_conditions: int = 0
    win_rate: float = 0.0
    active_months: int = 0

    # カテゴリ・マーケット分類
    category_pnl: dict[str, float] = field(default_factory=dict)
    sport_pnl: dict[str, float] = field(default_factory=dict)
    market_type_pnl: dict[str, float] = field(default_factory=dict)
    primary_category: str = ""

    # 校正曲線
    price_band_stats: list[dict] = field(default_factory=list)
    sweet_spot_concentration: float = 0.0  # 0.20-0.55 volume ratio

    # DCA
    dca_fraction: float = 0.0
    avg_trades_per_condition: float = 0.0
    avg_position_size: float = 0.0
    median_position_size: float = 0.0

    # リスク管理
    daily_sharpe: float = 0.0
    weekly_sharpe: float = 0.0
    max_drawdown_pct: float = 0.0
    max_drawdown_days: int = 0
    daily_win_rate: float = 0.0
    weekly_win_rate: float = 0.0
    profit_factor: float = 0.0
    consistency_score: float = 0.0

    # 月次トレンド
    monthly_pnl: dict[str, float] = field(default_factory=dict)

    # データ品質
    data_quality: str = "unknown"  # "complete" / "incomplete" / "unknown"
    leaderboard_pnl: float = 0.0
    leaderboard_volume: float = 0.0
    missing_trade_conditions: int = 0

    def to_dict(self) -> dict:
        """Serialize to JSON-compatible dict."""
        return {
            "username": self.username,
            "address": self.address,
            "total_volume": round(self.total_volume, 2),
            "net_cost": round(self.net_cost, 2),
            "total_pnl": round(self.total_pnl, 2),
            "roi_pct": round(self.roi_pct, 2),
            "total_conditions": self.total_conditions,
            "win_rate": round(self.win_rate, 4),
            "active_months": self.active_months,
            "category_pnl": {k: round(v, 2) for k, v in self.category_pnl.items()},
            "sport_pnl": {k: round(v, 2) for k, v in self.sport_pnl.items()},
            "market_type_pnl": {k: round(v, 2) for k, v in self.market_type_pnl.items()},
            "primary_category": self.primary_category,
            "price_band_stats": self.price_band_stats,
            "sweet_spot_concentration": round(self.sweet_spot_concentration, 4),
            "dca_fraction": round(self.dca_fraction, 4),
            "avg_trades_per_condition": round(self.avg_trades_per_condition, 2),
            "avg_position_size": round(self.avg_position_size, 2),
            "median_position_size": round(self.median_position_size, 2),
            "daily_sharpe": round(self.daily_sharpe, 3),
            "weekly_sharpe": round(self.weekly_sharpe, 3),
            "max_drawdown_pct": round(self.max_drawdown_pct, 2),
            "max_drawdown_days": self.max_drawdown_days,
            "daily_win_rate": round(self.daily_win_rate, 4),
            "weekly_win_rate": round(self.weekly_win_rate, 4),
            "profit_factor": round(self.profit_factor, 3),
            "consistency_score": round(self.consistency_score, 4),
            "monthly_pnl": {k: round(v, 2) for k, v in self.monthly_pnl.items()},
            "data_quality": self.data_quality,
            "leaderboard_pnl": round(self.leaderboard_pnl, 2),
            "leaderboard_volume": round(self.leaderboard_volume, 2),
            "missing_trade_conditions": self.missing_trade_conditions,
        }


def _ts_to_date(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _ts_to_week(ts: int) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    iso = dt.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _ts_to_month(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m")


def _compute_sharpe(pnl_series: list[float]) -> float:
    """Compute Sharpe ratio from a list of period P&L values."""
    if len(pnl_series) < 2:
        return 0.0
    avg = mean(pnl_series)
    sd = stdev(pnl_series)
    if sd == 0:
        return 0.0
    return avg / sd


def _compute_drawdown(cumulative_pnl: list[float]) -> tuple[float, int]:
    """Compute max drawdown % and recovery days from cumulative P&L series.

    Returns (max_drawdown_pct, max_drawdown_days).
    """
    if not cumulative_pnl:
        return 0.0, 0

    peak = cumulative_pnl[0]
    max_dd_pct = 0.0
    dd_start = 0
    max_dd_days = 0
    in_dd = False

    for i, val in enumerate(cumulative_pnl):
        if val > peak:
            peak = val
            if in_dd:
                max_dd_days = max(max_dd_days, i - dd_start)
                in_dd = False
        else:
            if peak > 0:
                dd_pct = (peak - val) / peak * 100
                if dd_pct > max_dd_pct:
                    max_dd_pct = dd_pct
            if not in_dd:
                dd_start = i
                in_dd = True

    if in_dd:
        max_dd_days = max(max_dd_days, len(cumulative_pnl) - dd_start)

    return max_dd_pct, max_dd_days


def build_profile(
    conditions: dict[str, dict],
    games: list[dict],
    username: str,
    address: str,
    lb_pnl: float = 0.0,
    lb_volume: float = 0.0,
    data_quality: str = "unknown",
    missing_trade_conditions: int = 0,
) -> StrategyProfile:
    """Build a StrategyProfile from condition-level and game-level P&L data."""
    profile = StrategyProfile(username=username, address=address)
    profile.leaderboard_pnl = lb_pnl
    profile.leaderboard_volume = lb_volume
    profile.data_quality = data_quality
    profile.missing_trade_conditions = missing_trade_conditions

    if not conditions:
        return profile

    # --- 基本指標 ---
    total_buy = sum(c["buy_cost"] for c in conditions.values())
    total_sell = sum(c["sell_proceeds"] for c in conditions.values())
    total_redeem = sum(c["redeem_usdc"] for c in conditions.values())
    total_merge = sum(c["merge_usdc"] for c in conditions.values())

    profile.total_volume = total_buy
    profile.net_cost = total_buy - total_sell
    profile.total_pnl = (total_redeem + total_merge) - profile.net_cost
    profile.roi_pct = (profile.total_pnl / profile.net_cost * 100) if profile.net_cost > 0 else 0.0
    profile.total_conditions = len(conditions)

    wins = sum(1 for c in conditions.values() if c["status"] == "WIN")
    losses = sum(1 for c in conditions.values() if c["status"] == "LOSS_OR_OPEN")
    profile.win_rate = wins / (wins + losses) if (wins + losses) > 0 else 0.0

    # --- カテゴリ / スポーツ / マーケットタイプ ---
    cat_pnl: dict[str, float] = defaultdict(float)
    sport_pnl: dict[str, float] = defaultdict(float)
    mt_pnl: dict[str, float] = defaultdict(float)

    for c in conditions.values():
        cat = c.get("category", classify_category(c.get("slug", ""), c.get("title", "")))
        cat_pnl[cat] += c["pnl"]
        if cat == "Sports":
            sport = c.get("sport", classify_sport(c.get("slug", "")))
            sport_pnl[sport] += c["pnl"]
        mt = c.get("market_type", classify_market_type(c.get("slug", "")))
        mt_pnl[mt] += c["pnl"]

    profile.category_pnl = dict(cat_pnl)
    profile.sport_pnl = dict(sport_pnl)
    profile.market_type_pnl = dict(mt_pnl)
    profile.primary_category = max(cat_pnl, key=lambda k: abs(cat_pnl[k])) if cat_pnl else "Other"

    # --- 校正曲線 (5¢ 刻み) ---
    bands: list[PriceBandStat] = []
    for lo_cents in range(0, 100, 5):
        lo = lo_cents / 100
        hi = (lo_cents + 5) / 100
        bands.append(
            PriceBandStat(
                band=f"{lo:.2f}-{hi:.2f}",
                lo=lo,
                hi=hi,
            )
        )

    sweet_spot_vol = 0.0
    total_vol = 0.0

    for c in conditions.values():
        avg_p = c.get("avg_buy_price", 0)
        cost = c.get("net_cost", 0)
        if avg_p <= 0 or cost <= 0:
            continue

        total_vol += cost
        if 0.20 <= avg_p < 0.55:
            sweet_spot_vol += cost

        for band in bands:
            if band.lo <= avg_p < band.hi or (band.hi == 1.0 and avg_p == 1.0):
                band.count += 1
                band.volume += cost
                band.pnl += c["pnl"]
                if c["status"] == "WIN":
                    band.wins += 1
                elif c["status"] == "LOSS_OR_OPEN":
                    band.losses += 1
                break

    profile.price_band_stats = [b.to_dict() for b in bands if b.count > 0]
    profile.sweet_spot_concentration = sweet_spot_vol / total_vol if total_vol > 0 else 0.0

    # --- DCA ---
    trade_counts = [c["trade_count"] for c in conditions.values() if c["trade_count"] > 0]
    if trade_counts:
        profile.avg_trades_per_condition = mean(trade_counts)
        profile.dca_fraction = sum(1 for tc in trade_counts if tc > 1) / len(trade_counts)

    position_sizes = [c["net_cost"] for c in conditions.values() if c["net_cost"] > 0]
    if position_sizes:
        profile.avg_position_size = mean(position_sizes)
        profile.median_position_size = median(position_sizes)

    # --- 日次/週次/月次 P&L 集計 ---
    daily_pnl: dict[str, float] = defaultdict(float)
    weekly_pnl: dict[str, float] = defaultdict(float)
    monthly_pnl: dict[str, float] = defaultdict(float)

    for g in games:
        date = g.get("date", "")
        month = g.get("month", "")
        pnl = g["total_pnl"]

        if date:
            daily_pnl[date] += pnl
        if month:
            monthly_pnl[month] += pnl

        # 週次: first_trade_ts が必要だが game には無いので date から推定
        if date:
            try:
                dt = datetime.strptime(date, "%Y-%m-%d")
                iso = dt.isocalendar()
                week_key = f"{iso[0]}-W{iso[1]:02d}"
                weekly_pnl[week_key] += pnl
            except ValueError:
                pass

    profile.monthly_pnl = dict(monthly_pnl)
    profile.active_months = len(monthly_pnl)

    # --- リスク指標 ---
    if daily_pnl:
        daily_values = [daily_pnl[d] for d in sorted(daily_pnl)]
        profile.daily_sharpe = _compute_sharpe(daily_values)
        positive_days = sum(1 for v in daily_values if v > 0)
        profile.daily_win_rate = positive_days / len(daily_values)

        cumulative = []
        cum = 0.0
        for v in daily_values:
            cum += v
            cumulative.append(cum)
        profile.max_drawdown_pct, profile.max_drawdown_days = _compute_drawdown(cumulative)

    if weekly_pnl:
        weekly_values = [weekly_pnl[w] for w in sorted(weekly_pnl)]
        profile.weekly_sharpe = _compute_sharpe(weekly_values)
        positive_weeks = sum(1 for v in weekly_values if v > 0)
        profile.weekly_win_rate = positive_weeks / len(weekly_values)

    # Profit factor = gross profit / gross loss
    gross_profit = sum(c["pnl"] for c in conditions.values() if c["pnl"] > 0)
    gross_loss = abs(sum(c["pnl"] for c in conditions.values() if c["pnl"] < 0))
    profile.profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    # Consistency: 月次黒字率
    if monthly_pnl:
        positive_months = sum(1 for v in monthly_pnl.values() if v > 0)
        profile.consistency_score = positive_months / len(monthly_pnl)

    return profile
