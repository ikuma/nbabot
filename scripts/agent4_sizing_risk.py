"""
Agent 4: Sizing & Risk Analysis for lhtsports.

Analyzes position sizing logic, risk metrics, equity curve,
Kelly efficiency, concentration risk, and holding periods.
"""

import json
import os
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

BASE = Path("/Users/taro/dev/nbabot")
DATA_PNL = BASE / "data/reports/lhtsports-pnl"
DATA_ANALYSIS = BASE / "data/reports/lhtsports-analysis"
OUT_DIR = DATA_PNL / "deep-analysis"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> list:
    with open(path) as f:
        return json.load(f)


def ts_to_date(ts: int) -> str:
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")


def ts_to_datetime(ts: int) -> datetime:
    return datetime.utcfromtimestamp(ts)


def size_bucket(net_cost: float) -> str:
    if net_cost < 100:
        return "<$100"
    elif net_cost < 500:
        return "$100-500"
    elif net_cost < 1000:
        return "$500-1K"
    elif net_cost < 5000:
        return "$1K-5K"
    elif net_cost < 10000:
        return "$5K-10K"
    elif net_cost < 50000:
        return "$10K-50K"
    else:
        return "$50K+"


BUCKET_ORDER = ["<$100", "$100-500", "$500-1K", "$1K-5K", "$5K-10K", "$10K-50K", "$50K+"]


def mean(vals):
    return sum(vals) / len(vals) if vals else 0.0


def median(vals):
    if not vals:
        return 0.0
    s = sorted(vals)
    n = len(s)
    if n % 2 == 1:
        return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / 2


def std(vals):
    if len(vals) < 2:
        return 0.0
    m = mean(vals)
    return (sum((x - m) ** 2 for x in vals) / (len(vals) - 1)) ** 0.5


def percentile(vals, p):
    if not vals:
        return 0.0
    s = sorted(vals)
    k = (len(s) - 1) * p / 100
    f = int(k)
    c = f + 1 if f + 1 < len(s) else f
    d = k - f
    return s[f] + d * (s[c] - s[f])


def correlation(xs, ys):
    """Pearson correlation coefficient."""
    if len(xs) < 2:
        return 0.0
    mx, my = mean(xs), mean(ys)
    sx, sy = std(xs), std(ys)
    if sx == 0 or sy == 0:
        return 0.0
    n = len(xs)
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (n - 1)
    return cov / (sx * sy)


def fmt_usd(v):
    return f"${v:,.2f}"


def fmt_pct(v):
    return f"{v:.2f}%"


def main():
    print("Loading data...")
    conditions = load_json(DATA_PNL / "condition_pnl.json")
    games = load_json(DATA_PNL / "game_pnl.json")
    redeems = load_json(DATA_ANALYSIS / "lhtsports_redeem.json")
    merges = load_json(DATA_ANALYSIS / "lhtsports_merge.json")

    print(f"  Conditions: {len(conditions)}")
    print(f"  Games: {len(games)}")
    print(f"  Redeems: {len(redeems)}")
    print(f"  Merges: {len(merges)}")

    # Build lookup maps for redeems/merges by conditionId
    redeem_by_cond = defaultdict(list)
    for r in redeems:
        redeem_by_cond[r["conditionId"]].append(r)

    merge_by_cond = defaultdict(list)
    for m in merges:
        merge_by_cond[m["conditionId"]].append(m)

    # =========================================================================
    # 1. POSITION SIZE DISTRIBUTION
    # =========================================================================
    print("\n1. Position size distribution...")
    all_sizes = [c["net_cost"] for c in conditions]
    size_stats = {
        "mean": mean(all_sizes),
        "median": median(all_sizes),
        "std": std(all_sizes),
        "min": min(all_sizes),
        "max": max(all_sizes),
        "p25": percentile(all_sizes, 25),
        "p75": percentile(all_sizes, 75),
        "p95": percentile(all_sizes, 95),
        "p99": percentile(all_sizes, 99),
    }
    print(f"  Mean: {fmt_usd(size_stats['mean'])}, Median: {fmt_usd(size_stats['median'])}")

    # Bucket distribution
    bucket_counts = defaultdict(int)
    bucket_total_cost = defaultdict(float)
    for c in conditions:
        b = size_bucket(c["net_cost"])
        bucket_counts[b] += 1
        bucket_total_cost[b] += c["net_cost"]

    # By sport
    sport_sizes = defaultdict(list)
    for c in conditions:
        sport_sizes[c["sport"]].append(c["net_cost"])

    sport_size_stats = {}
    for sport, sizes in sorted(sport_sizes.items(), key=lambda x: -sum(x[1])):
        sport_size_stats[sport] = {
            "count": len(sizes),
            "mean": mean(sizes),
            "median": median(sizes),
            "total": sum(sizes),
        }

    # By market_type
    mtype_sizes = defaultdict(list)
    for c in conditions:
        mtype_sizes[c["market_type"]].append(c["net_cost"])

    mtype_size_stats = {}
    for mt, sizes in sorted(mtype_sizes.items(), key=lambda x: -sum(x[1])):
        mtype_size_stats[mt] = {
            "count": len(sizes),
            "mean": mean(sizes),
            "median": median(sizes),
            "total": sum(sizes),
        }

    # =========================================================================
    # 2. SIZE DETERMINANTS
    # =========================================================================
    print("\n2. Size determinants...")

    # net_cost vs avg_buy_price
    valid_price_conds = [c for c in conditions if c["avg_buy_price"] > 0]
    prices = [c["avg_buy_price"] for c in valid_price_conds]
    sizes_for_price = [c["net_cost"] for c in valid_price_conds]
    corr_size_price = correlation(sizes_for_price, prices)
    print(f"  Corr(net_cost, avg_buy_price): {corr_size_price:.4f}")

    # Favorites vs underdogs
    fav_sizes = [c["net_cost"] for c in conditions if c["avg_buy_price"] >= 0.5]
    dog_sizes = [c["net_cost"] for c in conditions if 0 < c["avg_buy_price"] < 0.5]
    fav_vs_dog = {
        "favorites_count": len(fav_sizes),
        "favorites_avg_size": mean(fav_sizes),
        "underdogs_count": len(dog_sizes),
        "underdogs_avg_size": mean(dog_sizes),
    }
    print(f"  Favorites avg size: {fmt_usd(fav_vs_dog['favorites_avg_size'])}")
    print(f"  Underdogs avg size: {fmt_usd(fav_vs_dog['underdogs_avg_size'])}")

    # By month
    month_sizes = defaultdict(list)
    for c in conditions:
        month = datetime.utcfromtimestamp(c["first_trade_ts"]).strftime("%Y-%m")
        month_sizes[month].append(c["net_cost"])

    month_size_stats = {}
    for month in sorted(month_sizes.keys()):
        sizes = month_sizes[month]
        month_size_stats[month] = {
            "count": len(sizes),
            "mean": mean(sizes),
            "median": median(sizes),
            "total": sum(sizes),
        }

    # =========================================================================
    # 3. SIZE VS OUTCOME
    # =========================================================================
    print("\n3. Size vs outcome...")
    status_sizes = defaultdict(list)
    for c in conditions:
        status_sizes[c["status"]].append(c)

    all_statuses = sorted(status_sizes.keys())
    size_vs_outcome = {}
    for status in all_statuses:
        conds = status_sizes.get(status, [])
        costs = [c["net_cost"] for c in conds]
        size_vs_outcome[status] = {
            "count": len(conds),
            "avg_size": mean(costs),
            "median_size": median(costs),
            "total_cost": sum(costs),
        }
        print(f"  {status}: count={len(conds)}, avg={fmt_usd(mean(costs))}")

    # ROI by size bucket
    bucket_pnls = defaultdict(list)
    bucket_wins = defaultdict(int)
    bucket_total = defaultdict(int)
    for c in conditions:
        b = size_bucket(c["net_cost"])
        bucket_pnls[b].append(c["pnl"])
        bucket_total[b] += 1
        if c["status"] == "WIN":
            bucket_wins[b] += 1

    roi_by_bucket = {}
    for b in BUCKET_ORDER:
        pnls = bucket_pnls.get(b, [])
        total = bucket_total.get(b, 0)
        wins = bucket_wins.get(b, 0)
        costs = [c["net_cost"] for c in conditions if size_bucket(c["net_cost"]) == b]
        total_cost = sum(costs)
        total_pnl = sum(pnls)
        roi_by_bucket[b] = {
            "count": total,
            "win_rate": (wins / total * 100) if total > 0 else 0,
            "total_pnl": total_pnl,
            "total_cost": total_cost,
            "roi_pct": (total_pnl / total_cost * 100) if total_cost > 0 else 0,
        }

    # =========================================================================
    # 4. KELLY EFFICIENCY
    # =========================================================================
    print("\n4. Kelly efficiency analysis...")

    # Price bands in 0.05 increments
    price_bands = {}
    for c in conditions:
        if c["avg_buy_price"] <= 0:
            continue
        band_low = int(c["avg_buy_price"] / 0.05) * 0.05
        band_label = f"{band_low:.2f}-{band_low + 0.05:.2f}"
        if band_label not in price_bands:
            price_bands[band_label] = {"wins": 0, "total": 0, "costs": [], "pnls": [], "band_low": band_low}
        price_bands[band_label]["total"] += 1
        if c["status"] == "WIN":
            price_bands[band_label]["wins"] += 1
        price_bands[band_label]["costs"].append(c["net_cost"])
        price_bands[band_label]["pnls"].append(c["pnl"])

    kelly_analysis = []
    for band_label in sorted(price_bands.keys()):
        info = price_bands[band_label]
        if info["total"] < 10:
            continue
        win_rate = info["wins"] / info["total"]
        avg_price = info["band_low"] + 0.025  # midpoint
        # Payoff: buying at avg_price, payout is 1.0 on win
        # b = (1 - avg_price) / avg_price  (net gain per dollar risked on win)
        b = (1.0 - avg_price) / avg_price if avg_price > 0 else 0
        # Kelly: f* = (bp - q) / b where p=win_rate, q=1-win_rate
        q = 1 - win_rate
        kelly_f = (b * win_rate - q) / b if b > 0 else 0

        # Actual sizing: avg cost as fraction of total capital deployed that month
        avg_cost = mean(info["costs"])
        total_cost = sum(info["costs"])

        kelly_analysis.append({
            "band": band_label,
            "count": info["total"],
            "win_rate": win_rate,
            "avg_price": avg_price,
            "implied_odds": b,
            "kelly_fraction": max(kelly_f, 0),
            "avg_position_size": avg_cost,
            "total_cost": total_cost,
            "total_pnl": sum(info["pnls"]),
        })

    # Overall Kelly comparison: compute avg theoretical Kelly fraction
    kelly_fractions = [k["kelly_fraction"] for k in kelly_analysis if k["kelly_fraction"] > 0]
    avg_kelly = mean(kelly_fractions) if kelly_fractions else 0

    # Monthly capital deployed
    monthly_capital = defaultdict(float)
    for c in conditions:
        month = datetime.utcfromtimestamp(c["first_trade_ts"]).strftime("%Y-%m")
        monthly_capital[month] += c["net_cost"]

    # Actual bet fraction: average net_cost / monthly capital
    actual_fractions = []
    for c in conditions:
        month = datetime.utcfromtimestamp(c["first_trade_ts"]).strftime("%Y-%m")
        mc = monthly_capital[month]
        if mc > 0:
            actual_fractions.append(c["net_cost"] / mc)
    avg_actual_fraction = mean(actual_fractions) if actual_fractions else 0

    kelly_summary = {
        "avg_kelly_fraction": round(avg_kelly, 4),
        "avg_actual_fraction": round(avg_actual_fraction, 6),
        "actual_vs_kelly_ratio": round(avg_actual_fraction / avg_kelly, 4) if avg_kelly > 0 else None,
        "over_or_under_betting": "under-betting" if avg_actual_fraction < avg_kelly else "over-betting",
    }
    print(f"  Avg Kelly fraction: {avg_kelly:.4f}")
    print(f"  Avg actual fraction: {avg_actual_fraction:.6f}")
    print(f"  Verdict: {kelly_summary['over_or_under_betting']}")

    # =========================================================================
    # 5. EQUITY CURVE
    # =========================================================================
    print("\n5. Equity curve...")

    # Sort conditions by last_trade_ts for settlement ordering
    sorted_conds = sorted(conditions, key=lambda c: c["last_trade_ts"])
    cumulative_pnl = []
    running = 0.0
    for c in sorted_conds:
        running += c["pnl"]
        cumulative_pnl.append({
            "ts": c["last_trade_ts"],
            "date": ts_to_date(c["last_trade_ts"]),
            "pnl": c["pnl"],
            "cumulative": running,
        })
    print(f"  Final cumulative P&L: {fmt_usd(running)}")

    # Daily P&L from game_pnl (has date field)
    daily_pnl = defaultdict(float)
    for g in games:
        if g.get("date"):
            daily_pnl[g["date"]] += g["total_pnl"]

    # Also build daily from conditions for finer granularity
    daily_pnl_cond = defaultdict(float)
    for c in conditions:
        d = ts_to_date(c["last_trade_ts"])
        daily_pnl_cond[d] += c["pnl"]

    sorted_days = sorted(daily_pnl_cond.keys())
    daily_cumulative = []
    running_daily = 0.0
    for d in sorted_days:
        running_daily += daily_pnl_cond[d]
        daily_cumulative.append({"date": d, "daily_pnl": daily_pnl_cond[d], "cumulative": running_daily})

    # Weekly P&L
    weekly_pnl = defaultdict(float)
    for d, pnl in daily_pnl_cond.items():
        dt = datetime.strptime(d, "%Y-%m-%d")
        # ISO week
        iso_year, iso_week, _ = dt.isocalendar()
        week_key = f"{iso_year}-W{iso_week:02d}"
        weekly_pnl[week_key] += pnl

    sorted_weeks = sorted(weekly_pnl.keys())

    # =========================================================================
    # 6. RISK METRICS
    # =========================================================================
    print("\n6. Risk metrics...")

    daily_pnl_values = [daily_pnl_cond[d] for d in sorted_days]
    weekly_pnl_values = [weekly_pnl[w] for w in sorted_weeks]

    # Daily Sharpe
    daily_mean = mean(daily_pnl_values)
    daily_std = std(daily_pnl_values)
    daily_sharpe = (daily_mean / daily_std * (365 ** 0.5)) if daily_std > 0 else 0
    print(f"  Daily Sharpe (annualized): {daily_sharpe:.4f}")

    # Weekly Sharpe
    weekly_mean = mean(weekly_pnl_values)
    weekly_std = std(weekly_pnl_values)
    weekly_sharpe = (weekly_mean / weekly_std * (52 ** 0.5)) if weekly_std > 0 else 0
    print(f"  Weekly Sharpe (annualized): {weekly_sharpe:.4f}")

    # Maximum drawdown
    peak = 0.0
    max_dd = 0.0
    max_dd_peak = 0.0
    max_dd_trough = 0.0
    dd_start_date = None
    dd_end_date = None
    current_dd_start = None

    running_dd = 0.0
    for entry in daily_cumulative:
        cum = entry["cumulative"]
        if cum > peak:
            peak = cum
            current_dd_start = entry["date"]
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd
            max_dd_peak = peak
            max_dd_trough = cum
            dd_start_date = current_dd_start
            dd_end_date = entry["date"]

    max_dd_pct = (max_dd / max_dd_peak * 100) if max_dd_peak > 0 else 0
    if dd_start_date and dd_end_date:
        dd_duration = (datetime.strptime(dd_end_date, "%Y-%m-%d") - datetime.strptime(dd_start_date, "%Y-%m-%d")).days
    else:
        dd_duration = 0
    print(f"  Max drawdown: {fmt_usd(max_dd)} ({fmt_pct(max_dd_pct)}), duration: {dd_duration} days")

    # Max single-day loss/gain
    max_daily_loss = min(daily_pnl_values) if daily_pnl_values else 0
    max_daily_gain = max(daily_pnl_values) if daily_pnl_values else 0
    worst_day = sorted_days[daily_pnl_values.index(max_daily_loss)] if daily_pnl_values else ""
    best_day = sorted_days[daily_pnl_values.index(max_daily_gain)] if daily_pnl_values else ""
    print(f"  Max daily loss: {fmt_usd(max_daily_loss)} ({worst_day})")
    print(f"  Max daily gain: {fmt_usd(max_daily_gain)} ({best_day})")

    # Win rate by day
    winning_days = sum(1 for v in daily_pnl_values if v > 0)
    daily_win_rate = winning_days / len(daily_pnl_values) * 100 if daily_pnl_values else 0

    # Win rate by week
    winning_weeks = sum(1 for v in weekly_pnl_values if v > 0)
    weekly_win_rate = winning_weeks / len(weekly_pnl_values) * 100 if weekly_pnl_values else 0

    print(f"  Daily win rate: {fmt_pct(daily_win_rate)} ({winning_days}/{len(daily_pnl_values)})")
    print(f"  Weekly win rate: {fmt_pct(weekly_win_rate)} ({winning_weeks}/{len(weekly_pnl_values)})")

    risk_metrics = {
        "daily_sharpe": round(daily_sharpe, 4),
        "weekly_sharpe": round(weekly_sharpe, 4),
        "max_drawdown_usd": round(max_dd, 2),
        "max_drawdown_pct": round(max_dd_pct, 2),
        "max_drawdown_days": dd_duration,
        "max_drawdown_period": f"{dd_start_date} to {dd_end_date}",
        "max_daily_loss": round(max_daily_loss, 2),
        "max_daily_loss_date": worst_day,
        "max_daily_gain": round(max_daily_gain, 2),
        "max_daily_gain_date": best_day,
        "daily_win_rate": round(daily_win_rate, 2),
        "weekly_win_rate": round(weekly_win_rate, 2),
        "total_trading_days": len(daily_pnl_values),
        "total_trading_weeks": len(weekly_pnl_values),
        "daily_pnl_mean": round(daily_mean, 2),
        "daily_pnl_std": round(daily_std, 2),
        "weekly_pnl_mean": round(weekly_mean, 2),
        "weekly_pnl_std": round(weekly_std, 2),
    }

    # =========================================================================
    # 7. CONCENTRATION RISK
    # =========================================================================
    print("\n7. Concentration risk...")

    # Daily exposure: sum of net_cost for conditions entering that day
    daily_exposure = defaultdict(float)
    for c in conditions:
        d = ts_to_date(c["first_trade_ts"])
        daily_exposure[d] += c["net_cost"]

    max_daily_exp_date = max(daily_exposure, key=daily_exposure.get) if daily_exposure else ""
    max_daily_exp = daily_exposure.get(max_daily_exp_date, 0)
    print(f"  Max daily exposure: {fmt_usd(max_daily_exp)} ({max_daily_exp_date})")

    # Max single-game exposure (from game_pnl)
    max_game = max(games, key=lambda g: g["net_cost"]) if games else None
    max_game_exposure = max_game["net_cost"] if max_game else 0
    max_game_key = max_game["game_key"] if max_game else ""
    max_game_pnl = max_game["total_pnl"] if max_game else 0
    print(f"  Max single-game exposure: {fmt_usd(max_game_exposure)} ({max_game_key})")

    # Top 10 largest positions
    top10 = sorted(conditions, key=lambda c: -c["net_cost"])[:10]
    top10_list = []
    for c in top10:
        top10_list.append({
            "slug": c["slug"],
            "outcome": c["outcome_bought"],
            "net_cost": round(c["net_cost"], 2),
            "pnl": round(c["pnl"], 2),
            "roi_pct": round(c["roi_pct"], 2),
            "status": c["status"],
            "avg_buy_price": round(c["avg_buy_price"], 4),
        })

    concentration = {
        "max_daily_exposure": round(max_daily_exp, 2),
        "max_daily_exposure_date": max_daily_exp_date,
        "max_single_game_exposure": round(max_game_exposure, 2),
        "max_single_game_key": max_game_key,
        "max_single_game_pnl": round(max_game_pnl, 2),
        "top10_positions": top10_list,
    }

    # =========================================================================
    # 8. MERGE AS STOP-LOSS
    # =========================================================================
    print("\n8. MERGE as stop-loss analysis...")

    merged_conds = [c for c in conditions if c["status"] == "MERGED"]
    merge_analysis_records = []

    for c in merged_conds:
        cid = c["conditionId"]
        cond_merges = merge_by_cond.get(cid, [])
        if not cond_merges:
            continue

        # Last merge timestamp
        last_merge_ts = max(m["timestamp"] for m in cond_merges)
        total_merge_usdc = sum(m["usdcSize"] for m in cond_merges)

        # Time from last trade to merge
        time_to_merge_hours = (last_merge_ts - c["last_trade_ts"]) / 3600

        # Was this position already losing? (merge_usdc < net_cost => losing)
        was_losing = total_merge_usdc < c["net_cost"]

        merge_analysis_records.append({
            "conditionId": cid,
            "slug": c["slug"],
            "net_cost": c["net_cost"],
            "merge_payout": total_merge_usdc,
            "pnl": c["pnl"],
            "time_to_merge_hours": time_to_merge_hours,
            "merge_ratio": total_merge_usdc / c["net_cost"] if c["net_cost"] > 0 else 0,
            "was_losing": was_losing,
        })

    if merge_analysis_records:
        merge_times = [r["time_to_merge_hours"] for r in merge_analysis_records if r["time_to_merge_hours"] >= 0]
        merge_ratios = [r["merge_ratio"] for r in merge_analysis_records]
        losing_count = sum(1 for r in merge_analysis_records if r["was_losing"])

        merge_stats = {
            "total_merged_conditions": len(merged_conds),
            "matched_with_merge_data": len(merge_analysis_records),
            "avg_time_to_merge_hours": round(mean(merge_times), 2) if merge_times else None,
            "median_time_to_merge_hours": round(median(merge_times), 2) if merge_times else None,
            "avg_merge_ratio": round(mean(merge_ratios), 4),
            "median_merge_ratio": round(median(merge_ratios), 4),
            "pct_already_losing": round(losing_count / len(merge_analysis_records) * 100, 2),
            "avg_merge_pnl": round(mean([r["pnl"] for r in merge_analysis_records]), 2),
        }
    else:
        merge_stats = {
            "total_merged_conditions": len(merged_conds),
            "matched_with_merge_data": 0,
        }
    print(f"  Merged conditions: {len(merged_conds)}")
    print(f"  Matched with merge data: {merge_stats.get('matched_with_merge_data', 0)}")
    if merge_stats.get("avg_time_to_merge_hours") is not None:
        print(f"  Avg time to merge: {merge_stats['avg_time_to_merge_hours']:.1f} hours")
        print(f"  Avg merge ratio (payout/cost): {merge_stats['avg_merge_ratio']:.4f}")
        print(f"  % already losing at merge: {merge_stats['pct_already_losing']:.1f}%")

    # =========================================================================
    # 9. HOLDING PERIOD ANALYSIS
    # =========================================================================
    print("\n9. Holding period analysis...")

    holding_periods = []
    for c in conditions:
        cid = c["conditionId"]
        # Find settlement timestamp: latest redeem or merge
        settlement_ts = None

        cond_redeems = redeem_by_cond.get(cid, [])
        cond_merges = merge_by_cond.get(cid, [])

        if cond_redeems:
            settlement_ts = max(r["timestamp"] for r in cond_redeems)
        elif cond_merges:
            settlement_ts = max(m["timestamp"] for m in cond_merges)

        if settlement_ts and settlement_ts > c["first_trade_ts"]:
            hold_days = (settlement_ts - c["first_trade_ts"]) / 86400
            holding_periods.append({
                "slug": c["slug"],
                "hold_days": hold_days,
                "pnl": c["pnl"],
                "net_cost": c["net_cost"],
                "status": c["status"],
            })

    if holding_periods:
        hold_days_vals = [h["hold_days"] for h in holding_periods]
        hold_pnls = [h["pnl"] for h in holding_periods]
        corr_hold_pnl = correlation(hold_days_vals, hold_pnls)

        holding_stats = {
            "conditions_with_settlement": len(holding_periods),
            "mean_days": round(mean(hold_days_vals), 2),
            "median_days": round(median(hold_days_vals), 2),
            "min_days": round(min(hold_days_vals), 2),
            "max_days": round(max(hold_days_vals), 2),
            "p25_days": round(percentile(hold_days_vals, 25), 2),
            "p75_days": round(percentile(hold_days_vals, 75), 2),
            "corr_hold_pnl": round(corr_hold_pnl, 4),
        }

        # Holding period by status
        all_hold_statuses = sorted(set(h["status"] for h in holding_periods))
        holding_stats["by_status"] = {}
        for status in all_hold_statuses:
            vals = [h["hold_days"] for h in holding_periods if h["status"] == status]
            if vals:
                holding_stats["by_status"][status] = {
                    "mean_days": round(mean(vals), 2),
                    "median_days": round(median(vals), 2),
                    "count": len(vals),
                }
    else:
        holding_stats = {"conditions_with_settlement": 0, "mean_days": 0, "median_days": 0}

    print(f"  Conditions with settlement: {holding_stats['conditions_with_settlement']}")
    print(f"  Mean holding period: {holding_stats.get('mean_days', 0):.1f} days")
    print(f"  Median holding period: {holding_stats.get('median_days', 0):.1f} days")
    print(f"  Corr(hold_days, pnl): {holding_stats.get('corr_hold_pnl', 0):.4f}")

    # =========================================================================
    # KEY FINDINGS & RECOMMENDATIONS
    # =========================================================================
    print("\n10. Generating key findings and recommendations...")

    # Determine key findings
    key_findings = []

    # Size insight
    key_findings.append(
        f"Median position size is {fmt_usd(size_stats['median'])} with a wide range "
        f"({fmt_usd(size_stats['min'])} to {fmt_usd(size_stats['max'])}), "
        f"indicating dynamic sizing rather than fixed-bet."
    )

    # Favorite vs underdog
    if fav_vs_dog["favorites_avg_size"] > fav_vs_dog["underdogs_avg_size"]:
        key_findings.append(
            f"Larger positions on favorites (avg {fmt_usd(fav_vs_dog['favorites_avg_size'])}) "
            f"vs underdogs (avg {fmt_usd(fav_vs_dog['underdogs_avg_size'])}), "
            f"suggesting confidence-based sizing."
        )
    else:
        key_findings.append(
            f"Larger positions on underdogs (avg {fmt_usd(fav_vs_dog['underdogs_avg_size'])}) "
            f"vs favorites (avg {fmt_usd(fav_vs_dog['favorites_avg_size'])}), "
            f"suggesting value-seeking behavior."
        )

    # Win rate by size
    largest_bucket_wr = roi_by_bucket.get("$50K+", {}).get("win_rate", 0)
    smallest_bucket_wr = roi_by_bucket.get("<$100", {}).get("win_rate", 0)
    if largest_bucket_wr > smallest_bucket_wr:
        key_findings.append(
            f"Higher win rate on largest positions ({fmt_pct(largest_bucket_wr)}) vs smallest "
            f"({fmt_pct(smallest_bucket_wr)}), showing conviction sizing pays off."
        )

    # Risk
    key_findings.append(
        f"Daily Sharpe ratio of {daily_sharpe:.2f} and weekly Sharpe of {weekly_sharpe:.2f}. "
        f"Maximum drawdown of {fmt_usd(max_dd)} ({fmt_pct(max_dd_pct)}) lasting {dd_duration} days."
    )

    # Kelly
    key_findings.append(
        f"Kelly analysis shows lhtsports is {kelly_summary['over_or_under_betting']} "
        f"relative to theoretical Kelly optimal."
    )

    # Merge/stop-loss
    if merge_stats.get("pct_already_losing") is not None:
        key_findings.append(
            f"{merge_stats['pct_already_losing']:.0f}% of MERGEd positions were already losing, "
            f"confirming MERGE is used as a stop-loss mechanism."
        )

    # Daily win rate
    key_findings.append(
        f"Profitable on {fmt_pct(daily_win_rate)} of trading days and {fmt_pct(weekly_win_rate)} of weeks."
    )

    # Recommendations for nbabot
    nbabot_recommendations = []
    nbabot_recommendations.append(
        f"Target median position size around {fmt_usd(size_stats['median'])} as a starting reference, "
        f"scaled down for initial deployment."
    )
    nbabot_recommendations.append(
        "Use Kelly fraction with a conservative multiplier (0.25x) given the observed under-betting pattern. "
        "lhtsports' success despite under-betting suggests risk management > aggression."
    )
    nbabot_recommendations.append(
        f"Implement a stop-loss mechanism (MERGE equivalent) -- {merge_stats.get('pct_already_losing', 0):.0f}% of "
        f"lhtsports' merges were on losing positions, acting as effective risk control."
    )
    nbabot_recommendations.append(
        f"Set maximum single-game exposure limit: lhtsports' max was {fmt_usd(max_game_exposure)}. "
        f"For nbabot, start with $500-1000 max."
    )
    nbabot_recommendations.append(
        f"Target holding periods around {holding_stats.get('median_days', 0):.0f} days; "
        f"avoid positions that require very long holds."
    )
    nbabot_recommendations.append(
        f"Monitor daily Sharpe ratio. lhtsports achieves {daily_sharpe:.2f}; "
        f"aim for at least 1.0 before scaling up."
    )

    # =========================================================================
    # WRITE OUTPUTS
    # =========================================================================
    print("\nWriting outputs...")

    # Summary JSON
    summary = {
        "agent": "sizing-and-risk-analyst",
        "total_conditions": len(conditions),
        "total_games": len(games),
        "position_size_stats": {k: round(v, 2) for k, v in size_stats.items()},
        "size_distribution": {b: {"count": bucket_counts.get(b, 0), "total_cost": round(bucket_total_cost.get(b, 0), 2)} for b in BUCKET_ORDER},
        "size_by_sport": {s: {k: round(v, 2) if isinstance(v, float) else v for k, v in stats.items()} for s, stats in sport_size_stats.items()},
        "size_by_market_type": {m: {k: round(v, 2) if isinstance(v, float) else v for k, v in stats.items()} for m, stats in mtype_size_stats.items()},
        "size_determinants": {
            "corr_size_price": round(corr_size_price, 4),
            "favorites": {k: round(v, 2) if isinstance(v, float) else v for k, v in fav_vs_dog.items()},
        },
        "size_vs_outcome": {
            status: {"avg_size": round(info.get("avg_size", 0), 2), "count": info.get("count", 0)}
            for status, info in size_vs_outcome.items()
        },
        "roi_by_size_bucket": {b: {k: round(v, 2) if isinstance(v, float) else v for k, v in info.items()} for b, info in roi_by_bucket.items()},
        "equity_curve_final_pnl": round(running, 2),
        "risk_metrics": risk_metrics,
        "concentration": concentration,
        "kelly_analysis": kelly_summary,
        "kelly_by_price_band": [
            {k: round(v, 4) if isinstance(v, float) else v for k, v in ka.items()}
            for ka in kelly_analysis
        ],
        "merge_as_stoploss": merge_stats,
        "holding_period": holding_stats,
        "monthly_size_evolution": {m: {k: round(v, 2) if isinstance(v, float) else v for k, v in stats.items()} for m, stats in month_size_stats.items()},
        "key_findings": key_findings,
        "nbabot_recommendations": nbabot_recommendations,
    }

    with open(OUT_DIR / "agent4-summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print(f"  Wrote {OUT_DIR / 'agent4-summary.json'}")

    # Markdown report
    md = generate_markdown_report(
        size_stats, bucket_counts, bucket_total_cost,
        sport_size_stats, mtype_size_stats,
        corr_size_price, fav_vs_dog, month_size_stats,
        size_vs_outcome, roi_by_bucket,
        kelly_analysis, kelly_summary,
        daily_cumulative, weekly_pnl, sorted_weeks,
        risk_metrics, concentration, top10_list,
        merge_stats, merge_analysis_records,
        holding_stats, holding_periods,
        key_findings, nbabot_recommendations,
        running, conditions,
    )

    with open(OUT_DIR / "agent4-sizing-and-risk.md", "w") as f:
        f.write(md)
    print(f"  Wrote {OUT_DIR / 'agent4-sizing-and-risk.md'}")

    print(f"\nDone! Final cumulative P&L: {fmt_usd(running)}")


def generate_markdown_report(
    size_stats, bucket_counts, bucket_total_cost,
    sport_size_stats, mtype_size_stats,
    corr_size_price, fav_vs_dog, month_size_stats,
    size_vs_outcome, roi_by_bucket,
    kelly_analysis, kelly_summary,
    daily_cumulative, weekly_pnl, sorted_weeks,
    risk_metrics, concentration, top10_list,
    merge_stats, merge_analysis_records,
    holding_stats, holding_periods,
    key_findings, nbabot_recommendations,
    final_pnl, conditions,
):
    lines = []
    lines.append("# Agent 4: Sizing & Risk Analysis -- lhtsports")
    lines.append("")
    lines.append(f"**Total conditions analyzed**: {len(conditions):,}")
    lines.append(f"**Final cumulative P&L**: {fmt_usd(final_pnl)}")
    lines.append("")

    # ------------------------------------------------------------------
    lines.append("## 1. Position Size Distribution")
    lines.append("")
    lines.append("### Overall Statistics")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    for k in ["mean", "median", "std", "min", "max", "p25", "p75", "p95", "p99"]:
        lines.append(f"| {k.capitalize()} | {fmt_usd(size_stats[k])} |")
    lines.append("")

    lines.append("### Size Bucket Distribution")
    lines.append("")
    lines.append("| Bucket | Count | % of Total | Total Cost | Avg Cost |")
    lines.append("|--------|-------|-----------|------------|----------|")
    total_conds = sum(bucket_counts.values())
    for b in BUCKET_ORDER:
        cnt = bucket_counts.get(b, 0)
        tc = bucket_total_cost.get(b, 0)
        avg = tc / cnt if cnt > 0 else 0
        pct = cnt / total_conds * 100 if total_conds > 0 else 0
        lines.append(f"| {b} | {cnt:,} | {pct:.1f}% | {fmt_usd(tc)} | {fmt_usd(avg)} |")
    lines.append("")

    lines.append("### By Sport")
    lines.append("")
    lines.append("| Sport | Count | Mean Size | Median Size | Total Deployed |")
    lines.append("|-------|-------|-----------|-------------|----------------|")
    for sport, stats in sport_size_stats.items():
        lines.append(f"| {sport} | {stats['count']:,} | {fmt_usd(stats['mean'])} | {fmt_usd(stats['median'])} | {fmt_usd(stats['total'])} |")
    lines.append("")

    lines.append("### By Market Type")
    lines.append("")
    lines.append("| Market Type | Count | Mean Size | Median Size | Total Deployed |")
    lines.append("|-------------|-------|-----------|-------------|----------------|")
    for mt, stats in mtype_size_stats.items():
        lines.append(f"| {mt} | {stats['count']:,} | {fmt_usd(stats['mean'])} | {fmt_usd(stats['median'])} | {fmt_usd(stats['total'])} |")
    lines.append("")

    # ------------------------------------------------------------------
    lines.append("## 2. Size Determinants")
    lines.append("")
    lines.append(f"**Correlation (net_cost vs avg_buy_price)**: {corr_size_price:.4f}")
    lines.append("")
    lines.append("### Favorites vs Underdogs")
    lines.append("")
    lines.append("| Group | Count | Avg Size |")
    lines.append("|-------|-------|----------|")
    lines.append(f"| Favorites (price >= 0.50) | {fav_vs_dog['favorites_count']:,} | {fmt_usd(fav_vs_dog['favorites_avg_size'])} |")
    lines.append(f"| Underdogs (price < 0.50) | {fav_vs_dog['underdogs_count']:,} | {fmt_usd(fav_vs_dog['underdogs_avg_size'])} |")
    lines.append("")

    lines.append("### Monthly Evolution of Position Sizing")
    lines.append("")
    lines.append("| Month | Count | Mean Size | Median Size | Total Deployed |")
    lines.append("|-------|-------|-----------|-------------|----------------|")
    for month in sorted(month_size_stats.keys()):
        stats = month_size_stats[month]
        lines.append(f"| {month} | {stats['count']:,} | {fmt_usd(stats['mean'])} | {fmt_usd(stats['median'])} | {fmt_usd(stats['total'])} |")
    lines.append("")

    # ------------------------------------------------------------------
    lines.append("## 3. Size vs Outcome")
    lines.append("")
    lines.append("### Average Size by Outcome")
    lines.append("")
    lines.append("| Status | Count | Avg Size | Median Size | Total Cost |")
    lines.append("|--------|-------|----------|-------------|------------|")
    for status in sorted(size_vs_outcome.keys()):
        info = size_vs_outcome[status]
        lines.append(f"| {status} | {info.get('count', 0):,} | {fmt_usd(info.get('avg_size', 0))} | {fmt_usd(info.get('median_size', 0))} | {fmt_usd(info.get('total_cost', 0))} |")
    lines.append("")

    lines.append("### ROI & Win Rate by Size Bucket")
    lines.append("")
    lines.append("| Bucket | Count | Win Rate | Total P&L | Total Cost | ROI % |")
    lines.append("|--------|-------|----------|-----------|------------|-------|")
    for b in BUCKET_ORDER:
        info = roi_by_bucket.get(b, {})
        lines.append(
            f"| {b} | {info.get('count', 0):,} | {fmt_pct(info.get('win_rate', 0))} "
            f"| {fmt_usd(info.get('total_pnl', 0))} | {fmt_usd(info.get('total_cost', 0))} "
            f"| {fmt_pct(info.get('roi_pct', 0))} |"
        )
    lines.append("")

    # ------------------------------------------------------------------
    lines.append("## 4. Kelly Efficiency")
    lines.append("")
    lines.append(f"- **Avg theoretical Kelly fraction**: {kelly_summary['avg_kelly_fraction']:.4f}")
    lines.append(f"- **Avg actual bet fraction (of monthly capital)**: {kelly_summary['avg_actual_fraction']:.6f}")
    ratio_str = f"{kelly_summary['actual_vs_kelly_ratio']:.4f}" if kelly_summary['actual_vs_kelly_ratio'] is not None else "N/A"
    lines.append(f"- **Actual/Kelly ratio**: {ratio_str}")
    lines.append(f"- **Assessment**: {kelly_summary['over_or_under_betting']}")
    lines.append("")

    lines.append("### Kelly by Price Band")
    lines.append("")
    lines.append("| Price Band | Count | Win Rate | Kelly f* | Avg Size | Total P&L |")
    lines.append("|------------|-------|----------|----------|----------|-----------|")
    for ka in kelly_analysis:
        lines.append(
            f"| {ka['band']} | {ka['count']:,} | {fmt_pct(ka['win_rate'] * 100)} "
            f"| {ka['kelly_fraction']:.4f} | {fmt_usd(ka['avg_position_size'])} "
            f"| {fmt_usd(ka['total_pnl'])} |"
        )
    lines.append("")

    # ------------------------------------------------------------------
    lines.append("## 5. Equity Curve")
    lines.append("")
    lines.append(f"**Final cumulative P&L**: {fmt_usd(final_pnl)}")
    lines.append(f"**Trading period**: {daily_cumulative[0]['date']} to {daily_cumulative[-1]['date']}")
    lines.append("")

    # Monthly P&L summary
    monthly_pnl_summary = defaultdict(float)
    for entry in daily_cumulative:
        month = entry["date"][:7]
        monthly_pnl_summary[month] += entry["daily_pnl"]

    lines.append("### Monthly P&L Summary")
    lines.append("")
    lines.append("| Month | P&L | Cumulative |")
    lines.append("|-------|-----|------------|")
    cum_monthly = 0.0
    for month in sorted(monthly_pnl_summary.keys()):
        pnl = monthly_pnl_summary[month]
        cum_monthly += pnl
        lines.append(f"| {month} | {fmt_usd(pnl)} | {fmt_usd(cum_monthly)} |")
    lines.append("")

    # ------------------------------------------------------------------
    lines.append("## 6. Risk Metrics")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Daily Sharpe (annualized) | {risk_metrics['daily_sharpe']:.4f} |")
    lines.append(f"| Weekly Sharpe (annualized) | {risk_metrics['weekly_sharpe']:.4f} |")
    lines.append(f"| Max Drawdown (USD) | {fmt_usd(risk_metrics['max_drawdown_usd'])} |")
    lines.append(f"| Max Drawdown (%) | {fmt_pct(risk_metrics['max_drawdown_pct'])} |")
    lines.append(f"| Max Drawdown Duration | {risk_metrics['max_drawdown_days']} days |")
    lines.append(f"| Max Drawdown Period | {risk_metrics['max_drawdown_period']} |")
    lines.append(f"| Max Daily Loss | {fmt_usd(risk_metrics['max_daily_loss'])} ({risk_metrics['max_daily_loss_date']}) |")
    lines.append(f"| Max Daily Gain | {fmt_usd(risk_metrics['max_daily_gain'])} ({risk_metrics['max_daily_gain_date']}) |")
    lines.append(f"| Daily Win Rate | {fmt_pct(risk_metrics['daily_win_rate'])} ({risk_metrics['total_trading_days']} days) |")
    lines.append(f"| Weekly Win Rate | {fmt_pct(risk_metrics['weekly_win_rate'])} ({risk_metrics['total_trading_weeks']} weeks) |")
    lines.append(f"| Daily P&L Mean | {fmt_usd(risk_metrics['daily_pnl_mean'])} |")
    lines.append(f"| Daily P&L Std Dev | {fmt_usd(risk_metrics['daily_pnl_std'])} |")
    lines.append(f"| Weekly P&L Mean | {fmt_usd(risk_metrics['weekly_pnl_mean'])} |")
    lines.append(f"| Weekly P&L Std Dev | {fmt_usd(risk_metrics['weekly_pnl_std'])} |")
    lines.append("")

    # ------------------------------------------------------------------
    lines.append("## 7. Concentration Risk")
    lines.append("")
    lines.append(f"- **Max daily exposure**: {fmt_usd(concentration['max_daily_exposure'])} ({concentration['max_daily_exposure_date']})")
    lines.append(f"- **Max single-game exposure**: {fmt_usd(concentration['max_single_game_exposure'])} ({concentration['max_single_game_key']})")
    lines.append(f"- **Max single-game P&L**: {fmt_usd(concentration['max_single_game_pnl'])}")
    lines.append("")

    lines.append("### Top 10 Largest Positions")
    lines.append("")
    lines.append("| # | Slug | Outcome | Net Cost | P&L | ROI% | Status | Avg Price |")
    lines.append("|---|------|---------|----------|-----|------|--------|-----------|")
    for i, pos in enumerate(top10_list, 1):
        lines.append(
            f"| {i} | {pos['slug']} | {pos['outcome']} | {fmt_usd(pos['net_cost'])} "
            f"| {fmt_usd(pos['pnl'])} | {fmt_pct(pos['roi_pct'])} | {pos['status']} "
            f"| {pos['avg_buy_price']:.4f} |"
        )
    lines.append("")

    # ------------------------------------------------------------------
    lines.append("## 8. MERGE as Stop-Loss")
    lines.append("")
    lines.append(f"- **Total MERGED conditions**: {merge_stats['total_merged_conditions']}")
    lines.append(f"- **Matched with merge transaction data**: {merge_stats.get('matched_with_merge_data', 0)}")
    if merge_stats.get("avg_time_to_merge_hours") is not None:
        lines.append(f"- **Avg time from last trade to merge**: {merge_stats['avg_time_to_merge_hours']:.1f} hours ({merge_stats['avg_time_to_merge_hours']/24:.1f} days)")
        lines.append(f"- **Median time to merge**: {merge_stats['median_time_to_merge_hours']:.1f} hours ({merge_stats['median_time_to_merge_hours']/24:.1f} days)")
        lines.append(f"- **Avg merge ratio (payout/cost)**: {merge_stats['avg_merge_ratio']:.4f}")
        lines.append(f"- **Median merge ratio**: {merge_stats['median_merge_ratio']:.4f}")
        lines.append(f"- **% already losing at merge**: {merge_stats['pct_already_losing']:.1f}%")
        lines.append(f"- **Avg P&L on merged positions**: {fmt_usd(merge_stats['avg_merge_pnl'])}")
    lines.append("")

    lines.append("### Interpretation")
    lines.append("")
    if merge_stats.get("pct_already_losing") is not None:
        if merge_stats["pct_already_losing"] > 50:
            lines.append("MERGE is primarily used as a **stop-loss mechanism**. The majority of merged positions were already underwater at the time of merge, confirming that lhtsports exits losing positions rather than holding to expiry.")
        else:
            lines.append("MERGE is used for **profit-taking or hedging** rather than pure stop-loss, as many merged positions were not losing at exit time.")
    lines.append("")

    # ------------------------------------------------------------------
    lines.append("## 9. Holding Period Analysis")
    lines.append("")
    lines.append(f"- **Conditions with settlement data**: {holding_stats['conditions_with_settlement']:,}")
    lines.append(f"- **Mean holding period**: {holding_stats.get('mean_days', 0):.2f} days")
    lines.append(f"- **Median holding period**: {holding_stats.get('median_days', 0):.2f} days")
    if "min_days" in holding_stats:
        lines.append(f"- **Range**: {holding_stats['min_days']:.2f} to {holding_stats['max_days']:.2f} days")
        lines.append(f"- **P25/P75**: {holding_stats['p25_days']:.2f} / {holding_stats['p75_days']:.2f} days")
    lines.append(f"- **Correlation (holding period vs P&L)**: {holding_stats.get('corr_hold_pnl', 0):.4f}")
    lines.append("")

    if holding_stats.get("by_status"):
        lines.append("### By Outcome")
        lines.append("")
        lines.append("| Status | Count | Mean Days | Median Days |")
        lines.append("|--------|-------|-----------|-------------|")
        for status, info in sorted(holding_stats["by_status"].items()):
            lines.append(f"| {status} | {info['count']:,} | {info['mean_days']:.2f} | {info['median_days']:.2f} |")
        lines.append("")

    # ------------------------------------------------------------------
    lines.append("## 10. Key Findings")
    lines.append("")
    for i, finding in enumerate(key_findings, 1):
        lines.append(f"{i}. {finding}")
    lines.append("")

    # ------------------------------------------------------------------
    lines.append("## 11. Recommendations for nbabot")
    lines.append("")
    for i, rec in enumerate(nbabot_recommendations, 1):
        lines.append(f"{i}. {rec}")
    lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    main()
