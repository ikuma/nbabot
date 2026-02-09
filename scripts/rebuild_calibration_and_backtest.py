"""校正テーブル再構築 + 直近1000試合バックテスト.

lhtsports の全 NBA ML データ (2024-12 〜 2026-02) を使って:
1. 5-cent band の校正テーブルを再構築
2. 直近約1000試合で「このモデルの予測 vs lhtsports の実際の賭け vs 結果」を比較
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data/reports/lhtsports-analysis"


def load_data():
    with open(DATA_DIR / "lhtsports_all_trades.json") as f:
        trades = json.load(f)
    with open(DATA_DIR / "lhtsports_redeem.json") as f:
        redeems = json.load(f)
    with open(DATA_DIR / "lhtsports_merge.json") as f:
        merges = json.load(f)
    return trades, redeems, merges


def is_nba_ml(slug: str) -> bool:
    """NBA moneyline のみ (spread, total 除外)."""
    if not slug.startswith("nba-"):
        return False
    if "spread" in slug or "total-" in slug or "-over-" in slug or "-under-" in slug:
        return False
    return True


def build_condition_pnl(trades, redeems, merges):
    """conditionId 単位で P&L を計算 (NBA ML のみ)."""
    conditions = {}

    for t in trades:
        cid = t.get("conditionId", "")
        slug = t.get("slug", "")
        if not cid or not is_nba_ml(slug):
            continue

        if cid not in conditions:
            conditions[cid] = {
                "conditionId": cid,
                "slug": slug,
                "eventSlug": t.get("eventSlug", ""),
                "title": t.get("title", ""),
                "outcome": "",
                "buy_cost": 0.0,
                "buy_shares": 0.0,
                "sell_proceeds": 0.0,
                "trade_count": 0,
                "first_trade_ts": float("inf"),
                "last_trade_ts": 0,
                "redeem_usdc": 0.0,
                "merge_usdc": 0.0,
                "prices": [],
                "buy_prices": [],
            }

        c = conditions[cid]
        c["trade_count"] += 1
        c["first_trade_ts"] = min(c["first_trade_ts"], t["timestamp"])
        c["last_trade_ts"] = max(c["last_trade_ts"], t["timestamp"])
        price = float(t.get("price", 0))
        c["prices"].append(price)

        if t.get("side") == "BUY":
            c["buy_cost"] += float(t.get("usdcSize", 0))
            c["buy_shares"] += float(t.get("size", 0))
            c["buy_prices"].append(price)
            if not c["outcome"]:
                c["outcome"] = t.get("outcome", "")
        elif t.get("side") == "SELL":
            c["sell_proceeds"] += float(t.get("usdcSize", 0))

    # REDEEM
    for r in redeems:
        cid = r.get("conditionId", "")
        if cid in conditions:
            conditions[cid]["redeem_usdc"] += float(r.get("usdcSize", 0))

    # MERGE
    for m in merges:
        cid = m.get("conditionId", "")
        if cid in conditions:
            conditions[cid]["merge_usdc"] += float(m.get("usdcSize", 0))

    # P&L 算出
    for cid, c in conditions.items():
        c["net_cost"] = c["buy_cost"] - c["sell_proceeds"]
        c["total_payout"] = c["redeem_usdc"] + c["merge_usdc"]
        c["pnl"] = c["total_payout"] - c["net_cost"]
        c["avg_buy_price"] = (
            c["buy_cost"] / c["buy_shares"] if c["buy_shares"] > 0 else 0.0
        )
        # 最初の BUY 価格 (Polymarket 表示価格に近い)
        c["first_buy_price"] = c["buy_prices"][0] if c["buy_prices"] else 0.0

        if c["redeem_usdc"] > 0:
            c["status"] = "WIN"
        elif c["merge_usdc"] > 0:
            c["status"] = "MERGED"
        elif c["buy_cost"] > 0:
            c["status"] = "LOSS_OR_OPEN"
        else:
            c["status"] = "UNKNOWN"

        c["date"] = datetime.fromtimestamp(
            int(c["first_trade_ts"]), tz=timezone.utc
        ).strftime("%Y-%m-%d")

    return conditions


def build_calibration_table(conditions: dict) -> list[dict]:
    """5-cent band の校正テーブルを構築."""
    bands = []
    for lo_int in range(5, 100, 5):  # 0.05 〜 0.95
        lo = lo_int / 100
        hi = (lo_int + 5) / 100

        # この band に入る condition (avg_buy_price ベース)
        band_conds = [
            c for c in conditions.values()
            if lo <= c["avg_buy_price"] < hi
            and c["status"] in ("WIN", "LOSS_OR_OPEN")  # MERGED 除外
        ]

        if not band_conds:
            bands.append({
                "price_lo": lo,
                "price_hi": hi,
                "sample_size": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "total_cost": 0.0,
                "total_pnl": 0.0,
                "roi_pct": 0.0,
            })
            continue

        wins = sum(1 for c in band_conds if c["status"] == "WIN")
        losses = sum(1 for c in band_conds if c["status"] == "LOSS_OR_OPEN")
        total_cost = sum(c["net_cost"] for c in band_conds)
        total_pnl = sum(c["pnl"] for c in band_conds)

        bands.append({
            "price_lo": lo,
            "price_hi": hi,
            "sample_size": len(band_conds),
            "wins": wins,
            "losses": losses,
            "win_rate": wins / len(band_conds) if band_conds else 0.0,
            "total_cost": total_cost,
            "total_pnl": total_pnl,
            "roi_pct": total_pnl / total_cost * 100 if total_cost > 0 else 0.0,
        })

    return bands


def aggregate_games(conditions: dict) -> list[dict]:
    """eventSlug 単位で試合を集約."""
    games: dict[str, dict] = {}

    for cid, c in conditions.items():
        # slug から game_key を抽出 (eventSlug or slug)
        game_key = c["eventSlug"] or c["slug"]
        if not game_key:
            continue

        if game_key not in games:
            games[game_key] = {
                "game_key": game_key,
                "slug": c["slug"],
                "title": c["title"],
                "date": c["date"],
                "first_ts": c["first_trade_ts"],
                "conditions": [],
            }

        games[game_key]["conditions"].append(c)
        # 最も早い日付を使う
        if c["first_trade_ts"] < games[game_key]["first_ts"]:
            games[game_key]["first_ts"] = c["first_trade_ts"]
            games[game_key]["date"] = c["date"]

    result = []
    for gk, g in games.items():
        conds = g["conditions"]

        # lhtsports が実際にベットした condition (BUY があるもの)
        bet_conds = [c for c in conds if c["buy_cost"] > 0]

        # 決済済み condition のステータス
        settled = [c for c in conds if c["status"] in ("WIN", "LOSS_OR_OPEN")]
        has_win = any(c["status"] == "WIN" for c in conds)
        has_loss = any(c["status"] == "LOSS_OR_OPEN" for c in conds)
        has_merge = any(c["status"] == "MERGED" for c in conds)

        # 試合結果
        if has_win and not has_loss:
            game_result = "WIN"
        elif has_loss and not has_win:
            game_result = "LOSS"
        elif has_win and has_loss:
            # 複数 condition で WIN と LOSS が混在
            game_result = "MIXED"
        elif has_merge:
            game_result = "MERGED"
        else:
            game_result = "OPEN"

        total_pnl = sum(c["pnl"] for c in conds)
        total_cost = sum(c["net_cost"] for c in conds)

        result.append({
            "game_key": gk,
            "slug": g["slug"],
            "title": g["title"],
            "date": g["date"],
            "first_ts": g["first_ts"],
            "n_conditions": len(conds),
            "game_result": game_result,
            "total_cost": total_cost,
            "total_pnl": total_pnl,
            "bet_conditions": [
                {
                    "outcome": c["outcome"],
                    "avg_buy_price": round(c["avg_buy_price"], 4),
                    "first_buy_price": round(c["first_buy_price"], 4),
                    "net_cost": round(c["net_cost"], 2),
                    "pnl": round(c["pnl"], 2),
                    "status": c["status"],
                }
                for c in bet_conds
            ],
        })

    result.sort(key=lambda x: x["first_ts"])
    return result


def simulate_model(game: dict, calibration_table: list[dict]) -> dict | None:
    """モデルのシミュレーション: 各 condition の avg_buy_price で band を検索し、
    EV/$ が最も高いものを選択."""

    best = None
    best_ev = -999.0

    for bc in game["bet_conditions"]:
        price = bc["avg_buy_price"]
        if price <= 0 or price >= 1:
            continue

        # band lookup
        band = None
        for b in calibration_table:
            if b["price_lo"] <= price < b["price_hi"]:
                band = b
                break

        if band is None or band["sample_size"] == 0:
            continue

        wr = band["win_rate"]
        ev_per_dollar = wr / price - 1

        if ev_per_dollar > 0 and ev_per_dollar > best_ev:
            best_ev = ev_per_dollar
            best = {
                "outcome": bc["outcome"],
                "price": price,
                "band": f"{band['price_lo']:.2f}-{band['price_hi']:.2f}",
                "expected_wr": round(wr, 4),
                "ev_per_dollar": round(ev_per_dollar, 4),
                "in_sweet_spot": 0.25 <= price <= 0.55,
                "actual_status": bc["status"],
                "actual_pnl": bc["pnl"],
            }

    return best


def main():
    print("=" * 70)
    print("校正テーブル再構築 + バックテスト")
    print("=" * 70)

    print("\n[1] データ読み込み...")
    trades, redeems, merges = load_data()
    print(f"   TRADE: {len(trades):,} | REDEEM: {len(redeems):,} | MERGE: {len(merges):,}")

    print("\n[2] NBA ML condition P&L 計算...")
    conditions = build_condition_pnl(trades, redeems, merges)
    print(f"   {len(conditions):,} NBA ML conditions")

    # 日付範囲
    dates = [c["date"] for c in conditions.values()]
    print(f"   期間: {min(dates)} 〜 {max(dates)}")

    print("\n[3] 校正テーブル構築 (全データ: 2024-12 〜 2026-02)...")
    cal_table = build_calibration_table(conditions)

    print("\n   === 校正テーブル (5-cent bands) ===")
    print(f"   {'Band':>12} | {'N':>5} | {'Win':>5} | {'Loss':>5} | {'WinRate':>7} | {'ROI%':>7} | {'P&L':>10} | {'Conf':>6}")
    print(f"   {'-'*12}-+-{'-'*5}-+-{'-'*5}-+-{'-'*5}-+-{'-'*7}-+-{'-'*7}-+-{'-'*10}-+-{'-'*6}")

    for b in cal_table:
        if b["sample_size"] == 0:
            conf = "-"
        elif b["sample_size"] >= 100:
            conf = "high"
        elif b["sample_size"] >= 40:
            conf = "medium"
        else:
            conf = "low"

        label = f"{b['price_lo']:.2f}-{b['price_hi']:.2f}"
        wr_str = f"{b['win_rate']*100:.1f}%" if b["sample_size"] > 0 else "N/A"
        roi_str = f"{b['roi_pct']:.1f}%" if b["sample_size"] > 0 else "N/A"
        pnl_str = f"${b['total_pnl']:,.0f}" if b["sample_size"] > 0 else "-"

        print(
            f"   {label:>12} | {b['sample_size']:>5} | {b['wins']:>5} | "
            f"{b['losses']:>5} | {wr_str:>7} | {roi_str:>7} | {pnl_str:>10} | {conf:>6}"
        )

    # 有効な band (sample_size > 0) のみ
    valid_bands = [b for b in cal_table if b["sample_size"] > 0]
    total_conditions = sum(b["sample_size"] for b in valid_bands)
    total_wins = sum(b["wins"] for b in valid_bands)
    total_pnl = sum(b["total_pnl"] for b in valid_bands)
    total_cost = sum(b["total_cost"] for b in valid_bands)
    print(f"\n   合計: {total_conditions} conditions, 勝率 {total_wins/total_conditions*100:.1f}%, "
          f"P&L ${total_pnl:,.0f}, ROI {total_pnl/total_cost*100:.1f}%")

    # Python コード出力用
    print("\n   === calibration.py 用コード ===")
    print("   NBA_ML_CALIBRATION: list[CalibrationBand] = [")
    for b in cal_table:
        if b["sample_size"] == 0:
            continue
        if b["sample_size"] >= 100:
            conf = "high"
        elif b["sample_size"] >= 40:
            conf = "medium"
        else:
            conf = "low"

        sweet = "# sweet spot" if 0.25 <= b["price_lo"] < 0.55 else ""
        print(
            f'       CalibrationBand({b["price_lo"]:.2f}, {b["price_hi"]:.2f}, '
            f'{b["win_rate"]:.3f}, {b["roi_pct"]:.1f}, {b["sample_size"]}, "{conf}"),  {sweet}'
        )
    print("   ]")

    # ======================================================
    # バックテスト: 直近約1000試合
    # ======================================================
    print("\n\n[4] 試合集約...")
    all_games = aggregate_games(conditions)
    print(f"   {len(all_games)} 試合")

    # 決済済み試合のみ
    settled_games = [
        g for g in all_games
        if g["game_result"] in ("WIN", "LOSS", "MIXED")
    ]
    print(f"   うち決済済み: {len(settled_games)} 試合")

    # 直近1000試合
    recent_games = settled_games[-1000:]
    print(f"   バックテスト対象 (直近): {len(recent_games)} 試合")
    print(f"   期間: {recent_games[0]['date']} 〜 {recent_games[-1]['date']}")

    # バックテスト実行
    print(f"\n[5] バックテスト (校正テーブル: 0.25-0.95)...")

    # 0.25 以下を除外した table
    bt_table = [b for b in cal_table if b["price_lo"] >= 0.25 and b["sample_size"] > 0]

    results = {
        "signal_win": 0,
        "signal_loss": 0,
        "no_signal": 0,
        "signal_total_pnl": 0.0,
        "lhtsports_total_pnl": 0.0,
        "signal_games": [],
    }

    for game in recent_games:
        model = simulate_model(game, bt_table)
        lht_pnl = game["total_pnl"]

        results["lhtsports_total_pnl"] += lht_pnl

        if model is None:
            results["no_signal"] += 1
            continue

        if model["actual_status"] == "WIN":
            results["signal_win"] += 1
        else:
            results["signal_loss"] += 1

        results["signal_total_pnl"] += model["actual_pnl"]
        results["signal_games"].append({
            "game": game["game_key"],
            "date": game["date"],
            "outcome": model["outcome"],
            "price": model["price"],
            "band": model["band"],
            "expected_wr": model["expected_wr"],
            "ev_per_dollar": model["ev_per_dollar"],
            "sweet_spot": model["in_sweet_spot"],
            "actual": model["actual_status"],
            "pnl": model["actual_pnl"],
            "lhtsports_pnl": lht_pnl,
        })

    signal_total = results["signal_win"] + results["signal_loss"]

    print("\n   === バックテスト結果 ===")
    print(f"   対象試合数: {len(recent_games)}")
    print(f"   シグナル発生: {signal_total} 試合 ({signal_total/len(recent_games)*100:.1f}%)")
    print(f"   シグナルなし: {results['no_signal']} 試合")
    print(f"   勝ち: {results['signal_win']}  負け: {results['signal_loss']}")
    if signal_total > 0:
        wr = results["signal_win"] / signal_total * 100
        print(f"   勝率: {wr:.1f}%")
    print(f"\n   モデル P&L (シグナル発生分のみ): ${results['signal_total_pnl']:,.2f}")
    print(f"   lhtsports 実績 P&L (全試合):     ${results['lhtsports_total_pnl']:,.2f}")

    # Sweet spot vs outside の比較
    sweet_games = [g for g in results["signal_games"] if g["sweet_spot"]]
    outside_games = [g for g in results["signal_games"] if not g["sweet_spot"]]

    if sweet_games:
        sw_wins = sum(1 for g in sweet_games if g["actual"] == "WIN")
        sw_pnl = sum(g["pnl"] for g in sweet_games)
        print(f"\n   Sweet Spot (0.25-0.55):")
        print(f"     試合数: {len(sweet_games)}, 勝率: {sw_wins/len(sweet_games)*100:.1f}%, P&L: ${sw_pnl:,.2f}")

    if outside_games:
        out_wins = sum(1 for g in outside_games if g["actual"] == "WIN")
        out_pnl = sum(g["pnl"] for g in outside_games)
        print(f"   Outside (0.55-0.95):")
        print(f"     試合数: {len(outside_games)}, 勝率: {out_wins/len(outside_games)*100:.1f}%, P&L: ${out_pnl:,.2f}")

    # 月別サマリー
    print("\n   === 月別バックテスト結果 ===")
    monthly: dict[str, dict] = defaultdict(
        lambda: {"wins": 0, "losses": 0, "no_signal": 0, "pnl": 0.0, "lht_pnl": 0.0}
    )

    for game in recent_games:
        month = game["date"][:7]
        model = simulate_model(game, bt_table)
        monthly[month]["lht_pnl"] += game["total_pnl"]
        if model is None:
            monthly[month]["no_signal"] += 1
        elif model["actual_status"] == "WIN":
            monthly[month]["wins"] += 1
            monthly[month]["pnl"] += model["actual_pnl"]
        else:
            monthly[month]["losses"] += 1
            monthly[month]["pnl"] += model["actual_pnl"]

    print(f"   {'月':>8} | {'Signal':>7} | {'W':>4} | {'L':>4} | {'NoSig':>5} | {'勝率':>6} | {'Model P&L':>12} | {'LHT P&L':>12}")
    print(f"   {'-'*8}-+-{'-'*7}-+-{'-'*4}-+-{'-'*4}-+-{'-'*5}-+-{'-'*6}-+-{'-'*12}-+-{'-'*12}")
    for month in sorted(monthly):
        m = monthly[month]
        total = m["wins"] + m["losses"]
        wr = m["wins"] / total * 100 if total > 0 else 0
        print(
            f"   {month:>8} | {total:>7} | {m['wins']:>4} | {m['losses']:>4} | "
            f"{m['no_signal']:>5} | {wr:>5.1f}% | ${m['pnl']:>10,.2f} | ${m['lht_pnl']:>10,.2f}"
        )

    # Band 別の勝率
    print("\n   === Band 別バックテスト結果 ===")
    band_results: dict[str, dict] = defaultdict(
        lambda: {"wins": 0, "losses": 0, "pnl": 0.0}
    )
    for g in results["signal_games"]:
        br = band_results[g["band"]]
        if g["actual"] == "WIN":
            br["wins"] += 1
        else:
            br["losses"] += 1
        br["pnl"] += g["pnl"]

    print(f"   {'Band':>12} | {'N':>5} | {'W':>4} | {'L':>4} | {'勝率':>6} | {'P&L':>12}")
    print(f"   {'-'*12}-+-{'-'*5}-+-{'-'*4}-+-{'-'*4}-+-{'-'*6}-+-{'-'*12}")
    for band_key in sorted(band_results):
        br = band_results[band_key]
        n = br["wins"] + br["losses"]
        wr = br["wins"] / n * 100 if n > 0 else 0
        print(
            f"   {band_key:>12} | {n:>5} | {br['wins']:>4} | {br['losses']:>4} | "
            f"{wr:>5.1f}% | ${br['pnl']:>10,.2f}"
        )

    # 直近 20 試合の詳細
    print("\n   === 直近 20 試合の詳細 ===")
    print(f"   {'日付':>10} | {'試合':>35} | {'モデル':>8} | {'価格':>5} | {'Band':>12} | {'期待WR':>6} | {'EV/$':>6} | {'結果':>5} | {'P&L':>10}")
    print(f"   {'-'*10}-+-{'-'*35}-+-{'-'*8}-+-{'-'*5}-+-{'-'*12}-+-{'-'*6}-+-{'-'*6}-+-{'-'*5}-+-{'-'*10}")

    for game in recent_games[-20:]:
        model = simulate_model(game, bt_table)
        lht_bet = game["bet_conditions"][0] if game["bet_conditions"] else None

        if model:
            title = game["title"][:35] if game.get("title") else game["game_key"][:35]
            print(
                f"   {game['date']:>10} | {title:>35} | "
                f"{model['outcome'][:8]:>8} | {model['price']:.3f} | "
                f"{model['band']:>12} | {model['expected_wr']:.3f}  | {model['ev_per_dollar']:.3f}  | "
                f"{model['actual_status']:>5} | ${model['actual_pnl']:>8,.2f}"
            )
        else:
            title = game["title"][:35] if game.get("title") else game["game_key"][:35]
            lht_outcome = lht_bet["outcome"][:8] if lht_bet else "?"
            lht_price = lht_bet["avg_buy_price"] if lht_bet else 0
            print(
                f"   {game['date']:>10} | {title:>35} | "
                f"NO_SIGNAL | {lht_price:.3f} | {'N/A':>12} | {'N/A':>6} | {'N/A':>6} | "
                f"{game['game_result']:>5} | ${game['total_pnl']:>8,.2f}"
            )

    print("\n完了。")


if __name__ == "__main__":
    main()
