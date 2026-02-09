"""NO_SIGNAL 138試合と赤字バンドの原因分析."""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data/reports/lhtsports-analysis"


def is_nba_ml(slug: str) -> bool:
    if not slug.startswith("nba-"):
        return False
    if "spread" in slug or "total-" in slug or "-over-" in slug or "-under-" in slug:
        return False
    return True


def load_data():
    with open(DATA_DIR / "lhtsports_all_trades.json") as f:
        trades = json.load(f)
    with open(DATA_DIR / "lhtsports_redeem.json") as f:
        redeems = json.load(f)
    with open(DATA_DIR / "lhtsports_merge.json") as f:
        merges = json.load(f)
    return trades, redeems, merges


def build_condition_pnl(trades, redeems, merges):
    conditions = {}
    for t in trades:
        cid = t.get("conditionId", "")
        slug = t.get("slug", "")
        if not cid or not is_nba_ml(slug):
            continue
        if cid not in conditions:
            conditions[cid] = {
                "conditionId": cid, "slug": slug,
                "eventSlug": t.get("eventSlug", ""),
                "title": t.get("title", ""),
                "outcome": "", "buy_cost": 0.0, "buy_shares": 0.0,
                "sell_proceeds": 0.0, "sell_shares": 0.0,
                "trade_count": 0, "buy_count": 0, "sell_count": 0,
                "first_trade_ts": float("inf"), "last_trade_ts": 0,
                "redeem_usdc": 0.0, "merge_usdc": 0.0, "merge_shares": 0.0,
                "buy_prices": [], "all_buy_sizes": [],
            }
        c = conditions[cid]
        c["trade_count"] += 1
        c["first_trade_ts"] = min(c["first_trade_ts"], t["timestamp"])
        c["last_trade_ts"] = max(c["last_trade_ts"], t["timestamp"])
        price = float(t.get("price", 0))
        size = float(t.get("usdcSize", 0))
        shares = float(t.get("size", 0))
        if t.get("side") == "BUY":
            c["buy_cost"] += size
            c["buy_shares"] += shares
            c["buy_count"] += 1
            c["buy_prices"].append(price)
            c["all_buy_sizes"].append(size)
            if not c["outcome"]:
                c["outcome"] = t.get("outcome", "")
        elif t.get("side") == "SELL":
            c["sell_proceeds"] += size
            c["sell_shares"] += shares
            c["sell_count"] += 1

    for r in redeems:
        cid = r.get("conditionId", "")
        if cid in conditions:
            conditions[cid]["redeem_usdc"] += float(r.get("usdcSize", 0))
    for m in merges:
        cid = m.get("conditionId", "")
        if cid in conditions:
            conditions[cid]["merge_usdc"] += float(m.get("usdcSize", 0))
            conditions[cid]["merge_shares"] += float(m.get("size", 0))

    for cid, c in conditions.items():
        c["net_cost"] = c["buy_cost"] - c["sell_proceeds"]
        c["total_payout"] = c["redeem_usdc"] + c["merge_usdc"]
        c["pnl"] = c["total_payout"] - c["net_cost"]
        c["avg_buy_price"] = c["buy_cost"] / c["buy_shares"] if c["buy_shares"] > 0 else 0.0
        c["first_buy_price"] = c["buy_prices"][0] if c["buy_prices"] else 0.0
        # DCA 判定
        c["is_dca"] = c["buy_count"] > 1
        c["dca_count"] = c["buy_count"]
        # 価格の幅 (DCA の spread)
        if len(c["buy_prices"]) > 1:
            c["price_spread"] = max(c["buy_prices"]) - min(c["buy_prices"])
            c["min_buy_price"] = min(c["buy_prices"])
            c["max_buy_price"] = max(c["buy_prices"])
        else:
            c["price_spread"] = 0.0
            c["min_buy_price"] = c["first_buy_price"]
            c["max_buy_price"] = c["first_buy_price"]
        # SELL の有無
        c["has_sell"] = c["sell_proceeds"] > 0

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


def aggregate_games(conditions):
    games = {}
    for cid, c in conditions.items():
        game_key = c["eventSlug"] or c["slug"]
        if not game_key:
            continue
        if game_key not in games:
            games[game_key] = {
                "game_key": game_key, "slug": c["slug"], "title": c["title"],
                "date": c["date"], "first_ts": c["first_trade_ts"], "conditions": [],
            }
        games[game_key]["conditions"].append(c)
        if c["first_trade_ts"] < games[game_key]["first_ts"]:
            games[game_key]["first_ts"] = c["first_trade_ts"]
            games[game_key]["date"] = c["date"]

    result = []
    for gk, g in games.items():
        conds = g["conditions"]
        has_win = any(c["status"] == "WIN" for c in conds)
        has_loss = any(c["status"] == "LOSS_OR_OPEN" for c in conds)
        has_merge = any(c["status"] == "MERGED" for c in conds)

        if has_win and not has_loss:
            game_result = "WIN"
        elif has_loss and not has_win:
            game_result = "LOSS"
        elif has_win and has_loss:
            game_result = "MIXED"
        elif has_merge:
            game_result = "MERGED"
        else:
            game_result = "OPEN"

        total_pnl = sum(c["pnl"] for c in conds)
        total_cost = sum(c["net_cost"] for c in conds)

        bet_conds = [c for c in conds if c["buy_cost"] > 0]

        result.append({
            "game_key": gk, "slug": g["slug"], "title": g["title"],
            "date": g["date"], "first_ts": g["first_ts"],
            "n_conditions": len(conds), "game_result": game_result,
            "total_cost": total_cost, "total_pnl": total_pnl,
            "bet_conditions": bet_conds,
        })

    result.sort(key=lambda x: x["first_ts"])
    return result


# 校正テーブル (0.25-0.95)
CAL_TABLE = [
    (0.25, 0.30, 0.852), (0.30, 0.35, 0.822), (0.35, 0.40, 0.904),
    (0.40, 0.45, 0.917), (0.45, 0.50, 0.938), (0.50, 0.55, 0.947),
    (0.55, 0.60, 0.957), (0.60, 0.65, 0.974), (0.65, 0.70, 0.931),
    (0.70, 0.75, 0.933), (0.75, 0.80, 0.973), (0.80, 0.85, 1.000),
    (0.85, 0.90, 1.000), (0.90, 0.95, 1.000),
]

def lookup_band(price):
    for lo, hi, wr in CAL_TABLE:
        if lo <= price < hi:
            return (lo, hi, wr)
    return None


def has_signal(game):
    """モデルがシグナルを出すか."""
    for c in game["bet_conditions"]:
        price = c["avg_buy_price"]
        if price <= 0 or price >= 1:
            continue
        band = lookup_band(price)
        if band is None:
            continue
        ev = band[2] / price - 1
        if ev > 0:
            return True
    return False


def get_best_signal(game):
    """最高 EV のシグナルを返す."""
    best = None
    best_ev = -999
    for c in game["bet_conditions"]:
        price = c["avg_buy_price"]
        if price <= 0 or price >= 1:
            continue
        band = lookup_band(price)
        if band is None:
            continue
        ev = band[2] / price - 1
        if ev > 0 and ev > best_ev:
            best_ev = ev
            best = c
    return best


def main():
    print("=" * 70)
    print("NO_SIGNAL 試合 & 赤字原因分析")
    print("=" * 70)

    trades, redeems, merges = load_data()
    conditions = build_condition_pnl(trades, redeems, merges)
    all_games = aggregate_games(conditions)

    settled = [g for g in all_games if g["game_result"] in ("WIN", "LOSS", "MIXED")]
    recent = settled[-1000:]

    no_signal_games = [g for g in recent if not has_signal(g)]
    signal_games = [g for g in recent if has_signal(g)]

    # ============================================================
    # Part 1: NO_SIGNAL 138試合の分析
    # ============================================================
    print(f"\n{'='*70}")
    print(f"Part 1: NO_SIGNAL 試合の分析 ({len(no_signal_games)} 試合)")
    print(f"{'='*70}")

    # lhtsports がどの価格帯に賭けていたか
    ns_prices = []
    ns_results = {"WIN": 0, "LOSS": 0, "MIXED": 0}
    ns_pnl_total = 0.0
    ns_cost_total = 0.0

    for g in no_signal_games:
        ns_results[g["game_result"]] = ns_results.get(g["game_result"], 0) + 1
        ns_pnl_total += g["total_pnl"]
        ns_cost_total += g["total_cost"]
        for c in g["bet_conditions"]:
            ns_prices.append(c["avg_buy_price"])

    print(f"\n  lhtsports の結果:")
    print(f"    WIN: {ns_results.get('WIN', 0)}  LOSS: {ns_results.get('LOSS', 0)}  MIXED: {ns_results.get('MIXED', 0)}")
    wr = ns_results.get('WIN', 0) / len(no_signal_games) * 100 if no_signal_games else 0
    print(f"    勝率: {wr:.1f}%")
    print(f"    P&L: ${ns_pnl_total:,.2f}")
    print(f"    コスト: ${ns_cost_total:,.2f}")
    roi = ns_pnl_total / ns_cost_total * 100 if ns_cost_total > 0 else 0
    print(f"    ROI: {roi:.1f}%")

    # 価格帯分布
    print(f"\n  NO_SIGNAL 試合の lhtsports 購入価格帯:")
    price_buckets = defaultdict(lambda: {"count": 0, "pnl": 0.0, "wins": 0, "losses": 0})
    for g in no_signal_games:
        for c in g["bet_conditions"]:
            p = c["avg_buy_price"]
            if p < 0.10:
                bucket = "< 0.10"
            elif p < 0.15:
                bucket = "0.10-0.15"
            elif p < 0.20:
                bucket = "0.15-0.20"
            elif p < 0.25:
                bucket = "0.20-0.25"
            elif p >= 0.95:
                bucket = ">= 0.95"
            else:
                bucket = f"IN TABLE ({p:.2f})"
            pb = price_buckets[bucket]
            pb["count"] += 1
            pb["pnl"] += c["pnl"]
            if c["status"] == "WIN":
                pb["wins"] += 1
            else:
                pb["losses"] += 1

    print(f"    {'価格帯':>15} | {'N':>4} | {'W':>3} | {'L':>3} | {'勝率':>6} | {'P&L':>12}")
    print(f"    {'-'*15}-+-{'-'*4}-+-{'-'*3}-+-{'-'*3}-+-{'-'*6}-+-{'-'*12}")
    for bucket in sorted(price_buckets.keys()):
        pb = price_buckets[bucket]
        n = pb["wins"] + pb["losses"]
        w = pb["wins"] / n * 100 if n > 0 else 0
        print(f"    {bucket:>15} | {pb['count']:>4} | {pb['wins']:>3} | {pb['losses']:>3} | {w:>5.1f}% | ${pb['pnl']:>10,.2f}")

    # NO_SIGNAL の理由の内訳
    print(f"\n  NO_SIGNAL の理由:")
    reason_counts = defaultdict(int)
    for g in no_signal_games:
        reasons = set()
        for c in g["bet_conditions"]:
            price = c["avg_buy_price"]
            band = lookup_band(price)
            if band is None:
                if price < 0.25:
                    reasons.add(f"price < 0.25 ({price:.3f})")
                elif price >= 0.95:
                    reasons.add(f"price >= 0.95 ({price:.3f})")
            else:
                ev = band[2] / price - 1
                if ev <= 0:
                    reasons.add(f"negative EV ({price:.3f}, band wr={band[2]:.3f})")
        for r in reasons:
            reason_counts[r] += 1

    # グルーピング
    below_25 = sum(1 for g in no_signal_games
                   if all(c["avg_buy_price"] < 0.25 for c in g["bet_conditions"]))
    above_95 = sum(1 for g in no_signal_games
                   if any(c["avg_buy_price"] >= 0.95 for c in g["bet_conditions"]))
    # 両方の condition が table 外
    all_outside = sum(1 for g in no_signal_games
                      if all(lookup_band(c["avg_buy_price"]) is None for c in g["bet_conditions"]))

    print(f"    全 condition が price < 0.25:  {below_25} 試合")
    print(f"    全 condition が table 外:       {all_outside} 試合")

    # NO_SIGNAL 試合の詳細 (直近20件)
    print(f"\n  NO_SIGNAL 試合サンプル (直近20件):")
    print(f"    {'日付':>10} | {'試合':>30} | {'結果':>5} | {'P&L':>10} | {'Conditions':>50}")
    print(f"    {'-'*10}-+-{'-'*30}-+-{'-'*5}-+-{'-'*10}-+-{'-'*50}")
    for g in no_signal_games[-20:]:
        cond_info = []
        for c in g["bet_conditions"]:
            cond_info.append(f"{c['outcome'][:10]}@{c['avg_buy_price']:.3f}({c['status'][:4]})")
        title = (g["title"] or g["game_key"])[:30]
        print(f"    {g['date']:>10} | {title:>30} | {g['game_result']:>5} | ${g['total_pnl']:>8,.2f} | {', '.join(cond_info)[:50]}")

    # ============================================================
    # Part 2: 赤字バンドの原因分析
    # ============================================================
    print(f"\n\n{'='*70}")
    print(f"Part 2: 赤字・大損失の原因分析")
    print(f"{'='*70}")

    # シグナル発生した試合で band 別に深掘り
    print(f"\n  === Band 別の詳細分析 ===")

    band_details = defaultdict(lambda: {
        "wins": [], "losses": [], "dca_count": 0, "merge_count": 0,
        "sell_count": 0, "total_pnl": 0.0,
    })

    for g in signal_games:
        best = get_best_signal(g)
        if best is None:
            continue
        price = best["avg_buy_price"]
        band = lookup_band(price)
        if band is None:
            continue
        band_key = f"{band[0]:.2f}-{band[1]:.2f}"
        bd = band_details[band_key]

        info = {
            "game": g["game_key"],
            "date": g["date"],
            "title": g["title"],
            "outcome": best["outcome"],
            "price": price,
            "first_price": best["first_buy_price"],
            "pnl": best["pnl"],
            "cost": best["net_cost"],
            "status": best["status"],
            "dca": best["is_dca"],
            "dca_count": best["dca_count"],
            "has_sell": best["has_sell"],
            "price_spread": best["price_spread"],
            "merge_usdc": best["merge_usdc"],
            "buy_cost": best["buy_cost"],
        }

        if best["status"] == "WIN":
            bd["wins"].append(info)
        else:
            bd["losses"].append(info)

        bd["total_pnl"] += best["pnl"]
        if best["is_dca"]:
            bd["dca_count"] += 1
        if best["merge_usdc"] > 0:
            bd["merge_count"] += 1
        if best["has_sell"]:
            bd["sell_count"] += 1

    for band_key in sorted(band_details):
        bd = band_details[band_key]
        n_win = len(bd["wins"])
        n_loss = len(bd["losses"])
        n_total = n_win + n_loss
        wr = n_win / n_total * 100 if n_total > 0 else 0

        print(f"\n  --- Band {band_key} ---")
        print(f"    W: {n_win}  L: {n_loss}  勝率: {wr:.1f}%  P&L: ${bd['total_pnl']:,.2f}")
        print(f"    DCA使用: {bd['dca_count']}/{n_total} ({bd['dca_count']/n_total*100:.0f}%)")
        print(f"    MERGE あり: {bd['merge_count']}  SELL あり: {bd['sell_count']}")

        # WIN で P&L がマイナスのケース
        win_negative = [w for w in bd["wins"] if w["pnl"] < 0]
        if win_negative:
            print(f"    ⚠ WIN なのに P&L マイナス: {len(win_negative)} 件")
            for w in sorted(win_negative, key=lambda x: x["pnl"])[:5]:
                print(f"      {w['date']} {w['outcome'][:15]} @{w['price']:.3f} "
                      f"DCA={w['dca_count']}回 spread={w['price_spread']:.3f} "
                      f"cost=${w['cost']:,.0f} merge=${w['merge_usdc']:,.0f} "
                      f"P&L=${w['pnl']:,.0f}")

        # 負けの詳細
        if bd["losses"]:
            print(f"    LOSS 詳細:")
            loss_total = sum(l["pnl"] for l in bd["losses"])
            loss_dca = sum(1 for l in bd["losses"] if l["dca"])
            print(f"      合計損失: ${loss_total:,.2f}")
            print(f"      うち DCA: {loss_dca}/{n_loss}")
            for l in sorted(bd["losses"], key=lambda x: x["pnl"])[:5]:
                print(f"      {l['date']} {l['outcome'][:15]} @{l['price']:.3f} "
                      f"DCA={l['dca_count']}回 cost=${l['cost']:,.0f} "
                      f"P&L=${l['pnl']:,.0f}")

    # ============================================================
    # Part 3: 全体で最も大きな損失の試合 TOP 20
    # ============================================================
    print(f"\n\n{'='*70}")
    print(f"Part 3: 損失額 TOP 20 試合")
    print(f"{'='*70}")

    all_signal = []
    for g in signal_games:
        best = get_best_signal(g)
        if best:
            all_signal.append((g, best))

    all_signal.sort(key=lambda x: x[1]["pnl"])

    print(f"\n  {'日付':>10} | {'試合':>25} | {'Outcome':>12} | {'Avg$':>5} | {'1st$':>5} | "
          f"{'DCA':>3} | {'Spread':>6} | {'Cost':>10} | {'Merge':>8} | {'P&L':>10} | {'Status':>5}")
    print(f"  {'-'*10}-+-{'-'*25}-+-{'-'*12}-+-{'-'*5}-+-{'-'*5}-+-"
          f"{'-'*3}-+-{'-'*6}-+-{'-'*10}-+-{'-'*8}-+-{'-'*10}-+-{'-'*5}")

    for g, c in all_signal[:20]:
        title = (g["title"] or g["game_key"])[:25]
        print(f"  {g['date']:>10} | {title:>25} | {c['outcome'][:12]:>12} | "
              f"{c['avg_buy_price']:.3f} | {c['first_buy_price']:.3f} | "
              f"{c['dca_count']:>3} | {c['price_spread']:.3f}  | "
              f"${c['net_cost']:>8,.0f} | ${c['merge_usdc']:>6,.0f} | "
              f"${c['pnl']:>8,.0f} | {c['status'][:5]:>5}")

    # ============================================================
    # Part 4: DCA の影響分析
    # ============================================================
    print(f"\n\n{'='*70}")
    print(f"Part 4: DCA (ドルコスト平均法) の影響分析")
    print(f"{'='*70}")

    # シグナル試合の全 condition
    all_best = [get_best_signal(g) for g in signal_games]
    all_best = [c for c in all_best if c is not None]

    dca_conds = [c for c in all_best if c["is_dca"]]
    single_conds = [c for c in all_best if not c["is_dca"]]

    print(f"\n  シングルエントリー ({len(single_conds)} 件):")
    sw = sum(1 for c in single_conds if c["status"] == "WIN")
    sp = sum(c["pnl"] for c in single_conds)
    sc = sum(c["net_cost"] for c in single_conds)
    print(f"    勝率: {sw/len(single_conds)*100:.1f}%  P&L: ${sp:,.2f}  "
          f"コスト: ${sc:,.2f}  ROI: {sp/sc*100:.1f}%")

    print(f"\n  DCA エントリー ({len(dca_conds)} 件):")
    dw = sum(1 for c in dca_conds if c["status"] == "WIN")
    dp = sum(c["pnl"] for c in dca_conds)
    dc = sum(c["net_cost"] for c in dca_conds)
    print(f"    勝率: {dw/len(dca_conds)*100:.1f}%  P&L: ${dp:,.2f}  "
          f"コスト: ${dc:,.2f}  ROI: {dp/dc*100:.1f}%")

    # DCA 回数別
    print(f"\n  DCA 回数別パフォーマンス:")
    dca_by_count = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "cost": 0.0})
    for c in all_best:
        key = min(c["dca_count"], 10)  # 10+ をまとめる
        label = str(key) if key < 10 else "10+"
        d = dca_by_count[label]
        d["cost"] += c["net_cost"]
        d["pnl"] += c["pnl"]
        if c["status"] == "WIN":
            d["wins"] += 1
        else:
            d["losses"] += 1

    print(f"    {'BUY回数':>8} | {'N':>5} | {'W':>4} | {'L':>4} | {'勝率':>6} | {'P&L':>12} | {'ROI':>7}")
    print(f"    {'-'*8}-+-{'-'*5}-+-{'-'*4}-+-{'-'*4}-+-{'-'*6}-+-{'-'*12}-+-{'-'*7}")
    for key in sorted(dca_by_count.keys(), key=lambda x: int(x.replace("+", ""))):
        d = dca_by_count[key]
        n = d["wins"] + d["losses"]
        wr = d["wins"] / n * 100 if n > 0 else 0
        roi = d["pnl"] / d["cost"] * 100 if d["cost"] > 0 else 0
        print(f"    {key:>8} | {n:>5} | {d['wins']:>4} | {d['losses']:>4} | "
              f"{wr:>5.1f}% | ${d['pnl']:>10,.2f} | {roi:>6.1f}%")

    # ============================================================
    # Part 5: MERGE の影響
    # ============================================================
    print(f"\n\n{'='*70}")
    print(f"Part 5: MERGE (早期退出) の影響")
    print(f"{'='*70}")

    merge_conds = [c for c in all_best if c["merge_usdc"] > 0]
    no_merge_conds = [c for c in all_best if c["merge_usdc"] == 0]

    print(f"\n  MERGE なし ({len(no_merge_conds)} 件):")
    nmw = sum(1 for c in no_merge_conds if c["status"] == "WIN")
    nmp = sum(c["pnl"] for c in no_merge_conds)
    nmc = sum(c["net_cost"] for c in no_merge_conds)
    if no_merge_conds:
        print(f"    勝率: {nmw/len(no_merge_conds)*100:.1f}%  P&L: ${nmp:,.2f}  ROI: {nmp/nmc*100:.1f}%")

    print(f"\n  MERGE あり ({len(merge_conds)} 件):")
    if merge_conds:
        mw = sum(1 for c in merge_conds if c["status"] == "WIN")
        mp = sum(c["pnl"] for c in merge_conds)
        mc = sum(c["net_cost"] for c in merge_conds)
        print(f"    勝率: {mw/len(merge_conds)*100:.1f}%  P&L: ${mp:,.2f}  ROI: {mp/mc*100:.1f}%")
        print(f"    MERGE 額合計: ${sum(c['merge_usdc'] for c in merge_conds):,.2f}")
        # WIN なのに MERGE で赤字になったケース
        win_merge_neg = [c for c in merge_conds if c["status"] == "WIN" and c["pnl"] < 0]
        print(f"    WIN + MERGE + P&L < 0: {len(win_merge_neg)} 件")
        if win_merge_neg:
            for c in sorted(win_merge_neg, key=lambda x: x["pnl"])[:10]:
                print(f"      {c['date']} {c['outcome'][:15]} @{c['avg_buy_price']:.3f} "
                      f"buy=${c['buy_cost']:,.0f} merge=${c['merge_usdc']:,.0f} "
                      f"redeem=${c['redeem_usdc']:,.0f} P&L=${c['pnl']:,.0f}")
    else:
        print(f"    (該当なし)")

    print("\n完了。")


if __name__ == "__main__":
    main()
