"""Analyze a trader's P&L and generate strategy profile.

Orchestrates pnl.py and strategy_profile.py to produce:
- condition_pnl.json
- game_pnl.json
- strategy_profile.json
- pnl_report.md (full analysis only)

Usage:
  python scripts/analyze_trader.py --username lhtsports         # Full analysis
  python scripts/analyze_trader.py --all --profile-only          # Profiles only (fast)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TRADERS_DIR = PROJECT_ROOT / "data" / "traders"
REGISTRY_PATH = TRADERS_DIR / "registry.json"

# パッケージ import のため
sys.path.insert(0, str(PROJECT_ROOT))

from src.analysis.pnl import aggregate_by_game, build_condition_pnl, generate_report  # noqa: E402
from src.analysis.strategy_profile import build_profile  # noqa: E402


def load_registry() -> list[dict]:
    if REGISTRY_PATH.exists():
        with open(REGISTRY_PATH) as f:
            return json.load(f)
    return []


def save_registry(registry: list[dict]) -> None:
    with open(REGISTRY_PATH, "w") as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)


def analyze_trader(
    username: str,
    address: str,
    profile_only: bool = False,
) -> dict | None:
    """Run full analysis for one trader. Returns profile dict or None."""
    trader_dir = TRADERS_DIR / username

    # ファイル確認
    trades_path = trader_dir / "raw_trade.json"
    redeem_path = trader_dir / "raw_redeem.json"
    merge_path = trader_dir / "raw_merge.json"

    if not trades_path.exists():
        print(f"  No trade data for {username}. Run fetch_trader.py first.")
        return None

    # ロード
    with open(trades_path) as f:
        trades = json.load(f)
    redeems = json.load(open(redeem_path)) if redeem_path.exists() else []
    merges = json.load(open(merge_path)) if merge_path.exists() else []

    print(f"  Data: {len(trades):,} trades, {len(redeems):,} redeems, {len(merges):,} merges")

    # Condition P&L
    conditions = build_condition_pnl(trades, redeems, merges)
    print(f"  Conditions: {len(conditions):,}")

    # Game aggregation
    games = aggregate_by_game(conditions)
    print(f"  Games: {len(games):,}")

    # Leaderboard PnL 突合
    registry = load_registry()
    lb_entry = next(
        (r for r in registry if r.get("username", "").lower() == username.lower()),
        None,
    )
    lb_pnl = lb_entry.get("pnl", 0) if lb_entry else 0
    lb_volume = lb_entry.get("volume", 0) if lb_entry else 0

    calc_pnl = sum(c["pnl"] for c in conditions.values())
    missing = sum(
        1 for c in conditions.values() if c.get("data_quality") == "missing_trades"
    )
    missing_pnl = sum(
        c["pnl"]
        for c in conditions.values()
        if c.get("data_quality") == "missing_trades"
    )

    if lb_pnl and abs(calc_pnl - lb_pnl) / max(abs(lb_pnl), 1) > 0.2:
        print(f"  ⚠ PnL MISMATCH: Calculated ${calc_pnl:,.0f} vs Leaderboard ${lb_pnl:,.0f}")
        print(f"    {missing} conditions missing TRADE data (${missing_pnl:,.0f} phantom PnL)")
        data_quality = "incomplete"
    else:
        data_quality = "complete"

    # Strategy profile
    profile = build_profile(
        conditions,
        games,
        username,
        address,
        lb_pnl=lb_pnl,
        lb_volume=lb_volume,
        data_quality=data_quality,
        missing_trade_conditions=missing,
    )
    profile_dict = profile.to_dict()

    # 保存
    with open(trader_dir / "strategy_profile.json", "w") as f:
        json.dump(profile_dict, f, indent=2, ensure_ascii=False)

    if not profile_only:
        # Condition P&L JSON
        condition_list = sorted(conditions.values(), key=lambda x: x["pnl"], reverse=True)
        with open(trader_dir / "condition_pnl.json", "w") as f:
            json.dump(condition_list, f, indent=2, ensure_ascii=False)

        # Game P&L JSON
        with open(trader_dir / "game_pnl.json", "w") as f:
            json.dump(games, f, indent=2, ensure_ascii=False)

        # Full report
        report = generate_report(conditions, games, trader_name=username)
        with open(trader_dir / "pnl_report.md", "w") as f:
            f.write(report)
        print(f"  Report: {trader_dir / 'pnl_report.md'}")

    # Quick summary
    pnl_s = f"P&L: ${profile.total_pnl:,.2f}"
    roi_s = f"ROI: {profile.roi_pct:.2f}%"
    wr_s = f"Win%: {profile.win_rate:.1%}"
    print(f"  {pnl_s} | {roi_s} | {wr_s}")
    sh_s = f"Sharpe(d): {profile.daily_sharpe:.2f}"
    dd_s = f"MaxDD: {profile.max_drawdown_pct:.1f}%"
    cs_s = f"Consistency: {profile.consistency_score:.1%}"
    print(f"  {sh_s} | {dd_s} | {cs_s}")

    return profile_dict


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyze trader P&L and strategy profile")
    ap.add_argument("--username", type=str, help="Trader username")
    ap.add_argument("--all", action="store_true", help="Analyze all fetched traders")
    ap.add_argument(
        "--profile-only",
        action="store_true",
        help="Only compute strategy profile (skip full report)",
    )
    args = ap.parse_args()

    registry = load_registry()

    if args.all:
        fetched = [t for t in registry if t.get("status") in ("fetched", "analyzed")]
        if not fetched:
            print("No fetched traders. Run fetch_trader.py first.", file=sys.stderr)
            sys.exit(1)

        print(f"Analyzing {len(fetched)} traders...")
        for i, t in enumerate(fetched, 1):
            uname = t.get("username", t["proxy_wallet"][:10])
            wallet = t["proxy_wallet"]
            print(f"\n{'=' * 60}")
            print(f"[{i}/{len(fetched)}] {uname}")
            print(f"{'=' * 60}")

            result = analyze_trader(uname, wallet, profile_only=args.profile_only)
            if result:
                t["status"] = "analyzed"
                save_registry(registry)

        print(f"\nDone. Analyzed {len(fetched)} traders.")
        return

    if not args.username:
        print("Specify --username or --all.", file=sys.stderr)
        sys.exit(1)

    # 単一
    address = ""
    for t in registry:
        if t.get("username", "").lower() == args.username.lower():
            address = t["proxy_wallet"]
            break

    if not address:
        print(f"Username '{args.username}' not in registry.", file=sys.stderr)
        sys.exit(1)

    print(f"Analyzing: {args.username}")
    result = analyze_trader(args.username, address, profile_only=args.profile_only)
    if result:
        for t in registry:
            if t["proxy_wallet"].lower() == address.lower():
                t["status"] = "analyzed"
                save_registry(registry)
                break


if __name__ == "__main__":
    main()
