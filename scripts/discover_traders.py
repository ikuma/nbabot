"""Discover top traders from Polymarket Leaderboard API.

Fetches leaderboard across multiple categories and time periods,
identifies persistent winners (present in both ALL and MONTH rankings),
and saves to data/traders/registry.json.

Usage:
  python scripts/discover_traders.py                                # OVERALL + SPORTS top 30
  python scripts/discover_traders.py --categories ALL               # All categories
  python scripts/discover_traders.py --categories SPORTS,CRYPTO --limit 50
  python scripts/discover_traders.py --list                         # Show registered traders
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TRADERS_DIR = PROJECT_ROOT / "data" / "traders"
REGISTRY_PATH = TRADERS_DIR / "registry.json"

LEADERBOARD_URL = "https://data-api.polymarket.com/v1/leaderboard"

ALL_CATEGORIES = ["OVERALL", "SPORTS", "POLITICS", "CRYPTO", "CULTURE"]
DEFAULT_CATEGORIES = ["OVERALL", "SPORTS"]
TIME_PERIODS = ["ALL", "MONTH"]

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
}
SLEEP_SEC = 0.5


def fetch_leaderboard(
    category: str,
    time_period: str,
    limit: int = 30,
    offset: int = 0,
) -> list[dict]:
    """Fetch one page of leaderboard."""
    params = {
        "category": category,
        "timePeriod": time_period,
        "limit": limit,
        "offset": offset,
    }
    qs = urllib.parse.urlencode(params)
    url = f"{LEADERBOARD_URL}?{qs}"
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            return data if isinstance(data, list) else []
    except Exception as e:
        print(f"  Error fetching {category}/{time_period}: {e}", file=sys.stderr)
        return []


def discover(
    categories: list[str],
    limit: int = 30,
) -> list[dict]:
    """Fetch leaderboards and build trader records with persistent_winner flag."""
    traders: dict[str, dict] = {}

    for category in categories:
        for period in TIME_PERIODS:
            rank_key = f"{category}_{period}"
            print(f"Fetching {rank_key} (limit={limit})...")
            entries = fetch_leaderboard(category, period, limit=limit)
            print(f"  Got {len(entries)} entries")

            for rank, entry in enumerate(entries, 1):
                wallet = entry.get("proxyWallet", entry.get("userAddress", ""))
                if not wallet:
                    continue

                # API returns userName (camelCase) and vol (short)
                uname = entry.get("userName", entry.get("username", ""))

                if wallet not in traders:
                    traders[wallet] = {
                        "proxy_wallet": wallet,
                        "username": uname,
                        "categories": set(),
                        "ranks": {},
                        "pnl": 0.0,
                        "volume": 0.0,
                        "persistent_winner": False,
                        "status": "discovered",
                        "trade_count": 0,
                        "last_fetch_ts": None,
                    }

                t = traders[wallet]
                t["ranks"][rank_key] = rank
                t["categories"].add(category)
                if not t["username"]:
                    t["username"] = uname

                # PnL / Volume: 最大値を保持
                entry_pnl = float(entry.get("pnl", 0) or 0)
                entry_vol = float(entry.get("vol", entry.get("volume", 0)) or 0)
                if abs(entry_pnl) > abs(t["pnl"]):
                    t["pnl"] = entry_pnl
                if entry_vol > t["volume"]:
                    t["volume"] = entry_vol

            time.sleep(SLEEP_SEC)

    # persistent_winner: ALL と MONTH の両方に登場
    for wallet, t in traders.items():
        rank_keys = set(t["ranks"].keys())
        for category in categories:
            all_key = f"{category}_ALL"
            month_key = f"{category}_MONTH"
            if all_key in rank_keys and month_key in rank_keys:
                t["persistent_winner"] = True
                break

    # set -> list (JSON シリアライズ用)
    result = []
    for t in traders.values():
        t["categories"] = sorted(t["categories"])
        result.append(t)

    # PnL 降順ソート
    result.sort(key=lambda x: x["pnl"], reverse=True)
    return result


def load_registry() -> list[dict]:
    """Load existing registry.json, return empty list if not found."""
    if REGISTRY_PATH.exists():
        with open(REGISTRY_PATH) as f:
            return json.load(f)
    return []


def merge_registry(existing: list[dict], new_traders: list[dict]) -> list[dict]:
    """Merge new traders into existing registry (preserve status, update ranks)."""
    by_wallet: dict[str, dict] = {}
    for t in existing:
        by_wallet[t["proxy_wallet"]] = t

    for t in new_traders:
        wallet = t["proxy_wallet"]
        if wallet in by_wallet:
            old = by_wallet[wallet]
            # ランクとカテゴリを更新
            old["ranks"].update(t["ranks"])
            old_cats = set(old.get("categories", []))
            old_cats.update(t.get("categories", []))
            old["categories"] = sorted(old_cats)
            old["persistent_winner"] = old.get("persistent_winner", False) or t["persistent_winner"]
            if not old.get("username"):
                old["username"] = t.get("username", "")
            if abs(t["pnl"]) > abs(old.get("pnl", 0)):
                old["pnl"] = t["pnl"]
            if t["volume"] > old.get("volume", 0):
                old["volume"] = t["volume"]
        else:
            by_wallet[wallet] = t

    result = list(by_wallet.values())
    result.sort(key=lambda x: x.get("pnl", 0), reverse=True)
    return result


def save_registry(traders: list[dict]) -> None:
    """Save registry.json."""
    TRADERS_DIR.mkdir(parents=True, exist_ok=True)
    with open(REGISTRY_PATH, "w") as f:
        json.dump(traders, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(traders)} traders to {REGISTRY_PATH}")


def show_list() -> None:
    """Print registered traders."""
    registry = load_registry()
    if not registry:
        print("No traders registered yet. Run discover first.")
        return

    print(f"\n{'=' * 80}")
    print(f"Registered Traders: {len(registry)}")
    print(f"{'=' * 80}")
    hdr = (
        f"{'#':>3} {'Username':<20} {'PnL':>14}"
        f" {'Volume':>14} {'Status':<12} {'Persist':>7} Categories"
    )
    print(hdr)
    print("-" * 95)

    for i, t in enumerate(registry, 1):
        username = t.get("username", "?")[:20]
        pnl = t.get("pnl", 0)
        vol = t.get("volume", 0)
        status = t.get("status", "?")
        persist = "Yes" if t.get("persistent_winner") else ""
        cats = ", ".join(t.get("categories", []))
        print(
            f"{i:>3} {username:<20} ${pnl:>12,.0f} ${vol:>12,.0f} {status:<12} {persist:>7} {cats}"
        )

    persistent = [t for t in registry if t.get("persistent_winner")]
    print(f"\nPersistent winners: {len(persistent)} / {len(registry)}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Discover top Polymarket traders")
    ap.add_argument(
        "--categories",
        type=str,
        default=None,
        help="Comma-separated categories (OVERALL,SPORTS,POLITICS,CRYPTO,CULTURE) or ALL",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=30,
        help="Traders per category/period (default: 30)",
    )
    ap.add_argument("--list", action="store_true", help="Show registered traders")
    args = ap.parse_args()

    if args.list:
        show_list()
        return

    if args.categories:
        if args.categories.upper() == "ALL":
            categories = ALL_CATEGORIES
        else:
            categories = [c.strip().upper() for c in args.categories.split(",")]
    else:
        categories = DEFAULT_CATEGORIES

    print(f"Discovering traders in: {', '.join(categories)} (limit={args.limit})")
    new_traders = discover(categories, limit=args.limit)
    print(f"\nDiscovered {len(new_traders)} unique traders")

    persistent = [t for t in new_traders if t["persistent_winner"]]
    print(f"Persistent winners (ALL + MONTH): {len(persistent)}")

    existing = load_registry()
    merged = merge_registry(existing, new_traders)
    save_registry(merged)

    # Quick summary
    print(f"\n{'=' * 60}")
    print("Top 10 by PnL:")
    print(f"{'=' * 60}")
    for i, t in enumerate(merged[:10], 1):
        name = t.get("username", t["proxy_wallet"][:10])
        pw = "***" if t.get("persistent_winner") else "   "
        cats = ",".join(t.get("categories", []))
        pnl_s = f"PnL: ${t['pnl']:>12,.0f}"
        vol_s = f"Vol: ${t['volume']:>12,.0f}"
        print(f"  {i:>2}. {pw} {name:<20} {pnl_s}  {vol_s}  [{cats}]")


if __name__ == "__main__":
    main()
