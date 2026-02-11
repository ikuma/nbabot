"""Fetch Polymarket activity data for any trader.

Unified script that fetches TRADE, REDEEM, MERGE, and REWARD data
for a given address/username. Supports incremental fetching and quick mode.

Usage:
  python scripts/fetch_trader.py --username lhtsports            # Full fetch
  python scripts/fetch_trader.py --address 0x...                 # By address
  python scripts/fetch_trader.py --all --quick                   # All registered, 2000 trades each
  python scripts/fetch_trader.py --username X --incremental      # Delta only
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TRADERS_DIR = PROJECT_ROOT / "data" / "traders"
REGISTRY_PATH = TRADERS_DIR / "registry.json"

BASE_URL = "https://data-api.polymarket.com/activity"
LIMIT = 500
MAX_OFFSET = 3000  # API は offset>3000 で 400 を返すため
SLEEP_SEC = 0.4

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
}

ACTIVITY_TYPES = ["TRADE", "REDEEM", "MERGE", "REWARD"]


def fetch_page(
    address: str,
    activity_type: str,
    offset: int,
    end_ts: Optional[int] = None,
    start_ts: Optional[int] = None,
) -> list[dict]:
    """Fetch one page of activity."""
    params: dict[str, str | int] = {
        "user": address,
        "limit": LIMIT,
        "offset": offset,
        "type": activity_type,
    }
    if end_ts is not None:
        params["end"] = end_ts
    if start_ts is not None:
        params["start"] = start_ts
    qs = urllib.parse.urlencode(params)
    url = f"{BASE_URL}?{qs}"
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=45) as resp:
            data = json.loads(resp.read().decode())
            return data if isinstance(data, list) else []
    except Exception as e:
        print(f"  Error type={activity_type} offset={offset}: {e}", file=sys.stderr)
        return []


def fetch_all_for_type(
    address: str,
    activity_type: str,
    max_items: int = 0,
    start_ts: Optional[int] = None,
) -> list[dict]:
    """Fetch all records for a given activity type using time-range batching.

    Args:
        address: Proxy wallet address.
        activity_type: TRADE, REDEEM, MERGE, REWARD.
        max_items: Stop after this many items (0 = unlimited).
        start_ts: Only fetch records newer than this timestamp (incremental).
    """
    all_records: list[dict] = []
    end_ts: Optional[int] = None
    batch_num = 0

    while True:
        batch_num += 1
        batch: list[dict] = []
        offset = 0
        empty_streak = 0
        last_page_complete = False

        while offset <= MAX_OFFSET and empty_streak < 3:
            page = fetch_page(address, activity_type, offset, end_ts, start_ts)
            if not page:
                empty_streak += 1
                offset += LIMIT
                time.sleep(SLEEP_SEC)
                continue
            empty_streak = 0
            batch.extend(page)
            offset += LIMIT
            time.sleep(SLEEP_SEC)
            if len(page) < LIMIT:
                last_page_complete = False  # ページが不完全 = データ終端
                break
            last_page_complete = True  # ページが完全 = まだデータあり得る

        if not batch:
            break

        min_ts = min(r["timestamp"] for r in batch)
        all_records.extend(batch)

        if max_items and len(all_records) >= max_items:
            break
        if not last_page_complete:
            break

        end_ts = min_ts - 1
        if batch_num >= 200:
            print(f"  Stopping {activity_type} after 200 batches (safety).")
            break

    # Dedupe
    seen: set[tuple[int, str, str, float]] = set()
    unique: list[dict] = []
    for r in all_records:
        key = (
            r["timestamp"],
            r.get("transactionHash", ""),
            r.get("conditionId", r.get("asset", "")),
            float(r.get("size", 0)),
        )
        if key not in seen:
            seen.add(key)
            unique.append(r)

    unique.sort(key=lambda x: x["timestamp"])

    if max_items and len(unique) > max_items:
        unique = unique[-max_items:]  # 最新を保持

    return unique


def load_registry() -> list[dict]:
    """Load registry.json."""
    if REGISTRY_PATH.exists():
        with open(REGISTRY_PATH) as f:
            return json.load(f)
    return []


def save_registry(registry: list[dict]) -> None:
    """Save registry.json."""
    with open(REGISTRY_PATH, "w") as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)


def resolve_trader(
    username: str | None,
    address: str | None,
    registry: list[dict],
) -> tuple[str, str]:
    """Resolve username/address pair from arguments and registry.

    Returns (address, username).
    """
    if address and username:
        return address, username

    if username:
        for t in registry:
            if t.get("username", "").lower() == username.lower():
                return t["proxy_wallet"], t["username"]
        print(f"Username '{username}' not in registry. Use --address to specify.", file=sys.stderr)
        sys.exit(1)

    if address:
        for t in registry:
            if t["proxy_wallet"].lower() == address.lower():
                return t["proxy_wallet"], t.get("username", address[:10])
        return address, address[:10]

    print("Specify --username or --address.", file=sys.stderr)
    sys.exit(1)


def load_fetch_state(trader_dir: Path) -> dict:
    """Load incremental fetch state."""
    state_path = trader_dir / "fetch_state.json"
    if state_path.exists():
        with open(state_path) as f:
            return json.load(f)
    return {}


def save_fetch_state(trader_dir: Path, state: dict) -> None:
    """Save incremental fetch state."""
    with open(trader_dir / "fetch_state.json", "w") as f:
        json.dump(state, f, indent=2)


def merge_records(existing: list[dict], new: list[dict]) -> list[dict]:
    """Merge new records into existing, deduplicating."""
    seen: set[tuple[int, str, str, float]] = set()
    result: list[dict] = []
    for r in existing + new:
        key = (
            r["timestamp"],
            r.get("transactionHash", ""),
            r.get("conditionId", r.get("asset", "")),
            float(r.get("size", 0)),
        )
        if key not in seen:
            seen.add(key)
            result.append(r)
    result.sort(key=lambda x: x["timestamp"])
    return result


def fetch_trader(
    address: str,
    username: str,
    quick: bool = False,
    incremental: bool = False,
) -> dict[str, int]:
    """Fetch all activity types for a trader and save to data/traders/{username}/.

    Returns dict of {activity_type: count}.
    """
    trader_dir = TRADERS_DIR / username
    trader_dir.mkdir(parents=True, exist_ok=True)

    max_items = 2000 if quick else 0
    state = load_fetch_state(trader_dir) if incremental else {}
    counts: dict[str, int] = {}

    for activity_type in ACTIVITY_TYPES:
        filename = f"raw_{activity_type.lower()}.json"
        filepath = trader_dir / filename

        # インクリメンタル: 前回の最新 timestamp 以降のみ
        start_ts = None
        if incremental:
            start_ts = state.get(f"{activity_type}_max_ts")

        quick_note = f"(quick: max {max_items})" if quick else ""
        print(f"\n  Fetching {activity_type}{quick_note}...")
        records = fetch_all_for_type(
            address,
            activity_type,
            max_items=max_items,
            start_ts=start_ts,
        )
        print(f"  Got {len(records)} {activity_type} records")

        # インクリメンタル: 既存データとマージ
        if incremental and filepath.exists():
            with open(filepath) as f:
                existing = json.load(f)
            records = merge_records(existing, records)
            print(f"  Merged total: {len(records)}")

        if records:
            with open(filepath, "w") as f:
                json.dump(records, f, indent=2)

            # fetch_state 更新
            max_ts = max(r["timestamp"] for r in records)
            state[f"{activity_type}_max_ts"] = max_ts

        counts[activity_type] = len(records)

    # fetch_state 保存
    state["last_fetch_ts"] = int(time.time())
    state["quick"] = quick
    save_fetch_state(trader_dir, state)

    return counts


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch Polymarket trader activity data")
    ap.add_argument("--username", type=str, help="Trader username (from registry)")
    ap.add_argument("--address", type=str, help="Trader proxy wallet address")
    ap.add_argument("--all", action="store_true", help="Fetch all registered traders")
    ap.add_argument(
        "--quick",
        action="store_true",
        help="Quick mode: max 2000 trades per type",
    )
    ap.add_argument(
        "--incremental",
        action="store_true",
        help="Only fetch new records since last run",
    )
    args = ap.parse_args()

    registry = load_registry()

    if args.all:
        if not registry:
            print("No traders in registry. Run discover_traders.py first.", file=sys.stderr)
            sys.exit(1)

        print(f"Fetching data for {len(registry)} traders...")
        for i, t in enumerate(registry, 1):
            uname = t.get("username", t["proxy_wallet"][:10])
            wallet = t["proxy_wallet"]
            print(f"\n{'=' * 60}")
            print(f"[{i}/{len(registry)}] {uname} ({wallet[:10]}...)")
            print(f"{'=' * 60}")

            counts = fetch_trader(
                wallet,
                uname,
                quick=args.quick,
                incremental=args.incremental,
            )

            # registry を更新
            t["status"] = "fetched"
            t["trade_count"] = counts.get("TRADE", 0)
            t["last_fetch_ts"] = int(time.time())

            # 途中経過を保存
            save_registry(registry)

            total = sum(counts.values())
            detail = ", ".join(f"{k}={v}" for k, v in counts.items())
            print(f"\n  Total: {total} records ({detail})")

        print(f"\nDone. Fetched data for {len(registry)} traders.")
        return

    # 単一トレーダー
    address, username = resolve_trader(args.username, args.address, registry)
    print(f"Fetching: {username} ({address[:10]}...)")

    counts = fetch_trader(
        address,
        username,
        quick=args.quick,
        incremental=args.incremental,
    )

    # registry 更新 (登録済みの場合)
    for t in registry:
        if t["proxy_wallet"].lower() == address.lower():
            t["status"] = "fetched"
            t["trade_count"] = counts.get("TRADE", 0)
            t["last_fetch_ts"] = int(time.time())
            save_registry(registry)
            break

    total = sum(counts.values())
    print(f"\nDone. Total: {total} records ({', '.join(f'{k}={v}' for k, v in counts.items())})")
    print(f"Saved to: {TRADERS_DIR / username}/")


if __name__ == "__main__":
    main()
