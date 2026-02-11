"""Fetch @lhtsports non-TRADE activity (REDEEM, MERGE, REWARD) from Polymarket.

Each type must be queried separately (API does not support multi-type).
Uses the same time-range batching strategy as fetch_lhtsports_trades.py.

Usage:
  python scripts/fetch_lhtsports_activity.py
"""

from __future__ import annotations

import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional

BASE_URL = "https://data-api.polymarket.com/activity"
USER = "0xa6a856a8c8a7f14fd9be6ae11c367c7cbb755009"
LIMIT = 500
MAX_OFFSET = 10000
SLEEP_SEC = 0.4

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
}

OUTPUT_DIR = Path("data/reports/lhtsports-analysis")

ACTIVITY_TYPES = ["REDEEM", "MERGE", "REWARD"]


def fetch_page(activity_type: str, offset: int, end_ts: int | None = None) -> list[dict]:
    """Fetch one page of activity for a given type."""
    params: dict[str, str | int] = {
        "user": USER,
        "limit": LIMIT,
        "offset": offset,
        "type": activity_type,
    }
    if end_ts is not None:
        params["end"] = end_ts
    qs = urllib.parse.urlencode(params)
    url = f"{BASE_URL}?{qs}"
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=45) as resp:
            data = json.loads(resp.read().decode())
            return data if isinstance(data, list) else []
    except Exception as e:
        print(f"  Error type={activity_type} offset={offset} end={end_ts}: {e}", file=sys.stderr)
        return []


def fetch_all_for_type(activity_type: str) -> list[dict]:
    """Fetch all records for a given activity type using time-range batching."""
    all_records: list[dict] = []
    end_ts: int | None = None
    batch_num = 0

    while True:
        batch_num += 1
        print(f"  Batch {batch_num}: end_ts={end_ts}")
        batch: list[dict] = []
        offset = 0
        empty_streak = 0

        while offset <= MAX_OFFSET and empty_streak < 3:
            page = fetch_page(activity_type, offset, end_ts)
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
                break

        if not batch:
            print(f"  No more data.")
            break

        min_ts = min(r["timestamp"] for r in batch)
        print(f"  Got {len(batch)} records, oldest ts={min_ts}")
        all_records.extend(batch)

        if len(batch) < LIMIT:
            break
        end_ts = min_ts - 1

        if batch_num >= 50:
            print("  Stopping after 50 batches (safety).")
            break

    # Dedupe
    seen: set[tuple[int, str, str, float]] = set()
    unique: list[dict] = []
    for r in all_records:
        key = (
            r["timestamp"],
            r.get("transactionHash", ""),
            r.get("conditionId", r.get("asset", "")),
            r.get("size", 0),
        )
        if key not in seen:
            seen.add(key)
            unique.append(r)

    unique.sort(key=lambda x: x["timestamp"])
    return unique


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    combined: list[dict] = []

    for activity_type in ACTIVITY_TYPES:
        print(f"\n{'='*50}")
        print(f"Fetching {activity_type}...")
        print(f"{'='*50}")

        records = fetch_all_for_type(activity_type)
        print(f"Total {activity_type}: {len(records)} records")

        if records:
            # 個別ファイルに保存
            out_path = OUTPUT_DIR / f"lhtsports_{activity_type.lower()}.json"
            with open(out_path, "w") as f:
                json.dump(records, f, indent=2)
            print(f"Saved to {out_path}")
            combined.extend(records)

    # 全タイプ統合ファイル
    if combined:
        combined.sort(key=lambda x: x["timestamp"])
        combined_path = OUTPUT_DIR / "lhtsports_non_trade_activity.json"
        with open(combined_path, "w") as f:
            json.dump(combined, f, indent=2)
        print(f"\nCombined {len(combined)} records -> {combined_path}")

    # サマリー表示
    print(f"\n{'='*50}")
    print("SUMMARY")
    print(f"{'='*50}")
    from collections import Counter
    type_counts = Counter(r.get("type", "UNKNOWN") for r in combined)
    for t, c in type_counts.most_common():
        vol = sum(float(r.get("usdcSize", 0)) for r in combined if r.get("type") == t)
        print(f"  {t}: {c} records, ${vol:,.2f}")
    total_vol = sum(float(r.get("usdcSize", 0)) for r in combined)
    print(f"  TOTAL: {len(combined)} records, ${total_vol:,.2f}")


if __name__ == "__main__":
    main()
