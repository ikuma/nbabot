"""Fetch @lhtsports Polymarket trades in 2-3 batches using time-range (start/end).

API constraint: offset-based pagination returns at most ~3000 items per query.
Strategy: fetch newest batch (no end), then use end=<oldest_ts>-1 for older batches.

Usage:
  # Option A: Auto fetch all (2-3 batches with pauses)
  python scripts/fetch_lhtsports_trades.py --output data/reports/lhtsports-analysis/lhtsports_raw_trades.json

  # Option B: Manual batch (run 2-3 times, then merge)
  python scripts/fetch_lhtsports_trades.py --batch 1 --output data/reports/lhtsports-analysis/lhtsports_batch1.json
  python scripts/fetch_lhtsports_trades.py --batch 2 --end 1770052033 --output data/reports/lhtsports-analysis/lhtsports_batch2.json
  python scripts/fetch_lhtsports_trades.py --batch 3 --end <oldest_ts_from_batch2> --output ...
  python scripts/fetch_lhtsports_trades.py --merge batch1.json batch2.json batch3.json -o lhtsports_raw_trades.json

  # Option C: Already have ~3000 in lhtsports_raw_trades.json — fetch older batches then merge
  python scripts/fetch_lhtsports_trades.py --batch 2 --end 1770052033 -o data/reports/lhtsports-analysis/lhtsports_batch2.json
  python scripts/fetch_lhtsports_trades.py --batch 3 --end <oldest_ts> -o data/reports/lhtsports-analysis/lhtsports_batch3.json
  python scripts/fetch_lhtsports_trades.py --merge data/reports/lhtsports-analysis/lhtsports_raw_trades.json data/reports/lhtsports-analysis/lhtsports_batch2.json data/reports/lhtsports-analysis/lhtsports_batch3.json -o data/reports/lhtsports-analysis/lhtsports_raw_trades.json
"""

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional, Union

BASE_URL = "https://data-api.polymarket.com/activity"
USER = "0xa6a856a8c8a7f14fd9be6ae11c367c7cbb755009"
LIMIT = 500  # API max per request (docs: 0 <= limit <= 500)
MAX_OFFSET = 10000  # API cap; we stop when empty or offset hits this
SLEEP_SEC = 0.4  # Rate limiting

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
}


def fetch_page(offset: int, end_ts: Optional[int] = None) -> list[dict]:
    """Fetch one page of activity. Uses end (exclusive) to get older data."""
    params: dict[str, Union[str, int]] = {
        "user": USER,
        "limit": LIMIT,
        "offset": offset,
        "type": "TRADE",
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
        print(f"  Error offset={offset} end={end_ts}: {e}", file=sys.stderr)
        return []


def fetch_batch(end_ts: Optional[int] = None, max_items: int = 3500) -> list[dict]:
    """Fetch one batch (newest first). If end_ts set, only items with timestamp < end_ts."""
    batch: list[dict] = []
    offset = 0
    empty_streak = 0

    while offset <= MAX_OFFSET and empty_streak < 3:
        page = fetch_page(offset, end_ts)
        if not page:
            empty_streak += 1
            offset += LIMIT
            time.sleep(SLEEP_SEC)
            continue
        empty_streak = 0
        batch.extend(page)
        offset += LIMIT
        time.sleep(SLEEP_SEC)
        if len(batch) >= max_items:
            break
        if len(page) < LIMIT:
            break

    return batch


def run_auto_fetch(output_path: Path) -> None:
    """Fetch all trades in 2-3 batches by time range and merge."""
    all_trades: list[dict] = []
    end_ts: Optional[int] = None
    batch_num = 0

    while True:
        batch_num += 1
        print(f"Batch {batch_num}: fetching (end_ts={end_ts})...")
        batch = fetch_batch(end_ts=end_ts)
        if not batch:
            print(f"  No more data.")
            break
        min_ts = min(t["timestamp"] for t in batch)
        print(f"  Got {len(batch)} trades, oldest ts={min_ts}")
        all_trades.extend(batch)
        if len(batch) < LIMIT:
            # Last page was partial → no more in this range
            break
        end_ts = min_ts - 1
        if batch_num >= 50:
            print("  Stopping after 50 batches (safety).")
            break

    # Dedupe by (timestamp, transactionHash, asset, size) to be safe
    seen: set[tuple[int, str, str, float]] = set()
    unique: list[dict] = []
    for t in all_trades:
        key = (t["timestamp"], t.get("transactionHash", ""), t.get("asset", ""), t.get("size", 0))
        if key in seen:
            continue
        seen.add(key)
        unique.append(t)

    unique.sort(key=lambda x: (x["timestamp"], x.get("transactionHash", "")))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(unique, f, indent=2)
    print(f"\nTotal: {len(unique)} trades -> {output_path}")


def run_single_batch(batch_num: int, end_ts: Optional[int], output_path: Path) -> None:
    """Fetch one batch and save. Print hint for next batch."""
    print(f"Batch {batch_num}: end_ts={end_ts}...")
    batch = fetch_batch(end_ts=end_ts)
    if not batch:
        print("  No data.")
        return
    min_ts = min(t["timestamp"] for t in batch)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(batch, f, indent=2)
    print(f"  Saved {len(batch)} trades to {output_path}")
    print(f"  Oldest timestamp in this batch: {min_ts}")
    print(f"  Next run: python scripts/fetch_lhtsports_trades.py --batch {batch_num + 1} --end {min_ts - 1} --output <path>")


def run_merge(input_globs: list[str], output_path: Path) -> None:
    """Merge multiple batch JSON files (or paths), dedupe, sort, write output."""
    all_trades: list[dict] = []
    collected: set[Path] = set()
    for pattern in input_globs:
        p = Path(pattern)
        if p.is_file():
            if p in collected:
                continue
            collected.add(p)
            with open(p) as f:
                data = json.load(f)
            all_trades.extend(data if isinstance(data, list) else [data])
            continue
        for p in Path(".").glob(pattern):
            if not p.is_file() or p in collected:
                continue
            collected.add(p)
            with open(p) as f:
                data = json.load(f)
            all_trades.extend(data if isinstance(data, list) else [data])
    seen: set[tuple[int, str, str, float]] = set()
    unique: list[dict] = []
    for t in all_trades:
        key = (t["timestamp"], t.get("transactionHash", ""), t.get("asset", ""), t.get("size", 0))
        if key in seen:
            continue
        seen.add(key)
        unique.append(t)
    unique.sort(key=lambda x: (x["timestamp"], x.get("transactionHash", "")))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(unique, f, indent=2)
    print(f"Merged {len(unique)} unique trades -> {output_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch lhtsports Polymarket trades in batches")
    ap.add_argument("--output", "-o", type=Path, help="Output JSON path")
    ap.add_argument("--batch", type=int, default=0, help="Manual batch number (1, 2, 3...). Omit for auto.")
    ap.add_argument("--end", type=int, default=None, help="Only fetch trades with timestamp < end (for batch 2,3)")
    ap.add_argument("--merge", nargs="+", metavar="GLOB", help="Merge files matching GLOB(s) and write --output")
    args = ap.parse_args()

    if args.merge:
        if not args.output:
            print("--merge requires --output", file=sys.stderr)
            sys.exit(1)
        run_merge(args.merge, args.output)
        return

    if not args.output:
        args.output = Path("data/reports/lhtsports-analysis/lhtsports_raw_trades.json")

    if args.batch:
        run_single_batch(args.batch, args.end, args.output)
    else:
        run_auto_fetch(args.output)


if __name__ == "__main__":
    main()
