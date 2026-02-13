"""PositionGroup audit/risk report.

Usage:
  ./.venv/bin/python scripts/report_position_groups.py --db data/paper_trades.db
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.analysis.metrics import (  # noqa: E402
    compute_position_group_risk_metrics,
    format_position_group_risk_summary,
)
from src.store.db import (  # noqa: E402
    get_position_group_audit_events,
    get_position_group_risk_inputs,
)
from src.store.db_path import resolve_db_path  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Report PositionGroup audit/risk metrics")
    p.add_argument("--db", default="", help="SQLite DB path (optional override)")
    p.add_argument(
        "--execution",
        choices=["paper", "live", "dry-run"],
        default="paper",
        help="DB mode when --db is omitted",
    )
    p.add_argument("--event-slug", default="", help="Filter by one event slug")
    p.add_argument("--start-at", default="", help="ISO8601 start (inclusive)")
    p.add_argument("--end-at", default="", help="ISO8601 end (exclusive)")
    p.add_argument("--max-violation-rate", type=float, default=1.0, help="Percent threshold")
    p.add_argument("--max-violation-ratio", type=float, default=1.2, help="Times threshold")
    p.add_argument(
        "--fail-on-breach",
        action="store_true",
        help="Exit 1 when thresholds are breached",
    )
    return p


def main() -> int:
    args = _build_parser().parse_args()
    db_path = resolve_db_path(
        execution_mode=args.execution,
        explicit_db_path=args.db or None,
    )
    rows = get_position_group_risk_inputs(
        db_path=db_path,
        event_slug=args.event_slug or None,
        start_at=args.start_at or None,
        end_at=args.end_at or None,
    )
    metrics = compute_position_group_risk_metrics(rows)

    print("=== PositionGroup Risk Metrics ===")
    if args.event_slug:
        print(f"event_slug: {args.event_slug}")
    print(format_position_group_risk_summary(metrics))

    if args.event_slug:
        recent = get_position_group_audit_events(args.event_slug, limit=10, db_path=db_path)
        if recent:
            print("\nRecent audit events:")
            for ev in recent[-10:]:
                print(
                    f"- {ev.created_at} [{ev.audit_type}] {ev.prev_state}->{ev.new_state} "
                    f"reason={ev.reason} d={ev.d} d_max={ev.d_max} merge_amount={ev.merge_amount}"
                )
    else:
        per_event: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            slug = str(r.get("event_slug") or "")
            if slug:
                per_event[slug].append(r)
        if per_event:
            ranked = sorted(
                (
                    (
                        slug,
                        compute_position_group_risk_metrics(event_rows),
                    )
                    for slug, event_rows in per_event.items()
                ),
                key=lambda item: (item[1].violation_rate, item[1].max_violation_ratio),
                reverse=True,
            )
            print("\nTop events by violation rate:")
            for slug, m in ranked[:10]:
                print(
                    f"- {slug}: {m.violation_count}/{m.samples} "
                    f"({m.violation_rate*100:.2f}%), max_ratio={m.max_violation_ratio:.2f}x"
                )

    breach_rate = metrics.violation_rate * 100 > args.max_violation_rate
    breach_ratio = metrics.max_violation_ratio > args.max_violation_ratio
    if breach_rate or breach_ratio:
        print(
            "\nThreshold breach: "
            f"rate={metrics.violation_rate*100:.2f}% (limit={args.max_violation_rate:.2f}%), "
            f"max_ratio={metrics.max_violation_ratio:.2f}x "
            f"(limit={args.max_violation_ratio:.2f}x)"
        )
        if args.fail_on_breach:
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
