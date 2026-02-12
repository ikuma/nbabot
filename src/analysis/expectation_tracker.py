"""Expected vs Realized P&L tracker.

Computes the gap between calibration-table-predicted EV and actual P&L,
aggregated by month or week. Detects whether the edge is holding or decaying.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.store.models import ResultRecord, SignalRecord


@dataclass(frozen=True)
class ExpectationGap:
    """Monthly/weekly gap between expected and realized P&L."""

    period: str           # YYYY-MM or YYYY-Www
    expected_pnl: float   # sum(ev_per_dollar * kelly_size)
    realized_pnl: float   # sum(actual pnl)
    gap_usd: float        # realized - expected
    gap_pct: float        # gap / |expected| * 100
    n_signals: int


def compute_expectation_gaps(
    results_with_signals: list[tuple[ResultRecord, SignalRecord]],
    period: str = "monthly",
) -> list[ExpectationGap]:
    """Compute expected EV vs realized P&L gaps per period.

    Expected P&L for each signal:
        ev_per_dollar = expected_win_rate / poly_price - 1
        expected_pnl = ev_per_dollar * kelly_size

    Args:
        results_with_signals: List of (ResultRecord, SignalRecord) pairs.
        period: "monthly" (YYYY-MM) or "weekly" (YYYY-Www).

    Returns:
        List of ExpectationGap sorted by period.
    """
    buckets: dict[str, dict] = defaultdict(
        lambda: {"expected": 0.0, "realized": 0.0, "count": 0}
    )

    for result, signal in results_with_signals:
        # Derive expected EV from calibration fields
        expected_wr = signal.expected_win_rate
        price = signal.poly_price

        if not expected_wr or expected_wr <= 0 or price <= 0:
            continue

        ev_per_dollar = expected_wr / price - 1.0
        if ev_per_dollar <= 0:
            continue

        expected_pnl = ev_per_dollar * signal.kelly_size
        realized_pnl = result.pnl

        # Period key
        settled_date = result.settled_at[:10]  # YYYY-MM-DD
        if period == "weekly":
            from datetime import datetime
            try:
                dt = datetime.strptime(settled_date, "%Y-%m-%d")
                key = dt.strftime("%Y-W%W")
            except ValueError:
                key = settled_date[:7]
        else:
            key = settled_date[:7]  # YYYY-MM

        buckets[key]["expected"] += expected_pnl
        buckets[key]["realized"] += realized_pnl
        buckets[key]["count"] += 1

    gaps: list[ExpectationGap] = []
    for key in sorted(buckets):
        b = buckets[key]
        gap = b["realized"] - b["expected"]
        gap_pct = gap / abs(b["expected"]) * 100 if b["expected"] != 0 else 0.0

        gaps.append(ExpectationGap(
            period=key,
            expected_pnl=round(b["expected"], 2),
            realized_pnl=round(b["realized"], 2),
            gap_usd=round(gap, 2),
            gap_pct=round(gap_pct, 1),
            n_signals=b["count"],
        ))

    return gaps


def format_expectation_report(gaps: list[ExpectationGap]) -> list[str]:
    """Format expectation gaps as Markdown lines for report inclusion.

    Includes a warning if recent gaps show widening negative trend (edge decay).
    """
    if not gaps:
        return []

    out: list[str] = []
    out.append("### Expected vs Realized PnL")
    out.append("")
    out.append(
        "| Period | Signals | Expected | Realized | Gap ($) | Gap (%) |"
    )
    out.append(
        "|--------|---------|----------|----------|---------|---------|"
    )
    for g in gaps:
        out.append(
            f"| {g.period} | {g.n_signals} | ${g.expected_pnl:,.2f} "
            f"| ${g.realized_pnl:,.2f} | ${g.gap_usd:+,.2f} | {g.gap_pct:+.1f}% |"
        )
    out.append("")

    # 直近3期間で乖離が拡大傾向にある場合に警告
    if len(gaps) >= 3:
        recent = gaps[-3:]
        # gap_pct が連続で悪化 (より負に) しているか
        if all(
            recent[i].gap_pct < recent[i - 1].gap_pct
            for i in range(1, len(recent))
        ) and recent[-1].gap_pct < -10:
            out.append(
                "> **WARNING**: Edge may be decaying — "
                f"gap has widened for {len(recent)} consecutive periods "
                f"(latest: {recent[-1].gap_pct:+.1f}%)."
            )
            out.append("")

    return out
