#!/usr/bin/env python3
"""Paper-trade performance report generator.

Usage:
    python scripts/report_pnl.py           # Print to stdout
    python scripts/report_pnl.py --save    # Also save markdown report
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def generate_report(save: bool = False) -> str:
    """Generate paper-trade performance report as markdown."""
    from src.store.db import get_all_results, get_all_signals, get_performance

    stats = get_performance()
    signals = get_all_signals()
    results = get_all_results()

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines: list[str] = []

    lines.append("# Paper Trade Performance Report")
    lines.append(f"Generated: {now}\n")

    # --- Summary ---
    lines.append("## Summary\n")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Total signals | {stats.total_signals} |")
    lines.append(f"| Settled | {stats.settled_count} |")
    lines.append(f"| Unsettled | {stats.unsettled_count} |")
    lines.append(f"| Wins | {stats.wins} |")
    lines.append(f"| Losses | {stats.losses} |")
    lines.append(f"| Win rate | {stats.win_rate:.1%} |")
    lines.append(f"| Total PnL | ${stats.total_pnl:+.2f} |")
    lines.append(f"| Avg PnL/trade | ${stats.avg_pnl:+.2f} |")
    lines.append(f"| Max drawdown | ${stats.max_drawdown:.2f} |")
    lines.append(f"| Sharpe ratio | {stats.sharpe_ratio:.2f} |")
    lines.append("")

    # --- Phase 3 Readiness ---
    lines.append("## Phase 3 Readiness Checklist\n")
    check = lambda ok: "[x]" if ok else "[ ]"  # noqa: E731
    settled_ok = check(stats.settled_count >= 100)
    wr_ok = check(stats.win_rate > 0.55)
    sr_ok = check(stats.sharpe_ratio > 1.5)
    dd_ok = check(stats.max_drawdown < 10)
    lines.append(f"- {settled_ok} Settled trades >= 100 ({stats.settled_count}/100)")
    lines.append(f"- {wr_ok} Win rate > 55% ({stats.win_rate:.1%})")
    lines.append(f"- {sr_ok} Sharpe ratio > 1.5 ({stats.sharpe_ratio:.2f})")
    lines.append(f"- {dd_ok} Max drawdown < 10% (${stats.max_drawdown:.2f})")

    all_passed = (
        stats.settled_count >= 100
        and stats.win_rate > 0.55
        and stats.sharpe_ratio > 1.5
        and stats.max_drawdown < 10
    )
    lines.append("")
    if all_passed:
        lines.append("**READY for Phase 3 transition.**")
    else:
        lines.append("**Not ready for Phase 3.** Continue paper trading.")
    lines.append("")

    # --- Recent Signals ---
    lines.append("## Recent Signals (last 20)\n")
    if not signals:
        lines.append("No signals recorded yet.\n")
    else:
        lines.append("| # | Date | Game | Team | Edge% | Size$ | Result |")
        lines.append("|---|------|------|------|-------|-------|--------|")
        result_by_signal = {r.signal_id: r for r in results}
        for s in signals[:20]:
            date = s.created_at[:10]
            r = result_by_signal.get(s.id)
            if r:
                result_str = f"{'W' if r.won else 'L'} ${r.pnl:+.2f}"
            else:
                result_str = "pending"
            lines.append(
                f"| {s.id} | {date} | {s.game_title:.35s} | {s.team:.20s} "
                f"| {s.edge_pct:.1f}% | ${s.kelly_size:.0f} | {result_str} |"
            )
        lines.append("")

    report = "\n".join(lines)

    if save:
        report_dir = Path(__file__).resolve().parent.parent / "data" / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        path = report_dir / f"pnl-{today}.md"
        path.write_text(report)
        print(f"Report saved: {path}")

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Paper-trade performance report")
    parser.add_argument("--save", action="store_true", help="Save report as markdown file")
    args = parser.parse_args()

    report = generate_report(save=args.save)
    print(report)


if __name__ == "__main__":
    main()
