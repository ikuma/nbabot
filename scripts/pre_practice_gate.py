#!/usr/bin/env python3
"""One-command pre-practice gate for external connectivity + scheduler smoke.

Runs:
1) NBA.com / Polymarket API connectivity checks
2) Scheduler dry-run tick
3) Scheduler paper tick (optional)
4) Focused regression tests (optional)

This script is intended to catch practical breakages before live trading.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

ET = ZoneInfo("America/New_York")
PYTHON = sys.executable

FOCUSED_TESTS = [
    "tests/test_settle.py",
    "tests/test_order_manager.py",
    "tests/test_merge_executor_phase45.py",
    "tests/test_hedge_executor.py",
    "tests/test_nba_schedule.py",
]


@dataclass
class StepResult:
    name: str
    ok: bool
    detail: str
    required: bool = True


def _run_cmd(name: str, cmd: list[str], required: bool = True) -> StepResult:
    try:
        proc = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
    except Exception as exc:
        return StepResult(name=name, ok=False, detail=f"exception: {exc}", required=required)

    if proc.returncode == 0:
        return StepResult(name=name, ok=True, detail="ok", required=required)
    detail = proc.stderr.strip() or proc.stdout.strip() or f"exit={proc.returncode}"
    return StepResult(name=name, ok=False, detail=detail, required=required)


def _check_connectivity(
    strict_no_games: bool,
    strict_no_markets: bool,
    require_auth: bool,
) -> list[StepResult]:
    from src.config import settings
    from src.connectors.nba_schedule import fetch_todays_games
    from src.connectors.polymarket import (
        fetch_moneyline_for_game,
        fetch_nba_markets,
        get_usdc_balance,
    )

    results: list[StepResult] = []

    # NBA scoreboard connectivity
    try:
        games = fetch_todays_games()
        if games:
            results.append(StepResult("NBA scoreboard", True, f"games={len(games)}"))
        else:
            results.append(
                StepResult(
                    "NBA scoreboard",
                    ok=not strict_no_games,
                    detail="no games returned",
                    required=strict_no_games,
                )
            )
    except Exception as exc:
        results.append(StepResult("NBA scoreboard", False, f"error: {exc}"))
        games = []

    # Public Polymarket connectivity (CLOB/Gamma path)
    try:
        markets = fetch_nba_markets()
        if markets:
            results.append(StepResult("Polymarket public", True, f"markets={len(markets)}"))
        else:
            results.append(
                StepResult(
                    "Polymarket public",
                    ok=not strict_no_markets,
                    detail="no NBA markets",
                    required=strict_no_markets,
                )
            )
    except Exception as exc:
        results.append(StepResult("Polymarket public", False, f"error: {exc}"))

    # Optional: game-specific moneyline lookup (helpful when games exist)
    if games:
        sample = games[0]
        try:
            utc_dt = datetime.fromisoformat(sample.game_time_utc.replace("Z", "+00:00"))
            game_date = utc_dt.astimezone(ET).strftime("%Y-%m-%d")
            ml = fetch_moneyline_for_game(sample.away_team, sample.home_team, game_date)
            if ml:
                results.append(StepResult("Moneyline lookup", True, "found market for sample game"))
            else:
                results.append(StepResult("Moneyline lookup", True, "no market for sample game"))
        except Exception as exc:
            # Informational only: one game miss should not gate deployment alone.
            results.append(StepResult("Moneyline lookup", False, f"error: {exc}", required=False))

    # Auth connectivity
    if settings.polymarket_private_key:
        try:
            balance = get_usdc_balance()
            results.append(StepResult("Polymarket auth", True, f"usdc_balance={balance:.2f}"))
        except Exception as exc:
            results.append(StepResult("Polymarket auth", False, f"error: {exc}", required=True))
    else:
        results.append(
            StepResult(
                "Polymarket auth",
                ok=not require_auth,
                detail="POLYMARKET_PRIVATE_KEY not set",
                required=require_auth,
            )
        )

    return results


def _print_section(title: str) -> None:
    print("")
    print(f"== {title} ==")


def _print_results(results: list[StepResult]) -> tuple[int, int]:
    required_failures = 0
    optional_failures = 0
    for r in results:
        marker = "PASS" if r.ok else ("FAIL" if r.required else "WARN")
        print(f"[{marker}] {r.name}: {r.detail}")
        if not r.ok:
            if r.required:
                required_failures += 1
            else:
                optional_failures += 1
    return required_failures, optional_failures


def main() -> int:
    parser = argparse.ArgumentParser(description="One-command pre-practice gate")
    parser.add_argument("--date", help="Scheduler test date YYYY-MM-DD")
    parser.add_argument(
        "--skip-tests",
        action="store_true",
        help="Skip focused regression tests",
    )
    parser.add_argument(
        "--skip-paper",
        action="store_true",
        help="Skip scheduler paper tick",
    )
    parser.add_argument(
        "--strict-no-games",
        action="store_true",
        help="Treat no NBA games today as failure",
    )
    parser.add_argument(
        "--strict-no-markets",
        action="store_true",
        help="Treat no Polymarket NBA markets as failure",
    )
    parser.add_argument(
        "--require-auth",
        action="store_true",
        help="Require authenticated Polymarket balance check",
    )
    args = parser.parse_args()

    required_failures = 0
    optional_failures = 0

    _print_section("Connectivity")
    conn_results = _check_connectivity(
        strict_no_games=args.strict_no_games,
        strict_no_markets=args.strict_no_markets,
        require_auth=args.require_auth,
    )
    req, opt = _print_results(conn_results)
    required_failures += req
    optional_failures += opt

    _print_section("Scheduler Smoke")
    dry_cmd = [PYTHON, "scripts/schedule_trades.py", "--execution", "dry-run", "--no-settle"]
    if args.date:
        dry_cmd.extend(["--date", args.date])
    dry_res = _run_cmd("schedule dry-run", dry_cmd, required=True)
    req, opt = _print_results([dry_res])
    required_failures += req
    optional_failures += opt

    if not args.skip_paper:
        paper_cmd = [PYTHON, "scripts/schedule_trades.py", "--execution", "paper", "--no-settle"]
        if args.date:
            paper_cmd.extend(["--date", args.date])
        paper_res = _run_cmd("schedule paper", paper_cmd, required=True)
        req, opt = _print_results([paper_res])
        required_failures += req
        optional_failures += opt

    if not args.skip_tests:
        _print_section("Focused Tests")
        test_cmd = [PYTHON, "-m", "pytest", "-q", *FOCUSED_TESTS]
        test_res = _run_cmd("focused pytest", test_cmd, required=True)
        req, opt = _print_results([test_res])
        required_failures += req
        optional_failures += opt

    _print_section("Summary")
    print(f"required_failures={required_failures}")
    print(f"optional_warnings={optional_failures}")

    if required_failures > 0:
        print("pre_practice_gate: FAILED")
        return 1
    print("pre_practice_gate: PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
