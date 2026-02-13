"""Tests for Phase 4/5 merge executor helper behavior."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.scheduler.merge_executor import (
    _estimate_capital_release_benefit_usd,
    _in_rollout_cohort,
    process_merge_eligible,
)
from src.store.db import (
    _connect,
    log_signal,
    update_job_bothside,
    update_job_status,
    update_order_status,
    upsert_hedge_job,
    upsert_trade_job,
)


def test_rollout_cohort_bounds():
    gid = "bs-rollout-test"
    assert _in_rollout_cohort(gid, 0) is False
    assert _in_rollout_cohort(gid, 100) is True


def test_rollout_cohort_deterministic():
    gid = "bs-rollout-deterministic"
    a = _in_rollout_cohort(gid, 25)
    b = _in_rollout_cohort(gid, 25)
    assert a == b


def test_capital_release_benefit_positive_for_future_tipoff():
    # Far-future execute_before guarantees positive horizon even in CI runtime.
    benefit = _estimate_capital_release_benefit_usd(
        merge_amount=100.0,
        combined_vwap=0.95,
        execute_before="2099-01-01T00:00:00+00:00",
    )
    assert benefit > 0


def test_capital_release_benefit_zero_for_zero_principal():
    benefit = _estimate_capital_release_benefit_usd(
        merge_amount=0.0,
        combined_vwap=0.95,
        execute_before="2099-01-01T00:00:00+00:00",
    )
    assert benefit == 0.0


def test_live_merge_skips_groups_with_unfilled_signals(tmp_path, monkeypatch):
    db_path = tmp_path / "test_merge_unfilled.db"
    now = datetime.now(timezone.utc)
    slug = "nba-nyk-bos-2026-02-10"
    bothside_group_id = "bs-unfilled"

    upsert_trade_job(
        game_date="2026-02-10",
        event_slug=slug,
        home_team="Boston Celtics",
        away_team="New York Knicks",
        game_time_utc=(now + timedelta(hours=2)).isoformat(),
        execute_after=(now - timedelta(hours=1)).isoformat(),
        execute_before=(now + timedelta(hours=2)).isoformat(),
        job_side="directional",
        db_path=db_path,
    )
    conn = _connect(db_path)
    dir_id = conn.execute(
        "SELECT id FROM trade_jobs WHERE event_slug = ? AND job_side = 'directional'",
        (slug,),
    ).fetchone()[0]
    conn.close()

    hedge_id = upsert_hedge_job(
        directional_job_id=dir_id,
        event_slug=slug,
        game_date="2026-02-10",
        home_team="Boston Celtics",
        away_team="New York Knicks",
        game_time_utc=(now + timedelta(hours=2)).isoformat(),
        execute_after=(now - timedelta(minutes=30)).isoformat(),
        execute_before=(now + timedelta(hours=2)).isoformat(),
        bothside_group_id=bothside_group_id,
        db_path=db_path,
    )

    update_job_status(dir_id, "executed", db_path=db_path)
    update_job_status(int(hedge_id), "executed", db_path=db_path)
    update_job_bothside(dir_id, bothside_group_id=bothside_group_id, db_path=db_path)

    dir_signal_id = log_signal(
        game_title="Knicks vs Celtics",
        event_slug=slug,
        team="Celtics",
        side="BUY",
        poly_price=0.45,
        book_prob=0.0,
        edge_pct=3.0,
        kelly_size=20.0,
        token_id="tok_dir",
        signal_role="directional",
        bothside_group_id=bothside_group_id,
        condition_id="0xcond",
        db_path=db_path,
    )
    hedge_signal_id = log_signal(
        game_title="Knicks vs Celtics",
        event_slug=slug,
        team="Knicks",
        side="BUY",
        poly_price=0.50,
        book_prob=0.0,
        edge_pct=2.0,
        kelly_size=20.0,
        token_id="tok_hedge",
        signal_role="hedge",
        bothside_group_id=bothside_group_id,
        condition_id="0xcond",
        db_path=db_path,
    )
    update_order_status(dir_signal_id, "ord-dir", "placed", db_path=db_path)
    update_order_status(hedge_signal_id, "ord-hedge", "placed", db_path=db_path)

    monkeypatch.setattr("src.scheduler.merge_executor.settings.merge_enabled", True)
    monkeypatch.setattr("src.scheduler.merge_executor.settings.merge_early_partial_enabled", False)

    results = process_merge_eligible(execution_mode="live", db_path=str(db_path))
    assert results == []

    conn = _connect(db_path)
    merge_count = conn.execute("SELECT COUNT(*) FROM merge_operations").fetchone()[0]
    conn.close()
    assert merge_count == 0
