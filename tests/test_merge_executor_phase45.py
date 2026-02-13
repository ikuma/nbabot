"""Tests for Phase 4/5 merge executor helper behavior."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

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


def test_live_merge_executes_and_updates_db(tmp_path, monkeypatch):
    db_path = tmp_path / "test_merge_live_execute.db"
    now = datetime.now(timezone.utc)
    slug = "nba-lal-bos-2026-02-10"
    bothside_group_id = "bs-live-merge"

    upsert_trade_job(
        game_date="2026-02-10",
        event_slug=slug,
        home_team="Boston Celtics",
        away_team="Los Angeles Lakers",
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
        away_team="Los Angeles Lakers",
        game_time_utc=(now + timedelta(hours=2)).isoformat(),
        execute_after=(now - timedelta(minutes=30)).isoformat(),
        execute_before=(now + timedelta(hours=2)).isoformat(),
        bothside_group_id=bothside_group_id,
        db_path=db_path,
    )
    assert hedge_id is not None

    update_job_status(dir_id, "executed", db_path=db_path)
    update_job_status(int(hedge_id), "executed", db_path=db_path)
    update_job_bothside(
        dir_id,
        bothside_group_id=bothside_group_id,
        paired_job_id=int(hedge_id),
        db_path=db_path,
    )

    dir_signal_id = log_signal(
        game_title="Lakers vs Celtics",
        event_slug=slug,
        team="Celtics",
        side="BUY",
        poly_price=0.45,
        book_prob=0.0,
        edge_pct=3.0,
        kelly_size=45.0,
        token_id="tok_dir",
        signal_role="directional",
        bothside_group_id=bothside_group_id,
        condition_id="0xcond-live",
        db_path=db_path,
    )
    hedge_signal_id = log_signal(
        game_title="Lakers vs Celtics",
        event_slug=slug,
        team="Lakers",
        side="BUY",
        poly_price=0.50,
        book_prob=0.0,
        edge_pct=2.0,
        kelly_size=50.0,
        token_id="tok_hedge",
        signal_role="hedge",
        bothside_group_id=bothside_group_id,
        condition_id="0xcond-live",
        db_path=db_path,
    )
    update_order_status(dir_signal_id, "ord-dir", "filled", fill_price=0.45, db_path=db_path)
    update_order_status(hedge_signal_id, "ord-hedge", "filled", fill_price=0.50, db_path=db_path)

    monkeypatch.setattr("src.scheduler.merge_executor.settings.merge_enabled", True)
    monkeypatch.setattr("src.scheduler.merge_executor.settings.merge_early_partial_enabled", False)
    monkeypatch.setattr("src.scheduler.merge_executor.settings.polymarket_signature_type", 1)
    monkeypatch.setattr("src.scheduler.merge_executor.settings.merge_max_combined_vwap", 0.99)
    monkeypatch.setattr("src.scheduler.merge_executor.settings.merge_min_profit_usd", 0.01)
    monkeypatch.setattr("src.connectors.ctf.estimate_merge_gas", lambda *_: 0.0)
    monkeypatch.setattr("src.connectors.ctf.get_matic_usd_price", lambda: 1.0)
    monkeypatch.setattr(
        "src.connectors.ctf.merge_positions_via_safe",
        lambda *_: SimpleNamespace(
            success=True,
            tx_hash="0xmerge-live",
            gas_cost_usd=0.02,
            error=None,
        ),
    )

    results = process_merge_eligible(execution_mode="live", db_path=str(db_path))
    assert len(results) == 1
    assert results[0].status == "executed"

    conn = _connect(db_path)
    merge_row = conn.execute(
        "SELECT status, tx_hash, gas_cost_usd FROM merge_operations WHERE bothside_group_id = ?",
        (bothside_group_id,),
    ).fetchone()
    job_rows = conn.execute(
        "SELECT id, merge_status, merge_operation_id FROM trade_jobs WHERE id IN (?, ?)",
        (dir_id, int(hedge_id)),
    ).fetchall()
    conn.close()

    assert merge_row is not None
    assert merge_row["status"] == "executed"
    assert merge_row["tx_hash"] == "0xmerge-live"
    assert merge_row["gas_cost_usd"] == 0.02
    assert len(job_rows) == 2
    assert all(r["merge_status"] == "executed" for r in job_rows)
    assert all(r["merge_operation_id"] is not None for r in job_rows)


def test_paper_merge_simulates_and_marks_jobs_executed(tmp_path, monkeypatch):
    db_path = tmp_path / "test_merge_paper_execute.db"
    now = datetime.now(timezone.utc)
    slug = "nba-lal-bos-2026-02-10"
    bothside_group_id = "bs-paper-merge"

    upsert_trade_job(
        game_date="2026-02-10",
        event_slug=slug,
        home_team="Boston Celtics",
        away_team="Los Angeles Lakers",
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
        away_team="Los Angeles Lakers",
        game_time_utc=(now + timedelta(hours=2)).isoformat(),
        execute_after=(now - timedelta(minutes=30)).isoformat(),
        execute_before=(now + timedelta(hours=2)).isoformat(),
        bothside_group_id=bothside_group_id,
        db_path=db_path,
    )
    assert hedge_id is not None

    update_job_status(dir_id, "executed", db_path=db_path)
    update_job_status(int(hedge_id), "executed", db_path=db_path)
    update_job_bothside(
        dir_id,
        bothside_group_id=bothside_group_id,
        paired_job_id=int(hedge_id),
        db_path=db_path,
    )

    dir_signal_id = log_signal(
        game_title="Lakers vs Celtics",
        event_slug=slug,
        team="Celtics",
        side="BUY",
        poly_price=0.45,
        book_prob=0.0,
        edge_pct=3.0,
        kelly_size=45.0,
        token_id="tok_dir",
        signal_role="directional",
        bothside_group_id=bothside_group_id,
        condition_id="0xcond-paper",
        db_path=db_path,
    )
    hedge_signal_id = log_signal(
        game_title="Lakers vs Celtics",
        event_slug=slug,
        team="Lakers",
        side="BUY",
        poly_price=0.50,
        book_prob=0.0,
        edge_pct=2.0,
        kelly_size=50.0,
        token_id="tok_hedge",
        signal_role="hedge",
        bothside_group_id=bothside_group_id,
        condition_id="0xcond-paper",
        db_path=db_path,
    )
    update_order_status(dir_signal_id, "ord-dir", "filled", fill_price=0.45, db_path=db_path)
    update_order_status(hedge_signal_id, "ord-hedge", "filled", fill_price=0.50, db_path=db_path)

    monkeypatch.setattr("src.scheduler.merge_executor.settings.merge_enabled", True)
    monkeypatch.setattr("src.scheduler.merge_executor.settings.merge_early_partial_enabled", False)
    monkeypatch.setattr("src.scheduler.merge_executor.settings.polymarket_signature_type", 1)
    monkeypatch.setattr("src.scheduler.merge_executor.settings.merge_max_combined_vwap", 0.99)
    monkeypatch.setattr("src.scheduler.merge_executor.settings.merge_min_profit_usd", 0.01)

    results = process_merge_eligible(execution_mode="paper", db_path=str(db_path))
    assert len(results) == 1
    assert results[0].status == "executed"

    conn = _connect(db_path)
    merge_row = conn.execute(
        "SELECT status, tx_hash FROM merge_operations WHERE bothside_group_id = ?",
        (bothside_group_id,),
    ).fetchone()
    job_rows = conn.execute(
        "SELECT id, merge_status, merge_operation_id FROM trade_jobs WHERE id IN (?, ?)",
        (dir_id, int(hedge_id)),
    ).fetchall()
    conn.close()

    assert merge_row is not None
    assert merge_row["status"] == "simulated"
    assert merge_row["tx_hash"] == "simulated"
    assert len(job_rows) == 2
    assert all(r["merge_status"] == "executed" for r in job_rows)
    assert all(r["merge_operation_id"] is not None for r in job_rows)
