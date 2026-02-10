"""Tests for MERGE database operations (Phase B2)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.store.db import (
    _connect,
    get_merge_eligible_groups,
    get_merge_operation,
    log_merge_operation,
    log_signal,
    update_job_bothside,
    update_job_merge_status,
    update_job_status,
    update_merge_operation,
    upsert_hedge_job,
    upsert_trade_job,
)


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "test.db")


class TestConditionIdColumn:
    def test_condition_id_saved(self, db_path):
        """condition_id should be persisted in signals table."""
        sig_id = log_signal(
            game_title="Test Game",
            event_slug="nba-nyk-bos-2026-02-10",
            team="Celtics",
            side="BUY",
            poly_price=0.40,
            book_prob=0.6,
            edge_pct=5.0,
            kelly_size=25.0,
            token_id="tok123",
            condition_id="0xabc123def456",
            db_path=db_path,
        )

        conn = _connect(db_path)
        row = conn.execute("SELECT condition_id FROM signals WHERE id = ?", (sig_id,)).fetchone()
        conn.close()
        assert row[0] == "0xabc123def456"

    def test_condition_id_default_none(self, db_path):
        """condition_id should default to None when not provided."""
        sig_id = log_signal(
            game_title="Test Game",
            event_slug="nba-nyk-bos-2026-02-10",
            team="Celtics",
            side="BUY",
            poly_price=0.40,
            book_prob=0.6,
            edge_pct=5.0,
            kelly_size=25.0,
            token_id="tok123",
            db_path=db_path,
        )

        conn = _connect(db_path)
        row = conn.execute("SELECT condition_id FROM signals WHERE id = ?", (sig_id,)).fetchone()
        conn.close()
        assert row[0] is None


class TestMergeOperationsTable:
    def test_log_and_get(self, db_path):
        """Log a merge operation and retrieve it."""
        merge_id = log_merge_operation(
            bothside_group_id="bs-123",
            condition_id="0xcond1",
            event_slug="nba-nyk-bos-2026-02-10",
            dir_shares=100.0,
            hedge_shares=80.0,
            merge_amount=80.0,
            remainder_shares=20.0,
            remainder_side="directional",
            dir_vwap=0.35,
            hedge_vwap=0.50,
            combined_vwap=0.85,
            gross_profit_usd=12.0,
            gas_cost_usd=0.01,
            net_profit_usd=11.99,
            status="pending",
            db_path=db_path,
        )

        op = get_merge_operation("bs-123", db_path=db_path)
        assert op is not None
        assert op.id == merge_id
        assert op.bothside_group_id == "bs-123"
        assert op.merge_amount == pytest.approx(80.0)
        assert op.combined_vwap == pytest.approx(0.85)
        assert op.status == "pending"
        assert op.remainder_side == "directional"

    def test_get_nonexistent(self, db_path):
        """Nonexistent group should return None."""
        # Force table creation
        _connect(db_path).close()
        op = get_merge_operation("nonexistent", db_path=db_path)
        assert op is None

    def test_update_merge_operation(self, db_path):
        """Update status and tx_hash."""
        merge_id = log_merge_operation(
            bothside_group_id="bs-456",
            condition_id="0xcond2",
            event_slug="nba-lal-gsw-2026-02-10",
            dir_shares=50.0,
            hedge_shares=50.0,
            merge_amount=50.0,
            remainder_shares=0.0,
            remainder_side=None,
            dir_vwap=0.40,
            hedge_vwap=0.45,
            combined_vwap=0.85,
            db_path=db_path,
        )

        update_merge_operation(
            merge_id,
            status="executed",
            tx_hash="0xdeadbeef",
            gas_cost_usd=0.005,
            net_profit_usd=7.495,
            db_path=db_path,
        )

        op = get_merge_operation("bs-456", db_path=db_path)
        assert op.status == "executed"
        assert op.tx_hash == "0xdeadbeef"
        assert op.executed_at is not None
        assert op.net_profit_usd == pytest.approx(7.495)


class TestMergeEligibleGroups:
    def _setup_bothside_jobs(self, db_path, bs_gid="bs-group-1"):
        """Set up a pair of directional+hedge jobs that are both executed."""
        upsert_trade_job(
            game_date="2026-02-10",
            event_slug="nba-nyk-bos-2026-02-10",
            home_team="Boston Celtics",
            away_team="New York Knicks",
            game_time_utc="2026-02-10T23:00:00+00:00",
            execute_after="2026-02-10T15:00:00+00:00",
            execute_before="2026-02-10T23:00:00+00:00",
            job_side="directional",
            db_path=db_path,
        )
        # Get dir job id
        conn = _connect(db_path)
        dir_row = conn.execute("SELECT id FROM trade_jobs WHERE job_side='directional'").fetchone()
        dir_id = dir_row[0]

        # Set dir job to executed with bothside_group_id
        update_job_status(dir_id, "executed", db_path=db_path)
        update_job_bothside(dir_id, bothside_group_id=bs_gid, db_path=db_path)

        # Create hedge job
        hedge_id = upsert_hedge_job(
            directional_job_id=dir_id,
            event_slug="nba-nyk-bos-2026-02-10",
            game_date="2026-02-10",
            home_team="Boston Celtics",
            away_team="New York Knicks",
            game_time_utc="2026-02-10T23:00:00+00:00",
            execute_after="2026-02-10T15:30:00+00:00",
            execute_before="2026-02-10T23:00:00+00:00",
            bothside_group_id=bs_gid,
            db_path=db_path,
        )
        update_job_status(hedge_id, "executed", db_path=db_path)
        conn.close()

        return dir_id, hedge_id

    def test_eligible_when_both_executed(self, db_path):
        """Both jobs executed + merge_status='none' → eligible."""
        dir_id, hedge_id = self._setup_bothside_jobs(db_path)

        eligible = get_merge_eligible_groups(db_path=db_path)
        assert len(eligible) == 1
        assert eligible[0][0] == "bs-group-1"
        assert eligible[0][1] == dir_id
        assert eligible[0][2] == hedge_id

    def test_not_eligible_when_merge_done(self, db_path):
        """merge_status='executed' → not eligible."""
        dir_id, hedge_id = self._setup_bothside_jobs(db_path)
        update_job_merge_status(dir_id, "executed", db_path=db_path)

        eligible = get_merge_eligible_groups(db_path=db_path)
        assert len(eligible) == 0

    def test_not_eligible_when_hedge_pending(self, db_path):
        """Hedge still pending → not eligible."""
        upsert_trade_job(
            game_date="2026-02-10",
            event_slug="nba-lal-gsw-2026-02-10",
            home_team="Golden State Warriors",
            away_team="Los Angeles Lakers",
            game_time_utc="2026-02-10T23:00:00+00:00",
            execute_after="2026-02-10T15:00:00+00:00",
            execute_before="2026-02-10T23:00:00+00:00",
            job_side="directional",
            db_path=db_path,
        )

        conn = _connect(db_path)
        dir_row = conn.execute(
            "SELECT id FROM trade_jobs WHERE event_slug='nba-lal-gsw-2026-02-10'"
        ).fetchone()
        dir_id = dir_row[0]
        conn.close()

        update_job_status(dir_id, "executed", db_path=db_path)
        update_job_bothside(dir_id, bothside_group_id="bs-group-2", db_path=db_path)

        upsert_hedge_job(
            directional_job_id=dir_id,
            event_slug="nba-lal-gsw-2026-02-10",
            game_date="2026-02-10",
            home_team="Golden State Warriors",
            away_team="Los Angeles Lakers",
            game_time_utc="2026-02-10T23:00:00+00:00",
            execute_after="2026-02-10T15:30:00+00:00",
            execute_before="2026-02-10T23:00:00+00:00",
            bothside_group_id="bs-group-2",
            db_path=db_path,
        )
        # hedge is still pending

        eligible = get_merge_eligible_groups(db_path=db_path)
        assert len(eligible) == 0


class TestMergeJobColumns:
    def test_merge_status_default(self, db_path):
        """New trade_jobs should have merge_status='none'."""
        upsert_trade_job(
            game_date="2026-02-10",
            event_slug="nba-nyk-bos-2026-02-10",
            home_team="Boston Celtics",
            away_team="New York Knicks",
            game_time_utc="2026-02-10T23:00:00+00:00",
            execute_after="2026-02-10T15:00:00+00:00",
            execute_before="2026-02-10T23:00:00+00:00",
            db_path=db_path,
        )

        conn = _connect(db_path)
        row = conn.execute("SELECT merge_status FROM trade_jobs").fetchone()
        conn.close()
        # Default from migration is 'none'
        assert row[0] in ("none", None)

    def test_update_merge_status(self, db_path):
        """update_job_merge_status should set fields correctly."""
        upsert_trade_job(
            game_date="2026-02-10",
            event_slug="nba-nyk-bos-2026-02-10",
            home_team="Boston Celtics",
            away_team="New York Knicks",
            game_time_utc="2026-02-10T23:00:00+00:00",
            execute_after="2026-02-10T15:00:00+00:00",
            execute_before="2026-02-10T23:00:00+00:00",
            db_path=db_path,
        )

        conn = _connect(db_path)
        row = conn.execute("SELECT id FROM trade_jobs").fetchone()
        job_id = row[0]
        conn.close()

        update_job_merge_status(job_id, "executed", merge_operation_id=42, db_path=db_path)

        conn = _connect(db_path)
        row = conn.execute(
            "SELECT merge_status, merge_operation_id FROM trade_jobs WHERE id = ?", (job_id,)
        ).fetchone()
        conn.close()
        assert row[0] == "executed"
        assert row[1] == 42
