"""Tests for DCA-related DB operations (src/store/db.py)."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.store.db import (
    _connect,
    get_dca_active_jobs,
    get_dca_group_signals,
    log_signal,
    update_dca_job,
    upsert_trade_job,
)


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test_dca.db"


def _insert_signal(db_path: Path, **overrides) -> int:
    defaults = {
        "game_title": "Knicks vs Celtics",
        "event_slug": "nba-nyk-bos-2026-02-08",
        "team": "Celtics",
        "side": "BUY",
        "poly_price": 0.40,
        "book_prob": 0.6,
        "edge_pct": 5.0,
        "kelly_size": 25.0,
        "token_id": "tok123",
        "db_path": db_path,
    }
    defaults.update(overrides)
    return log_signal(**defaults)


def _insert_job(db_path: Path, **overrides) -> None:
    defaults = {
        "game_date": "2026-02-10",
        "event_slug": "nba-nyk-bos-2026-02-10",
        "home_team": "Boston Celtics",
        "away_team": "New York Knicks",
        "game_time_utc": "2026-02-11T01:00:00+00:00",
        "execute_after": "2026-02-10T17:00:00+00:00",
        "execute_before": "2026-02-11T01:00:00+00:00",
        "db_path": db_path,
    }
    defaults.update(overrides)
    upsert_trade_job(**defaults)


class TestDcaColumns:
    def test_signals_have_dca_columns(self, db_path: Path):
        sid = _insert_signal(db_path, dca_group_id="grp-1", dca_sequence=2)
        conn = _connect(db_path)
        row = conn.execute(
            "SELECT dca_group_id, dca_sequence FROM signals WHERE id = ?", (sid,)
        ).fetchone()
        conn.close()
        assert row[0] == "grp-1"
        assert row[1] == 2

    def test_signals_dca_defaults(self, db_path: Path):
        sid = _insert_signal(db_path)
        conn = _connect(db_path)
        row = conn.execute(
            "SELECT dca_group_id, dca_sequence FROM signals WHERE id = ?", (sid,)
        ).fetchone()
        conn.close()
        assert row[0] is None
        assert row[1] == 1

    def test_trade_jobs_have_dca_columns(self, db_path: Path):
        _insert_job(db_path)
        conn = _connect(db_path)
        row = conn.execute(
            "SELECT dca_entries_count, dca_max_entries, dca_group_id FROM trade_jobs LIMIT 1",
        ).fetchone()
        conn.close()
        assert row[0] == 0  # default
        assert row[1] == 1  # default
        assert row[2] is None  # default

    def test_trade_jobs_have_budget_columns(self, db_path: Path):
        _insert_job(db_path)
        conn = _connect(db_path)
        row = conn.execute(
            "SELECT dca_total_budget, dca_slice_size FROM trade_jobs LIMIT 1",
        ).fetchone()
        conn.close()
        assert row[0] is None  # default
        assert row[1] is None  # default


class TestGetDcaActiveJobs:
    def test_returns_dca_active_jobs(self, db_path: Path):
        _insert_job(db_path)
        # Set job to dca_active
        conn = _connect(db_path)
        conn.execute(
            "UPDATE trade_jobs SET status='dca_active', dca_entries_count=1, dca_max_entries=5, dca_group_id='grp-1'",
        )
        conn.commit()
        conn.close()

        jobs = get_dca_active_jobs("2026-02-10T18:00:00+00:00", db_path=db_path)
        assert len(jobs) == 1
        assert jobs[0].status == "dca_active"
        assert jobs[0].dca_group_id == "grp-1"

    def test_excludes_max_reached(self, db_path: Path):
        _insert_job(db_path)
        conn = _connect(db_path)
        conn.execute(
            "UPDATE trade_jobs SET status='dca_active', dca_entries_count=5, dca_max_entries=5",
        )
        conn.commit()
        conn.close()

        jobs = get_dca_active_jobs("2026-02-10T18:00:00+00:00", db_path=db_path)
        assert len(jobs) == 0

    def test_excludes_past_window(self, db_path: Path):
        _insert_job(db_path)
        conn = _connect(db_path)
        conn.execute(
            "UPDATE trade_jobs SET status='dca_active', dca_entries_count=1, dca_max_entries=5",
        )
        conn.commit()
        conn.close()

        # now > execute_before (game already started)
        jobs = get_dca_active_jobs("2026-02-11T02:00:00+00:00", db_path=db_path)
        assert len(jobs) == 0


class TestGetDcaGroupSignals:
    def test_returns_signals_in_order(self, db_path: Path):
        _insert_signal(db_path, dca_group_id="grp-1", dca_sequence=1, poly_price=0.40)
        _insert_signal(db_path, dca_group_id="grp-1", dca_sequence=2, poly_price=0.38)
        _insert_signal(db_path, dca_group_id="grp-2", dca_sequence=1, poly_price=0.50)

        signals = get_dca_group_signals("grp-1", db_path=db_path)
        assert len(signals) == 2
        assert signals[0].dca_sequence == 1
        assert signals[1].dca_sequence == 2

    def test_empty_group(self, db_path: Path):
        _connect(db_path).close()
        signals = get_dca_group_signals("nonexistent", db_path=db_path)
        assert signals == []


class TestUpdateDcaJob:
    def test_updates_dca_fields(self, db_path: Path):
        _insert_job(db_path)
        conn = _connect(db_path)
        job_id = conn.execute("SELECT id FROM trade_jobs LIMIT 1").fetchone()[0]
        conn.close()

        update_dca_job(
            job_id,
            dca_entries_count=3,
            dca_max_entries=5,
            dca_group_id="grp-99",
            status="dca_active",
            db_path=db_path,
        )

        conn = _connect(db_path)
        row = conn.execute(
            "SELECT dca_entries_count, dca_max_entries, dca_group_id, status FROM trade_jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
        conn.close()
        assert row[0] == 3
        assert row[1] == 5
        assert row[2] == "grp-99"
        assert row[3] == "dca_active"

    def test_update_dca_job_with_budget(self, db_path: Path):
        _insert_job(db_path)
        conn = _connect(db_path)
        job_id = conn.execute("SELECT id FROM trade_jobs LIMIT 1").fetchone()[0]
        conn.close()

        update_dca_job(
            job_id,
            dca_entries_count=1,
            dca_max_entries=5,
            dca_group_id="grp-budget",
            dca_total_budget=125.0,
            dca_slice_size=25.0,
            status="dca_active",
            db_path=db_path,
        )

        conn = _connect(db_path)
        row = conn.execute(
            "SELECT dca_total_budget, dca_slice_size FROM trade_jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
        conn.close()
        assert row[0] == 125.0
        assert row[1] == 25.0
