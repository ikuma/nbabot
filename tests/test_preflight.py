"""Tests for preflight checks (pending DCA exposure)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.store.db import get_pending_dca_exposure
from src.store.schema import _connect


@pytest.fixture()
def tmp_db(tmp_path: Path):
    """Create a temporary DB with trade_jobs schema."""
    db_path = tmp_path / "test.db"
    conn = _connect(db_path)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS trade_jobs (
            id INTEGER PRIMARY KEY,
            game_date TEXT,
            event_slug TEXT,
            home_team TEXT,
            away_team TEXT,
            game_time_utc TEXT,
            execute_after TEXT,
            execute_before TEXT,
            status TEXT DEFAULT 'pending',
            retry_count INTEGER DEFAULT 0,
            signal_id INTEGER,
            error_message TEXT,
            job_side TEXT DEFAULT 'directional',
            paired_job_id INTEGER,
            bothside_group_id TEXT,
            merge_status TEXT DEFAULT 'none',
            merge_operation_id INTEGER,
            dca_group_id TEXT,
            dca_entries_count INTEGER DEFAULT 0,
            dca_max_entries INTEGER DEFAULT 1,
            dca_total_budget REAL DEFAULT 0,
            dca_slice_size REAL DEFAULT 0,
            created_at TEXT,
            updated_at TEXT,
            UNIQUE(event_slug, job_side)
        )"""
    )
    conn.commit()
    conn.close()
    return db_path


def _insert_job(
    db_path: Path,
    *,
    status: str = "dca_active",
    dca_entries_count: int = 1,
    dca_max_entries: int = 5,
    dca_slice_size: float = 20.0,
    event_slug: str = "nba-nyk-bos-2026-02-10",
    job_side: str = "directional",
):
    conn = _connect(db_path)
    conn.execute(
        """INSERT INTO trade_jobs
           (game_date, event_slug, home_team, away_team, game_time_utc,
            execute_after, execute_before, status, retry_count, job_side,
            dca_entries_count, dca_max_entries, dca_slice_size,
            created_at, updated_at)
           VALUES ('2026-02-10', ?, 'BOS', 'NYK', '2026-02-10T20:00:00Z',
                   '2026-02-10T12:00:00Z', '2026-02-10T20:00:00Z',
                   ?, 0, ?, ?, ?, ?, '2026-02-10T12:00:00Z', '2026-02-10T12:00:00Z')""",
        (event_slug, status, job_side, dca_entries_count, dca_max_entries, dca_slice_size),
    )
    conn.commit()
    conn.close()


class TestGetPendingDcaExposure:
    def test_no_dca_jobs(self, tmp_db):
        """No DCA jobs → 0 exposure."""
        assert get_pending_dca_exposure(tmp_db) == 0.0

    def test_single_dca_active_job(self, tmp_db):
        """1 job: 5 max, 1 done, $20/slice → (5-1)*20 = $80 pending."""
        _insert_job(tmp_db, dca_entries_count=1, dca_max_entries=5, dca_slice_size=20.0)
        assert get_pending_dca_exposure(tmp_db) == pytest.approx(80.0)

    def test_multiple_dca_active_jobs(self, tmp_db):
        """Multiple DCA jobs sum correctly."""
        _insert_job(
            tmp_db, dca_entries_count=2, dca_max_entries=5, dca_slice_size=20.0,
            event_slug="nba-game1",
        )
        _insert_job(
            tmp_db, dca_entries_count=1, dca_max_entries=3, dca_slice_size=15.0,
            event_slug="nba-game2",
        )
        # (5-2)*20 + (3-1)*15 = 60 + 30 = 90
        assert get_pending_dca_exposure(tmp_db) == pytest.approx(90.0)

    def test_ignores_non_dca_active_status(self, tmp_db):
        """Only status='dca_active' counts."""
        _insert_job(tmp_db, status="executed", dca_entries_count=1, dca_max_entries=5)
        assert get_pending_dca_exposure(tmp_db) == 0.0

    def test_ignores_zero_slice_size(self, tmp_db):
        """Jobs with dca_slice_size=0 are excluded."""
        _insert_job(tmp_db, dca_slice_size=0.0)
        assert get_pending_dca_exposure(tmp_db) == 0.0

    def test_fully_filled_dca(self, tmp_db):
        """entries == max → 0 pending."""
        _insert_job(tmp_db, dca_entries_count=5, dca_max_entries=5, dca_slice_size=20.0)
        assert get_pending_dca_exposure(tmp_db) == 0.0
