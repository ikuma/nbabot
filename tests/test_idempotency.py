"""Tests for idempotency guards and crash recovery (Phase D2)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.store.schema import _connect


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.db"
    conn = _connect(db_path)
    conn.close()
    return str(db_path)


def _insert_job(db_path, event_slug, job_side="directional", status="executing"):
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    cur = conn.execute(
        """INSERT INTO trade_jobs
           (game_date, event_slug, home_team, away_team, game_time_utc,
            execute_after, execute_before, status, job_side, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        ("2026-02-10", event_slug, "Celtics", "Knicks",
         "2026-02-10T20:00:00Z", "2026-02-10T12:00:00Z", "2026-02-10T20:00:00Z",
         status, job_side, now, now),
    )
    conn.commit()
    job_id = cur.lastrowid
    conn.close()
    return job_id


def _insert_signal(db_path, event_slug, signal_role="directional"):
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    cur = conn.execute(
        """INSERT INTO signals
           (game_title, event_slug, team, side, poly_price, book_prob,
            edge_pct, kelly_size, token_id, created_at, signal_role, order_status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        ("NYK vs BOS", event_slug, "Knicks", "BUY", 0.40, 0.90,
         10.0, 10.0, "token123", now, signal_role, "paper"),
    )
    conn.commit()
    signal_id = cur.lastrowid
    conn.close()
    return signal_id


class TestRecoverExecutingJobs:
    def test_recover_with_signal_directional(self, tmp_db):
        """Executing job with matching directional signal → executed."""
        from src.scheduler.trade_scheduler import recover_executing_jobs

        slug = "nba-nyk-bos-2026-02-10"
        _insert_job(tmp_db, slug, "directional", "executing")
        _insert_signal(tmp_db, slug, "directional")

        recovered = recover_executing_jobs(db_path=tmp_db)
        assert recovered == 1

        conn = _connect(tmp_db)
        row = conn.execute("SELECT status FROM trade_jobs").fetchone()
        conn.close()
        assert row["status"] == "executed"

    def test_recover_without_signal(self, tmp_db):
        """Executing job without signal → pending."""
        from src.scheduler.trade_scheduler import recover_executing_jobs

        _insert_job(tmp_db, "nba-nyk-bos-2026-02-10", "directional", "executing")

        recovered = recover_executing_jobs(db_path=tmp_db)
        assert recovered == 1

        conn = _connect(tmp_db)
        row = conn.execute("SELECT status FROM trade_jobs").fetchone()
        conn.close()
        assert row["status"] == "pending"

    def test_recover_hedge_with_hedge_signal(self, tmp_db):
        """Executing hedge job with matching hedge signal → executed."""
        from src.scheduler.trade_scheduler import recover_executing_jobs

        slug = "nba-nyk-bos-2026-02-10"
        _insert_job(tmp_db, slug, "hedge", "executing")
        _insert_signal(tmp_db, slug, "hedge")

        recovered = recover_executing_jobs(db_path=tmp_db)
        assert recovered == 1

        conn = _connect(tmp_db)
        row = conn.execute("SELECT status FROM trade_jobs").fetchone()
        conn.close()
        assert row["status"] == "executed"

    def test_recover_hedge_without_hedge_signal(self, tmp_db):
        """Executing hedge job with only directional signal → pending (no matching role)."""
        from src.scheduler.trade_scheduler import recover_executing_jobs

        slug = "nba-nyk-bos-2026-02-10"
        _insert_job(tmp_db, slug, "hedge", "executing")
        _insert_signal(tmp_db, slug, "directional")  # wrong role

        recovered = recover_executing_jobs(db_path=tmp_db)
        assert recovered == 1

        conn = _connect(tmp_db)
        row = conn.execute("SELECT status FROM trade_jobs").fetchone()
        conn.close()
        assert row["status"] == "pending"


class TestHasSignalForSlugAndSide:
    def test_directional_found(self, tmp_db):
        from src.store.db import has_signal_for_slug_and_side

        slug = "nba-nyk-bos-2026-02-10"
        _insert_signal(tmp_db, slug, "directional")
        assert has_signal_for_slug_and_side(slug, "directional", db_path=tmp_db) is True
        assert has_signal_for_slug_and_side(slug, "hedge", db_path=tmp_db) is False

    def test_hedge_found(self, tmp_db):
        from src.store.db import has_signal_for_slug_and_side

        slug = "nba-nyk-bos-2026-02-10"
        _insert_signal(tmp_db, slug, "hedge")
        assert has_signal_for_slug_and_side(slug, "hedge", db_path=tmp_db) is True
        assert has_signal_for_slug_and_side(slug, "directional", db_path=tmp_db) is False
