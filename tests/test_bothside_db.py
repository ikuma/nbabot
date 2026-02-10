"""Tests for both-side DB operations (Phase B)."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.store.db import (
    _connect,
    get_bothside_signals,
    get_hedge_job_for_slug,
    has_signal_for_slug_and_side,
    log_signal,
    update_job_bothside,
    upsert_hedge_job,
    upsert_trade_job,
)


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test_bothside.db"


def _insert_job(db_path: Path, job_side: str = "directional", **overrides) -> None:
    defaults = {
        "game_date": "2026-02-10",
        "event_slug": "nba-nyk-bos-2026-02-10",
        "home_team": "Boston Celtics",
        "away_team": "New York Knicks",
        "game_time_utc": "2026-02-11T01:00:00+00:00",
        "execute_after": "2026-02-10T17:00:00+00:00",
        "execute_before": "2026-02-11T01:00:00+00:00",
        "job_side": job_side,
        "db_path": db_path,
    }
    defaults.update(overrides)
    upsert_trade_job(**defaults)


def _insert_signal(db_path: Path, **overrides) -> int:
    defaults = {
        "game_title": "Knicks vs Celtics",
        "event_slug": "nba-nyk-bos-2026-02-10",
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


class TestUniqueConstraint:
    def test_unique_allows_two_sides(self, db_path: Path):
        """Same slug can have directional + hedge jobs."""
        _insert_job(db_path, job_side="directional")
        _insert_job(db_path, job_side="hedge")

        conn = _connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM trade_jobs").fetchone()[0]
        conn.close()
        assert count == 2

    def test_unique_blocks_duplicate_side(self, db_path: Path):
        """Same slug + same side is rejected (INSERT OR IGNORE)."""
        _insert_job(db_path, job_side="directional")
        _insert_job(db_path, job_side="directional")

        conn = _connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM trade_jobs").fetchone()[0]
        conn.close()
        assert count == 1


class TestMigrationPreservesData:
    def test_migration_preserves_data(self, db_path: Path):
        """Existing data survives the UNIQUE constraint migration."""
        _insert_job(db_path, event_slug="nba-game-1-2026-02-10")
        _insert_job(db_path, event_slug="nba-game-2-2026-02-10")

        conn = _connect(db_path)
        jobs = conn.execute(
            "SELECT event_slug, job_side FROM trade_jobs ORDER BY event_slug"
        ).fetchall()
        conn.close()
        assert len(jobs) == 2
        assert jobs[0]["event_slug"] == "nba-game-1-2026-02-10"
        assert jobs[0]["job_side"] == "directional"
        assert jobs[1]["event_slug"] == "nba-game-2-2026-02-10"
        assert jobs[1]["job_side"] == "directional"


class TestUpsertHedgeJob:
    def test_creates_hedge_job(self, db_path: Path):
        _insert_job(db_path, job_side="directional")
        conn = _connect(db_path)
        dir_id = conn.execute("SELECT id FROM trade_jobs LIMIT 1").fetchone()[0]
        conn.close()

        job_id = upsert_hedge_job(
            directional_job_id=dir_id,
            event_slug="nba-nyk-bos-2026-02-10",
            game_date="2026-02-10",
            home_team="Boston Celtics",
            away_team="New York Knicks",
            game_time_utc="2026-02-11T01:00:00+00:00",
            execute_after="2026-02-10T17:30:00+00:00",
            execute_before="2026-02-11T01:00:00+00:00",
            bothside_group_id="bs-group-1",
            db_path=db_path,
        )
        assert job_id is not None

        conn = _connect(db_path)
        hedge = conn.execute("SELECT * FROM trade_jobs WHERE job_side = 'hedge'").fetchone()
        conn.close()
        assert hedge["paired_job_id"] == dir_id
        assert hedge["bothside_group_id"] == "bs-group-1"

    def test_idempotent(self, db_path: Path):
        """Second upsert_hedge_job with same slug returns None."""
        _insert_job(db_path, job_side="directional")
        conn = _connect(db_path)
        dir_id = conn.execute("SELECT id FROM trade_jobs LIMIT 1").fetchone()[0]
        conn.close()

        kwargs = dict(
            directional_job_id=dir_id,
            event_slug="nba-nyk-bos-2026-02-10",
            game_date="2026-02-10",
            home_team="Boston Celtics",
            away_team="New York Knicks",
            game_time_utc="2026-02-11T01:00:00+00:00",
            execute_after="2026-02-10T17:30:00+00:00",
            execute_before="2026-02-11T01:00:00+00:00",
            bothside_group_id="bs-group-1",
            db_path=db_path,
        )
        first = upsert_hedge_job(**kwargs)
        second = upsert_hedge_job(**kwargs)
        assert first is not None
        assert second is None


class TestGetHedgeJobForSlug:
    def test_returns_hedge(self, db_path: Path):
        _insert_job(db_path, job_side="directional")
        _insert_job(db_path, job_side="hedge")
        hedge = get_hedge_job_for_slug("nba-nyk-bos-2026-02-10", db_path=db_path)
        assert hedge is not None
        assert hedge.job_side == "hedge"

    def test_returns_none_when_no_hedge(self, db_path: Path):
        _insert_job(db_path, job_side="directional")
        hedge = get_hedge_job_for_slug("nba-nyk-bos-2026-02-10", db_path=db_path)
        assert hedge is None


class TestGetBothsideSignals:
    def test_returns_both_sides(self, db_path: Path):
        _insert_signal(db_path, team="Celtics", bothside_group_id="bs-1", signal_role="directional")
        _insert_signal(db_path, team="Knicks", bothside_group_id="bs-1", signal_role="hedge")
        _insert_signal(db_path, team="Other", bothside_group_id="bs-2", signal_role="directional")

        signals = get_bothside_signals("bs-1", db_path=db_path)
        assert len(signals) == 2
        roles = {s.signal_role for s in signals}
        assert roles == {"directional", "hedge"}

    def test_empty_group(self, db_path: Path):
        _connect(db_path).close()
        signals = get_bothside_signals("nonexistent", db_path=db_path)
        assert signals == []


class TestHasSignalForSlugAndSide:
    def test_detects_directional(self, db_path: Path):
        _insert_signal(db_path, signal_role="directional")
        assert has_signal_for_slug_and_side(
            "nba-nyk-bos-2026-02-10", "directional", db_path=db_path
        )
        assert not has_signal_for_slug_and_side("nba-nyk-bos-2026-02-10", "hedge", db_path=db_path)

    def test_detects_hedge(self, db_path: Path):
        _insert_signal(db_path, signal_role="hedge")
        assert has_signal_for_slug_and_side("nba-nyk-bos-2026-02-10", "hedge", db_path=db_path)


class TestUpdateJobBothside:
    def test_updates_bothside_fields(self, db_path: Path):
        _insert_job(db_path)
        conn = _connect(db_path)
        job_id = conn.execute("SELECT id FROM trade_jobs LIMIT 1").fetchone()[0]
        conn.close()

        update_job_bothside(job_id, bothside_group_id="bs-99", paired_job_id=42, db_path=db_path)

        conn = _connect(db_path)
        row = conn.execute(
            "SELECT bothside_group_id, paired_job_id FROM trade_jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
        conn.close()
        assert row[0] == "bs-99"
        assert row[1] == 42
