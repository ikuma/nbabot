"""Tests for scheduler retry limit wiring."""

from __future__ import annotations

from pathlib import Path

from src.scheduler.trade_scheduler import process_eligible_jobs
from src.store.db import _connect, get_eligible_jobs, upsert_trade_job


def _insert_job_with_retry(db_path: Path, event_slug: str, retry_count: int) -> None:
    upsert_trade_job(
        game_date="2026-02-10",
        event_slug=event_slug,
        home_team="Boston Celtics",
        away_team="New York Knicks",
        game_time_utc="2026-02-10T23:00:00+00:00",
        execute_after="2026-02-10T15:00:00+00:00",
        execute_before="2026-02-10T23:00:00+00:00",
        db_path=db_path,
    )
    conn = _connect(db_path)
    try:
        conn.execute(
            "UPDATE trade_jobs SET status='failed', retry_count=? WHERE event_slug=?",
            (retry_count, event_slug),
        )
        conn.commit()
    finally:
        conn.close()


def test_get_eligible_jobs_honors_max_retries(tmp_path: Path):
    db_path = tmp_path / "test_retry_limit.db"
    _insert_job_with_retry(db_path, "nba-nyk-bos-2026-02-10", retry_count=1)
    _insert_job_with_retry(db_path, "nba-lal-dal-2026-02-10", retry_count=3)

    jobs = get_eligible_jobs(
        "2026-02-10T20:00:00+00:00",
        max_retries=2,
        db_path=db_path,
    )
    slugs = {j.event_slug for j in jobs}
    assert "nba-nyk-bos-2026-02-10" in slugs
    assert "nba-lal-dal-2026-02-10" not in slugs


def test_process_eligible_jobs_passes_configured_max_retries(monkeypatch, tmp_path: Path):
    db_path = str(tmp_path / "test_retry_wiring.db")
    captured: dict[str, int] = {}

    monkeypatch.setattr("src.scheduler.trade_scheduler.recover_executing_jobs", lambda **_: 0)

    def _fake_get_eligible(now_utc: str, max_retries: int, db_path: str):
        captured["max_retries"] = max_retries
        return []

    monkeypatch.setattr("src.scheduler.trade_scheduler.get_eligible_jobs", _fake_get_eligible)
    monkeypatch.setattr("src.scheduler.trade_scheduler.settings.schedule_max_retries", 7)

    process_eligible_jobs(execution_mode="paper", db_path=db_path)

    assert captured["max_retries"] == 7
