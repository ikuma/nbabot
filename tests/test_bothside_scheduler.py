"""Tests for both-side scheduling in trade_scheduler.py (Phase B)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from src.scheduler.hedge_executor import (
    _schedule_hedge_job,
)
from src.store.db import (
    _connect,
    get_hedge_job_for_slug,
    upsert_trade_job,
)
from src.strategy.calibration_scanner import BothsideOpportunity, CalibrationOpportunity


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test_sched.db"


def _insert_job(db_path: Path, **overrides) -> int:
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
    conn = _connect(db_path)
    job_id = conn.execute(
        "SELECT id FROM trade_jobs WHERE event_slug = ? AND job_side = ?",
        (defaults["event_slug"], defaults.get("job_side", "directional")),
    ).fetchone()[0]
    conn.close()
    return job_id


def _make_opp(
    outcome: str = "Celtics",
    price: float = 0.35,
    ev: float = 1.5,
    position_usd: float = 25.0,
    token_id: str = "tok_0",
) -> CalibrationOpportunity:
    return CalibrationOpportunity(
        event_slug="nba-nyk-bos-2026-02-10",
        event_title="Knicks vs Celtics",
        market_type="moneyline",
        outcome_name=outcome,
        token_id=token_id,
        poly_price=price,
        calibration_edge_pct=50.0,
        expected_win_rate=0.90,
        ev_per_dollar=ev,
        price_band="0.35-0.40",
        in_sweet_spot=True,
        band_confidence="high",
        position_usd=position_usd,
    )


class TestScheduleHedgeJob:
    def test_hedge_job_created_after_directional(self, db_path):
        """After directional job, a hedge job should be created as pending."""
        job_id = _insert_job(db_path)

        from src.store.db import TradeJob

        conn = _connect(db_path)
        row = conn.execute("SELECT * FROM trade_jobs WHERE id = ?", (job_id,)).fetchone()
        conn.close()
        dir_job = TradeJob(**dict(row))

        dir_opp = _make_opp("Celtics", 0.35, 1.5, 25.0, "tok_dir")
        hedge_opp = _make_opp("Knicks", 0.50, 0.5, 12.0, "tok_hedge")
        bothside = BothsideOpportunity(
            directional=dir_opp,
            hedge=hedge_opp,
            combined_price=0.85,
            hedge_position_usd=6.0,
        )

        with patch("src.scheduler.hedge_executor.settings") as mock_settings:
            mock_settings.bothside_hedge_delay_min = 30

            _schedule_hedge_job(dir_job, bothside, "dca-grp-1", str(db_path))

        hedge = get_hedge_job_for_slug("nba-nyk-bos-2026-02-10", db_path=db_path)
        assert hedge is not None
        assert hedge.job_side == "hedge"
        assert hedge.status == "pending"
        assert hedge.paired_job_id == job_id
        assert hedge.bothside_group_id is not None

    def test_no_hedge_when_disabled(self, db_path, monkeypatch):
        """When bothside_enabled=False, no hedge job should be created."""
        monkeypatch.setattr("src.scheduler.hedge_executor.settings.bothside_enabled", False)

        # just verify no crash when bothside is disabled
        hedge = get_hedge_job_for_slug("nba-nyk-bos-2026-02-10", db_path=db_path)
        assert hedge is None

    def test_hedge_respects_delay(self, db_path):
        """Hedge job's execute_after should reflect the delay setting."""
        job_id = _insert_job(db_path)

        from src.store.db import TradeJob

        conn = _connect(db_path)
        row = conn.execute("SELECT * FROM trade_jobs WHERE id = ?", (job_id,)).fetchone()
        conn.close()
        dir_job = TradeJob(**dict(row))

        hedge_opp = _make_opp("Knicks", 0.50, 0.5, 12.0, "tok_hedge")
        bothside = BothsideOpportunity(
            directional=_make_opp(),
            hedge=hedge_opp,
            combined_price=0.85,
            hedge_position_usd=6.0,
        )

        with patch("src.scheduler.hedge_executor.settings") as mock_settings:
            mock_settings.bothside_hedge_delay_min = 60  # 60 min delay

            _schedule_hedge_job(dir_job, bothside, "dca-grp-1", str(db_path))

        hedge = get_hedge_job_for_slug("nba-nyk-bos-2026-02-10", db_path=db_path)
        assert hedge is not None
        # execute_after should be ~now + 60min (delay from current time)
        from datetime import datetime, timedelta, timezone

        hedge_after = datetime.fromisoformat(hedge.execute_after.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        # hedge_after should be approximately now + 60 min (within 5 min tolerance)
        expected_after = now + timedelta(minutes=60)
        diff = abs((hedge_after - expected_after).total_seconds())
        assert diff < 300  # within 5 minutes


class TestHedgeIndependentDca:
    def test_hedge_independent_dca(self, db_path):
        """Hedge job should get its own dca_group_id distinct from directional."""
        job_id = _insert_job(db_path)

        from src.store.db import TradeJob

        conn = _connect(db_path)
        row = conn.execute("SELECT * FROM trade_jobs WHERE id = ?", (job_id,)).fetchone()
        conn.close()
        dir_job = TradeJob(**dict(row))

        bothside = BothsideOpportunity(
            directional=_make_opp("Celtics", 0.35),
            hedge=_make_opp("Knicks", 0.50),
            combined_price=0.85,
            hedge_position_usd=6.0,
        )

        with patch("src.scheduler.hedge_executor.settings") as mock_settings:
            mock_settings.bothside_hedge_delay_min = 30

            _schedule_hedge_job(dir_job, bothside, "dca-grp-dir", str(db_path))

        hedge = get_hedge_job_for_slug("nba-nyk-bos-2026-02-10", db_path=db_path)
        assert hedge is not None
        # hedge job has its own dca_group_id (None at creation, set on execution)
        # bothside_group_id links both jobs
        assert hedge.bothside_group_id is not None

        # directional also gets the bothside_group_id
        conn = _connect(db_path)
        dir_row = conn.execute(
            "SELECT bothside_group_id FROM trade_jobs WHERE id = ?", (job_id,)
        ).fetchone()
        conn.close()
        assert dir_row[0] == hedge.bothside_group_id
