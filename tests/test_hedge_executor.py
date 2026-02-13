"""Tests for hedge executor retry behavior."""

from __future__ import annotations

from unittest.mock import patch

from src.scheduler.hedge_executor import process_hedge_job
from src.store.models import TradeJob


def _make_hedge_job(job_id: int = 1) -> TradeJob:
    return TradeJob(
        id=job_id,
        game_date="2026-02-10",
        event_slug="nba-nyk-bos-2026-02-10",
        home_team="Boston Celtics",
        away_team="New York Knicks",
        game_time_utc="2026-02-10T23:00:00+00:00",
        execute_after="2026-02-10T15:00:00+00:00",
        execute_before="2026-02-10T23:00:00+00:00",
        status="pending",
        signal_id=None,
        retry_count=0,
        error_message=None,
        created_at="2026-02-10T00:00:00+00:00",
        updated_at="2026-02-10T00:00:00+00:00",
        job_side="hedge",
    )


def test_live_no_market_defers_to_pending():
    job = _make_hedge_job()
    with patch("src.scheduler.hedge_executor.update_job_status") as mock_update:
        result = process_hedge_job(
            job=job,
            execution_mode="live",
            db_path=":memory:",
            fetch_moneyline_for_game=lambda *_: None,
            log_signal=lambda **_: 1,
            place_limit_buy=lambda *_: {},
            update_order_status=lambda *_args, **_kwargs: None,
        )

    assert result.status == "skipped"
    assert mock_update.call_args_list[0].args[1] == "executing"
    assert mock_update.call_args_list[1].args[1] == "pending"
    assert mock_update.call_args_list[1].kwargs["error_message"] == "No moneyline market"


def test_paper_no_market_stays_skipped():
    job = _make_hedge_job()
    with patch("src.scheduler.hedge_executor.update_job_status") as mock_update:
        result = process_hedge_job(
            job=job,
            execution_mode="paper",
            db_path=":memory:",
            fetch_moneyline_for_game=lambda *_: None,
            log_signal=lambda **_: 1,
            place_limit_buy=lambda *_: {},
            update_order_status=lambda *_args, **_kwargs: None,
        )

    assert result.status == "skipped"
    assert mock_update.call_args_list[0].args[1] == "executing"
    assert mock_update.call_args_list[1].args[1] == "skipped"
    assert mock_update.call_args_list[1].kwargs["error_message"] == "No moneyline market"
