"""Tests for preflight ordering in directional job executor."""

from __future__ import annotations

from src.connectors.polymarket import MoneylineMarket
from src.scheduler.job_executor import process_single_job
from src.store.models import TradeJob
from src.strategy.calibration_scanner import CalibrationOpportunity


def _make_job(job_id: int = 1) -> TradeJob:
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
        job_side="directional",
    )


def _make_market(job: TradeJob) -> MoneylineMarket:
    return MoneylineMarket(
        condition_id="0xcond",
        event_slug=job.event_slug,
        event_title="Knicks vs Celtics",
        home_team=job.home_team,
        away_team=job.away_team,
        outcomes=["Celtics", "Knicks"],
        prices=[0.46, 0.52],
        token_ids=["tok-dir", "tok-hedge"],
        sports_market_type="moneyline",
        active=True,
    )


def _make_opportunity(job: TradeJob) -> CalibrationOpportunity:
    return CalibrationOpportunity(
        event_slug=job.event_slug,
        event_title="Knicks vs Celtics",
        market_type="moneyline",
        outcome_name="Celtics",
        token_id="tok-dir",
        poly_price=0.46,
        calibration_edge_pct=5.0,
        expected_win_rate=0.58,
        ev_per_dollar=0.26,
        price_band="0.45-0.50",
        in_sweet_spot=True,
        band_confidence="high",
        position_usd=25.0,
    )


def test_live_preflight_failure_does_not_log_signal(monkeypatch):
    job = _make_job()
    market = _make_market(job)
    opp = _make_opportunity(job)

    monkeypatch.setattr("src.scheduler.job_executor.settings.bothside_enabled", False)
    monkeypatch.setattr("src.scheduler.job_executor.settings.dca_max_entries", 1)
    monkeypatch.setattr("src.scheduler.job_executor._build_liquidity_map", lambda *_: None)
    monkeypatch.setattr("src.scheduler.job_executor._fetch_live_balance", lambda *_: None)
    monkeypatch.setattr("src.scheduler.job_executor._preflight_check", lambda: False)

    status_updates: list[tuple] = []

    def _update_status(*args, **kwargs):
        status_updates.append((args, kwargs))
    monkeypatch.setattr("src.scheduler.job_executor.update_job_status", _update_status)

    log_calls: list[dict] = []

    def _log_signal(**kwargs):
        log_calls.append(kwargs)
        return 123

    result, bothside = process_single_job(
        job=job,
        execution_mode="live",
        db_path=":memory:",
        fetch_moneyline_for_game=lambda *_: market,
        scan_calibration=lambda *_args, **_kwargs: [opp],
        log_signal=_log_signal,
        place_limit_buy=lambda *_: {"orderID": "ord-1"},
        update_order_status=lambda *_args, **_kwargs: None,
        sizing_multiplier=1.0,
    )

    assert result.status == "failed"
    assert result.signal_id is None
    assert bothside is None
    assert log_calls == []
    assert status_updates[-1][0][1] == "failed"
    assert status_updates[-1][1]["error_message"] == "Preflight check failed"
