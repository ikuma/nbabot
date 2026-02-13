"""Tests for hedge executor retry behavior."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from src.connectors.polymarket import MoneylineMarket
from src.scheduler.hedge_executor import process_hedge_job
from src.sizing.position_sizer import DCABudget
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


def _make_dir_signal(order_status: str = "filled") -> SimpleNamespace:
    return SimpleNamespace(
        team="Celtics",
        kelly_size=20.0,
        fill_price=0.45,
        poly_price=0.45,
        order_status=order_status,
    )


def _make_dir_signal_with_time(
    order_status: str = "filled",
    created_at: str = "2026-02-10T00:00:00+00:00",
) -> SimpleNamespace:
    s = _make_dir_signal(order_status=order_status)
    s.created_at = created_at
    return s


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


def test_live_missing_directional_signals_defers_pending(monkeypatch):
    job = _make_hedge_job()
    job.paired_job_id = 99

    monkeypatch.setattr("src.scheduler.hedge_executor._get_directional_dca_group", lambda *_: "grp")
    monkeypatch.setattr(
        "src.scheduler.hedge_executor.get_dca_group_signals",
        lambda *_args, **_kwargs: [],
    )

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
    assert mock_update.call_args_list[1].args[1] == "pending"
    assert mock_update.call_args_list[1].kwargs["error_message"] == "No directional signals"


def test_live_directional_not_filled_defers_pending(monkeypatch):
    job = _make_hedge_job()
    job.paired_job_id = 99

    monkeypatch.setattr("src.scheduler.hedge_executor._get_directional_dca_group", lambda *_: "grp")
    monkeypatch.setattr(
        "src.scheduler.hedge_executor.get_dca_group_signals",
        lambda *_args, **_kwargs: [_make_dir_signal(order_status="placed")],
    )

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
    assert mock_update.call_args_list[1].args[1] == "pending"
    assert mock_update.call_args_list[1].kwargs["error_message"] == "Directional not filled yet"


def test_live_leg2_timeout_skips_hedge(monkeypatch):
    job = _make_hedge_job()
    job.paired_job_id = 99

    monkeypatch.setattr("src.scheduler.hedge_executor.settings.game_position_group_enabled", True)
    monkeypatch.setattr("src.scheduler.hedge_executor.settings.position_group_leg2_timeout_min", 30)
    monkeypatch.setattr("src.scheduler.hedge_executor._get_directional_dca_group", lambda *_: "grp")
    monkeypatch.setattr(
        "src.scheduler.hedge_executor.get_dca_group_signals",
        lambda *_args, **_kwargs: [
            _make_dir_signal_with_time(
                order_status="filled",
                created_at="2026-02-10T00:00:00+00:00",
            )
        ],
    )

    with patch("src.scheduler.hedge_executor.update_job_status") as mock_update:
        result = process_hedge_job(
            job=job,
            execution_mode="live",
            db_path=":memory:",
            fetch_moneyline_for_game=lambda *_: (_ for _ in ()).throw(
                AssertionError("must not fetch market after timeout")
            ),
            log_signal=lambda **_: 1,
            place_limit_buy=lambda *_: {},
            update_order_status=lambda *_args, **_kwargs: None,
        )

    assert result.status == "skipped"
    assert mock_update.call_args_list[1].args[1] == "skipped"
    assert mock_update.call_args_list[1].kwargs["error_message"] == "Leg2 timeout"


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


def test_paper_hedge_executes_and_returns_signal_id(monkeypatch):
    job = _make_hedge_job()
    ml = MoneylineMarket(
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
    fake_curve = SimpleNamespace(
        estimate=lambda _: SimpleNamespace(lower_bound=0.58, effective_sample_size=120),
    )

    monkeypatch.setattr("src.scheduler.hedge_executor.settings.dca_max_entries", 1)
    monkeypatch.setattr("src.scheduler.hedge_executor.settings.llm_analysis_enabled", False)
    monkeypatch.setattr(
        "src.scheduler.hedge_executor._compute_hedge_order_price",
        lambda **_: (0.52, 0.51),
    )
    monkeypatch.setattr(
        "src.strategy.calibration_curve.get_default_curve",
        lambda: fake_curve,
    )
    monkeypatch.setattr(
        "src.strategy.calibration_curve._confidence_from_sample_size",
        lambda *_: "high",
    )
    monkeypatch.setattr(
        "src.sizing.position_sizer.calculate_dca_budget",
        lambda **_: DCABudget(
            total_budget_usd=12.0,
            num_slices=1,
            slice_size_usd=12.0,
            constraint_binding="kelly",
        ),
    )

    logged: list[dict] = []

    def _log_signal(**kwargs):
        logged.append(kwargs)
        return 321

    with patch("src.scheduler.hedge_executor.update_job_status") as mock_update:
        result = process_hedge_job(
            job=job,
            execution_mode="paper",
            db_path=":memory:",
            fetch_moneyline_for_game=lambda *_: ml,
            log_signal=_log_signal,
            place_limit_buy=lambda *_: {},
            update_order_status=lambda *_args, **_kwargs: None,
        )

    assert result.status == "executed"
    assert result.signal_id == 321
    assert mock_update.call_args_list[-1].args[1] == "executed"
    assert logged and logged[0]["signal_role"] == "hedge"


def test_live_hedge_places_order_and_executes(monkeypatch):
    job = _make_hedge_job()
    ml = MoneylineMarket(
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
    fake_curve = SimpleNamespace(
        estimate=lambda _: SimpleNamespace(lower_bound=0.58, effective_sample_size=120),
    )

    monkeypatch.setattr("src.scheduler.hedge_executor.settings.dca_max_entries", 1)
    monkeypatch.setattr("src.scheduler.hedge_executor.settings.llm_analysis_enabled", False)
    monkeypatch.setattr("src.scheduler.hedge_executor._preflight_check", lambda: True)
    monkeypatch.setattr(
        "src.scheduler.hedge_executor._compute_hedge_order_price",
        lambda **_: (0.52, 0.51),
    )
    monkeypatch.setattr(
        "src.strategy.calibration_curve.get_default_curve",
        lambda: fake_curve,
    )
    monkeypatch.setattr(
        "src.strategy.calibration_curve._confidence_from_sample_size",
        lambda *_: "high",
    )
    monkeypatch.setattr(
        "src.sizing.position_sizer.calculate_dca_budget",
        lambda **_: DCABudget(
            total_budget_usd=12.0,
            num_slices=1,
            slice_size_usd=12.0,
            constraint_binding="kelly",
        ),
    )

    order_updates: list[tuple] = []

    def _update_order_status(*args, **kwargs):
        order_updates.append((args, kwargs))

    with patch("src.scheduler.hedge_executor.update_job_status") as mock_update:
        result = process_hedge_job(
            job=job,
            execution_mode="live",
            db_path=":memory:",
            fetch_moneyline_for_game=lambda *_: ml,
            log_signal=lambda **_: 555,
            place_limit_buy=lambda *_: {"orderID": "ord-live-1"},
            update_order_status=_update_order_status,
        )

    assert result.status == "executed"
    assert result.signal_id == 555
    assert len(order_updates) == 1
    assert order_updates[0][0][0] == 555
    assert order_updates[0][0][1] == "ord-live-1"
    assert order_updates[0][0][2] == "placed"
    assert mock_update.call_args_list[-1].args[1] == "executed"


def test_live_preflight_failure_does_not_log_signal(monkeypatch):
    job = _make_hedge_job()
    job.paired_job_id = 99
    ml = MoneylineMarket(
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
    fake_curve = SimpleNamespace(
        estimate=lambda _: SimpleNamespace(lower_bound=0.58, effective_sample_size=120),
    )

    monkeypatch.setattr("src.scheduler.hedge_executor._get_directional_dca_group", lambda *_: "grp")
    monkeypatch.setattr(
        "src.scheduler.hedge_executor.get_dca_group_signals",
        lambda *_args, **_kwargs: [_make_dir_signal(order_status="filled")],
    )
    monkeypatch.setattr("src.scheduler.hedge_executor.settings.dca_max_entries", 1)
    monkeypatch.setattr("src.scheduler.hedge_executor.settings.llm_analysis_enabled", False)
    monkeypatch.setattr("src.scheduler.hedge_executor._preflight_check", lambda: False)
    monkeypatch.setattr(
        "src.scheduler.hedge_executor._compute_hedge_order_price",
        lambda **_: (0.52, 0.51),
    )
    monkeypatch.setattr(
        "src.strategy.calibration_curve.get_default_curve",
        lambda: fake_curve,
    )
    monkeypatch.setattr(
        "src.strategy.calibration_curve._confidence_from_sample_size",
        lambda *_: "high",
    )
    monkeypatch.setattr(
        "src.sizing.position_sizer.calculate_dca_budget",
        lambda **_: DCABudget(
            total_budget_usd=12.0,
            num_slices=1,
            slice_size_usd=12.0,
            constraint_binding="kelly",
        ),
    )

    log_calls: list[dict] = []

    def _log_signal(**kwargs):
        log_calls.append(kwargs)
        return 777

    with patch("src.scheduler.hedge_executor.update_job_status") as mock_update:
        result = process_hedge_job(
            job=job,
            execution_mode="live",
            db_path=":memory:",
            fetch_moneyline_for_game=lambda *_: ml,
            log_signal=_log_signal,
            place_limit_buy=lambda *_: {"orderID": "ord-live-1"},
            update_order_status=lambda *_args, **_kwargs: None,
        )

    assert result.status == "failed"
    assert result.signal_id is None
    assert log_calls == []
    assert mock_update.call_args_list[-1].args[1] == "failed"
    assert mock_update.call_args_list[-1].kwargs["error_message"] == "Preflight check failed"
