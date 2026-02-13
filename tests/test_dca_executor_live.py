"""Tests for live DCA inventory guards."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from src.scheduler.dca_executor import process_dca_active_jobs
from src.store.models import TradeJob


def _make_dca_job(job_id: int = 1) -> TradeJob:
    return TradeJob(
        id=job_id,
        game_date="2026-02-10",
        event_slug="nba-nyk-bos-2026-02-10",
        home_team="Boston Celtics",
        away_team="New York Knicks",
        game_time_utc="2026-02-11T01:00:00+00:00",
        execute_after="2026-02-10T17:00:00+00:00",
        execute_before="2026-02-11T01:00:00+00:00",
        status="dca_active",
        signal_id=101,
        retry_count=0,
        error_message=None,
        created_at="2026-02-10T00:00:00+00:00",
        updated_at="2026-02-10T00:00:00+00:00",
        dca_entries_count=1,
        dca_max_entries=5,
        dca_group_id="grp-1",
        dca_total_budget=100.0,
        dca_slice_size=20.0,
        job_side="directional",
    )


def _make_signal(
    *,
    order_status: str = "filled",
    token_id: str = "tok-1",
    team: str = "Celtics",
) -> SimpleNamespace:
    return SimpleNamespace(
        order_status=order_status,
        created_at=datetime.now(timezone.utc).isoformat(),
        poly_price=0.40,
        fill_price=None,
        kelly_size=10.0,
        token_id=token_id,
        team=team,
        game_title="Knicks vs Celtics",
        event_slug="nba-nyk-bos-2026-02-10",
        book_prob=0.6,
        edge_pct=3.0,
        market_type="moneyline",
        calibration_edge_pct=2.0,
        expected_win_rate=0.62,
        price_band="0.35-0.40",
        in_sweet_spot=1,
        band_confidence="high",
        condition_id="cond-1",
    )


def test_live_dca_waits_when_placed_exists(monkeypatch):
    job = _make_dca_job()
    placed_signal = SimpleNamespace(order_status="placed")

    monkeypatch.setattr(
        "src.scheduler.dca_executor.get_dca_active_jobs",
        lambda *_args, **_kwargs: [job],
    )
    monkeypatch.setattr(
        "src.scheduler.dca_executor.get_dca_group_signals",
        lambda *_args, **_kwargs: [placed_signal],
    )
    monkeypatch.setattr(
        "src.connectors.polymarket.fetch_moneyline_for_game",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not fetch price")),
    )
    monkeypatch.setattr("src.store.db.log_signal", lambda **_: (_ for _ in ()).throw(
        AssertionError("must not log signal")
    ))

    results = process_dca_active_jobs(execution_mode="live", db_path=":memory:")
    assert results == []


def test_live_dca_waits_until_filled_inventory_exists(monkeypatch):
    job = _make_dca_job()
    failed_signal = SimpleNamespace(order_status="failed")

    monkeypatch.setattr(
        "src.scheduler.dca_executor.get_dca_active_jobs",
        lambda *_args, **_kwargs: [job],
    )
    monkeypatch.setattr(
        "src.scheduler.dca_executor.get_dca_group_signals",
        lambda *_args, **_kwargs: [failed_signal],
    )
    monkeypatch.setattr(
        "src.connectors.polymarket.fetch_moneyline_for_game",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not fetch price")),
    )
    monkeypatch.setattr("src.store.db.log_signal", lambda **_: (_ for _ in ()).throw(
        AssertionError("must not log signal")
    ))

    results = process_dca_active_jobs(execution_mode="live", db_path=":memory:")
    assert results == []


def test_dca_stops_when_position_group_target_already_reached(monkeypatch):
    job = _make_dca_job()
    sig = _make_signal(order_status="filled")
    called = {"status": None}

    monkeypatch.setattr("src.scheduler.dca_executor.settings.game_position_group_enabled", True)
    monkeypatch.setattr("src.scheduler.dca_executor.settings.dca_min_order_usd", 1.0)
    monkeypatch.setattr("src.scheduler.dca_executor.get_dca_active_jobs", lambda *_a, **_k: [job])
    monkeypatch.setattr("src.scheduler.dca_executor.get_dca_group_signals", lambda *_a, **_k: [sig])
    monkeypatch.setattr(
        "src.connectors.polymarket.fetch_moneyline_for_game",
        lambda *_a, **_k: SimpleNamespace(
            token_ids=["tok-1"],
            prices=[0.50],
            outcomes=["Celtics"],
        ),
    )
    monkeypatch.setattr(
        "src.strategy.dca_strategy.should_add_dca_entry",
        lambda *_a, **_k: SimpleNamespace(
            should_buy=True,
            reason="favorable",
            vwap=0.40,
            sequence=2,
        ),
    )
    monkeypatch.setattr(
        "src.sizing.position_sizer.calculate_target_order_size",
        lambda **_k: SimpleNamespace(order_size_usd=5.0, completion_reason=None),
    )
    monkeypatch.setattr(
        "src.scheduler.dca_executor.get_position_group",
        lambda *_a, **_k: SimpleNamespace(M_target=10.0, D_target=0.0),
    )
    monkeypatch.setattr(
        "src.scheduler.dca_executor.compute_position_group_inventory",
        lambda *_a, **_k: (10.0, 0.0, 0.0),
    )
    monkeypatch.setattr(
        "src.store.db.log_signal",
        lambda **_: (_ for _ in ()).throw(
            AssertionError("must not log signal when target reached")
        ),
    )
    monkeypatch.setattr(
        "src.scheduler.dca_executor.update_dca_job",
        lambda _job_id, **kwargs: called.update(status=kwargs.get("status")),
    )

    results = process_dca_active_jobs(execution_mode="paper", db_path=":memory:")
    assert results == []
    assert called["status"] == "executed"


def test_dca_size_is_capped_by_position_group_remaining_target(monkeypatch):
    job = _make_dca_job()
    sig = _make_signal(order_status="filled")
    captured = {"size": None}

    monkeypatch.setattr("src.scheduler.dca_executor.settings.game_position_group_enabled", True)
    monkeypatch.setattr("src.scheduler.dca_executor.settings.dca_min_order_usd", 1.0)
    monkeypatch.setattr("src.scheduler.dca_executor.get_dca_active_jobs", lambda *_a, **_k: [job])
    monkeypatch.setattr("src.scheduler.dca_executor.get_dca_group_signals", lambda *_a, **_k: [sig])
    monkeypatch.setattr(
        "src.connectors.polymarket.fetch_moneyline_for_game",
        lambda *_a, **_k: SimpleNamespace(
            token_ids=["tok-1"],
            prices=[0.50],
            outcomes=["Celtics"],
        ),
    )
    monkeypatch.setattr(
        "src.strategy.dca_strategy.should_add_dca_entry",
        lambda *_a, **_k: SimpleNamespace(
            should_buy=True,
            reason="favorable",
            vwap=0.40,
            sequence=2,
        ),
    )
    monkeypatch.setattr(
        "src.sizing.position_sizer.calculate_target_order_size",
        lambda **_k: SimpleNamespace(order_size_usd=5.0, completion_reason=None),
    )
    monkeypatch.setattr(
        "src.scheduler.dca_executor.get_position_group",
        lambda *_a, **_k: SimpleNamespace(M_target=15.0, D_target=0.0),
    )
    monkeypatch.setattr(
        "src.scheduler.dca_executor.compute_position_group_inventory",
        lambda *_a, **_k: (13.0, 0.0, 0.0),
    )

    def _fake_log_signal(**kwargs):
        captured["size"] = kwargs["kelly_size"]
        return 999

    monkeypatch.setattr("src.store.db.log_signal", _fake_log_signal)
    monkeypatch.setattr("src.store.db.update_signal_fee", lambda *_a, **_k: None)

    results = process_dca_active_jobs(execution_mode="paper", db_path=":memory:")
    assert len(results) == 1
    assert results[0].status == "executed"
    assert captured["size"] == 1.0  # remaining 2 shares * price 0.5
