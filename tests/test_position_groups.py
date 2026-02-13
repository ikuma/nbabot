"""Tests for GamePositionGroup data model and state machine (Track B)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.risk.models import CircuitBreakerLevel, RiskState
from src.scheduler.position_group_manager import process_position_groups
from src.scheduler.trade_scheduler import refresh_schedule
from src.store.db import (
    compute_position_group_inventory,
    get_position_group,
    log_merge_operation,
    log_result,
    log_signal,
    upsert_position_group,
    upsert_trade_job,
)


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test_position_groups.db"


def _insert_job(db_path: Path, *, event_slug: str, execute_before: str) -> None:
    upsert_trade_job(
        game_date="2026-02-10",
        event_slug=event_slug,
        home_team="Boston Celtics",
        away_team="New York Knicks",
        game_time_utc=execute_before,
        execute_after="2026-02-10T17:00:00+00:00",
        execute_before=execute_before,
        db_path=db_path,
    )


def _log_sig(
    db_path: Path,
    *,
    event_slug: str,
    signal_role: str,
    kelly_size: float,
    poly_price: float,
    order_status: str,
    expected_win_rate: float | None = None,
    band_confidence: str = "high",
) -> int:
    sid = log_signal(
        game_title="Knicks vs Celtics",
        event_slug=event_slug,
        team="Celtics" if signal_role == "directional" else "Knicks",
        side="BUY",
        poly_price=poly_price,
        book_prob=0.5,
        edge_pct=3.0,
        kelly_size=kelly_size,
        token_id=f"tok-{signal_role}-{order_status}-{kelly_size}",
        signal_role=signal_role,
        expected_win_rate=expected_win_rate,
        band_confidence=band_confidence,
        db_path=db_path,
    )
    if order_status != "paper":
        from src.store.db import update_order_status

        update_order_status(sid, f"oid-{sid}", order_status, fill_price=poly_price, db_path=db_path)
    return sid


def test_upsert_and_get_position_group(db_path: Path):
    upsert_position_group(
        event_slug="nba-nyk-bos-2026-02-10",
        game_date="2026-02-10",
        state="PLANNED",
        d_max=12.0,
        db_path=db_path,
    )
    group = get_position_group("nba-nyk-bos-2026-02-10", db_path=db_path)
    assert group is not None
    assert group.state == "PLANNED"
    assert group.d_max == 12.0


def test_compute_position_group_inventory_uses_filled_only(db_path: Path):
    event_slug = "nba-nyk-bos-2026-02-10"
    _log_sig(
        db_path,
        event_slug=event_slug,
        signal_role="directional",
        kelly_size=40.0,
        poly_price=0.40,
        order_status="filled",
    )  # 100 shares
    settled_sid = _log_sig(
        db_path,
        event_slug=event_slug,
        signal_role="directional",
        kelly_size=5.0,
        poly_price=0.50,
        order_status="filled",
    )  # 10 shares, but settled
    _log_sig(
        db_path,
        event_slug=event_slug,
        signal_role="hedge",
        kelly_size=20.0,
        poly_price=0.50,
        order_status="filled",
    )  # 40 shares
    _log_sig(
        db_path,
        event_slug=event_slug,
        signal_role="directional",
        kelly_size=10.0,
        poly_price=0.40,
        order_status="placed",
    )  # ignore

    log_result(
        signal_id=settled_sid,
        outcome="win",
        won=True,
        settlement_price=1.0,
        pnl=5.0,
        db_path=db_path,
    )

    log_merge_operation(
        bothside_group_id="bs-1",
        condition_id="cond-1",
        event_slug=event_slug,
        dir_shares=100.0,
        hedge_shares=40.0,
        merge_amount=15.0,
        remainder_shares=25.0,
        remainder_side="directional",
        dir_vwap=0.40,
        hedge_vwap=0.50,
        combined_vwap=0.90,
        status="executed",
        db_path=db_path,
    )

    q_dir, q_opp, merged_qty = compute_position_group_inventory(event_slug, db_path=db_path)
    assert q_dir == pytest.approx(85.0)  # 100 - merged 15
    assert q_opp == pytest.approx(25.0)  # 40 - merged 15
    assert merged_qty == pytest.approx(15.0)


def test_state_transition_planned_to_acquire(db_path: Path):
    event_slug = "nba-nyk-bos-2026-02-10"
    execute_before = "2026-02-11T01:00:00+00:00"
    _insert_job(db_path, event_slug=event_slug, execute_before=execute_before)
    upsert_position_group(
        event_slug=event_slug,
        game_date="2026-02-10",
        state="PLANNED",
        d_max=20.0,
        db_path=db_path,
    )

    now = datetime(2026, 2, 10, 18, 0, tzinfo=timezone.utc)
    results = process_position_groups(db_path=str(db_path), now_utc=now)
    assert len(results) == 1
    assert results[0].new_state == "ACQUIRE"


def test_state_transition_to_balance_when_d_exceeds_limit(db_path: Path):
    event_slug = "nba-nyk-bos-2026-02-10"
    execute_before = "2026-02-11T01:00:00+00:00"
    _insert_job(db_path, event_slug=event_slug, execute_before=execute_before)
    upsert_position_group(
        event_slug=event_slug,
        game_date="2026-02-10",
        state="ACQUIRE",
        d_max=10.0,
        db_path=db_path,
    )
    _log_sig(
        db_path,
        event_slug=event_slug,
        signal_role="directional",
        kelly_size=20.0,
        poly_price=0.50,
        order_status="filled",
    )  # 40 shares

    now = datetime(2026, 2, 10, 18, 0, tzinfo=timezone.utc)
    results = process_position_groups(db_path=str(db_path), now_utc=now)
    assert results[0].new_state == "BALANCE"
    assert results[0].q_dir == pytest.approx(40.0)
    assert results[0].q_opp == pytest.approx(0.0)


def test_state_transition_to_merge_loop_when_mergeable(db_path: Path, monkeypatch):
    event_slug = "nba-nyk-bos-2026-02-10"
    execute_before = "2026-02-11T01:00:00+00:00"
    _insert_job(db_path, event_slug=event_slug, execute_before=execute_before)
    upsert_position_group(
        event_slug=event_slug,
        game_date="2026-02-10",
        state="ACQUIRE",
        d_max=100.0,
        db_path=db_path,
    )
    _log_sig(
        db_path,
        event_slug=event_slug,
        signal_role="directional",
        kelly_size=3.0,
        poly_price=0.30,
        order_status="filled",
    )  # 10 shares
    _log_sig(
        db_path,
        event_slug=event_slug,
        signal_role="hedge",
        kelly_size=2.5,
        poly_price=0.25,
        order_status="filled",
    )  # 10 shares
    monkeypatch.setattr(
        "src.scheduler.position_group_manager.settings.position_group_min_merge_shares",
        5.0,
    )

    now = datetime(2026, 2, 10, 18, 0, tzinfo=timezone.utc)
    results = process_position_groups(db_path=str(db_path), now_utc=now)
    assert results[0].new_state == "MERGE_LOOP"
    assert results[0].m == pytest.approx(10.0)


def test_state_transition_to_residual_hold_near_tipoff(db_path: Path, monkeypatch):
    event_slug = "nba-nyk-bos-2026-02-10"
    now = datetime(2026, 2, 10, 18, 0, tzinfo=timezone.utc)
    execute_before = (now + timedelta(minutes=10)).isoformat()
    _insert_job(db_path, event_slug=event_slug, execute_before=execute_before)
    upsert_position_group(
        event_slug=event_slug,
        game_date="2026-02-10",
        state="ACQUIRE",
        d_max=20.0,
        db_path=db_path,
    )
    monkeypatch.setattr(
        "src.scheduler.position_group_manager.settings.position_group_new_risk_cutoff_min",
        30,
    )

    results = process_position_groups(db_path=str(db_path), now_utc=now)
    assert results[0].new_state == "RESIDUAL_HOLD"


def test_dmax_decay_near_tipoff_forces_balance(db_path: Path, monkeypatch):
    event_slug = "nba-nyk-bos-2026-02-10"
    now = datetime(2026, 2, 10, 18, 0, tzinfo=timezone.utc)
    execute_before = (now + timedelta(minutes=5)).isoformat()
    _insert_job(db_path, event_slug=event_slug, execute_before=execute_before)
    upsert_position_group(
        event_slug=event_slug,
        game_date="2026-02-10",
        state="ACQUIRE",
        d_max=10.0,
        db_path=db_path,
    )
    _log_sig(
        db_path,
        event_slug=event_slug,
        signal_role="directional",
        kelly_size=4.0,
        poly_price=0.50,
        order_status="filled",
    )  # 8 shares

    monkeypatch.setattr(
        "src.scheduler.position_group_manager.settings.position_group_dmax_decay_enabled",
        True,
    )
    monkeypatch.setattr(
        "src.scheduler.position_group_manager.settings.position_group_dmax_decay_start_min",
        60,
    )
    monkeypatch.setattr(
        "src.scheduler.position_group_manager.settings.position_group_dmax_decay_floor_ratio",
        0.2,
    )
    monkeypatch.setattr(
        "src.scheduler.position_group_manager.settings.position_group_new_risk_cutoff_min",
        0,
    )

    results = process_position_groups(db_path=str(db_path), now_utc=now)
    assert results[0].d_max < 10.0
    assert results[0].new_state == "BALANCE"


def test_state_transition_to_safe_stop_when_cb_orange(db_path: Path, monkeypatch):
    event_slug = "nba-nyk-bos-2026-02-10"
    execute_before = "2026-02-11T01:00:00+00:00"
    _insert_job(db_path, event_slug=event_slug, execute_before=execute_before)
    upsert_position_group(
        event_slug=event_slug,
        game_date="2026-02-10",
        state="ACQUIRE",
        d_max=20.0,
        db_path=db_path,
    )
    monkeypatch.setattr(
        "src.risk.risk_engine.load_or_compute_risk_state",
        lambda _db: RiskState(
            circuit_breaker_level=CircuitBreakerLevel.ORANGE,
            sizing_multiplier=0.0,
            current_balance=1000.0,
            last_known_balance=1000.0,
            flags=[],
        ),
    )

    now = datetime(2026, 2, 10, 18, 0, tzinfo=timezone.utc)
    results = process_position_groups(db_path=str(db_path), now_utc=now)
    assert results[0].new_state == "SAFE_STOP"


def test_state_transition_to_safe_stop_on_risk_flag(db_path: Path, monkeypatch):
    event_slug = "nba-nyk-bos-2026-02-10"
    execute_before = "2026-02-11T01:00:00+00:00"
    _insert_job(db_path, event_slug=event_slug, execute_before=execute_before)
    upsert_position_group(
        event_slug=event_slug,
        game_date="2026-02-10",
        state="ACQUIRE",
        d_max=20.0,
        db_path=db_path,
    )
    monkeypatch.setattr(
        "src.risk.risk_engine.load_or_compute_risk_state",
        lambda _db: RiskState(
            circuit_breaker_level=CircuitBreakerLevel.GREEN,
            sizing_multiplier=1.0,
            current_balance=1000.0,
            last_known_balance=1000.0,
            flags=["balance_anomaly"],
        ),
    )

    now = datetime(2026, 2, 10, 18, 0, tzinfo=timezone.utc)
    results = process_position_groups(db_path=str(db_path), now_utc=now)
    assert results[0].new_state == "SAFE_STOP"


def test_state_transition_to_safe_stop_on_risk_engine_error(db_path: Path, monkeypatch):
    event_slug = "nba-nyk-bos-2026-02-10"
    execute_before = "2026-02-11T01:00:00+00:00"
    _insert_job(db_path, event_slug=event_slug, execute_before=execute_before)
    upsert_position_group(
        event_slug=event_slug,
        game_date="2026-02-10",
        state="ACQUIRE",
        d_max=20.0,
        db_path=db_path,
    )
    monkeypatch.setattr(
        "src.scheduler.position_group_manager.settings.position_group_safe_stop_on_risk_error",
        True,
    )
    monkeypatch.setattr(
        "src.risk.risk_engine.load_or_compute_risk_state",
        lambda _db: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    now = datetime(2026, 2, 10, 18, 0, tzinfo=timezone.utc)
    results = process_position_groups(db_path=str(db_path), now_utc=now)
    assert results[0].new_state == "SAFE_STOP"


def test_process_updates_m_and_d_targets(db_path: Path):
    event_slug = "nba-nyk-bos-2026-02-10"
    execute_before = "2026-02-11T01:00:00+00:00"
    _insert_job(db_path, event_slug=event_slug, execute_before=execute_before)
    upsert_position_group(
        event_slug=event_slug,
        game_date="2026-02-10",
        state="ACQUIRE",
        d_max=20.0,
        db_path=db_path,
    )
    _log_sig(
        db_path,
        event_slug=event_slug,
        signal_role="directional",
        kelly_size=10.0,
        poly_price=0.40,
        order_status="paper",
        expected_win_rate=0.62,
        band_confidence="high",
    )
    _log_sig(
        db_path,
        event_slug=event_slug,
        signal_role="hedge",
        kelly_size=10.0,
        poly_price=0.55,
        order_status="paper",
    )

    now = datetime(2026, 2, 10, 18, 0, tzinfo=timezone.utc)
    process_position_groups(db_path=str(db_path), now_utc=now)
    group = get_position_group(event_slug, db_path=db_path)
    assert group is not None
    assert group.D_target > 0
    assert group.M_target > 0


def test_refresh_schedule_creates_position_group_when_enabled(db_path: Path, monkeypatch):
    @dataclass
    class _Game:
        away_team: str
        home_team: str
        game_time_utc: str
        game_status: int

    monkeypatch.setattr("src.scheduler.trade_scheduler.settings.game_position_group_enabled", True)
    monkeypatch.setattr("src.scheduler.trade_scheduler.settings.position_group_default_d_max", 33.0)
    monkeypatch.setattr(
        "src.connectors.nba_schedule.fetch_games_for_date",
        lambda _: [
            _Game(
                away_team="New York Knicks",
                home_team="Boston Celtics",
                game_time_utc="2026-02-11T01:00:00Z",
                game_status=1,
            )
        ],
    )
    monkeypatch.setattr(
        "src.connectors.team_mapping.build_event_slug",
        lambda away, home, game_date: f"nba-nyk-bos-{game_date}",
    )

    inserted = refresh_schedule("2026-02-10", db_path=str(db_path))
    assert inserted == 1

    group = get_position_group("nba-nyk-bos-2026-02-10", db_path=db_path)
    assert group is not None
    assert group.state == "PLANNED"
    assert group.d_max == pytest.approx(33.0)
