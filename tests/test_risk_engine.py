"""Tests for the risk engine: circuit breaker, sizing multiplier, calibration drift."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest  # noqa: F401

from src.risk.models import CircuitBreakerLevel, RiskState
from src.store.schema import _connect

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_db(tmp_path):
    """Create a temporary SQLite database with schema."""
    db_path = tmp_path / "test.db"
    conn = _connect(db_path)
    conn.close()
    return str(db_path)


def _insert_signal(db_path, event_slug="nba-nyk-bos-2026-02-10", kelly_size=10.0,
                   price_band="0.35-0.40", signal_role="directional", strategy_mode="calibration"):
    conn = _connect(db_path)
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        """INSERT INTO signals
           (game_title, event_slug, team, side, poly_price, book_prob,
            edge_pct, kelly_size, token_id, created_at,
            price_band, signal_role, strategy_mode)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        ("NYK vs BOS", event_slug, "Knicks", "BUY", 0.40, 0.90,
         10.0, kelly_size, "token123", now, price_band, signal_role, strategy_mode),
    )
    conn.commit()
    signal_id = cur.lastrowid
    conn.close()
    return signal_id


def _insert_result(db_path, signal_id, won, pnl, settled_at=None):
    conn = _connect(db_path)
    now = settled_at or datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT INTO results (signal_id, outcome, won, pnl, settled_at)
           VALUES (?, ?, ?, ?, ?)""",
        (signal_id, "Knicks" if won else "Celtics", int(won), pnl, now),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# CircuitBreakerLevel tests
# ---------------------------------------------------------------------------


class TestCircuitBreakerLevel:
    def test_level_ordering(self):
        assert CircuitBreakerLevel.GREEN < CircuitBreakerLevel.YELLOW
        assert CircuitBreakerLevel.YELLOW < CircuitBreakerLevel.ORANGE
        assert CircuitBreakerLevel.ORANGE < CircuitBreakerLevel.RED

    def test_level_values(self):
        assert int(CircuitBreakerLevel.GREEN) == 0
        assert int(CircuitBreakerLevel.RED) == 3


# ---------------------------------------------------------------------------
# evaluate_circuit_breaker tests
# ---------------------------------------------------------------------------


class TestEvaluateCircuitBreaker:
    def test_green_no_issues(self):
        from src.risk.risk_engine import evaluate_circuit_breaker

        state = RiskState(current_balance=1000.0)
        level, trigger = evaluate_circuit_breaker(state)
        assert level == CircuitBreakerLevel.GREEN
        assert trigger == ""

    def test_yellow_daily_loss_half(self):
        from src.risk.risk_engine import evaluate_circuit_breaker

        # daily_loss_limit_pct default = 3.0, half = 1.5
        state = RiskState(daily_loss_pct=1.5, current_balance=1000.0)
        level, trigger = evaluate_circuit_breaker(state)
        assert level == CircuitBreakerLevel.YELLOW
        assert "daily_loss" in trigger

    def test_yellow_consecutive_losses(self):
        from src.risk.risk_engine import evaluate_circuit_breaker

        state = RiskState(consecutive_losses=5, current_balance=1000.0)
        level, trigger = evaluate_circuit_breaker(state)
        assert level == CircuitBreakerLevel.YELLOW
        assert "consecutive_losses" in trigger

    def test_orange_daily_loss_full(self):
        from src.risk.risk_engine import evaluate_circuit_breaker

        state = RiskState(daily_loss_pct=3.0, current_balance=1000.0)
        level, trigger = evaluate_circuit_breaker(state)
        assert level == CircuitBreakerLevel.ORANGE
        assert "daily_loss" in trigger

    def test_orange_calibration_drift(self):
        from src.risk.risk_engine import evaluate_circuit_breaker

        state = RiskState(flags=["calibration_drift"], current_balance=1000.0)
        level, trigger = evaluate_circuit_breaker(state)
        assert level == CircuitBreakerLevel.ORANGE
        assert "calibration_drift" in trigger

    def test_red_weekly_loss(self):
        from src.risk.risk_engine import evaluate_circuit_breaker

        state = RiskState(weekly_loss_pct=5.0, current_balance=1000.0)
        level, trigger = evaluate_circuit_breaker(state)
        assert level == CircuitBreakerLevel.RED
        assert "weekly_loss" in trigger

    def test_red_drawdown(self):
        from src.risk.risk_engine import evaluate_circuit_breaker

        state = RiskState(max_drawdown_pct=15.0, current_balance=1000.0)
        level, trigger = evaluate_circuit_breaker(state)
        assert level == CircuitBreakerLevel.RED
        assert "drawdown" in trigger

    def test_boundary_daily_loss_just_below(self):
        """daily_loss_pct = 2.99 → not ORANGE."""
        from src.risk.risk_engine import evaluate_circuit_breaker

        state = RiskState(daily_loss_pct=2.99, current_balance=1000.0)
        level, _ = evaluate_circuit_breaker(state)
        assert level == CircuitBreakerLevel.YELLOW  # >= 1.5

    def test_boundary_daily_loss_exact(self):
        """daily_loss_pct = 3.00 → ORANGE."""
        from src.risk.risk_engine import evaluate_circuit_breaker

        state = RiskState(daily_loss_pct=3.00, current_balance=1000.0)
        level, _ = evaluate_circuit_breaker(state)
        assert level == CircuitBreakerLevel.ORANGE

    def test_boundary_daily_loss_just_above(self):
        """daily_loss_pct = 3.01 → ORANGE."""
        from src.risk.risk_engine import evaluate_circuit_breaker

        state = RiskState(daily_loss_pct=3.01, current_balance=1000.0)
        level, _ = evaluate_circuit_breaker(state)
        assert level == CircuitBreakerLevel.ORANGE

    def test_red_takes_priority_over_orange(self):
        """When both weekly loss and daily loss trigger, RED wins."""
        from src.risk.risk_engine import evaluate_circuit_breaker

        state = RiskState(daily_loss_pct=3.5, weekly_loss_pct=5.5, current_balance=1000.0)
        level, _ = evaluate_circuit_breaker(state)
        assert level == CircuitBreakerLevel.RED


# ---------------------------------------------------------------------------
# sizing_multiplier tests
# ---------------------------------------------------------------------------


class TestSizingMultiplier:
    def test_green_full_size(self):
        from src.risk.risk_engine import get_sizing_multiplier

        assert get_sizing_multiplier(CircuitBreakerLevel.GREEN) == 1.0

    def test_yellow_half_size(self):
        from src.risk.risk_engine import get_sizing_multiplier

        assert get_sizing_multiplier(CircuitBreakerLevel.YELLOW) == 0.5

    def test_orange_zero(self):
        from src.risk.risk_engine import get_sizing_multiplier

        assert get_sizing_multiplier(CircuitBreakerLevel.ORANGE) == 0.0

    def test_red_zero(self):
        from src.risk.risk_engine import get_sizing_multiplier

        assert get_sizing_multiplier(CircuitBreakerLevel.RED) == 0.0

    def test_orange_to_yellow_conservative(self):
        """Stepping down from ORANGE to YELLOW → 0.25x."""
        from src.risk.risk_engine import get_sizing_multiplier

        mult = get_sizing_multiplier(
            CircuitBreakerLevel.YELLOW,
            prev_level=CircuitBreakerLevel.ORANGE,
        )
        assert mult == 0.25


# ---------------------------------------------------------------------------
# can_trade tests
# ---------------------------------------------------------------------------


class TestCanTrade:
    def test_can_trade_green(self, tmp_db, monkeypatch):
        from src.risk import risk_engine

        monkeypatch.setattr(risk_engine, "_cached_state", None)
        monkeypatch.setattr(risk_engine, "_cached_at", 0.0)
        monkeypatch.setattr(
            risk_engine, "compute_risk_state",
            lambda db_path: RiskState(
                circuit_breaker_level=CircuitBreakerLevel.GREEN,
                sizing_multiplier=1.0,
            ),
        )

        allowed, reason, mult = risk_engine.can_trade(tmp_db)
        assert allowed is True
        assert mult == 1.0

    def test_can_trade_orange_blocked(self, tmp_db, monkeypatch):
        from src.risk import risk_engine

        monkeypatch.setattr(risk_engine, "_cached_state", None)
        monkeypatch.setattr(risk_engine, "_cached_at", 0.0)
        monkeypatch.setattr(
            risk_engine, "compute_risk_state",
            lambda db_path: RiskState(
                circuit_breaker_level=CircuitBreakerLevel.ORANGE,
                sizing_multiplier=0.0,
            ),
        )

        allowed, reason, mult = risk_engine.can_trade(tmp_db)
        assert allowed is False
        assert "ORANGE" in reason

    def test_degraded_mode_on_error(self, tmp_db, monkeypatch):
        from src.risk import risk_engine

        monkeypatch.setattr(risk_engine, "_cached_state", None)
        monkeypatch.setattr(risk_engine, "_cached_at", 0.0)

        def _raise(*args, **kwargs):
            raise RuntimeError("DB error")

        monkeypatch.setattr(risk_engine, "compute_risk_state", _raise)

        allowed, reason, mult = risk_engine.can_trade(tmp_db)
        assert allowed is True
        assert reason == "degraded_mode"
        assert mult == 0.5

    def test_disabled_risk_check(self, tmp_db, monkeypatch):
        from src.config import settings
        from src.risk import risk_engine

        monkeypatch.setattr(settings, "risk_check_enabled", False)
        allowed, reason, mult = risk_engine.can_trade(tmp_db)
        assert allowed is True
        assert mult == 1.0


# ---------------------------------------------------------------------------
# balance anomaly tests
# ---------------------------------------------------------------------------


class TestBalanceAnomaly:
    def test_no_anomaly(self):
        from src.risk.risk_engine import detect_balance_anomaly

        assert detect_balance_anomaly(950, 1000) is False

    def test_anomaly_10pct(self):
        from src.risk.risk_engine import detect_balance_anomaly

        assert detect_balance_anomaly(900, 1000) is True

    def test_anomaly_large_drop(self):
        from src.risk.risk_engine import detect_balance_anomaly

        assert detect_balance_anomaly(500, 1000) is True

    def test_no_previous_balance(self):
        from src.risk.risk_engine import detect_balance_anomaly

        assert detect_balance_anomaly(1000, 0) is False


# ---------------------------------------------------------------------------
# DB query tests
# ---------------------------------------------------------------------------


class TestRiskDBQueries:
    def test_empty_db_green(self, tmp_db):
        from src.store.db import get_consecutive_losses, get_daily_results, get_open_exposure

        daily = get_daily_results("2026-02-10", db_path=tmp_db)
        assert daily["pnl"] == 0.0
        assert daily["wins"] == 0

        losses = get_consecutive_losses(db_path=tmp_db)
        assert losses == 0

        exposure = get_open_exposure(db_path=tmp_db)
        assert exposure == 0.0

    def test_daily_results(self, tmp_db):
        from src.store.db import get_daily_results

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        sid = _insert_signal(tmp_db)
        _insert_result(tmp_db, sid, won=True, pnl=5.0)

        daily = get_daily_results(today, db_path=tmp_db)
        assert daily["pnl"] == 5.0
        assert daily["wins"] == 1

    def test_consecutive_losses(self, tmp_db):
        from src.store.db import get_consecutive_losses

        # 3 losses then 1 win (most recent first)
        for i in range(4):
            sid = _insert_signal(tmp_db, event_slug=f"nba-nyk-bos-2026-02-{10+i:02d}")
            _insert_result(tmp_db, sid, won=(i == 0), pnl=-5.0 if i > 0 else 5.0,
                          settled_at=f"2026-02-{10+i:02d}T12:00:00+00:00")

        losses = get_consecutive_losses(db_path=tmp_db)
        assert losses == 3

    def test_open_exposure(self, tmp_db):
        from src.store.db import get_open_exposure

        _insert_signal(tmp_db, kelly_size=25.0)
        _insert_signal(tmp_db, event_slug="nba-lal-gsw-2026-02-10", kelly_size=30.0)

        exposure = get_open_exposure(db_path=tmp_db)
        assert exposure == 55.0


# ---------------------------------------------------------------------------
# RiskState persistence roundtrip
# ---------------------------------------------------------------------------


class TestRiskStatePersistence:
    def test_save_and_load(self, tmp_db):
        from src.store.db import get_latest_risk_snapshot, save_risk_snapshot

        state = RiskState(
            daily_pnl=-15.0,
            weekly_pnl=-25.0,
            consecutive_losses=3,
            max_drawdown_pct=2.5,
            open_exposure=100.0,
            current_balance=1000.0,
            last_known_balance=1020.0,
            circuit_breaker_level=CircuitBreakerLevel.YELLOW,
            sizing_multiplier=0.5,
            lockout_until=None,
            flags=["calibration_drift"],
            checked_at="2026-02-10T12:00:00+00:00",
        )

        save_risk_snapshot(state, db_path=tmp_db)
        loaded = get_latest_risk_snapshot(db_path=tmp_db)

        assert loaded is not None
        assert loaded.daily_pnl == -15.0
        assert loaded.weekly_pnl == -25.0
        assert loaded.consecutive_losses == 3
        assert loaded.circuit_breaker_level == CircuitBreakerLevel.YELLOW
        assert loaded.sizing_multiplier == 0.5
        assert "calibration_drift" in loaded.flags

    def test_load_empty_db(self, tmp_db):
        from src.store.db import get_latest_risk_snapshot

        loaded = get_latest_risk_snapshot(db_path=tmp_db)
        assert loaded is None


# ---------------------------------------------------------------------------
# Calibration drift detection
# ---------------------------------------------------------------------------


class TestCalibrationDrift:
    def test_z_score_computation(self):
        from src.risk.calibration_monitor import _z_score

        # Expected 0.90, observed 0.70, n=25
        z = _z_score(0.70, 0.90, 25)
        assert z < -2.0  # significant negative drift

    def test_z_score_no_drift(self):
        from src.risk.calibration_monitor import _z_score

        # Expected 0.90, observed 0.88, n=25
        z = _z_score(0.88, 0.90, 25)
        assert z > -2.0  # within normal range

    def test_should_pause_band_sufficient_sample(self):
        from src.risk.calibration_monitor import should_pause_band
        from src.strategy.calibration import CalibrationBand

        band = CalibrationBand(0.35, 0.40, 0.904, 26.1, 104, "high")
        assert should_pause_band(band, 0.60, 25) is True  # severe drift

    def test_should_pause_band_insufficient_sample(self):
        from src.risk.calibration_monitor import should_pause_band
        from src.strategy.calibration import CalibrationBand

        band = CalibrationBand(0.35, 0.40, 0.904, 26.1, 104, "high")
        assert should_pause_band(band, 0.60, 10) is False  # too few samples

    def test_should_not_pause_healthy_band(self):
        from src.risk.calibration_monitor import should_pause_band
        from src.strategy.calibration import CalibrationBand

        band = CalibrationBand(0.35, 0.40, 0.904, 26.1, 104, "high")
        assert should_pause_band(band, 0.90, 30) is False


# ---------------------------------------------------------------------------
# DCA force stop
# ---------------------------------------------------------------------------


class TestDCAForceStop:
    def test_force_stop_dca_jobs(self, tmp_db):
        from src.store.db import force_stop_dca_jobs

        conn = _connect(tmp_db)
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """INSERT INTO trade_jobs
               (game_date, event_slug, home_team, away_team, game_time_utc,
                execute_after, execute_before, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'dca_active', ?, ?)""",
            ("2026-02-10", "nba-nyk-bos-2026-02-10", "Celtics", "Knicks",
             "2026-02-10T20:00:00Z", "2026-02-10T12:00:00Z", "2026-02-10T20:00:00Z",
             now, now),
        )
        conn.commit()
        conn.close()

        stopped = force_stop_dca_jobs(db_path=tmp_db)
        assert stopped == 1

        conn = _connect(tmp_db)
        row = conn.execute("SELECT status FROM trade_jobs LIMIT 1").fetchone()
        conn.close()
        assert row["status"] == "executed"


# ---------------------------------------------------------------------------
# position_sizer sizing_multiplier
# ---------------------------------------------------------------------------


class TestSizingMultiplierIntegration:
    def test_half_kelly_budget(self):
        from src.sizing.position_sizer import calculate_dca_budget

        full = calculate_dca_budget(kelly_usd=50.0, num_entries=5, sizing_multiplier=1.0)
        half = calculate_dca_budget(kelly_usd=50.0, num_entries=5, sizing_multiplier=0.5)

        assert half.total_budget_usd == pytest.approx(full.total_budget_usd * 0.5, abs=0.5)

    def test_zero_multiplier(self):
        from src.sizing.position_sizer import calculate_dca_budget

        budget = calculate_dca_budget(kelly_usd=50.0, num_entries=5, sizing_multiplier=0.0)
        assert budget.total_budget_usd == 0.0
        assert budget.slice_size_usd == 0.0
