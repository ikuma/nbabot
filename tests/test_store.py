"""Tests for paper-trade signal store (src/store/db.py)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tests.helpers import insert_signal as _insert_signal
from src.store.db import (
    _calc_max_drawdown,
    _calc_sharpe,
    _connect,
    get_all_results,
    get_all_signals,
    get_performance,
    get_unsettled,
    log_result,
)


class TestConnect:
    def test_creates_database(self, db_path: Path):
        """Database file is created on first connect."""
        assert not db_path.exists()
        conn = _connect(db_path)
        conn.close()
        assert db_path.exists()

    def test_creates_tables(self, db_path: Path):
        """Schema creates signals and results tables."""
        conn = _connect(db_path)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t[0] for t in tables]
        conn.close()
        assert "signals" in table_names
        assert "results" in table_names

    def test_idempotent_schema(self, db_path: Path):
        """Connecting twice doesn't fail (CREATE IF NOT EXISTS)."""
        conn1 = _connect(db_path)
        conn1.close()
        conn2 = _connect(db_path)
        conn2.close()


class TestLogSignal:
    def test_returns_row_id(self, db_path: Path):
        sid = _insert_signal(db_path)
        assert sid == 1

    def test_auto_increment(self, db_path: Path):
        s1 = _insert_signal(db_path)
        s2 = _insert_signal(db_path, team="New York Knicks")
        assert s2 == s1 + 1

    def test_signal_persisted(self, db_path: Path):
        _insert_signal(db_path, team="Boston Celtics", edge_pct=12.5)
        signals = get_all_signals(db_path=db_path)
        assert len(signals) == 1
        assert signals[0].team == "Boston Celtics"
        assert signals[0].edge_pct == 12.5


class TestLogResult:
    def test_returns_row_id(self, db_path: Path):
        sid = _insert_signal(db_path)
        rid = log_result(
            signal_id=sid, outcome="Boston Celtics",
            won=True, pnl=40.9, db_path=db_path,
        )
        assert rid == 1

    def test_result_persisted(self, db_path: Path):
        sid = _insert_signal(db_path)
        log_result(
            signal_id=sid,
            outcome="Boston Celtics",
            won=True,
            pnl=40.9,
            settlement_price=1.0,
            db_path=db_path,
        )
        results = get_all_results(db_path=db_path)
        assert len(results) == 1
        assert results[0].won is True
        assert results[0].pnl == pytest.approx(40.9)

    def test_unique_constraint(self, db_path: Path):
        """Cannot settle the same signal twice."""
        sid = _insert_signal(db_path)
        log_result(signal_id=sid, outcome="Boston Celtics", won=True, pnl=40.9, db_path=db_path)
        with pytest.raises(sqlite3.IntegrityError):
            log_result(
                signal_id=sid, outcome="New York Knicks",
                won=False, pnl=-50.0, db_path=db_path,
            )


class TestGetUnsettled:
    def test_all_unsettled(self, db_path: Path):
        _insert_signal(db_path, team="Boston Celtics")
        _insert_signal(db_path, team="New York Knicks")
        unsettled = get_unsettled(db_path=db_path)
        assert len(unsettled) == 2

    def test_settled_excluded(self, db_path: Path):
        s1 = _insert_signal(db_path, team="Boston Celtics")
        _insert_signal(db_path, team="New York Knicks")
        log_result(signal_id=s1, outcome="Boston Celtics", won=True, pnl=40.0, db_path=db_path)
        unsettled = get_unsettled(db_path=db_path)
        assert len(unsettled) == 1
        assert unsettled[0].team == "New York Knicks"

    def test_empty_db(self, db_path: Path):
        # force schema creation
        _connect(db_path).close()
        assert get_unsettled(db_path=db_path) == []


class TestGetPerformance:
    def test_empty_db(self, db_path: Path):
        _connect(db_path).close()
        stats = get_performance(db_path=db_path)
        assert stats.total_signals == 0
        assert stats.settled_count == 0
        assert stats.win_rate == 0.0
        assert stats.sharpe_ratio == 0.0

    def test_with_trades(self, db_path: Path):
        s1 = _insert_signal(db_path, team="Boston Celtics", kelly_size=50)
        s2 = _insert_signal(db_path, team="New York Knicks", kelly_size=30)
        _insert_signal(db_path, team="Los Angeles Lakers", kelly_size=40)

        # Win, Loss, unsettled
        log_result(signal_id=s1, outcome="Boston Celtics", won=True, pnl=40.0, db_path=db_path)
        log_result(signal_id=s2, outcome="Boston Celtics", won=False, pnl=-30.0, db_path=db_path)

        stats = get_performance(db_path=db_path)
        assert stats.total_signals == 3
        assert stats.settled_count == 2
        assert stats.unsettled_count == 1
        assert stats.wins == 1
        assert stats.losses == 1
        assert stats.win_rate == pytest.approx(0.5)
        assert stats.total_pnl == pytest.approx(10.0)
        assert stats.avg_pnl == pytest.approx(5.0)

    def test_all_wins(self, db_path: Path):
        s1 = _insert_signal(db_path, kelly_size=50)
        s2 = _insert_signal(db_path, kelly_size=50)
        log_result(signal_id=s1, outcome="Boston Celtics", won=True, pnl=40.0, db_path=db_path)
        log_result(signal_id=s2, outcome="Boston Celtics", won=True, pnl=35.0, db_path=db_path)

        stats = get_performance(db_path=db_path)
        assert stats.win_rate == 1.0
        assert stats.max_drawdown == 0.0


class TestCalcMaxDrawdown:
    def test_empty(self):
        assert _calc_max_drawdown([]) == 0.0

    def test_no_drawdown(self):
        assert _calc_max_drawdown([10, 20, 30]) == 0.0

    def test_single_drawdown(self):
        # cumulative: 10, 5, 15 → peak=10, dd=5 at step 2
        assert _calc_max_drawdown([10, -5, 10]) == 5.0

    def test_multiple_drawdowns(self):
        # cumulative: 10, 5, 25, 5 → max dd = 25-5 = 20
        assert _calc_max_drawdown([10, -5, 20, -20]) == 20.0


class TestCalcSharpe:
    def test_empty(self):
        assert _calc_sharpe([]) == 0.0

    def test_single_value(self):
        assert _calc_sharpe([10]) == 0.0

    def test_zero_std(self):
        """All same values → std=0 → Sharpe=0."""
        assert _calc_sharpe([5, 5, 5]) == 0.0

    def test_positive_sharpe(self):
        sr = _calc_sharpe([10, 12, 11, 13, 10])
        assert sr > 0

    def test_negative_sharpe(self):
        sr = _calc_sharpe([-10, -12, -11, -13, -10])
        assert sr < 0
