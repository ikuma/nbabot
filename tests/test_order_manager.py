"""Tests for Phase O: Order lifecycle manager (src/scheduler/order_manager.py)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.store.db import (
    _connect,
    get_active_placed_orders,
    get_order_events,
    log_order_event,
    log_signal,
    update_order_lifecycle,
    update_order_status,
    upsert_trade_job,
)
from src.store.models import OrderEvent, SignalRecord


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    """Temporary database path for each test."""
    return tmp_path / "test_order_manager.db"


def _make_signal(db_path: Path, event_slug: str = "nba-nyk-bos-2026-02-15", **kw) -> int:
    """Insert a signal with defaults and return its ID."""
    defaults = dict(
        game_title="Knicks vs Celtics",
        event_slug=event_slug,
        team="Celtics",
        side="BUY",
        poly_price=0.45,
        book_prob=0.0,
        edge_pct=5.0,
        kelly_size=25.0,
        token_id="tok_bos_123",
        strategy_mode="calibration",
        signal_role="directional",
        db_path=db_path,
    )
    defaults.update(kw)
    return log_signal(**defaults)


def _make_trade_job(db_path: Path, event_slug: str = "nba-nyk-bos-2026-02-15", **kw) -> bool:
    """Insert a trade job with defaults."""
    now = datetime.now(timezone.utc)
    defaults = dict(
        game_date="2026-02-15",
        event_slug=event_slug,
        home_team="Boston Celtics",
        away_team="New York Knicks",
        game_time_utc=(now + timedelta(hours=4)).isoformat(),
        execute_after=(now - timedelta(hours=8)).isoformat(),
        execute_before=(now + timedelta(hours=4)).isoformat(),
        job_side="directional",
        db_path=db_path,
    )
    defaults.update(kw)
    return upsert_trade_job(**defaults)


class TestSchemaCreation:
    """order_events table and lifecycle columns are created on connect."""

    def test_order_events_table_exists(self, db_path: Path):
        conn = _connect(db_path)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='order_events'"
        ).fetchall()
        conn.close()
        assert len(tables) == 1

    def test_signals_lifecycle_columns(self, db_path: Path):
        conn = _connect(db_path)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
        conn.close()
        assert "order_placed_at" in cols
        assert "order_replace_count" in cols
        assert "order_last_checked_at" in cols
        assert "order_original_price" in cols


class TestOrderLifecycleDB:
    """DB helper functions for order lifecycle."""

    def test_update_order_lifecycle(self, db_path: Path):
        sig_id = _make_signal(db_path)
        now_iso = datetime.now(timezone.utc).isoformat()

        update_order_lifecycle(
            sig_id,
            order_placed_at=now_iso,
            order_original_price=0.44,
            order_replace_count=1,
            db_path=db_path,
        )

        conn = _connect(db_path)
        row = conn.execute("SELECT * FROM signals WHERE id = ?", (sig_id,)).fetchone()
        conn.close()
        assert row["order_placed_at"] == now_iso
        assert row["order_original_price"] == 0.44
        assert row["order_replace_count"] == 1

    def test_log_order_event(self, db_path: Path):
        sig_id = _make_signal(db_path)
        eid = log_order_event(
            signal_id=sig_id,
            event_type="placed",
            order_id="order_abc",
            price=0.44,
            best_ask_at_event=0.45,
            db_path=db_path,
        )
        assert eid > 0

        events = get_order_events(sig_id, db_path=db_path)
        assert len(events) == 1
        assert events[0].event_type == "placed"
        assert events[0].order_id == "order_abc"
        assert events[0].price == 0.44
        assert events[0].best_ask_at_event == 0.45

    def test_get_active_placed_orders(self, db_path: Path):
        slug = "nba-nyk-bos-2026-02-15"
        _make_trade_job(db_path, event_slug=slug)
        sig_id = _make_signal(db_path, event_slug=slug)

        update_order_status(sig_id, "order_123", "placed", db_path=db_path)

        active = get_active_placed_orders(db_path=db_path)
        assert len(active) == 1
        assert active[0].id == sig_id
        assert active[0].order_id == "order_123"

    @patch("src.connectors.polymarket.cancel_order", return_value=True)
    @patch("src.connectors.polymarket.get_order_status", return_value={"status": "open"})
    def test_get_active_placed_orders_includes_past_tipoff_and_expires(
        self,
        _mock_status,
        _mock_cancel,
        db_path: Path,
    ):
        """Past-tipoff orders should be picked up and expired by order manager."""
        slug = "nba-nyk-bos-2026-02-14"
        now = datetime.now(timezone.utc)
        _make_trade_job(
            db_path,
            event_slug=slug,
            game_date="2026-02-14",
            game_time_utc=(now - timedelta(hours=1)).isoformat(),
            execute_before=(now - timedelta(hours=1)).isoformat(),
        )
        sig_id = _make_signal(db_path, event_slug=slug)
        update_order_status(sig_id, "order_old", "placed", db_path=db_path)

        active = get_active_placed_orders(db_path=db_path)
        assert len(active) == 1
        assert active[0].id == sig_id

        from src.scheduler.order_manager import check_single_order

        result = check_single_order(active[0], str(db_path))
        assert result.action == "expired"


class TestCheckSingleOrder:
    """Unit tests for check_single_order."""

    def _make_placed_signal(self, db_path: Path, **kw) -> SignalRecord:
        """Create a placed signal with trade job and return it."""
        slug = kw.pop("event_slug", "nba-nyk-bos-2026-02-15")
        _make_trade_job(db_path, event_slug=slug)
        sig_id = _make_signal(db_path, event_slug=slug, **kw)
        now_iso = datetime.now(timezone.utc).isoformat()
        update_order_status(sig_id, "order_test_123", "placed", db_path=db_path)
        update_order_lifecycle(
            sig_id,
            order_placed_at=(datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(),
            order_original_price=0.44,
            db_path=db_path,
        )
        active = get_active_placed_orders(db_path=db_path)
        return [s for s in active if s.id == sig_id][0]

    @patch("src.connectors.polymarket.get_order_status")
    @patch("src.connectors.polymarket.cancel_order")
    def test_filled_order_detected(self, mock_cancel, mock_status, db_path: Path):
        signal = self._make_placed_signal(db_path)
        mock_status.return_value = {"status": "matched", "price": "0.44"}

        from src.scheduler.order_manager import check_single_order

        result = check_single_order(signal, str(db_path))
        assert result.action == "filled"
        assert result.fill_price == 0.44
        mock_cancel.assert_not_called()

        # order_events にもイベントが記録される
        events = get_order_events(signal.id, db_path=db_path)
        assert any(e.event_type == "filled" for e in events)

    @patch("src.connectors.polymarket.get_order_status")
    def test_already_cancelled(self, mock_status, db_path: Path):
        signal = self._make_placed_signal(db_path)
        mock_status.return_value = {"status": "cancelled"}

        from src.scheduler.order_manager import check_single_order

        result = check_single_order(signal, str(db_path))
        assert result.action == "cancelled"

    @patch("src.scheduler.order_manager._get_best_ask", return_value=None)
    @patch("src.connectors.polymarket.get_order_status")
    def test_ttl_not_expired_kept(self, mock_status, mock_ask, db_path: Path):
        """Order within TTL should be kept."""
        slug = "nba-nyk-bos-2026-02-16"
        _make_trade_job(db_path, event_slug=slug, game_date="2026-02-16")
        sig_id = _make_signal(db_path, event_slug=slug)
        # order_placed_at = 1 minute ago (TTL = 5 min)
        now = datetime.now(timezone.utc)
        update_order_status(sig_id, "order_fresh", "placed", db_path=db_path)
        update_order_lifecycle(
            sig_id,
            order_placed_at=(now - timedelta(minutes=1)).isoformat(),
            order_original_price=0.44,
            db_path=db_path,
        )
        active = get_active_placed_orders(db_path=db_path)
        signal = [s for s in active if s.id == sig_id][0]

        mock_status.return_value = {"status": "open"}

        from src.scheduler.order_manager import check_single_order

        result = check_single_order(signal, str(db_path))
        assert result.action == "kept"

    @patch("src.scheduler.order_manager._get_best_ask", return_value=0.48)
    @patch("src.connectors.polymarket.cancel_and_replace_order")
    @patch("src.connectors.polymarket.get_order_status")
    def test_replace_on_price_move(self, mock_status, mock_replace, mock_ask, db_path: Path):
        """Order past TTL with price move should be replaced."""
        signal = self._make_placed_signal(db_path)
        mock_status.return_value = {"status": "open"}
        mock_replace.return_value = {"orderID": "new_order_456"}

        from src.scheduler.order_manager import check_single_order

        result = check_single_order(signal, str(db_path))
        assert result.action == "replaced"
        assert result.new_order_id == "new_order_456"
        mock_replace.assert_called_once()

    @patch("src.scheduler.order_manager._get_best_ask", return_value=0.48)
    @patch("src.connectors.polymarket.cancel_order")
    @patch("src.connectors.polymarket.get_order_status")
    def test_max_replaces_expired(self, mock_status, mock_cancel, mock_ask, db_path: Path):
        """Order at max replaces should be expired."""
        slug = "nba-nyk-bos-2026-02-17"
        _make_trade_job(db_path, event_slug=slug, game_date="2026-02-17")
        sig_id = _make_signal(db_path, event_slug=slug)
        update_order_status(sig_id, "order_old", "placed", db_path=db_path)
        update_order_lifecycle(
            sig_id,
            order_placed_at=(datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(),
            order_original_price=0.44,
            order_replace_count=3,  # max replaces
            db_path=db_path,
        )
        active = get_active_placed_orders(db_path=db_path)
        signal = [s for s in active if s.id == sig_id][0]

        mock_status.return_value = {"status": "open"}
        mock_cancel.return_value = True

        from src.scheduler.order_manager import check_single_order

        result = check_single_order(signal, str(db_path))
        assert result.action == "expired"
        mock_cancel.assert_called_once()


class TestCheckAndManageOrders:
    """Integration tests for the main entry point."""

    @patch("src.scheduler.order_manager.get_active_placed_orders", return_value=[])
    def test_no_orders_returns_empty(self, mock_get, db_path: Path):
        from src.scheduler.order_manager import check_and_manage_orders

        summary = check_and_manage_orders(execution_mode="live", db_path=str(db_path))
        assert summary.checked == 0

    def test_paper_mode_skipped(self, db_path: Path):
        from src.scheduler.order_manager import check_and_manage_orders

        summary = check_and_manage_orders(execution_mode="paper", db_path=str(db_path))
        assert summary.checked == 0

    @patch("src.scheduler.order_manager.settings")
    def test_disabled_skipped(self, mock_settings, db_path: Path):
        mock_settings.order_manager_enabled = False

        from src.scheduler.order_manager import check_and_manage_orders

        summary = check_and_manage_orders(execution_mode="live", db_path=str(db_path))
        assert summary.checked == 0


class TestSignalRecordFields:
    """Verify new SignalRecord fields work correctly."""

    def test_default_values(self):
        """New fields have correct defaults."""
        record = SignalRecord(
            id=1,
            game_title="Test",
            event_slug="nba-nyk-bos-2026-02-15",
            team="Celtics",
            side="BUY",
            poly_price=0.45,
            book_prob=0.0,
            edge_pct=5.0,
            kelly_size=25.0,
            token_id="tok_123",
            bookmakers_count=0,
            consensus_std=0.0,
            commence_time="",
            created_at="2026-02-15T00:00:00+00:00",
        )
        assert record.order_placed_at is None
        assert record.order_replace_count == 0
        assert record.order_last_checked_at is None
        assert record.order_original_price is None


class TestOrderEventModel:
    """OrderEvent dataclass tests."""

    def test_roundtrip(self, db_path: Path):
        """OrderEvent can be created and read back from DB."""
        sig_id = _make_signal(db_path)
        log_order_event(
            signal_id=sig_id,
            event_type="replaced",
            order_id="order_old",
            price=0.43,
            best_ask_at_event=0.45,
            db_path=db_path,
        )
        log_order_event(
            signal_id=sig_id,
            event_type="placed",
            order_id="order_new",
            price=0.44,
            best_ask_at_event=0.45,
            db_path=db_path,
        )

        events = get_order_events(sig_id, db_path=db_path)
        assert len(events) == 2
        assert events[0].event_type == "replaced"
        assert events[1].event_type == "placed"
        assert isinstance(events[0], OrderEvent)
