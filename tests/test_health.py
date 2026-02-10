"""Tests for health check system (Phase D3)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.store.schema import _connect


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.db"
    conn = _connect(db_path)
    conn.close()
    return str(db_path)


class TestLocalHealth:
    def test_healthy_db(self, tmp_db):
        from src.risk.health import check_local_health

        status = check_local_health(db_path=tmp_db)
        assert status.ok is True
        assert status.checks["db_connection"] is True
        assert status.checks["disk_space"] is True

    def test_bad_db_path(self, tmp_path):
        from src.risk.health import check_local_health

        bad_path = tmp_path / "nonexistent" / "test.db"
        status = check_local_health(db_path=bad_path)
        # DB connection may still succeed (sqlite creates file) or fail
        # The important thing is it doesn't crash
        assert isinstance(status.ok, bool)


class TestIntegrity:
    def test_integrity_ok(self, tmp_db):
        from src.risk.health import check_integrity

        status = check_integrity(db_path=tmp_db)
        assert status.ok is True
        assert status.checks["db_integrity"] is True


class TestCheckHealth:
    def test_tick_0_includes_api(self, tmp_db):
        """tick_count=0 should trigger API check (0 % 5 == 0)."""
        from src.risk.health import check_health

        # Mock API calls to avoid real network
        with patch("src.risk.health.check_api_health") as mock_api:
            from src.risk.health import HealthStatus

            mock_api.return_value = HealthStatus(
                ok=True, checks={"nba_api": True, "polymarket_api": True},
            )
            status = check_health(tick_count=0, db_path=tmp_db)
            mock_api.assert_called_once()
            assert status.ok is True

    def test_tick_3_skips_api(self, tmp_db):
        """tick_count=3 should skip API check."""
        from src.risk.health import check_health

        with patch("src.risk.health.check_api_health") as mock_api:
            check_health(tick_count=3, db_path=tmp_db)
            mock_api.assert_not_called()

    def test_tick_5_includes_api(self, tmp_db):
        """tick_count=5 should trigger API check."""
        from src.risk.health import check_health

        with patch("src.risk.health.check_api_health") as mock_api:
            from src.risk.health import HealthStatus

            mock_api.return_value = HealthStatus(
                ok=True, checks={"nba_api": True},
            )
            check_health(tick_count=5, db_path=tmp_db)
            mock_api.assert_called_once()


class TestTelegramAlerts:
    def test_send_risk_alert_format(self, monkeypatch):
        from src.config import settings
        from src.notifications.telegram import send_risk_alert

        monkeypatch.setattr(settings, "telegram_bot_token", "")
        result = send_risk_alert("YELLOW", "daily_loss=1.5%", daily_pnl=-15.0)
        assert result is False

    def test_send_health_alert_format(self, monkeypatch):
        from src.config import settings
        from src.notifications.telegram import send_health_alert

        monkeypatch.setattr(settings, "telegram_bot_token", "")
        result = send_health_alert(["DB connection failed", "Low disk space"])
        assert result is False

    def test_send_error_alert_format(self, monkeypatch):
        from src.config import settings
        from src.notifications.telegram import send_error_alert

        monkeypatch.setattr(settings, "telegram_bot_token", "")
        result = send_error_alert("OrderFailed", "Insufficient balance")
        assert result is False
