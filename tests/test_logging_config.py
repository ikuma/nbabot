"""Tests for structured logging configuration (Phase D3)."""

from __future__ import annotations

import json
import logging


class TestJSONFormatter:
    def test_json_output(self):
        from src.logging_config import JSONFormatter

        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Hello %s",
            args=("world",),
            exc_info=None,
        )
        record.tick_id = "abc123"

        output = formatter.format(record)
        data = json.loads(output)

        assert data["level"] == "INFO"
        assert data["logger"] == "test"
        assert data["msg"] == "Hello world"
        assert data["tick_id"] == "abc123"
        assert "ts" in data

    def test_json_without_tick_id(self):
        from src.logging_config import JSONFormatter

        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname="",
            lineno=0,
            msg="no tick",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        data = json.loads(output)
        assert data["tick_id"] == ""
        assert data["level"] == "WARNING"


class TestSetupLogging:
    def test_setup_returns_tick_id(self, tmp_path):
        from src.logging_config import setup_logging

        tick_id = setup_logging(log_dir=tmp_path)
        assert len(tick_id) == 12
        assert tick_id.isalnum()

        # Reset logging
        logging.getLogger().handlers.clear()

    def test_creates_log_dir(self, tmp_path):
        from src.logging_config import setup_logging

        log_dir = tmp_path / "nested" / "logs"
        setup_logging(log_dir=log_dir)
        assert log_dir.exists()

        # Reset logging
        logging.getLogger().handlers.clear()
