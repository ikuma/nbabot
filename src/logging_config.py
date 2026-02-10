"""Structured logging configuration.

JSON formatter + TimedRotatingFileHandler for scheduler logs.
ContextVar is NOT used — LoggerAdapter with tick_id is sufficient
for a single-process cron model.
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import uuid
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "data" / "logs"


class JSONFormatter(logging.Formatter):
    """JSON structured log formatter with tick_id support."""

    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "ts": self.formatTime(record),
                "level": record.levelname,
                "logger": record.name,
                "msg": record.getMessage(),
                "tick_id": getattr(record, "tick_id", ""),
            },
            ensure_ascii=False,
        )


def setup_logging(
    structured: bool = False,
    log_dir: Path | str | None = None,
) -> str:
    """Configure root logger. Returns the tick_id for this run.

    Args:
        structured: If True, use JSON format. Controlled by STRUCTURED_LOGGING env var.
        log_dir: Override log directory. Defaults to data/logs/.
    """
    tick_id = uuid.uuid4().hex[:12]
    log_path = Path(log_dir) if log_dir else LOG_DIR
    log_path.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # 既存ハンドラをクリア (重複防止)
    root.handlers.clear()

    # Console handler (human-readable)
    console = logging.StreamHandler()
    console.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    root.addHandler(console)

    # File handler (daily rotation, 30 days retention)
    use_structured = structured or os.environ.get("STRUCTURED_LOGGING", "").lower() in ("true", "1")
    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_path / "scheduler.log",
        when="midnight",
        backupCount=30,
        encoding="utf-8",
    )
    if use_structured:
        file_handler.setFormatter(JSONFormatter())
    else:
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
    root.addHandler(file_handler)

    return tick_id
