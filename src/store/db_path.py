"""Database path resolver for execution modes."""

from __future__ import annotations

from pathlib import Path

from src.config import settings
from src.store.schema import DEFAULT_DB_PATH

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _normalize_db_path(path_str: str) -> str:
    p = Path(path_str).expanduser()
    if p.is_absolute():
        return str(p)
    return str((PROJECT_ROOT / p).resolve())


def resolve_db_path(
    *,
    execution_mode: str | None = None,
    explicit_db_path: str | None = None,
) -> str:
    """Resolve DB path with optional explicit override.

    Priority:
    1) explicit_db_path
    2) mode-specific setting (live/paper/dry-run)
    3) DEFAULT_DB_PATH fallback
    """
    if explicit_db_path:
        return _normalize_db_path(explicit_db_path)

    mode = (execution_mode or settings.execution_mode or "paper").strip().lower()
    if mode == "live":
        path = settings.live_db_path
    elif mode == "dry-run":
        path = settings.dry_run_db_path or settings.paper_db_path
    else:
        path = settings.paper_db_path

    if not path:
        return str(DEFAULT_DB_PATH)
    return _normalize_db_path(path)
