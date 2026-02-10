"""3-tier health check system.

| Check     | Frequency    | Content                      |
|-----------|-------------|------------------------------|
| Local     | Every tick  | DB connection + disk space   |
| API       | Every 5th   | NBA.com + Polymarket reach   |
| Integrity | Daily       | PRAGMA integrity_check       |
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from src.store.schema import DEFAULT_DB_PATH

logger = logging.getLogger(__name__)


@dataclass
class HealthStatus:
    """Aggregated health check result."""

    ok: bool = True
    checks: dict[str, bool] = field(default_factory=dict)
    messages: list[str] = field(default_factory=list)

    def update(self, other: "HealthStatus") -> None:
        self.checks.update(other.checks)
        self.messages.extend(other.messages)
        if not other.ok:
            self.ok = False


def check_local_health(
    db_path: Path | str | None = None,
    min_disk_mb: int = 100,
) -> HealthStatus:
    """Check DB connection and disk space (every tick)."""
    status = HealthStatus()
    path = Path(db_path) if db_path else DEFAULT_DB_PATH

    # DB 接続
    try:
        import sqlite3

        conn = sqlite3.connect(str(path), timeout=5)
        conn.execute("SELECT 1")
        conn.close()
        status.checks["db_connection"] = True
    except Exception as e:
        status.ok = False
        status.checks["db_connection"] = False
        status.messages.append(f"DB connection failed: {e}")

    # ディスク空き容量
    try:
        usage = shutil.disk_usage(path.parent)
        free_mb = usage.free / (1024 * 1024)
        status.checks["disk_space"] = free_mb >= min_disk_mb
        if free_mb < min_disk_mb:
            status.ok = False
            status.messages.append(f"Low disk space: {free_mb:.0f}MB (min: {min_disk_mb}MB)")
    except Exception as e:
        status.checks["disk_space"] = False
        status.messages.append(f"Disk check failed: {e}")

    return status


def check_api_health() -> HealthStatus:
    """Check external API reachability (every 5th tick, ~10 min)."""
    import httpx

    from src.config import settings

    status = HealthStatus()

    # NBA.com
    try:
        resp = httpx.get(settings.nba_scoreboard_url, timeout=10)
        status.checks["nba_api"] = resp.status_code == 200
        if resp.status_code != 200:
            status.ok = False
            status.messages.append(f"NBA.com returned {resp.status_code}")
    except Exception as e:
        status.ok = False
        status.checks["nba_api"] = False
        status.messages.append(f"NBA.com unreachable: {e}")

    # Polymarket Gamma API
    try:
        resp = httpx.get(f"{settings.gamma_api_url}/markets?limit=1", timeout=10)
        status.checks["polymarket_api"] = resp.status_code == 200
        if resp.status_code != 200:
            status.ok = False
            status.messages.append(f"Polymarket returned {resp.status_code}")
    except Exception as e:
        status.ok = False
        status.checks["polymarket_api"] = False
        status.messages.append(f"Polymarket unreachable: {e}")

    return status


def check_integrity(db_path: Path | str | None = None) -> HealthStatus:
    """Run PRAGMA integrity_check (daily)."""
    import sqlite3

    status = HealthStatus()
    path = str(db_path or DEFAULT_DB_PATH)

    try:
        conn = sqlite3.connect(path, timeout=30)
        result = conn.execute("PRAGMA integrity_check").fetchone()
        conn.close()
        ok = result is not None and result[0] == "ok"
        status.checks["db_integrity"] = ok
        if not ok:
            status.ok = False
            status.messages.append(f"DB integrity check failed: {result}")
    except Exception as e:
        status.ok = False
        status.checks["db_integrity"] = False
        status.messages.append(f"DB integrity check error: {e}")

    return status


def check_health(
    tick_count: int = 0,
    db_path: Path | str | None = None,
) -> HealthStatus:
    """Run tiered health checks based on tick count.

    - Local: every tick
    - API: every 5th tick (~10 min)
    - Integrity: every 360th tick (~12 hours at 2-min ticks)
    """
    status = check_local_health(db_path)

    if tick_count % 5 == 0:
        try:
            api_status = check_api_health()
            status.update(api_status)
        except Exception as e:
            logger.warning("API health check failed: %s", e)

    if tick_count > 0 and tick_count % 360 == 0:
        try:
            integrity = check_integrity(db_path)
            status.update(integrity)
        except Exception as e:
            logger.warning("Integrity check failed: %s", e)

    if not status.ok:
        logger.warning("Health check issues: %s", "; ".join(status.messages))

    return status
