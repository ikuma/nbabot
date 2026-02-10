"""Risk management data models.

CircuitBreakerLevel, RiskState, and CalibrationHealthMetrics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum


class CircuitBreakerLevel(IntEnum):
    GREEN = 0   # 通常
    YELLOW = 1  # サイズ縮小
    ORANGE = 2  # 全停止 (自動解除条件あり)
    RED = 3     # 緊急停止 (手動解除のみ)


@dataclass
class RiskState:
    """Snapshot of current risk metrics, persisted to DB between ticks."""

    daily_pnl: float = 0.0
    daily_loss_pct: float = 0.0
    weekly_pnl: float = 0.0
    weekly_loss_pct: float = 0.0
    consecutive_losses: int = 0
    max_drawdown_pct: float = 0.0
    open_exposure: float = 0.0
    current_balance: float = 0.0
    last_known_balance: float = 0.0
    circuit_breaker_level: CircuitBreakerLevel = CircuitBreakerLevel.GREEN
    sizing_multiplier: float = 1.0
    lockout_until: str | None = None
    flags: list[str] = field(default_factory=list)
    checked_at: str = ""


@dataclass
class CalibrationHealthMetrics:
    """Per-band rolling calibration health."""

    band_label: str  # e.g. "0.35-0.40"
    expected_win_rate: float
    rolling_win_rate: float
    sample_size: int
    z_score: float  # 乖離度 (σ)
    drifted: bool  # z_score > threshold
