"""Core risk engine: circuit breaker evaluation and trade gating.

Computes RiskState from DB, evaluates circuit breaker level, and provides
can_trade() as the single entry point for the scheduler.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.config import settings
from src.risk.models import CircuitBreakerLevel, RiskState

logger = logging.getLogger(__name__)

# --- tick 内キャッシュ (TTL 30s, cron 終了で自動消滅) ---
_cached_state: RiskState | None = None
_cached_at: float = 0.0
_CACHE_TTL = 30.0

# --- ハードコード定数 (Config パラメータ削減) ---
CONSECUTIVE_LOSS_TRIGGER = 5
YELLOW_SIZING = 0.5
ORANGE_LOCKOUT_HOURS = 24
RED_LOCKOUT_HOURS = 72
RECOVERY_DAYS_FULL = 7
BALANCE_ANOMALY_PCT = 10.0  # 10%+ 急減で警告


def compute_risk_state(db_path: Path | str) -> RiskState:
    """Compute all risk metrics in a single pass from DB.

    Balance fetched from external API; falls back to last known on failure.
    """
    from src.store.db import (
        get_consecutive_losses,
        get_daily_results,
        get_latest_risk_snapshot,
        get_open_exposure,
        get_weekly_results,
    )

    now = datetime.now(timezone.utc)
    today_str = now.strftime("%Y-%m-%d")

    # 前回 snapshot を取得 (balance フォールバック用)
    prev = get_latest_risk_snapshot(db_path=db_path)
    last_balance = prev.last_known_balance if prev else 0.0

    # 残高取得
    current_balance = _fetch_balance_safe(last_balance)

    # 日次 P&L
    daily = get_daily_results(today_str, db_path=db_path)
    daily_pnl = daily["pnl"]
    if current_balance > 0 and daily_pnl < 0:
        daily_loss_pct = abs(daily_pnl / current_balance * 100)
    else:
        daily_loss_pct = 0.0

    # 週次 P&L
    weekly = get_weekly_results(today_str, db_path=db_path)
    weekly_pnl = weekly["pnl"]
    if current_balance > 0 and weekly_pnl < 0:
        weekly_loss_pct = abs(weekly_pnl / current_balance * 100)
    else:
        weekly_loss_pct = 0.0

    # 連敗
    consecutive_losses = get_consecutive_losses(db_path=db_path)

    # 最大ドローダウン (直近の累積 PnL ベース)
    max_drawdown_pct = _compute_drawdown_pct(daily_pnl, weekly_pnl, current_balance)

    # オープンエクスポージャー
    open_exposure = get_open_exposure(db_path=db_path)

    # 前回の CB レベル + lockout を引き継ぐ
    prev_level = prev.circuit_breaker_level if prev else CircuitBreakerLevel.GREEN
    prev_lockout = prev.lockout_until if prev else None
    prev_multiplier = prev.sizing_multiplier if prev else 1.0

    # 校正ドリフトフラグ
    flags: list[str] = []
    try:
        from src.risk.calibration_monitor import compute_calibration_health

        health = compute_calibration_health(db_path)
        drifted_bands = [h for h in health if h.drifted]
        if drifted_bands:
            flags.append("calibration_drift")
    except Exception:
        logger.warning("Calibration health check failed, skipping")

    # 残高急変検出
    if detect_balance_anomaly(current_balance, last_balance):
        flags.append("balance_anomaly")

    state = RiskState(
        daily_pnl=daily_pnl,
        daily_loss_pct=daily_loss_pct,
        weekly_pnl=weekly_pnl,
        weekly_loss_pct=weekly_loss_pct,
        consecutive_losses=consecutive_losses,
        max_drawdown_pct=max_drawdown_pct,
        open_exposure=open_exposure,
        current_balance=current_balance,
        last_known_balance=last_balance,
        circuit_breaker_level=prev_level,
        sizing_multiplier=prev_multiplier,
        lockout_until=prev_lockout,
        flags=flags,
        checked_at=now.isoformat(),
    )

    # CB 再評価
    new_level, trigger = evaluate_circuit_breaker(state)
    if new_level != prev_level:
        state.circuit_breaker_level = new_level
        # ロックアウト設定
        if new_level == CircuitBreakerLevel.ORANGE:
            state.lockout_until = (now + timedelta(hours=ORANGE_LOCKOUT_HOURS)).isoformat()
        elif new_level == CircuitBreakerLevel.RED:
            state.lockout_until = (now + timedelta(hours=RED_LOCKOUT_HOURS)).isoformat()
        elif new_level < prev_level:
            # 降格時はロック解除
            state.lockout_until = None

    # 段階的復帰の sizing_multiplier
    state.sizing_multiplier = get_sizing_multiplier(
        state.circuit_breaker_level, prev_level, prev_multiplier,
    )

    return state


def evaluate_circuit_breaker(
    state: RiskState,
) -> tuple[CircuitBreakerLevel, str]:
    """Pure function: evaluate CB level from current metrics.

    Returns (level, trigger_reason).
    """
    daily_limit = settings.daily_loss_limit_pct
    weekly_limit = settings.weekly_loss_limit_pct
    dd_limit = settings.max_drawdown_limit_pct

    # RED: 週次損失 >= 5% OR ドローダウン >= 15%
    if state.weekly_loss_pct >= weekly_limit:
        return CircuitBreakerLevel.RED, f"weekly_loss={state.weekly_loss_pct:.1f}%>={weekly_limit}%"
    if state.max_drawdown_pct >= dd_limit:
        return CircuitBreakerLevel.RED, f"drawdown={state.max_drawdown_pct:.1f}%>={dd_limit}%"

    # ORANGE: 日次損失 >= 限度 OR 校正ドリフト
    if state.daily_loss_pct >= daily_limit:
        return CircuitBreakerLevel.ORANGE, f"daily_loss={state.daily_loss_pct:.1f}%>={daily_limit}%"
    if "calibration_drift" in state.flags:
        return CircuitBreakerLevel.ORANGE, "calibration_drift"

    # YELLOW: 日次損失 >= 限度の50% OR 連敗 >= 5
    half_limit = daily_limit * 0.5
    if state.daily_loss_pct >= half_limit:
        return CircuitBreakerLevel.YELLOW, (
            f"daily_loss={state.daily_loss_pct:.1f}%>={half_limit:.1f}%"
        )
    if state.consecutive_losses >= CONSECUTIVE_LOSS_TRIGGER:
        return CircuitBreakerLevel.YELLOW, (
            f"consecutive_losses={state.consecutive_losses}"
            f">={CONSECUTIVE_LOSS_TRIGGER}"
        )

    # エクスポージャー上限チェック
    max_exposure = state.current_balance * settings.max_total_exposure_pct / 100.0
    if max_exposure > 0 and state.open_exposure >= max_exposure:
        return CircuitBreakerLevel.YELLOW, f"exposure={state.open_exposure:.0f}>={max_exposure:.0f}"

    return CircuitBreakerLevel.GREEN, ""


def get_sizing_multiplier(
    current_level: CircuitBreakerLevel,
    prev_level: CircuitBreakerLevel | None = None,
    prev_multiplier: float = 1.0,
) -> float:
    """Calculate sizing multiplier based on CB level and recovery state."""
    if current_level == CircuitBreakerLevel.GREEN:
        return 1.0
    if current_level == CircuitBreakerLevel.YELLOW:
        # 降格直後 (ORANGE→YELLOW): 保守的な復帰
        if prev_level is not None and prev_level >= CircuitBreakerLevel.ORANGE:
            return 0.25
        return YELLOW_SIZING
    # ORANGE, RED
    return 0.0


def can_trade(db_path: Path | str) -> tuple[bool, str, float]:
    """Main entry point: check if trading is allowed.

    Returns (allowed, reason, sizing_multiplier).
    On risk engine failure: degraded mode (allowed=True, multiplier=0.5).
    """
    if not settings.risk_check_enabled:
        return True, "", 1.0

    try:
        state = _get_cached_state(db_path)
        if state.circuit_breaker_level >= CircuitBreakerLevel.ORANGE:
            return False, f"circuit_breaker_{state.circuit_breaker_level.name}", 0.0
        # ロックアウト中チェック
        if state.lockout_until:
            now = datetime.now(timezone.utc)
            try:
                lockout = datetime.fromisoformat(state.lockout_until)
                if now < lockout:
                    return False, f"lockout_until_{state.lockout_until}", 0.0
            except (ValueError, TypeError):
                pass
        return True, "", state.sizing_multiplier
    except Exception as e:
        logger.error("Risk engine failed: %s — degraded mode", e)
        return True, "degraded_mode", 0.5


def detect_balance_anomaly(current: float, previous: float) -> bool:
    """Detect sudden balance drop (10%+ decrease)."""
    if previous <= 0 or current <= 0:
        return False
    drop_pct = (previous - current) / previous * 100
    return drop_pct >= BALANCE_ANOMALY_PCT


def load_or_compute_risk_state(db_path: Path | str) -> RiskState:
    """Load cached state or compute fresh. Used by scheduler main()."""
    return _get_cached_state(db_path)


# --- Internal helpers ---


def _get_cached_state(db_path: Path | str) -> RiskState:
    """Return cached RiskState if fresh, else recompute."""
    global _cached_state, _cached_at
    now = time.monotonic()
    if _cached_state is not None and (now - _cached_at) < _CACHE_TTL:
        return _cached_state
    state = compute_risk_state(db_path)
    _cached_state = state
    _cached_at = now
    return state


def invalidate_cache() -> None:
    """Force recomputation on next call. Useful after settle."""
    global _cached_state, _cached_at
    _cached_state = None
    _cached_at = 0.0


def _fetch_balance_safe(fallback: float) -> float:
    """Fetch USDC balance from Polymarket; return fallback on failure."""
    try:
        if not settings.polymarket_private_key:
            return fallback or 1000.0  # デフォルト (paper mode 用)
        from src.connectors.polymarket import get_usdc_balance

        balance = get_usdc_balance()
        return balance if balance > 0 else fallback
    except Exception:
        logger.warning("Balance fetch failed, using fallback=$%.2f", fallback)
        return fallback


def _compute_drawdown_pct(
    daily_pnl: float,
    weekly_pnl: float,
    balance: float,
) -> float:
    """Estimate max drawdown % from available PnL data."""
    if balance <= 0:
        return 0.0
    # weekly_pnl が累積損の近似
    if weekly_pnl >= 0:
        return 0.0
    return abs(weekly_pnl) / balance * 100
