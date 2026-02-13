"""GamePositionGroup state machine runner (Track B skeleton)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from src.config import settings
from src.store.db import (
    DEFAULT_DB_PATH,
    compute_position_group_inventory,
    get_group_execute_before,
    get_open_position_groups,
    get_position_group_sizing_snapshot,
    log_position_group_audit_event,
    update_position_group,
)
from src.strategy.position_group_sizing import (
    PositionGroupSizingInputs,
    compute_position_group_targets,
)

logger = logging.getLogger(__name__)

TERMINAL_STATES = {"CLOSED", "SAFE_STOP"}
ACTIVE_STATES = {"ACQUIRE", "BALANCE", "MERGE_LOOP"}
EPS = 1e-9


@dataclass
class PositionGroupTickResult:
    event_slug: str
    prev_state: str
    new_state: str
    q_dir: float
    q_opp: float
    merged_qty: float
    d: float
    m: float
    d_max: float


@dataclass
class _RiskContext:
    allow_new_increase: bool
    sizing_multiplier: float
    balance_usd: float
    force_safe_stop: bool
    safe_stop_reason: str | None = None


def _parse_iso8601(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_tipoff_passed(execute_before: str | None, now_utc: datetime) -> bool:
    tipoff = _parse_iso8601(execute_before)
    if tipoff is None:
        return False
    return now_utc >= tipoff


def _is_new_risk_blocked(execute_before: str | None, now_utc: datetime) -> bool:
    tipoff = _parse_iso8601(execute_before)
    if tipoff is None:
        return False
    cutoff = tipoff - timedelta(minutes=max(settings.position_group_new_risk_cutoff_min, 0))
    return now_utc >= cutoff


def _compute_dynamic_d_max(
    *,
    base_d_max: float,
    execute_before: str | None,
    now_utc: datetime,
) -> float:
    """Compute D_max(t): linearly decays near tipoff."""
    if base_d_max <= 0:
        return 0.0
    if not settings.position_group_dmax_decay_enabled:
        return base_d_max

    tipoff = _parse_iso8601(execute_before)
    if tipoff is None:
        return base_d_max

    start_min = max(settings.position_group_dmax_decay_start_min, 1)
    floor_ratio = max(min(settings.position_group_dmax_decay_floor_ratio, 1.0), 0.0)
    mins_to_tipoff = (tipoff - now_utc).total_seconds() / 60.0

    if mins_to_tipoff >= start_min:
        return base_d_max
    if mins_to_tipoff <= 0:
        return base_d_max * floor_ratio

    t = mins_to_tipoff / start_min
    ratio = floor_ratio + (1.0 - floor_ratio) * t
    return base_d_max * ratio


def _derive_transition_reason(
    *,
    prev_state: str,
    new_state: str,
    execute_before: str | None,
    now_utc: datetime,
    d: float,
    m: float,
    d_max: float,
    risk_ctx: _RiskContext,
) -> str:
    if prev_state == new_state:
        return "no_transition"
    if new_state == "SAFE_STOP":
        return risk_ctx.safe_stop_reason or "safe_stop"
    if prev_state == "PLANNED" and new_state == "ACQUIRE":
        return "start_acquire"
    if prev_state == "PLANNED" and new_state == "RESIDUAL_HOLD":
        return "new_risk_cutoff"
    if new_state == "BALANCE" and abs(d) > d_max:
        return "d_exceeds_dmax"
    if new_state == "MERGE_LOOP" and m >= settings.position_group_min_merge_shares:
        return "mergeable_inventory"
    if new_state == "RESIDUAL_HOLD" and _is_new_risk_blocked(execute_before, now_utc):
        return "new_risk_cutoff"
    if prev_state == "RESIDUAL_HOLD" and new_state == "EXIT":
        return "tipoff_passed"
    if prev_state == "EXIT" and new_state == "CLOSED":
        return "inventory_closed"
    return "transition"


def _parse_safe_stop_flags(raw: str) -> set[str]:
    return {part.strip() for part in raw.split(",") if part.strip()}


def _load_risk_context(db_path: str) -> _RiskContext:
    balance_fallback = settings.max_position_usd / max(settings.capital_risk_pct / 100.0, 1e-6)
    try:
        from src.risk.models import CircuitBreakerLevel
        from src.risk.risk_engine import load_or_compute_risk_state

        risk_state = load_or_compute_risk_state(db_path)
        balance = (
            risk_state.current_balance
            if risk_state.current_balance > 0
            else risk_state.last_known_balance
        )
        cb_blocked = risk_state.circuit_breaker_level >= CircuitBreakerLevel.ORANGE
        safe_stop_flags = _parse_safe_stop_flags(settings.position_group_safe_stop_flags)
        matched_flags = [f for f in (risk_state.flags or []) if f in safe_stop_flags]
        force_safe_stop = cb_blocked or bool(matched_flags)
        safe_stop_reason = None
        if cb_blocked:
            safe_stop_reason = f"cb_{risk_state.circuit_breaker_level.name}"
        elif matched_flags:
            safe_stop_reason = f"risk_flags:{','.join(matched_flags)}"
        return _RiskContext(
            allow_new_increase=not cb_blocked,
            sizing_multiplier=max(min(risk_state.sizing_multiplier, 1.0), 0.0),
            balance_usd=max(balance, balance_fallback),
            force_safe_stop=force_safe_stop,
            safe_stop_reason=safe_stop_reason,
        )
    except Exception:
        logger.exception("PositionGroup risk check failed")
        force_safe_stop = settings.position_group_safe_stop_on_risk_error
        return _RiskContext(
            allow_new_increase=not force_safe_stop,
            sizing_multiplier=0.0 if force_safe_stop else 1.0,
            balance_usd=balance_fallback,
            force_safe_stop=force_safe_stop,
            safe_stop_reason="risk_engine_error" if force_safe_stop else None,
        )


def _decide_next_state(
    *,
    state: str,
    d: float,
    m: float,
    d_max: float,
    execute_before: str | None,
    now_utc: datetime,
    allow_new_risk_by_cb: bool,
    force_safe_stop: bool,
) -> str:
    if state in TERMINAL_STATES:
        return state

    if force_safe_stop:
        return "SAFE_STOP"
    if not allow_new_risk_by_cb:
        return "SAFE_STOP"

    new_risk_blocked = _is_new_risk_blocked(execute_before, now_utc)
    tipoff_passed = _is_tipoff_passed(execute_before, now_utc)

    if state == "PLANNED":
        return "RESIDUAL_HOLD" if new_risk_blocked else "ACQUIRE"

    if state in ACTIVE_STATES:
        if new_risk_blocked:
            return "RESIDUAL_HOLD"
        if abs(d) > d_max:
            return "BALANCE"
        if m >= settings.position_group_min_merge_shares:
            return "MERGE_LOOP"
        return "ACQUIRE"

    if state == "RESIDUAL_HOLD":
        if tipoff_passed:
            return "EXIT"
        if abs(d) > d_max:
            return "BALANCE"
        return "RESIDUAL_HOLD"

    if state == "EXIT":
        if abs(d) <= EPS and m <= EPS:
            return "CLOSED"
        return "EXIT"

    logger.warning("Unknown position group state=%s; forcing SAFE_STOP", state)
    return "SAFE_STOP"


def process_position_groups(
    *,
    db_path: str | None = None,
    now_utc: datetime | None = None,
) -> list[PositionGroupTickResult]:
    """Advance open position groups by one scheduler tick."""
    path = db_path or str(DEFAULT_DB_PATH)
    now_dt = now_utc or datetime.now(timezone.utc)
    now_iso = now_dt.isoformat()

    groups = get_open_position_groups(db_path=path)
    if not groups:
        return []

    risk_ctx = _load_risk_context(path)
    results: list[PositionGroupTickResult] = []

    for group in groups:
        q_dir, q_opp, merged_qty = compute_position_group_inventory(group.event_slug, db_path=path)
        d = q_dir - q_opp
        m = min(q_dir, q_opp)
        base_d_max = group.d_max if group.d_max > 0 else settings.position_group_default_d_max
        sizing_snapshot = get_position_group_sizing_snapshot(group.event_slug, db_path=path)
        targets = compute_position_group_targets(
            inputs=PositionGroupSizingInputs(**sizing_snapshot),
            balance_usd=risk_ctx.balance_usd,
            u_regime=risk_ctx.sizing_multiplier,
        )

        execute_before = get_group_execute_before(group.event_slug, db_path=path)
        d_max_t = _compute_dynamic_d_max(
            base_d_max=base_d_max,
            execute_before=execute_before,
            now_utc=now_dt,
        )
        next_state = _decide_next_state(
            state=group.state,
            d=d,
            m=m,
            d_max=d_max_t,
            execute_before=execute_before,
            now_utc=now_dt,
            allow_new_risk_by_cb=risk_ctx.allow_new_increase,
            force_safe_stop=risk_ctx.force_safe_stop,
        )
        if next_state == "SAFE_STOP" and group.state != "SAFE_STOP":
            logger.warning(
                "PositionGroup %s entered SAFE_STOP: reason=%s",
                group.event_slug,
                risk_ctx.safe_stop_reason or "unknown",
            )

        update_position_group(
            group.event_slug,
            state=next_state,
            m_target=targets.m_target,
            d_target=targets.d_target,
            q_dir=q_dir,
            q_opp=q_opp,
            merged_qty=merged_qty,
            d_max=base_d_max,
            phase_time=now_iso if next_state != group.state else group.phase_time,
            db_path=path,
        )
        merge_amount = max(merged_qty - max(group.merged_qty, 0.0), 0.0)
        reason = _derive_transition_reason(
            prev_state=group.state,
            new_state=next_state,
            execute_before=execute_before,
            now_utc=now_dt,
            d=d,
            m=m,
            d_max=d_max_t,
            risk_ctx=risk_ctx,
        )
        try:
            log_position_group_audit_event(
                event_slug=group.event_slug,
                audit_type="tick",
                prev_state=group.state,
                new_state=next_state,
                reason=reason,
                m_target=targets.m_target,
                d_target=targets.d_target,
                q_dir=q_dir,
                q_opp=q_opp,
                d=d,
                m=m,
                d_max=d_max_t,
                merge_amount=merge_amount,
                merged_qty=merged_qty,
                created_at=now_iso,
                db_path=path,
            )
        except Exception:
            logger.exception("PositionGroup audit logging failed: %s", group.event_slug)

        results.append(
            PositionGroupTickResult(
                event_slug=group.event_slug,
                prev_state=group.state,
                new_state=next_state,
                q_dir=q_dir,
                q_opp=q_opp,
                merged_qty=merged_qty,
                d=d,
                m=m,
                d_max=d_max_t,
            )
        )

    return results
