"""MERGE execution: YES+NO token pairs → USDC via CTF mergePositions.

Extracted from src/scheduler/trade_scheduler.py — process_merge_eligible.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone

from src.config import settings
from src.scheduler.job_executor import JobResult
from src.store.db import DEFAULT_DB_PATH

logger = logging.getLogger(__name__)


def _in_rollout_cohort(bothside_group_id: str, rollout_pct: int) -> bool:
    """Deterministic cohort gating for phased rollout."""
    if rollout_pct >= 100:
        return True
    if rollout_pct <= 0:
        return False
    h = hashlib.sha1(bothside_group_id.encode("utf-8")).hexdigest()
    bucket = int(h[:8], 16) % 100
    return bucket < rollout_pct


def _parse_iso8601(ts: str) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def _estimate_capital_release_benefit_usd(
    merge_amount: float,
    combined_vwap: float,
    execute_before: str,
) -> float:
    """Estimate benefit from releasing principal before normal resolution."""
    released_principal = max(merge_amount * combined_vwap, 0.0)
    if released_principal <= 0:
        return 0.0

    now = datetime.now(timezone.utc)
    tipoff = _parse_iso8601(execute_before)
    hours_to_tipoff = 0.0
    if tipoff is not None:
        hours_to_tipoff = max((tipoff - now).total_seconds() / 3600.0, 0.0)
    horizon = hours_to_tipoff + max(settings.merge_early_partial_post_tipoff_hours, 0.0)
    return released_principal * settings.merge_early_partial_capital_rate_per_hour * horizon


def _early_partial_guardrail_ok(db_path: str) -> tuple[bool, str]:
    """Guardrail based on recent early-partial performance."""
    from src.store.db import get_recent_early_partial_merge_stats

    stats = get_recent_early_partial_merge_stats(
        limit=settings.merge_early_partial_guard_lookback,
        db_path=db_path,
    )
    count = int(stats.get("count", 0))
    avg_net = float(stats.get("avg_net_profit_usd", 0.0))

    if count < settings.merge_early_partial_guard_min_samples:
        return True, f"insufficient_samples={count}"
    if avg_net < settings.merge_early_partial_guard_min_avg_net_profit_usd:
        return (
            False,
            "guardrail_avg_net"
            f"={avg_net:.4f}<min={settings.merge_early_partial_guard_min_avg_net_profit_usd:.4f}",
        )
    return True, "ok"


def _update_per_signal_merge_data(
    dir_signals,
    hedge_signals,
    dir_shares: float,
    hedge_shares: float,
    merge_amount: float,
    combined_vwap: float,
    db_path,
    update_fn,
) -> None:
    """Write per-signal shares_merged and merge_recovery_usd after MERGE."""
    for signals, total_shares in [
        (dir_signals, dir_shares),
        (hedge_signals, hedge_shares),
    ]:
        if total_shares <= 0:
            continue
        for sig in signals:
            px = sig.fill_price if sig.fill_price is not None else sig.poly_price
            if not px or px <= 0:
                continue
            sig_shares = sig.kelly_size / px
            sig_merged = merge_amount * (sig_shares / total_shares)
            sig_recovery = sig_merged * px / combined_vwap
            update_fn(sig.id, sig_merged, sig_recovery, db_path=db_path)


def _update_merge_job_pair(
    dir_job_id: int,
    hedge_job_id: int,
    status: str,
    merge_id: int,
    db_path: str,
    update_fn,
) -> None:
    """Apply the same merge status update to directional and hedge jobs."""
    update_fn(dir_job_id, status, merge_id, db_path=db_path)
    update_fn(hedge_job_id, status, merge_id, db_path=db_path)


def process_merge_eligible(
    execution_mode: str = "paper",
    db_path: str | None = None,
) -> list[JobResult]:
    """Process bothside groups eligible for MERGE (post-DCA).

    Called after process_dca_active_jobs() and before auto_settle().
    Merges YES+NO token pairs into USDC via CTF mergePositions.
    """
    from src.connectors.ctf import merge_positions as ctf_merge
    from src.connectors.ctf import simulate_merge
    from src.store.db import (
        get_bothside_signals,
        get_merge_candidate_groups,
        get_position_group,
        log_merge_operation,
        log_position_group_audit_event,
        update_job_merge_status,
        update_merge_operation,
        update_signal_merge_data,
    )
    from src.strategy.merge_strategy import (
        calculate_combined_vwap,
        calculate_mergeable_shares,
        should_merge,
    )

    if not settings.merge_enabled:
        return []

    path = db_path or DEFAULT_DB_PATH
    candidates = get_merge_candidate_groups(
        include_dca_active=settings.merge_early_partial_enabled,
        db_path=path,
    )
    if not candidates:
        return []

    logger.info("Found %d MERGE candidate group(s)", len(candidates))

    sig_type = settings.polymarket_signature_type
    is_eoa = sig_type == 0
    is_poly_proxy = sig_type == 1
    is_supported_wallet = is_eoa or is_poly_proxy
    results: list[JobResult] = []
    early_partial_executed = 0

    for c in candidates:
        bs_gid = c["bothside_group_id"]
        dir_job_id = int(c["dir_id"])
        hedge_job_id = int(c["hedge_id"])
        dir_status = str(c["dir_status"])
        hedge_status = str(c["hedge_status"])
        execute_before = str(c.get("execute_before") or "")
        is_early_partial = dir_status == "dca_active" or hedge_status == "dca_active"

        try:
            if is_early_partial:
                if not settings.merge_early_partial_enabled:
                    logger.debug("MERGE early skip %s: disabled", bs_gid[:8])
                    continue
                if early_partial_executed >= settings.merge_early_partial_max_per_tick:
                    logger.info(
                        "MERGE early skip %s: max_per_tick=%d reached",
                        bs_gid[:8],
                        settings.merge_early_partial_max_per_tick,
                    )
                    continue
                if not _in_rollout_cohort(bs_gid, settings.merge_early_partial_rollout_pct):
                    logger.debug(
                        "MERGE early skip %s: rollout cohort %d%%",
                        bs_gid[:8],
                        settings.merge_early_partial_rollout_pct,
                    )
                    continue
                guard_ok, guard_reason = _early_partial_guardrail_ok(path)
                if not guard_ok:
                    logger.warning("MERGE early skip %s: %s", bs_gid[:8], guard_reason)
                    continue

            all_signals = get_bothside_signals(bs_gid, db_path=path)
            dir_signals = [s for s in all_signals if s.signal_role == "directional"]
            hedge_signals = [s for s in all_signals if s.signal_role == "hedge"]

            if not dir_signals or not hedge_signals:
                logger.warning("MERGE skip %s: missing signals", bs_gid[:8])
                continue

            # live 実運用では約定済み在庫のみを MERGE 対象にする
            if execution_mode == "live":
                non_filled = [s for s in all_signals if s.order_status != "filled"]
                if non_filled:
                    logger.info(
                        "MERGE skip %s: waiting fills (%d/%d not filled)",
                        bs_gid[:8],
                        len(non_filled),
                        len(all_signals),
                    )
                    continue

            # condition_id チェック (旧シグナルは condition_id なし)
            condition_id = dir_signals[0].condition_id
            if not condition_id:
                logger.info("MERGE skip %s: no condition_id", bs_gid[:8])
                continue

            # Shares 計算
            dir_shares, hedge_shares, merge_amount, remainder, remainder_side = (
                calculate_mergeable_shares(dir_signals, hedge_signals)
            )

            # Combined VWAP
            dir_vwap, hedge_vwap, combined_vwap = calculate_combined_vwap(
                dir_signals, hedge_signals
            )

            # Gas 見積もり (paper/dry-run は 0)
            gas_cost_usd = 0.0
            if execution_mode == "live":
                try:
                    from src.connectors.ctf import estimate_merge_gas, get_matic_usd_price

                    gas_matic = estimate_merge_gas(condition_id, merge_amount)
                    gas_cost_usd = gas_matic * get_matic_usd_price()
                except Exception:
                    logger.warning("Gas estimation failed for %s", bs_gid[:8])
                    gas_cost_usd = 0.01  # フォールバック

            additional_fee_usd = gas_cost_usd
            capital_release_benefit_usd = None
            if is_early_partial:
                if execution_mode != "live":
                    additional_fee_usd = settings.merge_early_partial_assumed_fee_usd
                capital_release_benefit_usd = _estimate_capital_release_benefit_usd(
                    merge_amount=merge_amount,
                    combined_vwap=combined_vwap,
                    execute_before=execute_before,
                )
                additional_fee_usd += settings.merge_early_partial_min_benefit_over_fee_usd

            # MERGE 判定
            do_merge, reason = should_merge(
                combined_vwap,
                merge_amount,
                settings,
                gas_cost_usd=gas_cost_usd,
                capital_release_benefit_usd=capital_release_benefit_usd,
                additional_fee_usd=additional_fee_usd if is_early_partial else None,
                is_eoa=is_eoa,
                is_supported_wallet=is_supported_wallet,
            )

            if not do_merge:
                logger.info(
                    "MERGE skip %s: %s (cvwap=%.4f, amount=%.2f)",
                    bs_gid[:8],
                    reason,
                    combined_vwap,
                    merge_amount,
                )
                continue

            gross_profit = merge_amount * (1.0 - combined_vwap)
            net_profit = gross_profit - gas_cost_usd
            event_slug = dir_signals[0].event_slug

            # merge_operations に記録 (pending)
            merge_id = log_merge_operation(
                bothside_group_id=bs_gid,
                condition_id=condition_id,
                event_slug=event_slug,
                dir_shares=dir_shares,
                hedge_shares=hedge_shares,
                merge_amount=merge_amount,
                remainder_shares=remainder,
                remainder_side=remainder_side,
                dir_vwap=dir_vwap,
                hedge_vwap=hedge_vwap,
                combined_vwap=combined_vwap,
                gross_profit_usd=gross_profit,
                gas_cost_usd=gas_cost_usd,
                net_profit_usd=net_profit,
                early_partial=is_early_partial,
                capital_release_benefit_usd=capital_release_benefit_usd,
                additional_fee_usd=additional_fee_usd if is_early_partial else gas_cost_usd,
                execution_stage="early_partial" if is_early_partial else "post_dca",
                status="pending",
                db_path=path,
            )

            # 実行
            if execution_mode == "live":
                if is_poly_proxy:
                    from src.connectors.ctf import merge_positions_via_safe

                    merge_result = merge_positions_via_safe(condition_id, merge_amount)
                else:
                    merge_result = ctf_merge(condition_id, merge_amount)
                if merge_result.success:
                    update_merge_operation(
                        merge_id,
                        status="executed",
                        tx_hash=merge_result.tx_hash,
                        gas_cost_usd=merge_result.gas_cost_usd,
                        net_profit_usd=gross_profit - merge_result.gas_cost_usd,
                        db_path=path,
                    )
                    # Per-signal merge データ更新
                    _update_per_signal_merge_data(
                        dir_signals, hedge_signals, dir_shares, hedge_shares,
                        merge_amount, combined_vwap, path,
                        update_signal_merge_data,
                    )
                    _update_merge_job_pair(
                        dir_job_id,
                        hedge_job_id,
                        "executed",
                        merge_id,
                        path,
                        update_job_merge_status,
                    )
                    logger.info(
                        "MERGE %s executed %s: %.2f shares, profit=$%.4f, tx=%s",
                        "early" if is_early_partial else "post-dca",
                        bs_gid[:8],
                        merge_amount,
                        gross_profit - merge_result.gas_cost_usd,
                        merge_result.tx_hash[:16],
                    )
                    if is_early_partial:
                        early_partial_executed += 1
                else:
                    update_merge_operation(
                        merge_id,
                        status="failed",
                        error_message=merge_result.error,
                        db_path=path,
                    )
                    _update_merge_job_pair(
                        dir_job_id,
                        hedge_job_id,
                        "failed",
                        merge_id,
                        path,
                        update_job_merge_status,
                    )
                    logger.warning(
                        "MERGE failed %s: %s",
                        bs_gid[:8],
                        merge_result.error,
                    )
                    results.append(
                        JobResult(dir_job_id, event_slug, "failed", error=merge_result.error)
                    )
                    continue
            else:
                # Paper/dry-run: シミュレーション
                simulate_merge(condition_id, merge_amount, combined_vwap, gas_cost_usd)
                status = "simulated"
                update_merge_operation(
                    merge_id,
                    status=status,
                    tx_hash="simulated",
                    gas_cost_usd=gas_cost_usd,
                    net_profit_usd=net_profit,
                    db_path=path,
                )
                # Per-signal merge データ更新
                _update_per_signal_merge_data(
                    dir_signals, hedge_signals, dir_shares, hedge_shares,
                    merge_amount, combined_vwap, path,
                    update_signal_merge_data,
                )
                _update_merge_job_pair(
                    dir_job_id,
                    hedge_job_id,
                    "executed",
                    merge_id,
                    path,
                    update_job_merge_status,
                )
                logger.info(
                    "[%s] MERGE %s simulated %s: %.2f shares, cvwap=%.4f, profit=$%.4f",
                    execution_mode,
                    "early" if is_early_partial else "post-dca",
                    bs_gid[:8],
                    merge_amount,
                    combined_vwap,
                    net_profit,
                )
                if is_early_partial:
                    early_partial_executed += 1

            # 即時通知 (Phase N)
            try:
                group = get_position_group(event_slug, db_path=path)
                log_position_group_audit_event(
                    event_slug=event_slug,
                    audit_type="merge",
                    prev_state=group.state if group else None,
                    new_state=group.state if group else None,
                    reason="merge_executed",
                    m_target=group.M_target if group else None,
                    d_target=group.D_target if group else None,
                    q_dir=group.q_dir if group else None,
                    q_opp=group.q_opp if group else None,
                    d=(
                        (group.q_dir - group.q_opp)
                        if group is not None
                        else None
                    ),
                    m=min(group.q_dir, group.q_opp) if group is not None else None,
                    d_max=group.d_max if group else None,
                    merge_amount=merge_amount,
                    merged_qty=group.merged_qty if group else None,
                    db_path=path,
                )
            except Exception:
                logger.debug("PositionGroup merge audit logging failed", exc_info=True)

            # 即時通知 (Phase N)
            try:
                from src.notifications.telegram import notify_merge

                _gas = gas_cost_usd
                _net = net_profit
                if execution_mode == "live":
                    _gas = merge_result.gas_cost_usd  # type: ignore[possibly-undefined]
                    _net = gross_profit - _gas
                notify_merge(
                    event_slug=event_slug,
                    merge_shares=merge_amount,
                    combined_vwap=combined_vwap,
                    gross_profit=gross_profit,
                    gas_cost=_gas,
                    net_profit=_net,
                    remainder_shares=remainder,
                    remainder_side=remainder_side,
                )
            except Exception:
                logger.debug("MERGE notification failed", exc_info=True)

            results.append(JobResult(dir_job_id, event_slug, "executed"))

        except Exception as e:
            logger.exception("MERGE error for group %s", bs_gid[:8])
            results.append(JobResult(dir_job_id, bs_gid, "failed", error=str(e)))

    return results
