"""MERGE execution: YES+NO token pairs → USDC via CTF mergePositions.

Extracted from src/scheduler/trade_scheduler.py — process_merge_eligible.
"""

from __future__ import annotations

import logging

from src.config import settings
from src.scheduler.job_executor import JobResult
from src.store.db import DEFAULT_DB_PATH

logger = logging.getLogger(__name__)


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
        get_merge_eligible_groups,
        log_merge_operation,
        update_job_merge_status,
        update_merge_operation,
    )
    from src.strategy.merge_strategy import (
        calculate_combined_vwap,
        calculate_mergeable_shares,
        should_merge,
    )

    if not settings.merge_enabled:
        return []

    path = db_path or DEFAULT_DB_PATH
    eligible = get_merge_eligible_groups(db_path=path)
    if not eligible:
        return []

    logger.info("Found %d MERGE-eligible bothside group(s)", len(eligible))

    sig_type = settings.polymarket_signature_type
    is_eoa = sig_type == 0
    is_poly_proxy = sig_type == 1
    is_supported_wallet = is_eoa or is_poly_proxy
    results: list[JobResult] = []

    for bs_gid, dir_job_id, hedge_job_id in eligible:
        try:
            all_signals = get_bothside_signals(bs_gid, db_path=path)
            dir_signals = [s for s in all_signals if s.signal_role == "directional"]
            hedge_signals = [s for s in all_signals if s.signal_role == "hedge"]

            if not dir_signals or not hedge_signals:
                logger.warning("MERGE skip %s: missing signals", bs_gid[:8])
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
                    from src.connectors.ctf import estimate_merge_gas

                    gas_matic = estimate_merge_gas(condition_id, merge_amount)
                    gas_cost_usd = gas_matic * 0.40  # 概算
                except Exception:
                    logger.warning("Gas estimation failed for %s", bs_gid[:8])
                    gas_cost_usd = 0.01  # フォールバック

            # MERGE 判定
            do_merge, reason = should_merge(
                combined_vwap,
                merge_amount,
                settings,
                gas_cost_usd=gas_cost_usd,
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
                    update_job_merge_status(dir_job_id, "executed", merge_id, db_path=path)
                    update_job_merge_status(hedge_job_id, "executed", merge_id, db_path=path)
                    logger.info(
                        "MERGE executed %s: %.2f shares, profit=$%.4f, tx=%s",
                        bs_gid[:8],
                        merge_amount,
                        gross_profit - merge_result.gas_cost_usd,
                        merge_result.tx_hash[:16],
                    )
                else:
                    update_merge_operation(
                        merge_id,
                        status="failed",
                        error_message=merge_result.error,
                        db_path=path,
                    )
                    update_job_merge_status(dir_job_id, "failed", merge_id, db_path=path)
                    update_job_merge_status(hedge_job_id, "failed", merge_id, db_path=path)
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
                update_job_merge_status(dir_job_id, "executed", merge_id, db_path=path)
                update_job_merge_status(hedge_job_id, "executed", merge_id, db_path=path)
                logger.info(
                    "[%s] MERGE simulated %s: %.2f shares, cvwap=%.4f, profit=$%.4f",
                    execution_mode,
                    bs_gid[:8],
                    merge_amount,
                    combined_vwap,
                    net_profit,
                )

            results.append(JobResult(dir_job_id, event_slug, "executed"))

        except Exception as e:
            logger.exception("MERGE error for group %s", bs_gid[:8])
            results.append(JobResult(dir_job_id, bs_gid, "failed", error=str(e)))

    return results
