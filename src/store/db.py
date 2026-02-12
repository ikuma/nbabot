"""SQLite store for paper-trade signal logging and result tracking."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

# Re-export models and schema for backward compatibility
from src.store.models import (  # noqa: F401
    JobStatus,
    JobSummary,
    MergeOperation,
    PerformanceStats,
    ResultRecord,
    SignalRecord,
    TradeJob,
)
from src.store.schema import (  # noqa: F401
    DEFAULT_DB_PATH,
    MERGE_OPERATIONS_SQL,
    SCHEMA_SQL,
    TRADE_JOBS_SQL,
    _connect,
)


def log_signal(
    *,
    game_title: str,
    event_slug: str,
    team: str,
    side: str,
    poly_price: float,
    book_prob: float,
    edge_pct: float,
    kelly_size: float,
    token_id: str,
    bookmakers_count: int = 0,
    consensus_std: float = 0.0,
    commence_time: str = "",
    market_type: str = "moneyline",
    calibration_edge_pct: float | None = None,
    expected_win_rate: float | None = None,
    price_band: str = "",
    in_sweet_spot: bool = False,
    band_confidence: str = "",
    strategy_mode: str = "bookmaker",
    liquidity_score: str = "unknown",
    ask_depth_5c: float | None = None,
    spread_pct: float | None = None,
    balance_usd_at_trade: float | None = None,
    constraint_binding: str = "kelly",
    dca_group_id: str | None = None,
    dca_sequence: int = 1,
    bothside_group_id: str | None = None,
    signal_role: str = "directional",
    condition_id: str | None = None,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> int:
    """Insert a signal and return its row id."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            """INSERT INTO signals
               (game_title, event_slug, team, side, poly_price, book_prob,
                edge_pct, kelly_size, token_id, bookmakers_count, consensus_std,
                commence_time, created_at,
                market_type, calibration_edge_pct, expected_win_rate,
                price_band, in_sweet_spot, band_confidence, strategy_mode,
                liquidity_score, ask_depth_5c, spread_pct,
                balance_usd_at_trade, constraint_binding,
                dca_group_id, dca_sequence,
                bothside_group_id, signal_role, condition_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                game_title,
                event_slug,
                team,
                side,
                poly_price,
                book_prob,
                edge_pct,
                kelly_size,
                token_id,
                bookmakers_count,
                consensus_std,
                commence_time,
                now,
                market_type,
                calibration_edge_pct,
                expected_win_rate,
                price_band,
                int(in_sweet_spot),
                band_confidence,
                strategy_mode,
                liquidity_score,
                ask_depth_5c,
                spread_pct,
                balance_usd_at_trade,
                constraint_binding,
                dca_group_id,
                dca_sequence,
                bothside_group_id,
                signal_role,
                condition_id,
            ),
        )
        conn.commit()
        return cur.lastrowid  # type: ignore[return-value]
    finally:
        conn.close()


def log_result(
    *,
    signal_id: int,
    outcome: str,
    won: bool,
    pnl: float,
    settlement_price: float | None = None,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> int:
    """Record a settlement result for a signal. Returns result row id."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            """INSERT INTO results (signal_id, outcome, won, settlement_price, pnl, settled_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (signal_id, outcome, int(won), settlement_price, pnl, now),
        )
        conn.commit()
        return cur.lastrowid  # type: ignore[return-value]
    finally:
        conn.close()


def get_unsettled(db_path: Path | str = DEFAULT_DB_PATH) -> list[SignalRecord]:
    """Return signals that have not been settled yet."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT s.* FROM signals s
               LEFT JOIN results r ON r.signal_id = s.id
               WHERE r.id IS NULL
               ORDER BY s.created_at DESC""",
        ).fetchall()
        return [SignalRecord(**dict(r)) for r in rows]
    finally:
        conn.close()


def get_signal_by_id(
    signal_id: int,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> SignalRecord | None:
    """Return a single signal by ID, or None if not found."""
    conn = _connect(db_path)
    try:
        row = conn.execute("SELECT * FROM signals WHERE id = ?", (signal_id,)).fetchone()
        if row:
            return SignalRecord(**dict(row))
        return None
    finally:
        conn.close()


def get_all_signals(db_path: Path | str = DEFAULT_DB_PATH) -> list[SignalRecord]:
    """Return all signals ordered by creation time (newest first)."""
    conn = _connect(db_path)
    try:
        rows = conn.execute("SELECT * FROM signals ORDER BY created_at DESC").fetchall()
        return [SignalRecord(**dict(r)) for r in rows]
    finally:
        conn.close()


def get_all_results(db_path: Path | str = DEFAULT_DB_PATH) -> list[ResultRecord]:
    """Return all results ordered by settlement time (newest first)."""
    conn = _connect(db_path)
    try:
        rows = conn.execute("SELECT * FROM results ORDER BY settled_at DESC").fetchall()
        return [ResultRecord(**{**dict(r), "won": bool(r["won"])}) for r in rows]
    finally:
        conn.close()


def get_performance(db_path: Path | str = DEFAULT_DB_PATH) -> PerformanceStats:
    """Compute aggregate paper-trade performance statistics."""
    conn = _connect(db_path)
    try:
        total_signals = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
        settled_count = conn.execute("SELECT COUNT(*) FROM results").fetchone()[0]
        unsettled_count = total_signals - settled_count

        wins = conn.execute("SELECT COUNT(*) FROM results WHERE won = 1").fetchone()[0]
        losses = settled_count - wins
        win_rate = wins / settled_count if settled_count > 0 else 0.0

        total_pnl_row = conn.execute("SELECT COALESCE(SUM(pnl), 0) FROM results").fetchone()
        total_pnl = float(total_pnl_row[0])
        avg_pnl = total_pnl / settled_count if settled_count > 0 else 0.0

        pnl_rows = conn.execute("SELECT pnl FROM results ORDER BY settled_at ASC").fetchall()
        pnl_series = [float(r[0]) for r in pnl_rows]

        max_drawdown = _calc_max_drawdown(pnl_series)
        sharpe_ratio = _calc_sharpe(pnl_series)

        return PerformanceStats(
            total_signals=total_signals,
            settled_count=settled_count,
            unsettled_count=unsettled_count,
            wins=wins,
            losses=losses,
            win_rate=win_rate,
            total_pnl=total_pnl,
            avg_pnl=avg_pnl,
            max_drawdown=max_drawdown,
            sharpe_ratio=sharpe_ratio,
        )
    finally:
        conn.close()


def _calc_max_drawdown(pnl_series: list[float]) -> float:
    """Max drawdown from cumulative PnL series."""
    if not pnl_series:
        return 0.0
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in pnl_series:
        cumulative += pnl
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd
    return max_dd


def _calc_sharpe(pnl_series: list[float], annualize_factor: float = 1.0) -> float:
    """Sharpe ratio from individual PnL values (risk-free rate = 0)."""
    if len(pnl_series) < 2:
        return 0.0
    mean = sum(pnl_series) / len(pnl_series)
    variance = sum((x - mean) ** 2 for x in pnl_series) / (len(pnl_series) - 1)
    std = variance**0.5
    if std == 0:
        return 0.0
    return (mean / std) * annualize_factor


# ---------------------------------------------------------------------------
# Execution tracking helpers
# ---------------------------------------------------------------------------


def update_order_status(
    signal_id: int,
    order_id: str | None,
    status: str,
    fill_price: float | None = None,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> None:
    """Update execution status for a signal."""
    conn = _connect(db_path)
    try:
        conn.execute(
            "UPDATE signals SET order_id = ?, order_status = ?, fill_price = ? WHERE id = ?",
            (order_id, status, fill_price, signal_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_todays_live_orders(
    date_str: str,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> int:
    """Count live orders placed today (order_status != 'paper')."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            """SELECT COUNT(*) FROM signals
               WHERE order_status NOT IN ('paper', 'failed')
               AND created_at LIKE ?""",
            (f"{date_str}%",),
        ).fetchone()
        return row[0]
    finally:
        conn.close()


def get_todays_exposure(
    date_str: str,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> float:
    """Sum of kelly_size for live orders placed today."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            """SELECT COALESCE(SUM(kelly_size), 0) FROM signals
               WHERE order_status NOT IN ('paper', 'failed', 'cancelled')
               AND created_at LIKE ?""",
            (f"{date_str}%",),
        ).fetchone()
        return float(row[0])
    finally:
        conn.close()


def get_placed_orders(
    db_path: Path | str = DEFAULT_DB_PATH,
) -> list[SignalRecord]:
    """Return signals with order_status='placed' (awaiting fill)."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT s.* FROM signals s
               LEFT JOIN results r ON r.signal_id = s.id
               WHERE r.id IS NULL AND s.order_status = 'placed'
               ORDER BY s.created_at DESC""",
        ).fetchall()
        return [SignalRecord(**dict(r)) for r in rows]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Trade jobs (per-game scheduler)
# ---------------------------------------------------------------------------


def upsert_trade_job(
    *,
    game_date: str,
    event_slug: str,
    home_team: str,
    away_team: str,
    game_time_utc: str,
    execute_after: str,
    execute_before: str,
    job_side: str = "directional",
    db_path: Path | str = DEFAULT_DB_PATH,
) -> bool:
    """Insert a trade job or update game time if changed. Returns True if inserted."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            """INSERT OR IGNORE INTO trade_jobs
               (game_date, event_slug, home_team, away_team, game_time_utc,
                execute_after, execute_before, status, retry_count, job_side,
                created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)""",
            (
                game_date,
                event_slug,
                home_team,
                away_team,
                game_time_utc,
                execute_after,
                execute_before,
                job_side,
                now,
                now,
            ),
        )
        inserted = cur.rowcount > 0

        if not inserted:
            conn.execute(
                """UPDATE trade_jobs
                   SET game_time_utc = ?, execute_after = ?, execute_before = ?,
                       updated_at = ?
                   WHERE event_slug = ? AND job_side = ?
                     AND game_time_utc != ?
                     AND status IN ('pending', 'failed')""",
                (
                    game_time_utc,
                    execute_after,
                    execute_before,
                    now,
                    event_slug,
                    job_side,
                    game_time_utc,
                ),
            )

        conn.commit()
        return inserted
    finally:
        conn.close()


def get_eligible_jobs(
    now_utc: str,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> list[TradeJob]:
    """Get jobs in the execution window: pending/failed, within window, retry < max."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT * FROM trade_jobs
               WHERE status IN ('pending', 'failed')
                 AND execute_after <= ?
                 AND execute_before > ?
                 AND retry_count < 3
               ORDER BY game_time_utc ASC""",
            (now_utc, now_utc),
        ).fetchall()
        return [TradeJob(**dict(r)) for r in rows]
    finally:
        conn.close()


def get_executing_jobs(
    db_path: Path | str = DEFAULT_DB_PATH,
) -> list[TradeJob]:
    """Get jobs stuck in 'executing' state (crash recovery)."""
    conn = _connect(db_path)
    try:
        rows = conn.execute("SELECT * FROM trade_jobs WHERE status = 'executing'").fetchall()
        return [TradeJob(**dict(r)) for r in rows]
    finally:
        conn.close()


def update_job_status(
    job_id: int,
    status: str,
    *,
    signal_id: int | None = None,
    error_message: str | None = None,
    increment_retry: bool = False,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> None:
    """Update a trade job's status and optional fields."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        if increment_retry:
            conn.execute(
                """UPDATE trade_jobs
                   SET status = ?, signal_id = COALESCE(?, signal_id),
                       error_message = ?, retry_count = retry_count + 1,
                       updated_at = ?
                   WHERE id = ?""",
                (status, signal_id, error_message, now, job_id),
            )
        else:
            conn.execute(
                """UPDATE trade_jobs
                   SET status = ?, signal_id = COALESCE(?, signal_id),
                       error_message = ?, updated_at = ?
                   WHERE id = ?""",
                (status, signal_id, error_message, now, job_id),
            )
        conn.commit()
    finally:
        conn.close()


def cancel_expired_jobs(
    now_utc: str,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> int:
    """Mark pending/failed/dca_active jobs past their execution window as expired/executed."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            """UPDATE trade_jobs
               SET status = 'expired', updated_at = ?
               WHERE status IN ('pending', 'failed')
                 AND execute_before <= ?""",
            (now, now_utc),
        )
        count = cur.rowcount
        cur2 = conn.execute(
            """UPDATE trade_jobs
               SET status = 'executed', updated_at = ?
               WHERE status = 'dca_active'
                 AND execute_before <= ?""",
            (now, now_utc),
        )
        count += cur2.rowcount
        conn.commit()
        return count
    finally:
        conn.close()


def get_job_summary(
    game_date: str,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> JobSummary:
    """Get status counts for jobs on a given game date."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT status, COUNT(*) as cnt FROM trade_jobs
               WHERE game_date = ?
               GROUP BY status""",
            (game_date,),
        ).fetchall()
        summary = JobSummary()
        for r in rows:
            status = r["status"]
            count = r["cnt"]
            if hasattr(summary, status):
                setattr(summary, status, count)
        return summary
    finally:
        conn.close()


def has_signal_for_slug(
    event_slug: str,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> bool:
    """Check if a signal exists for this event_slug (any execution mode)."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            """SELECT COUNT(*) FROM signals
               WHERE event_slug = ?
                 AND order_status IN ('placed', 'filled', 'paper')""",
            (event_slug,),
        ).fetchone()
        return row[0] > 0
    finally:
        conn.close()


def has_signal_for_slug_and_side(
    event_slug: str,
    signal_role: str = "directional",
    db_path: Path | str = DEFAULT_DB_PATH,
) -> bool:
    """Check if a signal exists for this event_slug and role."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            """SELECT COUNT(*) FROM signals
               WHERE event_slug = ?
                 AND signal_role = ?
                 AND order_status IN ('placed', 'filled', 'paper')""",
            (event_slug, signal_role),
        ).fetchone()
        return row[0] > 0
    finally:
        conn.close()


def upsert_hedge_job(
    *,
    directional_job_id: int,
    event_slug: str,
    game_date: str,
    home_team: str,
    away_team: str,
    game_time_utc: str,
    execute_after: str,
    execute_before: str,
    bothside_group_id: str,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> int | None:
    """Create a hedge job for a bothside pair. Idempotent via UNIQUE(event_slug, job_side)."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            """INSERT OR IGNORE INTO trade_jobs
               (game_date, event_slug, home_team, away_team, game_time_utc,
                execute_after, execute_before, status, retry_count, job_side,
                paired_job_id, bothside_group_id, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, 'hedge', ?, ?, ?, ?)""",
            (
                game_date,
                event_slug,
                home_team,
                away_team,
                game_time_utc,
                execute_after,
                execute_before,
                directional_job_id,
                bothside_group_id,
                now,
                now,
            ),
        )
        conn.commit()
        if cur.rowcount > 0:
            return cur.lastrowid
        return None
    finally:
        conn.close()


def get_hedge_job_for_slug(
    event_slug: str,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> TradeJob | None:
    """Get the hedge job for an event_slug."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM trade_jobs WHERE event_slug = ? AND job_side = 'hedge'",
            (event_slug,),
        ).fetchone()
        if row:
            return TradeJob(**dict(row))
        return None
    finally:
        conn.close()


def get_bothside_signals(
    bothside_group_id: str,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> list[SignalRecord]:
    """Get all signals in a bothside group (both directional and hedge)."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT * FROM signals
               WHERE bothside_group_id = ?
               ORDER BY signal_role ASC, dca_sequence ASC""",
            (bothside_group_id,),
        ).fetchall()
        return [SignalRecord(**dict(r)) for r in rows]
    finally:
        conn.close()


def update_job_bothside(
    job_id: int,
    *,
    bothside_group_id: str | None = None,
    paired_job_id: int | None = None,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> None:
    """Update bothside-related fields on a trade job."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        parts: list[str] = ["updated_at = ?"]
        params: list[object] = [now]
        if bothside_group_id is not None:
            parts.append("bothside_group_id = ?")
            params.append(bothside_group_id)
        if paired_job_id is not None:
            parts.append("paired_job_id = ?")
            params.append(paired_job_id)
        params.append(job_id)
        conn.execute(
            f"UPDATE trade_jobs SET {', '.join(parts)} WHERE id = ?",
            tuple(params),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DCA helpers
# ---------------------------------------------------------------------------


def get_dca_active_jobs(
    now_utc: str,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> list[TradeJob]:
    """Get jobs with status='dca_active' that haven't reached max DCA entries."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT * FROM trade_jobs
               WHERE status = 'dca_active'
                 AND execute_before > ?
                 AND dca_entries_count < dca_max_entries
               ORDER BY game_time_utc ASC""",
            (now_utc,),
        ).fetchall()
        return [TradeJob(**dict(r)) for r in rows]
    finally:
        conn.close()


def get_dca_group_signals(
    dca_group_id: str,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> list[SignalRecord]:
    """Get all signals in a DCA group, ordered by sequence."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT * FROM signals
               WHERE dca_group_id = ?
               ORDER BY dca_sequence ASC""",
            (dca_group_id,),
        ).fetchall()
        return [SignalRecord(**dict(r)) for r in rows]
    finally:
        conn.close()


def update_dca_job(
    job_id: int,
    *,
    dca_entries_count: int | None = None,
    dca_max_entries: int | None = None,
    dca_group_id: str | None = None,
    dca_total_budget: float | None = None,
    dca_slice_size: float | None = None,
    status: str | None = None,
    signal_id: int | None = None,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> None:
    """Update DCA-related fields on a trade job."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        parts: list[str] = ["updated_at = ?"]
        params: list[object] = [now]
        if dca_entries_count is not None:
            parts.append("dca_entries_count = ?")
            params.append(dca_entries_count)
        if dca_max_entries is not None:
            parts.append("dca_max_entries = ?")
            params.append(dca_max_entries)
        if dca_group_id is not None:
            parts.append("dca_group_id = ?")
            params.append(dca_group_id)
        if dca_total_budget is not None:
            parts.append("dca_total_budget = ?")
            params.append(dca_total_budget)
        if dca_slice_size is not None:
            parts.append("dca_slice_size = ?")
            params.append(dca_slice_size)
        if status is not None:
            parts.append("status = ?")
            params.append(status)
        if signal_id is not None:
            parts.append("signal_id = ?")
            params.append(signal_id)
        params.append(job_id)
        conn.execute(
            f"UPDATE trade_jobs SET {', '.join(parts)} WHERE id = ?",
            tuple(params),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# MERGE helpers (Phase B2)
# ---------------------------------------------------------------------------


def log_merge_operation(
    *,
    bothside_group_id: str,
    condition_id: str,
    event_slug: str,
    dir_shares: float,
    hedge_shares: float,
    merge_amount: float,
    remainder_shares: float,
    remainder_side: str | None,
    dir_vwap: float,
    hedge_vwap: float,
    combined_vwap: float,
    gross_profit_usd: float | None = None,
    gas_cost_usd: float | None = None,
    net_profit_usd: float | None = None,
    status: str = "pending",
    tx_hash: str | None = None,
    error_message: str | None = None,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> int:
    """Insert a merge operation and return its row id."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            """INSERT INTO merge_operations
               (bothside_group_id, condition_id, event_slug,
                dir_shares, hedge_shares, merge_amount, remainder_shares,
                remainder_side, dir_vwap, hedge_vwap, combined_vwap,
                gross_profit_usd, gas_cost_usd, net_profit_usd,
                status, tx_hash, error_message, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                bothside_group_id,
                condition_id,
                event_slug,
                dir_shares,
                hedge_shares,
                merge_amount,
                remainder_shares,
                remainder_side,
                dir_vwap,
                hedge_vwap,
                combined_vwap,
                gross_profit_usd,
                gas_cost_usd,
                net_profit_usd,
                status,
                tx_hash,
                error_message,
                now,
            ),
        )
        conn.commit()
        return cur.lastrowid  # type: ignore[return-value]
    finally:
        conn.close()


def get_merge_operation(
    bothside_group_id: str,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> MergeOperation | None:
    """Get the merge operation for a bothside group."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM merge_operations WHERE bothside_group_id = ?",
            (bothside_group_id,),
        ).fetchone()
        if row:
            return MergeOperation(**dict(row))
        return None
    finally:
        conn.close()


def get_merge_eligible_groups(
    db_path: Path | str = DEFAULT_DB_PATH,
) -> list[tuple[str, int, int]]:
    """Get bothside groups eligible for MERGE."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT d.bothside_group_id, d.id AS dir_id, h.id AS hedge_id
               FROM trade_jobs d
               JOIN trade_jobs h ON d.bothside_group_id = h.bothside_group_id
               WHERE d.job_side = 'directional'
                 AND h.job_side = 'hedge'
                 AND d.status = 'executed'
                 AND h.status = 'executed'
                 AND d.bothside_group_id IS NOT NULL
                 AND COALESCE(d.merge_status, 'none') = 'none'
               ORDER BY d.id ASC""",
        ).fetchall()
        return [(r[0], r[1], r[2]) for r in rows]
    finally:
        conn.close()


def update_signal_merge_data(
    signal_id: int,
    shares_merged: float,
    merge_recovery_usd: float,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> None:
    """Update per-signal merge data after MERGE execution."""
    conn = _connect(db_path)
    try:
        conn.execute(
            "UPDATE signals SET shares_merged = ?, merge_recovery_usd = ? WHERE id = ?",
            (shares_merged, merge_recovery_usd, signal_id),
        )
        conn.commit()
    finally:
        conn.close()


def update_merge_operation(
    merge_id: int,
    *,
    status: str | None = None,
    tx_hash: str | None = None,
    error_message: str | None = None,
    gross_profit_usd: float | None = None,
    gas_cost_usd: float | None = None,
    net_profit_usd: float | None = None,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> None:
    """Update a merge operation's fields."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        parts: list[str] = []
        params: list[object] = []
        if status is not None:
            parts.append("status = ?")
            params.append(status)
            if status in ("executed", "simulated"):
                parts.append("executed_at = ?")
                params.append(now)
        if tx_hash is not None:
            parts.append("tx_hash = ?")
            params.append(tx_hash)
        if error_message is not None:
            parts.append("error_message = ?")
            params.append(error_message)
        if gross_profit_usd is not None:
            parts.append("gross_profit_usd = ?")
            params.append(gross_profit_usd)
        if gas_cost_usd is not None:
            parts.append("gas_cost_usd = ?")
            params.append(gas_cost_usd)
        if net_profit_usd is not None:
            parts.append("net_profit_usd = ?")
            params.append(net_profit_usd)
        if not parts:
            return
        params.append(merge_id)
        conn.execute(
            f"UPDATE merge_operations SET {', '.join(parts)} WHERE id = ?",
            tuple(params),
        )
        conn.commit()
    finally:
        conn.close()


def update_job_merge_status(
    job_id: int,
    merge_status: str,
    merge_operation_id: int | None = None,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> None:
    """Update merge_status and merge_operation_id on a trade job."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        conn.execute(
            """UPDATE trade_jobs
               SET merge_status = ?, merge_operation_id = ?, updated_at = ?
               WHERE id = ?""",
            (merge_status, merge_operation_id, now, job_id),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Risk management queries (Phase D)
# ---------------------------------------------------------------------------


def get_daily_results(
    date_str: str,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> dict:
    """Get aggregated PnL for a single date. Returns {"pnl": float, "wins": int, "losses": int}."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            """SELECT
                 COALESCE(SUM(r.pnl), 0.0) AS pnl,
                 COALESCE(SUM(CASE WHEN r.won = 1 THEN 1 ELSE 0 END), 0) AS wins,
                 COALESCE(SUM(CASE WHEN r.won = 0 THEN 1 ELSE 0 END), 0) AS losses
               FROM results r
               WHERE r.settled_at LIKE ?""",
            (f"{date_str}%",),
        ).fetchone()
        return {"pnl": float(row[0]), "wins": int(row[1]), "losses": int(row[2])}
    finally:
        conn.close()


def get_weekly_results(
    end_date: str,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> dict:
    """Get aggregated PnL for the 7 days ending on end_date (inclusive)."""
    from datetime import timedelta

    try:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        return {"pnl": 0.0, "wins": 0, "losses": 0}
    start_dt = end_dt - timedelta(days=6)
    start_str = start_dt.strftime("%Y-%m-%d")

    conn = _connect(db_path)
    try:
        row = conn.execute(
            """SELECT
                 COALESCE(SUM(r.pnl), 0.0) AS pnl,
                 COALESCE(SUM(CASE WHEN r.won = 1 THEN 1 ELSE 0 END), 0) AS wins,
                 COALESCE(SUM(CASE WHEN r.won = 0 THEN 1 ELSE 0 END), 0) AS losses
               FROM results r
               WHERE r.settled_at >= ? AND r.settled_at < ?""",
            (start_str, end_date + "T99"),  # end_date 当日を含む
        ).fetchone()
        return {"pnl": float(row[0]), "wins": int(row[1]), "losses": int(row[2])}
    finally:
        conn.close()


def get_consecutive_losses(
    db_path: Path | str = DEFAULT_DB_PATH,
) -> int:
    """Count consecutive losses from most recent results (DESC order).

    Bothside MERGE ペアは除外 (リスクフリー).
    """
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT r.won, s.bothside_group_id, s.signal_role
               FROM results r
               JOIN signals s ON s.id = r.signal_id
               ORDER BY r.settled_at DESC
               LIMIT 50""",
        ).fetchall()
        count = 0
        for row in rows:
            # MERGE ペアの hedge は連敗カウントから除外
            if row["signal_role"] == "hedge":
                continue
            if row["won"]:
                break
            count += 1
        return count
    finally:
        conn.close()


def get_open_exposure(
    db_path: Path | str = DEFAULT_DB_PATH,
) -> float:
    """Sum of kelly_size for unsettled signals (bothside ネット考慮)."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            """SELECT COALESCE(SUM(s.kelly_size), 0.0)
               FROM signals s
               LEFT JOIN results r ON r.signal_id = s.id
               WHERE r.id IS NULL""",
        ).fetchone()
        return float(row[0])
    finally:
        conn.close()


def save_risk_snapshot(
    state,  # RiskState (avoid circular import)
    db_path: Path | str = DEFAULT_DB_PATH,
) -> int:
    """Persist a RiskState snapshot to risk_snapshots table."""
    import json

    conn = _connect(db_path)
    try:
        cur = conn.execute(
            """INSERT INTO risk_snapshots
               (checked_at, level, daily_pnl, weekly_pnl, consecutive_losses,
                max_drawdown_pct, open_exposure, sizing_multiplier,
                lockout_until, last_balance_usd, flags)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                state.checked_at,
                state.circuit_breaker_level.name,
                state.daily_pnl,
                state.weekly_pnl,
                state.consecutive_losses,
                state.max_drawdown_pct,
                state.open_exposure,
                state.sizing_multiplier,
                state.lockout_until,
                state.current_balance,
                json.dumps(state.flags),
            ),
        )
        conn.commit()
        return cur.lastrowid  # type: ignore[return-value]
    finally:
        conn.close()


def get_latest_risk_snapshot(
    db_path: Path | str = DEFAULT_DB_PATH,
):
    """Get the most recent RiskState snapshot, or None."""
    import json

    from src.risk.models import CircuitBreakerLevel, RiskState

    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM risk_snapshots ORDER BY id DESC LIMIT 1",
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        level_name = d.get("level", "GREEN")
        try:
            level = CircuitBreakerLevel[level_name]
        except (KeyError, ValueError):
            level = CircuitBreakerLevel.GREEN
        flags_raw = d.get("flags", "[]")
        try:
            flags = json.loads(flags_raw) if flags_raw else []
        except (json.JSONDecodeError, TypeError):
            flags = []
        return RiskState(
            daily_pnl=d.get("daily_pnl", 0.0),
            weekly_pnl=d.get("weekly_pnl", 0.0),
            consecutive_losses=d.get("consecutive_losses", 0),
            max_drawdown_pct=d.get("max_drawdown_pct", 0.0),
            open_exposure=d.get("open_exposure", 0.0),
            current_balance=d.get("last_balance_usd", 0.0) or 0.0,
            last_known_balance=d.get("last_balance_usd", 0.0) or 0.0,
            circuit_breaker_level=level,
            sizing_multiplier=d.get("sizing_multiplier", 1.0),
            lockout_until=d.get("lockout_until"),
            flags=flags,
            checked_at=d.get("checked_at", ""),
        )
    finally:
        conn.close()


def log_circuit_breaker_event(
    level: int,
    trigger: str,
    risk_state_json: str | None = None,
    db_path: Path | str = DEFAULT_DB_PATH,
) -> int:
    """Log a circuit breaker level change event."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            """INSERT INTO circuit_breaker_events (level, trigger, risk_state_json, created_at)
               VALUES (?, ?, ?, ?)""",
            (level, trigger, risk_state_json, now),
        )
        conn.commit()
        return cur.lastrowid  # type: ignore[return-value]
    finally:
        conn.close()


def get_active_circuit_breaker(
    db_path: Path | str = DEFAULT_DB_PATH,
) -> dict | None:
    """Get the most recent unresolved CB event, or None."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            """SELECT * FROM circuit_breaker_events
               WHERE resolved_at IS NULL
               ORDER BY id DESC LIMIT 1""",
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def resolve_circuit_breaker(
    event_id: int,
    resolved_by: str = "auto",
    db_path: Path | str = DEFAULT_DB_PATH,
) -> None:
    """Mark a CB event as resolved."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        conn.execute(
            """UPDATE circuit_breaker_events
               SET resolved_at = ?, resolved_by = ?
               WHERE id = ?""",
            (now, resolved_by, event_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_band_win_rates(
    db_path: Path | str = DEFAULT_DB_PATH,
) -> dict[str, dict]:
    """Get per-band win rates from settled results.

    Returns {band_label: {"wins": int, "losses": int, "total": int}}.
    """
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT s.price_band, r.won, COUNT(*) as cnt
               FROM results r
               JOIN signals s ON s.id = r.signal_id
               WHERE s.price_band IS NOT NULL AND s.price_band != ''
                 AND s.strategy_mode = 'calibration'
               GROUP BY s.price_band, r.won""",
        ).fetchall()
        result: dict[str, dict] = {}
        for row in rows:
            band = row["price_band"]
            if band not in result:
                result[band] = {"wins": 0, "losses": 0, "total": 0}
            if row["won"]:
                result[band]["wins"] += row["cnt"]
            else:
                result[band]["losses"] += row["cnt"]
            result[band]["total"] += row["cnt"]
        return result
    finally:
        conn.close()


def force_stop_dca_jobs(
    db_path: Path | str = DEFAULT_DB_PATH,
) -> int:
    """Force all dca_active jobs to executed (YELLOW+ CB response).

    Returns the number of jobs stopped.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            """UPDATE trade_jobs
               SET status = 'executed', updated_at = ?
               WHERE status = 'dca_active'""",
            (now,),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()
