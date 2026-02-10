"""SQLite store for paper-trade signal logging and result tracking."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "paper_trades.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS signals (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    game_title  TEXT NOT NULL,
    event_slug  TEXT NOT NULL,
    team        TEXT NOT NULL,
    side        TEXT NOT NULL DEFAULT 'BUY',
    poly_price  REAL NOT NULL,
    book_prob   REAL NOT NULL,
    edge_pct    REAL NOT NULL,
    kelly_size  REAL NOT NULL,
    token_id    TEXT NOT NULL,
    bookmakers_count INTEGER NOT NULL DEFAULT 0,
    consensus_std REAL NOT NULL DEFAULT 0.0,
    commence_time TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS results (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id   INTEGER NOT NULL REFERENCES signals(id),
    outcome     TEXT NOT NULL,
    won         INTEGER NOT NULL,
    settlement_price REAL,
    pnl         REAL NOT NULL,
    settled_at  TEXT NOT NULL,
    UNIQUE(signal_id)
);
"""


@dataclass
class SignalRecord:
    id: int
    game_title: str
    event_slug: str
    team: str
    side: str
    poly_price: float
    book_prob: float
    edge_pct: float
    kelly_size: float
    token_id: str
    bookmakers_count: int
    consensus_std: float
    commence_time: str
    created_at: str
    # 校正戦略カラム (既存レコードでは None / デフォルト値)
    market_type: str = "moneyline"
    calibration_edge_pct: float | None = None
    expected_win_rate: float | None = None
    price_band: str = ""
    in_sweet_spot: int = 0
    band_confidence: str = ""
    strategy_mode: str = "bookmaker"
    # 実弾取引カラム
    order_id: str | None = None
    order_status: str = "paper"
    fill_price: float | None = None
    # 流動性カラム
    liquidity_score: str = "unknown"
    ask_depth_5c: float | None = None
    spread_pct: float | None = None
    balance_usd_at_trade: float | None = None
    constraint_binding: str = "kelly"
    # DCA カラム
    dca_group_id: str | None = None
    dca_sequence: int = 1
    # Both-side カラム
    bothside_group_id: str | None = None
    signal_role: str = "directional"
    # MERGE カラム
    condition_id: str | None = None


@dataclass
class ResultRecord:
    id: int
    signal_id: int
    outcome: str
    won: bool
    settlement_price: float | None
    pnl: float
    settled_at: str


@dataclass
class PerformanceStats:
    total_signals: int
    settled_count: int
    unsettled_count: int
    wins: int
    losses: int
    win_rate: float
    total_pnl: float
    avg_pnl: float
    max_drawdown: float
    sharpe_ratio: float


TRADE_JOBS_SQL = """
CREATE TABLE IF NOT EXISTS trade_jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    game_date       TEXT NOT NULL,
    event_slug      TEXT NOT NULL UNIQUE,
    home_team       TEXT NOT NULL,
    away_team       TEXT NOT NULL,
    game_time_utc   TEXT NOT NULL,
    execute_after   TEXT NOT NULL,
    execute_before  TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    signal_id       INTEGER,
    retry_count     INTEGER DEFAULT 0,
    error_message   TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
"""


def _connect(db_path: Path | str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Open (or create) the SQLite database and ensure schema exists."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    conn.executescript(TRADE_JOBS_SQL)
    _ensure_calibration_columns(conn)
    _ensure_execution_columns(conn)
    _ensure_liquidity_columns(conn)
    _ensure_dca_columns(conn)
    _ensure_bothside_columns(conn)
    _ensure_merge_columns(conn)
    return conn


# 校正戦略用の新カラム (既存 DB との後方互換性のため ALTER TABLE で追加)
_CALIBRATION_COLUMNS = [
    ("market_type", "TEXT DEFAULT 'moneyline'"),
    ("calibration_edge_pct", "REAL"),
    ("expected_win_rate", "REAL"),
    ("price_band", "TEXT DEFAULT ''"),
    ("in_sweet_spot", "INTEGER DEFAULT 0"),
    ("band_confidence", "TEXT DEFAULT ''"),
    ("strategy_mode", "TEXT DEFAULT 'bookmaker'"),
]


def _ensure_calibration_columns(conn: sqlite3.Connection) -> None:
    """Add calibration columns to signals table if they don't exist."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
    for col_name, col_def in _CALIBRATION_COLUMNS:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_def}")
    conn.commit()


# 実弾取引用カラム (ALTER TABLE パターン)
_EXECUTION_COLUMNS = [
    ("order_id", "TEXT"),
    ("order_status", "TEXT DEFAULT 'paper'"),
    ("fill_price", "REAL"),
]


def _ensure_execution_columns(conn: sqlite3.Connection) -> None:
    """Add execution columns to signals table if they don't exist."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
    for col_name, col_def in _EXECUTION_COLUMNS:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_def}")
    conn.commit()


# 流動性メタデータ用カラム (ALTER TABLE パターン)
_LIQUIDITY_COLUMNS = [
    ("liquidity_score", "TEXT DEFAULT 'unknown'"),
    ("ask_depth_5c", "REAL"),
    ("spread_pct", "REAL"),
    ("balance_usd_at_trade", "REAL"),
    ("constraint_binding", "TEXT DEFAULT 'kelly'"),
]


def _ensure_liquidity_columns(conn: sqlite3.Connection) -> None:
    """Add liquidity columns to signals table if they don't exist."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
    for col_name, col_def in _LIQUIDITY_COLUMNS:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_def}")
    conn.commit()


# DCA 用カラム (signals + trade_jobs)
_DCA_SIGNAL_COLUMNS = [
    ("dca_group_id", "TEXT"),
    ("dca_sequence", "INTEGER DEFAULT 1"),
]

_DCA_JOB_COLUMNS = [
    ("dca_entries_count", "INTEGER DEFAULT 0"),
    ("dca_max_entries", "INTEGER DEFAULT 1"),
    ("dca_group_id", "TEXT"),
    ("dca_total_budget", "REAL"),
    ("dca_slice_size", "REAL"),
]


def _ensure_dca_columns(conn: sqlite3.Connection) -> None:
    """Add DCA columns to signals and trade_jobs tables if they don't exist."""
    sig_existing = {row[1] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
    for col_name, col_def in _DCA_SIGNAL_COLUMNS:
        if col_name not in sig_existing:
            conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_def}")

    job_existing = {row[1] for row in conn.execute("PRAGMA table_info(trade_jobs)").fetchall()}
    for col_name, col_def in _DCA_JOB_COLUMNS:
        if col_name not in job_existing:
            conn.execute(f"ALTER TABLE trade_jobs ADD COLUMN {col_name} {col_def}")
    conn.commit()


# Both-side 用カラム (signals)
_BOTHSIDE_SIGNAL_COLUMNS = [
    ("bothside_group_id", "TEXT"),
    ("signal_role", "TEXT DEFAULT 'directional'"),
]

# Both-side 用カラム (trade_jobs)
_BOTHSIDE_JOB_COLUMNS = [
    ("job_side", "TEXT DEFAULT 'directional'"),
    ("paired_job_id", "INTEGER"),
    ("bothside_group_id", "TEXT"),
]


def _ensure_bothside_columns(conn: sqlite3.Connection) -> None:
    """Add both-side columns and migrate UNIQUE constraint on trade_jobs."""
    # signals テーブルに bothside カラム追加
    sig_existing = {row[1] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
    for col_name, col_def in _BOTHSIDE_SIGNAL_COLUMNS:
        if col_name not in sig_existing:
            conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_def}")

    # trade_jobs テーブルに bothside カラム追加
    job_existing = {row[1] for row in conn.execute("PRAGMA table_info(trade_jobs)").fetchall()}
    for col_name, col_def in _BOTHSIDE_JOB_COLUMNS:
        if col_name not in job_existing:
            conn.execute(f"ALTER TABLE trade_jobs ADD COLUMN {col_name} {col_def}")

    # UNIQUE 制約マイグレーション: UNIQUE(event_slug) → UNIQUE(event_slug, job_side)
    # SQLite は ALTER TABLE DROP CONSTRAINT 不可のため、テーブル再作成で対応
    # index_list で既存 unique 制約をチェック
    indexes = conn.execute("PRAGMA index_list(trade_jobs)").fetchall()
    needs_migration = False
    for idx in indexes:
        if idx[2]:  # unique index
            idx_info = conn.execute(f"PRAGMA index_info({idx[1]})").fetchall()
            col_names = [info[2] for info in idx_info]
            if col_names == ["event_slug"]:
                needs_migration = True
                break

    if needs_migration:
        # 既存テーブルのカラム情報を取得
        cols_info = conn.execute("PRAGMA table_info(trade_jobs)").fetchall()
        col_names = [c[1] for c in cols_info]

        # 新テーブルを作成 (UNIQUE(event_slug, job_side))
        conn.executescript(f"""
            CREATE TABLE trade_jobs_new (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                game_date       TEXT NOT NULL,
                event_slug      TEXT NOT NULL,
                home_team       TEXT NOT NULL,
                away_team       TEXT NOT NULL,
                game_time_utc   TEXT NOT NULL,
                execute_after   TEXT NOT NULL,
                execute_before  TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'pending',
                signal_id       INTEGER,
                retry_count     INTEGER DEFAULT 0,
                error_message   TEXT,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL,
                dca_entries_count INTEGER DEFAULT 0,
                dca_max_entries INTEGER DEFAULT 1,
                dca_group_id    TEXT,
                dca_total_budget REAL,
                dca_slice_size  REAL,
                job_side        TEXT DEFAULT 'directional',
                paired_job_id   INTEGER,
                bothside_group_id TEXT,
                UNIQUE(event_slug, job_side)
            );
            INSERT INTO trade_jobs_new ({", ".join(col_names)})
                SELECT {", ".join(col_names)} FROM trade_jobs;
            DROP TABLE trade_jobs;
            ALTER TABLE trade_jobs_new RENAME TO trade_jobs;
        """)

    conn.commit()


# MERGE (Phase B2) 用カラム
_MERGE_SIGNAL_COLUMNS = [
    ("condition_id", "TEXT"),
]

_MERGE_JOB_COLUMNS = [
    ("merge_status", "TEXT DEFAULT 'none'"),
    ("merge_operation_id", "INTEGER"),
]

MERGE_OPERATIONS_SQL = """
CREATE TABLE IF NOT EXISTS merge_operations (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    bothside_group_id   TEXT NOT NULL,
    condition_id        TEXT NOT NULL,
    event_slug          TEXT NOT NULL,
    dir_shares          REAL NOT NULL,
    hedge_shares        REAL NOT NULL,
    merge_amount        REAL NOT NULL,
    remainder_shares    REAL NOT NULL,
    remainder_side      TEXT,
    dir_vwap            REAL NOT NULL,
    hedge_vwap          REAL NOT NULL,
    combined_vwap       REAL NOT NULL,
    gross_profit_usd    REAL,
    gas_cost_usd        REAL,
    net_profit_usd      REAL,
    status              TEXT NOT NULL DEFAULT 'pending',
    tx_hash             TEXT,
    error_message       TEXT,
    created_at          TEXT NOT NULL,
    executed_at         TEXT
);
"""


def _ensure_merge_columns(conn: sqlite3.Connection) -> None:
    """Add MERGE columns to signals and trade_jobs, and create merge_operations table."""
    # signals テーブルに condition_id 追加
    sig_existing = {row[1] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
    for col_name, col_def in _MERGE_SIGNAL_COLUMNS:
        if col_name not in sig_existing:
            conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_def}")

    # trade_jobs テーブルに merge カラム追加
    job_existing = {row[1] for row in conn.execute("PRAGMA table_info(trade_jobs)").fetchall()}
    for col_name, col_def in _MERGE_JOB_COLUMNS:
        if col_name not in job_existing:
            conn.execute(f"ALTER TABLE trade_jobs ADD COLUMN {col_name} {col_def}")

    # merge_operations テーブル作成
    conn.executescript(MERGE_OPERATIONS_SQL)
    conn.commit()


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

        # PnL series for drawdown and Sharpe
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
    """Sharpe ratio from individual PnL values (risk-free rate = 0).

    annualize_factor is kept at 1.0 for per-trade Sharpe by default.
    """
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


@dataclass
class TradeJob:
    id: int
    game_date: str
    event_slug: str
    home_team: str
    away_team: str
    game_time_utc: str
    execute_after: str
    execute_before: str
    status: str
    signal_id: int | None
    retry_count: int
    error_message: str | None
    created_at: str
    updated_at: str
    # DCA フィールド (既存 DB では None / デフォルト値)
    dca_entries_count: int = 0
    dca_max_entries: int = 1
    dca_group_id: str | None = None
    dca_total_budget: float | None = None
    dca_slice_size: float | None = None
    # Both-side フィールド
    job_side: str = "directional"
    paired_job_id: int | None = None
    bothside_group_id: str | None = None
    # MERGE フィールド
    merge_status: str = "none"  # none/pending/executed/failed
    merge_operation_id: int | None = None


@dataclass
class JobSummary:
    pending: int = 0
    executing: int = 0
    executed: int = 0
    skipped: int = 0
    failed: int = 0
    expired: int = 0
    cancelled: int = 0
    dca_active: int = 0


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
        # INSERT OR IGNORE で冪等性確保 (UNIQUE on event_slug, job_side)
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
            # 試合時刻が変わっていたら UPDATE (延期対応)
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
        # pending/failed → expired
        cur = conn.execute(
            """UPDATE trade_jobs
               SET status = 'expired', updated_at = ?
               WHERE status IN ('pending', 'failed')
                 AND execute_before <= ?""",
            (now, now_utc),
        )
        count = cur.rowcount
        # dca_active → executed (DCA 完了扱い: ゲーム開始で DCA ウィンドウ終了)
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
    """Create a hedge job for a bothside pair. Idempotent via UNIQUE(event_slug, job_side).

    Returns job id if inserted, None if already exists.
    """
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
    """Get jobs with status='dca_active' that haven't reached max DCA entries.

    Only returns jobs whose game hasn't started yet (execute_before > now).
    """
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


@dataclass
class MergeOperation:
    id: int
    bothside_group_id: str
    condition_id: str
    event_slug: str
    dir_shares: float
    hedge_shares: float
    merge_amount: float
    remainder_shares: float
    remainder_side: str | None
    dir_vwap: float
    hedge_vwap: float
    combined_vwap: float
    gross_profit_usd: float | None
    gas_cost_usd: float | None
    net_profit_usd: float | None
    status: str
    tx_hash: str | None
    error_message: str | None
    created_at: str
    executed_at: str | None


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
    """Get bothside groups eligible for MERGE.

    Returns list of (bothside_group_id, dir_job_id, hedge_job_id) where:
    - Both directional and hedge jobs are status='executed'
    - merge_status='none' (not yet merged)
    - bothside_group_id is not null
    """
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
