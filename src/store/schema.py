"""Database schema DDL and migration helpers.

Extracted from src/store/db.py — schema definitions and column migrations only.
"""

from __future__ import annotations

import sqlite3
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
    sig_existing = {row[1] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
    for col_name, col_def in _BOTHSIDE_SIGNAL_COLUMNS:
        if col_name not in sig_existing:
            conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_def}")

    job_existing = {row[1] for row in conn.execute("PRAGMA table_info(trade_jobs)").fetchall()}
    for col_name, col_def in _BOTHSIDE_JOB_COLUMNS:
        if col_name not in job_existing:
            conn.execute(f"ALTER TABLE trade_jobs ADD COLUMN {col_name} {col_def}")

    # UNIQUE 制約マイグレーション: UNIQUE(event_slug) -> UNIQUE(event_slug, job_side)
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
        cols_info = conn.execute("PRAGMA table_info(trade_jobs)").fetchall()
        col_names = [c[1] for c in cols_info]

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


def _ensure_merge_columns(conn: sqlite3.Connection) -> None:
    """Add MERGE columns to signals and trade_jobs, and create merge_operations table."""
    sig_existing = {row[1] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
    for col_name, col_def in _MERGE_SIGNAL_COLUMNS:
        if col_name not in sig_existing:
            conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_def}")

    job_existing = {row[1] for row in conn.execute("PRAGMA table_info(trade_jobs)").fetchall()}
    for col_name, col_def in _MERGE_JOB_COLUMNS:
        if col_name not in job_existing:
            conn.execute(f"ALTER TABLE trade_jobs ADD COLUMN {col_name} {col_def}")

    conn.executescript(MERGE_OPERATIONS_SQL)
    conn.commit()


RISK_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS circuit_breaker_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    level           INTEGER NOT NULL,
    trigger         TEXT NOT NULL,
    risk_state_json TEXT,
    created_at      TEXT NOT NULL,
    resolved_at     TEXT,
    resolved_by     TEXT
);

CREATE TABLE IF NOT EXISTS risk_snapshots (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    checked_at          TEXT NOT NULL,
    level               TEXT NOT NULL DEFAULT 'GREEN',
    daily_pnl           REAL NOT NULL DEFAULT 0.0,
    weekly_pnl          REAL NOT NULL DEFAULT 0.0,
    consecutive_losses  INTEGER NOT NULL DEFAULT 0,
    max_drawdown_pct    REAL NOT NULL DEFAULT 0.0,
    open_exposure       REAL NOT NULL DEFAULT 0.0,
    sizing_multiplier   REAL NOT NULL DEFAULT 1.0,
    lockout_until       TEXT,
    last_balance_usd    REAL,
    flags               TEXT DEFAULT '[]'
);
"""


def _ensure_risk_tables(conn: sqlite3.Connection) -> None:
    """Create risk management tables if they don't exist."""
    conn.executescript(RISK_TABLES_SQL)
    conn.commit()


def _ensure_indexes(conn: sqlite3.Connection) -> None:
    """Create performance indexes if they don't exist."""
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_signals_event_slug ON signals(event_slug)",
        "CREATE INDEX IF NOT EXISTS idx_signals_dca_group ON signals(dca_group_id)",
        "CREATE INDEX IF NOT EXISTS idx_signals_bothside_group ON signals(bothside_group_id)",
        "CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_results_settled_at ON results(settled_at)",
        "CREATE INDEX IF NOT EXISTS idx_trade_jobs_status ON trade_jobs(status)",
        "CREATE INDEX IF NOT EXISTS idx_trade_jobs_game_date ON trade_jobs(game_date)",
    ]
    for sql in indexes:
        conn.execute(sql)
    conn.commit()


def _connect(db_path: Path | str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Open (or create) the SQLite database and ensure schema exists."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA_SQL)
    conn.executescript(TRADE_JOBS_SQL)
    _ensure_calibration_columns(conn)
    _ensure_execution_columns(conn)
    _ensure_liquidity_columns(conn)
    _ensure_dca_columns(conn)
    _ensure_bothside_columns(conn)
    _ensure_merge_columns(conn)
    _ensure_risk_tables(conn)
    _ensure_indexes(conn)
    return conn
