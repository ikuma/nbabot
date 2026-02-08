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


def _connect(db_path: Path | str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Open (or create) the SQLite database and ensure schema exists."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    _ensure_calibration_columns(conn)
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
                price_band, in_sweet_spot, band_confidence, strategy_mode)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                game_title, event_slug, team, side, poly_price, book_prob,
                edge_pct, kelly_size, token_id, bookmakers_count, consensus_std,
                commence_time, now,
                market_type, calibration_edge_pct, expected_win_rate,
                price_band, int(in_sweet_spot), band_confidence, strategy_mode,
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
        rows = conn.execute(
            "SELECT * FROM signals ORDER BY created_at DESC"
        ).fetchall()
        return [SignalRecord(**dict(r)) for r in rows]
    finally:
        conn.close()


def get_all_results(db_path: Path | str = DEFAULT_DB_PATH) -> list[ResultRecord]:
    """Return all results ordered by settlement time (newest first)."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM results ORDER BY settled_at DESC"
        ).fetchall()
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
        pnl_rows = conn.execute(
            "SELECT pnl FROM results ORDER BY settled_at ASC"
        ).fetchall()
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
