"""Data models for the SQLite store.

Extracted from src/store/db.py — dataclasses only, no DB access.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class JobStatus(StrEnum):
    PENDING = "pending"
    EXECUTING = "executing"
    EXECUTED = "executed"
    SKIPPED = "skipped"
    FAILED = "failed"
    EXPIRED = "expired"
    DCA_ACTIVE = "dca_active"
    CANCELLED = "cancelled"


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
    shares_merged: float = 0.0
    merge_recovery_usd: float = 0.0
    # Fee tracking (Phase M3)
    fee_rate_bps: float = 0.0
    fee_usd: float = 0.0
    # Order lifecycle (Phase O)
    order_placed_at: str | None = None
    order_replace_count: int = 0
    order_last_checked_at: str | None = None
    order_original_price: float | None = None


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


@dataclass
class OrderEvent:
    id: int
    signal_id: int
    event_type: str  # 'placed'|'filled'|'cancelled'|'replaced'|'expired'
    order_id: str | None
    price: float | None
    best_ask_at_event: float | None
    created_at: str


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
    early_partial: int = 0
    capital_release_benefit_usd: float | None = None
    additional_fee_usd: float | None = None
    execution_stage: str = "post_dca"


@dataclass
class PositionGroupRecord:
    id: int
    event_slug: str
    game_date: str
    state: str
    M_target: float
    D_target: float
    q_dir: float
    q_opp: float
    merged_qty: float
    d_max: float
    phase_time: str | None
    created_at: str
    updated_at: str


@dataclass
class PositionGroupAuditEvent:
    id: int
    event_slug: str
    audit_type: str
    prev_state: str | None
    new_state: str | None
    reason: str | None
    M_target: float | None
    D_target: float | None
    q_dir: float | None
    q_opp: float | None
    d: float | None
    m: float | None
    d_max: float | None
    merge_amount: float | None
    merged_qty: float | None
    created_at: str
