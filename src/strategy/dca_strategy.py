"""DCA (Dollar Cost Averaging) decision logic with TWAP adaptive execution.

Pure logic module — no DB access or side effects.
Determines whether to add a DCA entry based on TWAP schedule with adaptive overlay.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class DCAConfig:
    """Configuration for DCA entry decisions."""

    max_entries: int = 5
    min_interval_min: int = 2  # 最小間隔 (分) — 1 tick = 2min
    max_price_spread: float = 0.15  # 初回→最新の最大価格差
    favorable_price_pct: float = 0.0  # 初回価格以下で favorable 判定
    unfavorable_price_pct: float = 10.0  # 10% 以上の上昇で unfavorable
    cutoff_before_tipoff_min: int = 30  # ティップオフ N 分前で DCA 打ち切り


@dataclass
class DCAEntry:
    """Represents a single DCA entry for VWAP calculation."""

    price: float
    size_usd: float
    created_at: datetime


@dataclass
class DCADecision:
    """Result of a DCA entry decision."""

    should_buy: bool
    reason: str  # scheduled / favorable_price / deferred / max_reached / etc.
    current_price: float
    vwap: float
    sequence: int  # 次のシーケンス番号


def calculate_vwap(entries: list[DCAEntry]) -> float:
    """Calculate volume-weighted average price from DCA entries.

    VWAP = total_cost / total_shares
    where shares = size_usd / price for each entry.
    """
    if not entries:
        return 0.0
    total_cost = sum(e.size_usd for e in entries)
    total_shares = sum(e.size_usd / e.price for e in entries if e.price > 0)
    if total_shares <= 0:
        return 0.0
    return total_cost / total_shares


def _calc_twap_schedule(
    first_entry_time: datetime,
    tipoff_time: datetime,
    num_slices: int,
    cutoff_before_tipoff_min: int = 30,
) -> list[datetime]:
    """Calculate TWAP slice times.

    Returns list of N-1 datetimes (slice 0 = first_entry_time, already done).
    """
    cutoff = tipoff_time - timedelta(minutes=cutoff_before_tipoff_min)
    window = (cutoff - first_entry_time).total_seconds()
    if window <= 0 or num_slices <= 1:
        return []

    interval = window / (num_slices - 1)  # N-1 intervals for N slices
    schedule = []
    for i in range(1, num_slices):  # Skip slice 0 (already done)
        schedule.append(first_entry_time + timedelta(seconds=interval * i))
    return schedule


def _is_slice_due(
    entries: list[DCAEntry],
    tipoff_time: datetime,
    now: datetime,
    config: DCAConfig,
) -> bool:
    """Check if the next TWAP slice is due based on schedule."""
    if not entries:
        return False
    first_entry_time = entries[0].created_at
    schedule = _calc_twap_schedule(
        first_entry_time,
        tipoff_time,
        config.max_entries,
        config.cutoff_before_tipoff_min,
    )
    # 次に必要なスライスのインデックス (0-based, entries[0]=slice 0)
    next_slice_idx = len(entries) - 1  # schedule は slice 1 から始まる
    if next_slice_idx < 0 or next_slice_idx >= len(schedule):
        return False
    return now >= schedule[next_slice_idx]


def should_add_dca_entry(
    current_price: float,
    entries: list[DCAEntry],
    tipoff_time: datetime,
    now: datetime,
    config: DCAConfig | None = None,
) -> DCADecision:
    """Decide whether to add a DCA entry using TWAP + adaptive overlay.

    Decision logic (in priority order):
    1. max_entries 到達 → no
    2. エントリーなし → no (初回は scheduler が処理)
    3. price spread 超過 → no (リスクガード)
    4. ティップオフ cutoff 分前を過ぎたら → no (window_closed)
    5. 最小間隔未経過 → no (too_soon)
    6. TWAP スライスが due → yes (scheduled)
    7. 価格が favorable → yes (前倒し購入)
    8. TWAP スライスが due だが価格が unfavorable → no (deferred)
    9. Otherwise → no (slice_not_due)
    """
    if config is None:
        config = DCAConfig()

    next_seq = len(entries) + 1
    vwap = calculate_vwap(entries)

    def _no(reason: str) -> DCADecision:
        return DCADecision(
            should_buy=False,
            reason=reason,
            current_price=current_price,
            vwap=vwap,
            sequence=next_seq,
        )

    def _yes(reason: str) -> DCADecision:
        return DCADecision(
            should_buy=True,
            reason=reason,
            current_price=current_price,
            vwap=vwap,
            sequence=next_seq,
        )

    # 1. max_entries チェック
    if len(entries) >= config.max_entries:
        return _no("max_reached")

    # 2. エントリーがない場合は初回 (scheduler が処理するので DCA 判定不要)
    if not entries:
        return _no("no_previous_entry")

    initial_price = entries[0].price
    last_entry = entries[-1]

    # 3. price spread ガード
    if initial_price > 0 and abs(current_price - initial_price) > config.max_price_spread:
        return _no("price_spread_exceeded")

    # 4. ティップオフ cutoff 前を過ぎたら終了
    cutoff_time = tipoff_time - timedelta(minutes=config.cutoff_before_tipoff_min)
    if now >= cutoff_time:
        return _no("window_closed")

    # 5. 最小間隔チェック
    time_since_last = now - last_entry.created_at
    if time_since_last < timedelta(minutes=config.min_interval_min):
        return _no("too_soon")

    # 価格状態を判定
    is_favorable = initial_price > 0 and current_price <= initial_price * (
        1.0 - config.favorable_price_pct / 100.0
    )
    is_unfavorable = initial_price > 0 and current_price > initial_price * (
        1.0 + config.unfavorable_price_pct / 100.0
    )

    # 6. TWAP スライスが due かチェック
    slice_due = _is_slice_due(entries, tipoff_time, now, config)

    if slice_due:
        # 8. due だが unfavorable → 1 tick 先送り
        if is_unfavorable:
            return _no("deferred")
        # 6. 通常の TWAP スケジュール購入
        return _yes("scheduled")

    # 7. スライス due でなくても、favorable なら前倒し購入
    if is_favorable:
        return _yes("favorable_price")

    return _no("slice_not_due")
