"""Pure PnL calculation functions for settlement.

Extracted from scripts/settle.py — no DB access or side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.store.db import MergeOperation, SignalRecord


def calc_signal_pnl(
    won: bool,
    kelly_size: float,
    poly_price: float,
    fill_price: float | None = None,
    shares_merged: float = 0.0,
    merge_recovery_usd: float = 0.0,
) -> float:
    """Per-signal PnL (self-contained, no group dependency).

    PnL = (remaining_shares × settlement_price) + merge_recovery_usd - cost
    """
    price = fill_price if fill_price is not None else poly_price
    if price <= 0:
        return -kelly_size
    shares_bought = kelly_size / price
    shares_remaining = shares_bought - shares_merged
    settlement_value = shares_remaining * (1.0 if won else 0.0)
    return settlement_value + merge_recovery_usd - kelly_size


def _calc_pnl(
    won: bool,
    kelly_size: float,
    poly_price: float,
    fill_price: float | None = None,
) -> float:
    """Calculate PnL for a single trade.

    BUY at price, risk kelly_size USD.
    Win: profit = kelly_size * (1/price - 1)  (shares pay $1 each)
    Lose: loss = -kelly_size

    Uses fill_price if available (live trade), otherwise poly_price (paper).
    """
    price = fill_price if fill_price is not None else poly_price
    if price <= 0:
        return -kelly_size
    if won:
        return kelly_size * (1.0 / price - 1.0)
    return -kelly_size


def _calc_dca_group_pnl(
    won: bool,
    signals: list[SignalRecord],
) -> float:
    """Calculate PnL for a DCA group (multiple entries on same outcome).

    total_cost = sum(kelly_size)
    total_shares = sum(kelly_size / price)
    win: pnl = total_shares * $1.00 - total_cost
    lose: pnl = -total_cost
    """
    total_cost = 0.0
    total_shares = 0.0
    for sig in signals:
        price = sig.fill_price if sig.fill_price is not None else sig.poly_price
        if price <= 0:
            total_cost += sig.kelly_size
            continue
        total_cost += sig.kelly_size
        total_shares += sig.kelly_size / price
    if won:
        return total_shares * 1.0 - total_cost
    return -total_cost


def _calc_bothside_pnl(
    winner_short: str,
    dir_signals: list[SignalRecord],
    hedge_signals: list[SignalRecord],
) -> tuple[float, float, float]:
    """Calculate combined PnL for a bothside game.

    Returns (dir_pnl, hedge_pnl, combined_pnl).
    """
    dir_won = dir_signals[0].team == winner_short if dir_signals else False
    hedge_won = hedge_signals[0].team == winner_short if hedge_signals else False

    if len(dir_signals) > 1:
        dir_pnl = _calc_dca_group_pnl(dir_won, dir_signals)
    elif dir_signals:
        fill_px = dir_signals[0].fill_price
        dir_pnl = _calc_pnl(dir_won, dir_signals[0].kelly_size, dir_signals[0].poly_price, fill_px)
    else:
        dir_pnl = 0.0

    if len(hedge_signals) > 1:
        hedge_pnl = _calc_dca_group_pnl(hedge_won, hedge_signals)
    elif hedge_signals:
        fill_px = hedge_signals[0].fill_price
        hedge_pnl = _calc_pnl(
            hedge_won, hedge_signals[0].kelly_size, hedge_signals[0].poly_price, fill_px
        )
    else:
        hedge_pnl = 0.0

    return dir_pnl, hedge_pnl, dir_pnl + hedge_pnl


def _calc_merge_pnl(
    merge_op: MergeOperation,
    winner_short: str,
    dir_signals: list[SignalRecord],
    hedge_signals: list[SignalRecord],
) -> tuple[float, float, float]:
    """Calculate PnL for a MERGE-settled bothside group.

    Returns (merge_pnl, remainder_pnl, total_pnl).
    - merge_pnl: net profit from mergePositions (gross - gas)
    - remainder_pnl: PnL from unmerged shares (normal win/loss)
    - total_pnl: merge_pnl + remainder_pnl
    """
    # MERGE 分: gross = merge_amount * (1 - combined_vwap), net = gross - gas
    gross = merge_op.merge_amount * (1.0 - merge_op.combined_vwap)
    gas = merge_op.gas_cost_usd or 0.0
    merge_pnl = gross - gas

    # 残余分: remainder_shares を remainder_side で判定
    remainder_pnl = 0.0
    if merge_op.remainder_shares > 0 and merge_op.remainder_side:
        if merge_op.remainder_side == "directional":
            rem_signals = dir_signals
        else:
            rem_signals = hedge_signals

        if rem_signals:
            rem_team = rem_signals[0].team
            rem_won = rem_team == winner_short

            # 残余シェアのコスト按分計算
            total_cost = sum(s.kelly_size for s in rem_signals)
            total_shares = 0.0
            for s in rem_signals:
                px = s.fill_price if s.fill_price is not None else s.poly_price
                if px > 0:
                    total_shares += s.kelly_size / px

            if total_shares > 0:
                rem_shares = merge_op.remainder_shares
                # 残余コスト = total_cost * (rem_shares / total_shares)
                rem_cost = total_cost * (rem_shares / total_shares)

                if rem_won:
                    remainder_pnl = rem_shares * 1.0 - rem_cost
                else:
                    remainder_pnl = -rem_cost

    total_pnl = merge_pnl + remainder_pnl
    return merge_pnl, remainder_pnl, total_pnl
