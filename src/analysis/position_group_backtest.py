"""First-principles backtest for MERGE-only / Directional-only / Composite."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PositionGroupGameInput:
    event_slug: str
    directional_price: float
    opposite_price: float
    directional_won: bool


@dataclass(frozen=True)
class StrategyAggregate:
    games: int
    total_pnl: float
    avg_pnl: float
    win_games: int
    win_rate: float


@dataclass(frozen=True)
class StrategyComparison:
    merge_only: StrategyAggregate
    directional_only: StrategyAggregate
    composite: StrategyAggregate
    composite_superior: bool


def _merge_pnl_per_game(
    *,
    directional_price: float,
    opposite_price: float,
    merge_shares: float,
    fee_per_share: float,
    gas_per_game: float,
) -> float:
    combined = directional_price + opposite_price
    edge = 1.0 - combined
    gross = merge_shares * edge
    costs = merge_shares * fee_per_share + gas_per_game
    return gross - costs


def _directional_pnl_per_game(
    *,
    directional_price: float,
    directional_shares: float,
    directional_won: bool,
    fee_per_share: float,
) -> float:
    payout = directional_shares if directional_won else 0.0
    cost = directional_shares * directional_price
    fees = directional_shares * fee_per_share
    return payout - cost - fees


def _aggregate(pnls: list[float]) -> StrategyAggregate:
    n = len(pnls)
    if n == 0:
        return StrategyAggregate(games=0, total_pnl=0.0, avg_pnl=0.0, win_games=0, win_rate=0.0)
    total = sum(pnls)
    wins = sum(1 for p in pnls if p > 0)
    return StrategyAggregate(
        games=n,
        total_pnl=total,
        avg_pnl=total / n,
        win_games=wins,
        win_rate=wins / n,
    )


def compare_position_group_strategies(
    games: list[PositionGroupGameInput],
    *,
    merge_shares: float,
    directional_shares: float,
    fee_per_share: float = 0.0,
    gas_per_game: float = 0.0,
) -> StrategyComparison:
    """Compare 3 strategies under a shared execution assumption.

    - merge_only: hold M shares on both sides, settle via MERGE.
    - directional_only: hold D shares on directional side only.
    - composite: MERGE(M) + directional residual(D).
    """
    if merge_shares < 0 or directional_shares < 0:
        raise ValueError("merge_shares and directional_shares must be >= 0")

    merge_pnls: list[float] = []
    directional_pnls: list[float] = []
    composite_pnls: list[float] = []

    for g in games:
        merge_pnl = _merge_pnl_per_game(
            directional_price=g.directional_price,
            opposite_price=g.opposite_price,
            merge_shares=merge_shares,
            fee_per_share=fee_per_share,
            gas_per_game=gas_per_game,
        )
        dir_pnl = _directional_pnl_per_game(
            directional_price=g.directional_price,
            directional_shares=directional_shares,
            directional_won=g.directional_won,
            fee_per_share=fee_per_share,
        )
        merge_pnls.append(merge_pnl)
        directional_pnls.append(dir_pnl)
        composite_pnls.append(merge_pnl + dir_pnl)

    merge_only = _aggregate(merge_pnls)
    directional_only = _aggregate(directional_pnls)
    composite = _aggregate(composite_pnls)
    composite_superior = (
        composite.total_pnl > merge_only.total_pnl
        and composite.total_pnl > directional_only.total_pnl
    )
    return StrategyComparison(
        merge_only=merge_only,
        directional_only=directional_only,
        composite=composite,
        composite_superior=composite_superior,
    )
