"""Sizing logic for GamePositionGroup targets (Track B3)."""

from __future__ import annotations

from dataclasses import dataclass

from src.config import settings


@dataclass
class PositionGroupSizingInputs:
    directional_price: float | None = None
    opposite_price: float | None = None
    directional_expected_win_rate: float | None = None
    directional_band_confidence: str = ""
    directional_vwap: float | None = None
    opposite_vwap: float | None = None


@dataclass
class PositionGroupTargets:
    m_target: float
    d_target: float
    q_dir_target: float
    q_opp_target: float
    merge_edge: float
    u_conf: float
    u_regime: float


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _kelly_low_fraction(*, p_low: float, price: float) -> float:
    """Kelly fraction from conservative win rate p_low and market price."""
    if price <= 0 or price >= 1:
        return 0.0
    p = _clamp(p_low, 0.0, 1.0)
    b = (1.0 / price) - 1.0
    if b <= 0:
        return 0.0
    q = 1.0 - p
    return max((b * p - q) / b, 0.0)


def _confidence_multiplier(confidence: str) -> float:
    c = (confidence or "").strip().lower()
    if c == "high":
        return 1.0
    if c == "medium":
        return 0.8
    if c == "low":
        return 0.6
    return 0.7


def _compute_d_target_shares(
    *,
    balance_usd: float,
    p_low: float | None,
    price: float | None,
    u_conf: float,
    u_regime: float,
) -> float:
    if not p_low or not price:
        return 0.0

    kelly_low = _kelly_low_fraction(p_low=p_low, price=price)
    raw_usd = balance_usd * settings.kelly_fraction * kelly_low * u_conf * u_regime
    capped_usd = min(raw_usd, settings.max_position_usd, settings.risk_max_single_game_usd)
    if capped_usd <= 0:
        return 0.0
    return max(capped_usd / price, 0.0)


def _compute_merge_edge_per_share(
    *,
    combined_price: float,
) -> float:
    denom = max(settings.merge_min_shares_floor, 1.0)
    overhead = (settings.merge_est_gas_usd + settings.merge_min_profit_usd) / denom
    return 1.0 - combined_price - overhead


def _compute_m_target_shares(
    *,
    balance_usd: float,
    dir_price: float | None,
    opp_price: float | None,
    dir_vwap: float | None,
    opp_vwap: float | None,
    u_regime: float,
) -> tuple[float, float]:
    price_dir = dir_vwap if (dir_vwap and dir_vwap > 0) else dir_price
    price_opp = opp_vwap if (opp_vwap and opp_vwap > 0) else opp_price
    if not price_dir or not price_opp:
        return 0.0, -1.0

    combined = price_dir + price_opp
    if combined <= 0 or combined >= 2:
        return 0.0, -1.0

    edge = _compute_merge_edge_per_share(combined_price=combined)
    if edge <= 0:
        return 0.0, edge

    merge_budget_usd = min(
        settings.max_position_usd,
        balance_usd * (settings.capital_risk_pct / 100.0),
    )
    merge_budget_usd *= u_regime
    if merge_budget_usd <= 0:
        return 0.0, edge

    return max(merge_budget_usd / combined, 0.0), edge


def compute_position_group_targets(
    *,
    inputs: PositionGroupSizingInputs,
    balance_usd: float,
    u_regime: float = 1.0,
) -> PositionGroupTargets:
    """Compute M*/D* and target inventories (shares)."""
    price = inputs.directional_price
    opp_price = inputs.opposite_price
    if price and not opp_price:
        opp_price = max(1.0 - price, 0.0)

    u_conf = _confidence_multiplier(inputs.directional_band_confidence)
    u_reg = _clamp(u_regime, 0.0, 1.0)
    bal = max(balance_usd, 0.0)

    d_target = _compute_d_target_shares(
        balance_usd=bal,
        p_low=inputs.directional_expected_win_rate,
        price=price,
        u_conf=u_conf,
        u_regime=u_reg,
    )
    m_target, merge_edge = _compute_m_target_shares(
        balance_usd=bal,
        dir_price=price,
        opp_price=opp_price,
        dir_vwap=inputs.directional_vwap,
        opp_vwap=inputs.opposite_vwap,
        u_regime=u_reg,
    )

    q_opp_target = m_target
    q_dir_target = m_target + d_target
    return PositionGroupTargets(
        m_target=m_target,
        d_target=d_target,
        q_dir_target=q_dir_target,
        q_opp_target=q_opp_target,
        merge_edge=merge_edge,
        u_conf=u_conf,
        u_regime=u_reg,
    )
