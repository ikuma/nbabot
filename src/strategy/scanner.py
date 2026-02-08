"""Legacy bookmaker divergence scanner (--mode bookmaker).

Compares Polymarket moneyline prices vs sportsbook consensus.
Superseded by calibration_scanner.py for the primary calibration strategy.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from src.config import settings
from src.connectors.odds_api import GameOdds
from src.connectors.polymarket import MoneylineMarket
from src.connectors.team_mapping import full_name_from_short

logger = logging.getLogger(__name__)


@dataclass
class Opportunity:
    game_title: str
    event_slug: str
    team: str
    poly_price: float
    book_prob: float
    edge_pct: float
    side: str  # "BUY"
    token_id: str
    kelly_size: float
    bookmakers_count: int
    consensus_std: float = 0.0
    consensus_detail: dict[str, float] = field(default_factory=dict)


def _kelly_bet_size(edge: float, odds_prob: float) -> float:
    """Fractional Kelly criterion bet sizing.

    edge: our estimated edge (e.g. 0.05 for 5%)
    odds_prob: the fair probability from bookmakers
    """
    if odds_prob <= 0 or odds_prob >= 1:
        return 0
    poly_price = odds_prob - edge
    if poly_price <= 0 or poly_price >= 1:
        return 0
    b = (1 / poly_price) - 1  # Polymarket price-based decimal odds
    p = odds_prob              # true probability (bookmaker consensus)
    q = 1 - p
    kelly_full = (b * p - q) / b if b > 0 else 0
    kelly_full = max(0, kelly_full)
    return kelly_full * settings.kelly_fraction


def scan(
    moneylines: list[MoneylineMarket],
    games: list[GameOdds],
    min_edge: float | None = None,
) -> list[Opportunity]:
    """Scan for divergences between Polymarket moneylines and sportsbook prices.

    Returns opportunities sorted by edge (largest first).
    Only emits BUY signals (Polymarket price is lower than book consensus).
    """
    min_edge = min_edge if min_edge is not None else settings.min_edge_pct / 100
    opportunities: list[Opportunity] = []

    # Index games by (home_team, away_team) for O(1) lookup
    game_by_teams: dict[tuple[str, str], GameOdds] = {
        (g.home_team, g.away_team): g for g in games
    }

    for ml in moneylines:
        if not ml.active:
            continue

        game = game_by_teams.get((ml.home_team, ml.away_team))
        if not game:
            logger.debug("No game match for moneyline: %s", ml.event_title)
            continue

        consensus = game.consensus_probs
        if not consensus:
            continue

        for i, outcome_name in enumerate(ml.outcomes):
            if i >= len(ml.prices) or i >= len(ml.token_ids):
                continue

            full_name = full_name_from_short(outcome_name)
            if not full_name:
                logger.debug("Unknown team short name: %s", outcome_name)
                continue

            book_prob = consensus.get(full_name)
            if book_prob is None:
                continue

            poly_price = ml.prices[i]
            edge = book_prob - poly_price

            # BUY only: skip negative EV and sub-threshold noise
            if edge < min_edge:
                continue

            kelly = _kelly_bet_size(edge, book_prob)
            position_usd = min(kelly * settings.max_position_usd * 10, settings.max_position_usd)

            # 確信度: ブックメーカー間の合意度 (std が小さいほど確信度高)
            team_std = game.consensus_std.get(full_name, 0.0)

            opportunities.append(Opportunity(
                game_title=ml.event_title,
                event_slug=ml.event_slug,
                team=full_name,
                poly_price=poly_price,
                book_prob=book_prob,
                edge_pct=edge * 100,
                side="BUY",
                token_id=ml.token_ids[i],
                kelly_size=position_usd,
                bookmakers_count=len(game.bookmakers),
                consensus_std=team_std,
                consensus_detail=consensus,
            ))

    opportunities.sort(key=lambda o: o.edge_pct, reverse=True)
    return opportunities
