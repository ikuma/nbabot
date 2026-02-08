"""Tests for divergence scanner (most critical)."""

from __future__ import annotations

import pytest

from src.connectors.odds_api import BookmakerOdds, GameOdds, TeamOdds
from src.connectors.polymarket import MoneylineMarket
from src.strategy.scanner import _kelly_bet_size, scan


class TestKellyBetSize:
    def test_positive_edge(self, monkeypatch):
        """Positive edge → positive Kelly size."""
        monkeypatch.setattr("src.strategy.scanner.settings.kelly_fraction", 0.25)
        # book_prob=0.65, edge=0.10 → poly_price=0.55
        # b = (1/0.55)-1 ≈ 0.818, p=0.65, q=0.35
        # kelly_full = (0.818*0.65 - 0.35) / 0.818 ≈ 0.222
        size = _kelly_bet_size(edge=0.10, odds_prob=0.65)
        assert size > 0

    def test_no_edge(self, monkeypatch):
        """Zero edge → zero Kelly size."""
        monkeypatch.setattr("src.strategy.scanner.settings.kelly_fraction", 0.25)
        # edge=0 → poly_price == odds_prob → fair bet → kelly = 0
        size = _kelly_bet_size(edge=0, odds_prob=0.5)
        assert size == 0

    def test_prob_zero(self, monkeypatch):
        monkeypatch.setattr("src.strategy.scanner.settings.kelly_fraction", 0.25)
        assert _kelly_bet_size(edge=0.1, odds_prob=0) == 0

    def test_prob_one(self, monkeypatch):
        monkeypatch.setattr("src.strategy.scanner.settings.kelly_fraction", 0.25)
        assert _kelly_bet_size(edge=0.1, odds_prob=1) == 0

    def test_poly_price_out_of_range(self, monkeypatch):
        """Edge larger than odds_prob → poly_price <= 0 → 0."""
        monkeypatch.setattr("src.strategy.scanner.settings.kelly_fraction", 0.25)
        assert _kelly_bet_size(edge=0.6, odds_prob=0.5) == 0

    def test_fraction_scales_result(self, monkeypatch):
        monkeypatch.setattr("src.strategy.scanner.settings.kelly_fraction", 0.5)
        size_half = _kelly_bet_size(edge=0.10, odds_prob=0.65)
        monkeypatch.setattr("src.strategy.scanner.settings.kelly_fraction", 0.25)
        size_quarter = _kelly_bet_size(edge=0.10, odds_prob=0.65)
        assert size_half > 0
        assert size_half == pytest.approx(size_quarter * 2, abs=0.001)


def _make_game(
    home: str,
    away: str,
    home_odds: int = -200,
    away_odds: int = 150,
) -> GameOdds:
    return GameOdds(
        game_id="g1",
        home_team=home,
        away_team=away,
        commence_time="2026-02-08T00:00:00Z",
        bookmakers=[
            BookmakerOdds("book1", "", [
                TeamOdds(
                    home, home_odds,
                    abs(home_odds) / (abs(home_odds) + 100)
                    if home_odds < 0
                    else 100 / (home_odds + 100),
                ),
                TeamOdds(
                    away, away_odds,
                    abs(away_odds) / (abs(away_odds) + 100)
                    if away_odds < 0
                    else 100 / (away_odds + 100),
                ),
            ]),
        ],
    )


def _make_moneyline(
    home: str,
    away: str,
    home_short: str,
    away_short: str,
    home_price: float = 0.55,
    away_price: float = 0.35,
    active: bool = True,
) -> MoneylineMarket:
    return MoneylineMarket(
        condition_id="cond1",
        event_slug="nba-test-slug",
        event_title=f"{away_short} vs {home_short}",
        home_team=home,
        away_team=away,
        outcomes=[away_short, home_short],
        prices=[away_price, home_price],
        token_ids=["token_away", "token_home"],
        sports_market_type="moneyline",
        active=active,
    )


class TestScan:
    def _patch_settings(self, monkeypatch):
        monkeypatch.setattr("src.strategy.scanner.settings.min_edge_pct", 5.0)
        monkeypatch.setattr("src.strategy.scanner.settings.kelly_fraction", 0.25)
        monkeypatch.setattr("src.strategy.scanner.settings.max_position_usd", 100.0)

    def test_buy_signal_when_edge_above_threshold(self, monkeypatch):
        """Polymarket price lower than book consensus → BUY opportunity."""
        self._patch_settings(monkeypatch)
        # BOS consensus ~0.625 (vig-removed), poly price 0.50 → edge ~12.5%
        game = _make_game("Boston Celtics", "New York Knicks", -200, 150)
        ml = _make_moneyline(
            "Boston Celtics", "New York Knicks", "Celtics", "Knicks",
            home_price=0.50, away_price=0.35,
        )
        opps = scan([ml], [game])
        assert len(opps) >= 1
        assert all(o.side == "BUY" for o in opps)

    def test_no_signal_when_edge_below_threshold(self, monkeypatch):
        """Edge < min_edge_pct → empty list."""
        self._patch_settings(monkeypatch)
        # BOS consensus ~0.625, poly price 0.61 → edge ~1.5% (< 5%)
        game = _make_game("Boston Celtics", "New York Knicks", -200, 150)
        ml = _make_moneyline(
            "Boston Celtics", "New York Knicks", "Celtics", "Knicks",
            home_price=0.61, away_price=0.40,
        )
        opps = scan([ml], [game])
        assert opps == []

    def test_no_sell_signal(self, monkeypatch):
        """Polymarket price HIGHER than book → no signal (BUY only)."""
        self._patch_settings(monkeypatch)
        # BOS consensus ~0.625, poly price 0.80 → edge is negative
        game = _make_game("Boston Celtics", "New York Knicks", -200, 150)
        ml = _make_moneyline(
            "Boston Celtics", "New York Knicks", "Celtics", "Knicks",
            home_price=0.80, away_price=0.90,
        )
        opps = scan([ml], [game])
        assert opps == []

    def test_unknown_team_skipped(self, monkeypatch):
        """Outcome short name not in mapping → skipped."""
        self._patch_settings(monkeypatch)
        game = _make_game("Boston Celtics", "New York Knicks")
        ml = _make_moneyline(
            "Boston Celtics", "New York Knicks", "Celtics", "UnknownTeam",
            home_price=0.50, away_price=0.30,
        )
        opps = scan([ml], [game])
        # UnknownTeam should be skipped
        assert all("Unknown" not in o.team for o in opps)

    def test_inactive_moneyline_skipped(self, monkeypatch):
        """Inactive moneyline markets are ignored."""
        self._patch_settings(monkeypatch)
        game = _make_game("Boston Celtics", "New York Knicks")
        ml = _make_moneyline(
            "Boston Celtics", "New York Knicks", "Celtics", "Knicks",
            home_price=0.50, away_price=0.30,
            active=False,
        )
        opps = scan([ml], [game])
        assert opps == []

    def test_multiple_games_sorted_by_edge(self, monkeypatch):
        """Multiple opportunities sorted by edge descending."""
        self._patch_settings(monkeypatch)
        # Game 1: BOS ~0.625 consensus, poly 0.50 → edge ~12.5%
        game1 = _make_game("Boston Celtics", "New York Knicks", -200, 150)
        ml1 = _make_moneyline(
            "Boston Celtics", "New York Knicks", "Celtics", "Knicks",
            home_price=0.50, away_price=0.35,
        )
        # Game 2: LAL ~0.625 consensus, poly 0.40 → edge ~22.5%
        game2 = GameOdds(
            game_id="g2",
            home_team="Los Angeles Lakers",
            away_team="Golden State Warriors",
            commence_time="2026-02-08T02:00:00Z",
            bookmakers=[
                BookmakerOdds("book1", "", [
                    TeamOdds("Los Angeles Lakers", -200, 0.6667),
                    TeamOdds("Golden State Warriors", 150, 0.4),
                ]),
            ],
        )
        ml2 = _make_moneyline(
            "Los Angeles Lakers", "Golden State Warriors", "Lakers", "Warriors",
            home_price=0.40, away_price=0.30,
        )

        opps = scan([ml1, ml2], [game1, game2])
        assert len(opps) >= 2
        # Sorted by edge descending
        for i in range(len(opps) - 1):
            assert opps[i].edge_pct >= opps[i + 1].edge_pct

    def test_no_game_match(self, monkeypatch):
        """Moneyline without matching game → skipped."""
        self._patch_settings(monkeypatch)
        game = _make_game("Boston Celtics", "New York Knicks")
        ml = _make_moneyline(
            "Los Angeles Lakers", "Golden State Warriors", "Lakers", "Warriors",
            home_price=0.40, away_price=0.30,
        )
        opps = scan([ml], [game])
        assert opps == []
