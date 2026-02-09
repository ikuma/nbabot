"""Tests for order book liquidity extraction and scoring."""

from __future__ import annotations

import pytest

from src.sizing.liquidity import (
    LiquiditySnapshot,
    _estimate_impact,
    _sum_depth,
    _sum_depth_bid,
    extract_liquidity,
    score_liquidity,
)


def _make_book(
    asks: list[tuple[str, str]] | None = None,
    bids: list[tuple[str, str]] | None = None,
) -> dict:
    """Build a mock order book dict."""
    return {
        "asks": [{"price": p, "size": s} for p, s in (asks or [])],
        "bids": [{"price": p, "size": s} for p, s in (bids or [])],
    }


class TestExtractLiquidity:
    def test_basic_extraction(self):
        book = _make_book(
            asks=[("0.45", "200"), ("0.47", "100"), ("0.50", "300")],
            bids=[("0.43", "150"), ("0.40", "250")],
        )
        snap = extract_liquidity(book, "token_a", order_size_usd=100.0)
        assert snap is not None
        assert snap.token_id == "token_a"
        assert snap.best_ask == 0.45
        assert snap.best_bid == 0.43
        assert snap.spread == pytest.approx(0.02)
        assert snap.midpoint == pytest.approx(0.44)

    def test_empty_book_returns_none(self):
        book = _make_book()
        snap = extract_liquidity(book, "token_a")
        assert snap is None

    def test_asks_only(self):
        book = _make_book(asks=[("0.50", "100")])
        snap = extract_liquidity(book, "token_a")
        assert snap is not None
        assert snap.best_ask == 0.50
        assert snap.best_bid == 0.0

    def test_bids_only(self):
        book = _make_book(bids=[("0.50", "100")])
        snap = extract_liquidity(book, "token_a")
        assert snap is not None
        assert snap.best_bid == 0.50
        assert snap.best_ask == 1.0

    def test_ask_depth_5c(self):
        # best_ask = 0.40; within 5c means ≤ 0.45
        book = _make_book(
            asks=[("0.40", "100"), ("0.44", "50"), ("0.46", "200")],
            bids=[("0.38", "100")],
        )
        snap = extract_liquidity(book, "token_a")
        assert snap is not None
        # depth_5c = 0.40*100 + 0.44*50 = 40 + 22 = 62
        assert snap.ask_depth_5c == pytest.approx(62.0)
        # depth_10c includes all: 0.40*100 + 0.44*50 + 0.46*200 = 40+22+92 = 154
        assert snap.ask_depth_10c == pytest.approx(154.0)

    def test_spread_pct(self):
        book = _make_book(
            asks=[("0.55", "100")],
            bids=[("0.45", "100")],
        )
        snap = extract_liquidity(book, "token_a")
        assert snap is not None
        # spread = 0.10, midpoint = 0.50, spread_pct = 20%
        assert snap.spread_pct == pytest.approx(20.0)


class TestSumDepth:
    def test_within_threshold(self):
        levels = [(0.40, 100), (0.42, 50), (0.46, 200)]
        # best_ask = 0.40, threshold = 0.05 → ≤ 0.45
        depth = _sum_depth(levels, 0.40, 0.05)
        assert depth == pytest.approx(0.40 * 100 + 0.42 * 50)

    def test_empty_levels(self):
        assert _sum_depth([], 0.40, 0.05) == 0.0


class TestSumDepthBid:
    def test_within_threshold(self):
        levels = [(0.50, 100), (0.48, 50), (0.44, 200)]
        # best_bid = 0.50, threshold = 0.05 → ≥ 0.45
        depth = _sum_depth_bid(levels, 0.50, 0.05)
        assert depth == pytest.approx(0.50 * 100 + 0.48 * 50)

    def test_empty_levels(self):
        assert _sum_depth_bid([], 0.50, 0.05) == 0.0


class TestEstimateImpact:
    def test_no_impact_for_small_order(self):
        """Order fits entirely in the best ask level → zero impact."""
        asks = [(0.40, 1000)]  # $400 at 0.40
        impact = _estimate_impact(asks, 50.0)
        assert impact == pytest.approx(0.0)

    def test_impact_across_levels(self):
        """Order crosses multiple levels → positive impact."""
        asks = [(0.40, 100), (0.45, 100)]  # $40 + $45
        impact = _estimate_impact(asks, 80.0)
        assert impact > 0

    def test_empty_asks(self):
        assert _estimate_impact([], 100.0) == 0.0

    def test_zero_order(self):
        assert _estimate_impact([(0.40, 100)], 0.0) == 0.0


class TestScoreLiquidity:
    def _snap(
        self,
        ask_depth_5c: float = 5000.0,
        spread_pct: float = 1.0,
    ) -> LiquiditySnapshot:
        return LiquiditySnapshot(
            token_id="t",
            timestamp="",
            best_ask=0.45,
            best_bid=0.44,
            ask_depth_5c=ask_depth_5c,
            ask_depth_10c=ask_depth_5c * 2,
            bid_depth_5c=ask_depth_5c,
            spread=0.01,
            spread_pct=spread_pct,
            midpoint=0.445,
            impact_estimate=0.0,
            ask_levels=5,
            bid_levels=5,
        )

    def test_high_liquidity(self):
        # $100 order is 2% of $5000 depth, spread 1%
        assert score_liquidity(self._snap(), 100.0) == "high"

    def test_medium_liquidity(self):
        # $100 order is 10% of $1000 depth, spread 5%
        assert score_liquidity(self._snap(ask_depth_5c=1000, spread_pct=5.0), 100.0) == "medium"

    def test_low_liquidity(self):
        # $100 order is 50% of $200 depth, spread 10%
        assert score_liquidity(self._snap(ask_depth_5c=200, spread_pct=10.0), 100.0) == "low"

    def test_insufficient_spread(self):
        # spread > 15%
        assert score_liquidity(self._snap(spread_pct=20.0), 100.0) == "insufficient"

    def test_zero_depth(self):
        assert score_liquidity(self._snap(ask_depth_5c=0), 100.0) == "insufficient"
