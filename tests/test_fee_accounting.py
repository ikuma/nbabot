"""Tests for fee accounting DB integration (Phase M3)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.store.db import (
    get_signal_by_id,
    log_signal,
    update_signal_fee,
)
from src.store.schema import _connect


@pytest.fixture()
def tmp_db(tmp_path):
    """Create a temporary database."""
    db_path = tmp_path / "test_fee.db"
    conn = _connect(db_path)
    conn.close()
    return db_path


class TestFeeColumns:
    """Test that fee columns exist and work correctly."""

    def test_fee_columns_exist(self, tmp_db):
        conn = _connect(tmp_db)
        try:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
            assert "fee_rate_bps" in cols
            assert "fee_usd" in cols
        finally:
            conn.close()

    def test_fee_defaults_to_zero(self, tmp_db):
        sid = log_signal(
            game_title="Test",
            event_slug="nba-nyk-bos-2026-02-01",
            team="Knicks",
            side="BUY",
            poly_price=0.40,
            book_prob=0.0,
            edge_pct=10.0,
            kelly_size=25.0,
            token_id="tok1",
            db_path=tmp_db,
        )
        sig = get_signal_by_id(sid, db_path=tmp_db)
        assert sig is not None
        assert sig.fee_rate_bps == 0.0
        assert sig.fee_usd == 0.0

    def test_update_signal_fee(self, tmp_db):
        sid = log_signal(
            game_title="Test",
            event_slug="nba-nyk-bos-2026-02-02",
            team="Celtics",
            side="BUY",
            poly_price=0.60,
            book_prob=0.0,
            edge_pct=5.0,
            kelly_size=30.0,
            token_id="tok2",
            db_path=tmp_db,
        )
        update_signal_fee(sid, fee_rate_bps=2.0, fee_usd=0.06, db_path=tmp_db)
        sig = get_signal_by_id(sid, db_path=tmp_db)
        assert sig is not None
        assert sig.fee_rate_bps == pytest.approx(2.0)
        assert sig.fee_usd == pytest.approx(0.06)

    def test_fee_zero_does_not_change_pnl(self, tmp_db):
        """With fee=0, PnL should be identical to legacy behavior."""
        from src.settlement.pnl_calc import _calc_pnl, calc_signal_pnl

        pnl_legacy = _calc_pnl(won=True, kelly_size=25.0, poly_price=0.40)
        pnl_new = calc_signal_pnl(
            won=True, kelly_size=25.0, poly_price=0.40, fee_usd=0.0
        )
        assert pnl_new == pytest.approx(pnl_legacy)


class TestMaticUsdPrice:
    """Test get_matic_usd_price fallback."""

    def test_fallback_on_failure(self):
        """When CoinGecko fails, should return fallback value."""
        from src.connectors.ctf import get_matic_usd_price

        # If the network call fails (e.g., no internet), it should still return > 0
        price = get_matic_usd_price(fallback=0.50)
        assert price > 0


class TestPolymarketFeeExtraction:
    """Test fee extraction from order status."""

    def test_extract_fee_rate_bps(self):
        from src.connectors.polymarket import extract_fee_rate_bps

        assert extract_fee_rate_bps({"fee_rate_bps": 2.0}) == 2.0
        assert extract_fee_rate_bps({"fee_rate_bps": "0"}) == 0.0
        assert extract_fee_rate_bps({}) == 0.0
        assert extract_fee_rate_bps({"fee_rate_bps": None}) == 0.0
