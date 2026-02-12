"""Tests for calc_signal_pnl (per-signal PnL with merge support)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.settlement.pnl_calc import _calc_pnl, calc_signal_pnl


class TestCalcSignalPnlNoMerge:
    """Without merge data, calc_signal_pnl should match _calc_pnl."""

    def test_win_no_merge(self):
        pnl_new = calc_signal_pnl(won=True, kelly_size=25.0, poly_price=0.40)
        pnl_old = _calc_pnl(won=True, kelly_size=25.0, poly_price=0.40)
        assert pnl_new == pytest.approx(pnl_old)

    def test_loss_no_merge(self):
        pnl_new = calc_signal_pnl(won=False, kelly_size=25.0, poly_price=0.40)
        pnl_old = _calc_pnl(won=False, kelly_size=25.0, poly_price=0.40)
        assert pnl_new == pytest.approx(pnl_old)

    def test_win_with_fill_price(self):
        pnl_new = calc_signal_pnl(won=True, kelly_size=25.0, poly_price=0.40, fill_price=0.39)
        pnl_old = _calc_pnl(won=True, kelly_size=25.0, poly_price=0.40, fill_price=0.39)
        assert pnl_new == pytest.approx(pnl_old)

    def test_price_zero(self):
        pnl = calc_signal_pnl(won=True, kelly_size=10.0, poly_price=0.0)
        assert pnl == pytest.approx(-10.0)


class TestCalcSignalPnlWithMerge:
    """With merge data, calc_signal_pnl uses remaining shares + recovery."""

    def test_partial_merge_team_loses(self):
        """IND@BKN directional (Nets): team lost, but merge recovery reduces loss."""
        # id=13: dir, Nets, price=0.665, kelly=19.85, merged=25.41
        # recovery = 25.41 * 0.665 / 0.97 = 17.42
        merged = 25.41
        recovery = 25.41 * 0.665 / 0.97
        pnl = calc_signal_pnl(
            won=False,
            kelly_size=19.85,
            poly_price=0.665,
            shares_merged=merged,
            merge_recovery_usd=recovery,
        )
        # shares_bought = 19.85 / 0.665 = 29.85
        # remaining = 29.85 - 25.41 = 4.44
        # settlement = 4.44 * 0 = 0 (lost)
        # pnl = 0 + 17.42 - 19.85 = -2.43
        expected = 0.0 + recovery - 19.85
        assert pnl == pytest.approx(expected, abs=0.01)

    def test_partial_merge_team_wins(self):
        """IND@BKN hedge (Pacers): team won, remaining shares pay $1."""
        # id=15: hedge, Pacers, price=0.305, kelly=23.25, merged=76.23
        # recovery = 76.23 * 0.305 / 0.97 = 23.96
        merged = 76.23
        recovery = 76.23 * 0.305 / 0.97
        pnl = calc_signal_pnl(
            won=True,
            kelly_size=23.25,
            poly_price=0.305,
            shares_merged=merged,
            merge_recovery_usd=recovery,
        )
        # shares_bought = 23.25 / 0.305 = 76.23
        # remaining = 76.23 - 76.23 = 0
        # settlement = 0 * 1 = 0
        # pnl = 0 + 23.96 - 23.25 = 0.71
        expected = 0.0 + recovery - 23.25
        assert pnl == pytest.approx(expected, abs=0.01)

    def test_all_shares_merged_win(self):
        """All shares merged — win doesn't matter, recovery is the only income."""
        kelly = 10.0
        price = 0.50
        shares = kelly / price  # 20 shares
        recovery = shares * price / 0.95  # some recovery

        pnl_win = calc_signal_pnl(
            won=True, kelly_size=kelly, poly_price=price,
            shares_merged=shares, merge_recovery_usd=recovery,
        )
        pnl_loss = calc_signal_pnl(
            won=False, kelly_size=kelly, poly_price=price,
            shares_merged=shares, merge_recovery_usd=recovery,
        )
        # All shares merged → remaining = 0 → settlement = 0 regardless
        assert pnl_win == pytest.approx(pnl_loss)
        assert pnl_win == pytest.approx(recovery - kelly)

    def test_zero_shares_merged(self):
        """shares_merged=0 → identical to no-merge case."""
        pnl_merge = calc_signal_pnl(
            won=True, kelly_size=25.0, poly_price=0.40,
            shares_merged=0.0, merge_recovery_usd=0.0,
        )
        pnl_plain = _calc_pnl(won=True, kelly_size=25.0, poly_price=0.40)
        assert pnl_merge == pytest.approx(pnl_plain)


class TestCalcSignalPnlWithFee:
    """Fee deduction in calc_signal_pnl (Phase M3)."""

    def test_fee_deducted_from_win(self):
        pnl_no_fee = calc_signal_pnl(won=True, kelly_size=25.0, poly_price=0.40)
        pnl_with_fee = calc_signal_pnl(won=True, kelly_size=25.0, poly_price=0.40, fee_usd=0.50)
        assert pnl_with_fee == pytest.approx(pnl_no_fee - 0.50)

    def test_fee_deducted_from_loss(self):
        pnl_no_fee = calc_signal_pnl(won=False, kelly_size=25.0, poly_price=0.40)
        pnl_with_fee = calc_signal_pnl(won=False, kelly_size=25.0, poly_price=0.40, fee_usd=0.10)
        assert pnl_with_fee == pytest.approx(pnl_no_fee - 0.10)

    def test_fee_zero_backward_compat(self):
        """fee_usd=0 gives same result as not passing it."""
        pnl1 = calc_signal_pnl(won=True, kelly_size=10.0, poly_price=0.50)
        pnl2 = calc_signal_pnl(won=True, kelly_size=10.0, poly_price=0.50, fee_usd=0.0)
        assert pnl1 == pytest.approx(pnl2)

    def test_fee_with_merge(self):
        """Fee deducted even when merge recovery is present."""
        fee = 0.25
        pnl = calc_signal_pnl(
            won=False, kelly_size=20.0, poly_price=0.50,
            shares_merged=30.0, merge_recovery_usd=18.0, fee_usd=fee,
        )
        pnl_no_fee = calc_signal_pnl(
            won=False, kelly_size=20.0, poly_price=0.50,
            shares_merged=30.0, merge_recovery_usd=18.0,
        )
        assert pnl == pytest.approx(pnl_no_fee - fee)

    def test_fee_with_zero_price(self):
        """Fee still deducted when price is zero."""
        pnl = calc_signal_pnl(won=True, kelly_size=10.0, poly_price=0.0, fee_usd=0.50)
        assert pnl == pytest.approx(-10.0 - 0.50)


class TestBothsideMergeIntegration:
    """Integration: bothside merge where dir loses, merge recovers partial loss."""

    def test_full_merge_both_sides(self):
        """All shares merged → PnL = merge_profit (regardless of game outcome)."""
        # Dir: kelly=30, price=0.60, shares=50
        # Hedge: kelly=20, price=0.40, shares=50
        # merge_amount = 50, combined_vwap = 0.60 + 0.40 = 1.00 → no profit
        # Use combined_vwap = 0.95 (below $1)
        combined_vwap = 0.95
        merge_amount = 50.0

        dir_price = 0.60
        dir_kelly = 30.0  # shares = 50
        hedge_price = 0.35
        hedge_kelly = 17.5  # shares = 50

        dir_merged = merge_amount  # all merged
        dir_recovery = dir_merged * dir_price / combined_vwap
        hedge_merged = merge_amount
        hedge_recovery = hedge_merged * hedge_price / combined_vwap

        # Dir team lost → remaining = 0 → settlement = 0
        dir_pnl = calc_signal_pnl(
            won=False, kelly_size=dir_kelly, poly_price=dir_price,
            shares_merged=dir_merged, merge_recovery_usd=dir_recovery,
        )
        # Hedge team won → remaining = 0 → settlement = 0
        hedge_pnl = calc_signal_pnl(
            won=True, kelly_size=hedge_kelly, poly_price=hedge_price,
            shares_merged=hedge_merged, merge_recovery_usd=hedge_recovery,
        )

        total = dir_pnl + hedge_pnl
        # Expected: merge_amount * (1 - combined_vwap) = 50 * 0.05 = $2.50
        expected = merge_amount * (1.0 - combined_vwap)
        assert total == pytest.approx(expected, abs=0.01)

    def test_partial_merge_dir_loses(self):
        """Dir has fewer shares → all dir merged, hedge remainder wins."""
        combined_vwap = 0.95
        dir_price = 0.60
        dir_kelly = 30.0
        dir_shares = dir_kelly / dir_price  # 50

        hedge_price = 0.35
        hedge_kelly = 28.0
        hedge_shares = hedge_kelly / hedge_price  # 80

        merge_amount = min(dir_shares, hedge_shares)  # 50

        # Dir: all merged
        dir_merged = merge_amount * (dir_shares / dir_shares)  # 50
        dir_recovery = dir_merged * dir_price / combined_vwap

        # Hedge: 50/80 merged
        hedge_merged = merge_amount * (hedge_shares / hedge_shares)
        # Actually per-signal, hedge_merged = merge_amount * (this_sig_shares / total_hedge_shares)
        # Since there's one hedge signal: hedge_merged = merge_amount = 50
        hedge_merged = merge_amount
        hedge_recovery = hedge_merged * hedge_price / combined_vwap

        # Dir team lost
        dir_pnl = calc_signal_pnl(
            won=False, kelly_size=dir_kelly, poly_price=dir_price,
            shares_merged=dir_merged, merge_recovery_usd=dir_recovery,
        )

        # Hedge team won, 30 remaining shares pay $1 each
        hedge_pnl = calc_signal_pnl(
            won=True, kelly_size=hedge_kelly, poly_price=hedge_price,
            shares_merged=hedge_merged, merge_recovery_usd=hedge_recovery,
        )

        # Dir: remaining=0, pnl = recovery - cost
        assert dir_pnl == pytest.approx(dir_recovery - dir_kelly)

        # Hedge: remaining = 80-50 = 30, pnl = 30*1 + recovery - 28
        expected_hedge = (hedge_shares - hedge_merged) * 1.0 + hedge_recovery - hedge_kelly
        assert hedge_pnl == pytest.approx(expected_hedge)

        # Total should include merge profit + remainder settlement
        total = dir_pnl + hedge_pnl
        merge_profit = merge_amount * (1.0 - combined_vwap)
        remainder_profit = (hedge_shares - merge_amount) * 1.0 - hedge_kelly * (
            (hedge_shares - merge_amount) / hedge_shares
        )
        assert total == pytest.approx(merge_profit + remainder_profit, abs=0.01)

    def test_dca_dir_signals_with_merge(self):
        """Multiple DCA dir entries, each gets proportional merge recovery."""
        combined_vwap = 0.96
        merge_amount = 40.0  # min(dir_total_shares, hedge_shares)

        entries = [
            {"kelly": 10.0, "price": 0.50},  # 20 shares
            {"kelly": 10.0, "price": 0.40},  # 25 shares
        ]
        dir_total_shares = sum(e["kelly"] / e["price"] for e in entries)  # 45

        dir_pnls = []
        for e in entries:
            sig_shares = e["kelly"] / e["price"]
            sig_merged = merge_amount * (sig_shares / dir_total_shares)
            sig_recovery = sig_merged * e["price"] / combined_vwap
            pnl = calc_signal_pnl(
                won=False, kelly_size=e["kelly"], poly_price=e["price"],
                shares_merged=sig_merged, merge_recovery_usd=sig_recovery,
            )
            dir_pnls.append(pnl)

        # Verify total dir recovery matches merge_amount * dir_vwap / combined_vwap
        total_dir_recovery = sum(
            merge_amount * ((e["kelly"] / e["price"]) / dir_total_shares)
            * e["price"] / combined_vwap for e in entries
        )
        # dir_vwap = total_cost / total_shares = 20 / 45 = 0.4444
        dir_vwap = 20.0 / dir_total_shares
        expected_total_recovery = merge_amount * dir_vwap / combined_vwap
        assert total_dir_recovery == pytest.approx(expected_total_recovery, abs=0.01)
