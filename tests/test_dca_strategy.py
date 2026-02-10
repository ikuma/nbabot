"""Tests for DCA strategy decision logic with TWAP adaptive execution."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.strategy.dca_strategy import (
    DCAConfig,
    DCAEntry,
    _calc_twap_schedule,
    _is_slice_due,
    calculate_vwap,
    should_add_dca_entry,
)


def _now() -> datetime:
    return datetime(2026, 2, 10, 18, 0, 0, tzinfo=timezone.utc)


def _tipoff() -> datetime:
    """Game tipoff at 2026-02-11 01:00 UTC (8pm ET)."""
    return datetime(2026, 2, 11, 1, 0, 0, tzinfo=timezone.utc)


def _make_entry(
    price: float = 0.40,
    size_usd: float = 25.0,
    hours_ago: float = 2.0,
) -> DCAEntry:
    return DCAEntry(
        price=price,
        size_usd=size_usd,
        created_at=_now() - timedelta(hours=hours_ago),
    )


class TestCalculateVwap:
    def test_empty(self):
        assert calculate_vwap([]) == 0.0

    def test_single_entry(self):
        entries = [DCAEntry(price=0.40, size_usd=50.0, created_at=_now())]
        assert calculate_vwap(entries) == pytest.approx(0.40)

    def test_multiple_entries(self):
        entries = [
            DCAEntry(price=0.40, size_usd=50.0, created_at=_now()),
            DCAEntry(price=0.35, size_usd=50.0, created_at=_now()),
        ]
        # total_cost = 100, total_shares = 50/0.40 + 50/0.35 = 125 + 142.857 = 267.857
        # vwap = 100 / 267.857 ≈ 0.3733
        vwap = calculate_vwap(entries)
        assert vwap == pytest.approx(100 / (50 / 0.40 + 50 / 0.35))

    def test_zero_price_entry(self):
        entries = [
            DCAEntry(price=0.0, size_usd=50.0, created_at=_now()),
            DCAEntry(price=0.40, size_usd=50.0, created_at=_now()),
        ]
        # Zero-price entry contributes cost but no shares
        vwap = calculate_vwap(entries)
        assert vwap == pytest.approx(100 / (50 / 0.40))


class TestCalcTwapSchedule:
    def test_basic_schedule(self):
        first = _now()
        tipoff = first + timedelta(hours=7)  # 7h window
        schedule = _calc_twap_schedule(first, tipoff, num_slices=5, cutoff_before_tipoff_min=30)
        # window = 7h - 30min = 6.5h = 390min
        # interval = 390 / 4 = 97.5min
        assert len(schedule) == 4  # N-1 slices (slice 0 is done)
        for i, t in enumerate(schedule, start=1):
            expected = first + timedelta(minutes=97.5 * i)
            assert abs((t - expected).total_seconds()) < 1

    def test_single_slice(self):
        schedule = _calc_twap_schedule(_now(), _tipoff(), num_slices=1)
        assert schedule == []

    def test_zero_window(self):
        # 初回エントリーが cutoff 以降
        first = _tipoff() - timedelta(minutes=10)
        schedule = _calc_twap_schedule(first, _tipoff(), num_slices=5, cutoff_before_tipoff_min=30)
        assert schedule == []

    def test_two_slices(self):
        first = _now()
        tipoff = first + timedelta(hours=4)
        schedule = _calc_twap_schedule(first, tipoff, num_slices=2, cutoff_before_tipoff_min=30)
        # window = 4h - 30min = 210min, interval = 210 / 1 = 210min
        assert len(schedule) == 1
        expected = first + timedelta(minutes=210)
        assert abs((schedule[0] - expected).total_seconds()) < 1


class TestShouldAddDcaEntry:
    def test_no_previous_entries(self):
        """No entries → should not buy (initial handled by scheduler)."""
        decision = should_add_dca_entry(0.40, [], _tipoff(), _now())
        assert not decision.should_buy
        assert decision.reason == "no_previous_entry"

    def test_max_entries_reached(self):
        config = DCAConfig(max_entries=2)
        entries = [_make_entry(hours_ago=4), _make_entry(hours_ago=2)]
        decision = should_add_dca_entry(0.40, entries, _tipoff(), _now(), config)
        assert not decision.should_buy
        assert decision.reason == "max_reached"

    def test_price_spread_exceeded(self):
        config = DCAConfig(max_price_spread=0.10)
        entries = [_make_entry(price=0.40, hours_ago=2)]
        # Current price 0.55 → spread = 0.15 > 0.10
        decision = should_add_dca_entry(0.55, entries, _tipoff(), _now(), config)
        assert not decision.should_buy
        assert decision.reason == "price_spread_exceeded"

    def test_too_soon(self):
        config = DCAConfig(min_interval_min=60)
        # Last entry 30 minutes ago
        entries = [_make_entry(hours_ago=0.5)]
        decision = should_add_dca_entry(0.40, entries, _tipoff(), _now(), config)
        assert not decision.should_buy
        assert decision.reason == "too_soon"

    def test_twap_scheduled(self):
        """TWAP slice due → scheduled purchase."""
        # 初回エントリー: 5時間前 (T-5h)
        # tipoff: T+7h, cutoff: T+6.5h
        # window = 11.5h, interval = 11.5h/4 = 2.875h = 172.5min
        # slice 1 due at: T-5h + 172.5min = T-5h + 2h52.5m = T-2h07.5m → now (T) is after
        config = DCAConfig(max_entries=5, min_interval_min=2, cutoff_before_tipoff_min=30)
        entries = [_make_entry(hours_ago=5)]
        tipoff = _now() + timedelta(hours=7)
        decision = should_add_dca_entry(0.40, entries, tipoff, _now(), config)
        assert decision.should_buy
        assert decision.reason == "scheduled"

    def test_slice_not_due(self):
        """TWAP slice not yet due → no buy."""
        # 初回エントリー: 10分前
        # tipoff: T+7h, cutoff T+6.5h
        # window ≈ 6h50min, interval ≈ 102.5min
        # slice 1 due at: T-10min + 102.5min = T+92.5min → now (T) is before
        # 現在価格 0.41 > 初回 0.40 → not favorable (favorable_pct=0 requires <= initial)
        config = DCAConfig(max_entries=5, min_interval_min=2, cutoff_before_tipoff_min=30)
        entries = [_make_entry(price=0.40, hours_ago=10 / 60)]  # 10分前
        tipoff = _now() + timedelta(hours=7)
        decision = should_add_dca_entry(0.41, entries, tipoff, _now(), config)
        assert not decision.should_buy
        assert decision.reason == "slice_not_due"

    def test_favorable_price_early_buy(self):
        """Price below initial → buy early even if slice not due."""
        # 初回エントリー: 10分前 (slice not due yet)
        # favorable_price_pct=0.0 → 初回価格以下で favorable
        config = DCAConfig(
            max_entries=5,
            min_interval_min=2,
            favorable_price_pct=0.0,
            cutoff_before_tipoff_min=30,
        )
        entries = [_make_entry(price=0.40, hours_ago=10 / 60)]  # 10分前
        tipoff = _now() + timedelta(hours=7)
        # 現在価格 0.38 < 初回 0.40 → favorable
        decision = should_add_dca_entry(0.38, entries, tipoff, _now(), config)
        assert decision.should_buy
        assert decision.reason == "favorable_price"

    def test_favorable_price_requires_dip(self):
        """favorable_price_pct=5 means price must be 5% below initial."""
        config = DCAConfig(
            max_entries=5,
            min_interval_min=2,
            favorable_price_pct=5.0,
            cutoff_before_tipoff_min=30,
        )
        entries = [_make_entry(price=0.40, hours_ago=10 / 60)]
        tipoff = _now() + timedelta(hours=7)
        # 0.39 → only 2.5% below 0.40, not enough for 5% threshold
        decision = should_add_dca_entry(0.39, entries, tipoff, _now(), config)
        assert not decision.should_buy
        assert decision.reason == "slice_not_due"

    def test_unfavorable_price_deferred(self):
        """Price 10%+ above initial, slice due → deferred."""
        # 初回エントリー: 5時間前 (slice is due)
        config = DCAConfig(
            max_entries=5,
            min_interval_min=2,
            unfavorable_price_pct=10.0,
            cutoff_before_tipoff_min=30,
        )
        entries = [_make_entry(price=0.40, hours_ago=5)]
        tipoff = _now() + timedelta(hours=7)
        # 現在価格 0.45 → 12.5% above 0.40 → unfavorable
        decision = should_add_dca_entry(0.45, entries, tipoff, _now(), config)
        assert not decision.should_buy
        assert decision.reason == "deferred"

    def test_window_closed(self):
        """Within cutoff before tipoff → no buy."""
        config = DCAConfig(cutoff_before_tipoff_min=30)
        entries = [_make_entry(hours_ago=5)]
        tipoff = _now() + timedelta(minutes=20)  # only 20 min to tipoff
        decision = should_add_dca_entry(0.40, entries, tipoff, _now(), config)
        assert not decision.should_buy
        assert decision.reason == "window_closed"

    def test_sequence_increments(self):
        entries = [_make_entry(hours_ago=4), _make_entry(hours_ago=2)]
        decision = should_add_dca_entry(0.40, entries, _tipoff(), _now())
        assert decision.sequence == 3


class TestIsSliceDue:
    def test_first_slice_due(self):
        """After enough time, first DCA slice should be due."""
        config = DCAConfig(max_entries=5, cutoff_before_tipoff_min=30)
        first = _now() - timedelta(hours=5)
        entries = [DCAEntry(price=0.40, size_usd=25.0, created_at=first)]
        tipoff = _now() + timedelta(hours=2)
        assert _is_slice_due(entries, tipoff, _now(), config)

    def test_no_entries(self):
        config = DCAConfig(max_entries=5)
        assert not _is_slice_due([], _tipoff(), _now(), config)

    def test_all_slices_done(self):
        """When all slices are placed, no more should be due."""
        config = DCAConfig(max_entries=3, cutoff_before_tipoff_min=30)
        entries = [
            _make_entry(hours_ago=6),
            _make_entry(hours_ago=4),
            _make_entry(hours_ago=2),
        ]
        assert not _is_slice_due(entries, _tipoff(), _now(), config)
