"""Tests for CTF connector (mergePositions)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.connectors.ctf import (
    BINARY_PARTITION,
    PARENT_COLLECTION_ID,
    MergeResult,
    _shares_to_wei,
    _wei_to_shares,
    simulate_merge,
)


class TestSharesConversion:
    def test_shares_to_wei(self):
        assert _shares_to_wei(1.0) == 1_000_000
        assert _shares_to_wei(0.5) == 500_000
        assert _shares_to_wei(100.123456) == 100_123_456

    def test_wei_to_shares(self):
        assert _wei_to_shares(1_000_000) == pytest.approx(1.0)
        assert _wei_to_shares(500_000) == pytest.approx(0.5)

    def test_roundtrip(self):
        original = 42.123456
        assert _wei_to_shares(_shares_to_wei(original)) == pytest.approx(original, abs=1e-6)


class TestSimulateMerge:
    def test_basic_simulation(self):
        result = simulate_merge(
            condition_id="0xabc123",
            merge_amount=100.0,
            combined_vwap=0.85,
            gas_cost_usd=0.001,
        )
        assert result.success is True
        assert result.amount_shares == pytest.approx(100.0)
        assert result.amount_usdc == pytest.approx(100.0)
        assert result.tx_hash == "simulated"
        assert result.gas_cost_usd == pytest.approx(0.001)
        assert result.error is None

    def test_zero_merge(self):
        result = simulate_merge(
            condition_id="0xabc123",
            merge_amount=0.0,
            combined_vwap=0.85,
        )
        assert result.success is True
        assert result.amount_shares == 0.0


class TestMergeResultDataclass:
    def test_fields(self):
        r = MergeResult(
            condition_id="0x123",
            amount_shares=50.0,
            amount_usdc=50.0,
            gas_cost_matic=0.01,
            gas_cost_usd=0.004,
            tx_hash="0xdeadbeef",
            success=True,
        )
        assert r.condition_id == "0x123"
        assert r.error is None

    def test_failed_result(self):
        r = MergeResult(
            condition_id="0x123",
            amount_shares=0,
            amount_usdc=0,
            gas_cost_matic=0,
            gas_cost_usd=0,
            tx_hash="",
            success=False,
            error="gas_too_high",
        )
        assert r.success is False
        assert r.error == "gas_too_high"


class TestConstants:
    def test_parent_collection_id(self):
        assert len(PARENT_COLLECTION_ID) == 32
        assert all(b == 0 for b in PARENT_COLLECTION_ID)

    def test_binary_partition(self):
        assert BINARY_PARTITION == [1, 2]
