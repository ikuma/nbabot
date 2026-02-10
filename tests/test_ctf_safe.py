"""Tests for Safe MERGE in src/connectors/ctf.py."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CONDITION_ID = "ab" * 32  # 64 hex chars


def _mock_settings(**overrides):
    """Build a mock Settings object for CTF tests."""
    defaults = dict(
        polymarket_private_key="0x" + "aa" * 32,
        polymarket_signature_type=1,
        polymarket_funder="0xSafeAddress",
        merge_ctf_address="0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
        merge_collateral_address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        merge_polygon_rpc="https://polygon-rpc.com",
        merge_gas_buffer_gwei=50,
        merge_safe_outer_gas_limit=400_000,
    )
    defaults.update(overrides)
    s = MagicMock()
    for k, v in defaults.items():
        setattr(s, k, v)
    return s


# ---------------------------------------------------------------------------
# TestGetTokenOwnerAddress
# ---------------------------------------------------------------------------


class TestGetTokenOwnerAddress:
    """_get_token_owner_address: EOA vs POLY_PROXY."""

    @patch("src.connectors.ctf.settings")
    def test_eoa_returns_account_address(self, mock_settings):
        mock_settings.polymarket_signature_type = 0
        mock_settings.polymarket_funder = ""

        from src.connectors.ctf import _get_token_owner_address

        w3 = MagicMock()
        account = MagicMock()
        account.address = "0xEOA"
        with patch("src.connectors.ctf._get_account", return_value=account):
            result = _get_token_owner_address(w3)
        assert result == "0xEOA"

    @patch("src.connectors.ctf.settings")
    def test_poly_proxy_returns_funder(self, mock_settings):
        mock_settings.polymarket_signature_type = 1
        mock_settings.polymarket_funder = "0xSafeAddr"

        from src.connectors.ctf import _get_token_owner_address

        w3 = MagicMock()
        w3.to_checksum_address.return_value = "0xSafeAddr"
        result = _get_token_owner_address(w3)
        assert result == "0xSafeAddr"


# ---------------------------------------------------------------------------
# TestMergePositionsViaSafe
# ---------------------------------------------------------------------------


class TestMergePositionsViaSafe:
    """merge_positions_via_safe: Safe MERGE パス。"""

    @patch("src.connectors.ctf.settings")
    def test_funder_not_set(self, mock_settings):
        mock_settings.polymarket_funder = ""
        mock_settings.polymarket_private_key = "0x" + "aa" * 32
        mock_settings.merge_polygon_rpc = "https://polygon-rpc.com"

        from src.connectors.ctf import merge_positions_via_safe

        with patch("src.connectors.ctf._get_web3") as mock_w3:
            mock_w3.return_value = MagicMock()
            result = merge_positions_via_safe(CONDITION_ID, 100.0)

        assert result.success is False
        assert "funder" in result.error

    @patch("src.connectors.ctf.settings")
    def test_validation_failure(self, mock_settings):
        for k, v in _mock_settings().__dict__.items():
            if not k.startswith("_"):
                setattr(mock_settings, k, v)

        from src.connectors.ctf import merge_positions_via_safe

        with (
            patch("src.connectors.ctf._get_web3") as mock_w3,
            patch("src.connectors.ctf._get_account") as mock_acc,
            patch("src.connectors.ctf._get_ctf_contract"),
            patch(
                "src.connectors.safe_tx.validate_safe_config",
                return_value=(False, "multisig_not_supported"),
            ),
        ):
            w3 = MagicMock()
            w3.to_checksum_address = lambda x: x
            mock_w3.return_value = w3
            mock_acc.return_value = MagicMock(address="0xOwner")

            result = merge_positions_via_safe(CONDITION_ID, 100.0)

        assert result.success is False
        assert "multisig_not_supported" in result.error

    @patch("src.connectors.ctf.settings")
    def test_token_balance_insufficient(self, mock_settings):
        for k, v in _mock_settings().__dict__.items():
            if not k.startswith("_"):
                setattr(mock_settings, k, v)

        from src.connectors.ctf import merge_positions_via_safe

        with (
            patch("src.connectors.ctf._get_web3") as mock_w3,
            patch("src.connectors.ctf._get_account") as mock_acc,
            patch("src.connectors.ctf._get_ctf_contract"),
            patch("src.connectors.ctf._compute_position_token_id", return_value=999),
            patch(
                "src.connectors.safe_tx.validate_safe_config",
                return_value=(True, "ok"),
            ),
            patch(
                "src.connectors.safe_tx.check_token_balances",
                return_value=(False, "yes_balance=0<required=100000000"),
            ),
        ):
            w3 = MagicMock()
            w3.to_checksum_address = lambda x: x
            mock_w3.return_value = w3
            mock_acc.return_value = MagicMock(address="0xOwner")

            result = merge_positions_via_safe(CONDITION_ID, 100.0)

        assert result.success is False
        assert "token_balance_insufficient" in result.error

    @patch("src.connectors.ctf.settings")
    def test_gas_price_exceeded(self, mock_settings):
        for k, v in _mock_settings(merge_gas_buffer_gwei=50).__dict__.items():
            if not k.startswith("_"):
                setattr(mock_settings, k, v)

        from src.connectors.ctf import merge_positions_via_safe

        with (
            patch("src.connectors.ctf._get_web3") as mock_w3,
            patch("src.connectors.ctf._get_account") as mock_acc,
            patch("src.connectors.ctf._get_ctf_contract"),
            patch("src.connectors.ctf._compute_position_token_id", return_value=999),
            patch(
                "src.connectors.safe_tx.validate_safe_config",
                return_value=(True, "ok"),
            ),
            patch(
                "src.connectors.safe_tx.check_token_balances",
                return_value=(True, "ok"),
            ),
        ):
            w3 = MagicMock()
            w3.to_checksum_address = lambda x: x
            # 100 gwei > 50 gwei buffer
            w3.eth.gas_price = 100_000_000_000
            mock_w3.return_value = w3
            mock_acc.return_value = MagicMock(address="0xOwner")

            result = merge_positions_via_safe(CONDITION_ID, 100.0)

        assert result.success is False
        assert "gas_price" in result.error

    @patch("src.connectors.ctf.settings")
    def test_matic_insufficient(self, mock_settings):
        for k, v in _mock_settings().__dict__.items():
            if not k.startswith("_"):
                setattr(mock_settings, k, v)

        from src.connectors.ctf import merge_positions_via_safe

        with (
            patch("src.connectors.ctf._get_web3") as mock_w3,
            patch("src.connectors.ctf._get_account") as mock_acc,
            patch("src.connectors.ctf._get_ctf_contract"),
            patch("src.connectors.ctf._compute_position_token_id", return_value=999),
            patch(
                "src.connectors.safe_tx.validate_safe_config",
                return_value=(True, "ok"),
            ),
            patch(
                "src.connectors.safe_tx.check_token_balances",
                return_value=(True, "ok"),
            ),
        ):
            w3 = MagicMock()
            w3.to_checksum_address = lambda x: x
            w3.eth.gas_price = 30_000_000_000  # 30 gwei — under buffer
            # MATIC 残高不足 (0.001 < 0.04)
            w3.eth.get_balance.return_value = 1_000_000_000_000_000  # 0.001 MATIC in wei
            w3.from_wei.return_value = 0.001
            mock_w3.return_value = w3
            mock_acc.return_value = MagicMock(address="0xOwner")

            result = merge_positions_via_safe(CONDITION_ID, 100.0)

        assert result.success is False
        assert "matic_balance" in result.error

    @patch("src.connectors.ctf.settings")
    def test_success_flow(self, mock_settings):
        for k, v in _mock_settings().__dict__.items():
            if not k.startswith("_"):
                setattr(mock_settings, k, v)

        from src.connectors.ctf import merge_positions_via_safe

        receipt = {
            "status": 1,
            "gasUsed": 200_000,
            "effectiveGasPrice": 30_000_000_000,
            "transactionHash": MagicMock(hex=lambda: "0xabc123"),
        }

        with (
            patch("src.connectors.ctf._get_web3") as mock_w3,
            patch("src.connectors.ctf._get_account") as mock_acc,
            patch("src.connectors.ctf._get_ctf_contract") as mock_ctf,
            patch("src.connectors.ctf._compute_position_token_id", return_value=999),
            patch(
                "src.connectors.safe_tx.validate_safe_config",
                return_value=(True, "ok"),
            ),
            patch(
                "src.connectors.safe_tx.check_token_balances",
                return_value=(True, "ok"),
            ),
            patch(
                "src.connectors.safe_tx.exec_safe_transaction",
                return_value=receipt,
            ),
        ):
            w3 = MagicMock()
            w3.to_checksum_address = lambda x: x
            w3.eth.gas_price = 30_000_000_000
            w3.eth.get_balance.return_value = 1_000_000_000_000_000_000  # 1 MATIC
            w3.from_wei.side_effect = lambda val, unit: val / 1e18 if unit == "ether" else val
            mock_w3.return_value = w3
            mock_acc.return_value = MagicMock(address="0xOwner")

            ctf_contract = MagicMock()
            ctf_contract.functions.mergePositions.return_value._encode_transaction_data.return_value = b"\x01"
            mock_ctf.return_value = ctf_contract

            result = merge_positions_via_safe(CONDITION_ID, 100.0)

        assert result.success is True
        assert result.amount_shares > 0
        assert result.tx_hash == "0xabc123"


# ---------------------------------------------------------------------------
# TestMergePositionsEoaUnchanged
# ---------------------------------------------------------------------------


class TestMergePositionsEoaUnchanged:
    """EOA merge_positions() パスがリグレッションしていないことを確認。"""

    @patch("src.connectors.ctf.settings")
    def test_eoa_merge_gas_check(self, mock_settings):
        """EOA パスの gas price チェックが機能すること。"""
        mock_settings.polymarket_signature_type = 0
        mock_settings.polymarket_private_key = "0x" + "aa" * 32
        mock_settings.merge_polygon_rpc = "https://polygon-rpc.com"
        mock_settings.merge_ctf_address = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
        mock_settings.merge_collateral_address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        mock_settings.merge_gas_buffer_gwei = 50

        from src.connectors.ctf import merge_positions

        with (
            patch("src.connectors.ctf._get_web3") as mock_w3,
            patch("src.connectors.ctf._get_account") as mock_acc,
            patch("src.connectors.ctf._get_ctf_contract"),
        ):
            w3 = MagicMock()
            w3.to_checksum_address = lambda x: x
            # 100 gwei > 50 gwei buffer → 拒否される
            w3.eth.gas_price = 100_000_000_000
            mock_w3.return_value = w3
            mock_acc.return_value = MagicMock(address="0xOwner")

            result = merge_positions(CONDITION_ID, 100.0)

        assert result.success is False
        assert "gas_price" in result.error
