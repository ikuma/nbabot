"""Tests for src/connectors/safe_tx.py — Gnosis Safe transaction helpers."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.connectors.safe_tx import (
    SAFE_ABI_MINIMAL,
    check_token_balances,
    exec_safe_transaction,
    get_safe_contract,
    sign_safe_tx_hash,
    validate_safe_config,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_w3(checksum_passthrough: bool = True):
    """Create a mock Web3 instance."""
    w3 = MagicMock()
    if checksum_passthrough:
        w3.to_checksum_address = lambda addr: addr
    return w3


# ---------------------------------------------------------------------------
# TestSafeAbi
# ---------------------------------------------------------------------------


class TestSafeAbi:
    """ABI 構造検証。"""

    def test_required_functions_present(self):
        names = {entry["name"] for entry in SAFE_ABI_MINIMAL if entry["type"] == "function"}
        expected = {"nonce", "getOwners", "getThreshold", "VERSION",
                    "getTransactionHash", "execTransaction"}
        assert expected.issubset(names)

    def test_version_function_exists(self):
        version_entry = [e for e in SAFE_ABI_MINIMAL if e.get("name") == "VERSION"]
        assert len(version_entry) == 1
        assert version_entry[0]["outputs"][0]["type"] == "string"


# ---------------------------------------------------------------------------
# TestValidateSafeConfig
# ---------------------------------------------------------------------------


class TestValidateSafeConfig:
    """1-of-1 Safe validation."""

    def test_valid_1of1(self):
        w3 = _mock_w3()
        safe_contract = MagicMock()
        safe_contract.functions.VERSION().call.return_value = "1.3.0"
        safe_contract.functions.getThreshold().call.return_value = 1
        safe_contract.functions.getOwners().call.return_value = ["0xOwner"]
        w3.eth.contract.return_value = safe_contract

        ok, reason = validate_safe_config(w3, "0xSafe", "0xOwner")
        assert ok is True
        assert reason == "ok"

    def test_version_1_4(self):
        w3 = _mock_w3()
        safe_contract = MagicMock()
        safe_contract.functions.VERSION().call.return_value = "1.4.1"
        safe_contract.functions.getThreshold().call.return_value = 1
        safe_contract.functions.getOwners().call.return_value = ["0xOwner"]
        w3.eth.contract.return_value = safe_contract

        ok, reason = validate_safe_config(w3, "0xSafe", "0xOwner")
        assert ok is True

    def test_unsupported_version(self):
        w3 = _mock_w3()
        safe_contract = MagicMock()
        safe_contract.functions.VERSION().call.return_value = "1.2.0"
        w3.eth.contract.return_value = safe_contract

        ok, reason = validate_safe_config(w3, "0xSafe", "0xOwner")
        assert ok is False
        assert "unsupported_version" in reason

    def test_multisig_rejected(self):
        w3 = _mock_w3()
        safe_contract = MagicMock()
        safe_contract.functions.VERSION().call.return_value = "1.3.0"
        safe_contract.functions.getThreshold().call.return_value = 2
        w3.eth.contract.return_value = safe_contract

        ok, reason = validate_safe_config(w3, "0xSafe", "0xOwner")
        assert ok is False
        assert reason == "multisig_not_supported"

    def test_wrong_owner(self):
        w3 = _mock_w3()
        safe_contract = MagicMock()
        safe_contract.functions.VERSION().call.return_value = "1.3.0"
        safe_contract.functions.getThreshold().call.return_value = 1
        safe_contract.functions.getOwners().call.return_value = ["0xOtherOwner"]
        w3.eth.contract.return_value = safe_contract

        ok, reason = validate_safe_config(w3, "0xSafe", "0xOwner")
        assert ok is False
        assert reason == "not_owner"

    def test_version_check_exception(self):
        w3 = _mock_w3()
        safe_contract = MagicMock()
        safe_contract.functions.VERSION().call.side_effect = Exception("RPC error")
        w3.eth.contract.return_value = safe_contract

        ok, reason = validate_safe_config(w3, "0xSafe", "0xOwner")
        assert ok is False
        assert "version_check_failed" in reason


# ---------------------------------------------------------------------------
# TestCheckTokenBalances
# ---------------------------------------------------------------------------


class TestCheckTokenBalances:
    """Safe の YES/NO トークン残高チェック。"""

    def test_sufficient_balances(self):
        w3 = _mock_w3()
        ctf_contract = MagicMock()
        # YES and NO both have enough
        ctf_contract.functions.balanceOf.return_value.call.return_value = 1_000_000
        w3.eth.contract.return_value = ctf_contract

        ok, reason = check_token_balances(
            w3, "0xSafe", "0xCTF",
            yes_token_id=111, no_token_id=222, amount_wei=500_000,
        )
        assert ok is True
        assert reason == "ok"

    def test_yes_insufficient(self):
        w3 = _mock_w3()
        ctf_contract = MagicMock()
        # YES 不足
        ctf_contract.functions.balanceOf.return_value.call.return_value = 100
        w3.eth.contract.return_value = ctf_contract

        ok, reason = check_token_balances(
            w3, "0xSafe", "0xCTF",
            yes_token_id=111, no_token_id=222, amount_wei=500_000,
        )
        assert ok is False
        assert "yes_balance" in reason

    def test_no_insufficient(self):
        w3 = _mock_w3()
        ctf_contract = MagicMock()
        # YES 十分だが NO 不足
        calls = [1_000_000, 100]
        ctf_contract.functions.balanceOf.return_value.call.side_effect = calls
        w3.eth.contract.return_value = ctf_contract

        ok, reason = check_token_balances(
            w3, "0xSafe", "0xCTF",
            yes_token_id=111, no_token_id=222, amount_wei=500_000,
        )
        assert ok is False
        assert "no_balance" in reason


# ---------------------------------------------------------------------------
# TestSignSafeTxHash
# ---------------------------------------------------------------------------


class TestSignSafeTxHash:
    """EIP-712 署名検証。"""

    def test_signature_length_65_bytes(self):
        w3 = _mock_w3()
        account = MagicMock()
        sig = MagicMock()
        sig.r = 123456789
        sig.s = 987654321
        sig.v = 27
        account.unsafe_sign_hash.return_value = sig

        packed = sign_safe_tx_hash(w3, account, b"\x00" * 32)
        assert len(packed) == 65

    def test_v_no_plus4(self):
        """v should remain 27 or 28, not +4."""
        w3 = _mock_w3()
        account = MagicMock()

        for expected_v in (27, 28):
            sig = MagicMock()
            sig.r = 1
            sig.s = 2
            sig.v = expected_v
            account.unsafe_sign_hash.return_value = sig

            packed = sign_safe_tx_hash(w3, account, b"\x00" * 32)
            # v is the last byte
            assert packed[-1] == expected_v

    def test_byteorder_big(self):
        """r and s should be big-endian encoded."""
        w3 = _mock_w3()
        account = MagicMock()
        sig = MagicMock()
        sig.r = 256  # 0x100 → big-endian: 00...0100
        sig.s = 1
        sig.v = 27
        account.unsafe_sign_hash.return_value = sig

        packed = sign_safe_tx_hash(w3, account, b"\x00" * 32)
        r_bytes = packed[:32]
        assert r_bytes == (256).to_bytes(32, byteorder="big")


# ---------------------------------------------------------------------------
# TestExecSafeTransaction
# ---------------------------------------------------------------------------


class TestExecSafeTransaction:
    """Safe execTransaction の構築・送信テスト。"""

    def _setup_mocks(self, w3, receipt_status: int = 1):
        safe_contract = MagicMock()
        safe_contract.functions.nonce().call.return_value = 42
        safe_contract.functions.getTransactionHash.return_value.call.return_value = b"\xab" * 32
        safe_contract.functions.execTransaction.return_value.build_transaction.return_value = {
            "to": "0xSafe",
            "data": b"",
        }
        w3.eth.contract.return_value = safe_contract
        w3.eth.gas_price = 30_000_000_000
        w3.eth.get_transaction_count.return_value = 5

        account = MagicMock()
        account.address = "0xOwner"
        sig = MagicMock()
        sig.r = 1
        sig.s = 2
        sig.v = 27
        account.unsafe_sign_hash.return_value = sig
        account.sign_transaction.return_value = MagicMock(raw_transaction=b"\x00")

        receipt = {"status": receipt_status, "gasUsed": 200_000, "effectiveGasPrice": 30_000_000_000}
        w3.eth.send_raw_transaction.return_value = b"\xaa" * 32
        w3.eth.wait_for_transaction_receipt.return_value = receipt

        return account, receipt

    def test_success_flow(self):
        w3 = _mock_w3()
        account, _ = self._setup_mocks(w3, receipt_status=1)

        receipt = exec_safe_transaction(
            w3, "0xSafe", account, to="0xCTF", data=b"\x01\x02",
        )
        assert receipt["status"] == 1

    def test_revert_flow(self):
        w3 = _mock_w3()
        account, _ = self._setup_mocks(w3, receipt_status=0)

        receipt = exec_safe_transaction(
            w3, "0xSafe", account, to="0xCTF", data=b"\x01\x02",
        )
        assert receipt["status"] == 0

    def test_exception_propagates(self):
        w3 = _mock_w3()
        account, _ = self._setup_mocks(w3)
        w3.eth.send_raw_transaction.side_effect = Exception("network error")

        with pytest.raises(Exception, match="network error"):
            exec_safe_transaction(w3, "0xSafe", account, to="0xCTF", data=b"\x01")

    def test_safe_tx_gas_zero(self):
        """Verify safeTxGas=0 is passed to getTransactionHash."""
        w3 = _mock_w3()
        account, _ = self._setup_mocks(w3)

        exec_safe_transaction(
            w3, "0xSafe", account, to="0xCTF", data=b"\x01",
            safe_tx_gas=0,
        )

        # getTransactionHash の呼び出しで safeTxGas=0 を確認
        safe_contract = w3.eth.contract.return_value
        call_args = safe_contract.functions.getTransactionHash.call_args
        # 5番目の引数が safeTxGas
        assert call_args[0][4] == 0
