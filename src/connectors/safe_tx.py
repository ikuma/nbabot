"""Gnosis Safe execTransaction helper for 1-of-1 POLY_PROXY wallets.

Generic Safe transaction builder / signer / executor.
Does NOT contain CTF-specific logic — that lives in ctf.py.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Minimal ABI subset for Gnosis Safe 1.3.x
SAFE_ABI_MINIMAL = [
    {
        "inputs": [],
        "name": "nonce",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "getOwners",
        "outputs": [{"name": "", "type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "getThreshold",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "VERSION",
        "outputs": [{"name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "value", "type": "uint256"},
            {"name": "data", "type": "bytes"},
            {"name": "operation", "type": "uint8"},
            {"name": "safeTxGas", "type": "uint256"},
            {"name": "baseGas", "type": "uint256"},
            {"name": "gasPrice", "type": "uint256"},
            {"name": "gasToken", "type": "address"},
            {"name": "refundReceiver", "type": "address"},
            {"name": "_nonce", "type": "uint256"},
        ],
        "name": "getTransactionHash",
        "outputs": [{"name": "", "type": "bytes32"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "value", "type": "uint256"},
            {"name": "data", "type": "bytes"},
            {"name": "operation", "type": "uint8"},
            {"name": "safeTxGas", "type": "uint256"},
            {"name": "baseGas", "type": "uint256"},
            {"name": "gasPrice", "type": "uint256"},
            {"name": "gasToken", "type": "address"},
            {"name": "refundReceiver", "type": "address"},
            {"name": "signatures", "type": "bytes"},
        ],
        "name": "execTransaction",
        "outputs": [{"name": "success", "type": "bool"}],
        "stateMutability": "payable",
        "type": "function",
    },
]

# ERC-1155 balanceOf (Safe のトークン残高チェック用)
ERC1155_BALANCE_OF_ABI = [
    {
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def get_safe_contract(w3, safe_address: str):
    """Get Safe contract instance."""
    return w3.eth.contract(
        address=w3.to_checksum_address(safe_address),
        abi=SAFE_ABI_MINIMAL,
    )


def validate_safe_config(
    w3, safe_address: str, owner_address: str
) -> tuple[bool, str]:
    """Validate that the Safe is a 1-of-1 with the expected owner.

    Checks:
    - VERSION() is 1.3.x or 1.4.x
    - threshold == 1
    - owner_address is in the owners list
    """
    safe = get_safe_contract(w3, safe_address)

    # VERSION チェック
    try:
        version = safe.functions.VERSION().call()
    except Exception as e:
        return False, f"version_check_failed: {e}"

    if not (version.startswith("1.3.") or version.startswith("1.4.")):
        return False, f"unsupported_version={version}"

    # threshold チェック
    threshold = safe.functions.getThreshold().call()
    if threshold != 1:
        return False, "multisig_not_supported"

    # owner チェック
    owners = safe.functions.getOwners().call()
    owner_cs = w3.to_checksum_address(owner_address)
    if owner_cs not in [w3.to_checksum_address(o) for o in owners]:
        return False, "not_owner"

    return True, "ok"


def check_token_balances(
    w3,
    safe_address: str,
    ctf_address: str,
    yes_token_id: int,
    no_token_id: int,
    amount_wei: int,
) -> tuple[bool, str]:
    """Check that the Safe holds enough YES and NO tokens for the merge.

    Args:
        yes_token_id: ERC-1155 token ID for YES position.
        no_token_id: ERC-1155 token ID for NO position.
        amount_wei: Required amount in wei (6 decimals).
    """
    ctf = w3.eth.contract(
        address=w3.to_checksum_address(ctf_address),
        abi=ERC1155_BALANCE_OF_ABI,
    )
    safe_cs = w3.to_checksum_address(safe_address)

    yes_bal = ctf.functions.balanceOf(safe_cs, yes_token_id).call()
    if yes_bal < amount_wei:
        return False, f"yes_balance={yes_bal}<required={amount_wei}"

    no_bal = ctf.functions.balanceOf(safe_cs, no_token_id).call()
    if no_bal < amount_wei:
        return False, f"no_balance={no_bal}<required={amount_wei}"

    return True, "ok"


def sign_safe_tx_hash(w3, account, tx_hash: bytes) -> bytes:
    """Sign a Safe EIP-712 transaction hash with raw ECDSA.

    Uses unsafe_sign_hash() which produces raw ECDSA (no prefix).
    v remains 27/28 — no v+=4 needed for Safe's EIP-712 verification path.

    Returns:
        Packed signature bytes (r + s + v = 65 bytes).
    """
    sig = account.unsafe_sign_hash(tx_hash)
    packed = (
        sig.r.to_bytes(32, byteorder="big")
        + sig.s.to_bytes(32, byteorder="big")
        + sig.v.to_bytes(1, byteorder="big")
    )
    return packed


def exec_safe_transaction(
    w3,
    safe_address: str,
    account,
    to: str,
    data: bytes,
    safe_tx_gas: int = 0,
    outer_gas_limit: int = 400_000,
) -> dict:
    """Build, sign, and execute a Safe execTransaction.

    safeTxGas=0: 内部失敗時に全体 revert + nonce 保全 (GS013)。
    gasPrice=0, gasToken=0x0, refundReceiver=0x0 (owner が直接 gas 支払い)。

    Returns:
        Transaction receipt dict.
    """
    safe = get_safe_contract(w3, safe_address)
    to_cs = w3.to_checksum_address(to)
    zero_addr = w3.to_checksum_address(ZERO_ADDRESS)

    # Safe nonce 取得
    safe_nonce = safe.functions.nonce().call()

    # EIP-712 tx hash 計算
    safe_tx_hash = safe.functions.getTransactionHash(
        to_cs,       # to
        0,           # value
        data,        # data
        0,           # operation (CALL)
        safe_tx_gas, # safeTxGas
        0,           # baseGas
        0,           # gasPrice
        zero_addr,   # gasToken
        zero_addr,   # refundReceiver
        safe_nonce,  # _nonce
    ).call()

    # Owner EOA で署名
    signature = sign_safe_tx_hash(w3, account, safe_tx_hash)

    # TX 構築
    gas_price = w3.eth.gas_price
    nonce = w3.eth.get_transaction_count(account.address)

    tx = safe.functions.execTransaction(
        to_cs,       # to
        0,           # value
        data,        # data
        0,           # operation (CALL)
        safe_tx_gas, # safeTxGas
        0,           # baseGas
        0,           # gasPrice
        zero_addr,   # gasToken
        zero_addr,   # refundReceiver
        signature,   # signatures
    ).build_transaction(
        {
            "from": account.address,
            "nonce": nonce,
            "gas": outer_gas_limit,
            "gasPrice": gas_price,
        }
    )

    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

    return receipt
