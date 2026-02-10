"""CTF (Conditional Token Framework) contract connector for mergePositions.

Calls the Polymarket CTF ERC-1155 contract on Polygon to merge
YES + NO token pairs into USDC collateral.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from src.config import settings

logger = logging.getLogger(__name__)

# Minimal ABI for CTF mergePositions + balanceOf + setApprovalForAll
CTF_ABI = [
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "partition", "type": "uint256[]"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "mergePositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
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
    {
        "inputs": [
            {"name": "operator", "type": "address"},
            {"name": "approved", "type": "bool"},
        ],
        "name": "setApprovalForAll",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "operator", "type": "address"},
        ],
        "name": "isApprovedForAll",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# parentCollectionId for root conditions
PARENT_COLLECTION_ID = b"\x00" * 32

# Binary partition: [1, 2] for YES/NO
BINARY_PARTITION = [1, 2]


@dataclass
class MergeResult:
    condition_id: str
    amount_shares: float  # MERGE したシェア数
    amount_usdc: float  # 受け取った USDC (= amount_shares * 1.0)
    gas_cost_matic: float
    gas_cost_usd: float  # MATIC→USD 換算 (概算)
    tx_hash: str
    success: bool
    error: str | None = None


def _shares_to_wei(shares: float) -> int:
    """Convert shares (float) to USDC-like 6-decimal wei, rounding down for safety."""
    return int(shares * 1e6)


def _wei_to_shares(wei: int) -> float:
    """Convert wei back to shares."""
    return wei / 1e6


def _get_web3():
    """Get Web3 instance connected to Polygon RPC."""
    from web3 import Web3

    return Web3(Web3.HTTPProvider(settings.merge_polygon_rpc))


def _get_account(w3):
    """Derive account from private key."""
    return w3.eth.account.from_key(settings.polymarket_private_key)


def _get_ctf_contract(w3):
    """Get CTF contract instance."""
    return w3.eth.contract(
        address=w3.to_checksum_address(settings.merge_ctf_address),
        abi=CTF_ABI,
    )


def _compute_position_token_id(condition_id_bytes: bytes, index_set: int) -> int:
    """Compute ERC-1155 position token ID from condition and index set."""
    from web3 import Web3

    collection_id = Web3.solidity_keccak(
        ["bytes32", "bytes32", "uint256"],
        [PARENT_COLLECTION_ID, condition_id_bytes, index_set],
    )
    return int.from_bytes(collection_id, "big")


def _get_token_owner_address(w3) -> str:
    """Resolve the address that holds CTF tokens.

    POLY_PROXY (sig_type=1): tokens are held by the Safe (funder address).
    EOA (sig_type=0): tokens are held by the EOA itself.
    """
    if settings.polymarket_signature_type == 1 and settings.polymarket_funder:
        return w3.to_checksum_address(settings.polymarket_funder)
    return _get_account(w3).address


def get_matic_balance() -> float:
    """Get MATIC balance for the configured wallet."""
    w3 = _get_web3()
    account = _get_account(w3)
    balance_wei = w3.eth.get_balance(account.address)
    return float(w3.from_wei(balance_wei, "ether"))


def get_ctf_balance(condition_id: str, index_set: int) -> float:
    """Get CTF token balance for a specific position.

    Args:
        condition_id: Hex string of the condition ID.
        index_set: 1 for YES, 2 for NO.
    """
    w3 = _get_web3()
    ctf = _get_ctf_contract(w3)

    cond_bytes = bytes.fromhex(condition_id.replace("0x", ""))
    token_id = _compute_position_token_id(cond_bytes, index_set)

    owner_address = _get_token_owner_address(w3)
    balance = ctf.functions.balanceOf(owner_address, token_id).call()
    return _wei_to_shares(balance)


def estimate_merge_gas(condition_id: str, amount: float) -> float:
    """Estimate gas cost for a merge operation in MATIC."""
    # Safe 経由の場合、gas estimation は不正確になるためフォールバック値を使用
    if settings.polymarket_signature_type == 1:
        return 0.02  # Safe execTransaction は ~2x gas

    w3 = _get_web3()
    account = _get_account(w3)
    ctf = _get_ctf_contract(w3)

    cond_bytes = bytes.fromhex(condition_id.replace("0x", ""))
    amount_wei = _shares_to_wei(amount)

    try:
        gas_estimate = ctf.functions.mergePositions(
            w3.to_checksum_address(settings.merge_collateral_address),
            PARENT_COLLECTION_ID,
            cond_bytes,
            BINARY_PARTITION,
            amount_wei,
        ).estimate_gas({"from": account.address})

        gas_price = w3.eth.gas_price
        cost_wei = gas_estimate * gas_price
        return float(w3.from_wei(cost_wei, "ether"))
    except Exception as e:
        logger.warning("Gas estimation failed: %s", e)
        return 0.01  # フォールバック: 0.01 MATIC (概算)


def merge_positions(condition_id: str, amount: float) -> MergeResult:
    """Execute mergePositions on the CTF contract.

    Args:
        condition_id: Hex string of the condition ID.
        amount: Number of shares to merge (float, 6 decimal precision).
    """
    w3 = _get_web3()
    account = _get_account(w3)
    ctf = _get_ctf_contract(w3)

    cond_bytes = bytes.fromhex(condition_id.replace("0x", ""))
    # 安全のため 1 wei 切り捨て
    amount_wei = max(0, _shares_to_wei(amount) - 1)

    try:
        # Gas price チェック
        gas_price = w3.eth.gas_price
        gas_price_gwei = gas_price / 1e9
        if gas_price_gwei > settings.merge_gas_buffer_gwei:
            return MergeResult(
                condition_id=condition_id,
                amount_shares=0,
                amount_usdc=0,
                gas_cost_matic=0,
                gas_cost_usd=0,
                tx_hash="",
                success=False,
                error=(
                    f"gas_price={gas_price_gwei:.1f}gwei > buffer={settings.merge_gas_buffer_gwei}"
                ),
            )

        # MATIC 残高チェック
        matic_balance = float(w3.from_wei(w3.eth.get_balance(account.address), "ether"))
        estimated_gas_matic = estimate_merge_gas(condition_id, amount)
        if matic_balance < estimated_gas_matic * 2:
            return MergeResult(
                condition_id=condition_id,
                amount_shares=0,
                amount_usdc=0,
                gas_cost_matic=0,
                gas_cost_usd=0,
                tx_hash="",
                success=False,
                error=f"matic_balance={matic_balance:.4f} < 2x gas={estimated_gas_matic:.4f}",
            )

        # TX 構築
        nonce = w3.eth.get_transaction_count(account.address)
        tx = ctf.functions.mergePositions(
            w3.to_checksum_address(settings.merge_collateral_address),
            PARENT_COLLECTION_ID,
            cond_bytes,
            BINARY_PARTITION,
            amount_wei,
        ).build_transaction(
            {
                "from": account.address,
                "nonce": nonce,
                "gas": 300_000,
                "gasPrice": gas_price,
            }
        )

        signed = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

        gas_used = receipt["gasUsed"]
        gas_cost_wei = gas_used * receipt.get("effectiveGasPrice", gas_price)
        gas_cost_matic = float(w3.from_wei(gas_cost_wei, "ether"))
        # MATIC→USD 概算 (0.40 USD/MATIC — 実環境では oracle 参照)
        gas_cost_usd = gas_cost_matic * 0.40

        success = receipt["status"] == 1
        merged_shares = _wei_to_shares(amount_wei) if success else 0

        return MergeResult(
            condition_id=condition_id,
            amount_shares=merged_shares,
            amount_usdc=merged_shares,
            gas_cost_matic=gas_cost_matic,
            gas_cost_usd=gas_cost_usd,
            tx_hash=tx_hash.hex(),
            success=success,
            error=None if success else "tx_reverted",
        )

    except Exception as e:
        logger.exception("mergePositions failed for condition %s", condition_id)
        return MergeResult(
            condition_id=condition_id,
            amount_shares=0,
            amount_usdc=0,
            gas_cost_matic=0,
            gas_cost_usd=0,
            tx_hash="",
            success=False,
            error=str(e),
        )


def merge_positions_via_safe(condition_id: str, amount: float) -> MergeResult:
    """Execute mergePositions via Gnosis Safe (1-of-1 POLY_PROXY).

    Args:
        condition_id: Hex string of the condition ID.
        amount: Number of shares to merge (float, 6 decimal precision).
    """
    from src.connectors.safe_tx import (
        check_token_balances,
        exec_safe_transaction,
        validate_safe_config,
    )

    w3 = _get_web3()
    account = _get_account(w3)
    ctf = _get_ctf_contract(w3)
    safe_address = settings.polymarket_funder

    if not safe_address:
        return MergeResult(
            condition_id=condition_id,
            amount_shares=0,
            amount_usdc=0,
            gas_cost_matic=0,
            gas_cost_usd=0,
            tx_hash="",
            success=False,
            error="polymarket_funder_not_set",
        )

    cond_bytes = bytes.fromhex(condition_id.replace("0x", ""))
    # 安全のため 1 wei 切り捨て
    amount_wei = max(0, _shares_to_wei(amount) - 1)

    try:
        # 1. Safe config 検証 (1-of-1 + owner + version)
        valid, reason = validate_safe_config(w3, safe_address, account.address)
        if not valid:
            return MergeResult(
                condition_id=condition_id,
                amount_shares=0,
                amount_usdc=0,
                gas_cost_matic=0,
                gas_cost_usd=0,
                tx_hash="",
                success=False,
                error=f"safe_validation_failed: {reason}",
            )

        # 2. Token 残高チェック (YES/NO)
        yes_token_id = _compute_position_token_id(cond_bytes, 1)
        no_token_id = _compute_position_token_id(cond_bytes, 2)

        bal_ok, bal_reason = check_token_balances(
            w3, safe_address, settings.merge_ctf_address,
            yes_token_id, no_token_id, amount_wei,
        )
        if not bal_ok:
            return MergeResult(
                condition_id=condition_id,
                amount_shares=0,
                amount_usdc=0,
                gas_cost_matic=0,
                gas_cost_usd=0,
                tx_hash="",
                success=False,
                error=f"token_balance_insufficient: {bal_reason}",
            )

        # 3. Gas price チェック
        gas_price = w3.eth.gas_price
        gas_price_gwei = gas_price / 1e9
        if gas_price_gwei > settings.merge_gas_buffer_gwei:
            return MergeResult(
                condition_id=condition_id,
                amount_shares=0,
                amount_usdc=0,
                gas_cost_matic=0,
                gas_cost_usd=0,
                tx_hash="",
                success=False,
                error=(
                    f"gas_price={gas_price_gwei:.1f}gwei"
                    f" > buffer={settings.merge_gas_buffer_gwei}"
                ),
            )

        # 4. MATIC 残高チェック (owner EOA が gas を支払う)
        matic_balance = float(w3.from_wei(w3.eth.get_balance(account.address), "ether"))
        estimated_gas_matic = 0.02  # Safe 概算
        if matic_balance < estimated_gas_matic * 2:
            return MergeResult(
                condition_id=condition_id,
                amount_shares=0,
                amount_usdc=0,
                gas_cost_matic=0,
                gas_cost_usd=0,
                tx_hash="",
                success=False,
                error=f"matic_balance={matic_balance:.4f} < 2x gas={estimated_gas_matic:.4f}",
            )

        # 5. mergePositions calldata 構築
        calldata = ctf.functions.mergePositions(
            w3.to_checksum_address(settings.merge_collateral_address),
            PARENT_COLLECTION_ID,
            cond_bytes,
            BINARY_PARTITION,
            amount_wei,
        )._encode_transaction_data()

        # 6. Safe execTransaction 実行
        receipt = exec_safe_transaction(
            w3,
            safe_address,
            account,
            to=settings.merge_ctf_address,
            data=calldata,
            safe_tx_gas=0,
            outer_gas_limit=settings.merge_safe_outer_gas_limit,
        )

        gas_used = receipt["gasUsed"]
        gas_cost_wei = gas_used * receipt.get("effectiveGasPrice", gas_price)
        gas_cost_matic = float(w3.from_wei(gas_cost_wei, "ether"))
        gas_cost_usd = gas_cost_matic * 0.40  # MATIC→USD 概算

        success = receipt["status"] == 1
        merged_shares = _wei_to_shares(amount_wei) if success else 0

        return MergeResult(
            condition_id=condition_id,
            amount_shares=merged_shares,
            amount_usdc=merged_shares,
            gas_cost_matic=gas_cost_matic,
            gas_cost_usd=gas_cost_usd,
            tx_hash=receipt["transactionHash"].hex(),
            success=success,
            error=None if success else "tx_reverted",
        )

    except Exception as e:
        logger.exception("mergePositions via Safe failed for condition %s", condition_id)
        return MergeResult(
            condition_id=condition_id,
            amount_shares=0,
            amount_usdc=0,
            gas_cost_matic=0,
            gas_cost_usd=0,
            tx_hash="",
            success=False,
            error=str(e),
        )


def simulate_merge(
    condition_id: str,
    merge_amount: float,
    combined_vwap: float,
    gas_cost_usd: float = 0.001,
) -> MergeResult:
    """Simulate a merge for paper/dry-run mode (no Web3 calls).

    Args:
        condition_id: Condition ID.
        merge_amount: Shares to merge.
        combined_vwap: Combined VWAP for profit calculation.
        gas_cost_usd: Simulated gas cost.
    """
    return MergeResult(
        condition_id=condition_id,
        amount_shares=merge_amount,
        amount_usdc=merge_amount,
        gas_cost_matic=0.0,
        gas_cost_usd=gas_cost_usd,
        tx_hash="simulated",
        success=True,
        error=None,
    )
