from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}

    # Polymarket
    polymarket_private_key: str = ""
    polymarket_host: str = "https://clob.polymarket.com"
    polymarket_chain_id: int = 137
    polymarket_signature_type: int = 0  # 0=EOA, 1=POLY_PROXY, 2=GNOSIS_SAFE
    polymarket_funder: str = ""  # proxy wallet address (POLY_PROXY 時のみ必要)

    # Gamma Markets API (for market search/filtering)
    gamma_api_url: str = "https://gamma-api.polymarket.com"

    # HTTP proxy for geo-restricted APIs (e.g. socks5://127.0.0.1:1080)
    http_proxy: str = ""

    # The Odds API
    odds_api_key: str = ""

    # Telegram (optional)
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # Risk parameters
    max_position_usd: float = 100.0
    daily_loss_limit_pct: float = 3.0
    min_edge_pct: float = 1.0
    kelly_fraction: float = 0.25

    # === Risk management (Phase D) ===
    weekly_loss_limit_pct: float = 5.0
    max_drawdown_limit_pct: float = 15.0
    risk_check_enabled: bool = True
    calibration_drift_threshold: float = 2.0
    max_total_exposure_pct: float = 30.0
    risk_max_single_game_usd: float = 200.0

    # === Calibration strategy ===
    strategy_mode: str = "calibration"  # "calibration" | "bookmaker"
    sweet_spot_lo: float = 0.20
    sweet_spot_hi: float = 0.55

    # === Execution ===
    execution_mode: str = "paper"  # "paper" | "live" | "dry-run"
    min_balance_usd: float = 50.0

    # === Capital management ===
    scan_moneyline: bool = True
    scan_total: bool = False  # Phase 3
    max_daily_positions: int = 20
    max_daily_exposure_usd: float = 2000.0

    # === NBA.com schedule ===
    nba_scoreboard_url: str = (
        "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
    )

    # === Odds API validation ===
    use_odds_api_validation: bool = False  # bookmaker cross-check

    # === Sizing ===
    capital_risk_pct: float = 2.0  # 残高の最大 N% per position
    liquidity_fill_pct: float = 10.0  # ask depth 5c の最大 N%
    max_spread_pct: float = 10.0  # スプレッド上限 % (超えたら skip)
    check_liquidity: bool = True  # 流動性チェック有効/無効

    # === Scheduler ===
    schedule_window_hours: float = 8.0  # ティップオフ何時間前から発注窓 (DCA 用に拡張)
    schedule_max_retries: int = 3  # 失敗時のリトライ上限
    max_orders_per_tick: int = 3  # 1 tick (2分) あたりの最大発注数 (暴走防止)

    # === DCA (Dollar Cost Averaging) ===
    dca_max_entries: int = 5  # 1 アウトカムあたりの最大購入回数 (sovereign 中央値 6-7)
    dca_max_price_spread: float = 0.15  # 初回→最新の最大価格差。超えたら DCA 停止
    dca_min_interval_min: int = 2  # DCA 最小間隔 (分) — TWAP では 1 tick = 2min
    dca_favorable_price_pct: float = 0.0  # 初回価格以下なら favorable (前倒し購入)
    dca_unfavorable_price_pct: float = 10.0  # 10% 以上の上昇で unfavorable (先送り)
    dca_cutoff_before_tipoff_min: int = 30  # ティップオフ N 分前で DCA 打ち切り

    # === Both-Side Betting (Phase B) ===
    bothside_enabled: bool = False  # 無効時は既存フロー完全保持
    bothside_max_combined_vwap: float = 0.995  # combined > threshold なら hedge しない
    bothside_hedge_kelly_mult: float = 0.5  # hedge 側 Kelly 乗数 (directional の半分)
    bothside_hedge_delay_min: int = 30  # directional 発注→ hedge 最小遅延 (分)
    bothside_hedge_max_price: float = 0.55  # hedge 価格上限 (sweet spot 上限と同値)

    # === MERGE (Phase B2) ===
    merge_enabled: bool = False  # BOTHSIDE_ENABLED とは独立
    merge_max_combined_vwap: float = 0.998  # これ以上なら MERGE しない
    merge_min_profit_usd: float = 0.10  # MERGE 利益最低額 (gas 負け防止)
    merge_gas_buffer_gwei: int = 50  # gas price 上限
    merge_max_retries: int = 3  # MERGE 失敗リトライ上限
    merge_ctf_address: str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    merge_collateral_address: str = (
        "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e on Polygon
    )
    merge_polygon_rpc: str = "https://polygon-rpc.com"


settings = Settings()
