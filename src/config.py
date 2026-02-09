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
    schedule_window_hours: float = 2.0  # ティップオフ何時間前から発注窓
    schedule_max_retries: int = 3  # 失敗時のリトライ上限
    max_orders_per_tick: int = 3  # 1 tick (5分) あたりの最大発注数 (暴走防止)


settings = Settings()
