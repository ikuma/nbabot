from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    # Polymarket
    polymarket_private_key: str = ""
    polymarket_host: str = "https://clob.polymarket.com"
    polymarket_chain_id: int = 137

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
    min_buy_price: float = 0.20
    max_buy_price: float = 0.85
    sweet_spot_lo: float = 0.25
    sweet_spot_hi: float = 0.55
    min_calibration_edge_pct: float = 3.0  # minimum calibration edge %

    # === Capital management ===
    scan_moneyline: bool = True
    scan_total: bool = False  # Phase 3
    max_daily_positions: int = 20
    max_daily_exposure_usd: float = 2000.0

    # === Odds API validation ===
    use_odds_api_validation: bool = False  # bookmaker cross-check


settings = Settings()
