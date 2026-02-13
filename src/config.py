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
    # PnL divergence drift (Phase drift-upgrade)
    pnl_divergence_short_days: int = 7
    pnl_divergence_long_days: int = 28
    pnl_divergence_min_total_short: int = 30
    pnl_divergence_min_total_long: int = 80
    pnl_divergence_min_band_short: int = 10
    pnl_divergence_yellow_total_gap_pct: float = -15.0
    pnl_divergence_yellow_total_gap_usd: float = -50.0
    pnl_divergence_yellow_band_gap_pct: float = -20.0
    pnl_divergence_yellow_band_gap_usd: float = -20.0
    pnl_divergence_yellow_band_count: int = 2
    pnl_divergence_orange_short_gap_pct: float = -25.0
    pnl_divergence_orange_short_gap_usd: float = -100.0
    pnl_divergence_orange_long_gap_pct: float = -10.0
    # Structural change detection (CUSUM)
    structural_change_window_days: int = 28
    structural_change_cusum_k: float = 0.5
    structural_change_cusum_h_yellow: float = 4.5
    structural_change_cusum_h_orange: float = 6.0
    structural_change_min_points: int = 8
    structural_change_yellow_band_count: int = 1
    structural_change_orange_band_count: int = 2

    # === Calibration strategy ===
    strategy_mode: str = "calibration"  # "calibration" | "bookmaker"
    sweet_spot_lo: float = 0.20
    sweet_spot_hi: float = 0.55

    # === Execution ===
    execution_mode: str = "paper"  # "paper" | "live" | "dry-run"
    min_balance_usd: float = 50.0
    paper_db_path: str = "data/paper_trades.db"
    live_db_path: str = "data/live_trades.db"
    dry_run_db_path: str = ""

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
    dca_per_entry_cap_mult: float = 2.0  # DCA per-entry cap = (remaining/entries) × mult
    dca_min_order_usd: float = 1.0  # DCA minimum order size (USD), below → skip

    # === Both-Side Betting (Phase B) ===
    bothside_enabled: bool = True  # 両サイド購入はデフォルト有効 (利益の核心)
    bothside_max_combined_vwap: float = 0.995  # combined > threshold なら MERGE しない
    bothside_target_combined: float = 0.97  # DEPRECATED: executor uses dynamic MERGE-based pricing
    bothside_target_mode: str = "static"  # DEPRECATED: kept for backward compat
    bothside_target_combined_min: float = 0.90  # DEPRECATED: kept for backward compat
    bothside_target_combined_max: float = 0.994  # DEPRECATED: kept for backward compat
    bothside_dynamic_estimated_fee_usd: float = 0.20  # DEPRECATED: kept for backward compat
    bothside_hedge_kelly_mult: float = 0.5  # hedge 側 Kelly 乗数 (MERGE-only パスのフォールバック)
    bothside_hedge_ratio_mode: str = "static"  # "static" | "optimized"
    bothside_hedge_ratio_file: str = "data/optimized/hedge_ratio.json"
    bothside_hedge_delay_min: int = 30  # directional 発注→ hedge 最小遅延 (分)
    bothside_hedge_max_price: float = 0.55  # DEPRECATED: scanner always returns hedge

    # === LLM Game Analysis (Phase L) ===
    llm_analysis_enabled: bool = False  # フィーチャーフラグ (デフォルト OFF)
    anthropic_api_key: str = ""  # Anthropic API キー
    llm_model: str = "claude-opus-4-6"  # デフォルト: Opus 4.6 (最高品質)
    llm_timeout_sec: int = 30  # 各ペルソナ呼び出しタイムアウト (秒)
    llm_max_sizing_modifier: float = 1.5  # sizing_modifier 上限
    llm_min_sizing_modifier: float = 0.5  # sizing_modifier 下限

    # === Calibration confidence (Phase Q) ===
    calibration_confidence_level: float = 0.90  # Beta posterior lower percentile

    # === Order lifecycle manager (Phase O) ===
    order_manager_enabled: bool = True
    order_ttl_min: int = 5  # 未約定注文の TTL (分)
    order_max_replaces: int = 3  # 最大再発注回数
    order_min_price_move: float = 0.01  # 再発注トリガーの最小価格移動
    order_check_batch_size: int = 10  # 1 tick あたり最大チェック数
    order_rate_limit_sleep: float = 0.5  # API 呼び出し間の sleep 秒

    # === MERGE (Phase B2/H) ===
    merge_enabled: bool = True  # MERGE はデフォルト有効 (BOTHSIDE とは独立)
    merge_max_combined_vwap: float = 0.998  # これ以上なら MERGE しない
    merge_min_profit_usd: float = 0.10  # MERGE 利益最低額 (gas 負け防止)
    merge_est_gas_usd: float = 0.05  # MERGE gas 見積もり USD (Polygon, 保守的)
    merge_min_shares_floor: float = 20.0  # MERGE 最小想定 shares (動的 margin 算出の安全弁)
    merge_gas_buffer_gwei: int = 50  # gas price 上限
    merge_max_retries: int = 3  # MERGE 失敗リトライ上限
    merge_early_partial_enabled: bool = False  # DCA 完了前の条件付き部分 MERGE
    merge_early_partial_assumed_fee_usd: float = 0.05  # paper/dry-run の追加 fee 仮定
    merge_early_partial_capital_rate_per_hour: float = 0.0005  # 解放資本の時間価値
    merge_early_partial_post_tipoff_hours: float = 3.0  # 解放効果の評価 horizon
    merge_early_partial_min_benefit_over_fee_usd: float = 0.0  # benefit が fee を上回る最低差
    merge_early_partial_rollout_pct: int = 25  # 段階ロールアウト比率
    merge_early_partial_max_per_tick: int = 1  # 1 tick あたりの早期部分 MERGE 上限
    merge_early_partial_guard_lookback: int = 20  # ガード用の直近サンプル数
    merge_early_partial_guard_min_samples: int = 5  # ガード発動に必要な最小サンプル
    merge_early_partial_guard_min_avg_net_profit_usd: float = -0.05  # 下回ったら停止
    merge_ctf_address: str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    merge_collateral_address: str = (
        "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e on Polygon
    )
    merge_polygon_rpc: str = "https://polygon-rpc.com"
    merge_safe_outer_gas_limit: int = 400_000  # Safe execTransaction の外側 gas limit

    # === Game Position Group state machine (Track B) ===
    game_position_group_enabled: bool = False  # Track B rollout flag
    position_group_default_d_max: float = 50.0  # Default max directional residual (shares)
    position_group_min_merge_shares: float = 5.0  # m >= threshold to enter MERGE_LOOP
    position_group_new_risk_cutoff_min: int = 30  # before tipoff, stop increasing risk
    position_group_utility_enabled: bool = True  # choose first leg by utility when Track B enabled
    position_group_utility_alpha_weight: float = 1.0
    position_group_utility_merge_weight: float = 1.0
    position_group_utility_slippage_weight: float = 1.0
    position_group_dmax_decay_enabled: bool = True  # D_max(t) decay near tipoff
    position_group_dmax_decay_start_min: int = 180  # decay starts N minutes before tipoff
    position_group_dmax_decay_floor_ratio: float = 0.3  # D_max floor as ratio of base
    position_group_leg2_timeout_min: int = 90  # timeout for waiting second leg completion
    position_group_safe_stop_flags: str = "balance_anomaly"  # comma-separated risk flags
    position_group_safe_stop_on_risk_error: bool = True  # fail-closed on risk engine error


settings = Settings()
