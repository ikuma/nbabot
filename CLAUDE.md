# nbabot

## 言語設定

- **応答は必ず日本語で行う**。コード内のコメント・docstring は英語 OK（コーディング規約に従う）。

## 概要

Polymarket NBA キャリブレーション Bot。Polymarket の構造的ミスプライシング（価格帯ごとの系統的な過小評価）を校正テーブルで検出し、広く刈り取る戦略。

## 戦略の変遷

### 旧方針: ブックメーカー乖離 (bookmaker divergence)
初期構想はブックメーカーコンセンサス (The Odds API) と Polymarket 価格の乖離を検出するテンポラルアービトラージだった。
しかし lhtsports の P&L 深掘り分析により、ブックメーカーオッズとの差分よりも **Polymarket 自体の構造的ミスプライシング** のほうがはるかに大きく安定したエッジ源であることが判明。
旧方式は `--mode bookmaker` で引き続き利用可能だが、主戦略は校正モードに完全移行済み。

### 現行方針: キャリブレーション (calibration) — 主戦略
- Polymarket は価格帯 0.20-0.55 のアウトカムを系統的に過小評価している (暗示確率 20-40% → 実勝率 71-90%)
- lhtsports の実績データ ($38.7M リスク → +$1.2M, ROI 3.11%) から導出した校正テーブルで期待勝率を推定
- 各試合で両アウトカムの EV/$ を比較し、高い方を 1 つだけ購入
- 予測モデル不要 — 価格帯ベースの構造的エッジ

## フェーズ進捗

> 詳細は `PLAN.md` を参照。

| Phase | 内容 | 状態 |
|-------|------|------|
| 1 | 校正スキャナー + ペーパートレード | **完了** |
| 2 | NBA.com 駆動ディスカバリー + 自動決済 | **完了** |
| 2.5 | 校正テーブル精緻化 + バックテスト | **完了** |
| 3 | Per-game スケジューラー (SQLite ジョブキュー) | **完了** |
| 3a | 流動性対応ポジションサイジング (3 層制約) | **完了** |
| 3b | DCA / TWAP 適応実行 (max 5 回, 2 分間隔) | **完了** |
| B | Both-Side Betting (directional + hedge) | **完了** |
| B2 | MERGE (CTF mergePositions — YES+NO→USDC) | **完了** |
| R | コードベースリファクタリング (500行分割) | **完了** |
| D | リスク管理 + インフラ強化 (CB, ドリフト, WAL, ログ) | **完了** |
| B3 | POLY_PROXY (Gnosis Safe) MERGE 対応 | **完了** |
| F1 | Bothside + MERGE デフォルト有効化 | **完了** |
| L | LLM ベース試合分析 (3 ペルソナ + シンセシス) | **完了** |
| C | Total (O/U) マーケット校正 | 未着手 |
| E | スケール + 本番運用 ($30-50K) | 未着手 |

## プロジェクト構成

```
nbabot/
├── src/
│   ├── config.py                     # Pydantic Settings (.env 読込)
│   ├── connectors/
│   │   ├── ctf.py                    # CTF コントラクト (mergePositions — Phase B2/B3)
│   │   ├── safe_tx.py                # Gnosis Safe execTransaction ヘルパー (Phase B3)
│   │   ├── nba_data.py               # ESPN チーム成績・怪我・B2B データ収集 (Phase L)
│   │   ├── nba_schedule.py           # NBA.com スコアボード (ゲーム発見 + スコア取得)
│   │   ├── odds_api.py               # The Odds API (レガシー — bookmaker モード用)
│   │   ├── polymarket.py             # Polymarket Gamma/CLOB API
│   │   └── team_mapping.py           # チーム名 ↔ abbr ↔ slug 変換
│   ├── strategy/
│   │   ├── calibration.py            # 校正テーブル (CalibrationBand, lookup)
│   │   ├── calibration_scanner.py    # 校正ベーススキャナー (主戦略)
│   │   ├── dca_strategy.py           # DCA 判定ロジック (時間/価格トリガー, VWAP 共通関数)
│   │   ├── merge_strategy.py         # MERGE 判定純関数 (shares 計算, VWAP, ガード)
│   │   ├── llm_analyzer.py          # 3 ペルソナ並列 LLM 分析 + シンセシス (Phase L)
│   │   ├── llm_cache.py             # LLM 分析 SQLite キャッシュ (Phase L)
│   │   ├── prompts/
│   │   │   └── game_analysis.py     # 4 プロンプト定義 (3 ペルソナ + シンセシス)
│   │   └── scanner.py               # ブックメーカー乖離スキャナー (レガシー)
│   ├── notifications/
│   │   └── telegram.py               # Telegram 通知
│   ├── scheduler/
│   │   ├── trade_scheduler.py        # ディスパッチャ (refresh, recover, process, summary)
│   │   ├── job_executor.py           # 初回発注処理 (directional)
│   │   ├── hedge_executor.py         # Hedge ジョブ処理 (bothside)
│   │   ├── dca_executor.py           # DCA 追加購入処理
│   │   └── merge_executor.py         # MERGE 処理 (CTF mergePositions)
│   ├── settlement/
│   │   ├── pnl_calc.py              # 決済 P&L 計算 (DCA, bothside, merge)
│   │   └── settler.py               # 決済コアロジック (auto_settle, settle_signal)
│   ├── sizing/
│   │   ├── liquidity.py              # 注文板流動性抽出 (LiquiditySnapshot, extract, score)
│   │   └── position_sizer.py         # 3層制約サイジング (Kelly×残高×流動性)
│   ├── analysis/
│   │   ├── pnl.py                    # 純関数 P&L 計算 (condition/game 単位)
│   │   ├── report_generator.py       # P&L レポート生成 (generate_report)
│   │   └── strategy_profile.py       # 軽量戦略フィンガープリント (Sharpe, DD 等)
│   ├── logging_config.py             # 構造化ログ (JSONFormatter, setup_logging)
│   ├── execution/                    # 注文実行 (未実装 — Phase E)
│   ├── risk/
│   │   ├── models.py                 # RiskState, CircuitBreakerLevel, CalibrationHealthMetrics
│   │   ├── risk_engine.py            # サーキットブレーカー + 段階的復帰 + degraded mode
│   │   ├── calibration_monitor.py    # 校正ドリフト検出 (バンド別 z-score)
│   │   └── health.py                 # 3 階層ヘルスチェック (local/API/integrity)
│   └── store/
│       ├── db.py                     # SQLite クエリ関数 (re-export 付き)
│       ├── models.py                 # データモデル (SignalRecord, TradeJob, JobStatus 等)
│       └── schema.py                 # DDL + マイグレーション (_connect, _ensure_*)
├── scripts/
│   ├── scan.py                       # 日次エッジスキャン (手動バックアップ用)
│   ├── settle.py                     # 決済 CLI (コアは src/settlement/)
│   ├── schedule_trades.py            # 試合別スケジューラー CLI (主エントリ)
│   ├── cron_schedule.sh              # スケジューラー cron ラッパー (15分間隔, 24/7, caffeinate 付き)
│   ├── cron_scan.sh                  # 旧 cron ラッパー (無効化済み・手動用)
│   ├── check_balance.py              # API 接続確認
│   ├── survey_liquidity.py           # NBA マーケット流動性調査
│   ├── discover_traders.py           # リーダーボードからトレーダー発見
│   ├── fetch_trader.py               # 任意トレーダーの取引データ取得
│   ├── analyze_trader.py             # P&L + 戦略プロファイル分析
│   └── compare_traders.py            # 複数トレーダー比較レポート
├── agents/                           # エージェントプロンプト
├── data/reports/                     # 日次レポート出力先 (.gitignore 対象)
├── data/logs/                        # スケジューラーログ (.gitignore 対象)
├── data/traders/                     # トレーダーデータ (.gitignore 対象)
├── tests/
├── PLAN.md                           # 戦略設計書 (フェーズ計画・リスクパラメータ)
├── pyproject.toml
└── .env                              # 秘密鍵・API キー (.gitignore 対象)
```

## 開発環境

- **Python**: 3.11+ 必須
- **依存インストール**: `pip install -e .` (venv 推奨)
- **スケジューラー (主)**: `python scripts/schedule_trades.py` (5分 cron で自動実行)
- **スケジューラー dry-run**: `python scripts/schedule_trades.py --execution dry-run`
- **未来日付テスト**: `python scripts/schedule_trades.py --date 2026-02-10 --execution dry-run`
- **手動スキャン (バックアップ)**: `python scripts/scan.py` (デフォルト: calibration モード)
- **モード指定**: `python scripts/scan.py --mode calibration|bookmaker|both`
- **自動決済**: `python scripts/settle.py --auto` (NBA.com スコア + Polymarket フォールバック)
- **決済 dry-run**: `python scripts/settle.py --auto --dry-run`
- **手動決済**: `python scripts/settle.py` (interactive)
- **未決済一覧**: `python scripts/settle.py --list`
- **ジョブ確認**: `sqlite3 data/paper_trades.db "SELECT * FROM trade_jobs"`
- **接続確認**: `python scripts/check_balance.py`
- **テスト**: `pytest`
- **リント**: `ruff check src/ scripts/`
- **フォーマット**: `ruff format src/ scripts/`

## コーディング規約

- 言語: Python 3.11+、型ヒント必須。`Any` の使用は外部 API レスポンス等やむを得ない箇所に限定。
- dataclass ベースのデータモデル (Pydantic は `config.py` のみ)。
- フォーマット/リント: Ruff (`pyproject.toml` の `[tool.ruff]` に設定済み)。
- ファイルは 500 行以下を目安に分割。
- docstring は英語、インラインコメントは日本語 OK。
- `import` の順序: stdlib → サードパーティ → ローカル (Ruff の `I` ルールで自動ソート)。

## データフロー

### Per-game スケジューラー (主戦略)

```
cron (15分間隔, 24/7)
     │
     ▼
scripts/schedule_trades.py
     │
     ├── 0. load_or_compute_risk_state() — リスクチェック (Phase D)
     │   daily PnL, weekly PnL, 連敗, ドローダウン, 校正ドリフト算出
     │   → CircuitBreakerLevel (GREEN/YELLOW/ORANGE/RED) + sizing_multiplier
     │   RED: settle-only モード → DCA 強制停止 → 通知 → 終了
     │   YELLOW+: DCA 新規エントリー停止
     │   risk engine 障害時: degraded mode (sizing_multiplier=0.5)
     │
     ├── 1. refresh_schedule()
     │   NBA.com → trade_jobs テーブルに UPSERT
     │   (試合時刻変更も UPDATE)
     │
     ├── 2. cancel_expired_jobs()
     │   execute_before < now → expired (pending/failed)
     │   execute_before < now → executed (dca_active — DCA 完了扱い)
     │
     ├── 3. process_eligible_jobs(sizing_multiplier) — 初回エントリー
     │   execute_after <= now < execute_before かつ status=pending
     │   ├── job_side='directional':
     │   │     → Gamma API で最新価格取得
     │   │     → CLOB API で注文板取得 (流動性チェック有効時)
     │   │     → [Phase L] LLM_ANALYSIS_ENABLED 時: build_game_context() → analyze_game()
     │   │       3 ペルソナ並列 LLM 呼び出し → シンセシス → GameAnalysis
     │   │       → LLM が directional (favored_team) を決定
     │   │       → sizing_modifier, hedge_ratio を取得
     │   │     → BOTHSIDE_ENABLED 時: scan_calibration_bothside() で両サイド EV 判定
     │   │     → それ以外: scan_calibration() で EV 判定 (3層制約: Kelly×残高×流動性)
     │   │     → 正の EV なら発注 → dca_group_id 生成
     │   │     → hedge 条件通過なら hedge ジョブを pending で作成
     │   │     → DCA 有効時: status → dca_active (1/N)
     │   └── job_side='hedge':
     │         → paired directional の反対アウトカムを特定
     │         → combined VWAP 再チェック (directional VWAP + hedge 現在価格)
     │         → NG → skip / OK → 発注 (独立 DCA グループ)
     │
     ├── 3b. process_dca_active_jobs() — DCA 追加購入
     │   status=dca_active かつ entries < max_entries
     │     → should_add_dca_entry() で時間/価格トリガー判定
     │     → yes なら同一アウトカムを追加購入 (dca_sequence++)
     │     → max 到達で status → executed
     │     → hedge DCA も同一ロジックで処理 (signal_role='hedge' 付与)
     │
     ├── 3c. process_merge_eligible() — MERGE (Phase B2)
     │   dir+hedge 両方 executed かつ merge_status='none'
     │     → calculate_mergeable_shares() で min(dir, hedge) ペア計算
     │     → calculate_combined_vwap() + should_merge() でガードチェック
     │     → Paper: simulate_merge() → status='simulated'
     │     → Live: CTF mergePositions() → status='executed' or 'failed'
     │     → merge_operations + trade_jobs.merge_status を更新
     │
     ├── 4. auto_settle() — DCA グループ + bothside + MERGE 一括決済
     │   DCA グループは VWAP ベース PnL (total_shares * $1 - total_cost)
     │   bothside グループは directional PnL + hedge PnL の combined 計算
     │   延期試合: settle スキップ + 警告ログ / OT: 正常 settle + 注記
     │
     ├── 4b. save_risk_snapshot() — リスク状態永続化 (Phase D)
     │   キャッシュ無効化 → 再計算 → risk_snapshots テーブルに保存
     │   レベル変更時: Telegram アラート通知
     │
     └── 5. Telegram サマリー通知
```

### Calibration モード (手動スキャン — バックアップ)

```
NBA.com Scoreboard ──→ NBAGame[] ──→ ゲームリスト + スコア
                                    │
Gamma Events API ──→ MoneylineMarket[] ──→ 両アウトカム価格
                                    │
                                    ↓
                      calibration_scanner.scan_calibration()
                        校正テーブル lookup → EV/$ 計算
                        → 高 EV 側を選択 (1 試合 1 シグナル)
                                    │
                                    ↓
                        CalibrationOpportunity[] (BUY のみ)
                                    │
                      ┌─────────────┼─────────────┐
                      ↓             ↓             ↓
                レポート (.md)  Telegram 通知  SQLite 記録
```

### Auto-Settle フロー

```
get_unsettled() ──→ SignalRecord[]
                         │
                    event_slug をパース
                    (away_abbr, home_abbr, date)
                         │
          ┌──────────────┼──────────────┐
          │ slug日付 == 今日             │ slug日付 ≠ 今日
          ↓                             ↓
   NBA.com スコアボード            Gamma Events API
   game_status==3 のみ            active==false かつ
   スコアから勝者判定              price >= 0.95 で判定
          │                             │
          └──────────────┬──────────────┘
                         ↓
              PnL 計算 → log_result() → Telegram
```

### Bookmaker モード (レガシー)

```
Odds API (h2h) ──→ GameOdds[] ──────────────────┐
                                                  │
Gamma Events API ──→ MoneylineMarket[] ──────────┤
                                                  ↓
                                          scanner.scan()
                                                  │
                                                  ↓
                                        Opportunity[] (BUY のみ)
```

## Polymarket slug 規則

- 形式: `nba-{away_abbr}-{home_abbr}-YYYY-MM-DD`
- 例: `nba-nyk-bos-2026-02-08`
- チーム略称は `team_mapping.py` の `NBA_TEAMS` dict で管理。
- `commence_time` (UTC) → US Eastern に変換して日付を決定。

## 環境変数 (.env)

| 変数 | 必須 | 説明 |
|------|------|------|
| `ODDS_API_KEY` | No* | The Odds API キー (*bookmaker モードのみ) |
| `HTTP_PROXY` | geo 制限時 | Polymarket 用プロキシ (`socks5://...`) |
| `POLYMARKET_PRIVATE_KEY` | 取引時 | Polygon ウォレット秘密鍵 |
| `TELEGRAM_BOT_TOKEN` | 通知時 | Telegram Bot トークン |
| `TELEGRAM_CHAT_ID` | 通知時 | 通知先チャット ID |
| `STRATEGY_MODE` | No | `calibration` (default) / `bookmaker` |
| `SWEET_SPOT_LO` | No | スイートスポット下限 (default: 0.20, フル Kelly) |
| `SWEET_SPOT_HI` | No | スイートスポット上限 (default: 0.55, 超えると 0.5x Kelly) |
| `MIN_EDGE_PCT` | No | bookmaker モード最小エッジ閾値 % (default: 1.0) |
| `KELLY_FRACTION` | No | Kelly 分数 (default: 0.25) |
| `MAX_POSITION_USD` | No | 1 取引最大額 (default: 100) |
| `MAX_DAILY_POSITIONS` | No | 1 日最大ポジション数 (default: 20) |
| `MAX_DAILY_EXPOSURE_USD` | No | 1 日最大エクスポージャー (default: 2000) |
| `CAPITAL_RISK_PCT` | No | 残高の最大 N% per position (default: 2.0) |
| `LIQUIDITY_FILL_PCT` | No | ask depth 5c の最大 N% (default: 10.0) |
| `MAX_SPREAD_PCT` | No | スプレッド上限 % (default: 10.0, 超えたら skip) |
| `CHECK_LIQUIDITY` | No | 流動性チェック有効/無効 (default: true) |
| `SCHEDULE_WINDOW_HOURS` | No | ティップオフ何時間前から発注窓 (default: 8.0, DCA 用に拡張) |
| `SCHEDULE_MAX_RETRIES` | No | 失敗時のリトライ上限 (default: 3) |
| `MAX_ORDERS_PER_TICK` | No | 1 tick あたりの最大発注数 (default: 3) |
| `DCA_MAX_ENTRIES` | No | 1 アウトカムあたり最大 DCA 回数 (default: 5) |
| `DCA_MIN_PRICE_DIP_PCT` | No | VWAP から N%+ 下落でボーナス購入 (default: 3.0) |
| `DCA_MAX_PRICE_SPREAD` | No | 初回→最新の最大価格差 (default: 0.15, 超えたら DCA 停止) |
| `DCA_MIN_INTERVAL_MIN` | No | DCA 最小間隔 (分, default: 30) |
| `BOTHSIDE_ENABLED` | No | 両サイドベット有効/無効 (default: true) |
| `BOTHSIDE_MAX_COMBINED_VWAP` | No | combined VWAP 上限 (default: 0.995, 超えたら hedge しない) |
| `BOTHSIDE_HEDGE_KELLY_MULT` | No | hedge 側 Kelly 乗数 (default: 0.5) |
| `BOTHSIDE_HEDGE_DELAY_MIN` | No | directional→hedge 最小遅延 (分, default: 30) |
| `BOTHSIDE_HEDGE_MAX_PRICE` | No | hedge 価格上限 (default: 0.55) |
| `MERGE_ENABLED` | No | MERGE 有効/無効 (default: true, BOTHSIDE_ENABLED とは独立) |
| `MERGE_MAX_COMBINED_VWAP` | No | MERGE 判定 combined VWAP 上限 (default: 0.998) |
| `MERGE_MIN_PROFIT_USD` | No | MERGE 最低利益 (default: 0.10, gas 負け防止) |
| `MERGE_GAS_BUFFER_GWEI` | No | gas price 上限 gwei (default: 50) |
| `MERGE_MAX_RETRIES` | No | MERGE 失敗リトライ上限 (default: 3) |
| `MERGE_CTF_ADDRESS` | No | CTF コントラクトアドレス (default: Polymarket CTF) |
| `MERGE_COLLATERAL_ADDRESS` | No | USDC コントラクトアドレス (default: USDC.e on Polygon) |
| `MERGE_POLYGON_RPC` | No | Polygon RPC URL (default: https://polygon-rpc.com) |
| `MERGE_SAFE_OUTER_GAS_LIMIT` | No | Safe execTransaction の外側 gas limit (default: 400000) |
| `DAILY_LOSS_LIMIT_PCT` | No | 日次損失限度 % → ORANGE トリガー (default: 3.0) |
| `WEEKLY_LOSS_LIMIT_PCT` | No | 週次損失限度 % → RED トリガー (default: 5.0) |
| `MAX_DRAWDOWN_LIMIT_PCT` | No | 最大ドローダウン % → RED トリガー (default: 15.0) |
| `RISK_CHECK_ENABLED` | No | リスクチェック有効/無効 (default: true) |
| `CALIBRATION_DRIFT_THRESHOLD` | No | 校正ドリフト検出閾値 σ (default: 2.0) |
| `MAX_TOTAL_EXPOSURE_PCT` | No | 資金の最大同時リスク % (default: 30.0) |
| `RISK_MAX_SINGLE_GAME_USD` | No | 1 試合あたり最大エクスポージャー (default: 200.0) |
| `LLM_ANALYSIS_ENABLED` | No | LLM 試合分析有効/無効 (default: false) |
| `ANTHROPIC_API_KEY` | LLM 時 | Anthropic API キー |
| `LLM_MODEL` | No | LLM モデル ID (default: claude-opus-4-6) |
| `LLM_TIMEOUT_SEC` | No | 各ペルソナ呼び出しタイムアウト秒 (default: 30) |
| `LLM_MAX_SIZING_MODIFIER` | No | LLM sizing_modifier 上限 (default: 1.5) |
| `LLM_MIN_SIZING_MODIFIER` | No | LLM sizing_modifier 下限 (default: 0.5) |

## セキュリティ

- `.env`、秘密鍵、API キーは絶対にコミットしない。
- `.env.example` にはダミー値のみ記載。
- ウォレット秘密鍵はローカル `.env` に保管。本番環境では Secrets Manager 等を使用。
- 外部 API レスポンスのバリデーションを怠らない。

## テスト

- フレームワーク: pytest
- テストファイル: `tests/` 配下、`test_*.py` 命名。
- API 呼び出しはモック化。実 API テストは環境変数で明示的に有効化。
- ロジック変更時は `pytest` を実行してからコミット。

## コミット規約

- Conventional Commits 形式: `type: message` (例: `feat: add team mapping`, `fix: correct slug date timezone`)
- type: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`
- 関連する変更はまとめ、無関係なリファクタは分離。

## 設計上の注意

- `py-clob-client` は CLOB フォールバック専用。Events API パスでは不要 (lazy import 済み)。
- `data/reports/*.md` は `.gitignore` 対象。
- Polymarket は地域制限あり。日本からは `HTTP_PROXY` が必要な場合がある。
- Odds API は **calibration モードでは不要**。NBA.com スコアボードでゲーム発見。bookmaker モードのみ使用。
- 校正スキャナーは BUY シグナルのみ。`BOTHSIDE_ENABLED=false` 時は 1 試合 1 シグナル (高 EV 側)。`true` (デフォルト) 時は両サイド購入可能。
- bothside + MERGE はデフォルト有効。sovereign2013 の利益の 46.5% が MERGE で、本システムの利益の核心。`BOTHSIDE_ENABLED=false` の状態は設計意図に反する。
- Kelly criterion の分数 (default 0.25) でポジションサイジング。フル Kelly は使わない。
- スイートスポット (0.20-0.55) 内はフル Kelly、外は 0.5x Kelly。
- `scanner.py` (bookmaker 乖離) はレガシーモードとして温存。削除しない。
- Auto-settle は NBA.com スコア (本日分) + Polymarket Gamma API (過去分) の二段構え。
- `scan.py` / `cron_scan.sh` は手動バックアップ用に温存。主エントリは `schedule_trades.py`。
- スケジューラーは cron (15分間隔, 24/7) + SQLite ジョブキュー。デーモンではない。"Dumb scheduler, smart worker" パターン: cron はハートビート、スクリプト内で today+tomorrow (ET) を探索し実行窓内ジョブのみ処理。窓外は ~3秒で終了。caffeinate -i で macOS スリープ防止。
- 二重発注防止は 5 層: flock → executing ロック → UNIQUE(event_slug, job_side) 制約 → signals 重複チェック → LIMIT 注文。
- `trade_jobs` テーブルのステートマシン: `pending → executing → executed/skipped/failed/expired` + DCA: `executing → dca_active → executed`。
- Both-side: directional ジョブ処理後に hedge ジョブを pending で作成。hedge は独立 DCA グループで TWAP 実行。combined VWAP ガードで利鞘なし取引を排除。
- MERGE (Phase B2/B3): CTF `mergePositions` で YES+NO トークンペアを即座に 1 USDC に変換。Post-DCA 一括 MERGE (gas 1 回)。`MERGE_ENABLED` フラグで制御。EOA (sig_type=0) は直接呼び出し、POLY_PROXY (sig_type=1, 1-of-1 Gnosis Safe) は `safe_tx.exec_safe_transaction()` 経由。マルチシグ Safe (threshold>1) と Magic Link は未対応。Paper mode では Web3 不要でシミュレーション。
- リスク管理 (Phase D): 3 段階サーキットブレーカー (GREEN→YELLOW→ORANGE→RED)。毎 tick で PnL・連敗・ドローダウン・校正ドリフトを算出。RED は手動解除のみ (72h ロック)、ORANGE は 24h 後に自動降格条件あり。段階的復帰メカニズムで即座のフルサイズ復帰を防止。
- Risk engine 障害時は degraded mode (sizing_multiplier=0.5) で保守的に続行。`RISK_CHECK_ENABLED=false` で無効化可能。
- 校正ドリフト検出: バンド別の rolling 勝率をテーブル期待勝率と z-score 比較。2σ 超の乖離で ORANGE トリガー。
- RiskState は `risk_snapshots` テーブルに永続化。cron ステートレス問題を解消。
- SQLite WAL モード有効。reader-writer 並行性向上 (手動 settle + cron の競合安全化)。
- 構造化ログ: `STRUCTURED_LOGGING=true` で JSON 出力。TimedRotatingFileHandler (30 日保持)。
- ヘルスチェック 3 階層: local (毎 tick — DB 接続 + ディスク), API (5 tick 毎 — NBA.com + Polymarket), integrity (日次 — PRAGMA integrity_check)。
- LLM 試合分析 (Phase L): 3 ペルソナ (Polymarket 凄腕トレーダー, クオンツ, リスク管理) 並列呼び出し + シンセシス統合。LLM が directional (favored_team) を決定し、校正テーブルはサイジングのみ。`LLM_ANALYSIS_ENABLED=false` (デフォルト) で無効化。全 LLM 障害は従来パイプラインにフォールバック。
- LLM 分析は `llm_analyses` テーブルに event_slug 単位でキャッシュ。DCA 後続・hedge は同一キャッシュを再利用。
- LLM コスト: Opus 4.6 ($72/月), Sonnet 4.5 ($14/月), Haiku 4.5 ($5/月)。`LLM_MODEL` env で切替可能。
