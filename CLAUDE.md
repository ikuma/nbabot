# nbabot

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

## プロジェクト構成

```
nbabot/
├── src/
│   ├── config.py                     # Pydantic Settings (.env 読込)
│   ├── connectors/
│   │   ├── nba_schedule.py           # NBA.com スコアボード (ゲーム発見 + スコア取得)
│   │   ├── odds_api.py               # The Odds API (レガシー — bookmaker モード用)
│   │   ├── polymarket.py             # Polymarket Gamma/CLOB API
│   │   └── team_mapping.py           # チーム名 ↔ abbr ↔ slug 変換
│   ├── strategy/
│   │   ├── calibration.py            # 校正テーブル (CalibrationBand, lookup)
│   │   ├── calibration_scanner.py    # 校正ベーススキャナー (主戦略)
│   │   └── scanner.py               # ブックメーカー乖離スキャナー (レガシー)
│   ├── notifications/
│   │   └── telegram.py               # Telegram 通知
│   ├── scheduler/
│   │   └── trade_scheduler.py        # 試合別タイミング発注 (cron 駆動ステートマシン)
│   ├── analysis/
│   │   ├── pnl.py                    # 純関数 P&L 計算 (condition/game 単位)
│   │   └── strategy_profile.py       # 軽量戦略フィンガープリント (Sharpe, DD 等)
│   ├── execution/                    # 注文実行 (未実装 — Phase 4)
│   ├── risk/                         # リスク管理 (未実装 — Phase 4)
│   └── store/
│       └── db.py                     # SQLite (シグナル・結果・trade_jobs ログ)
├── scripts/
│   ├── scan.py                       # 日次エッジスキャン (手動バックアップ用)
│   ├── settle.py                     # 決済 (--auto: 自動 / interactive: 手動)
│   ├── schedule_trades.py            # 試合別スケジューラー CLI (主エントリ)
│   ├── cron_schedule.sh              # スケジューラー cron ラッパー (5分間隔)
│   ├── cron_scan.sh                  # 旧 cron ラッパー (無効化済み・手動用)
│   ├── check_balance.py              # API 接続確認
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
cron (5分ごと)
     │
     ▼
scripts/schedule_trades.py
     │
     ├── 1. refresh_schedule()
     │   NBA.com → trade_jobs テーブルに UPSERT
     │   (試合時刻変更も UPDATE)
     │
     ├── 2. cancel_expired_jobs()
     │   execute_before < now → expired
     │
     ├── 3. process_eligible_jobs()
     │   execute_after <= now < execute_before かつ status=pending
     │     → Gamma API で最新価格取得
     │     → scan_calibration() で EV 判定
     │     → 正の EV なら発注 (mode に応じて paper/live)
     │     → signal_id を trade_jobs に紐付け
     │
     ├── 4. auto_settle()
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
| `SCHEDULE_WINDOW_HOURS` | No | ティップオフ何時間前から発注窓 (default: 2.0) |
| `SCHEDULE_MAX_RETRIES` | No | 失敗時のリトライ上限 (default: 3) |
| `MAX_ORDERS_PER_TICK` | No | 1 tick あたりの最大発注数 (default: 3) |

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
- 校正スキャナーは BUY シグナルのみ。1 試合で両アウトカムの EV を比較し、高い方を 1 つ選択。
- Kelly criterion の分数 (default 0.25) でポジションサイジング。フル Kelly は使わない。
- スイートスポット (0.20-0.55) 内はフル Kelly、外は 0.5x Kelly。
- `scanner.py` (bookmaker 乖離) はレガシーモードとして温存。削除しない。
- Auto-settle は NBA.com スコア (本日分) + Polymarket Gamma API (過去分) の二段構え。
- `scan.py` / `cron_scan.sh` は手動バックアップ用に温存。主エントリは `schedule_trades.py`。
- スケジューラーは cron (5分間隔) + SQLite ジョブキュー。デーモンではない。
- 二重発注防止は 5 層: flock → executing ロック → UNIQUE 制約 → signals 重複チェック → LIMIT 注文。
- `trade_jobs` テーブルのステートマシン: `pending → executing → executed/skipped/failed/expired`。
