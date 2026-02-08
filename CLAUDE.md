# nbabot

Polymarket NBA テンポラルアービトラージ Bot。ブックメーカーオッズを真の確率の代理指標とし、Polymarket の価格調整遅延（エッジ）を検出する。

## プロジェクト構成

```
nbabot/
├── src/
│   ├── config.py                # Pydantic Settings (.env 読込)
│   ├── connectors/
│   │   ├── odds_api.py          # The Odds API (スポーツブックオッズ)
│   │   ├── polymarket.py        # Polymarket Gamma/CLOB API
│   │   └── team_mapping.py      # チーム名 ↔ Polymarket slug 変換
│   ├── strategy/
│   │   └── scanner.py           # 乖離検出 (BUY シグナルのみ)
│   ├── notifications/
│   │   └── telegram.py          # Telegram 通知
│   ├── execution/               # 注文実行 (未実装 — Phase 2)
│   ├── risk/                    # リスク管理 (未実装 — Phase 2)
│   └── store/                   # 取引履歴 DB (未実装)
├── scripts/
│   ├── scan.py                  # 日次エッジスキャン (メインエントリ)
│   └── check_balance.py         # API 接続確認
├── agents/                      # エージェントプロンプト
├── data/reports/                 # 日次レポート出力先 (.gitignore 対象)
├── tests/
├── PLAN.md                      # 戦略設計書 (フェーズ計画・リスクパラメータ)
├── pyproject.toml
└── .env                         # 秘密鍵・API キー (.gitignore 対象)
```

## 開発環境

- **Python**: 3.11+ 必須
- **依存インストール**: `pip install -e .` (venv 推奨)
- **日次スキャン**: `python scripts/scan.py`
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

```
Odds API (h2h) ──→ GameOdds[] ──────────────────┐
                                                  │
Gamma Events API ──→ MoneylineMarket[] ──────────┤
  slug: nba-{away_abbr}-{home_abbr}-YYYY-MM-DD   │
                                                  ↓
                                          scanner.scan()
                                                  │
                                                  ↓
                                        Opportunity[] (BUY のみ)
                                                  │
                                    ┌─────────────┼─────────────┐
                                    ↓             ↓             ↓
                              レポート (.md)  Telegram 通知   (stdout)
```

## Polymarket slug 規則

- 形式: `nba-{away_abbr}-{home_abbr}-YYYY-MM-DD`
- 例: `nba-nyk-bos-2026-02-08`
- チーム略称は `team_mapping.py` の `NBA_TEAMS` dict で管理。
- `commence_time` (UTC) → US Eastern に変換して日付を決定。

## 環境変数 (.env)

| 変数 | 必須 | 説明 |
|------|------|------|
| `ODDS_API_KEY` | Yes | The Odds API キー |
| `HTTP_PROXY` | geo 制限時 | Polymarket 用プロキシ (`socks5://...`) |
| `POLYMARKET_PRIVATE_KEY` | 取引時 | Polygon ウォレット秘密鍵 |
| `TELEGRAM_BOT_TOKEN` | 通知時 | Telegram Bot トークン |
| `TELEGRAM_CHAT_ID` | 通知時 | 通知先チャット ID |
| `MIN_EDGE_PCT` | No | 最小エッジ閾値 % (default: 5) |
| `KELLY_FRACTION` | No | Kelly 分数 (default: 0.25) |
| `MAX_POSITION_USD` | No | 1 取引最大額 (default: 100) |

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
- Odds API 無料枠は月 500 リクエスト。スキャン頻度に注意。
- スキャナーは BUY シグナルのみ出力 (Polymarket が book consensus より安い場合)。
- Kelly criterion の分数 (default 0.25) でポジションサイジング。フル Kelly は使わない。
