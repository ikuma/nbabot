# Polymarket NBA 自動取引システム -- 統合計画書

> 4つの専門家視点（PM、NBAリサーチ、エンジニア、リスク管理）の分析を統合

---

## 全体コンセンサス

4専門家が一致した結論:

1. **主戦略は「テンポラルアービトラージ」** -- ブックメーカーオッズを真の確率の代理指標とし、Polymarketの価格調整遅延を利用する
2. **NBAマネーライン（試合勝敗）市場から開始** -- 流動性が高く、日次決済で資本効率が最良
3. **最大のエッジ源泉は「怪我情報の反映遅延」** -- Polymarket参加者はスポーツブック参加者より情報反応が2-7%遅い
4. **LLMは「判断」に使い「実行」には使わない** -- ハルシネーションリスクを実行層に持ち込まない
5. **段階的に: レポート → アラート → 半自動 → 全自動**

---

## Phase 1: Daily Edge Finder (1-2日で構築)

### やること
毎日、全NBA試合をスキャンし、Polymarket価格 vs ブックメーカーオッズの乖離を検出。LLMで怪我情報やコンテキストを分析し、推奨レポートを自動生成する。**取引は人間が手動で実行。**

### アーキテクチャ
```
[cron: 毎日 15:00 JST]
     |
     v
[Claude Code 1セッション]
  入力: Polymarket API + OddsAPI + WebSearch(怪我情報)
  処理: 乖離計算 + LLM分析
  出力: reports/YYYY-MM-DD.md + Telegram通知(任意)
```

### 最小ディレクトリ構造
```
nbabot/
├── CLAUDE.md
├── PLAN.md
├── pyproject.toml
├── .env.example
├── .gitignore
├── src/
│   ├── __init__.py
│   ├── config.py              # Pydantic Settings
│   ├── connectors/
│   │   ├── polymarket.py      # py-clob-client wrapper
│   │   └── odds_api.py        # The Odds API
│   ├── strategy/
│   │   └── scanner.py         # 乖離検出
│   ├── risk/
│   │   └── manager.py         # Kelly基準 + ポジション上限
│   ├── execution/
│   │   └── executor.py        # 注文実行 (Phase 2~)
│   ├── notifications/
│   │   └── telegram.py        # 通知
│   └── store/
│       └── db.py              # SQLite (取引履歴)
├── scripts/
│   ├── check_balance.py       # API接続確認
│   ├── scan.py                # 日次スキャン
│   └── daily_report.py        # P&Lレポート
├── agents/
│   └── daily-edge-finder.md   # エージェントプロンプト
├── data/
│   └── reports/               # 日次レポート出力先
└── tests/
```

### 技術スタック
- **言語**: Python 3.11+
- **Polymarket**: `py-clob-client` v0.34.5 (公式Python client)
- **オッズ**: The Odds API (無料500req/月、有料$20~/月)
- **NBAデータ**: `nba_api` (無料、NBA.comラッパー)
- **LLM**: Claude API (分析) / Claude Code (レポート生成)
- **DB**: SQLite (MVP) → PostgreSQL (スケール時)
- **通知**: python-telegram-bot

### コスト見積もり
| 項目 | 月額 |
|------|------|
| Claude Code API | $90-150 |
| The Odds API | $0 (無料枠) ~ $20 |
| nba_api | $0 |
| **合計** | **$90-170/月** |

---

## Phase 2: リアルタイムアラート + 半自動実行 (2-4週間後)

- WebSocket接続でPolymarket価格をリアルタイム監視
- オッズ変動トリガーのTelegram即時通知
- Telegramの `/approve` コマンドでワンクリック注文実行
- `dry_run=True` → `dry_run=False` への段階的切替

---

## Phase 3: 完全自動化 (2-3ヶ月後)

### 移行条件 (全て必須)
- [ ] ペーパートレード100取引以上完了
- [ ] 勝率 > 55%
- [ ] シャープレシオ > 1.5
- [ ] 最大ドローダウン < 10%
- [ ] 小額実弾テスト ($25-50) で50取引以上
- [ ] 3段階サーキットブレーカー実装済み
- [ ] Hot/Coldウォレット分離済み

---

## リスク管理パラメータ (初期値)

| パラメータ | Phase 2 | Phase 3 |
|-----------|---------|---------|
| Kelly分数 | 0.25 | 0.25 |
| 1取引最大額 | $100 | $500 |
| 日次損失上限 | 3% ($300) | 5% ($500) |
| 週次損失上限 | 5% ($500) | 8% ($800) |
| 最大ドローダウン | 15% | 20% |
| 最小エッジ | 5% | 4% |
| 最大総エクスポージャー | 30% | 40% |
| 連敗縮小トリガー | 3連敗→50%縮小 | 5連敗→50%縮小 |

### サーキットブレーカー
- **Level 1 (黄)**: 日次損失50%到達 or 3連敗 → サイズ50%縮小、15分クールダウン
- **Level 2 (橙)**: 日次損失上限到達 or 5連敗 → 全取引停止、人間の承認要
- **Level 3 (赤)**: 週次損失上限 or ドローダウン上限 → 全注文キャンセル、72時間停止

### LLMハルシネーション対策
- 数値の妥当性チェック (確率が0-1の範囲内か)
- チーム名の実在確認
- エッジ計算の独立再検証
- 怪我情報の複数ソース照合 (RotoWire + NBA公式 + X)

---

## NBAデータ 日次ワークフロー (JST)

| 時刻 | アクション |
|------|----------|
| 06:00 | 前日結果確認、P&L計算 |
| 07:00 | 本日NBA市場スキャン (Polymarket API) |
| 07:30 | ブックメーカーオッズ取得 (OddsAPI)、乖離計算 |
| 08:00 | 怪我レポート + チーム統計取得 (nba_api, RotoWire) |
| 08:30 | LLM深層分析、レポート生成 |
| 09:00 | 推奨アクション通知 (Telegram) |
| 09:00-12:30 | **試合前モニタリング** (ラインナップ確定、怪我確定を監視) |
| 12:30-16:00 | 試合中モニタリング |

---

## 次のアクション

1. **今すぐ**: `pyproject.toml` 作成、`scripts/check_balance.py` でPolymarket API接続確認
2. **今週**: `src/connectors/polymarket.py` + `src/strategy/scanner.py` 実装
3. **来週**: `src/notifications/telegram.py` + 日次スキャンのcron化
4. **2週間後**: 実データでのペーパートレード開始

---

## ウォレットセキュリティ
- **Cold Wallet (70%)**: ハードウェアウォレット、手動のみ
- **Hot Wallet (30%)**: BOT用、取引上限付き
- 秘密鍵は `.env` → 本番では AWS Secrets Manager
- Polygon上のUSDC、ガス代は$2-5のMATICで数千取引分

---

*本文書は戦略設計書であり、投資助言ではない。予測市場での取引には資金全額損失リスクがある。*
