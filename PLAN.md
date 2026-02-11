# Polymarket NBA 自動取引システム -- 統合計画書

> 4つの専門家視点（PM、NBAリサーチ、エンジニア、リスク管理）の分析を統合

---

## 戦略転換の経緯

### 旧方針: テンポラルアービトラージ (廃止)

初期構想では、ブックメーカーオッズ (The Odds API) を「真の確率」の代理指標とし、Polymarket の価格調整遅延を検出するテンポラルアービトラージを主戦略としていた。

**廃止理由:**
- lhtsports の P&L 深掘り分析により、ブックメーカーとの差分 (数%) よりも Polymarket 自体の構造的ミスプライシング (20-36%) のほうが桁違いに大きいことが判明
- ブックメーカーオッズは「方向性」の補助情報にはなるが、エッジの主源泉ではない
- Odds API の月 500 リクエスト制限がスケーラビリティのボトルネックだった

旧方式のコードは `src/strategy/scanner.py` に `--mode bookmaker` として温存。完全削除はしない。

### 現行方針: キャリブレーション戦略

lhtsports の P&L 深掘り分析 ($38.7M リスク → +$1.2M, ROI 3.11%) により、**Polymarket の構造的ミスプライシングを校正テーブルで広く刈り取る戦略**に転換。

**根拠:**
1. **予測モデル不要**: 価格帯ベースで購入 — Polymarket 価格 0.20-0.55 のアウトカムを広く買う
2. **校正エッジが本体**: Polymarket は系統的にアウトカムを過小評価 (0.20-0.55 帯: 暗示確率 20-55% → 実勝率 71-95%)
3. **エッジは構造的・安定的**: 2024-Q4 → 2026-Q1 で校正エッジ +20-36% を維持。減衰していない
4. **ML+Total の独立エッジ**: ROI 2.3% → 10.0%。phi 相関 0.05 = ほぼ独立した追加エッジ源

**サイド選択**: 校正カーブが凹型 → EV/$ は低価格側ほど高い → 自然にアンダードッグ優先 (~85% 校正 EV 数学、~15% 外部方向性確信)。

### 校正テーブル (2024-12 〜 2026-02, 1,395 settled conditions)

| Band | N | 勝率 | ROI | 信頼度 | ゾーン |
|------|---|------|-----|--------|--------|
| 0.20-0.25 | 45 | 71.1% | 30.3% | medium | sweet spot |
| 0.25-0.30 | 54 | 85.2% | 54.5% | medium | sweet spot |
| 0.30-0.35 | 73 | 82.2% | 20.0% | medium | sweet spot |
| 0.35-0.40 | 104 | 90.4% | 26.1% | high | sweet spot |
| 0.40-0.45 | 121 | 91.7% | 7.4% | high | sweet spot |
| 0.45-0.50 | 162 | 93.8% | 5.9% | high | sweet spot |
| 0.50-0.55 | 169 | 94.7% | 6.2% | high | sweet spot |
| 0.55-0.60 | 141 | 95.7% | 4.0% | high | 0.5x Kelly |
| 0.60-0.65 | 78 | 97.4% | 16.0% | medium | 0.5x Kelly |
| 0.65-0.70 | 58 | 93.1% | 2.1% | medium | 0.5x Kelly |
| 0.70-0.75 | 45 | 93.3% | 15.5% | medium | 0.5x Kelly |
| 0.75-0.80 | 37 | 97.3% | 15.9% | low | 0.5x Kelly |
| 0.80-0.85 | 33 | 100% | 17.4% | low | 0.5x Kelly |
| 0.85-0.90 | 30 | 100% | 14.1% | low | 0.5x Kelly |
| 0.90-0.95 | 22 | 100% | 8.8% | low | 0.5x Kelly |

**注意**: 上記勝率は lhtsports の DCA (ドルコスト平均法) 込みの数値。シングルエントリーでは勝率は低下する (DCA 97% vs シングル 66%)。

### バックテスト結果 (直近1000試合: 2025-03 〜 2026-02)

- シグナル発生: **86.2%** (862/1000)
- 勝率: **93.7%** (808勝 54敗)
- モデル P&L: **+$555K** vs lhtsports 実績: **+$572K** (差 3%)
- Sweet Spot (0.20-0.55): 勝率 91.4%, P&L +$276K
- Outside (0.55-0.95): 勝率 96.8%, P&L +$278K

---

## フェーズ進捗

### Phase 1: 校正スキャナー + ペーパートレード — **完了**
- 校正テーブル (`src/strategy/calibration.py`) と校正スキャナー (`calibration_scanner.py`) を実装
- SQLite でシグナル・結果を記録 (`src/store/db.py`)
- Telegram 通知 (`src/notifications/telegram.py`)
- 日次スキャン (`scripts/scan.py`)
- 手動決済 (`scripts/settle.py` interactive mode)

### Phase 2: NBA.com 駆動ディスカバリー + 自動決済 — **完了**
- NBA.com スコアボード API でゲーム発見 (`src/connectors/nba_schedule.py`)
  - Odds API 依存を除去。calibration モードでは Odds API 不要に
  - スコア取得 (`home_score`, `away_score`) も対応
- チーム名逆引き (`full_name_from_abbr`) を `team_mapping.py` に追加
- **Auto-settle** (`scripts/settle.py --auto`):
  - NBA.com スコア (本日の final ゲーム) で自動勝敗判定 + PnL 記録
  - Polymarket Gamma Events API フォールバック (過去日付のシグナル)
  - `--dry-run` で確認のみモード
  - 決済結果を Telegram 通知
- cron 統合 (`scripts/cron_scan.sh`): scan → auto-settle を一気通貫実行

### Phase 2.5: 校正テーブル精緻化 + バックテスト — **完了**
- エッジ閾値 (`MIN_CALIBRATION_EDGE_PCT`) を撤廃 — EV > 0 のみでフィルタ
- 価格レンジフィルタ (`MIN_BUY_PRICE`, `MAX_BUY_PRICE`) を撤廃 — テーブル範囲のみ
- 校正テーブルを全データ (2024-12 〜 2026-02) で再構築
- 0.20-0.25 帯を追加（勝率 71.1%, ROI 30.3%）
- 直近1000試合バックテスト実施: 勝率 93.7%, モデル P&L +$555K
- DCA・MERGE の影響分析: WIN でも赤字になるメカニズムを解明

### Phase 3: Per-game スケジューラー — **完了**
- 試合別 SQLite ジョブキュー (`trade_jobs` テーブル)
- cron 駆動ステートマシン (`pending → executing → executed/skipped/failed/expired`)
- `scripts/schedule_trades.py` を主エントリとして `scan.py` を手動バックアップに降格
- 二重発注防止 5 層ガード
- `--execution dry-run|paper|live` モード切替

### Phase 3a: 流動性対応ポジションサイジング — **完了**
- CLOB 注文板から流動性スナップショット抽出 (`src/sizing/liquidity.py`)
- 3 層制約サイジング: Kelly × 残高リスク % × 流動性フィル % (`src/sizing/position_sizer.py`)
- スプレッド上限ガード (`MAX_SPREAD_PCT`)
- `CHECK_LIQUIDITY=true` で流動性チェック有効化

### Phase 3b: DCA / TWAP 適応実行 — **完了**
lhtsports データで DCA の有効性が確認済み:
- **DCA 勝率 97.4%** vs シングル 66.0% (勝率 +31pt)
- **DCA 絶対 P&L +$468K** vs シングル +$10K (47倍)
- ただし **ROI は 4.8% vs 30.0%** に圧縮

実装内容:
- Pre-sized budget 方式: 初回に全体サイズ決定、1/N 均等分割 (`src/strategy/dca_strategy.py`)
- 適応 TWAP: 時間トリガー + 価格トリガー (favorable/unfavorable) の 2 軸判定
- DCA パラメータ: max 5 回、min 間隔 2 分、price spread 上限 0.15
- ティップオフ 30 分前でカットオフ
- `dca_active` → `executed` ステートマシン拡張
- DCA グループ単位の VWAP ベース決済

### Phase B: Both-Side Betting — **完了**
sovereign2013 の分析により、同一試合の directional + hedge の両サイド購入戦略を導入。

実装内容:
- `scan_calibration_bothside()`: 両アウトカムの EV 判定、hedge 側は Kelly 乗数 0.5x
- `bothside_group_id` で directional/hedge ペアをリンク
- hedge ジョブ自動作成 (directional 約定後、`BOTHSIDE_HEDGE_DELAY_MIN` 後に発注可能)
- combined VWAP ガード (`BOTHSIDE_MAX_COMBINED_VWAP < 0.995`)
- hedge 側も独立 DCA グループで TWAP 実行
- bothside 一括決済 (directional PnL + hedge PnL)

### Phase B2: MERGE (CTF mergePositions) — **完了**
sovereign2013 の MERGE データ:
- 19,015 MERGE 操作 / 656K 取引
- Combined VWAP 中央値: 0.9843 → 1.6¢/pair の即時利益
- MERGE レグ: 全 PnL の 46.5% ($586K)

実装内容:
- CTF コントラクト `mergePositions` で YES+NO トークンペアを即座に 1 USDC に変換 (`src/connectors/ctf.py`)
- Web3.py で直接コントラクト呼び出し (py-clob-client は MERGE 非対応)
- Post-DCA 一括 MERGE: 両サイド DCA 完了後にトリガー (gas 1 回)
- Partial MERGE: `min(dir_shares, hedge_shares)` ペアのみ。残余は通常決済
- MERGE 判定純関数 (`src/strategy/merge_strategy.py`): shares 計算, VWAP, ガード
- Paper mode シミュレーション (Web3 不要)
- EOA ウォレット (signature_type=0) のみ対応
- `merge_operations` テーブル + `trade_jobs.merge_status` 追跡
- MERGE PnL 決済: merge_pnl (即時利益) + remainder_pnl (残余の勝敗) の分離計算

### Phase R: コードベースリファクタリング — **完了**

500行ガイドラインを超過していた3ファイルを段階的に分割。ロジック変更なし (移動のみ)。

- **R1**: `scripts/settle.py` (851行) → `src/settlement/pnl_calc.py` + `src/settlement/settler.py` + CLI (~120行)
  - module→script 逆転依存 (`schedule_trades.py` → `scripts/settle.py`) を解消
- **R2**: `src/store/db.py` (1354行) → `src/store/models.py` + `src/store/schema.py` + クエリ (~500行)
  - `JobStatus(StrEnum)` 新設、re-export で後方互換維持
- **R3**: `src/scheduler/trade_scheduler.py` (1386行) → `job_executor.py` + `hedge_executor.py` + `dca_executor.py` + `merge_executor.py` + ディスパッチャ (~300行)
- **R4**: VWAP 計算統合 — `dca_strategy.calculate_vwap_from_pairs()` に一元化
  - `merge_strategy.calculate_combined_vwap()` とインライン VWAP を置換
- **R5**: `src/analysis/pnl.py` (736行) → `src/analysis/report_generator.py` + 計算 (~355行)

---

### Phase B3: POLY_PROXY (Gnosis Safe) MERGE 対応 — **完了**
Phase B2 MERGE は EOA のみだったが、POLY_PROXY (1-of-1 Gnosis Safe) ウォレットでの MERGE に対応。

実装内容:
- Safe `execTransaction()` 経由で CTF `mergePositions` を間接呼び出し (`src/connectors/safe_tx.py`)
- `validate_safe_config()`: 1-of-1 検証 (VERSION, threshold, owner チェック)
- Safe の YES/NO トークン残高事前チェック
- `safeTxGas=0`: 内部失敗時に全体 revert + nonce 保全
- EIP-712 直接署名 (`unsafe_sign_hash`, v+=4 不要)
- `merge_positions_via_safe()`: Safe 経由の MERGE 実行パス
- `merge_executor.py`: POLY_PROXY 判定 + Safe/EOA ディスパッチ
- `should_merge()` に `is_supported_wallet` パラメータ追加 (後方互換)
- スコープ: 1-of-1 Safe のみ。マルチシグ・Magic Link は未対応

---

### Phase F1: Bothside + MERGE デフォルト有効化 — **完了**
2/10 の 4 試合全敗 (-$450, DD 35%) を受け、`BOTHSIDE_ENABLED` と `MERGE_ENABLED` のデフォルトを `True` に変更。
sovereign2013 の利益の 46.5% が MERGE であり、bothside + MERGE がこのシステムの利益の核心。設定忘れ防止のためデフォルト有効化。

実装内容:
- `src/config.py`: `bothside_enabled` と `merge_enabled` のデフォルトを `True` に変更
- `src/scheduler/hedge_executor.py`: `_schedule_hedge_job()` から未使用引数 `directional_dca_group_id` を削除
- `src/scheduler/trade_scheduler.py`: 呼び出し側の空文字列引数を削除
- `.env.example`: デフォルト値を `true` に更新

---

## 今後のフェーズ (未着手)

### Phase C: Total (Over/Under) マーケット対応
- Moneyline に加え、Total (Over/Under) マーケットの校正テーブル構築
- lhtsports 分析で ML+Total の独立エッジ (phi 相関 0.05) を確認済み
- ROI 2.3% → 10.0% への改善ポテンシャル
- Polymarket の Total マーケット構造の調査が前提

### Phase D: リスク管理 + インフラ強化 ✅ 完了
- 3段階サーキットブレーカー (GREEN/YELLOW/ORANGE/RED) + DB 永続化
- 校正ドリフト検出 (バンドごとの rolling 勝率 vs テーブル期待勝率)
- 段階的復帰メカニズム (sizing_multiplier 0.1x → 1.0x)
- DB インデックス + WAL モード + 冪等性ガード
- 構造化ログ (JSON) + 3 階層ヘルスチェック + Telegram アラート強化
- 延期試合・OT 検出 + 例外ハンドリング具体化

### Phase E: スケール + 本番運用
- 資金規模 $30-50K へのスケール
- Hot/Cold ウォレット分離
- bookmaker クロスバリデーション (高確信シグナルの追加フィルタ)
- Rolling MERGE 移行検討 (DCA 回数増加時の資本効率)
- インフラ: AWS Secrets Manager、監視アラート

---

## リスク管理パラメータ

| パラメータ | 現在 (B2 完了) | Phase D (リスク管理) | Phase E (スケール) |
|-----------|---------------|---------------------|-------------------|
| Kelly分数 | 0.25 | 0.25 | 0.25 |
| 1取引最大額 | $25 | $500 | $2,000 |
| DCA 上限回数 | 5 | 5-10回 | 10回 |
| 日次最大ポジション | 5 | 10 | 20 |
| 日次最大エクスポージャー | $375 | $2,000 | $10,000 |
| 残高リスク % | 2% | 3% | 5% |
| 流動性フィル % | 10% | 10% | 15% |
| Bothside enabled | true | true | true |
| MERGE enabled | true | true | true |
| 日次損失上限 | 3% (未使用) | 3% (ORANGE) | 3% |
| 週次損失上限 | — (未実装) | 5% (RED) | 5% |
| 最大ドローダウン | — (未実装) | 15% (RED) | 15% |
| 連敗縮小トリガー | — (未実装) | 5連敗→YELLOW | 5連敗→YELLOW |
| 校正ドリフト閾値 | — (未実装) | 2σ (ORANGE) | 2σ |
| 最大同時エクスポージャー | — (未実装) | 30% | 30% |

### サーキットブレーカー (Phase D 実装済み)
- **GREEN**: 通常取引 (sizing_multiplier=1.0)
- **YELLOW**: 日次損失50%到達 or 連敗≥5 → サイズ0.5x、新規DCA停止
- **ORANGE**: 日次損失上限到達 or 校正ドリフト検出 → 全取引停止、24h後に自動YELLOW降格条件あり
- **RED**: 週次損失≥5% or ドローダウン≥15% → 緊急停止、72時間ロック (手動解除のみ)

### 段階的復帰
- RED → ORANGE: 手動解除、72h+ 経過必須
- ORANGE → YELLOW: 24h後 + 直近5決済の勝率≥60%
- YELLOW → GREEN: 3日連続黒字

---

## 本番移行の条件 (Phase E 前提)

- [ ] ペーパートレード 100 取引以上完了
- [ ] 勝率 > 55%
- [ ] シャープレシオ > 1.5
- [ ] 最大ドローダウン < 10%
- [ ] 小額実弾テスト ($25-50) で 50 取引以上
- [ ] サーキットブレーカー実装済み (Phase D)
- [ ] Hot/Cold ウォレット分離済み

---

## 日次ワークフロー (JST)

スケジューラー (`schedule_trades.py`) が 2 分間隔 cron で全自動実行:

| cron tick | 処理内容 |
|-----------|---------|
| refresh_schedule | NBA.com → trade_jobs にゲーム UPSERT |
| cancel_expired | ティップオフ後の未処理ジョブを expire |
| process_eligible | 発注窓内の pending ジョブを処理 (directional + hedge 作成) |
| process_dca_active | DCA 追加購入 (時間/価格トリガー) |
| process_merge_eligible | dir+hedge 完了後の MERGE 実行 |
| auto_settle | 試合終了後の PnL 決済 (DCA/bothside/MERGE 対応) |
| Telegram | サマリー通知 |

手動バックアップ: `scripts/scan.py` + `scripts/settle.py --auto`

---

## 技術スタック

- **言語**: Python 3.11+
- **Polymarket**: Gamma Events API (主) / `py-clob-client` (フォールバック)
- **CTF コントラクト**: Web3.py >= 6.0 (MERGE 用 — Polygon PoS)
- **ゲーム発見**: NBA.com Scoreboard API (Odds API は bookmaker モードのみ)
- **DB**: SQLite (`data/paper_trades.db`)
- **通知**: Telegram Bot API
- **CI**: pytest (401 tests) + ruff

---

## ウォレットセキュリティ
- **Cold Wallet (70%)**: ハードウェアウォレット、手動のみ
- **Hot Wallet (30%)**: BOT用、取引上限付き
- 秘密鍵は `.env` → 本番では AWS Secrets Manager
- Polygon上のUSDC、ガス代は$2-5のMATICで数千取引分

---

*本文書は戦略設計書であり、投資助言ではない。予測市場での取引には資金全額損失リスクがある。*
