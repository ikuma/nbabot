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
| 0.55-0.60 | 141 | 95.7% | 4.0% | high | CI-based (旧 0.5x) |
| 0.60-0.65 | 78 | 97.4% | 16.0% | medium | CI-based (旧 0.5x) |
| 0.65-0.70 | 58 | 93.1% | 2.1% | medium | CI-based (旧 0.5x) |
| 0.70-0.75 | 45 | 93.3% | 15.5% | medium | CI-based (旧 0.5x) |
| 0.75-0.80 | 37 | 97.3% | 15.9% | low | CI-based (旧 0.5x) |
| 0.80-0.85 | 33 | 100% | 17.4% | low | CI-based (旧 0.5x) |
| 0.85-0.90 | 30 | 100% | 14.1% | low | CI-based (旧 0.5x) |
| 0.90-0.95 | 22 | 100% | 8.8% | low | CI-based (旧 0.5x) |

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

### Phase L: LLM ベース試合分析 — **完了**
LLM (Anthropic Claude) が試合の方向性を判断し、校正テーブルでベット金額を決める。

**アーキテクチャ**: 方式 B (3 ペルソナ独立分析 + シンセシス統合)
- Expert 1: Polymarket 凄腕トレーダー
- Expert 2: ヘッジファンド天才クオンツ
- Expert 3: リスク管理担当
- シンセシス: 3 専門家の分析を統合し最終判断

**実装内容**:
- `src/connectors/nba_data.py`: ESPN API からチーム成績・怪我・B2B 情報を収集
- `src/strategy/prompts/game_analysis.py`: 4 つのプロンプト定義 (3 ペルソナ + シンセシス)
- `src/strategy/llm_analyzer.py`: `asyncio.gather()` で 3 ペルソナ並列呼び出し → シンセシス → `GameAnalysis`
- `src/strategy/llm_cache.py`: SQLite キャッシュ (event_slug 単位、DCA/hedge で再利用)
- `job_executor.py`: LLM-First directional 決定 + `sizing_modifier` 適用
- `hedge_executor.py`: LLM の `hedge_ratio` を Kelly 乗数に適用

**キーチェンジ**: 従来の `scan_calibration_bothside()` は「最高 EV 側を directional に選ぶ」だったが、新方式では **LLM が directional を決め**、校正テーブルはサイジングのみに使用。

**フォールバック**: `LLM_ANALYSIS_ENABLED=false` (デフォルト) または API 障害時は従来パイプラインに自動フォールバック。

**コスト**: Opus 4.6 で ~$72/月 (10 試合/日)。`LLM_MODEL` で Sonnet ($14/月) や Haiku ($5/月) に切替可能。

---

### Phase W: launchd 移行 + 死活監視 — **完了**

crontab (15分間隔) から macOS ネイティブの launchd に移行。スリープ復帰後の実行保証を強化。

**実装内容:**
- `launchd/com.nbabot.scheduler.plist`: `StartInterval: 900` (15分) — スリープ復帰時に launchd が自動で 1 回実行
- `launchd/com.nbabot.watchdog.plist`: `StartInterval: 600` (10分) — 独立した死活監視ジョブ
- `scripts/watchdog.py`: `data/heartbeat` の mtime を監視、35分超過で Telegram アラート、復旧通知付き
- `scripts/install_launchd.sh`: 冪等インストーラー (bootout → コピー → bootstrap → crontab クリーンアップ)
- `schedule_trades.py`: main() 末尾にハートビート書き出し (3行追加)

**設計判断:**
- `cron_schedule.sh` をそのまま launchd から呼ぶ (ロック・caffeinate・ログローテーションは既存のまま)
- watchdog は DB アクセスなし (ファイル mtime のみ) → SQLite ロック競合ゼロ
- アラートフラグファイル (`data/.watchdog_alerted`) で連続送信防止

---

### Phase L2: LLM-First Directional + Below-Market Limit — **完了**

Phase L の LLM 分析を実戦投入可能な発注戦略に統合。3 つの改善:

**1. LLM-First Directional**
- LLM が directional (favored_team) を決定、校正テーブルは EV 安全弁のみ
- Case A: hedge 存在 → 校正 directional/hedge を swap
- Case B: hedge=None → `evaluate_single_outcome()` で LLM 側を独立評価
- LLM 側にバンドなし or EV 非正 → 校正維持 (安全弁)

**2. Below-Market Limit Orders**
- 全注文を `best_ask - 0.01` で発注 (メイカー注文)
- 合計 < 1.0 が自然に成立 → MERGE 利益が生まれる
- fill は保証されないが NBA 価格変動で高確率

**3. Hedge Target Pricing**
- `max_hedge = min(hedge_max_price, target_combined - dir_vwap)`
- `BOTHSIDE_TARGET_COMBINED=0.97` → MERGE 利鞘 3%/share
- hedge は「フリーオプション」: fill しなくても directional だけで +EV

**変更ファイル:**
- `src/config.py`: `BOTHSIDE_TARGET_COMBINED` 追加
- `src/strategy/calibration_scanner.py`: `evaluate_single_outcome()` 公開関数追加
- `src/scheduler/job_executor.py`: LLM-First Case B + below-market pricing
- `src/scheduler/trade_scheduler.py`: hedge ジョブ常時作成
- `src/scheduler/hedge_executor.py`: target-based below-market limit + 注文板取得
- `src/scheduler/dca_executor.py`: below-market pricing + hedge combined フィルター
- `tests/test_llm_override.py`: 18 テスト

---

### Phase L-cache: LLM プロンプトキャッシング — **完了**

3 ペルソナ並列呼び出しで共通のナレッジベース (~4K+ tokens) を Anthropic Prompt Caching でキャッシュし、2 回目以降の呼び出しでトークン消費を ~60% 削減。

**実装内容:**
- `src/strategy/prompts/game_analysis.py`: `SHARED_KNOWLEDGE_BASE` 定数追加 — NBA 統計予測因子、予測市場バイアス、確率推定ガイドライン、分析プロトコルの 4 セクション構成
- `src/strategy/llm_analyzer.py`: `_call_llm()` を構造化システムメッセージに変更 — `cache_control: {"type": "ephemeral"}` 付きナレッジベースブロック + ペルソナ固有指示ブロック
- キャッシュ使用状況の debug ログ (`cache_read`, `cache_creation`, `input_tokens`)
- `tests/test_llm_analyzer.py`: `TestPromptCaching` クラス — トークン長検証、機密情報リーク防止、構造化システムメッセージ検証

**設計判断:**
- ナレッジベースは 4096+ tokens (Opus 4.6/4.5/Haiku 4.5 の最小キャッシュ閾値)
- 校正テーブル・Kelly 分数などの戦略パラメータは含めない (LLM への情報漏洩防止)
- 5 分 TTL で同一試合の 3 ペルソナ + シンセシス呼び出しをカバー
- コスト削減: Opus 4.6 で ~$72/月 → ~$30/月 (ナレッジベース分のキャッシュヒット)

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
- **CI**: pytest (537 tests) + ruff

---

## Phase N: Telegram 通知強化

### 概要
Telegram 通知を「いつ・何を・いくらで・なぜ」がリアルタイムにわかるレベルに強化。

### 実装内容
1. **即時通知**: 各 executor (job/hedge/dca/merge) が発注成功時に即座に Telegram 通知
2. **通知関数**: `notify_trade()`, `notify_hedge()`, `notify_dca()`, `notify_merge()` + `escape_md()`
3. **Tick summary enrichment**: `format_tick_summary()` が DB 参照でチーム名・価格・エッジ・DCA 進捗を表示
4. **決済通知拡充**: `SettleResult` にスコア・ROI を追加、`format_summary()` で表示
5. **Preflight 分離**: `_preflight_check()` を `src/scheduler/preflight.py` に分離 (500行対策)
6. **DB ヘルパー**: `get_signal_by_id()` 追加

### 通知フォーマット例
```
*Trade: BUY New York Knicks*
nba-nyk-bos-2026-02-11
@ 0.370 (ask 0.380) | $42 | edge 26.1%
Band: 0.35-0.40 [SWEET] | WR: 90.4% | DCA 1/5
LLM: NYK conf 0.72 sizing 1.2x
```

### 設計方針
- JobResult は不変、executor 層で即時通知
- 全通知は try/except で wrap、失敗しても処理に影響なし
- Markdown V1 エスケープ (`escape_md()`) で Telegram パースエラー防止

---

## Phase P: Per-Signal P&L 修正 ✅ 完了

### 背景
settler.py で DCA グループ処理が bothside/MERGE 処理より先に走り、merge 回収分が反映されない重大バグを発見。
IND@BKN (2/11) で実質 P&L -$26.42 が DB に -$77.11 と記録されていた ($50.69 の過少計上)。

### 修正方針
各シグナルが自分の P&L を自己完結で計算できるようにする。merge_executor が merge 時に per-signal の回収額を保存し、settler は単純な数式で P&L を算出。

### 核心: `calc_signal_pnl()` 関数
```python
pnl = (remaining_shares × settlement_price) + merge_recovery_usd - cost
```
- merge なし (shares_merged=0) → 従来の `_calc_pnl` と同一
- merge あり → remaining_shares × $1/$0 + 回収額 - コスト

### 実装内容
1. **Schema**: `signals` テーブルに `shares_merged`, `merge_recovery_usd` カラム追加 + backfill マイグレーション
2. **Models**: `SignalRecord` に 2 フィールド追加
3. **DB**: `update_signal_merge_data()` 関数追加
4. **merge_executor**: merge 成功後に per-signal merge データを更新
5. **pnl_calc**: `calc_signal_pnl()` 関数追加 (既存関数は温存)
6. **settler**: `auto_settle()` を per-signal settlement に簡素化。グループ/MERGE 分岐を廃止し、全シグナルを均一に `calc_signal_pnl()` で処理
7. **テスト**: `test_calc_signal_pnl.py` — 11 テスト (no-merge, partial merge, full merge, DCA+merge 統合)

---

## Phase M: 指標定義と検証設計の監査可能化 ✅ 完了

### 背景
「勝率」の定義が複数箇所で混在し、校正テーブルの検証に forward-looking bias があった。取引費用も P&L に完全反映されていなかった。これらを修正し、「本当に優位性があるか」を誤認しない監査可能な基盤を構築。

### Phase M1: 指標分解 — 完了
3つの独立した指標を定義:
- **試合的中率** (game_correct_rate): 試合の勝者を的中した割合 (`results.won`)
- **損益正率** (trade_profit_rate): P&L > 0 で終わった取引の割合 (`results.pnl > 0`)
- **MERGE 決済率** (merge_rate): MERGE で部分/全額回収された割合 (`signals.shares_merged > 0`)

実装:
- `src/analysis/metrics.py` (新規): `DecomposedMetrics` + `compute_decomposed_metrics()` + `format_decomposed_summary()`
- `src/store/db.py`: `get_band_decomposed_stats()`, `get_results_with_signals()` 追加
- `src/settlement/settler.py`: `AutoSettleSummary` に `profit_wins`, `profit_losses`, `merged_count` 追加
- `src/analysis/report_generator.py`: Decomposed Metrics セクション追加
- `src/risk/calibration_monitor.py`: `trade_profit` z-score 監視追加
- `tests/test_metrics.py`: 8 テスト

### Phase M2: 時系列分離バックテスト — 完了
全データでテーブル構築→同データでバックテストの in-sample bias を排除するため、walk-forward 分離を実装。

実装:
- `src/strategy/calibration_builder.py` (新規): `build_calibration_from_conditions()`, `walk_forward_split()`, `evaluate_split()`
- `src/strategy/calibration.py`: `load_calibration_table()` 追加 (JSON ファイル or ハードコードフォールバック)
- `scripts/rebuild_calibration_and_backtest.py`: `--split` モード追加 (walk-forward 検証)
- `tests/test_calibration_builder.py`: 16 テスト

### Phase M3: 取引費用の計上 — 完了
現在は maker fee=0 で P&L 計算は実質正確だが、「fee=0 であることを検証可能にする監査証跡」を構築。

実装:
- `src/store/schema.py`: `fee_rate_bps`, `fee_usd` カラム追加
- `src/store/db.py`: `update_signal_fee()` 追加
- `src/store/models.py`: `SignalRecord` に `fee_rate_bps`, `fee_usd` フィールド追加
- `src/settlement/pnl_calc.py`: `calc_signal_pnl()` に `fee_usd` パラメータ追加
- `src/connectors/ctf.py`: `get_matic_usd_price()` (CoinGecko + フォールバック)
- `src/connectors/polymarket.py`: `extract_fee_rate_bps()` ヘルパー追加
- `src/scheduler/job_executor.py`, `hedge_executor.py`, `dca_executor.py`: fee 記録呼び出し追加
- `tests/test_calc_signal_pnl.py`: fee テスト 5 件追加
- `tests/test_fee_accounting.py`: 6 テスト

### Phase S: 期待P&L vs 実現P&L トラッカー — 完了
校正テーブルの期待 EV と実現 P&L の乖離を月次/週次で追跡し、エッジの減衰を検出。

実装:
- `src/analysis/expectation_tracker.py` (新規): `ExpectationGap` + `compute_expectation_gaps()` + `format_expectation_report()`
- `src/analysis/report_generator.py`: Expected vs Realized PnL セクション追加 (乖離拡大時に警告表示)
- `tests/test_expectation_tracker.py`: 15 テスト

### Phase Q: 連続校正カーブ + 不確実性定量化 — 完了
離散 5 セントバンドの問題 (単調性違反、小サンプル過信、バンド境界の不連続性、不確実性無視) を解決。
Isotonic Regression (PAVA) + PCHIP 補間 + Beta 事後分布で連続・単調・保守的な price→win_rate 関数を構築。

実装:
- `src/strategy/calibration_curve.py` (新規): `ContinuousCalibration` + `WinRateEstimate` + `get_default_curve()`
- `src/strategy/calibration_scanner.py`: `lookup_band()` → `curve.estimate()` に切替 (3 関数)
- `src/scheduler/hedge_executor.py`: hedge EV 再検証を連続カーブに切替
- `src/strategy/calibration_builder.py`: `build_continuous_from_conditions()` + `evaluate_split_continuous()` 追加
- `src/risk/calibration_monitor.py`: `compute_continuous_drift()` 追加
- `src/config.py`: `calibration_confidence_level` (default 0.90) 追加
- `scripts/rebuild_calibration_and_backtest.py`: `--continuous` フラグ追加
- `pyproject.toml`: `scipy>=1.12` 追加
- `tests/test_calibration_curve.py`: 30 テスト

### Phase Q2: 保守的サイジング改革 (連続不確実性ベース) — 完了
Phase Q の連続校正カーブを活用し、2 つの残存問題を解決:

1. **固定スイートスポット境界の撤廃**: 価格 0.55 境界の `if not sweet: kelly *= 0.5` を、連続的な `_confidence_multiplier(est)` に置換。CI 幅 (lower_bound / point_estimate) で [0.5, 1.0] の乗数を算出。高勝率・高サンプルバンドの不当な過小サイジングを修正。`in_sweet_spot` はメタデータとして温存。
2. **DCA 未約定エクスポージャーの計上**: `get_pending_dca_exposure()` で dca_active ジョブの残りスライスを潜在エクスポージャーとして集計。preflight チェックで placed + pending DCA の合算で上限判定。

実装:
- `src/strategy/calibration_scanner.py`: `_confidence_multiplier()` 追加 + 3 関数の sweet spot ロジック置換
- `src/store/db.py`: `get_pending_dca_exposure()` 追加
- `src/scheduler/preflight.py`: pending DCA exposure 加算
- `tests/test_calibration_scanner.py`: `TestConfidenceMultiplier` 追加 + sizing テスト更新
- `tests/test_preflight.py`: pending DCA exposure テスト (新規)

---

## Phase O: 注文実行改善 (Order Lifecycle Manager) ✅

**目的**: 注文の寿命管理 + 短周期監視 → 約定率改善。best_ask が動いた場合に注文が取り残されるのを防ぐ。

**設計**: 2 分間隔の独立 order manager プロセス (launchd)。既存の 15 分戦略 tick とは独立して注文監視。

| プロセス | 間隔 | 役割 | ロック |
|----------|------|------|--------|
| scheduler | 900s | 戦略判断 + 発注 | `/tmp/nbabot-scheduler.lock` |
| order-manager | 120s | 注文監視 + cancel/re-place | `/tmp/nbabot-ordermgr.lock` |
| watchdog | 600s | 死活監視 | なし |

**ロジック**:
1. `get_active_placed_orders()` で未約定注文を取得
2. CLOB API で fill 検出 → DB 更新 + 通知
3. TTL 超過 (`ORDER_TTL_MIN=5`) → best_ask 取得 → cancel + re-place at `best_ask - 0.01`
4. `ORDER_MAX_REPLACES=3` 超過 → cancel + expired
5. ティップオフ過ぎ → cancel + expired
6. hedge は `target_combined` 制約を再チェック

**DB 変更**:
- signals: `order_placed_at`, `order_replace_count`, `order_last_checked_at`, `order_original_price`
- `order_events` テーブル (将来の約定確率モデルの学習データにもなる)

**settler 統合**: `_refresh_order_statuses()` が `order_manager.check_and_manage_orders()` に委譲。order_manager launchd 停止時も settler で最低限の fill 検出がフォールバック。

実装:
- `src/scheduler/order_manager.py`: 注文ライフサイクル管理コアロジック (~290 行)
- `src/store/schema.py`: signals に 4 カラム + `order_events` テーブル
- `src/store/models.py`: `SignalRecord` 新フィールド + `OrderEvent` dataclass
- `src/store/db.py`: `get_active_placed_orders()`, `log_order_event()`, `update_order_lifecycle()`, `get_order_events()`
- `src/config.py`: 6 設定パラメータ追加
- `src/connectors/polymarket.py`: `cancel_and_replace_order()` 追加
- `src/notifications/telegram.py`: `notify_order_replaced()`, `notify_order_filled_early()` 追加
- `src/settlement/settler.py`: `_refresh_order_statuses()` を order_manager に委譲 + legacy フォールバック
- `src/scheduler/job_executor.py`: 発注後に `order_placed_at`, `order_original_price` 記録
- `src/scheduler/hedge_executor.py`: 同上
- `src/scheduler/dca_executor.py`: 同上
- `scripts/order_tick.py`: launchd エントリポイント
- `scripts/cron_ordermgr.sh`: bash ラッパー (ロック + caffeinate)
- `launchd/com.nbabot.ordermgr.plist`: 120s 間隔 launchd ジョブ
- `scripts/install_launchd.sh`: 3 ジョブインストール対応
- `tests/test_order_manager.py`: 16 テスト

---

## Phase DCA2: 目標保有量方式 DCA (Target-Holding DCA) ✅

**目的**: 等分割 TWAP → Adaptive IS/Target Rebalancing への進化。価格が有利な時に厚く、不利な時に薄く (または停止) を自然に実現。

**背景**: 旧 DCA は `slice_size = total_budget / max_entries` の等分割。価格が下がっても上がっても同額を投入するため、有利な価格での積み増し機会を逃していた。

**設計 — Mark-to-Market Gap Fill**:
```
total_shares   = sum(cost / buy_price  for each existing entry)
current_value  = total_shares * current_price          # 時価評価
raw_gap        = max(0, total_budget - current_value)  # 目標との乖離
remaining_budget = total_budget - total_cost            # 未消化予算
remaining_entries = max(1, max_entries - entries_done)
per_entry_cap  = (remaining_budget / remaining_entries) * cap_mult
order_size     = min(raw_gap, remaining_budget, per_entry_cap)
```

**完了条件 (拡張)**:
- `max_entries` 到達 (従来通り)
- `budget_exhausted`: remaining_budget < min_order_usd
- `target_reached`: raw_gap < min_order_usd

**設定**:
- `DCA_PER_ENTRY_CAP_MULT=2.0` — 1 回の発注上限 = 残余予算の等分 × この倍率
- `DCA_MIN_ORDER_USD=1.0` — 最小発注額。これ未満はスキップ

**実装**:
- `src/sizing/position_sizer.py`: `TargetOrderResult` + `calculate_target_order_size()` 純関数
- `src/scheduler/dca_executor.py`: 固定 slice → target-holding 動的サイジング
- `src/store/db.py`: `get_pending_dca_exposure()` を signals 実績ベースに更新
- `src/config.py`: `dca_per_entry_cap_mult`, `dca_min_order_usd` 追加
- `tests/test_position_sizer.py`: 9 テスト追加
- `tests/test_dca_db.py`: 3 テスト追加

**旧データ互換性**: `dca_total_budget` が NULL の旧ジョブは equal-split フォールバック。

---

## Phase H: MERGE-First Hedge 改革 ✅

**目的**: MERGE を主戦略に昇格。hedge は「条件が合えば追加するオプション」ではなく「常に試みる。問題は指値の価格だけ」に。

**背景**: sovereign2013 の利益の 46.5% が MERGE。にもかかわらず旧設計ではスキャナーの静的ガード (`ev > 0`, `price <= 0.55`, `combined < 0.995`) が多くの MERGE 機会を潰していた。`BOTHSIDE_TARGET_COMBINED = 0.97` が固定で MERGE 利鞘 3% を要求 → 実際の gas+手数料はほぼゼロなのに保守的すぎた。

**設計原則**:
1. **常に hedge を試みる** — scanner は combined < max_combined_vwap の安全弁のみ
2. **MERGE 経済性から限界価格を導出** — `max_hedge = 1.0 - dir_vwap - min_margin`
3. **指値は注文板ベースで動的** — `best_ask - 0.01` を上限に収める
4. **fill しなくても OK** — directional 単体で +EV、hedge は「フリーオプション」

**変更一覧**:
- `calibration_scanner.py`: `_hedge_margin_multiplier()` 追加、hedge ガード簡素化 (EV/price ガード削除、動的乗数)
- `hedge_executor.py`: MERGE 経済性ベースの動的限界価格、EV チェック緩和 (MERGE-only パス)、コストベースサイジング
- `dca_executor.py`: 同じ動的限界価格適用
- `job_executor.py`: `hedge_max_price` 引数削除
- `config.py`: `merge_est_gas_usd`, `merge_min_shares_floor` 追加。`bothside_target_combined`, `bothside_hedge_max_price` を DEPRECATED
- テスト: 11 テスト (既存更新 + 新規 4 件)

**設定**:
- `MERGE_EST_GAS_USD=0.05` — MERGE gas 見積もり (Polygon, 保守的)
- `MERGE_MIN_SHARES_FLOOR=20.0` — 最小想定 shares (安全弁)
- `BOTHSIDE_TARGET_COMBINED` — DEPRECATED (executor が動的算出)
- `BOTHSIDE_HEDGE_MAX_PRICE` — DEPRECATED (scanner が常に hedge を返す)

---

## ウォレットセキュリティ
- **Cold Wallet (70%)**: ハードウェアウォレット、手動のみ
- **Hot Wallet (30%)**: BOT用、取引上限付き
- 秘密鍵は `.env` → 本番では AWS Secrets Manager
- Polygon上のUSDC、ガス代は$2-5のMATICで数千取引分

---

*本文書は戦略設計書であり、投資助言ではない。予測市場での取引には資金全額損失リスクがある。*
