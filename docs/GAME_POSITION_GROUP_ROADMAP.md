# GamePositionGroup Roadmap

## Goal
- MERGE収益とDirectional残差収益を同時最適化する。
- 取引判断は `filled` 在庫を唯一の真実(source of truth)として行う。
- 先行脚固定を廃止し、在庫制約ベースで執行順を決める。

## Non-Negotiables
- `M*` (MERGE在庫目標) と `D*` (Directional残差目標) を分離して管理する。
- `|d| <= D_max(t)` を常に満たす（`d = q_dir - q_opp`）。
- MERGEは `edge_merge > threshold` の時のみ実行する。
- リスクエンジン ORANGE/RED 時は新規リスク増加を禁止する。

## Track A: Stabilization (Current System Hardening)
最初に現行実装の計測歪みを除去し、次段の設計変更が安全にできる土台を作る。

### A1. Preflight順序修正 (P0)
- [x] live時は `log_signal()` より前に `preflight_check()` を実行する（directional/hedge双方）。
- [x] preflight失敗時は `signals` を作らず `trade_jobs` のみ `failed` にする。
- [x] テスト追加:
  - [x] preflight失敗で signal未作成
  - [x] preflight失敗で `signal_id` なし `failed`

### A2. Tipoff後 placed 注文回収 (P1)
- [x] `get_active_placed_orders()` の時間窓フィルタを外し、Order Managerでexpire判定する。
- [x] tipoff後注文が `expired/cancelled` へ収束することをテストで担保する。

### A3. Hedge単独実行ガード (P1)
- [x] directional実体（filled在庫）が無い場合は hedge を実行しない。
- [x] liveは `pending` に戻す、paper/dry-runは `skipped` にする。

### A4. Retry上限設定化 (P2)
- [x] `retry_count < 3` のハードコードを撤廃し、`SCHEDULE_MAX_RETRIES` を参照する。

### A5. DCAのfilled在庫化 (P2)
- [x] live DCAは `filled` のみを保有計算に使う。
- [x] `placed` が残る間は追加DCAしない。

### A Done Criteria
- [x] `pytest -q` 緑
- [x] preflight失敗時の擬似PnL混入ゼロ
- [x] tipoff後 placed 注文の滞留ゼロ（Order Managerの観測で確認）

## Track B: First-Principles Redesign (GamePositionGroup)

## B1. Data Model
- [x] `position_groups` テーブル追加
  - columns: `id, event_slug, state, M_target, D_target, q_dir, q_opp, merged_qty, d_max, phase_time, created_at, updated_at`
- [x] `signals` は注文履歴として維持し、在庫計算はfilled集計で行う。

## B2. State Machine v1
- [x] 状態導入: `PLANNED, ACQUIRE, BALANCE, MERGE_LOOP, RESIDUAL_HOLD, EXIT, CLOSED, SAFE_STOP`
- [x] 遷移ガード導入:
  - `|d| > D_max(t)` で BALANCE
  - `m >= m_min` で MERGE_LOOP
  - 新規リスク増不可時間帯で RESIDUAL_HOLD

## B3. Sizing v1 (M*/D*分離)
- [x] `D*` を directional期待値から算出
  - `D* = B * λ * Kelly_low * u_conf * u_regime`
- [x] `M*` を mergeエッジと流動性から算出
  - `edge_merge = 1 - (vwap_dir + vwap_opp) - fee - gas - buffer`
- [x] 目標在庫:
  - `q_dir_target = M* + D*`
  - `q_opp_target = M*`
- [x] DCAは「総目標分割手段」としてのみ利用（回数で総リスクを増やさない）。

## B4. Execution Priority v1
- [x] 先行脚固定を廃止し、`utility(side)` 最大の側から執行する。
- [x] `utility = fill_prob * (merge_improve + alpha_improve) - slippage - inventory_penalty`

## B5. Risk & Controls
- [x] `D_max(t)` 逓減（tipoff接近で縮小）
- [x] leg2未成立タイムアウト
- [x] SAFE_STOP遷移（CB/異常時）

## B Done Criteria
- [ ] バックテストで「MERGE-only」「Directional-only」より合成戦略が優位
- [ ] 在庫逸脱 (`|d| > D_max`) の違反率が閾値未満
- [ ] 監査ログで `M*`,`D*`,`q_dir`,`q_opp`,`merge_amount` を追跡可能

## Suggested Delivery Order
1. Track Aを完了（現行挙動の歪みを先に解消）
2. B1+B2で状態機械の骨格だけ導入（既存執行関数を再利用）
3. B3でM*/D* sizing導入
4. B4で先行脚自由化
5. B5で運用ガード強化

## Rollback Strategy
- Track A/Bを機能フラグで分離する。
- `GAME_POSITION_GROUP_ENABLED=false` で現行フローへ即時切戻し可能にする。
