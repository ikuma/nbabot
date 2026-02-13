# AI Runbook

日常の開発・検証でエージェントが優先的に使うコマンド集。

## セットアップ

- Python: 3.11+
- 依存インストール: `pip install -e .`

## 日常コマンド

- テスト: `pytest`
- リント: `ruff check src/ scripts/`
- フォーマット: `ruff format src/ scripts/`

## 実行系コマンド

- スケジューラー: `python scripts/schedule_trades.py`
- スケジューラー dry-run: `python scripts/schedule_trades.py --execution dry-run`
- 未来日付 dry-run: `python scripts/schedule_trades.py --date 2026-02-10 --execution dry-run`
- 手動スキャン: `python scripts/scan.py`
- モード指定スキャン: `python scripts/scan.py --mode calibration|bookmaker|both`
- 自動決済: `python scripts/settle.py --auto`
- 自動決済 dry-run: `python scripts/settle.py --auto --dry-run`
- 未決済一覧: `python scripts/settle.py --list`
- 接続確認: `python scripts/check_balance.py`
- 実運用前ゲート(疎通+shadow+paper+最小回帰): `python scripts/pre_practice_gate.py`
- PositionGroup監査/違反率レポート: `python scripts/report_position_groups.py --db data/paper_trades.db`
- PositionGroup戦略比較バックテスト: `python scripts/run_position_group_backtest.py --input <dataset.json>`

## launchd

- インストール: `bash scripts/install_launchd.sh`
- watchdog 手動実行: `python scripts/watchdog.py`

## DB確認

- ジョブ確認: `sqlite3 data/paper_trades.db "SELECT * FROM trade_jobs"`

## 実行方針

- 変更に直接関係する最小テストを優先し、必要に応じて全体テストへ拡張する。
- テスト未実施の場合は、理由と推奨コマンドを必ず報告する。
