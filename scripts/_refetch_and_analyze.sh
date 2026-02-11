#!/bin/bash
set -e
VENV="/Users/taro/dev/nbabot/.venv/bin/python"
cd /Users/taro/dev/nbabot

# 再取得が必要な8名 (小さい順)
TRADERS=(
  beachboy4
  Theo4
  MrSparklySimpsons
  gmpm
  kch123
  DrPufferfish
  GamblingIsAllYouNeed
  sovereign2013
)

echo "=== Phase 1: Re-fetch (${#TRADERS[@]} traders) ==="
for t in "${TRADERS[@]}"; do
  echo ""
  echo ">>> Fetching $t ..."
  $VENV scripts/fetch_trader.py --username "$t" 2>&1
  echo "<<< Done: $t"
done

echo ""
echo "=== Phase 2: Re-analyze ALL ==="
$VENV scripts/analyze_trader.py --all 2>&1

echo ""
echo "=== Phase 3: Comparison report ==="
$VENV scripts/compare_traders.py --sort-by sharpe 2>&1

echo ""
echo "=== Phase 4: PnL validation ==="
$VENV -c "
import json, sys
sys.path.insert(0, '.')
from pathlib import Path
from src.analysis.pnl import build_condition_pnl

reg = json.load(open('data/traders/registry.json'))
reg_map = {r.get('username','').lower(): r for r in reg}

print(f\"{'Trader':<22} {'Calc PnL':>14} {'LB PnL':>14} {'Ratio':>8} {'Diff%':>8} {'Missing':>8} {'Status'}\")
print('-' * 90)

for td in sorted(Path('data/traders').iterdir()):
    if not td.is_dir() or td.name.startswith('_'):
        continue
    tp = td / 'raw_trade.json'
    rp = td / 'raw_redeem.json'
    mp = td / 'raw_merge.json'
    if not tp.exists():
        continue
    trades = json.load(open(tp))
    redeems = json.load(open(rp)) if rp.exists() else []
    merges = json.load(open(mp)) if mp.exists() else []
    conditions = build_condition_pnl(trades, redeems, merges)
    
    calc = sum(c['pnl'] for c in conditions.values())
    missing = sum(1 for c in conditions.values() if c.get('data_quality') == 'missing_trades')
    lb = reg_map.get(td.name.lower(), {}).get('pnl', 0)
    
    if lb:
        ratio = calc / lb if lb else 0
        diff = abs(calc - lb) / max(abs(lb), 1) * 100
        flag = 'OK' if diff < 20 else 'MISMATCH'
    else:
        ratio = 0; diff = 0; flag = 'NO_LB'
    
    print(f'{td.name:<22} \${calc:>12,.0f}  \${lb:>12,.0f}  {ratio:>6.2f}x  {diff:>6.1f}%  {missing:>6}  {flag}')
"

echo ""
echo "=== ALL DONE ==="
