"""Quick status check for the running sweep. Run any time to see partial results."""
import sys, os
sys.path.insert(0, '.')
from pathlib import Path
import pandas as pd

csv = Path('outputs_v15_long_sweep/sweep_results.csv')
log = Path('outputs_v15_long_sweep/run_log.txt')

if not csv.exists():
    print("No results yet.")
    sys.exit()

df = pd.read_csv(csv)
total_run = 281

print(f"\n{'='*72}")
print(f"SWEEP STATUS: {len(df)}/{total_run} configs completed")
print(f"{'='*72}\n")

if df.empty:
    print("No results yet.")
    sys.exit()

# Show last 5 from log
if log.exists():
    lines = log.read_text(encoding='utf-8', errors='ignore').strip().splitlines()
    print("── Last 10 log lines ──")
    for l in lines[-10:]:
        print(f"  {l}")
    print()

# Sort by score
df_s = df.sort_values('score', ascending=False).reset_index(drop=True)
print("── TOP 15 by composite score ──")
cols = ['name','n_trades','win_pct','pf','net_rs','sharpe','score',
        'stop_pct','target_pct','adx_min','rsi_min','avwap_min','ema_min',
        'qs_min','lag_b_huge','win_end','entry_mode','rs_thr']
dc = [c for c in cols if c in df_s.columns]
top = df_s.head(15)[dc]
pd.set_option('display.width', 200)
pd.set_option('display.max_columns', 30)
print(top.to_string(index=True))

print()
min30 = df_s[df_s['n_trades'] >= 30]
print("── TOP 10 by PF (min 30 trades) ──")
print(min30.sort_values('pf', ascending=False).head(10)[dc].to_string(index=False))

print()
print("── TOP 10 by Net Rs. PnL (min 30 trades) ──")
print(min30.sort_values('net_rs', ascending=False).head(10)[dc].to_string(index=False))

if 'entry_mode' in df.columns:
    print("\n── Bar-close entry results ──")
    bc = df_s[df_s['entry_mode']=='bar_close']
    if len(bc):
        print(bc.head(10)[dc].to_string(index=False))
    else:
        print("  (Phase 2 not reached yet)")
