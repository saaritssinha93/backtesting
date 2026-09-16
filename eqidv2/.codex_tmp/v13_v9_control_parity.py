from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import json
import pandas as pd
import fno_v13_corrected_v5_backtest as v5
import fno_v13_v9_backtest as engine
import fno_v13_v9_research as research

root = research.DEFAULT_OUTPUT
parts, offset = [], 0
for month in ('26AUG', '26SEP'):
    files = list((root / 'dataset/native_v13_cache').glob(month + '_*.parquet'))
    if len(files) != 1:
        raise RuntimeError(f'Expected one verified complete native cache for {month}')
    frame = pd.read_parquet(files[0])
    frame['sid'] += offset
    offset = int(frame.sid.max()) + 1
    parts.append(frame)
signals = pd.concat(parts, ignore_index=True)
signals['day'] = pd.to_datetime(signals.day).dt.date
context = v5.v13_v3.load_nifty_first_bar_context(signals.contract_month.unique())
signals = v5.v13_v3.annotate_nifty_gate(signals, context)
signals = signals.loc[signals.nifty_first_bar_gate_pass].copy()
signals = v5.v13_v2.apply_policy(signals, v5.v13_v2.POLICIES[v5.BASE_POLICY_NAME])
orders = v5.select_orders(signals, v5.profile_setups(v5.PROFILES['higher_frequency']))
paths, quality = v5.materialize_raw_paths(orders, cutoff=v5.OFFICIAL_CUTOFF)
selected, ledger, summary = engine.evaluate(signals, paths, sorted(signals.day.unique()))
parity = research.published_parity(selected, ledger, root)
research.dump(root / 'baseline_preflight_summary.json', summary)
quality.to_csv(root / 'baseline_preflight_path_quality.csv', index=False)
print(json.dumps({'parity': parity, 'net': summary['net_profit_rupees'], 'trades': summary['portfolio_executed_trades']}, indent=2), flush=True)
