"""Check sealed 15 October G4 guard reselection before modifying G4."""
from pathlib import Path
import sys,json
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
import numpy as np
import pandas as pd
from research import g4_g3_mirror_backtest as g4
from research import g3_0950_long_backtest as morning
from research.g4_extended_target_sweep import read_frozen_csv

g4run=Path(r'C:\TradingData\eqidv2\backtesting_result_v13_v10_g_4\runs\2026-10-15\20261015T150117806347_ec1b84fc_late_slots')
g3run=Path(r'C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\backtests\runs\2026-10-15\20261015T171145325060_264a0ea2_current_full_history')
cfg=g4.configuration();name=cfg['primary_scenario'];summary=g4.read(g3run/'summary.json')
source_item=summary['daily_sources'][-1]
source_dir=g4.G3_ROOT/source_item['run_dir']
assert g4.sha256(source_dir/'run_manifest.json')==source_item['manifest_sha256']
manifest=g4.read(source_dir/'run_manifest.json')
record=manifest['artifacts']['candidate_signals'];file=source_dir/record['path']
assert g4.sha256(file)==record['sha256']
raw=morning.normalize_signals(read_frozen_csv(file))
ids=set(cfg['scenarios'][name])|set(cfg['five_minute_volume_ratio_overrides'][name]);clocks={int(g4.frozen_donor._ACTIVE_BY_ID[s].signal_end.replace(':','')) for s in ids}
raw=raw.loc[raw.hhmm_int.isin(clocks)].copy();raw['sid']=np.arange(97_000_000,97_000_000+len(raw))
pool=g4.donor.selection_audit(raw);pool=pool.loc[pool.setup_id.isin(ids)].copy()
print('Raw daily candidates',len(raw),'guard pool',len(pool),'symbols',pool.tradingsymbol.nunique(),flush=True)
prices=g4.Prices(summary);pool=prices.enrich(pool)
selected=g4.reselect(pool,cfg['scenarios'][name],cfg['five_minute_volume_ratio_overrides'][name])
current=read_frozen_csv(g3run/'selected_orders.csv');current=current.loc[current.day.eq('2026-10-15') & current.setup_id.isin(g4.frozen_donor._ACTIVE_BY_ID)]
before=current.loc[current.setup_id.isin(ids)]
print('G3selected old49',len(current),'guardeligiblebaseline',len(before),'G4reselections',len(selected),flush=True)
print('G3guard names',sorted((row.setup_id,row.tradingsymbol) for row in before.itertuples()),flush=True)
print('G4guard names',sorted((row.setup_id,row.tradingsymbol) for row in selected.itertuples()),flush=True)
print('Price source types',dict(pd.Series([r['source_kind'] for r in prices.proof.values()]).value_counts()),flush=True)
