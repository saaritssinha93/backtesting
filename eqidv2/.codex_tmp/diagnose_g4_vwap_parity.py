from datetime import date
import pandas as pd
from research import g4_g3_mirror_backtest as base
from research.g4_rejection_validation import REFERENCE

ref = base.read(REFERENCE / 'summary.json')
g3dir = base.Path(ref['baseline_g3_run'])
summary = base.read(g3dir / 'summary.json')
old = base.read_frozen_csv(g3dir / 'portfolio_trades.csv')
pool = pd.read_pickle(base.ROOT / '.codex_tmp/g4_vwap_rsi_prepared_pool/pool.pkl')
selected = base.reselect(pool, {})
cfg = base.configuration()
cfg['scenarios'] = {'diagnostic': {}}
identity = base.Identity(cfg, 'diagnostic')
selected = identity.identify(selected)
prices = base.Prices(summary)
actual = base._simulate(selected, prices.paths(selected), [date.fromisoformat(d) for d in summary['days']], identity)
actual.to_pickle(base.ROOT / '.codex_tmp/g4_vwap_diagnostic_g3.pkl')
left = actual.set_index('sid').sort_index()
right = old.loc[old.sid.isin(actual.sid)].set_index('sid').sort_index()
for field in ('entry_ts', 'exit_ts'):
    a, b = pd.to_datetime(left[field], utc=True), pd.to_datetime(right[field], utc=True)
    print(field, 'dtype', a.dtype, b.dtype, 'index', a.index.dtype, b.index.dtype, 'equals', a.equals(b))
    table = pd.DataFrame({'actual': a, 'expected': b, 'same': a.eq(b) | (a.isna() & b.isna())})
    print(table.loc[~table.same].to_string())
base._same_native(actual, old.loc[old.sid.isin(actual.sid)])
