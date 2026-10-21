"""Independent final accounting using the separate audited heap allocator."""
from pathlib import Path
import hashlib
import importlib.util
import json
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parents[1]
ROOT = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3')
spec = importlib.util.spec_from_file_location('independent_heap', HERE.parent / 'g4_midday_selected12_20261013/subset_audit.py')
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)
checks = []


def check(name, value):
    checks.append(dict(name=name, passed=bool(value)))
    assert value, name


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def read(path):
    return pd.read_csv(path, float_precision='round_trip')


def verify_manifest(folder, name='manifest.json'):
    manifest = read_json(folder / name)
    for file, digest in manifest.get('files', {}).items():
        check('hash ' + str(folder / file), audit.sha(folder / file) == digest)
    for record in manifest.get('artifacts', {}).values():
        check('artifact ' + str(folder / record['path']), audit.sha(folder / record['path']) == record['sha256'])
    for filename, digest in manifest.get('code_sha256', {}).items():
        check('code ' + filename, audit.sha(WORKSPACE / filename) == digest)
    return manifest


pointer = read_json(ROOT / 'latest/current_backtest.json')
run = ROOT / pointer['run_dir']
check('final pointer hash', audit.sha(run / 'manifest.json') == pointer['manifest_sha256'])
verify_manifest(run)
summary = read_json(run / 'summary.json')
config = read_json(run / 'config.json')
check('current V6 identity', summary['strategy_version'] == 'V13-V10-G-3-PRODUCTION-V6')
check('50 complete sessions', summary['sessions'] == 50 and summary['last_date'] == '2026-10-13')
check('44 configured setups', len(config['active_setups']) == 44 and summary['active_setup_count'] == 44)
ledger = read(run / 'portfolio_trades.csv')
allocation_input = ledger.copy()
if allocation_input.sid.duplicated().any():
    # Per-day source IDs may repeat across separately replayed dates. The day
    # and entry timestamp already resolve cross-day order; retain within-day IDs.
    check('native source IDs unique per day', not allocation_input.duplicated(['day', 'sid']).any())
    allocation_input['sid'] = allocation_input.groupby('day', sort=True).ngroup() * 100_000_000 + allocation_input.sid
expected = audit.independent_allocate(allocation_input)
check('every execution independently reproduced', np.array_equal(expected.portfolio_executed, ledger.portfolio_executed))
for raw, output in [('net', 'net_profit'), ('gross', 'gross_profit'), ('cost', 'cost')]:
    check('every ' + raw + ' amount', np.allclose(expected['audit_' + raw], ledger['portfolio_' + output + '_rupees'], rtol=0, atol=1e-6))
totals = audit.stats(expected, summary['days'])
for key, actual in summary['current_g3'].items():
    check('summary ' + key, np.isclose(totals[key], actual, rtol=0, atol=1e-6))
daily = read(run / 'daily_results.csv')
for day in summary['days']:
    st = audit.stats(expected.loc[expected.day.eq(day)], [day])
    row = daily.loc[daily.day.eq(day)].iloc[0]
    for published, metric in [('trades', 'executed_trades'), ('wins', 'wins'), ('losses', 'losses'),
                               ('net_pnl', 'net_pnl_rs'), ('gross_pnl', 'gross_pnl_rs'), ('cost', 'costs_rs')]:
        check(day + ' ' + published, np.isclose(row[published], st[metric], atol=1e-6, rtol=0))
setups = read(run / 'setup_results.csv')
for row in setups.to_dict('records'):
    frame = expected.loc[expected.setup_id.eq(row['setup_id'])]
    st = audit.stats(frame, summary['days'])
    for key in ('selected_orders', 'native_fills', 'executed_trades', 'wins', 'losses', 'net_pnl_rs', 'max_drawdown_rs'):
        check(row['setup_id'] + ' ' + key, np.isclose(row[key], st[key], rtol=0, atol=1e-6))
for row in read(run / 'monthly_results.csv').to_dict('records'):
    days = [day for day in summary['days'] if day.startswith(row['month'])]
    st = audit.stats(expected.loc[expected.day.isin(days)], days)
    for key in ('executed_trades', 'wins', 'losses', 'net_pnl_rs', 'max_drawdown_rs'):
        check(row['month'] + ' ' + key, np.isclose(row[key], st[key], rtol=0, atol=1e-6))
for record in [summary['base_source'], *summary['daily_sources']]:
    folder = ROOT / record['run_dir']
    check('source manifest ' + str(folder), audit.sha(folder / record['manifest_name']) == record['manifest_sha256'])
    verify_manifest(folder, record['manifest_name'])

old_pointer = read_json(ROOT / 'latest/archived/V13-V10-G-3-PRODUCTION-V5/current_backtest.json')
old_run = ROOT / old_pointer['run_dir']
check('prior V5 history preserved', audit.sha(old_run / 'manifest.json') == old_pointer['manifest_sha256'])
verify_manifest(old_run)
old = read(old_run / 'portfolio_trades.csv')
keys = ['day', 'setup_id', 'tradingsymbol']
previous = old.set_index(keys).sort_index()
kept = ledger.loc[~ledger.setup_id.isin(audit.TARGETS)].set_index(keys).sort_index()
check('all previous selections preserved', kept.index.equals(previous.index))
for col in ('filled', 'portfolio_executed', 'exit_reason'):
    check('prior ' + col, kept[col].astype(str).equals(previous[col].astype(str)))
for col in ('trigger', 'entry_price', 'exit_price', 'net_return_pct', 'native_target_pct', 'portfolio_net_profit_rupees'):
    check('prior ' + col, np.allclose(kept[col], previous[col], atol=1e-6, rtol=0, equal_nan=True))
for col in ('entry_ts', 'exit_ts'):
    check('prior ' + col, pd.to_datetime(kept[col], utc=True, errors='coerce').equals(pd.to_datetime(previous[col], utc=True, errors='coerce')))
added = expected.loc[expected.setup_id.isin(audit.TARGETS)]
check('only exact twelve additions', set(added.setup_id) == set(audit.TARGETS))
observations = []
for setup_id, target in audit.TARGETS.items():
    frame = added.loc[added.setup_id.eq(setup_id)]
    check('target ' + setup_id, frame.native_target_pct.eq(target).all())
    st = audit.stats(frame, summary['days'])
    observations.append(dict(setup_id=setup_id, target_pct=target, **st,
        observed_70=st['executed_trades'] >= 5 and st['win_rate_pct'] >= 70 and st['net_pnl_rs'] > 0))
receipt = dict(status='PASS', checks=len(checks), run_dir=str(run), full_history=totals,
    added_twelve=audit.stats(added, summary['days']), unchanged_existing_setups=32,
    all_twelve_at_least_70=all(row['observed_70'] for row in observations),
    below_70=[row for row in observations if not row['observed_70']], checks_detail=checks)
(HERE / 'independent_results_validation.json').write_text(json.dumps(receipt, indent=2), encoding='utf-8')
print(json.dumps({k:v for k,v in receipt.items() if k != 'checks_detail'}, indent=2))
