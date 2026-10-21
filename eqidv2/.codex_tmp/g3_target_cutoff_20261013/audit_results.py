"""Independently reconcile the target-cutoff report and heap-based allocation."""
from pathlib import Path
import importlib.util
import json
import sys
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parents[1]
sys.path.insert(0, str(WORKSPACE))
from research import g3_target_cutoff_contract as contract
ROOT = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3')
module = importlib.util.spec_from_file_location('independent_heap', HERE.parent / 'g4_midday_selected12_20261013/subset_audit.py')
audit = importlib.util.module_from_spec(module)
module.loader.exec_module(audit)
checks = []


def check(name, value):
    checks.append(dict(name=name, passed=bool(value)))
    assert value, name


def close(a, b):
    return a is None and b is None or a is not None and b is not None and bool(np.isclose(a, b, rtol=0, atol=1e-6))


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def read(path):
    return pd.read_csv(path, float_precision='round_trip')


pointer = read_json(ROOT / 'latest/current_backtest.json')
production_snapshot = read_json(WORKSPACE / 'docs/G3_V7_DETAILED_CONFIGURATION_2026-10-13.json')
for name, digest in production_snapshot['source_sha256'].items():
    check('PAPER/LIVE source unchanged ' + name, audit.sha(WORKSPACE / name) == digest)
run = ROOT / pointer['run_dir']
manifest = read_json(run / 'manifest.json')
check('current pointer manifest', audit.sha(run / 'manifest.json') == pointer['manifest_sha256'])
for name, digest in manifest['files'].items():
    check('artifact ' + name, audit.sha(run / name) == digest)
for name, digest in manifest['code_sha256'].items():
    check('code ' + name, audit.sha(Path(name)) == digest)
summary = read_json(run / 'summary.json')
config = read_json(run / 'config.json')
check('current configuration', config == contract.strategy_payload())
check('current identity', summary['strategy_fingerprint'] == contract.strategy_fingerprint())
check('same 50 sessions', summary['sessions'] == 50 and summary['first_date'] == '2026-07-29' and summary['last_date'] == '2026-10-13')
check('46 enabled', len(config['active_setups']) == summary['active_setup_count'] == 46)
check('13 disabled', set(read(run / 'disabled_setups.csv').setup_id) == contract.DISABLED_IDS)
ledger = read(run / 'portfolio_trades.csv')
check('no disabled selection', not set(ledger.setup_id) & contract.DISABLED_IDS)
check('all targets > .75', ledger.native_target_pct.gt(.75).all())
expected = audit.independent_allocate(ledger)
check('independent allocation decisions', np.array_equal(expected.portfolio_executed, ledger.portfolio_executed))
for raw, output in [('net', 'net_profit'), ('gross', 'gross_profit'), ('cost', 'cost')]:
    check('individual ' + raw, np.allclose(expected['audit_' + raw], ledger['portfolio_' + output + '_rupees'], atol=1e-6, rtol=0))
totals = audit.stats(expected, summary['days'])
for key, value in summary['current_g3'].items():
    check('summary ' + key, close(totals[key], value))
daily = read(run / 'daily_results.csv')
for row in daily.to_dict('records'):
    values = audit.stats(expected.loc[expected.day.eq(row['day'])], [row['day']])
    for published, metric in [('trades', 'executed_trades'), ('wins', 'wins'), ('losses', 'losses'),
                               ('net_pnl', 'net_pnl_rs'), ('gross_pnl', 'gross_pnl_rs'), ('cost', 'costs_rs')]:
        check(row['day'] + ' ' + published, close(row[published], values[metric]))
for row in read(run / 'setup_results.csv').to_dict('records'):
    values = audit.stats(expected.loc[expected.setup_id.eq(row['setup_id'])], summary['days'])
    for key in ('selected_orders', 'native_fills', 'executed_trades', 'wins', 'losses', 'net_pnl_rs', 'max_drawdown_rs'):
        check(row['setup_id'] + ' ' + key, close(row[key], values[key]))
for row in read(run / 'monthly_results.csv').to_dict('records'):
    days = [day for day in summary['days'] if day.startswith(row['month'])]
    values = audit.stats(expected.loc[expected.day.isin(days)], days)
    for key in ('executed_trades', 'wins', 'losses', 'net_pnl_rs', 'max_drawdown_rs'):
        check(row['month'] + ' ' + key, close(row[key], values[key]))
before = read(run / 'baseline_trades.csv')
old_pointer = read_json(ROOT / 'latest/archived/V13-V10-G-3-PRODUCTION-V7/current_backtest.json')
old_run = ROOT / old_pointer['run_dir']
check('prior V7 preserved', audit.sha(old_run / 'manifest.json') == old_pointer['manifest_sha256'])
old = read(old_run / 'portfolio_trades.csv').set_index('signal_id').sort_index()
reproduced = before.set_index('signal_id').sort_index()
check('replayed all prior orders', reproduced.index.equals(old.index))
for column in ('filled', 'portfolio_executed', 'exit_reason'):
    check('prior ' + column, reproduced[column].equals(old[column]))
for column in ('entry_price', 'exit_price', 'gross_return_pct', 'net_return_pct', 'portfolio_net_profit_rupees'):
    check('prior ' + column, np.allclose(reproduced[column], old[column], atol=1e-6, rtol=0, equal_nan=True))
mapped = ledger.source_signal_id.map(before.set_index('signal_id').portfolio_executed)
new = ledger.loc[ledger.portfolio_executed & ~mapped]
check('newly admitted inventory', set(new.signal_id) == set(read(run / 'newly_admitted_trades.csv').signal_id))
disabled_pnl = float(before.loc[before.setup_id.isin(contract.DISABLED_IDS), 'portfolio_net_profit_rupees'].sum())
check('before removal plus capital effect equals after', close(
    summary['baseline_g3_v7']['net_pnl_rs'] - disabled_pnl + summary['capital_reallocation_net_pnl_rs'], totals['net_pnl_rs']))
today = daily.loc[daily.day.eq('2026-10-13')].iloc[0].to_dict()
daily_pointer = read_json(ROOT / 'latest/current_run.json')
daily_run = ROOT / daily_pointer['run_dir']
daily_manifest = read_json(daily_run / 'run_manifest.json')
check('daily profile matches historical profile', daily_manifest['strategy_fingerprint'] == contract.strategy_fingerprint())
for historical, daily_key in [('trades', 'trades'), ('wins', 'wins'), ('losses', 'losses'), ('net_pnl', 'net_profit_rupees')]:
    check('fresh daily agrees with history ' + historical, close(today[historical], daily_manifest['metrics'][daily_key]))
fresh_daily = read(daily_run / daily_manifest['artifacts']['portfolio_trades']['path']).set_index('signal_id').sort_index()
history_daily = ledger.loc[ledger.day.eq('2026-10-13')].set_index('signal_id').sort_index()
check('fresh daily and historical selected signals agree', fresh_daily.index.equals(history_daily.index))
for column in ('filled', 'portfolio_executed', 'exit_reason'):
    check('fresh daily and historical ' + column, fresh_daily[column].equals(history_daily[column]))
for column in ('entry_price', 'exit_price', 'portfolio_net_profit_rupees'):
    check('fresh daily and historical ' + column, np.allclose(fresh_daily[column], history_daily[column], atol=1e-6, rtol=0, equal_nan=True))
receipt = dict(status='PASS', checks=len(checks), run_dir=str(run), full_history=totals,
    disabled_pnl_at_old_allocation=disabled_pnl, newly_admitted_count=len(new), latest_day=today,
    comparison_delta=summary['comparison_delta'], checks_detail=checks)
(HERE / 'independent_validation.json').write_text(json.dumps(receipt, indent=2), encoding='utf-8')
print(json.dumps({k:v for k,v in receipt.items() if k != 'checks_detail'}, indent=2))
