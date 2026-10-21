"""Read-only independent audit of the completed V7 daily publication."""
import hashlib
import json
import math
from pathlib import Path
import sys

import numpy as np
import pandas as pd

WORK = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WORK))
import fno_v13_v10_g_3_contract as contract
import backtesting_result_v13_v10_g_3_daily as publisher

ROOT = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3')
HERE = Path(__file__).parent
DONOR = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_4/target_sweeps/2026-10-13/20261013_afternoon_dual70_targets_v5/refined_config.json')
checks = 0


def check(condition, message):
    global checks
    if not bool(condition):
        raise AssertionError(message)
    checks += 1


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def load(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def package(pointer):
    directory = ROOT / pointer['run_dir']
    check(sha(directory / 'run_manifest.json') == pointer['manifest_sha256'], 'manifest')
    manifest = load(directory / 'run_manifest.json')
    for name, item in manifest['artifacts'].items():
        check(sha(directory / item['path']) == item['sha256'], 'artifact:' + name)
    return directory, manifest


pointer = load(ROOT / 'latest/current_run.json')
directory, manifest = package(pointer)
check(manifest['strategy_version'] == contract.STRATEGY_VERSION == 'V13-V10-G-3-PRODUCTION-V7', 'V7 identity')
check(manifest['strategy_fingerprint'] == contract.strategy_fingerprint(), 'fingerprint')
check(manifest['complete'] and manifest['state'] == 'SUCCESS', 'successful')
check(manifest['execution_authority'] is False, 'no execution authority')
check(manifest['days'] == ['2026-10-13'], 'exact daily session')
source = load(directory / 'source_manifest.json')
check(source['completed_trade_outputs_consumed'] is False, 'raw inputs only')
snapshot = Path(source['snapshot_manifest'])
check(sha(snapshot) == source['snapshot_manifest_sha256'], 'snapshot manifest')
for item in load(snapshot)['sources']:
    if item.get('captured'):
        check(sha(snapshot.parent / item['snapshot_relative_path']) == item['sha256'], 'snapshot source')
for item in source['code']:
    check(sha(item['path']) == item['sha256'], 'code provenance:' + item['path'])
check(any(Path(item['path']).name == 'fno_v13_v10_g_3_afternoon_setups.py' for item in source['code']), 'afternoon provenance')

features = pd.read_csv(directory / 'feature_ledger.csv')
clocks = {int(x.replace(':', '')) for x in contract.SIGNAL_TO_CONFIRMATION}
check(len(clocks) == 40 and max(clocks) == 1330, '40 clocks through13:30')
for symbol, frame in features.groupby('tradingsymbol'):
    check(set(frame.hhmm_int) == clocks and len(frame) == 40, 'complete clock coverage:' + symbol)
pool = pd.read_csv(directory / 'candidate_signals.csv')
orders = pd.read_csv(directory / 'selected_orders.csv')
trades = pd.read_csv(directory / 'portfolio_trades.csv')
donor = load(DONOR)
ids = {s['setup_id'] for s in donor['setups']}
audit_rows = []
for row in donor['setups']:
    threshold = row['thresholds']
    candidates = pool.loc[pool.hhmm_int.eq(int(row['signal_end'].replace(':', ''))) & pool.side.eq(row['side'])].copy()
    sign = 1 if row['side'] == 'LONG' else -1
    eligible = candidates.loc[
        (candidates.price_change_pct * sign).ge(threshold['price_change_pct'])
        & candidates.oi_change_pct.ge(threshold['oi_change_pct'])
        & candidates.volume_ratio.ge(threshold['five_minute_volume_ratio_min'])
        & candidates.body_ratio.between(threshold['one_minute_body_ratio_min'], 1)
        & candidates.wick_ratio.between(0, threshold['one_minute_wick_ratio_max'])
        & candidates.v9_1m_volume_ratio.ge(threshold['one_minute_volume_ratio_min'])
        & candidates.traded_value.ge(0)
    ].sort_values(['traded_value', 'tradingsymbol'], ascending=[False, True], kind='stable')
    selected = orders.loc[orders.setup_id.eq(row['setup_id'])]
    check(selected.tradingsymbol.tolist() == eligible.head(1).tradingsymbol.tolist(), 'independent maximum liquidity:' + row['setup_id'])
    check(selected.native_target_pct.eq(row['target_pct']).all(), 'target:' + row['setup_id'])
    check(selected.initial_stop_pct.eq(1.25).all() and selected.tightened_stop_pct.eq(1).all()
          and selected.tighten_after_minutes.eq(120).all(), 'stop:' + row['setup_id'])
    audit_rows.append(dict(setup_id=row['setup_id'], raw_candidates=len(candidates), eligible_candidates=len(eligible),
        selected=selected.tradingsymbol.tolist(), target_pct=row['target_pct']))
check(set(orders.setup_id) <= {s.setup_id for s in contract.ACTIVE_SETUPS}, 'only59 approved setups')
check(orders.signal_id.is_unique and trades.signal_id.is_unique, 'canonical identity uniqueness')
executed = trades.loc[trades.portfolio_executed.eq(True)]
check(np.allclose(executed.net_return_pct, executed.gross_return_pct - .05), '5bps roundtrip')
check(np.allclose(executed.portfolio_cost_rupees, 250), '250rupee cost')
check(np.allclose(executed.portfolio_net_profit_rupees, executed.net_return_pct * 5000), 'net rupee arithmetic')
metrics = manifest['metrics']
check(metrics['selected_orders'] == len(orders), 'selected order count')
check(metrics['trades'] == len(executed), 'executed count')
check(metrics['wins'] == int(executed.portfolio_net_profit_rupees.gt(0).sum()), 'win count')
check(math.isclose(metrics['net_profit_rupees'], executed.portfolio_net_profit_rupees.sum(), abs_tol=1e-8), 'net metric')
check((pd.to_datetime(executed.entry_ts) > pd.to_datetime(executed.confirmation_ts)).all(), 'entry after confirmation')
check(pd.to_datetime(executed.exit_ts).dt.strftime('%H:%M').le('15:15').all(), '15:15 exit')

prior_pointer = load(HERE / 'prior_current_run.json')
prior_dir, prior_manifest = package(prior_pointer)
check(prior_manifest['strategy_version'] == 'V13-V10-G-3-PRODUCTION-V6', 'priorV6')
check(load(ROOT / 'latest/archived/V13-V10-G-3-PRODUCTION-V6/current_run.json') == prior_pointer, 'archived daily pointer')
check(load(ROOT / 'latest/archived/V13-V10-G-3-PRODUCTION-V6/current_history.json') == load(HERE / 'prior_current_history.json'), 'archived history pointer')
old = pd.read_csv(prior_dir / 'portfolio_trades.csv')
current_old = trades.loc[~trades.setup_id.isin(ids)]
columns = ['setup_id', 'tradingsymbol', 'filled', 'portfolio_executed', 'entry_ts', 'exit_ts',
    'entry_price', 'exit_price', 'gross_return_pct', 'net_return_pct', 'portfolio_net_profit_rupees', 'exit_reason']
keys = ['setup_id', 'tradingsymbol']
pd.testing.assert_frame_equal(old[columns].sort_values(keys).reset_index(drop=True),
    current_old[columns].sort_values(keys).reset_index(drop=True), check_exact=False, atol=1e-9, rtol=1e-12)
check(True, 'all previous11 executions preserved')
history_dir, history = publisher._read_verified_pointer(ROOT, 'current_history.json', 'cumulative_history')
check(history['days'] == ['2026-10-13'] and history['strategy_version'] == manifest['strategy_version'], 'fresh V7 daily history')
added = executed.loc[executed.setup_id.isin(ids)]
summary = dict(passed=True, checks=checks, strategy_version=manifest['strategy_version'], run_dir=str(directory),
    history_run_dir=str(history_dir), metrics=metrics, universe_count=manifest['coverage']['universe_count'],
    included_stocks=manifest['coverage']['included_stocks'], excluded_stocks=manifest['coverage']['excluded_stocks'],
    opening_exclusions=manifest['coverage']['opening_0920_exclusions'], new_setup_selection_checks=audit_rows,
    afternoon_metrics=dict(orders=int(orders.setup_id.isin(ids).sum()), trades=len(added),
        wins=int(added.portfolio_net_profit_rupees.gt(0).sum()), net_pnl_rupees=added.portfolio_net_profit_rupees.sum()),
    prior_V6_daily_preserved=True)
(HERE / 'daily_validation.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
print(json.dumps({k: v for k, v in summary.items() if k not in ['new_setup_selection_checks', 'opening_exclusions']}, indent=2))
