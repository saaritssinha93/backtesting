"""Independent selected-12 accounting; no production allocator/runner imported."""
from pathlib import Path
import hashlib
import heapq
import json
import sys

import numpy as np
import pandas as pd

WORKSPACE = Path(__file__).resolve().parents[2]
DEST = Path(__file__).resolve().parent
SOURCE = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_4/target_sweeps/2026-10-13/20261013_midday_1135_1230_targets_10pct_v2')
SOURCE_HASH = '6d6f3065e3cd774f21f41a352cb222f4e6d44ab1de0b57851d0abe4448056f41'
TARGETS = {'1136_LONG': 7.15, '1136_SHORT': .87, '1141_LONG': 4.03, '1141_SHORT': 1.19,
           '1146_SHORT': 4.95, '1206_LONG': 1.40, '1211_SHORT': .79,
           '1216_LONG': 1.24, '1216_SHORT': 3.49, '1226_SHORT': 2.13,
           '1231_LONG': 2.53, '1231_SHORT': 1.85}
read = lambda path: pd.read_csv(path, float_precision='round_trip')
sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
CHECKS = []


def check(name, value):
    CHECKS.append(dict(name=name, passed=bool(value)))
    if not value:
        raise AssertionError(name)


def independent_allocate(raw):
    frame = raw.copy().reset_index(drop=True)
    check('unique identities', not frame.sid.duplicated().any() and not frame.signal_id.duplicated().any())
    frame['filled'] = frame.filled.astype(str).str.lower().eq('true')
    for col in ['entry_ts', 'exit_ts', 'confirmation_ts']:
        frame[col] = pd.to_datetime(frame[col], utc=True, errors='coerce')
    frame['portfolio_executed'] = False
    frame['portfolio_status'] = np.where(frame.filled, 'REJECTED', 'SOURCE_UNFILLED')
    frame['audit_open_positions'] = 0
    frame['audit_priority'] = np.select([frame.picker.eq('max_liquidity'), frame.picker.eq('max_volume'), frame.picker.eq('max_move')],
        [pd.to_numeric(frame.traded_value, errors='coerce').fillna(0),
         pd.to_numeric(frame.volume_ratio, errors='coerce').fillna(0),
         pd.to_numeric(frame.get('abs_price_change_pct', frame.price_change_pct.abs()), errors='coerce').fillna(0)], default=0.)
    frame['audit_confirmation'] = frame.confirmation_ts.fillna(frame.entry_ts)
    filled = frame.loc[frame.filled].sort_values(['entry_ts', 'audit_confirmation', 'setup_id', 'audit_priority', 'tradingsymbol', 'sid'],
        ascending=[True, True, True, False, True, True], kind='stable')
    active_exits = []
    for row in filled.itertuples():
        while active_exits and active_exits[0] <= row.entry_ts.value:
            heapq.heappop(active_exits)
        frame.loc[row.Index, 'audit_open_positions'] = len(active_exits)
        if len(active_exits) < 10:
            frame.loc[row.Index, 'portfolio_executed'] = True
            frame.loc[row.Index, 'portfolio_status'] = 'EXECUTED'
            heapq.heappush(active_exits, row.exit_ts.value)
    for target, rawcol in [('net', 'net_return_pct'), ('gross', 'gross_return_pct'), ('cost', 'cost_pct')]:
        frame['audit_' + target] = np.where(frame.portfolio_executed, frame[rawcol] * 5000., 0.)
    return frame


def stats(frame, days):
    done = frame.loc[frame.portfolio_executed]
    profits = done.audit_net
    day_pnl = frame.groupby('day').audit_net.sum().reindex(days, fill_value=0)
    curve = np.r_[0., day_pnl.cumsum()]
    gains = profits.loc[profits.gt(0)].sum()
    losses = -profits.loc[profits.lt(0)].sum()
    return dict(selected_orders=len(frame), native_fills=int(frame.filled.sum()),
        executed_trades=len(done), wins=int(profits.gt(0).sum()), losses=int(profits.lt(0).sum()),
        win_rate_pct=float(100 * profits.gt(0).mean()) if len(done) else None,
        net_pnl_rs=float(profits.sum()), gross_pnl_rs=float(done.audit_gross.sum()), costs_rs=float(done.audit_cost.sum()),
        max_drawdown_rs=float((np.maximum.accumulate(curve)-curve).max()),
        profit_factor=float(gains/losses) if losses else None,
        profitable_days=int(day_pnl.gt(0).sum()), loss_days=int(day_pnl.lt(0).sum()), flat_days=int(day_pnl.eq(0).sum()))


def main(result_dir=None):
    check('source manifest pinned', sha(SOURCE/'manifest.json') == SOURCE_HASH)
    manifest = json.loads((SOURCE/'manifest.json').read_text())
    for name, expected in manifest['files'].items():
        check('source artifact ' + name, sha(SOURCE/name) == expected)
    summary = json.loads((SOURCE/'summary.json').read_text())
    check('frozen targets preserved', all(summary['historical_targets'][k] == v for k, v in TARGETS.items()))
    reference = Path(summary['source']['run_dir'])
    check('entry source pinned', sha(reference/'manifest.json') == summary['source']['manifest_sha256'])
    entry_manifest = json.loads((reference/'manifest.json').read_text())
    for name, expected in entry_manifest['files'].items():
        check('entry artifact ' + name, sha(reference/name) == expected)
    saved_base = read(reference/'baseline_g4_trades.csv')
    baseline = independent_allocate(saved_base)
    check('37 baseline setups', baseline.setup_id.nunique() == 37)
    check('baseline saved execution parity', np.array_equal(baseline.portfolio_executed, saved_base.portfolio_executed))
    check('baseline saved pnl parity', np.allclose(baseline.audit_net, saved_base.portfolio_net_profit_rupees, rtol=0, atol=1e-6))
    original_raw = read(SOURCE/'historical_profitable70_only_added_raw_trades.csv')
    wanted = original_raw.loc[original_raw.setup_id.isin(TARGETS)].copy()
    check('exactly requested 12', set(wanted.setup_id) == set(TARGETS))
    check('requested target map', wanted.groupby('setup_id').target_pct.nunique().eq(1).all()
          and wanted.groupby('setup_id').target_pct.first().to_dict() == TARGETS)
    cache = pd.read_parquet(SOURCE/'minute_replay_all_targets.parquet', filters=[('target_pct','in',list(TARGETS.values()))])
    cache = pd.concat([cache.loc[cache.setup_id.eq(k) & cache.target_pct.eq(v)] for k,v in TARGETS.items()]).set_index('sid').sort_index()
    wanted_by_id = wanted.set_index('sid').sort_index()
    check('cache selected identities', cache.index.equals(wanted_by_id.index))
    for col in ['entry_price','exit_price','net_return_pct','gross_return_pct','cost_pct','target_pct']:
        check('cache raw parity ' + col, np.allclose(cache[col], wanted_by_id[col], rtol=0, atol=1e-10, equal_nan=True))
    for col in ['entry_ts','exit_ts','confirmation_ts']:
        check('cache timestamp parity ' + col, pd.to_datetime(cache[col],utc=True,errors='coerce').equals(pd.to_datetime(wanted_by_id[col],utc=True,errors='coerce')))
    full = independent_allocate(pd.concat([saved_base, wanted], ignore_index=True))
    added = full.loc[full.sid.isin(wanted.sid)]
    days = sorted(read(SOURCE/'daily_results.csv').day.unique())
    check('49 sessions', len(days) == 49 and days[0] == '2026-07-29' and days[-1] == '2026-10-12')
    check('49 configured setups', full.setup_id.nunique() == 49)
    periods = dict(all=days, train=days[:38], holdout=days[38:])
    totals = []
    for period, cohort in periods.items():
        for scope, frame in [('baseline',baseline),('combined',full),('additions',added),('additions_long',added.loc[added.side.eq('LONG')]),('additions_short',added.loc[added.side.eq('SHORT')])]:
            totals.append(dict(period=period,scope=scope,**stats(frame.loc[frame.day.isin(cohort)],cohort)))
    setup_stats = []
    for setup in sorted(TARGETS):
        for period, cohort in periods.items():
            sample = added.loc[added.setup_id.eq(setup)&added.day.isin(cohort)]
            setup_stats.append(dict(setup_id=setup, signal_end=wanted.loc[wanted.setup_id.eq(setup)].signal_end.iloc[0],
                target_pct=TARGETS[setup], period=period, **stats(sample,cohort)))
    old = full.loc[full.sid.isin(baseline.sid)].set_index('sid').sort_index()
    base = baseline.set_index('sid').sort_index()
    check('baseline executions unchanged', old.portfolio_executed.equals(base.portfolio_executed))
    check('baseline pnl unchanged', np.allclose(old.audit_net,base.audit_net,rtol=0,atol=1e-6))
    daily = pd.DataFrame([dict(day=day,**stats(full.loc[full.day.eq(day)],[day]),
        added_net_pnl_rs=float(added.loc[added.day.eq(day)].audit_net.sum())) for day in days])
    monthly = pd.DataFrame([dict(month=month,**stats(full.loc[full.day.str.startswith(month)],
        [day for day in days if day.startswith(month)]),
        added_net_pnl_rs=float(added.loc[added.day.str.startswith(month)].audit_net.sum())) for month in sorted({d[:7] for d in days})])
    if result_dir:
        result = Path(result_dir)
        check('result manifest present', (result/'manifest.json').exists())
        result_manifest = json.loads((result/'manifest.json').read_text())
        for name, expected in result_manifest['files'].items():
            check('result artifact ' + name, sha(result/name) == expected)
        for name, expected in result_manifest['code_sha256'].items():
            check('result code ' + name, sha(WORKSPACE/name) == expected)
        candidate_file = next((result/name for name in ['combined_trades.csv','variant_trades.csv','selected_midday_trades.csv'] if (result/name).exists()))
        actual = read(candidate_file).set_index('sid').sort_index()
        expected = full.set_index('sid').sort_index()
        check('result exact identities', actual.index.equals(expected.index))
        check('result execution allocation', actual.portfolio_executed.equals(expected.portfolio_executed))
        check('result PNL arithmetic', np.allclose(actual.portfolio_net_profit_rupees,expected.audit_net,rtol=0,atol=1e-6))
        for col in ['entry_price','exit_price','net_return_pct','gross_return_pct','cost_pct','target_pct']:
            check('result frozen raw parity '+col,np.allclose(actual[col],expected[col],rtol=0,atol=1e-10,equal_nan=True))
        for col in ['entry_ts','exit_ts','confirmation_ts']:
            check('result frozen timestamps '+col,pd.to_datetime(actual[col],utc=True,errors='coerce').equals(pd.to_datetime(expected[col],utc=True,errors='coerce')))
        result_config = json.loads((result/'config.json').read_text())
        check('result config only requested setups', len(result_config['setups']) == 12 and
            {s['setup_id']:s['target_pct'] for s in result_config['setups']} == TARGETS)
        source_configs = json.loads((SOURCE/'scenario_configs.json').read_text())
        source_specs = {s['setup_id']:s for s in source_configs['scenarios']['historical_profitable70_only']}
        for spec in result_config['setups']:
            source_spec = source_specs[spec['setup_id']]
            for key in ['thresholds','signal_end','confirmation_end','side','target_pct','initial_stop_pct','tightened_stop_pct','tighten_after_minutes','max_entries','picker']:
                check('config frozen '+spec['setup_id']+' '+key, spec[key] == source_spec[key])
        inventory = read(result/'enabled_setups.csv')
        check('49 configured in result inventory', len(inventory) == 49 and set(inventory.setup_id) == set(full.setup_id))
        check('all inventory enabled', inventory.enabled.eq(True).all())
        actual_summary = json.loads((result/'summary.json').read_text())
        count_fields = ['selected_orders','native_fills','executed_trades','wins','losses']
        numeric_fields = ['win_rate_pct','net_pnl_rs','gross_pnl_rs','costs_rs','max_drawdown_rs','profit_factor']
        for scope,key in [('combined','combined_g4'),('baseline','baseline_g4'),('additions','selected_additions')]:
            expected_stats = next(s for s in totals if s['scope']==scope and s['period']=='all')
            for field in count_fields+numeric_fields:
                check('summary '+scope+' '+field, abs(actual_summary[key][field]-expected_stats[field]) < 1e-6)
        actual_daily = read(result/'daily_results.csv')
        actual_monthly = read(result/'monthly_results.csv')
        actual_periods = read(result/'period_results.csv')
        for scope,frame in [('combined',full),('baseline',baseline),('additions',added)]:
            for row in actual_daily.loc[actual_daily.scope.eq(scope)].itertuples():
                expected_stats = stats(frame.loc[frame.day.eq(row.day)],[row.day])
                check('daily '+scope+' '+row.day, all(abs(getattr(row,f)-expected_stats[f])<1e-6 for f in count_fields+['net_pnl_rs','gross_pnl_rs','costs_rs']))
            for row in actual_monthly.loc[actual_monthly.scope.eq(scope)].itertuples():
                cohort = [day for day in days if day.startswith(row.month)]
                expected_stats = stats(frame.loc[frame.day.isin(cohort)],cohort)
                check('monthly '+scope+' '+row.month, all(abs(getattr(row,f)-expected_stats[f])<1e-6 for f in count_fields+numeric_fields))
            for row in actual_periods.loc[actual_periods.scope.eq(scope)].itertuples():
                period = {'all_49':'all','training_38':'train','later_11':'holdout'}[row.period]
                expected_stats = next(s for s in totals if s['scope']==scope and s['period']==period)
                check('period '+scope+' '+period, all(abs(getattr(row,f)-expected_stats[f])<1e-6 for f in count_fields+numeric_fields))
        actual_setups = read(result/'setup_results.csv')
        for row in actual_setups.itertuples():
            expected_stats = stats(full.loc[full.setup_id.eq(row.setup_id)],days)
            check('setup results '+row.setup_id, all(abs(getattr(row,f)-expected_stats[f])<1e-6 for f in count_fields+numeric_fields if expected_stats[f] is not None))
    DEST.mkdir(parents=True,exist_ok=True)
    pd.DataFrame(totals).to_csv(DEST/'expected_portfolio_results.csv',index=False)
    pd.DataFrame(setup_stats).to_csv(DEST/'expected_setup_results.csv',index=False)
    daily.to_csv(DEST/'expected_daily_results.csv',index=False)
    monthly.to_csv(DEST/'expected_monthly_results.csv',index=False)
    full.to_csv(DEST/'expected_ledger.csv',index=False)
    receipt = dict(status='PASS', checks=len(CHECKS), source_manifest_sha256=SOURCE_HASH,
        requested_setups=TARGETS, production_runner_imported=False, production_allocator_imported=False,
        result_dir=str(result_dir) if result_dir else None, results=totals,
        all_checks=CHECKS)
    if result_dir:
        receipt.update(result_manifest_sha256=sha(Path(result_dir)/'manifest.json'),
                       result_artifact_hashes=len(result_manifest['files']),
                       result_code_hashes=len(result_manifest['code_sha256']))
    (DEST/'independent_validation.json').write_text(json.dumps(receipt,indent=2)+'\n')
    print(json.dumps({key:receipt[key] for key in ['status','checks','result_dir','results']},indent=2))


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv)>1 else None)
