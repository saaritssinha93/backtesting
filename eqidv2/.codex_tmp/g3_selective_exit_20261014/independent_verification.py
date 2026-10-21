"""Independent numerical verification; does not import experimental code."""
from pathlib import Path
import hashlib
import json
import numpy as np
import pandas as pd

ROOT = Path('C:/TradingData/eqidv2/v13_v10_g_3_selective_exit_backtest/20261014T104120180836_0de09576_ridge_exit')
KEYS = ['sid', 'day', 'decision_at_ist', 'completed_bar_count']
summary = json.loads((ROOT/'summary.json').read_text())
protocol = json.loads((ROOT/'protocol.json').read_text())
model = json.loads((ROOT/'model.json').read_text())
selection = json.loads((ROOT/'selection_lock.json').read_text())
lock = json.loads((ROOT/'protocol_lock.json').read_text())
manifest = json.loads((ROOT/'manifest.json').read_text())
digest = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
for name, expected in manifest['files'].items():
    assert digest(ROOT/name) == expected, name
assert digest(ROOT/'model.json') == selection['model_sha256']
assert digest(ROOT/'protocol.json') == selection['protocol_sha256'] == lock['protocol_sha256']
for name, expected in lock['input_hashes'].items():
    assert digest(ROOT/'inputs'/name) == expected
source = Path(protocol['source_dataset'])
assert digest(source/'manifest.json') == lock['source_manifest_sha256']
for name, expected in json.loads((source/'manifest.json').read_text())['files'].items():
    assert digest(source/name) == expected, name

b = pd.read_csv(ROOT/'inputs/baseline.csv')
b = b[b.portfolio_executed.eq(True)].copy()
x = pd.read_parquet(ROOT/'inputs/decision_time_predictors.parquet')
y_original = pd.read_parquet(ROOT/'inputs/future_labels_do_not_use_as_features.parquet')
y = x[KEYS].merge(y_original, on=KEYS, validate='one_to_one', sort=False)
p = pd.read_csv(ROOT/'decision_predictions.csv')
p = x[KEYS].merge(p, on=KEYS, validate='one_to_one', sort=False)
assert len(x) == len(y) == len(p) == 1268
assert len(b) == 457 and x.sid.nunique() == 445
assert x[KEYS].equals(y[KEYS]) and x[KEYS].equals(p[KEYS])
days = sorted(b.day.unique())
assert len(days) == 50
periods = {'training25':days[:25], 'calibration10':days[25:35], 'evaluation15':days[35:]}
assert periods == summary['periods']
training = x.day.isin(days[:25])
calibration = x.day.isin(days[25:35])
evaluation = x.day.isin(days[35:])
for mask, next_day in [(training, days[25]), (calibration, days[35])]:
    mature = pd.to_datetime(y.loc[mask, 'label_available_at_ist'], utc=True, format='mixed')
    assert mature.lt(pd.Timestamp(next_day, tz='Asia/Kolkata').tz_convert('UTC')).all()
assert not set(x.loc[training, 'sid']) & set(x.loc[~training, 'sid'])

# Independently solve the weighted ridge as augmented least squares,
# including an unpenalized intercept, rather than the original normal equations.
features = protocol['features']
xt = x.loc[training]
values = xt[features].to_numpy(float)
w = (1/xt.groupby('sid').sid.transform('count')).to_numpy(float)
counts = pd.Series(w, index=xt.index).groupby(xt.sid).sum()
np.testing.assert_allclose(counts.to_numpy(), 1)
mean = np.average(values, axis=0, weights=w)
scale = np.sqrt(np.average((values-mean)**2, axis=0, weights=w))
scale[scale <= 1e-12] = 1
risk = x.sid.map(b.set_index('sid').exposure_per_entry_rupees) * x.sid.map(b.set_index('sid').initial_stop_pct)/100
target = y.label_exit_value_after_cost_rs/risk
z = (values-mean)/scale
design = np.column_stack([np.ones(len(z)), z])
penalty = np.diag(np.r_[0, np.repeat(np.sqrt(protocol['ridge_penalty']), len(features))])
augmented_x = np.vstack([design*np.sqrt(w)[:, None], penalty])
augmented_y = np.r_[target.loc[training].to_numpy()*np.sqrt(w), np.zeros(len(features)+1)]
coefficients, _, _, _ = np.linalg.lstsq(augmented_x, augmented_y, rcond=None)
scores = coefficients[0] + ((x[features].to_numpy()-mean)/scale)@coefficients[1:]
np.testing.assert_allclose(mean, model['mean'], rtol=1e-12, atol=1e-12)
np.testing.assert_allclose(scale, model['scale'], rtol=1e-12, atol=1e-12)
np.testing.assert_allclose(coefficients, [model['intercept']]+model['coefficients'], rtol=1e-12, atol=1e-12)
np.testing.assert_allclose(scores, p.score_initial_r, rtol=1e-12, atol=1e-12)
cal_target = calibration & y.label_native_target
assert int(cal_target.sum()) == selection['target_calibration_rows'] == 72
threshold = max(0., scores[cal_target].max()) + .05
assert threshold == selection['protected_threshold_initial_r'] == .05

# Reconcile labels independently with the original economic outcomes.
native = y.sid.map(b.set_index('sid').portfolio_net_profit_rupees)
np.testing.assert_allclose(native, y.label_native_net_pnl_rs)
cost = y.sid.map(b.set_index('sid').exposure_per_entry_rupees)*.001
np.testing.assert_allclose(cost, y.label_extra_exit_cost_rs)
np.testing.assert_allclose(y.label_exit_now_net_pnl_rs-native-cost, y.label_exit_value_after_cost_rs)
assert scores.max() < 0, 'No-action assertion does not hold globally'

def metrics(frame, session_days):
    pnl = frame.portfolio_net_profit_rupees
    daily = frame.groupby('day').portfolio_net_profit_rupees.sum().reindex(session_days, fill_value=0)
    equity = np.concatenate(([0], daily.cumsum().to_numpy()))
    dd = float((np.maximum.accumulate(equity)-equity).max())
    stops = int(frame.exit_reason.isin(['STOP', 'TIGHTENED_STOP']).sum())
    losses = float(-pnl[pnl<0].sum())
    return dict(sessions=len(session_days), trades=len(frame), baseline_net_rs=float(pnl.sum()),
        policy_net_rs=float(pnl.sum()), incremental_net_rs=0., early_exits=0,
        intervention_days=0, baseline_target_trades=int(frame.exit_reason.eq('TARGET').sum()),
        target_trades_touched=0, target_profit_sacrificed_rs=0., baseline_stop_trades=stops,
        remaining_native_stops=stops, stopped_trades_improved=0, stop_losses_saved_rs=0.,
        stopped_trades_worsened=0, baseline_winners_harmed=0, winner_profit_sacrificed_rs=0.,
        winners_turned_nonpositive=0, baseline_losing_trades=int((pnl<0).sum()),
        policy_losing_trades=int((pnl<0).sum()), baseline_realized_loss_rs=losses,
        policy_realized_loss_rs=losses, realized_loss_reduction_rs=0.,
        baseline_daily_close_drawdown_rs=dd, policy_daily_close_drawdown_rs=dd,
        baseline_losing_days=int((daily<0).sum()), policy_losing_days=int((daily<0).sum()),
        additional_exit_cost_rs=0.)

results = pd.read_csv(ROOT/'policy_results.csv')
assert len(results) == 24
all_periods = {**periods, 'all50_mixed_fit_calibration_evaluation':days}
for record in results.to_dict('records'):
    session_days = all_periods[record['period']]
    expected = metrics(b[b.day.isin(session_days)], session_days)
    for key, val in expected.items():
        np.testing.assert_allclose(record[key], val, rtol=1e-12, atol=1e-8, err_msg=key)
for name in ['evaluation', 'calibration', 'unprotected_evaluation_diagnostic']:
    session_days = periods['calibration10' if name=='calibration' else 'evaluation15']
    for key, val in metrics(b[b.day.isin(session_days)], session_days).items():
        np.testing.assert_allclose(summary[name][key], val, rtol=1e-12, atol=1e-8)
for arm in ['protected', 'unprotected_diagnostic']:
    for bps in [0, 10, 20]:
        ledger = pd.read_csv(ROOT/f'trades_{arm}_{bps}bps.csv').set_index('sid').sort_index()
        baseline = b.set_index('sid').sort_index()
        assert len(ledger)==457 and list(ledger.index)==list(baseline.index)
        assert ledger.intervened.eq(False).all()
        np.testing.assert_allclose(ledger.policy_net_pnl_rs, baseline.portfolio_net_profit_rupees, atol=1e-9)
        assert ledger.incremental_net_rs.eq(0).all() and ledger.extra_exit_cost_rs.eq(0).all()
        assert ledger.policy_exit_reason.eq(baseline.exit_reason).all()
        assert ledger.policy_exit_at_ist.eq(baseline.exit_ts).all()
assert pd.read_csv(ROOT/'evaluation_exit_events.csv').empty
saved_daily = pd.read_csv(ROOT/'evaluation_daily_comparison.csv')
expected_daily = b[b.day.isin(days[35:])].groupby('day').portfolio_net_profit_rupees.sum().reindex(days[35:], fill_value=0)
assert list(saved_daily.day)==list(expected_daily.index)
np.testing.assert_allclose(saved_daily.baseline_net_rs, expected_daily.to_numpy(), atol=1e-8)
np.testing.assert_allclose(saved_daily.policy_net_rs, expected_daily.to_numpy(), atol=1e-8)
assert saved_daily.incremental_net_rs.eq(0).all() and saved_daily.exits.eq(0).all()
assert summary['evaluation_incremental_net_block_interval']['lower_95_rs']==0
assert summary['evaluation_incremental_net_block_interval']['upper_95_rs']==0
report = {'verified':True, 'method':'Independent augmented weighted least squares with unpenalized intercept; no experiment-module import',
    'manifest_files_checked':len(manifest['files']), 'metrics_rows_checked':len(results), 'ledgers_checked':6,
    'maximum_prediction_error_initial_r':float(np.max(np.abs(scores-p.score_initial_r))),
    'calibration_native_target_score_max':float(scores[cal_target].max()), 'protected_threshold_initial_r':threshold,
    'ranges':{name:{'rows':int(x.day.isin(d).sum()), 'minimum':float(scores[x.day.isin(d)].min()), 'maximum':float(scores[x.day.isin(d)].max()),
                   'label_positive_rows':int((target[x.day.isin(d)]>0).sum())} for name,d in periods.items()},
    'baseline_trades':len(b), 'baseline_net_rs':float(b.portfolio_net_profit_rupees.sum()),
    'final15':metrics(b[b.day.isin(days[35:])], days[35:]),
    'conclusion':'Confirmed zero model scores exceed even the unprotected zero threshold. Zero exits is a model outcome, not a replay-selection bug. All 24 rows reconcile to unchanged original executed cohort.'}
destination = Path(__file__).with_name('independent_verification.json')
destination.write_text(json.dumps(report, indent=2), encoding='utf-8')
print(json.dumps(report, indent=2))
