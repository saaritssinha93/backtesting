"""Independent reconstruction audit; does not execute or optimize any policy."""
from pathlib import Path
import json
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from research import g4_failed_breakout_research as sealed
from research import g4_g3_mirror_backtest as base
from research.g4_iterative_diagnostics import load_paths

ROOT = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_4/research')
RUN = ROOT / '20261016T162554_29716e_iterative_protection_v2'
DATA = ROOT / '20261016_iterative_sealed_dataset_v1'
MINUTE = 60_000_000_000
EPS = 1e-8


def stats(before, after, days, native):
    indices = native.index[native.portfolio_executed & native.day.isin(days)]
    b, a = before.loc[indices], after.loc[indices]
    wins = b > EPS
    losses = b < -EPS
    winner_profit = float(b[wins].sum())
    loss_amount = float(-b[losses].sum())
    new_loss = float(-a[a < 0].sum())
    daily = a.groupby(native.loc[indices, 'day']).sum().reindex(days, fill_value=0.)
    equity = np.r_[0., daily.cumsum().to_numpy()]
    return dict(trades=len(indices), wins=int((a > EPS).sum()), losses=int((a < -EPS).sum()),
        net_pnl_rs=float(a.sum()), net_change_rs=float((a - b).sum()),
        negative_trade_sum_abs_rs=new_loss, loss_saved_rs=loss_amount - new_loss,
        original_winner_profit_rs=winner_profit, original_winner_profit_after_rs=float(a[wins].sum()),
        original_winner_profit_retention_pct=float(100 * a[wins].sum() / winner_profit),
        original_winners_turned_losses=int((wins & (a < -EPS)).sum()),
        original_losers_turned_winners=int((losses & (a > EPS)).sum()),
        changed_trades=int(((a - b).abs() > 1e-7).sum()),
        losing_days=int((daily < -EPS).sum()),
        daily_close_drawdown_rs=float(np.max(np.maximum.accumulate(equity) - equity)),
        baseline_losses=int(losses.sum()), baseline_wins=int(wins.sum()))


def strict(m):
    return (m['original_winner_profit_retention_pct'] >= 95 - EPS and m['net_change_rs'] >= -1e-7
        and m['losses'] < m['baseline_losses'] and m['wins'] > m['baseline_wins']
        and m['loss_saved_rs'] > 1e-7 and m['changed_trades'] >= 3)


def main():
    summary = sealed.package(RUN)
    provenance = sealed.package(DATA)
    assert base.sha256(DATA / 'manifest.json') == summary['dataset_manifest_sha256']
    assert base.sha256(Path(provenance['original_source']) / 'manifest.json') == summary['original_manifest_sha256']
    sealed.package(Path(provenance['original_source']))
    native = base.read_frozen_csv(DATA / 'native.csv').set_index('sid')
    native['day'] = native.day.astype(str).str[:10]
    rows = native.to_dict('index')
    paths = load_paths(DATA / 'historical_paths.npz')
    config = json.loads((RUN / 'config.json').read_text())
    policies = {x['id']: x for x in config['policy_catalog']}
    matrix = base.read_frozen_csv(RUN / 'candidate_net_returns.csv').set_index('sid')
    assert set(matrix.columns) == set(policies)
    baseline = native.net_return_pct.fillna(0.) * 5000.
    expected = pd.DataFrame({name: baseline for name in matrix.columns})
    events = base.read_frozen_csv(RUN / 'policy_events.csv')
    for e in events.to_dict('records'):
        sid, i = int(e['sid']), int(e['execution_path_index'])
        row, path = rows[sid], paths[sid]
        decision = pd.Timestamp(e['decision_at_ist'])
        opened = pd.Timestamp(e['next_bar_open_at_ist'])
        reserved = pd.Timestamp(e['modeled_execution_at_ist'])
        assert i > int(row['entry_path_index']) + 1
        assert decision.value == path['timestamp_ns'][i - 1]
        assert opened.value == path['timestamp_ns'][i] - MINUTE
        assert reserved.value == path['timestamp_ns'][i]
        assert decision - pd.Timedelta(minutes=1) >= pd.Timestamp(row['entry_ts'])
        assert opened < pd.Timestamp(row['exit_ts'])
        assert e['next_bar_open_price'] == path['open'][i]
        sign = 1 if row['side'] == 'LONG' else -1
        expected.loc[sid, e['policy']] = (sign * (path['open'][i] / row['entry_price'] - 1) * 100 - .05) * 5000.
    stops = base.read_frozen_csv(RUN / 'protective_stop_exits.csv')
    for e in stops.to_dict('records'):
        sid, i = int(e['sid']), int(e['exit_path_index'])
        row, path = rows[sid], paths[sid]
        p = policies[e['policy']]['params']
        sign, entry = (1 if row['side'] == 'LONG' else -1), float(row['entry_price'])
        end = pd.Timestamp(int(path['timestamp_ns'][i]), tz='UTC')
        actual = end - pd.Timedelta(minutes=1) if e['exit_event'] == 'OPEN' else end
        assert actual == pd.Timestamp(e['exit_execution_ts']) == pd.Timestamp(e['exit_ts'])
        assert end == pd.Timestamp(e['exit_bar_end_ts'])
        active_stop = 1.2
        if path['timestamp_ns'][i] - MINUTE >= pd.Timestamp(row['entry_ts']).value + p['tighten_after_minutes'] * MINUTE:
            active_stop = min(active_stop, p['tightened_stop_pct'])
        if bool(e['protection_applied']):
            arm, effective = int(e['protection_armed_path_index']), int(e['protection_effective_path_index'])
            assert arm > int(row['entry_path_index']) and effective == arm + 1 and i >= effective
            assert path['timestamp_ns'][arm] - MINUTE >= pd.Timestamp(row['entry_ts']).value
            activation = max(p['activation_floor_pct'], p['target_fraction'] * row['native_target_pct'])
            assert sign * (path['close'][arm] / entry - 1) * 100 >= activation - 1e-10
            active_stop = min(active_stop, -p['protected_profit_pct'])
        assert abs(active_stop - e['active_stop_pct_at_exit']) < 1e-12
        if e['exit_reason'] in ('STOP', 'TIGHTENED_STOP', 'PROTECTED_STOP'):
            price = path['open'][i] if e['exit_event'] == 'OPEN' else entry * (1 - sign * active_stop / 100)
            assert abs(price - e['exit_price']) < 1e-10
        elif e['exit_reason'] == 'TARGET':
            assert abs(e['exit_price'] - entry * (1 + sign * row['native_target_pct'] / 100)) < 1e-10
        gross = sign * (e['exit_price'] / entry - 1) * 100
        assert abs(gross - e['gross_return_pct']) < 1e-10
        expected.loc[sid, e['policy']] = (gross - .05) * 5000.
    partial_count = 0
    for name, item in policies.items():
        if item['kind'] == 'partial_exit':
            f, parent = item['params']['fraction'], item['params']['parent']
            expected[name] = f * expected[parent] + (1 - f) * baseline
            partial_count += 1
    expected = expected.reindex(index=matrix.index, columns=matrix.columns)
    max_error = float(np.max(np.abs(expected.to_numpy() - matrix.to_numpy())))
    assert max_error < 1e-6
    table = base.read_frozen_csv(RUN / 'comparison.csv')
    days = summary['days']
    periods = dict(all=days, development35=days[:35], later17=days[35:])
    audited = []
    for period, group in periods.items():
        for item in table.loc[table.period.eq(period) & table.policy.isin(policies)].to_dict('records'):
            actual = stats(baseline, expected[item['policy']], group, native)
            for key, value in actual.items():
                assert np.isclose(value, item[key], rtol=0, atol=1e-6), (period, item['policy'], key, value, item[key])
            assert strict(actual) == bool(item['qualifies'])
            audited.append(dict(period=period, policy=item['policy'], **actual))
    dev = pd.DataFrame(audited).loc[lambda x: x.period.eq('development35')]
    ordering = ['loss_saved_rs', 'net_change_rs', 'policy']
    qualify = dev.loc[dev.apply(strict, axis=1)].sort_values(ordering, ascending=[False, False, True])
    choice = 'baseline' if qualify.empty else qualify.iloc[0].policy
    acceptable = dev.loc[(dev.original_winner_profit_retention_pct >= 95 - EPS) & (dev.loss_saved_rs > 1e-7) & (dev.changed_trades >= 3)]
    acceptable = acceptable.sort_values(ordering, ascending=[False, False, True])
    tradeoff = 'baseline' if acceptable.empty else acceptable.iloc[0].policy
    assert choice == summary['development_selected_policy']
    assert tradeoff == summary['development_tradeoff_policy']
    portfolio_checks = []
    for file in (RUN / 'ledgers').glob('*.csv'):
        book = base.read_frozen_csv(file).set_index('sid')
        name = file.stem
        assert set(book.index) == set(native.index)
        assert np.allclose(book.net_profit_rupees, expected[name].reindex(book.index), atol=1e-6, rtol=0)
        assert np.allclose(book.loc[book.portfolio_executed, 'portfolio_cost_rupees'], 250., atol=1e-8, rtol=0)
        if policies[name]['kind'] == 'partial_exit':
            assert pd.to_datetime(book.exit_ts, utc=True).equals(pd.to_datetime(native.reindex(book.index).exit_ts, utc=True))
            assert book.portfolio_executed.equals(native.reindex(book.index).portfolio_executed)
            for row in book.loc[book.partial_exit_fraction.gt(0)].itertuples():
                legs = json.loads(row.partial_leg_audit)
                assert len(legs) == 2 and abs(sum(x['cost_rs'] for x in legs) - 250.) < 1e-8
                assert abs(sum(x['net_profit_rs'] for x in legs) - row.net_profit_rupees) < 1e-6
        working = book.loc[book.filled].copy()
        working['entry_order'] = pd.to_datetime(working.entry_ts, utc=True)
        working['confirmation_order'] = pd.to_datetime(working.confirmation_ts, utc=True)
        def priority(row):
            field = {'max_liquidity':'traded_value', 'max_volume':'volume_ratio', 'max_move':'price_change_pct'}.get(row.picker)
            value = row.get(field, 0.) if field else 0.
            value = 0. if pd.isna(value) else float(value)
            return abs(value) if row.picker == 'max_move' else value
        working['independent_priority'] = working.apply(priority, axis=1)
        working['sid_order'] = working.index
        working = working.sort_values(['entry_order','confirmation_order','setup_id','independent_priority','tradingsymbol','sid_order'], ascending=[True,True,True,False,True,True],kind='stable')
        active, accepted = [], set()
        for sid, row in working.iterrows():
            active = [exit_at for exit_at in active if exit_at > row.entry_order]
            if len(active) < 10:
                accepted.add(sid)
                active.append(pd.Timestamp(row.exit_ts))
        assert accepted == set(book.index[book.portfolio_executed])
        assert np.allclose(book.portfolio_net_profit_rupees, np.where(book.portfolio_executed, expected[name].reindex(book.index), 0.),atol=1e-6,rtol=0)
        portfolio_checks.append(dict(policy=name, allocated=len(accepted), newly_admitted=len(accepted - set(native.index[native.portfolio_executed]))))
    result = dict(run=str(RUN), manifest_sha256=base.sha256(RUN / 'manifest.json'),
        candidate_cells=matrix.size, policies=len(policies), close_events=len(events),
        changed_stop_outcomes=len(stops), partial_policies=partial_count, maximum_reconstruction_error_rs=max_error,
        checked_policy_period_rows=len(audited), strict_selected=choice, tradeoff_selected=tradeoff,
        portfolio_checks=portfolio_checks)
    target = Path(__file__).with_name('g4_refinement_independent_audit.json')
    target.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
