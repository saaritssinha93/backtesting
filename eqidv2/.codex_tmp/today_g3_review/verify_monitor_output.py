"""Independently reconcile a generated monitor run without importing its metrics."""
from pathlib import Path
import hashlib
import json
import sys

import numpy as np
import pandas as pd


def flag(series):
    return series.astype(str).str.lower().eq('true')


def main(directory):
    root = Path(directory)
    summary = json.loads((root / 'summary.json').read_text())
    manifest = json.loads((root / 'manifest.json').read_text())
    for name, expected in manifest['files'].items():
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == expected, name
    metrics = pd.read_csv(root / 'policy_results.csv')
    daily = pd.read_csv(root / 'daily_comparison.csv')
    comparison = pd.read_csv(root / 'trade_comparison.csv')
    baseline = pd.read_csv(root / 'ledgers/baseline.csv').set_index('sid')
    days = summary['days']
    output = []
    for policy in ['baseline', 'trend_failure', 'profit_giveback', 'combined']:
        ledger = pd.read_csv(root / 'ledgers' / f'{policy}.csv').set_index('sid')
        assert set(ledger.index) == set(baseline.index)
        ledger = ledger.reindex(baseline.index)
        executed = flag(ledger.portfolio_executed)
        original_executed = flag(baseline.portfolio_executed)
        changed = flag(ledger.monitor_exit)
        pnl = ledger.portfolio_net_profit_rupees
        original_pnl = baseline.portfolio_net_profit_rupees
        assert np.allclose(pnl[~executed], 0)
        assert np.allclose(pnl, ledger.portfolio_gross_profit_rupees-ledger.portfolio_cost_rupees)
        assert ledger.entry_ts.fillna('').equals(baseline.entry_ts.fillna(''))
        assert np.allclose(ledger.entry_price, baseline.entry_price, equal_nan=True)
        modified = ledger[changed]
        if len(modified):
            decision = pd.to_datetime(modified.monitor_decision_at_ist, utc=True)
            opened = pd.to_datetime(modified.exit_execution_ts, utc=True)
            proxy = pd.to_datetime(modified.exit_ts, utc=True)
            assert (opened >= decision).all()
            assert ((proxy-opened) == pd.Timedelta(minutes=1)).all()
            assert (opened < pd.to_datetime(baseline.loc[modified.index, 'exit_ts'], utc=True)).all()
            gross = modified.side.map({'LONG': 1, 'SHORT': -1})*(modified.exit_price/modified.entry_price-1)*100
            assert np.allclose(gross, modified.gross_return_pct)
            assert np.allclose(gross-.05, modified.net_return_pct)
        common = executed & original_executed
        admitted = executed & ~original_executed
        rejected = ~executed & original_executed
        trade_delta = pnl-original_pnl
        common_delta = trade_delta[common].sum()
        admission_delta = pnl[admitted].sum()-original_pnl[rejected].sum()
        assert np.isclose(trade_delta.sum(), common_delta+admission_delta)
        pnl_by_day = ledger.groupby('day').portfolio_net_profit_rupees.sum().reindex(days, fill_value=0)
        curve = np.r_[0, pnl_by_day.cumsum()]
        mdd = (np.maximum.accumulate(curve)-curve).max()
        expected = metrics.loc[(metrics.period == 'all') & (metrics.policy == policy)].iloc[0]
        checks = {'executed_trades': int(executed.sum()), 'wins': int((executed & (pnl>0)).sum()),
                  'losses': int((executed & (pnl<0)).sum()), 'net_pnl_rs': pnl.sum(),
                  'costs_rs': ledger.portfolio_cost_rupees.sum(), 'max_drawdown_rs': mdd,
                  'newly_admitted': int(admitted.sum()), 'newly_rejected': int(rejected.sum())}
        for key, actual in checks.items():
            assert np.isclose(actual, expected[key], atol=1e-6), (policy, key, actual, expected[key])
        reported_daily = daily.loc[daily.policy == policy].set_index('day').reindex(days)
        assert np.allclose(pnl_by_day, reported_daily.net_pnl_rs)
        comp = comparison[comparison.policy == policy].set_index('sid').reindex(baseline.index)
        assert np.allclose(comp.net_pnl_delta_rs, trade_delta)
        stops = ledger.exit_reason.isin(['STOP', 'TIGHTENED_STOP']) & executed
        data = dict(policy=policy, **checks,
                    stop_exits=int(stops.sum()), losing_days=int((pnl_by_day<0).sum()),
                    monitor_exits=int((changed & executed).sum()),
                    monitor_exits_negative=int((changed & executed & (pnl<0)).sum()),
                    negative_monitor_pnl=float(pnl[changed & executed & (pnl<0)].sum()),
                    common_trade_delta=float(common_delta), admission_delta=float(admission_delta),
                    total_delta=float(trade_delta.sum()),
                    gross_winner_profit_cut=float((-trade_delta[common & changed & (original_pnl>0) & (trade_delta<0)]).sum()),
                    winners_turned_nonpositive=int((common & changed & (original_pnl>0) & (pnl<=0)).sum()),
                    average_loss=float(pnl[executed & (pnl<0)].mean()),
                    worst_trade=float(pnl[executed].min()),
                    today_pnl=float(pnl_by_day.iloc[-1]))
        output.append(data)
    print(json.dumps({'independent_reconciliation': 'PASS', 'run': str(root), 'policies': output}, indent=2))


if __name__ == '__main__':
    main(sys.argv[1])
