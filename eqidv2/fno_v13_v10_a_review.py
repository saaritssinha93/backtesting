"""Reconcile frozen V10-A results, independent CLI replay and report artifacts."""
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_a_backtest as e


def main():
    root = e.DEFAULT_OUTPUT
    active = root / 'historical_fit'
    config = json.loads((active / 'frozen_config.json').read_text())
    for pair in [config['default'], *config['setups'].values()]:
        e.exit_config(pair)
    original = pd.read_csv(active / 'final/V13_V10_A/portfolio_trades.csv', float_precision='round_trip')
    cli = pd.read_csv(root / 'cli_replay/portfolio_trades.csv', float_precision='round_trip')
    parity = e.v10.v9.assert_control_parity(original, cli)
    a, b = original.sort_values('sid').reset_index(drop=True), cli.sort_values('sid').reset_index(drop=True)
    for col in ['filled', 'portfolio_executed', 'entry_ts', 'exit_ts', 'exit_reason', 'partial_pct',
                'runner_stop', 'v10_a_stop_pct', 'v10_a_target_pct']:
        pd.testing.assert_series_equal(a[col], b[col])
    filled = a.loc[a.filled]
    assert filled.partial_pct.eq(1).all() and filled.runner_stop.eq('INITIAL').all()
    assert not filled.exit_reason.str.contains('T1_THEN|BREAKEVEN').any()
    final_metrics = pd.read_csv(active / 'comparison_metrics.csv')
    daily = pd.read_csv(active / 'daily_results.csv')
    for version in final_metrics.version.unique():
        full = final_metrics.loc[final_metrics.version.eq(version) & final_metrics.period.eq('FULL') & final_metrics.cost_bps.eq(5)].iloc[0]
        d = daily.loc[daily.version.eq(version)]
        np.testing.assert_allclose(d.net_profit_rupees.sum(), full.net_profit_rupees, rtol=0, atol=1e-7)
        assert d.trades.sum() == full.trades
        assert d.wins.sum() == full.wins
        monthly = final_metrics.loc[final_metrics.version.eq(version) & final_metrics.period.isin(['JULY', 'AUGUST', 'SEPTEMBER']) & final_metrics.cost_bps.eq(5)]
        np.testing.assert_allclose(monthly.net_profit_rupees.sum(), full.net_profit_rupees, rtol=0, atol=1e-7)
    active_full = final_metrics.loc[final_metrics.version.eq('V10-A') & final_metrics.period.eq('FULL') & final_metrics.cost_bps.eq(5)].iloc[0]
    assert active_full.win_rate_pct >= 65
    source_baseline = pd.read_csv(e.v10.DEFAULT_OUTPUT / 'balanced/final/V13_V10/portfolio_trades.csv')
    assert set(a.sid) == set(source_baseline.sid)
    exits = original.loc[original.portfolio_executed].groupby('exit_reason').agg(
        trades=('sid', 'size'), net_profit_rupees=('portfolio_net_profit_rupees', 'sum'))
    exits.to_csv(active / 'exit_reason_summary.csv')
    checks = dict(independent_cli_parity=parity, cli_exit_and_allocation_equal=True,
                  same_115_selected_orders=True, full_exit_only=True, daily_and_monthly_reconciled=True,
                  historical_win_objective_met=True, evidence='FULL_HISTORY_IN_SAMPLE_FIT')
    e.metrics.dump(active / 'final_review.json', checks)
    sources = list(Path('.').glob('fno_v13_v10_a*.py')) + [Path('tests/test_fno_v13_v10_a.py')]
    for folder in [active, root]:
        snapshot = folder / 'source_snapshot'
        snapshot.mkdir(exist_ok=True)
        for source in sources:
            shutil.copy2(source, snapshot / source.name)
        manifest_path = folder / 'research_manifest.json'
        manifest = json.loads(manifest_path.read_text())
        for name, checksum in manifest['code_sha256'].items():
            if e.v10.sha(Path(name)) != checksum:
                raise RuntimeError(f'Code changed since research run: {name}')
        for source in sources:
            manifest['code_sha256'][str(source.resolve())] = e.v10.sha(source)
        manifest['artifacts'] = {str(p.relative_to(folder)): e.v10.sha(p) for p in sorted(folder.rglob('*'))
                                 if p.is_file() and p != manifest_path}
        e.metrics.dump(manifest_path, manifest)
    print(json.dumps(checks, indent=2))
    print(final_metrics.loc[final_metrics.version.isin(['V10', 'V10-A']) & final_metrics.period.isin(['FULL', 'JULY', 'AUGUST', 'SEPTEMBER']) & final_metrics.cost_bps.eq(5),
                           ['version', 'period', 'trades', 'wins', 'losses', 'win_rate_pct', 'profit_factor', 'net_profit_rupees']].to_string(index=False))


if __name__ == '__main__':
    main()
