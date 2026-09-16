"""V13-v10-B: V10-A full exits with every configured SL at least 0.60%."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_a_backtest as a

DEFAULT_SOURCE_CONFIG = a.DEFAULT_OUTPUT / 'historical_fit/frozen_config.json'
DEFAULT_OUTPUT = a.DEFAULT_OUTPUT.parent.parent / 'v13_corrected_v10_b/run_20260913'


def adjusted_pair(pair: dict) -> dict:
    old_stop, old_target = float(pair['stop_pct']), float(pair['target_pct'])
    if old_stop >= .5:
        return dict(stop_pct=old_stop, target_pct=old_target)
    target = min(3., round(old_target / old_stop * .6, 2))
    return dict(stop_pct=.6, target_pct=target)


def transform(source: dict) -> dict:
    result = dict(version='V13-v10-B', default=adjusted_pair(source['default']),
                  setups={name: adjusted_pair(pair) for name, pair in source['setups'].items()},
                  partial_exits=False, breakeven_stop=False, source_version='V13-v10-A',
                  rule='If A stop <0.50%, set stop=0.60% and preserve A target:SL ratio, capped at 3.00%; otherwise unchanged.',
                  target_cap_pct=3., minimum_stop_pct=.6,
                  evidence='DETERMINISTIC_TRANSFORMATION_OF_FULL_HISTORY_FITTED_V10_A')
    for pair in [result['default'], *result['setups'].values()]:
        validate_pair(pair)
    return result


def validate_pair(pair):
    stop, target = float(pair['stop_pct']), float(pair['target_pct'])
    if not np.isfinite(stop) or stop < .6 or stop > 1.5:
        raise ValueError('V10-B SL must be between 0.60% and 1.50%')
    if not np.isfinite(target) or target <= 0 or target > 3.:
        raise ValueError('V10-B target must be positive and no greater than 3.00%')
    if target + 1e-12 < 1.5 * stop:
        raise ValueError('V10-B target:SL must be at least 1.5')


def evaluate(dataset, settings):
    orders, paths, base = dataset['orders'], dataset['paths'], dataset['v9_config']
    default = settings['default']
    for pair in [default, *settings['setups'].values()]:
        validate_pair(pair)
    configured = orders.copy()
    configured['native_stop_pct'] = configured.setup_id.map(
        {key:value['stop_pct'] for key,value in settings['setups'].items()}).fillna(default['stop_pct'])
    configured['native_target_pct'] = configured.setup_id.map(
        {key:value['target_pct'] for key,value in settings['setups'].items()}).fillna(default['target_pct'])
    a.v10.v9.validate_paths(configured, paths)
    trades = a.v10.v9.v5.simulate_native(configured, paths, cost_bps=base.cost_bps,
        max_entry_delay_minutes=a.v10.v9.v5.MAX_ENTRY_DELAY_MINUTES)
    trades = a.v10.v9.v5.apply_fixed_capital_model(
        trades, base.capital_per_entry_rupees, base.leverage_factor)
    trades['v10_b_stop_pct'] = trades['native_stop_pct']
    trades['v10_b_target_pct'] = trades['native_target_pct']
    trades['v10_b_reward_risk'] = trades.v10_b_target_pct / trades.v10_b_stop_pct
    ledger, summary = a.v10.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())
    summary.update(version='V13-v10-B', settings=settings, evidence=settings['evidence'],
                   partial_exits=False, breakeven_stop=False, square_off='15:15 Asia/Kolkata')
    return trades, ledger, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config-json', type=Path, default=DEFAULT_OUTPUT / 'frozen_config.json')
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT / 'cli_replay')
    args = parser.parse_args()
    dataset = a.v10.load_source()
    settings = json.loads(args.config_json.read_text(encoding='utf-8'))
    trades, ledger, summary = evaluate(dataset, settings)
    a.metrics.save(args.output_dir, trades, ledger, summary)
    print(json.dumps(a.metrics.metric(ledger, dataset['days']), indent=2))


if __name__ == '__main__':
    main()
