"""V13-v10-A: V10 entries with a fixed full-exit target/SL per setup."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

import fno_v13_v10_backtest as v10
import fno_v13_v10_research as metrics

DEFAULT_OUTPUT = v10.DEFAULT_OUTPUT.parent.parent / 'v13_corrected_v10_a/run_20260913'


def exit_config(pair):
    stop, target = float(pair['stop_pct']), float(pair['target_pct'])
    cfg = v10.V10Config(initial_stop_pct=stop, first_target_pct=target,
                       runner_target_pct=target, partial_pct=1., runner_stop='INITIAL')
    cfg.validate()
    if target + 1e-12 < 1.5 * stop:
        raise ValueError('Target:SL must be at least 1.5')
    return cfg


def evaluate(dataset, settings):
    """Assign by setup known at selection; never by stock/day or future outcome."""
    orders, paths, base = dataset['orders'], dataset['paths'], dataset['v9_config']
    v10.v9.validate_paths(orders, paths)
    default = settings['default']
    exit_config(default)
    for pair in settings['setups'].values():
        exit_config(pair)
    parts = []
    for setup, rows in orders.groupby('setup_id', sort=True):
        cfg = exit_config(settings['setups'].get(setup, default))
        part = v10.simulate(rows, paths, cfg)
        part['v10_a_stop_pct'] = cfg.initial_stop_pct
        part['v10_a_target_pct'] = cfg.runner_target_pct
        part['v10_a_reward_risk'] = cfg.runner_target_pct / cfg.initial_stop_pct
        parts.append(part)
    trades = pd.concat(parts, ignore_index=True)
    trades = trades.set_index('sid').loc[orders.sid].reset_index()
    trades = v10.v9.v5.apply_fixed_capital_model(trades, base.capital_per_entry_rupees, base.leverage_factor)
    ledger, summary = v10.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())
    summary.update(version='V13-v10-A', settings=settings, evidence='PREVIOUSLY_SEEN_HISTORY_RESEARCH',
                   partial_exits=False, breakeven_stop=False, square_off='15:15 Asia/Kolkata')
    return trades, ledger, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config-json', type=Path,
                        default=DEFAULT_OUTPUT / 'historical_fit/frozen_config.json')
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT / 'cli_replay')
    args = parser.parse_args()
    dataset = v10.load_source()
    settings = json.loads(args.config_json.read_text(encoding='utf-8'))
    trades, ledger, summary = evaluate(dataset, settings)
    metrics.save(args.output_dir, trades, ledger, summary)
    print(json.dumps(metrics.metric(ledger, dataset['days']), indent=2))


if __name__ == '__main__':
    main()
