"""V13-v10-C: preserve every V10-B SL and use a full target exactly twice SL."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import fno_v13_v10_b_backtest as b

DEFAULT_SOURCE_CONFIG = b.DEFAULT_OUTPUT / 'frozen_config.json'
DEFAULT_OUTPUT = b.DEFAULT_OUTPUT.parent.parent / 'v13_corrected_v10_c/run_20260913'


def adjusted_pair(pair: dict) -> dict:
    stop = float(pair['stop_pct'])
    return dict(stop_pct=stop, target_pct=round(2 * stop, 2))


def transform(source: dict) -> dict:
    result = dict(version='V13-v10-C', default=adjusted_pair(source['default']),
                  setups={name: adjusted_pair(pair) for name, pair in source['setups'].items()},
                  partial_exits=False, breakeven_stop=False, source_version='V13-v10-B',
                  rule='Keep every V10-B stop unchanged; set full target to exactly 2x SL.',
                  reward_risk=2., evidence='DETERMINISTIC_TRANSFORMATION_OF_FULL_HISTORY_DERIVED_V10_B')
    for pair in [result['default'], *result['setups'].values()]:
        b.validate_pair(pair)
        if abs(pair['target_pct'] / pair['stop_pct'] - 2) > 1e-12:
            raise ValueError('V10-C target must equal exactly 2x SL')
    return result


def evaluate(dataset, settings):
    trades, ledger, summary = b.evaluate(dataset, settings)
    mapping = {'v10_b_stop_pct':'v10_c_stop_pct', 'v10_b_target_pct':'v10_c_target_pct',
               'v10_b_reward_risk':'v10_c_reward_risk'}
    trades = trades.rename(columns=mapping)
    ledger = ledger.rename(columns=mapping)
    summary.update(version='V13-v10-C', evidence=settings['evidence'])
    return trades, ledger, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config-json', type=Path, default=DEFAULT_OUTPUT / 'frozen_config.json')
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT / 'cli_replay')
    args = parser.parse_args()
    dataset = b.a.v10.load_source()
    settings = json.loads(args.config_json.read_text(encoding='utf-8'))
    trades, ledger, summary = evaluate(dataset, settings)
    b.a.metrics.save(args.output_dir, trades, ledger, summary)
    print(json.dumps(b.a.metrics.metric(ledger, dataset['days']), indent=2))


if __name__ == '__main__':
    main()
