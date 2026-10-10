"""V13-v10-E: V10-B exits with a causal 1m confirmation-volume filter."""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path

import numpy as np

import fno_v13_v10_d_backtest as d

b, v9, r = d.c.b, d.v9, d.r
DEFAULT_SOURCE = d.DEFAULT_SOURCE
DEFAULT_SOURCE_CONFIG = b.DEFAULT_OUTPUT / 'frozen_config.json'
DEFAULT_OUTPUT = b.DEFAULT_OUTPUT.parent.parent / 'v13_corrected_v10_e/run_20260913'


def config(source_exit: dict, minimum_volume_ratio: float = 1.2) -> dict:
    if not np.isfinite(minimum_volume_ratio) or minimum_volume_ratio <= 0:
        raise ValueError('Minimum confirmation volume ratio must be positive and finite')
    frozen_exit = copy.deepcopy(source_exit)
    for pair in [frozen_exit['default'], *frozen_exit['setups'].values()]:
        b.validate_pair(pair)
    return dict(
        version='V13-v10-E',
        minimum_confirmation_1m_volume_ratio=float(minimum_volume_ratio),
        exit=frozen_exit,
        entry_expiry_minutes=10,
        selection_rule=(
            'Require causal completed 1m confirmation volume / prior-20-bar mean '
            '>=1.20 before native setup ranking.'
        ),
        source_version='V13-v10-B',
        evidence='V10_B_EXITS_WITH_PREVIOUSLY_REVIEWED_VOLUME_FILTER',
    )


def load_source(source: Path = DEFAULT_SOURCE, settings: dict | None = None) -> dict:
    if settings is None:
        source_exit = json.loads(DEFAULT_SOURCE_CONFIG.read_text(encoding='utf-8'))
        settings = config(source_exit)
    return d.load_source(source, float(settings['minimum_confirmation_1m_volume_ratio']))


def evaluate(dataset: dict, settings: dict):
    checked = config(settings['exit'], float(settings['minimum_confirmation_1m_volume_ratio']))
    if int(settings.get('entry_expiry_minutes', 10)) != 10:
        raise ValueError('V10-E preserves the ten-minute entry expiry')
    trades, ledger, summary = b.evaluate(dataset, checked['exit'])
    mapping = {
        'v10_b_stop_pct': 'v10_e_stop_pct',
        'v10_b_target_pct': 'v10_e_target_pct',
        'v10_b_reward_risk': 'v10_e_reward_risk',
    }
    trades, ledger = trades.rename(columns=mapping), ledger.rename(columns=mapping)
    threshold = checked['minimum_confirmation_1m_volume_ratio']
    trades['v10_e_confirmation_volume_ratio_minimum'] = threshold
    ledger['v10_e_confirmation_volume_ratio_minimum'] = threshold
    summary.update(
        version='V13-v10-E', settings=checked, evidence=checked['evidence'],
        minimum_confirmation_1m_volume_ratio=threshold,
    )
    return trades, ledger, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-dir', type=Path, default=DEFAULT_SOURCE)
    parser.add_argument('--config-json', type=Path, default=DEFAULT_OUTPUT / 'frozen_config.json')
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT / 'cli_replay')
    args = parser.parse_args()
    settings = json.loads(args.config_json.read_text(encoding='utf-8'))
    dataset = load_source(args.source_dir, settings)
    trades, ledger, summary = evaluate(dataset, settings)
    r.save(args.output_dir, trades, ledger, summary)
    print(json.dumps(r.metric(ledger, dataset['days']), indent=2))


if __name__ == '__main__':
    main()
