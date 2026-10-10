"""V13-v10-D: V10-C exits with a causal 1m confirmation-volume filter."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_c_backtest as c

v10, v9, r = c.b.a.v10, c.b.a.v10.v9, c.b.a.metrics
DEFAULT_SOURCE = v10.DEFAULT_SOURCE
DEFAULT_SOURCE_CONFIG = c.DEFAULT_OUTPUT / 'frozen_config.json'
DEFAULT_OUTPUT = c.DEFAULT_OUTPUT.parent.parent / 'v13_corrected_v10_d/run_20260913'


def config(source_exit: dict, minimum_volume_ratio: float = 1.2) -> dict:
    if not np.isfinite(minimum_volume_ratio) or minimum_volume_ratio <= 0:
        raise ValueError('Minimum confirmation volume ratio must be positive and finite')
    for pair in [source_exit['default'], *source_exit['setups'].values()]:
        c.b.validate_pair(pair)
        if abs(pair['target_pct'] - 2 * pair['stop_pct']) > 1e-12:
            raise ValueError('V10-D preserves V10-C target=2xSL exits')
    return dict(version='V13-v10-D', minimum_confirmation_1m_volume_ratio=float(minimum_volume_ratio),
                exit=source_exit, entry_expiry_minutes=10,
                selection_rule='Require causal completed 1m confirmation volume / prior-20-bar mean >=1.20 before native setup ranking.',
                evidence='REQUESTED_PREVIOUSLY_SEEN_HISTORY_SENSITIVITY')


def select_orders(signals: pd.DataFrame, base: v9.V9Config, minimum_volume_ratio: float) -> pd.DataFrame:
    volume = pd.to_numeric(signals['v9_1m_volume_ratio'], errors='coerce')
    return v9.select_orders(signals.loc[volume.ge(minimum_volume_ratio)].copy(), base)


def load_source(source: Path = DEFAULT_SOURCE, minimum_volume_ratio: float = 1.2) -> dict:
    data = v10.load_source(source)  # Verifies the frozen manifest and all dataset hashes.
    signals = pd.read_parquet(source / 'dataset/signals.parquet')
    orders = select_orders(signals, data['v9_config'], minimum_volume_ratio)
    paths = {}
    with np.load(source / 'dataset/paths.npz', allow_pickle=False) as archive:
        for sid in orders.sid.astype(int):
            paths[sid] = {field: archive[f'{sid}_{field}']
                          for field in ('timestamp_ns','open','high','low','close')}
    v9.validate_paths(orders, paths)
    return {**data, 'signals':signals, 'orders':orders, 'paths':paths}


def evaluate(dataset: dict, settings: dict):
    threshold = float(settings['minimum_confirmation_1m_volume_ratio'])
    configured = config(settings['exit'], threshold)
    if int(settings.get('entry_expiry_minutes', 10)) != 10:
        raise ValueError('Frozen V10-D uses the unchanged 10-minute entry expiry')
    trades, ledger, summary = c.evaluate(dataset, configured['exit'])
    mapping = {'v10_c_stop_pct':'v10_d_stop_pct','v10_c_target_pct':'v10_d_target_pct',
               'v10_c_reward_risk':'v10_d_reward_risk'}
    trades, ledger = trades.rename(columns=mapping), ledger.rename(columns=mapping)
    trades['v10_d_confirmation_volume_ratio_minimum'] = threshold
    ledger['v10_d_confirmation_volume_ratio_minimum'] = threshold
    summary.update(version='V13-v10-D', settings=configured, evidence=configured['evidence'],
                   minimum_confirmation_1m_volume_ratio=threshold)
    return trades, ledger, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-dir',type=Path,default=DEFAULT_SOURCE)
    parser.add_argument('--config-json',type=Path,default=DEFAULT_OUTPUT/'frozen_config.json')
    parser.add_argument('--output-dir',type=Path,default=DEFAULT_OUTPUT/'cli_replay')
    args=parser.parse_args()
    settings=json.loads(args.config_json.read_text(encoding='utf-8'))
    dataset=load_source(args.source_dir,settings['minimum_confirmation_1m_volume_ratio'])
    trades,ledger,summary=evaluate(dataset,settings)
    r.save(args.output_dir,trades,ledger,summary)
    print(json.dumps(r.metric(ledger,dataset['days']),indent=2))


if __name__=='__main__':
    main()
