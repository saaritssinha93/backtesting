"""Seal an H-only G reference bundle from a completed V13-v9 dataset.

The historical G configuration is read from its pinned file. This tool has
no authority to alter G research, schedulers, dashboards, or broker orders.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import shutil
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd

import fno_v13_v10_g_backtest as g
from ai_platform.research import v13h_research as h


def build(root: Path, old_bundle: Path | None = None) -> Path:
    root = root.resolve()
    dataset = root / 'dataset'
    if (root / 'bundle_manifest.json').exists():
        raise FileExistsError('Bundle is already sealed')
    manifest = h.read_json(dataset / 'dataset_manifest.json')
    for name, expected in manifest['output_sha256'].items():
        if h.sha(dataset / name) != expected:
            raise ValueError(f'Unverified dataset artifact: {name}')
    index_dir = root / 'index_5m'
    for record in manifest['sources']:
        source_path = Path(record['path'])
        if not (source_path.name.startswith('NIFTY') and
                source_path.name.endswith('FUT_5minute.parquet')):
            continue
        if not source_path.is_file() or h.sha(source_path) != record['sha256']:
            # The sealed historical dataset remains usable. The relative
            # strength trial will report this optional source unavailable.
            continue
        index_dir.mkdir(exist_ok=True)
        snapshot = index_dir / source_path.name
        shutil.copy2(source_path, snapshot)
        if h.sha(snapshot) != record['sha256']:
            raise ValueError(f'NIFTY snapshot copy mismatch: {snapshot}')
    cutoff = manifest['through_day']
    days = list(manifest['days'])
    if not days or max(days) != cutoff:
        raise ValueError('Dataset day roster does not reach declared cutoff')
    config_path = (Path(h.read_json(old_bundle / 'g_backtest/run_metadata.json')['frozen_g_config'])
                   if old_bundle is not None else g.EXPANSION_OUTPUT / 'frozen_config.json')
    if h.sha(config_path) != h.G_CONFIG_SHA:
        raise ValueError('G configuration identity changed')
    settings = g.checked_settings(h.read_json(config_path))
    signals = pd.read_parquet(dataset / 'signals.parquet')
    base = g.v9.V9Config(portfolio_capital_rupees=1_000_000.,
                         capital_per_entry_rupees=100_000., leverage_factor=5.,
                         max_positions=None, cost_bps=5.)
    selection = g.selection_audit(signals, base, g.SelectionChange(**settings['selection_change']),
                                  core_first=True)
    orders = selection.loc[selection.v9_selected].copy().reset_index(drop=True)
    with np.load(dataset / 'paths.npz', allow_pickle=False) as archive:
        paths = {int(sid): {key: archive[f'{int(sid)}_{key}'].copy()
                            for key in ('timestamp_ns', 'open', 'high', 'low', 'close')}
                 for sid in orders.sid}
    g.v9.validate_paths(orders, paths)
    _, ledger, summary = g.evaluate(dict(signals=signals, orders=orders,
                                         paths=paths, v9_config=base), settings)
    full_metrics = g.r.metric(ledger, days)
    if not math.isclose(full_metrics['net_profit_rupees'], summary['net_profit_rupees'],
                        rel_tol=0, abs_tol=1e-7):
        raise ValueError('G summary and portfolio metric disagree')
    if old_bundle is not None:
        old_bundle = old_bundle.resolve()
        old = pd.read_csv(old_bundle / 'g_backtest/portfolio_trades.csv')
        current = ledger.loc[ledger.day.astype(str).le(str(old.day.astype(str).max()))].copy().reset_index(drop=True)
        keys = ['day', 'hhmm_int', 'side', 'setup_id', 'tradingsymbol']
        old['day'] = old.day.astype(str)
        current['day'] = current.day.astype(str)
        if old[keys].to_records(index=False).tolist() != current[keys].to_records(index=False).tolist():
            raise ValueError('Historical G selections drifted before new cutoff')
        for key in ('filled', 'portfolio_executed', 'exit_reason'):
            if old[key].astype(str).str.lower().tolist() != current[key].astype(str).str.lower().tolist():
                raise ValueError(f'Historical G execution state drifted: {key}')
        for key in ('entry_ts', 'exit_ts'):
            if not pd.to_datetime(old[key], utc=True).equals(pd.to_datetime(current[key], utc=True)):
                raise ValueError(f'Historical G execution clock drifted: {key}')
        for key in ('entry_price', 'exit_price', 'portfolio_net_profit_rupees',
                    'portfolio_cost_rupees'):
            if not np.allclose(pd.to_numeric(old[key]), pd.to_numeric(current[key]),
                               rtol=1e-10, atol=1e-7, equal_nan=True):
                raise ValueError(f'Historical G results drifted: {key}')
    output = root / 'g_backtest'
    output.mkdir(exist_ok=False)
    ledger.to_csv(output / 'portfolio_trades.csv', index=False)
    h.write_json(output / 'summary.json', summary)
    h.write_json(output / 'run_metadata.json', dict(
        through_day=cutoff, first_session=min(days), last_session=max(days),
        frozen_g_config=str(config_path.resolve()), frozen_g_config_sha256=h.G_CONFIG_SHA,
        metrics={'full_history': full_metrics},
        source_kind='H_ONLY_G_CONTROL_FROM_VERIFIED_CAUSAL_DATASET',
        historical_overlap_parity=old_bundle is not None))
    artifacts = {p.relative_to(root).as_posix(): {'sha256': h.sha(p), 'bytes': p.stat().st_size}
                 for p in sorted(root.rglob('*')) if p.is_file() and p.name != 'bundle_manifest.json'}
    h.write_json(root / 'bundle_manifest.json', dict(
        schema='V13_V10_H_SOURCE_BUNDLE_V1', state='COMPLETE',
        through_day=cutoff, artifacts=artifacts, execution_authority=False))
    # Read the sealed bundle with H's own validation before returning it.
    h.load_source(root)
    return root


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--old-bundle', type=Path)
    args = parser.parse_args()
    print(build(args.root, args.old_bundle))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
