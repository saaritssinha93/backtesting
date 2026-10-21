"""Immutable OHLCV research cache for original G4 post-entry experiments."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil

import pandas as pd

from research import g4_hybrid_monitor_backtest as hybrid
from research import g4_hybrid_features as features
from research import g4_failed_breakout_research as sealed
from research import g4_g3_mirror_backtest as base


def prepare(output):
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to((base.G4_ROOT / 'research').resolve()):
        raise ValueError('Require fresh isolated research cache')
    source = hybrid.DEFAULT_MONITOR
    if base.sha256(source / 'manifest.json') != hybrid.DEFAULT_MANIFEST_HASH:
        raise ValueError('Pinned monitor changed')
    monitor = sealed.package(source)
    original_source = Path(monitor['source_run'])
    if base.sha256(original_source / 'manifest.json') != monitor['source_manifest_sha256']:
        raise ValueError('Original G4 changed')
    original_summary = sealed.package(original_source)
    original = base.read_frozen_csv(original_source / 'g4_bounded_exit/portfolio_trades.csv')
    paths = hybrid.load_paths(source / 'historical_paths.npz')
    native = sealed.replay(original, paths, 'current_g4')
    sealed.parity(native, original)
    sealed.parity(native, base.read_frozen_csv(source / 'baseline/portfolio_trades.csv'))
    print('Exact original G4 native and portfolio parity passed', flush=True)
    output.mkdir(parents=True)
    code = {str(Path(name).resolve()): base.sha256(Path(name)) for name in
            (__file__, hybrid.__file__, features.__file__)}
    (output / 'code').mkdir()
    for filename in code:
        shutil.copyfile(filename, output / 'code' / Path(filename).name)
    contexts, proof = hybrid._feature_contexts(native, paths, monitor, original_summary)
    combined = pd.concat([value.assign(sid=key) for key, value in contexts.items()], ignore_index=True)
    combined.to_parquet(output / 'contexts.parquet', index=False)
    native.to_csv(output / 'native.csv', index=False)
    shutil.copyfile(source / 'historical_paths.npz', output / 'historical_paths.npz')
    summary = dict(variant_id='G4-ITERATIVE-SEALED-DATASET-V1', execution_authority=False,
        days=monitor['days'], original_source=str(original_source),
        original_manifest_sha256=monitor['source_manifest_sha256'],
        monitor_source=str(source), monitor_manifest_sha256=hybrid.DEFAULT_MANIFEST_HASH,
        native_control_exactly_reproduced=True, code=code, price_sources=proof,
        orders=len(native), executed_trades=int(native.portfolio_executed.sum()),
        context_rows=len(combined))
    (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n', encoding='utf-8')
    for filename, digest in code.items():
        if base.sha256(Path(filename)) != digest:
            raise ValueError('Dataset code changed during preparation')
    manifest = dict(files={p.relative_to(output).as_posix(): base.sha256(p)
                           for p in output.rglob('*') if p.is_file()})
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
    print(output, flush=True)
    return output


def load(directory):
    directory = Path(directory).resolve()
    summary = sealed.package(directory)
    if summary['variant_id'] != 'G4-ITERATIVE-SEALED-DATASET-V1':
        raise ValueError('Unexpected cache version')
    for filename, digest in summary['code'].items():
        if base.sha256(Path(filename)) != digest:
            raise ValueError('Dataset generation code changed')
    native = base.read_frozen_csv(directory / 'native.csv')
    contexts_frame = pd.read_parquet(directory / 'contexts.parquet')
    contexts = {int(sid): group.drop(columns='sid').reset_index(drop=True)
                for sid, group in contexts_frame.groupby('sid', sort=False)}
    paths = hybrid.load_paths(directory / 'historical_paths.npz')
    if set(contexts) != set(native.sid) or set(paths) != set(native.sid):
        raise ValueError('Incomplete dataset')
    return native, contexts, paths, summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', required=True, type=Path)
    prepare(parser.parse_args().output_dir)
