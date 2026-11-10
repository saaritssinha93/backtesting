"""Matched-history RSI timing research. No broker API, orders, or live writes."""
import argparse
import hashlib
import json
from pathlib import Path
import pickle
import platform
import sys

import numpy as np
import pandas as pd

from . import features, engine, inputs, reporting
from .. import engine as baseline_engine

PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parent.parent
POLICIES = ('baseline_v23', *engine.POLICIES)


def dump(path, value):
    path.write_text(json.dumps(value, indent=2, default=str)+'\n', encoding='utf-8')


def code_hashes():
    paths = {Path(__file__).resolve()}
    for module in tuple(sys.modules.values()):
        file = getattr(module, '__file__', None)
        if file and str(file).endswith('.py'):
            path = Path(file).resolve()
            if path.is_relative_to(ROOT) and path.is_file():
                paths.add(path)
    return {str(path): inputs.sha(path) for path in sorted(paths)}


def validate_alignment(orders, paths, windows):
    for row in orders.itertuples(index=False):
        path, window = paths[int(row.sid)], windows[int(row.sid)]
        if not np.array_equal(path['timestamp_ns'][:10], window['timestamp_ns'][1:11]):
            raise ValueError('Feature/execution clocks differ: '+str(row.sid))
        for key in ('open','high','low','close'):
            actual = np.asarray(window[key][1:11], dtype=float)
            present = np.isfinite(actual)
            if not np.allclose(actual[present], path[key][:10][present], rtol=0, atol=.001):
                raise ValueError('Feature/execution prices differ: '+str(row.sid)+' '+key)


def run(output=None, limit=None):
    output = Path(output).resolve() if output else PACKAGE/'results_through_20261109'
    if output == PACKAGE or not output.is_relative_to(PACKAGE) or output.exists():
        raise ValueError('Use a fresh results folder within this timing study')
    frozen = inputs.load_baseline()
    days = frozen['days'][:limit] if limit else frozen['days']
    orders = frozen['orders'].loc[frozen['orders'].day.isin(days)].copy()
    expected = frozen['expected'].loc[frozen['expected'].day.isin(days)]
    codes = code_hashes()
    signature = dict(code=codes, baseline_hashes=frozen['parent_hashes'], days=days,
                     engine=engine.configuration(), feature_version=features.VERSION)
    stamp = hashlib.sha256(json.dumps(signature, sort_keys=True).encode()).hexdigest()[:16]
    cache = PACKAGE/'cache'/stamp
    cache.mkdir(parents=True, exist_ok=True)
    windows_path, windows_meta = cache/'windows.pkl', cache/'windows.json'
    if windows_path.exists() and windows_meta.exists():
        meta = json.loads(windows_meta.read_text())
        inputs.verify({windows_path: meta['sha256']})
        prepared = pickle.loads(windows_path.read_bytes())
        inputs.verify(inputs.source_hashes(prepared['proofs']))
        print('Verified cached RSI windows', flush=True)
    else:
        print(f'Building continuous Wilder RSI14 for {len(orders)} admitted orders across {len(days)} days', flush=True)
        windows, audit, proofs = features.build_rsi_windows(orders)
        used = inputs.source_hashes(proofs)
        # Every source comes from the same stock-history lineage as V24.
        for path, digest in used.items():
            if path in frozen['historical_feature_hashes'] and digest != frozen['historical_feature_hashes'][path]:
                raise ValueError('RSI source differs from original V24 snapshot: '+path)
        for proof in proofs:
            path = proof.get('stock_path')
            if path and str(Path(path).resolve()) not in frozen['historical_feature_hashes']:
                raise ValueError('RSI stock source was not present in V24 provenance: '+path)
        prepared = dict(windows=windows, audit=audit, proofs=proofs)
        windows_path.write_bytes(pickle.dumps(prepared, protocol=5))
        dump(windows_meta, dict(sha256=inputs.sha(windows_path), signature=signature))
    windows = prepared['windows']
    books = {key: [] for key in POLICIES}
    order_audits = {key: [] for key in POLICIES}
    source_proofs, coverage = [], []
    for index, day in enumerate(days):
        selected = orders.loc[orders.day.eq(day)].copy()
        minutes, proof = inputs.load_minutes(frozen['metadata'][day], selected)
        source_proofs.append(proof)
        paths = baseline_engine.execution_paths(selected, minutes)
        validate_alignment(selected, paths, windows)
        baseline, fills = baseline_engine.simulate(selected, minutes)
        inputs.baseline_parity(baseline, expected.loc[expected.day.eq(day)])
        base_audit = selected[['sid', *inputs.IDENTITY, 'confirmation_ts', 'trigger']].merge(fills, on='sid', validate='one_to_one')
        base_audit['status'] = np.where(base_audit.filled, 'BASELINE_FILLED', 'BASELINE_NO_FILL')
        books['baseline_v23'].append(baseline)
        order_audits['baseline_v23'].append(base_audit)
        counts = {'baseline_v23': len(baseline)}
        for policy in engine.POLICIES:
            book, audit = engine.simulate(selected, paths, windows, policy)
            books[policy].append(book)
            order_audits[policy].append(audit)
            counts[policy] = len(book)
        feature_day = prepared['audit'].loc[prepared['audit'].day.eq(day)]
        coverage.append(dict(day=day, admitted_orders=len(selected),
            rsi_ready_at_confirmation=int(feature_day.available.sum()),
            rsi_unavailable_at_confirmation=int((~feature_day.available).sum()),
            all_window_bars_ready=int(feature_day.valid_bars.eq(11).sum()),
            future_bar_readiness_not_used_as_entry_prefilter=True))
        print(f'{index+1}/{len(days)} {day}: filled '+json.dumps(counts), flush=True)
    books = {key: pd.concat(parts, ignore_index=True) for key,parts in books.items()}
    order_audits = {key: pd.concat(parts, ignore_index=True) for key,parts in order_audits.items()}
    parity = inputs.baseline_parity(books['baseline_v23'], expected)
    inputs.verify(codes)
    inputs.verify(frozen['parent_hashes'])
    inputs.verify(inputs.source_hashes(prepared['proofs']))
    config = engine.configuration() | dict(historydays=days, first_day=days[0], last_day=days[-1],
        admitted_orders=len(orders), policies=list(POLICIES), baseline_parity=parity,
        source_baseline=str(inputs.BASE), source_fingerprints=signature,
        rsi_period=14, rsi_timeframe='1 minute', rsi_method='Wilder, arithmetic seed of first14 deltas, continuous across sessions',
        stock_selection='Unchanged frozen G3/V23 admitted inventory; no replacement of missed entries',
        original_pairs=34, original_cap=3, new_pairs=39, new_cap=1, all_pairs_through='14:00',
        unlimited_concurrent_capital=True, exits_rebased_to_actual_new_entry=True,
        live_changed=False, paper_changed=False, previous_results_changed=False,
        retrospective_only=True, automatic_promotion=False, new_session_excluded='2026-11-10 still in progress at study start',
        coverage_limitations=['Same inherited68 normal-session cohort as V24',
            'Oct1 incomplete data and Nov8 special session excluded',
            'Oct28 inherited211/215 expanded-stock coverage; no missing rows invented',
            'Earlier archived universe not strictly point-in-time'],
        environment=dict(python=platform.python_version(), pandas=pd.__version__, numpy=np.__version__))
    output.mkdir(parents=True)
    summary = reporting.publish(output, books, order_audits, days, config, coverage)
    prepared['audit'].to_csv(output/'feature_audit.csv', index=False)
    pd.DataFrame(coverage).to_csv(output/'coverage.csv', index=False)
    dump(output/'config.json', config)
    dump(output/'summary.json', summary)
    dump(output/'source_provenance.json', dict(feature_sources=prepared['proofs'],
        consumed_execution_caches=source_proofs, frozen_baseline_artifacts=frozen['parent_hashes'],
        RSI_cache_path=str(windows_path), RSI_cache_sha256=inputs.sha(windows_path)))
    dump(output/'verification.json', dict(status='PASS', baseline_parity=parity,
        old_artifacts_unchanged=len(frozen['parent_hashes']), local_code_files_unchanged=len(codes),
        feature_execution_price_and_clock_parity='PASS', policy_inventory_parity='PASS',
        thresholds_optimized=False, all_results_retrospective=True))
    dump(output/'manifest.json', {path.name:inputs.sha(path) for path in output.iterdir() if path.is_file()})
    print('BASELINE PARITY '+json.dumps(parity), flush=True)
    print('RESULTS '+str(output), flush=True)
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--limit', type=int)
    args = parser.parse_args()
    run(args.output, args.limit)
