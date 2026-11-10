"""Diagnostic labels only: every published G3 V24 baseline trade is retained."""
import argparse
import hashlib
import json
from pathlib import Path
import pickle
import platform
import sys

import numpy as np
import pandas as pd

from . import features, reporting
from ..rsi_timing_20261110 import inputs

PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parent.parent
LABELS = ['COMPRESSION_BREAKOUT', 'EARLIER_BREAKOUT_CONTINUATION',
          'BREAKOUT_WITHOUT_COMPRESSION', 'OTHER_READY',
          'EARLY_SESSION_NOT_APPLICABLE', 'UNAVAILABLE']


def dump(path, value):
    path.write_text(json.dumps(value, indent=2, default=str)+'\n', encoding='utf-8')


def code_hashes():
    files = {Path(__file__).resolve()}
    for module in tuple(sys.modules.values()):
        name = getattr(module, '__file__', None)
        if name and str(name).endswith('.py'):
            path = Path(name).resolve()
            if path.is_relative_to(ROOT) and path.is_file():
                files.add(path)
    return {str(path): inputs.sha(path) for path in sorted(files)}


def check_inventory(orders, audit):
    if audit.sid.duplicated().any() or set(audit.sid) != set(orders.sid):
        raise ValueError('Diagnostic changed admitted candidate inventory')
    left = orders.set_index('sid').sort_index()
    right = audit.set_index('sid').sort_index()
    for key in inputs.IDENTITY:
        if not left[key].astype(str).equals(right[key].astype(str)):
            raise ValueError('Diagnostic identity differs: '+key)
    if not right.compression_label.isin(LABELS).all():
        raise ValueError('Unexpected diagnostic label')
    if not right.is_primary.eq(right.compression_label.eq(LABELS[0])).all():
        raise ValueError('Primary flag differs from label')
    if not right.compression_ready.eq(right.compression_label.isin(LABELS[:4])).all():
        raise ValueError('Readiness differs from label')


def run(output=None, limit=None):
    output = Path(output).resolve() if output else PACKAGE/'results_through_20261109'
    if output == PACKAGE or not output.is_relative_to(PACKAGE) or output.exists():
        raise ValueError('Use a fresh results directory inside this diagnostic study')
    frozen = inputs.load_baseline()
    days = frozen['days'][:limit] if limit else frozen['days']
    orders = frozen['orders'].loc[frozen['orders'].day.isin(days)].copy()
    trades = frozen['expected'].loc[frozen['expected'].day.isin(days)].copy()
    if not orders.v9_filter_pass.eq(True).all():
        raise ValueError('Reference includes nonqualified G3 candidates')
    codes = code_hashes()
    signature = dict(code=codes, baseline_hashes=frozen['parent_hashes'],
                     days=days, feature_configuration=features.configuration())
    stamp = hashlib.sha256(json.dumps(signature, sort_keys=True).encode()).hexdigest()[:16]
    cache = PACKAGE/'cache'/stamp
    cache.mkdir(parents=True, exist_ok=True)
    cached, meta_path = cache/'features.pkl', cache/'features.json'
    if cached.exists() and meta_path.exists():
        meta = json.loads(meta_path.read_text())
        if meta['signature'] != signature:
            raise ValueError('Feature cache signature changed')
        inputs.verify({cached: meta['sha256']})
        prepared = pickle.loads(cached.read_bytes())
        inputs.verify(inputs.source_hashes(prepared['proofs']))
        print('Verified cached compression labels', flush=True)
    else:
        print(f'Calculating causal compression labels for {len(orders)} admitted orders, {len(days)} sessions', flush=True)
        audit, proofs = features.build_features(orders)
        used = inputs.source_hashes(proofs)
        for path, digest in used.items():
            if path in frozen['historical_feature_hashes'] and digest != frozen['historical_feature_hashes'][path]:
                raise ValueError('Stock-history lineage changed: '+path)
        for proof in proofs:
            path = proof.get('stock_path')
            if path and str(Path(path).resolve()) not in frozen['historical_feature_hashes']:
                raise ValueError('Stock source absent from V24 reference: '+path)
        prepared = dict(audit=audit, proofs=proofs)
        check_inventory(orders, audit)
        cached.write_bytes(pickle.dumps(prepared, protocol=5))
        dump(meta_path, dict(signature=signature, sha256=inputs.sha(cached)))
    audit = prepared['audit']
    check_inventory(orders, audit)
    baseline = inputs.baseline_parity(trades, frozen['expected'].loc[frozen['expected'].day.isin(days)])
    inputs.verify(codes)
    inputs.verify(frozen['parent_hashes'])
    inputs.verify(inputs.source_hashes(prepared['proofs']))
    config = dict(version='G3-V24-COMPRESSION-DIAGNOSTIC-20261110', scope='BACKTEST_ONLY_DIAGNOSTIC',
        execution_authority=False, label_column='compression_label', ready_column='compression_ready',
        primary_label=LABELS[0], labels=LABELS, ready_labels=LABELS[:4],
        feature_configuration=features.configuration(), historydays=days,
        first_day=days[0], last_day=days[-1], admitted_orders=len(orders), filled_trades=len(trades),
        baseline_parity=baseline, source_baseline=str(inputs.BASE), source_fingerprints=signature,
        timing_changed=False, stock_selection_changed=False, exits_changed=False,
        entry_filters_added=False, reranking=False, thresholds_optimized=False,
        baseline_volume_confirmation='Inherited for every admitted order; no additional RVOL cutoff',
        native_target_preserved=True, initial_stop_pct=1.50, exit_policy='breakeven_eighty_target',
        exposure_rs=500000, round_trip_cost_rs=250, unlimited_concurrent_capital=True,
        original_pairs=34, original_cap=3, new_pairs=39, new_cap=1, configured_pairs_through='14:00',
        early_session_policy='Retain all trades under separate diagnostic label; no overnight BB20 mixing',
        historical_percentile_policy='Previous100 valid widths only; prior completed sessions allowed',
        live_changed=False, paper_changed=False, previous_results_changed=False,
        retrospective_only=True, automatic_promotion=False,
        excluded_current_session='2026-11-10 incomplete at study start',
        source_limitations=['Same68-session V24 cohort throughNov9',
            'Oct1 incomplete source and Nov8 special session excluded',
            'Oct28 expanded stock coverage211/215 remains inherited',
            'Earlier archived universes not strictly point-in-time'],
        environment=dict(python=platform.python_version(), pandas=pd.__version__, numpy=np.__version__))
    output.mkdir(parents=True)
    summary = reporting.publish(output, orders, trades, audit, days, config)
    audit.to_csv(output/'feature_audit.csv', index=False)
    dump(output/'config.json', config)
    dump(output/'summary.json', summary)
    dump(output/'source_provenance.json', dict(stock_histories=prepared['proofs'],
        baseline_artifacts=frozen['parent_hashes'], feature_cache_path=str(cached), feature_cache_sha256=inputs.sha(cached),
        unchanged_execution_policy='Reuse published verified baseline fills, no execution or entry changes'))
    dump(output/'verification.json', dict(status='PASS', baseline_parity=baseline,
        inventory='ALL_ADMITTED_ORDERS_AND_ALL_FILLED_TRADES_RETAINED',
        baseline_artifacts_unchanged=len(frozen['parent_hashes']), code_files_unchanged=len(codes),
        scope='Diagnostic classification and reconciliation; no new strategy deployed'))
    dump(output/'manifest.json', {path.name:inputs.sha(path) for path in output.iterdir() if path.is_file()})
    print('LABEL COUNTS '+json.dumps(audit.compression_label.value_counts().to_dict()), flush=True)
    print('UNCHANGED BASELINE '+json.dumps(baseline), flush=True)
    print('RESULTS '+str(output), flush=True)
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--limit', type=int)
    args = parser.parse_args()
    run(args.output, args.limit)
