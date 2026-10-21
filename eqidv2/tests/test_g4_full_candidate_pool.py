"""Prepared-pool cache integrity and destination regression coverage."""
from pathlib import Path
from types import SimpleNamespace
import json

import pandas as pd
import pytest

from research import g4_full_candidate_pool as full


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding='utf-8')


def test_code_hashes_accept_serialized_paths():
    proof = full._cache_code()
    assert str(Path(full.__file__).resolve()) in proof
    assert all(Path(path).is_file() and len(digest) == 64 for path, digest in proof.items())


@pytest.fixture
def cache_fixture(tmp_path, monkeypatch):
    source = tmp_path/'source'
    source.mkdir()
    source_file = source/'input.txt'
    source_file.write_text('frozen candidate input', encoding='utf-8')
    history, recent = source/'history.json', source/'recent.json'
    write_json(history, {'extensions': []})
    write_json(recent, {'recent_sources': {
        '2026-10-08': {'feature_ledger': str(source_file)},
        '2026-10-09': {'feature_ledger': str(source_file)},
        '2026-10-12': {'scanner': 'obsolete unrelated scanner evidence'},
    }})
    for name in ('DEFAULT_SEALED_SOURCE', 'G3_RUN', 'OCTOBER12_SOURCE'):
        monkeypatch.setattr(full, name, source)
    monkeypatch.setattr(full.morning, 'DEFAULT_HISTORY_PROVENANCE', history)
    monkeypatch.setattr(full.morning, 'DEFAULT_RECENT_PROVENANCE', recent)
    monkeypatch.setattr(full.base, 'configuration', lambda: {'scenarios': {'guard_all_ten': {}}})
    monkeypatch.setattr(full.base, 'reselect', lambda frame, limits: frame.copy())
    reference = tmp_path/'reference'
    write_json(reference/'manifest.json', {'reference': 'frozen'})
    pool = pd.DataFrame([dict(day='2026-10-14', setup_id='0926_LONG',
                              tradingsymbol='TEST', sid=12, optional=None)])
    (reference/'guard_all_ten').mkdir()
    pool.to_csv(reference/'guard_all_ten/selected_orders.csv', index=False)
    summary = {'days': ['2026-10-14'], 'daily_path_evidence': []}
    proof = {'late_families': {}, 'native_eligible_candidates': 1}
    prices = SimpleNamespace(proof={str(source_file): {'sha256': full.base.sha256(source_file)}})
    cache = tmp_path/'prepared'
    full._cache_save(cache, pool, proof, prices, summary, reference, {})
    return SimpleNamespace(cache=cache, pool=pool, proof=proof, prices=prices,
                           summary=summary, reference=reference, source_file=source_file)


def test_prepared_cache_round_trip_checks_real_source_hashes(cache_fixture):
    f = cache_fixture
    restored_prices = SimpleNamespace(proof={})
    pool, proof = full._cache_load(f.cache, f.summary, f.pool, restored_prices, f.reference)
    pd.testing.assert_frame_equal(pool, f.pool)
    assert proof == f.proof
    assert restored_prices.proof == f.prices.proof


def test_prepared_cache_rejects_source_change(cache_fixture):
    f = cache_fixture
    f.source_file.write_text('changed', encoding='utf-8')
    with pytest.raises(ValueError, match='source changed'):
        full._cache_load(f.cache, f.summary, f.pool, SimpleNamespace(proof={}), f.reference)


def test_prepared_cache_rejects_artifact_change_before_unpickling(cache_fixture, monkeypatch):
    f = cache_fixture
    (f.cache/'pool.pkl').write_bytes(b'corrupt or malicious pickle')
    monkeypatch.setattr(full.pd, 'read_pickle', lambda _: pytest.fail('unverified pickle was read'))
    with pytest.raises(ValueError, match='Source artifact drift'):
        full._cache_load(f.cache, f.summary, f.pool, SimpleNamespace(proof={}), f.reference)


@pytest.mark.parametrize('with_cache', [False, True])
def test_fresh_build_keeps_prepared_destination_separate_from_source_caches(tmp_path, monkeypatch, with_cache):
    """Exercise all four source-cache iterations and the final save branch."""
    monkeypatch.setattr(full.base, 'ROOT', tmp_path)
    source = tmp_path/'source'
    source.mkdir()
    for name in ('DEFAULT_SEALED_SOURCE', 'G3_RUN', 'OCTOBER12_SOURCE'):
        monkeypatch.setattr(full, name, source)
    for name in ('signals.parquet', 'paths.npz', 'selection_audit_0920.csv'):
        (source/name).write_text('fixture', encoding='utf-8')
    write_json(source/'dataset_manifest.json', {'output_sha256': {
        name: full.base.sha256(source/name) for name in ('signals.parquet','paths.npz')}})
    write_json(source/'manifest.json', {'files': {
        'selection_audit_0920.csv': full.base.sha256(source/'selection_audit_0920.csv')}})
    day = pd.Timestamp('2026-10-14').date()
    summary = dict(first_date=str(day), last_date=str(day), sessions=1,
                   days=[str(day)], daily_path_evidence=[], coverage={'excluded_sessions': []})
    original = pd.DataFrame([dict(day=str(day), hhmm_int=925, signal_end='09:25',
        side='LONG', tradingsymbol='TEST', setup_id='0926_LONG', sid=1,
        v9_filter_pass=True, v9_selected=True, research_feature_status='COMPLETE')])
    monkeypatch.setattr(full.base, 'configuration', lambda: {'scenarios': {'guard_all_ten': {}}})
    monkeypatch.setattr(full.base, 'verify', lambda _: summary)
    monkeypatch.setattr(full.morning, 'load_sources', lambda *args: (original.copy(), [day], {}, {}))
    monkeypatch.setattr(full, '_source_days_and_snapshots', lambda: ([day], {}, {}))
    monkeypatch.setattr(full, '_normalize', lambda frame: frame.copy())
    monkeypatch.setattr(full.pd, 'read_parquet', lambda _: original.iloc[:0].copy())
    seen_source_caches = []
    def late(cache, *args):
        seen_source_caches.append(cache)
        return original.iloc[:0].copy(), {}
    monkeypatch.setattr(full, '_late_readonly', late)
    monkeypatch.setattr(full.base, 'read_frozen_csv', lambda path:
        original.iloc[:0].copy() if path.name == 'selection_audit_0920.csv' else original.copy())
    monkeypatch.setattr(full.base.donor, 'selection_audit', lambda frame: frame.assign(
        v9_selected=True, v9_filter_pass=True))
    monkeypatch.setattr(full.base, 'reselect', lambda frame, limits: frame.copy())
    def enrich(frame):
        return frame.assign(research_ema20=1., research_atr14=1.,
                            research_extension_atr=0., research_feature_source='fixture')
    prices = SimpleNamespace(enrich=enrich)
    saved = []
    monkeypatch.setattr(full, '_cache_save', lambda destination, *args: saved.append(destination))
    destination = tmp_path/'prepared'
    pool, proof = full.build_pool(summary, original, prices, tmp_path/'reference',
                                  cache_dir=destination if with_cache else None)
    assert len(seen_source_caches) == 4
    assert saved == ([destination.resolve()] if with_cache else [])
    assert pool.sid.tolist() == [1]
    assert proof['g3_selection_reproduced'] and proof['g4_selection_reproduced']
