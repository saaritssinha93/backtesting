"""Read-only V21 candidate inputs with isolated, provenance-checked V24 caches.

The full filter audit is retained, including candidates beyond the old quota.
Only execution minutes carry future bars; candidate rows contain no outcomes.
Existing versions, source snapshots, and their caches are never modified.
"""
from __future__ import annotations

from datetime import date
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import pickle
import re

import numpy as np
import pandas as pd

from g3_v18_20261102 import data as original_data
from g3_v18_20261102 import selection as old_selection
from g3_v21_expanded_20261104 import selection, source
from g3_v21_selective_confirmation_20261104 import selection as selective
from g3_v21_expanded_20261104.refinement_20261104.strict_new_pairs_20261104 import policy as strict
import fno_v13_v10_g_3_contract as donor


PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parent
HISTORY = ROOT / 'g3_v21_expanded_20261104/results_full_20261104'
NOV5 = ROOT / 'g3_v23_adx_rsi_20261106/through_20261105'
THROUGH = '2026-11-09'
VERSION = 'G3-V24-FULL-PREQUOTA-DATA-V1'
FORBIDDEN = re.compile(r'(^|_)(pnl|profit|loss|exit|executed|portfolio|outcome|mfe|mae)(_|$)|^filled$|^entry_(price|ts|time|timestamp)$', re.I)
TIMESTAMPS = ('signal_ts', 'confirmation_ts', 'signal_timestamp', 'confirmation_timestamp',
              'v9_5m_feature_ts', 'v9_1m_feature_ts', 'v9_feature_available_ts')


def available_days(through: str = THROUGH) -> list[str]:
    return source.available_days(through)


days = available_days()


@lru_cache(maxsize=4096)
def _hash_stat(path: str, size: int, modified_ns: int) -> str:
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def sha(path) -> str:
    path = Path(path).resolve()
    stat = path.stat()
    return _hash_stat(str(path), stat.st_size, stat.st_mtime_ns)


def _code() -> dict[str, str]:
    modules = (original_data, old_selection, selection, source, selective, strict, donor,
               source.raw, source.raw.hybrid)
    paths = [Path(__file__), *(Path(module.__file__) for module in modules)]
    return {str(path.resolve()): sha(path) for path in paths}


def _verify(files: dict[str, str]) -> None:
    for path, expected in files.items():
        if sha(path) != expected:
            raise ValueError('V24 input hash changed: ' + path)


def _identities(value) -> dict[str, str]:
    """Extract explicit path/hash pairs from inherited source proofs."""
    result = {}
    pairs = (('feature_path', 'features_sha256'), ('minute_path', 'minutes_sha256'),
             ('provenance_path', 'provenance_sha256'), ('run_manifest', 'run_manifest_sha256'),
             ('source_manifest', 'source_manifest_sha256'), ('snapshot_manifest', 'snapshot_manifest_sha256'),
             ('path', 'sha256'), ('manifest', 'manifest_sha256'))
    if isinstance(value, dict):
        for path_key, hash_key in pairs:
            if isinstance(value.get(path_key), str) and isinstance(value.get(hash_key), str):
                path = str(Path(value[path_key]).resolve())
                if path in result and result[path] != value[hash_key]:
                    raise ValueError('Conflicting input hashes: ' + path)
                result[path] = value[hash_key]
        children = value.values()
    elif isinstance(value, list):
        children = value
    else:
        return result
    for child in children:
        for path, expected in _identities(child).items():
            if path in result and result[path] != expected:
                raise ValueError('Conflicting input hashes: ' + path)
            result[path] = expected
    return result


def _consumed_cache_inputs(proof: dict) -> dict[str, str]:
    """Only files actually read by V18's verified cached-source branch.

    A cache provenance document can describe mutable ancestors used when the
    immutable cache was built. Those ancestors are not inputs to this replay.
    Its parquet payloads and the complete provenance document remain hashed.
    """
    if proof.get('loader_source') != 'HASH_VERIFIED_PRESELECTION_RESEARCH_CACHE':
        raise ValueError('Consumed-cache validation requires the verified cache branch')
    result = {}
    for path_key, hash_key in (('feature_path', 'features_sha256'),
                               ('minute_path', 'minutes_sha256'),
                               ('provenance_path', 'provenance_sha256')):
        path, expected = proof.get(path_key), proof.get(hash_key)
        if not isinstance(path, str) or not isinstance(expected, str):
            raise ValueError('Missing actual cache input identity: ' + path_key)
        result[str(Path(path).resolve())] = expected
    # V18 additionally reads this dated universe when available.
    result.update(_identities(proof.get('breadth_universe', {})))
    return result


def _expansion_coverage(expansion: dict, *, pinned_historical: bool) -> dict:
    """Keep the exact manifested historical cohort; never invent absent rows."""
    if expansion.get('through_clock') != '14:00':
        raise ValueError('V21 expanded source does not reach 14:00')
    expected = expansion.get('expected_append_rows')
    appended = expansion.get('appended_feature_rows')
    missing = expansion.get('missing_append_rows')
    if (not all(isinstance(x, int) and x >= 0 for x in (expected, appended, missing))
            or expected - appended != missing
            or expansion.get('missing_clock_rows', missing) != missing
            or sum(expansion.get('appended_clock_counts', {}).values()) != appended):
        raise ValueError('Inconsistent V21 expanded-source row accounting')
    if missing and not pinned_historical:
        raise ValueError('Incomplete newly rebuilt V21 expanded slot source')
    issues = expansion.get('source', {}).get('issues', [])
    return dict(policy='MANIFEST_PINNED_HISTORICAL_COHORT' if pinned_historical
                else 'NEW_REBUILD_REQUIRES_ALL_EXPANDED_ROWS',
                expected_appended_rows=expected, observed_appended_rows=appended,
                missing_appended_rows=missing, synthetic_or_filled_rows=0,
                source_issue_symbols=sorted({item['symbol'] for item in issues if item.get('symbol')}),
                explanation='Historical gaps are inherited identically by every arm. '
                            'Available original and appended rows remain eligible; no missing rows are invented.')


@lru_cache(maxsize=1)
def _saved_history():
    """The manifested CSV is the complete audit, not the selected trade file."""
    manifest_path = HISTORY / 'manifest.json'
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    names = ('selection_audit.csv', 'source_provenance.json', 'config.json', 'summary.json')
    files = {str((HISTORY / name).resolve()): manifest[name] for name in names}
    _verify(files)
    files[str(manifest_path.resolve())] = sha(manifest_path)
    proof = json.loads((HISTORY / 'source_provenance.json').read_text(encoding='utf-8'))
    _verify(proof['code'])
    files.update(proof['code'])
    config = json.loads((HISTORY / 'config.json').read_text(encoding='utf-8'))
    if any(config.get(key) != value for key, value in selection.configuration().items()):
        raise ValueError('V21 source configuration changed')
    summary = json.loads((HISTORY / 'summary.json').read_text(encoding='utf-8'))
    if summary['days'] != days[:64] or len(summary['days']) != 64:
        raise ValueError('Historical V21 day/SID cohort changed')
    audit = pd.read_csv(HISTORY / 'selection_audit.csv', low_memory=False, float_precision='round_trip')
    audit['day'] = audit.day.astype(str).str[:10]
    for field in TIMESTAMPS:
        if field in audit:
            audit[field] = pd.to_datetime(audit[field], utc=True).dt.tz_convert(original_data.IST).dt.as_unit('ns')
    return audit, {item['day']: item for item in proof['sessions']}, files


def _build(index: int, day: str) -> tuple[pd.DataFrame, pd.DataFrame, dict, dict]:
    if index < 64:
        full_audit, history_proofs, files = _saved_history()
        audit = full_audit.loc[full_audit.day.eq(day)].copy().reset_index(drop=True)
        _, minutes, proof = original_data.load_day(day)
        old_proof = history_proofs[day]
        for field in ('features_sha256', 'snapshot_manifest_sha256', 'run_manifest_sha256'):
            expected = old_proof['baseline'].get(field)
            if expected and proof.get(field) != expected:
                raise ValueError('Historical source identity drift: ' + day + ' ' + field)
        proof = dict(proof, v21_expansion=old_proof['expansion'],
                     v24_candidate_source='MANIFEST_VERIFIED_FULL_V21_SELECTION_AUDIT',
                     v24_historical_audit_path=str(HISTORY / 'selection_audit.csv'))
        files = dict(files)
    else:
        files = {}
        cached = NOV5 / 'raw_source_cache.pkl'
        cached_proof = NOV5 / 'raw_source_cache_proof.json'
        if day == '2026-11-05' and cached.is_file() and cached_proof.is_file():
            stored = json.loads(cached_proof.read_text(encoding='utf-8'))
            if stored['sha256'] != sha(cached) or stored['source_loader_sha256'] != sha(source.__file__):
                raise ValueError('Nov5 sealed raw-source cache changed')
            raw, minutes, proof = pickle.loads(cached.read_bytes())
            files = {str(cached.resolve()): sha(cached), str(cached_proof.resolve()): sha(cached_proof)}
        else:
            raw, minutes, proof = source.load_day(day)
        raw = raw.reset_index(drop=True).copy()
        raw['sid'] = index * 1_000_000 + np.arange(len(raw), dtype=np.int64)
        _, audit, _ = selection.select(raw)
        proof = dict(proof, v24_candidate_source='FULL_EXPANDED_HASH_VERIFIED_PRESELECTION_SOURCE')
    expansion = proof.get('v21_expansion', {})
    proof = dict(proof, v24_expansion_coverage=_expansion_coverage(
        expansion, pinned_historical=index < 64))
    if index < 64 and proof.get('loader_source') == 'HASH_VERIFIED_PRESELECTION_RESEARCH_CACHE':
        consumed = _consumed_cache_inputs(proof)
        ancestors = {path: digest for path, digest in _identities(proof).items()
                     if path not in consumed}
        proof = dict(proof, v24_input_validation=dict(
            policy='VERIFIED_SAVED_AUDIT_AND_ACTUALLY_CONSUMED_IMMUTABLE_CACHE_FILES',
            historical_ancestor_inputs=ancestors,
            historical_ancestors_reread=False,
            explanation='Nested cache ancestry records how the historical cache was built. '
                        'This replay consumes its hashed feature/minute payloads and hashed '
                        'provenance document, not mutable ancestor market-data files.'))
        files.update(consumed)
    else:
        files.update(_identities(proof))
    _verify(files)
    return audit, minutes, proof, files


def _validate(record, index: int, day: str) -> None:
    audit, minutes = record['audit'], record['minutes']
    required = {'sid', 'day', 'setup_id', 'tradingsymbol', 'v9_filter_pass', 'v9_selected', 'v24_core_pass'}
    if not required.issubset(audit) or audit.sid.duplicated().any():
        raise ValueError('Invalid full V24 candidate audit')
    if any(FORBIDDEN.search(str(column)) for column in audit.columns):
        raise ValueError('Candidate audit contains trade outcomes')
    if (not audit.day.astype(str).str[:10].eq(day).all() or
            not audit.sid.between(index * 1_000_000, (index + 1) * 1_000_000 - 1).all()):
        raise ValueError('V24 candidate session/SID mismatch')
    eligible = audit.loc[audit.v9_filter_pass.eq(True)]
    if not set(eligible.tradingsymbol).issubset(set(minutes.tradingsymbol)):
        raise ValueError('Candidate execution minutes missing')
    if not set(record['baseline_selected_sids']).issubset(set(eligible.sid)):
        raise ValueError('Baseline selection escaped eligible candidate pool')
    if set(audit.setup_id) - set(selection.SPECS):
        raise ValueError('Unexpected V21 setup ID')


def load_day(index: int, day: str, cache_dir: Path) -> dict:
    """Return full audit, eligible execution minutes, proofs, and baseline SIDs.

    The saved baseline IDs are after original cap selection and the new39
    volume gate, before G7 admission. All full-pool core flags are recomputed.
    Cache files are only created underneath this isolated V24 package.
    """
    day = str(day)[:10]
    if index < 0 or index >= len(days) or days[index] != day:
        raise ValueError('Day index must match the complete chronological V21 cohort')
    cache_dir = Path(cache_dir).resolve()
    if cache_dir == PACKAGE or not cache_dir.is_relative_to(PACKAGE):
        raise ValueError('V24 data cache must be inside the isolated V24 package')
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f'{index:02d}_{day}.pkl'
    meta_path = path.with_suffix('.json')
    code = _code()
    if path.exists() or meta_path.exists():
        if not path.is_file() or not meta_path.is_file():
            raise ValueError('Incomplete V24 data cache: ' + str(path))
        meta = json.loads(meta_path.read_text(encoding='utf-8'))
        if (meta.get('version') != VERSION or meta.get('day') != day or meta.get('index') != index
                or meta.get('code_sha256') != code or meta.get('cache_sha256') != sha(path)):
            raise ValueError('Stale or changed V24 data cache: ' + str(path))
        _verify(meta['input_sha256'])
        record = pickle.loads(path.read_bytes())
        _validate(record, index, day)
        return record
    audit, minutes, proof, files = _build(index, day)
    audit = audit.copy()
    audit['day'] = audit.day.astype(str).str[:10]
    baseline_selected = strict.apply(audit.loc[audit.v9_selected.eq(True)].copy())
    audit['v24_core_pass'] = [bool(row['v9_filter_pass']) and selection._passes(
        row, selection.CORE[row['setup_id']], date.fromisoformat(day)) for row in audit.to_dict('records')]
    eligible_symbols = set(audit.loc[audit.v9_filter_pass.eq(True), 'tradingsymbol'])
    minutes = minutes.loc[minutes.tradingsymbol.isin(eligible_symbols)].copy().reset_index(drop=True)
    proof = dict(proof, v24_version=VERSION, v24_day_index=index,
                 v24_all_audit_rows=len(audit), v24_eligible_rows=int(audit.v9_filter_pass.sum()),
                 v24_execution_symbols=len(eligible_symbols), v24_execution_minute_rows=len(minutes),
                 v24_baseline_pre_g7_orders=len(baseline_selected), execution_authority=False)
    record = dict(day=day, audit=audit, minutes=minutes, proof=proof,
                  baseline_selected_sids=sorted(baseline_selected.sid.astype(int).tolist()))
    _validate(record, index, day)
    if _code() != code:
        raise ValueError('Source/loader code changed while building V24 day')
    temporary = path.with_suffix('.tmp')
    temporary.write_bytes(pickle.dumps(record, protocol=pickle.HIGHEST_PROTOCOL))
    temporary.replace(path)
    meta = dict(version=VERSION, day=day, index=index, code_sha256=code,
                input_sha256=files, cache_sha256=sha(path))
    meta_path.write_text(json.dumps(meta, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    return record
