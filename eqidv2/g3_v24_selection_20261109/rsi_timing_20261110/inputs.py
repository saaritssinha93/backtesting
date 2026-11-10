"""Read the manifested V24 candidate inventory and its frozen execution inputs."""
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import pickle

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106.keep_entries_20261106.publish import verify_manifest

PACKAGE = Path(__file__).resolve().parent
PARENT = PACKAGE.parent
BASE = PARENT / 'results_through_20261109'
IDENTITY = ['day', 'setup_id', 'tradingsymbol', 'side']


@lru_cache(maxsize=10000)
def _hash(path, size, modified):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(4*1024*1024), b''):
            h.update(block)
    return h.hexdigest()


def sha(path):
    path = Path(path).resolve()
    stat = path.stat()
    return _hash(str(path), stat.st_size, stat.st_mtime_ns)


def verify(files):
    for path, expected in files.items():
        if sha(path) != expected:
            raise ValueError('Input changed: ' + str(path))


def source_hashes(value):
    """Only explicit file/hash pairs in actual RSI source proofs."""
    files = {}
    if isinstance(value, dict):
        pairs = [('stock_path','stock_sha256'), ('snapshot_manifest','snapshot_manifest_sha256'),
                 ('source_manifest','source_manifest_sha256'), ('run_manifest','run_manifest_sha256'),
                 ('path','sha256'), ('g3_source_proof','g3_source_proof_sha256')]
        for pk, hk in pairs:
            if isinstance(value.get(pk), str) and isinstance(value.get(hk), str):
                files[str(Path(value[pk]).resolve())] = value[hk]
        children = value.values()
    elif isinstance(value, list):
        children = value
    else:
        return files
    for child in children:
        for path, digest in source_hashes(child).items():
            if path in files and files[path] != digest:
                raise ValueError('Conflicting source hashes: ' + path)
            files[path] = digest
    return files


def load_baseline():
    verify_manifest(BASE)
    manifest = json.loads((BASE/'manifest.json').read_text())
    pinned = {str(BASE/name): digest for name,digest in manifest.items()}
    pinned[str(BASE/'manifest.json')] = sha(BASE/'manifest.json')
    config = json.loads((BASE/'config.json').read_text())
    orders = pd.read_csv(BASE/'baseline_v23_orders.csv', float_precision='round_trip')
    expected = pd.read_csv(BASE/'baseline_v23_trades.csv', float_precision='round_trip')
    if orders.sid.duplicated().any() or expected.sid.duplicated().any():
        raise ValueError('Duplicate frozen identities')
    for field in ['signal_ts','confirmation_ts','signal_timestamp','confirmation_timestamp',
                  'v9_5m_feature_ts','v9_1m_feature_ts','v9_feature_available_ts']:
        if field in orders:
            orders[field] = pd.to_datetime(orders[field], utc=True).dt.tz_convert('Asia/Kolkata').dt.as_unit('ns')
    days = config['historydays']
    if len(days) != 68 or days[-1] != '2026-11-09' or len(orders) != 930 or len(expected) != 824:
        raise ValueError('Frozen comparison cohort changed')
    proof = json.loads((BASE/'source_provenance.json').read_text())
    metadata = {item['metadata']['day']: item for item in proof[0]['source_cache_metadata']}
    if set(metadata) != set(days):
        raise ValueError('Frozen execution coverage mismatch')
    return dict(orders=orders, expected=expected, days=days, metadata=metadata,
                parent_hashes=pinned, historical_feature_hashes=source_hashes(proof[0]['features']))


def load_minutes(item, orders):
    """Consume hashed cached minutes; preserve, but do not reread, their ancestors."""
    metadata_path = Path(item['path']).resolve()
    if not metadata_path.is_relative_to(PARENT):
        raise ValueError('Frozen source cache escaped V24')
    verify({metadata_path: item['sha256']})
    meta = json.loads(metadata_path.read_text())
    if meta != item['metadata']:
        raise ValueError('Source metadata differs from published provenance')
    payload = metadata_path.with_suffix('.pkl')
    verify({payload: meta['cache_sha256']})
    record = pickle.loads(payload.read_bytes())
    if record['day'] != meta['day'] or not orders.day.eq(meta['day']).all():
        raise ValueError('Frozen execution day mismatch')
    audit = record['audit'].set_index('sid')
    if not set(orders.sid).issubset(audit.index):
        raise ValueError('Admitted SID absent from frozen source')
    original = audit.loc[orders.sid]
    for field in IDENTITY:
        if original[field].astype(str).tolist() != orders[field].astype(str).tolist():
            raise ValueError('Frozen source identity differs: ' + field)
    for field in ['trigger','native_target_pct']:
        if not np.allclose(original[field], orders[field], atol=1e-9, rtol=0):
            raise ValueError('Frozen source entry/target differs: ' + field)
    minutes = record['minutes']
    return minutes, dict(day=meta['day'], consumed_cache_path=str(payload),
        consumed_cache_sha256=meta['cache_sha256'], metadata_path=str(metadata_path),
        metadata_sha256=item['sha256'], ancestor_inputs=meta['input_sha256'],
        ancestor_code=meta['code_sha256'], ancestors_reread=False,
        policy='REUSE_PUBLISHED_HASH_VERIFIED_V24_EXECUTION_PAYLOAD_NO_SOURCE_RECONSTRUCTION')


def baseline_parity(actual, expected):
    a = actual.set_index('sid').sort_index()
    b = expected.set_index('sid').sort_index()
    if not a.index.equals(b.index):
        raise ValueError('Baseline filled IDs changed')
    for key in IDENTITY + ['exit_reason']:
        if not a[key].astype(str).equals(b[key].astype(str)):
            raise ValueError('Baseline identity/exit mismatch: ' + key)
    for key in ['entry_ts','exit_ts']:
        if not pd.to_datetime(a[key], utc=True).equals(pd.to_datetime(b[key], utc=True)):
            raise ValueError('Baseline timestamp mismatch: ' + key)
    for key in ['entry_price','exit_price','portfolio_net_profit_rupees']:
        if not np.allclose(a[key], b[key], atol=1e-6, rtol=0):
            raise ValueError('Baseline economics mismatch: ' + key)
    return dict(status='PASS', filled_trades=len(a),
                net_pnl_rs=float(a.portfolio_net_profit_rupees.sum()))
