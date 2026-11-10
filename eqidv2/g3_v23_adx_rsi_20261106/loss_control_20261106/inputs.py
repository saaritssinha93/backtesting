"""Hash-verified frozen paths and baseline for all66 completed sessions."""
from pathlib import Path
import json
import pickle

import pandas as pd

from g3_v23_adx_rsi_20261106 import run as original
from g3_v23_adx_rsi_20261106.keep_entries_20261106.publish import verify_manifest
from g3_v23_adx_rsi_20261106.through_20261105.run import assert_entries

PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parent
OLD = ROOT / 'keep_entries_20261106/exit_study/results_through_20261104'
LATEST = ROOT / 'through_20261105/source_20261105'
BASELINE = ROOT / 'through_20261105/results_through_20261105'


def load():
    proofs = {}
    for folder in (OLD, LATEST, BASELINE):
        verify_manifest(folder)
        provenance = json.loads((folder / 'provenance.json').read_text())
        for name, expected in provenance.get('code_sha256', {}).items():
            if original.sha(name) != expected:
                raise ValueError('Source code changed: ' + name)
        proofs[str(folder)] = original.sha(folder / 'manifest.json')
    old = pickle.loads((OLD / 'verified_inputs.pkl').read_bytes())
    latest = pickle.loads((LATEST / 'inputs.pkl').read_bytes())
    if set(old['paths']) & set(latest['paths']):
        raise ValueError('Duplicate historical path identity')
    trades = pd.read_csv(BASELINE / 'baseline_v21_g7_trades.csv')
    source_trades = pd.concat([old['trades'], latest['trades']], ignore_index=True)
    assert_entries(source_trades, trades)
    parity = pd.concat([old['parity'], latest['parity']], ignore_index=True)
    days = old['days'] + latest['days']
    paths = {**old['paths'], **latest['paths']}
    if len(trades) != 790 or len(days) != 66 or len(set(days)) != 66 or days[-1] != '2026-11-05':
        raise ValueError('Full history inventory changed')
    if not trades.sid.is_unique or not parity.sid.is_unique or set(parity.sid) != set(trades.sid) or set(paths) != set(trades.sid):
        raise ValueError('Incomplete or duplicate frozen paths/entries')
    audit = pd.read_csv(BASELINE / 'admission_audit.csv')
    original.check_identities(audit, trades)
    return dict(trades=trades, parity=parity, paths=paths, days=days, audit=audit,
                source_manifests=proofs, baseline_config=json.loads((BASELINE / 'config.json').read_text()))
