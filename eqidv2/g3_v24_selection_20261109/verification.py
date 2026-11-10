"""Independent before/after reproducibility check for the frozen V24 run.

Capture supplements the runner's dependency signature. Verification reads
actual source-cache input maps and actual indicator proofs, never mutable
ancestors merely described inside old immutable cache provenance documents.
Only this package's supplementary JSON records are written.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.metadata
import json
from pathlib import Path
import platform
import sys

from . import data, engine, reporting, run


PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parent
BEFORE = PACKAGE / 'supplementary_dependencies_before.json'
BASELINES = (run.BASELINE, run.G7_CACHE, data.HISTORY)


def _merge(target, additions):
    for path, digest in additions.items():
        path = str(Path(path).resolve())
        if path in target and target[path] != digest:
            raise ValueError('Conflicting consumed-file hashes: ' + path)
        target[path] = digest


def _manifest(folder):
    folder = Path(folder).resolve()
    path = folder / 'manifest.json'
    manifest = json.loads(path.read_text(encoding='utf-8'))
    files = {str(path): data.sha(path)}
    for name, digest in manifest.items():
        child = (folder / name).resolve()
        if not child.is_relative_to(folder) or child == folder:
            raise ValueError('Manifest path escaped result folder')
        files[str(child)] = digest
    data._verify(files)
    return files


def _environment():
    return dict(python=sys.version, executable=sys.executable, platform=platform.platform(),
                packages={name: importlib.metadata.version(name) for name in ('numpy', 'pandas', 'pyarrow')})


def _dependencies():
    # Importing the frozen runner brings its local helper graph into sys.modules.
    # The nested v5 module performs real native entry fills, so assert it is
    # represented even if its import mechanism changes later.
    required = (engine.native.g, engine.native.g.v9, engine.native.g.v9.v5)
    files = {}
    for module in list(sys.modules.values()) + list(required):
        name = getattr(module, '__file__', None)
        if not name:
            continue
        path = Path(name).resolve()
        if path.suffix == '.py' and path.is_relative_to(ROOT) and path.is_file():
            files[str(path)] = data.sha(path)
    files[str(Path(__file__).resolve())] = data.sha(__file__)
    for module in required:
        if str(Path(module.__file__).resolve()) not in files:
            raise ValueError('Native fill dependency was not captured')
    return files


def _write_new(path, payload):
    path = Path(path).resolve()
    if path == PACKAGE or not path.is_relative_to(PACKAGE):
        raise ValueError('Supplementary records must remain inside V24')
    with path.open('x', encoding='utf-8') as stream:
        stream.write(json.dumps(payload, indent=2, allow_nan=False) + '\n')


def capture():
    dependencies = _dependencies()
    old_files = {}
    for folder in BASELINES:
        _merge(old_files, _manifest(folder))
    payload = dict(scope='BACKTEST_REPRODUCIBILITY_ONLY', execution_authority=False,
        capture_time_utc=datetime.now(timezone.utc).isoformat(),
        timing='Captured during the frozen run, before completion; not claimed before process start',
        code_sha256=dependencies, baseline_artifact_sha256=old_files, environment=_environment())
    _write_new(BEFORE, payload)
    return dict(status='CAPTURED', path=str(BEFORE), dependencies=len(dependencies),
                baseline_artifacts=len(old_files))


def _auxiliary_inputs(value):
    result = data._identities(value)
    if isinstance(value, dict):
        for path_key, hash_key in (('stock_path', 'stock_sha256'),
                                   ('g3_source_proof', 'g3_source_proof_sha256')):
            if path_key in value and hash_key in value:
                _merge(result, {value[path_key]: value[hash_key]})
        children = value.values()
    elif isinstance(value, list):
        children = value
    else:
        return result
    for child in children:
        _merge(result, _auxiliary_inputs(child))
    return result


def verify(results: Path, report: Path | None = None):
    results = Path(results).resolve()
    if results == PACKAGE or not results.is_relative_to(PACKAGE):
        raise ValueError('Results must be inside V24')
    before = json.loads(BEFORE.read_text(encoding='utf-8'))
    data._verify(before['code_sha256'])
    data._verify(before['baseline_artifact_sha256'])
    if before['environment'] != _environment():
        raise ValueError('Execution environment changed since supplementary capture')
    for folder in BASELINES:
        _manifest(folder)
    result_files = _manifest(results)
    config = json.loads((results / 'config.json').read_text(encoding='utf-8'))
    fingerprint = config['source_fingerprints']
    data._verify(fingerprint['code'])
    if (fingerprint['baseline_manifest'] != data.sha(run.BASELINE / 'manifest.json') or
            fingerprint['g7_manifest'] != data.sha(run.G7_CACHE / 'manifest.json')):
        raise ValueError('Result baseline manifest differs from verified original')
    if config['baseline_parity']['status'] != 'PASS':
        raise ValueError('Saved V23 behavioral baseline parity did not pass')
    proofs = json.loads((results / 'source_provenance.json').read_text(encoding='utf-8'))
    consumed, source_metadata, cache_days = {}, {}, []
    for proof in proofs:
        for item in proof.get('source_cache_metadata', []):
            path = Path(item['path']).resolve()
            if not path.is_relative_to(PACKAGE):
                raise ValueError('Source cache metadata outside isolated V24')
            _merge(source_metadata, {str(path): item['sha256']})
            observed = json.loads(path.read_text(encoding='utf-8'))
            if observed != item['metadata']:
                raise ValueError('Exported source metadata differs from recorded cache')
            _merge(consumed, observed['input_sha256'])
            _merge(consumed, observed['code_sha256'])
            _merge(consumed, {str(path.with_suffix('.pkl')): observed['cache_sha256']})
            cache_days.append(observed['day'])
        # Actual feature/G7 consumption is checked separately from historical
        # data-cache ancestry, which is intentionally documentary evidence.
        for field in ('features', 'g7_new_candidates'):
            _merge(consumed, _auxiliary_inputs(proof.get(field, [])))
    if cache_days != config['historydays']:
        raise ValueError('Exported source-cache coverage differs from result history')
    data._verify(source_metadata)
    data._verify(consumed)
    payload = dict(status='PASS', verified_at_utc=datetime.now(timezone.utc).isoformat(),
        results=str(results), capture_path=str(BEFORE), capture_sha256=data.sha(BEFORE),
        sessions=len(cache_days), supplementary_code_files=len(before['code_sha256']),
        baseline_artifacts_unchanged=len(before['baseline_artifact_sha256']),
        result_artifacts_verified=len(result_files), source_cache_metadata_files=len(source_metadata),
        consumed_input_and_cache_files_verified=len(consumed),
        baseline_parity=config['baseline_parity'], historical_ancestors_reread=False,
        interpretation='File/code/environment consistency and source integrity, not a claim of future trading performance')
    report = Path(report) if report else PACKAGE / ('supplementary_verification_' + results.name + '.json')
    _write_new(report, payload)
    return payload


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--capture', action='store_true')
    mode.add_argument('--results', type=Path)
    parser.add_argument('--report', type=Path)
    arguments = parser.parse_args()
    print(json.dumps(capture() if arguments.capture else verify(arguments.results, arguments.report), indent=2))
