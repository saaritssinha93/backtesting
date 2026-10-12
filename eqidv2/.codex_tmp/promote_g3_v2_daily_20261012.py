"""Publish a verified local validation replay with a canonical copied snapshot."""
from datetime import datetime
import json
from pathlib import Path
import shutil

import backtesting_result_v13_v10_g_3_daily as publisher
import fno_v13_v10_g_3_daily_replay as replay
from ai_platform.observability.g3_analysis import verified_package


def main():
    validation = Path('.codex_tmp/g3_v2_daily_20261012_184957').resolve()
    root = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3').resolve()
    manifest_path = validation / 'run_manifest.json'
    original_manifest_bytes = manifest_path.read_bytes()
    result = json.loads(original_manifest_bytes)
    assert result['complete'] and result['state'] == 'SUCCESS'
    assert result['role'] == 'production_daily' and result['days'] == ['2026-10-12']
    assert replay.contract.supported_identity(result['strategy_version'], result['strategy_fingerprint'])
    for item in result['artifacts'].values():
        artifact = (validation / item['path']).resolve()
        assert artifact.is_relative_to(validation) and replay.sha256(artifact) == item['sha256']
    source_item = result['artifacts']['source_manifest']
    source_path = validation / source_item['path']
    original_source_bytes = source_path.read_bytes()
    source = json.loads(original_source_bytes)
    snapshot_path = Path(source['snapshot_manifest']).resolve()
    assert snapshot_path.is_relative_to(validation)
    assert replay.sha256(snapshot_path) == source['snapshot_manifest_sha256']
    replay.source._verify_input_snapshot(snapshot_path.parent, json.loads(snapshot_path.read_text(encoding='utf-8')))

    with publisher._lock(root):
        old = {}
        for name, role in [('current_run.json', 'production_daily'), ('current_history.json', 'cumulative_history')]:
            directory, manifest = publisher._read_verified_pointer(root, name, role, allow_legacy=True)
            assert manifest['strategy_version'] == replay.contract.LEGACY_STRATEGY_VERSION
            old[name] = dict(directory=directory, manifest=manifest,
                pointer_bytes=(root / 'latest' / name).read_bytes(),
                manifest_bytes=(directory / 'run_manifest.json').read_bytes())
        canonical = root / 'production' / 'runs' / result['session_date'] / publisher._run_id()
        assert not canonical.exists()
        shutil.copytree(validation, canonical)
        copied_snapshot = canonical / snapshot_path.relative_to(validation)
        assert replay.sha256(copied_snapshot) == source['snapshot_manifest_sha256']
        replay.source._verify_input_snapshot(copied_snapshot.parent, json.loads(copied_snapshot.read_text(encoding='utf-8')))
        source['snapshot_manifest'] = str(copied_snapshot)
        source['publication_copy'] = dict(
            method='HASH_VERIFIED_VALIDATION_REPLAY_WITH_CANONICAL_SNAPSHOT_COPY',
            validation_run=str(validation),
            validation_manifest_sha256=replay.sha256(manifest_path),
            validation_source_manifest_sha256=replay.sha256(source_path),
            copied_at=datetime.now(replay.contract.IST).isoformat(),
            validation_run_modified=False,
        )
        replay.atomic_json(canonical / source_item['path'], source)
        result['run_id'] = canonical.name
        result['artifacts']['source_manifest']['sha256'] = replay.sha256(canonical / source_item['path'])
        replay.atomic_json(canonical / 'run_manifest.json', result)
        replay.atomic_json(canonical / 'replay_result.json', {**result,
            'artifacts': {name: str((canonical / item['path']).resolve()) for name, item in result['artifacts'].items()}})
        summary = publisher.publish_result(root, canonical, result)
        history = publisher.refresh_history(root, canonical, result)
        assert history is not None and history['days'] == result['days']
        assert verified_package(root, history=False)[0] == canonical
        assert verified_package(root, history=True)[1]['strategy_version'] == result['strategy_version']
        for name, expected in old.items():
            archived = root / 'latest' / 'archived' / expected['manifest']['strategy_version'] / name
            assert json.loads(archived.read_text(encoding='utf-8')) == json.loads(expected['pointer_bytes'])
            assert (expected['directory'] / 'run_manifest.json').read_bytes() == expected['manifest_bytes']
            verified_package(root, history=name == 'current_history.json', allow_legacy=True,
                archived_version=expected['manifest']['strategy_version'])
        assert manifest_path.read_bytes() == original_manifest_bytes
        assert source_path.read_bytes() == original_source_bytes
        receipt = dict(state='PUBLISHED_VERIFIED_V2_DAILY_AND_HISTORY',
            strategy_version=result['strategy_version'], strategy_fingerprint=result['strategy_fingerprint'],
            validation_run=str(validation), canonical_daily=str(canonical), canonical_history=history['run_dir'],
            daily_metrics=result['metrics'], history_metrics=history['metrics'],
            archived_strategy_version=replay.contract.LEGACY_STRATEGY_VERSION,
            archived_manifests_unchanged=True, validation_run_unchanged=True,
            published_at=datetime.now(replay.contract.IST).isoformat())
        replay.atomic_json(Path('.codex_tmp/g3_v2_daily_publication_receipt_20261012.json'), receipt)
        print(json.dumps(receipt, indent=2))


if __name__ == '__main__':
    main()
