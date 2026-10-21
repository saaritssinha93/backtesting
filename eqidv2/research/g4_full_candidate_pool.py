"""Read-only reconstruction of all 49 G3/G4 setup pools before ranking.

Only candidates passing the unchanged native filters are returned. Excluding
failed native filters is equivalent for any experiment that only adds a gate.
No source cache, active configuration, result pointer, or runtime file is written.
"""
from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g4_g3_mirror_backtest as base
from research import g3_0950_long_backtest as morning
from research.g3_g4_late_combined_backtest import G3_RUN
from research.g4_late_slot_source import (
    DEFAULT_CACHE as LATE_CACHE, LATE_CLOCKS, CLOCK_POLICY, _source_days_and_snapshots,
)
from research.g4_extended_slot_backtest import (
    DEFAULT_CACHE, CLOCKS, MIDDAY_CACHE, MIDDAY_CLOCKS, AFTERNOON_CACHE, AFTERNOON_CLOCKS,
)
from fno_v13_v10_g_3_backtest import OCTOBER12_SOURCE
from fno_v13_v10_g_3_daily_replay import DEFAULT_SEALED_SOURCE


def _normalize(frame):
    frame = morning.normalize_signals(frame)
    for target, origin in {
        'signal_timestamp': 'signal_ts', 'confirmation_timestamp': 'confirmation_ts',
        'confirm_open': 'confirmation_open', 'confirm_high': 'confirmation_high',
        'confirm_low': 'confirmation_low', 'confirm_close': 'confirmation_close',
    }.items():
        frame[target] = frame[target].fillna(frame[origin]) if target in frame else frame[origin]
    return frame


def _checked_file(root, relative, digest):
    path = (Path(root) / relative).resolve()
    if not path.is_relative_to(Path(root).resolve()) or base.sha256(path) != digest:
        raise ValueError('Source artifact drift or escaping path: ' + str(path))
    return path


def _late_readonly(cache, clocks, sid_base, days, snapshots, sealed):
    """Read existing caches; unlike load_late_source, never rebuild a cache."""
    wanted = {int(clock.replace(':', '')) for clock in clocks}
    frames = [_normalize(sealed.loc[sealed.hhmm_int.isin(wanted)].copy())]
    recent_proof = {}
    sealed_days = {str(x) for x in base.read(DEFAULT_SEALED_SOURCE/'dataset_manifest.json')['days']}
    policy = CLOCK_POLICY if tuple(clocks) == LATE_CLOCKS and sid_base == 4_000_000 else (
        'G4_CLOCKS_' + '_'.join(clocks) + f'_SID_{sid_base}_V1')
    for day in days:
        if str(day) in sealed_days:
            continue
        folder = cache / str(day)
        manifest = base.read(folder/'manifest.json')
        snapshot = snapshots[day]
        if (manifest.get('complete') is not True or manifest.get('day') != str(day)
                or manifest.get('clock_policy') != policy
                or manifest.get('snapshot_manifest_sha256') != base.sha256(snapshot/'snapshot_manifest.json')):
            raise ValueError('Incomplete or changed late source cache: ' + str(folder))
        candidates = _checked_file(folder, 'candidates.parquet', manifest['candidate_sha256'])
        _checked_file(folder, 'paths.npz', manifest['paths_sha256'])
        frame = _normalize(pd.read_parquet(candidates))
        if len(frame) != manifest['candidate_count'] or not set(frame.hhmm_int).issubset(wanted):
            raise ValueError('Late source cache scope changed')
        frames.append(frame)
        recent_proof[str(day)] = dict(manifest=str(folder/'manifest.json'),
            manifest_sha256=base.sha256(folder/'manifest.json'), candidates_sha256=manifest['candidate_sha256'],
            paths_sha256=manifest['paths_sha256'], excluded_stocks=manifest.get('excluded_stocks'))
    return pd.concat(frames, ignore_index=True, sort=False), dict(
        cache=str(cache), clocks=list(clocks), recent_day_manifests=recent_proof)


def _cache_code():
    paths = list(base.code_proof(base.DEFAULT_CONFIG))
    paths.extend(str(base.ROOT / name) for name in (
        'research/g4_full_candidate_pool.py', 'research/g3_0950_long_backtest.py',
        'research/g3_six_confirmation_variants.py', 'research/g3_sep24_raw_pool.py',
        'research/g3_0920_strategy.py', 'research/g4_extended_slot_backtest.py',
        'fno_v13_v10_g_3_daily_replay.py', 'fno_v13_v10_g_daily_replay.py'))
    return {str(Path(p).resolve()):base.sha256(Path(p)) for p in paths}


def _cache_load(cache, g3summary, original, prices, reference):
    manifest = base.read(cache/'manifest.json')
    if (manifest['reference_manifest_sha256'] != base.sha256(reference/'manifest.json')
            or manifest['g3_summary_fingerprint'] != base.fingerprint(g3summary)
            or manifest['code_sha256'] != _cache_code()):
        raise ValueError('Prepared candidate cache identity or code changed')
    for filename,digest in manifest['source_sha256'].items():
        if base.sha256(Path(filename)) != digest:
            raise ValueError('Prepared candidate cache source changed: '+filename)
    for name,digest in manifest['files'].items():
        _checked_file(cache,name,digest)
    pool = pd.read_pickle(cache/'pool.pkl')
    proof = base.read(cache/'proof.json')
    prices.proof.update(base.read(cache/'price_proof.json'))
    if base.selected_keys(base.reselect(pool,{})) != base.selected_keys(original):
        raise ValueError('Prepared cache G3 selection mismatch')
    pinned_orders = base.read_frozen_csv(reference/'guard_all_ten/selected_orders.csv')
    limits = base.configuration()['scenarios']['guard_all_ten']
    if base.selected_keys(base.reselect(pool,limits)) != base.selected_keys(pinned_orders):
        raise ValueError('Prepared cache G4 selection mismatch')
    return pool,proof


def _cache_save(cache, pool, proof, prices, g3summary, reference, snapshots):
    if cache.exists():
        raise ValueError('Prepared candidate cache must be a fresh directory')
    directories = {DEFAULT_SEALED_SOURCE, G3_RUN, OCTOBER12_SOURCE}
    history=base.read(morning.DEFAULT_HISTORY_PROVENANCE)
    recent=base.read(morning.DEFAULT_RECENT_PROVENANCE)
    directories.update(Path(x['run']) for x in history['extensions'])
    # load_sources consumes Oct8/9 feature ledgers only. The provenance also
    # contains an older Oct12 scanner record with a different schema; the
    # full Oct12 source is covered separately by OCTOBER12_SOURCE above.
    directories.update(Path(recent['recent_sources'][day]['feature_ledger']).parent
                       for day in ('2026-10-08','2026-10-09'))
    directories.update(Path(x['run_dir']) for x in g3summary['daily_path_evidence'])
    files={Path(p) for p in prices.proof}
    files.update((morning.DEFAULT_HISTORY_PROVENANCE,morning.DEFAULT_RECENT_PROVENANCE))
    for directory in directories:
        files.update(p for p in directory.iterdir() if p.is_file())
    for directory in snapshots.values():
        manifest_path=directory/'snapshot_manifest.json'
        files.add(manifest_path)
        files.update(directory/x['snapshot_relative_path'] for x in base.read(manifest_path)['sources']
                     if x.get('captured'))
    for family in proof['late_families'].values():
        for item in family['recent_day_manifests'].values():
            folder=Path(item['manifest']).parent
            files.update(folder/x for x in ('manifest.json','candidates.parquet','paths.npz'))
    # The old Sep24 replay refers to its contemporaneous dated-universe file
    # outside the physical Sep25 snapshot; retain that source digest too.
    for directory in directories:
        path=directory/'source_manifest.json'
        if path.is_file():
            for item in base.read(path).get('sources',[]):
                if item.get('role')=='DATED_UNIVERSE' and item.get('path'):
                    files.add(Path(item['path']))
    digests={str(p.resolve()):base.sha256(p) for p in sorted(files)}
    cache.mkdir(parents=True)
    # Pickle preserves heterogeneous frozen input types; its digest is checked
    # before reading and the cache is restricted to this local workspace.
    pool.to_pickle(cache/'pool.pkl')
    (cache/'proof.json').write_text(json.dumps(proof,indent=2)+'\n',encoding='utf-8')
    (cache/'price_proof.json').write_text(json.dumps(prices.proof,indent=2)+'\n',encoding='utf-8')
    manifest=dict(reference_manifest_sha256=base.sha256(reference/'manifest.json'),
        g3_summary_fingerprint=base.fingerprint(g3summary),code_sha256=_cache_code(),source_sha256=digests,
        files={p.name:base.sha256(p) for p in cache.iterdir() if p.is_file()})
    (cache/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')


def build_pool(g3summary, original_orders, prices, pinned_g4_run, *, cache_dir=None):
    """Return (native-eligible pre-quota candidates, verified source evidence).

    Existing G3 and G4 SIDs are retained; new candidate IDs begin at 97 million.
    Existing 10:35 EMA/ATR inputs and the ten extra G4 extensions are calculated
    causally with the same helper as the pinned G4 replay.
    """
    base.configuration()
    pinned_g4_run = Path(pinned_g4_run)
    prepared_cache = Path(cache_dir).resolve() if cache_dir is not None else None
    if prepared_cache is not None and not prepared_cache.is_relative_to(base.ROOT.resolve()):
        raise ValueError('Prepared candidate cache must remain inside the workspace')
    if prepared_cache is not None and prepared_cache.exists():
        return _cache_load(prepared_cache,g3summary,original_orders,prices,pinned_g4_run)
    pinned = base.verify(pinned_g4_run)
    if any(pinned[field] != g3summary[field] for field in ('first_date','last_date','sessions')):
        raise ValueError('G3 and G4 reference calendars differ')
    signals, days, _, morning_proof = morning.load_sources(
        morning.DEFAULT_HISTORY_PROVENANCE, morning.DEFAULT_RECENT_PROVENANCE, OCTOBER12_SOURCE)
    snapshot_days, snapshots, snapshot_proof = _source_days_and_snapshots()
    if days != snapshot_days or [str(d) for d in days] != g3summary['days'][:49]:
        raise ValueError('Full candidate calendar differs from G3')
    snapshot_members = {}
    for day, directory in snapshots.items():
        manifest = base.read(directory/'snapshot_manifest.json')
        for item in manifest['sources']:
            if item.get('captured'):
                _checked_file(directory, item['snapshot_relative_path'], item['sha256'])
        snapshot_members[str(day)] = dict(manifest=str(directory/'snapshot_manifest.json'),
            sha256=base.sha256(directory/'snapshot_manifest.json'), members_verified=True)
    sealed_manifest = base.read(DEFAULT_SEALED_SOURCE/'dataset_manifest.json')
    for name in ('signals.parquet', 'paths.npz'):
        _checked_file(DEFAULT_SEALED_SOURCE, name, sealed_manifest['output_sha256'][name])
    sealed = pd.read_parquet(DEFAULT_SEALED_SOURCE/'signals.parquet')
    # Native source owns the original morning slots and 11:20; each added
    # clock family has its own frozen source envelope and cache.
    native = signals.loc[((signals.hhmm_int >= 925) & (signals.hhmm_int <= 1000))
                         | signals.hhmm_int.eq(1120)].copy()
    parts = [_normalize(native)]
    late_proof = {}
    for name, source_cache, clocks, sid_base in (
        ('late', LATE_CACHE, LATE_CLOCKS, 4_000_000),
        ('extended', DEFAULT_CACHE, CLOCKS, 5_000_000),
        ('midday', MIDDAY_CACHE, MIDDAY_CLOCKS, 6_000_000),
        ('afternoon', AFTERNOON_CACHE, AFTERNOON_CLOCKS, 7_000_000),
    ):
        frame, evidence = _late_readonly(source_cache, clocks, sid_base, days, snapshots, sealed)
        parts.append(frame)
        late_proof[name] = evidence
    opening_manifest = base.read(G3_RUN/'manifest.json')
    opening_path = _checked_file(G3_RUN, 'selection_audit_0920.csv',
                                opening_manifest['files']['selection_audit_0920.csv'])
    opening = base.read_frozen_csv(opening_path)
    opening = opening.loc[opening.side.eq('LONG') & opening.v9_filter_pass.eq(True)].copy()
    # This status describes verified historical input quality only. The
    # archived availability flags and RETROSPECTIVE_RESEARCH label stay intact.
    if not opening.research_feature_status.eq('COMPLETE').all():
        raise ValueError('Historical opening eligibility has incomplete inputs')
    opening['opening_0920_status'] = 'VALID'
    parts.append(_normalize(opening))
    daily_proof = []
    for item in g3summary['daily_path_evidence']:
        directory = Path(item['run_dir'])
        manifest = base.read(directory/'run_manifest.json')
        if manifest.get('complete') is not True or manifest.get('execution_authority') is not False:
            raise ValueError('Daily candidate source incomplete or armed')
        record = manifest['artifacts']['candidate_signals']
        path = _checked_file(directory, record['path'], record['sha256'])
        frame = _normalize(base.read_frozen_csv(path))
        parts.append(frame)
        daily_proof.append(dict(path=str(path), sha256=record['sha256'],
            run_manifest_sha256=base.sha256(directory/'run_manifest.json')))
    pool = pd.concat(parts, ignore_index=True, sort=False)
    pool['day'] = pool.day.astype(str).str[:10]
    if set(pool.day) - set(g3summary['days']):
        raise ValueError('Candidate escaped comparison calendar')
    if pool.duplicated(['day','hhmm_int','side','tradingsymbol']).any():
        raise ValueError('Duplicate source candidate in disjoint clock pools')
    pool['sid'] = np.arange(97_000_000,97_000_000+len(pool))
    # Preassign setup IDs before enrichment; the donor independently resolves
    # and checks them from the signal clock during selection_audit.
    pool['setup_id'] = [base.donor.setup_for(str(clock),str(side)).setup_id
        if base.donor.setup_for(str(clock),str(side)) is not None else ''
        for clock,side in zip(pool.signal_end,pool.side)]
    pool = pool.loc[pool.setup_id.isin(base.donor._ACTIVE_BY_ID)].reset_index(drop=True)
    guarded = pool.setup_id.isin({'1036_LONG','1036_SHORT'})
    features = prices.enrich(pool.loc[guarded].copy())
    pool.loc[guarded,'v9_1m_ema20'] = features.research_ema20
    pool.loc[guarded,'v9_1m_atr14'] = features.research_atr14
    audit = base.donor.selection_audit(pool)
    actual = base.selected_keys(audit.loc[audit.v9_selected])
    expected = base.selected_keys(original_orders)
    if actual != expected:
        raise ValueError(f'Full pre-quota G3 selection mismatch: missing={sorted(expected-actual)}, extra={sorted(actual-expected)}')
    pool = audit.loc[audit.v9_filter_pass.eq(True)].copy().reset_index(drop=True)
    # Also preserve all ten-setup archived IDs, including G4 replacements.
    archived_pool = base.read_frozen_csv(pinned_g4_run/'candidate_extension_audit.csv')
    identity = {}
    for frame in (original_orders,archived_pool):
        for row in frame.itertuples():
            key=(str(row.day)[:10],row.setup_id,row.tradingsymbol)
            if key in identity and identity[key] != int(row.sid):
                raise ValueError('Pinned G3/G4 source IDs disagree')
            identity[key]=int(row.sid)
    pool['sid'] = [identity.get((str(r.day)[:10],r.setup_id,r.tradingsymbol),int(r.sid))
                   for r in pool.itertuples()]
    if pool.sid.duplicated().any():
        raise ValueError('Full candidate IDs overlap')
    limits = base.configuration()['scenarios']['guard_all_ten']
    pool['research_extension_atr'] = np.nan
    extra = pool.setup_id.isin(limits)
    enriched = prices.enrich(pool.loc[extra].copy())
    for field in ('research_ema20','research_atr14','research_extension_atr','research_feature_source'):
        pool.loc[extra,field] = enriched[field]
    old_g4 = base.read_frozen_csv(pinned_g4_run/'guard_all_ten/selected_orders.csv')
    if base.selected_keys(base.reselect(pool,limits)) != base.selected_keys(old_g4):
        raise ValueError('Full pre-quota current G4 selection does not reproduce pinned reference')
    proof = dict(scope='ALL_49_ENABLED_SETUP_NATIVE_ELIGIBLE_CANDIDATES_BEFORE_QUOTA',
        enabled_setup_count=len(base.donor.ACTIVE_SETUPS), observed_setup_count=pool.setup_id.nunique(),
        source_matched_candidates=len(audit), native_filter_rejections=int((~audit.v9_filter_pass).sum()),
        native_eligible_candidates=len(pool), g3_selection_reproduced=True, g4_selection_reproduced=True,
        morning=morning_proof, late_families=late_proof, snapshot_inventory=snapshot_proof,
        snapshot_members=snapshot_members, daily=daily_proof,
        opening=dict(path=str(opening_path),sha256=base.sha256(opening_path),
            manifest_sha256=base.sha256(G3_RUN/'manifest.json'),
            native_eligible_count=len(opening), availability='RETROSPECTIVE_RESEARCH'),
        exclusions=g3summary['coverage']['excluded_sessions'],
        note='Failed native filters cannot become eligible under an additional rejection gate; they are excluded before feature computation.')
    if prepared_cache is not None:
        _cache_save(prepared_cache,pool,proof,prices,g3summary,pinned_g4_run,snapshots)
    return pool, proof
