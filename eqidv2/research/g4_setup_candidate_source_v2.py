"""Verified pre-setup-threshold candidates for the original 49 G4 setups.

The archived strict source has already applied common direction/EMA, price
0.10%, OI 0.05..1.00%, volume 0.80 and confirmation-direction gates. Those
shared gates are fixed in this research. Every subsequent setup rejection is
retained, permitting both lower and higher setup thresholds and replacement
before ranking. Opening candidates come from the complete dated feature export.
No default configuration, original cache or current-run pointer is written.
"""
from __future__ import annotations

import argparse
from datetime import timedelta
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g4_full_candidate_pool as legacy
from research import g4_g3_mirror_backtest as base
from research import g4_complete_inventory_backtest as complete
from research import g3_0950_long_backtest as morning
from research.g3_0920_joint_70_search import broad_profile
from research.g3_0920_strategy import selection_audit_0920

VERSION = 'G4-49-SETUP-CANDIDATE-SOURCE-V2'
DEFAULT_CACHE = base.ROOT/'outputs/g4_setup_candidate_source_v2_20261016'
OPENING_FEATURES = base.ROOT/'outputs/g4_0920_short_indicator_features_20261016/features_with_today.parquet'
BASELINE_RUN = base.G4_ROOT/'runs/2026-10-16/20261016T180807007853_d3e835ac_late_slots'
BASELINE_MANIFEST_SHA256 = '0c182c2742a50586b3d059b23c68efd1be8a099c6a136cd49a93292d6e72eff0'
SETUP_IDS = tuple(sorted(base.frozen_donor._ACTIVE_BY_ID))


def source_gate(row):
    """Fixed source/chronology/base validity, excluding tunable setup thresholds."""
    contract = base.production
    row = contract._candidate(row)
    spec = base.frozen_donor._ACTIVE_BY_ID.get(str(row.get('setup_id')))
    if spec is None:
        return False
    if spec.signal_end == '09:20' and str(row.get('source_reasons', '')):
        return False
    try:
        signal = contract._stamp(row.get('signal_timestamp'))
        confirm = contract._stamp(row.get('confirmation_timestamp'))
        feature = contract._stamp(row.get('v9_1m_feature_ts'))
        day = pd.Timestamp(row['day']).date()
        if (signal.date() != day or signal.strftime('%H:%M') != spec.signal_end
                or signal != signal.floor('min') or confirm != signal + timedelta(minutes=1)
                or feature != confirm or row.get('signal_end', spec.signal_end) != spec.signal_end):
            return False
        for key, ceiling in (('v9_5m_feature_ts',signal), ('v9_feature_available_ts',confirm)):
            if key in row and contract._stamp(row[key]) > ceiling:
                return False
    except (ValueError, TypeError):
        return False
    if not contract._confirmation_valid(row,spec.side):
        return False
    if contract.base_signal_side(row,spec.signal_end,session_date=day) != spec.side:
        return False
    fields = ('price_change_pct','oi_change_pct','volume_ratio','body_ratio',
              'wick_ratio','traded_value','v9_1m_volume_ratio')
    values = {key:contract._number(row.get(key)) for key in fields}
    return (all(np.isfinite(x) for x in values.values())
            and 0 <= values['body_ratio'] <= 1 and 0 <= values['wick_ratio'] <= 1
            and values['traded_value'] >= 0 and values['v9_1m_volume_ratio'] >= 0)


def _json(path, value):
    path.write_text(json.dumps(base.source._json_ready(value),indent=2,default=str,
                               allow_nan=False)+'\n',encoding='utf-8')


def load(cache_dir=DEFAULT_CACHE):
    cache = Path(cache_dir).resolve()
    manifest = base.read(cache/'manifest.json')
    if manifest.get('version') != VERSION or manifest.get('complete') is not True:
        raise ValueError('Incomplete candidate research package')
    if manifest['code_sha256'] != base.sha256(Path(__file__)):
        raise ValueError('Candidate source code changed; use a fresh research directory')
    for name,digest in manifest['files'].items():
        legacy._checked_file(cache,name,digest)
    for name,digest in manifest['source_sha256'].items():
        if base.sha256(Path(name)) != digest:
            raise ValueError('Candidate input changed: '+name)
    return pd.read_pickle(cache/'pool.pkl'),base.read(cache/'proof.json')


def build_pool(cache_dir=None, *, opening_features=OPENING_FEATURES):
    """Return (candidate DataFrame, source proof), optionally save fresh package.

    ``source_gate_pass`` separates immutable source/base validity from setup
    eligibility. ``baseline_filter_pass`` and ``baseline_selected`` are audit
    fields only; do not prefilter on them when testing relaxed settings.
    """
    cache = Path(cache_dir).resolve() if cache_dir is not None else None
    if cache is not None and cache.exists():
        return load(cache)
    g4_summary,g4_config = base._checked_artifacts(BASELINE_RUN,base.G4_ROOT,
        BASELINE_MANIFEST_SHA256,{'summary.json','config.json','selected_orders.csv',
                                  'g4_bounded_exit/portfolio_trades.csv'})
    g3_run = Path(g4_summary['current_g3_source_run'])
    g3_summary,_ = base._checked_artifacts(g3_run,base.G3_ROOT,
        g4_summary['current_g3_manifest_sha256'],{'summary.json','config.json','selected_orders.csv'})
    days = g4_summary['days']
    if days != g3_summary['days'] or days[-1] != '2026-10-16':
        raise ValueError('Candidate source requires the exact 53-session completed calendar')
    original = base.read_frozen_csv(BASELINE_RUN/'selected_orders.csv')
    original = original.loc[original.setup_id.isin(SETUP_IDS)].copy()
    effective_run = Path(g4_summary['baseline_g4_run'])
    effective_summary = base.read(effective_run/'summary.json')
    effective_config = base.read(effective_run/'config.json')
    scenario = effective_summary['primary_scenario']
    guards = dict(effective_config['scenarios'][scenario])
    volume_overrides = effective_config['five_minute_volume_ratio_overrides'][scenario]
    source_hashes = {str(BASELINE_RUN/'manifest.json'):BASELINE_MANIFEST_SHA256,
        str(g3_run/'manifest.json'):g4_summary['current_g3_manifest_sha256'],
        str(effective_run/'manifest.json'):g4_summary['baseline_g4_manifest_sha256']}
    signals,historical_days,_,morning_proof = morning.load_sources(
        morning.DEFAULT_HISTORY_PROVENANCE,morning.DEFAULT_RECENT_PROVENANCE,legacy.OCTOBER12_SOURCE)
    snapshot_days,snapshots,snapshot_proof = legacy._source_days_and_snapshots()
    if historical_days != snapshot_days or [str(d) for d in historical_days] != days[:49]:
        raise ValueError('Historical candidate calendar mismatch')
    sealed_manifest = base.read(legacy.DEFAULT_SEALED_SOURCE/'dataset_manifest.json')
    sealed_path = legacy._checked_file(legacy.DEFAULT_SEALED_SOURCE,'signals.parquet',
                                    sealed_manifest['output_sha256']['signals.parquet'])
    source_hashes[str(sealed_path)] = base.sha256(sealed_path)
    source_hashes[str(legacy.DEFAULT_SEALED_SOURCE/'dataset_manifest.json')] = base.sha256(legacy.DEFAULT_SEALED_SOURCE/'dataset_manifest.json')
    sealed = pd.read_parquet(sealed_path)
    native = signals.loc[((signals.hhmm_int>=925)&(signals.hhmm_int<=1000)) | signals.hhmm_int.eq(1120)].copy()
    parts = [legacy._normalize(native)]
    late_proof = {}
    for name,source_cache,clocks,sid_base in (
        ('late',legacy.LATE_CACHE,legacy.LATE_CLOCKS,4_000_000),
        ('extended',legacy.DEFAULT_CACHE,legacy.CLOCKS,5_000_000),
        ('midday',legacy.MIDDAY_CACHE,legacy.MIDDAY_CLOCKS,6_000_000),
        ('afternoon',legacy.AFTERNOON_CACHE,legacy.AFTERNOON_CLOCKS,7_000_000)):
        frame,evidence = legacy._late_readonly(source_cache,clocks,sid_base,
                                              historical_days,snapshots,sealed)
        parts.append(frame)
        late_proof[name] = evidence
        for item in evidence['recent_day_manifests'].values():
            folder = Path(item['manifest']).parent
            for filename,key in (('manifest.json','manifest_sha256'),
                                 ('candidates.parquet','candidates_sha256'),('paths.npz','paths_sha256')):
                source_hashes[str(folder/filename)] = item[key]
    opening_features = Path(opening_features).resolve()
    opening_frame = pd.read_parquet(opening_features)
    opening_frame = opening_frame.loc[opening_frame.session_date.astype(str).isin(days)].copy()
    if set(opening_frame.session_date.astype(str)) != set(days):
        raise ValueError('Opening full-universe export lacks completed session coverage')
    opening = selection_audit_0920(opening_frame,profile=broad_profile())
    opening = opening.loc[opening.side.eq('LONG')].copy()
    opening['opening_0920_status'] = np.where(opening.source_reasons.eq(''),'VALID','INVALID')
    parts.append(legacy._normalize(opening))
    source_hashes[str(opening_features)] = base.sha256(opening_features)
    daily_proof = []
    for item in g3_summary['daily_path_evidence']:
        directory = Path(item['run_dir'])
        manifest = base.read(directory/'run_manifest.json')
        if manifest.get('complete') is not True or manifest.get('execution_authority') is not False:
            raise ValueError('Incomplete daily source')
        record = manifest['artifacts']['candidate_signals']
        path = legacy._checked_file(directory,record['path'],record['sha256'])
        frame = legacy._normalize(base.read_frozen_csv(path))
        parts.append(frame.loc[~frame.hhmm_int.eq(920)].copy())
        source_hashes[str(path)] = record['sha256']
        source_hashes[str(directory/'run_manifest.json')] = base.sha256(directory/'run_manifest.json')
        daily_proof.append(dict(path=str(path),sha256=record['sha256']))
    pool = pd.concat(parts,ignore_index=True,sort=False)
    pool['day'] = pool.day.astype(str).str[:10]
    mapping = {(s.signal_end,s.side):s.setup_id for s in base.frozen_donor.ACTIVE_SETUPS}
    pool['setup_id'] = [mapping.get((str(c),str(s)),'') for c,s in zip(pool.signal_end,pool.side)]
    pool = pool.loc[pool.setup_id.isin(SETUP_IDS)].sort_values(
        ['day','setup_id','tradingsymbol'],kind='stable').reset_index(drop=True)
    if pool.duplicated(['day','setup_id','tradingsymbol']).any() or set(pool.day)-set(days):
        raise ValueError('Duplicate or out-of-calendar pre-setup candidate')
    pool['sid'] = np.arange(190_000_000,190_000_000+len(pool),dtype=np.int64)
    identities = {(str(r.day)[:10],r.setup_id,r.tradingsymbol):int(r.sid) for r in original.itertuples()}
    pool['sid'] = [identities.get((r.day,r.setup_id,r.tradingsymbol),int(r.sid)) for r in pool.itertuples()]
    if pool.sid.duplicated().any():
        raise ValueError('Stable candidate IDs overlap')
    pool['source_gate_pass'] = [source_gate(row) for row in pool.to_dict('records')]
    prices = complete.Prices(g3_summary)
    enrich_ids = set(guards)|set(base.production.MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP)
    enrich_mask = pool.source_gate_pass & pool.setup_id.isin(enrich_ids)
    print(f'Prepared {len(pool)} pre-setup rows; {int(pool.source_gate_pass.sum())} fixed-gate valid; enriching {int(enrich_mask.sum())}.',flush=True)
    enriched = prices.enrich(pool.loc[enrich_mask].sort_values(['tradingsymbol','day']))
    for field in ('research_ema20','research_atr14','research_extension_atr','research_feature_source'):
        pool.loc[enriched.index,field] = enriched[field]
    special = enriched.index[enriched.setup_id.isin(base.production.MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP)]
    pool.loc[special,'v9_1m_ema20'] = pool.loc[special,'research_ema20']
    pool.loc[special,'v9_1m_atr14'] = pool.loc[special,'research_atr14']
    # This audit is retained in full: FILTER_REJECTED rows remain research inputs.
    audit = base.donor.selection_audit(pool)
    audit['day'] = audit.day.astype(str).str[:10]
    selected = base.reselect(audit,guards,volume_overrides)
    actual,expected = base.selected_keys(selected),base.selected_keys(original)
    if actual != expected:
        raise ValueError(f'Current G4 selection mismatch: missing={sorted(expected-actual)}, extra={sorted(actual-expected)}')
    audit['baseline_filter_pass'] = audit.v9_filter_pass.astype(bool)
    audit['baseline_selected'] = [(r.day,r.setup_id,r.tradingsymbol) in expected for r in audit.itertuples()]
    for path,record in prices.proof.items():
        source_hashes[path] = record['sha256']
    for snap in prices.snapshots.values():
        manifest_path = snap/'snapshot_manifest.json'
        source_hashes[str(manifest_path)] = base.sha256(manifest_path)
    proof = dict(version=VERSION,scope='BACKTEST_ONLY',execution_authority=False,
        days=days,sessions=len(days),setup_ids=list(SETUP_IDS),candidate_count=len(audit),
        source_gate_valid_count=int(audit.source_gate_pass.sum()),
        baseline_setup_filter_rejections_retained=int((~audit.baseline_filter_pass).sum()),
        baseline_selection_reproduced=True,baseline_selected_orders=len(selected),
        baseline_run=str(BASELINE_RUN),baseline_manifest_sha256=BASELINE_MANIFEST_SHA256,
        g3_run=str(g3_run),g3_summary=g3_summary,
        effective_selection=dict(guards=guards,volume_overrides=volume_overrides,scenario=scenario),
        declared_configuration=g4_config,
        configuration_discrepancies=[dict(setup_id=s,declared_extension_atr=v,
            effective_extension_atr=guards.get(s)) for s,v in
            g4_config['maximum_confirmation_extension_atr_by_setup'].items()
            if s not in base.production.MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP and guards.get(s)!=v],
        fixed_gates=dict(minimum_directional_price_change_pct=.10,minimum_oi_change_pct=.05,
            maximum_oi_change_pct=1.,minimum_5m_volume_ratio=.80,
            ema_alignment='9>20>50 LONG; 9<20<50 SHORT',
            confirmation='Exact next-minute candle direction and break of signal close',
            nifty_gate='Existing 09:20/09:25 SHORT gate is fixed'),
        opening=dict(path=str(opening_features),sha256=base.sha256(opening_features),
                     evidence='RETROSPECTIVE_RESEARCH',all_filter_rejections_retained=True),
        morning=morning_proof,late_families=late_proof,daily=daily_proof,
        snapshot_inventory=snapshot_proof,price_sources=prices.proof,
        limitations=['Raw sources are complete before setup thresholds, not before fixed shared base gates.',
            'Opening historical OI is retrospective; candidate reconstruction does not establish live observability.',
            'Baseline uses the effective guards of the executed V13 ledger; declared/effective differences are disclosed.',
            'All 53 sessions were inspected previously; chronological testing is not a pristine holdout.'])
    if cache is not None:
        if not cache.is_relative_to(base.ROOT.resolve()) and not cache.is_relative_to((base.G4_ROOT/'research').resolve()):
            raise ValueError('Candidate cache must stay in workspace or G4 research root')
        cache.mkdir(parents=True)
        audit.to_pickle(cache/'pool.pkl')
        selected.to_csv(cache/'baseline_selected.csv',index=False)
        _json(cache/'proof.json',proof)
        _json(cache/'manifest.json',dict(version=VERSION,complete=True,
            code_sha256=base.sha256(Path(__file__)),source_sha256=source_hashes,
            files={p.name:base.sha256(p) for p in cache.iterdir() if p.is_file()}))
    return audit,proof


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=DEFAULT_CACHE)
    args = parser.parse_args()
    frame,evidence = build_pool(args.output)
    print(json.dumps({k:evidence[k] for k in ('candidate_count','source_gate_valid_count',
        'baseline_setup_filter_rejections_retained','baseline_selected_orders',
        'baseline_selection_reproduced','configuration_discrepancies')},indent=2),flush=True)
