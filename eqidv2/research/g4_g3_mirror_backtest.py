"""G4 mirror of current G3 historical rules with pre-quota extension experiments.

G3 artifacts and active configurations are read-only. Every portfolio is freshly
simulated from one-minute OHLC; changed setups are reselected from their complete
archived pre-quota candidate pools. The unchanged mirror must reproduce G3.
"""
from __future__ import annotations

import copy
from dataclasses import replace
from datetime import date, datetime
import hashlib
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import runtime_dir
import fno_v13_replay_source as source
import fno_v13_v10_g_3_contract as production
from g3_one_minute_extension import last_features, extension_atr
from research import g3_target_cutoff_contract as donor
from research.archived_g3_v11 import contract as frozen_donor
from research.g3_target_cutoff_backtest import _simulate, _same_native, _same_allocation, _daily
from research.g3_0920_backtest import _stats
from research.g4_extended_target_sweep import read_frozen_csv
from research.g4_late_slot_source import sha256, load_late_source, _source_days_and_snapshots

ROOT = Path(__file__).resolve().parents[1]
LEGACY_VERSION = 'V13-V10-G-4-G3-MIRROR-GUARD-RESEARCH-V7'
V8_VERSION = 'V13-V10-G-4-G3-MIRROR-GUARD-RESEARCH-V8'
PREVIOUS_VERSION = 'V13-V10-G-4-G3-MIRROR-GUARD-RESEARCH-V9'
VERSION = 'V13-V10-G-4-G3-MIRROR-GUARD-RESEARCH-V10'
DONOR_FINGERPRINT = 'fb0a2838879366715687f41700d8fe244cdbc0853805902de6806765c7369e27'
DEFAULT_CONFIG = ROOT / 'configs/strategies/v13_v10_g_4_g3_mirror_guard_v9.json'
G3_ROOT = runtime_dir('backtesting_result_v13_v10_g_3')
G4_ROOT = runtime_dir('backtesting_result_v13_v10_g_4')
LEGACY_SCENARIOS = {
    'g3_mirror_control': [],
    'guard_1235_short': ['1236_SHORT'],
    'guard_0945_long': ['0946_LONG'],
    'guard_0945_short': ['0946_SHORT'],
    'guard_1325_short': ['1326_SHORT'],
    'guard_all_four': ['0946_LONG', '0946_SHORT', '1236_SHORT', '1326_SHORT'],
}
NEW_GUARDS = {'0926_SHORT': 4.0, '0936_LONG': 3.5, '1001_LONG': 4.0,
              '1136_LONG': 3.5, '1146_SHORT': 4.0, '1301_SHORT': 3.5}
SCENARIOS = {name: {sid: 3.0 for sid in ids} for name, ids in LEGACY_SCENARIOS.items()}
SCENARIOS.update({
    'guard_0925_short': {'0926_SHORT': 4.0},
    'guard_0935_long': {'0936_LONG': 3.5},
    'guard_1000_long': {'1001_LONG': 4.0},
    'guard_1135_long': {'1136_LONG': 3.5},
    'guard_1145_short': {'1146_SHORT': 4.0},
    'guard_1300_short': {'1301_SHORT': 3.5},
    'guard_new_six': dict(NEW_GUARDS),
    'guard_all_ten': {**SCENARIOS['guard_all_four'], **NEW_GUARDS},
})


def guard_limits(extra):
    """V7 had a common 3 ATR ceiling; V8 declares each setup's own limit."""
    return dict(extra) if isinstance(extra, dict) else {sid: 3.0 for sid in extra}


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def configuration_payload(version=VERSION):
    if version not in (VERSION, PREVIOUS_VERSION, V8_VERSION, LEGACY_VERSION):
        raise ValueError('Unknown G4 mirror version')
    payload = dict(variant_id=version, strategy='V13-V10-G-4', scope='BACKTEST_ONLY',
        execution_authority=False, source_strategy_fingerprint=DONOR_FINGERPRINT,
        g3_contract=frozen_donor.strategy_payload(),
        primary_scenario='guard_all_ten', scenarios=copy.deepcopy(SCENARIOS),
        selection_method='RESELECT_CHANGED_SETUPS_BEFORE_QUOTA_THEN_FULL_MINUTE_PORTFOLIO_REPLAY')
    if version in (VERSION, PREVIOUS_VERSION):
        name = 'guard_all_ten_volume_0925_long_120'
        payload['scenarios'][name] = copy.deepcopy(SCENARIOS['guard_all_ten'])
        payload['five_minute_volume_ratio_overrides'] = {name: {'0926_LONG': 1.2}}
        payload['primary_scenario'] = name
    if version == VERSION:
        name = 'guard_all_ten_volume_0925_long_120_atr3'
        payload['scenarios'][name] = {**SCENARIOS['guard_all_ten'], '0926_LONG': 3.0}
        payload['five_minute_volume_ratio_overrides'][name] = {'0926_LONG': 1.2}
        payload['primary_scenario'] = name
    if version == LEGACY_VERSION:
        payload.update(maximum_additional_extension_atr=3.0, primary_scenario='guard_1235_short',
                       scenarios=copy.deepcopy(LEGACY_SCENARIOS))
    return payload


def configuration(path=DEFAULT_CONFIG):
    value = read(path)
    if frozen_donor.strategy_fingerprint() != DONOR_FINGERPRINT or value != configuration_payload(value.get('variant_id')):
        raise ValueError('G4 mirror donor or declared experiment changed; create a new version')
    return value


class Identity:
    def __init__(self, config, scenario):
        self.scenario = scenario
        self.extra = tuple(config['scenarios'][scenario])
        self.limits = guard_limits(config['scenarios'][scenario])
        self.volume_overrides = config.get('five_minute_volume_ratio_overrides', {}).get(scenario, {})
        self.STRATEGY_VERSION = config['variant_id'] + '-' + scenario.upper()
        self.ACTIVE_SETUPS = tuple(production.SetupSpec(**{
            **{key: value for key, value in vars(s).items() if key != 'setup_id'},
            'source_version': self.STRATEGY_VERSION,
            'volume_ratio': self.volume_overrides.get(s.setup_id, s.volume_ratio),
        }) for s in frozen_donor.ACTIVE_SETUPS)
        self._ACTIVE_BY_ID = {s.setup_id: s for s in self.ACTIVE_SETUPS}
        self.payload = copy.deepcopy(config['g3_contract'])
        self.payload.update(strategy_version=self.STRATEGY_VERSION, strategy_profile='V13_V10_G_4',
            accepted_variant=scenario, execution_authority=False, scope='BACKTEST_ONLY',
            source_strategy_version=frozen_donor.STRATEGY_VERSION, source_strategy_fingerprint=DONOR_FINGERPRINT)
        for key in ('active_setups', 'core_setups'):
            for item in self.payload[key]:
                item['source_version'] = self.STRATEGY_VERSION
                setup_id = item['confirmation_end'].replace(':', '') + '_' + item['side']
                if setup_id in self.volume_overrides:
                    item['volume_ratio'] = self.volume_overrides[setup_id]
        self.payload['maximum_confirmation_extension_atr_by_setup'].update(self.limits)
        self.payload['g4_five_minute_volume_ratio_overrides'] = dict(self.volume_overrides)

    def __getattr__(self, name):
        return getattr(donor, name)

    def strategy_fingerprint(self):
        return fingerprint(self.payload)

    def identify(self, frame):
        result = frame.copy()
        result['source_signal_id'] = result.signal_id
        result['signal_id'] = [production.canonical_signal_id(self.STRATEGY_VERSION,
            pd.Timestamp(row.day).date(), self._ACTIVE_BY_ID[row.setup_id].signal_end,
            self._ACTIVE_BY_ID[row.setup_id].confirmation_end, row.side, row.tradingsymbol)
            for row in result.itertuples()]
        result['strategy_version'] = self.STRATEGY_VERSION
        result['strategy_fingerprint'] = self.strategy_fingerprint()
        return result


def selected_keys(frame):
    return set(zip(frame.day.astype(str).str[:10], frame.setup_id, frame.tradingsymbol))


def reselect(pool, extra, volume_overrides=None):
    """Apply extra eligibility before the donor's unchanged ranking and quota."""
    volume_overrides = volume_overrides or {}
    limits = guard_limits(extra)
    if not (set(limits) | set(volume_overrides)).issubset(frozen_donor._ACTIVE_BY_ID):
        raise ValueError('G4 guard experiments must use the frozen 49-setup inventory')
    pool = pool.loc[pool.setup_id.isin(frozen_donor._ACTIVE_BY_ID)].copy()
    allowed = ~pool.setup_id.isin(limits) | pool.research_extension_atr.le(pool.setup_id.map(limits))
    eligible_pool = pool.loc[allowed].copy()
    unchanged = donor.select_orders(eligible_pool.loc[~eligible_pool.setup_id.isin(volume_overrides)].copy())
    unchanged = unchanged.loc[unchanged.setup_id.isin(frozen_donor._ACTIVE_BY_ID)].copy()
    selected = [unchanged]
    for setup_id, minimum in volume_overrides.items():
        original = frozen_donor._ACTIVE_BY_ID[setup_id]
        if minimum <= 0 or minimum > original.volume_ratio:
            raise ValueError('G4 volume override must relax one existing setup')
        setup = production.SetupSpec(**{
            **{key: value for key, value in vars(original).items() if key != 'setup_id'},
            'volume_ratio': minimum,
        })
        for day, group in eligible_pool.loc[eligible_pool.setup_id.eq(setup_id)].groupby('day'):
            rows = group.to_dict('records')
            passing = [row for row in rows if production.passes_selected_filters(row, setup, session_date=pd.Timestamp(day).date())]
            ranked = sorted(passing, key=lambda row: production._rank_key(row, setup))[:setup.max_entries]
            for rank, row in enumerate(ranked, 1):
                row.update(v9_selected=True, v9_filter_pass=True, v9_rank_in_setup_day=rank,
                           v9_decision='SELECTED', v10_g_f_core=True)
            if ranked:
                selected.append(pd.DataFrame(ranked))
    return pd.concat(selected, ignore_index=True, sort=False)


class Prices:
    def __init__(self, summary):
        _, self.snapshots, self.snapshot_proof = _source_days_and_snapshots()
        for item in summary['daily_path_evidence']:
            manifest = Path(item['snapshot_manifest'])
            if sha256(manifest) != item['snapshot_manifest_sha256']:
                raise ValueError('Daily snapshot manifest changed')
            self.snapshots[date.fromisoformat(read(manifest)['session_date'])] = manifest.parent
        self.expected = {}
        for directory in self.snapshots.values():
            manifest = read(directory / 'snapshot_manifest.json')
            self.expected.update({str((directory / x['snapshot_relative_path']).resolve()): x['sha256']
                                 for x in manifest['sources'] if x.get('captured')})
        self.cache, self.proof = {}, {}

    def frame(self, day, symbol):
        directory = self.snapshots.get(day)
        path = ((directory / 'equity_1m') if directory else source.DataRoots().equity_1m) / f'{symbol}_stocks_indicators_1min.parquet'
        key = str(path.resolve())
        if key not in self.cache:
            digest = sha256(path)
            if directory and self.expected.get(key) != digest:
                raise ValueError('Immutable minute file changed: ' + key)
            frame = pd.read_parquet(path)
            frame['ts'] = source.hybrid._to_ist(frame.date).dt.as_unit('ns')
            if frame.ts.isna().any():
                raise ValueError('Invalid minute timestamp: ' + key)
            self.cache[key] = frame.sort_values('ts', kind='stable').reset_index(drop=True)
            self.proof[key] = dict(sha256=digest, source_kind='IMMUTABLE_DAY_SNAPSHOT' if directory else 'CURRENT_HISTORICAL_PARQUET')
        frame = self.cache[key]
        target = frame.loc[frame.ts.dt.date.eq(day)]
        if target.ts.duplicated().any():
            raise ValueError(f'Duplicate minute timestamp: {day} {symbol}')
        return frame, key

    def enrich(self, pool):
        result = pool.copy()
        for index, row in result.iterrows():
            day = pd.Timestamp(row.day).date()
            raw, key = self.frame(day, row.tradingsymbol)
            stamp = pd.Timestamp(row.confirmation_ts)
            stamp = stamp.tz_localize(production.IST) if stamp.tzinfo is None else stamp.tz_convert(production.IST)
            observed = raw.loc[raw.ts.eq(stamp)]
            if len(observed) != 1 or stamp.date() != day:
                raise ValueError('Candidate has no exact confirmation candle')
            bar = observed.iloc[0]
            for name in ('open', 'high', 'low', 'close'):
                if not np.isclose(bar[name], row['confirmation_' + name], atol=.001, rtol=0):
                    raise ValueError(f'Candidate confirmation OHLC changed: {day} {row.tradingsymbol}')
            history = raw.loc[raw.ts.lt(stamp)].drop_duplicates('ts', keep='last')
            ema, atr = last_features(history, bar.to_dict())
            value = extension_atr(row.confirmation_close, ema, atr, row.side)
            if not np.isfinite(value):
                raise ValueError('Invalid extension features')
            result.loc[index, 'research_ema20'] = ema
            result.loc[index, 'research_atr14'] = atr
            result.loc[index, 'research_extension_atr'] = value
            result.loc[index, 'research_feature_source'] = key
        return result

    def paths(self, orders):
        paths = {}
        for row in orders.itertuples():
            day = pd.Timestamp(row.day).date()
            raw, _ = self.frame(day, row.tradingsymbol)
            stamp = pd.Timestamp(row.confirmation_ts)
            stamp = stamp.tz_localize(production.IST) if stamp.tzinfo is None else stamp.tz_convert(production.IST)
            cutoff = pd.Timestamp(source.slot_datetime(day, donor.SQUARE_OFF))
            frame = raw.loc[raw.ts.gt(stamp) & raw.ts.le(cutoff)].copy()
            required = pd.date_range(stamp + pd.Timedelta(minutes=1), cutoff, freq='min')
            if not pd.DatetimeIndex(frame.ts).equals(required):
                raise ValueError(f'Incomplete execution path: {day} {row.tradingsymbol}')
            for field in ('gap_filled', 'opening_snapshot', 'provisional_stale'):
                if field in frame:
                    flagged = pd.to_numeric(frame[field], errors='coerce').astype('Float64').fillna(0).ne(0) | frame[field].astype(str).str.lower().isin(['true','yes','on'])
                    if flagged.any():
                        raise ValueError(f'Flagged execution minute: {day} {row.tradingsymbol}')
            paths[int(row.sid)] = {'timestamp_ns': frame.ts.astype('int64').to_numpy(),
                **{name: frame[name].to_numpy(float) for name in ('open','high','low','close')}}
        return paths


def candidate_pool(summary, original, prices, scenarios, volume_overrides=None):
    from research import g3_0950_long_backtest as morning
    from fno_v13_v10_g_3_backtest import OCTOBER12_SOURCE
    from research.g4_extended_slot_backtest import AFTERNOON_CACHE, AFTERNOON_CLOCKS, MIDDAY_CACHE, MIDDAY_CLOCKS
    ids = {sid for extra in scenarios.values() for sid in extra}
    ids.update(sid for overrides in (volume_overrides or {}).values() for sid in overrides)
    if not ids.issubset(frozen_donor._ACTIVE_BY_ID):
        raise ValueError('G4 candidate pools must use the frozen 49-setup inventory')
    clocks = {int(frozen_donor._ACTIVE_BY_ID[sid].signal_end.replace(':', '')) for sid in ids}
    signals, days, _, morning_proof = morning.load_sources(morning.DEFAULT_HISTORY_PROVENANCE,
        morning.DEFAULT_RECENT_PROVENANCE, OCTOBER12_SOURCE)
    late, _, late_days, late_proof, _ = load_late_source(AFTERNOON_CACHE,
        clocks=AFTERNOON_CLOCKS, sid_base=7_000_000, verify_snapshots=True)
    if days != late_days or [str(d) for d in days] != summary['days'][:49]:
        raise ValueError('Candidate calendar differs from G3 baseline')
    parts = [signals.loc[signals.hhmm_int.isin(clocks & {925,935,945,1000})].copy(),
             late.loc[late.hhmm_int.isin(clocks)].copy()]
    midday_proof = None
    if clocks & {1135,1145}:
        midday, _, midday_days, midday_proof, _ = load_late_source(MIDDAY_CACHE,
            clocks=MIDDAY_CLOCKS, sid_base=6_000_000, verify_snapshots=True)
        if midday_days != days:
            raise ValueError('Midday candidate calendar differs from G3 baseline')
        parts.append(midday.loc[midday.hhmm_int.isin(clocks)].copy())
    daily_proof = []
    for item in summary['daily_path_evidence']:
        directory = Path(item['run_dir'])
        manifest = read(directory / 'run_manifest.json')
        record = manifest['artifacts']['candidate_signals']
        path = directory / record['path']
        if sha256(path) != record['sha256']:
            raise ValueError('Daily candidate evidence changed')
        frame = morning.normalize_signals(read_frozen_csv(path))
        parts.append(frame.loc[frame.hhmm_int.isin(clocks)].copy())
        daily_proof.append(dict(path=str(path), sha256=sha256(path)))
    pool = pd.concat(parts, ignore_index=True, sort=False)
    if pool.duplicated(['day','hhmm_int','side','tradingsymbol']).any():
        raise ValueError('Duplicate pre-quota candidate')
    pool['sid'] = np.arange(96_000_000, 96_000_000 + len(pool))
    pool = donor.selection_audit(pool)
    pool = pool.loc[pool.setup_id.isin(ids)].reset_index(drop=True)
    before = pool.loc[pool.v9_selected]
    expected = original.loc[original.setup_id.isin(ids)]
    if selected_keys(before) != selected_keys(expected):
        raise ValueError('Unchanged pre-quota selection does not reproduce G3')
    old_ids = {(str(row.day)[:10],row.setup_id,row.tradingsymbol): int(row.sid) for row in expected.itertuples()}
    pool['sid'] = [old_ids.get((str(row.day)[:10],row.setup_id,row.tradingsymbol),int(row.sid)) for row in pool.itertuples()]
    pool = prices.enrich(pool)
    return pool, dict(morning=morning_proof, afternoon=late_proof, midday=midday_proof, daily=daily_proof,
                      baseline_selection_reproduced=True, candidate_count=len(pool))


def code_proof(config_path):
    import fno_v13_v10_g_2_backtest as sim
    modules = [Path(__file__), Path(config_path), Path(production.__file__), Path(donor.__file__),
        Path(frozen_donor.__file__), frozen_donor.CONFIG,
        Path(source.__file__), ROOT/'g3_one_minute_extension.py',
        ROOT/'research/g3_target_cutoff_backtest.py', ROOT/'research/g4_late_slot_source.py',
        ROOT/'fno_v13_v10_g_4_backtest.py', Path(sim.__file__), Path(sim.g.__file__),
        Path(sim.g.v9.__file__), Path(sim.g.v9.v5.__file__), Path(sim.g.v9.v6.__file__)]
    return {str(path.resolve()):sha256(path) for path in modules}


def _checked_artifacts(directory, root, expected_digest, required):
    """Verify a saved package without invoking current strategy selection."""
    directory, root = Path(directory).resolve(), Path(root).resolve()
    if not directory.is_relative_to(root) or sha256(directory/'manifest.json') != expected_digest:
        raise ValueError('Invalid archived baseline reference')
    manifest = read(directory/'manifest.json')
    if not required.issubset(manifest.get('files', {})):
        raise ValueError('Archived baseline manifest lacks required evidence')
    for name, digest in manifest['files'].items():
        relative = Path(name)
        member = (directory/relative).resolve()
        if (relative.is_absolute() or '..' in relative.parts or not member.is_relative_to(directory)
                or sha256(member) != digest):
            raise ValueError('Archived baseline artifact changed: ' + name)
    return read(directory/'summary.json'), read(directory/'config.json')


def _resolve_frozen_baseline(pointer):
    """Keep existing G4 research on V11 when the current G3 book advances."""
    required = {'summary.json', 'config.json', 'selected_orders.csv', 'portfolio_trades.csv'}
    reference = pointer
    directory = (G3_ROOT/reference['run_dir']).resolve()
    summary, config = _checked_artifacts(directory, G3_ROOT, reference['manifest_sha256'], required)
    if config != frozen_donor.strategy_payload():
        # The last sealed G4 result records the exact G3 donor it reproduced.
        saved_pointer = read(G4_ROOT/'latest/g3_mirror.json')
        if saved_pointer.get('execution_authority') is not False:
            raise ValueError('Saved G4 baseline reference has execution authority')
        saved_run = (G4_ROOT/saved_pointer['run_dir']).resolve()
        saved, saved_config = _checked_artifacts(saved_run, G4_ROOT,
            saved_pointer['manifest_sha256'], {'summary.json', 'config.json'})
        if (saved.get('execution_authority') is not False or saved.get('mirror_parity_verified') is not True
                or saved_config.get('g3_contract') != frozen_donor.strategy_payload()
                or saved_config.get('source_strategy_fingerprint') != DONOR_FINGERPRINT):
            raise ValueError('Saved G4 result does not attest the frozen V11 baseline')
        directory = Path(saved['baseline_g3_run']).resolve()
        reference = dict(run_dir=str(directory), manifest_sha256=saved['baseline_g3_manifest_sha256'])
        summary, config = _checked_artifacts(directory, G3_ROOT, reference['manifest_sha256'], required)
    if (config != frozen_donor.strategy_payload()
            or summary.get('strategy_version') != frozen_donor.STRATEGY_VERSION
            or summary.get('strategy_fingerprint') != DONOR_FINGERPRINT
            or summary.get('active_setup_count') != len(frozen_donor.ACTIVE_SETUPS)):
        raise ValueError('G4 baseline differs from its frozen V11 contract')
    return directory, summary, reference


def run(config_path, output, *, through=None, candidate_pool_run=None):
    cfg = configuration(config_path)
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to(G4_ROOT.resolve()):
        raise ValueError('G4 experiment requires a fresh directory inside G4 result root')
    initial_code = code_proof(config_path)
    pointer_path = G3_ROOT/'latest/current_backtest.json'
    pointer_digest = sha256(pointer_path)
    pointer = read(pointer_path)
    baseline_dir, summary, pointer = _resolve_frozen_baseline(pointer)
    if through is not None and str(through) != summary['last_date']:
        raise ValueError('Requested cutoff must equal the frozen G4 donor historical cutoff')
    from fno_v13_v10_g_3_daily_replay import require_closed_session
    require_closed_session(date.fromisoformat(summary['last_date']))
    days = [date.fromisoformat(d) for d in summary['days']]
    original = read_frozen_csv(baseline_dir/'selected_orders.csv')
    saved = read_frozen_csv(baseline_dir/'portfolio_trades.csv')
    prices = Prices(summary)
    paths = prices.paths(original)
    control_identity = Identity(cfg, 'g3_mirror_control')
    control_orders = control_identity.identify(original)
    control = _simulate(control_orders, paths, days, control_identity)
    control['strategy'] = 'V13-V10-G-4'
    _same_native(control, saved)
    _same_allocation(control, saved)
    print('G4 mirror control exactly reproduces G3 native outcomes and portfolio allocation', flush=True)
    if candidate_pool_run is None:
        pool, candidate_proof = candidate_pool(summary, original, prices, cfg['scenarios'],
                                               cfg.get('five_minute_volume_ratio_overrides'))
    else:
        previous = Path(candidate_pool_run).resolve()
        if not previous.is_relative_to(G4_ROOT.resolve()):
            raise ValueError('Reused candidate pool must come from the isolated G4 result root')
        prior = verify(previous)
        if (prior['variant_id'] != PREVIOUS_VERSION or
                prior['baseline_g3_manifest_sha256'] != pointer['manifest_sha256'] or
                prior['first_date'] != summary['first_date'] or prior['last_date'] != summary['last_date']):
            raise ValueError('Reused candidate pool does not match the current G3 baseline')
        pool = read_frozen_csv(previous/'candidate_extension_audit.csv')
        requested = {sid for extra in cfg['scenarios'].values() for sid in extra}
        requested.update(sid for overrides in cfg.get('five_minute_volume_ratio_overrides', {}).values()
                         for sid in overrides)
        if not requested.issubset(set(pool.setup_id)):
            raise ValueError('Reused candidate pool lacks a requested setup')
        before = pool.loc[pool.v9_selected.eq(True)]
        expected = original.loc[original.setup_id.isin(requested)]
        if selected_keys(before) != selected_keys(expected):
            raise ValueError('Reused candidate pool no longer reproduces G3 selection')
        candidate_proof = dict(prior['candidate_source'], reused_verified_run=str(previous),
                               reused_manifest_sha256=sha256(previous/'manifest.json'))
    print(f'All requested setup candidate pools reproduce G3 selections: {len(pool)} candidates', flush=True)
    frames, rows, daily_frames, setup_rows, changes = {}, [], [], [], []
    for scenario, extra in cfg['scenarios'].items():
        identity = Identity(cfg, scenario)
        volume_overrides = identity.volume_overrides
        orders = original.copy()
        changed_ids = set(extra) | set(volume_overrides)
        if changed_ids:
            selected = reselect(pool.loc[pool.setup_id.isin(changed_ids)].copy(), extra, volume_overrides)
            orders = pd.concat([orders.loc[~orders.setup_id.isin(changed_ids)], selected], ignore_index=True, sort=False)
        if orders.sid.duplicated().any():
            raise ValueError('Experiment order SID collision')
        identified = identity.identify(orders)
        missing_paths = identified.loc[~identified.sid.isin(paths)]
        paths.update(prices.paths(missing_paths))
        ledger = control if not changed_ids else _simulate(identified, paths, days, identity)
        ledger['strategy'] = 'V13-V10-G-4'
        native_common = ledger.loc[ledger.sid.isin(saved.sid)]
        _same_native(native_common, saved.loc[saved.sid.isin(native_common.sid)])
        stats = _stats(ledger, days)
        rows.append(dict(scenario=scenario,**stats,net_delta_vs_g3=stats['net_pnl_rs']-summary['current_g3']['net_pnl_rs']))
        daily = _daily(ledger, days)
        daily['scenario'] = scenario
        base_daily = _daily(saved, days).set_index('day').net_pnl
        daily['g3_net_pnl'] = daily.day.map(base_daily)
        daily['delta_vs_g3'] = daily.net_pnl-daily.g3_net_pnl
        daily_frames.append(daily)
        for setup in frozen_donor.ACTIVE_SETUPS:
            setup_rows.append(dict(scenario=scenario,setup_id=setup.setup_id,signal_end=setup.signal_end,
                side=setup.side,**_stats(ledger.loc[ledger.setup_id.eq(setup.setup_id)],days)))
        old, new = selected_keys(original), selected_keys(orders)
        for action,keys in [('REMOVED',old-new),('ADDED',new-old)]:
            changes.extend(dict(scenario=scenario,action=action,day=d,setup_id=s,tradingsymbol=t) for d,s,t in sorted(keys))
        frames[scenario] = (identified, ledger, identity.payload)
        print(f"{scenario}: {stats['executed_trades']} trades; net Rs {stats['net_pnl_rs']:.2f}",flush=True)
    if code_proof(config_path) != initial_code or sha256(pointer_path) != pointer_digest:
        raise ValueError('Source code or G3 reference changed while running')
    for path, record in prices.proof.items():
        if sha256(Path(path)) != record['sha256']:
            raise ValueError('Market input changed while running')
    output.mkdir(parents=True)
    for scenario,(orders,ledger,payload) in frames.items():
        directory=output/scenario;directory.mkdir()
        orders.to_csv(directory/'selected_orders.csv',index=False)
        ledger.to_csv(directory/'portfolio_trades.csv',index=False)
        _daily(ledger,days).to_csv(directory/'daily_results.csv',index=False)
        (directory/'config.json').write_text(json.dumps(payload,indent=2)+'\n')
    results=pd.DataFrame(rows)
    results.to_csv(output/'scenario_comparison.csv',index=False)
    pd.concat(daily_frames,ignore_index=True).to_csv(output/'daily_comparison.csv',index=False)
    pd.DataFrame(setup_rows).to_csv(output/'setup_results.csv',index=False)
    pd.DataFrame(changes,columns=['scenario','action','day','setup_id','tradingsymbol']).to_csv(output/'selection_changes.csv',index=False)
    pool.to_csv(output/'candidate_extension_audit.csv',index=False)
    (output/'config.json').write_text(json.dumps(cfg,indent=2)+'\n')
    report=dict(variant_id=cfg['variant_id'],scope='BACKTEST_ONLY',execution_authority=False,
        first_date=summary['first_date'],last_date=summary['last_date'],sessions=len(days),
        baseline_g3_run=str(baseline_dir),baseline_g3_manifest_sha256=pointer['manifest_sha256'],
        g3_pointer_unchanged=True,mirror_parity_verified=True,pre_quota_reselection=True,
        active_setups=49,disabled_setups=10,primary_scenario=cfg['primary_scenario'],
        baseline_g3=summary['current_g3'],scenarios=rows,candidate_source=candidate_proof,
        scenario_guard_limits={name:guard_limits(extra) for name,extra in cfg['scenarios'].items()},
        scenario_five_minute_volume_ratio_overrides=cfg.get('five_minute_volume_ratio_overrides', {}),
        price_sources=prices.proof,coverage=summary['coverage'],
        limitations=['Historical inputs use immutable snapshots where available, otherwise current historical parquets.',
            'Confirmation OHLC and baseline execution outcomes reproduce G3; earlier mutable indicator history is not independently sealed.',
            'Historical setup/target choices and this nominated guard were inspected on these dates; not an untouched holdout.',
            'Maximum drawdown is measured on daily closes; current G3 sizing, costs and execution assumptions apply.'])
    (output/'summary.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
    write_report(output,report,results,pd.concat(daily_frames,ignore_index=True),pd.DataFrame(changes))
    manifest=dict(code_sha256=initial_code,files={p.relative_to(output).as_posix():sha256(p)
        for p in sorted(output.rglob('*')) if p.is_file()})
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    verify(output)
    latest=G4_ROOT/'latest';latest.mkdir(exist_ok=True)
    from fno_v13_v10_g_3_daily_replay import atomic_json
    atomic_json(latest/'g3_mirror.json',dict(variant_id=cfg['variant_id'],run_dir=output.relative_to(G4_ROOT).as_posix(),
        manifest_sha256=sha256(output/'manifest.json'),session_date=summary['last_date'],execution_authority=False))
    return report


def verify(output):
    output=Path(output);manifest=read(output/'manifest.json');summary=read(output/'summary.json')
    for name,digest in manifest['files'].items():
        if sha256(output/name)!=digest:
            raise ValueError('G4 output artifact changed: '+name)
    for row in summary['scenarios']:
        ledger=read_frozen_csv(output/row['scenario']/'portfolio_trades.csv')
        daily=read_frozen_csv(output/row['scenario']/'daily_results.csv')
        days=[date.fromisoformat(x) for x in daily.day]
        stats=_stats(ledger,days)
        for key,value in stats.items():
            if not np.isclose(value,row[key],atol=1e-6,rtol=0):
                raise ValueError('G4 summary accounting mismatch: '+key)
        pd.testing.assert_frame_equal(daily,_daily(ledger,days),check_dtype=False,check_exact=False,atol=1e-6,rtol=0)
    return summary


def write_report(output,summary,results,daily,changes):
    columns=['scenario','executed_trades','wins','losses','win_rate_pct','net_pnl_rs','net_delta_vs_g3','max_drawdown_rs']
    show=results[columns].copy()
    primary=daily.loc[daily.scenario.eq(summary['primary_scenario'])]
    primary_label = summary['primary_scenario'].replace('_', ' ')
    limits = summary['scenario_guard_limits'][summary['primary_scenario']]
    guards = pd.DataFrame([dict(setup=frozen_donor._ACTIVE_BY_ID[sid].signal_end,
        confirmation=frozen_donor._ACTIVE_BY_ID[sid].confirmation_end, side=frozen_donor._ACTIVE_BY_ID[sid].side,
        maximum_ATR=limit) for sid,limit in sorted(limits.items())])
    page=f'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G4 mirror vs G3</title>
<style>body{{font:16px/1.55 system-ui;background:#111827;color:#f3f4f6;padding:28px;max-width:1400px;margin:auto}}a{{color:#7dd3fc}}table{{border-collapse:collapse;width:100%;font-size:14px}}td,th{{padding:9px;border-bottom:1px solid #475569;text-align:left}}.scroll{{overflow-x:auto}}p{{max-width:1100px}}</style></head><body>
<h1>G4 matched to G3: full portfolio guard comparison</h1>
<p>{summary['first_date']} through {summary['last_date']} · {summary['sessions']} sessions · 49 enabled setups, 10 disabled · Backtesting only.</p>
<p>The control copies G3's entry thresholds, targets, stops, volume filters, existing 10:35 guards, costs and capital model. It reproduces G3's native trades and portfolio P&amp;L exactly. Each trial applies its configured ATR ceiling before ranking, allows replacement candidates and replays the entire portfolio.</p>
<div class="scroll">{show.to_html(index=False,float_format=lambda x:f'{x:,.2f}',border=0)}</div>
<p>Primary test: <strong>{html.escape(primary_label)}</strong>. Individual and combined rows are research scenarios. Guard clocks are signal times; confirmation is one minute later.</p>
<div class="scroll">{guards.to_html(index=False,border=0)}</div>
<p>Rupees use G3 backtest sizing: ₹100,000 capital per trade, 5× exposure, ₹1,000,000 portfolio capital and 5bps round-trip cost. Maximum drawdown means daily-close equity drawdown.</p>
<h2>{html.escape(primary_label)}: daywise comparison</h2><div class="scroll">{primary[['day','trades','wins','losses','net_pnl','g3_net_pnl','delta_vs_g3']].to_html(index=False,float_format=lambda x:f'{x:,.2f}',border=0)}</div>
<h2>Selection changes</h2><div class="scroll">{changes.to_html(index=False,border=0)}</div>
<h2>Limits</h2><ul>{''.join('<li>'+html.escape(x)+'</li>' for x in summary['limitations'])}</ul>
<p>G3 settings and result pointer were unchanged. Older G4 research profiles and results are retained.</p>
<p><a href="scenario_comparison.csv">Scenario totals</a> · <a href="daily_comparison.csv">All daywise results</a> · <a href="setup_results.csv">All setup results</a> · <a href="selection_changes.csv">Selected stock changes</a> · <a href="candidate_extension_audit.csv">Candidate inputs</a> · <a href="summary.json">Source evidence</a></p></body></html>'''
    (output/'report.html').write_text(page,encoding='utf-8')
