"""V13-v10-G session-effective backtest; frozen research is explicitly opt-in.

The default command replays today's IST session from recorded raw data. From
2026-10-06 this includes relaxed 09:25 LONG selection and the 1.25% -> 1.00%
stop after 120 minutes. Earlier dates retain their original G rules. Imported
research helpers remain frozen-compatible; use --frozen-research to replay the
sealed historical research bundle.
"""
from __future__ import annotations

import argparse
import copy
import json
from dataclasses import asdict, dataclass, replace
from datetime import date, time
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_f_backtest as f
import fno_v13_v10_g_policy as policy

v9, r = f.v9, f.r
DEFAULT_SOURCE = f.DEFAULT_SOURCE
THRESHOLD_OUTPUT = f.DEFAULT_OUTPUT.parent.parent / 'v13_corrected_v10_g/run_20260914_careful_thresholds'
EXPANSION_OUTPUT = THRESHOLD_OUTPUT.parent / 'run_20260914_opportunity_expansion'
MORNING_OUTPUT = EXPANSION_OUTPUT
DEFAULT_OUTPUT = EXPANSION_OUTPUT
DEFAULT_PRODUCTION_OUTPUT = v9.v5.common.FNO_ROOT / 'strategy_research/v13_corrected_v10_g/production_replays'
F_CORRECTED = f.DEFAULT_OUTPUT.parent / 'run_20260914_portfolio10l/CAPACITY_CORRECTED_5X'
EVIDENCE = 'EXPLORATORY_REUSED_HISTORY_NO_UNTOUCHED_TEST'
MORNING_DONORS = {
    '0951_LONG': ('09:50', '09:51', '0956_LONG'),
    '0956_SHORT': ('09:55', '09:56', '0951_SHORT'),
    '1001_SHORT': ('10:00', '10:01', '0951_SHORT'),
}
# These two known metadata changes do not regenerate frozen signal/path artifacts.
# Pin observed replacements too: future drift still fails closed.
KNOWN_METADATA_DRIFT = {
    str(Path('C:/TradingData/eqidv2/fno_oi/universe/contract_registry.parquet').resolve()).lower():
        '0b442e1fdf0a4c3bcff4857280fc72a16e156e75e4d4b44e97bff1b4a5094e00',
    str(Path(__file__).with_name('fno_oi_common.py').resolve()).lower():
        '13b6d4e7880e8858a96a4772ae2e681ec5a942d5102e66b07ba4c4bbea9dc4d2',
}


@dataclass(frozen=True)
class SelectionChange:
    price_multiplier: float = 1.
    oi_multiplier: float = 1.
    body_reduction: float = 0.
    wick_increase: float = 0.
    extra_setup_entries: int = 0
    expansion_side: str = 'BOTH'

    def validate(self):
        for name, minimum in [('price_multiplier', .65), ('oi_multiplier', .5)]:
            value = getattr(self, name)
            if not np.isfinite(value) or not minimum <= value <= 1:
                raise ValueError(f'Invalid {name}')
        for name in ('body_reduction', 'wick_increase'):
            if not np.isfinite(getattr(self, name)) or not 0 <= getattr(self, name) <= .1:
                raise ValueError(f'Invalid {name}')
        if self.extra_setup_entries not in (0, 1) or self.expansion_side not in ('LONG', 'SHORT', 'BOTH'):
            raise ValueError('Invalid quota or side')

    def complexity(self):
        return sum([self.price_multiplier != 1, self.oi_multiplier != 1,
                    self.body_reduction != 0, self.wick_increase != 0,
                    self.extra_setup_entries != 0])


def setup_pair(original, change):
    """All multipliers are relative to F, including its halved SHORT OI gate."""
    core = replace(original, oi_change_pct=max(.05, original.oi_change_pct *
                                               (.5 if original.side == 'SHORT' else 1.)))
    applied = change if change.expansion_side in ('BOTH', original.side) else SelectionChange()
    expanded = replace(core,
        price_change_pct=max(.10, core.price_change_pct * applied.price_multiplier),
        oi_change_pct=max(.05, core.oi_change_pct * applied.oi_multiplier),
        body_ratio=max(0., core.body_ratio - applied.body_reduction),
        max_wick_ratio=min(1., core.max_wick_ratio + applied.wick_increase),
        max_entries=core.max_entries + applied.extra_setup_entries)
    return core, expanded


def morning_setups():
    originals = {s.setup_id: s for s in v9.v5.profile_setups(v9.v5.PROFILES['higher_frequency'])}
    result = []
    for new_id, (signal_time, confirmation_time, donor_id) in MORNING_DONORS.items():
        donor, _ = setup_pair(originals[donor_id], SelectionChange())
        setup = replace(donor, signal_end=signal_time, confirmation_end=confirmation_time, max_entries=1)
        assert setup.setup_id == new_id
        result.append(setup)
    return result


def selection_audit(signals, base, change=SelectionChange(), *, core_first=True, morning_slots=False,
                    two_bar_continuation=False):
    change.validate()
    if not isinstance(morning_slots, bool):
        raise ValueError('morning_slots must be boolean')
    if not isinstance(two_bar_continuation, bool):
        raise ValueError('two_bar_continuation must be boolean')
    base.validate()
    if base.ranking != 'native':
        raise ValueError('Native ranking required')
    if signals.sid.duplicated().any():
        raise ValueError('Duplicate signal IDs')
    signals = signals.reset_index(drop=True)
    volume = pd.to_numeric(signals.v9_1m_volume_ratio, errors='coerce')
    available = signals.loc[np.isfinite(volume) & volume.ge(1.20)].copy()
    stamp = pd.to_datetime(available.v9_1m_feature_ts, utc=True, errors='coerce')
    cutoff = pd.to_datetime(available.confirmation_ts, utc=True, errors='coerce')
    if (stamp.isna() | cutoff.isna() | stamp.gt(cutoff)).any():
        raise ValueError('Noncausal or missing confirmation feature timestamp')
    parts = []
    setups = [(s, False) for s in v9.v5.profile_setups(v9.v5.PROFILES['higher_frequency'])]
    if morning_slots:
        setups.extend((s, True) for s in morning_setups())
    for original, is_morning in setups:
        # New slots inherit F gates exactly; G's short price relaxation applies
        # only to its existing setup book. No extra quota in the new slots.
        core, setup = (original, original) if is_morning else setup_pair(original, change)
        native_rows = v9.v5.replay._eligible(available, setup)
        if two_bar_continuation:
            import fno_v13_v10_g_two_bar as two_bar
            eligible_rows = two_bar.eligible(available, setup)
        else:
            eligible_rows = native_rows
        rows = v9._filter_and_score(eligible_rows, base)
        if rows.empty:
            continue
        if is_morning or two_bar_continuation:
            signal_clock = pd.to_datetime(rows.signal_ts, utc=True, errors='coerce')
            confirmation_clock = pd.to_datetime(rows.confirmation_ts, utc=True, errors='coerce')
            feature_clock = pd.to_datetime(rows.v9_1m_feature_ts, utc=True, errors='coerce')
            if (signal_clock.isna() | confirmation_clock.isna() |
                feature_clock.isna() | ~feature_clock.eq(confirmation_clock) |
                ~confirmation_clock.sub(signal_clock).eq(pd.Timedelta(minutes=1)) |
                ~signal_clock.dt.tz_convert(v9.v5.common.IST).dt.strftime('%H:%M').eq(setup.signal_end) |
                ~confirmation_clock.dt.tz_convert(v9.v5.common.IST).dt.strftime('%H:%M').eq(setup.confirmation_end)).any():
                raise ValueError('Morning and two-bar slots require exact signal and next-minute confirmation timestamps')
        passed = rows.loc[rows.v9_filter_pass].copy()
        picker = v9.PICKER_COLUMNS[setup.picker]
        if picker == 'abs_price_change_pct':
            rows[picker] = rows.price_change_pct.abs()
            passed[picker] = passed.price_change_pct.abs()
        ranked = passed.sort_values(['day', picker, 'traded_value', 'tradingsymbol'],
                                    ascending=[True, False, False, True], kind='stable')
        primary = passed.iloc[:0] if is_morning else v9.v5.replay.select_setup_rows(passed, core)
        selected_indices = []
        previous_indices = []
        for _, group in ranked.groupby('day', sort=False):
            kept = group.loc[group.index.isin(primary.index)] if core_first else group.iloc[:0]
            if two_bar_continuation:
                # Reserve the entire previous G selection, not just F's core.
                # Single-bar candidates fill the existing quota before the
                # alternate two-bar condition may fill remaining vacancies.
                single = group.loc[group.index.isin(native_rows.index)]
                previous = single.loc[~single.index.isin(kept.index)].head(setup.max_entries - len(kept))
                kept = pd.concat([kept, previous])
                previous_indices.extend(kept.index.tolist())
            additions = group.loc[~group.index.isin(kept.index)].head(setup.max_entries - len(kept))
            selected_indices.extend(kept.index.tolist() + additions.index.tolist())
        rows['v10_g_f_core'] = rows.index.isin(primary.index)
        rows['v10_g_morning_slot'] = is_morning
        if two_bar_continuation:
            rows['v10_g_previous_selection'] = rows.index.isin(previous_indices)
        rows['v9_selected'] = rows.index.isin(selected_indices)
        if two_bar_continuation:
            rows['v10_g_two_bar_selection'] = rows.v9_selected & ~rows.v10_g_previous_selection
        rows['v9_rank_in_setup_day'] = ranked.groupby('day', sort=False).cumcount() + 1
        rows['v9_decision'] = np.where(rows.v9_selected, 'SELECTED', 'RANKED_OUT')
        rows['v9_configuration'] = 'V13_V10_G_NEW'
        rows = v9._setup_metadata(rows, setup)
        for field in ('price_change_pct', 'oi_change_pct', 'body_ratio', 'max_wick_ratio', 'max_entries'):
            rows[f'v10_g_required_{field}'] = getattr(setup, field)
            rows[f'v10_g_f_required_{field}'] = getattr(core, field)
        parts.append(rows)
    if not parts:
        return f.selection_audit(available.iloc[:0], base)
    return pd.concat(parts, ignore_index=True).sort_values(
        ['day', 'hhmm_int', 'side', 'setup_id', 'tradingsymbol'], kind='stable').reset_index(drop=True)


def select_orders(signals, base, change=SelectionChange(), *, core_first=True, morning_slots=False,
                  two_bar_continuation=False):
    audit = selection_audit(signals, base, change, core_first=core_first, morning_slots=morning_slots,
                           two_bar_continuation=two_bar_continuation)
    return audit.loc[audit.v9_selected].reset_index(drop=True)


def frozen_f_settings():
    manifest = json.loads((f.DEFAULT_OUTPUT / 'research_manifest.json').read_text())
    artifacts = {k.replace('\\', '/'): v for k, v in manifest['artifacts'].items()}
    path = f.DEFAULT_OUTPUT / 'frozen_config.json'
    if f.v10.sha(path) != artifacts['frozen_config.json']:
        raise ValueError('Frozen F configuration drift')
    return json.loads(path.read_text(encoding='utf-8'))


def config(source_exit, change, *, core_first=True, morning_slots=False, two_bar_continuation=False):
    change.validate()
    if not isinstance(core_first, bool):
        raise ValueError('core_first must be boolean')
    if not isinstance(morning_slots, bool):
        raise ValueError('morning_slots must be boolean')
    if not isinstance(two_bar_continuation, bool):
        raise ValueError('two_bar_continuation must be boolean')
    for pair in [source_exit['default'], *source_exit['setups'].values()]:
        f.b.validate_pair(pair)
    result = dict(version='V13-v10-G-NEW', source_version='V13-v10-F',
        selection_change=asdict(change), core_first=core_first, exit=copy.deepcopy(source_exit),
        minimum_confirmation_1m_volume_ratio=1.20, entry_expiry_minutes=10,
        portfolio_capital_rupees=1_000_000., capital_per_entry_rupees=100_000.,
        leverage_factor=5., max_positions=None, cost_bps=5., partial_exits=False, breakeven_stop=False,
        source_oi_floor_pct=.05, evidence=EVIDENCE)
    if morning_slots:
        result['morning_slots'] = True
        result['morning_slot_donors'] = {key: value[2] for key, value in MORNING_DONORS.items()}
        for key, (_, _, donor_id) in MORNING_DONORS.items():
            if key in source_exit['setups']:
                raise ValueError('Morning exit already defined in source; pass the frozen F exit table')
            result['exit']['setups'][key] = copy.deepcopy(source_exit['setups'][donor_id])
    if two_bar_continuation:
        result['two_bar_continuation'] = True
        result['two_bar_rule'] = 'Exact same-session t,t-5,t-10 closes; net directional move >= setup price minimum; latest close-to-close and candle body both directional; preserve all original G choices before filling spare quota.'
    return result


def checked_settings(settings):
    result = config(frozen_f_settings()['exit'], SelectionChange(**settings['selection_change']),
                    core_first=settings.get('core_first', True), morning_slots=settings.get('morning_slots', False),
                    two_bar_continuation=settings.get('two_bar_continuation', False))
    if settings['exit'] != result['exit']:
        raise ValueError('G must retain frozen F exits and fixed morning donor exits exactly')
    for key, value in result.items():
        if settings.get(key) != value:
            raise ValueError(f'Conflicting fixed G setting: {key}')
    return result


def load_source(source=DEFAULT_SOURCE, *, two_bar_continuation=False):
    folder = source / 'dataset'
    manifest = json.loads((folder / 'dataset_manifest.json').read_text(encoding='utf-8'))
    drift = []
    for record in manifest['sources']:
        path = Path(record['path'])
        exists = path.is_file()
        observed = f.v10.sha(path) if exists else None
        if exists != record['exists'] or (exists and observed != record['sha256']):
            expected = KNOWN_METADATA_DRIFT.get(str(path.resolve()).lower())
            if expected is None or observed != expected:
                raise RuntimeError(f'Unexplained frozen source drift: {path}')
            drift.append(dict(path=str(path), frozen_sha256=record['sha256'], current_sha256=observed,
                reason='Known metadata refresh; frozen signal/path artifacts are used without regeneration'))
    for name, checksum in manifest['output_sha256'].items():
        if f.v10.sha(folder / name) != checksum:
            raise RuntimeError(f'Frozen V9 artifact drift: {name}')
    source_proof = json.loads((source / 'research_manifest.json').read_text())
    source_artifacts = {k.replace('\\', '/'): v for k, v in source_proof['artifacts'].items()}
    config_path = source / 'frozen_config.json'
    if f.v10.sha(config_path) != source_artifacts['frozen_config.json']:
        raise RuntimeError('Frozen V9 configuration drift')
    base = v9.V9Config(**json.loads(config_path.read_text()))
    if base.cost_bps != 5.:
        raise ValueError('Frozen F/G cost model must be 5bps')
    base = replace(base, portfolio_capital_rupees=1_000_000., capital_per_entry_rupees=100_000.,
                   leverage_factor=5., max_positions=None)
    base.validate()
    v9.validate_configuration()
    result = dict(v9_config=base, days=manifest['days'], manifest=manifest, source=source,
        source_verification=dict(all_frozen_artifacts_verified=True, known_metadata_drift=drift,
                                 dataset_manifest_sha256=f.v10.sha(folder / 'dataset_manifest.json'),
                                 v9_config_sha256=f.v10.sha(config_path)),
        signals=pd.read_parquet(folder / 'signals.parquet'))
    if two_bar_continuation:
        import fno_v13_v10_g_two_bar as two_bar
        result = two_bar.augment_source(result)
    return result


def evaluate(dataset, settings):
    checked_settings(settings)
    base = dataset['v9_config']
    for key in ('portfolio_capital_rupees', 'capital_per_entry_rupees', 'leverage_factor', 'max_positions', 'cost_bps'):
        if getattr(base, key) != settings[key]:
            raise ValueError(f'Portfolio mismatch: {key}')
    expected = select_orders(dataset['signals'], base, SelectionChange(**settings['selection_change']),
                             core_first=settings['core_first'], morning_slots=settings.get('morning_slots', False),
                             two_bar_continuation=settings.get('two_bar_continuation', False))
    keys = lambda frame: set(zip(frame.sid.astype(int), frame.setup_id.astype(str)))
    if len(dataset['orders']) != len(expected) or keys(dataset['orders']) != keys(expected):
        raise ValueError('Orders do not match causal G selection')
    result = f.b.evaluate({**dataset, 'orders': expected}, settings['exit'])
    for frame in result[:2]:
        frame.rename(columns={f'v10_b_{key}': f'v10_g_{key}' for key in ('stop_pct', 'target_pct', 'reward_risk')}, inplace=True)
    result[2].update(version='V13-v10-G-NEW', settings=settings, evidence=EVIDENCE)
    return result


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--session-date', '--date', type=date.fromisoformat,
                      help='One recorded IST session; defaults to today, using that date\'s production rules')
    mode.add_argument('--frozen-research', action='store_true',
                      help='Explicitly reproduce the original frozen G research bundle and fixed exits')
    parser.add_argument('--source-dir', type=Path, help='Frozen research source; requires --frozen-research')
    parser.add_argument('--config-json', type=Path, help='Frozen research configuration; requires --frozen-research')
    parser.add_argument('--output-dir', type=Path,
                        help='Production: new/empty directory, default a unique dated production_replays run; frozen: default cli_replay')
    return parser


def _run_frozen_research(source_dir, config_path, output_dir):
    settings = json.loads(config_path.read_text(encoding='utf-8'))
    checked_settings(settings)
    source = load_source(source_dir, two_bar_continuation=settings.get('two_bar_continuation', False))
    orders = select_orders(source['signals'], source['v9_config'], SelectionChange(**settings['selection_change']),
                           core_first=settings['core_first'], morning_slots=settings.get('morning_slots', False),
                           two_bar_continuation=settings.get('two_bar_continuation', False))
    if settings.get('two_bar_continuation', False):
        import fno_v13_v10_g_two_bar as two_bar
        dataset = {**source, 'orders': orders, 'paths': two_bar.load_paths(source, orders)}
    else:
        with np.load(source_dir / 'dataset/paths.npz', allow_pickle=False) as archive:
            dataset = f.orders_dataset(source, source['signals'], archive, orders)
    result = evaluate(dataset, settings)
    r.save(output_dir, *result)
    print(json.dumps(r.metric(result[1], source['days']), indent=2))
    return 0


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.frozen_research:
        return _run_frozen_research(
            args.source_dir or DEFAULT_SOURCE,
            args.config_json or DEFAULT_OUTPUT / 'frozen_config.json',
            args.output_dir or DEFAULT_OUTPUT / 'cli_replay',
        )
    if args.source_dir is not None or args.config_json is not None:
        parser.error('--source-dir and --config-json require --frozen-research')

    common = v9.v5.common
    now = common.now_ist()
    day = args.session_date or now.date()
    status = None
    exit_code = 2
    if day > now.date():
        status = 'BLOCKED_FUTURE_DATE'
    elif not common.is_trading_day(day, common.load_holidays()):
        status, exit_code = 'SKIPPED_NON_TRADING_DAY', 0
    elif day == now.date() and now.time().replace(tzinfo=None) < time(15, 30):
        status = 'WAITING_FOR_SESSION_CLOSE'
    if status:
        print(json.dumps(dict(strategy='V13-V10-G', session_date=day.isoformat(),
                              state=status, complete=False,
                              strategy_policy=policy.policy_for_day(day)), indent=2))
        return exit_code

    output = args.output_dir or DEFAULT_PRODUCTION_OUTPUT / day.isoformat() / now.strftime('%Y%m%dT%H%M%S%f')
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        parser.error('Production --output-dir must be new or empty; use a new directory for each replay')
    from fno_v13_v10_g_daily_replay import replay_day
    result = replay_day(day, output)
    print(json.dumps(result, indent=2))
    return 0 if result['complete'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
