"""G7 isolated G6 replica with versioned targets, entry vetoes and direction gate."""
from __future__ import annotations

from dataclasses import asdict, replace
from datetime import date
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g4_bounded_exit_backtest as bounded
from research import g4_g3_mirror_backtest as mirror
from research import g3_target_cutoff_contract as donor
from research import g4_1216_comparison as approved1216
from research import g7_nifty_direction as nifty_filter
from research import g7_primary_index_direction as primary_filter
from research import g6_g4_replica_backtest as g6_source
from research.g4_extended_target_sweep import read_frozen_csv

V1_VERSION = 'V13-V10-G-7-G6-REPLICA-V1'
V2_VERSION = 'V13-V10-G-7-G6-REPLICA-V2'
VERSION = 'V13-V10-G-7-G6-REPLICA-V3'
G7_ROOT = bounded.G4_ROOT.parent/'backtesting_result_v13_v10_g_7'
V1_CONFIG = bounded.ROOT/'configs/strategies/v13_v10_g_7_g6_replica_v1.json'
V2_CONFIG = bounded.ROOT/'configs/strategies/v13_v10_g_7_g6_replica_v2.json'
DEFAULT_CONFIG = bounded.ROOT/'configs/strategies/v13_v10_g_7_g6_replica_v3.json'
ADDED_IDS = frozenset({'0921_SHORT', '1011_LONG'})
APPROVED_0920_IDS = ('0921_LONG', '0921_SHORT')
PRIMARY_POLICY = 'g7_primary_index_both_counter'
NIFTY_POLICY = 'g7_nifty_futures_both_counter'
NIFTY50_SPOT_POLICY = 'g7_nifty50_spot_both_counter'
SELECTED_POLICY = 'g7_bounded_exit'
DONOR_FINGERPRINT = '871ed037495f5a68c916826200e13ce491fe60621f0b6a7f673e18ca601816cb'
APPROVED_RESEARCH = {
    '0921_LONG': {
        'artifact': '20261016T174500_g4_0920_long_parameters_v1',
        'manifest_sha256': 'a7d0c06f4381dae18ae0b8cf9852cc19a7b55a46f426db5912e3a54b1608f826',
        'decision': 'Retain the current G4 09:20 LONG settings, including the 2.00% target.',
    },
    '0921_SHORT': {
        'artifact': '20261016T190000_g4_0920_short_indicator_sweep_v1',
        'manifest_sha256': 'bf22828e2f01f0ab0b605d2938b32b3c98a65734984ef6857269f5c8eead8310',
        'decision': 'Retain the current G4 09:20 SHORT indicator and exit settings.',
    },
}


SLOT_ENTRY_FILTERS = {
    '1011_LONG': dict(field='oi_change_pct', source_minimum=0.05, minimum=0.125),
    '1206_LONG': dict(field='oi_change_pct', source_minimum=0.05, minimum=0.10),
    '1226_SHORT': dict(field='traded_value', source_minimum=0.0, minimum=10_000_000.0),
}

TWO_STAGE_EXIT_MODE = 'HALF_AT_HALF_TARGET_RUNNER_TO_FULL_TARGET_STOP_AT_FIRST_TARGET'
TWO_STAGE_EXIT_POLICY = dict(
    mode=TWO_STAGE_EXIT_MODE,
    first_exit_fraction=0.5,
    first_target_fraction_of_setup_target=0.5,
    runner_fraction=0.5,
    runner_target_fraction_of_setup_target=1.0,
    runner_stop='FIRST_TARGET',
    runner_stop_activation='NEXT_ONE_MINUTE_BAR_OPEN',
    ambiguous_bar_policy='STOP_FIRST',
)


def _v1_configuration_payload():
    """Rebuild the original uniform-target G7 V1 rollback configuration."""
    source = g6_source.configuration()
    value = json.loads(json.dumps(source))
    value.update(
        variant_id=V1_VERSION,
        strategy='V13-V10-G-7',
        replica_source_variant=g6_source.VERSION,
        active_setups=[
            {**setup, 'source_version': V1_VERSION, 'target_pct': 0.75}
            for setup in source['active_setups']
        ],
        target_floor_pct=0.75,
        target_cap_pct=0.75,
        target_rule='Set every active LONG and SHORT setup target to 0.75 percent',
        approved_0920_decisions={
            **source['approved_0920_decisions'],
            'configuration_changed': True,
            'decision': ('G7 experiment overrides every G6 setup target, including '
                         '09:21 LONG and SHORT, to 0.75%; entry and stop rules remain unchanged.'),
        },
        direction_filter={
            **source['direction_filter'],
            'selected_policy': NIFTY_POLICY,
            'alternatives': {
                primary_filter.PRIMARY_FILTER: PRIMARY_POLICY,
                primary_filter.NIFTY_FILTER: NIFTY_POLICY,
                primary_filter.OFF: 'g7_unfiltered',
            },
            'execution_authority': False,
        },
        execution_authority=False,
        scope='BACKTEST_ONLY',
    )
    return value


def _v2_configuration_spec():
    """Small checked-in V2 overlay; retained as the single-exit rollback."""
    return dict(
        variant_id=V2_VERSION,
        base_variant=V1_VERSION,
        replica_source_variant=g6_source.VERSION,
        target_policy='RESTORE_G6_PER_SETUP_TARGETS',
        slot_entry_filters=SLOT_ENTRY_FILTERS,
        filter_application='AFTER_SLOT_RANKING_BEFORE_EXECUTION_NO_REPLACEMENT',
        direction_filter_mode=primary_filter.NIFTY_FILTER,
        scope='BACKTEST_ONLY',
        execution_authority=False,
    )


def _v2_configuration_payload():
    """Restore G6 targets and add the three approved slot entry vetoes."""
    source = g6_source.configuration()
    value = json.loads(json.dumps(source))
    setups = []
    for original in source['active_setups']:
        setup = {**original, 'source_version': V2_VERSION}
        rule = SLOT_ENTRY_FILTERS.get(setup['setup_id'])
        if rule:
            parameter = 'min_traded_value' if rule['field'] == 'traded_value' else rule['field']
            setup[parameter] = rule['minimum']
        setups.append(setup)
    value.update(
        variant_id=V2_VERSION,
        strategy='V13-V10-G-7',
        replica_source_variant=g6_source.VERSION,
        active_setups=setups,
        target_floor_pct=source['target_floor_pct'],
        target_cap_pct=source['target_cap_pct'],
        target_rule='Restore every active setup target from the sealed G6 V2 configuration',
        target_policy=dict(mode='RESTORE_G6_SETUP_TARGETS', source_variant=g6_source.VERSION),
        slot_entry_filters=SLOT_ENTRY_FILTERS,
        filter_application='AFTER_SLOT_RANKING_BEFORE_EXECUTION_NO_REPLACEMENT',
        selection_rule=(source['selection_rule'] + ' Then veto a ranked order when its configured '
                        '10:11 LONG or 12:06 LONG OI minimum, or 12:26 SHORT traded-value minimum, fails.'),
        approved_0920_decisions={
            **source['approved_0920_decisions'],
            'configuration_changed': False,
            'decision': 'Retain the sealed G6 09:21 LONG and SHORT parameters and targets.',
        },
        direction_filter={
            **source['direction_filter'],
            'selected_policy': NIFTY_POLICY,
            'alternatives': {
                primary_filter.PRIMARY_FILTER: PRIMARY_POLICY,
                primary_filter.NIFTY_FILTER: NIFTY_POLICY,
                primary_filter.OFF: 'g7_unfiltered',
            },
            'execution_authority': False,
        },
        execution_authority=False,
        scope='BACKTEST_ONLY',
    )
    return value


def configuration_spec():
    """Small checked-in V3 overlay; full settings derive from the sealed V2 policy."""
    return dict(
        variant_id=VERSION,
        base_variant=V2_VERSION,
        replica_source_variant=g6_source.VERSION,
        target_policy='RESTORE_G6_PER_SETUP_TARGETS',
        slot_entry_filters=SLOT_ENTRY_FILTERS,
        filter_application='AFTER_SLOT_RANKING_BEFORE_EXECUTION_NO_REPLACEMENT',
        exit_policy=TWO_STAGE_EXIT_POLICY,
        direction_filter_mode=primary_filter.NIFTY_FILTER,
        scope='BACKTEST_ONLY',
        execution_authority=False,
    )


def configuration_payload():
    """V3 retains V2 entries and targets, then applies the requested two-stage exit."""
    value = _v2_configuration_payload()
    value.update(
        variant_id=VERSION,
        active_setups=[{**setup, 'source_version': VERSION}
                       for setup in value['active_setups']],
        target_rule=('Retain every active setup target from sealed G6; exit 50 percent at '
                     'half that target and retain the other 50 percent for the full target'),
        exit_policy=TWO_STAGE_EXIT_POLICY,
        exit_rule=('Keep the existing 1.20 percent stop and 120-minute tightening to 1.00 percent. '
                   'Before the first target, a stop exits 100 percent. At half the setup target, '
                   'exit 50 percent; the remaining 50 percent keeps the full setup target and its '
                   'stop becomes the first-target price from the next one-minute bar open.'),
        approved_0920_decisions={
            **value['approved_0920_decisions'],
            'configuration_changed': False,
            'exit_policy_changed': True,
            'decision': ('Retain the sealed G6 09:21 entry parameters and full targets; '
                         'apply the requested G7 V3 two-stage exit.'),
        },
    )
    return value


def configuration(path=DEFAULT_CONFIG):
    value = mirror.read(path)
    if value.get('variant_id') == V1_VERSION:
        if value != _v1_configuration_payload():
            raise ValueError('G7 V1 rollback configuration no longer matches its frozen payload')
        return value
    if value.get('variant_id') == V2_VERSION:
        if value != _v2_configuration_spec():
            raise ValueError('G7 V2 rollback overlay no longer matches its frozen payload')
        return _v2_configuration_payload()
    if value != configuration_spec():
        raise ValueError('G7 V3 overlay no longer matches the approved two-stage exit configuration')
    return configuration_payload()


def reselect_1011_short(retained, source_summary):
    """Apply G7's one entry-rule delta to the complete pre-quota pool."""
    from research.g4_1011_short_threshold_compare import _candidate_pool, _keys, _select_changed

    audit, provenance = _candidate_pool(source_summary)
    if not audit.setup_id.eq('1011_SHORT').all():
        raise ValueError('G7 10:11 audit contains another setup')
    revised, audit, changes = _select_changed(audit, retained)
    old_keys = _keys(retained.loc[retained.setup_id.eq('1011_SHORT')])
    new_keys = _keys(revised.loc[revised.setup_id.eq('1011_SHORT')])
    if revised.sid.duplicated().any() or revised.duplicated(
            ['day', 'setup_id', 'tradingsymbol']).any() or _keys(
            audit.loc[audit.selected_011.eq(True)]) != new_keys:
        raise ValueError('G7 10:11 revised selection differs from candidate audit')
    if set(changes.action) - {'ADDED', 'REMOVED'} or len(changes) != len(new_keys ^ old_keys):
        raise ValueError('G7 10:11 change audit differs from reselection')
    changes['action'] = changes.action + '_BY_1011_MOVE011'
    return revised, audit, changes, provenance


class Identity(mirror.Identity):
    def __init__(self, config):
        version = config['variant_id']
        self.STRATEGY_VERSION = version
        self.ACTIVE_SETUPS = tuple(mirror.production.SetupSpec(**{k:v for k,v in s.items() if k!='setup_id'})
                                   for s in config['active_setups'])
        self._ACTIVE_BY_ID = {s.setup_id:s for s in self.ACTIVE_SETUPS}
        active_targets = {s.setup_id:s.target_pct for s in self.ACTIVE_SETUPS}
        self.payload = donor.strategy_payload()
        self.payload.update(strategy_version=version, strategy_profile='V13_V10_G_7',
            accepted_variant=version, execution_authority=False, scope='BACKTEST_ONLY',
            source_strategy_version=donor.STRATEGY_VERSION, source_strategy_fingerprint=DONOR_FINGERPRINT,
            active_setups=[asdict(s) for s in self.ACTIVE_SETUPS],
            core_setups=[asdict(replace(s,source_version=version,stop_pct=1.2,
                target_pct=active_targets.get(s.setup_id,s.target_pct))) for s in donor.CORE_SETUPS],
            maximum_confirmation_extension_atr_by_setup=config['maximum_confirmation_extension_atr_by_setup'],
            g4_five_minute_volume_ratio_overrides=config['five_minute_volume_ratio_overrides'])
        self.payload['exit'].update(initial_stop_pct=1.2,
            setups={s.setup_id:dict(stop_pct=s.stop_pct,target_pct=s.target_pct) for s in self.ACTIVE_SETUPS})

    def identify(self, frame):
        result = super().identify(frame)
        result['native_stop_pct'] = 1.2
        result['native_target_pct'] = result.setup_id.map({s.setup_id:s.target_pct for s in self.ACTIVE_SETUPS})
        return result


def apply_slot_entry_filters(frame, config):
    """Apply versioned ranked-order vetoes without selecting replacements."""
    audit = frame[['sid','day','setup_id','tradingsymbol','side','confirmation_ts']].copy()
    audit['slot_entry_filter_applies'] = False
    audit['slot_entry_filter_field'] = ''
    audit['slot_entry_filter_value'] = np.nan
    audit['slot_entry_filter_minimum'] = np.nan
    audit['slot_entry_filter_allowed'] = True
    audit['slot_entry_filter_reason'] = 'NOT_APPLICABLE'
    for setup_id, rule in config.get('slot_entry_filters', {}).items():
        mask = frame.setup_id.eq(setup_id)
        values = pd.to_numeric(frame.loc[mask,rule['field']], errors='coerce')
        allowed = values.ge(float(rule['minimum']))
        audit.loc[mask,'slot_entry_filter_applies'] = True
        audit.loc[mask,'slot_entry_filter_field'] = rule['field']
        audit.loc[mask,'slot_entry_filter_value'] = values.to_numpy()
        audit.loc[mask,'slot_entry_filter_minimum'] = float(rule['minimum'])
        audit.loc[mask,'slot_entry_filter_allowed'] = allowed.to_numpy()
        audit.loc[mask,'slot_entry_filter_reason'] = np.where(
            allowed.to_numpy(),'PASS','BELOW_CONFIGURED_MINIMUM')
    rejected = audit.loc[~audit.slot_entry_filter_allowed.astype(bool)].copy()
    keep = audit.slot_entry_filter_allowed.astype(bool).to_numpy()
    return frame.loc[keep].copy().reset_index(drop=True), audit, rejected


def _simulate_two_stage_exit(path, entry_index, entry_price, is_long, stop_pct,
                             setup_target_pct, *, tighten_after_minutes,
                             tightened_stop_pct):
    """Exit half at half-target, then run half with a profit-lock at target one.

    The existing stop, scheduled tightening, stop gaps and stop-first ordering
    govern the full position until target one. After target one, the profit-lock
    becomes executable from the next one-minute bar open. A full-target touch on
    the target-one bar is credited because price must cross target one first.
    """
    target_pct = float(setup_target_pct)
    first_target_pct = target_pct * 0.5
    first = bounded.simulate_exit(
        path, int(entry_index), float(entry_price), bool(is_long), float(stop_pct),
        first_target_pct, tighten_after_minutes=float(tighten_after_minutes),
        tightened_stop_pct=float(tightened_stop_pct))
    common = dict(
        first_target_pct=first_target_pct,
        partial_pct=0.5,
        first_target_exit_fraction=0.5 if first['target_hit'] else 0.0,
        runner_fraction=0.5 if first['target_hit'] else 0.0,
        runner_target_pct=target_pct,
        runner_stop='FIRST_TARGET',
        runner_stop_activation='NEXT_ONE_MINUTE_BAR_OPEN',
        full_exit_fraction=0.0 if first['target_hit'] else 1.0,
    )
    if not first['target_hit']:
        outcome = ('FULL_STOP_BEFORE_T1' if first['stop_hit']
                   else 'FULL_TIME_EXIT_BEFORE_T1')
        first.update(common, staged_exit_outcome=outcome,
            first_target_exit_price=np.nan, first_target_exit_ts=pd.NaT,
            first_target_exit_path_index=np.nan, runner_exit_price=np.nan,
            runner_exit_ts=pd.NaT, runner_exit_path_index=np.nan,
            runner_stop_activation_path_index=np.nan,
            effective_exit_price=float(first['exit_price']))
        return first

    stamps = np.asarray(path['timestamp_ns'])
    values = {name:np.asarray(path[name],dtype=float)
              for name in ('open','high','low','close')}
    entry = float(entry_price)
    sign = 1 if is_long else -1
    target_one = entry * (1 + sign * first_target_pct / 100)
    target_two = entry * (1 + sign * target_pct / 100)
    t1_index = int(first['exit_path_index'])
    entry_end = pd.Timestamp(int(stamps[int(entry_index)]),tz='UTC').tz_convert('Asia/Kolkata')
    squareoff = entry_end.normalize()+pd.Timedelta(hours=15,minutes=15)
    matches = np.flatnonzero(stamps == int(squareoff.value))
    if len(matches) != 1:
        raise ValueError('Two-stage exit requires exactly one 15:15 square-off candle')
    squareoff_index = int(matches[0])
    if t1_index > squareoff_index:
        raise ValueError('First target occurs after square-off')

    favorable = values['high' if is_long else 'low']
    target_two_same_bar = (favorable[t1_index] >= target_two if is_long
                           else favorable[t1_index] <= target_two)
    exit_index = None
    runner_price = None
    runner_reason = None
    runner_event = None
    ambiguous = False
    gap = False
    gap_bps = 0.0
    runner_target_hit = False
    runner_stop_hit = False
    if target_two_same_bar:
        exit_index, runner_price = t1_index, target_two
        runner_reason, runner_event = 'T1_THEN_FULL_TARGET_SAME_BAR', 'INTRABAR'
        runner_target_hit = True
    else:
        for index in range(t1_index+1,squareoff_index+1):
            op,hi,lo,close = (float(values[name][index])
                              for name in ('open','high','low','close'))
            stop_open = op <= target_one if is_long else op >= target_one
            stop_hit = lo <= target_one if is_long else hi >= target_one
            target_hit = hi >= target_two if is_long else lo <= target_two
            if stop_hit:
                exit_index = index
                runner_price = op if stop_open else target_one
                runner_reason = 'T1_THEN_PROFIT_LOCK_STOP'
                runner_event = 'OPEN' if stop_open else 'INTRABAR'
                ambiguous = bool(not stop_open and target_hit)
                gap = bool(stop_open and abs(runner_price/target_one-1) > 1e-12)
                gap_bps = abs(runner_price/target_one-1)*10000 if gap else 0.0
                runner_stop_hit = True
                break
            if target_hit:
                exit_index, runner_price = index, target_two
                runner_reason, runner_event = 'T1_THEN_FULL_TARGET', 'INTRABAR'
                runner_target_hit = True
                break
            if index == squareoff_index:
                exit_index, runner_price = index, close
                runner_reason, runner_event = 'T1_THEN_TIME_EXIT_1515', 'CLOSE'
                break
    if exit_index is None:
        # Target one can occur on the 15:15 candle, leaving no later runner bar.
        exit_index, runner_price = t1_index, float(values['close'][t1_index])
        runner_reason, runner_event = 'T1_THEN_TIME_EXIT_1515', 'CLOSE'

    bar_end = pd.Timestamp(int(stamps[exit_index]),tz='UTC').tz_convert('Asia/Kolkata')
    executed = bar_end-pd.Timedelta(minutes=1) if runner_event == 'OPEN' else bar_end
    runner_gross = sign*(float(runner_price)/entry-1)*100
    gross = 0.5*first_target_pct+0.5*runner_gross
    effective_exit = entry*(1+sign*gross/100)
    last = exit_index-1 if runner_event == 'OPEN' else exit_index
    favorable_values = values['high' if is_long else 'low'][int(entry_index):last+1]
    adverse_values = values['low' if is_long else 'high'][int(entry_index):last+1]
    mfe = max(0.0,gross,float(np.max(sign*(favorable_values/entry-1)*100)))
    mae = min(0.0,gross,float(np.min(sign*(adverse_values/entry-1)*100)))
    first.update(common,
        exit_path_index=exit_index, exit_price=float(effective_exit),
        exit_reason=runner_reason, exit_event=runner_event, exit_ts=executed,
        exit_execution_ts=str(executed), exit_bar_end_ts=str(bar_end),
        active_stop_pct_at_exit=-first_target_pct,
        active_stop_price=float(target_one), gross_return_pct=float(gross),
        mfe_pct=float(mfe), mae_pct=float(mae),
        holding_minutes=(executed-entry_end).total_seconds()/60,
        stop_hit=runner_stop_hit, target_hit=True, first_target_hit=True,
        runner_target_hit=runner_target_hit, same_bar_ambiguous=ambiguous,
        exit_gap_through=gap, exit_gap_bps=float(gap_bps),
        staged_exit_outcome=runner_reason,
        first_target_exit_price=float(target_one),
        first_target_exit_ts=first['exit_ts'],
        first_target_exit_path_index=t1_index,
        runner_exit_price=float(runner_price), runner_exit_ts=executed,
        runner_exit_path_index=exit_index,
        runner_stop_activation_path_index=t1_index+1,
        effective_exit_price=float(effective_exit))
    return first


def _two_stage_ledger(native, paths, config):
    """Replay V3 exits and then rerun the unchanged capital allocator."""
    policy = config['exit_policy']
    if policy != TWO_STAGE_EXIT_POLICY:
        raise ValueError('Unexpected G7 two-stage exit policy')
    records = []
    cost_pct = float(config['model_cost_bps_round_trip'])/100
    for source in native.to_dict('records'):
        row = source.copy()
        target = float(np.clip(row['native_target_pct'],
                               config['target_floor_pct'],config['target_cap_pct']))
        row.update(native_stop_pct=config['initial_stop_pct'],
            initial_stop_pct=config['initial_stop_pct'],stop_pct=config['initial_stop_pct'],
            native_target_pct=target,target_pct=target,
            first_target_pct=target*0.5,partial_pct=0.5,runner_fraction=0.5,
            runner_target_pct=target,runner_stop='FIRST_TARGET',
            runner_stop_activation='NEXT_ONE_MINUTE_BAR_OPEN')
        if bool(source['filled']):
            result = _simulate_two_stage_exit(paths[int(row['sid'])],
                int(source['entry_path_index']),float(source['entry_price']),
                source['side']=='LONG',config['initial_stop_pct'],target,
                tighten_after_minutes=config['tighten_after_minutes'],
                tightened_stop_pct=config['tightened_stop_pct'])
            row.update(result)
            row.update(cost_pct=cost_pct,
                net_return_pct=result['gross_return_pct']-cost_pct)
        records.append(row)
    frame = pd.DataFrame(records)
    settings = bounded.history.simulator.g.v9.V9Config(
        portfolio_capital_rupees=config['portfolio_capital_rs'],
        capital_per_entry_rupees=config['capital_per_entry_rs'],
        leverage_factor=config['leverage'],max_positions=10,
        cost_bps=config['model_cost_bps_round_trip'])
    sized = bounded.history.simulator.g.v9.v5.apply_fixed_capital_model(
        frame,settings.capital_per_entry_rupees,settings.leverage_factor)
    ledger,_ = bounded.history.simulator.g.v9.v6.apply_portfolio_constraints(
        sized,settings.portfolio_config())
    ledger['day'] = ledger.day.astype(str).str[:10]
    ledger['strategy'] = 'V13-V10-G-7'
    ledger['strategy_version'] = config['variant_id']
    ledger['source_strategy_fingerprint'] = ledger['strategy_fingerprint']
    ledger['strategy_fingerprint'] = mirror.fingerprint(config)
    ledger['scope'] = 'BACKTEST_ONLY'
    ledger['execution_authority'] = False
    return ledger


def stats(book, days):
    """G7 metrics with explicit two-stage target and profit-lock counts."""
    result = bounded.stats(book,days)
    cohort = book.loc[book.day.isin(days)]
    trades = cohort.loc[cohort.portfolio_executed.eq(True)]
    reasons = trades.exit_reason.fillna('').astype(str)
    result['stop_exits'] = int(reasons.isin(
        ['STOP','TIGHTENED_STOP','PROTECTED_STOP','FULL_STOP',
         'T1_THEN_PROFIT_LOCK_STOP']).sum())
    result['first_target_hits'] = int(trades['first_target_hit'].eq(True).sum()
        if 'first_target_hit' in trades else 0)
    result['runner_target_hits'] = int(trades['runner_target_hit'].eq(True).sum()
        if 'runner_target_hit' in trades else 0)
    result['profit_lock_exits'] = int(reasons.eq('T1_THEN_PROFIT_LOCK_STOP').sum())
    return result


def reselect(pool, config):
    """Preserve guard-before-quota selection; new slots use the full G3 filter."""
    entry = mirror.configuration()
    scenario = entry['primary_scenario']
    retained = mirror.reselect(pool, entry['scenarios'][scenario],
                               entry['five_minute_volume_ratio_overrides'][scenario])
    added = donor.select_orders(pool.loc[pool.setup_id.isin(ADDED_IDS)].copy())
    nonempty=[part for part in [retained,added] if len(part)]
    combined=pd.concat(nonempty,ignore_index=True,sort=False) if nonempty else retained
    return Identity(config).identify(combined)


class Prices(mirror.Prices):
    """Bound retained price frames while keeping the original input validation."""
    def frame(self, day, symbol):
        frame, key = super().frame(day,symbol)
        while len(self.cache)>4:
            oldest = next(k for k in self.cache if k!=key)
            del self.cache[oldest]
        return frame,key


def _bounded(native, paths, config):
    if config.get('exit_policy',{}).get('mode') == TWO_STAGE_EXIT_MODE:
        ledger = _two_stage_ledger(native,paths,config)
    else:
        ledger = bounded.bounded_ledger(native,paths,config)
    ledger['strategy_version'] = config['variant_id']
    return ledger


def _approved_research_evidence():
    """Load and pin the two research decisions that the user approved."""
    evidence = {}
    research_root = bounded.G4_ROOT/'research'
    for setup_id, expected in APPROVED_RESEARCH.items():
        folder = research_root/expected['artifact']
        manifest = folder/'manifest.json'
        if not manifest.is_file() or mirror.sha256(manifest) != expected['manifest_sha256']:
            raise ValueError(f'Approved {setup_id} research evidence changed or is missing')
        summary = mirror.read(folder/'summary.json')
        comparison = read_frozen_csv(folder/'comparison.csv')
        if setup_id == '0921_LONG':
            if summary.get('selected_policy') != 'baseline' or summary.get('setup_id') != setup_id:
                raise ValueError('Approved 09:20 LONG decision is not baseline')
        elif summary.get('recommended_policy') != 'baseline':
            raise ValueError('Approved 09:20 SHORT decision is not baseline')
        evidence[setup_id] = dict(
            artifact=str(folder), manifest_sha256=expected['manifest_sha256'],
            decision=expected['decision'], summary=summary,
            baseline_comparison=comparison.loc[comparison.policy.eq('baseline')].to_dict('records'))
    return evidence


def _approved_configuration(identity, config):
    rows = []
    confirmation = config['minimum_confirmation_1m_volume_ratio_by_setup']
    source_identity = g6_source.Identity(g6_source.configuration())
    for setup_id in APPROVED_0920_IDS:
        spec = identity._ACTIVE_BY_ID[setup_id]
        before = asdict(source_identity._ACTIVE_BY_ID[setup_id])
        approved = asdict(spec)
        before.pop('source_version',None)
        approved.pop('source_version',None)
        before['minimum_confirmation_1m_volume_ratio'] = confirmation.get(setup_id)
        approved['minimum_confirmation_1m_volume_ratio'] = confirmation.get(setup_id)
        rows.append(dict(setup_id=setup_id, side=spec.side,
            configuration_changed=before != approved, before=json.dumps(before,sort_keys=True),
            approved=json.dumps(approved,sort_keys=True)))
    return pd.DataFrame(rows)


def _approved_comparison(baseline, result, days, configuration_changed):
    periods = {
        'development35': days[:35],
        f'later{len(days[35:])}': days[35:],
        'october_to_last_complete_session': [d for d in days if d >= '2026-10-01'],
        'all_complete_sessions': days,
        'latest_complete_session': days[-1:],
    }
    rows = []
    for setup_id in APPROVED_0920_IDS:
        before_setup = baseline.loc[baseline.setup_id.eq(setup_id)]
        approved_setup = result.loc[result.setup_id.eq(setup_id)]
        for period, cohort in periods.items():
            before = stats(before_setup, cohort)
            approved = stats(approved_setup, cohort)
            row = dict(setup_id=setup_id, period=period,
                       configuration_changed=bool(configuration_changed))
            for field in before:
                row['before_'+field] = before[field]
                row['approved_'+field] = approved[field]
                row['delta_'+field] = approved[field] - before[field]
            rows.append(row)
    return pd.DataFrame(rows)


def _load_g6_baseline(days):
    """Load the sealed selected G6 ledger for a causal target-only comparison."""
    pointer_path = g6_source.G6_ROOT / 'latest' / 'bounded_exit.json'
    pointer = mirror.read(pointer_path)
    root = (g6_source.G6_ROOT / pointer['run_dir']).resolve()
    if (not root.is_relative_to(g6_source.G6_ROOT.resolve()) or
            mirror.sha256(root / 'manifest.json') != pointer['manifest_sha256']):
        raise ValueError('Sealed G6 baseline pointer differs')
    summary = mirror.read(root / 'summary.json')
    if summary['variant_id'] != g6_source.VERSION or summary['days'] != list(days):
        raise ValueError('Sealed G6 baseline does not cover the G7 sessions')
    policy = pointer['selected_policy']
    if policy != summary['selected_policy']:
        raise ValueError('Sealed G6 selected policy differs')
    book = read_frozen_csv(root / policy / 'portfolio_trades.csv')
    return book, root, pointer


def _load_g7_baseline(days, variant_id, label):
    """Load the newest complete saved G7 package for a rollback comparison."""
    matches = []
    for summary_path in G7_ROOT.glob('runs/*/*/summary.json'):
        try:
            summary = mirror.read(summary_path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if summary.get('variant_id') == variant_id and summary.get('days') == list(days):
            matches.append((summary_path.parent, summary))
    if not matches:
        raise ValueError(f'No complete G7 {label} rollback package covers the requested sessions')
    root, summary = sorted(matches, key=lambda item: str(item[0]))[-1]
    manifest_path = root/'manifest.json'
    manifest = mirror.read(manifest_path)
    for name, digest in manifest['files'].items():
        member = (root/name).resolve()
        if not member.is_relative_to(root.resolve()) or mirror.sha256(member) != digest:
            raise ValueError(f'G7 {label} rollback artifact changed: '+name)
    policy = summary['selected_policy']
    return read_frozen_csv(root/policy/'portfolio_trades.csv'), root, mirror.sha256(manifest_path)


def _load_g7_v1_baseline(days):
    return _load_g7_baseline(days,V1_VERSION,'V1')


def _load_g7_v2_baseline(days):
    return _load_g7_baseline(days,V2_VERSION,'V2')


def _today_research(evidence, result, days):
    rows = evidence['0921_SHORT']['baseline_comparison']
    today = [row for row in rows if row.get('period') == 'today_2026_10_16']
    if len(today) != 1:
        raise ValueError('Approved 09:20 SHORT research is missing the 16 October baseline row')
    row = dict(today[0])
    official = stats(result.loc[result.setup_id.eq('0921_SHORT')], ['2026-10-16'])
    row.update(setup_id='0921_SHORT', evidence_scope='RETROSPECTIVE_RESEARCH_AT_STUDY_TIME',
        study_time_complete_decision_evidence_rows=0,
        study_time_official_status='UNAVAILABLE_IN_OFFICIAL_PARTIAL_REPLAY',
        current_g7_has_complete_session='2026-10-16' in days,
        **{'current_g7_'+key: value for key,value in official.items()})
    return pd.DataFrame([row])


PAIR_FIELDS = ('selected_orders','native_fills','trades','wins','losses','win_rate_pct',
    'profitable_days','losing_days','net_pnl_rs','negative_trade_sum_abs_rs',
    'stop_exits','daily_close_drawdown_rs')


def _pair_row(nifty_book, primary_book, days, **dimensions):
    old, new = stats(nifty_book, days), stats(primary_book, days)
    result = dict(dimensions)
    result.update({'nifty_'+key: old[key] for key in PAIR_FIELDS})
    result.update({'primary_'+key: new[key] for key in PAIR_FIELDS})
    result.update(trades_change=new['trades']-old['trades'],
        wins_change=new['wins']-old['wins'], losses_change=new['losses']-old['losses'],
        win_rate_change_pp=new['win_rate_pct']-old['win_rate_pct'],
        losing_days_change=new['losing_days']-old['losing_days'],
        net_pnl_change_rs=new['net_pnl_rs']-old['net_pnl_rs'],
        loss_amount_reduction_rs=old['negative_trade_sum_abs_rs']-new['negative_trade_sum_abs_rs'],
        stop_exits_change=new['stop_exits']-old['stop_exits'],
        drawdown_reduction_rs=old['daily_close_drawdown_rs']-new['daily_close_drawdown_rs'],
        profit_retained_pct=(100*new['net_pnl_rs']/old['net_pnl_rs']
                             if old['net_pnl_rs'] else np.nan))
    return result


def _day_status(value):
    return 'Profit' if value > 1e-8 else 'Loss' if value < -1e-8 else 'Flat'


def _direction_reports(identity, days, unfiltered, nifty_book, primary_book,
                       nifty_audit, primary_audit, snapshot_audit, order_mapping):
    """Build paired result tables without conflating decisions and allocation."""
    periods = {'all':days,'prior':days[:-1],'last_day':days[-1:]}
    summary = pd.DataFrame([_pair_row(nifty_book, primary_book, cohort, period=name)
                            for name,cohort in periods.items()])
    nifty_decisions = nifty_audit[['sid','nifty_contract','nifty_source_sha256','nifty_bar_ts',
        'slot_candle','session_open','both_counter_allowed']].rename(columns={
            'nifty_contract':'nifty_contract','nifty_source_sha256':'nifty_source_sha256',
            'nifty_bar_ts':'nifty_bar_ts','slot_candle':'nifty_slot_change_pct',
            'session_open':'nifty_session_change_pct','both_counter_allowed':'nifty_allowed'})
    primary_decisions = primary_audit[['sid','day','setup_id','tradingsymbol','side','confirmation_ts',
        'mapped_primary_index','current_snapshot_index','mapping_regime','mapping_effective_from',
        'mapping_effective_through','decision_index','index_instrument_token','index_source_symbol',
        'index_source_sha256','index_bar_ts','slot_change_pct','session_change_pct',
        'both_counter_allowed']].rename(columns={
            'tradingsymbol':'symbol','mapped_primary_index':'primary_index',
            'decision_index':'primary_decision_index','index_instrument_token':'primary_index_token',
            'index_source_symbol':'primary_index_source_symbol',
            'index_source_sha256':'primary_index_source_sha256','index_bar_ts':'primary_index_bar_ts',
            'slot_change_pct':'primary_slot_change_pct','session_change_pct':'primary_session_change_pct',
            'both_counter_allowed':'primary_allowed'})
    decision = primary_decisions.merge(nifty_decisions,on='sid',how='inner',validate='one_to_one')
    setup_times = {spec.setup_id:spec.signal_end for spec in identity.ACTIVE_SETUPS}
    decision.insert(3,'signal_end',decision.setup_id.map(setup_times))
    old_allowed, new_allowed = decision.nifty_allowed.astype(bool), decision.primary_allowed.astype(bool)
    decision['decision_transition'] = np.select([
        old_allowed & new_allowed, ~old_allowed & ~new_allowed,
        ~old_allowed & new_allowed, old_allowed & ~new_allowed],
        ['BOTH_ALLOW','BOTH_BLOCK','NIFTY_BLOCK_PRIMARY_ALLOW','NIFTY_ALLOW_PRIMARY_BLOCK'],
        default='INVALID')
    base_fields = ['sid','filled','portfolio_executed','portfolio_net_profit_rupees','exit_reason']
    decision = decision.merge(unfiltered[base_fields].rename(columns={
        'filled':'unfiltered_native_filled','portfolio_executed':'unfiltered_executed',
        'portfolio_net_profit_rupees':'unfiltered_net_pnl_rs','exit_reason':'unfiltered_exit_reason'}),
        on='sid',validate='one_to_one')
    for prefix, book in [('nifty',nifty_book),('primary',primary_book)]:
        decision = decision.merge(book[['sid','portfolio_executed','portfolio_net_profit_rupees',
            'exit_reason']].rename(columns={'portfolio_executed':prefix+'_executed',
                'portfolio_net_profit_rupees':prefix+'_net_pnl_rs',
                'exit_reason':prefix+'_exit_reason'}),on='sid',validate='one_to_one')
    decision['execution_change'] = np.select([
        ~decision.nifty_executed.astype(bool) & decision.primary_executed.astype(bool),
        decision.nifty_executed.astype(bool) & ~decision.primary_executed.astype(bool),
        decision.nifty_executed.astype(bool) & decision.primary_executed.astype(bool) &
            ~np.isclose(decision.nifty_net_pnl_rs,decision.primary_net_pnl_rs,rtol=0,atol=1e-9)],
        ['ADDED','REMOVED','RESIZED'],default='UNCHANGED')
    decision['net_pnl_change_rs'] = decision.primary_net_pnl_rs-decision.nifty_net_pnl_rs

    transition_counts = decision.groupby(['day','decision_transition']).size().unstack(fill_value=0)
    daily_rows=[]
    for day in days:
        row=_pair_row(nifty_book.loc[nifty_book.day.eq(day)],
                      primary_book.loc[primary_book.day.eq(day)],[day],day=day)
        row['nifty_day_status']=_day_status(row['nifty_net_pnl_rs'])
        row['primary_day_status']=_day_status(row['primary_net_pnl_rs'])
        row['loss_to_profit']=row['nifty_day_status']=='Loss' and row['primary_day_status']=='Profit'
        row['profit_to_loss']=row['nifty_day_status']=='Profit' and row['primary_day_status']=='Loss'
        row['day_outcome']='IMPROVED' if row['net_pnl_change_rs']>1e-8 else (
            'WORSENED' if row['net_pnl_change_rs'] < -1e-8 else 'UNCHANGED')
        for transition in ['BOTH_ALLOW','BOTH_BLOCK','NIFTY_BLOCK_PRIMARY_ALLOW','NIFTY_ALLOW_PRIMARY_BLOCK']:
            row['decisions_'+transition.lower()]=int(transition_counts.loc[day,transition]) \
                if day in transition_counts.index and transition in transition_counts else 0
        daily_rows.append(row)
    daily=pd.DataFrame(daily_rows)

    def grouped_rows(groups, fields):
        rows=[]
        for values, sids in groups:
            values=(values,) if not isinstance(values,tuple) else values
            dimensions=dict(zip(fields,values))
            sid_set=set(sids)
            n=nifty_book.loc[nifty_book.sid.isin(sid_set)]
            p=primary_book.loc[primary_book.sid.isin(sid_set)]
            row=_pair_row(n,p,days,**dimensions)
            subset=decision.loc[decision.sid.isin(sid_set)]
            row.update(nifty_blocks=int((~subset.nifty_allowed.astype(bool)).sum()),
                primary_blocks=int((~subset.primary_allowed.astype(bool)).sum()),
                decision_flips=int(subset.nifty_allowed.astype(bool).ne(
                    subset.primary_allowed.astype(bool)).sum()))
            rows.append(row)
        return pd.DataFrame(rows)

    setup_groups=[]
    for spec in identity.ACTIVE_SETUPS:
        sids=decision.loc[decision.setup_id.eq(spec.setup_id),'sid'].tolist()
        setup_groups.append(((spec.setup_id,spec.signal_end,spec.side),sids))
    setup_side=grouped_rows(setup_groups,['setup_id','signal_end','side'])
    side=grouped_rows([((name,),decision.loc[decision.side.eq(name),'sid'].tolist())
                       for name in ('LONG','SHORT')],['side'])
    index_groups=[]
    for values, part in decision.groupby(['primary_index','side'],sort=True):
        index_groups.append((values,part.sid.tolist()))
    index_side=grouped_rows(index_groups,['primary_index','side'])

    changed=decision.loc[decision.nifty_allowed.astype(bool).ne(
        decision.primary_allowed.astype(bool))].copy()
    execution_changes=decision.loc[decision.execution_change.ne('UNCHANGED') |
        decision.net_pnl_change_rs.abs().gt(1e-9)].copy()
    execution_changes['impact_source']=np.where(
        execution_changes.nifty_allowed.astype(bool).ne(execution_changes.primary_allowed.astype(bool)),
        'DIRECT_DECISION_FLIP','INDIRECT_REALLOCATION')

    transition_rows=[]
    for name,part in decision.groupby('decision_transition',sort=True):
        sids=set(part.sid)
        row=_pair_row(nifty_book.loc[nifty_book.sid.isin(sids)],
                      primary_book.loc[primary_book.sid.isin(sids)],days,
                      decision_transition=name,orders=len(part))
        native=unfiltered.loc[unfiltered.sid.isin(sids)&unfiltered.filled.eq(True)]
        row.update(native_fills=len(native),native_winners=int(native.net_return_pct.gt(0).sum()),
                   native_losers=int(native.net_return_pct.lt(0).sum()))
        transition_rows.append(row)
    transition_summary=pd.DataFrame(transition_rows)

    snapshot = snapshot_audit.set_index('sid').both_counter_allowed
    regime_rows=[]
    for regime,part in decision.groupby('mapping_regime',sort=True):
        sids=set(part.sid)
        row=_pair_row(nifty_book.loc[nifty_book.sid.isin(sids)],
                      primary_book.loc[primary_book.sid.isin(sids)],days,
                      mapping_regime=regime,orders=len(part))
        mapped=order_mapping.loc[order_mapping.sid.isin(sids)]
        row.update(membership_changed_orders=int(mapped.primary_index.ne(
            mapped.current_snapshot_index).sum()),
            effective_vs_snapshot_decision_changes=int(primary_audit.loc[
                primary_audit.sid.isin(sids),'both_counter_allowed'].astype(bool).ne(
                    primary_audit.loc[primary_audit.sid.isin(sids),'sid'].map(snapshot).astype(bool)).sum()))
        regime_rows.append(row)
    regime_summary=pd.DataFrame(regime_rows)
    return dict(summary=summary,daily=daily,setup_side=setup_side,side=side,
        index_side=index_side,decisions=decision,changed=changed,
        execution_changes=execution_changes,transition_summary=transition_summary,
        regime_summary=regime_summary)


def run(config_path, output, *, through=None, direction_filter_mode=None, nifty_filter_mode=None):
    config = configuration(config_path)
    if direction_filter_mode is not None and nifty_filter_mode is not None:
        raise ValueError('Choose direction_filter_mode or the legacy nifty_filter_mode, not both')
    requested_mode = (direction_filter_mode if direction_filter_mode is not None else
                      nifty_filter_mode if nifty_filter_mode is not None else
                      config['direction_filter']['mode'])
    if requested_mode not in primary_filter.MODES:
        raise ValueError('Unknown G7 direction filter mode')
    mode = primary_filter.canonical_mode(requested_mode)
    selected_policy = config['direction_filter']['alternatives'][mode]
    config = {**config, 'direction_filter': {**config['direction_filter'],
        'mode': mode, 'selected_policy': selected_policy}}
    approved_evidence = _approved_research_evidence()
    approved_1216 = approved1216.load_evidence()
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to(G7_ROOT.resolve()):
        raise ValueError('Fresh isolated G7 result directory required')
    codepaths = [Path(__file__),Path(config_path),Path(g6_source.__file__),Path(bounded.__file__),Path(mirror.__file__),
                 Path(donor.__file__),Path(mirror.production.__file__),
                 bounded.ROOT/'research/g4_1011_short_threshold_compare.py',
                 bounded.ROOT/'fno_v13_v10_g_6_backtest.py',
                 bounded.ROOT/'research/g3_sl_target_simulator.py',Path(bounded.history.__file__),
                 Path(approved1216.__file__),Path(nifty_filter.__file__),
                 Path(nifty_filter.nifty.__file__),Path(primary_filter.__file__),
                 primary_filter.MEMBERSHIP_CONFIG]
    codehash = {str(p.resolve()):mirror.sha256(p) for p in codepaths}
    old,old_summary,current,source_summary,pointer = bounded.checked_sources(bounded.configuration(),through)
    print('Verified source history; replaying retained entries and adding both G3 setups.',flush=True)
    prices = Prices(source_summary)
    retained,old_identity,changes,sources,candidates = bounded.extended_orders(old,old_summary,current,source_summary,prices)
    revised_retained, short_audit, short_changes, short_provenance = reselect_1011_short(
        retained, source_summary)
    g3 = read_frozen_csv(current/'selected_orders.csv')
    added = g3.loc[g3.setup_id.isin(ADDED_IDS)].copy()
    combined = pd.concat([revised_retained,added],ignore_index=True,sort=False)
    if combined.sid.duplicated().any() or combined.duplicated(['day','setup_id','tradingsymbol']).any():
        raise ValueError('Duplicate selected order in expanded G4 inventory')
    combined_before_slot_filters = combined.copy()
    combined, slot_filter_audit, slot_filter_rejections = apply_slot_entry_filters(combined, config)
    print(f'Applied slot entry filters: {len(slot_filter_rejections)} ranked orders vetoed.',flush=True)
    days = source_summary['days']
    g6_baseline, g6_baseline_root, g6_baseline_pointer = _load_g6_baseline(days)
    v1_baseline, v1_baseline_root, v1_baseline_manifest_sha256 = _load_g7_v1_baseline(days)
    v2_baseline, v2_baseline_root, v2_baseline_manifest_sha256 = _load_g7_v2_baseline(days)
    path_orders = pd.concat([combined, retained], ignore_index=True, sort=False).drop_duplicates('sid')
    paths = prices.paths(path_orders.sort_values(['tradingsymbol','day','confirmation_ts']))
    print(f'Validated {len(paths)} complete execution paths; {len(added)} added selected orders.',flush=True)
    calendar = [date.fromisoformat(d) for d in days]
    native = bounded.history._simulate(retained,paths,calendar,old_identity)
    previous = native.loc[native.day.le(old_summary['last_date'])]
    saved = read_frozen_csv(old/old_summary['primary_scenario']/'portfolio_trades.csv')
    bounded.history._same_native(previous,saved)
    bounded.history._same_allocation(previous,saved)
    control = _bounded(native,paths,bounded.configuration())
    identity = Identity(config)
    orders = identity.identify(combined)
    replay = bounded.history._simulate(orders,paths,calendar,identity)
    unfiltered = _bounded(replay,paths,config)
    unfiltered['g7_direction_filter_mode'] = primary_filter.OFF
    # The Nifty-futures policy is independently auditable. An optional
    # primary-index comparison must not block it when dated membership ends.
    nifty_audit, nifty_proof, nifty_inputs = nifty_filter.order_audit(unfiltered,prices)
    nifty_result, _ = nifty_filter.apply_filter(unfiltered,nifty_audit,config)
    nifty_result['g7_direction_filter_mode'] = primary_filter.NIFTY_FILTER
    primary_unavailable_reason = None
    try:
        (primary_audit, nifty50_spot_audit, snapshot_naive_audit, primary_proof,
            primary_inputs, effective_mapping, order_mapping,
            primary_metadata) = primary_filter.order_audits(unfiltered)
    except ValueError as exc:
        if mode == primary_filter.PRIMARY_FILTER or not str(exc).startswith(
                ('Expected one effective primary index:', 'Missing primary-index mapping:')):
            raise
        primary_unavailable_reason = str(exc)
    if primary_unavailable_reason is None:
        primary_result, _ = primary_filter.apply_filter(
            unfiltered,primary_audit,config,mode=primary_filter.PRIMARY_FILTER)
        nifty50_spot_result, _ = primary_filter.apply_filter(
            unfiltered,nifty50_spot_audit,config,mode=primary_filter.NIFTY50_SPOT_FILTER)
    else:
        primary_result = nifty50_spot_result = None
    for book in (nifty_result,primary_result,nifty50_spot_result):
        if book is None:
            continue
        book['strategy_fingerprint'] = unfiltered['strategy_fingerprint']
    for row in nifty_proof.itertuples():
        if mirror.sha256(Path(row.path)) != row.sha256:
            raise ValueError('Nifty futures input changed during G7 replay')
    if primary_unavailable_reason is None:
        for row in primary_proof.itertuples():
            if mirror.sha256(Path(row.path)) != row.sha256:
                raise ValueError('Primary cash-index input changed during G7 replay')
    policy_books = {NIFTY_POLICY:nifty_result,'g7_unfiltered':unfiltered}
    if primary_result is not None:
        policy_books[PRIMARY_POLICY] = primary_result
    result = policy_books[selected_policy].copy()
    result['g7_direction_filter_mode'] = mode
    expected_sids = set(g6_baseline.sid) - set(slot_filter_rejections.sid)
    if set(result.sid) != expected_sids or len(result) != len(expected_sids):
        raise ValueError('G7 selected-order inventory differs from sealed G6 plus configured vetoes')
    # Target and entry-filter changes must not alter surviving native entries.
    a = control.set_index('sid').sort_index()
    b = unfiltered.set_index('sid').sort_index()
    expected_retained_rejections = set(slot_filter_rejections.sid) & set(a.index)
    if set(a.index) - set(b.index) != expected_retained_rejections:
        raise ValueError('Configured slot vetoes do not explain retained-order inventory changes')
    common = a.index.intersection(b.index)
    for field in ['filled','entry_ts','entry_price']:
        pd.testing.assert_series_equal(a.loc[common,field],b.loc[common,field],
                                       check_names=False,check_dtype=False)
    for path,digest in codehash.items():
        if mirror.sha256(Path(path))!=digest:
            raise ValueError('Code changed during G4 replay')
    for path,proof in prices.proof.items():
        if mirror.sha256(Path(path))!=proof['sha256']:
            raise ValueError('Price source changed during G4 replay')
    if mirror.sha256(current/'manifest.json')!=pointer['manifest_sha256']:
        raise ValueError('G3 selection evidence changed during G4 replay')
    ledgers = {'g4_previous_49':control,'g6_current_targets':g6_baseline,
        'g7_uniform_075_previous':v1_baseline,
        'g7_single_exit_previous':v2_baseline,
        'g7_unfiltered':unfiltered,
        NIFTY_POLICY:nifty_result,SELECTED_POLICY:result}
    if primary_result is not None:
        ledgers[NIFTY50_SPOT_POLICY] = nifty50_spot_result
        ledgers[PRIMARY_POLICY] = primary_result
    periods = {'all':days,'prior':days[:-1],'last_day':[days[-1]]}
    comparison = pd.DataFrame([dict(policy=p,period=t,**stats(book,cohort))
        for p,book in ledgers.items() for t,cohort in periods.items()])
    daily = pd.DataFrame([dict(day=d,policy=p,**stats(book,[d]))
        for d in days for p,book in ledgers.items()])
    direction_reports = (_direction_reports(identity,days,unfiltered,nifty_result,primary_result,
        nifty_audit,primary_audit,snapshot_naive_audit,order_mapping)
        if primary_result is not None else None)
    approved_configuration = _approved_configuration(identity, config)
    approved_0920_changed = bool(config['approved_0920_decisions']['configuration_changed'])
    approved_comparison = _approved_comparison(
        g6_baseline, result, days, approved_0920_changed)
    today_research = _today_research(approved_evidence, unfiltered, days)
    summary = dict(variant_id=config['variant_id'],strategy='V13-V10-G-7',
        replica_source_variant=g6_source.VERSION,
        direction_filter_schema_version=2,direction_filter_mode=mode,
        direction_filter_requested_mode=requested_mode,direction_filter_enabled=mode!=primary_filter.OFF,
        selected_policy=selected_policy,compatibility_policy=SELECTED_POLICY,
        nifty_direction_filter_enabled=True,nifty_direction_filter_mode=nifty_filter.FILTER,
        nifty_reference_mode=nifty_filter.FILTER,nifty_reference_policy=NIFTY_POLICY,
        primary_index_reference_policy=PRIMARY_POLICY if primary_result is not None else None,
        nifty50_spot_reference_policy=NIFTY50_SPOT_POLICY if primary_result is not None else None,
        primary_index_reference_available=primary_result is not None,
        primary_index_unavailable_reason=primary_unavailable_reason,
        nifty_order_decisions=len(nifty_audit),nifty_source_proofs=nifty_proof.to_dict('records'),
        primary_index_order_decisions=len(primary_audit) if primary_result is not None else None,
        primary_index_source_proofs=primary_proof.to_dict('records') if primary_result is not None else [],
        primary_index_mapping_workbook_sha256=primary_metadata['workbook_sha256'] if primary_result is not None else None,
        primary_vs_nifty_raw_decision_changes=int(
            primary_audit.set_index('sid').both_counter_allowed.astype(bool).ne(
                nifty_audit.set_index('sid').both_counter_allowed.astype(bool)).sum()) if primary_result is not None else None,
        effective_vs_snapshot_raw_decision_changes=int(
            primary_audit.set_index('sid').both_counter_allowed.astype(bool).ne(
                snapshot_naive_audit.set_index('sid').both_counter_allowed.astype(bool)).sum()) if primary_result is not None else None,
        effective_membership_changed_orders=int(order_mapping.primary_index.ne(
            order_mapping.current_snapshot_index).sum()) if primary_result is not None else None,
        scope='BACKTEST_ONLY',execution_authority=False,
        sessions=len(days),first_date=days[0],last_date=days[-1],days=days,active_setups=51,
        added_setup_ids=sorted(ADDED_IDS),
        added_selected_orders=int(orders.setup_id.isin(ADDED_IDS).sum()),
        primary_policy=SELECTED_POLICY,control_policy='g6_current_targets',
        g6_baseline_run=str(g6_baseline_root),
        g6_baseline_manifest_sha256=g6_baseline_pointer['manifest_sha256'],
        g7_v1_rollback_run=str(v1_baseline_root),
        g7_v1_rollback_manifest_sha256=v1_baseline_manifest_sha256,
        g7_v2_rollback_run=str(v2_baseline_root),
        g7_v2_rollback_manifest_sha256=v2_baseline_manifest_sha256,
        original_g4_history_parity_verified=True,
        retained_native_entries_unchanged_for_surviving_orders=True,
        entry_override_changes=short_changes.to_dict('records'),
        entry_override_source=short_provenance,
        exit_policy=config.get('exit_policy'),
        slot_entry_filter_application=config.get('filter_application'),
        slot_entry_filters=config.get('slot_entry_filters',{}),
        slot_entry_filter_rejections=slot_filter_rejections.to_dict('records'),
        baseline_g4_run=str(old),baseline_g4_manifest_sha256=config['donor_g4_manifest_sha256'],
        current_g3_source_run=str(current),current_g3_manifest_sha256=pointer['manifest_sha256'],
        appended_sources=sources,price_sources=prices.proof,comparison=comparison.to_dict('records'),
        approved_1216_comparison=approved_1216['metadata'],
        approved_0920=dict(setup_ids=list(APPROVED_0920_IDS),
            configuration_changed=approved_0920_changed,
            winner_profit_give_up_limit_pct=10.0,
            decision=config['approved_0920_decisions']['decision'],
            research={setup_id: dict(artifact=value['artifact'],
                manifest_sha256=value['manifest_sha256'],decision=value['decision'])
                for setup_id,value in approved_evidence.items()},
            complete_session_comparison=approved_comparison.to_dict('records'),
            today_2026_10_16=dict(
                research_scope='RETROSPECTIVE_RESEARCH_AT_STUDY_TIME',
                study_time_complete_decision_evidence_rows=0,
                study_time_status='UNAVAILABLE_IN_OFFICIAL_PARTIAL_REPLAY',
                current_g7_has_complete_session='2026-10-16' in days,
                current_g7_metrics=stats(
                    unfiltered.loc[unfiltered.setup_id.eq('0921_SHORT')],['2026-10-16']))),
        limitations=['Backtest only; researched historical setups are not an untouched holdout.',
            'Complete one-minute paths, stop-first ambiguity, observed-open gaps and 5bps model costs.',
            'Rs100,000 capital per entry, 5x exposure, Rs1,000,000 portfolio; all allocation replayed.',
            'Opening entries require the same verified opening evidence and Nifty guard as G3.',
            'G7 V3 retains the sealed G6 per-setup targets and the V2 entry filters; V2 remains the single-exit rollback baseline.',
            'V3 exits 50% at half the setup target, keeps 50% for the full target, and moves the runner stop to target one from the next one-minute bar open.',
            'After target one, full portfolio capital remains conservatively reserved until the runner exits; integer-share rounding is not modeled.',
            'The three slot entry filters veto ranked orders without replacement before execution.',
            'The selected G7 direction mode is explicit; nifty_both_counter_skip and off remain rollback modes.',
            ('Each stock uses its effective-dated primary size index; the 2026-09-30 official reconstitution is applied as of that date.'
             if primary_result is not None else 'Optional primary-index comparison omitted because dated membership does not cover every order: '+primary_unavailable_reason),
            'Cash-index and Nifty-futures bar timestamps mark completed candles; actual feed receipt before submission is not independently established.',
            'The Nifty-futures to primary-cash comparison changes both benchmark choice and futures-versus-cash basis; the Nifty-50 cash reference isolates source basis.',
            'This current G3 reconstruction can differ from the frozen 54-session G4 research benchmark; compare this run with its own g7_unfiltered ledger.',
            'The separate 16 October research row records the evidence available at study time; the current complete G4 replay, when available, is reported independently.'])
    output.mkdir(parents=True)
    nifty_audit.to_csv(output/'nifty_direction_audit.csv',index=False)
    nifty_proof.to_csv(output/'nifty_source_proof.csv',index=False)
    (output/'nifty_inputs').mkdir()
    for day, frame in nifty_inputs.items():
        frame.to_parquet(output/'nifty_inputs'/(day+'.parquet'),index=False)
    if primary_result is not None:
        primary_filter.write_evidence(output,primary_audit,nifty50_spot_audit,
            snapshot_naive_audit,primary_proof,primary_inputs,effective_mapping,
            order_mapping,primary_metadata)
    for name,book in ledgers.items():
        folder = output/name
        folder.mkdir()
        book.to_csv(folder/'portfolio_trades.csv',index=False)
        bounded.history._daily(book,calendar).to_csv(folder/'daily_results.csv',index=False)
    orders.to_csv(output/'selected_orders.csv',index=False)
    combined_before_slot_filters.to_csv(output/'selected_orders_before_slot_filters.csv',index=False)
    slot_filter_audit.to_csv(output/'slot_entry_filter_audit.csv',index=False)
    slot_filter_rejections.to_csv(output/'slot_entry_filter_rejections.csv',index=False)
    result.loc[result.setup_id.isin(ADDED_IDS)].to_csv(output/'added_setup_orders.csv',index=False)
    result.loc[result.day.eq(days[-1])].to_csv(output/'last_day_orders.csv',index=False)
    pd.DataFrame([dict(setup_id=s.setup_id,signal_end=s.signal_end,side=s.side,
        target_pct=s.target_pct,stop_pct=s.stop_pct,**stats(result.loc[result.setup_id.eq(s.setup_id)],days))
        for s in identity.ACTIVE_SETUPS]).to_csv(output/'setup_results.csv',index=False)
    comparison.to_csv(output/'comparison.csv',index=False)
    daily.to_csv(output/'daily_comparison.csv',index=False)
    if direction_reports is not None:
        direction_reports['summary'].to_csv(output/'primary_vs_nifty_summary.csv',index=False)
        direction_reports['daily'].to_csv(output/'primary_vs_nifty_daily.csv',index=False)
        direction_reports['setup_side'].to_csv(output/'primary_vs_nifty_slot_side.csv',index=False)
        direction_reports['side'].to_csv(output/'primary_vs_nifty_side.csv',index=False)
        direction_reports['index_side'].to_csv(output/'primary_vs_nifty_index_side.csv',index=False)
        direction_reports['decisions'].to_csv(output/'direction_decision_comparison.csv',index=False)
        direction_reports['changed'].to_csv(output/'changed_direction_decisions.csv',index=False)
        direction_reports['execution_changes'].to_csv(output/'portfolio_execution_changes.csv',index=False)
        direction_reports['transition_summary'].to_csv(output/'decision_transition_summary.csv',index=False)
        direction_reports['regime_summary'].to_csv(output/'mapping_regime_summary.csv',index=False)
    approved_configuration.to_csv(output/'approved_0920_configuration.csv',index=False)
    approved_comparison.to_csv(output/'approved_0920_comparison.csv',index=False)
    today_research.to_csv(output/'approved_0920_today_research.csv',index=False)
    (output/'approved_0920_research_evidence.json').write_text(
        json.dumps(approved_evidence,indent=2,default=str)+'\n',encoding='utf-8')
    approved1216.write_evidence(output,approved_1216)
    candidates.to_csv(output/'appended_guard_candidate_audit.csv',index=False)
    changes.to_csv(output/'selection_changes.csv',index=False)
    short_audit.to_csv(output/'1011_short_candidate_audit.csv',index=False)
    short_changes.to_csv(output/'1011_short_entry_changes.csv',index=False)
    (output/'config.json').write_text(json.dumps(config,indent=2)+'\n',encoding='utf-8')
    (output/'summary.json').write_text(json.dumps(summary,indent=2,default=str)+'\n',encoding='utf-8')
    report = '<!doctype html><meta charset="utf-8"><title>G7 replica: 51 setups</title><style>body{background:#101927;color:#edf4ff;font:15px/1.6 system-ui;padding:24px}a{color:#8ed6ff}td,th{padding:8px;border-bottom:1px solid #476078;text-align:left}table{border-collapse:collapse}</style><h1>G7: isolated G6 replica, 51 setups</h1>'
    report += '<p>Selected direction mode: '+html.escape(mode)+'. Compatibility policy: g7_bounded_exit; explicit selected policy: '+html.escape(selected_policy)+'. Same-run rollback control: g7_unfiltered.</p>'
    report += '<p>Selected entry gate: the Nifty-futures rule rejects only when both completed five-minute and session directions oppose the trade. '+(
        'The optional effective-dated primary-index comparison is available.' if primary_result is not None else
        'The optional primary-index comparison is unavailable: '+html.escape(primary_unavailable_reason))+ '</p>'
    report += f'<p>{days[0]} through {days[-1]}: {len(days)} complete sessions. G7 V3 retains the sealed G6 per-setup targets and three V2 slot entry vetoes. It exits 50% at half target, runs 50% to the full target, and moves the runner stop to target one from the next one-minute bar. The initial stop remains 1.20%, tightening to 1.00% after 120 minutes before target one.</p>'
    fields = ['policy','period','trades','wins','losses','win_rate_pct','stop_exits','profitable_days','losing_days','net_pnl_rs']
    report += comparison[fields].to_html(index=False,border=0,float_format=lambda v:f'{v:,.2f}')
    paired_fields=['period','nifty_trades','primary_trades','trades_change','nifty_wins','primary_wins',
        'nifty_losses','primary_losses','nifty_losing_days','primary_losing_days','nifty_net_pnl_rs',
        'primary_net_pnl_rs','net_pnl_change_rs','loss_amount_reduction_rs','drawdown_reduction_rs']
    if direction_reports is not None:
        report += '<h2>Primary index versus prior Nifty-futures rule</h2>'+direction_reports['summary'][paired_fields].to_html(index=False,border=0,float_format=lambda v:f'{v:,.2f}')
    approved_fields = ['setup_id','period','configuration_changed','approved_trades','approved_wins',
        'approved_losses','approved_win_rate_pct','approved_net_pnl_rs','delta_net_pnl_rs']
    report += '<h2>09:21 retained configuration</h2><p>G7 V3 retains the sealed G6 entry parameters and full targets, including the audited 09:21 LONG and SHORT targets; only exit handling changes.</p>'
    report += approved_configuration.to_html(index=False,border=0)
    report += approved_comparison[approved_fields].to_html(index=False,border=0,float_format=lambda v:f'{v:,.2f}')
    report += '<h3>16 October research and G7 replay</h3><p>The retrospective 09:20 SHORT reconstruction is preserved as source evidence. G7 V3 retains the restored G6 target and applies the two-stage exit.</p>'
    report += today_research[['setup_id','period','setup_trades','setup_wins','setup_losses','setup_win_rate_pct','setup_net_pnl_rs','evidence_scope','study_time_official_status','current_g7_trades','current_g7_wins','current_g7_losses','current_g7_net_pnl_rs']].to_html(index=False,border=0,float_format=lambda v:f'{v:,.2f}')
    report += approved1216.report_section(approved_1216)
    report += '<h2>Daywise comparison</h2>'+daily[['day','policy',*fields[2:]]].to_html(index=False,border=0,float_format=lambda v:f'{v:,.2f}')
    report += '<h2>Scope and assumptions</h2><ul>'+''.join('<li>'+html.escape(x)+'</li>' for x in summary['limitations'])+'</ul>'
    report += '<p><a href="daily_comparison.csv">All-policy daywise CSV</a>'
    if direction_reports is not None:
        report += ' | <a href="primary_vs_nifty_daily.csv">Primary vs Nifty daywise</a> | <a href="primary_vs_nifty_slot_side.csv">Slot/side</a> | <a href="primary_vs_nifty_index_side.csv">Index/side</a> | <a href="changed_direction_decisions.csv">Changed decisions</a> | <a href="primary_index_effective_mapping.csv">Effective mapping</a>'
    report += '</p>'
    (output/'report.html').write_text(report,encoding='utf-8')
    (output/'manifest.json').write_text(json.dumps(dict(code_sha256=codehash,
        files={p.relative_to(output).as_posix():mirror.sha256(p) for p in output.rglob('*') if p.is_file()}),indent=2)+'\n',encoding='utf-8')
    verify(output)
    from fno_v13_v10_g_3_daily_replay import atomic_json
    atomic_json(G7_ROOT/'latest/bounded_exit.json',dict(variant_id=config['variant_id'],
        run_dir=output.relative_to(G7_ROOT).as_posix(),manifest_sha256=mirror.sha256(output/'manifest.json'),
        session_date=days[-1],direction_filter_mode=mode,selected_policy=selected_policy,
        execution_authority=False))
    return summary


def verify(output):
    output = Path(output)
    manifest = mirror.read(output/'manifest.json')
    summary = mirror.read(output/'summary.json')
    if summary['variant_id'] not in {V1_VERSION,V2_VERSION,VERSION} or summary['active_setups']!=51 or summary['execution_authority'] is not False:
        raise ValueError('Unexpected G4 result identity')
    if summary.get('approved_0920',{}).get('setup_ids') != list(APPROVED_0920_IDS):
        raise ValueError('Approved 09:20 decisions are missing')
    approved_changed = bool(summary['approved_0920'].get('configuration_changed'))
    if summary['variant_id'] in {V2_VERSION,VERSION} and approved_changed:
        raise ValueError('G7 V2/V3 must retain the restored 09:21 targets')
    if summary['variant_id'] == V1_VERSION and not approved_changed:
        raise ValueError('G7 V1 must record the uniform target change')
    for name,digest in manifest['files'].items():
        member = (output/name).resolve()
        if not member.is_relative_to(output.resolve()) or mirror.sha256(member)!=digest:
            raise ValueError('G4 result artifact changed: '+name)
    # Older V13 runs predate this comparison and remain valid saved baselines.
    if 'approved_1216_comparison' in summary:
        approved1216.verify_evidence(output,summary['approved_1216_comparison'])
    comparison = read_frozen_csv(output/'comparison.csv')
    approved = read_frozen_csv(output/'approved_0920_comparison.csv')
    if (set(approved.setup_id) != set(APPROVED_0920_IDS) or
            not approved.configuration_changed.astype(bool).eq(approved_changed).all()):
        raise ValueError('Approved 09:20 comparison identity changed')
    nifty_filter.verify_saved(output,summary)
    if summary.get('primary_index_reference_policy'):
        primary_filter.verify_saved(output,summary)
        names=['g4_previous_49','g6_current_targets','g7_uniform_075_previous',
               'g7_unfiltered',NIFTY_POLICY,NIFTY50_SPOT_POLICY,
               PRIMARY_POLICY,SELECTED_POLICY]
    else:
        names=['g4_previous_49','g6_current_targets','g7_uniform_075_previous',
               'g7_unfiltered',SELECTED_POLICY]
    if (output/'g7_single_exit_previous').is_dir():
        names.insert(3,'g7_single_exit_previous')
    for name in names:
        book = read_frozen_csv(output/name/'portfolio_trades.csv')
        for period,days in [('all',summary['days']),('prior',summary['days'][:-1]),('last_day',summary['days'][-1:])]:
            recorded = comparison.loc[comparison.policy.eq(name)&comparison.period.eq(period)]
            if len(recorded)!=1:
                raise ValueError('Missing G4 comparison row')
            for field,value in stats(book,days).items():
                if not np.isclose(value,recorded.iloc[0][field],rtol=0,atol=1e-6):
                    raise ValueError('G4 accounting changed: '+field)
    if summary.get('primary_index_reference_policy'):
        selected=read_frozen_csv(output/summary['selected_policy']/'portfolio_trades.csv')
        compatibility=read_frozen_csv(output/SELECTED_POLICY/'portfolio_trades.csv')
        pd.testing.assert_frame_equal(selected,compatibility,check_dtype=False,check_exact=False,
                                      rtol=0,atol=1e-12)
        paired=read_frozen_csv(output/'primary_vs_nifty_summary.csv')
        changed_decisions=read_frozen_csv(output/'changed_direction_decisions.csv')
        if len(changed_decisions) != summary['primary_vs_nifty_raw_decision_changes']:
            raise ValueError('Raw primary-versus-Nifty decision count differs')
        overall=paired.loc[paired.period.eq('all')]
        if len(overall)!=1:
            raise ValueError('Missing primary-versus-Nifty overall comparison')
        delta=float(overall.net_pnl_change_rs.iloc[0])
        daily=read_frozen_csv(output/'primary_vs_nifty_daily.csv')
        if not np.isclose(daily.net_pnl_change_rs.sum(),delta,rtol=0,atol=1e-6):
            raise ValueError('Daywise primary-versus-Nifty profit delta differs')
        for report in ('primary_vs_nifty_slot_side.csv','primary_vs_nifty_index_side.csv'):
            if not np.isclose(read_frozen_csv(output/report).net_pnl_change_rs.sum(),
                              delta,rtol=0,atol=1e-6):
                raise ValueError('Grouped primary-versus-Nifty profit delta differs: '+report)
        changes=read_frozen_csv(output/'portfolio_execution_changes.csv')
        if not np.isclose(changes.net_pnl_change_rs.sum(),delta,rtol=0,atol=1e-6):
            raise ValueError('Trade-level primary-versus-Nifty profit delta differs')
    return summary


