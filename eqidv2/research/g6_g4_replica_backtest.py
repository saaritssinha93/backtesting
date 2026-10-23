"""G6 replica of G4 V13 with the two-direction Nifty backtest entry gate."""
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
from research import g6_nifty_direction as nifty_filter
from research import g6_primary_index_direction as primary_filter
from research.g4_extended_target_sweep import read_frozen_csv

VERSION = 'V13-V10-G-6-G4-REPLICA-V2'
G6_ROOT = bounded.G4_ROOT.parent/'backtesting_result_v13_v10_g_6'
DEFAULT_CONFIG = bounded.ROOT/'configs/strategies/v13_v10_g_6_g4_replica_v2.json'
ADDED_IDS = frozenset({'0921_SHORT', '1011_LONG'})
APPROVED_0920_IDS = ('0921_LONG', '0921_SHORT')
PRIMARY_POLICY = 'g6_primary_index_both_counter'
NIFTY_POLICY = 'g6_nifty_futures_both_counter'
NIFTY50_SPOT_POLICY = 'g6_nifty50_spot_both_counter'
SELECTED_POLICY = 'g6_bounded_exit'
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


def configuration_payload():
    donor.validate_contract()
    if donor.strategy_fingerprint() != DONOR_FINGERPRINT or set(donor._ACTIVE_BY_ID)-set(mirror.frozen_donor._ACTIVE_BY_ID) != ADDED_IDS:
        raise ValueError('G6 entry donor changed')
    value = bounded.configuration()
    entry = mirror.configuration()
    scenario = entry['primary_scenario']
    guards = dict(donor.strategy_payload()['maximum_confirmation_extension_atr_by_setup'])
    guards.update(mirror.guard_limits(entry['scenarios'][scenario]))
    value.update(variant_id=VERSION, strategy='V13-V10-G-6',
        replica_source_variant='V13-V10-G-4-BOUNDED-EXIT-51-SETUPS-V13', active_setup_count=51,
        entry_override=dict(setup_id='1011_SHORT', source_price_change_pct=.13,
                            price_change_pct=.11, historical_replay_all_sessions=True,
                            production_effective_from_session='2026-10-22'),
        donor_g3_version=donor.STRATEGY_VERSION, donor_g3_fingerprint=DONOR_FINGERPRINT,
        added_setup_ids=sorted(ADDED_IDS),
        approved_0920_decisions=dict(
            setup_ids=list(APPROVED_0920_IDS), configuration_changed=False,
            winner_profit_give_up_limit_pct=10.0,
            decision='Retain the current 09:20 LONG and SHORT parameters; publish their audited comparisons with G4.',
            research={setup_id: dict(details) for setup_id, details in APPROVED_RESEARCH.items()}),
        selection_rule='Keep existing 49 G4 entry rules; add G3 09:20 SHORT and 10:10 LONG with their full entry filters and quotas; allocate the combined portfolio.',
        target_rule='Clip all 51 setup targets to 0.60 through 2.00 percent',
        active_setups=[dict(setup_id=s.setup_id, **asdict(replace(s, source_version=VERSION,
            stop_pct=1.2, target_pct=float(np.clip(s.target_pct,.6,2.))))) for s in donor.ACTIVE_SETUPS],
        maximum_confirmation_extension_atr_by_setup=guards,
        minimum_confirmation_1m_volume_ratio_by_setup=dict(donor.MIN_CONFIRMATION_VOLUME_BY_SETUP),
        five_minute_volume_ratio_overrides=entry['five_minute_volume_ratio_overrides'][scenario],
        direction_filter_schema_version=2,
        direction_filter=dict(
            mode=primary_filter.NIFTY_FILTER,
            selected_policy=NIFTY_POLICY,
            alternatives=dict(
                primary_index_both_counter_skip=PRIMARY_POLICY,
                nifty_both_counter_skip=NIFTY_POLICY,
                off='g6_unfiltered'),
            legacy_nifty_mode_alias=primary_filter.LEGACY_NIFTY_FILTER,
            execution_authority=False),
        primary_index_direction_filter=dict(
            mode=primary_filter.PRIMARY_FILTER,
            source='effective_dated_per_stock_primary_size_cash_index_5minute',
            membership_config=primary_filter.MEMBERSHIP_CONFIG.relative_to(bounded.ROOT).as_posix(),
            mapping_workbook=primary_filter.WORKBOOK.relative_to(bounded.ROOT).as_posix(),
            mapping_workbook_sha256='dc9afcfd00e69be53a81de6948cec537995167d06034368b81fc21e90ec1f3c5',
            slot_direction='exact_latest_completed_5minute_close_vs_open',
            session_direction='exact_latest_completed_5minute_close_vs_0920_open',
            block_when='both_slot_and_session_oppose_trade_side',
            flat_or_mixed='allow', missing_or_invalid='fail_run',
            membership_resolution='effective_dated_with_2026_09_30_reconstitution',
            execution_authority=False),
        nifty_direction_filter=dict(
            mode=nifty_filter.FILTER,
            source='dated_near_month_nifty_futures_5minute',
            slot_direction='exact_latest_completed_5minute_close_vs_open',
            session_direction='exact_latest_completed_5minute_close_vs_0920_open',
            block_when='both_slot_and_session_oppose_trade_side',
            flat_or_mixed='allow', missing_or_invalid='fail_run', execution_authority=False))
    return value


def configuration(path=DEFAULT_CONFIG):
    value = mirror.read(path)
    if value != configuration_payload():
        raise ValueError('G4 51-setup backtest configuration drift')
    return value


def reselect_1011_short(retained, source_summary):
    """Apply G6's one entry-rule delta to the complete pre-quota pool."""
    from research.g4_1011_short_threshold_compare import _candidate_pool, _keys, _select_changed

    audit, provenance = _candidate_pool(source_summary)
    if not audit.setup_id.eq('1011_SHORT').all():
        raise ValueError('G6 10:11 audit contains another setup')
    revised, audit, changes = _select_changed(audit, retained)
    old_keys = _keys(retained.loc[retained.setup_id.eq('1011_SHORT')])
    new_keys = _keys(revised.loc[revised.setup_id.eq('1011_SHORT')])
    if revised.sid.duplicated().any() or revised.duplicated(
            ['day', 'setup_id', 'tradingsymbol']).any() or _keys(
            audit.loc[audit.selected_011.eq(True)]) != new_keys:
        raise ValueError('G6 10:11 revised selection differs from candidate audit')
    if set(changes.action) - {'ADDED', 'REMOVED'} or len(changes) != len(new_keys ^ old_keys):
        raise ValueError('G6 10:11 change audit differs from reselection')
    changes['action'] = changes.action + '_BY_1011_MOVE011'
    return revised, audit, changes, provenance


class Identity(mirror.Identity):
    def __init__(self, config):
        self.STRATEGY_VERSION = VERSION
        self.ACTIVE_SETUPS = tuple(mirror.production.SetupSpec(**{k:v for k,v in s.items() if k!='setup_id'})
                                   for s in config['active_setups'])
        self._ACTIVE_BY_ID = {s.setup_id:s for s in self.ACTIVE_SETUPS}
        self.payload = donor.strategy_payload()
        self.payload.update(strategy_version=VERSION, strategy_profile='V13_V10_G_6',
            accepted_variant=VERSION, execution_authority=False, scope='BACKTEST_ONLY',
            source_strategy_version=donor.STRATEGY_VERSION, source_strategy_fingerprint=DONOR_FINGERPRINT,
            active_setups=[asdict(s) for s in self.ACTIVE_SETUPS],
            core_setups=[asdict(replace(s,source_version=VERSION,stop_pct=1.2,
                target_pct=float(np.clip(s.target_pct,.6,2.)))) for s in donor.CORE_SETUPS],
            maximum_confirmation_extension_atr_by_setup=config['maximum_confirmation_extension_atr_by_setup'],
            g4_five_minute_volume_ratio_overrides=config['five_minute_volume_ratio_overrides'])
        self.payload['exit'].update(initial_stop_pct=1.2,
            setups={s.setup_id:dict(stop_pct=s.stop_pct,target_pct=s.target_pct) for s in self.ACTIVE_SETUPS})

    def identify(self, frame):
        result = super().identify(frame)
        result['native_stop_pct'] = 1.2
        result['native_target_pct'] = result.setup_id.map({s.setup_id:s.target_pct for s in self.ACTIVE_SETUPS})
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
    for setup_id in APPROVED_0920_IDS:
        spec = identity._ACTIVE_BY_ID[setup_id]
        values = asdict(spec)
        values['minimum_confirmation_1m_volume_ratio'] = confirmation.get(setup_id)
        rows.append(dict(setup_id=setup_id, side=spec.side,
            configuration_changed=False, before=json.dumps(values,sort_keys=True),
            approved=json.dumps(values,sort_keys=True)))
    return pd.DataFrame(rows)


def _approved_comparison(result, days):
    periods = {
        'development35': days[:35],
        f'later{len(days[35:])}': days[35:],
        'october_to_last_complete_session': [d for d in days if d >= '2026-10-01'],
        'all_complete_sessions': days,
        'latest_complete_session': days[-1:],
    }
    rows = []
    for setup_id in APPROVED_0920_IDS:
        setup = result.loc[result.setup_id.eq(setup_id)]
        for period, cohort in periods.items():
            metrics = bounded.stats(setup, cohort)
            row = dict(setup_id=setup_id, period=period, configuration_changed=False)
            for field, value in metrics.items():
                row['before_'+field] = value
                row['approved_'+field] = value
                row['delta_'+field] = 0
            rows.append(row)
    return pd.DataFrame(rows)


def _today_research(evidence, result, days):
    rows = evidence['0921_SHORT']['baseline_comparison']
    today = [row for row in rows if row.get('period') == 'today_2026_10_16']
    if len(today) != 1:
        raise ValueError('Approved 09:20 SHORT research is missing the 16 October baseline row')
    row = dict(today[0])
    official = bounded.stats(result.loc[result.setup_id.eq('0921_SHORT')], ['2026-10-16'])
    row.update(setup_id='0921_SHORT', evidence_scope='RETROSPECTIVE_RESEARCH_AT_STUDY_TIME',
        study_time_complete_decision_evidence_rows=0,
        study_time_official_status='UNAVAILABLE_IN_OFFICIAL_PARTIAL_REPLAY',
        current_g4_has_complete_session='2026-10-16' in days,
        **{'current_g4_'+key: value for key,value in official.items()})
    return pd.DataFrame([row])


PAIR_FIELDS = ('selected_orders','native_fills','trades','wins','losses','win_rate_pct',
    'profitable_days','losing_days','net_pnl_rs','negative_trade_sum_abs_rs',
    'stop_exits','daily_close_drawdown_rs')


def _pair_row(nifty_book, primary_book, days, **dimensions):
    old, new = bounded.stats(nifty_book, days), bounded.stats(primary_book, days)
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
        raise ValueError('Unknown G6 direction filter mode')
    mode = primary_filter.canonical_mode(requested_mode)
    selected_policy = config['direction_filter']['alternatives'][mode]
    config = {**config, 'direction_filter': {**config['direction_filter'],
        'mode': mode, 'selected_policy': selected_policy}}
    approved_evidence = _approved_research_evidence()
    approved_1216 = approved1216.load_evidence()
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to(G6_ROOT.resolve()):
        raise ValueError('Fresh isolated G6 result directory required')
    codepaths = [Path(__file__),Path(config_path),Path(bounded.__file__),Path(mirror.__file__),
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
    days = source_summary['days']
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
    unfiltered['g6_direction_filter_mode'] = primary_filter.OFF
    # The Nifty-futures policy is independently auditable. An optional
    # primary-index comparison must not block it when dated membership ends.
    nifty_audit, nifty_proof, nifty_inputs = nifty_filter.order_audit(unfiltered,prices)
    nifty_result, _ = nifty_filter.apply_filter(unfiltered,nifty_audit,config)
    nifty_result['g6_direction_filter_mode'] = primary_filter.NIFTY_FILTER
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
            raise ValueError('Nifty futures input changed during G6 replay')
    if primary_unavailable_reason is None:
        for row in primary_proof.itertuples():
            if mirror.sha256(Path(row.path)) != row.sha256:
                raise ValueError('Primary cash-index input changed during G6 replay')
    policy_books = {NIFTY_POLICY:nifty_result,'g6_unfiltered':unfiltered}
    if primary_result is not None:
        policy_books[PRIMARY_POLICY] = primary_result
    result = policy_books[selected_policy].copy()
    result['g6_direction_filter_mode'] = mode
    # Extra positions may change allocation, but cannot change existing fills.
    a = control.set_index('sid').sort_index()
    b = unfiltered.set_index('sid').loc[a.index]
    for field in ['filled','entry_ts','entry_price','exit_ts','exit_price','exit_reason']:
        pd.testing.assert_series_equal(a[field],b[field],check_names=False,check_dtype=False)
    for path,digest in codehash.items():
        if mirror.sha256(Path(path))!=digest:
            raise ValueError('Code changed during G4 replay')
    for path,proof in prices.proof.items():
        if mirror.sha256(Path(path))!=proof['sha256']:
            raise ValueError('Price source changed during G4 replay')
    if mirror.sha256(current/'manifest.json')!=pointer['manifest_sha256']:
        raise ValueError('G3 selection evidence changed during G4 replay')
    ledgers = {'g4_previous_49':control,'g6_unfiltered':unfiltered,
        NIFTY_POLICY:nifty_result,SELECTED_POLICY:result}
    if primary_result is not None:
        ledgers[NIFTY50_SPOT_POLICY] = nifty50_spot_result
        ledgers[PRIMARY_POLICY] = primary_result
    periods = {'all':days,'prior':days[:-1],'last_day':[days[-1]]}
    comparison = pd.DataFrame([dict(policy=p,period=t,**bounded.stats(book,cohort))
        for p,book in ledgers.items() for t,cohort in periods.items()])
    daily = pd.DataFrame([dict(day=d,policy=p,**bounded.stats(book,[d]))
        for d in days for p,book in ledgers.items()])
    direction_reports = (_direction_reports(identity,days,unfiltered,nifty_result,primary_result,
        nifty_audit,primary_audit,snapshot_naive_audit,order_mapping)
        if primary_result is not None else None)
    approved_configuration = _approved_configuration(identity, config)
    approved_comparison = _approved_comparison(unfiltered, days)
    today_research = _today_research(approved_evidence, unfiltered, days)
    summary = dict(variant_id=VERSION,strategy='V13-V10-G-6',
        replica_source_variant='V13-V10-G-4-BOUNDED-EXIT-51-SETUPS-V13',
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
        added_setup_ids=sorted(ADDED_IDS),added_selected_orders=len(added),
        primary_policy=SELECTED_POLICY,control_policy='g6_unfiltered',
        original_g4_history_parity_verified=True,
        retained_native_fills_and_exits_unchanged_except_1011_short=True,
        entry_override_changes=short_changes.to_dict('records'),
        entry_override_source=short_provenance,
        baseline_g4_run=str(old),baseline_g4_manifest_sha256=config['donor_g4_manifest_sha256'],
        current_g3_source_run=str(current),current_g3_manifest_sha256=pointer['manifest_sha256'],
        appended_sources=sources,price_sources=prices.proof,comparison=comparison.to_dict('records'),
        approved_1216_comparison=approved_1216['metadata'],
        approved_0920=dict(setup_ids=list(APPROVED_0920_IDS),configuration_changed=False,
            winner_profit_give_up_limit_pct=10.0,
            decision='Current G4 09:20 LONG and SHORT settings retained after the approved studies.',
            research={setup_id: dict(artifact=value['artifact'],
                manifest_sha256=value['manifest_sha256'],decision=value['decision'])
                for setup_id,value in approved_evidence.items()},
            complete_session_comparison=approved_comparison.to_dict('records'),
            today_2026_10_16=dict(
                research_scope='RETROSPECTIVE_RESEARCH_AT_STUDY_TIME',
                study_time_complete_decision_evidence_rows=0,
                study_time_status='UNAVAILABLE_IN_OFFICIAL_PARTIAL_REPLAY',
                current_g4_has_complete_session='2026-10-16' in days,
                current_g4_metrics=bounded.stats(
                    unfiltered.loc[unfiltered.setup_id.eq('0921_SHORT')],['2026-10-16']))),
        limitations=['Backtest only; researched historical setups are not an untouched holdout.',
            'Complete one-minute paths, stop-first ambiguity, observed-open gaps and 5bps model costs.',
            'Rs100,000 capital per entry, 5x exposure, Rs1,000,000 portfolio; all allocation replayed.',
            'Opening entries require the same verified opening evidence and Nifty guard as G3.',
            'The selected G6 direction mode is explicit; nifty_both_counter_skip and off remain rollback modes.',
            ('Each stock uses its effective-dated primary size index; the 2026-09-30 official reconstitution is applied as of that date.'
             if primary_result is not None else 'Optional primary-index comparison omitted because dated membership does not cover every order: '+primary_unavailable_reason),
            'Cash-index and Nifty-futures bar timestamps mark completed candles; actual feed receipt before submission is not independently established.',
            'The Nifty-futures to primary-cash comparison changes both benchmark choice and futures-versus-cash basis; the Nifty-50 cash reference isolates source basis.',
            'This current G3 reconstruction can differ from the frozen 54-session G4 research benchmark; compare this run with its own g6_unfiltered ledger.',
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
    result.loc[result.setup_id.isin(ADDED_IDS)].to_csv(output/'added_setup_orders.csv',index=False)
    result.loc[result.day.eq(days[-1])].to_csv(output/'last_day_orders.csv',index=False)
    pd.DataFrame([dict(setup_id=s.setup_id,signal_end=s.signal_end,side=s.side,
        target_pct=s.target_pct,stop_pct=s.stop_pct,**bounded.stats(result.loc[result.setup_id.eq(s.setup_id)],days))
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
    report = '<!doctype html><meta charset="utf-8"><title>G6 replica: 51 setups</title><style>body{background:#101927;color:#edf4ff;font:15px/1.6 system-ui;padding:24px}a{color:#8ed6ff}td,th{padding:8px;border-bottom:1px solid #476078;text-align:left}table{border-collapse:collapse}</style><h1>G6: G4 replica, 51 setups</h1>'
    report += '<p>Selected direction mode: '+html.escape(mode)+'. Compatibility policy: g6_bounded_exit; explicit selected policy: '+html.escape(selected_policy)+'. Same-run rollback control: g6_unfiltered.</p>'
    report += '<p>Selected entry gate: the Nifty-futures rule rejects only when both completed five-minute and session directions oppose the trade. '+(
        'The optional effective-dated primary-index comparison is available.' if primary_result is not None else
        'The optional primary-index comparison is unavailable: '+html.escape(primary_unavailable_reason))+ '</p>'
    report += f'<p>{days[0]} through {days[-1]}: {len(days)} complete sessions. Added 09:20 SHORT and 10:10 LONG; each targets 1.50%. All setups retain G4 bounded exits: stop 1.20%, tightening to 1.00% after 120 minutes; targets 0.60â€“2.00%.</p>'
    fields = ['policy','period','trades','wins','losses','win_rate_pct','stop_exits','profitable_days','losing_days','net_pnl_rs']
    report += comparison[fields].to_html(index=False,border=0,float_format=lambda v:f'{v:,.2f}')
    paired_fields=['period','nifty_trades','primary_trades','trades_change','nifty_wins','primary_wins',
        'nifty_losses','primary_losses','nifty_losing_days','primary_losing_days','nifty_net_pnl_rs',
        'primary_net_pnl_rs','net_pnl_change_rs','loss_amount_reduction_rs','drawdown_reduction_rs']
    if direction_reports is not None:
        report += '<h2>Primary index versus prior Nifty-futures rule</h2>'+direction_reports['summary'][paired_fields].to_html(index=False,border=0,float_format=lambda v:f'{v:,.2f}')
    approved_fields = ['setup_id','period','configuration_changed','approved_trades','approved_wins',
        'approved_losses','approved_win_rate_pct','approved_net_pnl_rs','delta_net_pnl_rs']
    report += '<h2>Approved 09:20 LONG and SHORT</h2><p>The approved decision retains the current parameters, so every before/after delta is zero. The tables make that decision auditable without presenting rejected research variants as G4 settings.</p>'
    report += approved_configuration.to_html(index=False,border=0)
    report += approved_comparison[approved_fields].to_html(index=False,border=0,float_format=lambda v:f'{v:,.2f}')
    report += '<h3>16 October research and current official replay</h3><p>The retrospective 09:20 SHORT reconstruction is preserved as evidence from the study. Current G4 metrics come from the newer complete-session replay and are shown independently.</p>'
    report += today_research[['setup_id','period','setup_trades','setup_wins','setup_losses','setup_win_rate_pct','setup_net_pnl_rs','evidence_scope','study_time_official_status','current_g4_trades','current_g4_wins','current_g4_losses','current_g4_net_pnl_rs']].to_html(index=False,border=0,float_format=lambda v:f'{v:,.2f}')
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
    atomic_json(G6_ROOT/'latest/bounded_exit.json',dict(variant_id=VERSION,
        run_dir=output.relative_to(G6_ROOT).as_posix(),manifest_sha256=mirror.sha256(output/'manifest.json'),
        session_date=days[-1],direction_filter_mode=mode,selected_policy=selected_policy,
        execution_authority=False))
    return summary


def verify(output):
    output = Path(output)
    manifest = mirror.read(output/'manifest.json')
    summary = mirror.read(output/'summary.json')
    if summary['variant_id']!=VERSION or summary['active_setups']!=51 or summary['execution_authority'] is not False:
        raise ValueError('Unexpected G4 result identity')
    if summary.get('approved_0920',{}).get('setup_ids') != list(APPROVED_0920_IDS):
        raise ValueError('Approved 09:20 decisions are missing')
    if summary['approved_0920'].get('configuration_changed') is not False:
        raise ValueError('Approved 09:20 configuration must retain the researched baseline')
    for name,digest in manifest['files'].items():
        member = (output/name).resolve()
        if not member.is_relative_to(output.resolve()) or mirror.sha256(member)!=digest:
            raise ValueError('G4 result artifact changed: '+name)
    # Older V13 runs predate this comparison and remain valid saved baselines.
    if 'approved_1216_comparison' in summary:
        approved1216.verify_evidence(output,summary['approved_1216_comparison'])
    comparison = read_frozen_csv(output/'comparison.csv')
    approved = read_frozen_csv(output/'approved_0920_comparison.csv')
    if set(approved.setup_id) != set(APPROVED_0920_IDS) or approved.configuration_changed.astype(bool).any():
        raise ValueError('Approved 09:20 comparison identity changed')
    delta_columns = [name for name in approved if name.startswith('delta_')]
    if not np.allclose(approved[delta_columns].fillna(0).to_numpy(dtype=float),0,rtol=0,atol=1e-9):
        raise ValueError('Approved baseline comparison must have zero deltas')
    nifty_filter.verify_saved(output,summary)
    if summary.get('primary_index_reference_policy'):
        primary_filter.verify_saved(output,summary)
        names=['g4_previous_49','g6_unfiltered',NIFTY_POLICY,NIFTY50_SPOT_POLICY,
               PRIMARY_POLICY,SELECTED_POLICY]
    else:
        names=['g4_previous_49','g6_unfiltered',SELECTED_POLICY]
    for name in names:
        book = read_frozen_csv(output/name/'portfolio_trades.csv')
        for period,days in [('all',summary['days']),('prior',summary['days'][:-1]),('last_day',summary['days'][-1:])]:
            recorded = comparison.loc[comparison.policy.eq(name)&comparison.period.eq(period)]
            if len(recorded)!=1:
                raise ValueError('Missing G4 comparison row')
            for field,value in bounded.stats(book,days).items():
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
