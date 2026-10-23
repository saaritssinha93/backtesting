"""G4 V14: 51 setups with the approved 10:11 SHORT 0.11% entry move."""
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
from research.archived_g3_v13 import contract as frozen_donor
from research import g4_1216_comparison as approved1216
from research.g4_extended_target_sweep import read_frozen_csv

VERSION = 'V13-V10-G-4-BOUNDED-EXIT-51-SETUPS-V14'
PREVIOUS_VERSION = 'V13-V10-G-4-BOUNDED-EXIT-51-SETUPS-V13'
DEFAULT_CONFIG = bounded.ROOT/'configs/strategies/v13_v10_g_4_bounded_exit_51_setups_v14.json'
PREVIOUS_CONFIG = bounded.ROOT/'configs/strategies/v13_v10_g_4_bounded_exit_51_setups_v13.json'
PREVIOUS_CONFIG_SHA256 = '9d95188a2664fe3c58defbae679edbc87633dc7a1da52320d397bf0cb19b9c1c'
ADDED_IDS = frozenset({'0921_SHORT', '1011_LONG'})
APPROVED_0920_IDS = ('0921_LONG', '0921_SHORT')
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
        raise ValueError('G3 entry contract changed; create a new G4 profile')
    value = bounded.configuration()
    entry = mirror.configuration()
    scenario = entry['primary_scenario']
    guards = dict(donor.strategy_payload()['maximum_confirmation_extension_atr_by_setup'])
    guards.update(mirror.guard_limits(entry['scenarios'][scenario]))
    value.update(variant_id=VERSION, active_setup_count=51,
        donor_g3_version=donor.STRATEGY_VERSION, donor_g3_fingerprint=DONOR_FINGERPRINT,
        added_setup_ids=sorted(ADDED_IDS),
        approved_0920_decisions=dict(
            setup_ids=list(APPROVED_0920_IDS), configuration_changed=False,
            winner_profit_give_up_limit_pct=10.0,
            decision='Retain the current 09:20 LONG and SHORT parameters; publish their audited comparisons with G4.',
            research={setup_id: dict(details) for setup_id, details in APPROVED_RESEARCH.items()}),
        selection_rule='Keep existing 49 G4 entry rules, except 10:11 SHORT requires a 0.11% downward move; add G3 09:20 SHORT and 10:10 LONG with their full entry filters and quotas; allocate the combined portfolio.',
        target_rule='Clip all 51 setup targets to 0.60 through 2.00 percent',
        active_setups=[dict(setup_id=s.setup_id, **asdict(replace(s, source_version=VERSION,
            stop_pct=1.2, target_pct=float(np.clip(s.target_pct,.6,2.))))) for s in donor.ACTIVE_SETUPS],
        maximum_confirmation_extension_atr_by_setup=guards,
        minimum_confirmation_1m_volume_ratio_by_setup=dict(donor.MIN_CONFIRMATION_VOLUME_BY_SETUP),
        five_minute_volume_ratio_overrides=entry['five_minute_volume_ratio_overrides'][scenario])
    return value


def configuration(path=DEFAULT_CONFIG):
    value = mirror.read(path)
    if (Path(path).resolve() == PREVIOUS_CONFIG.resolve() and
            mirror.sha256(PREVIOUS_CONFIG) == PREVIOUS_CONFIG_SHA256 and
            value.get('variant_id') == PREVIOUS_VERSION):
        return value
    if value != configuration_payload():
        raise ValueError('G4 51-setup backtest configuration drift')
    return value


class Identity(mirror.Identity):
    def __init__(self, config):
        self.STRATEGY_VERSION = config['variant_id']
        self.ACTIVE_SETUPS = tuple(mirror.production.SetupSpec(**{k:v for k,v in s.items() if k!='setup_id'})
                                   for s in config['active_setups'])
        self._ACTIVE_BY_ID = {s.setup_id:s for s in self.ACTIVE_SETUPS}
        previous = self.STRATEGY_VERSION == PREVIOUS_VERSION
        self.payload = (frozen_donor if previous else donor).strategy_payload()
        self.payload.update(strategy_version=self.STRATEGY_VERSION, strategy_profile='V13_V10_G_4',
            accepted_variant=self.STRATEGY_VERSION, execution_authority=False, scope='BACKTEST_ONLY',
            source_strategy_version=config['donor_g3_version'], source_strategy_fingerprint=config['donor_g3_fingerprint'],
            active_setups=[asdict(s) for s in self.ACTIVE_SETUPS],
            core_setups=[asdict(replace(s,source_version=self.STRATEGY_VERSION,
                price_change_pct=.13 if previous and s.setup_id=='1011_SHORT' else s.price_change_pct,
                stop_pct=1.2,
                target_pct=float(np.clip(s.target_pct,.6,2.))))
                for s in donor.CORE_SETUPS],
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


def run(config_path, output, *, through=None):
    from research import g4_1011_short_threshold_compare as short_threshold
    config = configuration(config_path)
    approved_evidence = _approved_research_evidence()
    approved_1216 = approved1216.load_evidence()
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to(bounded.G4_ROOT.resolve()):
        raise ValueError('Fresh G4 result directory required')
    codepaths = [Path(__file__),Path(config_path),Path(short_threshold.__file__),Path(bounded.__file__),Path(mirror.__file__),
                 Path(donor.__file__),Path(mirror.production.__file__),
                 bounded.ROOT/'fno_v13_v10_g_4_backtest.py',
                 bounded.ROOT/'research/g3_sl_target_simulator.py',Path(bounded.history.__file__),
                 Path(approved1216.__file__)]
    codehash = {str(p.resolve()):mirror.sha256(p) for p in codepaths}
    old,old_summary,current,source_summary,pointer = bounded.checked_sources(bounded.configuration(),through)
    print('Verified source history; replaying retained entries and adding both G3 setups.',flush=True)
    prices = Prices(source_summary)
    frozen_retained,old_identity,changes,sources,candidates = bounded.extended_orders(old,old_summary,current,source_summary,prices)
    if config['variant_id'] == VERSION:
        short_audit,short_sources = short_threshold._candidate_pool(source_summary)
        retained,short_audit,short_changes = short_threshold._select_changed(short_audit,frozen_retained)
    else:
        retained = frozen_retained
        short_audit,short_sources = pd.DataFrame(),{}
        short_changes = pd.DataFrame(columns=['day','setup_id','tradingsymbol','action'])
    g3 = read_frozen_csv(current/'selected_orders.csv')
    added = g3.loc[g3.setup_id.isin(ADDED_IDS)].copy()
    combined = pd.concat([retained,added],ignore_index=True,sort=False)
    if combined.sid.duplicated().any() or combined.duplicated(['day','setup_id','tradingsymbol']).any():
        raise ValueError('Duplicate selected order in expanded G4 inventory')
    days = source_summary['days']
    path_orders = pd.concat([combined,frozen_retained.loc[~frozen_retained.sid.isin(combined.sid)]],
                            ignore_index=True,sort=False)
    paths = prices.paths(path_orders.sort_values(['tradingsymbol','day','confirmation_ts']))
    print(f'Validated {len(paths)} complete execution paths; {len(added)} added selected orders.',flush=True)
    calendar = [date.fromisoformat(d) for d in days]
    native = bounded.history._simulate(frozen_retained,paths,calendar,old_identity)
    previous = native.loc[native.day.le(old_summary['last_date'])]
    saved = read_frozen_csv(old/old_summary['primary_scenario']/'portfolio_trades.csv')
    bounded.history._same_native(previous,saved)
    bounded.history._same_allocation(previous,saved)
    control = _bounded(native,paths,bounded.configuration())
    identity = Identity(config)
    orders = identity.identify(combined)
    replay = bounded.history._simulate(orders,paths,calendar,identity)
    result = _bounded(replay,paths,config)
    # Extra positions may change allocation, but cannot change unchanged fills.
    a = control.set_index('sid').sort_index()
    b = result.set_index('sid').loc[a.index.intersection(result.sid)].sort_index()
    a = a.loc[b.index]
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
    ledgers = {'g4_previous_49':control,'g4_bounded_exit':result}
    periods = {'all':days,'prior':days[:-1],'last_day':[days[-1]]}
    comparison = pd.DataFrame([dict(policy=p,period=t,**bounded.stats(book,cohort))
        for p,book in ledgers.items() for t,cohort in periods.items()])
    daily = pd.DataFrame([dict(day=d,policy=p,**bounded.stats(book,[d]))
        for d in days for p,book in ledgers.items()])
    approved_configuration = _approved_configuration(identity, config)
    approved_comparison = _approved_comparison(result, days)
    today_research = _today_research(approved_evidence, result, days)
    summary = dict(variant_id=config['variant_id'],scope='BACKTEST_ONLY',execution_authority=False,
        sessions=len(days),first_date=days[0],last_date=days[-1],days=days,active_setups=51,
        added_setup_ids=sorted(ADDED_IDS),added_selected_orders=len(added),
        primary_policy='g4_bounded_exit',control_policy='g4_previous_49',
        original_g4_history_parity_verified=True,retained_native_fills_and_exits_unchanged=True,
        short_1011_entry_change=dict(previous_price_change_pct=.13,
            price_change_pct=.11 if config['variant_id']==VERSION else .13,
            selection_changes=short_changes.to_dict('records'),candidate_sources=short_sources,
            rule='Rerank the complete pre-quota 10:11 SHORT pool for every backtested session.'
                if config['variant_id']==VERSION else 'Preserve the frozen 0.13% G4 V13 entry rule.'),
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
                    result.loc[result.setup_id.eq('0921_SHORT')],['2026-10-16']))),
        limitations=['Backtest only; researched historical setups are not an untouched holdout.',
            'Complete one-minute paths, stop-first ambiguity, observed-open gaps and 5bps model costs.',
            'Rs100,000 capital per entry, 5x exposure, Rs1,000,000 portfolio; all allocation replayed.',
            'Opening entries require the same verified opening evidence and Nifty guard as G3.',
            'The four selective research guards are not enabled.',
            'The separate 16 October research row records the evidence available at study time; the current complete G4 replay, when available, is reported independently.'])
    output.mkdir(parents=True)
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
    approved_configuration.to_csv(output/'approved_0920_configuration.csv',index=False)
    approved_comparison.to_csv(output/'approved_0920_comparison.csv',index=False)
    today_research.to_csv(output/'approved_0920_today_research.csv',index=False)
    (output/'approved_0920_research_evidence.json').write_text(
        json.dumps(approved_evidence,indent=2,default=str)+'\n',encoding='utf-8')
    approved1216.write_evidence(output,approved_1216)
    candidates.to_csv(output/'appended_guard_candidate_audit.csv',index=False)
    changes.to_csv(output/'selection_changes.csv',index=False)
    short_audit.to_csv(output/'short_1011_candidate_audit.csv',index=False)
    short_changes.to_csv(output/'short_1011_selection_changes.csv',index=False)
    (output/'config.json').write_text(json.dumps(config,indent=2)+'\n',encoding='utf-8')
    (output/'summary.json').write_text(json.dumps(summary,indent=2,default=str)+'\n',encoding='utf-8')
    report = '<!doctype html><meta charset="utf-8"><title>G4 51 setups</title><style>body{background:#101927;color:#edf4ff;font:15px/1.6 system-ui;padding:24px}a{color:#8ed6ff}td,th{padding:8px;border-bottom:1px solid #476078;text-align:left}table{border-collapse:collapse}</style><h1>G4: 51 setups</h1>'
    report += f'<p>{days[0]} through {days[-1]}: {len(days)} complete sessions. Added 09:20 SHORT and 10:10 LONG; each targets 1.50%. All setups retain G4 bounded exits: stop 1.20%, tightening to 1.00% after 120 minutes; targets 0.60–2.00%.</p>'
    report += f'<p>10:11 SHORT required downward move: {0.11 if config["variant_id"]==VERSION else 0.13:.2f}%.</p>'
    fields = ['policy','period','trades','wins','losses','win_rate_pct','stop_exits','profitable_days','losing_days','net_pnl_rs']
    report += comparison[fields].to_html(index=False,border=0,float_format=lambda v:f'{v:,.2f}')
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
    report += '<p><a href="approved_0920_configuration.csv">Approved 09:20 configuration</a> | <a href="approved_0920_comparison.csv">Approved 09:20 comparison</a> | <a href="approved_0920_today_research.csv">16 October research</a> | <a href="added_setup_orders.csv">Added-setup orders</a> | <a href="setup_results.csv">All setup results</a> | <a href="daily_comparison.csv">Daywise CSV</a></p>'
    (output/'report.html').write_text(report,encoding='utf-8')
    (output/'manifest.json').write_text(json.dumps(dict(code_sha256=codehash,
        files={p.relative_to(output).as_posix():mirror.sha256(p) for p in output.rglob('*') if p.is_file()}),indent=2)+'\n',encoding='utf-8')
    verify(output)
    from fno_v13_v10_g_3_daily_replay import atomic_json
    atomic_json(bounded.G4_ROOT/'latest/bounded_exit.json',dict(variant_id=config['variant_id'],
        run_dir=output.relative_to(bounded.G4_ROOT).as_posix(),manifest_sha256=mirror.sha256(output/'manifest.json'),
        session_date=days[-1],execution_authority=False))
    return summary


def verify(output):
    output = Path(output)
    manifest = mirror.read(output/'manifest.json')
    summary = mirror.read(output/'summary.json')
    if summary['variant_id'] not in (VERSION,PREVIOUS_VERSION) or summary['active_setups']!=51 or summary['execution_authority'] is not False:
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
    for name in ['g4_previous_49','g4_bounded_exit']:
        book = read_frozen_csv(output/name/'portfolio_trades.csv')
        for period,days in [('all',summary['days']),('prior',summary['days'][:-1]),('last_day',summary['days'][-1:])]:
            recorded = comparison.loc[comparison.policy.eq(name)&comparison.period.eq(period)]
            if len(recorded)!=1:
                raise ValueError('Missing G4 comparison row')
            for field,value in bounded.stats(book,days).items():
                if not np.isclose(value,recorded.iloc[0][field],rtol=0,atol=1e-6):
                    raise ValueError('G4 accounting changed: '+field)
    return summary
