"""G4 V9 entries and guards with bounded targets and staged 1.20% stop.

The first 51 sessions are replayed against the sealed G4 V9 result. Each
subsequent closed day is selected from its sealed pre-quota G3 candidate file
with G4's unchanged ATR and volume guards. Research only.
"""
from __future__ import annotations

import copy
from dataclasses import replace
from datetime import date
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import runtime_dir
from research import g3_0950_long_backtest as morning
from research import g3_target_cutoff_backtest as history
from research import g3_target_cutoff_contract as current_g3
from research import g4_g3_mirror_backtest as mirror
from research.g3_sl_target_simulator import simulate_exit
from research.g4_extended_target_sweep import read_frozen_csv

ROOT=Path(__file__).resolve().parents[1]
VERSION='V13-V10-G-4-BOUNDED-EXIT-V11'
DEFAULT_CONFIG=ROOT/'configs/strategies/v13_v10_g_4_bounded_exit_v11.json'
G3_ROOT=runtime_dir('backtesting_result_v13_v10_g_3')
G4_ROOT=runtime_dir('backtesting_result_v13_v10_g_4')


def configuration(path=DEFAULT_CONFIG):
    value=mirror.read(path)
    if (value.get('variant_id')!=VERSION or value.get('strategy')!='V13-V10-G-4'
            or value.get('scope')!='BACKTEST_ONLY' or value.get('execution_authority') is not False
            or value.get('donor_g4_variant')!=mirror.PREVIOUS_VERSION
            or value.get('donor_g4_primary_scenario')!='guard_all_ten_volume_0925_long_120'
            or value.get('donor_g4_manifest_sha256')!='94d4edb50fb1f1854ed9b87873a50d256133238a4818f7fc394f4b6470e706f9'
            or value.get('active_setup_count')!=49
            or tuple(value.get(k) for k in ('initial_stop_pct','tightened_stop_pct',
                    'tighten_after_minutes','target_floor_pct','target_cap_pct',
                    'model_cost_bps_round_trip','capital_per_entry_rs','leverage',
                    'portfolio_capital_rs'))!=(1.2,1.,120,.6,2.,5.,100000.,5.,1000000.)):
        raise ValueError('G4 bounded backtest configuration drift')
    return value


def checked_sources(config,through):
    old=(G4_ROOT/config['donor_g4_run']).resolve()
    if not old.is_relative_to(G4_ROOT.resolve()) or mirror.sha256(old/'manifest.json')!=config['donor_g4_manifest_sha256']:
        raise ValueError('Pinned G4 run changed')
    old_summary=mirror.verify(old)
    if (old_summary['variant_id']!=mirror.PREVIOUS_VERSION
            or old_summary['primary_scenario']!=config['donor_g4_primary_scenario']
            or old_summary['last_date']!='2026-10-14' or old_summary['active_setups']!=49
            or mirror.read(old/'config.json')!=mirror.configuration_payload(mirror.PREVIOUS_VERSION)):
        raise ValueError('Pinned G4 strategy changed')
    pointer=mirror.read(G3_ROOT/'latest/current_backtest.json')
    current=(G3_ROOT/pointer['run_dir']).resolve()
    if not current.is_relative_to(G3_ROOT.resolve()) or mirror.sha256(current/'manifest.json')!=pointer['manifest_sha256']:
        raise ValueError('Current G3 run pointer changed')
    summary,g3_config=mirror._checked_artifacts(current,G3_ROOT,pointer['manifest_sha256'],
        {'summary.json','config.json','selected_orders.csv','portfolio_trades.csv'})
    source_profile = current_g3.contract_for_identity(
        summary['strategy_version'], summary['strategy_fingerprint'])
    if (source_profile is None or g3_config!=source_profile.strategy_payload()
            or len(source_profile.ACTIVE_SETUPS)!=51):
        raise ValueError('Current G3 daily evidence identity changed')
    last=date.fromisoformat(summary['last_date'])
    if through is not None and through!=last:
        raise ValueError('Requested cutoff differs from latest complete G3 source')
    from fno_v13_v10_g_3_daily_replay import require_closed_session
    require_closed_session(last)
    if last<=date.fromisoformat(old_summary['last_date']):
        raise ValueError('No completed day to append')
    frozen=mirror.frozen_donor._ACTIVE_BY_ID
    newer=source_profile._ACTIVE_BY_ID
    if set(newer)-set(frozen)!={'0921_SHORT','1011_LONG'}:
        raise ValueError('G3 entry inventory changed beyond two G3-only slots')
    for sid,original in frozen.items():
        if sid not in newer:
            raise ValueError(f'Missing old G4 setup {sid}')
        for field,val in vars(original).items():
            if field not in {'setup_id','source_version','volume_ratio'} and not (
                    sid == '1011_SHORT' and field == 'price_change_pct'
                    and val == .13 and getattr(newer[sid], field) in (.13, .11)) and getattr(newer[sid],field)!=val:
                raise ValueError(f'G3/G4 shared setup changed: {sid} {field}')
        if sid!='0926_LONG' and newer[sid].volume_ratio!=original.volume_ratio:
            raise ValueError(f'G3/G4 shared volume rule changed: {sid}')
    if newer['0926_LONG'].volume_ratio!=1.2:
        raise ValueError('0926 LONG volume override differs')
    return old,old_summary,current,summary,pointer


def extended_orders(old,old_summary,current,summary,prices):
    config=mirror.read(old/'config.json')
    scenario=old_summary['primary_scenario']
    guards=config['scenarios'][scenario]
    volumes=config['five_minute_volume_ratio_overrides'][scenario]
    guard_ids=set(guards)|set(volumes)
    clocks={int(mirror.frozen_donor._ACTIVE_BY_ID[s].signal_end.replace(':','')) for s in guard_ids}
    historical=read_frozen_csv(old/scenario/'selected_orders.csv')
    g3_orders=read_frozen_csv(current/'selected_orders.csv')
    sources=[];changes=[];extensions=[];candidate_frames=[]
    old_last=date.fromisoformat(old_summary['last_date'])
    for daystr in summary['days']:
        day=date.fromisoformat(daystr)
        if day<=old_last:
            continue
        candidates_source=None
        for record in summary['daily_sources']:
            directory=(G3_ROOT/record['run_dir']).resolve()
            if not directory.is_relative_to(G3_ROOT.resolve()) or mirror.sha256(directory/'run_manifest.json')!=record['manifest_sha256']:
                raise ValueError('Daily source manifest changed')
            manifest=mirror.read(directory/'run_manifest.json')
            if date.fromisoformat(manifest['session_date'])==day:
                candidates_source=(directory,manifest,record)
                break
        if candidates_source is None:
            raise ValueError(f'Missing sealed daily candidates: {day}')
        directory,manifest,record=candidates_source
        item=manifest['artifacts']['candidate_signals']
        file=directory/item['path']
        if mirror.sha256(file)!=item['sha256']:
            raise ValueError('Daily candidate file changed')
        raw=morning.normalize_signals(read_frozen_csv(file))
        short_raw=raw.loc[raw.hhmm_int.eq(1010) & raw.side.eq('SHORT')].copy()
        short_raw['sid']=np.arange(210_000_000+len(extensions)*100_000,
                                   210_000_000+len(extensions)*100_000+len(short_raw))
        short_audit=current_g3.selection_audit(short_raw)
        frozen_short=replace(current_g3._ACTIVE_BY_ID['1011_SHORT'],price_change_pct=.13)
        short_selected=current_g3.rank_candidates(short_audit.to_dict('records'),frozen_short,
                                                    session_date=day)
        raw=raw.loc[raw.hhmm_int.isin(clocks)].copy()
        raw['sid']=np.arange(97_000_000+len(extensions)*100_000,
                             97_000_000+len(extensions)*100_000+len(raw))
        pool=mirror.donor.selection_audit(raw)
        pool=prices.enrich(pool.loc[pool.setup_id.isin(guard_ids)].copy())
        candidate_frames.append(pool)
        selected=mirror.reselect(pool,guards,volumes)
        g3_day=g3_orders.loc[g3_orders.day.eq(daystr)
            & g3_orders.setup_id.isin(mirror.frozen_donor._ACTIVE_BY_ID)].copy()
        current_short=g3_day.loc[g3_day.setup_id.eq('1011_SHORT')]
        frozen_rows=[]
        for candidate in short_selected:
            previous=current_short.loc[current_short.tradingsymbol.eq(candidate['tradingsymbol'])]
            if len(previous)>1:
                raise ValueError('Duplicate G3 10:11 SHORT order')
            frozen_rows.append(previous.iloc[0].to_dict() if len(previous) else candidate)
        frozen_short_orders=pd.DataFrame(frozen_rows).reindex(columns=g3_day.columns)
        frozen_keys=mirror.selected_keys(frozen_short_orders)
        current_keys=mirror.selected_keys(current_short)
        for action,keys in [('REMOVED_BY_G4_1011_THRESHOLD',current_keys-frozen_keys),
                            ('ADDED_BY_G4_1011_THRESHOLD',frozen_keys-current_keys)]:
            changes.extend(dict(day=d,action=action,setup_id=s,tradingsymbol=t)
                           for d,s,t in sorted(keys))
        g3_day=pd.concat([g3_day.loc[g3_day.setup_id.ne('1011_SHORT')],frozen_short_orders],
                         ignore_index=True,sort=False)
        g3_guard=g3_day.loc[g3_day.setup_id.isin(guard_ids)]
        chosen=pd.concat([g3_day.loc[~g3_day.setup_id.isin(guard_ids)],selected],
                         ignore_index=True,sort=False)
        if chosen.sid.duplicated().any():
            raise ValueError('Duplicate appended G4 selection SID')
        for action,keys in [('REMOVED_BY_G4_GUARD',mirror.selected_keys(g3_guard)-mirror.selected_keys(selected)),
                            ('ADDED_BY_G4_GUARD',mirror.selected_keys(selected)-mirror.selected_keys(g3_guard))]:
            changes.extend(dict(day=d,action=action,setup_id=s,tradingsymbol=t)
                           for d,s,t in sorted(keys))
        extensions.append(chosen)
        sources.append(dict(day=daystr,candidate_path=str(file),candidate_sha256=item['sha256'],
                            candidate_count=len(pool),g3_guard_orders=len(g3_guard),g4_guard_orders=len(selected)))
    combined=pd.concat([historical,*extensions],ignore_index=True,sort=False)
    if combined.sid.duplicated().any():
        raise ValueError('G4 historical and appended IDs overlap')
    identity=mirror.Identity(config,scenario)
    identified=identity.identify(combined)
    prior=identified.loc[identified.day.astype(str).str[:10].le(old_summary['last_date'])]
    if mirror.selected_keys(prior)!=mirror.selected_keys(historical):
        raise ValueError('Previous G4 selection changed')
    return identified,identity,pd.DataFrame(changes,columns=['day','action','setup_id','tradingsymbol']),sources,pd.concat(candidate_frames,ignore_index=True,sort=False)


def bounded_ledger(native,paths,config):
    records=[]
    for source in native.to_dict('records'):
        row=source.copy()
        target=float(np.clip(row['native_target_pct'],.6,2.))
        row.update(native_stop_pct=1.2,initial_stop_pct=1.2,stop_pct=1.2,
                   native_target_pct=target,target_pct=target)
        if bool(source['filled']):
            result=simulate_exit(paths[int(row['sid'])],int(source['entry_path_index']),
                float(source['entry_price']),source['side']=='LONG',1.2,target,
                tighten_after_minutes=120,tightened_stop_pct=1.)
            row.update(result)
            row.update(cost_pct=.05,net_return_pct=result['gross_return_pct']-.05,
                       first_target_hit=result['target_hit'],runner_target_hit=result['target_hit'])
        records.append(row)
    frame=pd.DataFrame(records)
    settings=history.simulator.g.v9.V9Config(portfolio_capital_rupees=1000000.,
        capital_per_entry_rupees=100000.,leverage_factor=5.,max_positions=10,cost_bps=5.)
    sized=history.simulator.g.v9.v5.apply_fixed_capital_model(frame,settings.capital_per_entry_rupees,settings.leverage_factor)
    ledger,_=history.simulator.g.v9.v6.apply_portfolio_constraints(sized,settings.portfolio_config())
    ledger['day']=ledger.day.astype(str).str[:10]
    ledger['strategy']='V13-V10-G-4'
    ledger['strategy_version']=VERSION
    ledger['source_strategy_fingerprint']=ledger['strategy_fingerprint']
    ledger['strategy_fingerprint']=mirror.fingerprint(config)
    ledger['scope']='BACKTEST_ONLY'
    ledger['execution_authority']=False
    return ledger


def stats(book,days):
    cohort=book.loc[book.day.isin(days)]
    trades=cohort.loc[cohort.portfolio_executed.eq(True)]
    pnl=trades.portfolio_net_profit_rupees
    daily=trades.groupby('day').portfolio_net_profit_rupees.sum().reindex(days,fill_value=0.)
    equity=np.r_[0.,daily.cumsum().to_numpy()]
    return dict(sessions=len(days),selected_orders=len(cohort),native_fills=int(cohort.filled.sum()),
        trades=len(trades),wins=int(pnl.gt(1e-8).sum()),losses=int(pnl.lt(-1e-8).sum()),
        win_rate_pct=float(100*pnl.gt(1e-8).sum()/len(trades)) if len(trades) else 0.,
        profitable_days=int(daily.gt(1e-8).sum()),losing_days=int(daily.lt(-1e-8).sum()),
        net_pnl_rs=float(pnl.sum()),negative_trade_sum_abs_rs=float(-pnl.loc[pnl.lt(0)].sum()),
        stop_exits=int(trades.exit_reason.isin(['STOP','TIGHTENED_STOP']).sum()),
        daily_close_drawdown_rs=float(np.max(np.maximum.accumulate(equity)-equity)))


def report(summary,comparison,daily,orders,changes):
    show=comparison[['policy','period','trades','wins','losses','win_rate_pct','profitable_days',
        'losing_days','net_pnl_rs','negative_trade_sum_abs_rs','stop_exits','daily_close_drawdown_rs']]
    start='<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G4 bounded exit backtest</title><style>body{background:#0c1523;color:#e5edf6;font:15px/1.55 system-ui;max-width:1500px;margin:auto;padding:30px}a{color:#80cfff}table{border-collapse:collapse;width:100%;font-size:13px}td,th{padding:8px;border-bottom:1px solid #405269;text-align:left}th{background:#24364a}.scroll{overflow:auto}h1,h2{color:#f6fafc}.box{background:#19283a;padding:18px;border:1px solid #3d5570;border-radius:9px}</style></head><body>'
    parts=[start,'<h1>G4 bounded exit backtest</h1>',
        '<p class="box">'+html.escape(summary['first_date'])+' through '+html.escape(summary['last_date'])+
        ' · 52 completed sessions · 49 G4 setups · backtest only. Initial stop 1.20%; tighten to 1.00% after 120 minutes. Each existing target is clipped to 0.60–2.00%.</p>',
        '<h2>Full history, prior history and last day</h2><div class="scroll">'+
        show.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div>',
        '<h2>Last day: every selected G4 order</h2><p>Unfilled and portfolio-rejected orders have zero realized portfolio P&amp;L.</p><div class="scroll">'+
        orders.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div>',
        '<h2>Daywise results</h2><div class="scroll">'+daily.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div>',
        '<h2>New-day G4 guard changes</h2><div class="scroll">'+changes.to_html(index=False,border=0)+'</div>',
        '<p class="box">Earlier G4 V9 native trades and allocation are reproduced exactly. New-day guards are applied before ranking against sealed pre-quota candidates. Exits use complete one-minute price bars and a conservative stop-first rule. Sizing is ₹100,000 capital per entry at five-times exposure with 5bps round-trip cost. Historical results do not establish future performance.</p>',
        '<p><a href="comparison.csv">Summary CSV</a> · <a href="daily_comparison.csv">Daily CSV</a> · <a href="last_day_trade_comparison.csv">Trade-by-trade change</a> · <a href="setup_results.csv">Setup results</a> · <a href="summary.json">Source proof</a></p></body></html>']
    return ''.join(parts)


def run(config_path,output,*,through=None):
    config=configuration(config_path)
    output=Path(output).resolve()
    if output.exists() or not output.is_relative_to(G4_ROOT.resolve()):
        raise ValueError('Fresh G4 result directory required')
    codepaths=[Path(__file__),Path(config_path),Path(mirror.__file__),Path(history.__file__),
        ROOT/'research/g3_sl_target_simulator.py',mirror.frozen_donor.CONFIG,mirror.DEFAULT_CONFIG,
        ROOT/'fno_v13_v10_g_4_backtest.py']
    codehash={str(p.resolve()):mirror.sha256(p) for p in codepaths}
    old,old_summary,current,source_summary,g3_pointer=checked_sources(config,through)
    prices=mirror.Prices(source_summary)
    orders,identity,changes,sources,candidates=extended_orders(old,old_summary,current,source_summary,prices)
    days=source_summary['days']
    paths=prices.paths(orders)
    native=history._simulate(orders,paths,[date.fromisoformat(x) for x in days],identity)
    native['strategy']='V13-V10-G-4'
    previous=native.loc[native.day.le(old_summary['last_date'])]
    saved=read_frozen_csv(old/old_summary['primary_scenario']/'portfolio_trades.csv')
    history._same_native(previous,saved)
    history._same_allocation(previous,saved)
    print(f'Historical G4 parity: {len(previous)} orders, {int(previous.portfolio_executed.sum())} allocated fills',flush=True)
    bounded=bounded_ledger(native,paths,config)
    if not native[['sid','filled','entry_ts','entry_price']].equals(bounded[['sid','filled','entry_ts','entry_price']]):
        raise ValueError('Bounded exits changed entries')
    for path,digest in codehash.items():
        if mirror.sha256(Path(path))!=digest:
            raise ValueError('Code/config changed during G4 replay')
    if mirror.sha256(current/'manifest.json')!=g3_pointer['manifest_sha256']:
        raise ValueError('G3 evidence changed during G4 replay')
    for path,proof in prices.proof.items():
        if mirror.sha256(Path(path))!=proof['sha256']:
            raise ValueError('Price input changed during G4 replay')
    periods={'all':days,'prior':days[:-1],'last_day':[days[-1]]}
    ledgers={'g4_current_control':native,'g4_bounded_exit':bounded}
    comparison=pd.DataFrame([dict(policy=name,period=period,**stats(book,cohort))
        for name,book in ledgers.items() for period,cohort in periods.items()])
    daily=pd.DataFrame([dict(day=day,policy=name,**stats(book,[day])) for day in days
                        for name,book in ledgers.items()])
    last=days[-1]
    summary=dict(variant_id=VERSION,scope='BACKTEST_ONLY',execution_authority=False,
        first_date=days[0],last_date=last,sessions=len(days),active_setups=49,
        primary_policy='g4_bounded_exit',control_policy='g4_current_control',
        baseline_g4_run=str(old),baseline_g4_manifest_sha256=config['donor_g4_manifest_sha256'],
        baseline_g4_scenario=old_summary['primary_scenario'],
        original_g4_history_parity_verified=True,original_g4_history_end=old_summary['last_date'],
        current_g3_source_run=str(current),current_g3_manifest_sha256=g3_pointer['manifest_sha256'],
        excluded_new_g3_setups=['0921_SHORT','1011_LONG'],appended_sources=sources,
        selection_changes=len(changes),price_sources=prices.proof,
        comparison=comparison.to_dict('records'),
        limitations=['Retains G4 V9 49 setups and all of its ATR/volume guards; excludes two newer G3-only setups.',
          'The new day is reselected from the sealed raw candidate file before quota.',
          'Older historical minute data can be mutable current parquets; input hashes are checked and recorded.',
          'One-minute OHLC cannot resolve within-bar sequence, live spreads, stop gaps or broker latency.',
          'The setups were previously researched on this history; this is not an untouched holdout.',
          'The ₹500,000 modeled exposure and flat 5bps costs differ from live integer-share trading.'])
    output.mkdir(parents=True)
    for name,book in ledgers.items():
        folder=output/name
        folder.mkdir()
        book.to_csv(folder/'portfolio_trades.csv',index=False)
        pd.DataFrame([dict(day=day,**stats(book,[day])) for day in days]).to_csv(folder/'daily_results.csv',index=False)
    orders.to_csv(output/'selected_orders.csv',index=False)
    candidates.to_csv(output/'appended_guard_candidate_audit.csv',index=False)
    changes.to_csv(output/'selection_changes.csv',index=False)
    comparison.to_csv(output/'comparison.csv',index=False)
    daily.to_csv(output/'daily_comparison.csv',index=False)
    setup_rows=[]
    for name,book in ledgers.items():
        for spec in mirror.frozen_donor.ACTIVE_SETUPS:
            part=book.loc[book.setup_id.eq(spec.setup_id)]
            for period,cohort in [('all',days),('last_day',[last])]:
                setup_rows.append(dict(policy=name,period=period,setup_id=spec.setup_id,
                    signal_end=spec.signal_end,side=spec.side,current_target_pct=spec.target_pct,
                    bounded_target_pct=float(np.clip(spec.target_pct,.6,2.)),**stats(part,cohort)))
    pd.DataFrame(setup_rows).to_csv(output/'setup_results.csv',index=False)
    columns=['day','sid','setup_id','tradingsymbol','side','trigger','filled','portfolio_executed',
        'entry_ts','entry_price','initial_stop_pct','native_target_pct','exit_ts','exit_price',
        'exit_reason','gross_return_pct','net_return_pct','portfolio_net_profit_rupees',
        'portfolio_trade_exposure_rupees']
    control_orders=native.loc[native.day.eq(last),columns].copy()
    bound_orders=bounded.loc[bounded.day.eq(last),columns].copy()
    control_orders.to_csv(output/'last_day_control_orders.csv',index=False)
    bound_orders.to_csv(output/'last_day_orders.csv',index=False)
    paired=control_orders.merge(bound_orders,on='sid',suffixes=('_current','_bounded'),validate='one_to_one')
    paired['pnl_change_rs']=paired.portfolio_net_profit_rupees_bounded-paired.portfolio_net_profit_rupees_current
    paired.to_csv(output/'last_day_trade_comparison.csv',index=False)
    (output/'config.json').write_text(json.dumps(config,indent=2)+'\n',encoding='utf-8')
    (output/'summary.json').write_text(json.dumps(summary,indent=2,default=str)+'\n',encoding='utf-8')
    (output/'report.html').write_text(report(summary,comparison,daily,bound_orders,changes),encoding='utf-8')
    manifest=dict(code_sha256=codehash,files={p.relative_to(output).as_posix():mirror.sha256(p)
        for p in output.rglob('*') if p.is_file()})
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    verify(output)
    from fno_v13_v10_g_3_daily_replay import atomic_json
    latest=G4_ROOT/'latest'
    latest.mkdir(exist_ok=True)
    atomic_json(latest/'bounded_exit.json',dict(variant_id=VERSION,
        run_dir=output.relative_to(G4_ROOT).as_posix(),
        manifest_sha256=mirror.sha256(output/'manifest.json'),
        session_date=last,execution_authority=False))
    return summary


def verify(output):
    output=Path(output)
    manifest=mirror.read(output/'manifest.json')
    summary=mirror.read(output/'summary.json')
    for name,digest in manifest['files'].items():
        if mirror.sha256(output/name)!=digest:
            raise ValueError(f'G4 bounded artifact changed: {name}')
    for name,digest in manifest['code_sha256'].items():
        if mirror.sha256(Path(name))!=digest:
            raise ValueError(f'G4 bounded source changed: {name}')
    comparison=read_frozen_csv(output/'comparison.csv')
    days=read_frozen_csv(output/'daily_comparison.csv').day.drop_duplicates().tolist()
    if len(days)!=summary['sessions'] or days[0]!=summary['first_date'] or days[-1]!=summary['last_date']:
        raise ValueError('G4 bounded calendar changed')
    for name in ('g4_current_control','g4_bounded_exit'):
        book=read_frozen_csv(output/name/'portfolio_trades.csv')
        for period,cohort in [('all',days),('prior',days[:-1]),('last_day',[days[-1]])]:
            recorded=comparison.loc[comparison.policy.eq(name)&comparison.period.eq(period)]
            if len(recorded)!=1:
                raise ValueError('Missing comparison row')
            actual=stats(book,cohort)
            for field,value in actual.items():
                if not np.isclose(value,recorded.iloc[0][field],rtol=0,atol=1e-6):
                    raise ValueError(f'G4 bounded accounting differs: {name} {period} {field}')
    return summary
