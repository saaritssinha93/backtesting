"""Small causal strategy/entry/portfolio experiments on current selected G7 V4.

Published figures and conservative execution control are separate. Exactly
five predeclared hypotheses; all candidates and failed checks are disclosed.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from research import g7_v4_filtered_backtest as current
from research import g7_v4_conservative_allocation as admission
from research import g7_v4_simple_selection_features as features
from research.g7_entry_quality_20261026 import same_book, verify_members
from research.g8_exit_engine import _flags, metrics
from research.g8_vix_selection_research import dump, sha
import fno_v13_corrected_v5_backtest as native_engine

ROOT=current.ROOT
BASE=current.model.base
OUT_ROOT=BASE.OUT_ROOT/'practical_research'
VERSION='V13-V10-G-7-V4-PRACTICAL-RESEARCH'
POLICIES={
    'cmf20_confirm':'Reject only ready directional CMF20<0; warmup/zero weights preserve entry.',
    'vwap_slope_failure':'Reject opposing5-minute session-VWAP slope AND actual failed breakout of preceding5-bar edge.',
    'close75_interaction':'Retest earlier close75 hypothesis on current V4: require directional close location>=0.75 and agreeing candle body.',
    'gap_cancel10':'During original10-minute pending life, permanently cancel at an observed open >10bps beyond trigger; no later same-bar recovery fill.',
    'loss_lock2':'After two actually executed net-losing exits, block new admissions strictly after the second exit for that day; leave open positions managed normally.',
}
COSTS=(5,9,15,19,30,34)
SELECTION=dict(reference='execution_control',net_profit='>=control',losing_amount='<=90%control',
    loss_count='<control',win_rate='>=control',winner_retention='>=95%',trade_retention='>=80%',
    daily_close_drawdown='<=control',changed_executions_min=5,changed_days_min=3,
    ranking='development net profit; then smallest losing amount; then winners; then name',
    chronology='35 development choose once;14 validation and8 assessment use same gates; no replacement',
    costs='selected rule later-block net>=control at9/15/19/30/34bps',
    no_qualifier='retain saved current G7 V4; do not invent a winner')


def build_features(original,native):
    if (not original.index.is_unique or not native.sid.is_unique
            or not original.index.equals(pd.Index(original.sid,name=original.index.name))
            or set(original.index)!=set(native.sid)):
        raise ValueError('Feature inventory must be indexed by unique native SID')
    index=native.set_index('sid',drop=False);rows=[]
    groups=original.groupby(['stock_source_path','stock_source_sha256'],sort=False)
    for number,((filename,digest),group) in enumerate(groups,1):
        path=Path(filename)
        if sha(path)!=digest:raise ValueError('Stock source changed')
        columns=[c for c in ('date','open','high','low','close','volume','gap_filled','opening_snapshot','provisional_stale','is_synthetic') if c in pq.read_schema(path).names]
        # Arrow date predicates avoid converting years of irrelevant rows to strings.
        filters=[]
        for day in sorted(set(group.day)):
            start=pd.Timestamp(day,tz='Asia/Kolkata')
            filters.append([('date','>=',start),('date','<',start+pd.Timedelta(days=1))])
        raw=pd.read_parquet(path,columns=columns,filters=filters)
        raw['ts']=pd.to_datetime(raw.date,utc=True).dt.tz_convert('Asia/Kolkata')
        for sid in group.sid:
            result=features.feature_row(raw,index.loc[sid])
            rows.append(dict(sid=sid,simple_feature_available_at=result['feature_available_at'],
                **{k:v for k,v in result.items() if k.startswith('sel_')}))
        if sha(path)!=digest:raise ValueError('Stock source changed while reading')
        if number%60==0:print('SIMPLE INDICATOR PREFIXES',number,'/',len(groups),flush=True)
    return original.join(pd.DataFrame(rows).set_index('sid'),validate='one_to_one')


def entry_masks(f):
    t=pd.to_datetime(f.simple_feature_available_at,utc=True)
    c=pd.to_datetime(f.confirmation_ts,utc=True)
    if t.isna().any() or c.isna().any() or (t>c).any():raise ValueError('Selection feature available after confirmation')
    ready={n:_flags(f['sel_'+n+'_ready'],n) for n in ('candle','cmf20','vwap_slope5','prior5')}
    for name,column in [('cmf20','sel_signed_cmf20'),('vwap_slope5','sel_signed_vwap_slope5_pct'),('candle','sel_directional_close_location')]:
        if not np.isfinite(f.loc[ready[name],column]).all():raise ValueError('Missing ready feature '+column)
    if not ready['candle'].all() or not f.sel_directional_close_location.between(0,1).all():raise ValueError('Invalid confirmation candle')
    if not np.isfinite(f.sel_directional_body_return_pct).all():raise ValueError('Invalid directional body')
    cmf=~ready['cmf20'] | f.sel_signed_cmf20.ge(0)
    failed=_flags(f.sel_prior5_failed_breakout,'prior5 failed')
    slope=~(ready['vwap_slope5'] & ready['prior5'] & f.sel_signed_vwap_slope5_pct.lt(0) & failed)
    close=f.sel_directional_close_location.ge(.75) & f.sel_directional_body_return_pct.gt(0)
    return dict(cmf20_confirm=cmf,vwap_slope_failure=slope,close75_interaction=close)


def gap_cancel(native,paths,cap_bps=10.):
    """An observed open cancels before that bar's prospective trigger fill.

    This is a pending-order gap cancellation rule, NOT a stop-limit fill model.
    Native stop-entry touch logic is retained where no cancellation occurs.
    """
    if not np.isfinite(cap_bps) or cap_bps<0:raise ValueError('Invalid gap allowance')
    records=[]
    original_fills=_flags(native.filled,'native fills')
    for index,source in native.iterrows():
        row=source.to_dict();sid=int(row['sid']);p=paths[sid]
        stamp=current.model.features_v4.timestamp(row['confirmation_ts'])
        expected=pd.date_range(stamp+pd.Timedelta(minutes=1),periods=10,freq='min').tz_convert('UTC').asi8
        if not np.array_equal(p['timestamp_ns'][:10],expected):raise ValueError('Missing10-minute order lifetime')
        a=np.column_stack([p[k][:10] for k in ('open','high','low','close')]).astype(float)
        if not np.isfinite(a).all() or (a<=0).any() or (a[:,1]<a.max(axis=1)).any() or (a[:,2]>a.min(axis=1)).any():raise ValueError('Invalid pending-window OHLC')
        long=row['side']=='LONG'
        if row['side'] not in ('LONG','SHORT'):raise ValueError('Invalid side')
        trigger=float(row['trigger'])
        if not np.isfinite(trigger) or trigger<=0:raise ValueError('Invalid trigger')
        limit=trigger*(1+(1 if long else -1)*cap_bps/10000)
        # Validate, rather than regenerate, the sealed native execution event.
        # CSV decimals can differ from NPZ floats by one ULP, in either
        # direction. Applying tolerance to earlier bars could move a native
        # fill. Keep every original event and allow epsilon only when checking
        # that the recorded fill candle supports its sealed execution.
        tolerance=8*np.finfo(float).eps*max(1.,abs(trigger))
        filled=original_fills.loc[index]
        fill_index=int(row['entry_path_index']) if filled else None
        strict=native_engine._entry(SimpleNamespace(**row),p,delay_bars=0,trigger_buffer_pct=0.,worse_fill_bps=0.,max_entry_delay_minutes=10)
        if filled:
            if not 0<=fill_index<10:raise ValueError('Native fill outside original pending lifetime')
            touch=(p['high'][fill_index]>=trigger-tolerance if long else p['low'][fill_index]<=trigger+tolerance)
            price=max(p['open'][fill_index],trigger) if long else min(p['open'][fill_index],trigger)
            if (not touch or not np.isclose(price,row['entry_price'],atol=1e-6,rtol=0)
                    or (strict is not None and strict[0]<fill_index)):
                raise ValueError('Sealed native entry unsupported by its path')
        elif strict is not None:
            raise ValueError('Original native fill inventory inconsistent with10-minute expiry')
        canceled=None
        for i in range(fill_index+1 if filled else 10):
            if (a[i,0]>limit if long else a[i,0]<limit):canceled=i;break
        row.update(gap_cancel_bps=cap_bps,gap_cancel_path_index=canceled,
            gap_cancel_status='CANCELED_OPEN_GAP' if canceled is not None else 'NATIVE_RETAINED' if filled else 'UNFILLED')
        if canceled is not None:
            row.update(filled=False,g8_baseline_filled=False,entry_ts=pd.NaT,entry_price=np.nan,entry_path_index=np.nan,
                exit_ts=pd.NaT,exit_price=np.nan,exit_path_index=np.nan,gross_return_pct=np.nan,net_return_pct=np.nan,exit_reason='UNFILLED')
        records.append(row)
    return pd.DataFrame(records)


def integerize(raw,config):
    work=raw.copy(deep=True)
    filled=_flags(work.g8_baseline_filled,'native fills')
    nominal=float(config['capital_per_entry_rs'])*float(config['leverage'])
    units=pd.Series(0.,index=work.index)
    prices=pd.to_numeric(work.entry_price,errors='coerce')
    if not np.isfinite(prices[filled]).all() or prices[filled].le(0).any():raise ValueError('Invalid filled price')
    units.loc[filled]=np.floor(nominal/prices.loc[filled])
    scale=pd.Series(1.,index=work.index)
    can_trade=filled & units.gt(0)
    scale.loc[can_trade]=(units.loc[can_trade]*prices.loc[can_trade]/nominal).clip(upper=1.)
    work.loc[filled & ~can_trade,['filled','g8_baseline_filled']]=False
    work['g8_quantity_scale']=scale
    work['integer_cash_quantity']=units
    return work


def execute(plan,config,name,cost=5.):
    source=integerize(plan['raw'],config) if plan['integer'] else plan['raw']
    cfg=dict(config,max_positions_per_symbol=1,variant_id=VERSION,
        conservative_entry_clock=plan['clock'],integer_cash_shares=plan['integer'],daily_loss_limit=plan['limit'])
    result=admission.allocate(source,plan['allowed'],cfg,name,cost_bps=cost,
        conservative_entry_clock=plan['clock'],daily_loss_limit=plan['limit'])
    result['strategy']='V13-V10-G-7';result['strategy_version']=VERSION
    result['practical_policy']=name
    return result


def make_plans(native,paths,f,config,settings):
    base_allowed=_flags(f.allowed_g7_v2,'nifty') & ~current.model.decisions(f)['low_efficiency']
    raw=current.previous.replay(native,paths,config,current.protection_settings(settings))
    common=dict(raw=raw,allowed=base_allowed,integer=True,clock=True,limit=None)
    plans={'published_g7_v4':dict(common,integer=False,clock=False),'execution_control':dict(common)}
    masks=entry_masks(f)
    for name in ('cmf20_confirm','vwap_slope_failure','close75_interaction'):
        plans[name]=dict(common,allowed=base_allowed & masks[name])
    canceled=gap_cancel(native,paths)
    canceled_raw=current.previous.replay(canceled,paths,config,current.protection_settings(settings))
    plans['gap_cancel10']=dict(common,raw=canceled_raw)
    plans['loss_lock2']=dict(common,limit=2)
    return plans


def compare(book,control,published,days):
    row=current.model.changes.comparison(book,control,days)
    row['net_change_vs_published_rs']=row['net_pnl_rs']-metrics(published,days)['net_pnl_rs']
    return row


def choose(table,names,period='development'):
    t=table.loc[table.period.eq(period)].set_index('policy');b=t.loc['execution_control'];audit=[];eligible=[]
    for name in names:
        r=t.loc[name];fail=[]
        if r.net_pnl_rs<b.net_pnl_rs-1e-6:fail.append('net profit reduced')
        if r.negative_trade_sum_abs_rs>.9*b.negative_trade_sum_abs_rs+1e-6:fail.append('loss amount reduction below10%')
        if r.losses>=b.losses:fail.append('loss count not reduced')
        if r.win_rate_pct<b.win_rate_pct-1e-8:fail.append('win rate reduced')
        if r.wins<.95*b.wins:fail.append('winner retention below95%')
        if r.trades<.8*b.trades:fail.append('trade retention below80%')
        if r.daily_close_drawdown_rs>b.daily_close_drawdown_rs+1e-6:fail.append('larger daily-close drawdown')
        if r.changed_executions<5 or r.changed_days<3:fail.append('insufficient changed trades/days')
        audit.append(dict(policy=name,period=period,eligible=not fail,failure_reasons='; '.join(fail)))
        if not fail:eligible.append((r.net_pnl_rs,-r.negative_trade_sum_abs_rs,r.wins,name))
    return (max(eligible)[3] if eligible else None),audit


def minute_equity(book,paths,days):
    """Completed-minute-close MTM; no claim to cover intraminute drawdown."""
    records=[];carry=0.;peak=0.
    executed=book.loc[_flags(book.portfolio_executed,'executed')]
    for day in days:
        grid=pd.date_range(day+' 09:16',day+' 15:15',freq='min',tz='Asia/Kolkata')
        pnl=np.zeros(len(grid))
        for row in executed.loc[executed.day.eq(day)].itertuples():
            entry=pd.Timestamp(row.entry_ts);exit_=pd.Timestamp(row.exit_ts)
            after=grid>=exit_;active=(grid>=entry)&(grid<exit_)
            pnl[after]+=row.portfolio_net_profit_rupees
            if active.any():
                p=paths[int(row.sid)];where=np.searchsorted(p['timestamp_ns'],grid[active].asi8)
                if (where>=len(p['timestamp_ns'])).any() or not np.array_equal(p['timestamp_ns'][where],grid[active].asi8):raise ValueError('Missing minute-close mark')
                quantity=row.portfolio_trade_exposure_rupees/row.entry_price
                mark=(1 if row.side=='LONG' else -1)*(p['close'][where]-row.entry_price)*quantity-row.portfolio_cost_rupees
                pnl[active]+=mark
        if not np.isclose(pnl[-1],metrics(book,[day])['net_pnl_rs'],rtol=0,atol=1e-5):raise ValueError('MTM close does not reconcile')
        for time,value in zip(grid,pnl):
            equity=carry+value;peak=max(peak,equity)
            records.append(dict(day=day,ts=time,day_pnl_rs=value,cumulative_pnl_rs=equity,drawdown_rs=peak-equity))
        carry+=pnl[-1]
    return pd.DataFrame(records)


def protected_state():
    result=BASE.protected_hashes()
    for path in (current.CONFIG,current.OUT_ROOT/'latest.json',current.model.OUT_ROOT/'latest.json'):
        result[str(path)]=sha(path)
    return result


def write_report(out,summary,table,wide,audit,mtm_summary):
    show=lambda f:'<div class="scroll">'+f.to_html(index=False,float_format=lambda x:f'{x:,.2f}')+'</div>'
    html='''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G7 V4 practical improvements</title><style>body{font:16px/1.5 system-ui;margin:30px;max-width:1650px;color:#21354b}.result{background:#eef5fa;border-left:5px solid #3a7198;padding:18px}.scroll{overflow:auto}td,th{padding:8px;border:1px solid #cfd8e1;text-align:right}table{border-collapse:collapse;font-size:13px}a{color:#195e9b}</style></head><body>'''
    html+='<h1>G7 V4: simple selection and execution experiments</h1><p class="result">Five fixed hypotheses. Development selected: '+str(summary['selected_candidate'] or 'NONE')+'. Validation passed: '+str(summary['validation_qualified'])+'; assessment passed: '+str(summary['assessment_qualified'])+'; cost stresses passed: '+str(summary['cost_qualified'])+'. Current default remains saved separately.</p>'
    html+='<h2>Correct comparison</h2><p>Published G7 V4 uses theoretical fractional cash shares and minute-end entry labels for capacity. The execution control reserves capital/symbol at the entry minute open and rounds quantity down to integer cash shares. Every candidate uses those same conservative conventions. Reported native entry timestamps, prices, protection and exits are preserved. No claim is made that the conservative clock is an observed intraminute fill time.</p><p>The audit found two accepted same-symbol reentries on the exact prior intrabar exit minute: BHEL27Aug and TVSMOTOR28Aug. OHLC cannot establish their correct ordering. Controls separate this execution issue from strategy changes.</p>'
    html+='<h2>Predeclared tests</h2><ul>'+''.join('<li><b>'+k+'</b>: '+v+'</li>' for k,v in POLICIES.items())+'</ul>'
    html+='<p>CMF is volume-weighted candle location, not actual aggressor order flow. Close75 is an interaction retest of a previously tried hypothesis. Gap cancellation acts at the observed open before that minute trigger touch; it is not a stop-limit or retracement-fill simulation. The two-loss lock uses actually admitted realized after-cost losses, is recalculated under each fee stress, and leaves existing trades alone.</p>'
    cols=['policy','trades','wins','losses','net_pnl_rs','net_change_rs','net_change_vs_published_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs']
    for period in ('all','october','development','validation','assessment','known_oct26','final_oct27'):
        html+='<h2>'+period+'</h2>'+show(table.loc[table.period.eq(period),cols])
    html+='<h2>Development acceptance</h2><p>Net profit at least control, at least10% less losing-trade amount, fewer losses, no lower win rate,95% winner retention,80% trade retention, no higher daily-close drawdown,5 changed trades across3 days. One development lock; later periods cannot choose a replacement.</p>'+show(audit)
    html+='<h2>Minute-close equity</h2><p>Includes unrealized positions marked at completed minute closes; full modeled round-trip costs are recognized at the first completed entry-bar mark. This reveals intraday variation omitted by daily-close equity, but not intraminute lows, spread or market impact.</p>'+show(mtm_summary)
    html+='<h2>October daywise: every test</h2>'+show(wide.loc[wide.index.str.startswith('2026-10')].reset_index())
    html+='<h2>Evidence limits</h2><p>All 59 available dates, 29 July–27 October 2026, have been repeatedly examined. Earlier studies already tried 17 entry variants, 3 failure exits and 10 indicator variants, among other protection tests; these five additions do not create an independent holdout. Missing 24/25 August and 1 October sources are excluded. There was no later sealed session in inspected metadata.</p><p>Upstream fno_v13_replay_source._coverage checks all required OI timestamps and its build_day_features excludes the entire stock-day when any required timestamp is missing. Missing afternoon OI can therefore change morning ranking. This is missing-data selection bias, not proof of direct future-price leakage. The 603 selected opportunities cannot reconstruct omitted stocks or quota replacements; this study remains conditional on that universe. Full as-of pre-quota regeneration is still required for a live-valid universe.</p><p>Futures OI supplies signals; equity candles supply execution. Integer cash sizing does not make this a futures/option lot backtest. Fees of 5/15/30 bps and totals of 9/19/34 bps include a separately disclosed extra 4 bps execution-drag approximation (roughly 2 bps per side), not an exact spread/slippage-trigger replay. Lower loss amounts caused by removing winners or reducing size are not selection improvement.</p>'
    html+='<h2>Files</h2><ul>'+''.join(f'<li><a href="{n}">{n}</a></li>' for n in ('comparison.csv','all_period_totals.csv','october_daywise_all.csv','daywise_all.csv','monthly_totals.csv','cost_sensitivity.csv','trade_changes.csv','minute_close_equity.csv','minute_close_drawdown.csv','development_selection.csv','entry_features_and_decisions.csv','protocol.json','selection_lock.json','summary.json'))+'</ul></body></html>'
    (out/'report.html').write_text(html,encoding='utf-8')


def run():
    protected=protected_state();pointer=BASE.read(current.OUT_ROOT/'latest.json');source=Path(pointer['output'])
    verify_members(source,pointer['manifest_sha256']);current.verify(source)
    settings=BASE.read(current.CONFIG);source_summary=BASE.read(source/'summary.json')
    out=OUT_ROOT/'runs'/source_summary['last_day']/(datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')+'_five_fixed_tests')
    out.mkdir(parents=True,exist_ok=False)
    protocol=dict(version=VERSION,scope='BACKTEST_ONLY',execution_authority=False,baseline=str(source),
        baseline_manifest_sha256=sha(source/'manifest.json'),candidates=POLICIES,candidate_count=5,selection=SELECTION,
        baseline_adjustments=['entry-bar-open capacity reservation','integer cash-share quantities'],
        costs_bps=list(COSTS),execution_drag='extra4bps cost approximation, not explicit price/slippage replay',
        numerical_boundary='Preserve all sealed native entry events; validate recorded fill touches within8 binary64 epsilons times price; never normalize earlier bars or alter opens/prices',
        research_history='Previously examined same59 sessions; earlier17 entry,3 failure exit,10 indicator trials plus protection tests; no independent holdout',
        stop_rule='Exactly5 hypotheses; no threshold expansion/combination after outcomes; no October-driven replacement',
        original_exit_policy=settings['exit_policy'],independent_holdout=False,default_change=False)
    dump(out/'protocol.json',protocol);digest=sha(out/'protocol.json')
    print('FIXED PROTOCOL',out/'protocol.json',flush=True)
    cache=Path(source_summary['cache']);verify_members(cache,source_summary['cache_manifest_sha256'])
    native,_,_,paths,proof=BASE.load_cache(cache)
    original=pd.read_csv(source/'entry_features_and_decisions.csv').set_index('sid',drop=False)
    f=build_features(original,native);f.to_csv(out/'entry_features_and_decisions.csv',index=False)
    cfg=proof['base_config'];plans=make_plans(native,paths,f,cfg,settings)
    books={};training=[];periods=dict(proof['periods'],october=[d for d in proof['periods']['all'] if d.startswith('2026-10')],later_blocks=proof['periods']['validation']+proof['periods']['assessment'])
    for name,plan in plans.items():
        book=execute(plan,cfg,name)
        book['practical_protocol_sha256']=digest
        book['strategy_fingerprint']=__import__('hashlib').sha256((str(book.strategy_fingerprint.iloc[0])+digest).encode()).hexdigest()
        books[name]=book
        if name=='published_g7_v4':same_book(book,pd.read_csv(source/'portfolio_trades.csv'))
        if 'execution_control' in books:
            stats=compare(book,books['execution_control'],books['published_g7_v4'],periods['development'])
            training.append(dict(policy=name,period='development',**stats))
            print('DEVELOPMENT',name,'net',round(stats['net_pnl_rs'],2),'losses',stats['losses'],'loss amount',round(stats['negative_trade_sum_abs_rs'],2),flush=True)
    chosen,audit=choose(pd.DataFrame(training),list(POLICIES))
    dump(out/'selection_lock.json',dict(selected_candidate=chosen,uses='DEVELOPMENT_ONLY',protocol_sha256=digest,
        locked_at_utc=datetime.now(timezone.utc).isoformat(),replacement_forbidden=True))
    table=pd.DataFrame([dict(policy=name,period=p,**compare(book,books['execution_control'],books['published_g7_v4'],days)) for name,book in books.items() for p,days in periods.items()])
    valid=choose(table,[chosen],'validation')[0] if chosen else None
    assessed=choose(table,[chosen],'assessment')[0] if chosen else None
    daily,costs,deltas,months,mtm=[],[],[],[],[]
    for name,book in books.items():
        folder=out/name;folder.mkdir();book.to_csv(folder/'portfolio_trades.csv',index=False)
        for day in periods['all']:daily.append(dict(policy=name,day=day,**metrics(book,[day])))
        for month in sorted({d[:7] for d in periods['all']}):months.append(dict(policy=name,month=month,**metrics(book,[d for d in periods['all'] if d.startswith(month)])))
        deltas.append(current.model.changes.differences(book,books['execution_control']).assign(policy=name))
        for cost in COSTS:
            stress=execute(plans[name],cfg,name,cost)
            for p,days in periods.items():costs.append(dict(policy=name,period=p,cost_bps=cost,**metrics(stress,days)))
        mtm.append(minute_equity(book,paths,periods['all']).assign(policy=name))
    daily,costs=pd.DataFrame(daily),pd.DataFrame(costs)
    wide=daily.pivot(index='day',columns='policy',values='net_pnl_rs').reindex(columns=books)
    cost_ok=False
    if chosen:
        c=costs.loc[costs.period.eq('later_blocks')].set_index(['policy','cost_bps'])
        cost_ok=all(c.loc[(chosen,v)].net_pnl_rs>=c.loc[('execution_control',v)].net_pnl_rs-1e-6 for v in COSTS if v!=5)
    mtm=pd.concat(mtm,ignore_index=True)
    mtm_summary=mtm.groupby('policy',sort=False).agg(minute_close_drawdown_rs=('drawdown_rs','max'),worst_intraday_day_pnl_rs=('day_pnl_rs','min')).reset_index()
    table.to_csv(out/'comparison.csv',index=False);table.loc[table.period.eq('all')].to_csv(out/'all_period_totals.csv',index=False)
    daily.to_csv(out/'daywise_details.csv',index=False);wide.to_csv(out/'daywise_all.csv');wide.loc[periods['october']].to_csv(out/'october_daywise_all.csv')
    pd.DataFrame(months).to_csv(out/'monthly_totals.csv',index=False);costs.to_csv(out/'cost_sensitivity.csv',index=False)
    pd.concat(deltas,ignore_index=True).to_csv(out/'trade_changes.csv',index=False);pd.DataFrame(audit).to_csv(out/'development_selection.csv',index=False)
    mtm.to_csv(out/'minute_close_equity.csv',index=False);mtm_summary.to_csv(out/'minute_close_drawdown.csv',index=False)
    summary=dict(version=VERSION,baseline=str(source),baseline_manifest_sha256=sha(source/'manifest.json'),cache=str(cache),cache_manifest_sha256=sha(cache/'manifest.json'),
        current_settings=settings,protocol_sha256=digest,periods=periods,candidate_count=5,selected_candidate=chosen,
        validation_qualified=chosen is not None and valid==chosen,assessment_qualified=chosen is not None and assessed==chosen,cost_qualified=bool(cost_ok),
        retrospective_qualified=bool(chosen and valid==chosen and assessed==chosen and cost_ok),independent_holdout=False,default_changed=False,execution_authority=False,
        protected_files_unchanged=protected,universe_fixed=False)
    dump(out/'summary.json',summary);write_report(out,summary,table,wide,pd.DataFrame(audit),mtm_summary)
    if protected_state()!=protected or sha(out/'protocol.json')!=digest:raise ValueError('Protected files or protocol changed')
    code=[Path(__file__),Path(features.__file__),Path(admission.__file__),Path(current.__file__),current.CONFIG,ROOT/'research/g7_v4_selected_backtest.py',ROOT/'research/g8_exit_engine.py',ROOT/'research/g3_sl_target_simulator.py',ROOT/'tests/test_g7_v4_practical_research.py',ROOT/'tests/test_g7_v4_practical_features.py',ROOT/'tests/test_g7_v4_practical_execution.py',ROOT/'tests/test_g7_v4_practical_selection.py',ROOT/'tests/test_g7_v4_simple_selection_features.py',ROOT/'tests/test_g7_v4_conservative_allocation.py']
    BASE.seal(out,code={str(p.relative_to(ROOT)):sha(p) for p in code})
    verify(out)
    dump(OUT_ROOT/'latest.json',dict(output=str(out),manifest_sha256=sha(out/'manifest.json'),execution_authority=False))
    print(table.loc[table.period.isin(['all','october']),['policy','period','trades','wins','losses','net_pnl_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs']].to_string(index=False),flush=True)
    print('SELECTED',chosen,'OUTPUT',out,flush=True)
    return out


def verify(out):
    out=Path(out);verify_members(out);s=BASE.read(out/'summary.json')
    for filename,digest in BASE.read(out/'manifest.json')['code'].items():
        if sha(ROOT/filename)!=digest:raise ValueError('Research code changed '+filename)
    if sha(out/'protocol.json')!=s['protocol_sha256']:raise ValueError('Protocol changed')
    verify_members(s['baseline'],s['baseline_manifest_sha256']);cache=Path(s['cache']);verify_members(cache,s['cache_manifest_sha256'])
    native,_,_,paths,proof=BASE.load_cache(cache);cfg=proof['base_config']
    f=pd.read_csv(out/'entry_features_and_decisions.csv').set_index('sid',drop=False)
    plans=make_plans(native,paths,f,cfg,s['current_settings']);books={}
    for name,plan in plans.items():
        actual=execute(plan,cfg,name);saved=pd.read_csv(out/name/'portfolio_trades.csv');same_book(actual,saved);books[name]=actual
        for key in ('exit_reason','allocation_entry_ts','loss_lock_ts','daily_loss_lock_blocked'):
            if key.endswith('_ts'):
                if not pd.to_datetime(actual[key],utc=True).equals(pd.to_datetime(saved[key],utc=True)):raise ValueError('Event timing differs '+name)
            elif not actual[key].fillna('').astype(str).equals(saved[key].fillna('').astype(str)):raise ValueError('Event metadata differs '+name)
        if plan['integer']:
            done=_flags(actual.portfolio_executed,'executed');quantity=actual.loc[done,'g8_model_quantity']
            np.testing.assert_allclose(quantity,np.round(quantity),rtol=0,atol=1e-7)
    same_book(books['published_g7_v4'],pd.read_csv(Path(s['baseline'])/'portfolio_trades.csv'))
    table=pd.read_csv(out/'comparison.csv')
    for row in table.itertuples():
        stats=compare(books[row.policy],books['execution_control'],books['published_g7_v4'],s['periods'][row.period])
        for key in ('trades','wins','losses','net_pnl_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs','changed_executions'):
            if not np.isclose(stats[key],getattr(row,key),atol=1e-6,rtol=0):raise ValueError('Comparison differs')
    chosen,_=choose(table,list(POLICIES))
    if chosen!=s['selected_candidate'] or chosen!=BASE.read(out/'selection_lock.json')['selected_candidate']:raise ValueError('Selection differs')
    costs=pd.read_csv(out/'cost_sensitivity.csv').set_index(['policy','cost_bps','period'])
    for name,plan in plans.items():
        for cost in COSTS:
            stressed=execute(plan,cfg,name,cost)
            for p,days in s['periods'].items():
                stats=metrics(stressed,days)
                for key in ('trades','wins','losses','net_pnl_rs','negative_trade_sum_abs_rs','cost_rs'):
                    if not np.isclose(stats[key],costs.loc[(name,cost,p),key],rtol=0,atol=1e-6):raise ValueError('Cost/lock replay differs '+name)
    valid=choose(table,[chosen],'validation')[0] if chosen else None
    assessed=choose(table,[chosen],'assessment')[0] if chosen else None
    cost_ok=all(costs.loc[(chosen,v,'later_blocks')].net_pnl_rs>=costs.loc[('execution_control',v,'later_blocks')].net_pnl_rs-1e-6 for v in COSTS if v!=5) if chosen else False
    flags=dict(validation_qualified=chosen is not None and valid==chosen,assessment_qualified=chosen is not None and assessed==chosen,cost_qualified=bool(cost_ok),retrospective_qualified=bool(chosen and valid==chosen and assessed==chosen and cost_ok))
    for key,value in flags.items():
        if s[key]!=value:raise ValueError('Qualification differs')
    wide=pd.read_csv(out/'daywise_all.csv').set_index('day');marks=pd.read_csv(out/'minute_close_drawdown.csv').set_index('policy')
    for name,book in books.items():
        for day in s['periods']['all']:
            if not np.isclose(wide.loc[day,name],metrics(book,[day])['net_pnl_rs'],rtol=0,atol=1e-6):raise ValueError('Daily accounting differs')
        equity=minute_equity(book,paths,s['periods']['all'])
        if not np.isclose(equity.drawdown_rs.max(),marks.loc[name,'minute_close_drawdown_rs'],rtol=0,atol=1e-6):raise ValueError('Minute equity differs')
    return True


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--verify',type=Path);args=parser.parse_args()
    print('VERIFIED',verify(args.verify)) if args.verify else run()
