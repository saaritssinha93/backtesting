"""Stock/Nifty time-window exits on a fresh, current-rule G6 baseline.

Research output only. Run with --output-dir, then reuse with --resume to
analyse the sealed local minute paths without rereading market archives.
"""
from __future__ import annotations

import argparse
from datetime import date
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g6_g4_replica_backtest as g6
from research import g6_nifty_direction as nifty
from research.g3_sl_target_simulator import simulate_exit, MINUTE_NS
from research.g4_extended_target_sweep import read_frozen_csv

VERSION = 'G6-MOVEMENT-DURATION-EXIT-RESEARCH-V2'
DURATIONS = (5, 10, 15, 20, 30, 45, 60, 90, 120)
THRESHOLDS = {'stock': {'opposite_pct': .10, 'sideways_range_pct': .20},
              'nifty': {'opposite_pct': .03, 'sideways_range_pct': .10}}
base = g6.mirror


def dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2, default=str, allow_nan=False)+'\n', encoding='utf-8')


def prepare(output):
    config = g6.configuration()
    codefiles = [Path(__file__), Path(g6.__file__), Path(g6.bounded.__file__),
                 Path(base.__file__), Path(g6.donor.__file__), g6.DEFAULT_CONFIG,
                 base.ROOT/'research/g3_sl_target_simulator.py']
    codehash = {str(p): base.sha256(p) for p in codefiles}
    old, old_summary, current, summary, pointer = g6.bounded.checked_sources(g6.bounded.configuration(), None)
    prices = g6.Prices(summary)
    retained, identity, changes, sources, candidates = g6.bounded.extended_orders(old, old_summary, current, summary, prices)
    revised, audit, entry_changes, proof = g6.reselect_1011_short(retained, summary)
    source_orders = read_frozen_csv(current/'selected_orders.csv')
    combined = pd.concat([revised, source_orders.loc[source_orders.setup_id.isin(g6.ADDED_IDS)]], ignore_index=True)
    orders = g6.Identity(config).identify(combined)
    if orders.sid.duplicated().any() or orders.duplicated(['day','setup_id','tradingsymbol']).any():
        raise ValueError('Duplicate baseline orders')
    print(f'Preparing {len(orders)} current G6 orders over {len(summary["days"])} sessions', flush=True)
    paths = prices.paths(orders.sort_values(['tradingsymbol','day','confirmation_ts']))
    native = g6.bounded.history._simulate(orders, paths, [date.fromisoformat(d) for d in summary['days']], g6.Identity(config))
    unfiltered = g6._bounded(native, paths, config)
    direction, nifty_proof, nifty_inputs = nifty.order_audit(unfiltered, prices)
    baseline, _ = nifty.apply_filter(unfiltered, direction, config)
    for p, evidence in prices.proof.items():
        if base.sha256(Path(p)) != evidence['sha256']:
            raise ValueError('Price input changed while reading')
    for row in nifty_proof.itertuples():
        if base.sha256(Path(row.path)) != row.sha256:
            raise ValueError('Nifty input changed while reading')
    for p, digest in codehash.items():
        if base.sha256(Path(p)) != digest:
            raise ValueError('Baseline code changed during preparation')
    if base.sha256(current/'manifest.json') != pointer['manifest_sha256']:
        raise ValueError('G3 source changed')
    output.mkdir(parents=True, exist_ok=False)
    dump(output/'config.json', config)
    dump(output/'baseline_source.json', dict(version=VERSION, days=summary['days'],
        current_g3_source_run=str(current), current_g3_manifest_sha256=pointer['manifest_sha256'],
        baseline_variant=config['variant_id'], baseline_entry_filter='NIFTY_FUTURES_BOTH_COUNTER',
        price_sources=prices.proof, nifty_sources=nifty_proof.to_dict('records'), code_sha256=codehash,
        coverage_limitation='Inherited retrospective G3 source eligibility and excluded sessions/stocks apply.',
        thresholds=THRESHOLDS, durations=list(DURATIONS), development_days=35,
        loss_control_guard='Only trigger when stock gross close return <= 0.05%, the modeled net break-even.',
        opposite='Window net move opposes position by threshold, with at least 60% adverse candle bodies.',
        sideways='Window high-low range divided by window opening price is at most the range threshold.',
        execution='Completed full post-entry windows only; next stock minute open; existing stops/targets take precedence through the detection bar and on open gaps.',
        selection='Choose separately for stock and Nifty on first 35 days: highest net P&L among lower-loss candidates with <=10% baseline-winner give-up and no higher daily-close drawdown. Otherwise retain baseline.',
        research_limitation='Chronological stability check on repeatedly studied history, not an untouched holdout.',
        entry_rule_changes=entry_changes.to_dict('records')))
    for name, frame in [('baseline_unfiltered',unfiltered),('baseline',baseline),('nifty_direction_audit',direction),('entry_rule_changes',entry_changes)]:
        frame.to_csv(output/f'{name}.csv', index=False)
    pd.concat([pd.DataFrame(p).assign(sid=sid) for sid,p in paths.items()], ignore_index=True).to_parquet(output/'minute_paths.parquet', index=False)
    pd.concat([frame.assign(day=d) for d,frame in nifty_inputs.items()], ignore_index=True).to_parquet(output/'nifty_paths.parquet', index=False)
    dump(output/'input_manifest.json', {p.name:base.sha256(p) for p in output.iterdir() if p.is_file()})
    print('Baseline and execution inputs sealed', flush=True)


def trigger_index(path, entry_index, entry_price, is_long, window, duration, family, threshold, interval_minutes):
    """First completed qualifying window, returning its stock path index.

    Window candles may be stock minutes or Nifty five-minute candles. A
    candle which overlaps the entry candle is never a full observation.
    """
    if duration <= 0 or duration % interval_minutes:
        raise ValueError('Duration must be a positive multiple of input interval')
    count = duration // interval_minutes
    stamps = np.asarray(window['timestamp_ns'], dtype=np.int64)
    entry_ns = int(path['timestamp_ns'][entry_index])
    sign = 1 if is_long else -1
    op, hi, lo, close = (np.asarray(window[k], float) for k in ('open','high','low','close'))
    stock_stamps = np.asarray(path['timestamp_ns'], dtype=np.int64)
    for end in range(count-1, len(stamps)):
        start = end-count+1
        if stamps[start]-interval_minutes*MINUTE_NS < entry_ns:
            continue
        if stamps[end]-stamps[start] != (count-1)*interval_minutes*MINUTE_NS:
            raise ValueError('Noncontiguous movement window')
        ix = int(np.searchsorted(stock_stamps, stamps[end]))
        if ix >= len(stock_stamps) or stock_stamps[ix] != stamps[end]:
            continue
        if ix <= entry_index or ix+1 >= len(stock_stamps):
            continue
        stock_return = sign*(float(path['close'][ix])/entry_price-1)*100
        if stock_return > .05+1e-10:
            continue
        if family == 'opposite':
            move = sign*(close[end]/op[start]-1)*100
            share = np.mean(sign*(close[start:end+1]-op[start:end+1]) < 0)
            qualified = move <= -threshold+1e-10 and share >= .6-1e-10
        elif family == 'sideways':
            width = (hi[start:end+1].max()-lo[start:end+1].min())/op[start]*100
            qualified = width <= threshold+1e-10
        else:
            raise ValueError('Unknown movement family')
        if qualified:
            return ix
    return None


def early_result(row, path, detection_index, reason):
    """Apply a close decision at next open without borrowing its later range."""
    if detection_index is None:
        return None
    next_index = detection_index+1
    native_exit = int(row['exit_path_index'])
    if next_index > native_exit or (next_index == native_exit and row.get('exit_event') == 'OPEN'):
        return None
    entry_index = int(row['entry_path_index'])
    entry, sign = float(row['entry_price']), 1 if row['side']=='LONG' else -1
    entry_ns = int(path['timestamp_ns'][entry_index])
    exit_ns = int(path['timestamp_ns'][next_index])-MINUTE_NS
    if exit_ns <= entry_ns:
        return None
    active_stop = 1. if exit_ns-entry_ns >= 120*MINUTE_NS else 1.2
    price = float(path['open'][next_index])
    ret = sign*(price/entry-1)*100
    # Resting native stop/target orders already exist at this open.
    if ret <= -active_stop or ret >= float(row['target_pct']):
        return None
    end = pd.Timestamp(int(path['timestamp_ns'][next_index]), tz='UTC').tz_convert('Asia/Kolkata')
    stamp = end-pd.Timedelta(minutes=1)
    high = np.asarray(path['high' if sign==1 else 'low'])[entry_index:next_index]
    low = np.asarray(path['low' if sign==1 else 'high'])[entry_index:next_index]
    return dict(exit_path_index=next_index, exit_price=price, exit_reason=reason, exit_event='OPEN',
        exit_ts=stamp, exit_execution_ts=str(stamp), exit_bar_end_ts=str(end),
        gross_return_pct=ret, net_return_pct=ret-.05, cost_pct=.05,
        holding_minutes=(exit_ns-entry_ns)/MINUTE_NS, stop_hit=False, target_hit=False,
        first_target_hit=False, runner_target_hit=False, same_bar_ambiguous=False,
        exit_gap_through=False, exit_gap_bps=0., active_stop_pct_at_exit=active_stop,
        active_stop_price=entry*(1-sign*active_stop/100),
        mfe_pct=max(0.,ret,float(np.max(sign*(high/entry-1)*100))),
        mae_pct=min(0.,ret,float(np.min(sign*(low/entry-1)*100))),
        early_detection_ts=str(stamp), early_exit=True)


def allocate(book, audit, config):
    result, _ = nifty.apply_filter(book, audit, config)
    return result


def stats(book, days, baseline):
    result = g6.bounded.stats(book, days)
    trades = book.loc[book.day.isin(days)&book.portfolio_executed.eq(True)]
    pnl = trades.portfolio_net_profit_rupees
    winners = float(pnl.clip(lower=0).sum())
    losses = float(-pnl.clip(upper=0).sum())
    result['profit_factor'] = winners/losses if losses else None
    result['positive_trade_sum_rs'] = winners
    result['early_exits'] = int(trades.early_exit.eq(True).sum())
    result['mean_holding_minutes'] = float(trades.holding_minutes.mean()) if len(trades) else 0.
    reference = baseline.loc[baseline.day.isin(days)&baseline.portfolio_executed.eq(True)].set_index('sid')
    paired = book.set_index('sid').reindex(reference.index)
    before = reference.portfolio_net_profit_rupees
    after = paired.portfolio_net_profit_rupees.fillna(0.)
    winners_mask, losers_mask = before.gt(0), before.lt(0)
    sacrificed = float((before[winners_mask]-after[winners_mask]).clip(lower=0).sum())
    result.update(baseline_winner_profit_given_up_rs=sacrificed,
        baseline_winner_profit_given_up_pct=100*sacrificed/float(before[winners_mask].sum()) if winners_mask.any() else 0.,
        baseline_loser_savings_rs=float((after[losers_mask]-before[losers_mask]).sum()),
        winners_turned_losses=int((winners_mask&after.lt(0)).sum()),
        baseline_losers_improved=int((losers_mask&after.gt(before+1e-8)).sum()),
        baseline_losers_worsened=int((losers_mask&after.lt(before-1e-8)).sum()),
        newly_executed_orders=int((book.day.isin(days)&book.portfolio_executed.eq(True)&~book.sid.isin(reference.index)).sum()))
    return result


def choose(table, source):
    control = table.loc[table.policy.eq('baseline')].iloc[0]
    eligible = table.loc[table.source.eq(source)&table.negative_trade_sum_abs_rs.lt(control.negative_trade_sum_abs_rs)
        &table.net_pnl_rs.ge(control.net_pnl_rs)&table.baseline_winner_profit_given_up_pct.le(10.)
        &table.daily_close_drawdown_rs.le(control.daily_close_drawdown_rs+1e-6)]
    if eligible.empty:
        return 'baseline'
    return eligible.sort_values(['net_pnl_rs','early_exits','policy'], ascending=[False,True,True]).iloc[0].policy


def loss_tradeoff(table, source):
    """Show the loss-reduction cost even if no candidate improves net profit."""
    control = table.loc[table.policy.eq('baseline')].iloc[0]
    eligible = table.loc[table.source.eq(source)
        &table.negative_trade_sum_abs_rs.lt(control.negative_trade_sum_abs_rs)
        &table.baseline_winner_profit_given_up_pct.le(10.)]
    if eligible.empty:
        return 'baseline'
    return eligible.sort_values(['negative_trade_sum_abs_rs','net_pnl_rs'],ascending=[True,False]).iloc[0].policy


def analyze(output):
    for name,digest in base.read(output/'input_manifest.json').items():
        if base.sha256(output/name) != digest:
            raise ValueError('Sealed research input changed: '+name)
    evidence, config = base.read(output/'baseline_source.json'), base.read(output/'config.json')
    days = evidence['days']
    original = read_frozen_csv(output/'baseline_unfiltered.csv')
    saved = read_frozen_csv(output/'baseline.csv')
    audit = pd.read_csv(output/'nifty_direction_audit.csv')
    paths = {int(sid):{k:p[k].to_numpy() for k in ('timestamp_ns','open','high','low','close')}
             for sid,p in pd.read_parquet(output/'minute_paths.parquet').groupby('sid')}
    indices = {}
    for day,frame in pd.read_parquet(output/'nifty_paths.parquet').groupby('day'):
        ts = pd.to_datetime(frame['ts'] if 'ts' in frame else frame['timestamp'], utc=True).astype('int64').to_numpy()
        indices[str(day)] = {'timestamp_ns':ts, **{k:frame[k].to_numpy(float) for k in ('open','high','low','close')}}
    original['early_exit'] = False
    baseline = allocate(original,audit,config)
    for col in ('portfolio_net_profit_rupees','portfolio_executed','entry_price','exit_price'):
        pd.testing.assert_series_equal(baseline[col],saved[col],check_names=False,check_dtype=False)
    # Verify current-minute native replay exactly before introducing exits.
    for row in original.loc[original.filled.eq(True)].to_dict('records'):
        result = simulate_exit(paths[int(row['sid'])],int(row['entry_path_index']),float(row['entry_price']),row['side']=='LONG',1.2,float(row['target_pct']),tighten_after_minutes=120,tightened_stop_pct=1.)
        if result['exit_path_index'] != int(row['exit_path_index']) or result['exit_reason'] != row['exit_reason'] or not np.isclose(result['exit_price'],row['exit_price'],atol=1e-8,rtol=0):
            raise ValueError('Native exit parity failed')
    print('Baseline native exits and portfolio allocation reproduced exactly', flush=True)
    trigger_cache = {}
    records = original.to_dict('records')
    allowed = audit.set_index('sid').both_counter_allowed
    for n,row in enumerate(records):
        sid = int(row['sid'])
        if not row['filled'] or not bool(allowed.loc[sid]):
            continue
        path = paths[sid]
        for source in THRESHOLDS:
            window = path if source=='stock' else indices[row['day']]
            interval = 1 if source=='stock' else 5
            for family in ('opposite','sideways'):
                for duration in DURATIONS:
                    threshold = THRESHOLDS[source]['opposite_pct' if family=='opposite' else 'sideways_range_pct']
                    trigger_cache[sid,source,family,duration] = trigger_index(path,int(row['entry_path_index']),float(row['entry_price']),row['side']=='LONG',window,duration,family,threshold,interval)
        if n%100 == 0:
            print(f'Movement windows: {n}/{len(records)}',flush=True)
    policies = [dict(policy='baseline',source='none',opposite_minutes=None,sideways_minutes=None)]
    for source in THRESHOLDS:
        for adverse in (None,*DURATIONS):
            for sideways in (None,*DURATIONS):
                if adverse is None and sideways is None:
                    continue
                policies.append(dict(policy=f'{source}_op{adverse or 0}_side{sideways or 0}',source=source,
                    opposite_minutes=adverse,sideways_minutes=sideways))
    periods = {'all':days,'development':days[:35],'later_check':days[35:],'october':[d for d in days if d>='2026-10-01'],'latest':days[-1:]}
    books, results = {'baseline':baseline}, []
    for policy in policies:
        name = policy['policy']
        if name!='baseline':
            changed = []
            for row in records:
                row = row.copy()
                sid = int(row['sid'])
                hits=[]
                for family,field in (('opposite','opposite_minutes'),('sideways','sideways_minutes')):
                    minutes=policy[field]
                    ix=trigger_cache.get((sid,policy['source'],family,minutes)) if minutes else None
                    if ix is not None:
                        hits.append((ix,family))
                if hits:
                    ix,family=min(hits)
                    result=early_result(row,paths[sid],ix,f'EARLY_{policy["source"].upper()}_{family.upper()}')
                    if result:
                        row.update(result)
                changed.append(row)
            books[name]=allocate(pd.DataFrame(changed),audit,config)
        for period,cohort in periods.items():
            results.append(dict(**policy,period=period,**stats(books[name],cohort,baseline)))
    table=pd.DataFrame(results)
    for period in periods:
        ix=table.period.eq(period)
        control=table.loc[ix&table.policy.eq('baseline')].iloc[0]
        for field in ('net_pnl_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs','wins','losses','trades'):
            table.loc[ix,'delta_'+field]=table.loc[ix,field]-control[field]
    chosen={s:choose(table.loc[table.period.eq('development')],s) for s in THRESHOLDS}
    hindsight={s:choose(table.loc[table.period.eq('all')],s) for s in THRESHOLDS}
    tradeoff={s:loss_tradeoff(table.loc[table.period.eq('development')],s) for s in THRESHOLDS}
    hindsight_tradeoff={s:loss_tradeoff(table.loc[table.period.eq('all')],s) for s in THRESHOLDS}
    best_net={s:table.loc[table.period.eq('all')&table.source.eq(s)&table.early_exits.gt(0)].sort_values(
        ['net_pnl_rs','early_exits'],ascending=[False,True]).iloc[0].policy for s in THRESHOLDS}
    best_single={}
    for source in THRESHOLDS:
        for family,other in (('opposite','sideways_minutes'),('sideways','opposite_minutes')):
            candidates=table.loc[table.period.eq('all')&table.source.eq(source)&table[other].isna()&table.early_exits.gt(0)]
            best_single[source+'_'+family]=candidates.sort_values(['net_pnl_rs','early_exits'],ascending=[False,True]).iloc[0].policy
    selected=set(chosen.values())|set(hindsight.values())|set(tradeoff.values())|set(hindsight_tradeoff.values())|set(best_net.values())|set(best_single.values())|{'baseline'}
    table.to_csv(output/'all_duration_comparisons.csv',index=False)
    table.loc[table.policy.isin(selected)].to_csv(output/'selected_comparison.csv',index=False)
    shortlist={p['policy']:p for p in policies if p['policy'] in selected}
    daily=[]; trade_changes=[]; stress=[]; sides=[]
    for name in selected:
        book=books[name]
        book.to_csv(output/f'{name}_trades.csv',index=False)
        for d in days:
            daily.append(dict(policy=name,day=d,**stats(book,[d],baseline)))
        for side in ('LONG','SHORT'):
            sides.append(dict(policy=name,side=side,**stats(book.loc[book.side.eq(side)],days,baseline.loc[baseline.side.eq(side)])))
        for penalty in (0,2,5):
            work=book.copy()
            # Only new market exits incur this extra adverse-fill stress.
            work['net_return_pct']=work.gross_return_pct-.05-work.early_exit.astype(float)*penalty/100
            work['cost_pct']=.05+work.early_exit.astype(float)*penalty/100
            work['filled']=original.filled.to_numpy()
            stressed=allocate(work,audit,config)
            stress.append(dict(policy=name,additional_early_exit_bps=penalty,**stats(stressed,days,baseline)))
        b=baseline.set_index('sid'); a=book.set_index('sid')
        for sid in a.index:
            if a.loc[sid,'early_exit'] or a.loc[sid,'portfolio_executed'] != b.loc[sid,'portfolio_executed']:
                trade_changes.append(dict(policy=name,sid=int(sid),day=a.loc[sid,'day'],symbol=a.loc[sid,'tradingsymbol'],side=a.loc[sid,'side'],setup_id=a.loc[sid,'setup_id'],
                    baseline_executed=bool(b.loc[sid,'portfolio_executed']),new_executed=bool(a.loc[sid,'portfolio_executed']),
                    baseline_exit=str(b.loc[sid,'exit_ts']),new_exit=str(a.loc[sid,'exit_ts']),exit_reason=a.loc[sid,'exit_reason'],
                    baseline_net_rs=float(b.loc[sid,'portfolio_net_profit_rupees']),new_net_rs=float(a.loc[sid,'portfolio_net_profit_rupees']),
                    delta_net_rs=float(a.loc[sid,'portfolio_net_profit_rupees']-b.loc[sid,'portfolio_net_profit_rupees'])))
    for name,frame in [('daily_comparison',pd.DataFrame(daily)),('trade_changes',pd.DataFrame(trade_changes)),('slippage_stress',pd.DataFrame(stress)),('side_comparison',pd.DataFrame(sides))]:
        frame.to_csv(output/f'{name}.csv',index=False)
    # A paired day bootstrap reflects sampling stability, not a new validation set.
    rng=np.random.default_rng(621)
    bootstrap={}
    for source,name in {**chosen,**{k+'_loss_tradeoff':v for k,v in tradeoff.items()}}.items():
        daybook=pd.DataFrame(daily)
        ref=daybook.loc[daybook.policy.eq('baseline')].set_index('day').net_pnl_rs
        alt=daybook.loc[daybook.policy.eq(name)].set_index('day').net_pnl_rs
        delta=(alt-ref).reindex(days[35:]).to_numpy()
        sampled=rng.choice(delta,size=(10000,len(delta)),replace=True).sum(axis=1)
        bootstrap[source]=dict(policy=name,later_net_delta_rs=float(delta.sum()),
            paired_day_bootstrap_95pct_low_rs=float(np.percentile(sampled,2.5)),
            paired_day_bootstrap_95pct_high_rs=float(np.percentile(sampled,97.5)))
    summary=dict(version=VERSION,baseline_variant=config['variant_id'],baseline_entry_filter='NIFTY_FUTURES_BOTH_COUNTER',
        first_date=days[0],last_date=days[-1],sessions=len(days),development_first=days[0],development_last=days[34],later_first=days[35],
        tested_exit_variants=len(policies)-1,development_selected=chosen,full_sample_hindsight_selected=hindsight,
        development_loss_tradeoff=tradeoff,full_sample_loss_tradeoff=hindsight_tradeoff,full_sample_best_net_variant=best_net,
        best_active_single_condition=best_single,
        durations=list(DURATIONS),analysis_code_sha256=base.sha256(Path(__file__)),
        selected_rules=shortlist,thresholds=THRESHOLDS,loss_control_guard='Stock gross return at detection <= 0.05%',
        selected_comparison=table.loc[table.policy.isin(selected)].replace({np.nan:None}).to_dict('records'),
        later_paired_day_bootstrap=bootstrap,execution_authority=False,scope='RESEARCH_ONLY',
        limitations=[evidence['research_limitation'],evidence['coverage_limitation'],
            'One-minute modeled exits; prices do not establish executable spreads or latency. Extra early-exit slippage is separately stressed.',
            'Opposite movement means threshold net adverse move and >=60% adverse candle bodies within a completed window; sideways means a bounded high-low range.',
            'Newly released capital is reallocated using the existing G6 portfolio constraints. Same-entry winner/loser changes are reported separately.'])
    dump(output/'summary.json',summary)
    render(output,summary,table,pd.DataFrame(stress),pd.DataFrame(trade_changes))
    dump(output/'result_manifest.json',dict(version=VERSION,files={p.name:base.sha256(p) for p in output.iterdir() if p.is_file() and p.name!='result_manifest.json'}))
    print(json.dumps({k:summary[k] for k in ('sessions','development_selected','full_sample_hindsight_selected','development_loss_tradeoff','full_sample_loss_tradeoff','full_sample_best_net_variant','later_paired_day_bootstrap')},indent=2),flush=True)


def render_chart(output,table):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    full=table.loc[table.period.eq('all')]
    reference=float(full.loc[full.policy.eq('baseline'),'net_pnl_rs'].iloc[0])/100000
    fig,axes=plt.subplots(1,2,figsize=(14,5.5),sharey=True)
    fig.patch.set_facecolor('#f5f7fa')
    for ax,source in zip(axes,('stock','nifty')):
        ax.set_facecolor('white')
        for family,other,color in (('opposite','sideways_minutes','#c13f45'),('sideways','opposite_minutes','#2169ac')):
            part=full.loc[full.source.eq(source)&full[other].isna()].sort_values(family+'_minutes')
            ax.plot(part[family+'_minutes'],part.net_pnl_rs/100000,label=family.title()+' only',color=color,marker='o',linewidth=2)
        ax.axhline(reference,color='#2e6d4d',linestyle='--',linewidth=1.8,label=f'Current G6: {reference:.2f} lakh')
        ax.set_title('Traded stock' if source=='stock' else 'Nifty futures',fontweight='bold',fontsize=14)
        ax.set_xlabel('Completed movement window (minutes)')
        ax.set_xticks([5,15,30,45,60,90,120])
        ax.grid(axis='y',color='#dfe4eb',linewidth=.7)
        ax.spines[['top','right']].set_visible(False)
        ax.set_ylim(min(0,float(full.net_pnl_rs.min())/100000),max(reference,float(full.net_pnl_rs.max())/100000)*1.08)
        ax.legend(loc='lower right',fontsize=9,frameon=False)
        inactive=full.loc[full.source.eq(source)&full.opposite_minutes.isna()&full.early_exits.eq(0),'sideways_minutes'].dropna()
        if len(inactive):
            label='/'.join(str(int(x)) for x in inactive)+'-min sideways rules had no exits'
            ax.annotate(label,(float(inactive.mean()),reference),xytext=(58,reference*.80),
                arrowprops={'arrowstyle':'->','color':'#64748b'},fontsize=9,color='#46566b')
    axes[0].set_ylabel('Net profit (INR lakh)')
    fig.suptitle('G6: net profit across movement-exit durations',fontsize=17,fontweight='bold',x=.04,ha='left')
    fig.text(.04,.025,'5bps round-trip cost | Loss-control exits only at/below net break-even | Next-minute open fills',fontsize=9,color='#46566b')
    fig.tight_layout(rect=(.015,.065,.995,.9))
    fig.savefig(output/'duration_comparison.png',dpi=160,facecolor=fig.get_facecolor())
    plt.close(fig)


def render(output,summary,table,stress,changes):
    render_chart(output,table)
    chosen=set(summary['selected_rules'])
    fields=['policy','period','trades','wins','losses','win_rate_pct','net_pnl_rs','delta_net_pnl_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs','early_exits','baseline_winner_profit_given_up_pct']
    def tab(df):
        df=df.copy()
        if 'policy' in df:
            def label(value):
                if value=='baseline':
                    return 'Current G6'
                source,op,side=value.split('_')
                rules=[]
                if int(op[2:]): rules.append('opposite '+op[2:]+' min')
                if int(side[4:]): rules.append('sideways '+side[4:]+' min')
                return source.title()+': '+' / '.join(rules)
            df['policy']=df.policy.map(label)
        df=df.rename(columns={'policy':'Rule','period':'Period','trades':'Trades','wins':'Wins','losses':'Losses',
            'win_rate_pct':'Win %','net_pnl_rs':'Net profit (INR)','delta_net_pnl_rs':'Profit change (INR)',
            'negative_trade_sum_abs_rs':'Sum of losses (INR)','daily_close_drawdown_rs':'Daily-close drawdown (INR)',
            'early_exits':'Early exits','baseline_winner_profit_given_up_pct':'Original winner profit given up %',
            'additional_early_exit_bps':'Extra exit cost (bps)'})
        return '<div class="scroll">'+df.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div>'
    headline=table.loc[table.period.eq('all')&table.policy.isin({'baseline',*summary['best_active_single_condition'].values()})]
    improvement_count=int((table.period.eq('all')&table.delta_net_pnl_rs.gt(1e-6)&table.early_exits.gt(0)).sum())
    conclusion=('<strong>Result: retain the current exits.</strong> None of the '+str(summary['tested_exit_variants'])+
        ' tested variants improved full-period net profit. Some reduce the sum of losses while sacrificing more profit from recovering winners.'
        if improvement_count==0 else f'{improvement_count} variants improved full-period net profit; assess the later-period check before adopting a rule.')
    single=table.loc[table.period.eq('all')&((table.opposite_minutes.isna())|(table.sideways_minutes.isna()))]
    parts=['<!doctype html><html><head><meta charset="utf-8"><title>G6 duration exit comparison</title><style>body{font:15px/1.55 system-ui;max-width:1600px;margin:auto;padding:30px;background:#f5f7fa;color:#18273a}h1,h2{color:#133d69}.scroll{overflow:auto}table{border-collapse:collapse;font-size:13px}th,td{border-bottom:1px solid #ccd6e3;padding:8px;text-align:right;white-space:nowrap}th{background:#e2ebf5}.note{padding:18px;background:#e8eff7}a{color:#1362a8}</style></head><body>',
        '<h1>G6: exit after opposite or sideways movement</h1>',
        f'<p>{summary["first_date"]} to {summary["last_date"]} · {summary["sessions"]} sessions · current G6 V2 entries with the Nifty-futures entry filter.</p>',
        '<p class="note">'+conclusion+' Settings chosen on earlier dates are checked separately on later dates. Zero-trigger variants are inactive controls, not improved exit rules.</p>',
        '<h2>Best active duration for each individual condition</h2>'+tab(headline[fields]),
        '<p class="note">This is a research comparison. Each rule uses completed windows entirely after entry and executes at the next stock minute open. A new exit triggers only while the stock position is at or below modeled net break-even. Original stop, target and 15:15 square-off remain available. INR 100,000 margin × 5 exposure per trade, INR 1,000,000 portfolio capital, 5bps round-trip cost.</p>',
        '<h2>Rules and duration selection</h2><p>Opposite: window net movement against the trade direction of at least 0.10% in the stock or 0.03% in Nifty, with at least 60% adverse candle bodies. Sideways: window high-low range at most 0.20% in the stock or 0.10% in Nifty. Stock windows use one-minute bars; Nifty uses completed five-minute futures bars. Durations: 5, 10, 15, 20, 30, 45, 60, 90 and 120 minutes. Opposite-only, sideways-only and all pairs are compared: 198 variants. The 90/120-minute extension followed the initial 5–60 minute comparison because its longest windows caused the least damage.</p>',
        f'<p>Choose on {summary["development_first"]}–{summary["development_last"]}; check the choice on {summary["later_first"]}–{summary["last_date"]}. Selection maximizes net profit among variants with lower summed losses, no higher daily-close drawdown, and at most 10% original-winner profit given up. If none qualify, retain baseline.</p>',
        '<pre>'+html.escape(json.dumps({k:summary[k] for k in ('development_selected','full_sample_hindsight_selected','development_loss_tradeoff','full_sample_loss_tradeoff','full_sample_best_net_variant')},indent=2))+'</pre>',
        '<h2>Duration and profit</h2><img src="duration_comparison.png" alt="Net profit by duration and instrument" style="max-width:100%">',
        '<h2>Baseline and selected rules</h2>'+tab(table.loc[table.policy.isin(chosen),fields]),
        '<h2>Single-condition duration comparison</h2>'+tab(single[fields]),
        '<h2>Extra slippage on early exits</h2>'+tab(stress[['policy','additional_early_exit_bps','net_pnl_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs']]),
        '<h2>Largest trade changes</h2>'+tab(changes.sort_values('delta_net_rs').head(20) if len(changes) else changes),
        '<h2>Limitations</h2><ul>'+''.join('<li>'+html.escape(x)+'</li>' for x in summary['limitations'])+'</ul>',
        '<p><a href="all_duration_comparisons.csv">Every duration and period</a> · <a href="trade_changes.csv">Trade changes</a> · <a href="daily_comparison.csv">Daily comparison</a> · <a href="summary.json">Summary and evidence</a></p></body></html>']
    (output/'report.html').write_text(''.join(parts),encoding='utf-8')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',required=True,type=Path)
    parser.add_argument('--resume',action='store_true')
    args=parser.parse_args()
    output=args.output_dir.resolve()
    if not output.is_relative_to((g6.G6_ROOT/'research').resolve()):
        parser.error('Output must be inside G6 research')
    if not args.resume:
        prepare(output)
    analyze(output)


if __name__=='__main__':
    main()
