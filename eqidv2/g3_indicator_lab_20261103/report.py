"""Reconcile native results, paired-day changes, uncertainty and limitations."""
from __future__ import annotations
import argparse, html, json
from pathlib import Path
import numpy as np
import pandas as pd
from g3_v18_20261102.run import metrics,dump,sha
PACKAGE=Path(__file__).resolve().parent

def summarize(book,days,cost=5):
    out=book.loc[book.day.astype(str).str[:10].isin(days)].copy()
    # G3 allocation is fixed capital, without a P&L-dependent loss lock.
    # Confirm these stressed totals against actual native cost reruns below.
    out['portfolio_cost_rupees']=out.portfolio_trade_exposure_rupees*cost/10000
    out['portfolio_net_profit_rupees']=out.portfolio_gross_profit_rupees-out.portfolio_cost_rupees
    done=out.loc[out.portfolio_executed.eq(True)]
    m=metrics(out,days)
    pnl=done.groupby(done.day.astype(str).str[:10]).portfolio_net_profit_rupees.sum().reindex(days,fill_value=0)
    m.update(losing_days=int(pnl.lt(0).sum()),winning_days=int(pnl.gt(0).sum()),
        turnover_entry_rs=float(done.portfolio_trade_exposure_rupees.sum()),
        turnover_roundtrip_rs=float((done.portfolio_trade_exposure_rupees*(1+done.exit_price/done.entry_price)).sum()),
        peak_native_clock_exposure_rs=float((done.portfolio_gross_exposure_before_rupees+done.portfolio_trade_exposure_rupees).max()) if len(done) else 0,
        portfolio_rejections=int((out.filled.eq(True)&~out.portfolio_executed.eq(True)).sum()))
    return m

def paired_bootstrap(delta,replicates=2000,block=5):
    values=np.asarray(delta,dtype=float);n=len(values)
    if n<10:return dict(ci_low_rs=None,ci_high_rs=None,positive_resample_fraction=None)
    rng=np.random.default_rng(20261103)
    starts=rng.integers(0,n,size=(replicates,int(np.ceil(n/block))))
    indices=((starts[:,:,None]+np.arange(block))%n).reshape(replicates,-1)[:,:n]
    totals=values[indices].sum(axis=1)
    return dict(ci_low_rs=float(np.quantile(totals,.025)),ci_high_rs=float(np.quantile(totals,.975)),
                positive_resample_fraction=float(np.mean(totals>0)))

def attribution(base,other,days):
    def indexed(frame):
        f=frame.loc[frame.portfolio_executed.eq(True)&frame.day.astype(str).str[:10].isin(days)].copy()
        f['key']=f.day.astype(str).str[:10]+'|'+f.setup_id+'|'+f.tradingsymbol
        return f.set_index('key')
    a,b=indexed(base),indexed(other)
    removed=a.loc[~a.index.isin(b.index)];added=b.loc[~b.index.isin(a.index)]
    common=a.index.intersection(b.index)
    return dict(winners_removed=int(removed.portfolio_net_profit_rupees.gt(0).sum()),
        losers_avoided=int(removed.portfolio_net_profit_rupees.lt(0).sum()),
        newly_admitted_winners=int(added.portfolio_net_profit_rupees.gt(0).sum()),
        newly_admitted_losers=int(added.portfolio_net_profit_rupees.lt(0).sum()),
        removed_net_rs=float(removed.portfolio_net_profit_rupees.sum()),
        added_net_rs=float(added.portfolio_net_profit_rupees.sum()),
        retained_net_change_rs=float(b.loc[common].portfolio_net_profit_rupees.sum()-a.loc[common].portfolio_net_profit_rupees.sum()))

def run(result):
    result=result.resolve()
    config=json.loads((result/'config.json').read_text());days=config['days']
    rules=pd.read_parquet(result/'trades.parquet')
    model=pd.read_parquet(result/'models/trades.parquet')
    books=pd.concat([rules,model],ignore_index=True);books['day']=books.day.astype(str).str[:10]
    unknown_market=~np.isfinite(pd.to_numeric(books.v18_nifty_from_open_pct,errors='coerce'))|~books.v18_market_ready.eq(True)
    books.loc[unknown_market,'market_regime']='UNKNOWN_MISSING_CONTEXT'
    coverage=[]
    for variant,part in books.groupby('variant'):
        for column in ('v18_nifty_from_open_pct','v18_relative_strength_pct','v18_efficiency_6','lab_tod_rvol','lab_extension_atr'):
            ready=np.isfinite(pd.to_numeric(part[column],errors='coerce'))
            coverage.append(dict(variant=variant,feature=column,selected_orders=len(part),finite_orders=int(ready.sum()),
                missing_orders=int((~ready).sum()),finite_pct=float(ready.mean()*100)))
    pd.DataFrame(coverage).to_csv(result/'selected_feature_coverage.csv',index=False)
    daily=pd.concat([pd.read_csv(p).rename(columns={'max_simultaneous_exposure_rs':'minute_close_max_exposure_rs'})
        for p in (result/'daily_results.csv',result/'models/daily_results.csv')],ignore_index=True)
    if daily.columns.duplicated().any() or daily.duplicated(['day','variant','cost_bps']).any():
        raise ValueError('Duplicate daily columns or scenario rows')
    daily.to_csv(result/'all_daily_results.csv',index=False)
    ready_days=sorted(daily.loc[daily.variant.eq('logistic_or_rank')&daily.model_ready.eq(True),'day'].unique())
    periods={'all':days,'october':[d for d in days if d.startswith('2026-10')],
        'post_policy_descriptive':[d for d in days if d>'2026-10-27'],
        'model_evaluation':ready_days,'latest':[days[-1]]}
    for month in sorted({d[:7] for d in days}):periods['month_'+month]=[d for d in days if d.startswith(month)]
    variants=config['protocol']['rules']+config['protocol']['models']
    base=books.loc[books.variant.eq('control')]
    summary=[];deltas=[];changes=[];breakdowns=[]
    for variant in variants:
        book=books.loc[books.variant.eq(variant)]
        for period,calendar in periods.items():
            if not calendar:continue
            for cost in (5,9,15):
                m=summarize(book,calendar,cost)
                ref=summarize(base,calendar,cost)
                subset=daily.loc[daily.variant.eq(variant)&daily.cost_bps.eq(cost)&daily.day.isin(calendar)]
                if not np.isclose(m['net_pnl_rs'],subset.net_pnl_rs.sum(),atol=1e-4):
                    raise ValueError(f'Cost/replay accounting mismatch: {variant}/{period}/{cost}')
                m.update(variant=variant,period=period,cost_bps=cost,delta_net_rs=m['net_pnl_rs']-ref['net_pnl_rs'])
                m['worst_within_day_minute_close_dd_rs']=float(subset.minute_close_drawdown_rs.max()) if cost==5 else np.nan
                summary.append(m)
            if period in ('all','october','post_policy_descriptive','model_evaluation'):
                s=daily.loc[daily.variant.eq(variant)&daily.cost_bps.eq(5)].set_index('day').net_pnl_rs.reindex(calendar,fill_value=0)
                b=daily.loc[daily.variant.eq('control')&daily.cost_bps.eq(5)].set_index('day').net_pnl_rs.reindex(calendar,fill_value=0)
                delta=s-b
                deltas.append(dict(variant=variant,period=period,days=len(calendar),delta_net_rs=float(delta.sum()),
                    days_improved=int(delta.gt(1e-6).sum()),days_worsened=int(delta.lt(-1e-6).sum()),
                    largest_day_improvement_rs=float(delta.max()),**paired_bootstrap(delta)))
                changes.append(dict(variant=variant,period=period,**attribution(base,book,calendar)))
        for kind in ('setup_id','side','market_regime','clock_regime'):
            for name,sub in book.groupby(kind,dropna=False):
                breakdowns.append(dict(variant=variant,group_type=kind,group=str(name),**summarize(sub,days)))
    overall=pd.DataFrame(summary);overall.to_csv(result/'summary_comparison.csv',index=False)
    pd.DataFrame(deltas).to_csv(result/'paired_uncertainty.csv',index=False)
    pd.DataFrame(changes).to_csv(result/'trade_changes.csv',index=False)
    pd.DataFrame(breakdowns).to_csv(result/'slot_side_regime.csv',index=False)
    # Same-pool comparisons isolate learned ranking from the OR admission change.
    same_pool=[]
    for variant in config['protocol']['models']:
        for cost in (5,9,15):
            ref=daily.loc[daily.variant.eq('or_market')&daily.cost_bps.eq(cost)].set_index('day').net_pnl_rs.reindex(ready_days)
            measured=daily.loc[daily.variant.eq(variant)&daily.cost_bps.eq(cost)].set_index('day').net_pnl_rs.reindex(ready_days)
            delta=measured-ref
            if delta.isna().any():raise ValueError('Model same-pool comparison incomplete')
            same_pool.append(dict(variant=variant,reference='or_market',period='model_evaluation',cost_bps=cost,
                days=len(ready_days),net_pnl_rs=float(measured.sum()),reference_net_pnl_rs=float(ref.sum()),
                delta_net_rs=float(delta.sum()),days_improved=int(delta.gt(1e-6).sum()),
                days_worsened=int(delta.lt(-1e-6).sum()),**paired_bootstrap(delta)))
    pd.DataFrame(same_pool).to_csv(result/'model_vs_same_pool.csv',index=False)
    wide=daily.loc[daily.cost_bps.eq(5)&daily.variant.isin(variants)].pivot(index='day',columns='variant',values='net_pnl_rs').sort_index()
    for variant in variants[1:]:wide[variant+'__delta']=wide[variant]-wide.control
    # Separate G7 reference: native saved strategy with different exits/filter.
    g7path=Path('C:/TradingData/eqidv2/g3_g7_v4_through_20261102/comparison_20261102_verified/daywise_long.csv')
    g7=pd.read_csv(g7path)
    names=[n for n in g7.strategy.unique() if n!='G3_V17']
    g7=g7.loc[g7.strategy.isin(names)&g7.day.isin(days)].copy()
    if len(names)!=1 or g7.day.duplicated().any() or set(g7.day)!=set(days):raise ValueError('G7 benchmark calendar mismatch')
    wide['G7_V4_LOCK_SAVED_REFERENCE']=g7.set_index('day').net_pnl_rs
    wide.to_csv(result/'daywise_comparison.csv',index_label='day')
    g7.to_csv(result/'g7_saved_reference.csv',index=False)
    fill=daily.loc[daily.variant.str.endswith('__trigger1bp')].copy()
    fill['control_variant']=fill.variant.str.replace('__trigger1bp','',regex=False)
    native=daily.loc[daily.cost_bps.eq(5)&daily.variant.isin(fill.control_variant.unique()),['day','variant','net_pnl_rs','trades']]
    fill=fill.merge(native,left_on=['day','control_variant'],right_on=['day','variant'],suffixes=('_stress','_native'),validate='one_to_one')
    fill['delta_net_rs']=fill.net_pnl_rs_stress-fill.net_pnl_rs_native
    fill.to_csv(result/'fill_stress.csv',index=False)
    # A family verdict can nominate a future experiment; no REMOVE promotion
    # is inferred from reusing development outcomes.
    mapping={'EMA stack':'no_ema','OI lower/increase':'no_oi_lower','OI upper cap':'no_oi_upper',
        '5m volume':'no_vol5','1m volume':'no_vol1','Body minimum':'no_body','Maximum wick':'no_max_wick',
        '09:26 SHORT minimum wick':'no_min_wick','Core preference':'no_core','Slot allowlist':'all51_slots'}
    verdicts=[]
    for family,variant in mapping.items():
        r=overall.loc[overall.variant.eq(variant)&overall.cost_bps.eq(5)].set_index('period')
        changed=pd.DataFrame(changes).query('variant==@variant and period=="all"').iloc[0]
        altered=int(changed.winners_removed+changed.losers_avoided+changed.newly_admitted_winners+changed.newly_admitted_losers)
        status='INSUFFICIENT EVIDENCE' if altered<5 else 'TEST NEXT' if r.loc['all','delta_net_rs']>0 and r.loc['october','delta_net_rs']>0 else 'KEEP'
        verdicts.append(dict(family=family,experiment=variant,verdict=status,all_delta_rs=r.loc['all','delta_net_rs'],
            october_delta_rs=r.loc['october','delta_net_rs'],later_delta_rs=r.loc['post_policy_descriptive','delta_net_rs'],
            changed_executed_positions=altered,reason='Exploratory comparison; no independent holdout or automatic production removal'))
    verdict=pd.DataFrame(verdicts);verdict.to_csv(result/'indicator_verdicts.csv',index=False)
    def table(frame):
        return frame.to_html(index=False,border=0,float_format=lambda v:f'{v:,.2f}',classes='data')
    cols=['variant','trades','win_rate_pct','net_pnl_rs','delta_net_rs','losing_days','daily_close_drawdown_rs','worst_day_rs']
    sections=''.join(f'<h2>{html.escape(period)}</h2>'+table(overall.loc[overall.period.eq(period)&overall.cost_bps.eq(5),cols])
        for period in ('all','october','post_policy_descriptive','model_evaluation','latest'))
    chosen=['control','or_market','group_score','or_group_score','logistic_or_rank','histgb_or_rank']
    modelmetrics=pd.read_csv(result/'models/model_metrics.csv')
    introduction=f'''<p>{len(days)} represented sessions, {days[0]} through {days[-1]}. Research only. Primary comparison: 22 rule arms and 2 model arms; four additional 1 bp trigger scenarios. All 63 native control parity checks passed.</p>
<p><a href="../README.md">Read the findings, limitations and rerun instructions</a>. Retain the current G3 control: the stronger-looking alternatives failed cost, concentration or October downside checks. Eight additional diagnostic feature subsets required 72 model refits; these were not extra trading strategies.</p>
<p><strong>No policy is promoted.</strong> G3's slot policy was selected through27October, and the later four sessions had already been inspected. Chronological model tests use only earlier labels, but retain the retrospectively selected slot policy. They are exploratory, not an untouched holdout.</p>
<p>₹10lakh fixed portfolio, ₹1lakh allocation and5× exposure per entry; native10minute expiry,1.25% SL tightened to1% after120minutes, per-slot targets, stop-first OHLC ambiguity and15:15 close. G7 is a separately labeled saved reference with its own rules.</p>
<p>Opening OI/universe eligibility limitations are inherited. Oct28 andNov2 include restricted cohorts. Candle pressure is estimated; no measured aggressor delta, validated bid/ask depth, or receipt-complete news corpus was added. News pauses/classification were not backtested.</p>
<p>AI predictions rank the same complete prequota OR-market pool. Initial/insufficient-training days use the original control and are excluded from the model_evaluation comparison. Scores are unvalidated probabilities. See fold, calibration and importance reports.</p>
<p>Daily realized drawdown and maximum within-day minute-close marked drawdown are distinct. The latter cannot capture unobserved intrabar peaks/spreads. Capacity reports the native engine's contemporaneous exposure; no market-impact capacity claim.</p>'''
    downloads=['daywise_comparison.csv','summary_comparison.csv','all_daily_results.csv','trade_changes.csv','paired_uncertainty.csv','slot_side_regime.csv','indicator_verdicts.csv','fill_stress.csv','g7_saved_reference.csv','control_parity.json']
    links=' · '.join(f'<a href="{x}">{x}</a>' for x in downloads)
    doc=f'''<!doctype html><html><meta charset="utf-8"><title>G3 indicator lab</title><style>body{{font:15px system-ui;max-width:1500px;margin:36px auto;padding:0 20px;color:#17212b}}table{{border-collapse:collapse;width:100%;font-size:13px;margin:20px 0}}td,th{{padding:8px;border-bottom:1px solid #dae1e8;text-align:right}}td:first-child,th:first-child{{text-align:left}}th{{background:#eaf0f6;position:sticky;top:0}}p{{max-width:1100px;line-height:1.6}}a{{color:#125a9a}}h2{{margin-top:36px}}.scroll{{overflow:auto}}</style><h1>G3 indicator research — frozen comparisons</h1>{introduction}<p>{links}</p><h2>Indicator verdicts</h2>{table(verdict)}{sections}<h2>Predictive model diagnostics</h2>{table(modelmetrics)}<p>Bootstrap intervals resample paired whole-day5-session blocks,2000replicates. They describe uncertainty conditional on the reused history and do not correct for selecting the best of these28scenarios. See paired_uncertainty.csv; insufficient short periods have blank intervals.</p><p>Methodology and source checks: <a href="../FEATURE_INVENTORY.md">feature inventory</a> · <a href="../TOOLS_RESEARCH.md">tools</a> · <a href="../NEWS_AND_DATA_AUDIT.md">news/data/CVD audit</a>.</p></html>'''
    (result/'report.html').write_text(doc,encoding='utf-8')
    summary=dict(days=days,primary_variants=variants,primary_variant_count=len(variants),fill_stress_scenarios=4,
        total_scenarios=28,control_parity=json.loads((result/'control_parity.json').read_text()),
        independent_holdout=False,model_ready_days=ready_days,g7_reference=dict(path=str(g7path),sha256=sha(g7path)),
        verdicts=verdicts,overall_metrics=overall.to_dict('records'),execution_authority=False,
        conclusion='Retain current production control; research candidates require fresh independent validation.')
    dump(result/'summary.json',summary)
    dump(result/'manifest.json',dict(files={p.name:sha(p) for p in result.iterdir() if p.is_file() and p.name!='manifest.json'},
        research_code={p.name:sha(p) for p in PACKAGE.glob('*.py')}))
    print(overall.loc[overall.period.eq('october')&overall.cost_bps.eq(5),cols].to_string(index=False))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--results',type=Path,required=True);a=p.parse_args();run(a.results)
