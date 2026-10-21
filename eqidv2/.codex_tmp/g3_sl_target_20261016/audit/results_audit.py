"""Independent accounting and conditional diagnostic checks of frozen study output."""
from pathlib import Path
import hashlib
import json
import math

import numpy as np
import pandas as pd


STUDY = Path('C:/TradingData/eqidv2/g3_sl_target_research/20261016/study_v1')
OUT = Path(__file__).resolve().parent
SEED = 20261016
SAMPLES = 20000


def read(name):
    return pd.read_csv(STUDY/name)


def boolean(values):
    return values.astype(str).str.lower().eq('true')


def check_close(actual, expected, name):
    assert np.allclose(actual, expected, rtol=0, atol=1e-6, equal_nan=True), name


def wilson(successes, count):
    z=1.959963984540054
    p=successes/count
    center=(p+z*z/(2*count))/(1+z*z/count)
    half=z*math.sqrt(p*(1-p)/count+z*z/(4*count*count))/(1+z*z/count)
    return [100*(center-half), 100*(center+half)]


def independent_fees(buy, sell):
    brokerage=sum(min(value*3/10000,20) for value in (buy,sell))
    exchange=(buy+sell)*307/10000000
    sebi=(buy+sell)/1000000
    ipft=(buy+sell)/1000000000
    return brokerage+sell*25/100000+buy*3/100000+exchange+sebi+ipft+(brokerage+exchange+sebi+ipft)*18/100


def main():
    manifest=json.loads((STUDY/'manifest.json').read_text())
    summary=json.loads((STUDY/'summary.json').read_text())
    lock=json.loads((STUDY/'selection_lock.json').read_text())
    for name, digest in manifest['files'].items():
        assert hashlib.sha256((STUDY/name).read_bytes()).hexdigest()==digest, name
    assert lock['code_hashes']==manifest['code']==summary['code_hashes']
    for name,digest in manifest['code'].items():
        assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==digest,name
    source_cache=STUDY.parent/'source'
    assert hashlib.sha256((source_cache/'source.json').read_bytes()).hexdigest()==lock['source_cache_sha256']
    cached=json.loads((source_cache/'source.json').read_text())
    for name,digest in cached['files'].items():
        assert hashlib.sha256((source_cache/name).read_bytes()).hexdigest()==digest,name
    assert hashlib.sha256((STUDY/'protocol.json').read_bytes()).hexdigest()==lock['protocol_sha256']
    source_summary=json.loads((Path(summary['source'])/'summary.json').read_text())
    days=source_summary['days']
    periods={'development35':days[:35], 'later17':days[35:], 'all':days}
    assert len(days)==52 and len(periods['later17'])==17
    assert lock['development_days']==periods['development35']
    assert lock['later_days']==periods['later17']
    baseline=read('ledgers/original_g3.csv').set_index('sid')
    metrics=read('policy_comparison.csv')
    reported_daily=read('daily_comparison.csv')
    reported_live=read('live_cost_comparison.csv')
    setup=read('setup_recommendations.csv').set_index('setup_id')
    all_ledgers={}
    daily_by_policy={}
    records=[]
    paired=[]
    live_checks=[]
    for policy in ['original_g3','bounded_current','locked_global','locked_pooled']:
        ledger=read(f'ledgers/{policy}.csv').set_index('sid')
        assert ledger.index.is_unique and set(ledger.index)==set(baseline.index), policy
        ledger=ledger.reindex(baseline.index)
        all_ledgers[policy]=ledger
        fill=boolean(ledger.filled); executed=boolean(ledger.portfolio_executed)
        assert fill.equals(boolean(baseline.filled)), policy
        assert ledger.entry_ts.fillna('').equals(baseline.entry_ts.fillna('')), policy
        check_close(ledger.entry_price,baseline.entry_price,policy+' entries')
        check_close(ledger.portfolio_net_profit_rupees,
                    ledger.portfolio_gross_profit_rupees-ledger.portfolio_cost_rupees,policy+' accounting')
        check_close(ledger.loc[~executed,'portfolio_net_profit_rupees'],0,policy+' unexecuted')
        check_close(ledger.loc[executed,'portfolio_cost_rupees'],250,policy+' modeled cost')
        sign=ledger.side.map({'LONG':1,'SHORT':-1})
        gross=sign*(ledger.exit_price/ledger.entry_price-1)*100
        check_close(gross[fill],ledger.loc[fill,'gross_return_pct'],policy+' fills')
        if policy!='original_g3':
            assert ledger.loc[fill,'initial_stop_pct'].le(1.2+1e-10).all()
            assert ledger.loc[fill,'native_target_pct'].between(.6-1e-10,2+1e-10).all()
        daily_by_policy[policy]=ledger.groupby('day').portfolio_net_profit_rupees.sum().reindex(days,fill_value=0.)
        d=reported_daily.loc[reported_daily.policy.eq(policy)].set_index('day').reindex(days)
        check_close(daily_by_policy[policy],d.net_pnl_rs,policy+' daily')
        for period,calendar in periods.items():
            part=ledger.loc[ledger.day.isin(calendar)&executed]
            native=int((ledger.day.isin(calendar)&fill).sum())
            for extra in (0,10):
                pnl=part.portfolio_net_profit_rupees-part.portfolio_trade_exposure_rupees*extra/10000
                daily=pnl.groupby(part.day).sum().reindex(calendar,fill_value=0.)
                curve=np.r_[0.,daily.cumsum()]
                calculated=dict(trades=len(part),wins=int((pnl>1e-8).sum()),losses=int((pnl<-1e-8).sum()),
                    profitable_days=int((daily>1e-8).sum()),losing_days=int((daily<-1e-8).sum()),
                    zero_days=int((daily.abs()<=1e-8).sum()),net_pnl_rs=float(pnl.sum()),
                    native_fills=native,daily_close_drawdown_rs=float((np.maximum.accumulate(curve)-curve).max()))
                expected=metrics.loc[metrics.policy.eq(policy)&metrics.period.eq(period)&metrics.extra_roundtrip_bps.eq(extra)].iloc[0]
                for name,value in calculated.items():check_close(value,expected[name],(policy,period,extra,name))
                if extra:continue
                protected=part.exit_reason.eq('PROTECTED_STOP')
                row=dict(policy=policy,period=period,**calculated,
                    day_win_wilson95_pct=wilson(calculated['profitable_days'],len(calendar)),
                    positive_net_trade_profit=float(pnl[pnl>0].sum()),
                    negative_net_trade_loss_abs=float(-pnl[pnl<0].sum()),
                    median_winning_trade_rs=float(pnl[pnl>0].median()) if (pnl>0).any() else None,
                    mean_winning_trade_rs=float(pnl[pnl>0].mean()) if (pnl>0).any() else None,
                    protected_stop_count=int(protected.sum()),
                    protected_stop_wins=int((protected & (pnl>0)).sum()),
                    protected_stop_losses=int((protected & (pnl<0)).sum()),
                    protected_stop_net_pnl_rs=float(pnl[protected].sum()),
                    gross_loss_exceeds_1p2_count=int(part.gross_return_pct.lt(-1.2-1e-8).sum()),
                    max_gross_loss_pct=float(max(0.,-part.gross_return_pct.min())) if len(part) else None,
                    max_net_loss_pct_at_modeled5bps=float(max(0.,-part.net_return_pct.min())) if len(part) else None,
                    worst_net_trade_rs=float(pnl.min()) if len(part) else None,
                    initial_stop_gap_through_count=int((part.exit_reason.eq('STOP')&boolean(part.exit_gap_through)).sum()),
                    protected_stop_gap_through_count=int((protected&boolean(part.exit_gap_through)).sum()),
                    protected_stop_negative_gross_count=int((protected&part.gross_return_pct.lt(-1e-8)).sum()),
                    exit_reason_counts=part.exit_reason.value_counts().to_dict(),
                    wins_at_gross_return_le_point3pct=int(((pnl>0)&part.gross_return_pct.le(.3+1e-8)).sum()))
                records.append(row)
        projected=read(f'live_projection_{policy}.csv').set_index('sid')
        assert set(projected.index)==set(ledger.index[executed]),policy+' projected rows'
        actual=ledger.loc[projected.index]
        quantities=np.floor(50000/actual.trigger).astype(int)
        assert quantities.gt(0).all(),policy+' zero-share projection'
        check_close(projected.quantity,quantities,policy+' quantities')
        entry=quantities*actual.entry_price;exit_value=quantities*actual.exit_price
        buy=np.where(actual.side.eq('LONG'),entry,exit_value)
        sell=np.where(actual.side.eq('LONG'),exit_value,entry)
        fees=np.array([independent_fees(b,s) for b,s in zip(buy,sell)])
        check_close(projected.fees_rs,fees,policy+' fees')
        check_close(projected.gross_pnl_rs,sell-buy,policy+' live gross')
        check_close(projected.net_after_fees_rs,sell-buy-fees,policy+' live fees net')
        check_close(projected.net_after_fees_and_5bps_slippage_rs,sell-buy-fees-entry*.0005,policy+' live stressed net')
        for period,calendar in periods.items():
            cut=projected.loc[projected.day.isin(calendar)]
            for cost in ['net_after_fees_rs','net_after_fees_and_5bps_slippage_rs']:
                pnl=cut[cost];daily=pnl.groupby(cut.day).sum().reindex(calendar,fill_value=0.)
                result=reported_live.loc[reported_live.policy.eq(policy)&reported_live.period.eq(period)&reported_live.cost_case.eq(cost)].iloc[0]
                for key,value in dict(trades=len(cut),wins=int((pnl>0).sum()),losses=int((pnl<0).sum()),
                    profitable_days=int((daily>0).sum()),losing_days=int((daily<0).sum()),net_pnl_rs=float(pnl.sum())).items():
                    check_close(value,result[key],(policy,period,cost,key))
        protected_live=projected.loc[actual.exit_reason.eq('PROTECTED_STOP')]
        live_checks.append(dict(policy=policy,max_actual_entry_notional_rs=float(entry.max()),
            entries_above_planned_50k_notional=int((entry>50000+1e-8).sum()),
            protected_stop_count=len(protected_live),
            protected_stop_live_losses_after_fees_slippage=int(protected_live.net_after_fees_and_5bps_slippage_rs.lt(0).sum()),
            protected_stop_live_median_net_after_fees_slippage=float(protected_live.net_after_fees_and_5bps_slippage_rs.median()) if len(protected_live) else None,
            worst_live_net_after_fees_slippage_rs=float(projected.net_after_fees_and_5bps_slippage_rs.min()),
            max_live_net_loss_pct_after_fees_slippage=float(max(0.,-(projected.net_after_fees_and_5bps_slippage_rs/entry*100).min())),
            all_wins_le50rs_after_fees_slippage=int(projected.net_after_fees_and_5bps_slippage_rs.between(1e-10,50).sum())))
        for period,calendar in periods.items():
            use=ledger.day.isin(calendar)
            b=baseline.loc[use];p=ledger.loc[use]
            be=boolean(b.portfolio_executed);pe=boolean(p.portfolio_executed)
            bp=b.portfolio_net_profit_rupees;pp=p.portfolio_net_profit_rupees;delta=pp-bp
            common=be&pe;admitted=~be&pe;rejected=be&~pe
            base_positive=bp[bp>0].sum();policy_positive=pp[pp>0].sum()
            loss_saved=-bp[bp<0].sum()+pp[pp<0].sum()
            positive_sacrificed=base_positive-policy_positive
            check_close(delta.sum(),loss_saved-positive_sacrificed,policy+' profit/loss decomposition')
            check_close(delta.sum(),delta[common].sum()+pp[admitted].sum()-bp[rejected].sum(),policy+' allocation decomposition')
            paired.append(dict(policy=policy,period=period,net_delta_rs=float(delta.sum()),
                reduction_in_negative_trade_loss_magnitude_rs=float(loss_saved),
                reduction_in_positive_trade_profit_rs=float(positive_sacrificed),
                common_trade_net_delta_rs=float(delta[common].sum()),
                new_admissions_net_rs=float(pp[admitted].sum()),removed_trades_baseline_net_rs=float(bp[rejected].sum()),
                newly_admitted=int(admitted.sum()),newly_rejected=int(rejected.sum()),
                baseline_winners_reduced=int((common&bp.gt(0)&delta.lt(-1e-8)).sum()),
                baseline_winner_profit_sacrificed_rs=float(-delta[common&bp.gt(0)&delta.lt(0)].sum()),
                baseline_winners_turned_nonpositive=int((common&bp.gt(0)&pp.le(0)).sum()),
                baseline_losers_improved=int((common&bp.lt(0)&delta.gt(1e-8)).sum()),
                baseline_loser_improvement_rs=float(delta[common&bp.lt(0)&delta.gt(0)].sum())))
    # Reconcile the recommendations' executed setup accounting to the two portfolio books.
    for sid,row in setup.iterrows():
        for prefix,policy,calendar in [('baseline_all','original_g3',days),('pooled_all','locked_pooled',days),
                                       ('pooled_later','locked_pooled',periods['later17'])]:
            frame=all_ledgers[policy]
            part=frame.loc[frame.setup_id.eq(sid)&frame.day.isin(calendar)&boolean(frame.portfolio_executed)]
            check_close(part.portfolio_net_profit_rupees.sum(),row[prefix+'_net_pnl_rs'],sid+' setup net')
            assert len(part)==row[prefix+'_trades'],sid+' setup count'
    # Paired calendar-day bootstrap: frozen policies, no parameter reselection in resamples.
    rng=np.random.default_rng(SEED)
    n=len(periods['later17']);indices=rng.integers(0,n,size=(SAMPLES,n))
    boot=[]
    for policy in ['bounded_current','locked_global','locked_pooled']:
        for reference in ['original_g3','bounded_current']:
            if policy==reference:continue
            delta=(daily_by_policy[policy]-daily_by_policy[reference]).reindex(periods['later17']).to_numpy()
            means=delta[indices].mean(axis=1);low,high=np.quantile(means,[.025,.975])
            boot.append(dict(policy=policy,reference=reference,sessions=n,
                observed_mean_daily_net_delta_rs=float(delta.mean()),observed_total_net_delta_rs=float(delta.sum()),
                bootstrap95_mean_daily_net_delta_rs=[float(low),float(high)],
                bootstrap95_equivalent17session_total_delta_rs=[float(low*n),float(high*n)],
                bootstrap_fraction_mean_delta_positive=float((means>0).mean()),
                days_improved=int((delta>1e-8).sum()),days_worsened=int((delta<-1e-8).sum()),days_equal=int((abs(delta)<=1e-8).sum())))
    groups=[]
    primary=all_ledgers['locked_pooled']
    for period,calendar in periods.items():
        for group in sorted(primary.group.unique()):
            use=primary.group.eq(group)&primary.day.isin(calendar)
            b=baseline.loc[use];p=primary.loc[use]
            be=boolean(b.portfolio_executed);pe=boolean(p.portfolio_executed);common=be&pe
            bp=b.portfolio_net_profit_rupees;pp=p.portfolio_net_profit_rupees
            delta=pp-bp
            groups.append(dict(group=group,period=period,candidate=lock['group_candidates'][group],
                baseline_trades=int(be.sum()),policy_trades=int(pe.sum()),
                baseline_wins=int((bp>0).sum()),baseline_losses=int((bp<0).sum()),
                policy_wins=int((pp>0).sum()),policy_losses=int((pp<0).sum()),
                baseline_net_rs=float(bp.sum()),policy_net_rs=float(pp.sum()),delta_net_rs=float(delta.sum()),
                baseline_positive_trade_profit_rs=float(bp[bp>0].sum()),policy_positive_trade_profit_rs=float(pp[pp>0].sum()),
                baseline_negative_trade_loss_abs_rs=float(-bp[bp<0].sum()),policy_negative_trade_loss_abs_rs=float(-pp[pp<0].sum()),
                original_winners_turned_loss=int((common&bp.gt(0)&pp.lt(0)).sum()),
                original_losers_turned_win=int((common&bp.lt(0)&pp.gt(0)).sum()),
                original_winners_cut=int((common&bp.gt(0)&delta.lt(-1e-8)).sum()),
                original_winner_profit_sacrificed_rs=float(-delta[common&bp.gt(0)&delta.lt(0)].sum()),
                original_loser_improvement_rs=float(delta[common&bp.lt(0)&delta.gt(0)].sum()),
                stop_exits=int((pe&p.exit_reason.isin(['STOP','TIGHTENED_STOP','PROTECTED_STOP'])).sum()),
                original_winners_now_stopped=int((common&bp.gt(0)&p.exit_reason.isin(['STOP','TIGHTENED_STOP','PROTECTED_STOP'])).sum())))
    open_checks={}
    with np.load(source_cache/'paths.npz',allow_pickle=False) as archive:
        for policy,ledger in all_ledgers.items():
            if policy=='original_g3':continue
            opened=ledger.loc[boolean(ledger.portfolio_executed)&ledger.exit_event.eq('OPEN')]
            for sid,row in opened.iterrows():
                index=int(row.exit_path_index)
                check_close(row.exit_price,float(archive[f'{int(sid)}__open'][index]),policy+' observed open')
                actual_time=pd.Timestamp(int(archive[f'{int(sid)}__timestamp_ns'][index]),tz='UTC')-pd.Timedelta(minutes=1)
                assert pd.Timestamp(row.exit_ts)==actual_time,policy+' observed open time'
            open_checks[policy]=dict(status='PASS',executed_open_exits_checked=len(opened))
    report=dict(independent_accounting_reconciliation='PASS',study=str(STUDY),
        primary_policy=summary['primary_policy'],selection_changed=False,period_metrics=records,
        saved_candidate_open_exit_path_check=open_checks,
        paired_trade_contributions=paired,live_projection_checks=live_checks,
        primary_group_contributions=groups,
        candidate_gross_loss_exceeding_1p2_trades=[dict(policy=policy,**row) for policy,ledger in all_ledgers.items()
            if policy!='original_g3' for row in ledger.loc[boolean(ledger.portfolio_executed)&ledger.gross_return_pct.lt(-1.2-1e-8),
                ['day','setup_id','tradingsymbol','side','initial_stop_pct','active_stop_pct_at_exit','gross_return_pct',
                 'net_return_pct','exit_reason','exit_event','exit_gap_through','entry_price','exit_price']].to_dict('records')],
        later17_paired_day_bootstrap=dict(seed=SEED,resamples=SAMPLES,results=boot,
            interpretation='Conditional diagnostic only: iid calendar-day resampling of already selected historical policies; no parameter refitting. Existing strategy and historical cohort were previously inspected. Intervals are not forward-performance guarantees and do not correct historical selection or serial dependence.'),
        day_win_interval_interpretation='95% Wilson binomial intervals treating session outcomes as independent, conditional on this historically inspected policy/cohort; not a guarantee of future profitable-day probability.')
    (OUT/'results_audit.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps({'reconciliation':'PASS','primary_policy':summary['primary_policy'],
        'period_metrics':[r for r in records if r['period'] in ('later17','all')],
        'paired_primary':[r for r in paired if r['policy']=='locked_pooled'],
        'bootstrap_primary':[r for r in boot if r['policy']=='locked_pooled'],
        'live_projection_checks':live_checks},indent=2))


if __name__=='__main__':main()
