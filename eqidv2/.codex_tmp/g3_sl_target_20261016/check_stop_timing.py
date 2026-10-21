"""Post-hoc single-control diagnostic; does not change the frozen study."""
from pathlib import Path
import sys,json
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
import pandas as pd
from research import g3_sl_target_research as study

def main():
    out=study.OUTPUT/'followup_stop_timing'
    if out.exists():raise ValueError('Preserve completed diagnostic')
    out.mkdir()
    candidate=dict(candidate_id='bounded_targets_fixed_stop',family='bounded_current',stop_pct=1.2,target_pct=None)
    protocol=dict(scope='RESEARCH_ONLY',execution_authority=False,
        status='POST_HOC_EXPLORATORY',reason='Main study showed narrow stops harming winners; isolate whether removing existing120minute tightening tolerates pullbacks.',
        main_study_results_already_seen=True,untouched_holdout=False,candidate=candidate,
        unchanged='Frozen entries, target=clip(current_setup_target,0.60,2.00),15:15squareoff,portfolioallocation,costmodels',
        comparison='Existing bounded_current differs only by1.20→1.00after120minutes',
        created_at_ist=pd.Timestamp.now(tz='Asia/Kolkata').isoformat())
    study.dump(out/'protocol.json',protocol)
    base,paths,meta=study.load_source();days=meta['summary']['days']
    new=study.allocate(study.raw_candidate(base,paths,candidate))
    new.to_csv(out/'portfolio_trades.csv',index=False)
    ledgers={'original_g3':base,'bounded_current':study.read_frozen_csv(study.OUTPUT/'ledgers/bounded_current.csv'),
        candidate['candidate_id']:new}
    metrics=[];daily=[];live=[];setups=[];transitions=[]
    for name,ledger in ledgers.items():
        for period,cohort in [('development35',days[:35]),('later17',days[35:]),('all',days)]:
            for extra in (0,10):metrics.append(dict(policy=name,period=period,extra_roundtrip_bps=extra,**study.stats(ledger,cohort,extra)))
            _,ls=study.live_projection(ledger.loc[ledger.day.isin(cohort)],cohort)
            live.extend(dict(policy=name,period=period,**item) for item in ls)
        for day in days:daily.append(dict(policy=name,day=day,**study.stats(ledger,[day])))
        for spec in study.history.contract.ACTIVE_SETUPS:
            subset=ledger.loc[ledger.setup_id.eq(spec.setup_id)]
            for period,cohort in [('all',days),('later17',days[35:])]:
                setups.append(dict(policy=name,setup_id=spec.setup_id,signal_time=spec.signal_end,
                    confirmation_time=spec.confirmation_end,side=spec.side,period=period,**study.stats(subset,cohort)))
    merged=ledgers['bounded_current'].merge(new,on='sid',suffixes=('_timed','_fixed'))
    for row in merged.loc[merged.filled_timed.eq(True)&merged.exit_reason_timed.eq('TIGHTENED_STOP')].itertuples(index=False):
        transitions.append(dict(sid=row.sid,day=row.day_timed,setup_id=row.setup_id_timed,tradingsymbol=row.tradingsymbol_timed,
            timed_executed=row.portfolio_executed_timed,fixed_executed=row.portfolio_executed_fixed,
            timed_reason=row.exit_reason_timed,fixed_reason=row.exit_reason_fixed,
            timed_gross_pct=row.gross_return_pct_timed,fixed_gross_pct=row.gross_return_pct_fixed,
            timed_net_rs=row.portfolio_net_profit_rupees_timed,fixed_net_rs=row.portfolio_net_profit_rupees_fixed,
            net_change_rs=row.portfolio_net_profit_rupees_fixed-row.portfolio_net_profit_rupees_timed))
    for name,rows in [('policy_comparison',metrics),('daily_comparison',daily),('live_cost_comparison',live),
            ('setup_comparison',setups),('timed_stop_transitions',transitions)]:
        pd.DataFrame(rows).to_csv(out/f'{name}.csv',index=False)
    summary=dict(protocol=protocol,source_cache_sha256=study.history.sha256(study.SOURCE/'source.json'),
        all_metrics=[r for r in metrics if r['period']=='all' and r['extra_roundtrip_bps']==0],
        later_metrics=[r for r in metrics if r['period']=='later17' and r['extra_roundtrip_bps']==0],
        live=live,timed_stop_count=len(transitions),
        timed_stops_to_positive_gross=sum(r['fixed_gross_pct']>0 for r in transitions),
        net_change_on_timed_stop_rows=sum(r['net_change_rs'] for r in transitions),
        production_changed=False)
    study.dump(out/'summary.json',summary)
    study.dump(out/'manifest.json',dict(files={p.name:study.history.sha256(p) for p in out.iterdir() if p.is_file()},
        code={str(Path(__file__).resolve()):study.history.sha256(Path(__file__))}))
    print(pd.DataFrame(summary['all_metrics']).to_string(index=False))
    print(pd.DataFrame(summary['later_metrics']).to_string(index=False))
    print(out)

if __name__=='__main__':main()
