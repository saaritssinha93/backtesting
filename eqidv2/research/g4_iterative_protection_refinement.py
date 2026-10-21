"""Second declared G4 research iteration: resting protection and partial exits.

Keeps the failed first iteration intact. New mechanics are declared before this
iteration evaluates them; the earlier history is explicitly not untouched.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import secrets
import shutil

import numpy as np
import pandas as pd

from research import g4_iterative_exit_backtest as core
from research import g4_iterative_protective_stops as stops
from research import g4_iterative_partial_exits as partials

base, rules, adaptive = core.base, core.rules, core.adaptive
FIRST_RUN = base.G4_ROOT / 'research/20261016T161732_e29774_iterative_exits_v1'
VERSION = 'G4-ITERATIVE-PROTECTION-REFINEMENT-V2'
FRACTIONS = (.25,.5,.75)


def catalog():
    initial = rules.policy_catalog()
    extra = [dict(item,kind='resting_stop') for item in stops.catalog() if item['id']!='baseline']
    plans = []
    for item in initial:
        if item['kind'] != 'profit_protect':
            continue
        for fraction in FRACTIONS:
            plans.append(dict(id=f'partial{int(fraction*100)}_{item["id"]}',
                family='partial_'+item['family'],kind='partial_exit',
                params=dict(parent=item['id'],fraction=fraction),
                rationale='Exit declared fraction at first parent signal; remaining fraction retains original native exit and full original capital reservation'))
    return initial+extra+plans


def load_first(native):
    summary = core.sealed.package(FIRST_RUN)
    if summary['dataset_manifest_sha256'] != base.sha256(core.DEFAULT_DATA/'manifest.json'):
        raise ValueError('Different original cohort in first iteration')
    if summary['code'][str(Path(rules.__file__).resolve())] != base.sha256(Path(rules.__file__)):
        raise ValueError('Initial close-exit rules changed')
    returns = base.read_frozen_csv(FIRST_RUN/'candidate_net_returns.csv').set_index('sid')
    if set(returns.index) != set(native.sid) or set(returns.columns) != {x['id'] for x in rules.policy_catalog()}:
        raise ValueError('First iteration candidate matrix differs')
    events = {name:{} for name in returns.columns}
    for event in base.read_frozen_csv(FIRST_RUN/'policy_events.csv').to_dict('records'):
        name, sid = event.pop('policy'),int(event.pop('sid'))
        if sid in events[name]:
            raise ValueError('Repeated first event')
        events[name][sid] = event
    indexed = native.set_index('sid')
    for name, group in events.items():
        for sid,event in group.items():
            if not bool(indexed.loc[sid,'filled']):
                raise ValueError('Unfilled first-iteration exit')
    return returns,events,summary


def refine(native,paths,initial_returns,events,policies):
    records = native.set_index('sid').to_dict('index')
    stop_catalog = stops.catalog()
    stop_outcomes = {x['id']:{} for x in stop_catalog if x['id']!='baseline'}
    extra = {name:initial_returns.baseline.copy() for name in stop_outcomes}
    for count,(sid,row) in enumerate(records.items(),1):
        outcomes = stops.outcomes_for_trade(row,paths[sid],stop_catalog)
        if outcomes:
            control = outcomes['baseline']
            if (not np.isclose(control['exit_price'],row['exit_price'],atol=1e-10,rtol=0)
                    or pd.Timestamp(control['exit_ts']) != pd.Timestamp(row['exit_ts'])
                    or not np.isclose(control['gross_return_pct'],row['gross_return_pct'],atol=1e-10,rtol=0)):
                raise ValueError('Resting-stop native control differs')
            for name,outcome in outcomes.items():
                if name=='baseline':
                    continue
                extra[name].loc[sid] = (outcome['gross_return_pct']-.05)*5000.
                if (pd.Timestamp(outcome['exit_ts']) != pd.Timestamp(row['exit_ts'])
                        or abs(outcome['exit_price']-row['exit_price'])>1e-10):
                    stop_outcomes[name][sid] = outcome
        if count%100==0 or count==len(records):
            print(f'Resting-stop replay: {count}/{len(records)} original orders',flush=True)
    for item in policies:
        if item['kind']!='partial_exit':
            continue
        name = item['id']
        extra[name] = initial_returns.baseline.copy()
        for sid,event in events[item['params']['parent']].items():
            outcome = partials.partial_outcome(records[sid],event,item['params']['fraction'],path=paths[sid])
            extra[name].loc[sid] = outcome['net_profit_rs']
    print('Partial exits replayed as two proportional cashflow legs',flush=True)
    return pd.concat([initial_returns,pd.DataFrame(extra)],axis=1),stop_outcomes


def descriptor(row,sid,name,policies,events,stop_outcomes,paths):
    item = policies[name]
    if item['kind']=='partial_exit':
        event = events[item['params']['parent']].get(sid)
        if event:
            outcome = partials.partial_outcome(row,event,item['params']['fraction'],path=paths[sid])
            return dict(changed=True,mechanism='partial_exit',fraction=item['params']['fraction'],
                decision=event['decision_at_ist'],execution=event['next_bar_open_at_ist'],
                final_exit=str(row['exit_ts']),price=outcome['weighted_exit_price'],legs=outcome['legs'],
                extra_cost_5bps=250.*item['params']['fraction'])
    elif item['kind']=='resting_stop':
        outcome = stop_outcomes[name].get(sid)
        if outcome:
            return dict(changed=True,mechanism='resting_stop',fraction=1.,decision='',
                execution=str(outcome['exit_execution_ts']),final_exit=str(outcome['exit_ts']),
                price=outcome['exit_price'],legs=[],extra_cost_5bps=250.)
    else:
        event = events[name].get(sid)
        if event:
            return dict(changed=True,mechanism='close_next_open',fraction=1.,decision=event['decision_at_ist'],
                execution=event['next_bar_open_at_ist'],final_exit=event['modeled_execution_at_ist'],
                price=event['next_bar_open_price'],legs=[],extra_cost_5bps=250.)
    return dict(changed=False,mechanism='native',fraction=0.,decision='',execution=str(row['exit_execution_ts']),
        final_exit=str(row['exit_ts']),price=row['exit_price'],legs=[],extra_cost_5bps=0.)


def ledger(native,paths,name,policies,events,stop_outcomes):
    item = policies[name]
    if item['kind'] not in ('resting_stop','partial_exit'):
        return core.selected_ledger(native,paths,events[name],name)
    raw = native.copy()
    raw['monitor_exit']=False
    raw['monitor_flags']='[]'
    raw['partial_leg_audit']='[]'
    raw['partial_exit_fraction']=0.
    for index,row in native.iterrows():
        sid = int(row.sid)
        details = descriptor(row,sid,name,policies,events,stop_outcomes,paths)
        if not details['changed']:
            continue
        raw.loc[index,'monitor_exit']=True
        raw.loc[index,'monitor_flags']=json.dumps([name])
        if item['kind']=='resting_stop':
            outcome = stop_outcomes[name][sid]
            for key,value in outcome.items():
                raw.loc[index,key]=value
            raw.loc[index,'net_return_pct']=outcome['gross_return_pct']-.05
            raw.loc[index,'cost_pct']=.05
        else:
            event = events[item['params']['parent']][sid]
            outcome = partials.partial_outcome(row,event,item['params']['fraction'],path=paths[sid])
            raw.loc[index,'exit_price']=outcome['weighted_exit_price']
            raw.loc[index,'gross_return_pct']=outcome['gross_profit_rs']/5000.
            raw.loc[index,'net_return_pct']=outcome['net_profit_rs']/5000.
            raw.loc[index,'cost_pct']=.05
            raw.loc[index,'exit_reason']='PARTIAL_THEN_'+str(row.exit_reason)
            raw.loc[index,'exit_event']='TWO_LEGS_FULL_CAPITAL_RESERVED_TO_NATIVE_EXIT'
            raw.loc[index,'partial_exit_fraction']=item['params']['fraction']
            raw.loc[index,'partial_leg_audit']=json.dumps(outcome['legs'])
            raw.loc[index,'monitor_decision_at_ist']=event['decision_at_ist']
            raw.loc[index,'monitor_open_observed_at_ist']=event['next_bar_open_at_ist']
    result = core.previous.allocate(raw,name)
    result['strategy_version']=VERSION
    return result


def best_tradeoff(table):
    acceptable = table.loc[table.original_winner_profit_retention_pct.ge(95.-1e-8)
        & table.loss_saved_rs.gt(1e-7) & table.changed_trades.ge(3)]
    if acceptable.empty:
        return 'baseline'
    return str(acceptable.sort_values(['loss_saved_rs','net_change_rs','policy'],
        ascending=[False,False,True],kind='stable').iloc[0].policy)


def run(output):
    output=Path(output).resolve()
    if output.exists() or not output.is_relative_to((base.G4_ROOT/'research').resolve()):
        raise ValueError('Fresh research output required')
    native,contexts,paths,proof=core.dataset.load(core.DEFAULT_DATA)
    del contexts
    native['day']=native.day.astype(str).str[:10]
    native=native.sort_values(['day','confirmation_ts','sid'],kind='stable').reset_index(drop=True)
    meta=native.loc[native.portfolio_executed].set_index('sid',drop=False)
    days=proof['days']
    policies=catalog()
    by_id={item['id']:item for item in policies}
    if len(by_id)!=len(policies):
        raise ValueError('Policy identifiers overlap')
    modules=[__file__,core.__file__,stops.__file__,partials.__file__,rules.__file__,adaptive.__file__,
        core.dataset.__file__,core.diagnostics.__file__,base.__file__,core.sealed.__file__,core.previous.__file__,
        core.exit_helper.__file__,core.bounded.__file__,core.sealed.simulate_exit.__code__.co_filename,
        core.bounded.history.simulator.g.v9.__file__,core.bounded.history.simulator.g.v9.v5.__file__,
        core.bounded.history.simulator.g.v9.v6.__file__]
    code={str(Path(p).resolve()):base.sha256(Path(p)) for p in modules}
    pointers={str(p):base.sha256(p) for p in [base.G4_ROOT/'latest/bounded_exit.json',base.G4_ROOT/'latest/g3_mirror.json'] if p.exists()}
    output.mkdir(parents=True)
    (output/'code').mkdir()
    for filename in code:
        shutil.copyfile(filename,output/'code'/Path(filename).name)
    config=core.configuration(policies)
    config.update(version=VERSION,iteration=2,previous_iteration=str(FIRST_RUN),
        previous_iteration_manifest_sha256=base.sha256(FIRST_RUN/'manifest.json'),
        new_mechanics='Actual protective resting stop fills;25/50/75% next-open partial exits with original native remaining exits',
        partial_capital='Conservatively retain entire original margin until original native exit',
        partial_costs='5bps proportional entry-notional allocation across both legs; total original round-trip cost Rs250',
        selection_tradeoff='Separately disclose greatest development loss saving under95% original-winner retention even if net/winrate fail; never auto-recommend',
        stage2='104 protective-stop variations and756 partial-exit variations declared before this iteration; no additional fitted combinations',
        data_use='Iteration2 followed inspection of iteration1; all52 sessions remain reused exploratory history')
    core.json_write(output/'config.json',config)
    config_hash=base.sha256(output/'config.json')
    initial,events,first_summary=load_first(native)
    indexed_native=native.set_index('sid')
    for name,group in events.items():
        for sid,event in group.items():
            core.validate_event(indexed_native.loc[sid],paths[sid],event)
    returns,stop_outcomes=refine(native,paths,initial,events,policies)
    fixed=returns.loc[meta.index].copy()
    table=core.comparison_table(fixed,meta,policies,days)
    dev=table.loc[table.period.eq('development35')]
    selected=core.choose(dev)
    tradeoff=best_tradeoff(dev)
    print(f'Iteration2 selection: strict={selected}; loss-reduction tradeoff={tradeoff}',flush=True)
    core.json_write(output/'development_selection.json',dict(selected_policy=selected,
        tradeoff_policy=tradeoff,selected_parameters=by_id[selected],tradeoff_parameters=by_id[tradeoff],
        training_days=days[:35],diagnostic_days=days[35:],
        warning='Tradeoff choice may reduce net profit or win rate despite satisfying development winner-profit cap'))
    model=adaptive.select_adaptive(meta,fixed,days)
    fixed['adaptive_previous_sessions']=model['pnl'].reindex(fixed.index)
    model['assignments'].to_csv(output/'adaptive_assignments.csv',index=False)
    model['folds'].to_csv(output/'adaptive_folds.csv',index=False)
    setup_model=adaptive.select_setup_holdout(meta,returns.loc[meta.index],days)
    setup_model['assignments'].to_csv(output/'setup_selection_assignments.csv',index=False)
    setup_model['folds'].to_csv(output/'setup_selection_evidence.csv',index=False)
    table=core.comparison_table(fixed,meta,policies,days)
    adaptive_test=core.metrics(fixed.baseline,fixed.adaptive_previous_sessions,meta,days[20:])
    table=pd.concat([table,pd.DataFrame([dict(period='adaptive_evaluated32',policy='adaptive_previous_sessions',
        family='adaptive',qualifies=core.qualifies(adaptive_test),**adaptive_test)])],ignore_index=True)
    displayed=list(dict.fromkeys(['baseline',selected,tradeoff,'adaptive_previous_sessions']))
    daily,changes,stress,setup_rows=[],[],[],[]
    for name in displayed:
        charges=pd.Series(0.,index=meta.index)
        for sid,row in meta.iterrows():
            actual=str(model['policy_for_sid'].loc[sid]) if name=='adaptive_previous_sessions' else name
            details=descriptor(row,int(sid),actual,by_id,events,stop_outcomes,paths)
            charges.loc[sid]=details['extra_cost_5bps']
            changes.append(dict(policy=name,applied_policy=actual,sid=sid,day=row.day,setup_id=row.setup_id,
                symbol=row.tradingsymbol,side=row.side,baseline_net_rs=fixed.loc[sid,'baseline'],
                new_net_rs=fixed.loc[sid,name],delta_net_rs=fixed.loc[sid,name]-fixed.loc[sid,'baseline'],
                original_exit_reason=row.exit_reason,original_exit_ts=row.exit_ts,
                changed=details['changed'],mechanism=details['mechanism'],fraction=details['fraction'],
                decision_at_ist=details['decision'],execution_at_ist=details['execution'],
                final_exit_ts=details['final_exit'],effective_exit_price=details['price'],
                partial_legs=json.dumps(details['legs'])))
        for day in days:
            daily.append(dict(day=day,policy=name,**core.metrics(fixed.baseline,fixed[name],meta,[day])))
        for mode,charge in [('original5bps',charges*0),('extra5bps_changed_quantity',charges),('extra5bps_all_trades',pd.Series(250.,index=meta.index))]:
            baseline=fixed.baseline-(250. if mode=='extra5bps_all_trades' else 0.)
            for period,cohort in dict(all=days,later17=days[35:]).items():
                stress.append(dict(policy=name,cost_scenario=mode,period=period,
                    **core.metrics(baseline,fixed[name]-charge,meta,cohort)))
    for setup in sorted(native.setup_id.unique()):
        members=meta.loc[meta.setup_id.eq(setup)]
        for name in list(dict.fromkeys([selected,tradeoff,'adaptive_previous_sessions'])):
            for period,cohort in dict(all=days,development35=days[:35],later17=days[35:]).items():
                setup_rows.append(dict(setup_id=setup,period=period,policy=name,
                    **core.metrics(fixed.loc[members.index,'baseline'],fixed.loc[members.index,name],members,cohort)))
    daily,changes,setups=pd.DataFrame(daily),pd.DataFrame(changes),pd.DataFrame(setup_rows)
    portfolio=[]
    (output/'ledgers').mkdir()
    for name in list(dict.fromkeys(['baseline',selected,tradeoff])):
        book=ledger(native,paths,name,by_id,events,stop_outcomes)
        book.to_csv(output/'ledgers'/(name+'.csv'),index=False)
        saved=base.read_frozen_csv(output/'ledgers'/(name+'.csv')).set_index('sid')
        if not np.allclose(saved.loc[meta.index,'net_profit_rupees'],returns.loc[meta.index,name],atol=1e-6,rtol=0):
            raise ValueError('Full ledger and research matrix differ')
        for period,cohort in dict(all=days,development35=days[:35],later17=days[35:]).items():
            portfolio.append(dict(policy=name,period=period,**core.bounded.stats(book,cohort),
                newly_admitted=int((saved.portfolio_executed & ~native.set_index('sid').portfolio_executed & saved.day.isin(cohort)).sum()),
                originally_executed_now_rejected=int((~saved.portfolio_executed & native.set_index('sid').portfolio_executed & saved.day.isin(cohort)).sum())))
    pd.DataFrame(portfolio).to_csv(output/'portfolio_replay.csv',index=False)
    table.to_csv(output/'comparison.csv',index=False)
    daily.to_csv(output/'daily_comparison.csv',index=False)
    changes.to_csv(output/'trade_changes.csv',index=False)
    setups.to_csv(output/'setup_results.csv',index=False)
    pd.DataFrame(stress).to_csv(output/'stress_tests.csv',index=False)
    returns.reset_index().to_csv(output/'candidate_net_returns.csv',index=False)
    shutil.copyfile(FIRST_RUN/'policy_events.csv',output/'policy_events.csv')
    stop_records=[dict(policy=name,sid=sid,**outcome) for name,group in stop_outcomes.items() for sid,outcome in group.items()]
    pd.DataFrame(stop_records).to_csv(output/'protective_stop_exits.csv',index=False)
    intervals={name:{period:core.bootstrap_delta(fixed.baseline,fixed[name],meta,cohort)
        for period,cohort in dict(all=days,later17=days[35:]).items()} for name in displayed if name!='baseline'}
    core.json_write(output/'bootstrap.json',intervals)
    core.diagnostics.run(output/'historical_patterns')
    selected_all=table.loc[table.policy.eq(selected)&table.period.eq('all')].iloc[0].to_dict()
    selected_later=table.loc[table.policy.eq(selected)&table.period.eq('later17')].iloc[0].to_dict()
    tradeoff_all=table.loc[table.policy.eq(tradeoff)&table.period.eq('all')].iloc[0].to_dict()
    tradeoff_later=table.loc[table.policy.eq(tradeoff)&table.period.eq('later17')].iloc[0].to_dict()
    if selected=='baseline':
        conclusion='None of the tested alternatives met all development requirements: higher win rate, lower loss amount, no net-profit decline, and95% original-winner profit retention. Original G4 remains the qualified fallback.'
    else:
        conclusion=(f'Development selected {selected}. Across52 sessions: loss amount reduction {selected_all["loss_reduction_pct"]:.2f}%, net-profit change Rs{selected_all["net_change_rs"]:,.2f}, original-winner profit retained {selected_all["original_winner_profit_retention_pct"]:.2f}%. '
            +('The same requirements hold on the later17 sessions.' if selected_later['qualifies'] else 'The complete requirements do not hold on the later17 sessions.'))
    if tradeoff!='baseline' and tradeoff!=selected:
        conclusion+=(f' The separate loss-reduction tradeoff {tradeoff} saves {tradeoff_all["loss_reduction_pct"]:.2f}% of full-history loss money but changes net profit by Rs{tradeoff_all["net_change_rs"]:,.2f}; it retains {tradeoff_all["original_winner_profit_retention_pct"]:.2f}% of original winners’ profit. This is disclosed as a tradeoff, not an approved improvement.')
    limitations=first_summary['limitations'].copy()
    limitations=[x for x in limitations if not x.startswith('No entry filters')]
    limitations += [
        'Iteration2 added declared resting-stop and partial-exit mechanics after iteration1 failed. The catalogue is now1140 alternatives, increasing selection bias; this history is not future validation.',
        'Partial exits use two explicit proportional cashflow legs. The remaining position follows the original native exit; full margin stays reserved until that exit. Fractional sizing follows the original model.',
        'Protective stops arm only from completed full post-entry closes, activate next minute, fill gaps at the observed open, and use stop-first ordering when a minute touches stop and target.',
        'The separate loss-reduction tradeoff is selected on development data using the95% original-winner-profit floor; it may fail the net-profit and win-rate goals. Full-history and later-period floor breaches are displayed explicitly.',
        'No entry reranking, new entry filter, pyramiding, wider original stops or new price target is introduced.'
    ]
    summary=dict(variant_id=VERSION,scope='BACKTEST_ONLY',execution_authority=False,
        created_at_ist=datetime.now(base.production.IST).isoformat(),days=days,active_setups=51,
        orders=len(native),original_executed_trades=len(meta),dataset=str(core.DEFAULT_DATA),
        dataset_manifest_sha256=base.sha256(core.DEFAULT_DATA/'manifest.json'),original_source=proof['original_source'],
        original_manifest_sha256=proof['original_manifest_sha256'],native_control_exactly_reproduced=True,
        previous_iteration=str(FIRST_RUN),previous_iteration_manifest_sha256=base.sha256(FIRST_RUN/'manifest.json'),
        code=code,config_sha256=config_hash,unchanged_pointers=pointers,
        fixed_alternatives=len(policies)-1,iteration1_alternatives=280,new_alternatives=len(policies)-281,
        families=pd.DataFrame(policies).groupby('family').size().rename('candidates').reset_index().to_dict('records'),
        development_selected_policy=selected,selected_policy_parameters=by_id[selected],
        development_qualified=int(dev.qualifies.sum()),selected_all=selected_all,selected_later17=selected_later,
        development_tradeoff_policy=tradeoff,tradeoff_parameters=by_id[tradeoff],
        tradeoff_all=tradeoff_all,tradeoff_later17=tradeoff_later,adaptive_evaluated32=adaptive_test,
        setup_training_decisions=setup_model['folds'].reason.value_counts().to_dict(),
        conclusion=conclusion,search_description=f'Two declared iterations:280 completed-close exit alternatives;104 resting protection alternatives;756 partial-exit alternatives. Total{len(policies)-1} alternatives plus original G4. Expanding-window selection uses only prior sessions’ outcomes.',
        limitations=limitations,sources=core.SOURCES)
    core.json_write(output/'summary.json',summary)
    core.plot(output,daily,selected if selected!='baseline' else tradeoff)
    setup_display=setups.loc[setups.period.eq('all')&setups.policy.eq(selected if selected!='baseline' else tradeoff),
        ['setup_id','policy','trades','baseline_losses','losses','win_rate_pct','net_pnl_rs','net_change_rs','loss_reduction_pct','original_winner_profit_retention_pct','original_winners_turned_losses']]
    core.report(output,summary,table,setup_display,daily)
    # Explicit tradeoff comparison and full-ledger links accompany the strict table.
    report=(output/'report.html').read_text(encoding='utf-8')
    cols=['period','policy','wins','losses','win_rate_pct','negative_trade_sum_abs_rs','loss_reduction_pct','net_pnl_rs','net_change_rs','original_winner_profit_retention_pct','original_winners_turned_losses']
    add='<section><h2>Separate loss-reduction tradeoff</h2><p>This is the first35-selected best loss reduction under the95% winner-profit floor, even when net profit or win rate fails. It is not a live recommendation. Later/full-period retention must be checked independently.</p><div class="scroll">'+table.loc[table.policy.eq(tradeoff),cols].to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div><p><a href="portfolio_replay.csv">Full portfolio replay</a> · <a href="protective_stop_exits.csv">Protective stop executions</a> · <a href="setup_selection_evidence.csv">Per-setup evidence requirements</a> · <a href="historical_patterns/summary.json">Historical pattern diagnostics</a></p></section>'
    report=report.replace('<section><h2>What was tried</h2>',add+'<section><h2>What was tried</h2>')
    report=report.replace('Results apply the development-selected global policy.','Results apply the displayed development-selected policy (the separately labelled tradeoff when no strict candidate qualifies).')
    report=report.replace('All candidate results, including rejected variants, are saved.','All candidate results, including rejected variants, are saved. Partial and resting-stop executions have separate audits.')
    (output/'report.html').write_text(report,encoding='utf-8')
    for filename,digest in code.items():
        if base.sha256(Path(filename))!=digest:
            raise ValueError('Evaluated code changed')
    for filename,digest in pointers.items():
        if base.sha256(Path(filename))!=digest:
            raise ValueError('Research changed strategy pointer')
    if base.sha256(output/'config.json')!=config_hash:
        raise ValueError('Frozen candidate catalogue changed')
    core.json_write(output/'manifest.json',dict(version=VERSION,execution_authority=False,
        files={p.relative_to(output).as_posix():base.sha256(p) for p in output.rglob('*') if p.is_file()}))
    print(conclusion,flush=True)
    print(output,flush=True)
    return output


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path)
    args=parser.parse_args()
    stamp=datetime.now(base.production.IST)
    run(args.output_dir or base.G4_ROOT/'research'/f'{stamp:%Y%m%dT%H%M%S}_{secrets.token_hex(3)}_iterative_protection_v2')
