"""Re-evaluate the audited G4 exit universe under a changed winner-profit cap.

The cap changes selection, not any one-minute exit outcome. Reuse verified
outcomes; freshly replay selected static ledgers and re-fit earlier-session
adaptive selection. Keep the original 5% study and all strategy settings intact.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import html
import json
from pathlib import Path
import secrets
import shutil

import numpy as np
import pandas as pd

from research import g4_iterative_protection_refinement as replay
from research import g4_retention_policy_selector as selector

core,base=replay.core,replay.base
SOURCE=base.G4_ROOT/'research/20261016T162554_29716e_iterative_protection_v2'
SOURCE_HASH='23cd93512ac1a1b6276704bb43cfa6269241b50f35c72b14739b7e365e11eb0d'
VERSION='G4-WINNER-PROFIT-CAP-COMPARISON-V1'
WATCH='profit_floor_020'


def eligible(table,floor,minimum_changed=3,require_economic=True):
    if not np.isfinite(floor) or not 0<floor<=1:
        raise ValueError('Winner retention must be in (0,1]')
    keep=(table.original_winner_profit_retention_pct.ge(floor*100-1e-8)
          & table.loss_saved_rs.gt(1e-7) & table.changed_trades.ge(minimum_changed))
    if require_economic:
        keep &= (table.net_change_rs.ge(-1e-7) & table.wins.gt(table.baseline_wins)
                 & table.losses.lt(table.baseline_losses))
    return keep


def choose(table,floor,minimum_changed=3,require_economic=True):
    candidates=table.loc[eligible(table,floor,minimum_changed,require_economic)]
    if candidates.empty:
        return 'baseline'
    return str(candidates.sort_values(['loss_saved_rs','net_change_rs','policy'],
        ascending=[False,False,True],kind='stable').iloc[0].policy)


def selections(table,floor):
    # Later and full-history rows never participate in chronological selection.
    dev=table.loc[table.period.eq('development35')]
    return dict(minimum_original_winner_profit_retention=floor,
        strict_policy=choose(dev,floor),
        economic_sparse_candidate=choose(dev,floor,minimum_changed=1),
        loss_focused_policy=choose(dev,floor,require_economic=False),
        strict_qualified_count=int(eligible(dev,floor).sum()),
        economic_qualified_count=int(eligible(dev,floor,minimum_changed=1).sum()),
        loss_focused_eligible_count=int(eligible(dev,floor,require_economic=False).sum()))


def period_metrics(before,after,meta,days):
    item=core.metrics(before,after,meta,days)
    item['flat_trades']=item['trades']-item['wins']-item['losses']
    return item


def render(output,summary,comparison,october,setup_table):
    labels={'period':'Period','role':'Selection','policy':'Policy','trades':'Trades','wins':'Wins','losses':'Losses',
        'flat_trades':'Flat','win_rate_pct':'Win rate %','net_pnl_rs':'Net profit ₹','net_change_rs':'Net change ₹',
        'negative_trade_sum_abs_rs':'Loss amount ₹','loss_reduction_pct':'Loss reduction %',
        'original_winner_profit_retention_pct':'Original winner profit retained %',
        'original_winners_turned_losses':'Original winners turned losses','changed_trades':'Affected trades',
        'setup_id':'Setup','baseline_losses':'Original losses'}
    cols=['period','role','policy','wins','losses','flat_trades','win_rate_pct','negative_trade_sum_abs_rs',
          'loss_reduction_pct','net_pnl_rs','net_change_rs','original_winner_profit_retention_pct','changed_trades']
    css='body{background:#101a29;color:#e8eff7;font:15px/1.55 system-ui;padding:28px;margin:0}main{max-width:1600px;margin:auto}section{background:#19283d;padding:20px;margin:20px 0;border-radius:9px}a{color:#8ad4ff}.scroll{overflow:auto}table{border-collapse:collapse;font-size:12px}td,th{padding:8px;border-bottom:1px solid #40516a;text-align:right;white-space:nowrap}th{background:#263b56}td:first-child,th:first-child{text-align:left}strong{color:#a8e8cf}img{max-width:100%}'
    doc=f'<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G4: 10% winner-profit cap test</title><style>{css}</style></head><body><main><h1>G4: 10% winner-profit sacrifice test</h1>'
    doc+='<p>Same 51 original setups, 466 executed trades and 52 completed sessions: July 29–October 15, 2026. Original entries, costs, targets and candidate exit rules are unchanged.</p>'
    doc+='<section><strong>'+html.escape(summary['conclusion'])+'</strong><p>The new limit means retaining at least 90% of the original winners’ combined net profit. It is an aggregate constraint, not a per-trade or per-day promise.</p><p>There are two different decisions: the strict screen requires higher win rate, lower loss amount, no net-profit reduction and at least three affected development trades. The separate loss-focused choice only requires the profit-retention cap and loss reduction; it can reduce net profit. The sparse economic candidate is shown without the three-trade evidence guard.</p></section>'
    doc+='<section><h2>5% versus 10%: full-history comparison</h2><div class="scroll">'+comparison.loc[comparison.period.eq('all'),cols].rename(columns=labels).to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div></section>'
    doc+='<section><h2>Later 17 sessions</h2><p>Policies above were chosen using the first 35 sessions. These later sessions have already been examined during G4 research and are chronological diagnostics, not an untouched holdout.</p><div class="scroll">'+comparison.loc[comparison.period.eq('later17'),cols].rename(columns=labels).to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div></section>'
    doc+='<section><h2>October 15: every selected order</h2><p>The 10% loss-focused choice leaves the day unchanged. Executed amounts include the same modeled 5 bps round-trip cost; unfilled orders have zero P&amp;L.</p><div class="scroll">'+october.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div></section>'
    doc+='<section><h2>Policy selected under the 10% limit</h2><p>'+html.escape(summary['loss_focused_rule_explanation'])+'</p><pre>'+html.escape(json.dumps(summary['loss_focused_parameters'],indent=2))+'</pre><p>All original profitable trades remain net profitable under this candidate, but many earn less. The combined profit retained from original winners is reported separately from the win count.</p></section>'
    doc+='<section><h2>Sparse positive candidate retained from the 5% study</h2><p>The near-target stop, profit_floor_020, still adds ₹12,000, reduces loss amount by ₹10,500, and leaves every original winner unchanged. It affects only two trades: one in development and one later. Raising the allowance to 10% did not reveal a new candidate that passes the economic goals and the three-development-trade screening requirement.</p></section>'
    hindsight=summary['retrospective_maximum_loss_reduction']
    doc+='<section><h2>Retrospective maximum loss reduction under the 10% cap</h2><p>This policy was chosen after looking at all 52 sessions. It is not the development-selected policy.</p><div class="scroll">'+pd.DataFrame([hindsight])[['policy','wins','losses','flat_trades','win_rate_pct','loss_reduction_pct','net_change_rs','original_winner_profit_retention_pct']].rename(columns=labels).to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div><p>Its lower loss count includes many trades becoming flat. Its win rate falls, and its development-period winner retention breaches the new 90% floor. The October 15 effect is disclosed in the files as retrospective inspection, not a selectable advance prediction.</p></section>'
    doc+='<section><h2>All 51 setups under the development-selected 10% loss-focused policy</h2><div class="scroll">'+setup_table.rename(columns=labels).to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div></section>'
    doc+='<section><h2>Limits and verification</h2><ul>'+''.join('<li>'+html.escape(x)+'</li>' for x in summary['limitations'])+'</ul></section>'
    files=[('comparison.csv','Selected policies and periods'),('policy_comparison.csv','All 1,140 policies under the new cap'),('oct15_before_after.csv','October 15 comparison'),('oct15_all_inspected_policies.csv','October 15 including hindsight policy'),('setup_results.csv','Setup-by-setup results'),('daily_comparison.csv','Daily comparisons'),('trade_changes.csv','Matched trade changes'),('selection.json','5% and 10% selection decisions'),('adaptive_assignments.csv','Prior-session policy assignments'),('adaptive_folds.csv','Adaptive training audits'),('portfolio_replay.csv','Full portfolio replay'),('stress_tests.csv','Extra-cost sensitivity'),('summary.json','Summary and provenance'),('config.json','Frozen test specification')]
    doc+='<section><h2>Files</h2><ul>'+''.join(f'<li><a href="{p}">{t}</a></li>' for p,t in files)+'</ul></section></main></body></html>'
    (output/'report.html').write_text(doc,encoding='utf-8')


def run(output,minimum_retention=.90):
    output=Path(output).resolve()
    if output.exists() or not output.is_relative_to((base.G4_ROOT/'research').resolve()):
        raise ValueError('Fresh isolated output required')
    if minimum_retention!=.90:
        raise ValueError('This comparison is specified for the requested 10% sacrifice cap')
    if base.sha256(SOURCE/'manifest.json')!=SOURCE_HASH:
        raise ValueError('Pinned audited study changed')
    source_summary=core.sealed.package(SOURCE)
    for filename,digest in source_summary['code'].items():
        if base.sha256(Path(filename))!=digest:
            raise ValueError('Original evaluated source changed: '+filename)
    native,contexts,paths,proof=core.dataset.load(core.DEFAULT_DATA)
    del contexts
    native['day']=native.day.astype(str).str[:10]
    native=native.sort_values(['day','confirmation_ts','sid'],kind='stable').reset_index(drop=True)
    meta=native.loc[native.portfolio_executed].set_index('sid',drop=False)
    days=proof['days']
    policies={p['id']:p for p in replay.catalog()}
    matrix=base.read_frozen_csv(SOURCE/'candidate_net_returns.csv').set_index('sid')
    if set(matrix.columns)!=set(policies) or set(matrix.index)!=set(native.sid):
        raise ValueError('Outcome matrix or selected orders differ')
    fixed=matrix.loc[meta.index].copy()
    if not np.allclose(fixed.baseline,meta.portfolio_net_profit_rupees,atol=1e-6,rtol=0):
        raise ValueError('Original cohort accounting differs')
    table=base.read_frozen_csv(SOURCE/'comparison.csv')
    table=table.loc[table.policy.isin(policies)].copy()
    modules=[__file__,selector.__file__,replay.__file__,core.__file__]
    code={str(Path(p).resolve()):base.sha256(Path(p)) for p in modules}
    pointers=source_summary['unchanged_pointers']
    if not all(base.sha256(Path(p))==digest for p,digest in pointers.items()):
        raise ValueError('Original G4 pointer no longer matches pinned study')
    output.mkdir(parents=True)
    (output/'code').mkdir()
    for filename in code:
        shutil.copyfile(filename,output/'code'/Path(filename).name)
    config=dict(version=VERSION,execution_authority=False,scope='BACKTEST_ONLY',
        minimum_original_winner_profit_retention=.90,comparison_minimum_retention=.95,
        selection_training_sessions=35,chronological_diagnostic_sessions=17,
        selected_order_count=533,original_executed_trade_count=466,setup_count=51,
        fixed_alternatives=1140,exit_logic_changed=False,entry_logic_changed=False,
        verified_reused_source=str(SOURCE),verified_reused_manifest_sha256=SOURCE_HASH,
        reused_candidate_matrix_sha256=base.sha256(SOURCE/'candidate_net_returns.csv'),
        reuse_reason='Changing permitted winner-profit sacrifice only changes policy selection; deterministic minute exit outcomes remain identical.',
        selection_rank='Largest loss amount saved, then largest net change, then policy id',
        strict_conditions='Winner retention>=90%, net>=baseline, more wins, fewer losses, lower loss amount, at least3 changed development trades',
        sparse_economic_conditions='Same economic conditions without the minimum3 affected-trade evidence guard; report sparse evidence explicitly',
        loss_focused_conditions='Winner retention>=90%, lower loss amount, at least3 changed development trades; net profit and win rate may decline',
        adaptive_parameters=dict(selector.PARAMETERS,minimum_winner_profit_retention=.90),
        costs='Original5bps round trip on Rs500000 exposure; partial legs proportional; extra5bps stress',
        unchanged_execution='Full close then next observed open, native stop priority, protective stops arm for next minute, partial remainder unchanged',
        parameters_frozen_before_selection=True)
    core.json_write(output/'config.json',config)
    config_hash=base.sha256(output/'config.json')
    choices5,choices10=selections(table,.95),selections(table,.90)
    selected=choices10['loss_focused_policy']
    print('5% selections:',choices5,flush=True)
    print('10% selections:',choices10,flush=True)
    core.json_write(output/'selection.json',dict(cap5=choices5,cap10=choices10,
        development_days=days[:35],diagnostic_days=days[35:]))
    table['qualifies']=eligible(table,.90)
    table['economic_goals_met_without_sample_guard']=eligible(table,.90,minimum_changed=1)
    table['within_10pct_winner_profit_cap']=table.original_winner_profit_retention_pct.ge(90.-1e-8)
    table['flat_trades']=table.trades-table.wins-table.losses
    table.to_csv(output/'policy_comparison.csv',index=False)
    print('Refitting chronological adaptive selector under90% winner retention',flush=True)
    model=selector.select_adaptive(meta,fixed,days,minimum_winner_profit_retention=.90)
    fixed['adaptive_10pct']=model['pnl'].reindex(meta.index)
    old_adaptive=base.read_frozen_csv(SOURCE/'trade_changes.csv')
    old_adaptive=old_adaptive.loc[old_adaptive.policy.eq('adaptive_previous_sessions')].set_index('sid')
    fixed['adaptive_5pct']=old_adaptive.new_net_rs.reindex(meta.index)
    if fixed.isna().any().any():
        raise ValueError('Missing fixed cohort result')
    model['assignments'].to_csv(output/'adaptive_assignments.csv',index=False)
    model['folds'].to_csv(output/'adaptive_folds.csv',index=False)
    setup_model=selector.select_setup_holdout(meta,matrix.loc[meta.index],days,minimum_winner_profit_retention=.90)
    setup_model['folds'].to_csv(output/'setup_selection_evidence.csv',index=False)
    hindsight=choose(table.loc[table.period.eq('all')],.90,require_economic=False)
    roles={'original':'baseline','5% loss-focused':choices5['loss_focused_policy'],
           '10% loss-focused':selected,'sparse positive candidate':WATCH,
           'adaptive5%':'adaptive_5pct','adaptive10%':'adaptive_10pct'}
    statics=list(dict.fromkeys(['baseline',choices5['loss_focused_policy'],selected,WATCH,hindsight]))
    _,events,_=replay.load_first(native)
    stops={name:{} for name in statics if policies[name]['kind']=='resting_stop'}
    stop_catalog=[policies[name] for name in stops]
    for row in native.to_dict('records'):
        sid=int(row['sid'])
        for name,outcome in replay.stops.outcomes_for_trade(row,paths[sid],stop_catalog).items():
            if pd.Timestamp(outcome['exit_ts'])!=pd.Timestamp(row['exit_ts']) or abs(outcome['exit_price']-row['exit_price'])>1e-10:
                stops[name][sid]=outcome
    portfolio=[]
    (output/'ledgers').mkdir()
    for name in statics:
        book=replay.ledger(native,paths,name,policies,events,stops)
        saved=book.set_index('sid')
        if not np.allclose(saved.loc[meta.index,'net_profit_rupees'],matrix.loc[meta.index,name],atol=1e-6,rtol=0):
            raise ValueError('Fresh selected ledger differs from audited minute outcomes')
        book.to_csv(output/'ledgers'/(name+'.csv'),index=False)
        for period,cohort in dict(all=days,development35=days[:35],later17=days[35:],oct15=['2026-10-15']).items():
            portfolio.append(dict(policy=name,period=period,**core.bounded.stats(book,cohort),
                newly_admitted=int((saved.portfolio_executed&~native.set_index('sid').portfolio_executed&saved.day.isin(cohort)).sum()),
                originally_executed_now_rejected=int((~saved.portfolio_executed&native.set_index('sid').portfolio_executed&saved.day.isin(cohort)).sum())))
    pd.DataFrame(portfolio).to_csv(output/'portfolio_replay.csv',index=False)
    print('Selected ledgers freshly replayed; exact candidate-matrix parity',flush=True)
    comparison,daily,changes,setup_rows=[],[],[],[]
    for role,name in roles.items():
        for period,cohort in dict(all=days,development35=days[:35],later17=days[35:],oct15=['2026-10-15'],adaptive_evaluated32=days[20:]).items():
            comparison.append(dict(role=role,policy=name,period=period,**period_metrics(fixed.baseline,fixed[name],meta,cohort)))
        for day in days:
            daily.append(dict(role=role,policy=name,day=day,**period_metrics(fixed.baseline,fixed[name],meta,[day])))
        for sid,row in meta.iterrows():
            actual=str(model['policy_for_sid'].loc[sid]) if name=='adaptive_10pct' else str(old_adaptive.loc[sid,'applied_policy']) if name=='adaptive_5pct' else name
            changes.append(dict(role=role,policy=name,applied_policy=actual,sid=sid,day=row.day,
                setup_id=row.setup_id,symbol=row.tradingsymbol,side=row.side,
                baseline_net_rs=fixed.loc[sid,'baseline'],new_net_rs=fixed.loc[sid,name],
                delta_net_rs=fixed.loc[sid,name]-fixed.loc[sid,'baseline']))
    for setup in sorted(native.setup_id.unique()):
        members=meta.loc[meta.setup_id.eq(setup)]
        for name in [selected,WATCH,'adaptive_10pct']:
            for period,cohort in dict(all=days,later17=days[35:]).items():
                setup_rows.append(dict(setup_id=setup,policy=name,period=period,**period_metrics(
                    fixed.loc[members.index,'baseline'],fixed.loc[members.index,name],members,cohort)))
    comparison,daily,changes,setups=pd.DataFrame(comparison),pd.DataFrame(daily),pd.DataFrame(changes),pd.DataFrame(setup_rows)
    for name,frame in [('comparison',comparison),('daily_comparison',daily),('trade_changes',changes),('setup_results',setups)]:
        frame.to_csv(output/(name+'.csv'),index=False)
    day_rows=native.loc[native.day.eq('2026-10-15')].set_index('sid')
    october=day_rows[['setup_id','tradingsymbol','side','portfolio_executed']].copy()
    october['before_net_rs']=matrix.loc[day_rows.index,'baseline'].where(day_rows.portfolio_executed,0.)
    october['after_10pct_net_rs']=matrix.loc[day_rows.index,selected].where(day_rows.portfolio_executed,0.)
    october['delta_rs']=october.after_10pct_net_rs-october.before_net_rs
    october.reset_index().to_csv(output/'oct15_before_after.csv',index=False)
    expanded=october.copy()
    expanded['after_5pct_loss_focused_rs']=matrix.loc[day_rows.index,choices5['loss_focused_policy']].where(day_rows.portfolio_executed,0.)
    expanded['after_sparse_positive_rs']=matrix.loc[day_rows.index,WATCH].where(day_rows.portfolio_executed,0.)
    expanded['after_hindsight_loss_policy_rs']=matrix.loc[day_rows.index,hindsight].where(day_rows.portfolio_executed,0.)
    expanded.reset_index().to_csv(output/'oct15_all_inspected_policies.csv',index=False)
    stress=[]
    for name in [selected,WATCH]:
        charges=pd.Series(0.,index=meta.index)
        for sid,row in meta.iterrows():
            details=replay.descriptor(row,int(sid),name,policies,events,stops,paths)
            charges.loc[sid]=details['extra_cost_5bps']
        for mode,charge in [('original5bps',charges*0),('extra5bps_changed_quantity',charges),('extra5bps_all_trades',pd.Series(250.,index=meta.index))]:
            before=fixed.baseline-(250. if mode=='extra5bps_all_trades' else 0.)
            for period,cohort in dict(all=days,later17=days[35:],oct15=['2026-10-15']).items():
                stress.append(dict(policy=name,cost_scenario=mode,period=period,**period_metrics(before,fixed[name]-charge,meta,cohort)))
    pd.DataFrame(stress).to_csv(output/'stress_tests.csv',index=False)
    core.json_write(output/'bootstrap.json',{name:{period:core.bootstrap_delta(fixed.baseline,fixed[name],meta,cohort)
        for period,cohort in dict(all=days,later17=days[35:]).items()} for name in [selected,WATCH,'adaptive_10pct']})
    all_result=comparison.loc[comparison.role.eq('10% loss-focused')&comparison.period.eq('all')].iloc[0].to_dict()
    later_result=comparison.loc[comparison.role.eq('10% loss-focused')&comparison.period.eq('later17')].iloc[0].to_dict()
    winner_profit=float(meta.loc[meta.portfolio_net_profit_rupees.gt(1e-8),'portfolio_net_profit_rupees'].sum())
    conclusion=(f'The10% allowance selects {selected} for development loss reduction: full-history loss money falls {all_result["loss_reduction_pct"]:.2f}%, but net profit falls Rs{-all_result["net_change_rs"]:,.2f}. '
        f'Original winners retain {all_result["original_winner_profit_retention_pct"]:.2f}% of their combined profit. No new policy passes all economic goals plus the prespecified three-development-trade guard. The sparse near-target candidate remains unchanged.')
    parent=policies[selected]['params'].get('parent')
    params=dict(policy=policies[selected],parent=policies[parent] if parent else None)
    explanation='After a full post-entry minute reaches at least max(0.50%,60% of the native target), wait for a completed-close giveback of at least50% of peak closing profit while gross profit remains at least0.07%. Exit75% of the original quantity at the following observed open; retain the other25% to its original native exit. The rule needs at least four full post-entry bars. Entire original margin stays reserved until the native exit.'
    # Keep prose auditable against the selected frozen parameters rather than
    # silently describing a different candidate if the study inputs change.
    if selected!='partial75_target_giveback_091':
        explanation='See the exact selected policy and parent parameters below; no narrative threshold assumptions are applied.'
    summary=dict(variant_id=VERSION,scope='BACKTEST_ONLY',execution_authority=False,
        created_at_ist=datetime.now(base.production.IST).isoformat(),days=days,active_setups=51,
        original_executed_trades=466,selected_orders=533,fixed_alternatives=1140,
        source_run=str(SOURCE),source_manifest_sha256=SOURCE_HASH,
        dataset_manifest_sha256=base.sha256(core.DEFAULT_DATA/'manifest.json'),
        config_sha256=config_hash,code=code,unchanged_pointers=pointers,
        original_winner_profit_rs=winner_profit,allowed_winner_profit_sacrifice_rs=winner_profit*.10,
        minimum_original_winner_profit_rs=winner_profit*.90,choices_5pct=choices5,choices_10pct=choices10,
        selected_all=all_result,selected_later17=later_result,
        selected_oct15=comparison.loc[comparison.role.eq('10% loss-focused')&comparison.period.eq('oct15')].iloc[0].to_dict(),
        retrospective_maximum_loss_reduction=table.loc[table.policy.eq(hindsight)&table.period.eq('all')].iloc[0].to_dict(),
        adaptive_10pct=comparison.loc[comparison.role.eq('adaptive10%')].to_dict('records'),
        setup_training_decisions=setup_model['folds'].reason.value_counts().to_dict(),
        loss_focused_parameters=params,loss_focused_rule_explanation=explanation,conclusion=conclusion,
        limitations=[
            'The cap is a selection constraint. All1140 fixed minute-level exit outcomes are unchanged and reused from the independently audited sealed study. Static selected ledgers were freshly replayed and reconciled.',
            'The user requested at most10% sacrifice of original winners combined profit. This is not10% fewer winning trades, a10% overall net-profit cap, or a per-day/per-trade guarantee.',
            'The extra minimum of three affected development trades is an internal screening guard. It excludes the two-event positive candidate despite that candidate passing the economic goals.',
            'Development selection uses the first35 sessions; later17 and October15 results do not choose that policy. The explicitly labelled retrospective policy uses the full history.',
            'All these sessions were already used in G4 strategy development and prior exit research. This is reused exploratory history, not untouched future validation.',
            'Same original466-trade comparison; separate full533-order portfolio replay reports any new admissions. Fractional sizing, Rs500000 exposure and5bps round-trip costs match original G4.',
            'Protective stops activate after completed full post-entry closes. Partial exits use observed next opens and unchanged native remainders. Native gaps and ambiguous bars retain the original conservative execution rules.',
            'Extra spread, broker latency and market impact are not fully modeled. Additional5bps stress is separate; partial costs are proportional and full margin stays reserved to the original exit.',
            'Each setup has only1–21 original executions. All51 are reported, but individual setup fitting fails the predeclared minimum-data requirement.',
            'Daily-close drawdown omits intraday drawdown. Day-bootstrap intervals are descriptive and unadjusted for the large candidate search.',
            'Original G4 strategy settings, result pointers and live/paper orders are unchanged. Completed pinned history ends October15; partial October16 is excluded.'])
    core.json_write(output/'summary.json',summary)
    setup_display=setups.loc[setups.policy.eq(selected)&setups.period.eq('all'),['setup_id','trades','baseline_losses','losses','flat_trades','win_rate_pct','net_pnl_rs','net_change_rs','loss_reduction_pct','original_winner_profit_retention_pct']]
    render(output,summary,comparison,october.reset_index(drop=True),setup_display)
    for filename,digest in code.items():
        if base.sha256(Path(filename))!=digest:
            raise ValueError('Evaluated source changed')
    for filename,digest in pointers.items():
        if base.sha256(Path(filename))!=digest:
            raise ValueError('Strategy pointer changed')
    if base.sha256(SOURCE/'manifest.json')!=SOURCE_HASH or base.sha256(output/'config.json')!=config_hash:
        raise ValueError('Frozen inputs or configuration changed')
    core.json_write(output/'manifest.json',dict(variant_id=VERSION,execution_authority=False,
        files={p.relative_to(output).as_posix():base.sha256(p) for p in output.rglob('*') if p.is_file()}))
    print(conclusion,flush=True)
    print(output,flush=True)
    return output


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path)
    args=parser.parse_args()
    stamp=datetime.now(base.production.IST)
    run(args.output_dir or base.G4_ROOT/'research'/f'{stamp:%Y%m%dT%H%M%S}_{secrets.token_hex(3)}_winner_cap_10pct')
