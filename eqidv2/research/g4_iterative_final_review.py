"""Add audited, explicitly sparse-evidence findings to a sealed G4 study."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd

from research import g4_iterative_protection_refinement as refine

core,base=refine.core,refine.base
WATCH='profit_floor_020'


def run(output,tests_xml,audit_json):
    output=Path(output).resolve()
    old_manifest=base.sha256(output/'manifest.json')
    summary=core.sealed.package(output)
    if summary['variant_id']!=refine.VERSION:
        raise ValueError('Wrong completed study')
    metrics=base.read_frozen_csv(output/'comparison.csv')
    native,contexts,paths,proof=core.dataset.load(core.DEFAULT_DATA)
    del contexts
    policies={p['id']:p for p in refine.catalog()}
    stop_events={WATCH:{}}
    for row in native.to_dict('records'):
        sid=int(row['sid'])
        found=refine.stops.outcomes_for_trade(row,paths[sid],[policies[WATCH]])
        if found:
            value=found[WATCH]
            if pd.Timestamp(value['exit_ts'])!=pd.Timestamp(row['exit_ts']) or abs(value['exit_price']-row['exit_price'])>1e-10:
                stop_events[WATCH][sid]=value
    book=refine.ledger(native,paths,WATCH,policies,{},stop_events)
    book.to_csv(output/'ledgers'/f'{WATCH}.csv',index=False)
    baseline=native.set_index('sid')
    after=book.set_index('sid')
    if not after.portfolio_executed.equals(baseline.portfolio_executed):
        raise ValueError('Sparse candidate changed portfolio admissions')
    matrix=base.read_frozen_csv(output/'candidate_net_returns.csv').set_index('sid')
    executed_ids=baseline.index[baseline.portfolio_executed]
    if not np.allclose(after.loc[executed_ids,'net_profit_rupees'],matrix.loc[executed_ids,WATCH],atol=1e-6,rtol=0):
        raise ValueError('Sparse candidate ledger differs')
    trade_rows=[]
    for sid,event in stop_events[WATCH].items():
        row=baseline.loc[sid]
        if not row.portfolio_executed:
            continue
        trade_rows.append(dict(sid=sid,day=row.day,setup_id=row.setup_id,symbol=row.tradingsymbol,
            side=row.side,original_net_rs=row.portfolio_net_profit_rupees,
            protection_net_rs=after.loc[sid,'portfolio_net_profit_rupees'],
            delta_rs=after.loc[sid,'portfolio_net_profit_rupees']-row.portfolio_net_profit_rupees,
            native_target_pct=row.native_target_pct,entry_ts=row.entry_ts,
            original_exit_ts=row.exit_ts,protected_exit_ts=event['exit_ts'],
            activation_path_index=event['protection_armed_path_index'],
            effective_path_index=event['protection_effective_path_index']))
    trades=pd.DataFrame(trade_rows)
    trades.to_csv(output/'promising_candidate_trades.csv',index=False)
    stats=metrics.loc[metrics.policy.eq(WATCH)].copy()
    stats.to_csv(output/'promising_candidate_results.csv',index=False)
    matched=baseline.loc[baseline.portfolio_executed].copy()
    setup_rows=[]
    for setup in sorted(baseline.setup_id.unique()):
        members=matched.loc[matched.setup_id.eq(setup)]
        setup_rows.append(dict(setup_id=setup,policy=WATCH,**core.metrics(
            matrix.loc[members.index,'baseline'],matrix.loc[members.index,WATCH],members,proof['days'])))
    pd.DataFrame(setup_rows).to_csv(output/'promising_candidate_all51_setups.csv',index=False)
    acceptable=metrics.loc[metrics.period.eq('all') & metrics.original_winner_profit_retention_pct.ge(95.)]
    best=acceptable.sort_values(['loss_saved_rs','net_change_rs','policy'],ascending=[False,False,True]).iloc[0]
    prospective=stats.loc[stats.period.eq('all')].iloc[0].to_dict()
    if prospective['changed_trades']!=2 or prospective['original_winner_profit_retention_pct']<100.-1e-8:
        raise ValueError('Sparse candidate evidence differs from audit')
    reviewed_conclusion=('No candidate passed every prespecified screening gate, including at least three changed development trades. '
        'A small positive candidate, profit_floor_020, passes the economic goals on this history but changes only two trades (one development, one later): net profit +Rs12,000, total loss amount -Rs10,500 (2.62%), and all original winning trades unchanged. '
        'This is promising sparse evidence, not a validated improvement or drastic loss reduction. Original G4 exits remain unchanged.')
    old_conclusion=summary['conclusion']
    summary.update(conclusion=reviewed_conclusion,
        promising_sparse_candidate=dict(policy=WATCH,parameters=policies[WATCH],metrics=prospective,
            periods=stats.to_dict('records'),changed_trades=trade_rows,
            reason_not_selected='Only one changed development trade; declared minimum was three. This is an additional research evidence guard, not an economic failure.',
            full_portfolio_new_admissions=0,full_portfolio_new_rejections=0,
            selection_label='Audit-disclosed candidate; insufficient changed-trade sample and reused history'),
        retrospective_max_loss_saving_under_cap=best.to_dict(),
        post_review=dict(original_sealed_manifest_sha256=old_manifest,
            module=str(Path(__file__).resolve()),module_sha256=base.sha256(Path(__file__)),
            effect='Adds audited sparse-candidate evidence and corrects wording; no trial returns, parameters or selectors changed'))
    core.json_write(output/'summary.json',summary)
    doc=(output/'report.html').read_text(encoding='utf-8')
    import html
    doc=doc.replace(html.escape(old_conclusion),html.escape(reviewed_conclusion))
    doc=doc.replace('Qualification additionally requires higher win rate, lower total loss money, and no reduction in net profit on the development period.',
        'Additional research screening requires higher win rate, lower total loss money, no net-profit reduction, and at least three changed development trades. A policy can pass your profit cap and the economic goals while failing that extra sample-size guard.')
    show=metrics.loc[metrics.policy.isin(['baseline',WATCH]) & metrics.period.isin(['all','development35','later17']),
        ['period','policy','trades','wins','losses','win_rate_pct','negative_trade_sum_abs_rs','net_pnl_rs','net_change_rs','original_winner_profit_retention_pct','changed_trades']]
    labels={'period':'Period','policy':'Policy','trades':'Trades','wins':'Wins','losses':'Losses','win_rate_pct':'Win rate %','negative_trade_sum_abs_rs':'Total loss ₹','net_pnl_rs':'Net profit ₹','net_change_rs':'Net change ₹','original_winner_profit_retention_pct':'Original winner profit retained %','changed_trades':'Affected trades'}
    section='<section><h2>A small positive result: near-target profit stop</h2><p><strong>Net profit +₹12,000; loss amount −₹10,500; every original winner unchanged.</strong> Only two trades improve, one in each chronological period. The policy therefore fails the prespecified minimum of three affected development trades. This does not show robust or drastic loss reduction.</p><p>After a full post-entry minute closes at a profit of at least the greater of 0.40% or 90% of the original target, arm a stop protecting 0.20% gross profit from the next minute. Keep the original target and native stops. Under the original 5 bps cost model, the protected outcome is about +0.15% net before any gap/slippage.</p><div class="scroll">'+show.rename(columns=labels).to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div><div class="scroll">'+trades[['day','setup_id','symbol','side','original_net_rs','protection_net_rs','delta_rs']].to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div><p><a href="promising_candidate_all51_setups.csv">This candidate across all 51 original setups</a> · <a href="promising_candidate_trades.csv">The two changed trades</a> · <a href="ledgers/profit_floor_020.csv">Full replay ledger</a></p></section>'
    retrospective=f'<section><h2>Most optimistic retrospective loss reduction within the 5% winner-profit cap</h2><p>{html.escape(str(best.policy))}: total loss amount reduced {best.loss_reduction_pct:.2f}% (₹{best.loss_saved_rs:,.2f}), but net profit changes ₹{best.net_change_rs:,.2f}. This is chosen after looking at all 52 sessions and is not a held-out selection. Even this hindsight comparison does not show drastic loss reduction.</p></section>'
    doc=doc.replace('<section><h2>Original trades: selected policies</h2>',section+retrospective+'<section><h2>Original trades: selected policies</h2>')
    doc=doc.replace('</main>','<section><h2>Independent validation</h2><p><a href="validation.json">Validation record</a> · <a href="independent_audit.json">Independent reconstruction audit</a> · <a href="validation_tests.xml">Test results</a></p></section></main>')
    (output/'report.html').write_text(doc,encoding='utf-8')
    shutil.copyfile(tests_xml,output/'validation_tests.xml')
    shutil.copyfile(audit_json,output/'independent_audit.json')
    suites=ET.parse(tests_xml).getroot()
    suites=list(suites.iter('testsuite'))
    counts={name:sum(int(s.get(name,0)) for s in suites) for name in ('tests','failures','errors','skipped')}
    if counts['failures'] or counts['errors']:
        raise ValueError('Tests did not pass')
    test_files=sorted((base.ROOT/'tests').glob('test_g4_iterative_*.py'))
    (output/'code/tests').mkdir(exist_ok=True)
    for path in test_files:
        shutil.copyfile(path,output/'code/tests'/path.name)
    shutil.copyfile(__file__,output/'code'/Path(__file__).name)
    validation=dict(test_counts=counts,test_files={str(p):base.sha256(p) for p in test_files},
        original_trial_manifest_sha256=old_manifest,independent_audit=base.read(audit_json),
        source_strategy_unchanged=all(base.sha256(Path(p))==digest for p,digest in summary['unchanged_pointers'].items()),
        promising_candidate_full_portfolio_verified=True,monetary_results_unchanged=True)
    core.json_write(output/'validation.json',validation)
    for filename,digest in summary['code'].items():
        if base.sha256(Path(filename))!=digest:
            raise ValueError('Trial source changed before final review')
    core.json_write(output/'manifest.json',dict(version=refine.VERSION,execution_authority=False,
        files={p.relative_to(output).as_posix():base.sha256(p) for p in output.rglob('*')
               if p.is_file() and p!=output/'manifest.json'}))
    core.sealed.package(output)
    print(reviewed_conclusion)
    print(json.dumps(counts))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',required=True,type=Path)
    parser.add_argument('--tests-xml',required=True,type=Path)
    parser.add_argument('--audit-json',required=True,type=Path)
    args=parser.parse_args()
    run(args.output_dir,args.tests_xml,args.audit_json)
