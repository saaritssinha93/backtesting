"""Readable review of a sealed indicator experiment; cannot select a default."""
from __future__ import annotations
import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import pandas as pd
from research import g7_v4_indicator_iteration as study


def run(source):
    source=Path(source).resolve()
    study.verify_members(source)
    summary=study.base.read(source/'summary.json')
    table=pd.read_csv(source/'comparison.csv')
    dev=table.loc[table.period.eq('development') & table.policy.ne('g7_v4')]
    # Representative is chosen only from development; it does not override the lock.
    representative=dev.sort_values(
        ['net_pnl_rs','negative_trade_sum_abs_rs','policy'],ascending=[False,True,False]).iloc[0].policy
    focus=summary['selected_candidate'] or representative
    out=study.OUT_ROOT/'reviews'/study.base.FINAL_DAY/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
    out.mkdir(parents=True,exist_ok=False)
    allrows=table.loc[table.period.eq('all')].copy()
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,axes=plt.subplots(1,2,figsize=(15,8),constrained_layout=True)
    y=np.arange(len(allrows))
    colors=['#345d83' if n=='g7_v4' else '#188b87' if n in study.ROUND1 else '#d88b39' for n in allrows.policy]
    for ax,col,title in [(axes[0],'net_pnl_rs','Net profit after5bps costs'),(axes[1],'negative_trade_sum_abs_rs','Amount lost on losing trades')]:
        ax.barh(y,allrows[col]/100000,color=colors)
        baseline=float(allrows.loc[allrows.policy.eq('g7_v4'),col].iloc[0])/100000
        ax.axvline(baseline,color='#203349',ls='--',lw=1,label='Protected G7 V4 baseline')
        ax.set_yticks(y,allrows.policy);ax.invert_yaxis();ax.set_xlabel('INR lakh');ax.set_title(title)
        ax.grid(axis='x',alpha=.2);ax.set_axisbelow(True);ax.legend(fontsize=9)
    fig.suptitle('Two indicator stages: all59 available sessions through27 October2026',fontsize=14)
    fig.savefig(out/'indicator_comparison.png',dpi=140);plt.close(fig)
    wide=pd.read_csv(source/'october_daywise_all_candidates.csv').set_index('day')
    names=list(dict.fromkeys(['g7_v4',focus,'vwap_rsi','risk_confirm_next']))
    display=wide[names].copy()
    display.loc['TOTAL']=display.sum()
    display.to_csv(out/'october_daywise_focus.csv')
    diagnostic=pd.read_csv(source/'development_reversal_diagnostics.csv')
    audits=pd.read_csv(source/'development_selection.csv')
    audit=audits.loc[audits.policy.eq(focus)].iloc[0]
    b=table.set_index(['policy','period']).loc[('g7_v4','all')]
    a=table.set_index(['policy','period']).loc[(focus,'all')]
    show=lambda f:'<div class="scroll">'+f.to_html(index=False,float_format=lambda v:f'{v:,.2f}')+'</div>'
    html='''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G7 V4 indicator findings</title><style>body{font:16px/1.5 system-ui;margin:30px;max-width:1550px;color:#21344a}img{max-width:100%}.scroll{overflow:auto}table{border-collapse:collapse;font-size:13px}td,th{padding:8px;border:1px solid #d0d8e1;text-align:right}.finding{background:#eef5f9;border-left:5px solid #426d91;padding:18px}a{color:#185f96}</style></head><body>'''
    html+='<h1>G7 V4: detailed indicator findings</h1><p class="finding">'
    html+=('No candidate qualified on the fixed development screen.' if summary['selected_candidate'] is None else 'Development selected '+focus+'.')
    html+=' Existing protected G7 V4 remains the default. No paper/live changes.</p>'
    html+='<p>The baseline includes the requested gross+0.1% protection after a completed-close+0.6% move, one active position per symbol, unchanged setup targets and5bps costs. Ten candidates were predefined and disclosed across two stages.</p>'
    html+='<img src="indicator_comparison.png" alt="All candidate net profits and losses compared with protected V4">'
    html+='<h2>All available history: totals</h2>'+show(allrows[['policy','trades','wins','losses','win_rate_pct','net_pnl_rs','net_change_rs','negative_trade_sum_abs_rs','loss_amount_reduction_rs','daily_close_drawdown_rs']])
    html+='<h2>Development-chosen representative: '+focus+'</h2><p>'
    html+=('This rule passed the development screen. ' if audit.eligible else 'This is the highest-development-net candidate; it is an explanatory comparison and did not qualify under the fixed screen. ')
    html+=f'Across all59 sessions its net change is Rs{a.net_pnl_rs-b.net_pnl_rs:,.2f}; money saved on losing trades is Rs{b.negative_trade_sum_abs_rs-a.negative_trade_sum_abs_rs:,.2f}; winners change by {int(a.wins-b.wins):+d}, losers by {int(a.losses-b.losses):+d}.</p>'
    html+='<p>Development gate: '+str(audit.failure_reasons if pd.notna(audit.failure_reasons) else 'PASSED')+'. The later-period results do not select a replacement.</p>'
    html+=show(table.loc[table.policy.isin(['g7_v4',focus]) & table.period.isin(['development','validation','assessment','known_oct26','final_oct27','october']),['policy','period','trades','wins','losses','net_pnl_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs']])
    html+='<h2>Development diagnostics: do the indicators identify bad entries?</h2><p>Flagged and unflagged groups are measured within the same protected V4 executed trades on the35 development dates. Higher loss/reversal rates in flagged groups can motivate a hypothesis, but are not proof: individual groups can be small and missed winners still matter.</p>'
    html+=show(diagnostic.loc[diagnostic.indicator.isin(['vwap_rsi','historical_rsi','rolling_divergence','chop_rejection','low_efficiency','exhaustion_volume','consensus2'])])
    html+='<h2>October: totals for every candidate</h2>'+show(table.loc[table.period.eq('october'),['policy','trades','wins','losses','net_pnl_rs','net_change_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs']])
    html+='<h2>October daywise</h2><p>Includes baseline, the development-chosen representative and predefined VWAP/RSI and delayed-confirmation comparisons. The CSV in the source report includes all ten variants. Dates with missing source data are not assigned zero.</p>'+show(display.reset_index())
    costs=pd.read_csv(source/'cost_sensitivity.csv')
    html+='<h2>Cost sensitivity for the representative</h2>'+show(costs.loc[costs.policy.isin(['g7_v4',focus]) & costs.period.isin(['all','october']),['policy','period','cost_bps','wins','losses','net_pnl_rs','negative_trade_sum_abs_rs']])
    html+='<h2>Confidence and limits</h2><p>All59 dates were already inspected in previous research. These are retrospective comparisons, not independent forward validation. The inherited after-close universe and previously inspected setup targets remain limitations.15/30bps results are stresses, not a complete broker execution model. Repeated indicator experimentation can fit historical noise even when individual feature calculations are causal.</p>'
    html+='<p><a href="october_daywise_focus.csv">October focused CSV</a> | <a href="'+os.path.relpath(source/'report.html',out).replace('\\','/')+'">Full ten-candidate report, formulas and files</a></p></body></html>'
    (out/'review.html').write_text(html,encoding='utf-8')
    study.dump(out/'source.json',dict(source_output=str(source),source_manifest_sha256=study.sha(source/'manifest.json'),
        representative=representative,selected_candidate=summary['selected_candidate'],default_changed=False))
    study.base.seal(out,code={str(Path(__file__).relative_to(study.ROOT)):study.sha(Path(__file__))})
    study.verify_members(out)
    print('FOCUS',focus,'REVIEW',out,flush=True)
    print(display.to_string(),flush=True)
    return out


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('source',type=Path)
    run(parser.parse_args().source)
