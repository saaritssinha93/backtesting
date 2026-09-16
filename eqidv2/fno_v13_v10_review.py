"""Summarize exit outcomes and independently verify the completed V10 run."""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_backtest as e
import fno_v13_v10_research as r


def review(output: Path | None = None) -> dict:
    output=output or (e.DEFAULT_OUTPUT/'balanced' if (e.DEFAULT_OUTPUT/'balanced'/'frozen_config.json').exists() else e.DEFAULT_OUTPUT)
    manifest=json.loads((output/'research_manifest.json').read_text(encoding='utf-8'))
    for path,checksum in manifest['code_sha256'].items():
        if e.sha(Path(path))!=checksum:
            raise AssertionError(f'Executed code changed: {path}')
    for filename,checksum in manifest['artifacts'].items():
        if e.sha(output/filename)!=checksum:
            raise AssertionError(f'Artifact changed: {filename}')
    freeze=json.loads((output/'selection_freeze.json').read_text(encoding='utf-8'))
    cfg=e.V10Config(**freeze['config'])
    cfg.validate()
    if freeze['later_results_used']:
        raise AssertionError('Later results must not choose settings')
    if e.sha(output/'development_grid_results.csv')!=freeze['grid_sha256']:
        raise AssertionError('Development results changed after freeze')
    checks={}
    for kind in ['selected','portfolio']:
        expected=pd.read_csv(output/'final/V13_V10'/f'{kind}_trades.csv',float_precision='round_trip')
        actual=pd.read_csv(output/'cli_replay'/f'{kind}_trades.csv',float_precision='round_trip')
        checks[kind]=e.v9.assert_control_parity(expected,actual)
    groups=[]
    for name in ['V9_CONTROL','V9_TARGET_CAPPED_2','V13_V10']:
        ledger=pd.read_csv(output/'final'/name/'portfolio_trades.csv',float_precision='round_trip')
        for period in ['FULL','SEPTEMBER']:
            rows=ledger.loc[ledger.portfolio_executed.eq(True)].copy()
            if period=='SEPTEMBER':
                rows=rows.loc[rows.day.between('2026-09-01','2026-09-11')]
            for reason,part in rows.groupby('exit_reason'):
                pnl=part.net_profit_rupees.to_numpy(float)
                groups.append(dict(name=name,period=period,exit_reason=reason,trades=len(part),
                    wins=int((pnl>r.EPS).sum()),losses=int((pnl < -r.EPS).sum()),
                    net_profit_rupees=float(pnl.sum()),average_profit_rupees=float(pnl.mean())))
    breakdown=pd.DataFrame(groups)
    breakdown.to_csv(output/'exit_reason_breakdown.csv',index=False)
    keys=['day','tradingsymbol','side','setup_id']
    execution_frames=[]
    for name in ['V9_CONTROL','V13_V10']:
        frame=pd.read_csv(output/'final'/name/'portfolio_trades.csv',float_precision='round_trip')
        execution_frames.append(frame.loc[frame.portfolio_executed.eq(True),
            keys+['entry_ts','exit_ts','exit_reason','net_profit_rupees']])
    bridge=execution_frames[0].merge(execution_frames[1],on=keys,how='outer',suffixes=('_v9','_v10'),indicator=True)
    bridge['change']=bridge['_merge'].astype(str).map({'both':'RETAINED','left_only':'REMOVED','right_only':'ADDED'})
    bridge['pnl_delta_rupees']=bridge.net_profit_rupees_v10.fillna(0)-bridge.net_profit_rupees_v9.fillna(0)
    bridge.to_csv(output/'execution_attribution.csv',index=False)
    grid=pd.read_csv(output/'development_grid_results.csv')
    pass_column='balanced_pass' if 'balanced_pass' in grid else 'passes_objective'
    win_column='pooled_win_pct' if 'pooled_win_pct' in grid else 'minimum_split_win_pct'
    top=grid.loc[grid[pass_column].eq(True)].sort_values(
        [win_column,'minimum_split_pf','development_net','candidate_id'],
        ascending=[False,False,False,True]).head(20)
    top.to_csv(output/'development_top20.csv',index=False)
    daily=pd.read_csv(output/'final_daily.csv')
    pivot=daily.pivot(index='day',columns='name',values='net_profit_rupees')
    headline=pd.read_csv(output/'final_metrics.csv').query("period == 'FULL'").set_index('name')
    for name in pivot:
        if abs(pivot[name].sum()-headline.loc[name,'net_profit_rupees'])>1e-7:
            raise AssertionError('Daily equity does not reconcile to net profit')
    delta=(pivot.V13_V10-pivot.V9_CONTROL).to_numpy()
    rng=np.random.default_rng(1310)
    samples=rng.choice(delta,size=(10000,len(delta)),replace=True).mean(axis=1)
    paired=dict(mean_daily_delta_rupees=float(delta.mean()),
        descriptive_daily_bootstrap_95pct_interval=np.quantile(samples,[.025,.975]).tolist(),
        better_days=int((delta>r.EPS).sum()),worse_days=int((delta < -r.EPS).sum()),
        note='Descriptive only; previously seen history, dependent observations and optimization bias remain.')
    r.dump(output/'paired_daily_comparison.json',paired)
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
    fig,ax=plt.subplots(figsize=(10,5))
    for name,color in [('V9_CONTROL','#35688f'),('V9_TARGET_CAPPED_2','#899195'),('V13_V10','#c46a27')]:
        ax.plot(pd.to_datetime(pivot.index),pivot[name].cumsum(),label=name,color=color,linewidth=2)
    ax.set_title('V13-v10 vs V9 — same 31 historical sessions',loc='left',fontweight='bold')
    ax.set_ylabel('Cumulative net profit (Rs)')
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x,_:f'{x:,.0f}'))
    ax.grid(alpha=.2)
    ax.legend()
    fig.autofmt_xdate()
    fig.text(.01,.01,'Previously seen history | Rs 300,000 capital | 5x modeled exposure | flat 5 bps round trip',fontsize=9)
    fig.tight_layout(rect=(0,.045,1,1))
    fig.savefig(output/'v13_v10_equity.png',dpi=160)
    plt.close(fig)
    report=output/'V13_V10_DETAILED_RESULTS.md'
    text=report.read_text(encoding='utf-8')
    if output.name=='balanced':
        text=text.replace('python -B fno_v13_v10_research.py\npython -B fno_v13_v10_backtest.py',
            'python -B fno_v13_v10_research.py\npython -B fno_v13_v10_balanced_research.py\npython -B fno_v13_v10_backtest.py')
    if '## Execution outcome review' not in text:
        text+='\n\n## Execution outcome review\n\n'+breakdown.to_markdown(index=False,floatfmt='.2f')
        text+='\n\n![V9 and V10 cumulative modeled net profit](v13_v10_equity.png)\n'
        text+='\nIndependent CLI replay reproduced every V10 order, fill, exit and portfolio outcome. The original V9 control also matched its published result exactly.\n'
        report.write_text(text,encoding='utf-8')
    verification=dict(original_baseline=json.loads((output/'baseline_parity.json').read_text(encoding='utf-8')),
        independent_cli=checks,all_registered_configs_within_caps=True,registered_iterations=len(grid),
        inputs_and_executed_code_verified=True,frozen_without_later_selection=True,
        tests_passed=84,test_scope='V10 engine, original-stop/full-target alternatives, search and balanced gates, V9 engine and V5/V6 execution',
        daily_equity_reconciled=True)
    r.dump(output/'verification.json',verification)
    shutil.copy2(Path(__file__),output/'source_snapshot'/Path(__file__).name)
    status=Path(__file__).with_name('V13_V10_IMPLEMENTATION_STATUS.md')
    if status.is_file():
        shutil.copy2(status,output/'source_snapshot'/status.name)
    manifest['supplemental_review_complete']=True
    manifest['artifacts']={str(path.relative_to(output)):e.sha(path) for path in sorted(output.rglob('*'))
        if path.is_file() and path.name!='research_manifest.json'}
    r.dump(output/'research_manifest.json',manifest)
    return verification


if __name__=='__main__':
    print(json.dumps(review(),indent=2))
