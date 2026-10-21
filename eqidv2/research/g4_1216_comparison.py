"""Approved frozen 12:16 SHORT comparison; never change original G4 parameters."""
from __future__ import annotations

import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g4_g3_mirror_backtest as base

VERSION = 'G4-APPROVED-1216-COMPARISON-V1'
SETUP_ID = '1216_SHORT'
STUDY = base.G4_ROOT/'research/20261016_all49_entry_exit_v2'
STUDY_MANIFEST_SHA256 = 'f05ef5241cbf13551aaf2f41ae177d2a389af71c8c1430779af690d7bfdb70c2'
STUDY_LAST_DATE = '2026-10-16'
APPROVED_ENTRY = dict(signal_end='12:15',confirmation_end='12:16',side='SHORT',mode='FILTERED',
    max_entries=1,picker='max_liquidity',price_change_pct=.1,oi_change_pct=.0625,
    volume_ratio=1.,body_ratio=.15,max_wick_ratio=.75,min_traded_value=0.,stop_pct=1.2,
    target_pct=2.,source_version='V13-V10-G-4-BOUNDED-EXIT-51-SETUPS-V13')
APPROVED_EXIT = dict(name='stop_100',target_pct=2.,stop_pct=1.,tighten_after_minutes=120,
    tightened_stop_pct=1.,protect_after_close_pct=None,protected_profit_pct=0.)
CONFIGURATION_CHANGES = [dict(parameter='active_and_core_minimum_oi_change_pct',before=.05,after=.0625),
    dict(parameter='initial_stop_pct',before=1.2,after=1.)]


def validate_config(proposal, choice):
    """Pin the exact comparison approved on 17 October, including unchanged rules."""
    if (proposal.get('scope')!='G4_BACKTEST_ONLY' or proposal.get('setup_id')!=SETUP_ID
            or proposal.get('recommended_action')!='ADD_COMPARISON_ONLY'
            or proposal.get('original_g4_changed') is not False
            or proposal.get('target_changed') is not False
            or proposal.get('configuration_changes')!=CONFIGURATION_CHANGES
            or proposal.get('full_configuration')!=choice):
        raise ValueError('Approved comparison proposal/configuration changed')
    selected=choice.get('entry',{})
    if (choice.get('policy')!='oi_change_pct_x1.25__stop_100'
            or selected.get('setup_id')!=SETUP_ID or selected.get('name')!='oi_change_pct_x1.25'
            or selected.get('active')!=APPROVED_ENTRY or selected.get('core')!=APPROVED_ENTRY
            or selected.get('confirmation_volume')!=0. or selected.get('extension_max') is not None
            or selected.get('changes')!={'oi_change_pct':{'operation':'factor','value':1.25}}
            or choice.get('exit')!=APPROVED_EXIT):
        raise ValueError('Approved entry/exit parameters changed')


def _json(path, value):
    path.write_text(json.dumps(base.source._json_ready(value),indent=2,allow_nan=False,default=str)+'\n',encoding='utf-8')


def load_evidence():
    """Check immutable source bytes and independently reconcile saved results."""
    if base.sha256(STUDY/'manifest.json')!=STUDY_MANIFEST_SHA256:
        raise ValueError('Approved 12:16 study manifest changed')
    manifest=base.read(STUDY/'manifest.json')
    names=('approval_proposal.json','frozen_choices.json','search_plan.json',
           'baseline_trades.csv','review_1216_short_portfolio.csv','review_1216_short_comparison.csv')
    for name in names:
        if base.sha256(STUDY/name)!=manifest['files'].get(name):
            raise ValueError('Approved study artifact changed: '+name)
    proposal=base.read(STUDY/'approval_proposal.json')
    choice=base.read(STUDY/'frozen_choices.json')[SETUP_ID]
    validate_config(proposal,choice)
    plan=base.read(STUDY/'search_plan.json')
    days=plan['days']
    if len(days)!=53 or days[0]!='2026-07-29' or days[-1]!=STUDY_LAST_DATE:
        raise ValueError('Approved study calendar changed')
    before=base.read_frozen_csv(STUDY/'baseline_trades.csv')
    after=base.read_frozen_csv(STUDY/'review_1216_short_portfolio.csv')
    comparison=base.read_frozen_csv(STUDY/'review_1216_short_comparison.csv')
    fields=['setup_id','tradingsymbol','filled','entry_ts','entry_price','exit_ts','exit_price',
            'exit_reason','gross_return_pct','net_return_pct','portfolio_executed','portfolio_net_profit_rupees']
    old,new=(book.loc[~book.setup_id.eq(SETUP_ID)].set_index('sid')[fields].sort_index()
             for book in (before,after))
    pd.testing.assert_frame_equal(old,new,check_dtype=False,atol=1e-8,rtol=0)
    if any(book.sid.duplicated().any() for book in (before,after)):
        raise ValueError('Duplicate approved-study trade identity')
    from research.g4_setup_search_v2 import metrics
    periods=dict(train=plan['train'],validation=plan['validation'],
        october=plan['final_historical_diagnostic'],latest_complete_session=days[-1:],all=days)
    if len(comparison)!=10 or comparison.duplicated(['scope','period']).any():
        raise ValueError('Incomplete approved comparison')
    for scope,setup_id in [('full51_portfolio',None),(SETUP_ID,SETUP_ID)]:
        for period,cohort in periods.items():
            saved=comparison.loc[comparison.scope.eq(scope)&comparison.period.eq(period)]
            if len(saved)!=1:
                raise ValueError('Missing approved comparison period')
            for field,value in metrics(before,after,cohort,setup_id).items():
                recorded=saved.iloc[0][field]
                if value is None and pd.isna(recorded):
                    continue
                if not np.isclose(value,recorded,rtol=0,atol=1e-6):
                    raise ValueError('Approved study accounting changed: '+field)
    metadata=dict(version=VERSION,setup_id=SETUP_ID,scope='HISTORICAL_COMPARISON_ONLY',
        execution_authority=False,comparison_only=True,approved=True,approval_date='2026-10-17',
        original_parameters_changed=False,source_artifact=str(STUDY),
        source_manifest_sha256=STUDY_MANIFEST_SHA256,study_first_date=days[0],
        study_last_date=days[-1],study_sessions=len(days),configuration=choice,
        limitations=['Frozen historical study through 16 October 2026; later G4 run dates do not extend this comparison.',
            'The 18 candidate trades are insufficient to establish a stable future 75% win rate.',
            'All Rs 3,761.64 improvement occurs in training. Training win rate is 66.67%; validation and October are unchanged.',
            'Previously examined history; not a pristine holdout. Original G4 selection, targets and stops are unchanged.'])
    return dict(metadata=metadata,comparison=comparison,configurations=pd.DataFrame(CONFIGURATION_CHANGES),
                before=before,after=after)


def write_evidence(output, evidence):
    output=Path(output)
    for key,name in [('configurations','approved_1216_configuration.csv'),
            ('comparison','approved_1216_comparison.csv'),('before','approved_1216_study_baseline.csv'),
            ('after','approved_1216_study_portfolio.csv')]:
        evidence[key].to_csv(output/name,index=False)
    _json(output/'approved_1216_evidence.json',evidence['metadata'])


def report_section(evidence):
    meta=evidence['metadata']
    fields=['scope','period','before_trades','trades','before_win_rate_pct','win_rate_pct',
            'before_net_pnl_rs','net_pnl_rs','net_delta_rs','winner_retention_pct']
    return ('<section id="approved-1216"><h2>Approved 12:16 SHORT historical comparison</h2>'
        f'<p>Study: {html.escape(meta["study_first_date"])} through {html.escape(meta["study_last_date"])} '
        f'({meta["study_sessions"]} sessions). Comparison only; original G4 parameters and primary results remain unchanged. '
        'This frozen study is separate from the current run; it does not evaluate subsequent sessions.</p>'
        '<p>Candidate: active/core minimum OI change 0.0625%, initial stop 1.00%, unchanged target 2.00%. '
        'All improvement occurs in training; training win rate 66.67%. Validation and October results are unchanged.</p>'
        +evidence['configurations'].to_html(index=False,border=0)
        +'<div style="overflow:auto">'+evidence['comparison'][fields].to_html(index=False,border=0,float_format=lambda v:f'{v:,.2f}')+'</div>'
        '<p><a href="approved_1216_configuration.csv">Comparison parameters</a> | '
        '<a href="approved_1216_comparison.csv">Before/after results</a> | '
        '<a href="approved_1216_study_portfolio.csv">Study candidate trades</a> | '
        '<a href="approved_1216_evidence.json">Approval and study provenance</a></p></section>')


def verify_evidence(output, metadata):
    saved=base.read(Path(output)/'approved_1216_evidence.json')
    if (saved!=metadata or saved.get('source_manifest_sha256')!=STUDY_MANIFEST_SHA256
            or saved.get('scope')!='HISTORICAL_COMPARISON_ONLY'
            or saved.get('original_parameters_changed') is not False
            or saved.get('study_last_date')!=STUDY_LAST_DATE):
        raise ValueError('Approved 12:16 evidence scope changed')


def run(output):
    """Publish the approved comparison alone without changing any G4 pointer."""
    output=Path(output).resolve()
    if output.exists() or not output.is_relative_to(base.G4_ROOT.resolve()):
        raise ValueError('Fresh isolated G4 comparison output required')
    evidence=load_evidence()
    output.mkdir(parents=True)
    write_evidence(output,evidence)
    _json(output/'summary.json',evidence['metadata'])
    (output/'report.html').write_text('<!doctype html><html><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1"><title>G4 approved 12:16 comparison</title>'
        '<style>body{font:15px/1.6 system-ui;max-width:1500px;margin:30px auto;padding:18px;background:#101927;color:#edf4ff}'
        'a{color:#8ed6ff}table{border-collapse:collapse;font-size:13px}td,th{padding:8px;border-bottom:1px solid #476078}</style>'
        '</head><body><h1>G4 backtesting: approved comparison</h1>'+report_section(evidence)+'</body></html>',encoding='utf-8')
    _json(output/'manifest.json',dict(version=VERSION,code_sha256={str(Path(__file__).resolve()):base.sha256(Path(__file__))},
        files={p.relative_to(output).as_posix():base.sha256(p) for p in output.rglob('*') if p.is_file()}))
    verify_evidence(output,evidence['metadata'])
    return evidence['metadata']
