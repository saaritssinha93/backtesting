"""Separate recovery-room experiment; earlier tighter-stop results stay sealed."""
from __future__ import annotations
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106 import run as original,policy as admission
from g3_v23_adx_rsi_20261106.loss_control_20261106 import inputs
from g3_v23_adx_rsi_20261106.loss_control_20261106.reporting import compare_days,_day_table,_metrics_table,trade_effects
from g3_v23_adx_rsi_20261106.keep_entries_20261106.exit_study import run as native
from g3_v23_adx_rsi_20261106.through_20261105.run import assert_entries
from ..run import choose,promotion,metric,stress
from . import engine

PACKAGE=Path(__file__).resolve().parent
OUTPUT=PACKAGE/'results_through_20261105'
BASELINE='baseline_v21_g7'
LABELS={BASELINE:'Baseline:1.25%, tighten to1% after120 minutes',
        'constant_1p25':'Keep1.25% from entry throughout',
        'delay_tighten240':'1.25% from entry; tighten to1% only after240 minutes',
        'constant_1p50':'Keep1.50% from entry throughout (higher initial risk)'}


def replay_book(data,name,settings):
    frozen=data['trades']; lookup=data['parity'].set_index('sid')
    book=frozen.copy(); changes=[]; parity=[]
    for idx,row in frozen.iterrows():
        meta=lookup.loc[row.sid]; path=data['paths'][int(row.sid)]
        native.verify_path(path,int(meta.entry_index))
        result=engine.replay(path,int(meta.entry_index),float(row.entry_price),row.side=='LONG',
                              float(meta.native_target_pct),settings)
        stamp=native.exit_timestamp(path,result)
        if stamp<pd.Timestamp(row.exit_ts):
            raise ValueError('A looser stop unexpectedly exited earlier than baseline')
        unchanged=stamp==pd.Timestamp(row.exit_ts) and abs(result['price']-row.exit_price)<1e-8
        pnl=float(row.portfolio_net_profit_rupees) if unchanged else (
            (1 if row.side=='LONG' else -1)*(result['price']/row.entry_price-1)*500000-250)
        if abs(pnl)<1e-7:
            pnl=0.
        if name==BASELINE:
            derived=(1 if row.side=='LONG' else -1)*(result['price']/row.entry_price-1)*500000-250
            if not unchanged or result['reason']!=row.exit_reason or not np.isclose(derived,row.portfolio_net_profit_rupees,atol=1e-5,rtol=0):
                raise ValueError('Native baseline parity failed')
            parity.append(dict(sid=int(row.sid),day=row.day,native_parity=True))
        # This is an audit after replay, never a condition inside the strategy.
        if row.portfolio_net_profit_rupees>0 and (not unchanged or abs(pnl-row.portfolio_net_profit_rupees)>1e-7):
            raise ValueError('A baseline winner changed under a strictly looser stop')
        book.loc[idx,'exit_ts']=str(stamp); book.loc[idx,'exit_price']=result['price']
        book.loc[idx,'exit_reason']=result['reason']; book.loc[idx,'portfolio_net_profit_rupees']=pnl
        before=float(row.portfolio_net_profit_rupees)
        changes.append(dict(policy=name,sid=int(row.sid),day=row.day,baseline_pnl_rs=before,
            candidate_pnl_rs=pnl,pnl_change_rs=pnl-before,exit_changed=not unchanged,
            exit_reason=result['reason'],candidate_exit_ts=str(stamp),
            holding_extension_minutes=(stamp-pd.Timestamp(row.exit_ts)).total_seconds()/60,
            baseline_winner_harmed=before>0 and pnl<before-1e-7,
            baseline_loser_improved=before<0 and pnl>before+1e-7,
            winner_turned_loss=before>0 and pnl<0,loser_turned_win=before<0 and pnl>0,
            same_bar_ambiguous=result['ambiguous']))
    assert_entries(frozen,book)
    return book,pd.DataFrame(changes),pd.DataFrame(parity)


def report(output,overall,daily,changes,config):
    selected=config['selected_policy']; diagnostic=config['comparison_candidate']
    baseline=daily.loc[daily.policy.eq(BASELINE)]
    for name,stem,title in [(selected,'DAY_WISE_RESULTS','Current entry-SL result'),
                            (diagnostic,'CANDIDATE_DAY_WISE_RESULTS','Recovery-room comparison')]:
        compared=compare_days(baseline,daily.loc[daily.policy.eq(name)])
        csv='day_wise_results.csv' if stem=='DAY_WISE_RESULTS' else 'candidate_day_wise_results.csv'
        compared.to_csv(output/csv,index=False)
        status='Selected in this research result' if name==selected else 'Exploratory, not selected'
        (output/(stem+'.md')).write_text(_day_table(compared,title,
            f'Policy: **{name}**. {status}. All entry prices and all790 entries remain fixed.'),encoding='utf-8')
    full=overall.loc[overall.period.eq('full_66')].set_index('policy')
    lines=['# Entry-relative SL: preserving recovery room','',
        '**Separate follow-up to the seven tighter-stop tests.** Those tests increased total loss amounts. '
        'This study asks whether keeping the original entry stop in place longer avoids premature exits. '
        'It also tests one1.50% stop with explicitly higher initial risk. All four rules apply to all790 entries; '
        'the engine never knows which trades will be winners or losers.','',
        'LONG SL = actual entry × (1 − stop% /100); SHORT SL = actual entry × (1 + stop% /100). '
        'Targets remain unchanged and every position closes by15:15. All73 pairs through14:00 remain configured.','',
        '## Full66 sessions through5 November2026','']
    lines+=_metrics_table([(LABELS[name],full.loc[name]) for name in engine.CANDIDATES])
    lines+=['',f'Fit-selected candidate: **{config["training_candidate"]}**. '
        f'Current research policy: **{selected}**. Qualification: **{config["promotion_pass"]}**.','']
    if config['rejection_reasons']:
        lines+=['No new rule is adopted because: '+ '; '.join(config['rejection_reasons'])+'.','']
    else:
        lines+=['The candidate met the declared historical profit, loss and drawdown gates. '
                'This is retrospective evidence, not a future guarantee.','']
    rows=[]
    for period in ('fit_44','validation_15','later_7','nov05'):
        part=overall.loc[overall.period.eq(period)].set_index('policy')
        for name in dict.fromkeys([BASELINE,diagnostic]):
            rows.append((period+' / '+name,part.loc[name]))
    lines+=['## Period checks','']+_metrics_table(rows)
    lines+=['','Only the first44 sessions select a candidate. The following15 and final7 sessions gate that '
        'one candidate separately; a failed candidate is not replaced using later results. All history '
        'was previously observed in other research, so none is an untouched forward holdout.','',
        '## What changed in trades','',
        '| Policy | Baseline winners changed | Baseline losers improved | Losers turned wins | Trades held longer | Worst trade (Rs) |',
        '|:--|--:|--:|--:|--:|--:|']
    for name in engine.CANDIDATES:
        part=changes.loc[changes.policy.eq(name)]
        lines.append(f'| {name} | {int((part.baseline_pnl_rs.gt(0)&part.exit_changed).sum())} | '
            f'{int(part.baseline_loser_improved.sum())} | {int(part.loser_turned_win.sum())} | '
            f'{int(part.holding_extension_minutes.gt(0).sum())} | {full.loc[name,"worst_trade_rs"]:,.2f} |')
    lines+=['','Wider stops preserve the original modelled winners here because they cannot trigger earlier '
        'and targets are unchanged. Some original losers can lose more; the report compares all of them, '
        'not only the recoveries. Holding longer can also increase intraday exposure.','',
        '## Risk and data limits','',
        '- Keep1.25%: nominal initial stop loss remains Rs6,250 before Rs250 modelled costs. '
        'The original two-hour tightening is delayed or removed; time exposed to the initial risk increases.',
        '- Keep1.50%: nominal initial stop loss rises to Rs7,500 before Rs250 costs:20% higher price risk '
        'and Rs1,250 additional loss per entry. Price gaps can exceed both nominal limits.',
        '- Fixed exposure Rs500,000 per entry, unlimited concurrent capital; all790 entries and Rs197,500 '
        'modelled costs preserved. Extra changed-exit slippage is reported separately at0/1/2/5bps.',
        '- The inherited conservative entry-bar and stop-first assumptions remain. One-minute OHLC '
        'does not reveal exact intrabar sequencing or real bid/ask execution. Daily-close drawdown '
        'does not measure intraday drawdown.',
        '- Yesterday means5 November; the strategy session is complete through15:15. SAIL retains its '
        'source day exclusion and KAYNES its opening09:20 exclusion. October1 remains excluded from older history.',
        '- LIVE execution, approvals and scheduling are unchanged.','',
        '[Current day-wise results](DAY_WISE_RESULTS.md) · [Candidate day-wise results](CANDIDATE_DAY_WISE_RESULTS.md) · '
        '[All daily rows](daily.csv) · [Trade changes](trade_changes.csv) · [Slippage sensitivity](cost_stress.csv) · '
        '[Per-entry stop levels](stop_levels.csv) · [Configuration](config.json) · '
        '[Earlier tighter-stop study](../../results_through_20261105/REPORT.md)','']
    (output/'REPORT.md').write_text('\n'.join(lines),encoding='utf-8')


def run(output=OUTPUT):
    output=Path(output).resolve()
    if output.exists() or output==PACKAGE or not output.is_relative_to(PACKAGE):
        raise ValueError('Use a fresh recovery-room result directory')
    protocol=json.loads((PACKAGE/'protocol.json').read_text())
    if set(protocol['policies'])!=set(engine.CANDIDATES):
        raise ValueError('Fixed recovery-room hypothesis inventory changed')
    data=inputs.load();days=data['days']
    periods=dict(full_66=days,fit_44=days[:44],validation_15=days[44:59],later_7=days[59:],nov05=[days[-1]])
    counts=data['audit'].loc[admission.flag(data['audit'].all_g7_allowed)].groupby('day').size().to_dict()
    baseline,change,parity=replay_book(data,BASELINE,engine.CANDIDATES[BASELINE])
    output.mkdir(parents=True)
    (output/'predeclared_plan.json').write_text(json.dumps(dict(protocol=protocol,candidates=engine.CANDIDATES,
        engine_sha256=original.sha(engine.__file__)),indent=2)+'\n')
    books={BASELINE:baseline};differences=[change]
    for name,settings in engine.CANDIDATES.items():
        if name==BASELINE:continue
        book,change,_=replay_book(data,name,settings)
        books[name]=book;differences.append(change)
        print('Completed fixed recovery-room policy: '+name,flush=True)
    changes=pd.concat(differences,ignore_index=True)
    overall=[];daily=[]
    for name,book in books.items():
        for period,cohort in periods.items():
            values=metric(book,sum(counts.get(d,0) for d in cohort),cohort)
            values['worst_trade_rs']=float(book.loc[book.day.isin(cohort),'portfolio_net_profit_rupees'].min())
            overall.append(dict(policy=name,period=period,**values))
        for day in days:
            daily.append(dict(policy=name,day=day,**metric(book,counts.get(day,0),[day])))
    overall,daily=pd.DataFrame(overall),pd.DataFrame(daily)
    for name in books:
        full=overall.loc[overall.policy.eq(name)&overall.period.eq('full_66')].iloc[0]
        part=daily.loc[daily.policy.eq(name)]
        for key in ('trades','wins','losses','net_pnl_rs','gross_loss_rs','costs_rs'):
            if not np.isclose(full[key],part[key].sum(),atol=1e-5,rtol=0):
                raise ValueError('Daily/full result discrepancy')
    candidate,diagnostic=choose(overall);passed,reasons=promotion(overall,candidate)
    selected=candidate if passed else BASELINE
    config=dict(scope='BACKTEST_ONLY',execution_authority=False,live_changed=False,
        selected_policy=selected,training_candidate=candidate,comparison_candidate=diagnostic,
        promotion_pass=passed,rejection_reasons=reasons,definitions=LABELS,policy_settings=engine.CANDIDATES,
        historydays=days,periods=periods,selected_orders=891,filled_trades=790,
        baseline_winners_unchanged=535,notional_per_entry_rs=500000,cost_per_entry_rs=250,
        risk_note=protocol['risk_note'],retrospective_only=True,no_later_period_reselection=True)
    for name,book in books.items():book.to_csv(output/(name+'_trades.csv'),index=False)
    books[selected].to_csv(output/'selected_trades.csv',index=False)
    overall.to_csv(output/'overall.csv',index=False);daily.to_csv(output/'daily.csv',index=False)
    changes.to_csv(output/'trade_changes.csv',index=False)
    parity.to_csv(output/'baseline_parity.csv',index=False)
    stress(books,changes,periods).to_csv(output/'cost_stress.csv',index=False)
    levels=[]
    for name,settings in engine.CANDIDATES.items():
        frame=data['trades'][['sid','day','tradingsymbol','side','entry_price','entry_ts']].copy()
        frame['policy']=name;frame['initial_stop_pct']=settings['initial_stop_pct']
        frame['initial_stop_price']=frame.entry_price*(1-np.where(frame.side.eq('LONG'),1,-1)*frame.initial_stop_pct/100)
        levels.append(frame)
    pd.concat(levels,ignore_index=True).to_csv(output/'stop_levels.csv',index=False)
    (output/'config.json').write_text(json.dumps(config,indent=2)+'\n')
    report(output,overall,daily,changes,config)
    (output/'validation.json').write_text(json.dumps(dict(native_parity=790,same_entries=790,
        same_native_winners=535,sessions=66,no_missing_daily_rows=True,costs_rs=197500),indent=2)+'\n')
    from .. import run as first_phase
    from g3_v23_adx_rsi_20261106.loss_control_20261106 import reporting as helper
    code=[Path(__file__),PACKAGE/'engine.py',PACKAGE/'protocol.json',Path(first_phase.__file__),Path(helper.__file__)]
    (output/'provenance.json').write_text(json.dumps(dict(code_sha256={str(p):original.sha(p) for p in code},
        source_manifests=data['source_manifests']),indent=2)+'\n')
    (output/'manifest.json').write_text(json.dumps({p.name:original.sha(p) for p in output.iterdir() if p.is_file()},indent=2)+'\n')
    print(overall[['period','policy','trades','wins','losses','net_pnl_rs','gross_loss_rs','daily_close_drawdown_rs','worst_trade_rs']].to_string(index=False),flush=True)
    print('FIT_CANDIDATE='+candidate+' CURRENT='+selected+' PROMOTED='+str(passed),flush=True)
    return config


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=OUTPUT)
    run(parser.parse_args().output)
