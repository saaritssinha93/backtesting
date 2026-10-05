"""Shared research harness. Original strategy and reports are never modified."""
from __future__ import annotations
import json
from pathlib import Path
import numpy as np
import pandas as pd
import sl_tradeoff_timing as timing

ROOT=Path(__file__).resolve().parent.parent
OUT=ROOT/'outputs/v13_sl_innovation_20261004'
MINUTE=timing.MINUTE
ORIGINAL_EXIT=timing.exit_path

def context():
    published,segments,days=timing.source.prepared_segments()
    return published['base'],segments,days

def summarize(ledger, summary, segments, days, rule):
    ex=ledger.loc[ledger.portfolio_executed.eq(True)].copy()
    p=ex.portfolio_net_profit_rupees.to_numpy(float)
    losses=p[p < -1e-8]
    daily=ex.assign(day=ex.day.astype(str)).groupby('day').agg(
        net=('portfolio_net_profit_rupees','sum'),trades=('portfolio_executed','sum'),
        wins=('portfolio_net_profit_rupees',lambda x:int(x.gt(1e-8).sum())))
    daily=daily.reindex(list(map(str,days)),fill_value=0)
    m=timing.source.metrics(ledger,days,rule['hard_stop'])
    m.update(rule=rule,average_loss_magnitude_rupees=float(-losses.mean()) if len(losses) else 0.,
             worst_loss_rupees=float(max(0,-p.min())),
             minute_close_drawdown_rupees=timing.mark_to_market(ledger,segments,days),
             positive_session_pct_active=100*float(daily.loc[daily.trades.gt(0),'net'].gt(1e-8).mean()),
             active_days=int(daily.trades.gt(0).sum()),
             peak_open_initial_risk_rupees=summary['peak_open_initial_risk_rupees'],
             portfolio_rejected_trades=summary['portfolio_rejected_trades'],
             equal_5250_nominal_risk_net_rupees=float(p.sum())*1.05/(rule['hard_stop']+.05),
             daily=daily.reset_index(names='day').to_dict('records'),
             daily_net=daily.net.to_dict(),exit_counts=ex.exit_reason.value_counts().to_dict())
    m['slices']={}
    for label,mask in [('jul_aug',ex.day.astype(str).lt('2026-09-01')),
                       ('september',ex.day.astype(str).ge('2026-09-01')),
                       ('late_september',ex.day.astype(str).ge('2026-09-15'))]:
        q=ex.loc[mask,'portfolio_net_profit_rupees'].to_numpy(float)
        m['slices'][label]=dict(trades=len(q),wins=int((q>1e-8).sum()),net=float(q.sum()))
    m['cost_stress']=[]
    for bps in (5,10,15):
        q=p-(bps-5)/10000*ex.exposure_per_entry_rupees.to_numpy(float)
        m['cost_stress'].append(dict(total_cost_bps=bps,net=float(q.sum()),wins=int((q>1e-8).sum()),
            win_rate_pct=100*float((q>1e-8).mean()),profit_factor=float(q[q>0].sum()/-q[q<0].sum())))
    fields=['segment','day','sid','setup_id','tradingsymbol','side','entry_ts','entry_price',
            'exit_ts','exit_execution_ts','exit_bar_end_ts','exit_event','exit_price','exit_reason',
            'portfolio_net_profit_rupees','initial_stop_pct','active_stop_pct_at_exit','holding_minutes',
            'same_bar_ambiguous','exit_gap_through']
    fields=[f for f in fields if f in ex]
    m['trades_detail']=json.loads(ex[fields].to_json(orient='records',date_format='iso'))
    return m

def evaluate(ctx, rule, exit_function=None):
    """exit_function(path, entry_index, entry, is_long, target_pct, rule) -> native 5-tuple.

    The temporary function override is local to this sequential research process.
    For custom stopping, use TIGHTENED_STOP, STOP, TARGET, TIME_EXIT_1515,
    ADVERSE_TIMER_NEXT_OPEN, or STOP_GAP_BEFORE_TIMER for consistent ledger flags.
    Return signed adverse stop distance: -0.1 means a +0.1% gross-profit stop.
    """
    base,segments,days=ctx
    try:
        if exit_function is not None:
            timing.exit_path=lambda path,entry_index,entry,is_long,target_pct,hard_stop,**kwargs: exit_function(path,entry_index,entry,is_long,target_pct,rule)
        else:
            timing.exit_path=ORIGINAL_EXIT
        ledger,summary=timing.simulate(segments,base,rule)
    finally:
        timing.exit_path=ORIGINAL_EXIT
    return summarize(ledger,summary,segments,days,rule)

def controls(ctx):
    old=json.loads((ROOT/'outputs/v13_sl_tradeoff_20261004/timing_analysis.json').read_text())
    names=['STATIC_1.00','STATIC_1.25','TIGHTEN_1.25_TO_1.00_AFTER_120M','STATIC_2.75']
    rows=[]
    for name in names:
        previous=next(r for r in old['results'] if r['rule']['name']==name)
        row=evaluate(ctx,previous['rule'])
        assert row['trades']==previous['trades']==85
        for field in ('net_profit_rupees','minute_close_drawdown_rupees','daily_close_drawdown_rupees'):
            assert abs(row[field]-previous[field])<1e-6,(name,field,row[field],previous[field])
        assert all(abs(v-previous['daily_net'][d])<1e-6 for d,v in row['daily_net'].items())
        rows.append(row)
    return rows

def save(filename, rules, controls, checks, notes):
    OUT.mkdir(parents=True,exist_ok=True)
    output=dict(status='EXPLORATORY_REUSED_HISTORY_NO_UNTOUCHED_VALIDATION',
        window=['2026-07-29','2026-09-30'],sessions=43,october_1='EXCLUDED_INCOMPLETE',
        checks=checks,notes=notes,controls=controls,results=rules)
    (OUT/filename).write_text(json.dumps(timing.clean(output),indent=2,allow_nan=False),encoding='utf-8')
    return OUT/filename
