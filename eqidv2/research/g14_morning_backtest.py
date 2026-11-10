"""Research-only G14 copy of G7 V4 Lock, restricted to six morning signals.

The source ledgers retain all selected G7 opportunities before its later slot
allowlist. Their native minute fills and V4 exits are frozen; this runner
restores original entry permissions and freshly allocates the whole portfolio
under a twelve-setup allowlist and two-realized-loss lock. No broker path exists.
"""
from __future__ import annotations

import argparse,hashlib,json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd

from research import slot_win70_backtest as source
from research.g7_v4_practical_research import execute
from research.g8_exit_engine import metrics
from research.g7_g6_replica_backtest import configuration as g7_configuration
import fno_v13_v10_g_3_contract as production

ROOT=Path(__file__).resolve().parents[1]
CONFIG=ROOT/'configs/strategies/v13_v10_g_14_backtest.json'
OLD=ROOT/'outputs/full_backtest_20261029/shared_replay'
LATER=Path('C:/TradingData/eqidv2/g3_g7_v4_through_20261102/comparison_20261102_verified')
OUTROOT=Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14')
SLOTS=('09:20','09:25','09:30','09:35','09:40','09:45')
IST=ZoneInfo('Asia/Kolkata')
PARENT_RULES=dict(nifty_futures_two_direction_filter=True,
    selected_0926_short_min_lower_wick_ratio=.20,selected_low_efficiency_veto=True,
    initial_stop_pct=1.2,tightened_stop_pct=1.,tighten_after_minutes=120,
    protect_after_completed_close_pct=.6,protected_profit_pct=.1,
    daily_realized_net_loss_limit=2,integer_cash_shares=True,
    conservative_entry_clock=True,capital_per_entry_rs=100000,portfolio_capital_rs=1000000,
    leverage=5,entry_expiry_minutes=10,ambiguous_bar='STOP_FIRST')

def inherited_gates(parent,ids):
    return dict(confirmation_volume_ratio_defaults=dict(production.MIN_CONFIRMATION_VOLUME_BY_SIDE),
        confirmation_volume_ratio_overrides={k:v for k,v in parent['minimum_confirmation_1m_volume_ratio_by_setup'].items() if k in ids},
        five_minute_volume_ratio_overrides={k:v for k,v in parent['five_minute_volume_ratio_overrides'].items() if k in ids},
        extension_atr_caps={k:v for k,v in parent['maximum_confirmation_extension_atr_by_setup'].items() if k in ids})

def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for block in iter(lambda:f.read(4*1024*1024),b''):h.update(block)
    return h.hexdigest()

def dump(p,data):Path(p).write_text(json.dumps(data,indent=2,default=str,allow_nan=False)+'\n',encoding='utf-8')

def settings():
    c=json.loads(CONFIG.read_text(encoding='utf-8'))
    specs=c['active_setups']
    if (c['scope']!='BACKTEST_ONLY' or c['execution_authority'] is not False
        or c['active_setup_count']!=12 or len(specs)!=12 or len({s['setup_id'] for s in specs})!=12
        or {(s['signal_end'],s['side']) for s in specs}!={(t,x) for t in SLOTS for x in ('LONG','SHORT')}
        or set(c['signal_to_confirmation'])!=set(SLOTS)):
        raise ValueError('G14 configuration must contain exactly six LONG and six SHORT morning setups')
    for setup in specs:
        if setup['confirmation_end']!=c['signal_to_confirmation'][setup['signal_end']]:
            raise ValueError('Confirmation clock mismatch')
    for field,path in [('g7_v3_config_sha256',ROOT/'configs/strategies/v13_v10_g_7_g6_replica_v3.json'),
                       ('g7_v4_lock_config_sha256',ROOT/'configs/strategies/v13_v10_g_7_v4_lock_backtest.json')]:
        if sha(path)!=c['parent'][field]:raise ValueError('Pinned G7 parent configuration changed')
    parent=g7_configuration()
    by_id={x['setup_id']:x for x in parent['active_setups']}
    for spec in specs:
        expected=dict(by_id[spec['setup_id']],source_version=c['variant_id'])
        if spec!=expected:raise ValueError('Cached G7 candidate exits require the copied G7 setup parameters')
    if c['preserved_parent_rules']!=PARENT_RULES or c['inherited_confirmation_gates']!=inherited_gates(parent,set(by_id).intersection(s['setup_id'] for s in specs)):
        raise ValueError('Copied G7 guards changed; full pre-quota replay is required to tune them')
    return c

def inputs():
    manifest=json.loads((OLD/'artifact_manifest.json').read_text(encoding='utf-8'))
    name='g7_v4_lock2_portfolio_trades.csv'
    if sha(OLD/name)!=manifest['files'][name]:raise ValueError('Archived G7 ledger changed')
    old=pd.read_csv(OLD/name,low_memory=False)
    extension=LATER/'g7_extension_trades.csv'
    later=pd.read_csv(extension,low_memory=False)
    expected=json.loads((LATER/'source_provenance.json').read_text(encoding='utf-8'))['days']
    old_days=sorted(old.day.astype(str).str[:10].unique())
    new_days=sorted(later.day.astype(str).str[:10].unique())
    if len(expected)!=63 or len(old_days)!=59 or len(new_days)!=4 or old_days+new_days!=expected:
        raise ValueError('G7 input calendar incomplete or out of order')
    _,source_days,base=source.source_inputs()
    if source_days!=old_days:raise ValueError('Underlying G7 source calendar mismatch')
    old['day']=old.day.astype(str).str[:10]
    later['day']=later.day.astype(str).str[:10]
    # Do not concatenate the very wide old and late frames: their optional
    # fields differ. Allocation is independent for each trading session.
    records=[(d,old.loc[old.day.eq(d)].copy().reset_index(drop=True) if d in old_days
              else later.loc[later.day.eq(d)].copy().reset_index(drop=True)) for d in expected]
    proof={'old_path':str(OLD/name),'old_sha256':sha(OLD/name),'old_manifest_sha256':sha(OLD/'artifact_manifest.json'),
           'late_path':str(extension),'late_sha256':sha(extension),
           'late_provenance_path':str(LATER/'source_provenance.json'),
           'late_provenance_sha256':sha(LATER/'source_provenance.json')}
    return records,base,proof

def allocate(book,allowed,base,policy,cost):
    actual=execute(dict(raw=book,allowed=pd.Series(allowed.to_numpy(bool),index=book.sid.astype(int)),
                        integer=True,clock=True,limit=2),base,policy,cost=cost)
    if not np.array_equal(actual.sid.to_numpy(),book.sid.to_numpy()):raise ValueError('SID order changed')
    for column in ('entry_price','exit_price','gross_return_pct'):
        if not np.allclose(pd.to_numeric(actual[column],errors='coerce'),
                           pd.to_numeric(book[column],errors='coerce'),equal_nan=True,atol=1e-8,rtol=0):
            raise ValueError('Native fill/exit changed: '+column)
    return actual

def run(out):
    out=Path(out).resolve()
    if out.exists() or not out.is_relative_to(OUTROOT.resolve()):raise ValueError('New G14 output inside G14 root required')
    c=settings();records,base,proof=inputs()
    enabled={x['setup_id'] for x in c['active_setups']}
    source_clocks={x['setup_id']:x['signal_end'] for x in g7_configuration()['active_setups']}
    out.mkdir(parents=True)
    dump(out/'config.json',c)
    daily=[];books=[];parity=[];slot_rows=[]
    for n,(day,book) in enumerate(records,1):
        if book.sid.duplicated().any() or book.duplicated(['day','setup_id','tradingsymbol']).any():
            raise ValueError('Duplicate archived G7 opportunity: '+day)
        if 'signal_end' not in book:book['signal_end']=pd.NA
        configured_clock=book.setup_id.map(source_clocks)
        if (configured_clock.isna().any() or
            book.signal_end.notna().any() and not book.loc[book.signal_end.notna(),'signal_end'].eq(configured_clock.loc[book.signal_end.notna()]).all()):
            raise ValueError('Archived source slot clock differs from sealed G7 setup')
        # A recovered historic G7 order can omit the descriptive signal_end;
        # its immutable setup ID still determines the exact five-minute slot.
        book['signal_end']=configured_clock
        if not book.signal_end.isin(SLOTS).eq(book.setup_id.isin(enabled)).all():
            raise ValueError('Source setup ID/signal time differs from G14 configuration')
        if not {'slot_policy_source_base_entry_allowed','slot_policy_allowed','portfolio_executed','portfolio_net_profit_rupees'}.issubset(book):
            raise ValueError('Missing pre-slot-policy permissions')
        original_permission=source.flags(book.slot_policy_source_base_entry_allowed)
        old_slot=source.flags(book.slot_policy_allowed)
        book['base_entry_allowed']=original_permission.to_numpy(bool)
        baseline_allowed=original_permission&old_slot
        g14_allowed=original_permission&book.setup_id.isin(enabled)
        for cost in (5.,9.,15.):
            baseline=allocate(book,baseline_allowed,base,'g7_v4_lock2_reference',cost)
            if cost==5.:
                a,b=baseline,book
                if (not np.array_equal(source.flags(a.portfolio_executed),source.flags(b.portfolio_executed))
                    or not np.allclose(pd.to_numeric(a.portfolio_net_profit_rupees).fillna(0),
                                       pd.to_numeric(b.portfolio_net_profit_rupees).fillna(0),atol=1e-5,rtol=0)):
                    raise ValueError('Saved G7 portfolio parity failed: '+day)
                parity.append({'day':day,'orders':len(book),'passed':True})
            morning=book.loc[book.setup_id.isin(enabled)].copy().reset_index(drop=True)
            permission=source.flags(morning.slot_policy_source_base_entry_allowed)
            g14=allocate(morning,permission,base,'g14_morning_12_setups',cost)
            if len(g14) and not g14.signal_end.isin(SLOTS).all():raise ValueError('Out-of-window G14 order')
            for strategy,frame in [('G7_V4_LOCK2',baseline),('V13-V10-G-14',g14)]:
                m=metrics(frame,[day]);m.update(day=day,strategy=strategy,cost_bps=cost)
                daily.append(m)
                if cost==5. and strategy=='V13-V10-G-14':
                    frame['strategy']='V13-V10-G-14'
                    frame['strategy_version']=c['variant_id']
                    frame['scope']='BACKTEST_ONLY';frame['execution_authority']=False
                    books.append(frame)
                    for (slot,side),part in frame.groupby(['signal_end','side']):
                        x=part.loc[source.flags(part.portfolio_executed)]
                        slot_rows.append(dict(day=day,signal_end=slot,side=side,orders=len(part),
                            trades=len(x),wins=int(x.portfolio_net_profit_rupees.gt(0).sum()),
                            net_pnl_rs=float(x.portfolio_net_profit_rupees.sum())))
        print(f'{day} [{n}/{len(records)}] G7 parity=PASS G14 morning opportunities={int(g14_allowed.sum())}',flush=True)
    all_daily=pd.DataFrame(daily)
    all_daily.to_csv(out/'daywise_by_cost.csv',index=False)
    selected=all_daily.loc[all_daily.cost_bps.eq(5)].pivot(index='day',columns='strategy',values='net_pnl_rs')
    selected['g14_minus_g7_rs']=selected['V13-V10-G-14']-selected['G7_V4_LOCK2']
    selected.to_csv(out/'daywise_comparison.csv',index_label='day')
    pd.concat(books,ignore_index=True,sort=False).to_csv(out/'portfolio_trades.csv',index=False)
    pd.DataFrame(slot_rows).to_csv(out/'slot_side_daily.csv',index=False)
    pd.DataFrame(parity).to_csv(out/'parent_parity.csv',index=False)
    totals=[]
    for strategy in ('G7_V4_LOCK2','V13-V10-G-14'):
        for cost in (5.,9.,15.):
            part=all_daily.loc[all_daily.strategy.eq(strategy)&all_daily.cost_bps.eq(cost)]
            trades=int(part.trades.sum());wins=int(part.wins.sum());pnl=part.net_pnl_rs
            totals.append(dict(strategy=strategy,cost_bps=cost,sessions=len(part),trades=trades,wins=wins,
                win_rate_pct=100*wins/trades if trades else 0.,net_pnl_rs=float(pnl.sum()),
                losing_days=int(pnl.lt(0).sum()),worst_day_rs=float(pnl.min()),
                daily_drawdown_rs=float((pnl.cumsum().cummax()-pnl.cumsum()).max())))
    pd.DataFrame(totals).to_csv(out/'overall_comparison.csv',index=False)
    # Verify files that were not covered by the earlier immutable manifest.
    for key in ('late_path','late_provenance_path'):
        if sha(proof[key])!=proof[key.replace('_path','_sha256')]:raise ValueError('Late source changed')
    summary={'variant_id':c['variant_id'],'parent':'G7_V4_LOCK2','scope':'BACKTEST_ONLY','execution_authority':False,
             'sessions':len(records),'first_day':records[0][0],'last_day':records[-1][0],
             'enabled_setup_ids':sorted(enabled),'parent_parity_days':len(parity),
             'source_provenance':proof,'overall':totals,
             'method':'Frozen selected G7 opportunities and native V4 exits; restore pre-slot G7 permissions; freshly reallocate integer shares, conservative entry clock, capital, same-symbol capacity and two-loss lock under G14 twelve-setup allowlist.',
             'limitation':'Cannot admit stocks not selected by the original G7 pre-quota picker. Backtest uses previously modeled one-minute fills/exits and 5/9/15 bps assumed costs. History was previously researched; no untouched holdout.'}
    dump(out/'summary.json',summary)
    dump(out/'manifest.json',{'files':{p.name:sha(p) for p in out.iterdir() if p.is_file() and p.name!='manifest.json'}})
    print(pd.DataFrame(totals).to_string(index=False),flush=True)
    return summary

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path)
    args=parser.parse_args()
    now=datetime.now(IST)
    out=args.output_dir or OUTROOT/'runs'/str(now.date())/f'{now:%Y%m%dT%H%M%S}_morning12'
    run(out)
    print(out.resolve())

if __name__=='__main__':main()
