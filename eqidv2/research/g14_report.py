"""Summarize the completed G14 morning run without changing its trades."""
from pathlib import Path
import argparse,json
import numpy as np
import pandas as pd
from research.g14_morning_backtest import CONFIG,SLOTS,OUTROOT,sha,dump

def run(root):
    root=Path(root).resolve()
    if not root.is_relative_to(OUTROOT.resolve()):raise ValueError('G14 output only')
    cfg=json.loads(CONFIG.read_text(encoding='utf-8'))
    daily=pd.read_csv(root/'daywise_by_cost.csv')
    slots=pd.read_csv(root/'slot_side_daily.csv')
    trades=pd.read_csv(root/'portfolio_trades.csv',low_memory=False)
    summary=json.loads((root/'summary.json').read_text(encoding='utf-8'))
    if (len(daily)!=63*2*3 or len(slots)==0 or
        not trades.signal_end.isin(SLOTS).all() or
        set(trades.setup_id)-set(summary['enabled_setup_ids'])):
        raise ValueError('G14 execution/slot inventory differs from reported scope')
    spec={(x['signal_end'],x['side']):x for x in cfg['active_setups']}
    slot_table=slots.groupby(['signal_end','side'],as_index=False)[['orders','trades','wins','net_pnl_rs']].sum()
    reference=pd.DataFrame([dict(signal_end=t,side=s) for t in SLOTS for s in ('LONG','SHORT')])
    slot_table=reference.merge(slot_table,on=['signal_end','side'],how='left',validate='one_to_one').fillna(0)
    for field in ('orders','trades','wins'):slot_table[field]=slot_table[field].astype(int)
    slot_table['win_rate_pct']=np.where(slot_table.trades.gt(0),100*slot_table.wins/slot_table.trades,0.)
    slot_table['setup_id']=[spec[(r.signal_end,r.side)]['setup_id'] for r in slot_table.itertuples()]
    slot_table['target_pct']=[spec[(r.signal_end,r.side)]['target_pct'] for r in slot_table.itertuples()]
    slot_table['stop_pct']=[spec[(r.signal_end,r.side)]['stop_pct'] for r in slot_table.itertuples()]
    slot_table.to_csv(root/'slot_side_summary.csv',index=False)
    if (slot_table.trades.sum()!=int(daily.loc[daily.strategy.eq('V13-V10-G-14')&daily.cost_bps.eq(5),'trades'].sum())
        or not np.isclose(slot_table.net_pnl_rs.sum(),
                          daily.loc[daily.strategy.eq('V13-V10-G-14')&daily.cost_bps.eq(5),'net_pnl_rs'].sum(),atol=1e-6)):
        raise ValueError('Slot totals do not reconcile to daily portfolio')
    d=daily.loc[daily.cost_bps.eq(5)].copy();d['month']=d.day.str[:7]
    monthly=d.groupby(['month','strategy'],as_index=False).agg(days=('day','nunique'),trades=('trades','sum'),
        wins=('wins','sum'),net_pnl_rs=('net_pnl_rs','sum'),losing_days=('net_pnl_rs',lambda v:int(v.lt(0).sum())))
    monthly['win_rate_pct']=np.where(monthly.trades.gt(0),100*monthly.wins/monthly.trades,0.)
    monthly.to_csv(root/'monthly_comparison.csv',index=False)
    numbers=pd.read_csv(root/'overall_comparison.csv').query('cost_bps==5').set_index('strategy')
    g7,v14=numbers.loc['G7_V4_LOCK2'],numbers.loc['V13-V10-G-14']
    narrative=f'''# V13-V10-G-14: morning-only G7 V4 Lock copy

The new [configuration]({CONFIG.as_posix()}) has exactly **12 active setups**: LONG and SHORT at **09:20, 09:25, 09:30, 09:35, 09:40 and 09:45**. It removes the other **39 G7 setups** from V14's allowed entries. The confirmation clocks are 09:21 through 09:46 respectively. Each target, stop and other entry threshold is copied from its G7 setup. G7 V4's 09:26 SHORT lower-wick minimum, Nifty-futures direction rule, low-efficiency veto, profit protection, two realized-loss lock, integer-share sizing and capital limits are retained.

The completed backtest covers **63 represented sessions, 29 July–2 November 2026**. The parent G7 V4 Lock was freshly reallocated each day and matched its archived 5-bps ledger on **63/63 days**. V14 restores the parent's original pre-allowlist permissions, admits only these 12 setups, and freshly reallocates capital and the loss lock. The original minute fills and V4 exit outcomes are carried from the hash-verified G7 opportunity ledgers; no new pre-quota stock selection or new price-path simulation is claimed.

At **5 bps assumed costs**, V14 produced **{int(v14.trades)} trades**, **{v14.win_rate_pct:.2f}%** win rate and **₹{v14.net_pnl_rs:,.2f}** net P&L. G7 V4 Lock produced **{int(g7.trades)} trades**, **{g7.win_rate_pct:.2f}%** win rate and **₹{g7.net_pnl_rs:,.2f}** net P&L on the same dates. V14 therefore earned **₹{v14.net_pnl_rs-g7.net_pnl_rs:,.2f}** less while taking {int(g7.trades-v14.trades)} fewer trades. V14 had **{int(v14.losing_days)} losing days** versus G7's **{int(g7.losing_days)}**; its worst day was **₹{v14.worst_day_rs:,.2f}** versus **₹{g7.worst_day_rs:,.2f}**, and daily close drawdown **₹{v14.daily_drawdown_rs:,.2f}** versus **₹{g7.daily_drawdown_rs:,.2f}**.

See [daily comparison](daywise_comparison.csv), [daily results at 5/9/15 bps](daywise_by_cost.csv), [slot/side totals](slot_side_summary.csv), [monthly comparison](monthly_comparison.csv), [individual portfolio trades](portfolio_trades.csv), [parent parity](parent_parity.csv) and [all-period comparison](overall_comparison.csv).

Reproduce from the repository root with `python -B fno_v13_v10_g_14_backtest.py --output-dir C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14/runs/NEW_UNIQUE_DIR`, then `python -B -m research.g14_report --results C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14/runs/NEW_UNIQUE_DIR`. The output directory must be new. No LIVE/PAPER files or existing G7 results are modified.

These sessions and G7 slot rules were researched previously, so this is an exploratory comparison. Source cohorts have historical coverage exclusions. Fills use completed one-minute bars and stop-first ambiguity; assumed costs do not establish executable spreads or market impact. V14 cannot evaluate candidates the parent G7 picker never selected.
'''
    (root/'README.md').write_text(narrative,encoding='utf-8')
    dump(root/'manifest.json',{'files':{p.name:sha(p) for p in root.iterdir() if p.is_file() and p.name!='manifest.json'},
                               'strategy_code_sha256':sha(Path(__file__).parent/'g14_morning_backtest.py'),
                               'report_code_sha256':sha(Path(__file__))})
    print(slot_table.to_string(index=False))
    print(root/'README.md')

def main():
    p=argparse.ArgumentParser();p.add_argument('--results',type=Path,required=True);a=p.parse_args();run(a.results)

if __name__=='__main__':main()
