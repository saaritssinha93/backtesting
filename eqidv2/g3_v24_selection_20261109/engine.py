"""Reuse V23 entry simulation and its adopted 80%-target breakeven exits."""
import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as native
from g3_v18_20261102.data import execution_paths
from g3_v23_adx_rsi_20261106.target_trailing_20261106 import engine as exits
from g3_v23_adx_rsi_20261106.keep_entries_20261106.exit_study.run import exit_timestamp

SETTINGS=exits.CANDIDATES['breakeven_eighty_target']
COLUMNS=['sid','day','setup_id','tradingsymbol','side','entry_ts','exit_ts','entry_price',
         'exit_price','exit_reason','portfolio_net_profit_rupees']


def simulate(orders, minutes):
    if orders.empty:
        return pd.DataFrame(columns=COLUMNS),pd.DataFrame(columns=['sid','filled'])
    paths=execution_paths(orders,minutes)
    book=native.simulate_staged(orders,paths,cost_bps=5.,max_entry_delay_minutes=10)
    rows=[]
    for order in book.loc[book.filled.eq(True)].itertuples(index=False):
        path=paths[int(order.sid)]
        if len(path['timestamp_ns'])==0 or pd.Timestamp(int(path['timestamp_ns'][-1]),tz='UTC').tz_convert('Asia/Kolkata').strftime('%H:%M')!='15:15':
            raise ValueError('Execution path lacks exact square-off bar')
        atr=getattr(order,'v24_atr14_1m',np.nan)
        # The frozen cushion exit is completely ATR-independent. Its shared
        # API requires a positive placeholder when selection features are
        # unavailable. This sentinel never influences prices or admission.
        atr_available=bool(np.isfinite(atr) and atr>0)
        result=exits.replay(path,int(order.entry_path_index),float(order.entry_price),
            order.side=='LONG',float(order.native_target_pct),SETTINGS,float(atr) if atr_available else 1.)
        pnl=(1 if order.side=='LONG' else -1)*(result['price']/order.entry_price-1)*500000-250
        if abs(pnl)<1e-7:pnl=0.
        row={key:getattr(order,key) for key in ['sid','day','setup_id','tradingsymbol','side','entry_price']}
        row.update(entry_ts=str(order.entry_ts),exit_ts=str(exit_timestamp(path,result)),
            exit_price=result['price'],exit_reason=result['reason'],portfolio_net_profit_rupees=pnl,
            native_target_pct=float(order.native_target_pct),initial_stop_pct=1.5,
            stop_update_count=result['stop_update_count'],same_bar_ambiguous=result['ambiguous'],
            exposure_rs=500000.,cost_rs=250.,entry_atr_available=atr_available)
        rows.append(row)
    return pd.DataFrame(rows,columns=list(rows[0]) if rows else COLUMNS),book[['sid','filled']].copy()
