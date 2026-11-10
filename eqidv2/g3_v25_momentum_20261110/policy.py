"""Frozen full-universe momentum rules. No outcome fields enter admission."""
from __future__ import annotations
import numpy as np
import pandas as pd

FAMILIES = ('breakout_control','leader_breakout','leader_pullback','leader_union')
TARGETS = (0.8, 1.5)
MAX_PER_SIDE_DAY = 6


def signals(states):
    outputs = []
    for side, sign, suffix in [('LONG',1,'long'),('SHORT',-1,'short')]:
        x = states.copy()
        x['side'] = side
        x['rank_score'] = (x['session_rank_'+suffix]+x['recent_rank_'+suffix])/2
        x['directional_candle'] = sign*(x.close-x.open)>0
        x['broad_pass'] = ((x.close>x.prev5_high) if sign==1 else (x.close<x.prev5_low)) & x.directional_candle & (sign*x.return5_pct>0)
        x['leader_pass'] = x['session_rank_'+suffix]>=.8
        x['efficiency_pass'] = sign*x.signed_efficiency10>=.5
        extension = sign*(x.close-x.ema20)/x.atr14
        x['extension_pass'] = extension.between(0,2)
        x['extension_atr'] = extension
        impulse = sign*x.impulse
        depth = x.anchor_close-x.intervening_low if sign==1 else x.intervening_high-x.anchor_close
        x['impulse_pass'] = impulse>=x.anchor_atr
        x['pullback_depth_pass'] = (depth>=.25*x.anchor_atr) & (depth<=x.anchor_atr) & (depth<=.5*impulse)
        x['trend_hold_pass'] = (x.ema_gap_min5>=0) if sign==1 else (x.ema_gap_max5<=0)
        x['reclaim_pass'] = ((x.close>x.prev2_high) if sign==1 else (x.close<x.prev2_low)) & (sign*x.return3_pct>0) & x.directional_candle
        x['breakout_control'] = x.broad_pass
        x['leader_breakout'] = x.broad_pass & x.leader_pass & x.efficiency_pass & x.extension_pass
        x['leader_pullback'] = x.leader_pass & x.extension_pass & x.impulse_pass & x.pullback_depth_pass & x.trend_hold_pass & x.reclaim_pass
        x['leader_union'] = x.leader_breakout | x.leader_pullback
        outputs.append(x)
    out = pd.concat(outputs,ignore_index=True)
    if out.duplicated(['day','tradingsymbol','side','confirmation_ts']).any():
        raise ValueError('Duplicate full-universe signal identity')
    return out


def select(signal_frame, family):
    if family not in FAMILIES:
        raise ValueError('Unknown frozen family')
    x = signal_frame.copy().reset_index(drop=True)
    x['reason'] = np.where(x[family],'CLOCK_RANK','SIGNAL_NOT_MET')
    selected = []
    for (_,side), daily in x.groupby(['day','side'],sort=True):
        used = set()
        count = 0
        passing = daily.loc[daily[family]]
        for _, group in passing.groupby('confirmation_ts',sort=True):
            for idx in group.index:
                if x.at[idx,'tradingsymbol'] in used:
                    x.at[idx,'reason'] = 'REPEAT_SYMBOL_SIDE'
            group = group.loc[~group.tradingsymbol.isin(used)].sort_values(
                ['rank_score','tradingsymbol'],ascending=[False,True],kind='stable')
            if count>=MAX_PER_SIDE_DAY:
                x.loc[group.index,'reason']='DAILY_CAP'
                continue
            if not group.empty:
                idx = group.index[0]
                x.at[idx,'reason']='SELECTED'
                selected.append(idx)
                used.add(x.at[idx,'tradingsymbol'])
                count += 1
    orders = x.loc[selected].copy().sort_values(['day','confirmation_ts','side','tradingsymbol']).reset_index(drop=True)
    orders['setup_id'] = 'V25_'+family+'_'+orders.side
    for name in ('open','high','low','close'):
        orders['confirmation_'+name] = orders[name]
    # Root assigns globally deterministic SIDs using the source day index.
    keys=['day','tradingsymbol','side']
    flags=['broad_pass','leader_pass','efficiency_pass','extension_pass','impulse_pass',
           'pullback_depth_pass','trend_hold_pass','reclaim_pass']
    agg=x.groupby(keys,sort=True).agg(scan_clocks=('confirmation_ts','size'),signal_clocks=(family,'sum'))
    for flag in flags:
        agg[flag+'_clocks']=x.groupby(keys,sort=True)[flag].sum()
    reasons=pd.crosstab([x[k] for k in keys],x.reason)
    for reason in ['SIGNAL_NOT_MET','CLOCK_RANK','REPEAT_SYMBOL_SIDE','DAILY_CAP','SELECTED']:
        agg[reason.lower()+'_clocks']=reasons.get(reason,0)
    first=x.loc[x[family]].groupby(keys).confirmation_ts.min()
    admitted=orders.groupby(keys).confirmation_ts.min()
    agg['first_signal_ts']=first
    agg['first_admitted_ts']=admitted
    agg['family']=family
    return orders,agg.reset_index()
