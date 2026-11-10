"""Small causal feature set. No outcomes, fit parameters, or external writes."""
from __future__ import annotations
from collections import defaultdict, deque
import numpy as np
import pandas as pd


def add_day(raw: pd.DataFrame, history: dict) -> pd.DataFrame:
    out = raw.copy().sort_values(['tradingsymbol', 'signal_ts']).reset_index(drop=True)
    clock = out.signal_ts.dt.strftime('%H:%M')
    rvol, counts = [], []
    for row, hhmm in zip(out.itertuples(), clock):
        key = (row.tradingsymbol, hhmm)
        old = history.setdefault(key, deque(maxlen=20))
        counts.append(len(old))
        med = float(np.median(old)) if len(old) >= 5 else np.nan
        rvol.append(float(row.volume)/med if med > 0 else np.nan)
        # Each stock/clock occurs only once per session. No current observation
        # enters its own reference or the reference of another clock.
        if np.isfinite(row.volume) and row.volume >= 0:
            old.append(float(row.volume))
    out['lab_tod_rvol'] = rvol
    out['lab_tod_count'] = counts
    out['lab_or_high'] = np.nan
    out['lab_or_low'] = np.nan
    out['lab_ema20_slope_pct'] = out.groupby('tradingsymbol').ema20.pct_change(fill_method=None)*100
    for _, part in out.groupby('tradingsymbol', sort=False):
        opening = part.loc[part.signal_ts.dt.strftime('%H:%M').isin(['09:20', '09:25', '09:30'])]
        if len(opening) == 3 and opening.source_1m_count.eq(5).all():
            after = part.index[part.signal_ts.dt.strftime('%H:%M').gt('09:30')]
            out.loc[after, 'lab_or_high'] = opening.high.max()
            out.loc[after, 'lab_or_low'] = opening.low.min()
    return out


def grouped_scores(frame: pd.DataFrame) -> pd.DataFrame:
    """Fixed scales, equal groups; missing context is neutral, never a gate pass."""
    out = frame.copy()
    sign = np.where(out.side.eq('LONG'), 1., -1.)
    def num(key):
        return pd.to_numeric(out.get(key, pd.Series(np.nan, index=out.index)), errors='coerce')
    def unit(x):
        return pd.Series(x, index=out.index).clip(0, 1).fillna(.5)
    breadth = np.where(sign > 0, num('v18_breadth_above_open_pct'), num('v18_breadth_below_open_pct'))
    out['score_market'] = (unit(.5+sign*num('v18_nifty_from_open_pct')/2) + unit(breadth/100))/2
    out['score_strength'] = unit(.5+sign*num('v18_relative_strength_pct')/2)
    out['score_participation'] = (unit(num('volume_ratio')/3) + unit(num('oi_change_pct')/.3))/2
    extension = sign*(num('confirm_close')-num('v9_1m_ema20'))/num('v9_1m_atr14')
    out['lab_extension_atr'] = extension
    out['score_entry'] = (unit(num('body_ratio')) + unit(1-num('wick_ratio')) + unit(1-extension.clip(lower=0)/4))/3
    out['group_score'] = out[['score_market','score_strength','score_participation','score_entry']].mean(axis=1)
    out['market_regime'] = np.select([sign*num('v18_nifty_from_open_pct') >= .3,
        sign*num('v18_nifty_from_open_pct') <= -.3], ['aligned_strong','opposed_strong'], default='mixed_flat')
    out['clock_regime'] = np.select([out.hhmm_int <= 935, out.hhmm_int <= 1100], ['opening','morning'], default='midday')
    return out


def market_path(frame: pd.DataFrame, variant: str = 'or_market') -> pd.Series:
    sign = np.where(frame.side.eq('LONG'), 1., -1.)
    move, breadth = {'or_market':(.3,60), 'or_market_loose':(.2,55), 'or_market_strict':(.4,65)}[variant]
    directional_breadth = np.where(sign > 0, frame.v18_breadth_above_open_pct, frame.v18_breadth_below_open_pct)
    return (frame.v18_stock_ready.eq(True) & frame.v18_market_ready.eq(True)
        & frame.v18_breadth_coverage_pct.between(90,100)
        & (sign*frame.v18_nifty_from_open_pct >= move)
        & (directional_breadth >= breadth)
        & (sign*frame.v18_relative_strength_pct >= 0)
        & frame.volume_ratio.ge(1.2))
