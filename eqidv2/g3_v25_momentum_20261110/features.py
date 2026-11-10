"""Full-universe, causal price states and separately computed hindsight labels.

State generation deliberately has no dependency on G3 eligibility, OI, volume,
daily mover labels or trade outcomes. The historical universe is supplied by the
caller; this module cannot turn a retrospectively selected source into a genuine
point-in-time universe.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


IST = 'Asia/Kolkata'
OHLC = ('open', 'high', 'low', 'close')
SCAN_START = 9 * 60 + 35
SCAN_END = 14 * 60
VERSION = 'G3_V25_FULL_UNIVERSE_PRICE_STATES_V1'
STATE_COLUMNS = [
    'day', 'tradingsymbol', 'confirmation_ts', *OHLC,
    *['confirmation_' + name for name in OHLC],
    'session_open', 'session_return_pct', 'return3_pct', 'return5_pct',
    'return10_pct', 'return15_pct', 'atr14', 'ema20', 'signed_efficiency10',
    'prev2_high', 'prev2_low', 'prev5_high', 'prev5_low', 'impulse',
    'impulse_pct', 'anchor_close', 'anchor_atr', 'intervening_low',
    'intervening_high', 'ema_gap_close', 'ema_gap_min5', 'ema_gap_max5',
    'session_rank_long', 'session_rank_short', 'recent_rank_long',
    'recent_rank_short', 'universe_count',
]
MOVER_COLUMNS = [
    'day', 'symbol', 'tradingsymbol', 'side', 'rank', 'move_pct',
    'directional_move_pct', 'qualifies3pct', 'first3pct_ts', 'extreme_ts',
    'first_open', 'extreme_price', 'close_pct', 'top2', 'top5',
]


def configuration() -> dict:
    return dict(
        version=VERSION, scope='BACKTEST_ONLY', execution_authority=False,
        input='Complete observed stock-universe one-minute OHLC; no G3/OI gate',
        clocks='09:35 through14:00 inclusive, every5 completed minutes',
        session='End-labeled one-minute bars09:16 through15:15 Asia/Kolkata',
        ema20='Same-session ewm(span20,adjust=False), seeded with first close; publish after20 consecutive bars',
        atr14='Arithmetic mean of last14 true ranges; first-session TR=high-low',
        efficiency10='(close-close.shift10)/sum(abs(close.diff)) over last10 differences; flat=0',
        ranges='Previous2/5 bars, excluding the confirmation bar',
        impulse='close[t-5]-close[t-15]',
        anchor='close and ATR14 at t-5',
        intervening='Minimum low and maximum high over t-4..t-1 only',
        ema_gap_window='Minimum/maximum(close-EMA20) over t-4..t inclusive; uses same-session EMA recursion startup values, publishes only after20 bars',
        ranks='At each completed scan clock, ascending(method=average,pct=True) ranks of signed session and5-minute returns across all ready source stocks; higher is stronger',
        gap_policy='Omit a stock state after any missing minute since09:16; other stocks remain eligible',
        volume_required=False, outcomes_used_in_states=False,
        mover_labels='Separate after-close evaluation only; rank observed09:16open-to15:15high/low moves; top2/top5 also require>=3%',
    )


def _prepared(minutes: pd.DataFrame, day: str) -> pd.DataFrame:
    required = {'tradingsymbol', 'ts', *OHLC}
    if not required.issubset(minutes.columns):
        raise ValueError('V25 states require tradingsymbol, ts and minute OHLC')
    if minutes.empty:
        return pd.DataFrame(columns=['tradingsymbol', 'ts', *OHLC])
    times = pd.DatetimeIndex(pd.to_datetime(minutes.ts))
    if times.tz is None or times.hasnans:
        raise ValueError('V25 minute timestamps must be present and timezone aware')
    times = times.tz_convert(IST).as_unit('ns')
    start = pd.Timestamp(str(day)[:10] + ' 09:16', tz=IST)
    end = pd.Timestamp(str(day)[:10] + ' 15:15', tz=IST)
    keep = (times >= start) & (times <= end)
    frame = minutes.loc[keep, ['tradingsymbol', *OHLC]].copy()
    frame['ts'] = times[keep]
    if frame.empty:
        return frame
    if frame.tradingsymbol.isna().any():
        raise ValueError('Missing stock identity')
    frame['tradingsymbol'] = frame.tradingsymbol.astype(str)
    if frame.duplicated(['tradingsymbol', 'ts']).any():
        raise ValueError('Duplicate stock/minute identity')
    if not frame.ts.eq(frame.ts.dt.floor('min')).all():
        raise ValueError('Minute timestamps must be exact completed minute labels')
    frame[list(OHLC)] = frame[list(OHLC)].apply(pd.to_numeric, errors='coerce')
    numeric = frame[list(OHLC)].to_numpy(float)
    if not np.isfinite(numeric).all() or (numeric <= 0).any():
        raise ValueError('Invalid OHLC in supplied stock universe')
    if (frame.high.lt(frame[['open', 'close', 'low']].max(axis=1)).any()
            or frame.low.gt(frame[['open', 'close', 'high']].min(axis=1)).any()):
        raise ValueError('Inconsistent OHLC in supplied stock universe')
    return frame.sort_values(['tradingsymbol', 'ts'], kind='stable').reset_index(drop=True)


def build_states(minutes: pd.DataFrame, day: str) -> pd.DataFrame:
    """Return completed causal states; accept prefixes for invariance testing.

    Do not pass only future winners or admitted G3 symbols: percentile ranks
    require the complete available stock universe at every confirmation clock.
    OHLC values after a confirmation cannot enter that confirmation's fields.
    """
    frame = _prepared(minutes, day)
    if frame.empty:
        return pd.DataFrame(columns=STATE_COLUMNS)
    start = pd.Timestamp(str(day)[:10] + ' 09:16', tz=IST)
    pieces = []
    for symbol, part in frame.groupby('tradingsymbol', sort=True):
        p = part.reset_index(drop=True).copy()
        close = p.close
        difference = close.diff()
        previous = close.shift(1)
        tr = pd.concat([p.high - p.low, (p.high - previous).abs(),
                        (p.low - previous).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14, min_periods=14).mean()
        # The recursion exists from the first close. Readiness is enforced
        # separately, preserving a fully defined five-bar gap at the first scan.
        ema = close.ewm(span=20, adjust=False).mean()
        denominator = difference.abs().rolling(10, min_periods=10).sum()
        numerator = close - close.shift(10)
        efficiency = numerator.div(denominator.where(denominator.ne(0)))
        efficiency = efficiency.mask(denominator.eq(0), 0.)
        expected_offset = (p.ts - start).dt.total_seconds().div(60).to_numpy()
        contiguous = np.arange(len(p)) == expected_offset
        # Once a gap has occurred the count cannot catch up to the expected
        # offset; this is an explicit stock-only omission, never interpolation.
        minute = p.ts.dt.hour * 60 + p.ts.dt.minute
        take = (contiguous & (np.arange(len(p)) >= 19)
                & minute.between(SCAN_START, SCAN_END) & minute.mod(5).eq(0))
        p['day'] = str(day)[:10]
        p['confirmation_ts'] = p.ts
        p['session_open'] = float(p.open.iloc[0])
        p['session_return_pct'] = (close / p.session_open - 1.) * 100.
        for lag in (3, 5, 10, 15):
            p[f'return{lag}_pct'] = (close / close.shift(lag) - 1.) * 100.
        p['atr14'], p['ema20'] = atr, ema
        p['signed_efficiency10'] = efficiency
        for lag in (2, 5):
            p[f'prev{lag}_high'] = p.high.shift(1).rolling(lag, min_periods=lag).max()
            p[f'prev{lag}_low'] = p.low.shift(1).rolling(lag, min_periods=lag).min()
        p['impulse'] = close.shift(5) - close.shift(15)
        p['impulse_pct'] = (close.shift(5) / close.shift(15) - 1.) * 100.
        p['anchor_close'] = close.shift(5)
        p['anchor_atr'] = atr.shift(5)
        p['intervening_low'] = p.low.shift(1).rolling(4, min_periods=4).min()
        p['intervening_high'] = p.high.shift(1).rolling(4, min_periods=4).max()
        gap = close - ema
        p['ema_gap_close'] = gap
        p['ema_gap_min5'] = gap.rolling(5, min_periods=5).min()
        p['ema_gap_max5'] = gap.rolling(5, min_periods=5).max()
        for name in OHLC:
            p['confirmation_' + name] = p[name]
        pieces.append(p.loc[take].copy())
    if not pieces or not any(len(p) for p in pieces):
        return pd.DataFrame(columns=STATE_COLUMNS)
    result = pd.concat(pieces, ignore_index=True)
    group = result.groupby('confirmation_ts', sort=False)
    for prefix, field in [('session', 'session_return_pct'), ('recent', 'return5_pct')]:
        result[prefix + '_rank_long'] = group[field].rank(method='average', pct=True)
        result[prefix + '_rank_short'] = group[field].rank(method='average', pct=True, ascending=False)
    result['universe_count'] = group.tradingsymbol.transform('size').astype(int)
    result = result[STATE_COLUMNS].sort_values(['confirmation_ts', 'tradingsymbol'], kind='stable').reset_index(drop=True)
    if result.duplicated(['day', 'tradingsymbol', 'confirmation_ts']).any():
        raise ValueError('Duplicate V25 state identity')
    return result


def build_movers(minutes: pd.DataFrame, day: str) -> pd.DataFrame:
    """Hindsight-only labels from full sessions; never consumed by build_states."""
    frame = _prepared(minutes, day)
    if frame.empty:
        return pd.DataFrame(columns=MOVER_COLUMNS)
    expected = pd.date_range(str(day)[:10] + ' 09:16', str(day)[:10] + ' 15:15', freq='min', tz=IST)
    rows = []
    for symbol, p in frame.groupby('tradingsymbol', sort=True):
        if not pd.DatetimeIndex(p.ts).equals(expected):
            raise ValueError('Mover labels require a complete09:16–15:15 session: ' + symbol)
        opening = float(p.open.iloc[0])
        final_close = float(p.close.iloc[-1])
        for side, extreme_column, sign in [('LONG', 'high', 1), ('SHORT', 'low', -1)]:
            index = p.high.idxmax() if sign == 1 else p.low.idxmin()
            extreme = float(p.loc[index, extreme_column])
            move = (extreme / opening - 1.) * 100.
            crossing = p.loc[p.high.ge(opening * 1.03) if sign == 1 else p.low.le(opening * .97)]
            rows.append(dict(day=str(day)[:10], symbol=symbol, tradingsymbol=symbol,
                side=side, move_pct=move, directional_move_pct=sign * move,
                qualifies3pct=bool(len(crossing)),
                first3pct_ts=crossing.ts.iloc[0] if len(crossing) else pd.NaT,
                extreme_ts=p.loc[index, 'ts'], first_open=opening, extreme_price=extreme,
                close_pct=(final_close / opening - 1.) * 100.))
    result = pd.DataFrame(rows).sort_values(['side', 'directional_move_pct', 'symbol'],
        ascending=[True, False, True], kind='stable').reset_index(drop=True)
    result['rank'] = result.groupby('side', sort=False).cumcount() + 1
    result['top2'] = result.qualifies3pct & result['rank'].le(2)
    result['top5'] = result.qualifies3pct & result['rank'].le(5)
    result['first3pct_ts'] = pd.to_datetime(result.first3pct_ts, utc=True).dt.tz_convert(IST)
    return result[MOVER_COLUMNS]
