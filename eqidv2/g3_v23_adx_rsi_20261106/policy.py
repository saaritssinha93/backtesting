"""Fixed V23 hypotheses. Admission uses completed-bar features only, never P&L."""
from __future__ import annotations

import numpy as np
import pandas as pd

VERSION = 'G3-V23-ADX-RSI-20261106-RESEARCH'
DESCRIPTIONS = {
    'baseline_v21_g7': 'Unchanged V21 plus all G7 entry confirmations',
    'adx25_di_rsi50': 'ADX14 >= 25 AND directional DI AND directional RSI midpoint',
    'rising_adx_di_rsi': 'Non-falling 3-minute ADX AND directional DI AND non-weakening 3-minute RSI',
    'early_or_established': 'Directional DI AND ((rising ADX AND rising RSI) OR (ADX >= 25 AND directional RSI midpoint))',
    'balanced_or': 'Directional DI AND ((rising ADX AND directional RSI midpoint) OR (ADX >= 25 AND rising RSI))',
    'balanced_or_rsi_guard': 'Balanced OR plus LONG RSI <= 75 / SHORT RSI >= 25',
    'di_rsi_weakness_veto': 'Directional DI AND directional RSI midpoint AND NOT (ADX falling AND RSI weakening)',
    'rsi_momentum': 'Directional RSI midpoint AND non-weakening 3-minute RSI',
    'adx20_di_rsi': 'ADX14 >= 20 AND directional DI AND non-weakening 3-minute RSI',
}
SELECTION = dict(
    selection_period='early_59', training_cutoff='2026-10-27',
    minimum_trade_retention=.50, minimum_net_profit_retention=.75,
    minimum_win_rate_gain_pp=.50, require_lower_gross_loss=True,
    require_no_worse_daily_close_drawdown=True,
    ranking=['win_rate_pct DESC', 'daily_close_drawdown_rs ASC', 'net_pnl_rs DESC', 'policy ASC'],
    fallback='baseline_v21_g7',
    note='Fixed before this V23 run; later-period outcomes never select the policy.')


def flag(series):
    text = series.astype(str).str.strip().str.lower()
    if not text.isin(['true', 'false']).all():
        raise ValueError('Missing or invalid Boolean feature flag')
    return text.eq('true')


def masks(frame):
    """Unavailable warm-up indicators preserve the existing G7 decision.

    Readiness flags are explicit. A ready but NaN indicator is rejected as bad
    source data, not silently treated as a warm-up. No feature time may exceed
    the confirmation timestamp. The source uses session-reset Wilder14.
    """
    x = frame.copy()
    for name in ('all_g7_allowed', 'adx_ready', 'adx_delta3_ready', 'rsi_ready'):
        x[name] = flag(x[name])
    if not x.side.isin(['LONG', 'SHORT']).all():
        raise ValueError('Unknown trade direction')
    if (x.adx_delta3_ready & ~x.adx_ready).any():
        raise ValueError('ADX slope cannot be ready before ADX')
    for value, ready in [('adx14', 'adx_ready'), ('plus_di14', 'adx_ready'),
                         ('minus_di14', 'adx_ready'), ('adx_delta3', 'adx_delta3_ready'),
                         ('rsi14', 'rsi_ready'), ('signed_rsi_delta3', 'rsi_ready')]:
        x[value] = pd.to_numeric(x[value], errors='coerce')
        if not np.isfinite(x.loc[x[ready], value]).all():
            raise ValueError('Ready feature is not finite: ' + value)
        if value in ('adx14', 'plus_di14', 'minus_di14', 'rsi14'):
            if not x.loc[x[ready], value].between(0, 100).all():
                raise ValueError('Indicator outside its valid range: ' + value)
    confirmation = pd.to_datetime(x.confirmation_ts, utc=True)
    for name in ('adx_available_at', 'feature_available_at'):
        observed = pd.to_datetime(x[name], utc=True)
        if observed.isna().any() or confirmation.isna().any() or not observed.le(confirmation).all():
            raise ValueError('Indicator is not available at confirmation')
    long = x.side.eq('LONG')
    direction = ~x.adx_ready | ((long & x.plus_di14.gt(x.minus_di14)) | (~long & x.minus_di14.gt(x.plus_di14)))
    midpoint = ~x.rsi_ready | ((long & x.rsi14.ge(50)) | (~long & x.rsi14.le(50)))
    strength25 = ~x.adx_ready | x.adx14.ge(25)
    strength20 = ~x.adx_ready | x.adx14.ge(20)
    rising_adx = ~x.adx_delta3_ready | x.adx_delta3.ge(0)
    rising_rsi = ~x.rsi_ready | x.signed_rsi_delta3.ge(0)
    not_extreme = ~x.rsi_ready | ((long & x.rsi14.le(75)) | (~long & x.rsi14.ge(25)))
    early_or_established = direction & ((rising_adx & rising_rsi) | (strength25 & midpoint))
    balanced = direction & ((rising_adx & midpoint) | (strength25 & rising_rsi))
    raw = {
        'baseline_v21_g7': pd.Series(True, index=x.index),
        'adx25_di_rsi50': strength25 & direction & midpoint,
        'rising_adx_di_rsi': rising_adx & direction & rising_rsi,
        'early_or_established': early_or_established,
        'balanced_or': balanced,
        'balanced_or_rsi_guard': balanced & not_extreme,
        'di_rsi_weakness_veto': direction & midpoint & (rising_adx | rising_rsi),
        'rsi_momentum': midpoint & rising_rsi,
        'adx20_di_rsi': strength20 & direction & rising_rsi,
    }
    return {name: (x.all_g7_allowed & raw[name]).astype(bool) for name in DESCRIPTIONS}


def select_policy(overall):
    """Only earlier-session metrics participate in selection."""
    early = overall.loc[overall.period.eq('early_59')].copy()
    base = early.loc[early.policy.eq('baseline_v21_g7')].iloc[0]
    eligible = early.loc[
        early.policy.ne('baseline_v21_g7')
        & early.trades.ge(base.trades * SELECTION['minimum_trade_retention'])
        & early.net_pnl_rs.ge(base.net_pnl_rs * SELECTION['minimum_net_profit_retention'])
        & early.win_rate_pct.ge(base.win_rate_pct + SELECTION['minimum_win_rate_gain_pp'])
        & early.gross_loss_rs.lt(base.gross_loss_rs)
        & early.daily_close_drawdown_rs.le(base.daily_close_drawdown_rs)]
    if eligible.empty:
        return 'baseline_v21_g7'
    return eligible.sort_values(['win_rate_pct', 'daily_close_drawdown_rs', 'net_pnl_rs', 'policy'],
                                ascending=[False, True, False, True]).iloc[0].policy
