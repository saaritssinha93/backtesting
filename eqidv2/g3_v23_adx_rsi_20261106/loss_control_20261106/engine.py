"""Fixed causal loss-management hypotheses on confirmation-anchored paths.

EMA9/20, Wilder RSI14 and efficiency ratio ER10 consume only the completed
prefix of the execution path, which starts after confirmation. These are not
full-session indicators. New decisions activate on the next bar; entry-bar
extrema never activate a guard. The original entry and native exit conventions
are retained, including stop-first ambiguous candles and adverse opening gaps.
"""
from __future__ import annotations

import numpy as np

MINUTE_NS = 60_000_000_000
BASELINE = 'baseline_v21_g7'
RECOVERY = dict(min_mae_pct=.80, recovered_return_pct=-.10, stop_return_pct=-.25)
PROFIT_GUARD = dict(min_mfe_pct=.60, min_return_pct=.15, max_directional_rsi=50., stop_return_pct=.10)
CANDIDATES = {
    BASELINE: {},
    'sustained_loss_trend20': {
        'loss_trend': dict(min_minutes=20, max_return_pct=-.60, max_signed_er=-.35, consecutive=3)},
    'deep_loss_failure15': {
        'deep_loss': dict(min_minutes=15, max_return_pct=-.90, max_signed_er=-.25, max_directional_rsi=40.)},
    'stalled_loser45': {
        'stalled': dict(min_minutes=45, max_return_pct=-.40, max_mfe_pct=.20, max_abs_er=.20)},
    'recovery_guard': {'recovery': dict(RECOVERY)},
    'trend_failure_profit_guard': {'profit_guard': dict(PROFIT_GUARD)},
    'late_loss_trend90': {
        'late_loss': dict(min_minutes=90, max_return_pct=-.30, max_directional_rsi=45.)},
    'recovery_and_profit_guard': {'recovery': dict(RECOVERY), 'profit_guard': dict(PROFIT_GUARD)},
}
FEATURE_KEYS = ('ema9', 'ema20', 'ema9_slope3', 'rsi14', 'er10')


def build_features(path):
    """Return prefix-only indicators with NaN until explicit warm-up completes."""
    close = np.asarray(path['close'], dtype=float)
    if close.ndim != 1 or not len(close) or not np.isfinite(close).all() or not (close > 0).all():
        raise ValueError('Invalid completed close path')
    n = len(close)
    features = {key: np.full(n, np.nan, dtype=float) for key in FEATURE_KEYS}
    for span in (9, 20):
        ema = float(close[0])
        alpha = 2. / (span + 1)
        for j, value in enumerate(close):
            if j:
                ema = (1 - alpha) * ema + alpha * float(value)
            if j >= span - 1:
                features['ema' + str(span)][j] = ema
    features['ema9_slope3'][3:] = features['ema9'][3:] - features['ema9'][:-3]
    delta = np.diff(close)
    for j in range(10, n):
        denominator = float(np.abs(delta[j - 10:j]).sum())
        features['er10'][j] = (close[j] - close[j - 10]) / denominator if denominator else 0.
    if n > 14:
        gain = float(np.maximum(delta[:14], 0).mean())
        loss = float(np.maximum(-delta[:14], 0).mean())
        for j in range(14, n):
            if j > 14:
                gain = (gain * 13 + max(float(delta[j - 1]), 0.)) / 14
                loss = (loss * 13 + max(float(-delta[j - 1]), 0.)) / 14
            features['rsi14'][j] = (100 * gain / (gain + loss)) if gain + loss else 50.
    return features


def _validate_features(features, count):
    if set(features) != set(FEATURE_KEYS):
        raise ValueError('Unexpected prefix feature schema')
    if any(np.asarray(features[key]).shape != (count,) for key in FEATURE_KEYS):
        raise ValueError('Feature/path length mismatch')


def replay(path, entry_index, entry, is_long, target_pct, settings, features=None):
    """Replay one unchanged native fill with next-bar loss-management decisions.

    Full-position exits only: no position scaling, extra entries, targets or
    fees are introduced. Returned event semantics match the native simulator.
    """
    permitted = {'loss_trend', 'deep_loss', 'stalled', 'recovery', 'profit_guard', 'late_loss'}
    if not set(settings).issubset(permitted):
        raise ValueError('Unknown fixed loss-management condition')
    count = len(path['close'])
    if not 0 <= entry_index < count or entry <= 0 or target_pct <= 0:
        raise ValueError('Invalid native entry or target')
    if settings:
        features = build_features(path) if features is None else features
        _validate_features(features, count)
    sign = 1 if is_long else -1
    target = entry * (1 + sign * target_pct / 100)
    entry_end = int(path['timestamp_ns'][entry_index])
    guard_return, guard_reason = -np.inf, ''
    pending_market = ''
    max_favorable, max_adverse = 0., 0.
    sustained = 0
    for j in range(entry_index, count):
        at = int(path['timestamp_ns'][j])
        native_stop = 1.0 if at - MINUTE_NS >= entry_end + 120 * MINUTE_NS else 1.25
        stop_return = -native_stop
        reason = 'TIGHTENED_STOP' if native_stop < 1.25 else 'STOP'
        if guard_return > stop_return:
            stop_return, reason = guard_return, guard_reason
        stop = entry * (1 + sign * stop_return / 100)
        op, hi, lo, close = [float(path[key][j]) for key in ('open', 'high', 'low', 'close')]
        stop_open = op <= stop if is_long else op >= stop
        stop_hit = lo <= stop if is_long else hi >= stop
        target_hit = hi >= target if is_long else lo <= target
        if pending_market and j > entry_index:
            return dict(index=j, price=op, reason=reason if stop_open else pending_market,
                        event='OPEN', ambiguous=False)
        if stop_hit:
            open_fill = j > entry_index and stop_open
            return dict(index=j, price=op if open_fill else stop, reason=reason,
                        event='OPEN' if open_fill else 'INTRABAR',
                        ambiguous=bool(target_hit and not open_fill))
        if target_hit:
            return dict(index=j, price=target, reason='TARGET', event='INTRABAR', ambiguous=False)
        if not settings or j == entry_index:
            continue
        # Everything below is evaluated after the bar has completed. Any guard
        # or market decision becomes active only at the next loop iteration.
        favorable = sign * ((hi if is_long else lo) / entry - 1) * 100
        adverse = -sign * ((lo if is_long else hi) / entry - 1) * 100
        max_favorable = max(max_favorable, favorable)
        max_adverse = max(max_adverse, adverse)
        close_return = sign * (close / entry - 1) * 100
        age = (at - entry_end) / MINUTE_NS
        ema9, ema20, slope9, rsi, er = [float(features[key][j]) for key in FEATURE_KEYS]
        directional_rsi = rsi if is_long else 100 - rsi
        signed_er = sign * er
        below_ema9 = np.isfinite(ema9) and sign * (close - ema9) < 0
        falling_ema9 = np.isfinite(slope9) and sign * slope9 <= 0
        trend = settings.get('loss_trend')
        if trend:
            condition = (age >= trend['min_minutes'] and close_return <= trend['max_return_pct']
                         and np.isfinite(er) and signed_er <= trend['max_signed_er']
                         and below_ema9 and falling_ema9)
            sustained = sustained + 1 if condition else 0
            if sustained >= trend['consecutive']:
                pending_market = 'SUSTAINED_LOSS_TREND_EXIT'
        deep = settings.get('deep_loss')
        if deep and (age >= deep['min_minutes'] and close_return <= deep['max_return_pct']
                     and np.isfinite(er) and signed_er <= deep['max_signed_er']
                     and np.isfinite(rsi) and directional_rsi <= deep['max_directional_rsi']
                     and below_ema9):
            pending_market = 'DEEP_LOSS_TREND_EXIT'
        stalled = settings.get('stalled')
        if stalled and (age >= stalled['min_minutes'] and close_return <= stalled['max_return_pct']
                        and max_favorable < stalled['max_mfe_pct']
                        and np.isfinite(er) and abs(er) <= stalled['max_abs_er']):
            pending_market = 'STALLED_LOSS_EXIT'
        recovery = settings.get('recovery')
        if recovery and (max_adverse + 1e-10 >= recovery['min_mae_pct']
                         and close_return + 1e-10 >= recovery['recovered_return_pct']):
            if recovery['stop_return_pct'] > guard_return:
                guard_return, guard_reason = recovery['stop_return_pct'], 'RECOVERY_GUARD'
        protect = settings.get('profit_guard')
        if protect and (max_favorable + 1e-10 >= protect['min_mfe_pct']
                        and close_return + 1e-10 >= protect['min_return_pct']
                        and np.isfinite(rsi) and directional_rsi < protect['max_directional_rsi']
                        and np.isfinite(slope9) and sign * slope9 < 0):
            if protect['stop_return_pct'] > guard_return:
                guard_return, guard_reason = protect['stop_return_pct'], 'TREND_FAILURE_PROFIT_GUARD'
        late = settings.get('late_loss')
        if late and (age >= late['min_minutes'] and close_return <= late['max_return_pct']
                     and np.isfinite(ema9) and np.isfinite(ema20)
                     and sign * (ema9 - ema20) < 0 and sign * (close - ema20) < 0
                     and np.isfinite(rsi) and directional_rsi <= late['max_directional_rsi']):
            pending_market = 'LATE_LOSS_TREND_EXIT'
    return dict(index=count - 1, price=float(path['close'][-1]), reason='TIME_EXIT_1515',
                event='CLOSE', ambiguous=False)
