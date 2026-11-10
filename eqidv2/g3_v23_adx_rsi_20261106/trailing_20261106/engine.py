"""Ten fixed trailing hypotheses against the constant1.50% entry-stop baseline.

The runner variants disable the original profit target from entry. Indicators
use only the completed execution-path prefix after confirmation, not a full
session or overnight definition. ATR warm-up uses the verified ATR known at
confirmation. New stop/market decisions become active on the next bar.
"""
from __future__ import annotations

import numpy as np

MINUTE_NS = 60_000_000_000
BASELINE = 'fixed_1p50'
COST_BREAK_EVEN_PCT = .05
CANDIDATES = {
    BASELINE: dict(mode='fixed', target='native', initial_stop_pct=1.50),
    'fixed_1p50_runner': dict(mode='fixed', target='runner', initial_stop_pct=1.50),
    'breakeven_1r_runner': dict(mode='breakeven', target='runner', initial_stop_pct=1.50),
    'step_r_runner': dict(mode='step', target='runner', initial_stop_pct=1.50),
    'chandelier3_runner': dict(mode='chandelier', target='runner', initial_stop_pct=1.50),
    'swing_be_runner': dict(mode='swing', target='runner', initial_stop_pct=1.50),
    'ema20_runner': dict(mode='ema20', target='runner', initial_stop_pct=1.50),
    'hybrid_runner': dict(mode='hybrid', target='runner', initial_stop_pct=1.50),
    'hybrid_native_target': dict(mode='hybrid', target='native', initial_stop_pct=1.50),
    'hybrid_adaptive_runner': dict(mode='hybrid', target='runner', initial_stop_pct=1.50,
                                   adaptive_initial=True),
}
FEATURE_KEYS = ('atr14', 'ema9', 'ema20', 'er10', 'pivot_low', 'pivot_high',
                'pivot_low_index', 'pivot_high_index')


def build_features(path):
    """Prefix-only simple ATR14, EMA9/20, ER10 and confirmed2/2 pivots.

    ATR first appears at index14, averaging14 known close-to-close true ranges
    and excluding the first path bar's unknown preceding close. A pivot at k
    appears only at k+2; two bars on each side must be strictly less extreme.
    """
    close = np.asarray(path['close'], dtype=float)
    high = np.asarray(path['high'], dtype=float)
    low = np.asarray(path['low'], dtype=float)
    n = len(close)
    if not n or any(x.shape != (n,) for x in (close, high, low)):
        raise ValueError('Invalid prefix feature path shape')
    if not np.isfinite(np.column_stack((high, low, close))).all() or (low > high).any():
        raise ValueError('Invalid prefix feature prices')
    out = {key: np.full(n, np.nan, dtype=float) for key in FEATURE_KEYS}
    tr = np.full(n, np.nan, dtype=float)
    if n > 1:
        tr[1:] = np.maximum(high[1:] - low[1:],
            np.maximum(np.abs(high[1:] - close[:-1]), np.abs(low[1:] - close[:-1])))
    for j in range(14, n):
        out['atr14'][j] = float(tr[j-13:j+1].mean())
    for span in (9, 20):
        value = float(close[0])
        alpha = 2. / (span + 1)
        for j in range(n):
            if j:
                value += alpha * (float(close[j]) - value)
            if j >= span - 1:
                out['ema' + str(span)][j] = value
    delta = np.diff(close)
    for j in range(10, n):
        denominator = float(np.abs(delta[j-10:j]).sum())
        out['er10'][j] = (close[j] - close[j-10]) / denominator if denominator else 0.
    for confirmed in range(4, n):
        k = confirmed - 2
        neighbors = [k-2, k-1, k+1, k+2]
        if np.all(low[k] < low[neighbors]):
            out['pivot_low'][confirmed] = low[k]
            out['pivot_low_index'][confirmed] = k
        if np.all(high[k] > high[neighbors]):
            out['pivot_high'][confirmed] = high[k]
            out['pivot_high_index'][confirmed] = k
    return out


def _validate_features(features, count):
    if set(features) != set(FEATURE_KEYS):
        raise ValueError('Unexpected trailing feature schema')
    if any(np.asarray(features[key]).shape != (count,) for key in FEATURE_KEYS):
        raise ValueError('Trailing feature/path length mismatch')


def _settings(settings, initial_override):
    if not isinstance(settings, dict) or not set(settings).issubset(
            {'mode', 'target', 'initial_stop_pct', 'adaptive_initial'}):
        raise ValueError('Unknown trailing settings')
    mode = settings.get('mode')
    if mode not in {'fixed', 'breakeven', 'step', 'chandelier', 'swing', 'ema20', 'hybrid'}:
        raise ValueError('Unknown trailing mode')
    target_mode = settings.get('target')
    if target_mode not in {'native', 'runner'} or settings.get('initial_stop_pct') != 1.50:
        raise ValueError('Invalid target mode or declared baseline risk')
    adaptive = settings.get('adaptive_initial', False)
    if not isinstance(adaptive, bool) or (adaptive and mode != 'hybrid'):
        raise ValueError('Only the hybrid has an adaptive initial stop')
    if adaptive:
        if initial_override is None or not np.isfinite(initial_override):
            raise ValueError('Adaptive hybrid requires a verified initial distance')
        # A missing/invalid opposing edge uses the explicit1.50% fallback,
        # not an implicit clip to1.00% that would silently change the policy.
        initial = float(initial_override)
        if not (.40 <= initial <= 1.00 or initial == 1.50):
            raise ValueError('Adaptive risk must be0.40–1.00% or the1.50% fallback')
    else:
        if initial_override is not None and initial_override != 1.50:
            raise ValueError('Fixed-risk arms must preserve1.50% initial distance')
        initial = 1.50
    return mode, target_mode, initial


def replay(path, entry_index, entry, is_long, target_pct, settings,
           entry_atr, initial_override=None, features=None):
    """Return an exit plus an audit of next-bar protective-stop ratchets.

    The caller preserves entry identities, position sizes and modeled costs.
    Entry-bar extrema cannot arm trails or set the post-entry favorable peak.
    The original conservative entry-bar stop-first convention is retained;
    its open is not a valid stop fill because it can precede intrabar entry.
    """
    mode, target_mode, risk = _settings(settings, initial_override)
    if (not np.isfinite(entry) or entry <= 0 or not np.isfinite(target_pct) or target_pct <= 0
            or not np.isfinite(entry_atr) or entry_atr <= 0):
        raise ValueError('Entry, original target and verified entry ATR must be positive and finite')
    count = len(path['close'])
    if not isinstance(entry_index, (int, np.integer)) or not 0 <= entry_index < count:
        raise ValueError('Invalid native entry index')
    if any(len(path[key]) != count for key in ('timestamp_ns', 'open', 'high', 'low')):
        raise ValueError('Execution path arrays have different lengths')
    if count > 1 and not np.all(np.diff(path['timestamp_ns']) == MINUTE_NS):
        raise ValueError('Execution path must have every one-minute bar')
    if mode != 'fixed':
        features = build_features(path) if features is None else features
        _validate_features(features, count)
    sign = 1 if is_long else -1
    native_target = entry * (1 + sign * target_pct / 100)
    active_stop_return, active_reason = -risk, 'STOP'
    peak = None
    last_pivot = None
    armed = set()
    updates = []
    pending_market = None
    adverse_closes = 0

    def finish(index, price, reason, event, ambiguous=False):
        return dict(index=index, price=float(price), reason=reason, event=event,
                    ambiguous=bool(ambiguous), initial_risk_pct=risk,
                    target_mode=target_mode, runner=target_mode == 'runner',
                    activation_count=len(armed), activations=sorted(armed),
                    stop_update_count=len(updates), stop_updates=updates.copy(),
                    final_stop_return_pct=float(active_stop_return))

    for j in range(entry_index, count):
        op, hi, lo, close = [float(path[key][j]) for key in ('open', 'high', 'low', 'close')]
        stop = entry * (1 + sign * active_stop_return / 100)
        stop_open = op <= stop if is_long else op >= stop
        stop_hit = lo <= stop if is_long else hi >= stop
        target_hit = target_mode == 'native' and (hi >= native_target if is_long else lo <= native_target)
        if pending_market is not None and j > entry_index:
            return finish(j, op, active_reason if stop_open else pending_market, 'OPEN')
        if stop_hit:
            open_fill = j > entry_index and stop_open
            return finish(j, op if open_fill else stop, active_reason,
                          'OPEN' if open_fill else 'INTRABAR', target_hit and not open_fill)
        if target_hit:
            return finish(j, native_target, 'TARGET', 'INTRABAR')
        if mode == 'fixed' or j == entry_index or j == count - 1:
            continue
        # Every observation below is from a completed post-entry bar. Changes
        # take effect at j+1, never within the bar establishing the condition.
        favorable_price = hi if is_long else lo
        peak = favorable_price if peak is None else (max(peak, favorable_price) if is_long else min(peak, favorable_price))
        close_return = sign * (close / entry - 1) * 100
        atr = float(features['atr14'][j])
        atr = atr if np.isfinite(atr) and atr > 0 else float(entry_atr)
        pivot_key = 'pivot_low' if is_long else 'pivot_high'
        pivot = float(features[pivot_key][j])
        pivot_index = float(features[pivot_key + '_index'][j])
        if np.isfinite(pivot) and np.isfinite(pivot_index) and pivot_index > entry_index:
            last_pivot = pivot
        proposed, proposed_reason = active_stop_return, active_reason

        def propose(value, reason):
            nonlocal proposed, proposed_reason
            if np.isfinite(value) and value > proposed + 1e-12:
                proposed, proposed_reason = float(value), reason

        if mode in {'breakeven', 'step', 'swing', 'hybrid'} and close_return + 1e-10 >= risk:
            armed.add('cost_breakeven_1r')
            propose(COST_BREAK_EVEN_PCT, 'COST_BREAKEVEN')
        if mode in {'step', 'hybrid'}:
            if close_return + 1e-10 >= 1.5 * risk:
                armed.add('step_1p5r')
                propose(.5 * risk, 'STEP_0P5R_LOCK')
            if close_return + 1e-10 >= 2 * risk:
                armed.add('step_2r')
                propose(risk, 'STEP_1R_LOCK')
        if mode == 'chandelier':
            if close_return + 1e-10 >= .5 * risk:
                armed.add('chandelier_0p5r')
            if 'chandelier_0p5r' in armed:
                price = peak - sign * 3 * atr
                propose(sign * (price / entry - 1) * 100, 'CHANDELIER_3ATR')
        if mode == 'swing':
            if close_return + 1e-10 >= .5 * risk:
                armed.add('swing_0p5r')
            if 'swing_0p5r' in armed and last_pivot is not None:
                price = last_pivot - sign * .25 * atr
                propose(sign * (price / entry - 1) * 100, 'CONFIRMED_SWING_TRAIL')
        if mode == 'ema20':
            if close_return + 1e-10 >= .5 * risk:
                armed.add('ema20_0p5r')
            ema20 = float(features['ema20'][j])
            adverse = ('ema20_0p5r' in armed and np.isfinite(ema20)
                       and sign * (close - ema20) < 0)
            adverse_closes = adverse_closes + 1 if adverse else 0
            if adverse_closes >= 2:
                armed.add('ema20_exit_signal')
                pending_market = 'EMA20_MOMENTUM_EXIT'
        if mode == 'hybrid':
            if close_return + 1e-10 >= 2 * risk:
                armed.add('hybrid_2r')
            ema9, ema20, er = [float(features[key][j]) for key in ('ema9', 'ema20', 'er10')]
            trend = (np.isfinite(ema9) and np.isfinite(ema20) and np.isfinite(er)
                     and sign * (ema9 - ema20) > 0 and sign * er >= .30)
            if 'hybrid_2r' in armed and trend:
                armed.add('hybrid_confirmed_trail')
                price = peak - sign * 3 * atr
                propose(sign * (price / entry - 1) * 100, 'HYBRID_3ATR')
                if last_pivot is not None:
                    price = last_pivot - sign * .25 * atr
                    propose(sign * (price / entry - 1) * 100, 'HYBRID_CONFIRMED_SWING')
        if proposed > active_stop_return + 1e-12:
            active_stop_return, active_reason = proposed, proposed_reason
            updates.append(dict(observed_index=j, effective_index=j+1,
                                stop_return_pct=proposed, reason=proposed_reason))
    return finish(count-1, path['close'][-1], 'TIME_EXIT_1515', 'CLOSE')
