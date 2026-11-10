"""Six frozen target-relative stop hypotheses; native targets stay active.

All guard observations use completed post-entry bars. Stop updates apply on
the following bar, never loosen, and preserve adverse gap-open fills.
"""
from __future__ import annotations

import numpy as np

from g3_v23_adx_rsi_20261106.trailing_20261106.engine import (
    BASELINE, FEATURE_KEYS, MINUTE_NS, build_features, replay as baseline_replay)

CANDIDATES = {
    BASELINE: dict(mode='fixed', target='native', initial_stop_pct=1.50),
    'cushion_half_target': dict(mode='cushion', target='native', initial_stop_pct=1.50,
        trigger_fraction=.50, stop_return_pct=-1.00, minimum_bars=1, confirmation_closes=1),
    'cushion_threequarter_target': dict(mode='cushion', target='native', initial_stop_pct=1.50,
        trigger_fraction=.75, stop_return_pct=-.75, minimum_bars=1, confirmation_closes=1),
    'breakeven_eighty_target': dict(mode='cushion', target='native', initial_stop_pct=1.50,
        trigger_fraction=.80, stop_return_pct=.05, minimum_bars=1, confirmation_closes=1),
    'target_steps': dict(mode='steps', target='native', initial_stop_pct=1.50,
        stages=[(.50, -1.00), (.75, -.50), (.90, .05)]),
    'patient_half_cushion': dict(mode='cushion', target='native', initial_stop_pct=1.50,
        trigger_fraction=.50, stop_return_pct=-1.00, minimum_bars=5, confirmation_closes=2),
    'target_atr_hybrid': dict(mode='hybrid', target='native', initial_stop_pct=1.50,
        trigger_fraction=.75, minimum_bars=5, confirmation_closes=2,
        stop_floor_pct=-.75, atr_multiple=3., minimum_trail_pct=.35),
}


def _settings(settings, initial_override):
    # The shortlist is intentionally immutable rather than an optimization API.
    if not isinstance(settings, dict) or not any(settings == item for item in CANDIDATES.values()):
        raise ValueError('Settings must match one frozen target-trailing hypothesis')
    if initial_override is not None and initial_override != 1.50:
        raise ValueError('All target-relative policies preserve the initial 1.50% stop')
    return settings['mode']


def replay(path, entry_index, entry, is_long, target_pct, settings,
           entry_atr, initial_override=None, features=None):
    """Return the baseline-compatible exit and causal stop-update audit.

``minimum_bars=5`` means five completed bars strictly after the entry bar.
Two-close confirmation counts consecutive qualifying closes, resetting on a
nonqualifying close. Hybrid peaks use closes only, excluding the entry bar.
    """
    mode = _settings(settings, initial_override)
    if mode == 'fixed':
        return baseline_replay(path, entry_index, entry, is_long, target_pct,
            settings, entry_atr, initial_override=initial_override, features=features)
    if (not np.isfinite(entry) or entry <= 0 or not np.isfinite(target_pct) or target_pct <= 0
            or not np.isfinite(entry_atr) or entry_atr <= 0):
        raise ValueError('Entry, target and confirmed entry ATR must be positive and finite')
    count = len(path['close'])
    if not isinstance(entry_index, (int, np.integer)) or not 0 <= entry_index < count:
        raise ValueError('Invalid entry index')
    if any(len(path[key]) != count for key in ('timestamp_ns', 'open', 'high', 'low')):
        raise ValueError('Execution path arrays have different lengths')
    if count > 1 and not np.all(np.diff(path['timestamp_ns']) == MINUTE_NS):
        raise ValueError('Execution path must have every one-minute bar')
    values = np.column_stack([path[key] for key in ('open', 'high', 'low', 'close')])
    if not np.isfinite(values).all() or (values[:, 2] > values[:, 1]).any():
        raise ValueError('Invalid execution prices')
    if mode == 'hybrid':
        features = build_features(path) if features is None else features
        if set(features) != set(FEATURE_KEYS) or any(
                np.asarray(features[key]).shape != (count,) for key in FEATURE_KEYS):
            raise ValueError('Invalid prefix feature schema or shape')

    sign = 1 if is_long else -1
    target = entry * (1 + sign * target_pct / 100)
    active_stop_return, active_reason = -1.50, 'STOP'
    armed, updates = set(), []
    consecutive = 0
    peak_close_return = None

    def finish(index, price, reason, event, ambiguous=False):
        return dict(index=index, price=float(price), reason=reason, event=event,
            ambiguous=bool(ambiguous), initial_risk_pct=1.50, target_mode='native', runner=False,
            activation_count=len(armed), activations=sorted(armed), stop_update_count=len(updates),
            stop_updates=updates.copy(), final_stop_return_pct=float(active_stop_return))

    for j in range(entry_index, count):
        op, hi, lo, close = [float(path[key][j]) for key in ('open', 'high', 'low', 'close')]
        stop = entry * (1 + sign * active_stop_return / 100)
        stop_open = op <= stop if is_long else op >= stop
        stop_hit = lo <= stop if is_long else hi >= stop
        target_hit = hi >= target if is_long else lo <= target
        if stop_hit:
            open_fill = j > entry_index and stop_open
            return finish(j, op if open_fill else stop, active_reason,
                'OPEN' if open_fill else 'INTRABAR', target_hit and not open_fill)
        if target_hit:
            return finish(j, target, 'TARGET', 'INTRABAR')
        if j == entry_index or j == count - 1:
            continue

        close_return = sign * (close / entry - 1) * 100
        peak_close_return = (close_return if peak_close_return is None else
                             max(peak_close_return, close_return))
        proposed, proposed_reason = active_stop_return, active_reason

        def propose(value, reason):
            nonlocal proposed, proposed_reason
            if np.isfinite(value) and value > proposed + 1e-12:
                proposed, proposed_reason = float(value), reason

        if mode == 'steps':
            for fraction, stop_return in settings['stages']:
                if close_return + 1e-10 >= fraction * target_pct:
                    label = 'target_stage_' + format(fraction, '.2f')
                    armed.add(label)
                    propose(stop_return, label.upper())
        else:
            qualifies = close_return + 1e-10 >= settings['trigger_fraction'] * target_pct
            consecutive = consecutive + 1 if qualifies else 0
            if j - entry_index >= settings['minimum_bars'] and consecutive >= settings['confirmation_closes']:
                armed.add('target_progress_confirmed')
                if mode == 'cushion':
                    propose(settings['stop_return_pct'], 'TARGET_PROGRESS_CUSHION'
                        if settings['stop_return_pct'] < 0 else 'TARGET_PROGRESS_BREAKEVEN')
            if mode == 'hybrid' and 'target_progress_confirmed' in armed:
                atr = float(features['atr14'][j])
                atr = atr if np.isfinite(atr) and atr > 0 else float(entry_atr)
                distance = max(settings['minimum_trail_pct'], settings['atr_multiple'] * atr / entry * 100)
                propose(max(settings['stop_floor_pct'], peak_close_return - distance), 'TARGET_ATR_HYBRID')
        if proposed > active_stop_return + 1e-12:
            active_stop_return, active_reason = proposed, proposed_reason
            updates.append(dict(observed_index=j, effective_index=j+1,
                stop_return_pct=proposed, reason=proposed_reason))
    return finish(count-1, path['close'][-1], 'TIME_EXIT_1515', 'CLOSE')
