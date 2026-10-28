"""Causal G9 entry-size permission. No broker access or live configuration."""
from __future__ import annotations

import hashlib
import json

import numpy as np
import pandas as pd

from research.g8_exit_engine import allocate_variant, _flags


def permission_timeline(market, spec, settings):
    """Immediate risk reductions; increases need N consecutive ready bars.

    Each session starts independently. For an increase, the minimum indicated
    permission over the last N observations is the most that can be restored.
    Missing features or a missing clock reset recovery. No future rows are read.
    """
    n = int(spec.get('recovery', settings['recovery_observations']))
    if n < 1:
        raise ValueError('Recovery observations must be positive')
    mode = spec['mode']
    if mode not in {'baseline', 'availability', 'constant_half', 'both', 'rv', 'vix'}:
        raise ValueError('Unknown permission mode')
    rv_limit = float(spec.get('rv_threshold', settings['rv_ratio_threshold']))
    vix_limit = float(spec.get('vix_threshold', settings['vix_5m_abs_threshold_pct']))
    scales = settings['quantity_scales_by_warning_count']
    if len(scales) != 3 or not (1 >= scales[0] >= scales[1] >= scales[2] > 0):
        raise ValueError('Scales must be three positive nonincreasing fractions <= 1')
    f = market.copy().sort_values(['day', 'available_at']).reset_index(drop=True)
    if f.duplicated(['day', 'available_at']).any():
        raise ValueError('Duplicate market observation')
    f['rv_warning'] = f.rv_ratio.gt(rv_limit)
    f['vix_warning'] = f.vix_change_5m_pct.abs().ge(vix_limit)
    f['features_ready'] = _flags(f.features_ready, 'features_ready')
    f['raw_permission'] = 0.
    f['quantity_scale'] = 0.
    f['permission_reason'] = ''
    for _, part in f.groupby('day', sort=True):
        state, previous = None, None
        history = []
        for i, row in part.iterrows():
            stamp = pd.Timestamp(row.available_at)
            contiguous = previous is None or stamp-previous == pd.Timedelta(minutes=5)
            previous = stamp
            if not contiguous:
                state, history = 0., []
            if mode == 'baseline':
                desired, ready = 1., True
            else:
                ready = bool(row.features_ready) and np.isfinite([row.rv_ratio, row.vix_change_5m_pct]).all()
                warnings = ((int(row.rv_warning) if mode in ('both', 'rv') else 0)
                            + (int(row.vix_warning) if mode in ('both', 'vix') else 0))
                desired = .5 if mode == 'constant_half' else (1. if mode == 'availability' else scales[warnings])
            if not ready:
                state, history = 0., []
                reason = 'MISSING_FEATURES'
                desired = 0.
            else:
                history = (history+[desired])[-n:]
                if state is None:
                    state = desired
                elif desired <= state:
                    state = desired
                elif len(history) >= n:
                    state = max(state, min(history))
                reason = 'RECOVERY_PENDING' if state < desired else 'READY'
            f.loc[i, ['raw_permission', 'quantity_scale', 'permission_reason']] = [desired, state, reason]
    f['policy'] = spec['id']
    return f


def entry_permissions(book, timeline):
    """As-of lookup at confirmation, never at a later realized fill time."""
    if book.sid.duplicated().any():
        raise ValueError('Duplicate sid')
    grids = {day: p.sort_values('available_at') for day, p in timeline.groupby('day')}
    rows = []
    for row in book.itertuples():
        stamp = pd.Timestamp(row.confirmation_ts)
        if stamp.tzinfo is None:
            raise ValueError('Confirmation must include timezone')
        day = stamp.tz_convert('Asia/Kolkata').strftime('%Y-%m-%d')
        if day != row.day:
            raise ValueError('Confirmation and day differ')
        part = grids.get(day)
        evidence = part.loc[part.available_at.le(stamp)] if part is not None else None
        item = dict(sid=row.sid, day=day, confirmation_ts=stamp, quantity_scale=0.,
                    permission_reason='NO_FRESH_OBSERVATION')
        if evidence is not None and len(evidence):
            last = evidence.iloc[-1]
            age = (stamp-last.available_at).total_seconds()
            if 0 <= age < 300:
                item.update(last.to_dict())
                item['sid'] = row.sid
                item['confirmation_ts'] = stamp
                item['observation_age_seconds'] = age
        rows.append(item)
    result = pd.DataFrame(rows).set_index('sid', drop=False)
    if not result.quantity_scale.between(0, 1).all():
        raise ValueError('Invalid permission')
    return result


def allocate(book, decisions, config, policy, *, cost_bps=5., whole_shares=False):
    """Keep G8 entries and exits; replay capital and risk at the new quantity."""
    if set(decisions.index) != set(book.sid) or not decisions.index.is_unique:
        raise ValueError('One permission per selected order is required')
    work = book.drop(columns=[c for c in book if c.startswith('portfolio_')]).copy()
    requested = work.sid.map(decisions.quantity_scale)
    if not np.isfinite(requested).all() or not requested.between(0, 1).all():
        raise ValueError('Quantity permission must be finite and between zero and one')
    if not isinstance(whole_shares, (bool, np.bool_)):
        raise ValueError('whole_shares must be boolean')
    permitted = _flags(work.g8_direction_allowed, 'g8_direction_allowed')
    actual = requested.copy()
    if whole_shares:
        base_qty = float(config['capital_per_entry_rs'])*float(config['leverage'])/work.entry_price
        actual = np.floor(base_qty*requested)/base_qty
        actual = actual.fillna(0.)
    allow = pd.Series((permitted & actual.gt(0)).to_numpy(), index=work.sid)
    work['g9_requested_quantity_scale'] = requested
    work['g9_effective_quantity_scale'] = actual
    work['g8_quantity_scale'] = actual.where(actual.gt(0), 1.)
    result = allocate_variant(work, allow, config, policy, cost_bps)
    result['g9_original_direction_allowed'] = permitted.to_numpy()
    result['g9_original_baseline_executed'] = _flags(book.portfolio_executed, 'portfolio_executed').to_numpy()
    result['g9_permission_reason'] = result.sid.map(decisions.permission_reason)
    blocked = permitted.to_numpy() & actual.eq(0).to_numpy()
    result.loc[blocked, 'portfolio_status'] = 'G9_RISK_PERMISSION_BLOCKED'
    result.loc[blocked, 'portfolio_reject_reason'] = result.loc[blocked, 'g9_permission_reason']
    result['strategy'] = 'V13-V10-G-9'
    result['strategy_version'] = 'V13-V10-G-9-G8-RISK-PERMISSION-V1'
    result['strategy_fingerprint'] = hashlib.sha256(json.dumps(dict(
        config=config, policy=policy, cost_bps=cost_bps, whole_shares=whole_shares),
        sort_keys=True, default=str).encode()).hexdigest()
    result['g9_model_quantity'] = result.g8_model_quantity
    result['execution_authority'] = False
    return result
