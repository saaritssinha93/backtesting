"""Fit seven entry-stop hypotheses using only the first44 observed sessions.

Baseline winner labels are used solely in historical calibration. At a new
entry, distances consume frozen parameters and features already available at
confirmation; neither the future path nor that entry's outcome is required.
"""
from __future__ import annotations

import hashlib
import json

import numpy as np
import pandas as pd

VERSION = 'V23_ENTRY_SL_FIRST44_V1'
FIT_SESSION_COUNT = 44
FIT_CUTOFF = '2026-10-05'
FLOOR_PCT = .50
CAP_PCT = 1.25
MIN_BIN_WINNERS = 60
MAE_COLUMN = 'mae_upper_pct'
PNL_COLUMN = 'baseline_net_pnl_rs'
QUANTILES = {'mae_q90': .90, 'mae_q95': .95, 'mae_q975': .975, 'mae_q99': .99}
POLICIES = ('baseline_v21_g7', *QUANTILES, 'atr_q95', 'volatility_bin_q95', 'structure_q95')


def round_up(value, step):
    """Round upward while avoiding a floating-point extra step at exact ties."""
    if not np.isfinite(value) or value < 0 or not np.isfinite(step) or step <= 0:
        raise ValueError('Invalid nonnegative quantile or rounding increment')
    return float(np.ceil(value / step - 1e-12) * step)


def _stop_quantile(value):
    return float(np.clip(round_up(value, .05), FLOOR_PCT, CAP_PCT))


def _validate_features(frame):
    required = ['sid', 'day', 'side', 'entry_price', 'entry_ts', 'confirmation_ts',
                'feature_available_at', 'atr14_1m', 'atr14_pct',
                'confirmation_low', 'confirmation_high', 'structure_distance_pct']
    missing = set(required) - set(frame.columns)
    if missing:
        raise ValueError('Missing entry features: ' + ', '.join(sorted(missing)))
    if frame.sid.isna().any() or frame.sid.duplicated().any():
        raise ValueError('Missing or duplicate entry identity')
    if not frame.side.isin(['LONG', 'SHORT']).all():
        raise ValueError('Unknown side')
    if not frame.day.astype(str).str.fullmatch(r'\d{4}-\d{2}-\d{2}').all():
        raise ValueError('Session dates must be ISO date strings')
    for key in ('entry_price', 'atr14_1m', 'atr14_pct', 'confirmation_low', 'confirmation_high'):
        values = pd.to_numeric(frame[key], errors='coerce').to_numpy(float)
        if not np.isfinite(values).all() or not (values > 0).all():
            raise ValueError('Invalid positive entry feature: ' + key)
    if (frame.confirmation_low > frame.confirmation_high).any():
        raise ValueError('Inverted confirmation candle')
    if not np.allclose(frame.atr14_1m / frame.entry_price * 100, frame.atr14_pct,
                       atol=1e-8, rtol=1e-10):
        raise ValueError('ATR percentage is inconsistent with actual entry price')
    sign = np.where(frame.side.eq('LONG'), 1., -1.)
    edge = np.where(frame.side.eq('LONG'), frame.confirmation_low, frame.confirmation_high)
    structural = sign * (frame.entry_price - edge) / frame.entry_price * 100
    reported = pd.to_numeric(frame.structure_distance_pct, errors='coerce').to_numpy(float)
    if not np.isfinite(reported).all() or not np.allclose(structural, reported, atol=1e-8, rtol=1e-10):
        raise ValueError('Confirmed structural distance is inconsistent with entry/side')
    stamp = {}
    for key in ('feature_available_at', 'confirmation_ts', 'entry_ts'):
        stamp[key] = pd.to_datetime(frame[key], utc=True, errors='coerce', format='mixed')
        if stamp[key].isna().any():
            raise ValueError('Missing or invalid feature/entry timestamp')
    if not (stamp['feature_available_at'].le(stamp['confirmation_ts']) &
            stamp['confirmation_ts'].le(stamp['entry_ts'])).all():
        raise ValueError('Entry feature was unavailable at confirmation or entry')
    return structural.to_numpy(float)


def fit(frame, training_days):
    """Filter to the prescribed44 sessions before inspecting any trade labels.

    A frame containing future rows is permitted; those rows cannot affect
    validation, calibration, sample counts, or the training fingerprint.
    """
    days = [str(day) for day in training_days]
    if (len(days) != FIT_SESSION_COUNT or days != sorted(set(days)) or days[-1] != FIT_CUTOFF):
        raise ValueError('Fit requires the ordered first44 sessions through2026-10-05')
    if 'day' not in frame:
        raise ValueError('Missing session dates')
    training = frame.loc[frame.day.astype(str).isin(days)].copy().sort_values('sid').reset_index(drop=True)
    if training.empty:
        raise ValueError('No training entries')
    _validate_features(training)
    if MAE_COLUMN not in training or PNL_COLUMN not in training:
        raise ValueError('Training MAE upper bound or native outcome is missing')
    mae = pd.to_numeric(training[MAE_COLUMN], errors='coerce')
    pnl = pd.to_numeric(training[PNL_COLUMN], errors='coerce')
    if not np.isfinite(mae).all() or mae.lt(0).any() or not np.isfinite(pnl).all():
        raise ValueError('Training MAE or native P&L is invalid')
    winners = training.loc[pnl.gt(0)].copy()
    if winners.empty:
        raise ValueError('No native winners for MAE calibration')
    winners[MAE_COLUMN] = mae.loc[winners.index]
    raw_quantiles = {name: float(winners[MAE_COLUMN].quantile(q)) for name, q in QUANTILES.items()}
    fixed = {name: _stop_quantile(value) for name, value in raw_quantiles.items()}
    ratio_q95 = float((winners[MAE_COLUMN] / winners.atr14_pct).quantile(.95))
    multiplier = round_up(ratio_q95, .25)
    split = float(training.atr14_pct.median())
    bins = {}
    for name, mask in [('low', winners.atr14_pct.le(split)), ('high', winners.atr14_pct.gt(split))]:
        part = winners.loc[mask, MAE_COLUMN]
        enough = len(part) >= MIN_BIN_WINNERS
        quantile = float(part.quantile(.95)) if enough else raw_quantiles['mae_q95']
        bins[name] = dict(winners=int(len(part)), raw_mae_q95_pct=quantile,
                          stop_pct=_stop_quantile(quantile), pooled_fallback=not enough)
    fingerprint_columns = ['sid', 'day', 'side', 'entry_price', 'entry_ts', 'confirmation_ts',
                           'feature_available_at', 'atr14_1m', 'atr14_pct', 'confirmation_low',
                           'confirmation_high', 'structure_distance_pct', MAE_COLUMN, PNL_COLUMN]
    fingerprint = hashlib.sha256(training[fingerprint_columns].to_csv(index=False).encode()).hexdigest()
    params = dict(version=VERSION, training_days=days, training_cutoff=FIT_CUTOFF,
        training_rows=len(training), training_winners=len(winners), training_losses=int(pnl.lt(0).sum()),
        training_flat=int(pnl.eq(0).sum()), training_sha256=fingerprint,
        quantile_method='linear', mae_source=MAE_COLUMN, native_pnl_source=PNL_COLUMN,
        minimum_stop_pct=FLOOR_PCT, maximum_stop_pct=CAP_PCT, fixed_stop_pct=fixed,
        raw_winner_mae_quantiles_pct=raw_quantiles, atr_ratio_raw_q95=ratio_q95,
        atr_multiplier=multiplier, atr_definition='One-minute14 simple rolling true range at confirmation, divided by actual entry price',
        volatility_split_atr_pct=split, volatility_bins=bins, minimum_bin_winners=MIN_BIN_WINNERS,
        structure_atr_buffer_multiple=.25, structure_floor_pct=fixed['mae_q95'],
        structure_invalid_edge_fallback_pct=CAP_PCT,
        no_future_outcomes_used=True, all_history_retrospective=True)
    json.dumps(params, allow_nan=False)
    return params


def distances(features, params):
    """Return every entry unchanged with one initial-stop percentage per model."""
    structural = _validate_features(features)
    if params.get('version') != VERSION:
        raise ValueError('Unknown entry-stop calibration version')
    if (params.get('minimum_stop_pct') != FLOOR_PCT or params.get('maximum_stop_pct') != CAP_PCT
            or params.get('structure_atr_buffer_multiple') != .25
            or params.get('structure_invalid_edge_fallback_pct') != CAP_PCT
            or set(params.get('fixed_stop_pct', {})) != set(QUANTILES)):
        raise ValueError('Frozen model limits or hypothesis inventory changed')
    out = features[['sid']].copy()
    out['baseline_v21_g7'] = CAP_PCT
    for name in QUANTILES:
        value = float(params['fixed_stop_pct'][name])
        if not np.isfinite(value) or not FLOOR_PCT <= value <= CAP_PCT:
            raise ValueError('Invalid fitted stop')
        out[name] = value
    multiplier, split = float(params['atr_multiplier']), float(params['volatility_split_atr_pct'])
    if not np.isfinite(multiplier) or multiplier < 0 or not np.isfinite(split) or split <= 0:
        raise ValueError('Invalid ATR calibration')
    out['atr_q95'] = np.clip(multiplier * features.atr14_pct, FLOOR_PCT, CAP_PCT)
    bins = params['volatility_bins']
    if set(bins) != {'low', 'high'}:
        raise ValueError('Invalid volatility-bin inventory')
    for values in bins.values():
        if not np.isfinite(values['stop_pct']) or not FLOOR_PCT <= values['stop_pct'] <= CAP_PCT:
            raise ValueError('Invalid fitted volatility-bin stop')
    out['volatility_bin_q95'] = np.where(features.atr14_pct.le(split), bins['low']['stop_pct'], bins['high']['stop_pct'])
    floor = float(params['structure_floor_pct'])
    if floor != params['fixed_stop_pct']['mae_q95']:
        raise ValueError('Structure floor must equal the frozen global winner q95 stop')
    structural_stop = np.clip(structural + .25 * features.atr14_pct, floor, CAP_PCT)
    out['structure_q95'] = np.where(structural > 0, structural_stop, CAP_PCT)
    if not np.isfinite(out[list(POLICIES)].to_numpy(float)).all():
        raise ValueError('Invalid entry-stop output')
    if len(out) != len(features) or not out.sid.equals(features.sid):
        raise ValueError('Stop assignment changed entry inventory')
    return out
