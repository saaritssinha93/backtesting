"""Declared causal indicator hypotheses for the V14 PF-target experiment.

This module only adds features and eligibility masks. The caller applies the
fixed capacity/anti-chase base and retains native G3 ranking and execution.
Every new session feature uses real completed minutes through the signal;
ADX5 resets daily, while EMA9 retains the original full-history arithmetic.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd

from .features import IST, aggregate_five, clean_minutes

VERSION = "V14_PF_INDICATOR_HYPOTHESES_V1"
COMMON_FEATURES = ["pf_vwap", "pf_vwap_start5", "pf_vwap_slope5",
    "pf_opening5_high", "pf_opening5_low", "pf_prior5_high", "pf_prior5_low",
    "pf_efficiency10", "pf_adx5", "pf_plus_di5", "pf_minus_di5", "pf_ema9_slope5"]


@dataclass(frozen=True)
class Variant:
    name: str
    gates: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["gates"] = list(self.gates)
        return result


def variants() -> list[Variant]:
    return [
        Variant("guard_control"),
        Variant("vwap_acceptance", ("vwap",)),
        Variant("opening5m_range_breakout", ("opening",)),
        Variant("prior5m_range_breakout", ("prior",)),
        Variant("efficiency10", ("efficiency",)),
        Variant("fast_adx5", ("adx",)),
        Variant("ema9_slope5m", ("ema_slope",)),
        Variant("vwap_efficiency", ("vwap", "efficiency")),
        Variant("vwap_adx5", ("vwap", "adx")),
        Variant("opening_range_vwap", ("opening", "vwap")),
        Variant("prior_range_efficiency", ("prior", "efficiency")),
        Variant("vwap_efficiency_rvol1p5", ("vwap", "efficiency", "rvol")),
        Variant("vwap_adx5_rvol1p5", ("vwap", "adx", "rvol")),
        Variant("vwap_reclaim", ("reclaim",)),
    ]


def _timestamps(values: pd.Series) -> pd.Series:
    stamps = pd.to_datetime(values, errors="coerce")
    return stamps.dt.tz_localize(IST) if stamps.dt.tz is None else stamps.dt.tz_convert(IST)


def _adx5(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Wilder recursion, TA-Lib-compatible period-5 seed, zero unstable period.

    Seed DM/TR with the first four transitions, then recursively include the
    next five transitions. Their five DX values average into the first ADX at
    index9 (the tenth observed minute). Later ADX uses (4*previous + DX)/5.
    Equal competing directional moves are both zero; zero TR/DM yields zero
    DI/DX. No integer rounding, overnight carry or synthetic opening close.
    """
    count = len(close)
    adx, plus, minus = (np.full(count, np.nan, dtype=float) for _ in range(3))
    if count < 10:
        return adx, plus, minus
    up, down = np.diff(high), -np.diff(low)
    dm_plus = np.where((up > down) & (up > 0), up, 0.)
    dm_minus = np.where((down > up) & (down > 0), down, 0.)
    tr = np.maximum.reduce([high[1:] - low[1:], np.abs(high[1:] - close[:-1]), np.abs(low[1:] - close[:-1])])
    smooth_plus, smooth_minus, smooth_tr = (float(values[:4].sum()) for values in (dm_plus, dm_minus, tr))
    seed_dx = 0.
    previous_adx = np.nan
    for position in range(5, count):
        i = position - 1
        smooth_plus = smooth_plus * .8 + dm_plus[i]
        smooth_minus = smooth_minus * .8 + dm_minus[i]
        smooth_tr = smooth_tr * .8 + tr[i]
        p = 100 * smooth_plus / smooth_tr if smooth_tr > 0 else 0.
        m = 100 * smooth_minus / smooth_tr if smooth_tr > 0 else 0.
        dx = 100 * abs(p - m) / (p + m) if p + m > 0 else 0.
        plus[position], minus[position] = p, m
        if position <= 9:
            seed_dx += dx
            if position == 9:
                previous_adx = seed_dx / 5.
                adx[position] = previous_adx
        else:
            previous_adx = (previous_adx * 4 + dx) / 5.
            adx[position] = previous_adx
    return adx, plus, minus


def add_features(minute: pd.DataFrame, candidates: pd.DataFrame) -> pd.DataFrame:
    """Preserve candidate rows/index; append completed-signal-only features.

    Session features fail closed after the first missing/invalid minute from
    the actual09:16 opening bar. Only requested sessions are processed, but
    aggregate_five receives the entire native minute history for exact EMA9.
    """
    out = candidates.copy()
    for column in COMMON_FEATURES:
        out[column] = np.nan
    out["pf_session_complete"] = False
    if out.empty:
        return out
    signals = _timestamps(out["signal_ts"])
    native = clean_minutes(minute)
    five = aggregate_five(native)
    if five.empty:
        return out
    five = five.set_index("ts")
    previous = signals - pd.Timedelta(minutes=5)
    current_ema = signals.map(five.ema9)
    previous_ema = previous.map(five.ema9)
    out["pf_ema9_slope5"] = current_ema - previous_ema
    out["pf_prior5_high"] = previous.map(five.high)
    out["pf_prior5_low"] = previous.map(five.low)
    requested = signals.groupby(signals.dt.date).max().to_dict()
    daily = native.loc[native.ts.dt.date.isin(requested)].copy()
    parts = []
    for day, group in daily.groupby(daily.ts.dt.date, sort=False):
        group = group.loc[group.ts.le(requested[day])].copy().reset_index(drop=True)
        if group.empty:
            continue
        expected = pd.date_range(group.ts.iloc[0].normalize() + pd.Timedelta(hours=9, minutes=16),
                                 periods=len(group), freq="min")
        matching = group.ts.array == expected.array
        # A gap cannot be healed by a later return of bars within this session.
        bad = np.flatnonzero(~matching)
        if len(bad):
            group = group.iloc[:int(bad[0])].copy()
        if group.empty:
            continue
        prices = group[["high", "low", "close", "volume"]].astype(float)
        typical = (prices.high + prices.low + prices.close) / 3.
        cumulative_volume = prices.volume.cumsum()
        group["pf_vwap"] = (typical * prices.volume).cumsum() / cumulative_volume.where(cumulative_volume.gt(0))
        group["pf_vwap_start5"] = group.pf_vwap.shift(5)
        group["pf_vwap_slope5"] = group.pf_vwap - group.pf_vwap_start5
        movement = prices.close.diff()
        path_length = movement.abs().rolling(9, min_periods=9).sum()
        displacement = prices.close - prices.close.shift(9)
        efficiency = displacement / path_length.where(path_length.gt(0))
        group["pf_efficiency10"] = efficiency.where(path_length.ne(0), 0.).clip(-1., 1.)
        adx, plus, minus = _adx5(prices.high.to_numpy(), prices.low.to_numpy(), prices.close.to_numpy())
        group["pf_adx5"], group["pf_plus_di5"], group["pf_minus_di5"] = adx, plus, minus
        opening_end = group.ts.iloc[0].normalize() + pd.Timedelta(hours=9, minutes=20)
        opening = group.loc[group.ts.le(opening_end)]
        ready = group.ts.ge(opening_end) & (len(opening) == 5)
        group["pf_opening5_high"] = pd.Series(float(opening.high.max()), index=group.index).where(ready)
        group["pf_opening5_low"] = pd.Series(float(opening.low.min()), index=group.index).where(ready)
        group["pf_session_complete"] = True
        parts.append(group.set_index("ts"))
    if parts:
        enriched = pd.concat(parts)
        for column in COMMON_FEATURES:
            if column not in ("pf_ema9_slope5", "pf_prior5_high", "pf_prior5_low"):
                out[column] = signals.map(enriched[column])
        out["pf_session_complete"] = signals.map(enriched.pf_session_complete).eq(True)
    return out


def _number(rows: pd.DataFrame, column: str) -> pd.Series:
    return (pd.to_numeric(rows[column], errors="coerce").astype(float) if column in rows
            else pd.Series(np.nan, index=rows.index, dtype=float))


def participation_mask(rows: pd.DataFrame, variant: Variant) -> pd.Series:
    """Gate only the approved indicator hypotheses; caller applies base guards."""
    passed = pd.Series(True, index=rows.index, dtype=bool)
    if not variant.gates:
        return passed
    side = rows["side"] if "side" in rows else pd.Series(None, index=rows.index, dtype=object)
    sign = side.map({"LONG": 1., "SHORT": -1.})
    close, opening = _number(rows, "close"), _number(rows, "open")
    vwap, vwap_start = _number(rows, "pf_vwap"), _number(rows, "pf_vwap_start5")
    slope, efficiency = _number(rows, "pf_vwap_slope5"), _number(rows, "pf_efficiency10")
    adx = _number(rows, "pf_adx5")
    plus, minus = _number(rows, "pf_plus_di5"), _number(rows, "pf_minus_di5")
    vwap_gate = (close - vwap).mul(sign).gt(0) & slope.mul(sign).gt(0)
    gates = dict(
        vwap=vwap_gate & np.isfinite(close) & np.isfinite(vwap) & np.isfinite(slope),
        opening=((close - _number(rows, "pf_opening5_high")).gt(0) & side.eq("LONG")
                 | (close - _number(rows, "pf_opening5_low")).lt(0) & side.eq("SHORT")),
        prior=((close - _number(rows, "pf_prior5_high")).gt(0) & side.eq("LONG")
               | (close - _number(rows, "pf_prior5_low")).lt(0) & side.eq("SHORT")),
        efficiency=efficiency.mul(sign).ge(.5) & np.isfinite(efficiency) & efficiency.abs().le(1.),
        adx=adx.ge(25.) & (plus - minus).mul(sign).gt(0)
            & np.isfinite(adx) & np.isfinite(plus) & np.isfinite(minus),
        ema_slope=_number(rows, "pf_ema9_slope5").mul(sign).gt(0) & np.isfinite(_number(rows, "pf_ema9_slope5")),
        rvol=_number(rows, "interval_rvol").ge(1.5) & np.isfinite(_number(rows, "interval_rvol")),
        reclaim=vwap_gate & (opening - vwap_start).mul(sign).lt(0)
            & np.isfinite(close) & np.isfinite(opening) & np.isfinite(vwap)
            & np.isfinite(vwap_start) & np.isfinite(slope))
    for name in ("opening", "prior"):
        prefix = "pf_opening5" if name == "opening" else "pf_prior5"
        gates[name] &= np.isfinite(close) & np.isfinite(_number(rows, prefix + "_high")) & np.isfinite(_number(rows, prefix + "_low"))
    for name in variant.gates:
        if name not in gates:
            raise ValueError(f"Unknown fixed indicator gate: {name}")
        passed &= gates[name]
    return (passed & sign.notna()).fillna(False).astype(bool)


def specification() -> dict[str, Any]:
    return dict(version=VERSION, variants=[variant.to_dict() for variant in variants()],
        common_features=COMMON_FEATURES,
        base="Fixed capacity+anti-chase gates; native G3 indicators, ranks, entries and exits unchanged",
        vwap="Cumulative HLC3*volume/sum(volume) from actual09:16 minute; side-aligned close and exact5minute VWAP change, both strictly positive",
        opening_range="Completed09:16–09:20 high/low; signal close strictly outside in trade direction",
        prior_range="Exact immediately preceding completed5minute high/low; signal close strictly outside in trade direction",
        efficiency="Signed displacement over9returns from10real minute closes divided by sum absolute returns; side-adjusted>=0.5; zero-motion result0",
        fast_adx="Session-reset Wilder period5, firstADX at10th minute; ADX>=25 and strictly alignedDI; seed4transitions then5recursiveDX; zero unstable period",
        ema_slope="Native full-history5minute EMA9 minus EMA9 at exactsignal−5minutes; strictly aligned",
        rvol="Existing same-clock intervalRVOL>=1.5, unchanged arithmetic",
        reclaim="Signal5minute open strictly opposite side of VWAP at intervalSTART; close on correct side of currentVWAP; VWAP5minute slope aligned",
        availability="Completed signal only; all current-session features unavailable after first missing/invalid minute; no fills or synthetic candles",
        historical_status="Retrospective indicator hypotheses; no claim of a fresh historical holdout or guaranteedPF",
        adx_reference="https://github.com/TA-Lib/ta-lib/blob/main/src/ta_func/ta_ADX.c",
        execution_authority=False)
