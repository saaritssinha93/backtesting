"""Past-only cash features; stock futures/OI are never read or fabricated.

Input timestamps label completed minute ends in Asia/Kolkata. Existing G-3
EMAs and rolling volume ratios deliberately retain their cross-session native
arithmetic. New pressure features reset each session; same-clock references
exclude the current session and use twenty prior observed valid sessions.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd

IST = "Asia/Kolkata"
SIGNAL_TIMES = {925, 930, 935, 940, 945, 950, 955, 1000, 1120}
HISTORY_SESSIONS = 20


@dataclass(frozen=True)
class Variant:
    name: str
    family: str
    threshold: float = 0.0
    pressure: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def variants() -> list[Variant]:
    """Fixed before observing P&L; no arbitrary composite rank score."""
    return [
        Variant("no_oi_control", "none"),
        *[Variant(f"interval_rvol_{x:g}", "interval_rvol", x) for x in (1., 1.5, 2.)],
        *[Variant(f"cumulative_rvol_{x:g}", "cumulative_rvol", x) for x in (1., 1.5, 2.)],
        Variant("relative_turnover_1.5", "relative_turnover", 1.5),
        Variant("cmf_0", "cmf10", 0.),
        Variant("cmf_0.2", "cmf10", .2),
        Variant("signed_volume_0", "signed_volume10", 0.),
        Variant("signed_volume_0.2", "signed_volume10", .2),
        Variant("interval_rvol_1.5_cmf_0", "interval_rvol", 1.5, 0.),
        Variant("interval_rvol_1.5_cmf_0.2", "interval_rvol", 1.5, .2),
        Variant("mfi5_55", "mfi5", 55.),
        Variant("mfi5_60", "mfi5", 60.),
    ]


def participation_mask(rows: pd.DataFrame, variant: Variant) -> pd.Series:
    sign = rows["side"].map({"LONG": 1., "SHORT": -1.})
    if variant.family == "none":
        result = pd.Series(True, index=rows.index)
    else:
        values = pd.to_numeric(rows[variant.family], errors="coerce")
        if variant.family in ("cmf10", "signed_volume10"):
            result = values.mul(sign).ge(variant.threshold)
        elif variant.family == "mfi5":
            result = values.sub(50.).mul(sign).ge(variant.threshold - 50.)
        else:
            result = values.ge(variant.threshold)
        result &= np.isfinite(values)
    if variant.pressure is not None:
        pressure = pd.to_numeric(rows["cmf10"], errors="coerce")
        result &= np.isfinite(pressure) & pressure.mul(sign).ge(variant.pressure)
    return result.fillna(False)


def clean_minutes(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    source = out["ts"] if "ts" in out else out["date"]
    parsed = pd.to_datetime(source, errors="coerce")
    if parsed.dt.tz is None:
        parsed = parsed.dt.tz_localize(IST)
    else:
        parsed = parsed.dt.tz_convert(IST)
    out["ts"] = parsed
    values = ["open", "high", "low", "close", "volume"]
    out[values] = out[values].apply(pd.to_numeric, errors="coerce")
    valid = out.ts.notna() & np.isfinite(out[values]).all(axis=1)
    valid &= out[["open", "high", "low", "close"]].gt(0).all(axis=1) & out.volume.ge(0)
    valid &= out.high.ge(out[["open", "close"]].max(axis=1))
    valid &= out.low.le(out[["open", "close"]].min(axis=1)) & out.high.ge(out.low)
    offset = (out.ts - out.ts.dt.normalize()).dt.total_seconds() / 60 - 555
    valid &= offset.between(1, 375) & offset.eq(offset.round())
    for column in ("gap_filled", "opening_snapshot", "provisional_stale"):
        if column in out:
            flagged = (pd.to_numeric(out[column], errors="coerce").fillna(0).ne(0)
                       | out[column].astype(str).str.lower().isin({"true", "yes", "on"}))
            valid &= ~flagged
    return out.loc[valid].sort_values("ts").drop_duplicates("ts", keep="last").reset_index(drop=True)


def aggregate_five(minute: pd.DataFrame) -> pd.DataFrame:
    """Native five real minute bars, end-labelled 09:20 through15:30."""
    if minute.empty:
        return pd.DataFrame()
    work = minute.copy()
    session_open = work.ts.dt.normalize() + pd.Timedelta(hours=9, minutes=15)
    offset = (work.ts - session_open).dt.total_seconds() / 60
    work["slot_end"] = session_open + pd.to_timedelta((((offset - 1) // 5 + 1) * 5), unit="m")
    five = work.groupby("slot_end", sort=True, as_index=False).agg(
        open=("open", "first"), high=("high", "max"), low=("low", "min"),
        close=("close", "last"), volume=("volume", "sum"),
        source_1m_count=("ts", "size"), first=("ts", "first"), last=("ts", "last"))
    valid = (five.source_1m_count.eq(5) & five["first"].eq(five.slot_end - pd.Timedelta(minutes=4))
             & five["last"].eq(five.slot_end))
    five = five.loc[valid].drop(columns=["first", "last"]).rename(columns={"slot_end": "ts"}).reset_index(drop=True)
    for span in (9, 20, 50):
        five[f"ema{span}"] = five.close.ewm(span=span, adjust=False).mean()
    five["price_change_pct"] = (five.close / five.close.shift(1) - 1.) * 100.
    denominator = five.volume.shift(1).rolling(20, min_periods=5).mean()
    five["volume_ratio"] = five.volume / denominator.where(denominator.gt(0))
    five["traded_value"] = five.close * five.volume
    return five


def _relative_at_clock(frame: pd.DataFrame, source: str, window: int) -> tuple[pd.Series, pd.Series]:
    clock = frame.ts.dt.strftime("%H:%M")
    denominator = frame.groupby(clock, sort=False)[source].transform(
        lambda x: x.shift(1).rolling(window, min_periods=window).median())
    count = frame.groupby(clock, sort=False)[source].transform(
        lambda x: x.shift(1).rolling(window, min_periods=1).count())
    return frame[source] / denominator.where(denominator.gt(0)), count


def add_participation(five: pd.DataFrame, minute: pd.DataFrame,
                      history_sessions: int = HISTORY_SESSIONS) -> pd.DataFrame:
    out = five.copy()
    if out.empty:
        return out
    out["interval_rvol"], out["rvol_history_count"] = _relative_at_clock(out, "volume", history_sessions)
    out["relative_turnover"], _ = _relative_at_clock(out, "traded_value", history_sessions)
    # A cumulative interval is usable only when every prior five-minute bar
    # from this session's open is present. Never treat a partial sum as full.
    day = out.ts.dt.date
    observed = out.groupby(day).cumcount() + 1
    expected = (out.ts.dt.hour * 60 + out.ts.dt.minute - 555) / 5
    out["cumulative_volume"] = out.groupby(day).volume.cumsum().where(observed.eq(expected))
    out["cumulative_rvol"], out["cumulative_history_count"] = _relative_at_clock(out, "cumulative_volume", history_sessions)
    m = minute.copy()
    day = m.ts.dt.date
    gap = m.high - m.low
    # Zero-range bars contribute neutral close-location pressure, not infinity.
    clv = ((2 * m.close - m.high - m.low) / gap.where(gap.gt(0))).fillna(0.)
    m["clv_volume"] = clv * m.volume
    m["typical"] = (m.high + m.low + m.close) / 3.
    move = m.groupby(day).close.diff()
    m["signed_volume"] = np.sign(move) * m.volume
    typical_move = m.groupby(day).typical.diff()
    raw_money = m.typical * m.volume
    m["positive_money"] = raw_money.where(typical_move.gt(0), 0.).where(typical_move.notna())
    m["negative_money"] = raw_money.where(typical_move.lt(0), 0.).where(typical_move.notna())
    group = m.groupby(day, sort=False)
    volume10 = group.volume.transform(lambda x: x.rolling(10, min_periods=10).sum())
    cmf_num = group.clv_volume.transform(lambda x: x.rolling(10, min_periods=10).sum())
    # First bar direction is its own open-to-close, without yesterday's close.
    first = group.cumcount().eq(0)
    m.loc[first, "signed_volume"] = np.sign(m.loc[first, "close"] - m.loc[first, "open"]) * m.loc[first, "volume"]
    signed_num = m.groupby(day).signed_volume.transform(lambda x: x.rolling(10, min_periods=10).sum())
    continuous10 = m.ts.sub(m.groupby(day).ts.shift(9)).eq(pd.Timedelta(minutes=9))
    m["cmf10"] = (cmf_num / volume10.where(volume10.gt(0))).where(continuous10)
    m["signed_volume10"] = (signed_num / volume10.where(volume10.gt(0))).where(continuous10)
    positive = group.positive_money.transform(lambda x: x.rolling(5, min_periods=5).sum())
    negative = group.negative_money.transform(lambda x: x.rolling(5, min_periods=5).sum())
    total = positive + negative
    continuous6 = m.ts.sub(m.groupby(day).ts.shift(5)).eq(pd.Timedelta(minutes=5))
    m["mfi5"] = (100 * positive / total.where(total.gt(0))).where(continuous6)
    return out.merge(m[["ts", "cmf10", "signed_volume10", "mfi5"]], on="ts", how="left", validate="one_to_one")


def build_features(frame: pd.DataFrame, symbol: str, *, history_sessions: int = HISTORY_SESSIONS) -> pd.DataFrame:
    """All configured signal times with exact confirmations, before strict gates."""
    minute = clean_minutes(frame)
    five = add_participation(aggregate_five(minute), minute, history_sessions)
    if five.empty:
        return five
    five["hhmm_int"] = five.ts.dt.hour * 100 + five.ts.dt.minute
    five = five.loc[five.hhmm_int.isin(SIGNAL_TIMES)].copy()
    five["signal_ts"] = five.ts
    five["confirmation_ts"] = five.ts + pd.Timedelta(minutes=1)
    five["day"] = five.ts.dt.date.astype(str)
    five["hhmm"] = five.ts.dt.strftime("%H%M")
    five["signal_close"] = five.close
    five["tradingsymbol"] = symbol
    five["price_source"] = "NSE_EQUITY_1M_EXACT_AGGREGATION"
    five["participation_source"] = "NSE_CASH_OHLCV_PAST_ONLY"
    five["v9_5m_ema_bull"] = five.ema9.gt(five.ema20) & five.ema20.gt(five.ema50)
    five["v9_5m_ema_bear"] = five.ema9.lt(five.ema20) & five.ema20.lt(five.ema50)
    five["v9_5m_feature_ts"] = five.signal_ts
    denominator = minute.volume.shift(1).rolling(20, min_periods=5).mean()
    confirmation = minute[["ts", "open", "high", "low", "close", "volume"]].copy()
    confirmation["v9_1m_volume_ratio"] = minute.volume / denominator.where(denominator.gt(0))
    confirmation["v9_1m_feature_ts"] = confirmation.ts
    span = (confirmation.high - confirmation.low).where(confirmation.high.gt(confirmation.low))
    confirmation["body_ratio"] = (confirmation.close - confirmation.open).abs() / span
    confirmation["v9_1m_upper_wick_ratio"] = (confirmation.high - confirmation[["open", "close"]].max(axis=1)) / span
    confirmation["v9_1m_lower_wick_ratio"] = (confirmation[["open", "close"]].min(axis=1) - confirmation.low) / span
    confirmation = confirmation.rename(columns={"ts": "confirmation_ts", **{
        key: f"confirmation_{key}" for key in ("open", "high", "low", "close", "volume")}})
    five = five.merge(confirmation, on="confirmation_ts", how="left", validate="one_to_one")
    five["v9_exact_confirmation_present"] = five.v9_1m_feature_ts.notna()
    five["v9_feature_available_ts"] = five.v9_1m_feature_ts
    return five.reset_index(drop=True)


def strict_signals(pool: pd.DataFrame) -> pd.DataFrame:
    """Exact native base gates excluding only the stock futures OI predicate."""
    if pool.empty:
        return pool.copy()
    out = pool.copy()
    longs = out.v9_5m_ema_bull.fillna(False) & out.price_change_pct.ge(.10)
    shorts = out.v9_5m_ema_bear.fillna(False) & out.price_change_pct.le(-.10)
    out["side"] = np.where(longs, "LONG", "SHORT")
    sign = np.where(longs, 1., -1.)
    strict = (out.volume_ratio.ge(.80) & (longs | shorts)
              & out.v9_exact_confirmation_present & out.confirmation_high.gt(out.confirmation_low)
              & ((out.confirmation_close - out.confirmation_open) * sign).gt(0)
              & ((out.confirmation_close - out.signal_close) * sign).gt(0))
    gated = out.hhmm_int.eq(925) & out.side.eq("SHORT")
    nifty = pd.to_numeric(out["nifty_first_bar_return_pct"], errors="coerce")
    strict &= ~gated | (np.isfinite(nifty) & nifty.le(-.05))
    out["wick_ratio"] = np.where(longs, out.v9_1m_upper_wick_ratio, out.v9_1m_lower_wick_ratio)
    out["trigger"] = np.where(longs, out.confirmation_high, out.confirmation_low)
    return out.loc[strict].copy()
