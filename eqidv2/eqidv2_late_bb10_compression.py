"""Shared detector and entry resolver for the late BB10 compression breakout.

The implementation is deliberately independent of the stored indicator columns:
all features below are rebuilt causally from OHLCV so V11 and V7 use the same
rules as the frozen three-month research candidate.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Callable, Iterable

import numpy as np
import pandas as pd


SETUP = "L_LATE_BB10_COMPRESSION_BREAKOUT"
IST_TZ = "Asia/Kolkata"
TICK_SIZE = 0.05
ENTRY_VALID_MINUTES = 3
ENTRY_MAX_GAP_PCT = 0.20
SIGNAL_START_MINUTE = 14 * 60
SIGNAL_END_MINUTE = 14 * 60 + 29


def _timestamps(values: pd.Series) -> pd.Series:
    out = pd.to_datetime(values, errors="coerce")
    if getattr(out.dt, "tz", None) is None:
        return out.dt.tz_localize(IST_TZ)
    return out.dt.tz_convert(IST_TZ)


def _wilder(values: pd.Series, length: int) -> pd.Series:
    return values.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()


def add_features(raw: pd.DataFrame) -> pd.DataFrame:
    """Rebuild the frozen setup's causal features from an OHLCV frame."""
    d = raw.copy()
    d["date"] = _timestamps(d["date"])
    d = d.dropna(subset=["date"]).sort_values("date").drop_duplicates("date", keep="last")
    for col in ("open", "high", "low", "close", "volume"):
        d[col] = pd.to_numeric(d[col], errors="coerce")
    d["session"] = d["date"].dt.normalize()
    d["minute"] = d["date"].dt.hour * 60 + d["date"].dt.minute

    prev_close = d["close"].shift()
    true_range = pd.concat(
        [
            (d["high"] - d["low"]).abs(),
            (d["high"] - prev_close).abs(),
            (d["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    d["atr"] = _wilder(true_range, 14)
    d["atr_pct"] = d["atr"] / d["close"] * 100.0

    delta = d["close"].diff()
    gain = _wilder(delta.clip(lower=0), 14)
    loss = _wilder((-delta).clip(lower=0), 14)
    rs = gain / loss.replace(0, np.nan)
    d["rsi"] = 100.0 - 100.0 / (1.0 + rs)

    up = d["high"].diff()
    down = -d["low"].diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    plus_di = 100.0 * _wilder(plus_dm, 14) / d["atr"].replace(0, np.nan)
    minus_di = 100.0 * _wilder(minus_dm, 14) / d["atr"].replace(0, np.nan)
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    d["adx"] = _wilder(dx, 14)

    d["ema9"] = d["close"].ewm(span=9, adjust=False, min_periods=9).mean()
    d["ema20"] = d["close"].ewm(span=20, adjust=False, min_periods=20).mean()
    d["ema20_slope3"] = d["ema20"] / d["ema20"].shift(3) - 1.0
    low14 = d["low"].rolling(14, min_periods=14).min()
    high14 = d["high"].rolling(14, min_periods=14).max()
    d["stoch_k"] = 100.0 * (d["close"] - low14) / (high14 - low14).replace(0, np.nan)
    d["stoch_d"] = d["stoch_k"].rolling(3, min_periods=3).mean()

    direction = np.sign(d["close"].diff()).fillna(0)
    d["obv"] = (direction * d["volume"].fillna(0)).cumsum()
    d["obv_up5"] = d["obv"] > d.groupby("session")["obv"].shift(5)
    d["adx_inc3"] = (
        (d["adx"] > d["adx"].shift(1))
        & (d["adx"].shift(1) > d["adx"].shift(2))
        & (d["adx"].shift(2) > d["adx"].shift(3))
    )
    d["rsi_inc2"] = (
        (d["rsi"] > d["rsi"].shift(1))
        & (d["rsi"].shift(1) > d["rsi"].shift(2))
    )

    typical = (d["high"] + d["low"] + d["close"]) / 3.0
    pv = typical * d["volume"].fillna(0)
    cum_pv = pv.groupby(d["session"]).cumsum()
    cum_volume = d["volume"].fillna(0).groupby(d["session"]).cumsum()
    fallback = typical.groupby(d["session"]).expanding().mean().reset_index(level=0, drop=True)
    d["avwap"] = (cum_pv / cum_volume.replace(0, np.nan)).fillna(fallback)
    d["avwap_ext"] = (d["close"] / d["avwap"] - 1.0) * 100.0

    slot_median = d.groupby("minute", sort=False)["volume"].transform(
        lambda values: values.shift(1).rolling(10, min_periods=5).median()
    )
    d["rel_volume"] = d["volume"] / slot_median.replace(0, np.nan)
    candle_range = (d["high"] - d["low"]).replace(0, np.nan)
    d["range_atr"] = candle_range / d["atr"].replace(0, np.nan)
    d["close_loc"] = (d["close"] - d["low"]) / candle_range
    d["upper_wick_frac"] = (
        d["high"] - d[["open", "close"]].max(axis=1)
    ) / candle_range
    d["traded_value"] = d["close"] * d["volume"]
    d["prev_high10"] = d.groupby("session")["high"].transform(
        lambda values: values.shift(1).rolling(10, min_periods=10).max()
    )

    mid20 = d.groupby("session")["close"].transform(
        lambda values: values.rolling(20, min_periods=20).mean()
    )
    std20 = d.groupby("session")["close"].transform(
        lambda values: values.rolling(20, min_periods=20).std()
    )
    width = 4.0 * std20 / mid20.replace(0, np.nan)
    q25 = width.groupby(d["session"]).transform(
        lambda values: values.shift(1).rolling(20, min_periods=20).quantile(0.25)
    )
    d["bb_compressed"] = width.groupby(d["session"]).shift(1) <= q25
    d["valid"] = (
        d[["open", "high", "low", "close", "volume"]].notna().all(axis=1)
        & (d["close"] > 0)
        & (d["high"] >= d[["open", "close", "low"]].max(axis=1))
        & (d["low"] <= d[["open", "close", "high"]].min(axis=1))
        & ~d.get("gap_filled", pd.Series(False, index=d.index)).fillna(False).astype(bool)
        & ~d.get("opening_snapshot", pd.Series(False, index=d.index)).fillna(False).astype(bool)
    )
    return d


def _stock_mask(d: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    score = (
        d["adx"].between(14, 28).astype(int)
        + d["adx_inc3"].astype(int)
        + d["rsi"].between(52, 68).astype(int)
        + d["rsi_inc2"].astype(int)
        + ((d["stoch_k"] > d["stoch_d"]) & d["stoch_k"].between(20, 88)).astype(int)
        + d["rel_volume"].ge(1.25).astype(int)
        + d["obv_up5"].astype(int)
        + d["close_loc"].ge(0.60).astype(int)
    )
    breakout_extension = (d["close"] / d["prev_high10"] - 1.0) * 100.0
    mask = (
        d["valid"]
        & d["minute"].between(SIGNAL_START_MINUTE, SIGNAL_END_MINUTE)
        & d["close"].ge(50.0)
        & d["traded_value"].ge(1_000_000.0)
        & d["atr_pct"].between(0.15, 0.90)
        & d["close"].ge(d["avwap"])
        & d["avwap_ext"].le(0.60)
        & d["ema9"].gt(d["ema20"])
        & d["ema20_slope3"].gt(0)
        & d["range_atr"].le(2.20)
        & d["upper_wick_frac"].le(0.45)
        & d["bb_compressed"]
        & breakout_extension.between(0.0, 0.80)
        & score.ge(6)
    )
    return mask.fillna(False), score


def signal_for_slot(raw: pd.DataFrame, slot_ist: Any) -> dict[str, Any] | None:
    """Return the first qualifying signal for this ticker/session, if it is slot."""
    slot = pd.Timestamp(slot_ist)
    slot = slot.tz_localize(IST_TZ) if slot.tz is None else slot.tz_convert(IST_TZ)
    if not SIGNAL_START_MINUTE <= slot.hour * 60 + slot.minute <= SIGNAL_END_MINUTE:
        return None
    d = add_features(raw)
    session = slot.normalize()
    same_day = d["session"].eq(session)
    mask, score = _stock_mask(d)
    eligible = d.loc[same_day & mask].sort_values("date")
    if eligible.empty or pd.Timestamp(eligible.iloc[0]["date"]).floor("min") != slot.floor("min"):
        return None
    idx = eligible.index[0]
    row = d.loc[idx]
    breakout = float(row["prev_high10"])
    trigger = math.ceil((float(row["high"]) + TICK_SIZE) / TICK_SIZE - 1e-9) * TICK_SIZE
    cancel = max(float(row["low"]), breakout)
    rank_score = (
        float(score.loc[idx])
        + min(float(row["rel_volume"]), 2.0) * 0.75
        + max(0.0, min(float(row["adx"] - d["adx"].shift(2).loc[idx]), 10.0)) * 0.05
        + max(0.0, min(float(row["rsi"] - d["rsi"].shift(2).loc[idx]), 15.0)) * 0.035
        - max(0.0, float(row["avwap_ext"])) * 1.50
        - max(0.0, float(row["range_atr"])) * 0.20
    )
    return {
        **row.to_dict(),
        "confirmation_score": int(score.loc[idx]),
        "quality_score": float(rank_score),
        "breakout_level": breakout,
        "entry_trigger_price": round(trigger, 2),
        "entry_cancel_price": round(cancel, 2),
        "entry_valid_minutes": ENTRY_VALID_MINUTES,
        "entry_max_gap_pct": ENTRY_MAX_GAP_PCT,
    }


def resolve_entry_1m(
    bars: pd.DataFrame,
    signal_ts: Any,
    trigger: float,
    cancel: float,
    valid_minutes: int = ENTRY_VALID_MINUTES,
    max_gap_pct: float = ENTRY_MAX_GAP_PCT,
) -> tuple[pd.Timestamp, float] | None:
    """Resolve the frozen trigger/cancel rules on 1-minute OHLC."""
    if bars is None or bars.empty or trigger <= 0 or cancel <= 0:
        return None
    signal = pd.Timestamp(signal_ts)
    signal = signal.tz_localize(IST_TZ) if signal.tz is None else signal.tz_convert(IST_TZ)
    start = signal + pd.Timedelta(minutes=1)
    end = signal + pd.Timedelta(minutes=int(valid_minutes))
    sub = bars[(bars.index >= start) & (bars.index <= end)]
    for ts, bar in sub.iterrows():
        op = float(bar["open"])
        high = float(bar["high"])
        low = float(bar["low"])
        if low <= cancel:
            return None
        if high >= trigger:
            fill = max(trigger, op)
            if (fill / trigger - 1.0) * 100.0 > float(max_gap_pct):
                return None
            return pd.Timestamp(ts), float(fill)
    return None


def market_alignment_for_slots(
    tickers: Iterable[str],
    slots: Iterable[Any],
    loader: Callable[[str], pd.DataFrame | None],
) -> dict[str, dict[str, float]]:
    """Calculate exact causal breadth and Nifty EMA alignment for requested slots."""
    normalised = []
    for value in slots:
        ts = pd.Timestamp(value)
        ts = ts.tz_localize(IST_TZ) if ts.tz is None else ts.tz_convert(IST_TZ)
        normalised.append(ts.floor("min"))
    slot_keys = {ts.isoformat(): ts for ts in normalised}
    counts = {key: [0, 0] for key in slot_keys}
    for ticker in tickers:
        name = str(ticker).upper().strip()
        if not name or "NIFTY" in name:
            continue
        try:
            raw = loader(name)
            if raw is None or raw.empty:
                continue
            # Breadth needs only causal same-session AVWAP, so avoid rebuilding
            # the full indicator stack for every universe member a second time.
            d = raw.copy()
            d["date"] = _timestamps(d["date"])
            d = d.dropna(subset=["date"]).sort_values("date").drop_duplicates("date", keep="last")
            for col in ("open", "high", "low", "close", "volume"):
                d[col] = pd.to_numeric(d[col], errors="coerce")
            d["session"] = d["date"].dt.normalize()
            typical = (d["high"] + d["low"] + d["close"]) / 3.0
            pv = typical * d["volume"].fillna(0)
            cum_pv = pv.groupby(d["session"]).cumsum()
            cum_volume = d["volume"].fillna(0).groupby(d["session"]).cumsum()
            d["avwap"] = cum_pv / cum_volume.replace(0, np.nan)
            d["valid"] = (
                d[["open", "high", "low", "close", "volume"]].notna().all(axis=1)
                & (d["close"] > 0)
                & (d["high"] >= d[["open", "close", "low"]].max(axis=1))
                & (d["low"] <= d[["open", "close", "high"]].min(axis=1))
                & ~d.get("gap_filled", pd.Series(False, index=d.index)).fillna(False).astype(bool)
                & ~d.get("opening_snapshot", pd.Series(False, index=d.index)).fillna(False).astype(bool)
            )
            by_time = d.set_index(d["date"].dt.floor("min"))
            for key, slot in slot_keys.items():
                if slot not in by_time.index:
                    continue
                row = by_time.loc[slot]
                if isinstance(row, pd.DataFrame):
                    row = row.iloc[-1]
                if bool(row.get("valid", False)) and np.isfinite(float(row.get("avwap", np.nan))):
                    counts[key][1] += 1
                    counts[key][0] += int(float(row["close"]) >= float(row["avwap"]))
        except Exception:
            continue

    nifty_up = {key: 0.0 for key in slot_keys}
    for nifty_name in ("NIFTY", "NIFTY50_INDEX", "NIFTY50", "NIFTY_50"):
        try:
            raw = loader(nifty_name)
            if raw is None or raw.empty:
                continue
            d = add_features(raw).set_index("date")
            for key, slot in slot_keys.items():
                if slot in d.index:
                    row = d.loc[slot]
                    if isinstance(row, pd.DataFrame):
                        row = row.iloc[-1]
                    nifty_up[key] = float(
                        float(row["close"]) > float(row["ema20"])
                        and float(row["ema9"]) > float(row["ema20"])
                    )
            break
        except Exception:
            continue
    return {
        key: {
            "market_breadth": (
                float(above) / float(total) if total else float("nan")
            ),
            "nifty_ema_up": nifty_up[key],
        }
        for key, (above, total) in counts.items()
    }
