"""Causal price/volume features for completed G4 one-minute candles.

The caller supplies one instrument's history, including pre-entry observations.
``volume`` must be incremental bar volume, not a cumulative session counter.
Stored indicator columns never participate in the calculations.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


REQUIRED_COLUMNS = ("ts", "open", "high", "low", "close", "volume")
FEATURE_COLUMNS = (
    "ema20", "atr14", "session_vwap", "relvol20", "relvol_time",
    "roc3_atr", "ema_slope3_atr", "upper_wick_ratio", "lower_wick_ratio",
    "body_ratio",
)


def _validated_frame(raw: pd.DataFrame) -> pd.DataFrame:
    missing = set(REQUIRED_COLUMNS).difference(raw.columns)
    if missing:
        raise ValueError(f"Missing required OHLCV columns: {sorted(missing)}")
    if not raw.columns.is_unique:
        raise ValueError("OHLCV column names must be unique")
    if not isinstance(raw["ts"].dtype, pd.DatetimeTZDtype):
        raise ValueError("ts must contain timezone-aware pandas timestamps")
    if raw["ts"].isna().any():
        raise ValueError("ts must not contain missing timestamps")
    if raw["ts"].duplicated().any():
        raise ValueError("Duplicate OHLCV timestamps are not permitted")

    frame = raw.sort_values("ts", kind="stable").reset_index(drop=True).copy()
    frame["ts"] = frame["ts"].dt.tz_convert("Asia/Kolkata").dt.as_unit("ns")
    for column in REQUIRED_COLUMNS[1:]:
        try:
            values = pd.to_numeric(frame[column], errors="raise")
            if pd.api.types.is_complex_dtype(values.dtype):
                raise ValueError("Complex prices or volumes are invalid")
            frame[column] = values.astype("float64")
        except (ValueError, TypeError) as exc:
            raise ValueError(f"{column} must contain real numeric values") from exc
        if not np.isfinite(frame[column].to_numpy()).all():
            raise ValueError(f"{column} must contain only finite values")
    if (frame[list(REQUIRED_COLUMNS[1:5])] <= 0).any().any():
        raise ValueError("OHLC prices must be positive")
    if (frame["volume"] < 0).any():
        raise ValueError("volume must be nonnegative")
    if ((frame["high"] < frame[["open", "close", "low"]].max(axis=1))
            | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))).any():
        raise ValueError("OHLC ordering requires low <= open/close <= high")
    return frame


def _same_time_prior_volume(frame: pd.DataFrame, sessions: pd.Series) -> np.ndarray:
    """Mean same-minute volume over the preceding 20 observed sessions.

    The session grid ensures a missing minute does not stretch the window beyond
    20 sessions. At least five previous nonmissing observations are required.
    If input contains several distinct timestamps in a minute, their volumes are
    summed for historical minute totals; current-session totals are never used.
    """
    minutes = frame["ts"].dt.hour * 60 + frame["ts"].dt.minute
    grid = pd.DataFrame({"session": sessions, "minute": minutes, "volume": frame["volume"]})
    history = grid.groupby(["session", "minute"], sort=True)["volume"].sum().unstack("minute")
    prior = history.shift(1).rolling(20, min_periods=5).mean()
    rows = prior.index.get_indexer(sessions)
    columns = prior.columns.get_indexer(minutes)
    return prior.to_numpy()[rows, columns]


def build_features(raw: pd.DataFrame) -> pd.DataFrame:
    """Return timestamp-sorted rows plus trailing OHLCV features.

    All features include the completed candle at ``ts``. EMA and ATR span session
    boundaries; VWAP resets each IST date. Relative volume uses either the prior
    20 bars (minimum 20) or matching minutes in the prior 20 sessions (minimum 5).
    Zero or negative denominators produce NaN. Extra input columns are preserved,
    while any columns named in FEATURE_COLUMNS are recomputed from raw OHLCV.
    The caller's frame is not modified; output has a fresh RangeIndex.
    """
    frame = _validated_frame(raw)
    if frame.empty:
        for name in FEATURE_COLUMNS:
            frame[name] = pd.Series(dtype="float64")
        return frame

    close = frame["close"]
    frame["ema20"] = close.ewm(span=20, adjust=False).mean()
    prior_close = close.shift(1)
    true_range = pd.concat([
        frame["high"] - frame["low"],
        (frame["high"] - prior_close).abs(),
        (frame["low"] - prior_close).abs(),
    ], axis=1).max(axis=1)
    frame["atr14"] = true_range.rolling(14, min_periods=14).mean()

    sessions = frame["ts"].dt.normalize()
    typical_price = (frame["high"] + frame["low"] + close) / 3.0
    cumulative_volume = frame["volume"].groupby(sessions).cumsum()
    cumulative_value = (typical_price * frame["volume"]).groupby(sessions).cumsum()
    frame["session_vwap"] = cumulative_value / cumulative_volume.where(cumulative_volume > 0)

    prior_volume20 = frame["volume"].shift(1).rolling(20, min_periods=20).mean()
    frame["relvol20"] = frame["volume"] / prior_volume20.where(prior_volume20 > 0)
    prior_same_time = pd.Series(_same_time_prior_volume(frame, sessions), index=frame.index)
    frame["relvol_time"] = frame["volume"] / prior_same_time.where(prior_same_time > 0)

    positive_atr = frame["atr14"].where(frame["atr14"] > 0)
    frame["roc3_atr"] = (close - close.shift(3)) / positive_atr
    frame["ema_slope3_atr"] = (frame["ema20"] - frame["ema20"].shift(3)) / positive_atr
    candle_range = (frame["high"] - frame["low"]).where(frame["high"] > frame["low"])
    frame["upper_wick_ratio"] = (frame["high"] - frame[["open", "close"]].max(axis=1)) / candle_range
    frame["lower_wick_ratio"] = (frame[["open", "close"]].min(axis=1) - frame["low"]) / candle_range
    frame["body_ratio"] = (close - frame["open"]).abs() / candle_range
    return frame
