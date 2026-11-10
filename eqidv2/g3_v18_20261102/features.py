"""Causal market/stock participation features for isolated G3 V18 research.

All five-minute timestamps label COMPLETED bar ends. The VWAP below is a
five-minute typical-price approximation, not exchange trade-level VWAP.
Pressure is a candle/volume proxy, not measured aggressor order flow. No
future outcomes, filled-forward market bars, or retrospective daily totals
enter these calculations. Prices must already use one consistent adjustment
basis. ``prev_close`` in the source pool means prior BAR, not prior session.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

IST = "Asia/Kolkata"
SOURCE_FLAGS = ("gap_filled", "opening_snapshot", "provisional_stale", "is_synthetic")
REQUIRED = ("tradingsymbol", "signal_ts", "open", "high", "low", "close", "volume", "source_1m_count")


def _flags(values: pd.Series) -> np.ndarray:
    return values.astype(str).str.strip().str.lower().isin(("true", "1", "1.0", "yes", "on")).to_numpy()


def _timestamps(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values, utc=True, errors="raise").dt.tz_convert(IST)


def _numeric(frame: pd.DataFrame, name: str) -> np.ndarray:
    return pd.to_numeric(frame[name], errors="coerce").to_numpy(dtype=float)


def _valid_ohlc(frame: pd.DataFrame) -> np.ndarray:
    o, h, low, c = (_numeric(frame, name) for name in ("open", "high", "low", "close"))
    valid = np.isfinite(np.column_stack((o, h, low, c))).all(axis=1)
    valid &= (o > 0) & (low > 0) & (c > 0) & (h >= np.maximum(o, c)) & (low <= np.minimum(o, c))
    for flag in SOURCE_FLAGS:
        if flag in frame:
            valid &= ~_flags(frame[flag])
    return valid


def _continuous_prefix(frame: pd.DataFrame, stamps: pd.Series, *, stock: bool) -> np.ndarray:
    """Require every exact five-minute bar from 09:20 through this row."""
    start = stamps.iloc[0].normalize() + pd.Timedelta(hours=9, minutes=20)
    expected = pd.date_range(start, periods=len(frame), freq="5min")
    valid = _valid_ohlc(frame) & (stamps.to_numpy() == expected.to_numpy())
    valid &= ~stamps.duplicated(keep=False).to_numpy()
    if stock:
        volume = _numeric(frame, "volume")
        valid &= np.isfinite(volume) & (volume >= 0) & (_numeric(frame, "source_1m_count") == 5)
    return np.logical_and.accumulate(valid)


def enrich_day(raw: pd.DataFrame, nifty: pd.DataFrame, expected_universe: int | None = None) -> pd.DataFrame:
    """Return original rows/index plus causal ``v18_`` fields for ONE session.

    ``nifty`` must be the true Nifty spot OHLC series, with ``ts`` or ``date``
    timestamps. The caller must verify instrument identity and source hashes.
    Supply a dated, point-in-time stock-universe count when available. If it
    is absent, coverage uses the observed opening cohort and is explicitly
    labelled as such. ``breadth_ready`` requires >=95% coverage and never
    >100%; numerical breadth is also exposed at lower nonzero coverage so
    explicitly documented research variants may apply their own threshold.

    Directional features (RS, VWAP distance/slope, pressure, extension) have
    positive values for bullish movement; SHORT selectors multiply by -1.
    A first-bar VWAP slope is unknown (NaN), never filled from a future bar.
    Missing/bad stock prefix disables that stock for the remaining session.
    Missing/bad Nifty prefix disables market context for the remainder.
    """
    missing = set(REQUIRED) - set(raw.columns)
    if missing:
        raise ValueError(f"Missing stock feature columns: {sorted(missing)}")
    out = raw.copy()
    float_fields = ("vwap", "vwap_distance_pct", "vwap_slope_pct", "relative_strength_pct",
                    "stock_from_open_pct", "nifty_from_open_pct", "breadth_above_open_pct", "breadth_below_open_pct",
                    "breadth_coverage_pct", "efficiency_6", "pressure_6", "range_extension")
    for name in float_fields:
        out[f"v18_{name}"] = np.nan
    for name in ("stock_ready", "market_ready", "breadth_ready"):
        out[f"v18_{name}"] = False
    out["v18_breadth_universe_basis"] = "DATED_UNIVERSE" if expected_universe is not None else "OBSERVED_OPENING_COHORT"
    if out.empty:
        return out
    stamp = _timestamps(raw.signal_ts)
    days = stamp.dt.normalize().unique()
    if len(days) != 1:
        raise ValueError("enrich_day requires exactly one session")
    day = stamp.iloc[0].normalize()
    if raw.tradingsymbol.isna().any():
        raise ValueError("Stock symbol is missing")
    work = raw.copy().reset_index(drop=True)
    work["_ts"] = stamp.to_numpy()
    # Explicit Timestamp construction preserves timezone across pandas versions.
    work["_ts"] = _timestamps(work["_ts"])
    work["_position"] = np.arange(len(work))
    work = work.sort_values(["tradingsymbol", "_ts"], kind="stable")
    values = {f"v18_{name}": np.full(len(work), np.nan) for name in float_fields}
    stock_ready = np.zeros(len(work), dtype=bool)
    open_clock = day + pd.Timedelta(hours=9, minutes=20)
    opening_cohort = set(work.loc[work._ts.eq(open_clock), "tradingsymbol"].astype(str))
    denominator = expected_universe if expected_universe is not None else len(opening_cohort)
    if isinstance(denominator, bool) or not isinstance(denominator, (int, np.integer)) or denominator < 0:
        raise ValueError("expected_universe must be a nonnegative integer")
    for _, part in work.groupby("tradingsymbol", sort=False):
        pos = part._position.to_numpy(dtype=int)
        valid = _continuous_prefix(part, part._ts, stock=True)
        o, h, low, close, vol = (_numeric(part, name) for name in ("open", "high", "low", "close", "volume"))
        cumulative_volume = np.cumsum(np.where(np.isfinite(vol), vol, 0.))
        numerator = np.cumsum(np.nan_to_num((h + low + close) / 3 * vol, nan=0.))
        vwap = np.divide(numerator, cumulative_volume, out=np.full(len(part), np.nan), where=cumulative_volume > 0)
        valid &= cumulative_volume > 0
        vwap[~valid] = np.nan
        stock_ready[pos] = valid
        values["v18_vwap"][pos] = vwap
        values["v18_vwap_distance_pct"][pos] = 100 * (close / vwap - 1)
        prior_vwap = pd.Series(vwap).shift(1).to_numpy()
        values["v18_vwap_slope_pct"][pos] = 100 * (vwap / prior_vwap - 1)
        values["v18_stock_from_open_pct"][pos] = np.where(valid, 100 * (close / o[0] - 1), np.nan)
        previous = np.r_[o[0], close[:-1]]
        true_range = np.maximum.reduce((h - low, np.abs(h - previous), np.abs(low - previous)))
        mean_range = pd.Series(true_range).rolling(6, min_periods=3).mean().to_numpy()
        values["v18_range_extension"][pos] = np.divide(close - vwap, mean_range, out=np.full(len(part), np.nan), where=mean_range > 0)
        # Rolling path begins at the first included bar's prior close (session
        # open for the first bar), so net/path efficiency is bounded by one.
        moves = np.abs(close - previous)
        path_length = pd.Series(moves).rolling(6, min_periods=2).sum().to_numpy()
        starting = np.r_[np.full(min(6, len(part)), o[0]), close[:max(0, len(part)-6)]]
        efficiency = np.divide(np.abs(close - starting), path_length, out=np.full(len(part), np.nan), where=path_length > 0)
        efficiency[~valid] = np.nan
        values["v18_efficiency_6"][pos] = efficiency
        location = np.divide(2 * close - h - low, h - low, out=np.zeros(len(part)), where=(h - low) > 0)
        weighted_location = pd.Series(location * vol).rolling(6, min_periods=1).sum().to_numpy()
        rolling_volume = pd.Series(vol).rolling(6, min_periods=1).sum().to_numpy()
        pressure = np.divide(weighted_location, rolling_volume, out=np.full(len(part), np.nan), where=rolling_volume > 0)
        pressure[~valid] = np.nan
        values["v18_pressure_6"][pos] = pressure

    # Build the exact-clock spot map, never an as-of or nearest-bar merge.
    time_field = "ts" if "ts" in nifty else "date"
    if time_field not in nifty or set(("open", "high", "low", "close")) - set(nifty.columns):
        raise ValueError("Nifty requires ts/date and OHLC")
    market = nifty.copy()
    market["_ts"] = _timestamps(market[time_field])
    market = market.loc[market._ts.dt.normalize().eq(day) & market._ts.ge(open_clock)].sort_values("_ts", kind="stable")
    market_valid = np.zeros(len(raw), dtype=bool)
    nifty_returns = np.full(len(raw), np.nan)
    if len(market):
        ready = _continuous_prefix(market, market._ts, stock=False)
        market_return = np.where(ready, 100 * (_numeric(market, "close") / float(market.open.iloc[0]) - 1), np.nan)
        context = pd.DataFrame({"ts": market._ts.to_numpy(), "ret": market_return, "ready": ready})
        # Duplicated spot bars never have a usable value.
        context = context.loc[~context.ts.duplicated(keep=False)].set_index("ts")
        nifty_returns = stamp.map(context.ret).to_numpy(dtype=float)
        market_valid = stamp.map(context.ready).eq(True).to_numpy(dtype=bool)
    values["v18_nifty_from_open_pct"] = nifty_returns
    values["v18_relative_strength_pct"] = values["v18_stock_from_open_pct"] - nifty_returns
    breadth_ready = np.zeros(len(raw), dtype=bool)
    # Every contributing stock has a validated continuous prefix. A deficient
    # observed cohort is not disguised by shrinking the denominator per bar.
    for _, indices in pd.Series(np.arange(len(raw))).groupby(stamp.reset_index(drop=True), sort=False):
        positions = indices.to_numpy(dtype=int)
        valid_positions = positions[stock_ready[positions]]
        count = len(valid_positions)
        coverage = 100 * count / denominator if denominator else 0.
        values["v18_breadth_coverage_pct"][positions] = coverage
        ready = denominator > 0 and 95 <= coverage <= 100
        breadth_ready[positions] = ready
        if count > 0 and 0 < coverage <= 100:
            breadth = 100 * np.mean(values["v18_stock_from_open_pct"][valid_positions] > 0)
            values["v18_breadth_above_open_pct"][positions] = breadth
            values["v18_breadth_below_open_pct"][positions] = 100 * np.mean(values["v18_stock_from_open_pct"][valid_positions] < 0)
    for name, value in values.items():
        out[name] = value
    out["v18_stock_ready"] = stock_ready
    out["v18_market_ready"] = market_valid
    out["v18_breadth_ready"] = breadth_ready
    return out


def add_time_of_day_rvol(frame: pd.DataFrame, lookback: int = 20, min_periods: int = 5) -> pd.DataFrame:
    """Same-symbol/same-clock volume divided by mean of PRIOR observed days.

    This auxiliary feature has a separate warmup. It is unavailable rather
    than backfilled for the earliest sessions. Missing sessions are not
    imputed; the lookback is the preceding observed valid-session rows.
    """
    if not (1 <= min_periods <= lookback):
        raise ValueError("Require 1 <= min_periods <= lookback")
    out = frame.copy()
    if out.empty:
        out["v18_tod_rvol"] = pd.Series(dtype=float)
        out["v18_tod_history_count"] = pd.Series(dtype=int)
        out["v18_tod_rvol_ready"] = pd.Series(dtype=bool)
        return out
    stamps = _timestamps(frame.signal_ts)
    work = pd.DataFrame({"symbol": frame.tradingsymbol.to_numpy(), "ts": stamps.to_numpy(),
                         "clock": stamps.dt.strftime("%H:%M").to_numpy(),
                         "day": stamps.dt.strftime("%Y-%m-%d").to_numpy(),
                         "volume": pd.to_numeric(frame.volume, errors="coerce").to_numpy(),
                         "position": np.arange(len(frame))})
    if work.duplicated(["symbol", "day", "clock"]).any():
        raise ValueError("Duplicate stock/session/clock for time-of-day volume")
    valid = np.isfinite(work.volume) & work.volume.ge(0)
    if "v18_stock_ready" in frame:
        valid &= frame.v18_stock_ready.to_numpy(dtype=bool)
    work.loc[~valid, "volume"] = np.nan
    ratio, counts = np.full(len(frame), np.nan), np.zeros(len(frame), dtype=int)
    for _, part in work.sort_values("ts", kind="stable").groupby(["symbol", "clock"], sort=False):
        prior = part.volume.shift(1)
        mean = prior.rolling(lookback, min_periods=min_periods).mean()
        count = prior.rolling(lookback, min_periods=0).count().to_numpy(dtype=int)
        pos = part.position.to_numpy(dtype=int)
        ratio[pos] = part.volume.div(mean.where(mean.gt(0))).to_numpy()
        counts[pos] = count
    out["v18_tod_rvol"] = ratio
    out["v18_tod_history_count"] = counts
    out["v18_tod_rvol_ready"] = np.isfinite(ratio)
    return out
