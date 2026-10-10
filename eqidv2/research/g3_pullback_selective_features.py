"""Causal features for selective, observer-only G-3 pullback research.

The dense monitor already contains indicators calculated from completed candles.
This module adds entry-relative context without consulting exits or outcomes.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

IST = "Asia/Kolkata"
FEATURES = (
    "side_short", "trade_age_minutes", "minutes_since_open", "atr_pct",
    "threshold_atr", "momentum3_atr", "momentum5_atr",
    "adverse_body_fraction", "micro_break", "trend_damage", "giveback_atr",
    "volume_ratio20", "close_sma5_atr", "sma5_sma13_atr", "unrealized_atr",
    "favorable_excursion_atr", "giveback_fraction",
)


def _timestamps(values: pd.Series, name: str) -> pd.Series:
    """Interpret naive exchange timestamps as IST, normalize aware ones to UTC."""
    try:
        stamps = pd.to_datetime(values, errors="raise")
        if stamps.dt.tz is None:
            stamps = stamps.dt.tz_localize(IST)
        stamps = stamps.dt.tz_convert("UTC")
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError(f"Invalid {name} timestamps") from exc
    if stamps.isna().any():
        raise ValueError(f"Missing {name} timestamps")
    return stamps


def _numeric(frame: pd.DataFrame, name: str, *, positive: bool = False,
             missing_allowed: bool = False) -> pd.Series:
    try:
        values = pd.to_numeric(frame[name], errors="raise").astype(float)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Invalid numeric {name}") from exc
    invalid = np.isinf(values) | (values.isna() & (not missing_allowed))
    if positive:
        invalid |= values.le(0)
    if invalid.any():
        raise ValueError(f"{name} must be finite" + (" and positive" if positive else ""))
    return values


def build_features(dense: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    """Return every input row, labels intact, with the fixed causal FEATURES.

    Only ``trade_id``, ``entry_price`` and ``entry_ts`` are read from ``trades``.
    Existing signed monitor features already treat unfavorable LONG and SHORT
    moves alike. Newly derived price distances use the same sign convention.
    Favorable excursion is the entry-to-best-completed-close move, not an
    intrabar high/low. ``giveback_fraction`` is zero when that excursion is zero;
    the separate excursion and unrealized features retain that distinction.

    ``model_stage`` and ``is_grid`` are routing/evaluation metadata, not model
    inputs. Rows are never filtered based on an outcome or feature readiness.
    """
    required = {
        "trade_id", "side", "decision_ts", "decision_close", "atr14", "sma5",
        "sma13", "horizon_minutes", "threshold_pct", "feature_ready",
        "momentum3_atr", "momentum5_atr", "adverse_body_fraction",
        "micro_break", "trend_damage", "giveback_atr", "volume_ratio20",
    }
    missing = required - set(dense.columns)
    if missing:
        raise ValueError(f"Missing dense columns: {sorted(missing)}")
    entry_columns = ["trade_id", "entry_price", "entry_ts"]
    missing = set(entry_columns) - set(trades.columns)
    if missing:
        raise ValueError(f"Missing trade columns: {sorted(missing)}")
    if set(entry_columns[1:]) & set(dense.columns):
        raise ValueError("Dense input must not already contain entry_price or entry_ts")
    entries = trades.loc[:, entry_columns].copy()
    if entries.trade_id.isna().any() or entries.trade_id.duplicated().any():
        raise ValueError("Trade IDs must be present and unique in trades")
    if dense.trade_id.isna().any() or not dense.trade_id.isin(entries.trade_id).all():
        raise ValueError("Every dense trade_id must match an entry")
    work = dense.copy().merge(entries, on="trade_id", how="left", sort=False,
                              validate="many_to_one")
    work.index = dense.index.copy()
    if not work.side.isin(["LONG", "SHORT"]).all():
        raise ValueError("Side must be LONG or SHORT")
    horizon = _numeric(work, "horizon_minutes", positive=True)
    if not horizon.isin([5, 30]).all():
        raise ValueError("Supported horizons are 5 and 30 minutes")
    work["decision_ts"] = _timestamps(work.decision_ts, "decision_ts")
    work["entry_ts"] = _timestamps(work.entry_ts, "entry_ts")
    if work.duplicated(["trade_id", "horizon_minutes", "decision_ts"]).any():
        raise ValueError("Duplicate trade/horizon/decision rows")
    age = (work.decision_ts - work.entry_ts).dt.total_seconds() / 60
    if age.le(0).any():
        raise ValueError("Decisions must be strictly after entry")
    close = _numeric(work, "decision_close", positive=True)
    atr = _numeric(work, "atr14", positive=True)
    entry = _numeric(work, "entry_price", positive=True)
    sma5 = _numeric(work, "sma5", positive=True)
    sma13 = _numeric(work, "sma13", positive=True)
    threshold = _numeric(work, "threshold_pct", positive=True)
    for name in ("momentum3_atr", "momentum5_atr", "adverse_body_fraction",
                 "giveback_atr"):
        work[name] = _numeric(work, name)
    work["volume_ratio20"] = _numeric(work, "volume_ratio20", missing_allowed=True)
    for name in ("micro_break", "trend_damage"):
        if not work[name].isin([True, False]).all():
            raise ValueError(f"{name} must be boolean")
        work[name] = work[name].astype(int)
    if work.giveback_atr.lt(-1e-10).any():
        raise ValueError("giveback_atr must be nonnegative")
    work["side_short"] = work.side.eq("SHORT").astype(int)
    sign = 1 - 2 * work.side_short
    favorable_price = close + sign * work.giveback_atr * atr
    if favorable_price.le(0).any():
        raise ValueError("Reconstructed favorable price must be positive")
    favorable_move = sign * (favorable_price - entry)
    if favorable_move.lt(-1e-9).any():
        raise ValueError("Favorable close must be at least as favorable as entry")
    favorable_move = favorable_move.clip(lower=0)
    local = work.decision_ts.dt.tz_convert(IST)
    market_open = local.dt.normalize() + pd.Timedelta(hours=9, minutes=15)
    work["trade_age_minutes"] = age
    work["minutes_since_open"] = (local - market_open).dt.total_seconds() / 60
    work["atr_pct"] = 100 * atr / close
    work["threshold_atr"] = threshold / work.atr_pct
    work["close_sma5_atr"] = sign * (close - sma5) / atr
    work["sma5_sma13_atr"] = sign * (sma5 - sma13) / atr
    work["unrealized_atr"] = sign * (close - entry) / atr
    work["favorable_excursion_atr"] = favorable_move / atr
    work["favorable_excursion_pct"] = 100 * favorable_move / entry
    work["giveback_fraction"] = (
        work.giveback_atr / work.favorable_excursion_atr.where(
            work.favorable_excursion_atr.gt(1e-12))
    ).fillna(0.0)
    work["model_stage"] = np.select(
        [age.le(30), work.favorable_excursion_pct.ge(.30 - 1e-10)],
        ["ENTRY_RISK", "PROFIT_GIVEBACK"], default="LATE_NO_ESTABLISHED_PROFIT",
    )
    first = work.groupby(["trade_id", "horizon_minutes"], sort=False)[
        "decision_ts"].transform("min")
    elapsed = (work.decision_ts - first).dt.total_seconds() / 60
    work["is_grid"] = np.isclose(elapsed % horizon, 0.0, atol=1e-9, rtol=0)
    return work
