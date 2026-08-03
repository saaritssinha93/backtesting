"""Pure, causal pre-entry momentum features shared by V7 live and V11.

OHLCV features are calculated only from the supplied immutable raw 1-minute
snapshot.  The function never substitutes a mutable historical parquet.  The
completed 5-minute signal values are supplied on the candidate row, freezing
the exact inputs used by the live scanner.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd


IST_TZ = "Asia/Kolkata"
FEATURE_VERSION = "shared_immutable_slot_features_v1"


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return out if np.isfinite(out) else float(default)


def normalise_1m_bars(raw: pd.DataFrame | None) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()
    bars = raw.reset_index() if "date" not in raw.columns and raw.index.name == "date" else raw.copy()
    if "date" not in bars.columns:
        return pd.DataFrame()
    bars["date"] = pd.to_datetime(bars["date"], errors="coerce")
    if getattr(bars["date"].dt, "tz", None) is None:
        bars["date"] = bars["date"].dt.tz_localize(IST_TZ)
    else:
        bars["date"] = bars["date"].dt.tz_convert(IST_TZ)
    for col in ("open", "high", "low", "close", "volume", "ADX", "RSI"):
        if col in bars.columns:
            bars[col] = pd.to_numeric(bars[col], errors="coerce")
    required = ["date", "open", "high", "low", "close", "volume"]
    if not set(required).issubset(bars.columns):
        return pd.DataFrame()
    return (
        bars.dropna(subset=["date"])
        .sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )


def _normalise_ts(value: Any) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tz is None:
        return ts.tz_localize(IST_TZ)
    return ts.tz_convert(IST_TZ)


def _last_finite(series: pd.Series) -> float:
    vals = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    return float(vals.iloc[-1]) if len(vals) else float("nan")


def _calc_rsi_last(bars: pd.DataFrame, period: int = 14) -> float:
    if "close" not in bars.columns or len(bars) < period + 1:
        return float("nan")
    close = pd.to_numeric(bars["close"], errors="coerce")
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    rsi = rsi.mask((avg_loss == 0.0) & (avg_gain > 0.0), 100.0)
    rsi = rsi.mask((avg_loss == 0.0) & (avg_gain <= 0.0), 50.0)
    return _last_finite(rsi)


def _calc_adx_last(bars: pd.DataFrame, period: int = 14) -> float:
    if not {"high", "low", "close"}.issubset(bars.columns) or len(bars) < period + 2:
        return float("nan")
    high = pd.to_numeric(bars["high"], errors="coerce")
    low = pd.to_numeric(bars["low"], errors="coerce")
    close = pd.to_numeric(bars["close"], errors="coerce")
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
        index=bars.index,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=bars.index,
    )
    tr = pd.concat(
        [high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean().replace(0.0, np.nan)
    plus_di = 100.0 * plus_dm.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean() / atr
    minus_di = 100.0 * minus_dm.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean() / atr
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    return _last_finite(dx.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean())


def calculate_features(
    *,
    raw_1m: pd.DataFrame,
    candidate: Mapping[str, Any],
    entry_price: float,
    stop_price: float,
    cutoff_ist: Any,
) -> tuple[dict[str, float], str]:
    """Return deterministic features using data strictly before ``cutoff_ist``."""

    ticker = str(candidate.get("ticker", "")).upper().strip()
    side = str(candidate.get("side", "")).upper().strip()
    entry = _safe_float(entry_price)
    stop = _safe_float(stop_price)
    risk = abs(entry - stop)
    cutoff = _normalise_ts(cutoff_ist)
    if not ticker or side not in {"LONG", "SHORT"} or not np.isfinite(risk) or risk <= 0:
        return {}, "invalid entry/risk"
    if pd.isna(cutoff):
        return {}, "invalid cutoff"

    bars = normalise_1m_bars(raw_1m)
    if bars.empty:
        return {}, "missing immutable raw 1m snapshot"
    bars = bars[
        (bars["date"].dt.date == cutoff.date())
        & (bars["date"] < cutoff.floor("min"))
    ].copy()
    if len(bars) < 16:
        return {"pre_bars": float(len(bars))}, f"insufficient pre-entry 1m bars ({len(bars)})"

    d = 1.0 if side == "LONG" else -1.0
    last = bars.iloc[-1]
    close = _safe_float(last.get("close"))
    open_ = _safe_float(last.get("open"))
    high = _safe_float(last.get("high"))
    low = _safe_float(last.get("low"))
    rng = max(high - low, 1e-9)
    out: dict[str, float] = {"pre_bars": float(len(bars))}

    for n in (1, 2, 3, 5, 10, 15):
        old_close = _safe_float(bars.iloc[-(n + 1)].get("close")) if len(bars) > n else float("nan")
        out[f"pre{n}_mom_r"] = (
            float(d * (close - old_close) / risk)
            if np.isfinite(old_close)
            else float("nan")
        )

    prior = bars.iloc[:-1].tail(20)
    vol_base = pd.to_numeric(prior["volume"], errors="coerce").mean() if len(prior) else float("nan")
    for n in (3, 5, 10, 15):
        if len(bars) < n:
            continue
        window = bars.tail(n)
        w_high = _safe_float(pd.to_numeric(window["high"], errors="coerce").max())
        w_low = _safe_float(pd.to_numeric(window["low"], errors="coerce").min())
        w_rng = max(w_high - w_low, 1e-9)
        out[f"pre{n}_close_pos"] = float(
            (close - w_low) / w_rng if side == "LONG" else (w_high - close) / w_rng
        )
        open_s = pd.to_numeric(window["open"], errors="coerce")
        close_s = pd.to_numeric(window["close"], errors="coerce")
        out[f"pre{n}_dir_count"] = float((d * (close_s - open_s) > 0).sum())
        out[f"pre{n}_body_sum_r"] = float(d * (close_s - open_s).sum() / risk)
        out[f"pre{n}_range_r"] = float(w_rng / risk)
        out[f"pre{n}_vol_ratio20"] = (
            float(pd.to_numeric(window["volume"], errors="coerce").mean() / vol_base)
            if vol_base and np.isfinite(vol_base)
            else float("nan")
        )

    out["pre1_body_r"] = float(d * (close - open_) / risk)
    out["pre1_close_pos"] = float(
        (close - low) / rng if side == "LONG" else (high - close) / rng
    )
    out["pre1_range_r"] = float((high - low) / risk)
    out["pre1_dir"] = 1.0 if d * (close - open_) > 0 else 0.0
    raw_adx = _last_finite(bars["ADX"]) if "ADX" in bars.columns else float("nan")
    raw_rsi = _last_finite(bars["RSI"]) if "RSI" in bars.columns else float("nan")
    out["pre1_adx"] = raw_adx if np.isfinite(raw_adx) else _calc_adx_last(bars)
    rsi = raw_rsi if np.isfinite(raw_rsi) else _calc_rsi_last(bars)
    out["pre1_rsi_dir"] = (
        float(rsi if side == "LONG" else 100.0 - rsi)
        if np.isfinite(rsi)
        else float("nan")
    )

    finite_mom = [
        value for value in (
            out.get("pre1_body_r"), out.get("pre3_mom_r"), out.get("pre5_mom_r")
        )
        if value is not None and np.isfinite(value)
    ]
    mom = float(np.mean(finite_mom)) if finite_mom else 0.0
    finite_pos = [
        value for value in (out.get("pre3_close_pos"), out.get("pre5_close_pos"))
        if value is not None and np.isfinite(value)
    ]
    pos = float(np.mean(finite_pos)) if finite_pos else 0.5
    vol = out.get("pre3_vol_ratio20", float("nan"))
    vol_component = min(float(vol), 3.0) / 3.0 if np.isfinite(vol) else 0.33
    out["pre_entry_momentum_score"] = float(
        50 + 25 * np.tanh(2 * mom) + 15 * (pos - 0.5) + 10 * (vol_component - 0.33)
    )

    sig_open = _safe_float(candidate.get("signal_open"))
    sig_high = _safe_float(candidate.get("signal_high"))
    sig_low = _safe_float(candidate.get("signal_low"))
    sig_close = _safe_float(candidate.get("signal_close"))
    if all(np.isfinite(v) for v in (sig_open, sig_high, sig_low, sig_close)):
        sig_rng = max(sig_high - sig_low, 1e-9)
        out["sig5_body_r"] = float(d * (sig_close - sig_open) / risk)
        out["sig5_range_r"] = float(sig_rng / risk)
        out["sig5_close_pos"] = float(
            (sig_close - sig_low) / sig_rng
            if side == "LONG"
            else (sig_high - sig_close) / sig_rng
        )
    sig_adx = _safe_float(candidate.get("signal_adx"))
    sig_rsi = _safe_float(candidate.get("signal_rsi"))
    sig_vol_ratio = _safe_float(
        candidate.get("signal_vol_ratio20", candidate.get("vol_ratio"))
    )
    out["sig5_adx_calc"] = sig_adx
    out["sig5_rsi_dir"] = (
        float(sig_rsi if side == "LONG" else 100.0 - sig_rsi)
        if np.isfinite(sig_rsi)
        else float("nan")
    )
    out["sig5_vol_ratio20"] = sig_vol_ratio
    return out, ""


def evaluate_terms(
    features: Mapping[str, float],
    terms: Sequence[Sequence[Any]],
) -> tuple[bool, str]:
    failed: list[str] = []
    for feature, op, threshold_raw in terms:
        threshold = float(threshold_raw)
        value = _safe_float(features.get(str(feature)))
        if not np.isfinite(value):
            failed.append(f"{feature}=nan {op} {threshold:.6g}")
            continue
        ok = value >= threshold if str(op) == ">=" else value <= threshold
        if not ok:
            failed.append(f"{feature}={value:.4f} {op} {threshold:.6g}")
    return not failed, "; ".join(failed)

