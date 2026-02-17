# -*- coding: utf-8 -*-
"""
ML meta-label filter/sizer for eqidv2 live + backtest usage.

Strategy v1 — AVWAP Rejection + ML Filter + Sizing
===================================================
Expanded feature set (30-60 features), LightGBM support, confidence-based
position sizing, ATR volatility cap, and comprehensive risk controls.
"""
from __future__ import annotations

import json
import math
import pickle
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class MetaFilterConfig:
    model_path: str = "eqidv2/models/meta_model.pkl"
    feature_path: str = "eqidv2/models/meta_features.json"
    pwin_threshold: float = 0.62

    # Position sizing (Section 10)
    risk_per_trade_pct: float = 0.20       # % of capital risked per trade
    conf_mult_min: float = 0.7
    conf_mult_max: float = 1.2
    conf_mult_ref_upper: float = 0.75      # T_upper for scaling formula
    atr_pctile_vol_cap: float = 80.0       # ATR percentile above which m <= 1.0

    # Risk controls (Section 11)
    max_open_positions: int = 3
    max_trades_per_day: int = 10
    max_trades_per_ticker_per_day: int = 1
    daily_loss_kill_R: float = -1.0        # kill-switch in R units
    daily_loss_kill_capital_pct: float = -0.8  # kill-switch as % capital
    no_entry_after_ist: str = "14:30"      # HH:MM IST cutoff
    eod_force_exit: bool = True
    min_bars_left_in_day: int = 4

    # Slippage/fees for backtest realism (Section 5)
    slippage_bps: float = 3.0              # per side
    commission_bps: float = 2.0            # per side

    # Backward compat aliases
    min_mult: float = 0.7
    max_mult: float = 1.2


# ---------------------------------------------------------------------------
# Feature groups (Section 7) — canonical feature names
# ---------------------------------------------------------------------------
FEATURE_GROUPS = {
    "A_price_vol": [
        "ret_1", "ret_2", "ret_3",
        "atr_val", "tr_val", "atr_pctile_50",
        "bb_width", "bb_position",
        "range_to_atr",
    ],
    "B_trend_structure": [
        "ema20_slope", "ema50_slope",
        "adx_val", "di_plus", "di_minus", "di_diff",
        "vwap_dist_atr", "avwap_dist_atr",
    ],
    "C_volume_liquidity": [
        "vol_zscore_20", "dollar_vol", "illiquidity_proxy",
    ],
    "D_time_context": [
        "minute_bucket", "bars_left_in_day",
        "gap_pct",
    ],
    "E_avwap_rejection": [
        "touch_depth", "rejection_body_ratio",
        "upper_wick_ratio", "consec_below_avwap",
        "pullback_from_low20_atr",
    ],
    "F_side": [
        "side",
    ],
}

ALL_FEATURES: List[str] = []
for _grp in FEATURE_GROUPS.values():
    ALL_FEATURES.extend(_grp)


# ---------------------------------------------------------------------------
# Feature builder — works on a single signal dict (candle-level data)
# ---------------------------------------------------------------------------
def build_feature_vector(signal: Dict[str, Any]) -> Dict[str, float]:
    """
    Build the full v1 feature vector from a signal dict.

    The signal dict should contain raw indicator/candle values keyed by
    their canonical names (or common aliases).  Missing values are filled
    with safe defaults so inference never crashes.
    """

    def _f(key: str, default: float = 0.0) -> float:
        v = signal.get(key, default)
        try:
            v = float(v)
            return v if np.isfinite(v) else default
        except Exception:
            return default

    # --- A) Price & Volatility ---
    ret_1 = _f("ret_1")
    ret_2 = _f("ret_2")
    ret_3 = _f("ret_3")
    atr_val = _f("atr_val", _f("atr", _f("ATR15", 0.0)))
    tr_val = _f("tr_val", _f("tr", 0.0))
    atr_pctile_50 = _f("atr_pctile_50", 50.0)
    bb_width = _f("bb_width")
    bb_position = _f("bb_position", 0.5)
    range_to_atr = _f("range_to_atr")

    # --- B) Trend & Structure ---
    ema20_slope = _f("ema20_slope")
    ema50_slope = _f("ema50_slope")
    adx_val = _f("adx_val", _f("adx", _f("adx_signal", _f("ADX15", 20.0))))
    di_plus = _f("di_plus", _f("DI_PLUS15", 0.0))
    di_minus = _f("di_minus", _f("DI_MINUS15", 0.0))
    di_diff = _f("di_diff", di_minus - di_plus)
    vwap_dist_atr = _f("vwap_dist_atr")
    avwap_dist_atr = _f("avwap_dist_atr", _f("AVWAP_dist_atr", 0.0))

    # --- C) Volume & Liquidity ---
    vol_zscore_20 = _f("vol_zscore_20")
    dollar_vol = _f("dollar_vol")
    illiquidity_proxy = _f("illiquidity_proxy")

    # --- D) Time & Context ---
    minute_bucket = _f("minute_bucket")
    bars_left_in_day = _f("bars_left_in_day")
    gap_pct = _f("gap_pct")

    # --- E) AVWAP Rejection Quality ---
    touch_depth = _f("touch_depth")
    rejection_body_ratio = _f("rejection_body_ratio")
    upper_wick_ratio = _f("upper_wick_ratio")
    consec_below_avwap = _f("consec_below_avwap")
    pullback_from_low20_atr = _f("pullback_from_low20_atr")

    # --- F) Side ---
    side_raw = str(signal.get("side", "SHORT")).upper().strip()
    side = 1.0 if side_raw == "LONG" else -1.0

    return {
        "ret_1": ret_1,
        "ret_2": ret_2,
        "ret_3": ret_3,
        "atr_val": atr_val,
        "tr_val": tr_val,
        "atr_pctile_50": atr_pctile_50,
        "bb_width": bb_width,
        "bb_position": bb_position,
        "range_to_atr": range_to_atr,
        "ema20_slope": ema20_slope,
        "ema50_slope": ema50_slope,
        "adx_val": adx_val,
        "di_plus": di_plus,
        "di_minus": di_minus,
        "di_diff": di_diff,
        "vwap_dist_atr": vwap_dist_atr,
        "avwap_dist_atr": avwap_dist_atr,
        "vol_zscore_20": vol_zscore_20,
        "dollar_vol": dollar_vol,
        "illiquidity_proxy": illiquidity_proxy,
        "minute_bucket": minute_bucket,
        "bars_left_in_day": bars_left_in_day,
        "gap_pct": gap_pct,
        "touch_depth": touch_depth,
        "rejection_body_ratio": rejection_body_ratio,
        "upper_wick_ratio": upper_wick_ratio,
        "consec_below_avwap": consec_below_avwap,
        "pullback_from_low20_atr": pullback_from_low20_atr,
        "side": side,
    }


def build_features_from_candles(
    df: pd.DataFrame,
    signal_idx: int,
    side: str,
    avwap_col: str = "AVWAP",
    vwap_col: str = "VWAP",
) -> Dict[str, float]:
    """
    Compute the full feature vector from a candle DataFrame at signal bar index.
    Uses ONLY completed candles up to and including signal_idx (no leakage).
    """
    if df.empty or signal_idx < 3:
        return {f: 0.0 for f in ALL_FEATURES}

    def _col(name: str, default: float = np.nan) -> float:
        if name in df.columns:
            v = df.iloc[signal_idx].get(name, default)
            try:
                v = float(v)
                return v if np.isfinite(v) else default
            except Exception:
                return default
        return default

    def _col_at(idx: int, name: str, default: float = np.nan) -> float:
        if idx < 0 or idx >= len(df) or name not in df.columns:
            return default
        v = df.iloc[idx].get(name, default)
        try:
            v = float(v)
            return v if np.isfinite(v) else default
        except Exception:
            return default

    close = _col("close", 0.0)
    open_ = _col("open", close)
    high = _col("high", close)
    low = _col("low", close)
    volume = _col("volume", 0.0)

    # A) Price & Volatility
    close_1 = _col_at(signal_idx - 1, "close", close)
    close_2 = _col_at(signal_idx - 2, "close", close)
    close_3 = _col_at(signal_idx - 3, "close", close)

    eps = 1e-10
    ret_1 = math.log(close / close_1) if close > 0 and close_1 > 0 else 0.0
    ret_2 = math.log(close / close_2) if close > 0 and close_2 > 0 else 0.0
    ret_3 = math.log(close / close_3) if close > 0 and close_3 > 0 else 0.0

    # ATR from indicator columns (try various names)
    atr_val = _col("ATR15", _col("ATR", _col("atr", 0.0)))
    tr_val = high - low

    # ATR percentile over last 50 bars
    start_pctile = max(0, signal_idx - 49)
    if "ATR15" in df.columns:
        atr_window = pd.to_numeric(df["ATR15"].iloc[start_pctile:signal_idx + 1], errors="coerce").dropna()
    elif "ATR" in df.columns:
        atr_window = pd.to_numeric(df["ATR"].iloc[start_pctile:signal_idx + 1], errors="coerce").dropna()
    else:
        atr_window = pd.Series(dtype=float)

    if len(atr_window) >= 5 and atr_val > 0:
        atr_pctile_50 = float((atr_window <= atr_val).mean() * 100.0)
    else:
        atr_pctile_50 = 50.0

    # Bollinger Band features
    bb_upper = _col("BB_UPPER", _col("bb_upper", np.nan))
    bb_lower = _col("BB_LOWER", _col("bb_lower", np.nan))
    if np.isfinite(bb_upper) and np.isfinite(bb_lower) and (bb_upper - bb_lower) > eps:
        bb_width = (bb_upper - bb_lower) / (close + eps)
        bb_position = (close - bb_lower) / (bb_upper - bb_lower)
    else:
        bb_width = 0.0
        bb_position = 0.5

    range_to_atr = (high - low) / (atr_val + eps) if atr_val > 0 else 0.0

    # B) Trend & Structure
    # EMA slopes over last 6 bars
    def _ema_slope(col_name: str, lookback: int = 6) -> float:
        if col_name not in df.columns:
            return 0.0
        start_s = max(0, signal_idx - lookback + 1)
        vals = pd.to_numeric(df[col_name].iloc[start_s:signal_idx + 1], errors="coerce").dropna()
        if len(vals) < 2:
            return 0.0
        return float((vals.iloc[-1] - vals.iloc[0]) / (len(vals) * (close + eps)))

    ema20_slope = _ema_slope("EMA20", 6)
    ema50_slope = _ema_slope("EMA50", 6)

    adx_val = _col("ADX15", _col("ADX", 20.0))
    di_plus = _col("DI_PLUS15", _col("DI_PLUS", _col("PLUS_DI", 0.0)))
    di_minus = _col("DI_MINUS15", _col("DI_MINUS", _col("MINUS_DI", 0.0)))
    di_diff = di_minus - di_plus

    vwap = _col(vwap_col, _col("VWAP", close))
    avwap = _col(avwap_col, close)
    vwap_dist_atr = (close - vwap) / (atr_val + eps) if atr_val > 0 else 0.0
    avwap_dist_atr = (close - avwap) / (atr_val + eps) if atr_val > 0 else 0.0

    # C) Volume & Liquidity
    start_vol = max(0, signal_idx - 19)
    if "volume" in df.columns:
        vol_window = pd.to_numeric(df["volume"].iloc[start_vol:signal_idx + 1], errors="coerce").dropna()
    else:
        vol_window = pd.Series(dtype=float)

    if len(vol_window) >= 5:
        vol_mean = float(vol_window.mean())
        vol_std = float(vol_window.std())
        vol_zscore_20 = (volume - vol_mean) / (vol_std + eps)
    else:
        vol_zscore_20 = 0.0

    dollar_vol = close * volume
    illiquidity_proxy = ((high - low) / (close + eps)) / max(volume, 1.0)

    # D) Time & Context
    if "date" in df.columns:
        ts = df.iloc[signal_idx]["date"]
        try:
            ts = pd.Timestamp(ts)
            minute_bucket = float((ts.hour * 60 + ts.minute) // 30)  # 30-min buckets
        except Exception:
            minute_bucket = 0.0
    else:
        minute_bucket = 0.0

    # Bars left in day (assume 5-min candles, market 09:15-15:30 = 75 bars)
    total_5m_bars = 75
    if "date" in df.columns:
        try:
            ts = pd.Timestamp(df.iloc[signal_idx]["date"])
            mins_from_open = (ts.hour * 60 + ts.minute) - (9 * 60 + 15)
            current_bar = max(0, mins_from_open // 5)
            bars_left_in_day = float(max(0, total_5m_bars - current_bar))
        except Exception:
            bars_left_in_day = 30.0
    else:
        bars_left_in_day = 30.0

    # Gap pct vs previous day close
    gap_pct = _col("gap_pct", 0.0)
    if gap_pct == 0.0 and signal_idx > 0:
        prev_close = _col_at(0, "close", close)  # first bar of day
        day_open = _col_at(0, "open", close)
        if prev_close > 0:
            gap_pct = (day_open - prev_close) / prev_close * 100.0

    # E) AVWAP Rejection Quality
    touch_depth = abs(close - avwap) / (atr_val + eps) if atr_val > 0 else 0.0

    body = abs(close - open_)
    candle_range = high - low + eps
    rejection_body_ratio = body / candle_range

    upper_wick = high - max(close, open_)
    upper_wick_ratio = upper_wick / candle_range

    # Consecutive closes below/above AVWAP
    consec = 0
    if avwap_col in df.columns:
        for j in range(signal_idx, max(-1, signal_idx - 10), -1):
            c_j = _col_at(j, "close", np.nan)
            a_j = _col_at(j, avwap_col, np.nan)
            if not np.isfinite(c_j) or not np.isfinite(a_j):
                break
            if side.upper() == "SHORT" and c_j < a_j:
                consec += 1
            elif side.upper() == "LONG" and c_j > a_j:
                consec += 1
            else:
                break
    consec_below_avwap = float(min(consec, 10))

    # Pullback from recent low/high (20 bars) in ATR units
    start_pull = max(0, signal_idx - 19)
    if "low" in df.columns and side.upper() == "SHORT":
        recent_low = float(pd.to_numeric(df["low"].iloc[start_pull:signal_idx + 1], errors="coerce").min())
        pullback_from_low20_atr = (close - recent_low) / (atr_val + eps) if atr_val > 0 and np.isfinite(recent_low) else 0.0
    elif "high" in df.columns and side.upper() == "LONG":
        recent_high = float(pd.to_numeric(df["high"].iloc[start_pull:signal_idx + 1], errors="coerce").max())
        pullback_from_low20_atr = (recent_high - close) / (atr_val + eps) if atr_val > 0 and np.isfinite(recent_high) else 0.0
    else:
        pullback_from_low20_atr = 0.0

    side_enc = 1.0 if side.upper() == "LONG" else -1.0

    return {
        "ret_1": ret_1,
        "ret_2": ret_2,
        "ret_3": ret_3,
        "atr_val": atr_val,
        "tr_val": tr_val,
        "atr_pctile_50": atr_pctile_50,
        "bb_width": bb_width,
        "bb_position": bb_position,
        "range_to_atr": range_to_atr,
        "ema20_slope": ema20_slope,
        "ema50_slope": ema50_slope,
        "adx_val": adx_val,
        "di_plus": di_plus,
        "di_minus": di_minus,
        "di_diff": di_diff,
        "vwap_dist_atr": vwap_dist_atr,
        "avwap_dist_atr": avwap_dist_atr,
        "vol_zscore_20": vol_zscore_20,
        "dollar_vol": dollar_vol,
        "illiquidity_proxy": illiquidity_proxy,
        "minute_bucket": minute_bucket,
        "bars_left_in_day": bars_left_in_day,
        "gap_pct": gap_pct,
        "touch_depth": touch_depth,
        "rejection_body_ratio": rejection_body_ratio,
        "upper_wick_ratio": upper_wick_ratio,
        "consec_below_avwap": consec_below_avwap,
        "pullback_from_low20_atr": pullback_from_low20_atr,
        "side": side_enc,
    }


# ---------------------------------------------------------------------------
# MetaLabelFilter — main inference class
# ---------------------------------------------------------------------------
class MetaLabelFilter:
    """Fast inference wrapper with safe fallback heuristic when model is unavailable."""

    def __init__(self, cfg: Optional[MetaFilterConfig] = None):
        self.cfg = cfg or MetaFilterConfig()
        self.model = None
        self.features: Optional[List[str]] = None
        self._load_model_if_present()

    def _load_model_if_present(self) -> None:
        mp = Path(self.cfg.model_path)
        fp = Path(self.cfg.feature_path)

        if mp.exists() and fp.exists():
            try:
                self.model = pickle.loads(mp.read_bytes())
                self.features = json.loads(fp.read_text(encoding="utf-8"))
            except Exception:
                self.model = None
                self.features = None
        else:
            self.model = None
            self.features = None

    @staticmethod
    def _as_float(x: Any, default: float = 0.0) -> float:
        try:
            if x is None:
                return float(default)
            if isinstance(x, str) and x.strip() == "":
                return float(default)
            return float(x)
        except Exception:
            return float(default)

    def build_feature_row(self, signal: Dict[str, Any]) -> Dict[str, float]:
        """
        Build feature row — supports both legacy (5-feature) and v1 (30-feature) modes.

        If the loaded model expects only legacy features, produce those.
        Otherwise produce the full v1 feature vector.
        """
        # If model expects legacy 5-feature set, produce that
        if self.features and set(self.features) == {"quality_score", "atr_pct", "rsi_centered", "adx_norm", "side"}:
            return self._build_legacy_features(signal)

        # Full v1 feature vector
        return build_feature_vector(signal)

    def _build_legacy_features(self, signal: Dict[str, Any]) -> Dict[str, float]:
        """Legacy 5-feature mode for backward compatibility."""
        quality_score = self._as_float(signal.get("quality_score", 0.0), 0.0)
        atr_pct = self._as_float(signal.get("atr_pct", signal.get("atr_pct_signal", 0.0)), 0.0)
        rsi = self._as_float(signal.get("rsi", signal.get("rsi_signal", 50.0)), 50.0)
        adx = self._as_float(signal.get("adx", signal.get("adx_signal", 20.0)), 20.0)
        side = str(signal.get("side", "LONG")).upper().strip()

        return {
            "quality_score": float(quality_score),
            "atr_pct": float(atr_pct),
            "rsi_centered": float((rsi - 50.0) / 50.0),
            "adx_norm": float(adx / 50.0),
            "side": 1.0 if side == "LONG" else -1.0,
        }

    def predict_pwin(self, signal: Dict[str, Any]) -> float:
        """
        Returns p_win (probability trade is net-positive) in [0.01, 0.99].
        Uses trained model if present; otherwise uses a conservative heuristic.
        """
        if self.model is not None and self.features:
            try:
                row = self.build_feature_row(signal)
                X = pd.DataFrame([{f: row.get(f, 0.0) for f in self.features}])
                p = float(self.model.predict_proba(X)[:, 1][0])
                return float(max(0.01, min(0.99, p)))
            except Exception:
                pass  # fallback below

        # Heuristic fallback (conservative)
        row = build_feature_vector(signal)
        score = 0.0
        score += 0.5 * row.get("adx_val", 20.0) / 50.0
        score += -0.3 * abs(row.get("avwap_dist_atr", 0.0))
        score += 0.2 * row.get("consec_below_avwap", 0.0) / 5.0
        score += -0.4 * max(0.0, row.get("atr_pctile_50", 50.0) - 70.0) / 30.0
        score += 0.3 * row.get("rejection_body_ratio", 0.5)
        score += -0.2 * abs(row.get("ret_1", 0.0)) * 10.0
        score += 0.1 * min(row.get("bars_left_in_day", 30.0), 30.0) / 30.0
        p = 1.0 / (1.0 + math.exp(-score))
        return float(max(0.01, min(0.99, p)))

    def confidence_multiplier(self, p_win: float) -> float:
        """
        Strategy v1 sizing (Section 10):
        Below threshold -> 0.0 (skip trade).
        Above threshold -> m = clip(0.7, 1.2, 0.7 + 1.0*(p - T)/(0.75 - T))
        """
        p = float(p_win)
        T = float(self.cfg.pwin_threshold)
        if p < T:
            return 0.0

        T_upper = float(self.cfg.conf_mult_ref_upper)
        denom = max(1e-9, T_upper - T)
        m = self.cfg.conf_mult_min + 1.0 * (p - T) / denom
        m = max(self.cfg.conf_mult_min, min(self.cfg.conf_mult_max, m))
        return float(m)

    def confidence_multiplier_with_vol_cap(
        self,
        p_win: float,
        atr_pctile: float = 50.0,
    ) -> float:
        """Confidence multiplier with ATR volatility cap (Section 10)."""
        m = self.confidence_multiplier(p_win)
        if m <= 0.0:
            return 0.0
        if atr_pctile > self.cfg.atr_pctile_vol_cap:
            m = min(m, 1.0)
        return float(m)

    def compute_position_size(
        self,
        capital: float,
        stop_distance: float,
        p_win: float,
        atr_pctile: float = 50.0,
    ) -> float:
        """
        Strategy v1 position sizing (Section 10):
        risk_amount = risk_per_trade_pct% of capital
        qty_base = risk_amount / stop_distance
        final_qty = qty_base * confidence_multiplier (vol-capped)
        """
        if stop_distance <= 0 or capital <= 0:
            return 0.0

        risk_amount = capital * (self.cfg.risk_per_trade_pct / 100.0)
        qty_base = risk_amount / stop_distance
        m = self.confidence_multiplier_with_vol_cap(p_win, atr_pctile)
        if m <= 0.0:
            return 0.0
        return float(qty_base * m)


# ---------------------------------------------------------------------------
# Backward-compatible simple trainer (use eqidv2_meta_train_walkforward.py
# for proper walk-forward training)
# ---------------------------------------------------------------------------
def train_simple_baseline(
    dataset_csv: str,
    out_model_path: str = "eqidv2/models/meta_model.pkl",
    out_features_path: str = "eqidv2/models/meta_features.json",
    label_col: str = "label",
) -> Dict[str, Any]:
    """
    Quick baseline trainer (LogisticRegression on legacy features).
    For proper WF training use eqidv2_meta_train_walkforward.py.
    """
    from sklearn.linear_model import LogisticRegression

    df = pd.read_csv(dataset_csv)

    # Try v1 features first, fall back to legacy
    v1_cols = [c for c in ALL_FEATURES if c in df.columns]
    if len(v1_cols) >= 15:
        x = df[v1_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        feats = v1_cols
    else:
        # Legacy 5-feature fallback
        req = {"quality_score", "atr_pct", "rsi", "adx", "side", label_col}
        miss = req - set(df.columns)
        if miss:
            raise ValueError(f"Dataset missing required columns: {sorted(miss)}")
        x = pd.DataFrame({
            "quality_score": pd.to_numeric(df["quality_score"], errors="coerce").fillna(0.0),
            "atr_pct": pd.to_numeric(df["atr_pct"], errors="coerce").fillna(0.0),
            "rsi_centered": (pd.to_numeric(df["rsi"], errors="coerce").fillna(50.0) - 50.0) / 50.0,
            "adx_norm": pd.to_numeric(df["adx"], errors="coerce").fillna(20.0) / 50.0,
            "side": np.where(df["side"].astype(str).str.upper().eq("LONG"), 1.0, -1.0),
        })
        feats = list(x.columns)

    y = pd.to_numeric(df[label_col], errors="coerce").fillna(0).astype(int)

    clf = LogisticRegression(max_iter=1000, class_weight="balanced")
    clf.fit(x, y)

    Path(out_model_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_model_path).write_bytes(pickle.dumps(clf))
    Path(out_features_path).write_text(json.dumps(feats, indent=2), encoding="utf-8")

    return {
        "rows": int(len(df)),
        "pos_rate": float(y.mean()) if len(y) else 0.0,
        "features": feats,
        "out_model_path": out_model_path,
        "out_features_path": out_features_path,
    }
