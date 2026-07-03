r"""lib_long_disc.py  --  shared engine for the FAST-MOMENTUM LONG (~0.75% symmetric) discovery.

RESEARCH ONLY. No live trades, no final_setup_conf edits.

Design (kept faithful to the existing v11/v7 backtest pipeline, while studying RAW data):
  * SIGNALS come from RAW 5-minute bars (close-stamped: bar @T covers [T-5m, T]).
    All family triggers use ONLY information known at the bar close (no lookahead).
  * ENTRY = next 1-minute OPEN at floor(T)+1min (search up to +3min), exactly like
    avwap_5min_ID_v11_backtesting._first_1m_entry / setup_train_test._entry.
    LONG fill = open * (1 + slippage_bps/1e4), rounded 2dp.  qty = max(1, NOTIONAL/open).
  * EXIT resolved on 1-MINUTE bars from entry to EOD cutoff (15:20 IST), mirroring
    v17D_exit_resolver.resolve: if SL and TARGET are both touched inside the SAME 1-min
    bar -> pessimistic SL-first.  We additionally COUNT those tie-break bars, and add
    optional time-exit / break-even variants on top of the tight bracket.
  * COSTS = the repo statutory NSE intraday model (walkforward_gate.net_pnl_vectorized +
    nse_intraday_costs.CostConfig) PLUS adverse 15 bps/leg slippage on entry AND exit.
  * Indicators (session-VWAP, EMA, ATR, RSI, ADX, MACD-hist) are RECOMPUTED causally here
    so the study does not depend on the known-stale parquet `VWAP` column.

This file only defines functions/constants; the numbered scripts call it.
"""
from __future__ import annotations

import json
import sys
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

# ---- repo wiring (cost parity) ------------------------------------------------
_P = Path(__file__).resolve()
TT_DIR = next(par for par in _P.parents if par.name == "Train_and_Test")
REPO_ROOT = TT_DIR.parent
for _d in (str(REPO_ROOT), str(TT_DIR)):
    if _d not in sys.path:
        sys.path.insert(0, _d)

# Namespaced under claude_engine/ so this pipeline never collides with the parallel
# tight_raw_long_discovery.py run that shares the parent folder.
OUTDIR = TT_DIR / "long_setup_discovery_from_raw_data" / "claude_engine"
RESULTS = OUTDIR / "results"
CAND = OUTDIR / "candidates"
LOGS = OUTDIR / "logs"
for _d in (RESULTS, CAND, LOGS):
    _d.mkdir(parents=True, exist_ok=True)

# Cost config + vectorized statutory cost (exact repo functions)
from nse_intraday_costs import CostConfig            # noqa: E402
import walkforward_gate as wfg                        # noqa: E402

CFG = CostConfig()
SLIPPAGE_BPS = 15.0          # adverse half-spread per leg (entry & exit), repo default
NOTIONAL_RS = 200000.0       # v11 V7_SIGNAL_NOTIONAL_RS (margin*leverage); set from repo below
try:
    import avwap_5min_ID_v11_backtesting as _v11      # noqa: E402
    NOTIONAL_RS = float(_v11.V7_SIGNAL_NOTIONAL_RS)
except Exception:
    pass

# ---- data paths ---------------------------------------------------------------
D5 = Path(r"C:/TradingData/eqidv2/stocks_indicators_5min_eq_live2")   # 5-min backtest source
D1 = Path(r"C:/TradingData/eqidv2/stocks_indicators_1min_eq")          # 1-min exit source
DD = Path(r"C:/TradingData/eqidv2/stocks_indicators_daily_eq")         # daily (STALE -> not used)

EOD_CUT_H, EOD_CUT_M = 15, 20          # forced-exit cutoff (matches v17D_exit_resolver)
IST = "Asia/Kolkata"

# ---- bracket grid (the headline: tight + symmetric around 0.75/0.75) ----------
# (sl_pct, tgt_pct) keyed by short label.  All resolved on 1-min, SL-first tie-break.
BRACKETS = {
    "b_075_075": (0.75, 0.75),   # anchor 1:1
    "b_050_050": (0.50, 0.50),
    "b_060_060": (0.60, 0.60),
    "b_075_100": (0.75, 1.00),   # let winners run a touch
    "b_050_075": (0.50, 0.75),
}
ANCHOR = "b_075_075"
# exit variants layered on the anchor bracket (time-exit N 5-min bars, break-even move)
VARIANTS = {
    "v_t12": dict(time_exit_bars=12),                       # exit if unresolved in 12x5m=60min
    "v_t18": dict(time_exit_bars=18),                       # 90min
    "v_be04": dict(be_after_pct=0.40),                      # move SL->BE after +0.40% favorable
    "v_be04_t18": dict(be_after_pct=0.40, time_exit_bars=18),
}

# ---- EXTENSION grid: wider targets / R-multiples (outside the tight theme) -----
# Tests the hypothesis that the small real price-path edge (~+0.10%/trade) clears the
# fixed transaction cost once the target is large enough. Used by 05/06_*_ext scripts.
BRACKETS_EXT = {
    "x_075_075": (0.75, 0.75),   # tight symmetric anchor (for SHORT-vs-LONG comparison)
    "x_075_100": (0.75, 1.00),   # tight ref (most cost-robust tight bracket)
    "x_075_150": (0.75, 1.50),   # 1:2, tight stop
    "x_100_150": (1.00, 1.50),   # 1:1.5
    "x_100_200": (1.00, 2.00),   # 1:2
    "x_075_200": (0.75, 2.00),   # 1:2.67, tight stop / runner
    "x_125_250": (1.25, 2.50),   # 1:2
    "x_150_150": (1.50, 1.50),   # 1:1 wide
    "x_100_300": (1.00, 3.00),   # 1:3 runner
}
EXT_VARIANTS = {
    "xv_t18": dict(time_exit_bars=18),       # 90-min time stop on a runner
    "xv_be05": dict(be_after_pct=0.50),      # BE after +0.5% on a wide target
}


# ==============================================================================
# Loaders
# ==============================================================================
def _norm_ist(df: pd.DataFrame) -> pd.DataFrame | None:
    if df is None or df.empty or "date" not in df.columns:
        return None
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize("UTC").dt.tz_convert(IST)
    else:
        df["date"] = df["date"].dt.tz_convert(IST)
    df = df.dropna(subset=["date"]).sort_values("date")
    return df if not df.empty else None


@lru_cache(maxsize=4096)
def load_5m_raw(ticker: str) -> pd.DataFrame | None:
    p = D5 / f"{ticker.upper()}_stocks_indicators_5min.parquet"
    if not p.exists():
        return None
    cols = ["date", "open", "high", "low", "close", "volume", "opening_snapshot"]
    try:
        df = pd.read_parquet(p, columns=cols)
    except Exception:
        df = pd.read_parquet(p, columns=["date", "open", "high", "low", "close", "volume"])
        df["opening_snapshot"] = False
    df = _norm_ist(df)
    if df is None:
        return None
    # DQ: drop the 09:15 opening_snapshot duplicate of the first real 09:20 bar
    if "opening_snapshot" in df.columns:
        df = df[~df["opening_snapshot"].fillna(False).astype(bool)]
    df = df.drop_duplicates(subset=["date"], keep="last")
    # tz-naive IST calendar day so it matches pd.Timestamp("YYYY-MM-DD") from sessions.json
    df["sess"] = df["date"].dt.tz_localize(None).dt.normalize()
    return df.reset_index(drop=True)


def _ns(series: pd.Series) -> np.ndarray:
    """tz-aware datetime Series -> int64 ns since epoch (version-robust: pandas 1.3 & 2.2)."""
    naive = series.dt.tz_convert("UTC").dt.tz_localize(None)
    return naive.to_numpy(dtype="datetime64[ns]").astype("int64")


@lru_cache(maxsize=4096)
def load_1m_raw(ticker: str):
    """Return (ts_ns int64[], open[], high[], low[], close[]) sorted, or None."""
    p = D1 / f"{ticker.upper()}_stocks_indicators_1min.parquet"
    if not p.exists():
        return None
    df = pd.read_parquet(p, columns=["date", "open", "high", "low", "close"])
    df = _norm_ist(df)
    if df is None:
        return None
    return (_ns(df["date"]),
            df["open"].to_numpy(float), df["high"].to_numpy(float),
            df["low"].to_numpy(float), df["close"].to_numpy(float))


# ==============================================================================
# Causal 5-min features (computed per session)
# ==============================================================================
def _ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False, min_periods=1).mean()


def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    d = close.diff()
    gain = d.clip(lower=0.0)
    loss = -d.clip(upper=0.0)
    ag = gain.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    al = loss.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    rs = ag / al.replace(0.0, np.nan)
    out = 100.0 - 100.0 / (1.0 + rs)
    out = out.mask((al == 0) & (ag > 0), 100.0).mask((al == 0) & (ag <= 0), 50.0)
    return out


def _atr(h, l, c, period: int = 14) -> pd.Series:
    pc = c.shift(1)
    tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()


def _adx(h, l, c, period: int = 14) -> pd.Series:
    up = h.diff()
    dn = -l.diff()
    plus = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=h.index)
    minus = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=h.index)
    pc = c.shift(1)
    tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean().replace(0.0, np.nan)
    pdi = 100.0 * plus.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean() / atr
    mdi = 100.0 * minus.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean() / atr
    dx = 100.0 * (pdi - mdi).abs() / (pdi + mdi).replace(0.0, np.nan)
    return dx.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """All columns are CAUSAL (known at the bar's close). Cross-session indicators
    (EMA/RSI/ATR/ADX/MACD) run over the full loaded series; intraday ones (VWAP, slot,
    session highs) reset per session."""
    if df is None or df.empty:
        return df
    df = df.copy()
    o, h, l, c, v = df["open"], df["high"], df["low"], df["close"], df["volume"]
    rng = (h - l)
    df["range_pct"] = rng / c * 100.0
    df["body"] = c - o
    df["body_frac"] = (c - o) / rng.replace(0.0, np.nan)
    df["close_loc"] = (c - l) / rng.replace(0.0, np.nan)          # 1 = closed on high
    df["upper_wick"] = (h - np.maximum(o, c)) / rng.replace(0.0, np.nan)
    df["lower_wick"] = (np.minimum(o, c) - l) / rng.replace(0.0, np.nan)
    df["green"] = (c > o).astype(int)

    # cross-session causal indicators
    df["ema9"] = _ema(c, 9)
    df["ema20"] = _ema(c, 20)
    df["ema50"] = _ema(c, 50)
    df["rsi"] = _rsi(c, 14)
    df["atr"] = _atr(h, l, c, 14)
    df["atr_pct"] = df["atr"] / c * 100.0
    df["adx"] = _adx(h, l, c, 14)
    ema12, ema26 = _ema(c, 12), _ema(c, 26)
    macd = ema12 - ema26
    df["macd_hist"] = macd - _ema(macd, 9)
    df["macd_hist_prev"] = df["macd_hist"].shift(1)
    df["rsi_prev"] = df["rsi"].shift(1)
    df["ema20_slope"] = df["ema20"].diff()

    # momentum / pressure (prior bars; shifted where "prior")
    df["mom2_pct"] = (c / c.shift(2) - 1.0) * 100.0
    df["mom3_pct"] = (c / c.shift(3) - 1.0) * 100.0
    df["roc1_pct"] = (c / c.shift(1) - 1.0) * 100.0
    df["green3"] = df["green"].rolling(3).sum()         # # green of last 3 (incl current)
    df["green_prev3"] = df["green"].shift(1).rolling(3).sum()
    df["red_prev2"] = (1 - df["green"]).shift(1).rolling(2).sum()
    df["body_prev"] = df["body"].shift(1)
    df["body_prev2"] = df["body"].shift(2)
    df["body_expand"] = (df["body"] > df["body_prev"]) & (df["body_prev"] > df["body_prev2"])

    # per-session resets: VWAP, slot index, session highs, opening range
    out = []
    for _, g in df.groupby("sess", sort=False):
        g = g.copy()
        tp = (g["high"] + g["low"] + g["close"]) / 3.0
        cv = g["volume"].cumsum().replace(0.0, np.nan)
        g["vwap"] = (tp * g["volume"]).cumsum() / cv
        g["slot"] = np.arange(len(g))                       # 0 = first 5-min bar (~09:20)
        # prior-bar / prior-N session highs & lows (exclude current bar -> shift 1)
        g["prev_high"] = g["high"].shift(1)
        g["prev_close"] = g["close"].shift(1)
        g["hh5"] = g["high"].rolling(5).max().shift(1)
        g["hh10"] = g["high"].rolling(10).max().shift(1)
        g["ll5"] = g["low"].rolling(5).min().shift(1)
        g["ll10"] = g["low"].rolling(10).min().shift(1)
        g["session_high"] = g["high"].cummax().shift(1)     # session high BEFORE this bar
        g["session_low"] = g["low"].cummin().shift(1)
        # opening range = first 3 bars (≈09:20-09:35); known from slot>=3 onward
        orh = g["high"].iloc[:3].max() if len(g) >= 3 else np.nan
        orl = g["low"].iloc[:3].min() if len(g) >= 3 else np.nan
        g["or_high"] = orh
        g["or_low"] = orl
        # consolidation: range of prior 5 bars relative to atr (compression)
        prng = (g["high"].rolling(5).max() - g["low"].rolling(5).min()).shift(1)
        g["compress5_atr"] = prng / g["atr"]
        out.append(g)
    df = pd.concat(out, axis=0).sort_values("date").reset_index(drop=True)

    # volume baseline (within full series rolling is fine; use trailing 20)
    df["vol_ma20"] = df["volume"].rolling(20, min_periods=5).mean()
    df["vol_ratio"] = df["volume"] / df["vol_ma20"]
    df["vol_ratio_prev"] = df["vol_ratio"].shift(1)

    # trend / location helpers
    df["vwap_dist_atr"] = (df["close"] - df["vwap"]) / df["atr"]
    df["ema20_dist_atr"] = (df["close"] - df["ema20"]) / df["atr"]
    df["above_vwap"] = df["close"] > df["vwap"]
    df["above_ema20"] = df["close"] > df["ema20"]
    df["minute"] = df["date"].dt.hour * 60 + df["date"].dt.minute
    return df


# ==============================================================================
# Setup families  (each returns a boolean Series over the feature frame)
#   All conditions use only causal columns. Triggers are intentionally permissive
#   (structure + a light momentum/volume gate); the search tightens them.
# ==============================================================================
def family_triggers(d: pd.DataFrame) -> dict[str, pd.Series]:
    c, o, h = d["close"], d["open"], d["high"]
    base_ok = d["atr_pct"].notna() & (d["atr_pct"] > 0) & d["vwap"].notna() & (d["slot"] >= 1)
    green = d["close"] > d["open"]
    F = {}
    # 1. VWAP Reclaim Momentum: was below VWAP recently, now closes back above it, green.
    F["F1_VWAP_RECLAIM"] = base_ok & green & d["above_vwap"] & (d["prev_close"] <= d["vwap"]) & (d["close_loc"] >= 0.5)
    # 2. Pressure Burst Breakout: big green body, close near high, breaks prior bar high.
    F["F2_PRESSURE_BURST"] = base_ok & green & (d["close"] > d["prev_high"]) & (d["body_frac"] >= 0.55) & (d["close_loc"] >= 0.6)
    # 3. Consolidation Expansion Breakout: prior 5-bar range compressed, now breaks out.
    F["F3_CONSOL_EXPANSION"] = base_ok & green & (d["close"] > d["hh5"]) & (d["compress5_atr"] <= 2.5)
    # 4. Failed Breakdown Reversal: prior bar took out ll5 (broke down) but THIS bar reclaims & closes green above prev_close.
    F["F4_FAILED_BREAKDOWN"] = base_ok & green & (d["low"].shift(1) <= d["ll10"]) & (d["close"] > d["prev_close"]) & (d["close"] > d["open"]) & (d["lower_wick"] >= 0.2)
    # 5. Pullback Continuation: above ema20 & vwap (uptrend), pulled back (prev red), now green continuation.
    F["F5_PULLBACK_CONT"] = base_ok & green & d["above_ema20"] & d["above_vwap"] & (d["red_prev2"] >= 1) & (d["close"] > d["prev_high"])
    # 6. Volume Expansion Breakout: volume spike vs avg + breaks prior 5-bar high.
    F["F6_VOLUME_EXPANSION"] = base_ok & green & (d["vol_ratio"] >= 1.5) & (d["close"] > d["hh5"]) & (d["close_loc"] >= 0.5)
    # 7. EMA/VWAP Trend Continuation: stacked ema9>ema20>ema50, above vwap, green, new session high.
    F["F7_TREND_CONT"] = base_ok & green & (d["ema9"] > d["ema20"]) & (d["ema20"] > d["ema50"]) & d["above_vwap"] & (d["close"] > d["session_high"])
    # 8. Opening Strength Continuation: early session (slot<=6), above vwap, breaks OR high.
    F["F8_OPENING_STRENGTH"] = base_ok & green & (d["slot"] <= 6) & d["above_vwap"] & (d["close"] > d["or_high"])
    # 9. Midday Reclaim Continuation: midday (slot in 12..40), reclaims vwap & breaks prev high.
    F["F9_MIDDAY_RECLAIM"] = base_ok & green & (d["slot"] >= 12) & (d["slot"] <= 42) & d["above_vwap"] & (d["close"] > d["prev_high"]) & (d["prev_close"] <= d["vwap"] * 1.001)
    # 10. Range Expansion After Compression: range_pct expands vs prior, atr not tiny, breaks hh5.
    F["F10_RANGE_EXPANSION"] = base_ok & green & (d["range_pct"] > d["range_pct"].shift(1) * 1.4) & (d["close"] > d["hh5"]) & (d["close_loc"] >= 0.55)
    return {k: v.fillna(False) for k, v in F.items()}


FAMILY_LABELS = {
    "F1_VWAP_RECLAIM": "LONG VWAP Reclaim Momentum",
    "F2_PRESSURE_BURST": "LONG Pressure Burst Breakout",
    "F3_CONSOL_EXPANSION": "LONG Consolidation Expansion Breakout",
    "F4_FAILED_BREAKDOWN": "LONG Failed Breakdown Reversal",
    "F5_PULLBACK_CONT": "LONG Pullback Continuation",
    "F6_VOLUME_EXPANSION": "LONG Volume Expansion Breakout",
    "F7_TREND_CONT": "LONG EMA/VWAP Trend Continuation",
    "F8_OPENING_STRENGTH": "LONG Opening Strength Continuation",
    "F9_MIDDAY_RECLAIM": "LONG Midday Reclaim Continuation",
    "F10_RANGE_EXPANSION": "LONG Range Expansion After Compression",
}

def short_family_triggers(d: pd.DataFrame) -> dict[str, pd.Series]:
    """SHORT mirror of family_triggers (breakdown structure). Causal, no lookahead."""
    c, o = d["close"], d["open"]
    base_ok = d["atr_pct"].notna() & (d["atr_pct"] > 0) & d["vwap"].notna() & (d["slot"] >= 1)
    red = d["close"] < d["open"]
    F = {}
    F["S1_VWAP_LOSE"] = base_ok & red & (~d["above_vwap"]) & (d["prev_close"] >= d["vwap"]) & (d["close_loc"] <= 0.5)
    F["S2_PRESSURE_DUMP"] = base_ok & red & (d["close"] < d["ll5"].where(d["ll5"].notna(), d["low"].shift(1))) & (d["body_frac"] <= -0.55) & (d["close_loc"] <= 0.4)
    F["S3_CONSOL_BREAKDOWN"] = base_ok & red & (d["close"] < d["ll5"]) & (d["compress5_atr"] <= 2.5)
    F["S4_FAILED_BREAKOUT"] = base_ok & red & (d["high"].shift(1) >= d["hh10"]) & (d["close"] < d["prev_close"]) & (d["upper_wick"] >= 0.2)
    F["S5_PULLBACK_CONT"] = base_ok & red & (~d["above_ema20"]) & (~d["above_vwap"]) & (d["green_prev3"].sub(0) >= 1) & (d["close"] < d["ll5"].where(d["ll5"].notna(), d["low"].shift(1)))
    F["S6_VOLUME_DUMP"] = base_ok & red & (d["vol_ratio"] >= 1.5) & (d["close"] < d["ll5"]) & (d["close_loc"] <= 0.5)
    F["S7_TREND_CONT"] = base_ok & red & (d["ema9"] < d["ema20"]) & (d["ema20"] < d["ema50"]) & (~d["above_vwap"]) & (d["close"] < d["session_low"])
    F["S8_OPENING_WEAK"] = base_ok & red & (d["slot"] <= 6) & (~d["above_vwap"]) & (d["close"] < d["or_low"])
    F["S9_MIDDAY_LOSE"] = base_ok & red & (d["slot"] >= 12) & (d["slot"] <= 42) & (~d["above_vwap"]) & (d["close"] < d["low"].shift(1)) & (d["prev_close"] >= d["vwap"] * 0.999)
    F["S10_RANGE_EXP"] = base_ok & red & (d["range_pct"] > d["range_pct"].shift(1) * 1.4) & (d["close"] < d["ll5"]) & (d["close_loc"] <= 0.45)
    return {k: v.fillna(False) for k, v in F.items()}


SHORT_FAMILY_LABELS = {
    "S1_VWAP_LOSE": "SHORT VWAP Loss Momentum", "S2_PRESSURE_DUMP": "SHORT Pressure Dump Breakdown",
    "S3_CONSOL_BREAKDOWN": "SHORT Consolidation Breakdown", "S4_FAILED_BREAKOUT": "SHORT Failed Breakout Reversal",
    "S5_PULLBACK_CONT": "SHORT Pullback Continuation", "S6_VOLUME_DUMP": "SHORT Volume Expansion Breakdown",
    "S7_TREND_CONT": "SHORT EMA/VWAP Trend Continuation", "S8_OPENING_WEAK": "SHORT Opening Weakness Continuation",
    "S9_MIDDAY_LOSE": "SHORT Midday Loss Continuation", "S10_RANGE_EXP": "SHORT Range Expansion After Compression",
}

# feature columns carried into the signal table (for the search masks)
SIGNAL_FEATS = [
    "open", "high", "low", "close", "volume", "atr_pct", "range_pct", "body_frac",
    "close_loc", "upper_wick", "lower_wick", "rsi", "rsi_prev", "adx", "macd_hist",
    "macd_hist_prev", "ema20_slope", "mom2_pct", "mom3_pct", "roc1_pct", "green3",
    "green_prev3", "vol_ratio", "vol_ratio_prev", "vwap_dist_atr", "ema20_dist_atr",
    "compress5_atr", "slot", "minute", "above_vwap", "above_ema20", "vwap",
]


# ==============================================================================
# 1-min exit resolver  (mirrors v17D_exit_resolver.resolve + tie-break count + variants)
# ==============================================================================
def resolve_path(ts_ns, op, hi, lo, cl, e_idx, entry_px, sl_pct, tgt_pct,
                 time_exit_bars=None, be_after_pct=None, side="LONG"):
    """Walk 1-min bars from e_idx to EOD cutoff for LONG or SHORT.
    Returns dict(outcome, exit_px_raw, bars_held, tie, exit_idx).
    SL-first when both touched in the same 1-min bar (pessimistic) -> tie=True there.
    Optional: move SL to break-even after favorable move; force time-exit if unresolved."""
    n = len(cl)
    entry_ts = ts_ns[e_idx]
    day0 = pd.Timestamp(entry_ts, tz=IST).normalize()
    eod_ns = (day0 + pd.Timedelta(hours=EOD_CUT_H, minutes=EOD_CUT_M)).value
    time_cut_ns = None
    if time_exit_bars is not None:
        time_cut_ns = entry_ts + int(time_exit_bars) * 5 * 60 * 1_000_000_000
    is_long = (side == "LONG")
    if is_long:
        sl_price = entry_px * (1.0 - sl_pct / 100.0)
        tgt_price = entry_px * (1.0 + tgt_pct / 100.0)
        be_trigger = entry_px * (1.0 + be_after_pct / 100.0) if be_after_pct is not None else None
    else:
        sl_price = entry_px * (1.0 + sl_pct / 100.0)
        tgt_price = entry_px * (1.0 - tgt_pct / 100.0)
        be_trigger = entry_px * (1.0 - be_after_pct / 100.0) if be_after_pct is not None else None
    be_armed = False

    i = e_idx
    held = 0
    while i < n and ts_ns[i] <= eod_ns:
        held += 1
        bar_hi, bar_lo = hi[i], lo[i]
        if be_trigger is not None and not be_armed:
            if (is_long and bar_hi >= be_trigger) or ((not is_long) and bar_lo <= be_trigger):
                be_armed = True
                sl_price = min(sl_price, entry_px) if not is_long else max(sl_price, entry_px)
        if is_long:
            sl_hit = bar_lo <= sl_price
            tgt_hit = bar_hi >= tgt_price
        else:
            sl_hit = bar_hi >= sl_price
            tgt_hit = bar_lo <= tgt_price
        be_now = be_armed and ((is_long and sl_price >= entry_px) or ((not is_long) and sl_price <= entry_px))
        if sl_hit and tgt_hit:
            return dict(outcome="SL", exit_px_raw=float(sl_price), bars_held=held, tie=True, exit_idx=i)
        if sl_hit:
            return dict(outcome="BE" if be_now else "SL",
                        exit_px_raw=float(sl_price), bars_held=held, tie=False, exit_idx=i)
        if tgt_hit:
            return dict(outcome="TARGET", exit_px_raw=float(tgt_price), bars_held=held, tie=False, exit_idx=i)
        # time-based exit (only if neither bracket hit within window)
        if time_cut_ns is not None and ts_ns[i] >= time_cut_ns:
            return dict(outcome="TIME", exit_px_raw=float(cl[i]), bars_held=held, tie=False, exit_idx=i)
        i += 1
    # EOD: close at last in-window bar
    j = min(i, n - 1)
    # step back to last bar <= eod
    while j > e_idx and ts_ns[j] > eod_ns:
        j -= 1
    return dict(outcome="EOD", exit_px_raw=float(cl[j]), bars_held=max(1, j - e_idx + 1), tie=False, exit_idx=j)


def _entry_index(ts_ns, op, sig_ts_ns, max_delay_min=3):
    """First 1-min bar with ts >= floor(sig)+1min and <= +1+max_delay min. Return (idx, open) or None."""
    minute_ns = 60 * 1_000_000_000
    floor_sig = (sig_ts_ns // minute_ns) * minute_ns
    intended = floor_sig + minute_ns
    latest = intended + max_delay_min * minute_ns
    lo = np.searchsorted(ts_ns, intended, side="left")
    if lo >= len(ts_ns) or ts_ns[lo] > latest:
        return None
    px = op[lo]
    if not np.isfinite(px) or px <= 0:
        return None
    return lo, float(px)


# ==============================================================================
# Metrics  (net of statutory costs + slippage; LONG only)
# ==============================================================================
def net_pnl(fill, exit_px_slipped, qty, side="LONG"):
    return float(wfg.net_pnl_vectorized(np.array([fill]), np.array([exit_px_slipped]),
                                        np.array([qty]), np.array([side]), CFG)[0])


def net_pnl_vec(fill, exit_px_slipped, qty):
    side = np.array(["LONG"] * len(fill))
    return wfg.net_pnl_vectorized(np.asarray(fill, float), np.asarray(exit_px_slipped, float),
                                  np.asarray(qty, float), side, CFG)


def attach_net(df: pd.DataFrame, key: str, slip_bps: float) -> np.ndarray:
    """Recompute net P&L for exit `key` at an arbitrary per-leg slippage (bps), from the
    stored unslipped entry open (e_open) and raw exit price. qty is slippage-independent
    (= int(NOTIONAL/e_open), as in the repo). Lets the search/edge-study judge the SAME
    resolved price-paths under different cost assumptions without re-walking 1-min bars."""
    s = slip_bps / 1e4
    e = df["e_open"].to_numpy(float)
    raw = df["raw_" + key].to_numpy(float)
    qty = df["qty"].to_numpy(float)
    if "side" in df.columns:
        side = df["side"].to_numpy()
        is_long = (side == "LONG")
        fill = np.round(np.where(is_long, e * (1.0 + s), e * (1.0 - s)), 2)
        exit_px = np.where(is_long, raw * (1.0 - s), raw * (1.0 + s))
    else:                                  # legacy LONG-only cache
        side = np.array(["LONG"] * len(e))
        fill = np.round(e * (1.0 + s), 2)
        exit_px = raw * (1.0 - s)
    return wfg.net_pnl_vectorized(fill, exit_px, qty.astype(float), side, CFG)


def exit_slip(px_raw, side="LONG"):
    s = SLIPPAGE_BPS / 1e4
    return px_raw * (1.0 - s) if side == "LONG" else px_raw * (1.0 + s)


def metrics_from_trades(df: pd.DataFrame, n_sessions: int) -> dict:
    """df must have columns: net, outcome, bars_held, _day, ticker, tie. LONG only."""
    n = len(df)
    if n == 0:
        return dict(trades=0, wins=0, losses=0, win_rate=0.0, gross_profit=0.0, gross_loss=0.0,
                    net_pnl=0.0, pf=0.0, avg_win=0.0, avg_loss=0.0, expectancy=0.0, max_dd=0.0,
                    avg_hold_min=0.0, sl_cnt=0, tgt_cnt=0, time_cnt=0, eod_cnt=0, be_cnt=0,
                    tie_cnt=0, tie_pct=0.0, trades_per_day=0.0, day_dom=0.0, sym_dom=0.0,
                    top_trade_share=0.0)
    net = df["net"].to_numpy(float)
    wins = net > 0
    gp = float(net[wins].sum())
    gl = float(-net[~wins].sum())
    pf = (gp / gl) if gl > 1e-9 else (float("inf") if gp > 0 else 0.0)
    eq = np.cumsum(net)
    dd = float((np.maximum.accumulate(eq) - eq).max()) if n else 0.0
    by_day = df.groupby("_day")["net"].sum()
    by_sym = df.groupby("ticker")["net"].sum()
    tot = float(net.sum())
    # dominance = most positive single day/symbol share of total gross profit
    day_dom = float(by_day[by_day > 0].max() / gp) if gp > 1e-9 and (by_day > 0).any() else 0.0
    sym_dom = float(by_sym[by_sym > 0].max() / gp) if gp > 1e-9 and (by_sym > 0).any() else 0.0
    top_trade = float(net[wins].max() / gp) if gp > 1e-9 and wins.any() else 0.0
    oc = df["outcome"].value_counts().to_dict()
    return dict(
        trades=int(n), wins=int(wins.sum()), losses=int((~wins).sum()),
        win_rate=round(100.0 * wins.mean(), 2),
        gross_profit=round(gp, 0), gross_loss=round(gl, 0), net_pnl=round(tot, 0),
        pf=round(pf, 3) if np.isfinite(pf) else 999.0,
        avg_win=round(float(net[wins].mean()) if wins.any() else 0.0, 1),
        avg_loss=round(float(net[~wins].mean()) if (~wins).any() else 0.0, 1),
        expectancy=round(tot / n, 1), max_dd=round(dd, 0),
        avg_hold_min=round(float(df["bars_held"].mean()), 1),
        sl_cnt=int(oc.get("SL", 0)), tgt_cnt=int(oc.get("TARGET", 0)),
        time_cnt=int(oc.get("TIME", 0)), eod_cnt=int(oc.get("EOD", 0)), be_cnt=int(oc.get("BE", 0)),
        tie_cnt=int(df["tie"].sum()), tie_pct=round(100.0 * df["tie"].mean(), 2),
        trades_per_day=round(n / max(1, n_sessions), 2),
        day_dom=round(day_dom, 3), sym_dom=round(sym_dom, 3), top_trade_share=round(top_trade, 3),
    )


def select_book(sig: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Apply a candidate's family/mask/guards and produce the realistic, DEPLOYABLE book:
    one position per symbol at a time (no overlap), per-day-per-symbol cap, top_n per
    (day,slot), and a global concurrent-position cap. cfg keys:
      family (str|None), bracket (key), mask [(feat,op,thr)], min_minute, max_minute,
      top_n, rank_feat, max_per_sym_day, max_book_concurrent."""
    import heapq
    d = sig
    if cfg.get("side") and "side" in d.columns:        # ext: restrict to one side
        d = d[d["side"] == cfg["side"]]
    fam = cfg.get("family")
    if fam and not fam.startswith("ALL"):              # ALL / ALL_LONG / ALL_SHORT = no family filter
        d = d[d["f_" + fam]]
    for feat, op, thr in cfg.get("mask", []):
        x = pd.to_numeric(d[feat], errors="coerce")
        if op == ">=":
            d = d[x >= thr]
        elif op == "<=":
            d = d[x <= thr]
        elif op == "==":
            d = d[x == thr]
        elif op == "!=":
            d = d[x != thr]
    mn, mx = cfg.get("min_minute"), cfg.get("max_minute")
    if mn is not None:
        d = d[d["minute"] >= mn]
    if mx is not None:
        d = d[d["minute"] <= mx]
    if d.empty:
        return d
    key = cfg["bracket"]
    d = d.copy()
    entry_ns = _ns(d["signal_ts"].dt.tz_localize(IST) if d["signal_ts"].dt.tz is None else d["signal_ts"]) \
        + 60 * 1_000_000_000
    d["_entry_ns"] = entry_ns
    d["_exit_ns"] = entry_ns + (d["held_" + key].to_numpy().astype("int64") * 60 * 1_000_000_000)
    tn = cfg.get("top_n")
    if tn:
        rf = cfg.get("rank_feat", "atr_pct")
        d = d.sort_values(rf, ascending=False).groupby(["_day", "slot"], sort=False).head(int(tn))
    d = d.sort_values("_entry_ns")
    idx = d.index.to_numpy()
    tks = d["ticker"].to_numpy()
    days = d["_day"].to_numpy()
    e_ns = d["_entry_ns"].to_numpy()
    x_ns = d["_exit_ns"].to_numpy()
    cap_sym = cfg.get("max_per_sym_day")
    keep, last_exit, cnt = [], {}, {}
    for i in range(len(idx)):
        tk = tks[i]
        le = last_exit.get(tk)
        if le is not None and e_ns[i] < le:              # no concurrent same-symbol position
            continue
        if cap_sym:
            kc = (tk, days[i])
            if cnt.get(kc, 0) >= cap_sym:
                continue
            cnt[kc] = cnt.get(kc, 0) + 1
        last_exit[tk] = x_ns[i]
        keep.append(idx[i])
    d = d.loc[keep]
    cap = cfg.get("max_book_concurrent")
    if cap and len(d):
        d = d.sort_values("_entry_ns")
        idx = d.index.to_numpy(); e_ns = d["_entry_ns"].to_numpy(); x_ns = d["_exit_ns"].to_numpy()
        open_ex, keep2 = [], []
        for i in range(len(idx)):
            while open_ex and open_ex[0] <= e_ns[i]:
                heapq.heappop(open_ex)
            if len(open_ex) >= cap:
                continue
            heapq.heappush(open_ex, x_ns[i])
            keep2.append(idx[i])
        d = d.loc[keep2]
    return d


def evaluate(sig: pd.DataFrame, cfg: dict, n_sessions: int) -> dict:
    """select_book + net P&L (at cfg['slip_bps']) + metrics."""
    book = select_book(sig, cfg)
    key = cfg["bracket"]
    slip = cfg.get("slip_bps", 5.0)
    if book.empty:
        return metrics_from_trades(book.assign(net=[], outcome=[], bars_held=[], tie=[]) if False
                                   else pd.DataFrame(columns=["net", "outcome", "bars_held", "_day", "ticker", "tie"]),
                                   n_sessions)
    if cfg.get("cost_mode") == "gross":          # absolute zero-cost price-path ceiling
        diff = (book["raw_" + key].to_numpy(float) - book["e_open"].to_numpy(float)) * book["qty"].to_numpy(float)
        if "side" in book.columns:
            net = np.where(book["side"].to_numpy() == "LONG", diff, -diff)
        else:
            net = diff
    else:
        net = attach_net(book, key, slip)
    t = pd.DataFrame({"net": net, "outcome": book["out_" + key].to_numpy(),
                      "bars_held": book["held_" + key].to_numpy(), "_day": book["_day"].to_numpy(),
                      "ticker": book["ticker"].to_numpy(), "tie": book["tie_" + key].to_numpy()})
    return metrics_from_trades(t, n_sessions)


def save_json(path: Path, obj):
    path.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")


def load_universe():
    return json.loads((RESULTS / "universe.json").read_text())


def load_sessions():
    return json.loads((RESULTS / "sessions.json").read_text())
