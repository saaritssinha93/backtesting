"""Deterministic staged rescue sweeps for A_PULLBACK_C2_THEN_BREAK_C2_LOW.

Research-only. This script writes artifacts only inside
Train_and_Test/setup_pf_1_4_full_loop/A_PULLBACK_C2_THEN_BREAK_C2_LOW/.

It reuses a_pullback_c2_low_full_loop.py for the entry, guard, premom, dedupe,
mask, portfolio overlay, costs, and exit resolver. The only addition is an
approval-loop-safe feature enrichment step from the native 5-minute indicator
parquet store under C:/TradingData/eqidv2/stocks_indicators_5min_eq_live2.
"""
from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import sys
import time
import warnings
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
SCRIPT_DIR = HERE.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import a_pullback_c2_low_full_loop as loop  # noqa: E402


warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.*",
)

SETUP = loop.SETUP
WORK = loop.WORK
POOLS = WORK / "pools"
FEATURE_CACHE = POOLS / "feature5m_signal_cache.csv"
RESULTS_CSV = WORK / "staged_rescue_results.csv"
SUMMARY_MD = WORK / "staged_rescue_summary.md"
RUN_SUMMARY = WORK / "run_summary_staged.json"

QGRID = [0.01, 0.025, 0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 0.95, 0.975, 0.99]
BASE_EXITS = [(1.20, 1.50), (1.00, 1.25), (1.20, 2.00), (0.85, 1.25), (1.50, 1.50), (0.70, 1.25)]
PM_EXITS = [(1.20, 0.80), (1.20, 1.00), (1.20, 1.25), (1.20, 1.50), (1.20, 2.00), (1.20, 2.50)]
ALL_EXITS = [(sl, tgt) for sl in loop.SL_GRID for tgt in loop.TGT_GRID]
STAGE0_EXITS = list(dict.fromkeys([
    (1.20, 1.50),
    (1.00, 1.25),
    (1.20, 2.00),
    (0.85, 1.25),
    (1.50, 1.50),
    (0.70, 1.25),
    (1.20, 0.80),
    (1.20, 2.50),
]))

CORE_GUARDS: list[dict[str, Any] | None] = [
    None,
    {"max_slot": "11:30"},
    {"max_slot": "12:00"},
    {"max_slot": "12:30"},
    {"max_slot": "13:00"},
    {"max_slot": "14:30"},
    {"top_n": 1},
    {"top_n": 2},
    {"max_slot": "11:30", "top_n": 1},
    {"max_slot": "12:30", "top_n": 1},
    {"max_slot": "14:30", "top_n": 2},
    {"min_slot": "10:00", "max_slot": "14:30"},
    {"min_slot": "10:30", "max_slot": "14:30"},
]

NARROW_GUARDS: list[dict[str, Any] | None] = [
    None,
    {"max_slot": "11:30", "top_n": 1},
    {"max_slot": "12:30", "top_n": 1},
    {"top_n": 1},
    {"max_slot": "14:30", "top_n": 2},
]

STAGE0_GUARDS: list[dict[str, Any] | None] = [
    None,
    {"max_slot": "11:30", "top_n": 1},
    {"max_slot": "12:30", "top_n": 1},
    {"top_n": 1},
    {"max_slot": "14:30", "top_n": 2},
]

RAW_5M_COLS = [
    "date", "open", "high", "low", "close", "volume",
    "RSI", "ATR", "EMA_20", "EMA_50", "EMA_200", "20_SMA", "VWAP",
    "CCI", "MFI", "OBV", "MACD", "MACD_Signal", "MACD_Hist",
    "Upper_Band", "Lower_Band", "ADX", "Recent_High", "Recent_Low",
    "Intra_Change", "Prev_Day_Close", "Daily_Change", "gap_filled",
    "Stoch_%K", "Stoch_%D", "opening_snapshot",
]

FEATURE_COLS = [
    "feat5_rsi", "feat5_rsi3max", "feat5_adx", "feat5_atr_pct",
    "feat5_ema20_dist_pct", "feat5_ema50_dist_pct", "feat5_ema20_slope_3",
    "feat5_ema20_vs_ema50_pct", "feat5_macd", "feat5_macd_signal",
    "feat5_macd_hist", "feat5_macd_hist_delta", "feat5_cci", "feat5_mfi",
    "feat5_stoch_k", "feat5_stoch_d", "feat5_bb_pos", "feat5_bb_width_pct",
    "feat5_stock_ret_5m_pct", "feat5_stock_ret_15m_pct", "feat5_stock_ret_30m_pct",
    "feat5_volume_ratio_20", "feat5_range_pct", "feat5_body_efficiency",
    "feat5_close_location", "feat5_upper_wick_pct", "feat5_lower_wick_pct",
    "feat5_vwap_dist_pct", "feat5_recent_low_dist_pct", "feat5_recent_high_dist_pct",
    "feat5_opening_snapshot",
]

CANONICAL_FILL = {
    "rsi": "feat5_rsi",
    "rsi3max": "feat5_rsi3max",
    "adx": "feat5_adx",
    "macd_hist": "feat5_macd_hist",
    "macd_hist_delta": "feat5_macd_hist_delta",
    "ema20_slope": "feat5_ema20_slope_3",
    "stock_ret": "feat5_stock_ret_30m_pct",
    "lower_wick_price_pct": "feat5_lower_wick_pct",
}

MASK_FEATURES = list(dict.fromkeys([
    *loop.MASK_FEATS,
    "rsi", "rsi3max", "adx", "macd_hist", "macd_hist_delta", "ema20_slope",
    "stock_ret", "lower_wick_price_pct", "source_quality_score",
    "day_value_so_far_rs", "score", "pnl", "notional_exposure_rs",
    "v7_signal_notional_rs", "signal_volume", "v8_entry_delay_minutes",
    *FEATURE_COLS,
]))

PM_FEATURES = list(loop.PM_FEATS)

MANUAL_THRESHOLDS: dict[str, list[float]] = {
    "quality_score": [90.0, 105.0, 123.7606, 140.0, 170.0, 220.0],
    "rsi": [25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0],
    "feat5_rsi": [25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0],
    "adx": [15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 50.0],
    "feat5_adx": [15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 50.0],
    "sig5_adx_calc": [15.0, 20.0, 21.4683, 25.0, 30.0, 40.0, 50.0],
    "feat5_mfi": [20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0],
    "feat5_stoch_k": [20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0],
    "close_loc": [0.15, 0.20, 0.25, 0.30, 0.35, 0.40],
    "feat5_close_location": [0.15, 0.20, 0.25, 0.30, 0.35, 0.40],
    "vol_ratio": [1.4, 1.75, 2.0, 2.5, 3.0, 4.0],
    "feat5_volume_ratio_20": [1.2, 1.5, 2.0, 2.5, 3.0, 4.0],
}


def clean(obj: Any) -> Any:
    return loop.clean_float(obj)


def as_json(obj: Any) -> str:
    return json.dumps(clean(obj), indent=2, default=str)


def to_ist_naive(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, errors="coerce")
    try:
        if getattr(ts.dt, "tz", None) is None:
            ts = ts.dt.tz_localize("Asia/Kolkata")
        else:
            ts = ts.dt.tz_convert("Asia/Kolkata")
    except Exception:
        ts = pd.to_datetime(series, errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata")
    return ts.dt.tz_localize(None).dt.floor("min")


def numeric(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce")
    return pd.Series(np.nan, index=df.index, dtype=float)


def safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
    den = den.replace(0.0, np.nan)
    return num / den


def feature_roots() -> list[Path]:
    roots = [
        Path(os.getenv("EQIDV2_V7_HIST_INDICATORS_5M_DIR", r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")),
        Path(os.getenv("EQIDV2_V7_LIVE_INDICATORS_5M_DIR", r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live")),
    ]
    return list(dict.fromkeys(roots))


def read_5m_parquet(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path, columns=RAW_5M_COLS)
    except Exception:
        try:
            df = pd.read_parquet(path)
        except Exception:
            return None
    if "date" not in df.columns:
        return None
    for col in RAW_5M_COLS:
        if col not in df.columns:
            df[col] = np.nan
    return df[RAW_5M_COLS].copy()


def load_5m_features_for_ticker(ticker: str) -> pd.DataFrame:
    name = f"{str(ticker).upper()}_stocks_indicators_5min.parquet"
    frames = []
    for root in feature_roots():
        df = read_5m_parquet(root / name)
        if df is None or df.empty:
            continue
        norm = loop.tt.v11._normalise_bars_date_index(df, naive_tz="UTC")
        if norm is not None and not norm.empty:
            frames.append(norm.reset_index())
            break
    if not frames:
        cols = ["_sig_join", "feature5m_bar_time", *FEATURE_COLS]
        return pd.DataFrame(columns=cols)

    bars = pd.concat(frames, ignore_index=True, sort=False)
    bars = bars.dropna(subset=["date"]).sort_values("date").drop_duplicates("date", keep="last")
    bars = bars.reset_index(drop=True)
    bars["_sig_join"] = to_ist_naive(bars["date"])
    bars["feature5m_bar_time"] = bars["_sig_join"]
    day = bars["_sig_join"].dt.normalize()

    open_px = numeric(bars, "open")
    high = numeric(bars, "high")
    low = numeric(bars, "low")
    close = numeric(bars, "close")
    volume = numeric(bars, "volume")
    rng = (high - low).replace(0.0, np.nan)
    ema20 = numeric(bars, "EMA_20")
    ema50 = numeric(bars, "EMA_50")
    macd = numeric(bars, "MACD")
    macd_signal = numeric(bars, "MACD_Signal")
    macd_hist = numeric(bars, "MACD_Hist")
    upper_band = numeric(bars, "Upper_Band")
    lower_band = numeric(bars, "Lower_Band")
    recent_high = numeric(bars, "Recent_High")
    recent_low = numeric(bars, "Recent_Low")
    vwap = numeric(bars, "VWAP")
    rsi = numeric(bars, "RSI")
    adx = numeric(bars, "ADX")
    atr = numeric(bars, "ATR")

    g = bars.groupby(day, sort=False)
    close_1 = close.groupby(day, sort=False).shift(1)
    close_3 = close.groupby(day, sort=False).shift(3)
    close_6 = close.groupby(day, sort=False).shift(6)
    ema20_3 = ema20.groupby(day, sort=False).shift(3)
    macd_hist_1 = macd_hist.groupby(day, sort=False).shift(1)
    vol_avg20 = volume.groupby(day, sort=False).transform(lambda s: s.shift(1).rolling(20, min_periods=5).mean())
    rsi3max = rsi.groupby(day, sort=False).transform(lambda s: s.rolling(3, min_periods=1).max())

    out = pd.DataFrame({
        "_sig_join": bars["_sig_join"],
        "feature5m_bar_time": bars["feature5m_bar_time"],
        "feat5_rsi": rsi,
        "feat5_rsi3max": rsi3max,
        "feat5_adx": adx,
        "feat5_atr_pct": safe_div(atr, close) * 100.0,
        "feat5_ema20_dist_pct": safe_div(close - ema20, close) * 100.0,
        "feat5_ema50_dist_pct": safe_div(close - ema50, close) * 100.0,
        "feat5_ema20_slope_3": safe_div(ema20 - ema20_3, close) * 100.0,
        "feat5_ema20_vs_ema50_pct": safe_div(ema20 - ema50, close) * 100.0,
        "feat5_macd": macd,
        "feat5_macd_signal": macd_signal,
        "feat5_macd_hist": macd_hist,
        "feat5_macd_hist_delta": macd_hist - macd_hist_1,
        "feat5_cci": numeric(bars, "CCI"),
        "feat5_mfi": numeric(bars, "MFI"),
        "feat5_stoch_k": numeric(bars, "Stoch_%K"),
        "feat5_stoch_d": numeric(bars, "Stoch_%D"),
        "feat5_bb_pos": safe_div(close - lower_band, upper_band - lower_band),
        "feat5_bb_width_pct": safe_div(upper_band - lower_band, close) * 100.0,
        "feat5_stock_ret_5m_pct": (safe_div(close, close_1) - 1.0) * 100.0,
        "feat5_stock_ret_15m_pct": (safe_div(close, close_3) - 1.0) * 100.0,
        "feat5_stock_ret_30m_pct": (safe_div(close, close_6) - 1.0) * 100.0,
        "feat5_volume_ratio_20": safe_div(volume, vol_avg20),
        "feat5_range_pct": safe_div(high - low, close) * 100.0,
        "feat5_body_efficiency": safe_div(close - open_px, rng),
        "feat5_close_location": safe_div(close - low, rng),
        "feat5_upper_wick_pct": safe_div(high - pd.concat([open_px, close], axis=1).max(axis=1), rng),
        "feat5_lower_wick_pct": safe_div(pd.concat([open_px, close], axis=1).min(axis=1) - low, rng),
        "feat5_vwap_dist_pct": safe_div(close - vwap, close) * 100.0,
        "feat5_recent_low_dist_pct": safe_div(close - recent_low, close) * 100.0,
        "feat5_recent_high_dist_pct": safe_div(recent_high - close, close) * 100.0,
        "feat5_opening_snapshot": numeric(bars, "opening_snapshot"),
    })
    return out.replace([np.inf, -np.inf], np.nan)


def parse_day_value(text: Any) -> float:
    if not isinstance(text, str) or not text.strip():
        return float("nan")
    try:
        obj = json.loads(text)
    except Exception:
        return float("nan")
    for key in ("day_value_so_far_rs", "day_value", "day_value_rs"):
        val = obj.get(key)
        try:
            out = float(val)
            return out if np.isfinite(out) else float("nan")
        except Exception:
            continue
    return float("nan")


def build_feature_cache(keys: pd.DataFrame) -> pd.DataFrame:
    POOLS.mkdir(parents=True, exist_ok=True)
    keys = keys.dropna(subset=["ticker", "_sig_join"]).drop_duplicates(["ticker", "_sig_join"]).copy()
    keys["ticker"] = keys["ticker"].astype(str).str.upper().str.strip()
    cache_cols = ["ticker", "_sig_join", "feature5m_status", "feature5m_bar_time", *FEATURE_COLS]
    if FEATURE_CACHE.exists():
        try:
            cache = pd.read_csv(FEATURE_CACHE, low_memory=False)
            cache["_sig_join"] = pd.to_datetime(cache["_sig_join"], errors="coerce")
            cache["feature5m_bar_time"] = pd.to_datetime(cache.get("feature5m_bar_time"), errors="coerce")
        except Exception:
            cache = pd.DataFrame(columns=cache_cols)
    else:
        cache = pd.DataFrame(columns=cache_cols)
    for col in cache_cols:
        if col not in cache.columns:
            cache[col] = np.nan
    cache = cache[cache_cols].dropna(subset=["ticker", "_sig_join"], how="any")

    have = set(zip(cache["ticker"].astype(str), cache["_sig_join"].astype("datetime64[ns]").astype(str)))
    miss = keys[~pd.Series(zip(keys["ticker"].astype(str), keys["_sig_join"].astype("datetime64[ns]").astype(str))).isin(have).to_numpy()].copy()
    if miss.empty:
        return cache

    rows = []
    print(f"[feature5m] building cache for {len(miss)} missing signal rows across {miss['ticker'].nunique()} tickers", flush=True)
    t0 = time.time()
    for i, (ticker, want) in enumerate(miss.groupby("ticker", sort=True), 1):
        feats = load_5m_features_for_ticker(ticker)
        want = want[["ticker", "_sig_join"]].sort_values("_sig_join").copy()
        if feats.empty:
            joined = want.copy()
            for col in ["feature5m_bar_time", *FEATURE_COLS]:
                joined[col] = np.nan
            joined["feature5m_status"] = "missing_file"
        else:
            joined = pd.merge_asof(
                want.sort_values("_sig_join"),
                feats.sort_values("_sig_join"),
                on="_sig_join",
                direction="backward",
                tolerance=pd.Timedelta(minutes=5),
            )
            joined["feature5m_status"] = np.where(joined["feature5m_bar_time"].notna(), "ok", "missing_bar")
        rows.append(joined[cache_cols])
        if i % 100 == 0:
            add = pd.concat(rows, ignore_index=True, sort=False) if rows else pd.DataFrame(columns=cache_cols)
            cache = pd.concat([cache, add], ignore_index=True, sort=False)
            cache = cache.drop_duplicates(["ticker", "_sig_join"], keep="last").sort_values(["ticker", "_sig_join"])
            cache.to_csv(FEATURE_CACHE, index=False)
            rows = []
            print(f"[feature5m] {i}/{miss['ticker'].nunique()} tickers elapsed={time.time() - t0:.0f}s", flush=True)

    add = pd.concat(rows, ignore_index=True, sort=False) if rows else pd.DataFrame(columns=cache_cols)
    cache = pd.concat([cache, add], ignore_index=True, sort=False)
    cache = cache.drop_duplicates(["ticker", "_sig_join"], keep="last").sort_values(["ticker", "_sig_join"])
    cache.to_csv(FEATURE_CACHE, index=False)
    print(f"[feature5m] wrote {FEATURE_CACHE} rows={len(cache)}", flush=True)
    return cache


def add_feature_columns(df: pd.DataFrame, cache: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["_sig_join"] = to_ist_naive(out["tt_sig_ts"] if "tt_sig_ts" in out.columns else out["signal_time_ist"])
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out = out.merge(cache, on=["ticker", "_sig_join"], how="left", suffixes=("", "_feature5m"))
    for src in FEATURE_COLS:
        out[src] = pd.to_numeric(out[src], errors="coerce") if src in out.columns else np.nan
    for dest, src in CANONICAL_FILL.items():
        vals = pd.to_numeric(out[src], errors="coerce") if src in out.columns else pd.Series(np.nan, index=out.index)
        if dest in out.columns:
            cur = pd.to_numeric(out[dest], errors="coerce")
            out[dest] = cur.where(cur.notna(), vals)
        else:
            out[dest] = vals
    if "diagnostics_json" in out.columns:
        out["day_value_so_far_rs"] = out["diagnostics_json"].map(parse_day_value)
    return out


def enrich_frames(frames: dict[str, pd.DataFrame]) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    all_keys = []
    for label, df in frames.items():
        if df.empty:
            continue
        k = pd.DataFrame({
            "ticker": df["ticker"].astype(str).str.upper().str.strip(),
            "_sig_join": to_ist_naive(df["tt_sig_ts"] if "tt_sig_ts" in df.columns else df["signal_time_ist"]),
        })
        all_keys.append(k)
    keys = pd.concat(all_keys, ignore_index=True, sort=False) if all_keys else pd.DataFrame(columns=["ticker", "_sig_join"])
    cache = build_feature_cache(keys)
    out = {label: add_feature_columns(df, cache) for label, df in frames.items()}

    coverage: dict[str, Any] = {}
    for col in [*CANONICAL_FILL.keys(), "day_value_so_far_rs", *FEATURE_COLS]:
        coverage[col] = {label: int(pd.to_numeric(df[col], errors="coerce").notna().sum()) if col in df.columns else 0 for label, df in out.items()}
    report = {
        "cache_path": str(FEATURE_CACHE),
        "cache_rows": int(len(cache)),
        "cache_status_counts": cache["feature5m_status"].value_counts(dropna=False).to_dict() if "feature5m_status" in cache.columns else {},
        "coverage": coverage,
    }
    return out, report


def metric_short(m: dict[str, Any] | None) -> str:
    if not m:
        return "None/None/RsNone"
    return f"{m.get('n')}/{m.get('net_pf')}/Rs{m.get('net_pnl')}"


def cfg_key(cfg: dict[str, Any]) -> str:
    payload = {
        "sl": float(cfg["sl"]),
        "tgt": float(cfg["tgt"]),
        "mask_terms": [list(x) for x in cfg.get("mask_terms", []) or []],
        "premom_terms": [list(x) for x in cfg.get("premom_terms", []) or []],
        "guard": cfg.get("guard") or {},
        "max_positions": int(cfg.get("max_positions") or 20),
        "daily_loss_rs": float(cfg.get("daily_loss_rs") or 0.0),
    }
    return json.dumps(payload, sort_keys=True, default=str)


def make_cfg(
    sl: float,
    tgt: float,
    mask_terms: list[tuple[str, str, float]] | None = None,
    premom_terms: list[tuple[str, str, float]] | None = None,
    guard: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "sl": float(sl),
        "tgt": float(tgt),
        "mask_terms": list(mask_terms or []),
        "premom_terms": list(premom_terms or []),
        "guard": dict(guard) if guard else None,
        "status": "OK",
        "max_positions": 20,
        "daily_loss_rs": 0.0,
    }


def band_reward(pf: float) -> float:
    if not math.isfinite(float(pf)):
        return -2.0
    pf = float(pf)
    if pf <= loop.PF_HI:
        return pf
    return loop.PF_HI - 1.6 * (pf - loop.PF_HI)


def fitval_score(fit_m: dict[str, Any], val_m: dict[str, Any], min_split_trades: int) -> float:
    nf = int(fit_m.get("n") or 0)
    nv = int(val_m.get("n") or 0)
    if nf < min_split_trades or nv < max(4, min_split_trades // 2):
        return -5.0 + min(nf, nv) / max(1, min_split_trades)
    fit_pf = float(fit_m.get("net_pf") or 0.0)
    val_pf = float(val_m.get("net_pf") or 0.0)
    score = min(band_reward(fit_pf), band_reward(val_pf))
    score -= 0.55 * abs(fit_pf - val_pf)
    score += max(-0.35, min(0.35, (float(fit_m.get("net_pnl") or 0) + float(val_m.get("net_pnl") or 0)) / 25000.0))
    if float(fit_m.get("net_pnl") or 0) <= 0:
        score -= 0.45
    if float(val_m.get("net_pnl") or 0) <= 0:
        score -= 0.75
    return float(score)


def should_confirm_train(fit_m: dict[str, Any], val_m: dict[str, Any], min_split_trades: int) -> bool:
    nf = int(fit_m.get("n") or 0)
    nv = int(val_m.get("n") or 0)
    if nf < min_split_trades or nv < max(4, min_split_trades // 2):
        return False
    fit_pf = float(fit_m.get("net_pf") or 0.0)
    val_pf = float(val_m.get("net_pf") or 0.0)
    fit_net = float(fit_m.get("net_pnl") or 0.0)
    val_net = float(val_m.get("net_pnl") or 0.0)
    if fit_net < -18000 or val_net < -18000:
        return False
    if min(fit_pf, val_pf) >= 0.55 and max(fit_pf, val_pf) >= 0.90:
        return True
    if fit_net + val_net > 0 and min(fit_pf, val_pf) >= 0.45:
        return True
    return False


def pass_candidate_with_notes(cfg: dict[str, Any], train_m: dict[str, Any], test_m: dict[str, Any]) -> tuple[bool, list[str]]:
    passed, reasons = loop.pass_candidate(train_m, test_m)
    if cfg.get("premom_terms") and abs(float(cfg["sl"]) - loop.PM_CACHE_SL) > 1e-9:
        passed = False
        reasons.append(f"pre-momentum cache uses SL {loop.PM_CACHE_SL}; candidate SL {cfg['sl']} is not approval-safe")
    feature_terms = [t for t in (cfg.get("mask_terms") or []) if str(t[0]).startswith("feat5_")]
    if feature_terms:
        reasons.append("approval requires live candidate rows to expose feat5_* 5-minute feature columns")
    return passed, reasons


def classify_cfg(cfg: dict[str, Any]) -> str:
    groups = []
    feats = {str(t[0]).lower() for t in (cfg.get("mask_terms") or [])}
    pm = {str(t[0]).lower() for t in (cfg.get("premom_terms") or [])}
    if float(cfg["sl"]) != 1.2 or float(cfg["tgt"]) != 1.5:
        groups.append("exit")
    if feats:
        if any(x in feats for x in ["rsi", "adx", "macd_hist", "macd_hist_delta", "ema20_slope"]) or any(x.startswith("feat5_") for x in feats):
            groups.append("indicator")
        if feats & {"body_pct", "close_loc", "signal_range_pct", "upper_wick_pct", "lower_wick_pct", "wick_skew_pct", "feat5_body_efficiency", "feat5_close_location"}:
            groups.append("price_action")
        if feats & {"quality_score", "ranker_score", "rs_pct", "market_ret_pct", "market_abs_ret_pct", "day_value_so_far_rs"}:
            groups.append("filter")
    if pm:
        groups.append("pre_momentum")
    if cfg.get("guard"):
        groups.append("guard")
    return "+".join(dict.fromkeys(groups)) or "raw"


def build_terms(df: pd.DataFrame, features: list[str], min_nonnull: int, manual: dict[str, list[float]]) -> list[tuple[str, str, float]]:
    terms: list[tuple[str, str, float]] = []
    for feat in features:
        if feat not in df.columns:
            continue
        s = pd.to_numeric(df[feat], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(s) < min_nonnull or s.nunique() <= 1:
            continue
        vals = [float(s.quantile(q)) for q in QGRID]
        vals.extend(float(x) for x in manual.get(feat, []))
        clean_vals = []
        for val in vals:
            if np.isfinite(val):
                clean_vals.append(round(float(val), 6))
        for thr in sorted(set(clean_vals)):
            terms.append((feat, ">=", thr))
            terms.append((feat, "<=", thr))
    return terms


def stable_unique_terms(items: list[tuple[float, tuple[str, str, float]]], limit: int) -> list[tuple[str, str, float]]:
    out = []
    seen = set()
    for _score, term in sorted(items, key=lambda x: x[0], reverse=True):
        key = tuple(term)
        if key in seen:
            continue
        seen.add(key)
        out.append(term)
        if len(out) >= limit:
            break
    return out


def exit_token(sl: float, tgt: float) -> str:
    return f"{float(sl):.2f}_{float(tgt):.2f}".replace(".", "p")


class FastEvaluator:
    def __init__(self, frames: dict[str, pd.DataFrame]) -> None:
        self.frames: dict[str, pd.DataFrame] = {}
        for label, df in frames.items():
            d = df.copy().reset_index(drop=True)
            d["_row_id_fast"] = np.arange(len(d), dtype=int)
            self.frames[label] = d
        self._built: set[tuple[str, float, float]] = set()
        self._prebook_cache: dict[str, pd.DataFrame] = {}

    def ensure_exit(self, label: str, sl: float, tgt: float) -> None:
        key = (label, float(sl), float(tgt))
        if key in self._built:
            return
        df = self.frames[label]
        tok = exit_token(sl, tgt)
        net_col = f"_fast_net_{tok}"
        exit_col = f"_fast_exit_iso_{tok}"
        outcome_col = f"_fast_outcome_{tok}"
        exit_px_col = f"_fast_exit_px_{tok}"
        print(f"[fast-exit] {label} SL/Tgt={sl}/{tgt} rows={len(df)}", flush=True)
        t0 = time.time()
        nets: list[float] = []
        exits: list[Any] = []
        outcomes: list[Any] = []
        exit_pxs: list[float] = []
        for r in df.itertuples():
            full = loop.tt._resolve_full(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), int(r.tt_qty), float(sl), float(tgt))
            if full is None:
                nets.append(float("nan"))
                exits.append(pd.NaT)
                outcomes.append("")
                exit_pxs.append(float("nan"))
                continue
            exit_iso, outcome, exit_px = full
            net = loop.tt._trade_net(r.side, float(r.tt_fill), int(r.tt_qty), str(outcome), float(exit_px))
            nets.append(float(net))
            exits.append(exit_iso)
            outcomes.append(str(outcome))
            exit_pxs.append(float(exit_px))
        df[net_col] = nets
        df[exit_col] = exits
        df[outcome_col] = outcomes
        df[exit_px_col] = exit_pxs
        self._built.add(key)
        print(f"[fast-exit] {label} SL/Tgt={sl}/{tgt} done elapsed={time.time() - t0:.0f}s", flush=True)

    def prebook_key(self, label: str, cfg: dict[str, Any]) -> str:
        payload = {
            "label": label,
            "guard": cfg.get("guard") or {},
            "premom_terms": [list(t) for t in (cfg.get("premom_terms") or [])],
        }
        return json.dumps(payload, sort_keys=True, default=str)

    def get_prebook(self, label: str, cfg: dict[str, Any]) -> pd.DataFrame:
        key = self.prebook_key(label, cfg)
        if key in self._prebook_cache:
            return self._prebook_cache[key]
        df = self.frames[label]
        rows = df[df["setup"].astype(str).eq(SETUP)].copy()
        rows = loop.tt.apply_guards(rows, cfg.get("guard"))
        rows = loop.apply_premom_cached(rows, cfg.get("premom_terms"))
        deduped = loop.tt.dedupe_family(rows)
        self._prebook_cache[key] = deduped
        return deduped

    def attach_exit_cols(self, label: str, book: pd.DataFrame, tok: str) -> pd.DataFrame:
        cols = [
            f"_fast_net_{tok}",
            f"_fast_exit_iso_{tok}",
            f"_fast_outcome_{tok}",
            f"_fast_exit_px_{tok}",
        ]
        if all(c in book.columns for c in cols):
            return book
        right = self.frames[label][["_row_id_fast", *cols]]
        drop_cols = [c for c in cols if c in book.columns]
        if drop_cols:
            book = book.drop(columns=drop_cols)
        return book.merge(right, on="_row_id_fast", how="left")

    def fast_overlay(self, book: pd.DataFrame, cfg: dict[str, Any], tok: str) -> pd.DataFrame:
        cap = int(cfg.get("max_positions") or 20)
        limit = abs(float(cfg.get("daily_loss_rs") or 0.0))
        if book.empty or (cap <= 0 and limit <= 0):
            return book
        net_col = f"_fast_net_{tok}"
        exit_col = f"_fast_exit_iso_{tok}"
        recs = []
        for idx, r in book.iterrows():
            net = float(r.get(net_col, float("nan")))
            exit_iso = r.get(exit_col)
            if not np.isfinite(net) or pd.isna(exit_iso):
                continue
            entry_ts = pd.Timestamp(r["tt_entry_iso"])
            recs.append((entry_ts, pd.Timestamp(exit_iso), net, entry_ts.normalize().tz_localize(None), idx))
        if not recs:
            return book.iloc[0:0]
        recs.sort(key=lambda x: (x[0], x[4]))
        open_pos: list[tuple[pd.Timestamp, float, pd.Timestamp]] = []
        realized_by_day: dict[Any, float] = {}
        taken_idx: list[Any] = []
        for entry_ts, exit_ts, net, day, idx in recs:
            still = []
            for ex_ts, n, dy in open_pos:
                if ex_ts <= entry_ts:
                    realized_by_day[dy] = realized_by_day.get(dy, 0.0) + n
                else:
                    still.append((ex_ts, n, dy))
            open_pos = still
            if limit > 0 and realized_by_day.get(day, 0.0) <= -limit:
                continue
            if cap > 0 and len(open_pos) >= cap:
                continue
            taken_idx.append(idx)
            open_pos.append((exit_ts, net, day))
        return book.loc[taken_idx]

    def metrics(self, book: pd.DataFrame, cfg: dict[str, Any], tok: str, detail: bool) -> dict[str, Any]:
        net_col = f"_fast_net_{tok}"
        outcome_col = f"_fast_outcome_{tok}"
        exit_col = f"_fast_exit_iso_{tok}"
        exit_px_col = f"_fast_exit_px_{tok}"
        if book.empty or net_col not in book.columns:
            return loop.empty_metrics(book, detail)
        net = pd.to_numeric(book[net_col], errors="coerce").to_numpy(dtype=float)
        finite = np.isfinite(net)
        book = book.loc[finite].reset_index(drop=True)
        netf = net[finite]
        if not len(netf):
            return loop.empty_metrics(book, detail)
        gp = float(netf[netf > 0].sum())
        gl = float(-netf[netf < 0].sum())
        wins = netf[netf > 0]
        losses = netf[netf < 0]
        eq = netf.cumsum()
        dd = eq - np.maximum.accumulate(eq)
        total = float(netf.sum())
        day_dom = sym_dom = 9.99
        top_day = top_sym = None
        if len(book):
            day_sum = pd.Series(netf, index=book["_day"].to_numpy()).groupby(level=0).sum()
            sym_sum = pd.Series(netf, index=book["ticker"].to_numpy()).groupby(level=0).sum()
            if total > 0:
                day_dom = round(float(day_sum.max()) / total, 3)
                sym_dom = round(float(sym_sum.max()) / total, 3)
            top_day = f"{pd.Timestamp(day_sum.idxmax()).date()}: Rs{day_sum.max():,.0f}" if len(day_sum) else None
            top_sym = f"{sym_sum.idxmax()}: Rs{sym_sum.max():,.0f}" if len(sym_sum) else None
        oc = book[outcome_col].astype(str).str.upper() if outcome_col in book.columns else pd.Series("", index=book.index)
        sl_cnt = int((oc == "SL").sum())
        tgt_cnt = int((oc == "TARGET").sum())
        eod_cnt = int((oc == "EOD").sum())
        other_cnt = int((~oc.isin(["SL", "TARGET", "EOD"])).sum())
        day_p = loop.tt._day_block_p(book, netf) if len(book) else float("nan")
        det = pd.DataFrame()
        if detail:
            det = pd.DataFrame({
                "trade_date": book["tt_sig_ts"].map(lambda x: pd.Timestamp(x).date()),
                "ticker": book["ticker"].to_numpy(),
                "side": book["side"].to_numpy(),
                "setup": book["setup"].to_numpy(),
                "signal_time": book["tt_sig_ts"].astype(str).to_numpy(),
                "entry_time": book["tt_entry_iso"].astype(str).to_numpy(),
                "entry_price": pd.to_numeric(book["tt_fill"], errors="coerce").round(2).to_numpy(),
                "exit_price": pd.to_numeric(book[exit_px_col], errors="coerce").round(2).to_numpy() if exit_px_col in book.columns else np.nan,
                "qty": pd.to_numeric(book["tt_qty"], errors="coerce").fillna(0).astype(int).to_numpy(),
                "sl_pct": float(cfg["sl"]),
                "tgt_pct": float(cfg["tgt"]),
                "outcome": oc.to_numpy(),
                "exit_time": book[exit_col].astype(str).to_numpy() if exit_col in book.columns else "",
                "net_pnl_rs": netf.round(2),
            })
        return {
            "n": int(len(netf)),
            "net_pf": round(float(loop.tt._pf(netf)), 3),
            "net_pnl": round(total, 0),
            "day_block_p": None if not np.isfinite(day_p) else round(float(day_p), 4),
            "wins": int((netf > 0).sum()),
            "losses": int((netf < 0).sum()),
            "win_rate": round(float((netf > 0).mean()) * 100, 1),
            "gross_profit": round(gp, 0),
            "gross_loss": round(gl, 0),
            "avg_win": round(float(wins.mean()), 0) if len(wins) else 0.0,
            "avg_loss": round(float(losses.mean()), 0) if len(losses) else 0.0,
            "max_dd": round(float(dd.min()), 0) if len(dd) else 0.0,
            "n_days": int(book["_day"].nunique()) if len(book) else 0,
            "n_syms": int(book["ticker"].nunique()) if len(book) else 0,
            "trades_per_day": round(len(netf) / max(1, int(book["_day"].nunique()) if len(book) else 1), 2),
            "sl_cnt": sl_cnt,
            "tgt_cnt": tgt_cnt,
            "eod_cnt": eod_cnt,
            "other_cnt": other_cnt,
            "target_rate": round(float(tgt_cnt / len(netf)) * 100, 1) if len(netf) else 0.0,
            "trade_dom_gross": round(float(netf.max()) / gp, 3) if gp > 0 and len(netf) else 9.99,
            "day_dom": day_dom,
            "sym_dom": sym_dom,
            "top_day": top_day,
            "top_sym": top_sym,
            "detail": det,
        }

    def eval(self, cfg: dict[str, Any], label: str, detail: bool = False) -> dict[str, Any]:
        sl = float(cfg["sl"])
        tgt = float(cfg["tgt"])
        self.ensure_exit(label, sl, tgt)
        tok = exit_token(sl, tgt)
        deduped = self.get_prebook(label, cfg)
        book = loop.tt.apply_mask_terms(deduped, cfg.get("mask_terms", []))
        if book.empty:
            return loop.empty_metrics(book, detail)
        book = self.attach_exit_cols(label, book, tok)
        book = loop.tt._apply_regime_align(book)
        book = self.fast_overlay(book, cfg, tok)
        return self.metrics(book, cfg, tok, detail)


def rerun_command(args: argparse.Namespace) -> str:
    return (
        f"python Train_and_Test\\setup_pf_1_4_full_loop\\{SETUP}\\scripts\\staged_rescue_sweeps.py "
        f"--top_mask_terms {args.top_mask_terms} --top_pm_terms {args.top_pm_terms} "
        f"--max_configs {args.max_configs} --min_split_trades {args.min_split_trades}"
    )


def run_staged_search(args: argparse.Namespace, frames: dict[str, pd.DataFrame], fast_eval: FastEvaluator) -> dict[str, Any]:
    fit = frames["FIT"]
    val = frames["VAL"]
    train = frames["TRAIN"]
    test = frames["TEST"]
    mask_terms = build_terms(fit, MASK_FEATURES, args.min_feature_nonnull, MANUAL_THRESHOLDS)
    pm_terms = build_terms(fit, PM_FEATURES, args.min_pm_nonnull, MANUAL_THRESHOLDS)
    print(f"[staged] mask_terms={len(mask_terms)} pm_terms={len(pm_terms)}", flush=True)

    rows: list[dict[str, Any]] = []
    tested: dict[str, dict[str, Any]] = {}
    passing: list[dict[str, Any]] = []
    train_band: list[dict[str, Any]] = []
    atomic_mask_scores: list[tuple[float, tuple[str, str, float]]] = []
    atomic_pm_scores: list[tuple[float, tuple[str, str, float]]] = []
    t0 = time.time()

    def evaluate(cfg: dict[str, Any], stage: str, reason: str) -> dict[str, Any] | None:
        if len(rows) >= args.max_configs:
            return None
        key = cfg_key(cfg)
        if key in tested:
            return tested[key]
        fit_m = fast_eval.eval(cfg, "FIT", detail=False)
        val_m = fast_eval.eval(cfg, "VAL", detail=False)
        score = fitval_score(fit_m, val_m, args.min_split_trades)
        train_m: dict[str, Any] | None = None
        test_m: dict[str, Any] | None = None
        keep = "REJECT_FITVAL"
        failure = "FIT/VAL gate failed"
        tested_test = False

        if should_confirm_train(fit_m, val_m, args.min_split_trades):
            train_fast = fast_eval.eval(cfg, "TRAIN", detail=False)
            train_m = train_fast
            keep = "REJECT_FULL_TRAIN"
            failure = f"full TRAIN PF/n/net = {train_fast.get('net_pf')}/{train_fast.get('n')}/{train_fast.get('net_pnl')}"
            if (
                loop.PF_LO <= float(train_fast.get("net_pf") or 0) <= loop.PF_HI
                and float(train_fast.get("net_pnl") or 0) > 0
                and int(train_fast.get("n") or 0) >= 20
            ):
                train_m = fast_eval.eval(cfg, "TRAIN", detail=True)
                test_m = fast_eval.eval(cfg, "TEST", detail=True)
                tested_test = True
                ok, reasons = pass_candidate_with_notes(cfg, train_m, test_m)
                if ok and args.exact_validate_passes:
                    exact_train = loop.eval_cfg(cfg, train, detail=True)
                    exact_test = loop.eval_cfg(cfg, test, detail=True)
                    exact_ok, exact_reasons = pass_candidate_with_notes(cfg, exact_train, exact_test)
                    if exact_ok:
                        train_m = exact_train
                        test_m = exact_test
                    else:
                        ok = False
                        reasons.extend([f"exact validation: {r}" for r in exact_reasons])
                train_band.append({
                    "cfg": clean(cfg),
                    "stage": stage,
                    "reason": reason,
                    "train": loop.strip_detail(train_m),
                    "test": loop.strip_detail(test_m),
                    "pass": bool(ok),
                    "reasons": reasons,
                })
                if ok:
                    keep = "PASS_APPROVAL_REQUIRED"
                    failure = ""
                    cid = f"{SETUP}_staged_candidate_{len(passing) + 1:03d}"
                    passing.append({
                        "candidate_id": cid,
                        "cfg": clean(loop.cfg_to_block(cfg)),
                        "stage": stage,
                        "reason": reason,
                        "train": loop.strip_detail(train_m),
                        "test": loop.strip_detail(test_m),
                    })
                else:
                    keep = "REJECT_TEST_OR_STABILITY"
                    failure = "; ".join(reasons)

        row = {
            "iteration": len(rows) + 1,
            "stage": stage,
            "parameter_group": classify_cfg(cfg),
            "reason": reason,
            "sl": cfg["sl"],
            "tgt": cfg["tgt"],
            "mask_terms": loop.terms_text(cfg.get("mask_terms")),
            "premom_terms": loop.terms_text(cfg.get("premom_terms")),
            "guard": json.dumps(cfg.get("guard") or {}, sort_keys=True),
            "fit_n": fit_m.get("n"),
            "fit_pf": fit_m.get("net_pf"),
            "fit_net": fit_m.get("net_pnl"),
            "val_n": val_m.get("n"),
            "val_pf": val_m.get("net_pf"),
            "val_net": val_m.get("net_pnl"),
            "train_n": train_m.get("n") if train_m else None,
            "train_pf": train_m.get("net_pf") if train_m else None,
            "train_net": train_m.get("net_pnl") if train_m else None,
            "test_n": test_m.get("n") if test_m else None,
            "test_pf": test_m.get("net_pf") if test_m else None,
            "test_net": test_m.get("net_pnl") if test_m else None,
            "score": round(score, 6),
            "keep_reject": keep,
            "failure_classification": failure,
            "tested_test": tested_test,
            "cfg_json": cfg_key(cfg),
        }
        rows.append(row)
        tested[key] = row
        if len(rows) % args.progress_every == 0:
            best = max(rows, key=lambda r: r["score"])
            print(
                f"[staged] {len(rows)} configs stage={stage} best_score={best['score']} "
                f"passes={len(passing)} train_band={len(train_band)} elapsed={time.time() - t0:.0f}s",
                flush=True,
            )
        return row

    # Stage 0: compact exit and guard sanity check. The exhaustive exit surface
    # is explored later only around filters that survive the FIT/VAL screens.
    block = loop.fsc.FINAL_SETUP_CONF.get(SETUP) or loop.fsc.RESEARCH_WATCH_CONF.get(SETUP)
    if block:
        evaluate(loop.conf_to_cfg(block), "baseline_reference", "current final_setup_conf block")
    for sl, tgt in STAGE0_EXITS:
        for guard in STAGE0_GUARDS:
            evaluate(make_cfg(sl, tgt, guard=guard), "exit_guard", "raw setup with exit/guard only")

    # Stage 1A: every single term at baseline exit/no guard.
    for term in mask_terms:
        row = evaluate(make_cfg(1.20, 1.50, mask_terms=[term]), "atomic_mask", f"single mask {term}")
        if row:
            atomic_mask_scores.append((float(row["score"]), term))
    for term in pm_terms:
        row = evaluate(make_cfg(1.20, 1.50, premom_terms=[term]), "atomic_premom", f"single premom {term}")
        if row:
            atomic_pm_scores.append((float(row["score"]), term))

    top_mask = stable_unique_terms(atomic_mask_scores, args.top_mask_terms)
    top_pm = stable_unique_terms(atomic_pm_scores, args.top_pm_terms)
    print(f"[staged] selected top_mask={len(top_mask)} top_pm={len(top_pm)}", flush=True)

    # Stage 1B: top single terms across practical exits/guards.
    for term in top_mask:
        for sl, tgt in BASE_EXITS:
            for guard in NARROW_GUARDS:
                evaluate(make_cfg(sl, tgt, mask_terms=[term], guard=guard), "single_mask_x_exit_guard", f"top single mask {term}")
    for term in top_pm:
        for sl, tgt in PM_EXITS:
            for guard in NARROW_GUARDS:
                evaluate(make_cfg(sl, tgt, premom_terms=[term], guard=guard), "single_premom_x_exit_guard", f"top single premom {term}")

    # Stage 2: combinations from train-side stable single terms.
    top_mask2 = top_mask[: args.top_mask_pair_terms]
    top_pm2 = top_pm[: args.top_pm_pair_terms]
    for t1, t2 in itertools.combinations(top_mask2, 2):
        if t1[0] == t2[0] and t1[1] == t2[1]:
            continue
        for sl, tgt in BASE_EXITS[:4]:
            for guard in NARROW_GUARDS[:4]:
                evaluate(make_cfg(sl, tgt, mask_terms=[t1, t2], guard=guard), "mask_pair", f"mask pair {t1} + {t2}")

    for t1, t2 in itertools.combinations(top_pm2, 2):
        if t1[0] == t2[0] and t1[1] == t2[1]:
            continue
        for sl, tgt in PM_EXITS:
            for guard in NARROW_GUARDS[:4]:
                evaluate(make_cfg(sl, tgt, premom_terms=[t1, t2], guard=guard), "premom_pair", f"premom pair {t1} + {t2}")

    for mt in top_mask[: args.top_mask_pm_terms]:
        for pt in top_pm[: args.top_mask_pm_pm_terms]:
            for sl, tgt in PM_EXITS:
                for guard in NARROW_GUARDS[:4]:
                    evaluate(make_cfg(sl, tgt, mask_terms=[mt], premom_terms=[pt], guard=guard), "mask_plus_premom", f"mask {mt} + premom {pt}")

    rows_df = pd.DataFrame(rows)
    rows_df.to_csv(RESULTS_CSV, index=False)
    return {
        "engine": "deterministic staged 5m-enriched rescue sweeps",
        "n_configs": int(len(rows)),
        "mask_terms_tested": int(len(mask_terms)),
        "premom_terms_tested": int(len(pm_terms)),
        "top_mask_terms": [list(x) for x in top_mask],
        "top_pm_terms": [list(x) for x in top_pm],
        "rows": rows,
        "passing": passing,
        "train_band": train_band,
    }


def metric_line(m: dict[str, Any]) -> str:
    return loop.metric_line(m)


def write_feature_report(feature_report: dict[str, Any]) -> None:
    lines = [
        f"# {SETUP} - FEATURE_ENRICHMENT_REPORT",
        "",
        "Research-only 5-minute feature enrichment.",
        "",
        f"- Cache: `{feature_report['cache_path']}`",
        f"- Cache rows: {feature_report['cache_rows']}",
        f"- Cache status counts: {feature_report['cache_status_counts']}",
        "",
        "## Coverage",
        "",
        "| feature | FIT | VAL | TRAIN | TEST |",
        "|---|---:|---:|---:|---:|",
    ]
    for feat, cov in feature_report["coverage"].items():
        lines.append(f"| {feat} | {cov.get('FIT', 0)} | {cov.get('VAL', 0)} | {cov.get('TRAIN', 0)} | {cov.get('TEST', 0)} |")
    (WORK / "FEATURE_ENRICHMENT_REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def choose_candidate(search: dict[str, Any]) -> dict[str, Any] | None:
    passing = search.get("passing") or []
    if not passing:
        return None
    passing = sorted(passing, key=lambda c: (c["test"]["net_pf"], c["test"]["net_pnl"], c["train"]["n"]), reverse=True)
    return passing[0]


def write_candidate_outputs(search: dict[str, Any], chosen: dict[str, Any] | None) -> None:
    cand_dir = WORK / "candidates"
    cand_dir.mkdir(exist_ok=True)
    lines = [
        f"# {SETUP} - CANDIDATE_CONFIGS",
        "",
        "## Staged 5m-Enriched Rescue Search",
        "",
    ]
    passing = search.get("passing") or []
    if not passing:
        lines += [
            "No staged candidate passed all acceptance checks.",
            f"- TRAIN PF must be {loop.PF_LO}-{loop.PF_HI}.",
            f"- TEST PF must be > {loop.TEST_PF_MIN}.",
            "- TRAIN and TEST net PnL must be positive.",
            "- Domination checks must pass.",
        ]
        (cand_dir / "NO_CANDIDATES_STAGED.md").write_text("\n".join(lines), encoding="utf-8")
    else:
        for i, c in enumerate(sorted(passing, key=lambda x: (x["test"]["net_pf"], x["test"]["net_pnl"]), reverse=True), 1):
            cid = f"{SETUP}_staged_candidate_{i:03d}"
            c["candidate_id"] = cid
            (cand_dir / f"{cid}.json").write_text(as_json(c), encoding="utf-8")
            lines += [
                f"## {cid}",
                "",
                "```json",
                as_json(c["cfg"]),
                "```",
                "",
                f"- Stage: {c.get('stage')} ({c.get('reason')})",
                f"- TRAIN: {metric_line(c['train'])}",
                f"- TEST: {metric_line(c['test'])}",
                "",
            ]
    if chosen:
        lines += [
            "## Selected For Approval Review",
            "",
            f"- Candidate: {chosen['candidate_id']}",
            "- No final config was edited.",
        ]
    (WORK / "CANDIDATE_CONFIGS.md").write_text("\n".join(lines), encoding="utf-8")


def write_summary(search: dict[str, Any], feature_report: dict[str, Any], baseline: dict[str, Any], chosen: dict[str, Any] | None, args: argparse.Namespace) -> None:
    rows = pd.DataFrame(search["rows"])
    lines = [
        f"# {SETUP} - STAGED_RESCUE_SUMMARY",
        "",
        f"- Engine: {search['engine']}",
        f"- Configs evaluated: {search['n_configs']}",
        f"- Mask terms built from FIT: {search['mask_terms_tested']}",
        f"- Pre-momentum terms built from FIT: {search['premom_terms_tested']}",
        f"- Feature cache rows/status: {feature_report['cache_rows']} / {feature_report['cache_status_counts']}",
        f"- Acceptance: TRAIN PF {loop.PF_LO}-{loop.PF_HI}, TEST PF > {loop.TEST_PF_MIN}, positive TRAIN/TEST net, no domination.",
        "",
        "## Baseline Reference",
        "",
        f"- TRAIN: {metric_line(baseline['metrics']['TRAIN'])}",
        f"- TEST: {metric_line(baseline['metrics']['TEST'])}",
        "",
    ]
    if not rows.empty:
        top = rows.sort_values(["score", "fit_pf", "val_pf"], ascending=False).head(25)
        lines += ["## Top 25 FIT/VAL Rows", "", "| iter | stage | group | config | FIT | VAL | TRAIN | TEST | keep/reject |", "|---|---|---|---|---|---|---|---|---|"]
        for _, r in top.iterrows():
            cfg = f"SL/Tgt={r['sl']}/{r['tgt']} mask=[{r['mask_terms']}] premom=[{r['premom_terms']}] guard={r['guard']}"
            lines.append(
                f"| {int(r['iteration'])} | {r['stage']} | {r['parameter_group']} | {cfg} | "
                f"{r['fit_n']}/{r['fit_pf']}/Rs{r['fit_net']} | {r['val_n']}/{r['val_pf']}/Rs{r['val_net']} | "
                f"{r['train_n']}/{r['train_pf']}/Rs{r['train_net']} | {r['test_n']}/{r['test_pf']}/Rs{r['test_net']} | {r['keep_reject']} |"
            )
        counts = rows["keep_reject"].value_counts(dropna=False).to_dict()
        lines += ["", "## Outcome Counts", "", f"- {counts}", ""]
        band = rows[rows["tested_test"].fillna(False)]
        lines += [f"- Full TRAIN-band rows tested on TEST: {len(band)}"]
    if chosen:
        lines += [
            "",
            "## Approval Candidate",
            "",
            f"- Candidate: {chosen['candidate_id']}",
            f"- TRAIN: {metric_line(chosen['train'])}",
            f"- TEST: {metric_line(chosen['test'])}",
            "- Approval is still required before any final config edit.",
        ]
    else:
        lines += [
            "",
            "## Recommendation",
            "",
            "- No passing staged candidate. Do not promote this setup from the staged rescue search.",
        ]
    lines += [
        "",
        "## Rerun",
        "",
        "```powershell",
        rerun_command(args),
        "```",
    ]
    SUMMARY_MD.write_text("\n".join(lines), encoding="utf-8")


def append_or_replace_section(path: Path, title: str, body: str) -> None:
    text = path.read_text(encoding="utf-8") if path.exists() else ""
    marker = f"\n## {title}\n"
    if marker in text:
        text = text.split(marker)[0].rstrip() + marker + body.strip() + "\n"
    else:
        if text and not text.endswith("\n"):
            text += "\n"
        text += f"\n## {title}\n{body.strip()}\n"
    path.write_text(text, encoding="utf-8")


def update_primary_reports(search: dict[str, Any], feature_report: dict[str, Any], chosen: dict[str, Any] | None) -> None:
    rows = pd.DataFrame(search["rows"])
    counts = rows["keep_reject"].value_counts(dropna=False).to_dict() if not rows.empty else {}
    inv_body = [
        "",
        "Additional 5-minute features were joined from `C:\\TradingData\\eqidv2\\stocks_indicators_5min_eq_live2` by ticker and signal bar time.",
        f"Feature cache: `{feature_report['cache_path']}`.",
        "",
        "Searchable enriched feature columns:",
        "",
        "- " + ", ".join(f"`{c}`" for c in [*CANONICAL_FILL.keys(), "day_value_so_far_rs", *FEATURE_COLS]),
    ]
    append_or_replace_section(WORK / "PARAMETER_INVENTORY.md", "Staged 5m-Enriched Feature Addendum", "\n".join(inv_body))

    sweep_body = [
        "",
        f"- Engine: {search['engine']}",
        f"- Configs evaluated: {search['n_configs']}",
        f"- Outcome counts: {counts}",
        f"- Passing candidates: {len(search.get('passing') or [])}",
        f"- Full TRAIN-band rows tested on TEST: {sum(1 for r in search.get('rows', []) if r.get('tested_test'))}",
        f"- Detailed results: `{RESULTS_CSV}` and `{SUMMARY_MD}`.",
    ]
    append_or_replace_section(WORK / "PARAMETER_SWEEP_SUMMARY.md", "Staged 5m-Enriched Rescue Sweep", "\n".join(sweep_body))

    fail_body = [
        "",
        f"- Staged outcome counts: {counts}",
        "- TEST was only evaluated after full TRAIN landed inside the PF band.",
    ]
    if chosen:
        fail_body.append(f"- Staged candidate selected for approval review: {chosen['candidate_id']}.")
    else:
        fail_body.append("- No staged candidate passed TRAIN/TEST/stability gates.")
    append_or_replace_section(WORK / "FAILURE_ANALYSIS.md", "Staged 5m-Enriched Failure Update", "\n".join(fail_body))


def write_final_recommendation(chosen: dict[str, Any] | None, baseline: dict[str, Any], split: dict[str, Any], args: argparse.Namespace) -> None:
    rec = "YES" if chosen else "NO"
    block = chosen["cfg"] if chosen else loop.cfg_to_block(baseline["cfg"])
    heading = "Best Candidate" if chosen else "Baseline Reference (No Passing Candidate)"
    final_path = loop.REPO / "final_setup_conf.py"
    lines = [
        f"# {SETUP} - APPROVAL_REQUIRED_FINAL_RECOMMENDATION",
        "",
        f"Approval recommendation: {rec}",
        "",
        f"## {heading}",
        "",
        "```json",
        as_json(block),
        "```",
        "",
        "## Metrics",
        "",
    ]
    if chosen:
        lines += [
            f"- TRAIN: {metric_line(chosen['train'])}",
            f"- TEST: {metric_line(chosen['test'])}",
            f"- Candidate config path: `{WORK / 'candidates' / (chosen['candidate_id'] + '.json')}`",
            "- No final config edit was performed.",
        ]
    else:
        lines += [
            "- No passing staged candidate. Baseline retained for reference only.",
            f"- Baseline TRAIN: {metric_line(baseline['metrics']['TRAIN'])}",
            f"- Baseline TEST: {metric_line(baseline['metrics']['TEST'])}",
        ]
    lines += [
        "",
        "## Final File That Would Need Approval Before Edit",
        "",
        f"- `{final_path}`",
        "",
        "## Proposed Patch",
        "",
        "- Do not apply automatically. If approved, replace only this setup block with the JSON-equivalent block above.",
        "",
        "DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES",
        "",
        "## Rerun Commands",
        "",
        "```powershell",
        rerun_command(args),
        "```",
        "",
        "## Risk Notes",
        "",
        f"- TRAIN sessions: {loop.rng_text(split['train_sessions'])} ({len(split['train_sessions'])}).",
        f"- TEST sessions: {loop.rng_text(split['test_sessions'])} ({len(split['test_sessions'])}).",
        "- 5-minute enriched filters require the same feature fields to be available before any live promotion.",
        "- No live trades, order placement, or final config edits were performed.",
    ]
    (WORK / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--recreate_pool", action="store_true")
    ap.add_argument("--top_mask_terms", type=int, default=70)
    ap.add_argument("--top_pm_terms", type=int, default=35)
    ap.add_argument("--top_mask_pair_terms", type=int, default=35)
    ap.add_argument("--top_pm_pair_terms", type=int, default=20)
    ap.add_argument("--top_mask_pm_terms", type=int, default=35)
    ap.add_argument("--top_mask_pm_pm_terms", type=int, default=20)
    ap.add_argument("--max_configs", type=int, default=60000)
    ap.add_argument("--min_split_trades", type=int, default=5)
    ap.add_argument("--min_feature_nonnull", type=int, default=30)
    ap.add_argument("--min_pm_nonnull", type=int, default=20)
    ap.add_argument("--progress_every", type=int, default=100)
    ap.add_argument("--exact_validate_passes", action=argparse.BooleanOptionalAction, default=True)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    WORK.mkdir(parents=True, exist_ok=True)
    POOLS.mkdir(parents=True, exist_ok=True)
    (WORK / "candidates").mkdir(exist_ok=True)

    print(f"[staged] setup={SETUP}")
    print(f"[staged] work={WORK}")
    if args.recreate_pool or not (POOLS / loop.FNAME).exists():
        loop.recreate_pool()

    fit, val, train, test, split = loop.split_frames(POOLS)
    frames, feature_report = enrich_frames({"FIT": fit, "VAL": val, "TRAIN": train, "TEST": test})
    print(f"[staged] enriched FIT={len(frames['FIT'])} VAL={len(frames['VAL'])} TRAIN={len(frames['TRAIN'])} TEST={len(frames['TEST'])}", flush=True)
    write_feature_report(feature_report)
    fast_eval = FastEvaluator(frames)

    block = loop.fsc.FINAL_SETUP_CONF.get(SETUP) or loop.fsc.RESEARCH_WATCH_CONF.get(SETUP)
    if not block:
        raise SystemExit(f"{SETUP} missing from final_setup_conf.py")
    baseline = loop.run_baseline(block, frames["FIT"], frames["VAL"], frames["TRAIN"], frames["TEST"])

    search = run_staged_search(args, frames, fast_eval)
    chosen = choose_candidate(search)
    write_candidate_outputs(search, chosen)
    write_summary(search, feature_report, baseline, chosen, args)
    update_primary_reports(search, feature_report, chosen)
    write_final_recommendation(chosen, baseline, split, args)

    run_summary = {
        "setup": SETUP,
        "feature_report": feature_report,
        "baseline": {k: loop.strip_detail(v) for k, v in baseline["metrics"].items()},
        "search": {k: v for k, v in search.items() if k != "rows"},
        "best_candidate": chosen,
        "approval_recommendation": "YES" if chosen else "NO",
        "results_csv": str(RESULTS_CSV),
        "summary_md": str(SUMMARY_MD),
        "rerun_command": rerun_command(args),
    }
    RUN_SUMMARY.write_text(as_json(run_summary), encoding="utf-8")
    print(f"[staged] wrote {RESULTS_CSV}")
    print(f"[staged] approval_recommendation={run_summary['approval_recommendation']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
