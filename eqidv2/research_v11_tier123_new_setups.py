from __future__ import annotations

import math
import os
import gc
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd

import avwap_5min_ID_v2_backtesting as v2
import avwap_5min_ID_v11_backtesting as v11


DATA_ROOT = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
OUT_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_tier123_new_setup_probe")
BASE_TRADES_CSV = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\trades.csv")
BASE_PROFILE = "production_core_ab_max_pnl_low_valid_residual_overlay"

START_DATE = pd.Timestamp("2025-06-02")
END_DATE = pd.Timestamp("2026-05-29")
TRAIN_END = pd.Timestamp("2026-01-31")
VALID_START = pd.Timestamp("2026-02-01")
VALID_END = pd.Timestamp("2026-03-31")
HOLDOUT_START = pd.Timestamp("2026-04-01")
HOLDOUT_END = pd.Timestamp("2026-05-29")

MAX_RAW_PER_SETUP = 2500
UNIVERSE_MODE = os.getenv("EQIDV2_TIER123_UNIVERSE", "futures").strip().lower()
SCAN_WORKERS = max(1, int(os.getenv("EQIDV2_TIER123_WORKERS", "4")))
SCAN_START_INDEX = max(1, int(os.getenv("EQIDV2_TIER123_START_INDEX", "1")))
_SCAN_END_INDEX_RAW = os.getenv("EQIDV2_TIER123_END_INDEX", "").strip()
SCAN_END_INDEX = int(_SCAN_END_INDEX_RAW) if _SCAN_END_INDEX_RAW else None
SCAN_TAG = os.getenv("EQIDV2_TIER123_SCAN_TAG", "").strip()
SCAN_ONLY = os.getenv("EQIDV2_TIER123_SCAN_ONLY", "0").strip() == "1"
COMBINE_CHUNKS = os.getenv("EQIDV2_TIER123_COMBINE_CHUNKS", "0").strip() == "1"
CHUNK_GLOB = os.getenv("EQIDV2_TIER123_CHUNK_GLOB", "tier123_raw_candidates_chunk_*.csv").strip()

PROBE_EXIT_RULES: dict[str, tuple[float, float]] = {
    # Tier 1
    "E_ORB_RETEST_HOLD_LONG": (0.70, 1.00),
    "E_ORB_RETEST_HOLD_SHORT": (0.70, 1.00),
    "E_FAILED_OR_BREAKOUT_TRAP_SHORT": (0.75, 1.00),
    "E_FAILED_OR_BREAKDOWN_TRAP_LONG": (0.75, 1.00),
    "V_RECLAIM_PULLBACK_LONG": (0.70, 1.00),
    "V_REJECTION_PULLBACK_SHORT": (0.70, 1.00),
    "M_EXPANSION_FIRST_PULLBACK_LONG": (0.80, 1.20),
    "M_EXPANSION_FIRST_PULLBACK_SHORT": (0.80, 1.20),
    "C_LATE_MORNING_COMPRESSION_BREAK_LONG": (0.75, 1.10),
    "C_LATE_MORNING_COMPRESSION_BREAK_SHORT": (0.75, 1.10),
    # Tier 2
    "G_GAP_HOLD_CONTINUATION_LONG": (0.80, 1.20),
    "G_GAP_HOLD_CONTINUATION_SHORT": (0.80, 1.20),
    "A_HVN_ABSORPTION_BREAK_LONG": (0.75, 1.00),
    "A_HVN_ABSORPTION_BREAK_SHORT": (0.75, 1.00),
    "T_TREND_DAY_EMA_STAIR_LONG": (0.70, 1.00),
    "T_TREND_DAY_EMA_STAIR_SHORT": (0.70, 1.00),
    # Tier 3
    "P_PDH_BREAK_RETEST_LONG": (0.75, 1.00),
    "P_PDL_BREAK_RETEST_SHORT": (0.75, 1.00),
    "MR_CONTROLLED_VWAP_EXTREME_FADE_LONG": (0.70, 0.80),
    "MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT": (0.70, 0.80),
}

SETUP_TIERS: dict[str, str] = {
    "E_ORB_RETEST_HOLD_LONG": "Tier 1",
    "E_ORB_RETEST_HOLD_SHORT": "Tier 1",
    "E_FAILED_OR_BREAKOUT_TRAP_SHORT": "Tier 1",
    "E_FAILED_OR_BREAKDOWN_TRAP_LONG": "Tier 1",
    "V_RECLAIM_PULLBACK_LONG": "Tier 1",
    "V_REJECTION_PULLBACK_SHORT": "Tier 1",
    "M_EXPANSION_FIRST_PULLBACK_LONG": "Tier 1",
    "M_EXPANSION_FIRST_PULLBACK_SHORT": "Tier 1",
    "C_LATE_MORNING_COMPRESSION_BREAK_LONG": "Tier 1",
    "C_LATE_MORNING_COMPRESSION_BREAK_SHORT": "Tier 1",
    "S_SECTOR_RS_CONTINUATION_LONG": "Tier 2 blocked",
    "S_SECTOR_RW_CONTINUATION_SHORT": "Tier 2 blocked",
    "G_GAP_HOLD_CONTINUATION_LONG": "Tier 2",
    "G_GAP_HOLD_CONTINUATION_SHORT": "Tier 2",
    "A_HVN_ABSORPTION_BREAK_LONG": "Tier 2",
    "A_HVN_ABSORPTION_BREAK_SHORT": "Tier 2",
    "T_TREND_DAY_EMA_STAIR_LONG": "Tier 2",
    "T_TREND_DAY_EMA_STAIR_SHORT": "Tier 2",
    "P_PDH_BREAK_RETEST_LONG": "Tier 3",
    "P_PDL_BREAK_RETEST_SHORT": "Tier 3",
    "MR_CONTROLLED_VWAP_EXTREME_FADE_LONG": "Tier 3",
    "MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT": "Tier 3",
}

SECTOR_BLOCKED_ROWS = [
    {
        "setup": "S_SECTOR_RS_CONTINUATION_LONG",
        "tier": "Tier 2",
        "status": "blocked_missing_sector_5m_data",
        "reason": "No sector index/ETF 5-minute files found in active data root; keep as future infrastructure setup.",
    },
    {
        "setup": "S_SECTOR_RW_CONTINUATION_SHORT",
        "tier": "Tier 2",
        "status": "blocked_missing_sector_5m_data",
        "reason": "No sector index/ETF 5-minute files found in active data root; keep as future infrastructure setup.",
    },
]

_WORKER_MARKET_CTX: dict[str, dict[pd.Timestamp, dict]] = {}


def _normalise_ts(value) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tz is None:
        return ts.tz_localize("Asia/Kolkata")
    return ts.tz_convert("Asia/Kolkata")


def _pf(pnl: pd.Series | np.ndarray) -> float:
    s = pd.Series(pnl, dtype="float64").fillna(0.0)
    gains = float(s[s > 0].sum())
    losses = float(-s[s < 0].sum())
    if losses <= 0:
        return math.inf if gains > 0 else math.nan
    return gains / losses


def _pf_score(value: float) -> float:
    if pd.isna(value):
        return -1.0
    if value == math.inf:
        return 8.0
    return float(value)


def _split_masks(df: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "train": df["date"] <= TRAIN_END,
        "valid": (df["date"] >= VALID_START) & (df["date"] <= VALID_END),
        "holdout": (df["date"] >= HOLDOUT_START) & (df["date"] <= HOLDOUT_END),
        "full": pd.Series(True, index=df.index),
    }


def _metrics(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {"trades": 0, "days": 0, "pnl": 0.0, "pf": math.nan, "win_pct": math.nan, "avg_trade": math.nan}
    pnl = pd.to_numeric(frame["pnl"], errors="coerce").fillna(0.0)
    return {
        "trades": int(len(frame)),
        "days": int(frame["date"].dt.date.nunique()),
        "pnl": float(pnl.sum()),
        "pf": float(_pf(pnl)),
        "win_pct": float((pnl > 0).mean() * 100.0),
        "avg_trade": float(pnl.mean()),
    }


def _stats_by_split(frame: pd.DataFrame) -> dict[str, dict]:
    if frame.empty:
        return {split: _metrics(frame) for split in ["train", "valid", "holdout", "full"]}
    return {split: _metrics(frame.loc[mask]) for split, mask in _split_masks(frame).items()}


def _flatten_stats(label: str, setup: str, frame: pd.DataFrame) -> list[dict]:
    rows = []
    for split, stats in _stats_by_split(frame).items():
        row = {"label": label, "setup": setup, "tier": SETUP_TIERS.get(setup, ""), "split": split}
        row.update(stats)
        rows.append(row)
    return rows


def _prepare_probe_5m(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "ATR" not in out.columns or out["ATR"].isna().all() or "VWAP" not in out.columns or out["VWAP"].isna().all():
        return v2._prepare_5m(out)
    out["Volume_SMA20"] = out.groupby("date_only")["volume"].transform(
        lambda s: s.shift(1).rolling(v2.VWAP_LOOKBACK, min_periods=8).mean()
    )
    out["traded_value_rs"] = out["close"] * out["volume"]
    out["day_value_so_far_rs"] = out.groupby("date_only")["traded_value_rs"].cumsum()
    out["range"] = out["high"] - out["low"]
    range_nonzero = out["range"].replace(0, np.nan)
    out["body_pct"] = (out["close"] - out["open"]).abs() / range_nonzero
    out["close_loc"] = (out["close"] - out["low"]) / range_nonzero
    out["upper_wick_pct"] = (out["high"] - out[["open", "close"]].max(axis=1)) / range_nonzero
    out["lower_wick_pct"] = (out[["open", "close"]].min(axis=1) - out["low"]) / range_nonzero
    out["vol_ratio"] = out["volume"] / out["Volume_SMA20"].replace(0, np.nan)
    out["atr_pct"] = out["ATR"] / out["close"].replace(0, np.nan)
    # --- VWAP FIX (2026-06-13) -------------------------------------------------------------------
    # The 5-min source parquet's "VWAP" column is UNRELIABLE here: it is often stale/anchored, sitting
    # ~constant far from price (e.g. 360ONE 2026-04-07: VWAP~1106 all day while price trades 906-934).
    # That corrupts (a) vwap_dist_atr (exploded to +-50..170) AND (b) every VWAP-based detection
    # (close>VWAP / close<VWAP) AND (c) regime (computed from NIFTY's VWAP in _market_context).
    # Fix at the root: recompute a proper intraday SESSION VWAP (typical-price x volume, reset each day).
    out["VWAP_parquet"] = out["VWAP"]                              # keep the raw column for reference
    _tp = (out["high"] + out["low"] + out["close"]) / 3.0
    _cum_pv = (_tp * out["volume"]).groupby(out["date_only"]).cumsum()
    _cum_v = out["volume"].groupby(out["date_only"]).cumsum().replace(0, np.nan)
    _sess_vwap = _cum_pv / _cum_v
    out["VWAP"] = _sess_vwap.where(_sess_vwap.notna(), out["VWAP_parquet"])  # fallback if volume==0
    # vwap_dist_atr with a tiny-ATR floor (>=0.03% of close) + sane clip, matching the production O(1) scale
    _atr_floor = out["ATR"].where(out["ATR"] >= 0.0003 * out["close"], 0.0003 * out["close"])
    out["vwap_dist_atr"] = ((out["close"] - out["VWAP"]) / _atr_floor.replace(0, np.nan)).clip(-15.0, 15.0)
    return out


def _read_5m(ticker: str) -> pd.DataFrame | None:
    path = DATA_ROOT / f"{ticker}_stocks_indicators_5min.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    if df.empty or "date" not in df.columns:
        return None
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize("Asia/Kolkata")
    else:
        df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    df["date_only"] = df["date"].dt.strftime("%Y-%m-%d")
    df = df.loc[(df["date"].dt.date >= START_DATE.date()) & (df["date"].dt.date <= END_DATE.date())].copy()
    if df.empty:
        return None
    return _prepare_probe_5m(df)


def _market_context() -> dict[str, dict[pd.Timestamp, dict]]:
    # NIFTYBEES first for live parity + volume-bearing VWAP regime (true NIFTY 50
    # index has zero volume and must not become the regime source).
    for ticker in ["NIFTYBEES", "NIFTY", "NIFTY50", "NIFTY_50"]:
        df = _read_5m(ticker)
        if df is not None and not df.empty:
            ctx: dict[str, dict[pd.Timestamp, dict]] = {}
            for day, g in df.groupby("date_only", sort=True):
                g = g.reset_index(drop=True)
                day_open = float(g["open"].iloc[0])
                by_ts = {}
                for _, row in g.iterrows():
                    close = float(row["close"])
                    ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
                    vwap = float(row.get("VWAP", np.nan))
                    adx = float(row.get("ADX", np.nan))
                    if np.isfinite(vwap) and close > vwap and ret >= 0.20:
                        regime = "BULL"
                    elif np.isfinite(vwap) and close < vwap and ret <= -0.20:
                        regime = "BEAR"
                    elif np.isfinite(adx) and adx >= 25:
                        regime = "TREND"
                    else:
                        regime = "NEUTRAL"
                    by_ts[pd.Timestamp(row["date"])] = {"market_ret_pct": float(ret), "regime": regime}
                ctx[str(day)] = by_ts
            return ctx
    return {}


def _bar_context(ctx: dict[str, dict[pd.Timestamp, dict]], day: str, ts: pd.Timestamp) -> tuple[float, str]:
    by_ts = ctx.get(str(day), {})
    if not by_ts:
        return 0.0, "UNKNOWN"
    if ts in by_ts:
        rec = by_ts[ts]
        return float(rec["market_ret_pct"]), str(rec["regime"])
    keys = [k for k in by_ts if k <= ts]
    if not keys:
        return 0.0, "UNKNOWN"
    rec = by_ts[max(keys)]
    return float(rec["market_ret_pct"]), str(rec["regime"])


def _quality(side: str, setup: str, row: pd.Series, rs_pct: float, market_ret: float, regime: str) -> float:
    close_loc = float(row.get("close_loc", 0.5))
    vol_ratio = float(row.get("vol_ratio", 1.0))
    vwap_dist = float(row.get("vwap_dist_atr", 0.0))
    body_pct = float(row.get("body_pct", 0.0))
    adx = float(row.get("ADX", 0.0))
    score = 20.0
    if side == "LONG":
        score += 18.0 * max(rs_pct, 0.0)
        score += 15.0 * max(close_loc - 0.45, 0.0)
        score += 5.0 if market_ret >= -0.20 else -8.0
        score += 5.0 if regime != "BEAR" else -10.0
    else:
        score += 18.0 * max(-rs_pct, 0.0)
        score += 15.0 * max(0.55 - close_loc, 0.0)
        score += 5.0 if market_ret <= 0.20 else -8.0
        score += 5.0 if regime != "BULL" else -10.0
    score += 8.0 * min(max(vol_ratio - 1.0, 0.0), 3.0)
    score += 8.0 * body_pct
    score += 0.35 * max(adx - 15.0, 0.0)
    if "FADE" in setup:
        score += 4.0 * min(abs(vwap_dist), 3.5)
    elif side == "LONG":
        score += 5.0 if vwap_dist >= -0.25 else -5.0
    else:
        score += 5.0 if vwap_dist <= 0.25 else -5.0
    return float(score)


def _candidate(ticker: str, setup: str, side: str, row: pd.Series, rs_pct: float, market_ret: float, regime: str, reason: str) -> dict:
    ts = _normalise_ts(row["date"])
    score = _quality(side, setup, row, rs_pct, market_ret, regime)
    return {
        "candidate_id": f"{ticker}|{side}|{setup}|{ts.isoformat()}",
        "scan_session": "tier123_new_setup_probe",
        "selection_mode": "tier123_probe_gate",
        "candidate_family": SETUP_TIERS.get(setup, "tier123_probe"),
        "scan_slot_ist": v11._fmt_ist(ts),
        "signal_time_ist": v11._fmt_ist(ts),
        "bar_time_ist": v11._fmt_ist(ts),
        "ticker": ticker,
        "side": side,
        "setup": setup,
        "signal_open": float(row["open"]),
        "signal_high": float(row["high"]),
        "signal_low": float(row["low"]),
        "signal_close": float(row["close"]),
        "signal_volume": float(row.get("volume", 0.0)),
        "quality_score": score,
        "ranker_score": score,
        "score": score,
        "rs_pct": float(rs_pct),
        "market_ret_pct": float(market_ret),
        "regime": regime,
        "vol_ratio": float(row.get("vol_ratio", np.nan)),
        "atr_pct": float(row.get("atr_pct", np.nan)),
        "body_pct": float(row.get("body_pct", np.nan)),
        "close_loc": float(row.get("close_loc", np.nan)),
        "vwap_dist_atr": float(row.get("vwap_dist_atr", np.nan)),
        "reason": reason,
        "status": "TIER123_PROBE_RAW",
        "created_at_ist": pd.Timestamp.now(tz="Asia/Kolkata").isoformat(),
    }


def _prev_day_levels(prepared: pd.DataFrame) -> pd.DataFrame:
    prepared = prepared.drop(columns=["prev_day_high", "prev_day_low", "prev_day_close_calc"], errors="ignore")
    daily = prepared.groupby("date_only", sort=True).agg(day_high=("high", "max"), day_low=("low", "min"), day_close=("close", "last"))
    daily["prev_day_high"] = daily["day_high"].shift(1)
    daily["prev_day_low"] = daily["day_low"].shift(1)
    daily["prev_day_close_calc"] = daily["day_close"].shift(1)
    levels = daily[["prev_day_high", "prev_day_low", "prev_day_close_calc"]]
    return prepared.join(levels, on="date_only")


def _scan_ticker(ticker: str, market_ctx: dict[str, dict[pd.Timestamp, dict]]) -> list[dict]:
    df = _read_5m(ticker)
    if df is None or df.empty:
        return []
    df = _prev_day_levels(df)
    rows: list[dict] = []
    for day, group in df.groupby("date_only", sort=True):
        g = group.reset_index(drop=True)
        if len(g) < 15:
            continue
        day_open = float(g["open"].iloc[0])
        or_high, or_low = v2._opening_range(g)
        opening_high = float(g["high"].iloc[:3].max())
        opening_low = float(g["low"].iloc[:3].min())
        first15_close = float(g["close"].iloc[min(2, len(g) - 1)])
        first15_ret = (first15_close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
        prev_day_close = float(g.get("Prev_Day_Close", pd.Series(np.nan, index=g.index)).iloc[0])
        if not np.isfinite(prev_day_close):
            prev_day_close = float(g.get("prev_day_close_calc", pd.Series(np.nan, index=g.index)).iloc[0])
        gap_pct = (day_open / prev_day_close - 1.0) * 100.0 if np.isfinite(prev_day_close) and prev_day_close > 0 else np.nan
        prev_day_high = float(g.get("prev_day_high", pd.Series(np.nan, index=g.index)).iloc[0])
        prev_day_low = float(g.get("prev_day_low", pd.Series(np.nan, index=g.index)).iloc[0])
        prior_broke_high = False
        prior_broke_low = False
        prior_broke_pdh = False
        prior_broke_pdl = False
        reclaimed_vwap_seen = False
        lost_vwap_seen = False
        washout_below_vwap_seen = False
        washout_above_vwap_seen = False

        for i in range(4, len(g) - 1):
            row = g.iloc[i]
            prev = g.iloc[i - 1]
            ts = _normalise_ts(row["date"])
            minute = ts.hour * 60 + ts.minute
            if minute < 570 or minute > 900:
                continue
            close = float(row["close"])
            open_px = float(row["open"])
            high = float(row["high"])
            low = float(row["low"])
            atr = float(row.get("ATR", np.nan))
            vwap = float(row.get("VWAP", np.nan))
            ema20 = float(row.get("EMA_20", np.nan))
            ema50 = float(row.get("EMA_50", np.nan))
            adx = float(row.get("ADX", np.nan))
            rsi = float(row.get("RSI", np.nan))
            stoch = float(row.get("Stoch_%K", np.nan))
            vol_ratio = float(row.get("vol_ratio", np.nan))
            body_pct = float(row.get("body_pct", np.nan))
            close_loc = float(row.get("close_loc", np.nan))
            lower_wick = float(row.get("lower_wick_pct", np.nan))
            upper_wick = float(row.get("upper_wick_pct", np.nan))
            vwap_dist = float(row.get("vwap_dist_atr", np.nan))
            rng = float(row.get("range", np.nan))
            market_ret, regime = _bar_context(market_ctx, str(day), ts)
            stock_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
            rs_pct = stock_ret - market_ret
            above_vwap = np.isfinite(vwap) and close > vwap
            below_vwap = np.isfinite(vwap) and close < vwap
            long_struct = close > open_px and close_loc >= 0.55
            short_struct = close < open_px and close_loc <= 0.45
            if np.isfinite(or_high) and high > or_high:
                pass
            if np.isfinite(or_low) and low < or_low:
                pass
            had_broke_high = prior_broke_high
            had_broke_low = prior_broke_low
            had_broke_pdh = prior_broke_pdh
            had_broke_pdl = prior_broke_pdl
            had_reclaimed_vwap = reclaimed_vwap_seen
            had_lost_vwap = lost_vwap_seen

            if not np.isfinite(atr) or atr <= 0 or not np.isfinite(vol_ratio) or vol_ratio < 1.0:
                if np.isfinite(or_high) and high > or_high:
                    prior_broke_high = True
                if np.isfinite(or_low) and low < or_low:
                    prior_broke_low = True
                if np.isfinite(prev_day_high) and high > prev_day_high:
                    prior_broke_pdh = True
                if np.isfinite(prev_day_low) and low < prev_day_low:
                    prior_broke_pdl = True
                if np.isfinite(vwap_dist) and vwap_dist <= -0.50:
                    washout_below_vwap_seen = True
                if np.isfinite(vwap_dist) and vwap_dist >= 0.50:
                    washout_above_vwap_seen = True
                if washout_below_vwap_seen and above_vwap:
                    reclaimed_vwap_seen = True
                if washout_above_vwap_seen and below_vwap:
                    lost_vwap_seen = True
                continue
            if close < 25 or float(row.get("day_value_so_far_rs", 0.0)) < 20_000_000:
                if np.isfinite(or_high) and high > or_high:
                    prior_broke_high = True
                if np.isfinite(or_low) and low < or_low:
                    prior_broke_low = True
                if np.isfinite(prev_day_high) and high > prev_day_high:
                    prior_broke_pdh = True
                if np.isfinite(prev_day_low) and low < prev_day_low:
                    prior_broke_pdl = True
                if np.isfinite(vwap_dist) and vwap_dist <= -0.50:
                    washout_below_vwap_seen = True
                if np.isfinite(vwap_dist) and vwap_dist >= 0.50:
                    washout_above_vwap_seen = True
                if washout_below_vwap_seen and above_vwap:
                    reclaimed_vwap_seen = True
                if washout_above_vwap_seen and below_vwap:
                    lost_vwap_seen = True
                continue
            if np.isfinite(rng) and rng > 3.5 * atr:
                if np.isfinite(or_high) and high > or_high:
                    prior_broke_high = True
                if np.isfinite(or_low) and low < or_low:
                    prior_broke_low = True
                if np.isfinite(prev_day_high) and high > prev_day_high:
                    prior_broke_pdh = True
                if np.isfinite(prev_day_low) and low < prev_day_low:
                    prior_broke_pdl = True
                if np.isfinite(vwap_dist) and vwap_dist <= -0.50:
                    washout_below_vwap_seen = True
                if np.isfinite(vwap_dist) and vwap_dist >= 0.50:
                    washout_above_vwap_seen = True
                if washout_below_vwap_seen and above_vwap:
                    reclaimed_vwap_seen = True
                if washout_above_vwap_seen and below_vwap:
                    lost_vwap_seen = True
                continue

            # Tier 1: OR retest and OR fakeout.
            if minute <= 630 and had_broke_high and long_struct and above_vwap and low <= or_high + 0.45 * atr and close > or_high and rs_pct >= 0.0 and regime != "BEAR" and abs(vwap_dist) <= 2.5:
                rows.append(_candidate(ticker, "E_ORB_RETEST_HOLD_LONG", "LONG", row, rs_pct, market_ret, regime, "orb_break_retest_hold_long"))
            if minute <= 630 and had_broke_low and short_struct and below_vwap and high >= or_low - 0.45 * atr and close < or_low and rs_pct <= 0.0 and regime != "BULL" and abs(vwap_dist) <= 2.5:
                rows.append(_candidate(ticker, "E_ORB_RETEST_HOLD_SHORT", "SHORT", row, rs_pct, market_ret, regime, "orb_break_retest_hold_short"))
            if minute <= 630 and high > or_high and close < or_high and (below_vwap or close_loc <= 0.35) and upper_wick >= 0.30 and rs_pct <= 0.10 and regime != "BULL":
                rows.append(_candidate(ticker, "E_FAILED_OR_BREAKOUT_TRAP_SHORT", "SHORT", row, rs_pct, market_ret, regime, "failed_or_breakout_trap_short"))
            if minute <= 630 and low < or_low and close > or_low and (above_vwap or close_loc >= 0.65) and lower_wick >= 0.30 and rs_pct >= -0.10 and regime != "BEAR":
                rows.append(_candidate(ticker, "E_FAILED_OR_BREAKDOWN_TRAP_LONG", "LONG", row, rs_pct, market_ret, regime, "failed_or_breakdown_trap_long"))

            # Tier 1: structured VWAP reclaim/rejection after washout.
            if minute <= 690 and had_reclaimed_vwap and long_struct and low >= vwap - 0.35 * atr and low <= vwap + 0.45 * atr and close > max(float(prev["high"]), vwap) and vol_ratio >= 1.25 and rs_pct >= 0.0 and regime != "BEAR" and vwap_dist <= 1.8:
                rows.append(_candidate(ticker, "V_RECLAIM_PULLBACK_LONG", "LONG", row, rs_pct, market_ret, regime, "vwap_reclaim_first_pullback_hold_long"))
            if minute <= 690 and had_lost_vwap and short_struct and high <= vwap + 0.35 * atr and high >= vwap - 0.45 * atr and close < min(float(prev["low"]), vwap) and vol_ratio >= 1.25 and rs_pct <= 0.0 and regime != "BULL" and vwap_dist >= -1.8:
                rows.append(_candidate(ticker, "V_REJECTION_PULLBACK_SHORT", "SHORT", row, rs_pct, market_ret, regime, "vwap_rejection_first_pullback_short"))

            # Tier 1: first pullback after momentum expansion.
            recent = g.iloc[max(0, i - 4):i]
            expansion = recent.loc[
                (recent["range"] >= 1.40 * recent["range"].shift(1).rolling(20, min_periods=5).mean())
                | (recent["range"] >= 1.40 * recent["ATR"])
            ].copy()
            if not expansion.empty:
                exp = expansion.iloc[-1]
                exp_mid = (float(exp["high"]) + float(exp["low"])) / 2.0
                exp_long = float(exp["close"]) > float(exp["open"]) and float(exp.get("close_loc", 0.5)) >= 0.70 and float(exp.get("vol_ratio", 0.0)) >= 1.8
                exp_short = float(exp["close"]) < float(exp["open"]) and float(exp.get("close_loc", 0.5)) <= 0.30 and float(exp.get("vol_ratio", 0.0)) >= 1.8
                if minute <= 840 and exp_long and long_struct and close >= exp_mid and (not np.isfinite(ema20) or low >= ema20 - 0.25 * atr) and above_vwap and rs_pct >= 0.10 and regime != "BEAR":
                    rows.append(_candidate(ticker, "M_EXPANSION_FIRST_PULLBACK_LONG", "LONG", row, rs_pct, market_ret, regime, "momentum_expansion_first_pullback_long"))
                if minute <= 840 and exp_short and short_struct and close <= exp_mid and (not np.isfinite(ema20) or high <= ema20 + 0.25 * atr) and below_vwap and rs_pct <= -0.10 and regime != "BULL":
                    rows.append(_candidate(ticker, "M_EXPANSION_FIRST_PULLBACK_SHORT", "SHORT", row, rs_pct, market_ret, regime, "momentum_expansion_first_pullback_short"))

            # Tier 1: late-morning compression break.
            if 645 <= minute <= 750 and i >= 8:
                comp = g.iloc[i - 7:i]
                comp_width = float(comp["high"].max() - comp["low"].min())
                comp_high = float(comp["high"].max())
                comp_low = float(comp["low"].min())
                if comp_width <= 1.00 * atr and vol_ratio >= 1.25:
                    if long_struct and above_vwap and close > comp_high and rs_pct >= 0.05 and regime != "BEAR":
                        rows.append(_candidate(ticker, "C_LATE_MORNING_COMPRESSION_BREAK_LONG", "LONG", row, rs_pct, market_ret, regime, "late_morning_compression_break_long"))
                    if short_struct and below_vwap and close < comp_low and rs_pct <= -0.05 and regime != "BULL":
                        rows.append(_candidate(ticker, "C_LATE_MORNING_COMPRESSION_BREAK_SHORT", "SHORT", row, rs_pct, market_ret, regime, "late_morning_compression_break_short"))

            # Tier 2: controlled gap continuation.
            if minute <= 660 and np.isfinite(gap_pct) and 0.50 <= gap_pct <= 2.50 and opening_low >= prev_day_close * 0.997 and long_struct and above_vwap and close > or_high and rs_pct >= 0.15 and regime != "BEAR":
                rows.append(_candidate(ticker, "G_GAP_HOLD_CONTINUATION_LONG", "LONG", row, rs_pct, market_ret, regime, "gap_up_hold_continuation_long"))
            if minute <= 660 and np.isfinite(gap_pct) and -2.50 <= gap_pct <= -0.50 and opening_high <= prev_day_close * 1.003 and short_struct and below_vwap and close < or_low and rs_pct <= -0.15 and regime != "BULL":
                rows.append(_candidate(ticker, "G_GAP_HOLD_CONTINUATION_SHORT", "SHORT", row, rs_pct, market_ret, regime, "gap_down_hold_continuation_short"))

            # Tier 2: high-volume narrow-range absorption then break.
            if i >= 1:
                ab = prev
                ab_range = float(ab.get("range", np.nan))
                ab_atr = float(ab.get("ATR", np.nan))
                ab_vol = float(ab.get("vol_ratio", np.nan))
                ab_close_loc = float(ab.get("close_loc", np.nan))
                if np.isfinite(ab_range) and np.isfinite(ab_atr) and ab_atr > 0 and np.isfinite(ab_vol) and ab_vol >= 1.8 and ab_range <= 0.85 * ab_atr:
                    near_vwap = np.isfinite(vwap) and abs(float(ab["close"]) - vwap) <= 0.75 * atr
                    if near_vwap and close > float(ab["high"]) and long_struct and ab_close_loc >= 0.50 and rs_pct >= 0.0 and regime != "BEAR":
                        rows.append(_candidate(ticker, "A_HVN_ABSORPTION_BREAK_LONG", "LONG", row, rs_pct, market_ret, regime, "high_volume_narrow_range_absorption_break_long"))
                    if near_vwap and close < float(ab["low"]) and short_struct and ab_close_loc <= 0.50 and rs_pct <= 0.0 and regime != "BULL":
                        rows.append(_candidate(ticker, "A_HVN_ABSORPTION_BREAK_SHORT", "SHORT", row, rs_pct, market_ret, regime, "high_volume_narrow_range_absorption_break_short"))

            # Tier 2: trend-day EMA stair continuation.
            ema20_slope = float(g["EMA_20"].iloc[i] - g["EMA_20"].iloc[max(0, i - 3)]) if np.isfinite(ema20) else np.nan
            if np.isfinite(ema20) and np.isfinite(ema50) and np.isfinite(ema20_slope) and minute <= 840:
                if regime in {"BULL", "TREND"} and ema20_slope > 0 and close > ema20 >= ema50 and low <= ema20 + 0.25 * atr and long_struct and above_vwap and rs_pct >= 0.05:
                    rows.append(_candidate(ticker, "T_TREND_DAY_EMA_STAIR_LONG", "LONG", row, rs_pct, market_ret, regime, "trend_day_ema20_stair_long"))
                if regime in {"BEAR", "TREND"} and ema20_slope < 0 and close < ema20 <= ema50 and high >= ema20 - 0.25 * atr and short_struct and below_vwap and rs_pct <= -0.05:
                    rows.append(_candidate(ticker, "T_TREND_DAY_EMA_STAIR_SHORT", "SHORT", row, rs_pct, market_ret, regime, "trend_day_ema20_stair_short"))

            # Tier 3: previous-day level retest.
            if np.isfinite(prev_day_high) and had_broke_pdh and long_struct and low <= prev_day_high + 0.35 * atr and close > prev_day_high and above_vwap and rs_pct >= 0.0 and regime != "BEAR":
                rows.append(_candidate(ticker, "P_PDH_BREAK_RETEST_LONG", "LONG", row, rs_pct, market_ret, regime, "previous_day_high_break_retest_long"))
            if np.isfinite(prev_day_low) and had_broke_pdl and short_struct and high >= prev_day_low - 0.35 * atr and close < prev_day_low and below_vwap and rs_pct <= 0.0 and regime != "BULL":
                rows.append(_candidate(ticker, "P_PDL_BREAK_RETEST_SHORT", "SHORT", row, rs_pct, market_ret, regime, "previous_day_low_break_retest_short"))

            # Tier 3: controlled VWAP extreme fade only in non-trend regimes.
            controlled = regime in {"NEUTRAL", "UNKNOWN"} or (np.isfinite(adx) and adx <= 22)
            if minute >= 660 and controlled and vwap_dist <= -2.0 and lower_wick >= 0.35 and close > float(prev["high"]) and (not np.isfinite(rsi) or rsi <= 38) and (not np.isfinite(stoch) or stoch <= 35):
                rows.append(_candidate(ticker, "MR_CONTROLLED_VWAP_EXTREME_FADE_LONG", "LONG", row, rs_pct, market_ret, regime, "controlled_vwap_extreme_fade_long"))
            if minute >= 660 and controlled and vwap_dist >= 2.0 and upper_wick >= 0.35 and close < float(prev["low"]) and (not np.isfinite(rsi) or rsi >= 62) and (not np.isfinite(stoch) or stoch >= 65):
                rows.append(_candidate(ticker, "MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT", "SHORT", row, rs_pct, market_ret, regime, "controlled_vwap_extreme_fade_short"))

            if np.isfinite(or_high) and high > or_high:
                prior_broke_high = True
            if np.isfinite(or_low) and low < or_low:
                prior_broke_low = True
            if np.isfinite(prev_day_high) and high > prev_day_high:
                prior_broke_pdh = True
            if np.isfinite(prev_day_low) and low < prev_day_low:
                prior_broke_pdl = True
            if np.isfinite(vwap_dist) and vwap_dist <= -0.50:
                washout_below_vwap_seen = True
            if np.isfinite(vwap_dist) and vwap_dist >= 0.50:
                washout_above_vwap_seen = True
            if washout_below_vwap_seen and above_vwap:
                reclaimed_vwap_seen = True
            if washout_above_vwap_seen and below_vwap:
                lost_vwap_seen = True

    return rows


def _init_scan_worker(market_ctx: dict[str, dict[pd.Timestamp, dict]]) -> None:
    global _WORKER_MARKET_CTX
    _WORKER_MARKET_CTX = market_ctx


def _scan_ticker_job(ticker: str) -> tuple[str, list[dict], str | None, float]:
    t0 = time.time()
    try:
        return ticker, _scan_ticker(ticker, _WORKER_MARKET_CTX), None, time.time() - t0
    except Exception as exc:
        return ticker, [], repr(exc), time.time() - t0


def _probe_gate(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return raw
    work = raw.copy()
    work["_day"] = pd.to_datetime(work["signal_time_ist"], errors="coerce").dt.strftime("%Y-%m-%d")
    work["_score"] = pd.to_numeric(work["quality_score"], errors="coerce").fillna(0.0)
    work = (
        work.sort_values(["setup", "ticker", "_day", "_score"], ascending=[True, True, True, False])
        .drop_duplicates(subset=["setup", "ticker", "_day"], keep="first")
    )
    frames = []
    for _, group in work.groupby("setup", sort=True):
        frames.append(group.sort_values("_score", ascending=False).head(MAX_RAW_PER_SETUP))
    out = pd.concat(frames, ignore_index=True) if frames else work.iloc[0:0].copy()
    return out.drop(columns=["_day", "_score"], errors="ignore").reset_index(drop=True)


def _load_probe_universe() -> list[str]:
    if UNIVERSE_MODE in {"futures", "fo", "nfo"}:
        try:
            from filtered_stocks_NSE_futures_only import selected_stocks as futures_stocks

            return sorted({str(x).upper().replace(".NS", "").strip() for x in futures_stocks if str(x).strip()})
        except Exception as exc:
            print(f"[universe] futures import failed: {exc}; falling back to configs/universe_fo.csv", flush=True)

    if UNIVERSE_MODE in {"fo_csv", "liquid"}:
        path = Path("configs/universe_fo.csv")
        if path.exists():
            df = pd.read_csv(path)
            col = "ticker" if "ticker" in df.columns else df.columns[0]
            return sorted({str(x).upper().replace(".NS", "").strip() for x in df[col].dropna() if str(x).strip()})
        print(f"[universe] {path} not found; falling back to full v2 universe", flush=True)

    if UNIVERSE_MODE == "all":
        return sorted({str(x).upper().replace(".NS", "").strip() for x in v2._load_universe() if str(x).strip()})

    print(f"[universe] unknown EQIDV2_TIER123_UNIVERSE={UNIVERSE_MODE!r}; falling back to futures", flush=True)
    try:
        from filtered_stocks_NSE_futures_only import selected_stocks as futures_stocks

        return sorted({str(x).upper().replace(".NS", "").strip() for x in futures_stocks if str(x).strip()})
    except Exception:
        return sorted({str(x).upper().replace(".NS", "").strip() for x in v2._load_universe() if str(x).strip()})


def _scan_all() -> pd.DataFrame:
    market_ctx = _market_context()
    raw_universe = _load_probe_universe()
    full_universe = [
        x
        for x in raw_universe
        if not str(x).upper().startswith("NIFTY")
        and not str(x).upper().endswith("BEES")
        and (DATA_ROOT / f"{str(x).upper()}_stocks_indicators_5min.parquet").exists()
    ]
    end_index = min(SCAN_END_INDEX or len(full_universe), len(full_universe))
    universe = full_universe[SCAN_START_INDEX - 1 : end_index]
    suffix = f"_{SCAN_TAG}" if SCAN_TAG else ""
    pd.DataFrame(
        {
            "ticker": full_universe,
            "universe_mode": UNIVERSE_MODE,
            "selected_for_this_run": [ticker in set(universe) for ticker in full_universe],
            "scan_start_index": SCAN_START_INDEX,
            "scan_end_index": end_index,
        }
    ).to_csv(OUT_DIR / f"tier123_universe{suffix}.csv", index=False)
    workers = min(SCAN_WORKERS, max(len(universe), 1))
    print(
        f"[scan] universe_mode={UNIVERSE_MODE} universe_slice={len(universe):,}/{len(full_universe):,} "
        f"index={SCAN_START_INDEX}-{end_index} workers={workers} data_root={DATA_ROOT}",
        flush=True,
    )
    rows: list[dict] = []
    skipped_rows: list[dict] = []
    partial_path = OUT_DIR / f"tier123_raw_candidates_partial{suffix}.csv"
    skipped_path = OUT_DIR / f"tier123_scan_skipped_symbols{suffix}.csv"
    t0 = time.time()
    if workers <= 1:
        for i, ticker in enumerate(universe, 1):
            ticker_rows: list[dict] = []
            t1 = time.time()
            try:
                ticker_rows = _scan_ticker(str(ticker).upper(), market_ctx)
                rows.extend(ticker_rows)
            except Exception as exc:
                skipped_rows.append({"ticker": ticker, "status": "scan_exception", "reason": repr(exc)})
                print(f"  [skip {i:4d}/{len(universe)}] {ticker}: {exc!r}", flush=True)
            finally:
                ticker_count = len(ticker_rows)
                del ticker_rows
                gc.collect()
            if i % 5 == 0 or i == len(universe):
                pd.DataFrame(rows).to_csv(partial_path, index=False)
                pd.DataFrame(skipped_rows).to_csv(skipped_path, index=False)
                print(
                    f"  [scan {i:4d}/{len(universe)}] last={str(ticker).upper()} rows={ticker_count:,} raw_rows={len(rows):,} ticker_sec={time.time() - t1:.1f} elapsed={time.time() - t0:.1f}s",
                    flush=True,
                )
    else:
        with ProcessPoolExecutor(max_workers=workers, initializer=_init_scan_worker, initargs=(market_ctx,)) as pool:
            future_to_ticker = {pool.submit(_scan_ticker_job, str(ticker).upper()): str(ticker).upper() for ticker in universe}
            for i, future in enumerate(as_completed(future_to_ticker), 1):
                ticker = future_to_ticker[future]
                ticker_done, ticker_rows, err, ticker_sec = future.result()
                if err:
                    skipped_rows.append({"ticker": ticker_done, "status": "scan_exception", "reason": err})
                    print(f"  [skip {i:4d}/{len(universe)}] {ticker_done}: {err}", flush=True)
                else:
                    rows.extend(ticker_rows)
                ticker_count = len(ticker_rows)
                del ticker_rows
                if i % 5 == 0 or i == len(universe) or err:
                    pd.DataFrame(rows).to_csv(partial_path, index=False)
                    pd.DataFrame(skipped_rows).to_csv(skipped_path, index=False)
                    print(
                        f"  [scan {i:4d}/{len(universe)}] last={ticker} rows={ticker_count:,} raw_rows={len(rows):,} ticker_sec={ticker_sec:.1f} elapsed={time.time() - t0:.1f}s",
                        flush=True,
                    )
    raw = pd.DataFrame(rows)
    if raw.empty:
        return raw
    return _probe_gate(raw)


def _resolve_candidates(candidates: pd.DataFrame, label: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if candidates.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    signals, raw_entries, rejects = v11._build_v7_entry_engine_signals(candidates)
    if signals.empty:
        return pd.DataFrame(), raw_entries, rejects
    trades = v11._resolve_v7_entry_engine_signals(
        signals,
        label=label,
        entry_fill_model="ltp_on_signal_1m_open",
        selected_strategy_profile="none",
    )
    return trades, raw_entries, rejects


def _prepare_trades(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    out["date"] = pd.to_datetime(out["trade_date"])
    out["pnl"] = pd.to_numeric(out["v6_net_pnl_rs"], errors="coerce").fillna(0.0)
    return out


def _decision_row(setup: str, frame: pd.DataFrame) -> dict:
    stats = _stats_by_split(frame)
    row = {"setup": setup, "tier": SETUP_TIERS.get(setup, "")}
    for split, metric in stats.items():
        for key, value in metric.items():
            row[f"{split}_{key}"] = value
    row["train_pf_pass"] = bool(row["train_trades"] >= 15 and _pf_score(row["train_pf"]) > 1.5)
    row["validation_ok"] = bool(row["valid_trades"] >= 5 and row["valid_pnl"] > 0 and _pf_score(row["valid_pf"]) >= 1.0)
    row["holdout_read_ok"] = bool(row["holdout_pnl"] > 0 and _pf_score(row["holdout_pf"]) >= 1.0)
    row["full_pf_pass"] = bool(_pf_score(row["full_pf"]) >= 1.3 and row["full_pnl"] > 0)
    if row["train_pf_pass"] and row["validation_ok"] and row["holdout_read_ok"] and _pf_score(row["full_pf"]) >= 1.5:
        decision = "promising_probation"
    elif row["train_pf_pass"] and row["validation_ok"]:
        decision = "train_valid_pass_holdout_or_full_weak"
    elif row["train_pf_pass"]:
        decision = "train_only_pass_reject_for_now"
    else:
        decision = "reject_train_pf_or_count"
    row["decision"] = decision
    return row


def _scenario_rows(label: str, frame: pd.DataFrame) -> list[dict]:
    rows = []
    for split, stats in _stats_by_split(frame).items():
        row = {"label": label, "split": split}
        row.update(stats)
        rows.append(row)
    return rows


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    v11.v6.SETUP_EXIT_RULES.update(PROBE_EXIT_RULES)

    suffix = f"_{SCAN_TAG}" if SCAN_TAG else ""
    raw_path = OUT_DIR / f"tier123_raw_candidates{suffix}.csv" if SCAN_ONLY else OUT_DIR / "tier123_raw_candidates.csv"
    if raw_path.exists():
        candidates = pd.read_csv(raw_path)
        print(f"[load] cached candidates={len(candidates):,}", flush=True)
    elif COMBINE_CHUNKS:
        chunk_paths = sorted(OUT_DIR.glob(CHUNK_GLOB))
        if not chunk_paths:
            raise SystemExit(f"[tier123] no chunk files matched {CHUNK_GLOB!r}")
        frames = [pd.read_csv(path) for path in chunk_paths]
        candidates = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
        candidates = _probe_gate(candidates)
        candidates.to_csv(raw_path, index=False)
        print(f"[combine] chunks={len(chunk_paths):,} candidates={len(candidates):,} -> {raw_path}", flush=True)
    else:
        candidates = _scan_all()
        candidates.to_csv(raw_path, index=False)
        print(f"[scan] wrote {raw_path} rows={len(candidates):,}", flush=True)

    if SCAN_ONLY:
        candidates.groupby("setup").size().reset_index(name="raw_candidates").to_csv(
            OUT_DIR / f"tier123_raw_counts_by_setup{suffix}.csv",
            index=False,
        )
        print(f"[scan-only] wrote {raw_path}", flush=True)
        return

    pd.DataFrame(SECTOR_BLOCKED_ROWS).to_csv(OUT_DIR / "tier123_blocked_setups.csv", index=False)
    if candidates.empty:
        raise SystemExit("[tier123] no candidates generated")

    candidates.groupby("setup").size().reset_index(name="raw_candidates").to_csv(
        OUT_DIR / "tier123_raw_counts_by_setup.csv",
        index=False,
    )

    all_trades, all_raw_entries, all_rejects = _resolve_candidates(candidates, "tier123_all_probe")
    all_trades = _prepare_trades(all_trades)
    all_trades.to_csv(OUT_DIR / "tier123_all_probe_trades.csv", index=False)
    all_raw_entries.to_csv(OUT_DIR / "tier123_all_probe_entry_engine_raw_entries.csv", index=False)
    all_rejects.to_csv(OUT_DIR / "tier123_all_probe_entry_engine_rejects.csv", index=False)

    standalone_frames = []
    standalone_rows = []
    for setup, group in candidates.groupby("setup", sort=True):
        print(f"[standalone] {setup} raw={len(group):,}", flush=True)
        trades, _, _ = _resolve_candidates(group.copy(), f"tier123_standalone_{setup}")
        trades = _prepare_trades(trades)
        if not trades.empty:
            standalone_frames.append(trades)
            standalone_rows.extend(_flatten_stats("standalone", setup, trades))
        else:
            standalone_rows.extend(_flatten_stats("standalone", setup, trades))
    standalone_all = pd.concat(standalone_frames, ignore_index=True) if standalone_frames else pd.DataFrame()
    standalone_all.to_csv(OUT_DIR / "tier123_standalone_trades.csv", index=False)
    pd.DataFrame(standalone_rows).to_csv(OUT_DIR / "tier123_standalone_by_setup_split.csv", index=False)

    decision_rows = []
    if not standalone_all.empty:
        for setup, group in standalone_all.groupby("setup", sort=True):
            decision_rows.append(_decision_row(setup, group.copy()))
    for blocked in SECTOR_BLOCKED_ROWS:
        decision_rows.append({
            "setup": blocked["setup"],
            "tier": blocked["tier"],
            "decision": blocked["status"],
            "blocked_reason": blocked["reason"],
        })
    decisions = pd.DataFrame(decision_rows)
    decisions.to_csv(OUT_DIR / "tier123_decisions_by_setup.csv", index=False)

    scenario_rows = []
    if BASE_TRADES_CSV.exists():
        base_pool = pd.read_csv(BASE_TRADES_CSV)
        base_selected, _, _ = v11._apply_selected_strategy_profile(base_pool, BASE_PROFILE)
        base_selected = _prepare_trades(base_selected)
        scenario_rows.extend(_scenario_rows("base_current_residual_overlay_profile", base_selected))
        if not standalone_all.empty:
            current_keys = set(base_selected["ticker"].astype(str) + "|" + base_selected["trade_date"].astype(str))
            new_non_overlap = standalone_all.loc[
                ~(standalone_all["ticker"].astype(str) + "|" + standalone_all["trade_date"].astype(str)).isin(current_keys)
            ].copy()
            scenario_rows.extend(_scenario_rows("tier123_standalone_all", standalone_all))
            scenario_rows.extend(_scenario_rows("tier123_non_overlap_vs_current", new_non_overlap))
            scenario_rows.extend(_scenario_rows("current_plus_tier123_non_overlap", pd.concat([base_selected, new_non_overlap], ignore_index=True)))
            pass_setups = decisions.loc[decisions["decision"].astype(str).eq("promising_probation"), "setup"].astype(str).tolist()
            pass_non_overlap = new_non_overlap.loc[new_non_overlap["setup"].astype(str).isin(pass_setups)].copy()
            scenario_rows.extend(_scenario_rows("tier123_promising_non_overlap", pass_non_overlap))
            scenario_rows.extend(_scenario_rows("current_plus_tier123_promising_non_overlap", pd.concat([base_selected, pass_non_overlap], ignore_index=True)))
    pd.DataFrame(scenario_rows).to_csv(OUT_DIR / "tier123_portfolio_scenarios.csv", index=False)

    setup_exit_rows = [{"setup": setup, "sl_pct": sl, "target_pct": tgt, "tier": SETUP_TIERS.get(setup, "")} for setup, (sl, tgt) in sorted(PROBE_EXIT_RULES.items())]
    pd.DataFrame(setup_exit_rows).to_csv(OUT_DIR / "tier123_probe_exit_rules.csv", index=False)

    print(f"[done] wrote {OUT_DIR}", flush=True)
    if not decisions.empty:
        cols = [c for c in ["setup", "tier", "full_trades", "train_trades", "train_pf", "valid_trades", "valid_pf", "holdout_trades", "holdout_pf", "full_pf", "decision"] if c in decisions.columns]
        print(decisions[cols].to_string(index=False, max_colwidth=80), flush=True)


if __name__ == "__main__":
    main()
