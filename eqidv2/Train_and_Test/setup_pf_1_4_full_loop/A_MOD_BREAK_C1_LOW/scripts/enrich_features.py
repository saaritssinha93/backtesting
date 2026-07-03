r"""enrich_features.py — join CAUSAL point-in-time 5-minute indicator/context features
onto every A_MOD_BREAK_C1_LOW pool row, widening the search dictionary beyond the 11
columns the scanner exports.

RESEARCH-ONLY. Writes ONLY pools/A_MOD_BREAK_C1_LOW/enriched_features.csv.

Causality: every feature uses bars <= the signal bar (shift-based slopes, session-scoped
cumulatives). Session VWAP uses the corrected per-session cumsum convention. Gap uses the
prior session's last close (known at open). Nothing reads past the signal bar.

Sources:
  - 5-min OHLCV+RSI/ATR/EMA20/EMA50/ADX: C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2
    (RSI/ATR/EMA/ADX reused where present ~99%; MACD/BB/CCI/MFI/OBV/Stoch/VWAP recomputed
    from OHLCV because their column coverage is only ~61% in the campaign window).

Run from repo root:
  py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_LOW\scripts\enrich_features.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
WORK = _HERE.parent
SETUP = "A_MOD_BREAK_C1_LOW"
POOL_CSV = WORK / "pools" / SETUP / "historical_all_available_pre_dedupe_live_candidates.csv"
OUT_CSV = WORK / "pools" / SETUP / "enriched_features.csv"
DATA_ROOT = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")

ENRICHED_FEATS = [
    # indicator group
    "rsi", "rsi_slope3", "adx5", "adx_slope3",
    "ema20_dist_atr", "ema50_dist_atr", "ema20_slope_atr", "ema_stack_atr",
    "macd_hist_atr", "macd_hist_slope3",
    "bb_pos", "bb_width_atr", "stoch_k", "stoch_kd",
    "cci20", "mfi14", "obv_slope6", "vol_z",
    # session / day context
    "sess_vwap_dist_atr", "below_vwap_streak6",
    "day_pos", "day_low_dist_atr", "day_high_dist_atr",
    "bars_since_day_low", "bars_since_day_high",
    "gap_pct", "day_ret_pct", "c1_range_atr", "c1_break_depth_atr",
    # price action
    "ret3_atr", "ret6_atr", "ret12_atr", "red_streak",
    "body_sum6_atr", "range6_atr", "range_expansion",
]


def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()


def compute_ticker_features(df: pd.DataFrame) -> pd.DataFrame:
    """df: one ticker's 5-min bars sorted by date. Returns df indexed by date with features."""
    df = df.sort_values("date").reset_index(drop=True)
    o, h, l, c, v = (pd.to_numeric(df[k], errors="coerce") for k in ("open", "high", "low", "close", "volume"))
    dt = pd.to_datetime(df["date"])
    day = dt.dt.normalize()

    atr = pd.to_numeric(df.get("ATR"), errors="coerce")
    tr = pd.concat([(h - l), (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    atr = atr.fillna(tr.rolling(14, min_periods=5).mean())
    atr_safe = atr.replace(0, np.nan)

    out = pd.DataFrame({"date": df["date"]})   # keep tz-aware dtype (.values would coerce to UTC)
    rsi = pd.to_numeric(df.get("RSI"), errors="coerce")
    out["rsi"] = rsi
    out["rsi_slope3"] = rsi - rsi.shift(3)
    adx = pd.to_numeric(df.get("ADX"), errors="coerce")
    out["adx5"] = adx
    out["adx_slope3"] = adx - adx.shift(3)

    ema20 = pd.to_numeric(df.get("EMA_20"), errors="coerce").fillna(_ema(c, 20))
    ema50 = pd.to_numeric(df.get("EMA_50"), errors="coerce").fillna(_ema(c, 50))
    out["ema20_dist_atr"] = (c - ema20) / atr_safe
    out["ema50_dist_atr"] = (c - ema50) / atr_safe
    out["ema20_slope_atr"] = (ema20 - ema20.shift(3)) / atr_safe
    out["ema_stack_atr"] = (ema20 - ema50) / atr_safe

    macd = _ema(c, 12) - _ema(c, 26)
    hist = macd - _ema(macd, 9)
    out["macd_hist_atr"] = hist / atr_safe
    out["macd_hist_slope3"] = (hist - hist.shift(3)) / atr_safe

    mid20 = c.rolling(20, min_periods=10).mean()
    std20 = c.rolling(20, min_periods=10).std()
    out["bb_pos"] = (c - mid20) / (2 * std20.replace(0, np.nan))
    out["bb_width_atr"] = (4 * std20) / atr_safe

    ll14 = l.rolling(14, min_periods=7).min()
    hh14 = h.rolling(14, min_periods=7).max()
    k = 100 * (c - ll14) / (hh14 - ll14).replace(0, np.nan)
    out["stoch_k"] = k
    out["stoch_kd"] = k - k.rolling(3, min_periods=1).mean()

    tp = (h + l + c) / 3
    tp_ma = tp.rolling(20, min_periods=10).mean()
    tp_md = (tp - tp_ma).abs().rolling(20, min_periods=10).mean()
    out["cci20"] = (tp - tp_ma) / (0.015 * tp_md.replace(0, np.nan))

    mf = tp * v
    pos_mf = mf.where(tp > tp.shift(1), 0.0).rolling(14, min_periods=7).sum()
    neg_mf = mf.where(tp < tp.shift(1), 0.0).rolling(14, min_periods=7).sum()
    out["mfi14"] = 100 - 100 / (1 + pos_mf / neg_mf.replace(0, np.nan))

    obv = (np.sign(c.diff().fillna(0)) * v).cumsum()
    vol20 = v.rolling(20, min_periods=10).mean()
    out["obv_slope6"] = (obv - obv.shift(6)) / (vol20.replace(0, np.nan) * 6)
    out["vol_z"] = (v - vol20) / v.rolling(20, min_periods=10).std().replace(0, np.nan)

    # ---- session-scoped context ----
    g = day
    first_open = o.groupby(g).transform("first")
    day_high = h.groupby(g).cummax()
    day_low = l.groupby(g).cummin()
    rng = (day_high - day_low).replace(0, np.nan)
    out["day_pos"] = (c - day_low) / rng
    out["day_low_dist_atr"] = (c - day_low) / atr_safe
    out["day_high_dist_atr"] = (day_high - c) / atr_safe
    bar_no = g.groupby(g).cumcount()
    is_low = (l <= day_low + 1e-9)
    is_high = (h >= day_high - 1e-9)
    low_bar = pd.Series(np.where(is_low, bar_no, np.nan)).groupby(g).ffill()
    high_bar = pd.Series(np.where(is_high, bar_no, np.nan)).groupby(g).ffill()
    out["bars_since_day_low"] = bar_no - low_bar
    out["bars_since_day_high"] = bar_no - high_bar

    # prev-session close: map each session -> previous session's final close
    sess_last = c.groupby(g).last()
    prev_map = sess_last.shift(1)
    out["gap_pct"] = (first_open - g.map(prev_map)) / g.map(prev_map) * 100.0
    out["day_ret_pct"] = (c - first_open) / first_open * 100.0

    c1_high = h.groupby(g).transform("first")
    c1_low = l.groupby(g).transform("first")
    out["c1_range_atr"] = (c1_high - c1_low) / atr_safe
    out["c1_break_depth_atr"] = (c1_low - c) / atr_safe

    tpv = tp * v
    cum_tpv = tpv.groupby(g).cumsum()
    cum_v = v.groupby(g).cumsum().replace(0, np.nan)
    svwap = cum_tpv / cum_v
    out["sess_vwap_dist_atr"] = (c - svwap) / atr_safe
    below = (c < svwap).astype(int)
    out["below_vwap_streak6"] = below.rolling(6, min_periods=1).sum()

    # ---- price action ----
    out["ret3_atr"] = (c - c.shift(3)) / atr_safe
    out["ret6_atr"] = (c - c.shift(6)) / atr_safe
    out["ret12_atr"] = (c - c.shift(12)) / atr_safe
    dn = (c < o).astype(int)
    out["red_streak"] = dn * (dn.groupby((dn != dn.shift()).cumsum()).cumcount() + 1)
    out["body_sum6_atr"] = (c - o).rolling(6, min_periods=3).sum() / atr_safe
    out["range6_atr"] = (h.rolling(6, min_periods=3).max() - l.rolling(6, min_periods=3).min()) / atr_safe
    mean_rng6 = (h - l).shift(1).rolling(6, min_periods=3).mean()
    out["range_expansion"] = (h - l) / mean_rng6.replace(0, np.nan)

    return out


def main() -> int:
    pool = pd.read_csv(POOL_CSV, usecols=["ticker", "signal_time_ist"], low_memory=False)
    sig = pd.to_datetime(pool["signal_time_ist"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata")
    pool["_sig"] = sig.dt.tz_localize(None)
    tickers = sorted(pool["ticker"].astype(str).str.upper().unique())
    print(f"[enrich] {len(pool)} rows / {len(tickers)} tickers")

    frames = []
    t0 = time.time()
    missing = 0
    for i, tk in enumerate(tickers, 1):
        p = DATA_ROOT / f"{tk}_stocks_indicators_5min.parquet"
        if not p.exists():
            missing += 1
            continue
        want = pool.loc[pool["ticker"].str.upper() == tk, "_sig"]
        try:
            df = pd.read_parquet(p, columns=["date", "open", "high", "low", "close", "volume",
                                             "RSI", "ATR", "EMA_20", "EMA_50", "ADX"])
        except Exception:
            df = pd.read_parquet(p)
        feats = compute_ticker_features(df)
        fdt = pd.to_datetime(feats["date"])
        if fdt.dt.tz is not None:
            fdt = fdt.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
        feats["_sig"] = fdt
        sel = feats[feats["_sig"].isin(set(want))].copy()
        if sel.empty:
            continue
        sel["ticker"] = tk
        frames.append(sel.drop(columns=["date"]))
        if i % 200 == 0:
            print(f"[enrich] {i}/{len(tickers)} tickers, {sum(len(f) for f in frames)} rows, "
                  f"{time.time()-t0:.0f}s", flush=True)
    out = pd.concat(frames, ignore_index=True)
    out.to_csv(OUT_CSV, index=False)
    match = len(pool.merge(out, on=["ticker", "_sig"], how="inner"))
    print(f"[enrich] wrote {len(out)} feature rows -> {OUT_CSV}")
    print(f"[enrich] pool match rate: {match}/{len(pool)} ({match/len(pool)*100:.1f}%) | "
          f"missing parquets: {missing}")
    nn = out[ENRICHED_FEATS].notna().mean().sort_values()
    print("[enrich] lowest-coverage features:")
    print(nn.head(8).to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
