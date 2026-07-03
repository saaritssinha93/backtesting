r"""enrich_pool_features.py — recompute true indicator / pre-momentum / structural features
for every A_MOD_BREAK_C1_HIGH pool row, directly from the raw 5-min parquet.

Leak-safety: every feature uses only bars <= the signal bar (same-bar values are known at
bar close, identical to live detection). Prev-day levels use the prior session only.

Outputs:
  pools/pool_enriched/            all rows + ~35 new feature columns
  pools/pool_enriched_first/      structural dedupe: FIRST signal per (ticker, day)
  pools/pool_enriched_first_am/   dedupe + signal_minute <= 665 (11:05)
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
DATA = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
FNAME = "historical_all_available_pre_dedupe_live_candidates.csv"
OR_BARS = 3  # opening range = first 3 five-min bars (09:15-09:30), mirrors v2._opening_range


def enrich_ticker(g: pd.DataFrame, rows: pd.DataFrame) -> list[dict]:
    """g: full parquet frame for one ticker (tz-normalised, sorted). rows: pool rows."""
    g = g.reset_index(drop=True)
    ts_index = {t: i for i, t in enumerate(g["date"])}
    day_of = g["date"].dt.strftime("%Y-%m-%d")
    out = []

    # prev-day levels per session
    day_high = g.groupby(day_of)["high"].max()
    day_close = g.groupby(day_of)["close"].last()
    days_sorted = sorted(day_high.index)
    prev_high = {d: day_high[days_sorted[k - 1]] if k > 0 else np.nan for k, d in enumerate(days_sorted)}
    prev_close = {d: day_close[days_sorted[k - 1]] if k > 0 else np.nan for k, d in enumerate(days_sorted)}

    high20 = g["high"].shift(1).rolling(20, min_periods=8).max()
    vol20 = g["volume"].shift(1).rolling(20, min_periods=8).mean()

    for ridx, r in rows.iterrows():
        t = r["_sig_ts"]
        i = ts_index.get(t)
        f = {"_row_id": ridx}
        if i is None or i < 6:
            out.append(f)
            continue
        d = day_of.iloc[i]
        day_slice = g[day_of == d]
        j0 = day_slice.index[0]                      # first bar index of the session
        bar_of_day = i - j0
        intraday = g.iloc[j0:i + 1]

        close = float(g["close"].iloc[i]); atr = float(g["ATR"].iloc[i])
        if not (np.isfinite(atr) and atr > 0):
            out.append(f); continue

        def col(name, k=0):
            v = g[name].iloc[i - k] if name in g.columns and i - k >= 0 else np.nan
            try:
                return float(v)
            except Exception:
                return np.nan

        # ---- indicators at signal bar ----
        f["rsi_x"] = col("RSI")
        f["rsi_slope3"] = col("RSI") - col("RSI", 3)
        f["adx_x"] = col("ADX")
        f["cci_x"] = col("CCI")
        f["mfi_x"] = col("MFI")
        f["stoch_k"] = col("Stoch_%K")
        f["stoch_cross"] = col("Stoch_%K") - col("Stoch_%D")
        f["macd_hist_x"] = col("MACD_Hist")
        f["macd_hist_delta3"] = col("MACD_Hist") - col("MACD_Hist", 3)
        f["macd_above_sig"] = col("MACD") - col("MACD_Signal")
        ema20, ema50, ema200 = col("EMA_20"), col("EMA_50"), col("EMA_200")
        f["ema20_dist_atr"] = (close - ema20) / atr if np.isfinite(ema20) else np.nan
        f["ema50_dist_atr"] = (close - ema50) / atr if np.isfinite(ema50) else np.nan
        f["ema20_slope5_atr"] = (ema20 - col("EMA_20", 5)) / atr if np.isfinite(ema20) else np.nan
        f["ema_stack"] = float((ema20 > ema50) + (ema50 > ema200)) if np.isfinite(ema20) and np.isfinite(ema50) and np.isfinite(ema200) else np.nan
        ub, lb = col("Upper_Band"), col("Lower_Band")
        f["bb_pos"] = (close - lb) / (ub - lb) if np.isfinite(ub) and np.isfinite(lb) and ub > lb else np.nan
        f["bb_width_pct"] = 100.0 * (ub - lb) / close if np.isfinite(ub) and np.isfinite(lb) else np.nan
        obv5 = col("OBV") - col("OBV", 5)
        vsum5 = float(g["volume"].iloc[max(0, i - 4):i + 1].sum())
        f["obv_slope5_norm"] = obv5 / vsum5 if vsum5 > 0 else np.nan

        # ---- pre-momentum (strictly pre-signal bars) ----
        c = g["close"]
        f["pre1_ret_atr"] = (float(c.iloc[i - 1]) - float(c.iloc[i - 2])) / atr
        f["pre3_ret_atr"] = (float(c.iloc[i - 1]) - float(c.iloc[i - 4])) / atr if i >= 4 else np.nan
        f["pre5_ret_atr"] = (float(c.iloc[i - 1]) - float(c.iloc[i - 6])) / atr if i >= 6 else np.nan
        streak = 0
        for k in range(i - 1, max(j0, i - 8) - 1, -1):
            if float(g["close"].iloc[k]) > float(g["open"].iloc[k]):
                streak += 1
            else:
                break
        f["green_streak_pre"] = float(streak)
        v20 = float(vol20.iloc[i]) if np.isfinite(vol20.iloc[i]) else np.nan
        f["pre3_vol_ratio"] = float(g["volume"].iloc[i - 3:i].mean()) / v20 if np.isfinite(v20) and v20 > 0 else np.nan
        rng3 = (g["high"] - g["low"]).iloc[i - 3:i].mean()
        f["range_compress3"] = float(rng3) / atr if np.isfinite(rng3) else np.nan
        f["pre_rsi"] = col("RSI", 1)
        vwap_ser = g["VWAP"].iloc[j0:i + 1]
        close_ser = g["close"].iloc[j0:i + 1]
        above = (close_ser.values > vwap_ser.values)
        hold = 0
        for k in range(len(above) - 1, -1, -1):
            if above[k]:
                hold += 1
            else:
                break
        f["vwap_hold_bars"] = float(hold)

        # ---- structural / non-indicator ----
        f["break_margin_atr"] = (close - float(g["high"].iloc[i - 1])) / atr
        h20 = float(high20.iloc[i]) if np.isfinite(high20.iloc[i]) else np.nan
        f["is_20bar_high"] = float(close > h20) if np.isfinite(h20) else np.nan
        f["dist_20bar_high_atr"] = (close - h20) / atr if np.isfinite(h20) else np.nan
        orh = float(day_slice["high"].iloc[:OR_BARS].max()) if len(day_slice) >= OR_BARS else np.nan
        f["or_high_dist_atr"] = (close - orh) / atr if np.isfinite(orh) else np.nan
        f["above_or_high"] = float(close > orh) if np.isfinite(orh) else np.nan
        pdh = prev_high.get(d, np.nan)
        pdc = prev_close.get(d, np.nan)
        f["pdh_dist_atr"] = (close - pdh) / atr if np.isfinite(pdh) else np.nan
        f["above_pdh"] = float(close > pdh) if np.isfinite(pdh) else np.nan
        day_open = float(day_slice["open"].iloc[0])
        f["gap_pct"] = 100.0 * (day_open - pdc) / pdc if np.isfinite(pdc) and pdc > 0 else np.nan
        f["day_ret_pct"] = 100.0 * (close / day_open - 1.0) if day_open > 0 else np.nan
        dlo = float(intraday["low"].min()); dhi = float(intraday["high"].max())
        f["day_range_pos"] = (close - dlo) / (dhi - dlo) if dhi > dlo else np.nan
        f["upmove_from_daylow_atr"] = (close - dlo) / atr
        f["bar_of_day"] = float(bar_of_day)
        f["dow"] = float(pd.Timestamp(t).dayofweek)
        f["price_level"] = close
        f["notional_5m_rs"] = close * float(g["volume"].iloc[i])
        out.append(f)
    return out


def main() -> int:
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else WORK / "pools" / "pool_full"
    pool = pd.read_csv(src / FNAME, low_memory=False)
    ts = pd.to_datetime(pool["signal_time_ist"], errors="coerce")
    if getattr(ts.dt, "tz", None) is None:
        ts = ts.dt.tz_localize("Asia/Kolkata")
    else:
        ts = ts.dt.tz_convert("Asia/Kolkata")
    pool["_sig_ts"] = ts
    print(f"[enrich] pool rows={len(pool)} tickers={pool['ticker'].nunique()}")

    t0 = time.time()
    feats: list[dict] = []
    tickers = sorted(pool["ticker"].astype(str).str.upper().unique())
    for n, tk in enumerate(tickers, 1):
        rows = pool[pool["ticker"].astype(str).str.upper() == tk]
        p = DATA / f"{tk}_stocks_indicators_5min.parquet"
        if not p.exists():
            feats.extend({"_row_id": ridx} for ridx in rows.index)
            continue
        g = pd.read_parquet(p)
        g["date"] = pd.to_datetime(g["date"], errors="coerce")
        if getattr(g["date"].dt, "tz", None) is None:
            g["date"] = g["date"].dt.tz_localize("Asia/Kolkata")
        else:
            g["date"] = g["date"].dt.tz_convert("Asia/Kolkata")
        g = g.dropna(subset=["date"]).sort_values("date")
        feats.extend(enrich_ticker(g, rows))
        if n % 100 == 0 or n == len(tickers):
            print(f"[enrich] {n}/{len(tickers)} elapsed={time.time()-t0:.0f}s", flush=True)

    fdf = pd.DataFrame(feats).set_index("_row_id")
    new_cols = [c for c in fdf.columns]
    merged = pool.drop(columns=["_sig_ts"]).join(fdf, how="left")
    print(f"[enrich] new cols: {new_cols}")
    print(f"[enrich] coverage: " + ", ".join(f"{c}={merged[c].notna().mean():.0%}" for c in new_cols[:12]))

    out_all = WORK / "pools" / "pool_enriched"
    out_all.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_all / FNAME, index=False)

    # FIRST signal per (ticker, day) — structural, time-causal dedupe
    st = pd.to_datetime(merged["signal_time_ist"], errors="coerce")
    merged["_d"] = st.dt.strftime("%Y-%m-%d")
    first = merged.sort_values("signal_time_ist").groupby(["ticker", "_d"], as_index=False).head(1)
    out_first = WORK / "pools" / "pool_enriched_first"
    out_first.mkdir(parents=True, exist_ok=True)
    first.drop(columns=["_d"]).to_csv(out_first / FNAME, index=False)

    minute = st.dt.hour * 60 + st.dt.minute
    first_am = merged[merged.index.isin(first.index) & (minute <= 665)]
    out_am = WORK / "pools" / "pool_enriched_first_am"
    out_am.mkdir(parents=True, exist_ok=True)
    first_am.drop(columns=["_d"]).to_csv(out_am / FNAME, index=False)

    print(f"[enrich] wrote pool_enriched={len(merged)} pool_enriched_first={len(first)} pool_enriched_first_am={len(first_am)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
