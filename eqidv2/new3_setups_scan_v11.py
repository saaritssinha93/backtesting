"""
new3_setups_scan_v11.py — three more brand-new candidate setups, honest pipeline. Chosen to fire across
MANY days (mean-reversion / trap-reversal) so the edge has a chance to spread (the property GAP_UP_HOLD /
POWER_HOUR lacked -> both died on day-concentration):
  - GAP_DOWN_FADE_RECLAIM   (LONG)  : oversold gap DOWN that reclaims VWAP/opening range (mean-reversion)
  - FIRST_HOUR_LOW_RECLAIM  (LONG)  : washout below first-hour low then reclaims it (bear-trap reversal)
  - FIRST_HOUR_HIGH_FAIL    (SHORT) : false breakout above first-hour high then loses it (bull-trap)

LOOSE structural skeletons; discriminators ENRICHED for the search to gate. NOT during market hours.
Usage: py -3.12 new3_setups_scan_v11.py [--dry-run | --limit N]
"""
from __future__ import annotations
import argparse, time
from pathlib import Path
import numpy as np
import pandas as pd
import research_v11_tier123_new_setups as r123
import avwap_5min_ID_v2_backtesting as v2

OUT = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_new_setups_probe\new3_setups_standalone_trades.csv")
GAP_FADE = "GAP_DOWN_FADE_RECLAIM"
FH_LOW_RECLAIM = "FIRST_HOUR_LOW_RECLAIM"
FH_HIGH_FAIL = "FIRST_HOUR_HIGH_FAIL"


def _scan_ticker(ticker, market_ctx):
    df = r123._read_5m(ticker)
    if df is None or df.empty:
        return []
    df = r123._prev_day_levels(df)
    out = []
    for day, group in df.groupby("date_only", sort=True):
        g = group.reset_index(drop=True)
        if len(g) < 18:
            continue
        day_open = float(g["open"].iloc[0])
        or_high, or_low = v2._opening_range(g)
        opening_low = float(g["low"].iloc[:3].min())
        fh_high = float(g["high"].iloc[:12].max())   # first-hour (≈09:15-10:15) range
        fh_low = float(g["low"].iloc[:12].min())
        prev_day_close = float(g.get("Prev_Day_Close", pd.Series(np.nan, index=g.index)).iloc[0])
        if not np.isfinite(prev_day_close):
            prev_day_close = float(g.get("prev_day_close_calc", pd.Series(np.nan, index=g.index)).iloc[0])
        gap_pct = (day_open / prev_day_close - 1.0) * 100.0 if (np.isfinite(prev_day_close) and prev_day_close > 0) else np.nan
        ema20_arr = (g["EMA_20"] if "EMA_20" in g.columns else pd.Series(np.nan, index=g.index)).to_numpy()
        rsi_s = g["RSI"] if "RSI" in g.columns else pd.Series(np.nan, index=g.index)
        rsi3min = rsi_s.rolling(3, min_periods=2).min().to_numpy()
        rsi3max = rsi_s.rolling(3, min_periods=2).max().to_numpy()
        lows = g["low"].to_numpy(float); highs = g["high"].to_numpy(float)
        broke_fh_low = False; broke_fh_high = False

        for i in range(4, len(g) - 1):
            row = g.iloc[i]
            ts = r123._normalise_ts(row["date"]); minute = ts.hour * 60 + ts.minute
            close = float(row["close"]); open_px = float(row["open"]); high = float(row["high"]); low = float(row["low"])
            atr = float(row.get("ATR", np.nan)); vwap = float(row.get("VWAP", np.nan))
            ema20 = float(row.get("EMA_20", np.nan)); adx = float(row.get("ADX", np.nan)); rsi = float(row.get("RSI", np.nan))
            vol_ratio = float(row.get("vol_ratio", np.nan)); close_loc = float(row.get("close_loc", np.nan))
            atr_pct = float(row.get("atr_pct", np.nan)); vwap_dist_atr = float(row.get("vwap_dist_atr", np.nan))
            body_pct = float(row.get("body_pct", np.nan)); upper_wick = float(row.get("upper_wick_pct", np.nan))
            lower_wick = float(row.get("lower_wick_pct", np.nan))
            if not (np.isfinite(atr) and atr > 0 and np.isfinite(vwap)):
                # still must update trap flags using raw prices
                if i >= 12 and np.isfinite(fh_low) and low < fh_low: broke_fh_low = True
                if i >= 12 and np.isfinite(fh_high) and high > fh_high: broke_fh_high = True
                continue
            market_ret, regime = r123._bar_context(market_ctx, str(day), ts)
            stock_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
            rs_pct = stock_ret - market_ret
            ema20_slope = ema20 - float(ema20_arr[i - 3]) if (i >= 3 and np.isfinite(ema20_arr[i - 3])) else np.nan
            extra = {"rsi": rsi, "rsi3max": float(rsi3max[i]) if np.isfinite(rsi3max[i]) else np.nan,
                     "rsi3min": float(rsi3min[i]) if np.isfinite(rsi3min[i]) else np.nan, "adx": adx,
                     "ema20_slope": ema20_slope, "stock_ret": stock_ret, "gap_pct": gap_pct,
                     "upper_wick_pct": upper_wick, "lower_wick_pct": lower_wick}

            # ---------- LONG: GAP_DOWN_FADE_RECLAIM ----------
            if (585 <= minute <= 750
                    and np.isfinite(gap_pct) and -5.0 <= gap_pct <= -0.5            # real gap down
                    and close > vwap and np.isfinite(or_high) and close > or_high    # reclaims VWAP + opening range
                    and close > open_px and np.isfinite(close_loc) and close_loc >= 0.50
                    and rs_pct >= -1.5 and np.isfinite(vol_ratio) and vol_ratio >= 1.2
                    and regime in {"BULL", "TREND", "NEUTRAL"}):
                c = r123._candidate(ticker, GAP_FADE, "LONG", row, rs_pct, market_ret, regime, "gap_down_fade_reclaim")
                c.update(extra); out.append(c)

            # ---------- LONG: FIRST_HOUR_LOW_RECLAIM (bear trap) ----------
            if (630 <= minute <= 840 and broke_fh_low
                    and np.isfinite(fh_low) and close > fh_low                       # reclaimed back above FH low
                    and close > open_px and np.isfinite(close_loc) and close_loc >= 0.55
                    and np.isfinite(vol_ratio) and vol_ratio >= 1.2
                    and regime in {"BULL", "TREND", "NEUTRAL"}):
                c = r123._candidate(ticker, FH_LOW_RECLAIM, "LONG", row, rs_pct, market_ret, regime, "first_hour_low_reclaim_bear_trap")
                c.update(extra); c["fh_break_depth_atr"] = (fh_low - low) / atr if low < fh_low else 0.0
                out.append(c)

            # ---------- SHORT: FIRST_HOUR_HIGH_FAIL (bull trap) ----------
            if (630 <= minute <= 870 and broke_fh_high
                    and np.isfinite(fh_high) and close < fh_high                     # lost back below FH high
                    and close < open_px and np.isfinite(close_loc) and close_loc <= 0.45
                    and np.isfinite(vol_ratio) and vol_ratio >= 1.3
                    and regime in {"BEAR", "TREND", "NEUTRAL"}):
                c = r123._candidate(ticker, FH_HIGH_FAIL, "SHORT", row, rs_pct, market_ret, regime, "first_hour_high_fail_bull_trap")
                c.update(extra); c["fh_break_ext_atr"] = (high - fh_high) / atr if high > fh_high else 0.0
                out.append(c)

            # update trap flags AFTER evaluating (the break must have happened on a PRIOR bar)
            if i >= 12 and np.isfinite(fh_low) and low < fh_low: broke_fh_low = True
            if i >= 12 and np.isfinite(fh_high) and high > fh_high: broke_fh_high = True
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true"); ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    mc = r123._market_context()
    uni = [x for x in r123._load_probe_universe() if not str(x).upper().startswith("NIFTY")
           and (r123.DATA_ROOT / f"{str(x).upper()}_stocks_indicators_5min.parquet").exists()]
    if a.dry_run:
        uni = uni[:3]
    elif a.limit:
        uni = uni[:a.limit]
    print(f"[new3_scan] universe={len(uni)} (dry_run={a.dry_run})", flush=True)
    rows = []; t0 = time.time()
    for i, tk in enumerate(uni, 1):
        try:
            rows.extend(_scan_ticker(str(tk).upper(), mc))
        except Exception as e:
            print(f"  [skip {tk}] {e!r}", flush=True)
        if i % 25 == 0 or i == len(uni):
            print(f"  [{i}/{len(uni)}] rows={len(rows)} {time.time()-t0:.0f}s", flush=True)
    df = pd.DataFrame(rows)
    if len(df):
        print("\n[new3_scan] fire counts:", df.groupby(["setup", "side"]).size().to_dict())
        for s in (GAP_FADE, FH_LOW_RECLAIM, FH_HIGH_FAIL):
            sub = df[df["setup"] == s]
            if len(sub):
                d = pd.to_datetime(sub["signal_time_ist"], errors="coerce").dt.strftime("%Y-%m-%d")
                print(f"   {s}: n={len(sub)} train(<=2026-04-30)={(d<='2026-04-30').sum()} test(>=2026-05-01)={(d>='2026-05-01').sum()} days={d.nunique()}")
    else:
        print("\n[new3_scan] NO candidates.")
    if not a.dry_run:
        df.to_csv(OUT, index=False); print(f"[new3_scan] wrote {len(df)} -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
