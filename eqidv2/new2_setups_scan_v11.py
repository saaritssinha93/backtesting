"""
new2_setups_scan_v11.py — structural scan for TWO genuinely new candidate setups, honest pipeline:
  - GAP_UP_HOLD_BREAK            (LONG)  : overnight gap up that HOLDS the opening range, then breaks it
                                          (gap-and-go). NOTE: a tier123 overlay G_GAP_HOLD_CONTINUATION_LONG
                                          exists but fires ~0 in the standard pool and was never validated;
                                          this STANDALONE scan gives the idea its first real population/test.
  - POWER_HOUR_LAGGARD_BREAKDOWN (SHORT) : a day-long laggard (rs<0) breaking a NEW intraday low in the
                                          power hour (>=13:30) on rising volume = late-day capitulation.
                                          GENUINELY NEW (no tier123/book equivalent).

LOOSE structural skeletons (fire a workable population); discriminators ENRICHED for the search to gate
(same methodology as S_UPTHRUST / the loose-L salvage). NOT run during market hours. Run AFTER 15:30 IST.
Usage: py -3.12 new2_setups_scan_v11.py [--dry-run | --limit N]
"""
from __future__ import annotations
import argparse, time
from pathlib import Path
import numpy as np
import pandas as pd
import research_v11_tier123_new_setups as r123
import avwap_5min_ID_v2_backtesting as v2

OUT = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_new_setups_probe\new2_setups_standalone_trades.csv")
LONG_SETUP = "GAP_UP_HOLD_BREAK"
SHORT_SETUP = "POWER_HOUR_LAGGARD_BREAKDOWN"


def _scan_ticker(ticker, market_ctx):
    df = r123._read_5m(ticker)
    if df is None or df.empty:
        return []
    df = r123._prev_day_levels(df)
    out = []
    for day, group in df.groupby("date_only", sort=True):
        g = group.reset_index(drop=True)
        if len(g) < 15:
            continue
        day_open = float(g["open"].iloc[0])
        or_high, or_low = v2._opening_range(g)
        opening_low = float(g["low"].iloc[:3].min())
        opening_high = float(g["high"].iloc[:3].max())
        prev_day_close = float(g.get("Prev_Day_Close", pd.Series(np.nan, index=g.index)).iloc[0])
        if not np.isfinite(prev_day_close):
            prev_day_close = float(g.get("prev_day_close_calc", pd.Series(np.nan, index=g.index)).iloc[0])
        gap_pct = (day_open / prev_day_close - 1.0) * 100.0 if (np.isfinite(prev_day_close) and prev_day_close > 0) else np.nan
        lows = g["low"].to_numpy(float); highs = g["high"].to_numpy(float)
        ema20_arr = (g["EMA_20"] if "EMA_20" in g.columns else pd.Series(np.nan, index=g.index)).to_numpy()
        rsi_s = g["RSI"] if "RSI" in g.columns else pd.Series(np.nan, index=g.index)
        rsi3max = rsi_s.rolling(3, min_periods=2).max().to_numpy()

        for i in range(4, len(g) - 1):
            row = g.iloc[i]
            ts = r123._normalise_ts(row["date"]); minute = ts.hour * 60 + ts.minute
            close = float(row["close"]); open_px = float(row["open"]); high = float(row["high"]); low = float(row["low"])
            atr = float(row.get("ATR", np.nan)); vwap = float(row.get("VWAP", np.nan))
            ema20 = float(row.get("EMA_20", np.nan)); ema50 = float(row.get("EMA_50", np.nan))
            adx = float(row.get("ADX", np.nan)); rsi = float(row.get("RSI", np.nan))
            vol_ratio = float(row.get("vol_ratio", np.nan)); close_loc = float(row.get("close_loc", np.nan))
            atr_pct = float(row.get("atr_pct", np.nan)); vwap_dist_atr = float(row.get("vwap_dist_atr", np.nan))
            body_pct = float(row.get("body_pct", np.nan))
            if not (np.isfinite(atr) and atr > 0 and np.isfinite(vwap)):
                continue
            market_ret, regime = r123._bar_context(market_ctx, str(day), ts)
            stock_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
            rs_pct = stock_ret - market_ret
            ema20_slope = ema20 - float(ema20_arr[i - 3]) if (i >= 3 and np.isfinite(ema20_arr[i - 3])) else np.nan
            day_low_prior = float(np.nanmin(lows[:i])) if i > 0 else np.nan
            day_high_prior = float(np.nanmax(highs[:i])) if i > 0 else np.nan
            common_extra = {"rsi": rsi, "rsi3max": float(rsi3max[i]) if np.isfinite(rsi3max[i]) else np.nan,
                            "adx": adx, "ema20_slope": ema20_slope, "stock_ret": stock_ret, "gap_pct": gap_pct}

            # ---------- LONG: GAP_UP_HOLD_BREAK ----------
            if (585 <= minute <= 750
                    and np.isfinite(gap_pct) and 0.30 <= gap_pct <= 5.0          # real gap up (not micro/blow-off)
                    and np.isfinite(opening_low) and opening_low >= prev_day_close * 0.997  # opening range HELD the gap
                    and np.isfinite(or_high) and close > or_high                  # breaks opening range
                    and close > vwap and close > open_px and np.isfinite(close_loc) and close_loc >= 0.50
                    and rs_pct >= 0.0 and np.isfinite(vol_ratio) and vol_ratio >= 1.1
                    and regime in {"BULL", "TREND", "NEUTRAL"}):
                c = r123._candidate(ticker, LONG_SETUP, "LONG", row, rs_pct, market_ret, regime,
                                    "gap_up_hold_opening_range_break")
                c.update(common_extra)
                c["dist_above_or_atr"] = (close - or_high) / atr if np.isfinite(or_high) else np.nan
                out.append(c)

            # ---------- SHORT: POWER_HOUR_LAGGARD_BREAKDOWN ----------
            if (810 <= minute <= 900                                              # power hour (13:30-15:00)
                    and rs_pct <= -0.20 and stock_ret < 0.0                        # laggard, red on the day
                    and np.isfinite(ema20) and close < vwap and close < ema20      # below VWAP & EMA20
                    and np.isfinite(day_low_prior) and close < day_low_prior       # NEW intraday low
                    and close < open_px and np.isfinite(close_loc) and close_loc <= 0.45
                    and np.isfinite(vol_ratio) and vol_ratio >= 1.2
                    and regime in {"BEAR", "TREND", "NEUTRAL"}):
                c = r123._candidate(ticker, SHORT_SETUP, "SHORT", row, rs_pct, market_ret, regime,
                                    "power_hour_laggard_new_low_breakdown")
                c.update(common_extra)
                c["dist_below_vwap_atr"] = vwap_dist_atr
                out.append(c)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true"); ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    mc = r123._market_context()
    if not mc:
        print("!! market context empty (NIFTY 5m not found)")
    uni = [x for x in r123._load_probe_universe() if not str(x).upper().startswith("NIFTY")
           and (r123.DATA_ROOT / f"{str(x).upper()}_stocks_indicators_5min.parquet").exists()]
    if a.dry_run:
        uni = uni[:3]
    elif a.limit:
        uni = uni[:a.limit]
    print(f"[new2_scan] universe={len(uni)} (dry_run={a.dry_run})", flush=True)
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
        print("\n[new2_scan] fire counts:", df.groupby(["setup", "side"]).size().to_dict())
        for s in (LONG_SETUP, SHORT_SETUP):
            sub = df[df["setup"] == s]
            if len(sub):
                d = pd.to_datetime(sub["signal_time_ist"], errors="coerce").dt.strftime("%Y-%m-%d")
                print(f"   {s}: n={len(sub)} train(<=2026-04-30)={(d<='2026-04-30').sum()} test(>=2026-05-01)={(d>='2026-05-01').sum()}")
    else:
        print("\n[new2_scan] NO candidates — loosen skeletons.")
    if not a.dry_run:
        df.to_csv(OUT, index=False); print(f"[new2_scan] wrote {len(df)} -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
