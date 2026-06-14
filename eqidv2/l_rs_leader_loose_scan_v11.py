"""
l_rs_leader_loose_scan_v11.py — HONEST SAMPLE SALVAGE for L_RS_LEADER_VWAP_HOLD.

The original detection (rs_pct>=0.75 + 11 ANDed conditions) fired only 5 test candidates -> could not
be validated. That is a SAMPLE failure caused by PRE-IMPOSING a tight skeleton. The honest fix (same as
S_UPTHRUST_TRAP_FADE): emit a LOOSE structural skeleton that fires a workable population and ENRICH the
discriminators (rs, stock_ret, ema stack/slope, ADX, RSI, close>prev_high, wicks) so the robustness-first
search can find the gate with proper anti-overfit validation — instead of hand-fixing the thresholds.

Core idea kept: an intraday LEADER that tests VWAP from ABOVE and HOLDS it on a green bar (continuation).
Loosened: rs_pct>=0.0 (search finds the leadership cut), close>VWAP & close>EMA20 (holds above), touched
near VWAP (low<=VWAP+0.60ATR), green, close_loc>=0.50, vol_ratio>=1.1, regime!=BEAR, 09:30-14:30.
DROPPED into enriched features (for the search to gate): EMA20>=EMA50, ema20_slope>0, close>prev_high,
ADX>=20, 50<=RSI<=72.

Writes candidates as setup L_RS_LEADER_VWAP_HOLD_LOOSE in the standalone schema.
Run AFTER 15:30 IST. Usage: py -3.12 l_rs_leader_loose_scan_v11.py [--limit N | --dry-run]
"""
from __future__ import annotations
import argparse, time
from pathlib import Path
import numpy as np
import pandas as pd
import research_v11_tier123_new_setups as r123

OUT = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_new_setups_probe\l_rs_leader_loose_trades.csv")
SETUP = "L_RS_LEADER_VWAP_HOLD_LOOSE"


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
        ema20_arr = (g["EMA_20"] if "EMA_20" in g.columns else pd.Series(np.nan, index=g.index)).to_numpy()
        rsi_s = g["RSI"] if "RSI" in g.columns else pd.Series(np.nan, index=g.index)
        rsi3max = rsi_s.rolling(3, min_periods=2).max().to_numpy()
        for i in range(10, len(g) - 1):
            row = g.iloc[i]
            ts = r123._normalise_ts(row["date"]); minute = ts.hour * 60 + ts.minute
            if minute < 570 or minute > 870:
                continue
            close = float(row["close"]); open_px = float(row["open"]); low = float(row["low"])
            atr = float(row.get("ATR", np.nan)); vwap = float(row.get("VWAP", np.nan))
            ema20 = float(row.get("EMA_20", np.nan)); ema50 = float(row.get("EMA_50", np.nan))
            adx = float(row.get("ADX", np.nan)); rsi = float(row.get("RSI", np.nan))
            vol_ratio = float(row.get("vol_ratio", np.nan)); close_loc = float(row.get("close_loc", np.nan))
            if not (np.isfinite(atr) and atr > 0 and np.isfinite(vwap) and np.isfinite(ema20)):
                continue
            market_ret, regime = r123._bar_context(market_ctx, str(day), ts)
            stock_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
            rs_pct = stock_ret - market_ret
            ema20_slope = ema20 - float(ema20_arr[i - 3]) if (i >= 3 and np.isfinite(ema20_arr[i - 3])) else np.nan
            prev_high = float(g["high"].iloc[i - 1])
            # LOOSE skeleton: leader holds VWAP from above on a green bar (continuation)
            if (rs_pct >= 0.0 and stock_ret >= 0.0
                    and close > ema20 and low <= vwap + 0.60 * atr and close > vwap
                    and close > open_px and np.isfinite(close_loc) and close_loc >= 0.50
                    and np.isfinite(vol_ratio) and vol_ratio >= 1.1
                    and regime in {"BULL", "TREND", "NEUTRAL"}):
                c = r123._candidate(ticker, SETUP, "LONG", row, rs_pct, market_ret, regime,
                                    "rs_leader_vwap_hold_continuation_loose")
                c.update({
                    "rsi": rsi, "rsi3max": float(rsi3max[i]) if np.isfinite(rsi3max[i]) else np.nan,
                    "adx": adx, "ema20_slope": ema20_slope, "stock_ret": stock_ret,
                    "ema20_ge_ema50": float(1.0 if (np.isfinite(ema50) and ema20 >= ema50) else 0.0),
                    "break_prev_high": float(1.0 if close > prev_high else 0.0),
                    "upper_wick_pct": float(row.get("upper_wick_pct", np.nan)),
                    "lower_wick_pct": float(row.get("lower_wick_pct", np.nan)),
                })
                out.append(c)
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
    print(f"[loose_L] universe={len(uni)}", flush=True)
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
        d = pd.to_datetime(df["signal_time_ist"], errors="coerce").dt.strftime("%Y-%m-%d")
        print(f"[loose_L] n={len(df)} train(<=04-30)={(d<='2026-04-30').sum()} test(>=05-01)={(d>='2026-05-01').sum()}")
    if not a.dry_run:
        df.to_csv(OUT, index=False); print(f"[loose_L] wrote {len(df)} -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
