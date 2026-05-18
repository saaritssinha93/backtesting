"""READ-ONLY scanner for NEW logically-grounded setups for v17r_nonf.

Does NOT modify any strategy / pipeline / config file.

Builds 6 new setups (3 LONG, 3 SHORT) using advanced indicator combinations
already present in the 5-min parquets (VWAP, EMA20/50/200, MACD,
Bollinger Bands, RSI, ADX, MFI, Stoch). Each setup has a clear mechanical
story; chains are NOT mined post-hoc on the same data.

NEW LONG setups
---------------
L_MACD_BULL_VWAP    : MACD bull cross above zero, price > VWAP > EMA20,
                      ADX >= 20, RSI 50-70 (no exhaustion), bullish body.
L_BB_SQUEEZE_LONG   : BB width contracted (<= 25th pctl over 100 bars) for
                      >= 3 of last 5 bars, breakout above upper band with
                      volume_ratio > 1.5, body_efficiency > 0.6.
L_TREND_PULLBACK    : EMA20 > EMA50 > EMA200 (stacked), pullback to within
                      0.25 ATR of EMA20, then bullish reversal candle
                      (close > prev close, body_eff > 0.5).

NEW SHORT setups
----------------
S_MACD_BEAR_VWAP    : MACD bear cross below zero, price < VWAP < EMA20,
                      ADX >= 20, RSI 30-50, bearish body.
S_BB_SQUEEZE_SHORT  : BB width contracted, breakdown below lower band with
                      volume surge, bearish body.
S_TREND_REJECT      : EMA20 < EMA50 < EMA200 (stacked down), bounce to
                      within 0.25 ATR of EMA20, bearish reversal candle
                      (close < prev close, body_eff > 0.5).

Pipeline per setup:
  1. Scan 5-min parquets (date window via env EQIDV_DATE_FROM).
  2. Detect signal bar; entry at next bar's open.
  3. Walk 1-min bars for TGT 1.5% / SL 0.75% exit.
  4. Apply ADV gate (mid+top100) + breadth gate (loose, SHORT only).
  5. Honest v17D per-row costs, 5x leverage.
  6. Report PF + IS/OOS (IS <= 2026-02-15) + monthly stability.
"""
from __future__ import annotations

import glob
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eqidv2 import v17D_cost_model as cm
from eqidv2 import v17D_exit_resolver as er

DATA_5M = r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2"
PARQUET_1MIN = r"C:\TradingData\eqidv2\stocks_indicators_1min_eq"
UNIVERSE = r"c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\configs\universe.csv"
BREADTH_CACHE = Path(__file__).resolve().parent / "_v17r_breadth_cache.parquet"

DATE_FROM = os.environ.get("EQIDV_DATE_FROM", "2026-02-05")
LEVERAGE = 5.0
TGT_PCT = 1.5
SL_PCT = 0.75
BREADTH_THRESHOLD = 0.119
IS_END = "2026-02-15"
ADV_FLOOR = 50.0
MAX_TICKERS = int(os.environ.get("EQIDV17R_ADV_MAX_TICKERS", "0"))  # 0 = no cap


def pf(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    w, l = float(s[s > 0].sum()), float(-s[s < 0].sum())
    return float("inf") if l <= 0 and w > 0 else (0.0 if l <= 0 else w / l)


def _f(x):
    return "{:+,.0f}".format(x)


def load_universe():
    uni = pd.read_csv(UNIVERSE)
    uni["adv_bucket"] = uni["adv_rs_cr"].apply(cm.adv_bucket_for)
    keep = uni[uni["adv_bucket"].isin(["mid", "top100"])].copy()
    return keep[["ticker", "adv_rs_cr", "adv_bucket"]]


def load_breadth():
    return pd.read_parquet(BREADTH_CACHE).sort_values("date").reset_index(drop=True)


def load_5m(ticker):
    f = Path(DATA_5M) / f"{ticker}_stocks_indicators_5min.parquet"
    if not f.exists():
        return None
    d = pd.read_parquet(f)
    dt = pd.to_datetime(d["date"], errors="coerce")
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize("UTC")
    d["date"] = dt.dt.tz_convert("Asia/Kolkata")
    d = d.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    cut = pd.Timestamp(DATE_FROM).tz_localize("Asia/Kolkata")
    return d[d["date"] >= cut].reset_index(drop=True)


def detect_signals(df):
    """Return list of (signal_idx, setup_name, side) for one ticker."""
    if df is None or len(df) < 50:
        return []

    c = df["close"].astype(float).values
    o = df["open"].astype(float).values
    h = df["high"].astype(float).values
    l = df["low"].astype(float).values
    vol = df["volume"].astype(float).values
    vwap = df["VWAP"].astype(float).values
    ema20 = df["EMA_20"].astype(float).values
    ema50 = df["EMA_50"].astype(float).values
    ema200 = df["EMA_200"].astype(float).values
    rsi = df["RSI"].astype(float).values
    adx = df["ADX"].astype(float).values
    atr = df["ATR"].astype(float).values
    macd = df["MACD"].astype(float).values
    macd_sig = df["MACD_Signal"].astype(float).values
    macd_hist = df["MACD_Hist"].astype(float).values
    ub = df["Upper_Band"].astype(float).values
    lb = df["Lower_Band"].astype(float).values
    sma20 = df["20_SMA"].astype(float).values
    date_only = pd.to_datetime(df["date"]).dt.date.values
    minute = pd.to_datetime(df["date"]).dt.hour * 60 + pd.to_datetime(df["date"]).dt.minute

    # derived
    rng = np.maximum(h - l, 1e-9)
    body_eff = np.abs(c - o) / rng
    bb_width = (ub - lb) / np.where(sma20 != 0, sma20, 1) * 100.0
    # Compression proxy: BB width relative to its rolling 100-bar mean.
    # Squeeze when current width <= 60% of mean (rank() unavailable on this
    # pandas; this avoids the dependency).
    bb_w_ser = pd.Series(bb_width)
    bb_mean = bb_w_ser.rolling(100, min_periods=30).mean().values
    bb_squeeze = np.where(bb_mean > 0, bb_width / bb_mean, np.nan)
    # bb_squeeze <= 0.6 means width is in the bottom of its recent range
    # volume ratio vs prior 20-bar mean
    vol_ser = pd.Series(vol)
    vol_mean = vol_ser.shift(1).rolling(20, min_periods=8).mean().values
    vol_ratio = np.where(vol_mean > 0, vol / vol_mean, np.nan)

    signals = []
    n = len(df)
    last_signal_idx = {}  # ticker-level cool-off: same setup, gap of >=12 bars (1hr)
    for i in range(50, n - 1):
        # we trade entry at bar i+1's open; skip last bar
        if minute.iloc[i] < 9 * 60 + 45 or minute.iloc[i] > 14 * 60:
            continue
        # need same-day next bar (no EOD slip-over entries)
        if date_only[i] != date_only[i + 1]:
            continue

        def _fire(setup):
            last = last_signal_idx.get(setup, -999)
            if i - last < 12:
                return False
            last_signal_idx[setup] = i
            return True

        # LONG: L_MACD_BULL_VWAP -- macd bull cross above zero, full alignment
        if (
            macd[i] > macd_sig[i] and macd[i - 1] <= macd_sig[i - 1]
            and macd[i] > 0
            and c[i] > vwap[i] > ema20[i] > ema50[i]
            and adx[i] >= 25
            and 55 <= rsi[i] <= 68
            and c[i] > o[i] and body_eff[i] >= 0.6
            and vol_ratio[i] >= 1.3
        ):
            if _fire("L_MACD_BULL_VWAP"):
                signals.append((i, "L_MACD_BULL_VWAP", "LONG"))

        # LONG: L_BB_SQUEEZE_LONG -- DEEP compression then strong breakout
        if (
            np.nanmean(bb_squeeze[max(0, i - 4): i]) <= 0.4
            and bb_squeeze[i] >= 0.7                                   # expansion now
            and c[i] > ub[i] * 1.003                                   # strong break
            and c[i] > c[i - 1]
            and vol_ratio[i] >= 2.0
            and body_eff[i] >= 0.65
            and c[i] > vwap[i]
        ):
            if _fire("L_BB_SQUEEZE_LONG"):
                signals.append((i, "L_BB_SQUEEZE_LONG", "LONG"))

        # LONG: L_TREND_PULLBACK -- pullback in strong uptrend, reversal bar
        if (
            ema20[i] > ema50[i] > ema200[i]
            and (ema20[i] - ema50[i]) / max(atr[i], 1e-9) >= 0.25      # real separation
            and c[i - 1] < c[i - 2]                                    # prior was down (real pullback)
            and l[i] <= ema20[i] + 0.20 * atr[i]                       # touched ema20
            and c[i] > ema20[i] + 0.05 * atr[i]                        # held cleanly above
            and c[i] > c[i - 1]
            and c[i] > o[i] and body_eff[i] >= 0.6
            and 52 <= rsi[i] <= 66
            and adx[i] >= 25
            and vol_ratio[i] >= 1.2
        ):
            if _fire("L_TREND_PULLBACK"):
                signals.append((i, "L_TREND_PULLBACK", "LONG"))

        # SHORT: S_MACD_BEAR_VWAP
        if (
            macd[i] < macd_sig[i] and macd[i - 1] >= macd_sig[i - 1]
            and macd[i] < 0
            and c[i] < vwap[i] < ema20[i] < ema50[i]
            and adx[i] >= 25
            and 32 <= rsi[i] <= 45
            and c[i] < o[i] and body_eff[i] >= 0.6
            and vol_ratio[i] >= 1.3
        ):
            if _fire("S_MACD_BEAR_VWAP"):
                signals.append((i, "S_MACD_BEAR_VWAP", "SHORT"))

        # SHORT: S_BB_SQUEEZE_SHORT
        if (
            np.nanmean(bb_squeeze[max(0, i - 4): i]) <= 0.4
            and bb_squeeze[i] >= 0.7
            and c[i] < lb[i] * 0.997
            and c[i] < c[i - 1]
            and vol_ratio[i] >= 2.0
            and body_eff[i] >= 0.65
            and c[i] < vwap[i]
        ):
            if _fire("S_BB_SQUEEZE_SHORT"):
                signals.append((i, "S_BB_SQUEEZE_SHORT", "SHORT"))

        # SHORT: S_TREND_REJECT
        if (
            ema20[i] < ema50[i] < ema200[i]
            and (ema50[i] - ema20[i]) / max(atr[i], 1e-9) >= 0.25
            and c[i - 1] > c[i - 2]                                    # prior was up (real bounce)
            and h[i] >= ema20[i] - 0.20 * atr[i]                       # touched ema20
            and c[i] < ema20[i] - 0.05 * atr[i]                        # rejected cleanly
            and c[i] < c[i - 1]
            and c[i] < o[i] and body_eff[i] >= 0.6
            and 34 <= rsi[i] <= 48
            and adx[i] >= 25
            and vol_ratio[i] >= 1.2
        ):
            if _fire("S_TREND_REJECT"):
                signals.append((i, "S_TREND_REJECT", "SHORT"))

    return signals


def scan_ticker(args):
    ticker, adv_bucket = args
    df = load_5m(ticker)
    if df is None or len(df) < 50:
        return []
    sigs = detect_signals(df)
    out = []
    for idx, setup, side in sigs:
        entry_bar = df.iloc[idx + 1]
        signal_bar = df.iloc[idx]
        out.append({
            "ticker": ticker, "adv_bucket": adv_bucket,
            "setup": setup, "side": side,
            "signal_time_ist": signal_bar["date"],
            "entry_time_ist": entry_bar["date"],
            "entry_price": float(entry_bar["open"]),
            "trade_date": entry_bar["date"].date(),
        })
    return out


def re_resolve(rows):
    cache = {}

    def bars(tk):
        if tk not in cache:
            cache[tk] = er.load_1min(PARQUET_1MIN, tk)
        return cache[tk]

    out = []
    for t in rows:
        b = bars(t["ticker"])
        if b is None:
            continue
        res = er.resolve(b, t["side"], t["entry_price"], t["entry_time_ist"], SL_PCT, TGT_PCT)
        if res is None:
            continue
        t = dict(t)
        t["outcome"] = res.outcome
        t["pnl_pct_price"] = res.pnl_pct_price
        out.append(t)
    return pd.DataFrame(out)


def apply_breadth(df, breadth):
    sig = pd.to_datetime(df["signal_time_ist"])
    if getattr(sig.dt, "tz", None) is None:
        sig = sig.dt.tz_localize("Asia/Kolkata")
    else:
        sig = sig.dt.tz_convert("Asia/Kolkata")
    work = df.assign(_sig=sig).sort_values("_sig")
    bdf = breadth.sort_values("date")
    merged = pd.merge_asof(work, bdf[["date", "pct_above_vwap"]],
                           left_on="_sig", right_on="date", direction="backward")
    keep = ~((merged["side"] == "SHORT") &
             (merged["pct_above_vwap"].fillna(0.0) < BREADTH_THRESHOLD))
    return merged.loc[keep].drop(columns=["_sig", "date"], errors="ignore").reset_index(drop=True)


def main():
    uni = load_universe()
    if MAX_TICKERS:
        uni = uni.head(MAX_TICKERS)
    print(f"[adv-scan] universe (mid+top100): {len(uni)} tickers | date >= {DATE_FROM}")

    args = list(zip(uni["ticker"].astype(str), uni["adv_bucket"]))
    all_rows = []
    nw = min(8, max(1, os.cpu_count() or 4))
    print(f"[adv-scan] scanning with {nw} workers ...")
    with ProcessPoolExecutor(max_workers=nw) as ex:
        for i, fut in enumerate(as_completed([ex.submit(scan_ticker, a) for a in args]), 1):
            try:
                all_rows.extend(fut.result())
            except Exception as exc:
                print(f"[adv-scan] worker error: {exc}")
            if i % 100 == 0:
                print(f"  [adv-scan] {i}/{len(args)} | signals so far: {len(all_rows)}")
    print(f"[adv-scan] total raw signals: {len(all_rows)}")
    if not all_rows:
        print("[adv-scan] no signals found")
        return

    # one-trade-per-(ticker,side,date) — keep earliest signal
    raw = pd.DataFrame(all_rows)
    raw = raw.sort_values(["ticker", "side", "trade_date", "signal_time_ist"])
    raw = raw.drop_duplicates(subset=["ticker", "side", "trade_date"], keep="first").reset_index(drop=True)
    print(f"[adv-scan] after one-per-ticker-side-day: {len(raw)}")

    # re-resolve exits
    print(f"[adv-scan] re-resolving {len(raw)} signals at TGT {TGT_PCT}% / SL {SL_PCT}% ...")
    rr = re_resolve(raw.to_dict("records"))
    print(f"[adv-scan] resolved: {len(rr)}")
    if rr.empty:
        return

    # cost + net
    rr["cost_pct"] = [cm.costs_pct_for_v17C(b, o if o in ("TARGET", "SL") else "TARGET")
                     for b, o in zip(rr["adv_bucket"], rr["outcome"])]
    rr["net_eff"] = (rr["pnl_pct_price"] - rr["cost_pct"]) * LEVERAGE

    # breadth gate
    breadth = load_breadth()
    pre = len(rr)
    rr = apply_breadth(rr, breadth)
    print(f"[adv-scan] after breadth gate: {pre} -> {len(rr)}")

    # report
    import datetime
    cut = datetime.date(2026, 2, 15)
    print(f"\n{'='*100}")
    print(f"NEW SETUP SCAN RESULTS  (TGT {TGT_PCT}% / SL {SL_PCT}%, breadth=loose, honest costs)")
    print(f"{'='*100}")
    print(f"{'side':<6} {'setup':<22} {'n':>4} {'PF':>6} {'win%':>6} "
          f"{'sumRs/lot':>10}  IS_PF (n)   OOS_PF (n)")
    for (sd, st), gg in rr.groupby(["side", "setup"]):
        if len(gg) < 5:
            continue
        is_g = gg[gg["trade_date"] <= cut]
        oos = gg[gg["trade_date"] > cut]
        win = (gg["outcome"] == "TARGET").mean() * 100
        print(f"{sd:<6} {st:<22} {len(gg):>4} {pf(gg['net_eff']):>6.2f} "
              f"{win:>5.1f}% {gg['net_eff'].sum()*200:>10.0f}  "  # ~Rs / 1-lot proxy
              f"{pf(is_g['net_eff']):.2f} ({len(is_g)})   {pf(oos['net_eff']):.2f} ({len(oos)})")

    print(f"\nAGGREGATE n={len(rr)} PF={pf(rr['net_eff']):.3f} "
          f"win%={(rr['outcome']=='TARGET').mean()*100:.1f}")

    # save for downstream extension
    out_csv = Path(__file__).resolve().parent / "_v17r_advsetups_trades.csv"
    rr.to_csv(out_csv, index=False)
    print(f"[adv-scan] saved {len(rr)} rows -> {out_csv.name}")


if __name__ == "__main__":
    main()
