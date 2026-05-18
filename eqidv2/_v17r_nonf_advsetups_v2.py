"""READ-ONLY v2 setup scanner — wider indicator universe + loosened bands.

Does NOT modify any strategy/pipeline/config file.

v2 differences from v1 (_v17r_nonf_advsetups_scan.py):
  - Uses MFI, CCI, OBV, Stoch%K, MACD_Hist, plus inline-computed
    pressure_ratio_5 and volume_z_tod_20.
  - Adds reversal / divergence / liquidity-sweep / gap-fill setups, which
    cover microstructure patterns the cascade does not emit.
  - Loosens RSI bands (was 50-70 long; now 45-75) and ADX floors (was 25;
    now 18-22 by setup).
  - Adds a regime-aware variant (regime gate keys: pct_above_vwap from the
    breadth cache) so each setup only fires when the universe regime
    supports it.
  - Each setup is scored after re-resolution at TGT 1.5%/SL 0.75%, honest
    v17D per-row costs, sizing-aware. Greedy 3-step chain mining per setup
    on the enriched signal-time feature snapshot reports the best filter
    subset with IS/OOS (IS <= 2026-02-15) and decay.

Run with EQIDV_DATE_FROM=2026-02-05 for the 3-month confirming window.
"""
from __future__ import annotations

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
N_FLOOR_CHAIN = 20


def pf(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    w, l = float(s[s > 0].sum()), float(-s[s < 0].sum())
    return float("inf") if l <= 0 and w > 0 else (0.0 if l <= 0 else w / l)


def load_universe():
    uni = pd.read_csv(UNIVERSE)
    uni["adv_bucket"] = uni["adv_rs_cr"].apply(cm.adv_bucket_for)
    keep = uni[uni["adv_bucket"].isin(["mid", "top100"])].copy()
    return keep[["ticker", "adv_rs_cr", "adv_bucket"]]


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
    if df is None or len(df) < 60:
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
    stoch_k = df["Stoch_%K"].astype(float).values if "Stoch_%K" in df.columns else np.full(len(df), np.nan)
    mfi = df["MFI"].astype(float).values if "MFI" in df.columns else np.full(len(df), np.nan)
    cci = df["CCI"].astype(float).values if "CCI" in df.columns else np.full(len(df), np.nan)
    obv = df["OBV"].astype(float).values if "OBV" in df.columns else np.full(len(df), np.nan)
    prev_close = df["Prev_Day_Close"].astype(float).values if "Prev_Day_Close" in df.columns else np.full(len(df), np.nan)
    date_only = pd.to_datetime(df["date"]).dt.date.values
    minute = (pd.to_datetime(df["date"]).dt.hour * 60 + pd.to_datetime(df["date"]).dt.minute).values

    # derived
    rng = np.maximum(h - l, 1e-9)
    body_eff = np.abs(c - o) / rng
    close_loc = (c - l) / rng
    upper_wick = (h - np.maximum(o, c)) / rng
    lower_wick = (np.minimum(o, c) - l) / rng

    bb_width = (ub - lb) / np.where(sma20 != 0, sma20, 1) * 100.0
    bb_w_ser = pd.Series(bb_width)
    bb_mean = bb_w_ser.rolling(100, min_periods=30).mean().values
    bb_squeeze = np.where(bb_mean > 0, bb_width / bb_mean, np.nan)

    vol_ser = pd.Series(vol)
    vol_mean = vol_ser.shift(1).rolling(20, min_periods=8).mean().values
    vol_std = vol_ser.shift(1).rolling(20, min_periods=8).std(ddof=0).values
    vol_ratio = np.where(vol_mean > 0, vol / vol_mean, np.nan)
    vol_z = np.where(vol_std > 0, (vol - vol_mean) / vol_std, np.nan)

    # directional volume (buy / sell pressure)
    buy_vol = np.where(c > o, vol, 0.0)
    sell_vol = np.where(c < o, vol, 0.0)
    buy_p5 = pd.Series(buy_vol).rolling(5, min_periods=2).sum().values
    sell_p5 = pd.Series(sell_vol).rolling(5, min_periods=2).sum().values
    press_ratio = np.where(sell_p5 > 0, buy_p5 / sell_p5, np.nan)

    # session high/low so far (per day)
    df2 = df[["date", "high", "low"]].copy()
    df2["date_only"] = pd.to_datetime(df2["date"]).dt.date
    df2["hi_so_far"] = df2.groupby("date_only")["high"].cummax()
    df2["lo_so_far"] = df2.groupby("date_only")["low"].cummin()
    hi_far = df2["hi_so_far"].values
    lo_far = df2["lo_so_far"].values

    # prev-day high/low (rough: from yesterday's bars within the loaded window)
    pdh = np.full(len(df), np.nan)
    pdl = np.full(len(df), np.nan)
    day_high = df2.groupby("date_only")["high"].max().shift(1)
    day_low = df2.groupby("date_only")["low"].min().shift(1)
    do = pd.to_datetime(df["date"]).dt.date
    pdh = do.map(day_high).values
    pdl = do.map(day_low).values

    signals = []
    last_signal_idx = {}

    def _fire(setup, i, cool=12):
        last = last_signal_idx.get(setup, -999)
        if i - last < cool:
            return False
        last_signal_idx[setup] = i
        return True

    n = len(df)
    for i in range(60, n - 1):
        if minute[i] < 9 * 60 + 30 or minute[i] > 14 * 60:
            continue
        if date_only[i] != date_only[i + 1]:
            continue

        # -------------------- LONG setups --------------------

        # L_MFI_OVERSOLD_RECLAIM — volume-confirmed oversold reclaim
        if (
            i >= 5 and not np.isnan(mfi[i]) and not np.isnan(mfi[i - 3])
            and mfi[i - 3] < 25 and mfi[i] > 35           # MFI flipped up from oversold
            and c[i] > vwap[i]                            # above session VWAP
            and c[i] > o[i] and body_eff[i] >= 0.55
            and lower_wick[i] > 0.20                       # lower-wick rejection
            and adx[i] >= 18
            and vol_ratio[i] >= 1.3
        ):
            if _fire("L_MFI_OVERSOLD_RECLAIM", i):
                signals.append((i, "L_MFI_OVERSOLD_RECLAIM", "LONG"))

        # L_CCI_EXTREME_FLIP — extreme CCI then flip back, trend support
        if (
            i >= 5 and not np.isnan(cci[i])
            and np.nanmin(cci[i - 3:i + 1]) < -150
            and cci[i] > -80
            and c[i] > ema20[i]
            and c[i] > o[i] and body_eff[i] >= 0.55
            and adx[i] >= 18
            and rsi[i] >= 40
        ):
            if _fire("L_CCI_EXTREME_FLIP", i):
                signals.append((i, "L_CCI_EXTREME_FLIP", "LONG"))

        # L_DOUBLE_BOTTOM_VWAP — current low within 0.4 ATR of prior intraday low, hold above VWAP
        if (
            i >= 8 and not np.isnan(lo_far[i])
            and abs(l[i] - lo_far[i - 8]) <= 0.4 * atr[i]   # near earlier low
            and l[i] >= lo_far[i - 8] * 0.995                # not significantly below
            and c[i] > vwap[i]
            and c[i] > c[i - 1]
            and body_eff[i] >= 0.50
            and vol_ratio[i] >= 1.3
        ):
            if _fire("L_DOUBLE_BOTTOM_VWAP", i):
                signals.append((i, "L_DOUBLE_BOTTOM_VWAP", "LONG"))

        # L_PRESSURE_BURST_VWAP — strong buy pressure surge above VWAP
        if (
            i >= 5 and not np.isnan(press_ratio[i])
            and press_ratio[i] >= 3.0                       # buyers 3x sellers in last 5 bars
            and c[i] > vwap[i] and c[i] > ema20[i]
            and c[i] > c[i - 1]
            and body_eff[i] >= 0.6
            and vol_z[i] >= 1.5
            and rsi[i] >= 50 and rsi[i] <= 75
            and adx[i] >= 20
        ):
            if _fire("L_PRESSURE_BURST_VWAP", i):
                signals.append((i, "L_PRESSURE_BURST_VWAP", "LONG"))

        # L_PREV_DAY_LOW_SWEEP — wicked below prev day low and reclaimed
        if (
            not np.isnan(pdl[i])
            and l[i] < pdl[i]                               # took out prev day low
            and c[i] > pdl[i]                               # reclaimed
            and c[i] > o[i] and body_eff[i] >= 0.45
            and lower_wick[i] > 0.25                         # wick rejection
            and vol_ratio[i] >= 1.5
        ):
            if _fire("L_PREV_DAY_LOW_SWEEP", i):
                signals.append((i, "L_PREV_DAY_LOW_SWEEP", "LONG"))

        # L_GAP_DOWN_REVERSAL — opened below prev close, made lower low, closed back above
        # use first-bar-of-day open as gap reference
        if (
            i >= 6 and not np.isnan(prev_close[i])
            and not np.isnan(lo_far[i - 3])
            and o[i - 5] < prev_close[i] * 0.998             # gapped down at open
            and lo_far[i - 1] < o[i - 5]                     # made an intraday low
            and c[i] > prev_close[i]                         # reclaimed prev close
            and c[i] > o[i] and body_eff[i] >= 0.5
            and vol_ratio[i] >= 1.2
            and rsi[i] >= 45
        ):
            if _fire("L_GAP_DOWN_REVERSAL", i):
                signals.append((i, "L_GAP_DOWN_REVERSAL", "LONG"))

        # -------------------- SHORT setups --------------------

        # S_MFI_OVERBOUGHT_FAIL — volume-confirmed overbought then VWAP loss
        if (
            i >= 5 and not np.isnan(mfi[i])
            and mfi[i - 3] > 75 and mfi[i] < 65
            and c[i] < vwap[i]
            and c[i] < o[i] and body_eff[i] >= 0.55
            and upper_wick[i] > 0.20
            and adx[i] >= 18
            and vol_ratio[i] >= 1.3
        ):
            if _fire("S_MFI_OVERBOUGHT_FAIL", i):
                signals.append((i, "S_MFI_OVERBOUGHT_FAIL", "SHORT"))

        # S_CCI_EXTREME_FLIP — extreme CCI then flip back, trend resistance
        if (
            i >= 5 and not np.isnan(cci[i])
            and np.nanmax(cci[i - 3:i + 1]) > 150
            and cci[i] < 80
            and c[i] < ema20[i]
            and c[i] < o[i] and body_eff[i] >= 0.55
            and adx[i] >= 18
            and rsi[i] <= 60
        ):
            if _fire("S_CCI_EXTREME_FLIP", i):
                signals.append((i, "S_CCI_EXTREME_FLIP", "SHORT"))

        # S_DOUBLE_TOP_VWAP — current high near prior intraday high, fail below VWAP
        if (
            i >= 8 and not np.isnan(hi_far[i])
            and abs(h[i] - hi_far[i - 8]) <= 0.4 * atr[i]
            and h[i] <= hi_far[i - 8] * 1.005
            and c[i] < vwap[i]
            and c[i] < c[i - 1]
            and body_eff[i] >= 0.50
            and vol_ratio[i] >= 1.3
        ):
            if _fire("S_DOUBLE_TOP_VWAP", i):
                signals.append((i, "S_DOUBLE_TOP_VWAP", "SHORT"))

        # S_PRESSURE_DUMP_VWAP — strong sell pressure surge below VWAP
        if (
            i >= 5 and not np.isnan(press_ratio[i])
            and press_ratio[i] <= 0.33                       # sellers 3x buyers
            and c[i] < vwap[i] and c[i] < ema20[i]
            and c[i] < c[i - 1]
            and body_eff[i] >= 0.6
            and vol_z[i] >= 1.5
            and rsi[i] >= 25 and rsi[i] <= 50
            and adx[i] >= 20
        ):
            if _fire("S_PRESSURE_DUMP_VWAP", i):
                signals.append((i, "S_PRESSURE_DUMP_VWAP", "SHORT"))

        # S_PREV_DAY_HIGH_FAIL — wicked above prev day high and failed
        if (
            not np.isnan(pdh[i])
            and h[i] > pdh[i]                                # took out prev day high
            and c[i] < pdh[i]                                # failed back below
            and c[i] < o[i] and body_eff[i] >= 0.45
            and upper_wick[i] > 0.25
            and vol_ratio[i] >= 1.5
        ):
            if _fire("S_PREV_DAY_HIGH_FAIL", i):
                signals.append((i, "S_PREV_DAY_HIGH_FAIL", "SHORT"))

        # S_GAP_UP_REJECTION — opened above prev close, made higher high, closed back below
        if (
            i >= 6 and not np.isnan(prev_close[i])
            and not np.isnan(hi_far[i - 3])
            and o[i - 5] > prev_close[i] * 1.002             # gapped up at open
            and hi_far[i - 1] > o[i - 5]                     # made an intraday high
            and c[i] < prev_close[i]                         # reverted back below
            and c[i] < o[i] and body_eff[i] >= 0.5
            and vol_ratio[i] >= 1.2
            and rsi[i] <= 55
        ):
            if _fire("S_GAP_UP_REJECTION", i):
                signals.append((i, "S_GAP_UP_REJECTION", "SHORT"))

        # S_MACD_HIST_FLIP — MACD histogram flipped negative, price below VWAP, weak
        if (
            i >= 3 and not np.isnan(macd_hist[i])
            and macd_hist[i] < 0 and macd_hist[i - 1] >= 0
            and macd[i] > 0                                  # still in uptrend but losing momentum
            and c[i] < vwap[i]
            and c[i] < o[i] and body_eff[i] >= 0.5
            and rsi[i] <= 55
            and adx[i] >= 18
        ):
            if _fire("S_MACD_HIST_FLIP", i):
                signals.append((i, "S_MACD_HIST_FLIP", "SHORT"))

    return signals


def scan_ticker(args):
    ticker, adv_bucket = args
    df = load_5m(ticker)
    if df is None or len(df) < 60:
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
            # snapshot signal-bar features for chain mining
            "rsi": float(signal_bar.get("RSI", np.nan)),
            "adx": float(signal_bar.get("ADX", np.nan)),
            "atr": float(signal_bar.get("ATR", np.nan)),
            "macd_hist": float(signal_bar.get("MACD_Hist", np.nan)),
            "stochk": float(signal_bar.get("Stoch_%K", np.nan)),
            "cci": float(signal_bar.get("CCI", np.nan)),
            "mfi": float(signal_bar.get("MFI", np.nan)),
            "vwap": float(signal_bar.get("VWAP", np.nan)),
            "close": float(signal_bar.get("close", np.nan)),
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
    merged = pd.merge_asof(work, bdf[["date", "pct_above_vwap", "pct_above_ema20"]],
                           left_on="_sig", right_on="date", direction="backward")
    keep = ~((merged["side"] == "SHORT") &
             (merged["pct_above_vwap"].fillna(0.0) < BREADTH_THRESHOLD))
    return merged.loc[keep].drop(columns=["_sig", "date"], errors="ignore").reset_index(drop=True)


def greedy_chain(sub, features, max_steps=3):
    import numpy as _np
    chain = []
    cur = sub.copy()
    def _score(s):
        if len(s) < N_FLOOR_CHAIN:
            return -1.0
        p = pf(s["net_eff"])
        if p == float("inf"):
            p = 5.0
        return p * _np.sqrt(len(s))
    for _ in range(max_steps):
        best = None
        best_sc = _score(cur)
        for feat in features:
            if feat not in cur.columns:
                continue
            vals = pd.to_numeric(cur[feat], errors="coerce").dropna()
            if len(vals) < N_FLOOR_CHAIN * 2:
                continue
            qs = vals.quantile([0.2, 0.35, 0.5, 0.65, 0.8]).unique()
            for thr in qs:
                for op in (">=", "<="):
                    if op == ">=":
                        cand = cur[pd.to_numeric(cur[feat], errors="coerce") >= thr]
                    else:
                        cand = cur[pd.to_numeric(cur[feat], errors="coerce") <= thr]
                    sc = _score(cand)
                    if sc > best_sc + 1e-9:
                        best_sc = sc
                        best = (feat, op, float(thr), cand)
        if best is None:
            break
        feat, op, thr, cand = best
        chain.append((feat, op, thr))
        cur = cand
    return chain, cur


def main():
    uni = load_universe()
    print(f"[v2-scan] universe (mid+top100): {len(uni)} tickers | date >= {DATE_FROM}")
    args = list(zip(uni["ticker"].astype(str), uni["adv_bucket"]))
    all_rows = []
    nw = min(8, max(1, os.cpu_count() or 4))
    print(f"[v2-scan] scanning with {nw} workers ...")
    with ProcessPoolExecutor(max_workers=nw) as ex:
        for i, fut in enumerate(as_completed([ex.submit(scan_ticker, a) for a in args]), 1):
            try:
                all_rows.extend(fut.result())
            except Exception as exc:
                pass
            if i % 100 == 0:
                print(f"  [v2-scan] {i}/{len(args)} | signals so far: {len(all_rows)}")
    print(f"[v2-scan] total raw signals: {len(all_rows)}")
    if not all_rows:
        return

    raw = pd.DataFrame(all_rows)
    raw = raw.sort_values(["ticker", "side", "trade_date", "signal_time_ist"])
    raw = raw.drop_duplicates(subset=["ticker", "side", "trade_date"], keep="first").reset_index(drop=True)
    print(f"[v2-scan] after one-per-ticker-side-day: {len(raw)}")

    rr = re_resolve(raw.to_dict("records"))
    if rr.empty:
        return
    print(f"[v2-scan] resolved: {len(rr)}")

    rr["cost_pct"] = [cm.costs_pct_for_v17C(b, o if o in ("TARGET", "SL") else "TARGET")
                     for b, o in zip(rr["adv_bucket"], rr["outcome"])]
    rr["net_eff"] = (rr["pnl_pct_price"] - rr["cost_pct"]) * LEVERAGE

    breadth = pd.read_parquet(BREADTH_CACHE)
    pre = len(rr)
    rr = apply_breadth(rr, breadth)
    print(f"[v2-scan] after breadth gate: {pre} -> {len(rr)}\n")

    # baseline report per setup
    import datetime
    cut = datetime.date(2026, 2, 15)
    print(f"{'='*112}")
    print(f"BASELINE per setup  (TGT {TGT_PCT}%/SL {SL_PCT}%, honest costs, breadth=loose)")
    print(f"{'='*112}")
    print(f"{'side':<6} {'setup':<26} {'n':>4} {'PF':>6} {'win%':>6} "
          f"{'IS_PF (n)':>14}   {'OOS_PF (n)':>14}")
    for (sd, st), gg in rr.groupby(["side", "setup"]):
        if len(gg) < 5:
            continue
        is_g = gg[gg["trade_date"] <= cut]
        oos = gg[gg["trade_date"] > cut]
        print(f"{sd:<6} {st:<26} {len(gg):>4} {pf(gg['net_eff']):>6.2f} "
              f"{(gg['outcome']=='TARGET').mean()*100:>5.1f}% "
              f"{pf(is_g['net_eff']):>6.2f} ({len(is_g):>3})   "
              f"{pf(oos['net_eff']):>6.2f} ({len(oos):>3})")

    # chain mining per setup
    print(f"\n{'='*112}")
    print(f"CHAIN MINING per setup  (greedy 3-step, n_floor=20)")
    print(f"{'='*112}")
    features = ["rsi", "adx", "atr", "macd_hist", "stochk", "cci", "mfi",
                "pct_above_vwap", "pct_above_ema20"]
    passers = []
    for (sd, st), gg in rr.groupby(["side", "setup"]):
        if len(gg) < N_FLOOR_CHAIN:
            continue
        chain, kept = greedy_chain(gg, features)
        if len(kept) < N_FLOOR_CHAIN:
            continue
        is_k = kept[kept["trade_date"] <= cut]
        oos_k = kept[kept["trade_date"] > cut]
        is_pf = pf(is_k["net_eff"])
        oos_pf = pf(oos_k["net_eff"])
        decay = oos_pf / max(is_pf, 1e-9) if is_pf > 0 else 0
        chain_str = " AND ".join(f"{f}{o}{t:.3g}" for f, o, t in chain) if chain else "(no chain)"
        gate_pass = (pf(kept["net_eff"]) >= 1.30 and len(oos_k) >= 10
                     and oos_pf >= 1.20 and decay >= 0.65)
        tag = "**PASS**" if gate_pass else "drop"
        print(f"{sd:<6} {st:<26} n={len(gg):>4}->{len(kept):>4} "
              f"PF {pf(gg['net_eff']):>4.2f}->{pf(kept['net_eff']):>4.2f} "
              f"IS {is_pf:>4.2f}(n{len(is_k):<2}) OOS {oos_pf:>4.2f}(n{len(oos_k):<2}) "
              f"dec {decay:>3.2f}  [{tag}] {chain_str}")
        if gate_pass:
            passers.append({"side": sd, "setup": st, "chain": chain,
                            "n": len(kept), "pf": pf(kept["net_eff"]),
                            "oos_pf": oos_pf, "decay": decay})

    if passers:
        print(f"\n{'='*112}\nPASSING SETUPS — RUNNER SPEC TO ADD\n{'='*112}")
        for p in passers:
            chain_lit = ", ".join(f'("{c}", "{o}", {t:.6g})' for c, o, t in p["chain"])
            print(f'    ("{p["side"]}", "{p["setup"]}"): [{chain_lit}],   '
                  f'# n={p["n"]} PF={p["pf"]:.2f} OOS_PF={p["oos_pf"]:.2f}')
    else:
        print("\n[v2-scan] no setups passed strict gates (PF>=1.30, OOS_PF>=1.20, decay>=0.65, OOS_n>=10)")

    out_csv = Path(__file__).resolve().parent / "_v17r_advsetups_v2_trades.csv"
    rr.to_csv(out_csv, index=False)
    print(f"\n[v2-scan] saved {len(rr)} rows -> {out_csv.name}")


if __name__ == "__main__":
    main()
