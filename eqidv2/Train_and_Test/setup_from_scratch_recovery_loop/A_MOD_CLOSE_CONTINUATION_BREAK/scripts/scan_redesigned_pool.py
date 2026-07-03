r"""scan_redesigned_pool.py — from-scratch redesigned detection for
A_MOD_CLOSE_CONTINUATION_BREAK (research-only; no live execution).

WHY A REDESIGNED SCAN (from-scratch review findings)
----------------------------------------------------
The production pool for this setup is structurally broken as a research object:
 1. COLLAPSE SHADOWING — candidate_scan keeps ONE candidate per (ticker, 5-min bar)
    with alphabetical tie-break, so A_MOD_BREAK_C1_HIGH (regime != BEAR) absorbs
    every non-BEAR bar. The pool that reached research was 96.8% BEAR-day rows:
    a counter-trend LONG residue that two full campaigns (1,673 iterations) proved
    edgeless. The card's actual pattern was never tested outside bear days.
 2. LATE SCAN START — v2._scan_day loops from bar index max(VWAP_LOOKBACK=20, ...)
    => first scanned bar ~10:55 IST. The entire morning continuation window was
    never scanned for ANY A_MOD signal.
 3. The card idea itself (moderate-impulse bar closing near its high, breaking the
    prior bar's high, above session VWAP, RS-positive, on real volume) is a
    classic trend-continuation pattern whose natural habitat is trending/positive
    tape — exactly what the collapse removed.

This scanner re-detects the SAME card conditions faithfully (same read-layer
features via v2._prepare_5m, same liquidity floors via _passes_common logic, same
quality score, same market-regime context) but:
  * emits EVERY qualifying (ticker, bar) — no cross-setup collapse;
  * scans from the earliest bar where vol_ratio exists (~10:00) instead of 10:55;
  * adds structural flags for two-stage variants (first-break-of-day, fresh-break,
    pullback-then-break) so redesigned versions are mask-searchable.

Causality: every feature comes from bars <= signal bar close (session VWAP is the
causal cumulative; vol_ratio uses the SHIFTED 20-bar mean; prev-day levels only).
Entry remains the NEXT 1-min open + slippage in the evaluation pipeline. Deployment
of any winner requires a flag-gated detector extension (S9/DOC5D pattern).

Usage: py -3.12 scan_redesigned_pool.py [--start 2026-03-01] [--end 2026-07-02]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import time as dtime
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
TT_DIR = HERE.parents[3]
REPO = TT_DIR.parent
for p in (str(REPO), str(TT_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

import avwap_5min_ID_v2_backtesting as v2  # noqa: E402  (faithful read-layer + context)

SETUP = "A_MOD_CLOSE_CONTINUATION_BREAK"
FNAME = "historical_all_available_pre_dedupe_live_candidates.csv"
WARMUP = pd.Timestamp("2026-02-16", tz="Asia/Kolkata")   # ~2 weeks warmup for ATR/vol means
SIG_START_MIN = 9 * 60 + 30
SIG_END_MIN = 14 * 60 + 30
CLOSE_LOC_MIN = 0.75          # card value (detector line 707)
RS_MIN = 0.0
VOL_RATIO_MIN_CARD = 1.4      # dominated by the common 1.5 floor, kept for fidelity
MIN_QS = 6.8


def market_frame() -> pd.DataFrame:
    ctx = v2._load_market_context()
    rows = []
    for day, by_ts in ctx.items():
        for ts, d in by_ts.items():
            rows.append({"date": ts, "market_ret_pct": float(d["market_ret_pct"]),
                         "regime": str(d["regime"])})
    mf = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    print(f"[scan] market context bars: {len(mf)} "
          f"({mf['date'].min()} .. {mf['date'].max()})", flush=True)
    return mf


def scan_ticker(tk: str, path: Path, mf: pd.DataFrame, start_ts, end_ts) -> pd.DataFrame | None:
    try:
        raw = v2._read_ohlcv(path)
    except Exception:
        return None
    raw = raw[(raw["date"] >= WARMUP) & (raw["date"] <= end_ts + pd.Timedelta(days=1))]
    if len(raw) < 60:
        return None
    p = v2._prepare_5m(raw).reset_index(drop=True)

    g = p.groupby("date_only", sort=False)
    day_open = g["open"].transform("first")
    prev_high1 = g["high"].shift(1)
    prev_open1 = g["open"].shift(1)
    prev_close1 = g["close"].shift(1)
    prev_closeloc1 = g["close_loc"].shift(1)
    prev_brk = (prev_close1 > g["high"].shift(2))          # prev bar already closed above ITS prior high
    bar_i = g.cumcount()
    day_size = g["date"].transform("size")
    minutes = p["date"].dt.hour * 60 + p["date"].dt.minute

    close, open_, rng, atr = p["close"], p["open"], p["range"], p["ATR"]
    vol_ratio, close_loc = p["vol_ratio"], p["close_loc"]

    stock_ret = (close / day_open - 1.0) * 100.0

    common = (
        (close >= v2.MIN_PRICE)
        & (p["traded_value_rs"] >= v2.MIN_5M_TRADED_VALUE_RS)
        & ~((minutes >= 600) & (p["day_value_so_far_rs"] < v2.MIN_DAY_VALUE_BY_1000_RS))
        & ~(atr.gt(0) & rng.gt(v2.MAX_CANDLE_RANGE_ATR * atr))
        & vol_ratio.between(v2.VOL_RATIO_MIN, v2.MAX_VOL_RATIO)
    )
    struct = (close > open_) & (close_loc >= CLOSE_LOC_MIN)
    above_vwap = close > p["VWAP"]
    mod_imp = atr.gt(0) & (rng >= 0.60 * atr) & (rng <= 2.20 * atr)
    brk = close > prev_high1

    base = (common & struct & above_vwap & mod_imp & brk
            & (vol_ratio >= VOL_RATIO_MIN_CARD)
            & minutes.between(SIG_START_MIN, SIG_END_MIN)
            & (bar_i < day_size - 1))
    if not base.any():
        return None

    sub = p.loc[base].copy()
    sub["_stock_ret"] = stock_ret[base]
    # market context: exact-timestamp join, then last-known within the day (as _bar_context)
    sub = pd.merge_asof(sub.sort_values("date"), mf, on="date", direction="backward",
                        tolerance=pd.Timedelta(hours=7))
    sub["market_ret_pct"] = sub["market_ret_pct"].fillna(0.0)
    sub["regime"] = sub["regime"].fillna("UNKNOWN")
    sub["rs_pct"] = sub["_stock_ret"] - sub["market_ret_pct"]
    sub = sub[sub["rs_pct"] > RS_MIN]
    if sub.empty:
        return None

    # quality score (v2._score, LONG momentum branch, vectorised)
    vr = sub["vol_ratio"].astype(float)
    cl = sub["close_loc"].astype(float)
    vda = sub["vwap_dist_atr"].astype(float)
    ap = sub["atr_pct"].astype(float)
    qs = (25.0 * sub["rs_pct"].clip(lower=0.0)
          + 12.0 * (vr - 1.0).clip(lower=0.0).clip(upper=4.0)
          + 18.0 * cl.clip(lower=0.0)
          + np.where(vda > 0, 8.0, -10.0)
          + np.where(sub["regime"].isin(["BULL", "TREND", "UNKNOWN"]), 10.0, -18.0))
    qs = qs - np.where(ap > 0.018, 20.0, 0.0) - np.where(vda.abs() > 3.0, 12.0, 0.0)
    sub["quality_score"] = qs.astype(float)
    sub = sub[sub["quality_score"] >= MIN_QS]
    if sub.empty:
        return None

    # structural flags for redesigned variants
    idx = sub.index
    sub["x_bar_i"] = bar_i.reindex(idx)
    sub["x_fresh_break"] = (~prev_brk.reindex(idx).fillna(False)).astype(float)
    sub["x_prev_pullback"] = ((prev_close1.reindex(idx) < prev_open1.reindex(idx))
                              | (prev_closeloc1.reindex(idx) < 0.5)).astype(float)
    order = sub.sort_values("date").groupby(sub["date_only"]).cumcount()
    sub["x_break_rank_day"] = order.astype(float)
    sub["x_first_break_of_day"] = (order == 0).astype(float)

    out = pd.DataFrame({
        "ticker": tk, "side": "LONG", "setup": SETUP,
        "signal_time_ist": sub["date"].map(lambda t: t.isoformat()),
        "scan_slot_ist": sub["date"].map(lambda t: t.strftime("%H:%M")),
        "signal_open": sub["open"], "signal_high": sub["high"],
        "signal_low": sub["low"], "signal_close": sub["close"],
        "signal_volume": sub["volume"],
        "quality_score": sub["quality_score"].round(3),
        "rs_pct": sub["rs_pct"].round(4), "market_ret_pct": sub["market_ret_pct"].round(4),
        "regime": sub["regime"], "vol_ratio": sub["vol_ratio"].round(4),
        "atr_pct": sub["atr_pct"].round(6), "body_pct": sub["body_pct"].round(4),
        "close_loc": sub["close_loc"].round(4), "vwap_dist_atr": sub["vwap_dist_atr"].round(4),
        "reason": "moderate_close_near_high_continuation_redesigned_uncollapsed",
        "status": "CANDIDATE",
        "x_bar_i": sub["x_bar_i"], "x_fresh_break": sub["x_fresh_break"],
        "x_prev_pullback": sub["x_prev_pullback"],
        "x_break_rank_day": sub["x_break_rank_day"],
        "x_first_break_of_day": sub["x_first_break_of_day"],
    })
    ts = pd.to_datetime(out["signal_time_ist"])
    out = out[(ts >= start_ts) & (ts < end_ts + pd.Timedelta(days=1))]
    return out if len(out) else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-03-01")
    ap.add_argument("--end", default="2026-07-02")
    ap.add_argument("--out", default=str(WORK / "pools" / "pool_redesigned"))
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    start_ts = pd.Timestamp(args.start, tz="Asia/Kolkata")
    end_ts = pd.Timestamp(args.end, tz="Asia/Kolkata")
    mf = market_frame()

    files = sorted(v2.DATA_ROOT_5M.glob("*_stocks_indicators_5min.parquet"))
    print(f"[scan] {len(files)} ticker files under {v2.DATA_ROOT_5M}", flush=True)
    frames = []
    t0 = time.time()
    for i, f in enumerate(files, 1):
        tk = f.name.replace("_stocks_indicators_5min.parquet", "").upper()
        if tk in {"NIFTYBEES", "NIFTY", "NIFTY 50", "NIFTY50"}:
            continue
        r = scan_ticker(tk, f, mf, start_ts, end_ts)
        if r is not None:
            frames.append(r)
        if i % 200 == 0:
            n = sum(len(x) for x in frames)
            print(f"[scan] {i}/{len(files)} tickers, {n} signals, {time.time()-t0:.0f}s", flush=True)

    pool = pd.concat(frames, ignore_index=True)
    pool = pool.drop_duplicates(subset=["ticker", "side", "setup", "signal_time_ist"])
    pool = pool.sort_values("signal_time_ist").reset_index(drop=True)

    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)
    pool.to_csv(outdir / FNAME, index=False)
    d = pd.to_datetime(pool["signal_time_ist"]).dt.date.astype(str)
    sessions = sorted(d.unique())
    manifest = {
        "setup": SETUP, "generator": "scan_redesigned_pool.py (uncollapsed, faithful card conditions)",
        "data_root": str(v2.DATA_ROOT_5M),
        "requested_range": [args.start, args.end],
        "rows": len(pool), "n_tickers": int(pool["ticker"].nunique()),
        "n_sessions": len(sessions), "first_session": sessions[0], "last_session": sessions[-1],
        "regime_mix": pool["regime"].value_counts().to_dict(),
        "per_day_median": float(d.value_counts().median()),
        "sessions": sessions,
    }
    (outdir / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[scan] wrote {outdir / FNAME} rows={len(pool)} sessions={len(sessions)} "
          f"regimes={manifest['regime_mix']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
