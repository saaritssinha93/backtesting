r"""enrich_pool_features.py — add a full causal indicator/price-action feature set to the
recreated A_MOD_CLOSE_CONTINUATION_BREAK pool (research-only).

Why: the raw pool rows only carry 11 usable signal features. The 5-min parquet store has
more indicators, but several stored columns (MACD/BB/CCI/MFI/OBV/VWAP/prev-day) are ONLY
populated in the historical vintage (0% in June 2026) and stored Stochastic is ~40% in
TRAIN — mixing vintages would make TRAIN and TEST see different data. So every indicator
below is recomputed HERE from OHLCV, vectorised, using bars up to and including the signal
bar only (causal; entry remains the NEXT 1-min open). Stored RSI/ATR/ADX/EMA20/50/200 are
kept as well (96-100% coverage in both windows).

Features are prefixed x_ to avoid any collision with pipeline columns.

Also precomputes the 8 repo pre-momentum features per row (at SL 0.70) as x_pm_* columns
so the search can treat them as cheap mask columns; any final candidate using x_pm_* terms
must be re-verified through the true pre_momentum_terms path (done by the campaign).

Usage: py -3.12 enrich_pool_features.py [--pool pools/pool_full] [--out pools/pool_enriched]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
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

DATA_5M = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
FNAME = "historical_all_available_pre_dedupe_live_candidates.csv"
WARMUP_START = pd.Timestamp("2026-02-01", tz="Asia/Kolkata")


def _ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()


def compute_features(bars: pd.DataFrame) -> pd.DataFrame:
    """bars: date/open/high/low/close/volume (+ stored RSI/ATR/ADX/EMA_*), ascending.
    All features use data up to and including each bar (shift-free causal at bar close)."""
    b = bars.reset_index(drop=True).copy()
    c, h, l, v, o = b["close"], b["high"], b["low"], b["volume"], b["open"]
    day = b["date"].dt.normalize()

    out = pd.DataFrame(index=b.index)
    out["date"] = b["date"]

    # --- stored (validated coverage) ---
    out["x_rsi"] = pd.to_numeric(b.get("RSI"), errors="coerce")
    out["x_adx"] = pd.to_numeric(b.get("ADX"), errors="coerce")
    atr = pd.to_numeric(b.get("ATR"), errors="coerce")
    # fallback ATR(14) where stored is missing
    tr = pd.concat([(h - l), (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    atr = atr.fillna(tr.rolling(14).mean())
    out["x_atr_pct"] = atr / c
    ema20 = pd.to_numeric(b.get("EMA_20"), errors="coerce").fillna(_ema(c, 20))
    ema50 = pd.to_numeric(b.get("EMA_50"), errors="coerce").fillna(_ema(c, 50))
    ema200 = pd.to_numeric(b.get("EMA_200"), errors="coerce").fillna(_ema(c, 200))

    # --- trend / MA structure ---
    out["x_rsi_slope3"] = out["x_rsi"] - out["x_rsi"].shift(3)
    out["x_adx_slope3"] = out["x_adx"] - out["x_adx"].shift(3)
    out["x_ema20_dist_atr"] = (c - ema20) / atr
    out["x_ema50_dist_atr"] = (c - ema50) / atr
    out["x_ema200_dist_atr"] = (c - ema200) / atr
    out["x_ema20_gt50"] = (ema20 > ema50).astype(float)
    out["x_ema_stack"] = ((ema20 > ema50) & (ema50 > ema200)).astype(float)
    out["x_ema20_slope3_atr"] = (ema20 - ema20.shift(3)) / atr

    # --- MACD (12,26,9) ---
    macd = _ema(c, 12) - _ema(c, 26)
    sig = _ema(macd, 9)
    out["x_macd_hist_atr"] = (macd - sig) / atr
    out["x_macd_hist_delta_atr"] = ((macd - sig) - (macd - sig).shift(1)) / atr
    out["x_macd_above_sig"] = (macd > sig).astype(float)

    # --- Bollinger (20,2) / Keltner (20, 1.5 ATR) ---
    sma20 = c.rolling(20).mean()
    sd20 = c.rolling(20).std()
    out["x_bb_pos"] = (c - (sma20 - 2 * sd20)) / (4 * sd20)
    out["x_bb_width_pct"] = (4 * sd20) / c
    out["x_kelt_pos"] = (c - (ema20 - 1.5 * atr)) / (3 * atr)

    # --- oscillators ---
    ll14, hh14 = l.rolling(14).min(), h.rolling(14).max()
    k = 100 * (c - ll14) / (hh14 - ll14)
    out["x_stoch_k"] = k
    out["x_stoch_d"] = k.rolling(3).mean()
    out["x_willr"] = -100 * (hh14 - c) / (hh14 - ll14)
    tp = (h + l + c) / 3.0
    tp_sma20 = tp.rolling(20).mean()
    # vectorised mean-deviation approximation (|tp - sma20| smoothed) — standard fast CCI
    mad20 = (tp - tp_sma20).abs().rolling(20).mean()
    out["x_cci20"] = (tp - tp_sma20) / (0.015 * mad20.replace(0, np.nan))
    # MFI(14)
    mf = tp * v
    pos_mf = mf.where(tp > tp.shift(), 0.0).rolling(14).sum()
    neg_mf = mf.where(tp < tp.shift(), 0.0).rolling(14).sum()
    out["x_mfi14"] = 100 - 100 / (1 + pos_mf / neg_mf.replace(0, np.nan))
    # OBV slope (in volume units, normalised by 20-bar avg volume)
    obv = (np.sign(c.diff().fillna(0.0)) * v).cumsum()
    out["x_obv_slope5"] = (obv - obv.shift(5)) / v.rolling(20).mean()

    # --- momentum / ROC ---
    out["x_roc3"] = c.pct_change(3) * 100
    out["x_roc6"] = c.pct_change(6) * 100
    out["x_roc12"] = c.pct_change(12) * 100

    # --- candle / range structure ---
    rng = (h - l)
    out["x_range_vs_avg20"] = rng / rng.rolling(20).mean()
    up = np.sign(c - o)
    out["x_consec_up3"] = (pd.Series(up).rolling(3).apply(lambda x: (x > 0).sum(), raw=True))
    out["x_vol_vs_avg20"] = v / v.rolling(20).mean()

    # --- session/day context (causal groupby-cum within day) ---
    g = b.groupby(day, sort=False)
    day_open = g["open"].transform("first")
    day_high = g["high"].cummax()
    day_low = g["low"].cummin()
    cum_v = g["volume"].cumsum()
    cum_tpv = (tp * v).groupby(day, sort=False).cumsum()
    svwap = cum_tpv / cum_v.replace(0, np.nan)
    out["x_svwap_dist_atr"] = (c - svwap) / atr
    out["x_svwap_dist_pct"] = (c / svwap - 1) * 100
    out["x_day_ret_pct"] = (c / day_open - 1) * 100
    out["x_dist_dayhigh_atr"] = (day_high - c) / atr        # 0 = closing AT the day high
    out["x_dayrange_atr"] = (day_high - day_low) / atr
    out["x_pos_in_dayrange"] = (c - day_low) / (day_high - day_low)
    out["x_bar_idx"] = g.cumcount().astype(float)
    # opening range = first 3 bars (09:15/09:20/09:25)
    bar_i = g.cumcount()
    orh = h.where(bar_i <= 2).groupby(day).transform("max")
    out["x_orh_dist_atr"] = (c - orh) / atr
    # prev-day levels
    d_high = g["high"].transform("max")     # full-day high (only used via shift to PRIOR day)
    d_low = g["low"].transform("min")
    d_close = g["close"].transform("last")
    day_key = day
    per_day = pd.DataFrame({"day": day_key, "dh": d_high, "dl": d_low, "dc": d_close}).drop_duplicates("day")
    per_day["pdh"] = per_day["dh"].shift(1)
    per_day["pdl"] = per_day["dl"].shift(1)
    per_day["pdc"] = per_day["dc"].shift(1)
    m = day_key.map(per_day.set_index("day")[["pdh", "pdl", "pdc"]].to_dict("index"))
    pdh = pd.Series([x["pdh"] if isinstance(x, dict) else np.nan for x in m], index=b.index)
    pdl = pd.Series([x["pdl"] if isinstance(x, dict) else np.nan for x in m], index=b.index)
    pdc = pd.Series([x["pdc"] if isinstance(x, dict) else np.nan for x in m], index=b.index)
    out["x_pdh_dist_atr"] = (c - pdh) / atr
    out["x_pdl_dist_atr"] = (c - pdl) / atr
    out["x_gap_pct"] = (day_open / pdc - 1) * 100
    out["x_above_pdh"] = (c > pdh).astype(float)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=str(WORK / "pools" / "pool_full"))
    ap.add_argument("--out", default=str(WORK / "pools" / "pool_enriched"))
    ap.add_argument("--premom", action="store_true", default=True)
    ap.add_argument("--no-premom", dest="premom", action="store_false")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    pool = pd.read_csv(Path(args.pool) / FNAME, low_memory=False)
    ts = pd.to_datetime(pool["signal_time_ist"], errors="coerce")
    ts = ts.dt.tz_localize("Asia/Kolkata") if ts.dt.tz is None else ts.dt.tz_convert("Asia/Kolkata")
    pool["_sig_ts"] = ts
    print(f"[enrich] pool rows {len(pool)} tickers {pool['ticker'].nunique()}")

    feats_all = []
    t0 = time.time()
    tickers = sorted(pool["ticker"].astype(str).str.upper().unique())
    for i, tk in enumerate(tickers, 1):
        f = DATA_5M / f"{tk}_stocks_indicators_5min.parquet"
        if not f.exists():
            continue
        try:
            bars = pd.read_parquet(f, columns=["date", "open", "high", "low", "close", "volume",
                                               "RSI", "ATR", "ADX", "EMA_20", "EMA_50", "EMA_200"])
        except Exception:
            try:
                bars = pd.read_parquet(f, columns=["date", "open", "high", "low", "close", "volume"])
            except Exception:
                continue
        bars = bars[bars["date"] >= WARMUP_START].sort_values("date")
        if bars.empty:
            continue
        fx = compute_features(bars)
        fx.insert(0, "ticker", tk)
        want = pool.loc[pool["ticker"].astype(str).str.upper() == tk, "_sig_ts"]
        fx = fx[fx["date"].isin(set(want))]
        if not fx.empty:
            feats_all.append(fx)
        if i % 200 == 0:
            print(f"[enrich] {i}/{len(tickers)} tickers, {time.time()-t0:.0f}s", flush=True)

    fdf = pd.concat(feats_all, ignore_index=True)
    fdf = fdf.drop_duplicates(subset=["ticker", "date"], keep="first")
    print(f"[enrich] feature rows {len(fdf)}")

    merged = pool.merge(fdf, left_on=["ticker", "_sig_ts"], right_on=["ticker", "date"],
                        how="left", suffixes=("", "_xdup"))
    merged = merged.drop(columns=[c for c in ("date_xdup", "_sig_ts") if c in merged.columns])
    xcols = [c for c in merged.columns if c.startswith("x_")]
    cov = {c: round(float(pd.to_numeric(merged[c], errors='coerce').notna().mean()) * 100, 1) for c in xcols}
    print("[enrich] coverage %:", json.dumps(cov, indent=0))

    # --- premom features as x_pm_* columns (search-only; re-verified via true premom path) ---
    if args.premom:
        import setup_train_test as tt
        tt.POOL_DIRS = [str(Path(args.pool))]
        tt.SLIPPAGE_BPS = 15.0
        lp = tt.load_pool()
        lp = lp[lp["setup"].astype(str).eq("A_MOD_CLOSE_CONTINUATION_BREAK")]
        lp = tt.attach_entries(lp)
        pm_feats = ["pre_entry_momentum_score", "sig5_adx_calc", "sig5_rsi_dir", "sig5_vol_ratio20",
                    "pre1_adx", "pre3_range_r", "pre5_mom_r", "pre3_close_pos"]
        recs = []
        for j, r in enumerate(lp.itertuples(), 1):
            feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), 0.70,
                                       r.tt_sig_ts.isoformat())
            fd = dict(feats) if not reason else {}
            recs.append({"ticker": r.ticker, "signal_time_ist": r.signal_time_ist,
                         **{f"x_pm_{k}": fd.get(k, np.nan) for k in pm_feats}})
            if j % 1000 == 0:
                print(f"[enrich] premom {j}/{len(lp)} {time.time()-t0:.0f}s", flush=True)
        pmdf = pd.DataFrame(recs).drop_duplicates(subset=["ticker", "signal_time_ist"])
        merged = merged.merge(pmdf, on=["ticker", "signal_time_ist"], how="left")
        pmcols = [c for c in merged.columns if c.startswith("x_pm_")]
        print("[enrich] premom coverage %:",
              {c: round(float(merged[c].notna().mean()) * 100, 1) for c in pmcols})

    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)
    merged.to_csv(outdir / FNAME, index=False)
    man = {"src_pool": str(args.pool), "rows": len(merged),
           "x_feature_coverage_pct": cov, "generated": pd.Timestamp.now().isoformat()}
    (outdir / "_manifest.json").write_text(json.dumps(man, indent=2), encoding="utf-8")
    print(f"[enrich] wrote {outdir / FNAME}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
