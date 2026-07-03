r"""enrich_pool.py — ROUND 2: join point-in-time indicator / price-action features
onto the B_AVWAP_RECLAIM_REVERSAL recreated pool at the signal bar.

RESEARCH-ONLY. Reads pools/B_AVWAP_RECLAIM_REVERSAL/, writes pools/B_AVWAP_RECLAIM_REVERSAL_enriched/ (same
filename convention). Never touches conf or repo pools.

All features are computed from the SAME production 5-min indicator parquets
(stocks_indicators_5min_eq_live2) using only bars at or before the signal bar
(rolling windows / prev-day aggregates -> no lookahead). A live wiring of any
candidate that uses these features would read the same columns from the live
5-min feed at signal time (documented promotion caveat: the current conf gate
only sees scanner-emitted candidate fields, so the gate would need a small
extension to look up indicator columns at apply time).

Run from repo root:
  py -3.12 Train_and_Test\setup_pf_1_4_full_loop\B_AVWAP_RECLAIM_REVERSAL\scripts\enrich_pool.py
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

SETUP = "B_AVWAP_RECLAIM_REVERSAL"
_HERE = Path(__file__).resolve().parent
WORK = _HERE.parent
POOLS = WORK / "pools"
SRC = POOLS / SETUP / "historical_all_available_pre_dedupe_live_candidates.csv"
OUT_DIR = POOLS / (SETUP + "_enriched")
DATA_5M = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")

FEATS = ['rsi', 'rsi_slope3', 'adx5', 'adx5_slope3', 'mfi', 'cci', 'stoch_k', 'stoch_d', 'stoch_kd', 'macd_atr', 'macd_sig_atr', 'macd_hist_atr', 'bb_pos', 'bb_width_pct', 'ema20_dist_atr', 'ema50_dist_atr', 'ema200_dist_atr', 'ema_stack_atr', 'ema20_slope3_atr', 'sma20_dist_atr', 'roc5_pct', 'willr14', 'obv_slope10_norm', 'vol_z20', 'pressure5', 'candle_range_atr', 'rechigh_dist_atr', 'reclow_dist_atr', 'day_ret_pct', 'gap_pct', 'dist_day_high_atr', 'dist_day_low_atr', 'or15_break_atr', 'or15_lose_atr', 'pdh_dist_atr', 'pdl_dist_atr', 'prev_green', 'prev3_up', 'prev_body_pct']


def _ist_naive(s: pd.Series) -> pd.Series:
    d = pd.to_datetime(s, errors="coerce", utc=True)
    return d.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)


def ticker_features(tk: str) -> pd.DataFrame | None:
    fp = DATA_5M / f"{tk}_stocks_indicators_5min.parquet"
    if not fp.exists():
        return None
    d = pd.read_parquet(fp)
    if d.empty or "date" not in d.columns:
        return None
    dd = pd.to_datetime(d["date"], errors="coerce")
    if getattr(dd.dt, "tz", None) is not None:
        dd = dd.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    d = d.assign(_ts=dd).dropna(subset=["_ts"]).sort_values("_ts")
    d = d.drop_duplicates(subset=["_ts"], keep="last").reset_index(drop=True)

    o = pd.to_numeric(d["open"], errors="coerce")
    h = pd.to_numeric(d["high"], errors="coerce")
    l = pd.to_numeric(d["low"], errors="coerce")
    c = pd.to_numeric(d["close"], errors="coerce")
    v = pd.to_numeric(d["volume"], errors="coerce")
    atr = pd.to_numeric(d.get("ATR"), errors="coerce").replace(0.0, np.nan)

    out = pd.DataFrame({"_ts": d["_ts"]})
    rsi = pd.to_numeric(d.get("RSI"), errors="coerce")
    adx = pd.to_numeric(d.get("ADX"), errors="coerce")
    out["rsi"] = rsi
    out["rsi_slope3"] = rsi - rsi.shift(3)
    out["adx5"] = adx
    out["adx5_slope3"] = adx - adx.shift(3)
    out["mfi"] = pd.to_numeric(d.get("MFI"), errors="coerce")
    out["cci"] = pd.to_numeric(d.get("CCI"), errors="coerce")
    sk = pd.to_numeric(d.get("Stoch_%K"), errors="coerce")
    sd = pd.to_numeric(d.get("Stoch_%D"), errors="coerce")
    out["stoch_k"], out["stoch_d"], out["stoch_kd"] = sk, sd, sk - sd
    macd = pd.to_numeric(d.get("MACD"), errors="coerce")
    macds = pd.to_numeric(d.get("MACD_Signal"), errors="coerce")
    macdh = pd.to_numeric(d.get("MACD_Hist"), errors="coerce")
    out["macd_atr"] = macd / atr
    out["macd_sig_atr"] = (macd - macds) / atr
    out["macd_hist_atr"] = macdh / atr
    ub = pd.to_numeric(d.get("Upper_Band"), errors="coerce")
    lb = pd.to_numeric(d.get("Lower_Band"), errors="coerce")
    bw = (ub - lb)
    out["bb_pos"] = (c - lb) / bw.replace(0.0, np.nan)
    out["bb_width_pct"] = bw / c.replace(0.0, np.nan) * 100.0
    e20 = pd.to_numeric(d.get("EMA_20"), errors="coerce")
    e50 = pd.to_numeric(d.get("EMA_50"), errors="coerce")
    e200 = pd.to_numeric(d.get("EMA_200"), errors="coerce")
    s20 = pd.to_numeric(d.get("20_SMA"), errors="coerce")
    out["ema20_dist_atr"] = (c - e20) / atr
    out["ema50_dist_atr"] = (c - e50) / atr
    out["ema200_dist_atr"] = (c - e200) / atr
    out["ema_stack_atr"] = (e20 - e50) / atr
    out["ema20_slope3_atr"] = (e20 - e20.shift(3)) / atr
    out["sma20_dist_atr"] = (c - s20) / atr
    out["roc5_pct"] = (c / c.shift(5) - 1.0) * 100.0
    h14 = h.rolling(14, min_periods=14).max()
    l14 = l.rolling(14, min_periods=14).min()
    out["willr14"] = (h14 - c) / (h14 - l14).replace(0.0, np.nan) * -100.0
    obv = pd.to_numeric(d.get("OBV"), errors="coerce")
    vmean20 = v.rolling(20, min_periods=10).mean()
    vstd20 = v.rolling(20, min_periods=10).std()
    out["obv_slope10_norm"] = (obv - obv.shift(10)) / (vmean20 * 10.0).replace(0.0, np.nan)
    out["vol_z20"] = (v - vmean20) / vstd20.replace(0.0, np.nan)
    upv = v.where(c > o, 0.0).rolling(5, min_periods=5).sum()
    dnv = v.where(c < o, 0.0).rolling(5, min_periods=5).sum()
    out["pressure5"] = (upv / dnv.replace(0.0, np.nan)).clip(upper=50.0).fillna(50.0)
    out["candle_range_atr"] = (h - l) / atr
    out["rechigh_dist_atr"] = (c - pd.to_numeric(d.get("Recent_High"), errors="coerce")) / atr
    out["reclow_dist_atr"] = (c - pd.to_numeric(d.get("Recent_Low"), errors="coerce")) / atr

    day = d["_ts"].dt.normalize()
    g = out.assign(_day=day, _o=o, _h=h, _l=l, _c=c).groupby("_day", group_keys=False)
    day_open = g["_o"].transform("first")
    day_hi = g["_h"].cummax()
    day_lo = g["_l"].cummin()
    out["day_ret_pct"] = (c / day_open.replace(0.0, np.nan) - 1.0) * 100.0
    out["dist_day_high_atr"] = (day_hi - c) / atr
    out["dist_day_low_atr"] = (c - day_lo) / atr
    pdc = pd.to_numeric(d.get("Prev_Day_Close"), errors="coerce")
    out["gap_pct"] = (day_open / pdc.replace(0.0, np.nan) - 1.0) * 100.0

    idx_in_day = g.cumcount()
    or_h = h.where(idx_in_day < 3).groupby(day).transform(lambda s: s.cummax().ffill())
    or_l = l.where(idx_in_day < 3).groupby(day).transform(lambda s: s.cummin().ffill())
    out["or15_break_atr"] = (c - or_h) / atr
    out["or15_lose_atr"] = (c - or_l) / atr

    daily = pd.DataFrame({"_day": day, "h": h, "l": l}).groupby("_day").agg(dh=("h", "max"), dl=("l", "min"))
    daily = daily.shift(1)  # prev completed day
    pdh = day.map(daily["dh"])
    pdl = day.map(daily["dl"])
    out["pdh_dist_atr"] = (c - pdh) / atr
    out["pdl_dist_atr"] = (c - pdl) / atr

    out["prev_green"] = (c.shift(1) > o.shift(1)).astype(float)
    green = (c > o).astype(float)
    out["prev3_up"] = green.shift(1).rolling(3, min_periods=3).sum()
    rng = (h - l).replace(0.0, np.nan)
    out["prev_body_pct"] = ((c - o).abs() / rng).shift(1)
    return out


def main() -> int:
    t0 = time.time()
    pool = pd.read_csv(SRC, low_memory=False)
    pool["_sig"] = _ist_naive(pool["signal_time_ist"])
    tickers = sorted(pool["ticker"].astype(str).str.upper().unique())
    print(f"[enrich] {SETUP}: {len(pool)} rows, {len(tickers)} tickers, {len(FEATS)} features")

    pieces = []
    hit = 0
    for i, tk in enumerate(tickers, 1):
        sub = pool[pool["ticker"].astype(str).str.upper() == tk]
        feats = ticker_features(tk)
        if feats is None:
            pieces.append(sub)
            continue
        m = sub.merge(feats.rename(columns={"_ts": "_sig"}), on="_sig", how="left")
        m.index = sub.index
        pieces.append(m)
        hit += int(m[FEATS[0]].notna().sum())
        if i % 200 == 0:
            print(f"[enrich] {i}/{len(tickers)} tickers | matched rows so far ~{hit}", flush=True)
    out = pd.concat(pieces).sort_index()
    out = out.drop(columns=["_sig"])

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = OUT_DIR / "historical_all_available_pre_dedupe_live_candidates.csv"
    out.to_csv(out_csv, index=False)

    cov = {f: round(float(pd.to_numeric(out[f], errors="coerce").notna().mean()) * 100, 1) for f in FEATS}
    (OUT_DIR / "_enrich_manifest.json").write_text(json.dumps({
        "built_utc": datetime.now(timezone.utc).isoformat(),
        "setup": SETUP, "rows": int(len(out)), "features": FEATS,
        "coverage_pct": cov, "source": str(SRC), "data_root": str(DATA_5M),
        "elapsed_sec": round(time.time() - t0, 1),
    }, indent=2), encoding="utf-8")
    low = {k: v for k, v in cov.items() if v < 80}
    print(f"[enrich] wrote {out_csv} in {time.time()-t0:.0f}s")
    print(f"[enrich] feature coverage <80%: {low or 'none'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
