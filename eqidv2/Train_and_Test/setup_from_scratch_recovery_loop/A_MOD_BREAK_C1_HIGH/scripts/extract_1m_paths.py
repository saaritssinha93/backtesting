r"""extract_1m_paths.py — one-time extraction of 1-minute price paths for every signal.

For each pool row: slice the ticker's 1-min bars from signal_time to 15:20 IST and store
the full path (long format, float32) plus a per-signal summary (entry px/ts, MFE/MAE
geometry, confirmation/retest timing). This makes ANY entry/exit redesign evaluable in
milliseconds without re-touching parquet.

Leak-safety: paths start AT the signal bar close (next 1-min bar onward is tradeable).
Validation: for a random sample, my bracket walk must reproduce the canonical
tt._entry/_resolve_full outcome (0.70/1.00) — match rate is printed and must be ~100%.

Output (in ../paths/):
  paths.parquet    signal_row_id, k (bar index), ts, o, h, l, c
  summary.csv      per-signal entry/geometry summary
  validation.json  sample match-rate vs setup_train_test resolver
"""
from __future__ import annotations

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
for p in (REPO, TT_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

DATA_1M = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
FNAME = "historical_all_available_pre_dedupe_live_candidates.csv"
SLIP = 15.0 / 1e4
NOTIONAL = 100_000.0
EOD_H, EOD_M = 15, 20


def load_1m(ticker: str) -> pd.DataFrame | None:
    p = DATA_1M / f"{ticker}_stocks_indicators_1min.parquet"
    if not p.exists():
        return None
    try:
        df = pd.read_parquet(p, columns=["date", "open", "high", "low", "close"])
    except Exception:
        return None
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize("Asia/Kolkata")
    else:
        df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
    return df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)


def main() -> int:
    pool = pd.read_csv(WORK / "pools" / "pool_base" / FNAME, low_memory=False)
    ts = pd.to_datetime(pool["signal_time_ist"], errors="coerce")
    ts = ts.dt.tz_localize("Asia/Kolkata") if getattr(ts.dt, "tz", None) is None else ts.dt.tz_convert("Asia/Kolkata")
    pool["_sig_ts"] = ts
    # signal bar is 5-min: tradeable from signal_ts + 5min? Convention check: pool signal_time_ist
    # is the SLOT time; live entries fill at signal_time + ~1min. tt._entry searches 1-min opens
    # from sig_ts (bar-start convention -> first tradeable 1-min bar opens at slot+5min if slot is
    # bar START; but tt uses sig_ts directly). We mirror tt exactly: first 1-min bar >= sig_ts.
    print(f"[paths] pool rows={len(pool)}")

    out_dir = WORK / "paths"
    out_dir.mkdir(parents=True, exist_ok=True)

    path_chunks: list[pd.DataFrame] = []
    summaries: list[dict] = []
    t0 = time.time()
    tickers = sorted(pool["ticker"].astype(str).str.upper().unique())
    for n, tk in enumerate(tickers, 1):
        rows = pool[pool["ticker"].astype(str).str.upper() == tk]
        bars = load_1m(tk)
        if bars is None or bars.empty:
            continue
        bidx = bars["date"].values
        for rid, r in rows.iterrows():
            st = r["_sig_ts"]
            if pd.isna(st):
                continue
            eod = st.normalize() + pd.Timedelta(hours=EOD_H, minutes=EOD_M)
            i0 = np.searchsorted(bidx, np.datetime64(st.tz_convert("UTC").tz_localize(None)))
            # numpy datetime64 comparison: bars date tz-aware -> values are UTC datetime64
            sub = bars.iloc[i0:]
            sub = sub[(sub["date"] >= st) & (sub["date"] <= eod)]
            if sub.empty:
                continue
            m = len(sub)
            path_chunks.append(pd.DataFrame({
                "sid": np.full(m, rid, dtype=np.int32),
                "k": np.arange(m, dtype=np.int16),
                "min_from_sig": ((sub["date"] - st).dt.total_seconds() // 60).astype(np.int16),
                "o": sub["open"].astype(np.float32).values,
                "h": sub["high"].astype(np.float32).values,
                "l": sub["low"].astype(np.float32).values,
                "c": sub["close"].astype(np.float32).values,
            }))
            o = sub["open"].values; h = sub["high"].values; l = sub["low"].values; c = sub["close"].values
            e_open = float(o[0])
            mfe = float(h.max() / e_open - 1.0) * 100.0
            mae = float(l.min() / e_open - 1.0) * 100.0
            k_mfe = int(np.argmax(h)); k_mae = int(np.argmin(l))
            mae_before_mfe = float(l[:k_mfe + 1].min() / e_open - 1.0) * 100.0 if k_mfe >= 0 else mae
            sig_high = float(r.get("signal_high", np.nan))
            conf_k = -1
            if np.isfinite(sig_high):
                hits = np.nonzero(h[1:] > sig_high)[0]      # confirmation strictly after entry bar
                conf_k = int(hits[0] + 1) if len(hits) else -1
            summaries.append({
                "sid": rid, "ticker": tk, "sig_ts": str(st), "n_bars": m,
                "entry_open": e_open, "entry_ts": str(sub["date"].iloc[0]),
                "eod_close": float(c[-1]), "eod_ret_pct": (float(c[-1]) / e_open - 1.0) * 100.0,
                "mfe_pct": mfe, "mae_pct": mae, "k_mfe": k_mfe, "k_mae": k_mae,
                "mae_before_mfe_pct": mae_before_mfe,
                "conf_k": conf_k,
                "min_low_30m": float(l[:min(30, m)].min() / e_open - 1.0) * 100.0,
                "min_low_60m": float(l[:min(60, m)].min() / e_open - 1.0) * 100.0,
            })
        if n % 150 == 0 or n == len(tickers):
            print(f"[paths] {n}/{len(tickers)} elapsed={time.time()-t0:.0f}s chunks={len(path_chunks)}", flush=True)

    paths = pd.concat(path_chunks, ignore_index=True)
    paths.to_parquet(out_dir / "paths.parquet", index=False)
    sdf = pd.DataFrame(summaries)
    sdf.to_csv(out_dir / "summary.csv", index=False)
    print(f"[paths] wrote paths={len(paths):,} rows, summary={len(sdf):,} signals")

    # ---- validation vs canonical tt resolver (sample) ----
    import setup_train_test as tt
    rng = np.random.default_rng(7)
    sample = sdf.sample(n=min(400, len(sdf)), random_state=7)
    match = 0; total = 0; mism = []
    for _, s in sample.iterrows():
        r = pool.loc[int(s["sid"])]
        e = tt._entry(str(s["ticker"]), "LONG", pd.Timestamp(s["sig_ts"]).isoformat())
        if e is None:
            continue
        entry_ts, fill, qty = e
        rr = tt._resolve_full(str(s["ticker"]), "LONG", entry_ts, fill, qty, 0.70, 1.00)
        if rr is None:
            continue
        # my walk
        sub = paths[paths["sid"] == int(s["sid"])]
        if sub.empty:
            continue
        h = sub["h"].values.astype(float); l = sub["l"].values.astype(float); c = sub["c"].values.astype(float)
        slp = fill * (1 - 0.70 / 100); tgp = fill * (1 + 1.00 / 100)
        out = "EOD"
        for i in range(len(sub)):
            if l[i] <= slp:
                out = "SL"; break
            if h[i] >= tgp:
                out = "TARGET"; break
        total += 1
        if out == str(rr[1]).upper():
            match += 1
        else:
            mism.append({"sid": int(s["sid"]), "mine": out, "tt": str(rr[1])})
    val = {"sampled": total, "matched": match, "match_rate": round(match / max(total, 1), 4),
           "mismatch_examples": mism[:8]}
    (out_dir / "validation.json").write_text(json.dumps(val, indent=2), encoding="utf-8")
    print(f"[paths] VALIDATION: {match}/{total} matched ({val['match_rate']:.1%})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
