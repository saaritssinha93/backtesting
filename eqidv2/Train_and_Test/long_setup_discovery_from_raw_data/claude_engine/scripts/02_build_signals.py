r"""02_build_signals.py -- build the union LONG signal table from RAW 5-min families and
resolve every signal on RAW 1-min data under the full bracket grid + exit variants.

One row per (ticker, 5-min signal bar). Columns:
  ticker, signal_ts, _day, slot, minute, + SIGNAL_FEATS, f_<FAMILY> booleans,
  entry_ok, e_open, fill, qty, and per-exit: out_<key>, net_<key>, held_<key>, tie_<key>.

Cache -> results/signals_resolved.parquet   (read by 03_edge_study.py and 04_search.py)
Run: py -3.12 Train_and_Test/long_setup_discovery_from_raw_data/scripts/02_build_signals.py
"""
from __future__ import annotations
import sys, time
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import lib_long_disc as L


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    uni = L.load_universe()["tickers"]
    S = L.load_sessions()
    signal_days = set(pd.Timestamp(x) for x in (S["train"] + S["test"]))   # emit signals here
    load_days = set(pd.Timestamp(x) for x in (S["warmup"] + S["train"] + S["test"]))  # +warmup for indicators
    fam_keys = list(L.FAMILY_LABELS.keys())
    exits = [(k, dict(sl_pct=v[0], tgt_pct=v[1])) for k, v in L.BRACKETS.items()] + \
            [(k, dict(sl_pct=L.BRACKETS[L.ANCHOR][0], tgt_pct=L.BRACKETS[L.ANCHOR][1], **v)) for k, v in L.VARIANTS.items()]
    print(f"[build] universe={len(uni)} signal_days={len(signal_days)} exits={[k for k,_ in exits]}")

    rows = []
    t0 = time.time()
    fam_counts = {k: 0 for k in fam_keys}
    for ti, tk in enumerate(uni):
        d5 = L.load_5m_raw(tk)
        if d5 is None:
            continue
        d5 = d5[d5["sess"].isin(load_days)]
        if len(d5) < 30:
            continue
        feat = L.compute_features(d5)
        fams = L.family_triggers(feat)
        any_fam = np.zeros(len(feat), dtype=bool)
        for k in fam_keys:
            any_fam |= fams[k].to_numpy()
        emit = feat["sess"].isin(signal_days).to_numpy() & any_fam
        if not emit.any():
            L.load_5m_raw.cache_clear()  # keep memory bounded
            continue
        sub = feat[emit].copy()
        for k in fam_keys:
            sub["f_" + k] = fams[k][emit].to_numpy()
            fam_counts[k] += int(fams[k][emit].sum())

        arr = L.load_1m_raw(tk)
        if arr is None:
            continue
        ts, op, hi, lo, cl = arr
        slip = L.SLIPPAGE_BPS / 1e4
        rec = []
        for r in sub.itertuples():
            sig_ns = pd.Timestamp(r.date).value
            ei = L._entry_index(ts, op, sig_ns)
            base = dict(ticker=tk, signal_ts=pd.Timestamp(r.date), _day=pd.Timestamp(r.sess),
                        slot=int(r.slot), minute=int(r.minute))
            for f in L.SIGNAL_FEATS:
                base[f] = getattr(r, f)
            for k in fam_keys:
                base["f_" + k] = bool(getattr(r, "f_" + k))
            if ei is None:
                base["entry_ok"] = False
                rec.append(base)
                continue
            e_idx, e_open = ei
            fill = round(e_open * (1 + slip), 2)
            qty = max(1, int(L.NOTIONAL_RS / e_open))
            base.update(entry_ok=True, e_open=e_open, fill=fill, qty=qty)
            for key, kw in exits:
                res = L.resolve_path(ts, op, hi, lo, cl, e_idx, e_open, **kw)
                base["out_" + key] = res["outcome"]
                base["raw_" + key] = res["exit_px_raw"]
                base["held_" + key] = res["bars_held"]
                base["tie_" + key] = res["tie"]
            rec.append(base)
        rows.extend(rec)
        L.load_1m_raw.cache_clear()
        L.load_5m_raw.cache_clear()
        if (ti + 1) % 25 == 0:
            print(f"  ...{ti+1}/{len(uni)} tickers, {len(rows):,} signals, {time.time()-t0:.0f}s")

    df = pd.DataFrame(rows)
    print(f"[build] raw signal rows={len(df):,}  entry_ok={int(df['entry_ok'].sum()):,}")
    df = df[df["entry_ok"]].copy().reset_index(drop=True)

    # vectorized NET pnl per exit (statutory + exit-leg slippage)
    slip = L.SLIPPAGE_BPS / 1e4
    fill = df["fill"].to_numpy(float)
    qty = df["qty"].to_numpy(float)
    for key, _ in exits:
        raw = df["raw_" + key].to_numpy(float)
        exit_px = raw * (1.0 - slip)            # LONG exit slips down
        df["net_" + key] = L.net_pnl_vec(fill, exit_px, qty)
    df.to_parquet(L.RESULTS / "signals_resolved.parquet", index=False)

    print("\n[build] family signal counts (TRAIN+TEST, pre-dedupe):")
    for k in fam_keys:
        print(f"   {k:24s} {fam_counts[k]:>7,}")
    print(f"[build] anchor 0.75/0.75 base-rate (all union signals): "
          f"win%={100*(df['net_'+L.ANCHOR]>0).mean():.1f}  "
          f"TARGET%={100*(df['out_'+L.ANCHOR]=='TARGET').mean():.1f}  "
          f"tie%={100*df['tie_'+L.ANCHOR].mean():.2f}")
    print(f"[build] wrote results/signals_resolved.parquet  ({len(df):,} rows)")


if __name__ == "__main__":
    main()
