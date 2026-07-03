r"""05_build_signals_ext.py -- EXTENSION build: LONG + SHORT signals resolved under the
WIDER target / R-multiple bracket grid (outside the tight ±0.75% theme).

One row per (ticker, side, 5-min signal bar). Columns: ticker, side, signal_ts, _day, slot,
minute, SIGNAL_FEATS, f_<FAMILY> (long F* and short S*), entry_ok, e_open, fill, qty,
and per-exit out_/raw_/held_/tie_ for BRACKETS_EXT + EXT_VARIANTS.

Cache -> results/signals_resolved_ext.parquet
Run: py -3.12 .../claude_engine/scripts/05_build_signals_ext.py
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
    signal_days = set(pd.Timestamp(x) for x in (S["train"] + S["test"]))
    load_days = set(pd.Timestamp(x) for x in (S["warmup"] + S["train"] + S["test"]))
    long_keys = list(L.FAMILY_LABELS.keys())
    short_keys = list(L.SHORT_FAMILY_LABELS.keys())
    exits = [(k, dict(sl_pct=v[0], tgt_pct=v[1])) for k, v in L.BRACKETS_EXT.items()] + \
            [(k, dict(sl_pct=L.BRACKETS_EXT["x_100_200"][0], tgt_pct=L.BRACKETS_EXT["x_100_200"][1], **v))
             for k, v in L.EXT_VARIANTS.items()]
    print(f"[ext] universe={len(uni)} exits={[k for k,_ in exits]}")

    rows = []
    t0 = time.time()
    slip = L.SLIPPAGE_BPS / 1e4
    for ti, tk in enumerate(uni):
        d5 = L.load_5m_raw(tk)
        if d5 is None:
            continue
        d5 = d5[d5["sess"].isin(load_days)]
        if len(d5) < 30:
            L.load_5m_raw.cache_clear(); continue
        feat = L.compute_features(d5)
        lf = L.family_triggers(feat)
        sf = L.short_family_triggers(feat)
        is_sig = feat["sess"].isin(signal_days).to_numpy()
        any_long = np.zeros(len(feat), bool); any_short = np.zeros(len(feat), bool)
        for k in long_keys:
            any_long |= lf[k].to_numpy()
        for k in short_keys:
            any_short |= sf[k].to_numpy()
        arr = L.load_1m_raw(tk)
        if arr is None:
            L.load_5m_raw.cache_clear(); continue
        ts, op, hi, lo, cl = arr

        for side, fams, fkeys, anymask in (("LONG", lf, long_keys, any_long), ("SHORT", sf, short_keys, any_short)):
            emit = is_sig & anymask
            if not emit.any():
                continue
            sub = feat[emit]
            famvals = {k: fams[k][emit].to_numpy() for k in fkeys}
            for ridx, r in enumerate(sub.itertuples()):
                sig_ns = pd.Timestamp(r.date).value
                ei = L._entry_index(ts, op, sig_ns)
                base = dict(ticker=tk, side=side, signal_ts=pd.Timestamp(r.date), _day=pd.Timestamp(r.sess),
                            slot=int(r.slot), minute=int(r.minute))
                for f in L.SIGNAL_FEATS:
                    base[f] = getattr(r, f)
                for k in fkeys:
                    base["f_" + k] = bool(famvals[k][ridx])
                if ei is None:
                    base["entry_ok"] = False
                    rows.append(base); continue
                e_idx, e_open = ei
                fill = round(e_open * (1 + slip), 2) if side == "LONG" else round(e_open * (1 - slip), 2)
                qty = max(1, int(L.NOTIONAL_RS / e_open))
                base.update(entry_ok=True, e_open=e_open, fill=fill, qty=qty)
                for key, kw in exits:
                    res = L.resolve_path(ts, op, hi, lo, cl, e_idx, e_open, side=side, **kw)
                    base["out_" + key] = res["outcome"]; base["raw_" + key] = res["exit_px_raw"]
                    base["held_" + key] = res["bars_held"]; base["tie_" + key] = res["tie"]
                rows.append(base)
        L.load_1m_raw.cache_clear(); L.load_5m_raw.cache_clear()
        if (ti + 1) % 25 == 0:
            print(f"  ...{ti+1}/{len(uni)} tickers, {len(rows):,} signals, {time.time()-t0:.0f}s")

    df = pd.DataFrame(rows)
    df = df[df["entry_ok"]].copy().reset_index(drop=True)
    for key, _ in exits:
        df["net_" + key] = L.attach_net(df, key, L.SLIPPAGE_BPS)
    df.to_parquet(L.RESULTS / "signals_resolved_ext.parquet", index=False)
    n_long = int((df["side"] == "LONG").sum()); n_short = int((df["side"] == "SHORT").sum())
    print(f"[ext] rows={len(df):,} (LONG {n_long:,} / SHORT {n_short:,})")
    for side in ("LONG", "SHORT"):
        s = df[df["side"] == side]
        for key in ("x_075_100", "x_100_200", "x_100_300"):
            oc = s["out_" + key]
            print(f"   {side} {key}: TARGET-first%={100*(oc=='TARGET').mean():.1f} "
                  f"net-win%@5bps={100*(L.attach_net(s,key,5.0)>0).mean():.1f}")
    df.to_parquet(L.RESULTS / "signals_resolved_ext.parquet", index=False)
    print(f"[ext] wrote results/signals_resolved_ext.parquet")


if __name__ == "__main__":
    main()
