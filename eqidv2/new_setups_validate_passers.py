"""
new_setups_validate_passers.py — anti-overfit validation battery for the SHORT finalists from
new_setups_search_v11.py (the two LONGs reject on sample). Uses the cached paths (fast, no rescan).

Finalist gates (rounded from the search hits):
  S_UPTHRUST_TRAP_FADE : ema20_slope<=-0.0059 & pre2_mom_r>=0.128 & rsi3max>=50.66   (exit ~0.7/0.8)
  N_MORNING_ZERO_WICK_SHORT : rs_pct>=-0.34 & sig5_adx_calc>=21.4 & pre2_mom_r>=0.17 (exit 1.1/2.0)

Battery (same standard as G/L/T validators):
  1. THRESHOLD SENSITIVITY — vary EACH term over a neighbourhood (monotone/contiguous => real; knife-edge => overfit)
  2. TERM DROP-OUT — remove each term (how load-bearing is each?)
  3. CHOSEN GATE full breakdown across exits — train + halves + test day-block p + day-concentration top1day
  4. MONTHLY PnL of the gated book (>=70% months positive?)
Run:  py -3.12 new_setups_validate_passers.py
"""
from __future__ import annotations
from pathlib import Path
import pickle
import numpy as np
import pandas as pd
import walkforward_gate as wfg
from nse_intraday_costs import CostConfig

CACHE = Path(r"C:\TradingData\eqidv2\v11_newsetups_paths_cache.pkl")
TRAIN_END, TEST_START = "2026-04-30", "2026-05-01"
CFG = CostConfig()

CFGS = {
    "S_UPTHRUST_TRAP_FADE": {
        "side": "SHORT",
        "gate": [("ema20_slope", "<=", -0.005899), ("pre2_mom_r", ">=", 0.128328), ("rsi3max", ">=", 50.655474)],
        "exits": [(0.7, 0.8), (1.1, 0.8), (0.7, 1.0), (0.9, 0.8), (0.5, 0.8)],
        "primary_exit": (0.7, 0.8),
        "sens": {
            ("ema20_slope", "<="): [-0.003, -0.005, -0.005899, -0.008, -0.012],
            ("pre2_mom_r", ">="): [0.08, 0.105, 0.128328, 0.16, 0.20],
            ("rsi3max", ">="): [45.0, 48.0, 50.655474, 53.0, 57.0],
        },
    },
    "N_MORNING_ZERO_WICK_SHORT": {
        "side": "SHORT",
        "gate": [("rs_pct", ">=", -0.339376), ("sig5_adx_calc", ">=", 21.382617), ("pre2_mom_r", ">=", 0.172406)],
        "exits": [(1.1, 2.0), (1.1, 1.25), (0.7, 1.5), (0.9, 1.5), (0.9, 2.0)],
        "primary_exit": (1.1, 2.0),
        "sens": {
            ("rs_pct", ">="): [-0.6, -0.45, -0.339376, -0.22, -0.10],
            ("sig5_adx_calc", ">="): [16.0, 19.0, 21.382617, 25.0, 29.0],
            ("pre2_mom_r", ">="): [0.10, 0.14, 0.172406, 0.21, 0.26],
        },
    },
}


def net_for_exit(recs, sl, tgt):
    out = np.empty(len(recs))
    for i, p in enumerate(recs):
        side = p["side"]
        e = p["raw_open"] * (0.9995 if side == "SHORT" else 1.0005)
        qty = max(1, int(100000.0 / p["raw_open"]))
        h, l, c = p["highs"], p["lows"], p["closes"]
        if side == "SHORT":
            slp, tgp = e * (1 + sl / 100), e * (1 - tgt / 100); slh, tgh = h >= slp, l <= tgp
        else:
            slp, tgp = e * (1 - sl / 100), e * (1 + tgt / 100); slh, tgh = l <= slp, h >= tgp
        fsl = int(np.argmax(slh)) if slh.any() else 10 ** 9
        ftg = int(np.argmax(tgh)) if tgh.any() else 10 ** 9
        xp = slp if (slh.any() and fsl <= ftg) else (tgp if tgh.any() else c[-1])
        out[i] = wfg.net_pnl_vectorized(np.array([e]), np.array([xp]), np.array([qty]), np.array([side]), CFG)[0]
    return out


def _pf(n):
    n = np.asarray(n, float); a, b = n[n > 0].sum(), -n[n < 0].sum()
    return float(a / b) if b > 0 else (float('inf') if a > 0 else 0.0)


def _dbp(dts, net, nb=8000, seed=7):
    s = pd.Series(net, index=pd.to_datetime(dts)).groupby(level=0).sum().to_numpy()
    if len(s) < 3:
        return float('nan')
    r = np.random.default_rng(seed)
    return float((s[r.integers(0, len(s), size=(nb, len(s)))].mean(axis=1) <= 0).mean())


def _top1(dts, net):
    s = pd.Series(net, index=pd.to_datetime(dts)).groupby(level=0).sum(); tot = s.sum()
    return round(float(s.max() / tot * 100)) if tot > 0 else float('nan')


def _term_mask(df, term):
    f, op, v = term
    col = df[f].to_numpy(float)
    return (col <= v) if op == "<=" else (col >= v)


def validate(setup, cfg, allp):
    recs = allp.get(setup, [])
    print("\n" + "#" * 78 + f"\n# {setup}  (n={len(recs)}, {cfg['side']})\n" + "#" * 78)
    if len(recs) < 25:
        print(f"  insufficient paths ({len(recs)}) — cannot validate."); return
    df = pd.DataFrame([{k: v for k, v in r.items() if k not in ("highs", "lows", "closes")} for r in recs])
    dts = df["date"].to_numpy()
    is_tr, is_te = dts <= TRAIN_END, dts >= TEST_START
    mid = np.sort(dts[is_tr])[is_tr.sum() // 2]
    h1, h2 = is_tr & (dts <= mid), is_tr & (dts > mid)
    gate_terms = cfg["gate"]
    base = np.ones(len(df), bool)
    for t in gate_terms:
        base = base & _term_mask(df, t) & np.isfinite(df[t[0]].to_numpy(float))
    pe = cfg["primary_exit"]
    net_pe = net_for_exit(recs, *pe)

    # 1. threshold sensitivity (vary each term, hold others at chosen)
    print(f"\n=== 1. THRESHOLD SENSITIVITY (exit {pe[0]}/{pe[1]}; vary one term, others held) ===")
    for (f, op), grid in cfg["sens"].items():
        others = np.ones(len(df), bool)
        for t in gate_terms:
            if t[0] == f:
                continue
            others = others & _term_mask(df, t) & np.isfinite(df[t[0]].to_numpy(float))
        col = df[f].to_numpy(float); fin = np.isfinite(col)
        cells = []
        for v in grid:
            m = others & fin & ((col <= v) if op == "<=" else (col >= v))
            tr, te = m & is_tr, m & is_te
            star = "*" if abs(v - dict((t[0], t[2]) for t in gate_terms)[f]) < 1e-9 else " "
            cells.append(f"{op}{v:>8.4f}{star} tr{int(tr.sum()):>3} {_pf(net_pe[tr]):>4.2f} | te{int(te.sum()):>2} {_pf(net_pe[te]):>4.2f}")
        print(f"  {f:>18}:")
        for c in cells:
            print(f"        {c}")

    # 2. term drop-out
    print(f"\n=== 2. TERM DROP-OUT (exit {pe[0]}/{pe[1]}; remove one term) ===")
    full_tr, full_te = base & is_tr, base & is_te
    print(f"  FULL GATE        : tr n={int(full_tr.sum())} pf={_pf(net_pe[full_tr]):.2f} | te n={int(full_te.sum())} pf={_pf(net_pe[full_te]):.2f}")
    for drop in gate_terms:
        m = np.ones(len(df), bool)
        for t in gate_terms:
            if t == drop:
                continue
            m = m & _term_mask(df, t) & np.isfinite(df[t[0]].to_numpy(float))
        tr, te = m & is_tr, m & is_te
        print(f"  drop {drop[0]:>16}: tr n={int(tr.sum())} pf={_pf(net_pe[tr]):.2f} | te n={int(te.sum())} pf={_pf(net_pe[te]):.2f}")

    # 3. chosen gate full breakdown across exits
    print(f"\n=== 3. CHOSEN GATE breakdown (gate = {'; '.join(f'{f}{o}{v}' for f,o,v in gate_terms)}) ===")
    for sl, tgt in cfg["exits"]:
        net = net_for_exit(recs, sl, tgt)
        tr, te = base & is_tr, base & is_te
        bh1, bh2 = base & h1, base & h2
        p = _dbp(df.loc[te, "date"].to_numpy(), net[te]); t1 = _top1(df.loc[te, "date"].to_numpy(), net[te])
        print(f"  exit {sl}/{tgt}: TRAIN n={int(tr.sum())} pf={_pf(net[tr]):.2f} [h1 {_pf(net[bh1]):.2f} n{int(bh1.sum())} / h2 {_pf(net[bh2]):.2f} n{int(bh2.sum())}]"
              f" | TEST n={int(te.sum())}(d{df.loc[te,'date'].nunique()}) pf={_pf(net[te]):.2f} win={(net[te]>0).mean()*100:.0f}% p={p:.3f} top1d={t1}%"
              f" | full pf={_pf(net[base]):.2f} net Rs{net[base].sum():,.0f}")

    # 4. monthly (primary exit)
    print(f"\n=== 4. MONTHLY PnL of the gated book (exit {pe[0]}/{pe[1]}) ===")
    g = df[base].copy(); g["net"] = net_pe[base]; g["month"] = pd.to_datetime(g["date"]).dt.strftime("%Y-%m")
    pos = 0; tot = 0
    for mth, sub in g.groupby("month"):
        tot += 1; ispos = sub["net"].sum() > 0; pos += int(ispos)
        print(f"  {mth}: n={len(sub):>2} pf={_pf(sub['net'].to_numpy()):>5.2f} net Rs{sub['net'].sum():>9,.0f} win={(sub['net']>0).mean()*100:>4.0f}% {'+' if ispos else '-'}")
    print(f"  >> {pos}/{tot} months positive ({pos/tot*100:.0f}%)")

    # reference ungated
    ig = net_for_exit(recs, *pe)
    print(f"\n=== ref: UNGATED (exit {pe[0]}/{pe[1]}) TRAIN pf={_pf(ig[is_tr]):.2f} | TEST pf={_pf(ig[is_te]):.2f} | full net Rs{ig.sum():,.0f} ===")


def main():
    with open(CACHE, "rb") as fh:
        allp = pickle.load(fh)
    print(f"loaded cached paths: { {k: len(v) for k, v in allp.items()} }")
    for setup, cfg in CFGS.items():
        validate(setup, cfg, allp)
    print("\n[verdict] REAL pocket if PF survives across the sensitivity neighbourhood, no single term is "
          "the whole edge on drop-out, both train halves >1.3, >=70% months +, top1day<=~55%, p<0.10.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
