"""
L_validate_passer.py — anti-overfit validation of the L* aggressive-search passers.
Uses the L path cache (fast). Same treatment as G_validate_passer.

PRIMARY candidate: L_DOUBLE_BOTTOM_VWAP momentum/ADX gate
  pre_entry_momentum_score>=~79.6 & sig5_adx_calc>=~28.4  (and pre2_mom_r sibling)
FRAGILITY check: L_PRESSURE_BURST_VWAP 4-term passers (do they have a robust 2-term core?)

Checks: threshold-sensitivity neighbourhood, train halves, monthly, exit stability.
Run:  py -3.12 L_validate_passer.py
"""
from __future__ import annotations
from pathlib import Path
import pickle
import numpy as np
import pandas as pd
import walkforward_gate as wfg
from nse_intraday_costs import CostConfig

CACHE = Path(r"C:\TradingData\eqidv2\v11_L_paths_cache.pkl")
TRAIN_END, TEST_START, TRAIN_MID = "2026-04-30", "2026-05-01", "2026-02-15"
CFG = CostConfig()


def net_for_exit(recs, sl, tgt):
    out = np.empty(len(recs))
    for i, p in enumerate(recs):
        e = p["raw_open"] * 1.0005
        qty = max(1, int(100000.0 / p["raw_open"]))
        h, l, c = p["highs"], p["lows"], p["closes"]
        slp, tgp = e * (1 - sl / 100), e * (1 + tgt / 100)
        slh, tgh = l <= slp, h >= tgp
        fsl = int(np.argmax(slh)) if slh.any() else 10 ** 9
        ftg = int(np.argmax(tgh)) if tgh.any() else 10 ** 9
        xp = slp if (slh.any() and fsl <= ftg) else (tgp if tgh.any() else c[-1])
        out[i] = wfg.net_pnl_vectorized(np.array([e]), np.array([xp]), np.array([qty]), np.array(["LONG"]), CFG)[0]
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


def frame(recs):
    df = pd.DataFrame([{k: v for k, v in r.items() if k not in ("highs", "lows", "closes")} for r in recs])
    dts = df["date"].to_numpy()
    return df, dts, (dts <= TRAIN_END), (dts >= TEST_START), (dts <= TRAIN_END) & (dts <= TRAIN_MID), (dts <= TRAIN_END) & (dts > TRAIN_MID)


def sensitivity(name, recs, fa, gridA, fb, gridB, exits):
    df, dts, is_tr, is_te, _, _ = frame(recs)
    A = df[fa].to_numpy(float); B = df[fb].to_numpy(float)
    fin = np.isfinite(A) & np.isfinite(B)
    print(f"\n=== SENSITIVITY {name}: {fa}>=A & {fb}>=B  [train_pf / test_pf (test_n)] ===")
    for sl, tgt in exits:
        net = net_for_exit(recs, sl, tgt)
        print(f"  -- exit {sl}/{tgt} --   {fb}>=  " + "".join(f"{b:>15}" for b in gridB))
        for a in gridA:
            cells = []
            for b in gridB:
                m = fin & (A >= a) & (B >= b)
                tr, te = m & is_tr, m & is_te
                if tr.sum() < 12:
                    cells.append("     .        ")
                else:
                    cells.append(f"{_pf(net[tr]):>5.2f}/{_pf(net[te]):>4.2f}(n{int(te.sum()):>2})")
            print(f"   {fa}>={a:<7} " + "".join(f"{c:>15}" for c in cells))


def gate_detail(name, recs, gate_fn, exits):
    df, dts, is_tr, is_te, h1, h2 = frame(recs)
    g = gate_fn(df)
    print(f"\n=== {name} CHOSEN GATE breakdown ===")
    for sl, tgt in exits:
        net = net_for_exit(recs, sl, tgt)
        tr, te = g & is_tr, g & is_te
        print(f"  exit {sl}/{tgt}: TRAIN n={int(tr.sum())} pf={_pf(net[tr]):.2f} "
              f"[h1 {_pf(net[g & h1]):.2f} n{int((g&h1).sum())} / h2 {_pf(net[g & h2]):.2f} n{int((g&h2).sum())}] "
              f"| TEST n={int(te.sum())} pf={_pf(net[te]):.2f} win={(net[te]>0).mean()*100:.0f}% "
              f"p={_dbp(df.loc[te,'date'].to_numpy(), net[te]):.3f} | full {_pf(net[g]):.2f} net Rs{net[g].sum():,.0f}")
    # monthly at the first exit
    sl, tgt = exits[0]; net = net_for_exit(recs, sl, tgt)
    gg = df[g].copy(); gg["net"] = net[g]; gg["m"] = pd.to_datetime(gg["date"]).dt.strftime("%Y-%m")
    print(f"  monthly (exit {sl}/{tgt}):")
    for m, sub in gg.groupby("m"):
        print(f"    {m}: n={len(sub):>2} pf={_pf(sub['net'].to_numpy()):>5.2f} net Rs{sub['net'].sum():>8,.0f} win={(sub['net']>0).mean()*100:>3.0f}%")


def main():
    with open(CACHE, "rb") as fh:
        allp = pickle.load(fh)

    # ---------- PRIMARY: L_DOUBLE_BOTTOM_VWAP momentum/ADX gate ----------
    recs = allp["L_DOUBLE_BOTTOM_VWAP"]
    print("#" * 70 + "\n# L_DOUBLE_BOTTOM_VWAP  (raw n=%d)  baseline = train 0.71 / test 0.57 LOSER\n" % len(recs) + "#" * 70)
    sensitivity("DOUBLE_BOTTOM A", recs, "pre_entry_momentum_score", [60, 70, 75, 79, 85],
                "sig5_adx_calc", [24, 26, 28, 30, 32], [(0.9, 1.5), (0.7, 1.5)])
    sensitivity("DOUBLE_BOTTOM B", recs, "pre2_mom_r", [0.20, 0.30, 0.42, 0.50],
                "sig5_adx_calc", [24, 26, 28, 30, 32], [(0.9, 1.5), (0.7, 1.5)])
    gate_detail("L_DOUBLE_BOTTOM_VWAP  (pre_entry_momentum_score>=75 & sig5_adx_calc>=28)", recs,
                lambda df: (df["pre_entry_momentum_score"].to_numpy(float) >= 75)
                           & (df["sig5_adx_calc"].to_numpy(float) >= 28),
                [(0.9, 1.5), (0.7, 1.5), (0.9, 1.25), (1.1, 2.0)])

    # ---------- FRAGILITY: L_PRESSURE_BURST_VWAP 4-term passers ----------
    recs = allp["L_PRESSURE_BURST_VWAP"]
    print("\n" + "#" * 70 + "\n# L_PRESSURE_BURST_VWAP  (raw n=%d)  baseline = train 0.66 / test 0.69 LOSER\n" % len(recs) + "#" * 70)
    df, dts, is_tr, is_te, h1, h2 = frame(recs)
    print("\n=== PRESSURE passer #1: regime==NEUTRAL & vol_ratio>=4.69 & pre10_mom_r>=0.311 & pre1_adx<=47 (exit 0.7/1.5) — term drop-out ===")
    base = [("regime==NEUTRAL", (df["regime"] == "NEUTRAL").to_numpy()),
            ("vol_ratio>=4.69", (df["vol_ratio"].to_numpy(float) >= 4.69)),
            ("pre10_mom_r>=0.311", (df["pre10_mom_r"].to_numpy(float) >= 0.311)),
            ("pre1_adx<=47", (df["pre1_adx"].to_numpy(float) <= 47))]
    net = net_for_exit(recs, 0.7, 1.5)
    for drop in range(-1, len(base)):
        sel = np.ones(len(df), bool); label = "ALL 4 terms"
        for j, (nm, m) in enumerate(base):
            if j == drop:
                label = "drop: " + nm; continue
            sel = sel & m
        tr, te = sel & is_tr, sel & is_te
        if tr.sum() < 8:
            print(f"   {label:24} -> train n={int(tr.sum())} (too few)"); continue
        print(f"   {label:24} -> TRAIN n={int(tr.sum()):>3} pf={_pf(net[tr]):.2f} | TEST n={int(te.sum()):>3} "
              f"pf={_pf(net[te]):.2f} p={_dbp(df.loc[te,'date'].to_numpy(), net[te]):.3f}")
    print("\n  (if dropping any single term collapses test -> the config is a fragile 4-term overfit)")

    print("\n[verdict guidance] REAL pocket = PF>1.5 across a contiguous sensitivity region + both halves >1.3 "
          "+ mostly-positive months + robust to term drop-out. Knife-edge / single-term-dependent => overfit.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
