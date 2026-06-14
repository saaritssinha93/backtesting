"""
G_validate_passer.py — is the G_HIGHER_HIGH_BREAK 2-term momentum/ADX gate a REAL
pocket or multiple-testing luck? Uses the cached paths (fast).

Candidate gate: pre2_mom_r >= ~0.55  AND  sig5_adx_calc >= ~26   (drop the no-op market_ret term)
Checks:
  1. THRESHOLD SENSITIVITY neighborhood (does PF survive when thresholds move?) at 2 exits
  2. monthly PnL of the masked book
  3. train-halves + full + test day-block
  4. exit stability
Run:  py -3.12 G_validate_passer.py
"""
from __future__ import annotations
from pathlib import Path
import pickle
import numpy as np
import pandas as pd
import walkforward_gate as wfg
from nse_intraday_costs import CostConfig

CACHE = Path(r"C:\TradingData\eqidv2\v11_G_paths_cache.pkl")
PROP = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_traintest_pool\proposals")
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


def main():
    with open(CACHE, "rb") as fh:
        recs = pickle.load(fh)
    df = pd.DataFrame([{k: v for k, v in r.items() if k not in ("highs", "lows", "closes")} for r in recs])
    dts = df["date"].to_numpy()
    is_tr, is_te = dts <= TRAIN_END, dts >= TEST_START
    h1 = is_tr & (dts <= TRAIN_MID); h2 = is_tr & (dts > TRAIN_MID)
    mom = df["pre2_mom_r"].to_numpy(float); adx = df["sig5_adx_calc"].to_numpy(float)
    finite = np.isfinite(mom) & np.isfinite(adx)
    print(f"paths={len(df)}  with finite pre2_mom_r & sig5_adx_calc = {finite.sum()}")

    def book(mask, net):
        return {"n": int(mask.sum()), "pf": round(_pf(net[mask]), 2),
                "net": round(float(net[mask].sum()), 0),
                "win": round((net[mask] > 0).mean() * 100, 1) if mask.sum() else float('nan')}

    # -------- 1. threshold sensitivity --------
    print("\n=== 1. THRESHOLD SENSITIVITY (gate: pre2_mom_r>=MOM & sig5_adx_calc>=ADX) ===")
    for sl, tgt in [(0.9, 2.5), (0.7, 1.25)]:
        net = net_for_exit(recs, sl, tgt)
        print(f"\n  --- exit {sl}/{tgt} ---  [train_pf / test_pf (test_n)]   (neighbourhood of the hit)")
        print("        ADX>=  " + "".join(f"{a:>16}" for a in (20, 24, 26, 28, 30)))
        for m in (0.40, 0.50, 0.55, 0.60, 0.70):
            cells = []
            for a in (20, 24, 26, 28, 30):
                msk = finite & (mom >= m) & (adx >= a)
                tr = msk & is_tr; te = msk & is_te
                if tr.sum() < 15:
                    cells.append("      .        ")
                    continue
                cells.append(f"{_pf(net[tr]):>5.2f}/{_pf(net[te]):>4.2f}(n{int(te.sum()):>2})")
            print(f"  mom>={m:<5} " + "".join(f"{c:>16}" for c in cells))

    # -------- 2/3. chosen gate full breakdown --------
    MOM, ADX = 0.55, 26.0
    gate = finite & (mom >= MOM) & (adx >= ADX)
    print(f"\n=== 2. CHOSEN GATE  pre2_mom_r>={MOM} & sig5_adx_calc>={ADX}  (rounded from the hit) ===")
    for sl, tgt in [(0.9, 2.5), (0.7, 1.25), (0.9, 1.5), (0.7, 1.0)]:
        net = net_for_exit(recs, sl, tgt)
        tr, te = gate & is_tr, gate & is_te
        bh1, bh2 = gate & h1, gate & h2
        print(f"  exit {sl}/{tgt}: TRAIN n={int(tr.sum())} pf={_pf(net[tr]):.2f} "
              f"[h1 {_pf(net[bh1]):.2f} n{int(bh1.sum())} / h2 {_pf(net[bh2]):.2f} n{int(bh2.sum())}] "
              f"| TEST n={int(te.sum())} pf={_pf(net[te]):.2f} win={(net[te]>0).mean()*100:.0f}% "
              f"p={_dbp(df.loc[te,'date'].to_numpy(), net[te]):.3f} | full pf={_pf(net[gate]):.2f} net Rs{net[gate].sum():,.0f}")

    # -------- 4. monthly (at 0.9/2.5) --------
    net = net_for_exit(recs, 0.9, 2.5)
    print("\n=== 3. MONTHLY PnL of the gated book (exit 0.9/2.5) ===")
    g = df[gate].copy(); g["net"] = net[gate]
    g["month"] = pd.to_datetime(g["date"]).dt.strftime("%Y-%m")
    for mth, sub in g.groupby("month"):
        print(f"  {mth}: n={len(sub):>2} pf={_pf(sub['net'].to_numpy()):>5.2f} net Rs{sub['net'].sum():>9,.0f} "
              f"win={(sub['net']>0).mean()*100:>4.0f}%")

    # -------- baseline (no gate) for reference --------
    print("\n=== 4. reference: UNGATED full setup (exit 0.9/2.5) ===")
    print(f"  TRAIN n={int(is_tr.sum())} pf={_pf(net[is_tr]):.2f} | TEST n={int(is_te.sum())} pf={_pf(net[is_te]):.2f} | full net Rs{net.sum():,.0f}")
    print("\n[verdict guidance] REAL pocket if PF>1.5 across most of the sensitivity grid AND both train halves >1.3 AND monthly mostly positive. Knife-edge/few-months => overfit.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
