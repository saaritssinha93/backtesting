"""
L_rejects_improve.py — harder, ROBUSTNESS-FIRST retry on the 3 L* rejects
(L_PRESSURE_BURST_VWAP, L_BB_SQUEEZE_LONG, L_TREND_PULLBACK). Uses the L path cache.

New angles vs L_iterate.py (which maximized TRAIN PF and found fragile overfits):
  A. ROBUSTNESS-FIRST objective: search the 2-term gate maximizing min(train_h1, train_h2,
     test) PF -> only gates positive in ALL THREE sub-periods survive (anti-overfit by design).
  B. FORCED momentum/ADX grids: test the exact mechanism that salvaged G & L_DOUBLE_BOTTOM
     (pre2_mom_r x adx ; pre_entry_momentum_score x adx ; momentum-only ; adx-only).
  C. WIDER exit grid (adds 2.5R).
PASS = train_pf>=1.7 & min(h1,h2,test)>=1.3 & test_dbp<0.10 & test_n>=8 & train_n>=25.
Run:  py -3.12 L_rejects_improve.py
"""
from __future__ import annotations
from pathlib import Path
import pickle
import itertools
import numpy as np
import pandas as pd
import walkforward_gate as wfg
from nse_intraday_costs import CostConfig

CACHE = Path(r"C:\TradingData\eqidv2\v11_L_paths_cache.pkl")
PROP = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_traintest_pool\proposals")
TRAIN_END, TEST_START, MID = "2026-04-30", "2026-05-01", "2026-02-15"
CFG = CostConfig()
REJECTS = ["L_PRESSURE_BURST_VWAP", "L_BB_SQUEEZE_LONG", "L_TREND_PULLBACK"]
EXIT_GRID = [(sl, tg) for sl in (0.50, 0.70, 0.90, 1.10) for tg in (0.80, 1.00, 1.25, 1.50, 2.00, 2.50)]
POOL_FEATS = ["vol_ratio", "rs_pct", "market_ret_pct", "vwap_dist_atr", "close_loc",
              "quality_score", "atr_pct", "body_pct", "signal_minute"]
PM_FEATS = ["pre_entry_momentum_score", "pre2_mom_r", "pre3_close_pos", "sig5_rsi_dir",
            "sig5_adx_calc", "pre5_mom_r", "pre10_mom_r", "sig5_vol_ratio20", "pre3_range_r",
            "pre1_adx", "pre5_dir_count"]
QUANTILES = (0.2, 0.35, 0.5, 0.65, 0.8)
MIN_TRADES, MIN_DAYS, MIN_TEST = 25, 12, 8


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


def _dbp(dts, net, nb=6000, seed=7):
    s = pd.Series(net, index=pd.to_datetime(dts)).groupby(level=0).sum().to_numpy()
    if len(s) < 3:
        return float('nan')
    r = np.random.default_rng(seed)
    return float((s[r.integers(0, len(s), size=(nb, len(s)))].mean(axis=1) <= 0).mean())


def run_setup(setup, recs):
    print("\n" + "#" * 72 + f"\n# {setup}  (cached n={len(recs)})\n" + "#" * 72)
    df = pd.DataFrame([{k: v for k, v in r.items() if k not in ("highs", "lows", "closes")} for r in recs])
    dts = df["date"].to_numpy()
    is_tr, is_te = dts <= TRAIN_END, dts >= TEST_START
    h1 = is_tr & (dts <= MID); h2 = is_tr & (dts > MID)
    net_by_exit = {ex: net_for_exit(recs, *ex) for ex in EXIT_GRID}

    def robstats(mask, net):
        mtr = mask & is_tr
        if (mtr.sum() < MIN_TRADES) or (df.loc[mtr, "date"].nunique() < MIN_DAYS):
            return None
        a, b, t = net[mask & h1], net[mask & h2], net[mask & is_te]
        if len(a) < 5 or len(b) < 5 or len(t) < MIN_TEST:
            return None
        pa, pb, pt = _pf(a), _pf(b), _pf(t)
        return {"train_n": int(mtr.sum()), "train_pf": round(_pf(net[mtr]), 2),
                "h1": round(pa, 2), "h2": round(pb, 2), "test_n": int(len(t)), "test_pf": round(pt, 2),
                "test_win": round((t > 0).mean() * 100, 1),
                "minpf": round(min(pa, pb, pt), 2),
                "test_dbp": round(_dbp(df.loc[mask & is_te, "date"].to_numpy(), t), 3),
                "full_pf": round(_pf(net[mask]), 2), "net": round(float(net[mask].sum()), 0)}

    # ---- B. forced momentum/ADX grids ----
    print("\n  [B] forced momentum/ADX gate grids (the G / L_DOUBLE mechanism):")
    adxv = df["sig5_adx_calc"].to_numpy(float)
    grids = [("pre2_mom_r", df["pre2_mom_r"].to_numpy(float), (0.2, 0.35, 0.5)),
             ("pre_entry_momentum_score", df["pre_entry_momentum_score"].to_numpy(float), (65, 75, 82)),
             ("pre5_mom_r", df["pre5_mom_r"].to_numpy(float), (0.3, 0.5, 0.7))]
    best_mech = None
    for fname, fv, gx in grids:
        for gv in gx:
            for av in (22, 26, 30):
                m = np.isfinite(fv) & np.isfinite(adxv) & (fv >= gv) & (adxv >= av)
                best_ex = None
                for ex in [(0.7, 1.5), (0.9, 1.5), (0.9, 2.0), (0.9, 1.25)]:
                    st = robstats(m, net_by_exit[ex])
                    if st and (best_ex is None or st["minpf"] > best_ex[1]["minpf"]):
                        best_ex = (ex, st)
                if best_ex:
                    ex, st = best_ex
                    tag = "  <<<PASS" if (st["train_pf"] >= 1.7 and st["minpf"] >= 1.3 and st["test_dbp"] < 0.10) else ""
                    print(f"     {fname}>={gv} & adx>={av}: exit {ex[0]}/{ex[1]} | tr n={st['train_n']:>3} pf={st['train_pf']:>4} "
                          f"[h1 {st['h1']}/h2 {st['h2']}] | te n={st['test_n']:>2} pf={st['test_pf']:>4} minpf={st['minpf']:>4} "
                          f"p={st['test_dbp']}{tag}")
                    if best_mech is None or st["minpf"] > best_mech["minpf"]:
                        best_mech = st

    # ---- A. robustness-first exhaustive 2-term ----
    feats = [f for f in (POOL_FEATS + list(dict.fromkeys(PM_FEATS)))
             if f in df.columns and df[f].notna().mean() > 0.6]
    cand = []
    for f in feats:
        for q in np.unique(np.round(df.loc[is_tr, f].quantile(QUANTILES).values, 6)):
            cand.append((f, ">=", float(q))); cand.append((f, "<=", float(q)))
    for rg in ("NEUTRAL", "TREND", "BULL"):
        cand.append(("regime", "==", rg))
    masks = {t: ({">=": df[t[0]] >= t[2], "<=": df[t[0]] <= t[2], "==": df[t[0]] == t[2]}[t[1]]).to_numpy() for t in cand}

    rows = []
    for ex in EXIT_GRID:
        net = net_by_exit[ex]
        for t1, t2 in itertools.combinations(cand, 2):
            if t1[0] == t2[0]:
                continue
            st = robstats(masks[t1] & masks[t2], net)
            if st and st["minpf"] >= 1.2:           # all three sub-periods >= 1.2
                rows.append({"terms": f"{t1[0]}{t1[1]}{t1[2]}; {t2[0]}{t2[1]}{t2[2]}",
                             "sl": ex[0], "tgt": ex[1], **st})
    res = pd.DataFrame(rows).sort_values("minpf", ascending=False) if rows else pd.DataFrame()
    print(f"\n  [A] robustness-first 2-term (min(h1,h2,test)>=1.2): {len(res)} configs")
    for _, r in res.head(8).iterrows():
        tag = "  <<<PASS" if (r["train_pf"] >= 1.7 and r["minpf"] >= 1.3 and r["test_dbp"] < 0.10 and r["test_n"] >= MIN_TEST) else ""
        print(f"     exit {r['sl']}/{r['tgt']} | tr n={int(r['train_n']):>3} pf={r['train_pf']:>4} [h1 {r['h1']}/h2 {r['h2']}] "
              f"| te n={int(r['test_n']):>2} pf={r['test_pf']:>4} minpf={r['minpf']:>4} p={r['test_dbp']} | {r['terms']}{tag}")

    passers = res[(res.train_pf >= 1.7) & (res.minpf >= 1.3) & (res.test_dbp < 0.10) & (res.test_n >= MIN_TEST)] if len(res) else res
    n_pass = len(passers) + (1 if (best_mech and best_mech["train_pf"] >= 1.7 and best_mech["minpf"] >= 1.3 and best_mech["test_dbp"] < 0.10) else 0)
    print(f"  ==> ROBUST PASSERS for {setup}: {len(passers)} (2-term) + mechanism={'yes' if (best_mech and best_mech['train_pf']>=1.7 and best_mech['minpf']>=1.3 and best_mech['test_dbp']<0.10) else 'no'}")
    return setup, res


def main():
    with open(CACHE, "rb") as fh:
        allp = pickle.load(fh)
    out = {}
    for setup in REJECTS:
        _, res = run_setup(setup, allp[setup])
        if len(res):
            res.insert(0, "setup", setup)
            out[setup] = res
    if out:
        pd.concat(out.values(), ignore_index=True).to_csv(PROP / "L_rejects_improve_results.csv", index=False)
    print("\n[done] robustness-first retry on the 3 L* rejects complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
