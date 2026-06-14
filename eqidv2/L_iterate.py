"""
L_iterate.py — aggressive multi-iteration + anti-overfit search for the L* family.
The gated clean pool starves L* (only L_BB_SQUEEZE_LONG survives, n=1 test), so this
reads the RAW pre-gate candidate pool (the only place with a real test sample). Four L*
setups (all LONG): L_PRESSURE_BURST_VWAP, L_DOUBLE_BOTTOM_VWAP, L_TREND_PULLBACK,
L_BB_SQUEEZE_LONG. Per setup: build entry->EOD PATHS + pre-momentum feats (cache), then
  - baseline at production exit + (where a gate exists) pre-momentum ON/OFF
  - greedy forward-selection (objective robust train PF = min of train halves)
  - exhaustive 2-term on top thresholds + 40k randomized 3-term, exit co-optimized
Surface every train-PF>=2 config with its HONEST test PF + day-block p. NET of cost.
CAVEAT printed: evaluated on RAW pre-gate candidates (production v8/research gates remove
most L* candidates) — any survivor must be reconciled with live gating.
Run:  py -3.12 L_iterate.py
"""
from __future__ import annotations
from pathlib import Path
import glob
import os
import pickle
import itertools
import numpy as np
import pandas as pd
import avwap_5min_ID_v11_backtesting as v11
import walkforward_gate as wfg
from nse_intraday_costs import CostConfig

CLEAN = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_cleanpool")
PROP = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_traintest_pool\proposals")
CACHE = Path(r"C:\TradingData\eqidv2\v11_L_paths_cache.pkl")
TRAIN_END, TEST_START, TRAIN_MID = "2026-04-30", "2026-05-01", "2026-02-15"
CFG = CostConfig()

SETUPS = {  # side, production exit, production pre-momentum gate (or None)
    "L_PRESSURE_BURST_VWAP": ("LONG", (1.10, 0.90), None),
    "L_DOUBLE_BOTTOM_VWAP": ("LONG", (0.70, 0.80), None),
    "L_TREND_PULLBACK": ("LONG", (0.70, 0.90),
                         (("pre_entry_momentum_score", ">=", 73.021), ("pre2_mom_r", ">=", 0.233909))),
    "L_BB_SQUEEZE_LONG": ("LONG", (0.75, 0.75), None),
}
# stratified sampling caps (train, test) to keep the path build tractable
SAMPLE = {
    "L_PRESSURE_BURST_VWAP": (800, 450),
    "L_DOUBLE_BOTTOM_VWAP": (700, 450),
    "L_TREND_PULLBACK": (10 ** 9, 10 ** 9),
    "L_BB_SQUEEZE_LONG": (10 ** 9, 10 ** 9),
}
POOL_FEATS = ["vol_ratio", "rs_pct", "market_ret_pct", "vwap_dist_atr", "close_loc",
              "quality_score", "atr_pct", "body_pct", "signal_minute"]
PM_FEATS = ["pre_entry_momentum_score", "pre2_mom_r", "pre3_close_pos", "sig5_rsi_dir",
            "sig5_adx_calc", "pre5_mom_r", "pre10_mom_r", "sig5_vol_ratio20", "pre3_range_r",
            "pre1_adx", "pre5_dir_count"]
QUANTILES = (0.15, 0.25, 0.35, 0.5, 0.65, 0.75, 0.85)
EXIT_GRID = [(sl, tg) for sl in (0.50, 0.70, 0.90, 1.10) for tg in (0.60, 0.80, 1.00, 1.25, 1.50, 2.00)]
MIN_TRADES, MIN_DAYS, TRAIN_PF_TARGET = 25, 12, 2.0


# ---------------------------------------------------------------- raw pool load
def load_raw():
    files = glob.glob(os.path.join(str(CLEAN), "chunk_*", "historical_all_available_raw_candidates.csv"))
    frames = []
    for f in files:
        try:
            frames.append(pd.read_csv(f, low_memory=False))
        except Exception:
            pass
    df = pd.concat(frames, ignore_index=True)
    df = df[df["setup"].astype(str).isin(SETUPS)].copy()
    df["sig"] = pd.to_datetime(df["signal_time_ist"], errors="coerce")
    df = df.dropna(subset=["sig"])
    df["d"] = df["sig"].dt.strftime("%Y-%m-%d")
    df = df.drop_duplicates(subset=["ticker", "setup", "signal_time_ist"])
    return df


def build_paths():
    raw = load_raw()
    out = {}
    for setup, (side, _ex, _pm) in SETUPS.items():
        e = raw[raw["setup"] == setup]
        cap_tr, cap_te = SAMPLE[setup]
        tr = e[e["d"] <= TRAIN_END]; te = e[e["d"] >= TEST_START]
        if len(tr) > cap_tr:
            tr = tr.sample(cap_tr, random_state=7)
        if len(te) > cap_te:
            te = te.sample(cap_te, random_state=7)
        e = pd.concat([tr, te], ignore_index=True)
        pm_terms = SETUPS[setup][2]
        paths = []
        for r in e.itertuples():
            bars1 = v11._load_1m_with_open(r.ticker)
            if bars1 is None or bars1.empty:
                continue
            ent = v11._first_1m_entry(bars1, r.sig, max_delay_minutes=3)
            if ent is None:
                continue
            ets, raw_open = ent
            if raw_open <= 0:
                continue
            eod = ets.normalize() + pd.Timedelta(hours=15, minutes=20)
            sub = bars1[(bars1.index >= ets) & (bars1.index <= eod)]
            if sub.empty:
                continue
            d = r.sig
            rec = {"date": d.strftime("%Y-%m-%d"), "signal_minute": float(d.hour * 60 + d.minute),
                   "regime": str(getattr(r, "regime", "")), "raw_open": float(raw_open),
                   "highs": sub["high"].to_numpy(float), "lows": sub["low"].to_numpy(float),
                   "closes": sub["close"].to_numpy(float)}
            for f in POOL_FEATS:
                if f == "signal_minute":
                    continue
                rec[f] = float(getattr(r, f, np.nan)) if pd.notna(getattr(r, f, np.nan)) else np.nan
            fill = raw_open * 1.0005
            stop = fill * (1 - 0.9 / 100)
            feats, reason = v11._pre_entry_momentum_features_v11(r.ticker, side, fill, stop, ets, d)
            rec["premom_pass"] = (not reason) and (pm_terms is None or v11._eval_pre_momentum_terms(feats, pm_terms)[0])
            for f in set(PM_FEATS):
                try:
                    rec[f] = float(feats.get(f, np.nan)) if feats else np.nan
                except Exception:
                    rec[f] = np.nan
            paths.append(rec)
        out[setup] = paths
        print(f"[L_iterate] {setup}: {len(paths)} paths built (from {len(e)} sampled raw candidates)")
    return out


def load_or_build():
    if CACHE.exists():
        with open(CACHE, "rb") as fh:
            out = pickle.load(fh)
        print(f"[L_iterate] loaded cached paths: " + ", ".join(f"{k}={len(v)}" for k, v in out.items()))
        return out
    out = build_paths()
    with open(CACHE, "wb") as fh:
        pickle.dump(out, fh)
    return out


# ---------------------------------------------------------------- sim/stats
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


def search_setup(setup, recs):
    side, prod_exit, pm_terms = SETUPS[setup]
    df = pd.DataFrame([{k: v for k, v in r.items() if k not in ("highs", "lows", "closes")} for r in recs])
    dts = df["date"].to_numpy()
    is_tr, is_te = dts <= TRAIN_END, dts >= TEST_START
    h1 = is_tr & (dts <= TRAIN_MID); h2 = is_tr & (dts > TRAIN_MID)
    net_by_exit = {ex: net_for_exit(recs, *ex) for ex in EXIT_GRID}

    feats = [f for f in (POOL_FEATS + list(dict.fromkeys(PM_FEATS)))
             if f in df.columns and df[f].notna().mean() > 0.6]
    cand = []
    for f in feats:
        for q in np.unique(np.round(df.loc[is_tr, f].quantile(QUANTILES).values, 6)):
            cand.append((f, ">=", float(q))); cand.append((f, "<=", float(q)))
    for rg in ("NEUTRAL", "TREND", "BULL"):
        cand.append(("regime", "==", rg)); cand.append(("regime", "!=", rg))

    def tmask(t):
        f, op, v = t; col = df[f]
        return {">=": col >= v, "<=": col <= v, "==": col == v, "!=": col != v}[op].to_numpy()

    def stats(mask, net):
        mtr = mask & is_tr
        ntr = net[mtr]; days = df.loc[mtr, "date"].nunique()
        if len(ntr) < MIN_TRADES or days < MIN_DAYS:
            return None
        a, b = net[mask & h1], net[mask & h2]
        robust = min(_pf(a), _pf(b)) if (len(a) >= 5 and len(b) >= 5) else 0.0
        nte = net[mask & is_te]
        return {"train_n": int(len(ntr)), "train_pf": round(_pf(ntr), 2), "train_days": int(days),
                "robust_pf": round(robust, 2), "test_n": int(len(nte)),
                "test_pf": round(_pf(nte), 2) if len(nte) else float('nan'),
                "test_win": round((nte > 0).mean() * 100, 1) if len(nte) else float('nan'),
                "test_dbp": round(_dbp(df.loc[mask & is_te, "date"].to_numpy(), nte), 3) if len(nte) >= 3 else float('nan'),
                "full_pf": round(_pf(net[mask]), 2), "net_rs": round(float(net[mask].sum()), 0)}

    rows = []

    def rec(stage, terms, ex, st):
        rows.append({"setup": setup, "stage": stage,
                     "terms": "; ".join(f"{a}{o}{b}" for a, o, b in terms) or "(none)",
                     "n_terms": len(terms), "sl": ex[0], "tgt": ex[1], **st})

    # baseline + premom on/off
    base_lines = []
    for label, sel in [("ALL", np.ones(len(df), bool)),
                       ("premom_ON", df["premom_pass"].to_numpy()),
                       ("premom_OFF", ~df["premom_pass"].to_numpy())]:
        net = net_by_exit[prod_exit] if prod_exit in net_by_exit else net_for_exit(recs, *prod_exit)
        st = stats(sel, net)
        if st:
            base_lines.append(f"{setup:22} {label:11} exit {prod_exit[0]}/{prod_exit[1]} | "
                              f"TRAIN n={st['train_n']:>4} PF={st['train_pf']:.2f} | TEST n={st['test_n']:>3} "
                              f"PF={st['test_pf']:.2f} win={st['test_win']:.0f}% p={st['test_dbp']} | net Rs{st['net_rs']:,.0f}")

    # Stage 1 greedy
    for ex in EXIT_GRID:
        net = net_by_exit[ex]; cur, cm = [], np.ones(len(df), bool)
        b = stats(cm, net); obj = b["robust_pf"] if b else -1
        for _ in range(4):
            bt, bo, bs = None, obj, None
            for t in cand:
                st = stats(cm & tmask(t), net)
                if st and st["robust_pf"] > bo + 1e-9:
                    bt, bo, bs = t, st["robust_pf"], st
            if bt is None:
                break
            cur.append(bt); cm = cm & tmask(bt); obj = bo; rec("greedy", list(cur), ex, bs)

    # Stage 2 exhaustive 2-term
    topf = ["vol_ratio", "rs_pct", "vwap_dist_atr", "close_loc", "quality_score", "market_ret_pct",
            "sig5_adx_calc", "pre_entry_momentum_score", "pre2_mom_r", "signal_minute", "body_pct"]
    tt = [t for t in cand if t[0] in topf]
    for ex in EXIT_GRID[::2]:
        net = net_by_exit[ex]
        for t1, t2 in itertools.combinations(tt, 2):
            if t1[0] == t2[0]:
                continue
            st = stats(tmask(t1) & tmask(t2), net)
            if st and st["train_pf"] >= 1.6:
                rec("2term", [t1, t2], ex, st)

    # Stage 3 randomized 3-term
    rng = np.random.default_rng(11)
    for _ in range(40000):
        ex = EXIT_GRID[rng.integers(len(EXIT_GRID))]
        ts = [cand[i] for i in rng.choice(len(cand), size=3, replace=False)]
        if len({t[0] for t in ts}) < 3:
            continue
        st = stats(tmask(ts[0]) & tmask(ts[1]) & tmask(ts[2]), net_by_exit[ex])
        if st and st["train_pf"] >= TRAIN_PF_TARGET:
            rec("rand3", ts, ex, st)

    return base_lines, rows


def main():
    PROP.mkdir(parents=True, exist_ok=True)
    allp = load_or_build()
    print("\n" + "=" * 30 + " CAVEAT " + "=" * 30)
    print("L* evaluated on RAW pre-gate candidates (the gated clean pool starves L*:")
    print("only L_BB_SQUEEZE survives, n=1 test). Production v8/research gates remove most")
    print("L* candidates — any survivor below must be reconciled with live gating.\n")

    all_rows, all_base = [], []
    for setup, recs in allp.items():
        if len(recs) < MIN_TRADES:
            all_base.append(f"{setup}: {len(recs)} paths INSUFFICIENT")
            continue
        bl, rows = search_setup(setup, recs)
        all_base += bl; all_rows += rows
        print(f"[L_iterate] {setup}: searched, {len(rows)} configs recorded")

    res = pd.DataFrame(all_rows)
    res.to_csv(PROP / "L_iterate_results.csv", index=False)

    print("\n=== BASELINE (production exit) + premom on/off ===")
    for s in all_base:
        print("  " + s)

    for setup in SETUPS:
        s = res[res.setup == setup] if len(res) else res
        if not len(s):
            continue
        hit = s[(s.train_pf >= TRAIN_PF_TARGET) & (s.train_n >= MIN_TRADES) & (s.train_days >= MIN_DAYS)]
        hit = hit.sort_values(["test_pf", "robust_pf"], ascending=False)
        print(f"\n=== {setup}: TRAIN PF>=2 configs = {len(hit)} (ranked by HONEST test PF) ===")
        for _, r in hit.head(10).iterrows():
            flag = "  <<< PASS" if (r["test_pf"] >= 1.3 and r["test_dbp"] < 0.10 and r["test_n"] >= 8) else ""
            print(f"  [{r['stage']:6}] exit {r['sl']}/{r['tgt']} | TR n={int(r['train_n']):>3} PF={r['train_pf']:>5} "
                  f"rob={r['robust_pf']:>4} | TE n={int(r['test_n']):>3} PF={r['test_pf']:>5} win={r['test_win']:>4}% "
                  f"p={r['test_dbp']} | {r['terms']}{flag}")
        passers = hit[(hit.test_pf >= 1.3) & (hit.test_dbp < 0.10) & (hit.test_n >= 8)]
        print(f"  HONEST PASSERS (train>=2 & test>=1.3 & p<0.10 & test_n>=8): {len(passers)}")

    print(f"\n[L_iterate] wrote L_iterate_results.csv -> {PROP}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
