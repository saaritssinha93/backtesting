"""
S_iterate.py — aggressive iteration + anti-overfit search for the S* (SHORT) family.
Two setups with data:
  - S_BB_SQUEEZE_SHORT  : GATED CLEAN pool (196/16) — preferred, no raw caveat. exit 1.00/1.50
  - S_MACD_HIST_FLIP    : RAW pre-gate pool (4808/297, sampled) — gated out, caveat. exit 0.70/1.50
Per setup: build entry->EOD PATHS (SHORT) + pre-momentum feats; then
  - baseline at production exit
  - greedy forward-selection (robust objective = min train-halves)
  - exhaustive 2-term + 40k random 3-term, exit co-optimized
Reports BOTH (a) train-PF>=2 configs ranked by honest test, and (b) robustness-first
(min(h1,h2,test)) configs. Day-block bootstrap computed only for FINALISTS (fast). NET of cost.
Run:  py -3.12 S_iterate.py
"""
from __future__ import annotations
from pathlib import Path
import glob
import os
import pickle
import itertools
import numpy as np
import pandas as pd
import setup_train_test as stt
import avwap_5min_ID_v11_backtesting as v11
import walkforward_gate as wfg
from nse_intraday_costs import CostConfig

CLEAN = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_cleanpool")
PROP = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_traintest_pool\proposals")
CACHE = Path(r"C:\TradingData\eqidv2\v11_S_paths_cache.pkl")
TRAIN_END, TEST_START, MID = "2026-04-30", "2026-05-01", "2026-02-15"
CFG = CostConfig()

# setup -> (side, source, production exit, train/test sample caps)
SETUPS = {
    "S_BB_SQUEEZE_SHORT": ("SHORT", "clean", (1.00, 1.50), (10**9, 10**9)),
    "S_MACD_HIST_FLIP": ("SHORT", "raw", (0.70, 1.50), (1500, 800)),
}
POOL_FEATS = ["vol_ratio", "rs_pct", "market_ret_pct", "vwap_dist_atr", "close_loc",
              "quality_score", "atr_pct", "body_pct", "signal_minute"]
PM_FEATS = ["pre_entry_momentum_score", "pre2_mom_r", "pre3_close_pos", "sig5_rsi_dir",
            "sig5_adx_calc", "pre5_mom_r", "pre10_mom_r", "sig5_vol_ratio20", "pre3_range_r",
            "pre1_adx", "pre5_dir_count"]
QUANTILES = (0.2, 0.35, 0.5, 0.65, 0.8)
EXIT_GRID = [(sl, tg) for sl in (0.50, 0.70, 0.90, 1.10) for tg in (0.80, 1.00, 1.25, 1.50, 2.00, 2.50)]
MIN_TRADES, MIN_DAYS, MIN_TEST, TRAIN_PF_TARGET = 25, 12, 8, 2.0


def _load_raw_setup(setup):
    files = glob.glob(os.path.join(str(CLEAN), "chunk_*", "historical_all_available_raw_candidates.csv"))
    frames = []
    for f in files:
        try:
            d = pd.read_csv(f, low_memory=False)
            frames.append(d[d["setup"].astype(str) == setup])
        except Exception:
            pass
    df = pd.concat(frames, ignore_index=True)
    df["sig"] = pd.to_datetime(df["signal_time_ist"], errors="coerce")
    df = df.dropna(subset=["sig"]).drop_duplicates(subset=["ticker", "setup", "signal_time_ist"])
    return df


def build_paths():
    out = {}
    clean = None
    for setup, (side, src, _ex, (cap_tr, cap_te)) in SETUPS.items():
        if src == "clean":
            if clean is None:
                stt.POOL_DIRS = [CLEAN]
                clean = stt.load_pool().rename(columns={"tt_sig_ts": "sig"})
            e = clean[clean["setup"].astype(str) == setup].copy()
            e["d"] = pd.to_datetime(e["sig"]).dt.strftime("%Y-%m-%d")
        else:
            e = _load_raw_setup(setup)
            e["d"] = e["sig"].dt.strftime("%Y-%m-%d")
        tr = e[e["d"] <= TRAIN_END]; te = e[e["d"] >= TEST_START]
        if len(tr) > cap_tr:
            tr = tr.sample(cap_tr, random_state=7)
        if len(te) > cap_te:
            te = te.sample(cap_te, random_state=7)
        e = pd.concat([tr, te], ignore_index=True)
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
                   "regime": str(getattr(r, "regime", "")), "raw_open": float(raw_open), "side": side,
                   "highs": sub["high"].to_numpy(float), "lows": sub["low"].to_numpy(float),
                   "closes": sub["close"].to_numpy(float)}
            for f in POOL_FEATS:
                if f == "signal_minute":
                    continue
                v = getattr(r, f, np.nan)
                rec[f] = float(v) if pd.notna(v) else np.nan
            fill = raw_open * (0.9995 if side == "SHORT" else 1.0005)
            stop = fill * (1 + 0.9 / 100) if side == "SHORT" else fill * (1 - 0.9 / 100)
            feats, reason = v11._pre_entry_momentum_features_v11(r.ticker, side, fill, stop, ets, d)
            for f in set(PM_FEATS):
                try:
                    rec[f] = float(feats.get(f, np.nan)) if feats else np.nan
                except Exception:
                    rec[f] = np.nan
            paths.append(rec)
        out[setup] = paths
        print(f"[S_iterate] {setup}: {len(paths)} paths ({src} pool, {len(e)} sampled)")
    return out


def load_or_build():
    if CACHE.exists():
        with open(CACHE, "rb") as fh:
            out = pickle.load(fh)
        print("[S_iterate] loaded cache: " + ", ".join(f"{k}={len(v)}" for k, v in out.items()))
        return out
    out = build_paths()
    with open(CACHE, "wb") as fh:
        pickle.dump(out, fh)
    return out


def net_for_exit(recs, sl, tgt):
    out = np.empty(len(recs))
    for i, p in enumerate(recs):
        side = p["side"]
        e = p["raw_open"] * (0.9995 if side == "SHORT" else 1.0005)
        qty = max(1, int(100000.0 / p["raw_open"]))
        h, l, c = p["highs"], p["lows"], p["closes"]
        if side == "SHORT":
            slp, tgp = e * (1 + sl / 100), e * (1 - tgt / 100)
            slh, tgh = h >= slp, l <= tgp
        else:
            slp, tgp = e * (1 - sl / 100), e * (1 + tgt / 100)
            slh, tgh = l <= slp, h >= tgp
        fsl = int(np.argmax(slh)) if slh.any() else 10 ** 9
        ftg = int(np.argmax(tgh)) if tgh.any() else 10 ** 9
        xp = slp if (slh.any() and fsl <= ftg) else (tgp if tgh.any() else c[-1])
        out[i] = wfg.net_pnl_vectorized(np.array([e]), np.array([xp]), np.array([qty]), np.array([side]), CFG)[0]
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


def search(setup, recs):
    side, src, prod_exit, _ = SETUPS[setup]
    print("\n" + "#" * 70 + f"\n# {setup}  ({src} pool, n={len(recs)}, side {side})\n" + "#" * 70)
    df = pd.DataFrame([{k: v for k, v in r.items() if k not in ("highs", "lows", "closes")} for r in recs])
    dts = df["date"].to_numpy()
    is_tr, is_te = dts <= TRAIN_END, dts >= TEST_START
    h1 = is_tr & (dts <= MID); h2 = is_tr & (dts > MID)
    net_by_exit = {ex: net_for_exit(recs, *ex) for ex in EXIT_GRID}

    def stats(mask, net, boot=False):
        mtr = mask & is_tr
        if mtr.sum() < MIN_TRADES or df.loc[mtr, "date"].nunique() < MIN_DAYS:
            return None
        a, b, t = net[mask & h1], net[mask & h2], net[mask & is_te]
        if len(a) < 5 or len(b) < 5 or len(t) < MIN_TEST:
            return None
        pa, pb, pt = _pf(a), _pf(b), _pf(t)
        return {"train_n": int(mtr.sum()), "train_pf": round(_pf(net[mtr]), 2), "h1": round(pa, 2),
                "h2": round(pb, 2), "test_n": int(len(t)), "test_pf": round(pt, 2),
                "test_win": round((t > 0).mean() * 100, 1), "minpf": round(min(pa, pb, pt), 2),
                "test_dbp": round(_dbp(df.loc[mask & is_te, "date"].to_numpy(), t), 3) if boot else np.nan,
                "full_pf": round(_pf(net[mask]), 2), "net": round(float(net[mask].sum()), 0)}

    prod_net = net_by_exit[prod_exit] if prod_exit in net_by_exit else net_for_exit(recs, *prod_exit)
    st0 = stats(np.ones(len(df), bool), prod_net, boot=True)
    if st0:
        print(f"  baseline ALL exit {prod_exit[0]}/{prod_exit[1]}: TR n={st0['train_n']} pf={st0['train_pf']} "
              f"| TE n={st0['test_n']} pf={st0['test_pf']} p={st0['test_dbp']} net Rs{st0['net']:,.0f}")

    feats = [f for f in (POOL_FEATS + list(dict.fromkeys(PM_FEATS)))
             if f in df.columns and df[f].notna().mean() > 0.6]
    cand = []
    for f in feats:
        for q in np.unique(np.round(df.loc[is_tr, f].quantile(QUANTILES).values, 6)):
            cand.append((f, ">=", float(q))); cand.append((f, "<=", float(q)))
    for rg in ("NEUTRAL", "TREND", "BEAR"):
        cand.append(("regime", "==", rg))
    masks = {t: ({">=": df[t[0]] >= t[2], "<=": df[t[0]] <= t[2], "==": df[t[0]] == t[2]}[t[1]]).to_numpy() for t in cand}

    rows = []
    def rec(stage, terms, ex, st):
        rows.append({"stage": stage, "terms": "; ".join(f"{a}{o}{b}" for a, o, b in terms),
                     "sl": ex[0], "tgt": ex[1], **st})

    # greedy (robust)
    for ex in EXIT_GRID:
        net = net_by_exit[ex]; cur, cm = [], np.ones(len(df), bool)
        b = stats(cm, net); obj = b["minpf"] if b else -1
        for _ in range(4):
            bt, bo, bs = None, obj, None
            for t in cand:
                st = stats(cm & masks[t], net)
                if st and st["minpf"] > bo + 1e-9:
                    bt, bo, bs = t, st["minpf"], st
            if bt is None:
                break
            cur.append(bt); cm = cm & masks[bt]; obj = bo; rec("greedy", list(cur), ex, bs)
    # 2-term exhaustive
    for ex in EXIT_GRID:
        net = net_by_exit[ex]
        for t1, t2 in itertools.combinations(cand, 2):
            if t1[0] == t2[0]:
                continue
            st = stats(masks[t1] & masks[t2], net)
            if st and (st["minpf"] >= 1.2 or st["train_pf"] >= TRAIN_PF_TARGET):
                rec("2term", [t1, t2], ex, st)
    # random 3-term
    rng = np.random.default_rng(11)
    for _ in range(40000):
        ex = EXIT_GRID[rng.integers(len(EXIT_GRID))]
        ts = [cand[i] for i in rng.choice(len(cand), size=3, replace=False)]
        if len({t[0] for t in ts}) < 3:
            continue
        st = stats(masks[ts[0]] & masks[ts[1]] & masks[ts[2]], net_by_exit[ex])
        if st and (st["minpf"] >= 1.3 or st["train_pf"] >= TRAIN_PF_TARGET):
            rec("rand3", ts, ex, st)

    res = pd.DataFrame(rows)
    if not len(res):
        print("  no qualifying configs.")
        return setup, res
    res["setup"] = setup

    # finalists: top by minpf and top by test among train-PF>=2 -> compute bootstrap
    fin_idx = set(res.sort_values("minpf", ascending=False).head(25).index) | \
              set(res[res.train_pf >= TRAIN_PF_TARGET].sort_values("test_pf", ascending=False).head(25).index)
    for i in fin_idx:
        m = np.ones(len(df), bool)
        for term in res.loc[i, "terms"].split("; "):
            for op in (">=", "<=", "=="):
                if op in term:
                    f, v = term.split(op)
                    f, v = f.strip(), v.strip()
                    col = df[f]
                    if op == ">=":
                        mm = col >= float(v)
                    elif op == "<=":
                        mm = col <= float(v)
                    else:
                        mm = col == v
                    m = m & mm.to_numpy()
                    break
        t = net_by_exit[(res.loc[i, "sl"], res.loc[i, "tgt"])][m & is_te]
        res.loc[i, "test_dbp"] = round(_dbp(df.loc[m & is_te, "date"].to_numpy(), t), 3)

    # report
    hit = res[(res.train_pf >= TRAIN_PF_TARGET) & res.test_dbp.notna()].sort_values("test_pf", ascending=False)
    print(f"\n  [train-PF>=2 configs, ranked by honest test] ({len(res[res.train_pf>=TRAIN_PF_TARGET])} total):")
    for _, r in hit.head(8).iterrows():
        tag = "  <<<PASS" if (r["test_pf"] >= 1.3 and r["test_dbp"] < 0.10 and r["test_n"] >= MIN_TEST) else ""
        print(f"    {r['stage']:6} {r['sl']}/{r['tgt']} | TR n={int(r['train_n']):>3} pf={r['train_pf']:>4} [h1 {r['h1']}/h2 {r['h2']}]"
              f" | TE n={int(r['test_n']):>2} pf={r['test_pf']:>4} p={r['test_dbp']} | {r['terms']}{tag}")
    rob = res[res.test_dbp.notna()].sort_values("minpf", ascending=False)
    print(f"\n  [robustness-first, ranked by min(h1,h2,test)]:")
    for _, r in rob.head(8).iterrows():
        tag = "  <<<PASS" if (r["train_pf"] >= 1.7 and r["minpf"] >= 1.3 and r["test_dbp"] < 0.10 and r["test_n"] >= MIN_TEST) else ""
        print(f"    {r['stage']:6} {r['sl']}/{r['tgt']} | TR n={int(r['train_n']):>3} pf={r['train_pf']:>4} [h1 {r['h1']}/h2 {r['h2']}]"
              f" | TE n={int(r['test_n']):>2} pf={r['test_pf']:>4} minpf={r['minpf']:>4} p={r['test_dbp']} | {r['terms']}{tag}")
    return setup, res


def main():
    PROP.mkdir(parents=True, exist_ok=True)
    allp = load_or_build()
    outs = []
    for setup in SETUPS:
        if len(allp.get(setup, [])) < MIN_TRADES:
            print(f"\n{setup}: insufficient paths"); continue
        _, res = search(setup, allp[setup])
        if len(res):
            outs.append(res)
    if outs:
        pd.concat(outs, ignore_index=True).to_csv(PROP / "S_iterate_results.csv", index=False)
    print(f"\n[S_iterate] done -> {PROP}\\S_iterate_results.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
