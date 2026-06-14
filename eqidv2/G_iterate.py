"""
G_iterate.py — aggressive multi-iteration search for a train-PF>=2 config on
G_HIGHER_HIGH_BREAK (LONG) that ALSO survives the honest test window.

Stages (all NET of cost, exit co-optimized, day-block bootstrap on test):
  0. build entry->EOD 1-min PATHS once + rich feature vector (pool + pre-momentum feats); cache.
  1. greedy forward-selection (up to 4 mask terms) per exit, objective = ROBUST train PF
     (min of the two train halves) to resist within-train overfit.
  2. exhaustive 2-term search on the most promising feature thresholds x exits.
  3. randomized 3-term combos (thousands) x random exit.
For EVERY config reaching train PF>=2.0 (>=25 trades, >=12 days, both train halves >0),
report the HONEST test PF + day-block p + test n. PASS = test_pf>=1.3 & dbp<0.10 & test_n>=8.
Run:  py -3.12 G_iterate.py
"""
from __future__ import annotations
from pathlib import Path
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
CACHE = Path(r"C:\TradingData\eqidv2\v11_G_paths_cache.pkl")
TRAIN_END, TEST_START = "2026-04-30", "2026-05-01"
TRAIN_MID = "2026-02-15"   # splits train into two halves for the robust objective
CFG = CostConfig()
SETUP, SIDE = "G_HIGHER_HIGH_BREAK", "LONG"
PM_TERMS = (("pre3_close_pos", "<=", 0.985417), ("sig5_rsi_dir", "<=", 67.878))
EXIT_GRID = [(sl, tg) for sl in (0.50, 0.70, 0.90, 1.10) for tg in (0.80, 1.00, 1.25, 1.50, 2.00, 2.50)]
# numeric features eligible as mask thresholds (pool + a few pre-momentum feats)
POOL_FEATS = ["vol_ratio", "rs_pct", "market_ret_pct", "vwap_dist_atr", "close_loc",
              "quality_score", "atr_pct", "body_pct", "signal_minute"]
PM_FEATS = ["pre3_close_pos", "sig5_rsi_dir", "sig5_adx_calc", "sig5_rsi_dir",
            "pre5_mom_r", "pre10_mom_r", "pre_entry_momentum_score", "sig5_vol_ratio20",
            "pre3_range_r", "pre2_mom_r", "pre1_adx", "pre5_dir_count"]
QUANTILES = (0.15, 0.25, 0.35, 0.5, 0.65, 0.75, 0.85)
MIN_TRADES, MIN_DAYS = 25, 12
TRAIN_PF_TARGET = 2.0


# ---------------------------------------------------------------- path build
def build_paths():
    stt.POOL_DIRS = [CLEAN]
    pool = stt.load_pool().rename(columns={"tt_sig_ts": "tt_sig"})
    e = pool[pool["setup"].astype(str) == SETUP]
    recs = []
    for r in e.itertuples():
        bars1 = v11._load_1m_with_open(r.ticker)
        if bars1 is None or bars1.empty:
            continue
        ent = v11._first_1m_entry(bars1, r.tt_sig, max_delay_minutes=3)
        if ent is None:
            continue
        ets, raw_open = ent
        if raw_open <= 0:
            continue
        eod = ets.normalize() + pd.Timedelta(hours=15, minutes=20)
        sub = bars1[(bars1.index >= ets) & (bars1.index <= eod)]
        if sub.empty:
            continue
        d = r.tt_sig
        rec = {"date": d.strftime("%Y-%m-%d"), "signal_minute": float(d.hour * 60 + d.minute),
               "regime": str(getattr(r, "regime", "")), "raw_open": float(raw_open),
               "highs": sub["high"].to_numpy(float), "lows": sub["low"].to_numpy(float),
               "closes": sub["close"].to_numpy(float)}
        for f in POOL_FEATS:
            if f == "signal_minute":
                continue
            rec[f] = float(getattr(r, f, np.nan))
        fill = raw_open * 1.0005
        stop = fill * (1 - 0.9 / 100)
        feats, reason = v11._pre_entry_momentum_features_v11(r.ticker, SIDE, fill, stop, ets, d)
        rec["premom_pass"] = (not reason) and v11._eval_pre_momentum_terms(feats, PM_TERMS)[0]
        for f in set(PM_FEATS):
            try:
                rec[f] = float(feats.get(f, np.nan)) if feats else np.nan
            except Exception:
                rec[f] = np.nan
        recs.append(rec)
    print(f"[G_iterate] built {len(recs)} paths")
    return recs


def load_or_build():
    if CACHE.exists():
        with open(CACHE, "rb") as fh:
            recs = pickle.load(fh)
        print(f"[G_iterate] loaded {len(recs)} cached paths")
        return recs
    recs = build_paths()
    with open(CACHE, "wb") as fh:
        pickle.dump(recs, fh)
    return recs


# ---------------------------------------------------------------- sim (vectorized per exit)
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
    n = np.asarray(n, float)
    a, b = n[n > 0].sum(), -n[n < 0].sum()
    return float(a / b) if b > 0 else (float('inf') if a > 0 else 0.0)


def _dbp(dts, net, nb=6000, seed=7):
    s = pd.Series(net, index=pd.to_datetime(dts)).groupby(level=0).sum().to_numpy()
    if len(s) < 3:
        return float('nan')
    r = np.random.default_rng(seed)
    return float((s[r.integers(0, len(s), size=(nb, len(s)))].mean(axis=1) <= 0).mean())


# ---------------------------------------------------------------- main
def main():
    PROP.mkdir(parents=True, exist_ok=True)
    recs = load_or_build()
    df = pd.DataFrame([{k: v for k, v in r.items() if k not in ("highs", "lows", "closes")} for r in recs])
    dts = df["date"].to_numpy()
    is_train = dts <= TRAIN_END
    is_test = dts >= TEST_START
    tr_h1 = is_train & (dts <= TRAIN_MID)
    tr_h2 = is_train & (dts > TRAIN_MID)

    # precompute net for every exit
    net_by_exit = {ex: net_for_exit(recs, *ex) for ex in EXIT_GRID}
    print(f"[G_iterate] precomputed net for {len(EXIT_GRID)} exits")

    # candidate threshold terms (numeric, both directions, at quantiles) + regime categoricals
    feats = [f for f in (POOL_FEATS + list(dict.fromkeys(PM_FEATS)))
             if f in df.columns and df[f].notna().mean() > 0.6]
    cand_terms = []
    for f in feats:
        qs = df.loc[is_train, f].quantile(QUANTILES).values
        for q in np.unique(np.round(qs, 6)):
            cand_terms.append((f, ">=", float(q)))
            cand_terms.append((f, "<=", float(q)))
    for rg in ("NEUTRAL", "TREND", "BULL"):
        cand_terms.append(("regime", "==", rg))
        cand_terms.append(("regime", "!=", rg))

    def term_mask(t):
        f, op, v = t
        col = df[f]
        if op == ">=":
            return (col >= v).to_numpy()
        if op == "<=":
            return (col <= v).to_numpy()
        if op == "==":
            return (col == v).to_numpy()
        return (col != v).to_numpy()

    def stats(mask, net):
        mtr, mte = mask & is_train, mask & is_test
        ntr = net[mtr]
        days = df.loc[mtr, "date"].nunique()
        if len(ntr) < MIN_TRADES or days < MIN_DAYS:
            return None
        h1, h2 = net[mask & tr_h1], net[mask & tr_h2]
        robust = min(_pf(h1), _pf(h2)) if (len(h1) >= 5 and len(h2) >= 5) else 0.0
        nte = net[mte]
        return {"train_n": int(len(ntr)), "train_pf": round(_pf(ntr), 2), "train_days": int(days),
                "robust_pf": round(robust, 2), "test_n": int(len(nte)),
                "test_pf": round(_pf(nte), 2) if len(nte) else float('nan'),
                "test_win": round((nte > 0).mean() * 100, 1) if len(nte) else float('nan'),
                "test_dbp": round(_dbp(df.loc[mte, "date"].to_numpy(), nte), 3) if len(nte) >= 3 else float('nan'),
                "full_pf": round(_pf(net[mask]), 2), "net_rs": round(float(net[mask].sum()), 0)}

    results = []

    def record(stage, terms, ex, st):
        results.append({"stage": stage, "terms": "; ".join(f"{a}{o}{b}" for a, o, b in terms) or "(none)",
                        "n_terms": len(terms), "sl": ex[0], "tgt": ex[1], **st})

    # ---- Stage 1: greedy forward-selection per exit (objective robust_pf) ----
    print("\n[Stage 1] greedy forward-selection per exit (objective = robust train PF)...")
    for ex in EXIT_GRID:
        net = net_by_exit[ex]
        cur, cur_mask = [], np.ones(len(df), bool)
        base = stats(cur_mask, net)
        cur_obj = base["robust_pf"] if base else -1
        for _step in range(4):
            best_t, best_obj, best_st = None, cur_obj, None
            for t in cand_terms:
                m = cur_mask & term_mask(t)
                st = stats(m, net)
                if st is None:
                    continue
                if st["robust_pf"] > best_obj + 1e-9:
                    best_t, best_obj, best_st = t, st["robust_pf"], st
            if best_t is None:
                break
            cur.append(best_t)
            cur_mask = cur_mask & term_mask(best_t)
            cur_obj = best_obj
            record("greedy", list(cur), ex, best_st)

    # ---- Stage 2: exhaustive 2-term on the most promising thresholds ----
    print("[Stage 2] exhaustive 2-term on top features x exits...")
    top_feats = ["vwap_dist_atr", "rs_pct", "vol_ratio", "close_loc", "quality_score",
                 "market_ret_pct", "sig5_adx_calc", "pre_entry_momentum_score", "signal_minute"]
    two_terms = [t for t in cand_terms if t[0] in top_feats]
    for ex in EXIT_GRID[::2]:           # half the exits (speed) — still 12 exits
        net = net_by_exit[ex]
        for t1, t2 in itertools.combinations(two_terms, 2):
            if t1[0] == t2[0]:
                continue
            m = term_mask(t1) & term_mask(t2)
            st = stats(m, net)
            if st and st["train_pf"] >= 1.6:
                record("2term", [t1, t2], ex, st)

    # ---- Stage 3: randomized 3-term combos ----
    print("[Stage 3] randomized 3-term combos...")
    rng = np.random.default_rng(11)
    for _ in range(40000):
        ex = EXIT_GRID[rng.integers(len(EXIT_GRID))]
        ts = [cand_terms[i] for i in rng.choice(len(cand_terms), size=3, replace=False)]
        if len({t[0] for t in ts}) < 3:
            continue
        m = term_mask(ts[0]) & term_mask(ts[1]) & term_mask(ts[2])
        st = stats(m, net_by_exit[ex])
        if st and st["train_pf"] >= TRAIN_PF_TARGET:
            record("rand3", ts, ex, st)

    res = pd.DataFrame(results)
    res.to_csv(PROP / "G_iterate_results.csv", index=False)

    # ---- report ----
    print(f"\n[G_iterate] total configs recorded: {len(res)}")
    hit2 = res[(res.train_pf >= TRAIN_PF_TARGET) & (res.train_n >= MIN_TRADES) & (res.train_days >= MIN_DAYS)]
    hit2 = hit2.sort_values(["test_pf", "robust_pf"], ascending=False)
    print(f"\n=== configs reaching TRAIN PF>={TRAIN_PF_TARGET} (n>={MIN_TRADES}, days>={MIN_DAYS}): {len(hit2)} ===")
    print("    ranked by HONEST test PF (the real arbiter):\n")
    for _, r in hit2.head(25).iterrows():
        flag = "  <<< PASS" if (r["test_pf"] >= 1.3 and r["test_dbp"] < 0.10 and r["test_n"] >= 8) else ""
        print(f"  [{r['stage']:6}] exit {r['sl']}/{r['tgt']} | TRAIN n={int(r['train_n']):>3} PF={r['train_pf']:>5} "
              f"robust={r['robust_pf']:>4} | TEST n={int(r['test_n']):>3} PF={r['test_pf']:>5} win={r['test_win']:>4}% "
              f"p={r['test_dbp']} | {r['terms']}{flag}")

    passers = hit2[(hit2.test_pf >= 1.3) & (hit2.test_dbp < 0.10) & (hit2.test_n >= 8)]
    print(f"\n=== HONEST PASSERS (train PF>=2 AND test PF>=1.3 AND day-block p<0.10 AND test n>=8): {len(passers)} ===")
    if len(passers):
        for _, r in passers.head(15).iterrows():
            print(f"  exit {r['sl']}/{r['tgt']} | train {r['train_pf']} (n{int(r['train_n'])}) "
                  f"test {r['test_pf']} (n{int(r['test_n'])}, p{r['test_dbp']}) | {r['terms']}")
    else:
        # show how badly the best train configs fail OOS (the overfit story)
        best_train = res.sort_values("train_pf", ascending=False).head(8)
        print("  NONE. The highest-train-PF configs and what they do OOS (overfit evidence):")
        for _, r in best_train.iterrows():
            print(f"  train PF={r['train_pf']} (n{int(r['train_n'])}) -> TEST PF={r['test_pf']} "
                  f"(n{int(r['test_n'])}, p{r['test_dbp']}) | {r['terms'][:90]}")
    print(f"\n[G_iterate] wrote G_iterate_results.csv -> {PROP}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
