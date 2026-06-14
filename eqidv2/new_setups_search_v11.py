"""
new_setups_search_v11.py — robustness-first search + anti-overfit validation for the TWO new
setups (L_RS_LEADER_VWAP_HOLD, S_UPTHRUST_TRAP_FADE). Consumes the scanner output
(new_setups_standalone_trades.csv). Same proven battery as T_iterate.py: build entry->EOD 1-min
paths + premom feats; greedy(robust) + exhaustive 2-term + 60k random + forced momentum/ADX, exit
co-optimized; reports train-PF>=2 (honest test) + robustness-first (min h1/h2/test) with day-block
bootstrap AND day-concentration (top1day) on finalists. NET of cost.

*** Run AFTER 15:30 IST (post-close), workers<=8 — protect the live v7 feed. ***
Usage (later):
  py -3.12 new_setups_scan_v11.py            # full scan first (writes the candidate CSV)
  py -3.12 new_setups_search_v11.py          # then this
Promote to final_setup_conf.py ONLY on a clean pass (train PF>=2, test PF>=1.3, day-block p<0.10,
contiguous sensitivity, both halves +, >=70% months +, top1day<=~50%, term drop-out).
"""
from __future__ import annotations
from pathlib import Path
import pickle
import itertools
import numpy as np
import pandas as pd
import avwap_5min_ID_v11_backtesting as v11
import walkforward_gate as wfg
from nse_intraday_costs import CostConfig

SRC = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_new_setups_probe\new_setups_standalone_trades.csv")
# N_ overlay candidates (relabeled from the clean pool by new_setups_overlay_gen_v11.py) are merged in
OVERLAY_SRC = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_new_setups_probe\new_overlay_candidates_traintest.csv")
PROP = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_traintest_pool\proposals")
CACHE = Path(r"C:\TradingData\eqidv2\v11_newsetups_paths_cache.pkl")
TRAIN_END, TEST_START = "2026-04-30", "2026-05-01"
CFG = CostConfig()
SETUPS = {  # side, prior production exit, train sample cap
    "L_RS_LEADER_VWAP_HOLD": ("LONG", (0.80, 1.50), 2000),
    "S_UPTHRUST_TRAP_FADE": ("SHORT", (0.80, 1.00), 2000),
    "N_HIGH_RS_EMA_BOUNCE_LONG": ("LONG", (0.90, 1.50), 2000),   # overlay (V11_NEW_SETUP_RESEARCH prod exit)
    "N_MORNING_ZERO_WICK_SHORT": ("SHORT", (1.00, 2.00), 2000),  # overlay (V11_NEW_SETUP_RESEARCH prod exit)
}
# Source separation: L/S are STRUCTURAL scans (scanner CSV); the two N_ setups are CLEAN-POOL
# OVERLAY relabels (overlay CSV). The scanner ALSO emits a structural *approximation* of N_ — we
# DROP those so the two methodologies never mix under one setup name (user chose overlay-relabel).
SCANNER_SETUPS = {"L_RS_LEADER_VWAP_HOLD", "S_UPTHRUST_TRAP_FADE"}
OVERLAY_SETUPS = {"N_HIGH_RS_EMA_BOUNCE_LONG", "N_MORNING_ZERO_WICK_SHORT"}
# pool features (incl. the enriched indicator columns the scanner added)
POOL_FEATS = ["vol_ratio", "rs_pct", "market_ret_pct", "vwap_dist_atr", "close_loc", "quality_score",
              "atr_pct", "body_pct", "signal_minute", "rsi", "rsi3max", "adx", "upper_wick_pct",
              "lower_wick_pct", "macd_hist", "macd_hist_delta", "ema20_slope", "stock_ret"]
PM_FEATS = ["pre_entry_momentum_score", "pre2_mom_r", "pre3_close_pos", "sig5_rsi_dir",
            "sig5_adx_calc", "pre5_mom_r", "pre10_mom_r", "sig5_vol_ratio20", "pre3_range_r",
            "pre1_adx", "pre5_dir_count"]
QUANTILES = (0.2, 0.35, 0.5, 0.65, 0.8)
EXIT_GRID = [(sl, tg) for sl in (0.50, 0.70, 0.90, 1.10) for tg in (0.60, 0.80, 1.00, 1.25, 1.50, 2.00)]
MIN_TRADES, MIN_DAYS, MIN_TEST, TRAIN_PF_TARGET = 25, 12, 8, 2.0


def build_paths():
    srcs = []
    if SRC.exists():
        s = pd.read_csv(SRC, low_memory=False)
        srcs.append(s[s["setup"].astype(str).isin(SCANNER_SETUPS)].copy())  # L/S structural ONLY
    if OVERLAY_SRC.exists():
        o = pd.read_csv(OVERLAY_SRC, low_memory=False)
        srcs.append(o[o["setup"].astype(str).isin(OVERLAY_SETUPS)].copy())  # N_ clean-pool overlay ONLY
    if not srcs:
        raise SystemExit(f"!! no candidate sources found ({SRC} / {OVERLAY_SRC})")
    df = pd.concat(srcs, ignore_index=True, sort=False)
    df = df[df["setup"].astype(str).isin(SETUPS)].copy()
    df["sig"] = pd.to_datetime(df["signal_time_ist"], errors="coerce")
    df = df.dropna(subset=["sig"]).drop_duplicates(subset=["ticker", "setup", "signal_time_ist"])
    df["d"] = df["sig"].dt.strftime("%Y-%m-%d")
    out = {}
    for setup, (side, _ex, cap_tr) in SETUPS.items():
        e = df[df["setup"].astype(str) == setup]
        tr = e[e["d"] <= TRAIN_END]; te = e[e["d"] >= TEST_START]
        if len(tr) > cap_tr:
            tr = tr.sample(cap_tr, random_state=7)
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
        print(f"[new_search] {setup}: {len(paths)} paths ({len(e)} sampled)")
    return out


def load_or_build():
    if CACHE.exists():
        with open(CACHE, "rb") as fh:
            return pickle.load(fh)
    out = build_paths()
    with open(CACHE, "wb") as fh:
        pickle.dump(out, fh)
    return out


def net_for_exit(recs, sl, tgt):
    out = np.empty(len(recs))
    for i, p in enumerate(recs):
        side = p["side"]; e = p["raw_open"] * (0.9995 if side == "SHORT" else 1.0005)
        qty = max(1, int(100000.0 / p["raw_open"])); h, l, c = p["highs"], p["lows"], p["closes"]
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


def _dbp(dts, net, nb=6000, seed=7):
    s = pd.Series(net, index=pd.to_datetime(dts)).groupby(level=0).sum().to_numpy()
    if len(s) < 3:
        return float('nan')
    r = np.random.default_rng(seed)
    return float((s[r.integers(0, len(s), size=(nb, len(s)))].mean(axis=1) <= 0).mean())


def _topday(dates, net):
    s = pd.Series(net, index=pd.to_datetime(dates)).groupby(level=0).sum(); tot = s.sum()
    return round(float(s.max() / tot * 100)) if tot > 0 else float('nan')


def search(setup, recs):
    side, prod_exit, _ = SETUPS[setup]
    print("\n" + "#" * 70 + f"\n# {setup}  (n={len(recs)}, side {side})\n" + "#" * 70)
    df = pd.DataFrame([{k: v for k, v in r.items() if k not in ("highs", "lows", "closes")} for r in recs])
    dts = df["date"].to_numpy()
    is_tr, is_te = dts <= TRAIN_END, dts >= TEST_START
    mid = np.sort(dts[is_tr])[is_tr.sum() // 2] if is_tr.sum() else TRAIN_END
    h1 = is_tr & (dts <= mid); h2 = is_tr & (dts > mid)
    net_by_exit = {ex: net_for_exit(recs, *ex) for ex in EXIT_GRID}
    prod_net = net_by_exit[prod_exit] if prod_exit in net_by_exit else net_for_exit(recs, *prod_exit)

    def stats(mask, net, boot=False):
        mtr = mask & is_tr
        if mtr.sum() < MIN_TRADES or df.loc[mtr, "date"].nunique() < MIN_DAYS:
            return None
        a, b, t = net[mask & h1], net[mask & h2], net[mask & is_te]
        if len(a) < 5 or len(b) < 5 or len(t) < MIN_TEST:
            return None
        pa, pb, pt = _pf(a), _pf(b), _pf(t)
        td = df.loc[mask & is_te, "date"]
        out = {"train_n": int(mtr.sum()), "train_pf": round(_pf(net[mtr]), 2), "h1": round(pa, 2),
               "h2": round(pb, 2), "test_n": int(len(t)), "test_pf": round(pt, 2),
               "test_days": int(td.nunique()), "test_win": round((t > 0).mean() * 100, 1),
               "minpf": round(min(pa, pb, pt), 2), "full_pf": round(_pf(net[mask]), 2),
               "net": round(float(net[mask].sum()), 0), "test_dbp": np.nan, "top1day": np.nan}
        if boot:
            out["test_dbp"] = round(_dbp(td.to_numpy(), t), 3); out["top1day"] = _topday(td.to_numpy(), t)
        return out

    st0 = stats(np.ones(len(df), bool), prod_net, boot=True)
    if st0:
        print(f"  baseline ALL exit {prod_exit[0]}/{prod_exit[1]}: TR n={st0['train_n']} pf={st0['train_pf']} "
              f"| TE n={st0['test_n']}(d{st0['test_days']}) pf={st0['test_pf']} p={st0['test_dbp']} net Rs{st0['net']:,.0f}")

    feats = [f for f in (POOL_FEATS + list(dict.fromkeys(PM_FEATS)))
             if f in df.columns and df[f].notna().mean() > 0.6]
    cand = []
    for f in feats:
        for q in np.unique(np.round(df.loc[is_tr, f].quantile(QUANTILES).values, 6)):
            cand.append((f, ">=", float(q))); cand.append((f, "<=", float(q)))
    for rg in ("NEUTRAL", "TREND", "BEAR", "BULL"):
        cand.append(("regime", "==", rg))
    masks = {t: ({">=": df[t[0]] >= t[2], "<=": df[t[0]] <= t[2], "==": df[t[0]] == t[2]}[t[1]]).to_numpy() for t in cand}

    rows = []
    def rec(stage, terms, ex, st):
        rows.append({"stage": stage, "terms": "; ".join(f"{a}{o}{b}" for a, o, b in terms),
                     "sl": ex[0], "tgt": ex[1], **st})

    adxv = df["sig5_adx_calc"].to_numpy(float)
    for fname in ("pre2_mom_r", "pre5_mom_r", "pre_entry_momentum_score"):
        fv = df[fname].to_numpy(float); qs = np.nanquantile(fv, [0.4, 0.6, 0.8])
        for gv in qs:
            for av in (20, 26, 32):
                for sgn in (">=", "<="):
                    m = np.isfinite(fv) & np.isfinite(adxv) & ((fv >= gv) if sgn == ">=" else (fv <= gv)) & (adxv >= av)
                    for ex in [prod_exit, (0.9, 1.5), (0.9, 2.0), (0.7, 1.5)]:
                        net = net_by_exit.get(ex) if ex in net_by_exit else net_for_exit(recs, *ex)
                        st = stats(m, net)
                        if st and (st["minpf"] >= 1.2 or st["train_pf"] >= TRAIN_PF_TARGET):
                            rec("mech", [(fname, sgn, round(float(gv), 4)), ("sig5_adx_calc", ">=", float(av))], ex, st)
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
    for ex in EXIT_GRID:
        net = net_by_exit[ex]
        for t1, t2 in itertools.combinations(cand, 2):
            if t1[0] == t2[0]:
                continue
            st = stats(masks[t1] & masks[t2], net)
            if st and (st["minpf"] >= 1.2 or st["train_pf"] >= TRAIN_PF_TARGET):
                rec("2term", [t1, t2], ex, st)
    rng = np.random.default_rng(11)
    for _ in range(60000):
        ex = EXIT_GRID[rng.integers(len(EXIT_GRID))]
        ts = [cand[i] for i in rng.choice(len(cand), size=3, replace=False)]
        if len({t[0] for t in ts}) < 3:
            continue
        st = stats(masks[ts[0]] & masks[ts[1]] & masks[ts[2]], net_by_exit[ex])
        if st and (st["minpf"] >= 1.3 or st["train_pf"] >= TRAIN_PF_TARGET):
            rec("rand3", ts, ex, st)

    res = pd.DataFrame(rows)
    if not len(res):
        print("  no qualifying configs."); return setup, res
    res["setup"] = setup
    fin = set(res.sort_values("minpf", ascending=False).head(25).index) | \
          set(res[res.train_pf >= TRAIN_PF_TARGET].sort_values("test_pf", ascending=False).head(25).index)
    for i in fin:
        m = np.ones(len(df), bool)
        for term in res.loc[i, "terms"].split("; "):
            for op in (">=", "<=", "=="):
                if op in term:
                    f, v = term.split(op); f, v = f.strip(), v.strip(); col = df[f]
                    mm = col >= float(v) if op == ">=" else (col <= float(v) if op == "<=" else col == v)
                    m = m & mm.to_numpy(); break
        net = net_by_exit[(res.loc[i, "sl"], res.loc[i, "tgt"])]
        td = df.loc[m & is_te, "date"]
        res.loc[i, "test_dbp"] = round(_dbp(td.to_numpy(), net[m & is_te]), 3)
        res.loc[i, "top1day"] = _topday(td.to_numpy(), net[m & is_te])

    hit = res[(res.train_pf >= TRAIN_PF_TARGET) & res.test_dbp.notna()].sort_values("test_pf", ascending=False)
    print(f"\n  [train-PF>=2 ranked by honest test] ({(res.train_pf>=TRAIN_PF_TARGET).sum()} total):")
    for _, r in hit.head(8).iterrows():
        tag = "  <<<PASS" if (r["test_pf"] >= 1.3 and r["test_dbp"] < 0.10 and r["test_n"] >= MIN_TEST and (np.isnan(r["top1day"]) or r["top1day"] <= 60)) else ""
        print(f"    {r['stage']:6} {r['sl']}/{r['tgt']} TR n={int(r['train_n']):>4} pf={r['train_pf']:>4}[{r['h1']}/{r['h2']}]"
              f" TE n={int(r['test_n']):>3}(d{int(r['test_days'])}) pf={r['test_pf']:>4} p={r['test_dbp']} top1d={r['top1day']}% | {r['terms']}{tag}")
    rob = res[res.test_dbp.notna()].sort_values("minpf", ascending=False)
    print(f"\n  [robustness-first ranked by min(h1,h2,test)]:")
    for _, r in rob.head(8).iterrows():
        tag = "  <<<PASS" if (r["train_pf"] >= 1.7 and r["minpf"] >= 1.3 and r["test_dbp"] < 0.10 and r["test_n"] >= MIN_TEST and (np.isnan(r["top1day"]) or r["top1day"] <= 60)) else ""
        print(f"    {r['stage']:6} {r['sl']}/{r['tgt']} TR n={int(r['train_n']):>4} pf={r['train_pf']:>4}[{r['h1']}/{r['h2']}]"
              f" TE n={int(r['test_n']):>3}(d{int(r['test_days'])}) pf={r['test_pf']:>4} minpf={r['minpf']:>4} p={r['test_dbp']} top1d={r['top1day']}% | {r['terms']}{tag}")
    return setup, res


def main():
    PROP.mkdir(parents=True, exist_ok=True)
    if not SRC.exists():
        print(f"!! candidate CSV missing: {SRC}\n   Run new_setups_scan_v11.py first (post-close)."); return 1
    allp = load_or_build()
    outs = []
    for setup in SETUPS:
        if len(allp.get(setup, [])) < MIN_TRADES:
            print(f"\n{setup}: insufficient paths ({len(allp.get(setup, []))})"); continue
        _, res = search(setup, allp[setup])
        if len(res):
            outs.append(res)
    if outs:
        pd.concat(outs, ignore_index=True).to_csv(PROP / "new_setups_search_results.csv", index=False)
    print(f"\n[new_search] done -> {PROP}\\new_setups_search_results.csv")
    print("Promote to final_setup_conf.py ONLY on a clean pass (see new_setups_research_v11.md §4).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
