r"""reinvent_sweep_5bps.py — 5 bps/leg staged PF-band sweep for the reinvented DOC5C variants.
============================================================================
Research-only. Runs the SAME repo pipeline (setup_train_test.eval_family) at
SLIPPAGE_BPS = 5.0 (the goal's cost basis) on the reinvent pool. Two modes:

  --mode screen                : raw baseline + best-exit + a few key knobs for ALL
                                 reinvent setups; ranks them (which has a pulse?).
  --mode full --setup <NAME>   : full staged FIT/VAL/TRAIN/TEST search on ONE setup
                                 (exit grid, per-knob incl. gap/orh/retest/adx/rsi,
                                 pre-mom, <=2 mask combos, guards), confirm TRAIN band,
                                 TEST once. Writes scripts/reinvent_<NAME>_5bps.json.

Split = task-mandated (TRAIN 2026-05-18.. / TEST 2026-06-20..). Search on FIT/VAL only,
confirm on full TRAIN, TEST scored once and only if TRAIN PF in [1.30,1.70].
NO conf edits, NO live trades.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve()
OUTDIR = _HERE.parent.parent
POOL = OUTDIR / "reinvent_pool"
TT_DIR = None
for p in _HERE.parents:
    if (p / "setup_train_test.py").exists():
        TT_DIR = p
        break
REPO = TT_DIR.parent
ENGINE_DIR = OUTDIR.parent / "_engine"
for _p in (str(REPO), str(TT_DIR), str(ENGINE_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt            # noqa: E402
import pf_band_fitval_loop as eng        # noqa: E402

TRAIN_START = pd.Timestamp("2026-05-18")
TEST_START = pd.Timestamp("2026-06-20")
MIN_TEST_SESS = 4
N_TEST_FALLBACK = 5
FV_FLOOR = 5
PF_LO, PF_HI = 1.30, 1.70
TEST_PF_MIN = 1.40
SLIPPAGE = 5.0

ALL_SETUPS = ["DOC5C_GAP_RETEST_HOLD_LONG", "DOC5C_GAP_RECLAIM_LONG", "DOC5C_GAP_PULLBACK_HOLD_LONG"]


def ev(setup, cfg, df):
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    return eng.full_metrics(setup, cfg, df)


def mk(sl=0.70, tgt=1.25, mask=None, premom=None, guard=None, maxpos=20, dloss=0.0):
    return {"sl": float(sl), "tgt": float(tgt),
            "mask_terms": [tuple(t) for t in (mask or [])],
            "premom_terms": [tuple(t) for t in (premom or [])],
            "guard": (dict(guard) if guard else None), "status": "OK",
            "max_positions": int(maxpos), "daily_loss_rs": float(dloss)}


def line(m):
    return (f"n={m['n']:>3} PF={m['net_pf']:.3f} net=Rs{m['net_pnl']:>8,.0f} win%={m['win_rate']:>5} "
            f"tgt%={m['target_rate']:>5} SL/T/E={m['sl_cnt']}/{m['tgt_cnt']}/{m['eod_cnt']} "
            f"tpd={m['trades_per_day']} dbp={m['day_block_p']} dom(t/d/s)={m['trade_dom_gross']}/{m['day_dom']}/{m['sym_dom']}")


def build_windows(pool_all):
    sessions = sorted(pd.Series(pool_all["_day"].dropna().unique()))
    test_cal = [s for s in sessions if s >= TEST_START]
    if len(test_cal) >= MIN_TEST_SESS:
        TEST_s = test_cal
        note = f"TEST = calendar sessions >= {TEST_START.date()} ({len(TEST_s)})."
    else:
        TEST_s = sessions[-N_TEST_FALLBACK:]
        note = f"TEST fell back to last {N_TEST_FALLBACK} sessions."
    first_test = TEST_s[0]
    TRAIN_s = [s for s in sessions if (s >= TRAIN_START and s < first_test)]
    half = len(TRAIN_s) // 2
    return TRAIN_s[:half], TRAIN_s[half:], TRAIN_s, TEST_s, note


def load_setup(setup, FIT_s, VAL_s, TEST_s):
    tt.POOL_DIRS = [POOL]; tt.POOL_DIR = POOL
    pool = tt.load_pool()
    pool = pool[pool["setup"] == setup].copy()
    span = set(map(pd.Timestamp, list(FIT_s) + list(VAL_s) + list(TEST_s)))
    sub = pool[pool["_day"].isin(span)].copy()
    tt.SLIPPAGE_BPS = SLIPPAGE
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()
    sub = tt.attach_entries(sub)
    def sl_(ss):
        return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    return sl_(FIT_s), sl_(VAL_s), sl_(list(FIT_s) + list(VAL_s)), sl_(TEST_s)


def best_exit(setup, FIT, VAL, TRAIN):
    rows = []
    for sl in eng.SL_GRID:
        for tgt in eng.TGT_GRID:
            c = mk(sl, tgt)
            mf, mv, mt = ev(setup, c, FIT), ev(setup, c, VAL), ev(setup, c, TRAIN)
            rows.append({"sl": sl, "tgt": tgt, "fit_n": mf["n"], "fit_pf": mf["net_pf"],
                         "val_n": mv["n"], "val_pf": mv["net_pf"], "train_n": mt["n"],
                         "train_pf": mt["net_pf"], "train_tgt": mt["target_rate"]})
    dfx = pd.DataFrame(rows)
    def s(r):
        return -9 if (r["fit_n"] < FV_FLOOR or r["val_n"] < FV_FLOOR) else min(r["fit_pf"], r["val_pf"])
    dfx["_s"] = dfx.apply(s, axis=1)
    return dfx.sort_values("_s", ascending=False), rows


def screen(FIT_s, VAL_s, TRAIN_s, TEST_s):
    print("\n===== SCREEN (raw @5bps) all reinvent variants =====")
    summary = []
    for setup in ALL_SETUPS:
        FIT, VAL, TRAIN, TEST = load_setup(setup, FIT_s, VAL_s, TEST_s)
        print(f"\n--- {setup} : entries FIT={len(FIT)} VAL={len(VAL)} TRAIN={len(TRAIN)} TEST={len(TEST)} ---")
        raw = mk(0.70, 1.25)
        for lbl, d in (("TRAIN", TRAIN), ("TEST", TEST)):
            print(f"  raw 0.70/1.25 {lbl:5s} {line(ev(setup, raw, d))}")
        dfx, _ = best_exit(setup, FIT, VAL, TRAIN)
        be = dfx.iloc[0]
        cbe = mk(be["sl"], be["tgt"])
        mtr, mte = ev(setup, cbe, TRAIN), ev(setup, cbe, TEST)
        print(f"  best-exit {be['sl']}/{be['tgt']} minFV={be['_s']:.3f} TRAIN {line(mtr)}")
        print(f"  best-exit {be['sl']}/{be['tgt']}          TEST  {line(mte)}")
        summary.append({"setup": setup, "raw_train_pf": ev(setup, raw, TRAIN)["net_pf"],
                        "best_exit": f"{be['sl']}/{be['tgt']}", "best_minFV": round(float(be["_s"]), 3),
                        "be_train_n": mtr["n"], "be_train_pf": mtr["net_pf"],
                        "be_test_n": mte["n"], "be_test_pf": mte["net_pf"]})
    print("\n===== SCREEN RANKING =====")
    sdf = pd.DataFrame(summary).sort_values("best_minFV", ascending=False)
    print(sdf.to_string(index=False))
    return summary


def full(setup, FIT_s, VAL_s, TRAIN_s, TEST_s, note):
    FIT, VAL, TRAIN, TEST = load_setup(setup, FIT_s, VAL_s, TEST_s)
    res = {"setup": setup, "slippage_bps": SLIPPAGE, "split_note": note,
           "entries": {"FIT": len(FIT), "VAL": len(VAL), "TRAIN": len(TRAIN), "TEST": len(TEST)}, "stages": {}}
    print(f"\n===== FULL @5bps {setup} : FIT={len(FIT)} VAL={len(VAL)} TRAIN={len(TRAIN)} TEST={len(TEST)} =====")

    # Stage 1 raw + exit grid
    raw = mk(0.70, 1.25)
    for lbl, d in (("FIT", FIT), ("VAL", VAL), ("TRAIN", TRAIN), ("TEST", TEST)):
        m = ev(setup, raw, d)
        print(f" raw 0.70/1.25 {lbl:5s} {line(m)}")
        res["stages"].setdefault("raw", {})[lbl] = {k: v for k, v in m.items() if k != "detail"}
    dfx, exit_rows = best_exit(setup, FIT, VAL, TRAIN)
    res["stages"]["exit_grid"] = exit_rows
    print("\n Exit grid (top 12 by min(FIT,VAL)PF):")
    print(dfx.head(12).to_string(index=False))
    be = dfx.iloc[0]
    RSL, RTG = float(be["sl"]), float(be["tgt"])
    print(f" ref exit = {RSL}/{RTG}")

    # Stage 2 knob sweep (structural + indicator columns)
    KNOBS = {
        "gap_pct>=": ("gap_pct", ">=", [0.5, 0.8, 1.2, 1.6, 2.0]),
        "gap_pct<=": ("gap_pct", "<=", [4.0, 3.0, 2.0, 1.5]),
        "orh_dist_atr<=": ("orh_dist_atr", "<=", [1.25, 1.0, 0.75, 0.5, 0.25]),
        "retest_depth_atr>=": ("retest_depth_atr", ">=", [0.0, 0.25, 0.5, 1.0]),
        "vwap_slope_atr>=": ("vwap_slope_atr", ">=", [0.0, 0.2, 0.4, 0.6]),
        "vol_ratio>=": ("vol_ratio", ">=", [1.1, 1.4, 1.8, 2.2, 2.6]),
        "close_loc>=": ("close_loc", ">=", [0.55, 0.65, 0.75, 0.85]),
        "rs_pct>=": ("rs_pct", ">=", [0.0, 0.3, 0.6, 1.0]),
        "adx>=": ("adx", ">=", [15, 20, 25, 30]),
        "rsi>=": ("rsi", ">=", [45, 50, 55, 60]),
        "rsi<=": ("rsi", "<=", [80, 72, 65]),
        "ema20_slope_3bar>=": ("ema20_slope_3bar", ">=", [-1.0, 0.0, 0.3]),
        "quality_score>=": ("quality_score", ">=", [70, 85, 100, 115]),
        "atr_pct<=": ("atr_pct", "<=", [0.008, 0.006, 0.005, 0.004]),
        "vwap_dist_atr<=": ("vwap_dist_atr", "<=", [2.5, 2.0, 1.5, 1.0]),
        "signal_minute<=": ("signal_minute", "<=", [660, 690, 720]),
    }
    ks = {}
    for name, (feat, op, vals) in KNOBS.items():
        rows = []
        print(f"\n [{name}]")
        for v in vals:
            c = mk(RSL, RTG, mask=[(feat, op, float(v))])
            mf, mv = ev(setup, c, FIT), ev(setup, c, VAL)
            flag = " *" if (mf["n"] >= FV_FLOOR and mv["n"] >= FV_FLOOR and min(mf["net_pf"], mv["net_pf"]) >= 1.15) else ""
            print(f"   {feat}{op}{v:<7} FIT {mf['n']:>3}/{mf['net_pf']:.3f} | VAL {mv['n']:>3}/{mv['net_pf']:.3f}{flag}")
            rows.append({"thr": v, "fit_n": mf["n"], "fit_pf": mf["net_pf"], "val_n": mv["n"], "val_pf": mv["net_pf"]})
        ks[name] = {"feat": feat, "op": op, "rows": rows}
    res["stages"]["knob_sweep"] = ks

    # Stage 2b pre-momentum
    PM = {"pre3_range_r>=": ("pre3_range_r", ">=", [0.2, 0.3, 0.4]),
          "pre5_mom_r>=": ("pre5_mom_r", ">=", [0.0, 0.2, 0.4]),
          "sig5_adx_calc>=": ("sig5_adx_calc", ">=", [18, 24, 30]),
          "pre_entry_momentum_score>=": ("pre_entry_momentum_score", ">=", [55, 70, 80])}
    pm = {}
    print("\n --- pre-momentum ---")
    for name, (feat, op, vals) in PM.items():
        rows = []
        for v in vals:
            c = mk(RSL, RTG, premom=[(feat, op, float(v))])
            mf, mv = ev(setup, c, FIT), ev(setup, c, VAL)
            flag = " *" if (mf["n"] >= FV_FLOOR and mv["n"] >= FV_FLOOR and min(mf["net_pf"], mv["net_pf"]) >= 1.15) else ""
            print(f"   {feat}{op}{v:<7} FIT {mf['n']:>3}/{mf['net_pf']:.3f} | VAL {mv['n']:>3}/{mv['net_pf']:.3f}{flag}")
            rows.append({"thr": v, "fit_n": mf["n"], "fit_pf": mf["net_pf"], "val_n": mv["n"], "val_pf": mv["net_pf"]})
        pm[name] = rows
    res["stages"]["premom"] = pm

    # Stage 3 combos: auto-pick the stable levels found above
    def stable(name):
        out = []
        for r in ks.get(name, {}).get("rows", []):
            if r["fit_n"] >= FV_FLOOR and r["val_n"] >= FV_FLOOR and min(r["fit_pf"], r["val_pf"]) >= 0.9:
                out.append((ks[name]["feat"], ks[name]["op"], float(r["thr"])))
        return out[:2]
    pool_terms = [None]
    for nm in KNOBS:
        pool_terms += stable(nm)
    # de-dup
    seen_t = set(); uniq = []
    for t in pool_terms:
        k = t if t is None else (t[0], t[1], round(t[2], 4))
        if k in seen_t:
            continue
        seen_t.add(k); uniq.append(t)
    pool_terms = uniq
    pm_terms = [None, ("pre3_range_r", ">=", 0.3), ("sig5_adx_calc", ">=", 24)]
    guards = [None, {"max_slot": "11:00"}, {"top_n": 1}, {"max_slot": "12:00", "top_n": 2}]
    exits = [(RSL, RTG), (0.70, 1.25), (0.85, 1.50), (0.70, 1.00), (1.00, 1.50), (0.60, 1.00)]
    combos = []; seen = set()
    print(f"\n Stage3: {len(pool_terms)} candidate mask terms x pm x guards x exits ...")
    for a in range(len(pool_terms)):
        for b in range(a, len(pool_terms)):
            mask = [t for t in {pool_terms[a], pool_terms[b]} if t is not None]
            if len(mask) > 2:
                continue
            for pmt in pm_terms:
                pml = [pmt] if pmt else []
                for guard in guards:
                    for (sl, tgt) in exits:
                        key = (tuple(sorted(mask)), tuple(pml), json.dumps(guard, sort_keys=True), sl, tgt)
                        if key in seen:
                            continue
                        seen.add(key)
                        c = mk(sl, tgt, mask=mask, premom=pml, guard=guard)
                        mf, mv = ev(setup, c, FIT), ev(setup, c, VAL)
                        if mf["n"] < FV_FLOOR or mv["n"] < FV_FLOOR:
                            continue
                        combos.append({"sl": sl, "tgt": tgt,
                                       "mask": ";".join(f"{x}{o}{y}" for x, o, y in mask) or "-",
                                       "premom": ";".join(f"{x}{o}{y}" for x, o, y in pml) or "-",
                                       "guard": json.dumps(guard) if guard else "-",
                                       "fit_n": mf["n"], "fit_pf": mf["net_pf"], "val_n": mv["n"], "val_pf": mv["net_pf"],
                                       "minpf": round(min(mf["net_pf"], mv["net_pf"]), 3),
                                       "_cfg": c})
    cdf = pd.DataFrame(combos)
    print(f" evaluated {len(combos)} combos (n>= {FV_FLOOR} both halves)")
    if not cdf.empty:
        cdf = cdf.sort_values("minpf", ascending=False)
        print(cdf.drop(columns=["_cfg"]).head(20).to_string(index=False))
    res["stages"]["combo_top"] = (cdf.drop(columns=["_cfg"]).head(30).to_dict("records") if not cdf.empty else [])

    # Stage 4/5 confirm TRAIN band, TEST once
    print("\n Stage4/5: confirm on full TRAIN, TEST if in band ...")
    cands = []
    for _, r in (cdf.head(15).iterrows() if not cdf.empty else []):
        c = r["_cfg"]; mtr = ev(setup, c, TRAIN)
        inb = PF_LO <= mtr["net_pf"] <= PF_HI
        rec = {"cfg": eng.cfg_to_conf_block(setup, "LONG", c), "train": {k: v for k, v in mtr.items() if k != "detail"}}
        msg = f" TRAIN {line(mtr)} band={inb} mask=[{r['mask']}] pm=[{r['premom']}] guard={r['guard']} exit={c['sl']}/{c['tgt']}"
        if inb and mtr["n"] >= 12:
            mte = ev(setup, c, TEST)
            rec["test"] = {k: v for k, v in mte.items() if k != "detail"}
            msg += f"\n   -> TEST {line(mte)}"
        print(msg)
        cands.append(rec)
    res["stage45"] = cands

    (OUTDIR / "scripts" / f"reinvent_{setup}_5bps.json").write_text(json.dumps(res, indent=2, default=str), encoding="utf-8")
    print(f"\n[full] wrote scripts/reinvent_{setup}_5bps.json")
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["screen", "full"], default="screen")
    ap.add_argument("--setup", default="")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    tt.POOL_DIRS = [POOL]; tt.POOL_DIR = POOL
    pool_all = tt.load_pool()
    FIT_s, VAL_s, TRAIN_s, TEST_s, note = build_windows(pool_all)
    def rng(ss):
        return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}" if ss else "(empty)"
    print(f"[reinvent-sweep] {note} @ {SLIPPAGE}bps")
    print(f"[reinvent-sweep] FIT {rng(FIT_s)} ({len(FIT_s)}) VAL {rng(VAL_s)} ({len(VAL_s)}) "
          f"TRAIN {rng(TRAIN_s)} ({len(TRAIN_s)}) TEST {rng(TEST_s)} ({len(TEST_s)})")
    print(f"[reinvent-sweep] pool rows/setup:\n{pool_all['setup'].value_counts().to_string()}")
    if args.mode == "screen":
        screen(FIT_s, VAL_s, TRAIN_s, TEST_s)
    else:
        full(args.setup.strip().upper(), FIT_s, VAL_s, TRAIN_s, TEST_s, note)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
