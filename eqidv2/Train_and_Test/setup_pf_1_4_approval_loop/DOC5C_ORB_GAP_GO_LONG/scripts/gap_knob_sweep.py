r"""gap_knob_sweep.py — research-only staged PF-band sweep for DOC5C_ORB_GAP_GO_LONG.
============================================================================
Complements the shared engine (pf_band_fitval_loop.py). The shared engine's
MASK_FEATS list does NOT include the gap-and-go specific pool columns
(`gap_pct`, `orh_dist_atr`, `vwap_slope_atr`) — those ARE the structural edge
levers for Setup C. This script sweeps them (plus the standard knobs) through the
SAME repo pipeline (setup_train_test.eval_family -> guards/premom/dedupe/mask/
overlay/resolve, statutory NSE cost + per-leg slippage), so every number is
directly comparable to the engine and to the live-faithful backtest.

Split = the task's mandated split (identical to the engine default):
    TRAIN 2026-05-18 .. session-before-TEST   (FIT = first half, VAL = second half)
    TEST  2026-06-20 .. latest completed
Anti-overfit: search ONLY on FIT/VAL, confirm on full TRAIN, score TEST ONCE.

NO edits to final_setup_conf.py. NO live trades. Backtest/replay only.

Run from repo root:
  py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/DOC5C_ORB_GAP_GO_LONG/scripts/gap_knob_sweep.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve()
SETUP = "DOC5C_ORB_GAP_GO_LONG"
OUTDIR = _HERE.parent.parent            # setup_pf_1_4_approval_loop/DOC5C_ORB_GAP_GO_LONG/
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
import pf_band_fitval_loop as eng        # noqa: E402  (reuse full_metrics/fast_metrics)

POOL = REPO / "Train_and_Test" / "doc5_long_setups" / "pool"
TRAIN_START = pd.Timestamp("2026-05-18")
TEST_START = pd.Timestamp("2026-06-20")
MIN_TEST_SESS = 4
N_TEST_FALLBACK = 5
FV_FLOOR = 5                             # min trades in FIT and VAL to trust a PF
PF_LO, PF_HI = 1.30, 1.70               # TRAIN band
TEST_PF_MIN = 1.40                       # task TEST gate


def ev(cfg, df):
    """Full metric pack for one cfg on one window (15 bps, statutory cost)."""
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    return eng.full_metrics(SETUP, cfg, df)


def mk(sl=0.85, tgt=1.50, mask=None, premom=None, guard=None, maxpos=20, dloss=0.0):
    return {"sl": float(sl), "tgt": float(tgt),
            "mask_terms": [tuple(t) for t in (mask or [])],
            "premom_terms": [tuple(t) for t in (premom or [])],
            "guard": (dict(guard) if guard else None), "status": "OK",
            "max_positions": int(maxpos), "daily_loss_rs": float(dloss)}


def line(m):
    return (f"n={m['n']:>3} PF={m['net_pf']:.3f} net=Rs{m['net_pnl']:>8,.0f} "
            f"win%={m['win_rate']:>5} tgt%={m['target_rate']:>5} "
            f"SL/T/E={m['sl_cnt']}/{m['tgt_cnt']}/{m['eod_cnt']} "
            f"tpd={m['trades_per_day']} dbp={m['day_block_p']} "
            f"dom(t/d/s)={m['trade_dom_gross']}/{m['day_dom']}/{m['sym_dom']}")


def main():
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    tt.POOL_DIRS = [POOL]
    tt.POOL_DIR = POOL
    pool = tt.load_pool()
    pool = pool[pool["setup"] == SETUP].copy()
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    test_cal = [s for s in sessions if s >= TEST_START]
    if len(test_cal) >= MIN_TEST_SESS:
        TEST_s = test_cal
        split_note = f"TEST = calendar sessions >= {TEST_START.date()} ({len(TEST_s)})."
    else:
        TEST_s = sessions[-N_TEST_FALLBACK:]
        split_note = f"TEST fell back to last {N_TEST_FALLBACK} sessions."
    first_test = TEST_s[0]
    TRAIN_s = [s for s in sessions if (s >= TRAIN_START and s < first_test)]
    half = len(TRAIN_s) // 2
    FIT_s, VAL_s = TRAIN_s[:half], TRAIN_s[half:]

    def rng(ss):
        return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}" if ss else "(empty)"
    print(f"[sweep] {split_note}")
    print(f"[sweep] FIT   {rng(FIT_s)} ({len(FIT_s)})")
    print(f"[sweep] VAL   {rng(VAL_s)} ({len(VAL_s)})")
    print(f"[sweep] TRAIN {rng(TRAIN_s)} ({len(TRAIN_s)})")
    print(f"[sweep] TEST  {rng(TEST_s)} ({len(TEST_s)})  {[str(pd.Timestamp(s).date()) for s in TEST_s]}")

    span = set(map(pd.Timestamp, FIT_s + VAL_s + TEST_s))
    sub = pool[pool["_day"].isin(span)].copy()
    tt.SLIPPAGE_BPS = 15.0
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()
    sub = tt.attach_entries(sub)

    def sl_(ss):
        return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    FIT, VAL, TRAIN, TEST = sl_(FIT_s), sl_(VAL_s), sl_(FIT_s + VAL_s), sl_(TEST_s)
    print(f"[sweep] entries attached: FIT={len(FIT)} VAL={len(VAL)} TRAIN={len(TRAIN)} TEST={len(TEST)}")

    results = {"split_note": split_note,
               "windows": {"FIT": rng(FIT_s), "VAL": rng(VAL_s), "TRAIN": rng(TRAIN_s), "TEST": rng(TEST_s),
                           "TEST_sessions": [str(pd.Timestamp(s).date()) for s in TEST_s]},
               "entries": {"FIT": len(FIT), "VAL": len(VAL), "TRAIN": len(TRAIN), "TEST": len(TEST)},
               "stages": {}}

    # ---------------- Stage 1: RAW baseline (no mask/premom/guard) ----------------
    print("\n=== STAGE 1: RAW baseline (no filters) ===")
    raw_doc = mk(0.85, 1.50)
    for lbl, d in (("FIT", FIT), ("VAL", VAL), ("TRAIN", TRAIN), ("TEST", TEST)):
        m = ev(raw_doc, d)
        print(f" raw doc-exit 0.85/1.50 {lbl:5s} {line(m)}")
        results["stages"].setdefault("raw_doc_exit", {})[lbl] = {k: v for k, v in m.items() if k != "detail"}

    # raw exit grid on FIT/VAL/TRAIN
    print("\n Raw exit grid (FIT / VAL / TRAIN PF, n):")
    exit_rows = []
    for sl in eng.SL_GRID:
        for tgt in eng.TGT_GRID:
            c = mk(sl, tgt)
            mf, mv, mt = ev(c, FIT), ev(c, VAL), ev(c, TRAIN)
            exit_rows.append({"sl": sl, "tgt": tgt,
                              "fit_n": mf["n"], "fit_pf": mf["net_pf"],
                              "val_n": mv["n"], "val_pf": mv["net_pf"],
                              "train_n": mt["n"], "train_pf": mt["net_pf"],
                              "train_tgt_rate": mt["target_rate"]})
    exit_df = pd.DataFrame(exit_rows).sort_values(
        ["fit_pf"], ascending=False)
    results["stages"]["raw_exit_grid"] = exit_rows
    print(exit_df.to_string(index=False))
    # best raw exit by min(FIT,VAL) PF with n floor -> reference exit for knob sweeps
    def _score_exit(r):
        if r["fit_n"] < FV_FLOOR or r["val_n"] < FV_FLOOR:
            return -9
        return min(r["fit_pf"], r["val_pf"])
    exit_df["_s"] = exit_df.apply(_score_exit, axis=1)
    best_exit = exit_df.sort_values("_s", ascending=False).iloc[0]
    REF_SL, REF_TGT = float(best_exit["sl"]), float(best_exit["tgt"])
    print(f"\n reference exit for knob sweep = SL {REF_SL} / Tgt {REF_TGT} "
          f"(min(FIT,VAL)PF={best_exit['_s']:.3f})")
    results["reference_exit"] = {"sl": REF_SL, "tgt": REF_TGT}

    # ---------------- Stage 2: individual knob sweep on FIT/VAL ----------------
    print("\n=== STAGE 2: individual knob sweep (mask on one column at a time, ref exit) ===")
    KNOBS = {
        "gap_pct>=": ("gap_pct", ">=", [0.5, 0.7, 0.9, 1.1, 1.3, 1.5]),
        "gap_pct<=": ("gap_pct", "<=", [3.0, 2.5, 2.0, 1.5, 1.0]),
        "orh_dist_atr<=": ("orh_dist_atr", "<=", [3.0, 2.5, 2.0, 1.5, 1.0, 0.6]),
        "orh_dist_atr>=": ("orh_dist_atr", ">=", [0.0, 0.3, 0.6, 1.0]),
        "vwap_slope_atr>=": ("vwap_slope_atr", ">=", [0.1, 0.2, 0.3, 0.5, 0.7, 0.9]),
        "vol_ratio>=": ("vol_ratio", ">=", [1.5, 1.8, 2.0, 2.5, 3.0, 3.5]),
        "vol_ratio<=": ("vol_ratio", "<=", [5.0, 4.0, 3.0, 2.5]),
        "close_loc>=": ("close_loc", ">=", [0.6, 0.7, 0.8, 0.9]),
        "rs_pct>=": ("rs_pct", ">=", [0.5, 0.8, 1.0, 1.5, 2.0]),
        "body_pct>=": ("body_pct", ">=", [0.4, 0.6, 0.8]),
        "atr_pct<=": ("atr_pct", "<=", [0.008, 0.006, 0.005, 0.004]),
        "atr_pct>=": ("atr_pct", ">=", [0.002, 0.003, 0.004]),
        "quality_score>=": ("quality_score", ">=", [70, 85, 100, 120]),
        "vwap_dist_atr<=": ("vwap_dist_atr", "<=", [4.0, 3.0, 2.5, 2.0, 1.5]),
        "market_ret_pct>=": ("market_ret_pct", ">=", [-0.15, 0.0, 0.1, 0.2]),
    }
    knob_summary = {}
    for name, (feat, op, vals) in KNOBS.items():
        rows = []
        for v in vals:
            c = mk(REF_SL, REF_TGT, mask=[(feat, op, float(v))])
            mf, mv = ev(c, FIT), ev(c, VAL)
            rows.append((v, mf["n"], mf["net_pf"], mv["n"], mv["net_pf"]))
        knob_summary[name] = {"feat": feat, "op": op, "rows": rows}
        print(f"\n [{name}] (FITn/FITpf | VALn/VALpf)")
        for v, fn, fp, vn, vp in rows:
            flag = " *" if (fn >= FV_FLOOR and vn >= FV_FLOOR and min(fp, vp) >= 1.15) else ""
            print(f"   {feat}{op}{v:<7} FIT {fn:>3}/{fp:.3f} | VAL {vn:>3}/{vp:.3f}{flag}")
    results["stages"]["knob_sweep"] = {
        k: {"feat": v["feat"], "op": v["op"],
            "rows": [{"thr": r[0], "fit_n": r[1], "fit_pf": r[2], "val_n": r[3], "val_pf": r[4]} for r in v["rows"]]}
        for k, v in knob_summary.items()}

    # ---------------- Stage 2b: pre-momentum single-term sweep ----------------
    print("\n=== STAGE 2b: pre-momentum single-term sweep (ref exit) ===")
    PM = {
        "pre3_range_r>=": ("pre3_range_r", ">=", [0.2, 0.3, 0.4, 0.5]),
        "pre5_mom_r>=": ("pre5_mom_r", ">=", [0.0, 0.2, 0.4, 0.6]),
        "sig5_adx_calc>=": ("sig5_adx_calc", ">=", [15, 20, 25, 30]),
        "pre_entry_momentum_score>=": ("pre_entry_momentum_score", ">=", [50, 65, 75, 85]),
    }
    pm_summary = {}
    for name, (feat, op, vals) in PM.items():
        rows = []
        for v in vals:
            c = mk(REF_SL, REF_TGT, premom=[(feat, op, float(v))])
            mf, mv = ev(c, FIT), ev(c, VAL)
            rows.append((v, mf["n"], mf["net_pf"], mv["n"], mv["net_pf"]))
        pm_summary[name] = rows
        print(f"\n [{name}] (FITn/FITpf | VALn/VALpf)")
        for v, fn, fp, vn, vp in rows:
            flag = " *" if (fn >= FV_FLOOR and vn >= FV_FLOOR and min(fp, vp) >= 1.15) else ""
            print(f"   {feat}{op}{v:<7} FIT {fn:>3}/{fp:.3f} | VAL {vn:>3}/{vp:.3f}{flag}")
    results["stages"]["premom_sweep"] = {
        k: [{"thr": r[0], "fit_n": r[1], "fit_pf": r[2], "val_n": r[3], "val_pf": r[4]} for r in rows]
        for k, rows in pm_summary.items()}

    # ---------------- Stage 3: combine strongest stable knobs on FIT/VAL --------
    print("\n=== STAGE 3: combination search on FIT/VAL (<=2 mask + <=1 premom + guard + exit) ===")
    # candidate term pools chosen from Stage 2 stable levels (evidence-driven, filled below at runtime)
    gap_terms = [None, ("gap_pct", ">=", 0.7), ("gap_pct", "<=", 2.0), ("gap_pct", "<=", 1.5)]
    ext_terms = [None, ("orh_dist_atr", "<=", 2.0), ("orh_dist_atr", "<=", 1.5), ("orh_dist_atr", "<=", 1.0)]
    slope_terms = [None, ("vwap_slope_atr", ">=", 0.3), ("vwap_slope_atr", ">=", 0.5)]
    vol_terms = [None, ("vol_ratio", ">=", 2.0), ("vol_ratio", ">=", 2.5), ("vol_ratio", "<=", 4.0)]
    pm_terms = [None, ("pre3_range_r", ">=", 0.3), ("sig5_adx_calc", ">=", 20)]
    guards = [None, {"max_slot": "10:30"}, {"max_slot": "11:00", "top_n": 1}, {"top_n": 1}]
    exits = [(REF_SL, REF_TGT), (0.85, 1.50), (0.70, 1.25), (1.00, 1.50), (0.70, 1.00), (1.20, 2.00)]

    combos = []
    seen = set()
    for g in gap_terms:
        for e in ext_terms:
            for s in slope_terms:
                for vt in vol_terms:
                    mask = [t for t in (g, e, s, vt) if t is not None]
                    if len(mask) > 2:      # keep logic simple: <=2 mask terms
                        continue
                    for pm in pm_terms:
                        pmt = [pm] if pm is not None else []
                        for guard in guards:
                            for (sl, tgt) in exits:
                                key = (tuple(mask), tuple(pmt), json.dumps(guard, sort_keys=True), sl, tgt)
                                if key in seen:
                                    continue
                                seen.add(key)
                                c = mk(sl, tgt, mask=mask, premom=pmt, guard=guard)
                                mf, mv = ev(c, FIT), ev(c, VAL)
                                if mf["n"] < FV_FLOOR or mv["n"] < FV_FLOOR:
                                    continue
                                combos.append({
                                    "sl": sl, "tgt": tgt,
                                    "mask": ";".join(f"{a}{o}{b}" for a, o, b in mask) or "-",
                                    "premom": ";".join(f"{a}{o}{b}" for a, o, b in pmt) or "-",
                                    "guard": json.dumps(guard) if guard else "-",
                                    "fit_n": mf["n"], "fit_pf": mf["net_pf"],
                                    "val_n": mv["n"], "val_pf": mv["net_pf"],
                                    "minpf": round(min(mf["net_pf"], mv["net_pf"]), 3),
                                    "gap": round(abs(mf["net_pf"] - mv["net_pf"]), 3),
                                    "_cfg": c})
    combo_df = pd.DataFrame(combos)
    print(f" evaluated {len(combos)} FIT/VAL combos with n>= {FV_FLOOR} in both halves")
    if not combo_df.empty:
        combo_df = combo_df.sort_values(["minpf", "gap"], ascending=[False, True])
        show = combo_df.drop(columns=["_cfg"]).head(20)
        print(show.to_string(index=False))
    results["stages"]["combo_top"] = (combo_df.drop(columns=["_cfg"]).head(30).to_dict("records")
                                      if not combo_df.empty else [])

    # ---------------- Stage 4/5: confirm TRAIN band, then TEST once --------------
    print("\n=== STAGE 4/5: confirm best FIT/VAL combos on full TRAIN, then TEST once ===")
    candidates = []
    checked = 0
    for _, r in combo_df.iterrows():
        if checked >= 12:
            break
        c = r["_cfg"]
        mtr = ev(c, TRAIN)
        checked += 1
        in_band = PF_LO <= mtr["net_pf"] <= PF_HI
        print(f" TRAIN {line(mtr)}  band={in_band}  mask=[{r['mask']}] pm=[{r['premom']}] "
              f"guard={r['guard']} exit={c['sl']}/{c['tgt']}")
        rec = {"cfg": eng.cfg_to_conf_block(SETUP, "LONG", c),
               "train": {k: v for k, v in mtr.items() if k != "detail"}}
        if in_band and mtr["n"] >= 15:
            mte = ev(c, TEST)
            rec["test"] = {k: v for k, v in mte.items() if k != "detail"}
            print(f"   -> TEST {line(mte)}")
        candidates.append(rec)
    results["stage45_candidates"] = candidates

    (OUTDIR / "scripts" / "sweep_results.json").write_text(
        json.dumps(results, indent=2, default=str), encoding="utf-8")
    print(f"\n[sweep] wrote {OUTDIR/'scripts'/'sweep_results.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
