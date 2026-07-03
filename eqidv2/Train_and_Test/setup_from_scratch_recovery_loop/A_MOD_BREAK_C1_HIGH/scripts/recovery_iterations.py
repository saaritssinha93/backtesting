r"""recovery_iterations.py — staged redesign iterations for A_MOD_BREAK_C1_HIGH.

Blocks (one logical group per iteration):
  A exits on R1 base (first-per-day + 20bh, next-open entry): SL x TGT grid, time caps,
    breakeven, trailing
  B confirmation entry (stop-buy above signal high within K min) x exit variants
  C retest-limit entry (depth x window) x exit variants
  D time-of-day windows on the best entry/exit family
  E masks (volume/structure/regime) on the best family
  F crowding/risk guards (top_n, max_trades_day, daily_loss)
  G combinations of FIT+VAL-stable pieces

Discipline: FIT -> VAL for every iteration; full-TRAIN confirm when FIT&VAL >= 1.10;
TEST evaluated ONCE per candidate only when full-TRAIN PF in [1.30, 1.80].
Every iteration appended to iterations.csv.
"""
from __future__ import annotations

import itertools
import json
import sys
import time
from dataclasses import asdict, replace
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
sys.path.insert(0, str(HERE.parent))
from path_engine import PathEngine, Variant  # noqa: E402

PF_LO, PF_HI, TEST_PF_MIN = 1.30, 1.80, 1.40
LOG = WORK / "iterations.csv"


def main() -> int:
    eng = PathEngine()
    rows: list[dict] = []
    it = 0
    t0 = time.time()

    def run(v: Variant, block: str, note: str):
        nonlocal it
        it += 1
        mf = eng.evaluate(v, "FIT")
        mv = eng.evaluate(v, "VAL")
        rec = {"iter": it, "block": block, "name": v.name, "note": note,
               "cfg": json.dumps({k: val for k, val in asdict(v).items() if k != "name"}, default=str),
               "fit_n": mf["n"], "fit_pf": mf.get("pf"), "fit_net": mf.get("net"),
               "val_n": mv["n"], "val_pf": mv.get("pf"), "val_net": mv.get("net"),
               "train_n": None, "train_pf": None, "train_net": None, "train_dayp": None,
               "test_n": None, "test_pf": None, "test_net": None, "verdict": ""}
        ok_fv = (mf["n"] >= 40 and mv["n"] >= 25
                 and np.isfinite(mf.get("pf", np.nan)) and np.isfinite(mv.get("pf", np.nan))
                 and mf["pf"] >= 1.10 and mv["pf"] >= 1.10)
        if ok_fv:
            mt = eng.evaluate(v, "TRAIN")
            rec.update({"train_n": mt["n"], "train_pf": mt.get("pf"), "train_net": mt.get("net"),
                        "train_dayp": mt.get("day_p"), "train_dom_t": mt.get("dom_trade"),
                        "train_dom_d": mt.get("dom_day"), "train_dom_s": mt.get("dom_sym"),
                        "train_avg_loss": mt.get("avg_loss")})
            if mt["n"] >= 60 and np.isfinite(mt.get("pf", np.nan)) and PF_LO <= mt["pf"] <= PF_HI:
                me = eng.evaluate(v, "TEST")
                rec.update({"test_n": me["n"], "test_pf": me.get("pf"), "test_net": me.get("net"),
                            "test_dayp": me.get("day_p"), "test_dom_t": me.get("dom_trade"),
                            "test_dom_d": me.get("dom_day"), "test_dom_s": me.get("dom_sym"),
                            "test_avg_loss": me.get("avg_loss")})
                passes = (me["n"] >= 15 and np.isfinite(me.get("pf", np.nan))
                          and me["pf"] > TEST_PF_MIN and me.get("net", 0) > 0)
                rec["verdict"] = "CANDIDATE" if passes else "BAND_BUT_TEST_FAIL"
            else:
                rec["verdict"] = "TRAIN_CONFIRMED" if mt["pf"] and mt["pf"] >= 1.0 else "TRAIN_WEAK"
        rows.append(rec)
        if it % 10 == 0:
            pd.DataFrame(rows).to_csv(LOG, index=False)
            print(f"[it {it}] {block} elapsed={time.time()-t0:.0f}s", flush=True)
        return rec

    base = Variant(name="R1", entry="next_open", first_per_day=True, require_20bh=True)

    # ---------------- Block A: exit geometry on R1 ----------------
    for sl, tgt in itertools.product([0.35, 0.45, 0.55, 0.70, 1.0, 1.5],
                                     [0.6, 0.8, 1.0, 1.25, 1.75, 2.5]):
        run(replace(base, name=f"A_sl{sl}_t{tgt}", sl_pct=sl, tgt_pct=tgt), "A", "exit grid")
    for tc in (60, 120, 180):
        run(replace(base, name=f"A_tc{tc}", sl_pct=0.45, tgt_pct=1.0, time_cap_min=tc), "A", "time cap")
    for be in (0.3, 0.5):
        run(replace(base, name=f"A_be{be}", sl_pct=0.45, tgt_pct=1.25, breakeven_at=be), "A", "breakeven")
    for tr in (0.6, 0.9, 1.25):
        run(replace(base, name=f"A_tr{tr}", sl_pct=0.55, tgt_pct=9.9, trail_pct=tr), "A", "trail-only")
        run(replace(base, name=f"A_tr{tr}_t2", sl_pct=0.55, tgt_pct=2.0, trail_pct=tr), "A", "trail+tgt")

    # pick best-A by FIT+VAL pf sum among n>=100
    df = pd.DataFrame(rows)
    dA = df[(df.block == "A") & (df.fit_n >= 100)].copy()
    dA["s"] = dA.fit_pf.fillna(0) + dA.val_pf.fillna(0)
    bestA = json.loads(dA.sort_values("s", ascending=False).iloc[0]["cfg"]) if len(dA) else {}
    vA = replace(base, name="bestA", **{k: bestA[k] for k in ("sl_pct", "tgt_pct", "time_cap_min", "breakeven_at", "trail_pct") if k in bestA})
    print(f"[driver] bestA = sl{vA.sl_pct}/t{vA.tgt_pct}/tc{vA.time_cap_min}/be{vA.breakeven_at}/tr{vA.trail_pct}")

    # ---------------- Block B: confirmation entry ----------------
    for ck in (5, 10, 15, 30):
        for sl, tgt in itertools.product([0.35, 0.45, 0.55, 0.7], [0.8, 1.0, 1.25, 1.75]):
            run(replace(base, name=f"B_k{ck}_sl{sl}_t{tgt}", entry="confirm", confirm_k=ck,
                        sl_pct=sl, tgt_pct=tgt), "B", "confirm entry")
    for ck in (10, 30):
        run(replace(base, name=f"B_k{ck}_trail", entry="confirm", confirm_k=ck,
                    sl_pct=0.55, tgt_pct=9.9, trail_pct=0.9), "B", "confirm+trail")
        run(replace(base, name=f"B_k{ck}_tc120", entry="confirm", confirm_k=ck,
                    sl_pct=0.45, tgt_pct=1.25, time_cap_min=120), "B", "confirm+timecap")

    # ---------------- Block C: retest-limit entry ----------------
    for dep in (0.15, 0.30, 0.50):
        for ck in (15, 30, 60):
            for sl, tgt in [(0.45, 1.0), (0.55, 1.25), (0.7, 1.75)]:
                run(replace(base, name=f"C_d{dep}_k{ck}_sl{sl}_t{tgt}", entry="retest",
                            retest_depth_atr=dep, confirm_k=ck, sl_pct=sl, tgt_pct=tgt),
                    "C", "retest entry")

    # ---------------- pick best family so far ----------------
    df = pd.DataFrame(rows)
    dd = df[df.fit_n >= 80].copy()
    dd["s"] = dd.fit_pf.fillna(0) + dd.val_pf.fillna(0)
    top = dd.sort_values("s", ascending=False).iloc[0]
    vTop = replace(base, name="bestFam", **{k: v for k, v in json.loads(top["cfg"]).items()
                                            if k in ("entry", "confirm_k", "retest_depth_atr", "retest_arm_min",
                                                     "sl_pct", "tgt_pct", "time_cap_min", "breakeven_at", "trail_pct")})
    print(f"[driver] best family: {top['name']} fit_pf={top['fit_pf']} val_pf={top['val_pf']}")

    # ---------------- Block D: time windows ----------------
    for mn, mx, tag in [(0, 665, "am"), (665, 810, "mid"), (780, 0, "late"),
                        (0, 810, "no_late"), (600, 0, "no_open")]:
        run(replace(vTop, name=f"D_{tag}", min_slot=mn, max_slot=mx), "D", "time window")

    # ---------------- Block E: masks ----------------
    E_MASKS = [
        ("vol22", [("vol_ratio", ">=", 2.2)]), ("vol30", [("vol_ratio", ">=", 3.0)]),
        ("bmarg", [("break_margin_atr", ">=", 0.10)]),
        ("gapdn", [("gap_pct", "<=", 0.0)]), ("gapup", [("gap_pct", ">=", 0.0)]),
        ("dret", [("day_ret_pct", ">=", 1.0)]), ("emastack", [("ema_stack", ">=", 2.0)]),
        ("vwaphold", [("vwap_hold_bars", ">=", 6.0)]),
        ("rsi", [("rsi_x", ">=", 60.0)]), ("adx", [("adx_x", ">=", 25.0)]),
        ("prevol", [("pre3_vol_ratio", ">=", 1.4)]), ("rcomp", [("range_compress3", ">=", 0.76)]),
        ("nearvwap", [("vwap_dist_atr", "<=", 2.0)]), ("regNB", [("regime", "!=", "BEAR")]),
        ("qs", [("quality_score", ">=", 120.0)]),
    ]
    for tag, mask in E_MASKS:
        run(replace(vTop, name=f"E_{tag}", mask=mask), "E", "single mask")

    # ---------------- Block F: crowding / risk ----------------
    for tn in (1, 2, 3):
        run(replace(vTop, name=f"F_top{tn}", top_n=tn), "F", "top_n vwap_dist")
        run(replace(vTop, name=f"F_top{tn}vol", top_n=tn, rank_col="vol_ratio"), "F", "top_n vol")
    for mtd in (10, 20, 40):
        run(replace(vTop, name=f"F_mtd{mtd}", max_trades_day=mtd), "F", "max trades/day")
    run(replace(vTop, name="F_dloss4k", daily_loss_rs=4000.0), "F", "daily loss stop")
    run(replace(vTop, name="F_open10", max_open=10), "F", "max open 10")

    # ---------------- Block G: combos of stable D/E/F pieces ----------------
    df = pd.DataFrame(rows)
    stable = []
    for _, r in df[df.block.isin(["D", "E", "F"])].iterrows():
        if (r.fit_n >= 60 and r.val_n >= 40 and pd.notna(r.fit_pf) and pd.notna(r.val_pf)
                and r.fit_pf >= float(top["fit_pf"]) - 0.02 and r.val_pf >= float(top["val_pf"]) - 0.02):
            stable.append(r)
    stable = sorted(stable, key=lambda r: -(r.fit_pf + r.val_pf))[:5]
    print(f"[driver] stable D/E/F pieces: {[r['name'] for r in stable]}")
    for a, b in itertools.combinations(stable, 2):
        ca, cb = json.loads(a["cfg"]), json.loads(b["cfg"])
        merged = dict(vTop.__dict__)
        for c in (ca, cb):
            for k in ("min_slot", "max_slot", "top_n", "rank_col", "max_trades_day", "daily_loss_rs", "max_open"):
                if c.get(k) not in (0, 0.0, "vwap_dist_atr", 20, None):
                    merged[k] = c[k]
            if c.get("mask"):
                merged["mask"] = (merged.get("mask") or []) + [tuple(t) for t in c["mask"]]
        merged["name"] = f"G_{a['name']}+{b['name']}"
        run(Variant(**merged), "G", "combo")

    out = pd.DataFrame(rows)
    out.to_csv(LOG, index=False)
    print(f"\n[driver] total iterations: {it}  elapsed={time.time()-t0:.0f}s")
    cands = out[out.verdict == "CANDIDATE"]
    band = out[out.verdict.isin(["CANDIDATE", "BAND_BUT_TEST_FAIL"])]
    print(f"[driver] TRAIN-band configs: {len(band)}  CANDIDATES passing TEST: {len(cands)}")
    cols = ["iter", "name", "fit_pf", "val_pf", "train_n", "train_pf", "test_n", "test_pf", "test_net", "verdict"]
    if len(band):
        print(band[cols].to_string(index=False))
    print("\n[driver] top 15 by FIT+VAL:")
    out["s"] = out.fit_pf.fillna(0) + out.val_pf.fillna(0)
    print(out.sort_values("s", ascending=False).head(15)[cols].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
