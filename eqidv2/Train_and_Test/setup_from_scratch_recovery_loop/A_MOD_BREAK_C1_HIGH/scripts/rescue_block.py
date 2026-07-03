r"""rescue_block.py — Stage-9 rescue iterations (Block H).

Re-runs the day-stop pieces under leak-free realized-only accounting, then pushes:
loss-count day stops, 3/4-way stacks of stable pieces, exit re-tunes around the best stack,
confirm-entry variants of the stack, and morning/midday splits. Appends to iterations.csv.
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
    prev = pd.read_csv(LOG) if LOG.exists() else pd.DataFrame()
    rows: list[dict] = []
    it = int(prev["iter"].max()) if len(prev) else 0
    t0 = time.time()

    def run(v: Variant, note: str):
        nonlocal it
        it += 1
        mf = eng.evaluate(v, "FIT")
        mv = eng.evaluate(v, "VAL")
        rec = {"iter": it, "block": "H", "name": v.name, "note": note,
               "cfg": json.dumps({k: val for k, val in asdict(v).items() if k != "name"}, default=str),
               "fit_n": mf["n"], "fit_pf": mf.get("pf"), "fit_net": mf.get("net"),
               "val_n": mv["n"], "val_pf": mv.get("pf"), "val_net": mv.get("net"),
               "train_n": None, "train_pf": None, "train_net": None, "train_dayp": None,
               "test_n": None, "test_pf": None, "test_net": None, "verdict": ""}
        if (mf["n"] >= 40 and mv["n"] >= 25 and np.isfinite(mf.get("pf", np.nan))
                and np.isfinite(mv.get("pf", np.nan)) and mf["pf"] >= 1.10 and mv["pf"] >= 1.10):
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
                rec["verdict"] = "TRAIN_CONFIRMED" if (mt.get("pf") or 0) >= 1.0 else "TRAIN_WEAK"
        rows.append(rec)
        if it % 10 == 0:
            pd.DataFrame(list(prev.to_dict("records")) + rows).to_csv(LOG, index=False)
            print(f"[it {it}] H elapsed={time.time()-t0:.0f}s", flush=True)
        return rec

    base = Variant(name="H", entry="next_open", first_per_day=True, require_20bh=True,
                   sl_pct=1.5, tgt_pct=1.75)
    rcomp = [("range_compress3", ">=", 0.76)]

    # H1: honest re-run of day-stop pieces (realized-only accounting)
    for dl in (2000, 4000, 6000):
        run(replace(base, name=f"H_dloss{dl}", daily_loss_rs=float(dl)), "realized-only rupee stop")
    for ml in (1, 2, 3):
        run(replace(base, name=f"H_mloss{ml}", max_losses_day=ml), "loss-count day stop")

    # H2: stacks
    run(replace(base, name="H_dl4k_rc", daily_loss_rs=4000.0, mask=rcomp), "stack2 honest")
    run(replace(base, name="H_ml2_rc", max_losses_day=2, mask=rcomp), "stack2 losscount")
    for ml in (1, 2):
        run(replace(base, name=f"H_ml{ml}_rc_am", max_losses_day=ml, mask=rcomp, max_slot=665), "stack3 am")
        run(replace(base, name=f"H_ml{ml}_rc_o10", max_losses_day=ml, mask=rcomp, max_open=10), "stack3 open10")
        run(replace(base, name=f"H_ml{ml}_rc_mtd5", max_losses_day=ml, mask=rcomp, max_trades_day=5), "stack3 mtd5")
        run(replace(base, name=f"H_ml{ml}_rc_mtd3", max_losses_day=ml, mask=rcomp, max_trades_day=3), "stack3 mtd3")
    run(replace(base, name="H_dl4k_rc_am", daily_loss_rs=4000.0, mask=rcomp, max_slot=665), "stack3")
    run(replace(base, name="H_dl4k_rc_o10", daily_loss_rs=4000.0, mask=rcomp, max_open=10), "stack3")
    run(replace(base, name="H_dl4k_rc_mtd5", daily_loss_rs=4000.0, mask=rcomp, max_trades_day=5), "stack3")
    run(replace(base, name="H_dl4k_rc_am_o10", daily_loss_rs=4000.0, mask=rcomp, max_slot=665, max_open=10), "stack4")

    # H3: exit re-tunes on the two best stacks
    for sl, tgt in itertools.product([1.0, 1.25, 1.5, 1.75], [1.25, 1.75, 2.25, 3.0]):
        run(replace(base, name=f"H_dl4k_rc_sl{sl}_t{tgt}", daily_loss_rs=4000.0, mask=rcomp,
                    sl_pct=sl, tgt_pct=tgt), "exit retune dl4k+rc")
    for tr in (0.9, 1.25):
        run(replace(base, name=f"H_dl4k_rc_tr{tr}", daily_loss_rs=4000.0, mask=rcomp,
                    sl_pct=1.25, tgt_pct=9.9, trail_pct=tr), "trail on stack")
        run(replace(base, name=f"H_dl4k_rc_tr{tr}_be", daily_loss_rs=4000.0, mask=rcomp,
                    sl_pct=1.25, tgt_pct=9.9, trail_pct=tr, breakeven_at=0.5), "trail+BE")
    for tc in (90, 150):
        run(replace(base, name=f"H_dl4k_rc_tc{tc}", daily_loss_rs=4000.0, mask=rcomp,
                    time_cap_min=tc), "timecap on stack")

    # H4: confirm-entry versions of the stack
    for ck in (10, 30):
        run(replace(base, name=f"H_conf{ck}_dl4k_rc", entry="confirm", confirm_k=ck,
                    daily_loss_rs=4000.0, mask=rcomp), "confirm stack")
    # H5: retest versions of the stack
    for dep in (0.25, 0.40):
        run(replace(base, name=f"H_ret{dep}_dl4k_rc", entry="retest", retest_depth_atr=dep,
                    confirm_k=45, daily_loss_rs=4000.0, mask=rcomp), "retest stack")

    # H6: volume/context second mask on best stack
    for tag, m2 in [("vol22", ("vol_ratio", ">=", 2.2)), ("adx25", ("adx_x", ">=", 25.0)),
                    ("bmarg", ("break_margin_atr", ">=", 0.10)), ("dret1", ("day_ret_pct", ">=", 1.0))]:
        run(replace(base, name=f"H_dl4k_rc_{tag}", daily_loss_rs=4000.0, mask=rcomp + [m2]), "second mask")

    allrows = pd.DataFrame(list(prev.to_dict("records")) + rows)
    allrows.to_csv(LOG, index=False)
    print(f"\n[rescue] iterations now: {it} (+{len(rows)}) elapsed={time.time()-t0:.0f}s")
    h = pd.DataFrame(rows)
    h["s"] = h.fit_pf.fillna(0) + h.val_pf.fillna(0)
    cols = ["iter", "name", "fit_n", "fit_pf", "val_n", "val_pf", "train_n", "train_pf", "test_n", "test_pf", "verdict"]
    print(h.sort_values("s", ascending=False).head(18)[cols].to_string(index=False))
    band = h[h.verdict.isin(["CANDIDATE", "BAND_BUT_TEST_FAIL"])]
    print(f"\n[rescue] band members: {len(band)}  candidates: {len(h[h.verdict=='CANDIDATE'])}")
    if len(band):
        print(band[cols + ["test_net"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
