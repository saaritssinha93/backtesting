r"""rescue_round2.py — Stage 8 round 2 for C_OR_BREAKDOWN: exhaust the strongest
observed pocket (conf-ADX gate x quiet-bar filters x exits x time guards).

The 1,382-iteration loop found the only above-water FIT/VAL blocks were:
  conf gate (sig5_adx>=39.67 & pre1_adx<=21.37) + small signal_range_pct /
  small pm_pre3_range_r / mid body_pct / vol_ratio cap / sig5_rsi_dir cap.
This round tests all 1-3-term combinations of those blocks on top of the gate,
across 6 exits and 3 time guards — FIT/VAL first, TRAIN confirm, TEST once (shared
budget with the main loop: 15 total).
"""
from __future__ import annotations

import itertools
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[2] / "_shared"))
import recovery_lib as rl  # noqa: E402

SETUP = "C_OR_BREAKDOWN"
WORK = HERE.parents[1]


def main() -> int:
    eng = rl.ResearchEngine(SETUP, WORK)
    w = eng.w
    log = rl.IterLog(WORK)

    import pandas as pd

    def q(feat, qq, df=w["FIT"]):
        s = pd.to_numeric(df[feat], errors="coerce").dropna()
        return round(float(s.quantile(qq)), 6) if len(s) else None

    GATE = [("sig5_adx_calc", ">=", 39.670518), ("pre1_adx", "<=", 21.368044)]
    BROAD = [("sig5_adx_calc", ">=", 30.0), ("pre1_adx", "<=", 25.0)]

    # building blocks (mask side)
    MASK_BLOCKS = {
        "srp_q25": ("signal_range_pct", "<=", q("signal_range_pct", 0.25)),
        "srp_q50": ("signal_range_pct", "<=", q("signal_range_pct", 0.50)),
        "body_q50": ("body_pct", ">=", q("body_pct", 0.50)),
        "volcap_q50": ("vol_ratio", "<=", q("vol_ratio", 0.50)),
        "uwick_q50": ("upper_wick_pct", ">=", q("upper_wick_pct", 0.50)),
        "near_vwap": ("vwap_dist_atr", ">=", q("vwap_dist_atr", 0.75)),
        "fresh3": ("fresh_age_bars", "<=", 3.0),
    }
    PM_BLOCKS = {
        "p3r_q25": ("pre3_range_r", "<=", q("pm_pre3_range_r", 0.25)),
        "p3r_q50": ("pre3_range_r", "<=", q("pm_pre3_range_r", 0.50)),
        "rsidir_q75cap": ("sig5_rsi_dir", "<=", q("pm_sig5_rsi_dir", 0.75)),
    }
    EXITS = [(0.9, 2.0), (0.9, 1.5), (0.9, 1.25), (0.7, 1.25), (1.1, 1.5), (1.1, 2.0)]
    GUARDS = [None, {"max_slot": "12:30"}, {"max_slot": "13:30"}]

    def ev(cfg, wn):
        return eng.eval_cfg(cfg, w[wn], wname=wn, day_block=(wn in ("TRAIN", "TEST")))

    used_test = int(pd.read_csv(WORK / "iteration_log.csv")["stage"].eq("S7_test").sum()) if (WORK / "iteration_log.csv").exists() else 0
    budget = 15 - used_test
    results = []
    scored = []

    mask_names = list(MASK_BLOCKS)
    pm_names = list(PM_BLOCKS)
    combos = []
    for gate_name, gate in (("conf", GATE), ("broad", BROAD)):
        for r_m in (0, 1, 2):
            for mc in itertools.combinations(mask_names, r_m):
                for r_p in (0, 1):
                    for pc in itertools.combinations(pm_names, r_p):
                        if r_m + r_p == 0:
                            continue
                        combos.append((gate_name, gate, mc, pc))
    print(f"[R2] {len(combos)} block combos x {len(EXITS)} exits x {len(GUARDS)} guards (staged)")

    for gate_name, gate, mc, pc in combos:
        mask = [MASK_BLOCKS[m] for m in mc if MASK_BLOCKS[m][2] is not None]
        pm = list(gate) + [PM_BLOCKS[p] for p in pc if PM_BLOCKS[p][2] is not None]
        base = {"sl": 0.9, "tgt": 1.5, "prefilter_terms": [], "mask_terms": mask,
                "premom_terms": pm, "guard": None, "max_positions": 20, "daily_loss_rs": 0.0}
        mF, mV = ev(base, "FIT"), ev(base, "VAL")
        sc = rl.score_fit_val(mF, mV)
        tag = f"{gate_name}+{'+'.join(mc)}+{'+'.join(pc)}"
        log.log("S8_round2", "blocks", tag, base, mF, mV)
        scored.append((sc, tag, base, mF, mV))
    scored.sort(key=lambda x: -x[0])

    # exits+guards refinement on the top 10 block combos
    refined = []
    for sc, tag, base, mF0, mV0 in scored[:10]:
        for sl, tgt in EXITS:
            for gd in GUARDS:
                c = json.loads(json.dumps(base, default=str))
                for k in ("prefilter_terms", "mask_terms", "premom_terms"):
                    c[k] = [tuple(t) for t in (c.get(k) or [])]
                c["sl"], c["tgt"], c["guard"] = sl, tgt, gd
                mF, mV = ev(c, "FIT"), ev(c, "VAL")
                s2 = rl.score_fit_val(mF, mV)
                log.log("S8_round2", "refine", f"{tag} exit {sl}/{tgt} g={gd}", c, mF, mV)
                refined.append((s2, f"{tag}|{sl}/{tgt}|{gd}", c))
    refined.sort(key=lambda x: -x[0])
    log.flush()

    print(f"[R2] top refined scores: {[round(s,3) for s,_,_ in refined[:8]]}")
    n_cand = 0
    for s2, tag, c in refined[:12]:
        if s2 < 0.8:
            break
        mTR = ev(c, "TRAIN")
        note = f"TRAIN {mTR['n']}/{mTR['net_pf']}/Rs{mTR['net_pnl']:,.0f}"
        in_band = (rl.PF_LO <= mTR["net_pf"] <= rl.PF_HI and mTR["n"] >= rl.MIN_TRADES_TRAIN
                   and mTR["net_pnl"] > 0 and rl.dom_ok(mTR))
        if not in_band:
            log.log("S8_round2", "train", tag, c, None, None, mTR, decision="reject_train", note=note)
            print(f"[R2] reject_train {tag}: {note}")
            continue
        if budget <= 0:
            log.log("S8_round2", "train", tag, c, None, None, mTR,
                    decision="in_band_no_test_budget", note=note)
            continue
        budget -= 1
        mTE = ev(c, "TEST")
        ok = (mTE["net_pf"] > rl.TEST_PF_MIN and mTE["n"] >= rl.MIN_TRADES_TEST
              and mTE["net_pnl"] > 0 and rl.dom_ok(mTE)
              and (mTE["day_block_p"] is None or mTE["day_block_p"] <= 0.10))
        log.log("S8_round2", "test", tag, c, None, None, mTR, mTE,
                decision="CANDIDATE" if ok else "reject_test",
                note=note + f" | TEST {mTE['n']}/{mTE['net_pf']}")
        print(f"[R2] {'PASS' if ok else 'fail'} {tag} | {note} | TEST {mTE['n']}/{mTE['net_pf']}")
        if ok:
            n_cand += 1
            results.append({"cfg": c, "train": {k: v for k, v in mTR.items() if k != "detail"},
                            "test": {k: v for k, v in mTE.items() if k != "detail"}})
    log.flush()
    for i, cand in enumerate(results, 1):
        (WORK / "candidates" / f"{SETUP}_r2_candidate_{i:03d}.json").write_text(
            json.dumps(cand, indent=2, default=str), encoding="utf-8")
    print(f"[R2] done. candidates={n_cand} best_score={refined[0][0] if refined else None}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
