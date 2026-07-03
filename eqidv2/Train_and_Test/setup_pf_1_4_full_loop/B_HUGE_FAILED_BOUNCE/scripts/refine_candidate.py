r"""refine_candidate.py — ROUND 3: disciplined LOCAL refinement of the round-2
near-band anchor for B_HUGE_FAILED_BOUNCE (SHORT).

Anchor (round-2 finalist 1, FIT 1.343 / VAL 1.198 / TRAIN 1.263 n=43):
  mask  regime != BULL
  premom pre3_close_pos <= 0.564802 AND pre3_range_r >= 0.279969
  guards min_slot 12:00, max_slot 14:00, top_n 1
  exit  SL 1.20 / Tgt 1.50, max_positions 10

Protocol (anti-overfit, TRAIN-side only):
  Stage A: exits fixed at anchor; local grid over premom threshold steps x slot
           windows x optional ONE fixed-threshold structural confirmation
           (ema20_dist_atr<=0 = below EMA20; day_ret_pct<=0 = down day so far),
           scored on FIT/VAL with the band objective.
  Stage B: top-10 stage-A configs x exit grid (SL 1.0/1.2/1.5 x Tgt 1.25/1.5/2.0).
  Stage C: shortlist = FIT PF>=1.30 AND VAL PF>=1.30 (both n>=10); confirm on full
           TRAIN: PF in [1.30,1.80], n>=25, positive net, target-fill>=12%,
           trade<=0.35 gross, day<=0.40 net, sym<=0.40 net, tpd<=6.
  Stage D: SINGLE best surviving config (by min(FIT,VAL) then TRAIN stability):
           robustness (neighborhood+dropout via engine) then TEST scored ONCE.

RESEARCH-ONLY. Artifacts -> round3/ . No conf edits, no live execution.
Run from repo root:
  py -3.12 Train_and_Test\setup_pf_1_4_full_loop\B_HUGE_FAILED_BOUNCE\scripts\refine_candidate.py
"""
from __future__ import annotations

import argparse
import itertools
import json
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
WORK = _HERE.parent
TT_DIR = WORK.parent.parent
REPO = TT_DIR.parent
ENGINE_DIR = TT_DIR / "setup_pf_1_4_approval_loop" / "_engine"
for _p in (str(REPO), str(TT_DIR), str(ENGINE_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt      # noqa: E402
import pf_band_fitval_loop as eng  # noqa: E402

SETUP = "B_HUGE_FAILED_BOUNCE"
SIDE = "SHORT"
PF_LO, PF_HI = 1.30, 1.80
eng.PF_LO, eng.PF_HI = PF_LO, PF_HI
TEST_PF_MIN = 1.40
OUT = WORK / "round3"
OUT.mkdir(exist_ok=True)

ANCHOR = {
    "sl": 1.20, "tgt": 1.50,
    "mask_terms": [("regime", "!=", "BULL")],
    "premom_terms": [("pre3_close_pos", "<=", 0.564802), ("pre3_range_r", ">=", 0.279969)],
    "guard": {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1},
    "status": "OK", "max_positions": 10, "daily_loss_rs": 0.0,
}

PM1_STEPS = [0.48, 0.564802, 0.64]        # pre3_close_pos <=
PM2_STEPS = [0.21, 0.279969, 0.36]        # pre3_range_r >=
MIN_SLOTS = ["11:30", "12:00", "12:30"]
MAX_SLOTS = ["13:30", "14:00", "14:30"]
EXTRA_MASKS = [None, ("ema20_dist_atr", "<=", 0.0), ("day_ret_pct", "<=", 0.0)]
SL_GRID = [1.00, 1.20, 1.50]
TGT_GRID = [1.25, 1.50, 2.00]


def evalf(cfg, df):
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    return eng._eval_fast(SETUP, cfg, df)


def evalm(cfg, df):
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    return eng.full_metrics(SETUP, cfg, df)


def band_score(pf_f, pf_v, nf, nv):
    if nf < 10 or nv < 10:
        return -5.0 + min(nf, nv) / 10.0
    cf, cv = eng._clamp_pf(pf_f), eng._clamp_pf(pf_v)
    return eng.band_reward(min(cf, cv)) - 0.80 * abs(cf - cv)


def dom_ok(m):
    return (m["trade_dom_gross"] is not None and m["trade_dom_gross"] <= 0.35
            and m["day_dom"] is not None and m["day_dom"] <= 0.40
            and m["sym_dom"] is not None and m["sym_dom"] <= 0.40)


def mline(m):
    return (f"n={m['n']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} win%={m['win_rate']} "
            f"SL/TGT/EOD={m['sl_cnt']}/{m['tgt_cnt']}/{m['eod_cnt']} tgt%={m['target_rate']} "
            f"tpd={m['trades_per_day']} tradeDom={m['trade_dom_gross']} dayDom={m['day_dom']} "
            f"symDom={m['sym_dom']} dbp={m['day_block_p']}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=str(WORK / "pools" / (SETUP + "_enriched")))
    ap.add_argument("--train_start", default="2026-03-01")
    ap.add_argument("--train_end", default="2026-05-30")
    ap.add_argument("--test_start", default="2026-06-01")
    ap.add_argument("--test_end", default="2026-07-02")
    ap.add_argument("--fit_frac", type=float, default=0.60)
    ap.add_argument("--search_slippage_bps", type=float, default=15.0)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    tt.POOL_DIRS = [Path(args.pool)]
    tt.POOL_DIR = Path(args.pool)
    pool = tt.load_pool()
    pool = pool[pool["setup"] == SETUP].copy()
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))

    def _in(s, lo, hi):
        return pd.Timestamp(lo) <= s <= pd.Timestamp(hi)
    TRAIN_s = [s for s in sessions if _in(s, args.train_start, args.train_end)]
    TEST_s = [s for s in sessions if _in(s, args.test_start, args.test_end)]
    n_fit = max(1, int(round(len(TRAIN_s) * args.fit_frac)))
    FIT_s, VAL_s = TRAIN_s[:n_fit], TRAIN_s[n_fit:]
    print(f"[refine] {SETUP} TRAIN {len(TRAIN_s)} sess | TEST {len(TEST_s)} sess "
          f"| FIT {len(FIT_s)} / VAL {len(VAL_s)}")

    span = set(map(pd.Timestamp, TRAIN_s + TEST_s))
    sub = pool[pool["_day"].isin(span)].copy()
    eng._set_slippage(args.search_slippage_bps)
    sub = tt.attach_entries(sub)

    def _slice(ss):
        return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    FIT, VAL, TRAIN, TEST = _slice(FIT_s), _slice(VAL_s), _slice(TRAIN_s), _slice(TEST_s)
    print(f"[refine] rows FIT={len(FIT)} VAL={len(VAL)} TRAIN={len(TRAIN)} TEST={len(TEST)}")

    rows = []
    t0 = time.time()

    def try_cfg(stage, cfg):
        nf, pf_f, _ = evalf(cfg, FIT)
        nv, pf_v, _ = evalf(cfg, VAL)
        sc = band_score(pf_f, pf_v, nf, nv)
        rows.append({"stage": stage, "sl": cfg["sl"], "tgt": cfg["tgt"],
                     "mask": "; ".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]),
                     "premom": "; ".join(f"{a}{o}{b}" for a, o, b in cfg["premom_terms"]),
                     "guard": json.dumps(cfg["guard"]),
                     "fit_n": nf, "fit_pf": round(pf_f, 3), "val_n": nv, "val_pf": round(pf_v, 3),
                     "score": round(sc, 4), "cfg": json.dumps(tt._json_sanitize(cfg), default=str)})
        return sc, nf, pf_f, nv, pf_v

    # ---- Stage A: premom x slots x extra-mask (exits at anchor) ----
    for pm1, pm2, ms, xs, xm in itertools.product(PM1_STEPS, PM2_STEPS, MIN_SLOTS, MAX_SLOTS, EXTRA_MASKS):
        cfg = eng._copy_cfg(ANCHOR)
        cfg["premom_terms"] = [("pre3_close_pos", "<=", pm1), ("pre3_range_r", ">=", pm2)]
        cfg["guard"] = {"min_slot": ms, "max_slot": xs, "top_n": 1}
        cfg["mask_terms"] = list(ANCHOR["mask_terms"]) + ([xm] if xm else [])
        try_cfg("A", cfg)
    dfA = pd.DataFrame(rows)
    print(f"[refine] stage A done: {len(dfA)} configs in {time.time()-t0:.0f}s; "
          f"best score {dfA['score'].max():.3f}")

    # ---- Stage B: top-10 x exit grid ----
    topA = dfA.sort_values("score", ascending=False).drop_duplicates("cfg").head(10)
    for _, r in topA.iterrows():
        base = json.loads(r["cfg"])
        base["mask_terms"] = [tuple(t) for t in base["mask_terms"]]
        base["premom_terms"] = [tuple(t) for t in base["premom_terms"]]
        for slv, tg in itertools.product(SL_GRID, TGT_GRID):
            if (slv, tg) == (base["sl"], base["tgt"]):
                continue
            cfg = eng._copy_cfg(base)
            cfg["sl"], cfg["tgt"] = slv, tg
            try_cfg("B", cfg)
    df = pd.DataFrame(rows)
    df.to_csv(OUT / "refine_trials.csv", index=False)
    print(f"[refine] stage B done: {len(df)} total configs")

    # ---- Stage C: shortlist -> TRAIN confirm ----
    short = df[(df["fit_pf"] >= PF_LO) & (df["val_pf"] >= PF_LO)
               & (df["fit_n"] >= 10) & (df["val_n"] >= 10)].sort_values("score", ascending=False)
    print(f"[refine] shortlist (FIT&VAL both >= {PF_LO}): {len(short)}")
    survivors = []
    seen = set()
    for _, r in short.iterrows():
        if r["cfg"] in seen:
            continue
        seen.add(r["cfg"])
        cfg = json.loads(r["cfg"])
        cfg["mask_terms"] = [tuple(t) for t in cfg["mask_terms"]]
        cfg["premom_terms"] = [tuple(t) for t in cfg["premom_terms"]]
        mTR = evalm(cfg, TRAIN)
        ok = (PF_LO <= mTR["net_pf"] <= PF_HI and mTR["n"] >= 25 and mTR["net_pnl"] > 0
              and mTR["target_rate"] >= 12.0 and mTR["trades_per_day"] <= 6.0 and dom_ok(mTR))
        print(f"[refine] TRAIN confirm score={r['score']}: {mline(mTR)} -> {'OK' if ok else 'reject'}")
        if ok:
            survivors.append((float(r["score"]), cfg, mTR))
        if len(survivors) >= 3 or len(seen) >= 15:
            break

    result = {"setup": SETUP, "side": SIDE, "anchor": tt._json_sanitize(ANCHOR),
              "generated": date.today().isoformat(),
              "n_configs": int(len(df)), "n_shortlist": int(len(short)),
              "n_train_survivors": len(survivors), "passed": False}

    if not survivors:
        print("[refine] no TRAIN survivor — refinement REJECT (no TEST spent)")
        (OUT / "refine_summary.json").write_text(json.dumps(tt._json_sanitize(result), indent=2, default=str),
                                                 encoding="utf-8")
        return 0

    # ---- Stage D: single best -> robustness + ONE TEST ----
    survivors.sort(key=lambda x: -x[0])
    sc, cfg, mTR = survivors[0]
    mask_quant, pm_quant = {}, {}
    for f in ("pre3_close_pos", "pre3_range_r"):
        s = []
        for j, r in enumerate(TRAIN.sample(n=min(800, len(TRAIN)), random_state=7).itertuples()):
            feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), 0.90,
                                       r.tt_sig_ts.isoformat())
            if not reason:
                v = dict(feats).get(f)
                if v is not None and np.isfinite(v):
                    s.append(float(v))
        s = pd.Series(s)
        if len(s) >= 8:
            pm_quant[f] = {q: float(s.quantile(q)) for q in eng.QGRID}
    for f in ("ema20_dist_atr", "day_ret_pct"):
        s = pd.to_numeric(TRAIN.get(f), errors="coerce").dropna()
        if len(s) >= 8:
            mask_quant[f] = {q: float(s.quantile(q)) for q in eng.QGRID}
    ns = argparse.Namespace(min_trades_train=25, neighborhood_pf_min=1.15, dropout_pf_min=1.00)
    robust = eng.robustness_report(SETUP, cfg, TRAIN, mask_quant, pm_quant, ns)
    mTE = evalm(cfg, TEST)
    print(f"[refine] BEST cfg: {json.dumps(tt._json_sanitize(cfg), default=str)}")
    print(f"[refine] TRAIN: {mline(mTR)}")
    print(f"[refine] TEST : {mline(mTE)}")
    print(f"[refine] robustness: neighbor={robust['neighbor_pass']} dropout={robust['dropout_pass']}")

    hard = []
    if not (PF_LO <= mTR["net_pf"] <= PF_HI):
        hard.append("TRAIN PF out of band")
    if mTE["n"] < 5:
        hard.append("TEST n < 5")
    else:
        if mTE["net_pf"] <= TEST_PF_MIN:
            hard.append(f"TEST PF {mTE['net_pf']} <= {TEST_PF_MIN}")
        if mTE["net_pnl"] <= 0:
            hard.append("TEST net not positive")
        if not dom_ok(mTE):
            hard.append("TEST domination")
        if mTE["day_block_p"] is None or not np.isfinite(float(mTE["day_block_p"])) or mTE["day_block_p"] > 0.10:
            hard.append(f"TEST day-block p {mTE['day_block_p']} > 0.10")
    if not robust["neighbor_pass"]:
        hard.append("neighborhood robustness failed")
    if not robust["dropout_pass"]:
        hard.append("dropout robustness failed")
    passed = not hard
    result.update({"passed": passed, "hard_reasons": hard,
                   "best_cfg": eng.cfg_to_conf_block(SETUP, SIDE, cfg),
                   "train": {k: v for k, v in mTR.items() if k != "detail"},
                   "test": {k: v for k, v in mTE.items() if k != "detail"},
                   "robust": {k: robust[k] for k in ("neighbor_pass", "dropout_pass", "passed")}})
    (OUT / "refine_summary.json").write_text(json.dumps(tt._json_sanitize(result), indent=2, default=str),
                                             encoding="utf-8")
    mTR["detail"].to_csv(OUT / "refine_best_trades_train.csv", index=False)
    if mTE["detail"] is not None and not mTE["detail"].empty:
        mTE["detail"].to_csv(OUT / "refine_best_trades_test.csv", index=False)
    print(f"[refine] VERDICT: {'PASS — APPROVAL REQUIRED' if passed else 'REJECT'} "
          + ("" if passed else f"({'; '.join(hard)})"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
