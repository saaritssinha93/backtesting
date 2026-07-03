r"""add_indicator_sweep.py — ADD one indicator term to an anchor config and sweep its threshold.

Takes the anchor config and ADDS a single new condition [feature op threshold], sweeping threshold across
a realistic range, holding everything else fixed. Reports FIT/VAL/TRAIN/TEST (n, net PF) per threshold,
flags the best (min(FIT_PF,VAL_PF) with a per-fold trade floor), and writes the best-as-config JSON so the
coordinate sweep (config_param_iterate.py) can be re-run on it. Default feature = sig5_adx_calc (5-min ADX).

Reuses setup_train_test + pf_band_search. Net of cost @ --slippage_bps. No live trades.

Run:
  py -3.12 .../scripts/add_indicator_sweep.py --setup B_AVWAP_RECLAIM_REVERSAL --pool <pool> \
      --train_start 2026-05-18 --test_start 2026-06-20 --feature sig5_adx_calc --op ">=" [--slippage_bps 15]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_P = Path(__file__).resolve()
TT_DIR = next(par for par in _P.parents if par.name == "Train_and_Test")
REPO_ROOT = TT_DIR.parent
for _d in (str(REPO_ROOT), str(TT_DIR), str(_P.parent)):
    if _d not in sys.path:
        sys.path.insert(0, _d)

import setup_train_test as tt          # noqa: E402
import pf_band_search as pb            # noqa: E402

BAND_LO, BAND_HI = 1.30, 1.70
PREMOM_SET = set(pb.PM_FEATS)

DEFAULT_CONFIG = {
    "exit": {"sl_pct": 0.9, "tgt_pct": 3.0},
    "mask_terms": [["vwap_dist_atr", "<=", 1.0], ["vol_ratio", ">=", 3.537825], ["atr_pct", "<=", 0.003921]],
    "pre_momentum_terms": [["pre1_adx", ">=", 30.675856], ["pre5_mom_r", ">=", 0.317166]],
    "entry_guards": {"max_slot": "14:00"}, "max_positions": 20, "daily_loss_rs": 0.0,
}
# Classic ADX trend-strength bands (low / medium / strong); generic grids for a few other features.
GRIDS = {
    "sig5_adx_calc": [12, 15, 18, 20, 22, 25, 28, 30, 33, 35, 40],
    "pre1_adx":      [12, 15, 18, 20, 22, 25, 28, 30, 33, 35, 40],
    "sig5_rsi_dir":  [45, 50, 52, 55, 58, 60, 63, 66, 70],
}


def _cfg_from(j):
    return {"sl": float(j["exit"]["sl_pct"]), "tgt": float(j["exit"]["tgt_pct"]),
            "mask_terms": [list(t) for t in j.get("mask_terms", [])],
            "premom_terms": [list(t) for t in j.get("pre_momentum_terms", [])],
            "guard": dict(j.get("entry_guards") or {}),
            "max_positions": int(j.get("max_positions", 20)), "daily_loss_rs": float(j.get("daily_loss_rs", 0.0))}


def _to_json(cfg):
    return {"exit": {"sl_pct": cfg["sl"], "tgt_pct": cfg["tgt"]},
            "mask_terms": [list(t) for t in cfg["mask_terms"]],
            "pre_momentum_terms": [list(t) for t in cfg["premom_terms"]],
            "entry_guards": cfg["guard"] or {}, "max_positions": cfg["max_positions"], "daily_loss_rs": cfg["daily_loss_rs"]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    ap.add_argument("--pool", required=True)
    ap.add_argument("--train_start", default="2026-05-18")
    ap.add_argument("--test_start", default="2026-06-20")
    ap.add_argument("--slippage_bps", type=float, default=15.0)
    ap.add_argument("--min_fold", type=int, default=6)
    ap.add_argument("--feature", default="sig5_adx_calc")
    ap.add_argument("--op", default=">=", choices=[">=", "<="])
    ap.add_argument("--config", default="")
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass

    setup = args.setup.strip().upper()
    feat, op = args.feature, args.op
    outdir = Path(args.out) if args.out else (TT_DIR / "setup_pf_1_4_approval_loop" / setup)
    outdir.mkdir(parents=True, exist_ok=True)
    anchor_json = json.loads(Path(args.config).read_text(encoding="utf-8")) if args.config else DEFAULT_CONFIG
    BASE = _cfg_from(anchor_json)

    tt.POOL_DIRS = [Path(args.pool)]; tt.POOL_DIR = Path(args.pool)
    pb._set_slip(args.slippage_bps)
    pool = tt.load_pool(); pool = pool[pool["setup"] == setup].copy()
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    ts, te = pd.Timestamp(args.train_start), pd.Timestamp(args.test_start)
    TEST_s = [s for s in sessions if s >= te]
    TRAIN_s = [s for s in sessions if ts <= s < (TEST_s[0] if TEST_s else sessions[-1] + pd.Timedelta(days=1))]
    half = len(TRAIN_s) // 2
    FIT_s, VAL_s = TRAIN_s[:half], TRAIN_s[half:]
    span = set(map(pd.Timestamp, TRAIN_s + TEST_s))
    sub = tt.attach_entries(pool[pool["_day"].isin(span)].copy())

    def _sl(ss): return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    FIT, VAL, TRAIN, TEST = _sl(FIT_s), _sl(VAL_s), _sl(TRAIN_s), _sl(TEST_s)

    def _d(ss): return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}"
    print(f"[add-ind] {setup} @ {args.slippage_bps:.0f}bps | add [{feat}{op}thr] | "
          f"TRAIN {_d(TRAIN_s)} ({len(TRAIN_s)}) TEST {_d(TEST_s)} ({len(TEST_s)})")

    mask_quant, pm_quant = pb.build_quantiles(TRAIN)
    grid = list(GRIDS.get(feat, []))
    q = mask_quant.get(feat) or pm_quant.get(feat)
    if q:
        grid += [round(float(v), 6) for v in q.values()]
    grid = sorted(set(round(float(g), 6) for g in grid))

    def add_term(thr):
        c = {**BASE, "mask_terms": [list(t) for t in BASE["mask_terms"]],
             "premom_terms": [list(t) for t in BASE["premom_terms"]], "guard": dict(BASE["guard"])}
        (c["premom_terms"] if feat in PREMOM_SET else c["mask_terms"]).append([feat, op, thr])
        return c

    def metrics(cfg):
        nf, pf_f, _ = pb._light(setup, cfg, FIT)
        nv, pf_v, _ = pb._light(setup, cfg, VAL)
        nt, pf_t, _ = pb._light(setup, cfg, TRAIN)
        ne, pf_e, _ = pb._light(setup, cfg, TEST)
        score = min(pf_f, pf_v) if (nf >= args.min_fold and nv >= args.min_fold) else -1.0
        return dict(fn=nf, fp=round(pf_f, 3), vn=nv, vp=round(pf_v, 3),
                    tn=nt, tp=round(pf_t, 3), en=ne, ep=round(pf_e, 3), score=round(score, 3))

    base_m = metrics(BASE)
    rows = [("(anchor — no " + feat + ")", base_m, None)]
    for thr in grid:
        rows.append((f"{feat}{op}{thr:g}", metrics(add_term(thr)), thr))
    viable = [(lbl, m, thr) for lbl, m, thr in rows[1:] if m["score"] > -1]
    best = max(viable, key=lambda x: x[1]["score"]) if viable else None

    print(f"[add-ind] anchor score={base_m['score']} (TRAIN n{base_m['tn']} PF{base_m['tp']} | TEST n{base_m['en']} PF{base_m['ep']})")
    if best:
        bl, bm, bthr = best
        print(f"[add-ind] best add = {bl}  score={bm['score']} (TRAIN n{bm['tn']} PF{bm['tp']} | TEST n{bm['en']} PF{bm['ep']})")

    # write best-as-config JSON for re-running the coordinate sweep
    best_cfg_path = None
    if best:
        new_cfg = add_term(best[2])
        best_cfg_path = outdir / f"config_with_{feat}.json"
        best_cfg_path.write_text(json.dumps(_to_json(new_cfg), indent=2), encoding="utf-8")

    # markdown
    md = [f"# ADD_INDICATOR — {setup}: add `{feat} {op} X` to the anchor config", "",
          f"Net of cost @ {args.slippage_bps:.0f} bps/leg. Each row ADDS only `{feat}{op}threshold` to the anchor; "
          f"everything else fixed. score = min(FIT_PF,VAL_PF) (−1 if a fold < {args.min_fold} trades). "
          "✓ on TRAIN = inside [1.30,1.70]. TEST shown for reference only.", "",
          f"- TRAIN {_d(TRAIN_s)} ({len(TRAIN_s)}) · TEST {_d(TEST_s)} ({len(TEST_s)})  | feature `{feat}` is a "
          f"{'pre-momentum' if feat in PREMOM_SET else 'mask'} term", "",
          "| added condition | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |", "|---|---|---|---|---|---|"]
    for lbl, m, thr in rows:
        tag = ""
        if best and lbl == best[0]: tag = " ✅best"
        if thr is None: tag += " *(anchor)*"
        inband = BAND_LO <= m["tp"] <= BAND_HI
        md.append(f"| {lbl}{tag} | {m['fn']}/{m['fp']} | {m['vn']}/{m['vp']} | {m['tn']}/{m['tp']}{'✓' if inband else ''} | "
                  f"{m['en']}/{m['ep']} | {m['score']} |")
    md.append("")
    if best:
        bl, bm, bthr = best
        improved = bm["score"] > base_m["score"] + 1e-9
        md += [f"## Verdict",
               f"- anchor score {base_m['score']} (TEST PF {base_m['ep']}, n {base_m['en']}).",
               f"- best `{feat}{op}{bthr:g}`: score {bm['score']}, TRAIN n{bm['tn']} PF{bm['tp']}"
               f"{' (in-band)' if BAND_LO<=bm['tp']<=BAND_HI else ''}, **TEST n{bm['en']} PF{bm['ep']}**.",
               f"- adding `{feat}` {'IMPROVES' if improved else 'does NOT improve'} the FIT/VAL score; "
               f"TEST PF goes {base_m['ep']} → {bm['ep']} on n {base_m['en']} → {bm['en']}.",
               f"- new config written to `{best_cfg_path.name}` (for the coordinate re-sweep)." if best_cfg_path else ""]
    else:
        md += ["## Verdict", f"- no threshold of `{feat}` kept both folds above the {args.min_fold}-trade floor."]
    (outdir / f"ADD_INDICATOR_{feat}.md").write_text("\n".join(md), encoding="utf-8")
    print(f"[add-ind] wrote {outdir/('ADD_INDICATOR_'+feat+'.md')}" + (f" + {best_cfg_path.name}" if best_cfg_path else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
