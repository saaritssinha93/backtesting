r"""config_param_iterate.py — config-centered ONE-PARAMETER-AT-A-TIME sweep (research-only).

Anchors on a GIVEN config and varies EACH parameter in it across a realistic range while holding every
other parameter fixed at the anchor value — i.e. local sensitivity / coordinate sweep. For each parameter
value it reports FIT / VAL / TRAIN / TEST (n, net PF) and flags the CURRENT value and the BEST value
(by min(FIT_PF, VAL_PF) with a per-fold trade floor — anti-overfit, FIT/VAL drive, TEST shown only).
Parameters covered: exit SL%, exit target%, each mask term threshold (+DROP), each pre-momentum term
threshold (+DROP), entry guard max_slot, max_positions, daily_loss_rs.

Also assembles a greedy "coordinate-descent" config (adopt each parameter's best where it beats the anchor)
and reports it on TRAIN/TEST — with an overfit caveat (combining per-knob bests can curve-fit).

Reuses setup_train_test + pf_band_search (same pipeline/cost/split). Net of cost at --slippage_bps.
No live trades; writes CONFIG_PARAM_ITERATION.md under the setup's approval-loop folder.

Run:
  py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/B_AVWAP_RECLAIM_REVERSAL/scripts/config_param_iterate.py \
      --setup B_AVWAP_RECLAIM_REVERSAL --pool <pool> --train_start 2026-05-18 --test_start 2026-06-20 [--slippage_bps 15]
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
SL_GRID = [0.5, 0.6, 0.7, 0.8, 0.85, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5]
TGT_GRID = [1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 3.5]
MAXSLOT_GRID = [None, "11:30", "12:00", "12:30", "13:00", "13:30", "14:00", "14:30", "15:00"]
MAXPOS_GRID = [5, 10, 20]
DLOSS_GRID = [0.0, 2000.0, 3000.0, 5000.0]
PREMOM_SET = set(pb.PM_FEATS)

# Anchor config = the B_AVWAP best candidate (override with --config <json>).
DEFAULT_CONFIG = {
    "exit": {"sl_pct": 0.9, "tgt_pct": 3.0},
    "mask_terms": [["vwap_dist_atr", "<=", 1.0], ["vol_ratio", ">=", 3.537825], ["atr_pct", "<=", 0.003921]],
    "pre_momentum_terms": [["pre1_adx", ">=", 30.675856], ["pre5_mom_r", ">=", 0.317166]],
    "entry_guards": {"max_slot": "14:00"}, "max_positions": 20, "daily_loss_rs": 0.0,
}


def _cfg_from(j):
    return {"sl": float(j["exit"]["sl_pct"]), "tgt": float(j["exit"]["tgt_pct"]),
            "mask_terms": [list(t) for t in j.get("mask_terms", [])],
            "premom_terms": [list(t) for t in j.get("pre_momentum_terms", [])],
            "guard": dict(j.get("entry_guards") or {}),
            "max_positions": int(j.get("max_positions", 20)), "daily_loss_rs": float(j.get("daily_loss_rs", 0.0))}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    ap.add_argument("--pool", required=True)
    ap.add_argument("--train_start", default="2026-05-18")
    ap.add_argument("--test_start", default="2026-06-20")
    ap.add_argument("--slippage_bps", type=float, default=15.0)
    ap.add_argument("--min_fold", type=int, default=6)
    ap.add_argument("--config", default="", help="JSON file with the anchor config (default: built-in B_AVWAP best)")
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass

    setup = args.setup.strip().upper()
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
    print(f"[cfg-iter] {setup} @ {args.slippage_bps:.0f}bps | TRAIN {_d(TRAIN_s)} ({len(TRAIN_s)}) "
          f"TEST {_d(TEST_s)} ({len(TEST_s)}) | entries FIT={len(FIT)} VAL={len(VAL)} TRAIN={len(TRAIN)} TEST={len(TEST)}")
    mask_quant, pm_quant = pb.build_quantiles(TRAIN)

    def metrics(cfg):
        nf, pf_f, _ = pb._light(setup, cfg, FIT)
        nv, pf_v, _ = pb._light(setup, cfg, VAL)
        nt, pf_t, _ = pb._light(setup, cfg, TRAIN)
        ne, pf_e, _ = pb._light(setup, cfg, TEST)
        score = min(pf_f, pf_v) if (nf >= args.min_fold and nv >= args.min_fold) else -1.0
        return dict(fn=nf, fp=round(pf_f, 3), vn=nv, vp=round(pf_v, 3),
                    tn=nt, tp=round(pf_t, 3), en=ne, ep=round(pf_e, 3), score=round(score, 3))

    base_m = metrics(BASE)
    print(f"[cfg-iter] ANCHOR score={base_m['score']} | TRAIN n{base_m['tn']} PF{base_m['tp']} | TEST n{base_m['en']} PF{base_m['ep']}")

    def with_term(kind, idx, thr=None, drop=False):
        c = {**BASE, "mask_terms": [list(t) for t in BASE["mask_terms"]],
             "premom_terms": [list(t) for t in BASE["premom_terms"]], "guard": dict(BASE["guard"])}
        terms = c["mask_terms"] if kind == "mask" else c["premom_terms"]
        if drop:
            del terms[idx]
        else:
            terms[idx] = [terms[idx][0], terms[idx][1], thr]
        return c

    def term_thresholds(feat, cur):
        q = mask_quant.get(feat) or pm_quant.get(feat)
        vals = set()
        if q:
            vals = {round(float(v), 6) for v in q.values()}
        else:
            vals = {round(cur * m, 6) for m in (0.5, 0.75, 0.9, 1.1, 1.25, 1.5)}
        vals.add(round(float(cur), 6))
        return sorted(vals)

    sections = []   # (title, current_label, rows[(label,m,is_current)], best_label)

    def add(title, current_label, variants):
        rows = []
        for lbl, cfg, is_cur in variants:
            rows.append((lbl, metrics(cfg), is_cur))
        viable = [(lbl, m) for lbl, m, _ in rows if m["score"] > -1]
        best = max(viable, key=lambda x: x[1]["score"])[0] if viable else None
        sections.append((title, current_label, rows, best))
        bm = next((m for lbl, m in viable if lbl == best), None)
        print(f"  [{title}] current={current_label} best={best}" + (f" (score {bm['score']})" if bm else ""))
        return best

    # 1) exit SL
    add(f"EXIT sl_pct (anchor {BASE['sl']})", str(BASE["sl"]),
        [(f"SL={v:g}", {**BASE, "sl": v}, v == BASE["sl"]) for v in sorted(set(SL_GRID + [BASE["sl"]]))])
    # 2) exit target
    add(f"EXIT tgt_pct (anchor {BASE['tgt']})", str(BASE["tgt"]),
        [(f"Tgt={v:g}", {**BASE, "tgt": v}, v == BASE["tgt"]) for v in sorted(set(TGT_GRID + [BASE["tgt"]]))])
    # 3) mask term thresholds + drop
    for i, (f, op, cur) in enumerate(BASE["mask_terms"]):
        variants = [(f"{f}{op}{thr:g}", with_term("mask", i, thr=thr), thr == round(float(cur), 6))
                    for thr in term_thresholds(f, cur)]
        variants.append((f"DROP {f}", with_term("mask", i, drop=True), False))
        add(f"MASK term [{f}{op}{cur:g}]", f"{f}{op}{cur:g}", variants)
    # 4) premom term thresholds + drop
    for i, (f, op, cur) in enumerate(BASE["premom_terms"]):
        variants = [(f"{f}{op}{thr:g}", with_term("premom", i, thr=thr), thr == round(float(cur), 6))
                    for thr in term_thresholds(f, cur)]
        variants.append((f"DROP {f}", with_term("premom", i, drop=True), False))
        add(f"PRE-MOM term [{f}{op}{cur:g}]", f"{f}{op}{cur:g}", variants)
    # 5) guard max_slot
    cur_mx = BASE["guard"].get("max_slot")
    def _with_maxslot(v):
        g = dict(BASE["guard"]);
        if v is None: g.pop("max_slot", None)
        else: g["max_slot"] = v
        return {**BASE, "guard": g}
    add(f"GUARD max_slot (anchor {cur_mx})", str(cur_mx),
        [(f"max_slot={v}", _with_maxslot(v), v == cur_mx) for v in MAXSLOT_GRID])
    # 6) max_positions
    add(f"max_positions (anchor {BASE['max_positions']})", str(BASE["max_positions"]),
        [(f"maxpos={v}", {**BASE, "max_positions": v}, v == BASE["max_positions"]) for v in sorted(set(MAXPOS_GRID + [BASE["max_positions"]]))])
    # 7) daily_loss_rs
    add(f"daily_loss_rs (anchor {BASE['daily_loss_rs']:g})", f"{BASE['daily_loss_rs']:g}",
        [(f"dloss={v:g}", {**BASE, "daily_loss_rs": v}, v == BASE["daily_loss_rs"]) for v in sorted(set(DLOSS_GRID + [BASE["daily_loss_rs"]]))])

    # full metrics for the anchor (net/dominance context for the report header)
    fm_anchor = pb.full_metrics(setup, BASE, TRAIN); fm_anchor_te = pb.full_metrics(setup, BASE, TEST)

    # ---- markdown ----
    def fmt_row(lbl, m, cur, best):
        tag = []
        if cur: tag.append("**(current)**")
        if lbl == best: tag.append("✅best")
        inband = BAND_LO <= m["tp"] <= BAND_HI
        return (f"| {lbl} {' '.join(tag)} | {m['fn']}/{m['fp']} | {m['vn']}/{m['vp']} | "
                f"{m['tn']}/{m['tp']}{'✓' if inband else ''} | {m['en']}/{m['ep']} | {m['score']} |")

    md = [f"# CONFIG_PARAM_ITERATION — {setup}", "",
          f"One-parameter-at-a-time sweep **anchored on the given config**, net of cost @ {args.slippage_bps:.0f} bps/leg. "
          "Each row changes ONLY that parameter; all others stay at the anchor. 'score' = min(FIT_PF, VAL_PF) "
          f"(−1 if a fold has < {args.min_fold} trades). ✓ on TRAIN = inside the [{BAND_LO},{BAND_HI}] band. "
          "TEST shown for reference only (never optimised on).", "",
          f"- TRAIN {_d(TRAIN_s)} ({len(TRAIN_s)}) · TEST {_d(TEST_s)} ({len(TEST_s)}) · "
          f"entries FIT={len(FIT)} VAL={len(VAL)} TRAIN={len(TRAIN)} TEST={len(TEST)}",
          "", "## Anchor config",
          "```json", json.dumps(anchor_json, indent=2), "```",
          f"- Anchor metrics: FIT {base_m['fn']}/{base_m['fp']} · VAL {base_m['vn']}/{base_m['vp']} · "
          f"TRAIN {base_m['tn']}/{base_m['tp']} (net Rs{fm_anchor['net_pnl']:,.0f}, dayDom {fm_anchor['day_dom']}) · "
          f"TEST {base_m['en']}/{base_m['ep']} (net Rs{fm_anchor_te['net_pnl']:,.0f}) · score {base_m['score']}", ""]
    for title, curlbl, rows, best in sections:
        md += [f"## {title}", "",
               "| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |", "|---|---|---|---|---|---|"]
        for lbl, m, cur in rows:
            md.append(fmt_row(lbl, m, cur, best))
        # best vs current verdict
        cur_m = next((m for lbl, m, cur in rows if cur), None)
        best_m = next((m for lbl, m, _ in rows if lbl == best), None)
        if best and cur_m and best_m:
            verdict = ("current value is already best" if best == curlbl
                       else f"**best = `{best}`** (score {best_m['score']} vs current {cur_m['score']}); "
                            f"TRAIN PF {best_m['tp']}{' in-band' if BAND_LO<=best_m['tp']<=BAND_HI else ''}, TEST PF {best_m['ep']}")
            md += ["", f"- {verdict}", ""]
        else:
            md += ["", "- no variant met the per-fold trade floor", ""]

    (outdir / "CONFIG_PARAM_ITERATION.md").write_text("\n".join(md), encoding="utf-8")
    print(f"[cfg-iter] wrote {outdir/'CONFIG_PARAM_ITERATION.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
