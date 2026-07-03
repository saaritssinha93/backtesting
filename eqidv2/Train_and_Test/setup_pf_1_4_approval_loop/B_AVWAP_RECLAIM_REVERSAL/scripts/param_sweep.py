r"""param_sweep.py — Stage-2 INDIVIDUAL-KNOB range sweep (research-only).

For ONE setup, on the FIT/VAL split, sweep each knob across a reasonable VALUE RANGE one at a time
(exit SL, exit target, every numeric mask feature, every pre-momentum feature, the categorical regime
filter, and entry guards min_slot/max_slot/top_n), holding everything else at a clean base
(raw detection + card exit). For each knob it prints FIT and VAL (n, PF) at every tested value and
identifies the BEST STABLE RANGE (values where BOTH FIT and VAL hold up) vs REJECTED values.

This is Stage 2 of the pf_1_4 protocol; Stage 3+ (combinations, full-TRAIN confirm, TEST) is done by
pf_band_search.py. Reuses setup_train_test + pf_band_search (same pipeline / cost / split). Net of cost
@15 bps/leg. No live trades; writes PARAMETER_SWEEP_SUMMARY.md under the setup's approval-loop folder.

Run:
  py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/<SETUP>/scripts/param_sweep.py \
      --setup <SETUP> --pool <per-setup pool> --train_start 2026-05-18 --test_start 2026-06-20
"""
from __future__ import annotations

import argparse
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

SL_SWEEP = [0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00, 1.10, 1.20, 1.30, 1.50]
TGT_SWEEP = [0.60, 0.80, 1.00, 1.25, 1.50, 1.75, 2.00, 2.50, 3.00]
SWEEP_Q = [0.2, 0.4, 0.5, 0.6, 0.8]   # must be keys present in pf_band_search.build_quantiles (QGRID)
BAND_LO, BAND_HI = 1.30, 1.70
REGIME_OPTS = [("regime", "==", "NEUTRAL"), ("regime", "==", "TREND"),
               ("regime", "!=", "BEAR"), ("regime", "!=", "BULL")]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    ap.add_argument("--pool", required=True)
    ap.add_argument("--train_start", default="2026-05-18")
    ap.add_argument("--test_start", default="2026-06-20")
    ap.add_argument("--slippage_bps", type=float, default=15.0)
    ap.add_argument("--min_fold", type=int, default=6)
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass

    setup = args.setup.strip().upper()
    outdir = Path(args.out) if args.out else (TT_DIR / "setup_pf_1_4_approval_loop" / setup)
    outdir.mkdir(parents=True, exist_ok=True)

    tt.POOL_DIRS = [Path(args.pool)]; tt.POOL_DIR = Path(args.pool)
    pb._set_slip(args.slippage_bps)
    pool = tt.load_pool(); pool = pool[pool["setup"] == setup].copy()
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    ts, te = pd.Timestamp(args.train_start), pd.Timestamp(args.test_start)
    TEST_s = [s for s in sessions if s >= te]
    TRAIN_s = [s for s in sessions if ts <= s < (TEST_s[0] if TEST_s else sessions[-1] + pd.Timedelta(days=1))]
    if not TRAIN_s:
        print(f"[param-sweep] no TRAIN sessions for {setup} (pool {sessions[0].date()}..{sessions[-1].date()})"); return 1
    half = len(TRAIN_s) // 2
    FIT_s, VAL_s = TRAIN_s[:half], TRAIN_s[half:]
    span = set(map(pd.Timestamp, TRAIN_s + TEST_s))
    sub = tt.attach_entries(pool[pool["_day"].isin(span)].copy())

    def _sl(ss): return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    FIT, VAL, TRAIN = _sl(FIT_s), _sl(VAL_s), _sl(TRAIN_s)

    def _d(ss): return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}"
    print(f"[param-sweep] {setup}  FIT {_d(FIT_s)} ({len(FIT_s)})  VAL {_d(VAL_s)} ({len(VAL_s)})  "
          f"TRAIN {_d(TRAIN_s)} ({len(TRAIN_s)})  entries FIT={len(FIT)} VAL={len(VAL)}")

    card = pb.CARD_BASELINES.get(setup, {})
    base_sl = float(card.get("sl", 0.90)); base_tgt = float(card.get("tgt", 1.50))
    RAW = {"sl": base_sl, "tgt": base_tgt, "mask_terms": [], "premom_terms": [], "guard": None,
           "max_positions": 20, "daily_loss_rs": 0.0}
    mask_quant, pm_quant = pb.build_quantiles(TRAIN)

    def fv(cfg):
        nf, pf_f, _ = pb._light(setup, cfg, FIT)
        nv, pf_v, _ = pb._light(setup, cfg, VAL)
        return nf, round(pf_f, 3), nv, round(pf_v, 3)

    def best_stable(rows):
        """rows: list of (label, nf, pf_f, nv, pf_v). Stable = both folds >= min_fold trades AND
        min(FIT,VAL) PF >= BAND_LO. Return (best_label, best_minpf, stable_labels)."""
        ok = [(lbl, min(pf_f, pf_v)) for (lbl, nf, pf_f, nv, pf_v) in rows
              if nf >= args.min_fold and nv >= args.min_fold]
        if not ok:
            return None, None, []
        stable = [lbl for lbl, mp in ok if mp >= BAND_LO]
        best = max(ok, key=lambda x: x[1])
        return best[0], round(best[1], 3), stable

    sections = []   # (title, note, rows, best_line)

    def add_section(title, note, rows):
        bl, bp, stable = best_stable(rows)
        if bl is None:
            best_line = "**best stable range:** _none — no value kept >= min_fold trades on both folds_"
        elif not stable:
            best_line = (f"**best stable range:** _none reaches min(FIT,VAL) PF ≥ {BAND_LO}_ ; "
                         f"closest = `{bl}` (min-PF {bp})")
        else:
            best_line = f"**best stable range:** {', '.join('`'+s+'`' for s in stable)}  (best = `{bl}`, min-PF {bp})"
        sections.append((title, note, rows, best_line))
        print(f"  [{title}] {best_line}")

    # ---- exit SL (fix target at base) ----
    add_section(f"EXIT — stop-loss % (target fixed at {base_tgt})", "smaller↔wider SL; reject SL that bleeds VAL",
                [(f"SL={sl:g}", *fv({**RAW, "sl": sl})) for sl in SL_SWEEP])
    # ---- exit target (fix SL at base) ----
    add_section(f"EXIT — target % (SL fixed at {base_sl})", "smaller↔larger target; reject too-ambitious targets",
                [(f"Tgt={tg:g}", *fv({**RAW, "tgt": tg})) for tg in TGT_SWEEP])

    # ---- numeric mask features ----
    for f in pb.MASK_FEATS:
        if f not in mask_quant:
            continue
        rows = []
        for op in (">=", "<="):
            for q in SWEEP_Q:
                thr = round(float(mask_quant[f][q]), 6)
                rows.append((f"{f}{op}{thr} (q{q})", *fv({**RAW, "mask_terms": [[f, op, thr]]})))
        add_section(f"FILTER (mask) — {f}", "indicator/price-action filter; range across train quantiles", rows)

    # ---- pre-momentum features ----
    for f in pb.PM_FEATS:
        if f not in pm_quant:
            continue
        rows = []
        for op in (">=", "<="):
            for q in SWEEP_Q:
                thr = round(float(pm_quant[f][q]), 6)
                rows.append((f"{f}{op}{thr} (q{q})", *fv({**RAW, "premom_terms": [[f, op, thr]]})))
        add_section(f"PRE-MOMENTUM — {f}", "1-min pre-entry confirmation; range across train quantiles", rows)

    # ---- regime (categorical) ----
    add_section("FILTER — regime (categorical)", "don't-fight-the-tape regime filter",
                [(f"regime{op}{val}", *fv({**RAW, "mask_terms": [[ff, op, val]]})) for (ff, op, val) in REGIME_OPTS])

    # ---- guards ----
    add_section("GUARD — min_slot (entry not before)", "avoid early-session traps",
                [(f"min_slot={s}", *fv({**RAW, "guard": {"min_slot": s}})) for s in ["09:30", "09:45", "10:00", "10:30", "11:00"]])
    add_section("GUARD — max_slot (entry not after)", "avoid late-day low-quality entries",
                [(f"max_slot={s}", *fv({**RAW, "guard": {"max_slot": s}})) for s in ["12:00", "12:30", "13:00", "14:00", "14:30"]])
    add_section("GUARD — top_n (best N per slot by vwap_dist_atr)", "selectivity per signal slot",
                [(f"top_n={n}", *fv({**RAW, "guard": {"top_n": n}})) for n in [1, 2, 3]])

    # ---- write markdown ----
    md = [f"# PARAMETER_SWEEP_SUMMARY — {setup}", "",
          f"Stage-2 individual-knob range sweep, net of cost @ {args.slippage_bps:.0f} bps/leg. Each knob is varied "
          f"ONE at a time from a clean base (raw detection + card exit SL {base_sl}/Tgt {base_tgt}); everything else "
          "fixed. Optimised on FIT/VAL only. 'Stable' = both folds keep ≥ "
          f"{args.min_fold} trades AND min(FIT_PF, VAL_PF) ≥ {BAND_LO}.", "",
          f"- FIT {_d(FIT_s)} ({len(FIT_s)}) · VAL {_d(VAL_s)} ({len(VAL_s)}) · TRAIN {_d(TRAIN_s)} ({len(TRAIN_s)})",
          f"- entries @ {args.slippage_bps:.0f}bps: FIT={len(FIT)} VAL={len(VAL)}",
          f"- searchable mask={sorted(mask_quant)} | premom={sorted(pm_quant)}", ""]
    for title, note, rows, best_line in sections:
        md += [f"## {title}", f"_{note}_", "", best_line, "",
               "| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |", "|---|---:|---:|---:|---:|---:|"]
        for (lbl, nf, pf_f, nv, pf_v) in rows:
            mp = round(min(pf_f, pf_v), 3)
            flag = " ✅" if (nf >= args.min_fold and nv >= args.min_fold and mp >= BAND_LO) else ""
            md.append(f"| {lbl} | {nf} | {pf_f} | {nv} | {pf_v} | {mp}{flag} |")
        md.append("")
    (outdir / "PARAMETER_SWEEP_SUMMARY.md").write_text("\n".join(md), encoding="utf-8")
    print(f"[param-sweep] wrote {outdir/'PARAMETER_SWEEP_SUMMARY.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
