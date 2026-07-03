"""aggressive_pf_tuner.py — DEEP, multi-iteration search pushing TRAIN PF as high as
ROBUSTLY possible (target > 2) for each conf setup, honestly reporting TEST.

Aggression vs setup_train_test (standard tuner), all reusing its tested primitives
(search_setup / _greedy_maxobj / _robust_obj / resolve / eval_family):

  * PREMOM universe  : ALL 40 pre-entry features (vs the 8 wired) — the main lever.
  * MASK universe    : the full signal-feature set + any numeric pool columns.
  * DEEPER gates     : up to --max-mask (default 4) mask + --max-premom (3) premom terms.
  * FINER thresholds : denser quantile grid (5%..95% step 5%).
  * FINER SL/TGT     : 8 x 7 grid.
  * MULTI-START      : --restarts random feature-order restarts per setup. The greedy is
                       order-sensitive, so restarts escape local optima -> keep the best
                       robust train PF.
  * OBJECTIVE        : worse-of-two-train-halves PF (robust; NOT raw in-sample PF) =
                       maxpf, so PF is pushed high but only where it holds across both halves.

Per setup it reports train PF / test PF / trades + a verdict:
  ROBUST  = train PF >= target AND test PF >= 1.30 AND oos/is >= 0.55 AND test n >= 5
  OVERFIT = train PF >= target but test fails (the honest "looked great, didn't hold")
  BELOW   = could not reach the target robustly
Per-setup checkpoint JSON (resumable). NO --approve — review only.

Run AFTER market close:
  py -3.12 Train_and_Test\aggressive_pf_tuner.py [--restarts 6] [--target-pf 2.0]
        [--min-trades 20] [--max-mask 4] [--max-premom 3] [--max-secs-per-setup 1200]
        [--setups A_MOD_BREAK_C1_LOW,...]
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
for _p in (str(_HERE.parent), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt          # noqa: E402  (reuse the tested search machinery)
import final_setup_conf as fc          # noqa: E402
import eqidv2_final_conf_live_bootstrap as boot   # noqa: E402  (readmit vs native basis)

DEFAULT_POOL = r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool"
OUT_DIR = _HERE / "aggressive_pf_proposals"

# Full pre-entry-momentum universe (from v11._pre_entry_momentum_features_v11).
PREMOM_ALL = [
    "pre1_adx", "pre1_body_r", "pre1_close_pos", "pre1_dir", "pre1_mom_r", "pre1_range_r", "pre1_rsi_dir",
    "pre2_mom_r",
    "pre3_body_sum_r", "pre3_close_pos", "pre3_dir_count", "pre3_mom_r", "pre3_range_r", "pre3_vol_ratio20",
    "pre5_body_sum_r", "pre5_close_pos", "pre5_dir_count", "pre5_mom_r", "pre5_range_r", "pre5_vol_ratio20",
    "pre10_body_sum_r", "pre10_close_pos", "pre10_dir_count", "pre10_mom_r", "pre10_range_r", "pre10_vol_ratio20",
    "pre15_body_sum_r", "pre15_close_pos", "pre15_dir_count", "pre15_mom_r", "pre15_range_r", "pre15_vol_ratio20",
    "pre_entry_momentum_score", "pre_bars",
    "sig5_adx_calc", "sig5_body_r", "sig5_close_pos", "sig5_range_r", "sig5_rsi_dir", "sig5_vol_ratio20",
]


def _mask_universe(pool: pd.DataFrame) -> list[str]:
    uni = list(dict.fromkeys(tt.SIGNAL_FEATURES))
    skip = {"tt_fill", "tt_qty", "notional", "signal_open", "signal_high", "signal_low",
            "signal_close", "signal_volume", "quantity"}
    for c in pool.columns:
        if c in uni or c in skip:
            continue
        col = pd.to_numeric(pool[c], errors="coerce")
        if col.notna().mean() > 0.5 and col.nunique() > 5:
            uni.append(c)
    return uni


def _cfg_of(best: dict) -> dict:
    return {"sl": best["sl"], "tgt": best["tgt"], "mask_terms": best["mask_terms"],
            "premom_terms": best["premom_terms"], "guard": best["guard"], "status": "OK"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool_dir", default=DEFAULT_POOL)
    ap.add_argument("--setups", default="", help="comma list (default: all conf setups)")
    ap.add_argument("--restarts", type=int, default=6)
    ap.add_argument("--target-pf", type=float, default=2.0)
    ap.add_argument("--min-trades", type=int, default=20)
    ap.add_argument("--max-mask", type=int, default=4)
    ap.add_argument("--max-premom", type=int, default=3)
    ap.add_argument("--max-secs-per-setup", type=float, default=1200.0)
    ap.add_argument("--max-candidates", type=int, default=8000,
                    help="cap train rows per setup (sample if larger) to bound the premom search")
    ap.add_argument("--seed", type=int, default=12345)
    # Optional window pinning (default = the dynamic train_test_window). Use when the
    # pool's data ends before 'today' so the holdout lands on days that actually have
    # candidates (otherwise the TEST tail is empty / too thin).
    ap.add_argument("--train_start", default="")
    ap.add_argument("--train_end", default="")
    ap.add_argument("--test_start", default="")
    ap.add_argument("--test_end", default="")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    random.seed(args.seed)

    # --- aggressive global overrides on the shared tuner ---
    tt.POOL_DIRS = [Path(args.pool_dir)]
    tt.POOL_DIR = Path(args.pool_dir)
    tt.OBJECTIVE = "maxpf"
    tt.FORCE_PREMOM = True
    tt.MAX_MASK_TERMS = int(args.max_mask)
    tt.MAX_PREMOM_TERMS = int(args.max_premom)
    tt.MIN_TRAIN_TRADES = int(args.min_trades)
    tt.QUANTILES = [round(float(q), 2) for q in np.arange(0.05, 0.96, 0.05)]
    # SL drives the per-sl premom recompute (the cost); keep it to 5. TGT is cheap (resolve only).
    tt.SL_GRID = (0.5, 0.7, 0.9, 1.1, 1.3)
    tt.TGT_GRID = (0.6, 0.8, 1.0, 1.2, 1.5, 2.0, 2.5)
    if args.train_start or args.train_end:
        tt.TRAIN = (args.train_start or tt.TRAIN[0], args.train_end or tt.TRAIN[1])
    if args.test_start or args.test_end:
        tt.TEST = (args.test_start or tt.TEST[0], args.test_end or tt.TEST[1])

    want = set(fc.FINAL_SETUP_CONF)
    if args.setups:
        want = {s.strip() for s in args.setups.split(",") if s.strip()}

    print(f"[aggressive] window TRAIN {tt.TRAIN[0]}..{tt.TRAIN[1]}  TEST {tt.TEST[0]}..{tt.TEST[1]}")
    print(f"[aggressive] objective=maxpf target_pf={args.target_pf} min_trades={args.min_trades} "
          f"max_mask={args.max_mask} max_premom={args.max_premom} restarts={args.restarts} "
          f"quantiles={len(tt.QUANTILES)} sl|tgt={len(tt.SL_GRID)}x{len(tt.TGT_GRID)}")

    pool = tt.load_pool()
    pool = pool[pool["setup"].isin(want)].copy()
    tr, te = tt.split_train_test(pool)
    tr = tt.attach_entries(tr)
    te = tt.attach_entries(te)
    base_signal = _mask_universe(pool)
    base_premom = list(dict.fromkeys(PREMOM_ALL))
    print(f"[aggressive] resolved train={len(tr)} test={len(te)} | mask universe={len(base_signal)} "
          f"premom universe={len(base_premom)}\n")

    readmit = set(boot.readmit_setups())   # native* (not in readmit) = raw/ungated pool -> not live-faithful
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    hdr = f"{'setup':34} | {'basis':7} | {'TRAIN pf/n':>12} | {'TEST pf/n':>11} | {'oos/is':>6} | verdict"
    print(hdr); print("-" * len(hdr))
    rows: list[dict] = []
    for setup in sorted(want):
        basis = "readmit" if setup in readmit else "native*"
        tr_s = tr[tr["setup"] == setup]
        sampled = len(tr_s) > args.max_candidates
        if sampled:
            tr_s = tr_s.sample(args.max_candidates, random_state=args.seed)
        t0 = time.time()
        best = None
        for _ in range(max(1, args.restarts)):
            if time.time() - t0 > args.max_secs_per_setup:
                break
            tt.SIGNAL_FEATURES = random.sample(base_signal, len(base_signal))
            tt.PREMOM_FEATURES = random.sample(base_premom, len(base_premom))
            cfg = tt.search_setup(setup, tr_s)
            if not cfg or cfg.get("status") in ("NO_VIABLE_CONFIG", "TOO_FEW_TRAIN"):
                continue
            if best is None or cfg["train_pf"] > best["train_pf"]:
                best = cfg
        if best is None:
            rec = {"setup": setup, "basis": basis, "verdict": "NO_CONFIG", "secs": round(time.time() - t0, 0)}
            rows.append(rec); (OUT_DIR / f"{setup}.json").write_text(json.dumps(tt._json_sanitize(rec), indent=2, default=str))
            print(f"{setup:34} | {basis:7} | {'-':>12} | {'-':>11} | {'-':>6} | NO_CONFIG")
            continue
        tem = tt.eval_family({setup: _cfg_of(best)}, te)
        train_pf, train_n = float(best["train_pf"]), int(best["train_n"])
        test_pf, test_n = float(tem.get("net_pf", 0.0)), int(tem.get("trades", 0))
        ratio = (test_pf / train_pf) if train_pf > 0 else 0.0
        if train_pf >= args.target_pf and test_pf >= 1.30 and ratio >= 0.55 and test_n >= 5:
            verdict = "ROBUST"
        elif train_pf >= args.target_pf:
            verdict = "OVERFIT"
        else:
            verdict = "BELOW_TARGET"
        rec = {
            "setup": setup, "basis": basis, "verdict": verdict, "sampled": sampled,
            "train_pf": round(train_pf, 2), "train_n": train_n, "train_net": round(float(best["train_net"]), 0),
            "test_pf": round(test_pf, 2), "test_n": test_n, "test_net": round(float(tem.get("net_pnl", 0.0)), 0),
            "oos_is_ratio": round(ratio, 2),
            "sl": best["sl"], "tgt": best["tgt"], "guard": best["guard"],
            "mask_terms": best["mask_terms"], "premom_terms": best["premom_terms"],
            "secs": round(time.time() - t0, 0),
        }
        rows.append(rec)
        (OUT_DIR / f"{setup}.json").write_text(json.dumps(tt._json_sanitize(rec), indent=2, default=str))
        print(f"{setup:34} | {basis:7} | {train_pf:6.2f}/{train_n:<4d} | {test_pf:6.2f}/{test_n:<3d} | "
              f"{ratio:6.2f} | {verdict}  ({rec['secs']:.0f}s)")

    (OUT_DIR / "_summary.json").write_text(json.dumps(tt._json_sanitize(rows), indent=2, default=str), encoding="utf-8")
    robust = [r for r in rows if r.get("verdict") == "ROBUST"]
    print("-" * len(hdr))
    print(f"ROBUST (train>= {args.target_pf} AND test holds): {len(robust)}/{len(rows)} -> "
          f"{sorted(r['setup'] for r in robust)}")
    print(f"proposals: {OUT_DIR}  (review only — nothing written to final_setup_conf.py)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
