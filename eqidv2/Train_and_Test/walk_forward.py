"""walk_forward.py — multi-fold walk-forward validation (the honest judge).
=========================================================================

The fix for the single-window overfit trap. Instead of ONE train/test split,
roll many folds across ALL available history and require a setup's edge to hold
in MOST folds, not one lucky window. A config that only works in a single window
fails the cross-fold aggregate — which is exactly what we want.

Method (anchored / expanding):
  - For each fold: TRAIN = all history up to the fold start; TEST = the next
    `--test_weeks` weeks. Slide the fold start forward by `--step_weeks`.
  - On each fold's TRAIN we re-run the SAME band search (reusing
    setup_train_test.search_setup, strict knobs), then score it on that fold's
    unseen TEST (eval_family). Re-tuning per fold is deliberate: it measures
    whether the METHOD finds an edge that generalizes, fold after fold.

Robustness verdict (per setup):
  ROBUST  = >= --min_folds folds had test data AND >= 60% of them are test PF>=1.3
            AND median test PF >= 1.3
  FRAGILE = positive in some folds but not consistent
  DEAD    = rarely/never test-positive

Anti-overfit by construction + reuses the tested primitives. NO --approve.

Run AFTER market close (heavy: resolves 1-min entries across full history):
  py -3.12 Train_and_Test/walk_forward.py --pool_dir C:/TradingData/eqidv2/outputs_ID_v11_unified_pool \
     --setups A_MOD_BREAK_C1_LOW,B_HUGE_RED_FAILED_BOUNCE,C_OR_BREAKDOWN,D_AVWAP_LOSE_REVERSAL,... \
     [--test_weeks 4 --step_weeks 4 --min_train_weeks 12 --min_folds 4]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
for _p in (str(_HERE.parent), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt   # noqa: E402  (reuse load_pool/attach_entries/search_setup/eval_family)

DEFAULT_POOL = r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool"
OUT_DIR = _HERE / "walk_forward_results"
# Features banned from the search universe (the regime-band overfit vector).
BAN_FEATURES = {"market_ret_pct", "market_abs_ret_pct", "signal_minute", "notional"}


def _pf(net: np.ndarray) -> float:
    net = net[np.isfinite(net)]
    return tt.wfg._profit_factor(net) if len(net) else 0.0


def lean_load_pool(wanted: set[str]) -> pd.DataFrame:
    """Memory-lean pool load: stream the unified CSV in chunks and keep ONLY the
    wanted setups' rows BEFORE running the heavy feature derivation. Lets the
    walk-forward run without OOM (and without pressuring the live feed) during
    market hours, since it never holds the full 256k-row pool in memory."""
    one = tt.POOL_DIR / "historical_all_available_pre_dedupe_live_candidates.csv"
    if not one.exists():
        return tt.load_pool()  # fallback to the standard loader
    wU = {str(s).upper() for s in wanted}
    keep = []
    for chunk in pd.read_csv(one, low_memory=False, chunksize=50000):
        if "setup" not in chunk.columns:
            continue
        keep.append(chunk[chunk["setup"].astype(str).str.upper().isin(wU)])
    df = pd.concat(keep, ignore_index=True) if keep else pd.DataFrame()
    if df.empty:
        raise SystemExit(f"[walk_forward] no rows for {sorted(wanted)} in {one}")
    for c in ("ticker", "side", "setup", "signal_time_ist"):
        if c not in df.columns:
            df[c] = ""
    df = df.drop_duplicates(subset=["ticker", "side", "setup", "signal_time_ist"], keep="first").reset_index(drop=True)
    df["setup"] = df["setup"].astype(str).str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["tt_sig_ts"] = df["signal_time_ist"].map(tt.v11._normalise_ts)
    df = df.dropna(subset=["tt_sig_ts"]).copy()
    df["_day"] = df["tt_sig_ts"].dt.normalize().dt.tz_localize(None)
    df["_slot"] = df["tt_sig_ts"].map(tt.v11._fmt_ist)
    df = tt.v11._selected_strategy_features(df)
    return df


def make_folds(day_min: pd.Timestamp, day_max: pd.Timestamp, *, test_weeks: int,
               step_weeks: int, min_train_weeks: int) -> list[dict]:
    """Anchored expanding folds: train = [day_min, test_start-1d], test = test_weeks."""
    folds = []
    first_test = (day_min + pd.Timedelta(weeks=min_train_weeks)).normalize()
    ts = first_test
    while ts + pd.Timedelta(weeks=test_weeks) - pd.Timedelta(days=1) <= day_max:
        te = ts + pd.Timedelta(weeks=test_weeks) - pd.Timedelta(days=1)
        folds.append({"train_start": day_min, "train_end": ts - pd.Timedelta(days=1),
                      "test_start": ts, "test_end": te})
        ts = ts + pd.Timedelta(weeks=step_weeks)
    return folds


def walk_setup(setup: str, attached: pd.DataFrame, folds: list[dict], min_test: int) -> dict:
    """Run the per-fold re-tune+test for one setup. `attached` = entries already resolved."""
    rows = attached[attached["setup"] == setup]
    fold_recs = []
    for i, f in enumerate(folds):
        d = rows["_day"]
        tr = rows[(d >= f["train_start"]) & (d <= f["train_end"])]
        te = rows[(d >= f["test_start"]) & (d <= f["test_end"])]
        if len(tr) < tt.MIN_TRAIN_TRADES:
            continue
        cfg = tt.search_setup(setup, tr)
        if not cfg or cfg.get("status") != "OK":
            fold_recs.append({"fold": i, "test_start": str(f["test_start"].date()),
                              "train_pf": (cfg or {}).get("train_pf"), "test_pf": None,
                              "test_n": int(len(te)), "status": (cfg or {}).get("status", "NONE")})
            continue
        m = tt.eval_family({setup: cfg}, te) if len(te) else {"trades": 0, "net_pf": 0.0, "net_pnl": 0.0}
        fold_recs.append({
            "fold": i, "test_start": str(f["test_start"].date()),
            "train_pf": round(float(cfg["train_pf"]), 2), "train_n": int(cfg["train_n"]),
            "test_pf": round(float(m["net_pf"]), 2), "test_n": int(m["trades"]),
            "test_net": round(float(m["net_pnl"]), 0),
            "sl": cfg["sl"], "tgt": cfg["tgt"],
            "mask": [list(t) for t in cfg["mask_terms"]], "premom": [list(t) for t in cfg["premom_terms"]],
        })
    # aggregate over folds that actually had test trades
    ev = [r for r in fold_recs if r.get("test_pf") is not None and r["test_n"] >= min_test]
    n_ev = len(ev)
    pos = [r for r in ev if r["test_pf"] >= 1.3]
    med_test = float(np.median([r["test_pf"] for r in ev])) if ev else 0.0
    med_train = float(np.median([r["train_pf"] for r in ev])) if ev else 0.0
    frac_pos = (len(pos) / n_ev) if n_ev else 0.0
    return {"setup": setup, "folds_total": len(folds), "folds_evaluated": n_ev,
            "folds_test_pos": len(pos), "frac_test_pos": round(frac_pos, 2),
            "median_train_pf": round(med_train, 2), "median_test_pf": round(med_test, 2),
            "fold_detail": fold_recs}


def verdict(agg: dict, min_folds: int) -> str:
    if agg["folds_evaluated"] < min_folds:
        return "INSUFFICIENT_DATA"
    if agg["frac_test_pos"] >= 0.60 and agg["median_test_pf"] >= 1.30:
        return "ROBUST"
    if agg["frac_test_pos"] >= 0.40 or agg["median_test_pf"] >= 1.10:
        return "FRAGILE"
    return "DEAD"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool_dir", default=DEFAULT_POOL)
    ap.add_argument("--setups", default="", help="comma list of setups (required unless --family)")
    ap.add_argument("--family", default="", help="family prefix instead of --setups")
    ap.add_argument("--test_weeks", type=int, default=4)
    ap.add_argument("--step_weeks", type=int, default=4)
    ap.add_argument("--min_train_weeks", type=int, default=12)
    ap.add_argument("--min_folds", type=int, default=4, help="min evaluated folds to render a verdict")
    ap.add_argument("--min_test", type=int, default=6, help="min test trades for a fold to count")
    ap.add_argument("--max_mask_terms", type=int, default=2)
    ap.add_argument("--max_premom_terms", type=int, default=1)
    ap.add_argument("--min_train_trades", type=int, default=50)
    ap.add_argument("--train_pf_min", type=float, default=1.4,
                    help="lower edge of the train PF band; keep modest and trade-rich")
    ap.add_argument("--train_pf_max", type=float, default=1.7,
                    help="upper preference for the train PF band")
    ap.add_argument("--min_minhalf_pf", type=float, default=1.1)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    # ---- configure the shared search machinery (strict, regime-band banned) ----
    tt.POOL_DIRS = [Path(args.pool_dir)]
    tt.POOL_DIR = Path(args.pool_dir)
    tt.OBJECTIVE = "band"
    tt.MAX_MASK_TERMS = int(args.max_mask_terms)
    tt.MAX_PREMOM_TERMS = int(args.max_premom_terms)
    tt.MIN_TRAIN_TRADES = int(args.min_train_trades)
    tt.TRAIN_PF_MIN = float(args.train_pf_min)
    tt.TRAIN_PF_MAX = float(args.train_pf_max)
    tt.KEEP_MIN_MINHALF_PF = float(args.min_minhalf_pf)
    tt.QUANTILES = tt.FINE_QUANTILES
    tt.SIGNAL_FEATURES = [f for f in tt.SIGNAL_FEATURES if f not in BAN_FEATURES]
    tt.PREMOM_FEATURES = [f for f in tt.PREMOM_FEATURES if f not in BAN_FEATURES]

    want = [s.strip() for s in args.setups.split(",") if s.strip()]
    if args.family and not want:
        # need the full pool to enumerate a family; fall back to standard load
        _full = tt.load_pool()
        want = [s for s in sorted(_full["setup"].astype(str).unique())
                if s.startswith(args.family.upper() + "_") and s not in tt.EXCLUDED]
        pool = _full[_full["setup"].astype(str).isin(want)].copy()
    elif want:
        pool = lean_load_pool(set(want))   # memory-lean: only these setups' rows
    else:
        raise SystemExit("[walk_forward] no setups selected (use --setups or --family)")

    day_min, day_max = pool["_day"].min(), pool["_day"].max()
    folds = make_folds(day_min, day_max, test_weeks=args.test_weeks,
                       step_weeks=args.step_weeks, min_train_weeks=args.min_train_weeks)
    print(f"[walk_forward] data {day_min.date()}..{day_max.date()} | {len(folds)} folds "
          f"(test={args.test_weeks}w step={args.step_weeks}w min_train={args.min_train_weeks}w)")
    print(f"[walk_forward] setups={len(want)} | banned={sorted(BAN_FEATURES)} "
          f"mask<= {tt.MAX_MASK_TERMS} premom<= {tt.MAX_PREMOM_TERMS} "
          f"target_pf={tt.TRAIN_PF_MIN}..{tt.TRAIN_PF_MAX}\n")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    hdr = f"{'setup':34} | {'folds':>5} | {'tested':>6} | {'%pos':>5} | {'med_train':>9} | {'med_test':>8} | verdict"
    print(hdr); print("-" * len(hdr))
    summary = []
    for s in want:
        attached = tt.attach_entries(pool[pool["setup"] == s].copy())
        agg = walk_setup(s, attached, folds, args.min_test)
        v = verdict(agg, args.min_folds)
        agg["verdict"] = v
        summary.append({k: agg[k] for k in agg if k != "fold_detail"} | {"verdict": v})
        (OUT_DIR / f"{s}.json").write_text(json.dumps(agg, indent=2, default=str), encoding="utf-8")
        print(f"{s:34} | {agg['folds_total']:5d} | {agg['folds_evaluated']:6d} | "
              f"{agg['frac_test_pos']*100:4.0f}% | {agg['median_train_pf']:9.2f} | "
              f"{agg['median_test_pf']:8.2f} | {v}")

    (OUT_DIR / "_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print("-" * len(hdr))
    rob = [r["setup"] for r in summary if r["verdict"] == "ROBUST"]
    print(f"ROBUST (holds across folds): {len(rob)}/{len(summary)} -> {rob}")
    print(f"results: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
