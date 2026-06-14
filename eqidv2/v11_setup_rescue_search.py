"""
v11_setup_rescue_search.py
==========================

Honest, out-of-sample search for whether the v11 losing setups can be rescued by
a single indicator / non-indicator threshold — WITHOUT in-sample curve fitting.

For each target setup it:
  1. Establishes the no-filter OOS baseline (net-of-cost, global-calendar folds).
  2. For every candidate feature, fits the best single threshold on TRAIN ONLY
     (walkforward_gate.make_threshold_fitter), scores it OUT-OF-SAMPLE net-of-cost,
     and runs a day-clustered (block) bootstrap on the OOS-selected book.
  3. Flags the overfit trap (strong in-sample / weak OOS) and reports a verdict.

A feature "rescues" a setup only if, OUT OF SAMPLE, it lifts net PF above the bar,
stays fold-consistent, is day-block significant, and does NOT trip the overfit
flag. Anything that only looks good in-sample is reported as NOT a rescue — which
is exactly the failure mode this guards against.

This is research only: it writes a report and recommends rules. It does not edit
the v11 backtester. Encode survivors into avwap_5min_ID_v11_backtesting.py
explicitly, with the OOS justification noted inline.

Usage:
  python v11_setup_rescue_search.py
  python v11_setup_rescue_search.py --train_days 40 --test_days 15
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

import walkforward_gate as wfg
from nse_intraday_costs import CostConfig
from v11_gate_report import _load_and_map, FEATURE_COLS, _block_bootstrap_p, DEFAULT_TRADES


# The six losing setups to interrogate (from the Jan-Jun gross book).
TARGET_SETUPS = [
    "E_VWAP_LOSE_EARLY_SHORT",
    "A_MOD_BREAK_C1_LOW",
    "E_ORB_BREAKOUT_SHORT",
    "D_EMA20_BOUNCE",
    "L_BB_SQUEEZE_LONG",
    "A_MOD_CLOSE_CONTINUATION_BREAK",
]

# Rescue acceptance bar (OOS, net-of-cost).
MIN_OOS_TRADES = 30
MIN_NET_PF_OOS = 1.20
MIN_FOLD_CONSISTENCY = 0.55
MAX_DAY_BLOCK_P = 0.10


def _net(df: pd.DataFrame, cfg: CostConfig) -> np.ndarray:
    return wfg.net_pnl_vectorized(
        df["entry_price"].to_numpy(), df["exit_price"].to_numpy(),
        df["qty"].to_numpy(), df["side"].to_numpy(), cfg,
    )


def _wf_eval(sdf: pd.DataFrame, feature: str | None, fold_days, wf, cfg,
             n_boot: int, seed: int, min_keep: int = 10) -> dict:
    """Walk-forward evaluate a setup, optionally under a TRAIN-fitted threshold on
    `feature` (None = no filter baseline). Returns OOS net-of-cost metrics +
    day-clustered bootstrap on the OOS-selected book."""
    if feature is None:
        fit = wfg.no_fit
        apply = wfg.apply_identity
    else:
        fit, apply = wfg.make_threshold_fitter(feature, cfg, min_keep=min_keep)

    by_day = {d: g for d, g in sdf.groupby(pd.to_datetime(sdf["date"]).dt.normalize())}
    oos_net, oos_dates, is_net, fold_sums, params = [], [], [], [], []

    for tr_days, te_days in wfg.purged_walk_forward_folds(fold_days, wf):
        train = (pd.concat([by_day[d] for d in tr_days if d in by_day], ignore_index=True)
                 if any(d in by_day for d in tr_days) else sdf.iloc[0:0])
        test = (pd.concat([by_day[d] for d in te_days if d in by_day], ignore_index=True)
                if any(d in by_day for d in te_days) else sdf.iloc[0:0])
        if len(train) == 0 or len(test) == 0:
            continue
        p = fit(train)
        params.append(p)
        sel_te = apply(test, p)
        sel_tr = apply(train, p)
        if len(sel_te):
            n = _net(sel_te, cfg)
            oos_net.append(n)
            oos_dates.append(pd.to_datetime(sel_te["date"]).dt.normalize().to_numpy())
            fold_sums.append(float(n.sum()))
        if len(sel_tr):
            is_net.append(_net(sel_tr, cfg))

    oos = np.concatenate(oos_net) if oos_net else np.array([])
    ins = np.concatenate(is_net) if is_net else np.array([])
    dates = np.concatenate(oos_dates) if oos_dates else np.array([], dtype="datetime64[ns]")

    n_oos = int(len(oos))
    pf_oos = wfg._profit_factor(oos) if n_oos else 0.0
    pf_is = wfg._profit_factor(ins) if len(ins) else 0.0
    fc = (sum(1 for s in fold_sums if s > 0) / len(fold_sums)) if fold_sums else 0.0
    if n_oos:
        daily = pd.Series(oos, index=pd.DatetimeIndex(dates)).groupby(level=0).sum()
        dbp = _block_bootstrap_p(daily.to_numpy(), n_boot, seed)
    else:
        dbp = float("nan")
    ratio = pf_oos / pf_is if (pf_is > 1.0 and np.isfinite(pf_oos)) else float("nan")
    overfit = bool(np.isfinite(ratio) and ratio < 0.60 and pf_is >= 1.3)

    rule = ""
    real = [p for p in params if p]
    if real and feature is not None:
        direction = pd.Series([p["direction"] for p in real]).mode().iloc[0]
        thr = float(np.median([p["threshold"] for p in real]))
        rule = f"{feature} {direction} {thr:.4g}"

    return dict(
        feature=feature or "(no filter)", rule=rule, n_oos=n_oos,
        net_pf_oos=round(pf_oos, 3), total_net_oos=round(float(oos.sum()) if n_oos else 0.0, 1),
        fold_consistency=round(fc, 3), net_pf_is=round(pf_is, 3),
        oos_is_ratio=round(ratio, 3) if np.isfinite(ratio) else np.nan,
        overfit_flag=overfit, day_block_p=round(dbp, 4) if np.isfinite(dbp) else np.nan,
        n_folds=len(fold_sums),
    )


def _is_rescue(r: dict) -> bool:
    return (r["n_oos"] >= MIN_OOS_TRADES
            and np.isfinite(r["net_pf_oos"]) and r["net_pf_oos"] >= MIN_NET_PF_OOS
            and r["fold_consistency"] >= MIN_FOLD_CONSISTENCY
            and not r["overfit_flag"]
            and np.isfinite(r["day_block_p"]) and r["day_block_p"] <= MAX_DAY_BLOCK_P)


def main() -> int:
    ap = argparse.ArgumentParser(description="Honest OOS rescue search for v11 losing setups")
    ap.add_argument("--trades", type=str, default=str(DEFAULT_TRADES))
    ap.add_argument("--out_dir", type=str, default="")
    ap.add_argument("--train_days", type=int, default=40)
    ap.add_argument("--test_days", type=int, default=15)
    ap.add_argument("--embargo_days", type=int, default=1)
    ap.add_argument("--n_bootstrap", type=int, default=10000)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    trades_path = Path(args.trades)
    out_dir = Path(args.out_dir) if args.out_dir else trades_path.parent
    cfg = CostConfig()
    df = _load_and_map(trades_path)
    fold_days = sorted(pd.to_datetime(df["date"]).dt.normalize().unique())
    wf = wfg.WalkForwardConfig(
        train_days=args.train_days, test_days=args.test_days, embargo_days=args.embargo_days,
        n_bootstrap=args.n_bootstrap, random_seed=args.seed, global_calendar_folds=True,
    )

    feats_all = [c for c in FEATURE_COLS if c in df.columns]
    print("=" * 100)
    print("v11 SETUP RESCUE SEARCH — honest OOS (net-of-cost, global-calendar WF, day-clustered bootstrap)")
    print(f"  trades  : {trades_path}")
    print(f"  WF      : train={wf.train_days} test={wf.test_days} embargo={wf.embargo_days} "
          f"global_calendar_folds={wf.global_calendar_folds}")
    print(f"  bar     : OOS n>={MIN_OOS_TRADES}, net_pf_oos>={MIN_NET_PF_OOS}, "
          f"fold_consistency>={MIN_FOLD_CONSISTENCY}, day_block_p<={MAX_DAY_BLOCK_P}, not overfit")
    print(f"  features: {', '.join(feats_all)}")
    print("=" * 100)

    all_rows: list[dict] = []
    summary_rows: list[dict] = []
    for setup in TARGET_SETUPS:
        sdf = df[df["setup"] == setup].reset_index(drop=True)
        if sdf.empty:
            print(f"\n### {setup}: no trades in book — skip")
            continue
        n_days = sdf["date"].nunique()
        base = _wf_eval(sdf, None, fold_days, wf, cfg, args.n_bootstrap, args.seed)
        base["setup"] = setup
        base["candidate"] = "BASELINE_no_filter"
        all_rows.append(base)

        results = []
        for feat in feats_all:
            col = sdf[feat]
            if col.notna().sum() < 20 or col.nunique(dropna=True) < 5:
                continue
            r = _wf_eval(sdf, feat, fold_days, wf, cfg, args.n_bootstrap, args.seed)
            r["setup"] = setup
            r["candidate"] = feat
            results.append(r)
            all_rows.append(r)

        rescuers = [r for r in results if _is_rescue(r)]
        rescuers.sort(key=lambda r: (r["net_pf_oos"], r["total_net_oos"]), reverse=True)
        best = rescuers[0] if rescuers else (
            max(results, key=lambda r: (r["net_pf_oos"], r["total_net_oos"])) if results else None)

        print(f"\n### {setup}  (book: {len(sdf)} trades / {n_days} days)")
        print(f"  baseline (no filter): n_oos={base['n_oos']} net_pf_oos={base['net_pf_oos']} "
              f"total_net_oos=Rs {base['total_net_oos']:,} fold_consistency={base['fold_consistency']} "
              f"day_block_p={base['day_block_p']}")
        if not results:
            print("  -> insufficient feature variation / trades to fit any threshold OOS  => NO RESCUE (block)")
            verdict = "INSUFFICIENT_DATA_BLOCK"
        elif rescuers:
            print(f"  -> {len(rescuers)} feature(s) clear the OOS bar. Best:")
            for r in rescuers[:3]:
                print(f"       {r['rule']:<34} n_oos={r['n_oos']:>3} net_pf_oos={r['net_pf_oos']:>5} "
                      f"net=Rs {r['total_net_oos']:>8,} fc={r['fold_consistency']} "
                      f"p={r['day_block_p']} is_ratio={r['oos_is_ratio']}")
            verdict = f"RESCUE: {best['rule']}"
        else:
            print("  -> NO feature clears the OOS bar. Best in-sample-looking candidate (does NOT survive):")
            if best:
                print(f"       {best['rule']:<34} n_oos={best['n_oos']:>3} net_pf_oos={best['net_pf_oos']:>5} "
                      f"net=Rs {best['total_net_oos']:>8,} fc={best['fold_consistency']} "
                      f"p={best['day_block_p']} overfit={best['overfit_flag']}")
            verdict = "NO_RESCUE_BLOCK"

        summary_rows.append({
            "setup": setup, "book_trades": len(sdf), "book_days": n_days,
            "baseline_n_oos": base["n_oos"], "baseline_net_pf_oos": base["net_pf_oos"],
            "baseline_net_oos": base["total_net_oos"],
            "n_features_tested": len(results), "n_rescuers": len(rescuers),
            "verdict": verdict,
            "best_rule": best["rule"] if best else "",
            "best_net_pf_oos": best["net_pf_oos"] if best else np.nan,
            "best_day_block_p": best["day_block_p"] if best else np.nan,
        })

    detail = pd.DataFrame(all_rows)
    summary = pd.DataFrame(summary_rows)
    detail_path = out_dir / "v11_setup_rescue_detail.csv"
    summary_path = out_dir / "v11_setup_rescue_summary.csv"
    detail.to_csv(detail_path, index=False)
    summary.to_csv(summary_path, index=False)

    print("\n" + "=" * 100)
    print("VERDICT SUMMARY")
    print("-" * 100)
    print(summary[["setup", "book_trades", "book_days", "baseline_net_pf_oos",
                   "n_rescuers", "verdict"]].to_string(index=False))
    print("-" * 100)
    print(f"  wrote {summary_path}")
    print(f"  wrote {detail_path}")
    print("=" * 100)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
