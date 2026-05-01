"""
Validation harness for v17r research filters proposed by Codex.

Decides whether the rules below are real signal or curve-fit noise, BEFORE
running a multi-hour full-pipeline backtest.

It does four things on the latest v17q ALL_DAYS trades CSV:

  1. Baseline PF on full / train / test / post-Feb halves (no filter).
  2. Apply Codex's exact thresholds, report PF on each half.
  3. Re-mine the best threshold on the train half only, apply to test --
     if test PF beats baseline, the *idea* is robust even if the specific
     numbers shift.
  4. Sensitivity: wiggle each Codex threshold by +/-10% on the test half.
  5. Per-month PF, baseline vs Codex-filtered.

Train cutoff is 2026-01-31 (so test is roughly post-Feb, matching the user's
existing validation convention).

Usage:
  python _v17r_validate.py
  python _v17r_validate.py --csv <path>
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_CSV = (
    r"C:/TradingData/eqidv2/outputs_v17q_5min/"
    r"avwap_longshort_trades_v16_5min_ALL_DAYS_20260427_115710.csv"
)
PNL_COL = "pnl_pct"
TRAIN_END = "2026-01-31"

# Codex's proposed causal thresholds.
# (side, setup, column, op, threshold)
CODEX_RULES = [
    ("LONG",  "G_HIGHER_HIGH_BREAK",   "quality_score",          "<", 2.257),
    ("LONG",  "C_OR_BREAKOUT",         "quality_score",          "<", 1.485),
    ("LONG",  "A_MOD_BREAK_C1_HIGH",   "avwap_dist_atr_signal",  "<", 1.539),
    ("SHORT", "G_LOWER_LOW_BREAK",     "ema20_gap_atr_signal",   ">", 2.944),
]


def pf(pnl: pd.Series) -> float:
    pnl = pd.to_numeric(pnl, errors="coerce").dropna()
    if pnl.empty:
        return float("nan")
    gain = pnl[pnl > 0].sum()
    loss = -pnl[pnl < 0].sum()
    if loss <= 0:
        return float("inf")
    return float(gain / loss)


def stats(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"n": 0, "pf": float("nan"), "sum_pnl": 0.0, "win_rate": float("nan")}
    pnl = pd.to_numeric(df[PNL_COL], errors="coerce")
    return {
        "n": len(df),
        "pf": pf(pnl),
        "sum_pnl": float(pnl.sum()),
        "win_rate": float((pnl > 0).mean()),
    }


def fmt(s: dict) -> str:
    return (
        f"n={s['n']:>5}  PF={s['pf']:.3f}  "
        f"sumPnL={s['sum_pnl']:+8.1f}%  win={s['win_rate']*100:5.2f}%"
    )


def apply_drop_rule(df: pd.DataFrame, rule: tuple) -> pd.DataFrame:
    side, setup, col, op, thr = rule
    if col not in df.columns or "setup" not in df.columns or "side" not in df.columns:
        return df
    setup_norm = df["setup"].astype(str).str.upper().str.strip()
    side_norm  = df["side"].astype(str).str.upper().str.strip()
    vals = pd.to_numeric(df[col], errors="coerce")
    in_target = side_norm.eq(side) & setup_norm.eq(setup)
    if op == "<":
        drop = in_target & (vals < thr)
    elif op == ">":
        drop = in_target & (vals > thr)
    else:
        raise ValueError(op)
    return df.loc[~drop.fillna(False)].copy()


def apply_all_rules(df: pd.DataFrame, rules: list[tuple]) -> pd.DataFrame:
    out = df
    for r in rules:
        out = apply_drop_rule(out, r)
    return out


def split_train_test(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    d = pd.to_datetime(df["trade_date"], errors="coerce")
    cutoff = pd.Timestamp(TRAIN_END)
    return df.loc[d <= cutoff].copy(), df.loc[d > cutoff].copy()


# ---- mining: find best threshold for one rule on a slice ----
def mine_threshold(
    slice_df: pd.DataFrame,
    side: str,
    setup: str,
    col: str,
    op: str,
    *,
    min_keep_frac: float = 0.40,
    grid: int = 41,
) -> tuple[float | None, dict]:
    """
    Sweep candidate thresholds across the empirical column distribution for
    rows matching (side, setup). Pick the threshold that maximises PF of the
    KEPT rows for that (side, setup) bucket, as long as we keep at least
    min_keep_frac of the bucket.
    """
    if "setup" not in slice_df.columns or "side" not in slice_df.columns or col not in slice_df.columns:
        return None, {"reason": "missing column"}
    setup_norm = slice_df["setup"].astype(str).str.upper().str.strip()
    side_norm  = slice_df["side"].astype(str).str.upper().str.strip()
    bucket = slice_df.loc[side_norm.eq(side) & setup_norm.eq(setup)].copy()
    if bucket.empty:
        return None, {"reason": "empty bucket", "n": 0}
    vals = pd.to_numeric(bucket[col], errors="coerce")
    bucket = bucket.loc[vals.notna()].copy()
    if bucket.empty:
        return None, {"reason": "no numeric values", "n": 0}
    quantiles = np.linspace(0.05, 0.95, grid)
    candidates = sorted(set(vals.dropna().quantile(quantiles).round(4).tolist()))
    n_total = len(bucket)
    best = (None, -np.inf, None)
    for thr in candidates:
        if op == "<":
            kept = bucket.loc[~(pd.to_numeric(bucket[col], errors="coerce") < thr)]
        else:
            kept = bucket.loc[~(pd.to_numeric(bucket[col], errors="coerce") > thr)]
        if len(kept) / n_total < min_keep_frac:
            continue
        s = stats(kept)
        if np.isfinite(s["pf"]) and s["pf"] > best[1]:
            best = (thr, s["pf"], s)
    if best[0] is None:
        return None, {"reason": "no threshold met min_keep_frac", "n": n_total}
    return best[0], {"pf_kept_bucket": best[1], "n_bucket": n_total, "stats": best[2]}


# ---- sensitivity ----
def sensitivity(df: pd.DataFrame, rules: list[tuple], pct: float = 0.10) -> pd.DataFrame:
    rows = []
    base = apply_all_rules(df, rules)
    base_s = stats(base)
    rows.append({"variant": "codex_exact", "pf": base_s["pf"], "n": base_s["n"], "sum_pnl": base_s["sum_pnl"]})
    for i, r in enumerate(rules):
        side, setup, col, op, thr = r
        for sign in (-1, +1):
            new_thr = thr * (1 + sign * pct)
            new_rules = list(rules)
            new_rules[i] = (side, setup, col, op, new_thr)
            s = stats(apply_all_rules(df, new_rules))
            rows.append({
                "variant": f"{side[:1]}.{setup[:18]} {op} {thr:.3f} -> {new_thr:.3f}",
                "pf": s["pf"], "n": s["n"], "sum_pnl": s["sum_pnl"],
            })
    return pd.DataFrame(rows)


# ---- per-month PF ----
def per_month_pf(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    d = pd.to_datetime(df["trade_date"], errors="coerce")
    pnl = pd.to_numeric(df[PNL_COL], errors="coerce")
    out = pd.DataFrame({"month": d.dt.to_period("M").astype(str), "pnl": pnl})
    g = out.groupby("month")["pnl"]
    return pd.DataFrame({
        "n": g.size(),
        "pf": g.apply(lambda s: pf(s)),
        "sum_pnl": g.sum().round(1),
    })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=DEFAULT_CSV)
    args = ap.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"ERROR: csv not found: {csv_path}", file=sys.stderr)
        sys.exit(2)
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows from {csv_path.name}")
    print(f"trade_date range: {df['trade_date'].min()} -> {df['trade_date'].max()}")
    print(f"PnL column: {PNL_COL}")
    print(f"Train cutoff: <= {TRAIN_END}    (test = > cutoff)\n")

    train, test = split_train_test(df)
    print(f"split: train={len(train)}  test={len(test)}\n")

    # ---- Section 1: baseline ----
    print("=" * 78)
    print("SECTION 1 -- BASELINE (no filter)")
    print("=" * 78)
    print(f"  full  : {fmt(stats(df))}")
    print(f"  train : {fmt(stats(train))}")
    print(f"  test  : {fmt(stats(test))}")
    print()

    # ---- Section 2: Codex thresholds applied ----
    print("=" * 78)
    print("SECTION 2 -- CODEX EXACT THRESHOLDS")
    print("=" * 78)
    for r in CODEX_RULES:
        side, setup, col, op, thr = r
        print(f"  drop {side:5} {setup:24} {col:24} {op} {thr}")
    print()
    f_full  = apply_all_rules(df,    CODEX_RULES)
    f_train = apply_all_rules(train, CODEX_RULES)
    f_test  = apply_all_rules(test,  CODEX_RULES)
    print(f"  full  : {fmt(stats(f_full))}")
    print(f"  train : {fmt(stats(f_train))}")
    print(f"  test  : {fmt(stats(f_test))}    <-- the OOS-ish number that matters")
    print()

    base_test_pf = stats(test)["pf"]
    codex_test_pf = stats(f_test)["pf"]
    delta = codex_test_pf - base_test_pf
    print(f"  TEST PF: baseline={base_test_pf:.3f}  codex={codex_test_pf:.3f}  delta={delta:+.3f}")
    if delta < 0.05:
        print("  >>> Codex thresholds add <0.05 PF on test. Likely overfit to train.")
    elif delta < 0.15:
        print("  >>> Marginal lift on test. Borderline — re-mine and check.")
    else:
        print("  >>> Healthy lift on test. Worth a full pipeline run.")
    print()

    # ---- Section 3: re-mine on train only, apply to test ----
    print("=" * 78)
    print("SECTION 3 -- RE-MINE THRESHOLDS ON TRAIN, APPLY TO TEST")
    print("=" * 78)
    remined: list[tuple] = []
    for side, setup, col, op, codex_thr in CODEX_RULES:
        thr, info = mine_threshold(train, side, setup, col, op)
        if thr is None:
            print(f"  {side:5} {setup:24} {col:24} {op}: SKIP ({info.get('reason')})")
            continue
        print(
            f"  {side:5} {setup:24} {col:24} {op} "
            f"codex={codex_thr:.3f}  remined={thr:.3f}  "
            f"(bucket n={info['n_bucket']}, kept-bucket PF={info['pf_kept_bucket']:.3f})"
        )
        remined.append((side, setup, col, op, thr))
    print()
    if remined:
        rm_test = apply_all_rules(test, remined)
        rm_test_pf = stats(rm_test)["pf"]
        print(f"  remined-on-train applied to TEST: {fmt(stats(rm_test))}")
        print(f"  TEST PF: baseline={base_test_pf:.3f}  remined={rm_test_pf:.3f}  "
              f"delta={rm_test_pf - base_test_pf:+.3f}")
        if rm_test_pf - base_test_pf > 0.10 and codex_test_pf - base_test_pf > 0.10:
            print("  >>> Both Codex AND re-mined improve TEST. The IDEA is robust.")
        elif rm_test_pf - base_test_pf > 0.10:
            print("  >>> Re-mined helps but Codex specific thresholds don't. "
                  "Codex thresholds are overfit; the per-setup rule structure is fine.")
        else:
            print("  >>> Even re-mined-on-train fails on test. The rule structure itself is suspect.")
    print()

    # ---- Section 4: sensitivity (+/-10%) on test ----
    print("=" * 78)
    print("SECTION 4 -- +/-10% SENSITIVITY OF CODEX THRESHOLDS ON TEST HALF")
    print("=" * 78)
    sens = sensitivity(test, CODEX_RULES, pct=0.10)
    print(sens.to_string(index=False))
    pf_min = sens["pf"].min()
    pf_max = sens["pf"].max()
    print(f"\n  TEST PF range under +/-10% wiggle: [{pf_min:.3f}, {pf_max:.3f}]")
    if pf_min < base_test_pf:
        print("  >>> Some +/-10% wiggles push TEST PF BELOW baseline. Knife-edge fit.")
    print()

    # ---- Section 5: per-month PF ----
    print("=" * 78)
    print("SECTION 5 -- PER-MONTH PF (baseline vs codex-filtered)")
    print("=" * 78)
    base_m = per_month_pf(df).rename(columns={"n": "n_base", "pf": "pf_base", "sum_pnl": "sum_base"})
    codex_m = per_month_pf(f_full).rename(columns={"n": "n_codex", "pf": "pf_codex", "sum_pnl": "sum_codex"})
    merged = base_m.join(codex_m, how="outer").fillna({"n_codex": 0, "pf_codex": np.nan, "sum_codex": 0})
    merged["delta_pf"] = merged["pf_codex"] - merged["pf_base"]
    print(merged.round(3).to_string())
    helped = (merged["delta_pf"] > 0).sum()
    hurt   = (merged["delta_pf"] < 0).sum()
    print(f"\n  months helped by codex: {helped}    hurt: {hurt}")
    if hurt > helped:
        print("  >>> Codex filter hurts more months than it helps. "
              "Lift may be coming from killing a few bad months only.")
    print()

    # ---- final verdict ----
    print("=" * 78)
    print("VERDICT")
    print("=" * 78)
    verdict_score = 0
    if codex_test_pf - base_test_pf >= 0.15:
        verdict_score += 2
    elif codex_test_pf - base_test_pf >= 0.05:
        verdict_score += 1
    if remined and (stats(apply_all_rules(test, remined))["pf"] - base_test_pf) >= 0.10:
        verdict_score += 1
    if pf_min >= base_test_pf:
        verdict_score += 1
    if helped > hurt:
        verdict_score += 1
    print(f"  Robustness score: {verdict_score} / 5")
    if verdict_score >= 4:
        print("  -> GO. Run the full v17q pipeline with EQIDV17R_PROFILE=causal.")
    elif verdict_score >= 2:
        print("  -> MAYBE. Lift is partial; re-mine with looser min_keep_frac and re-check.")
    else:
        print("  -> NO-GO. Don't burn hours on a full backtest.")


if __name__ == "__main__":
    main()
