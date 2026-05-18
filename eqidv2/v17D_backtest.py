"""v17D backtest — one-command honest production pipeline.

Wraps an existing v17C-runner trades CSV and pushes it through the full
v17D evaluation chain:

    1. Drop disabled setups (from DROP_SETUPS)
    2. Enrich features (di_plus/minus, atr_pct_rank_60d)
    3. Re-resolve exits with MAE/MFE-recommended SL/TGT
    4. Apply Pareto-derived per-setup filters
    5. Apply per-row realistic ADV-bucketed cost model
    6. Apply universe filter (drop ADV < Rs 50 cr long_tail)
    7. Emit frozen production CSV + per-setup summary

This is NOT a signal-generating backtester — it consumes the v17C runner's
trades CSV. The v17C runner remains the source of truth for signal
detection + governors + sizing. v17D adds the honest-cost evaluation layer.

Usage:
    python -m eqidv2.v17D_backtest \
        --trades       outputs_v17C_noNF_live_5min/avwap_..._candE3.csv \
        --universe     eqidv2/configs/universe.csv \
        --mae-mfe      eqidv2/outputs_v17D_phase0/mae_mfe_v17C_candE4_summary.csv \
        --indicator-dir C:/TradingData/eqidv2/stocks_indicators_5min_eq_live2 \
        --daily-dir     C:/TradingData/eqidv2/stocks_indicators_daily_eq \
        --onemin-dir    C:/TradingData/eqidv2/stocks_indicators_1min_eq \
        --output-dir   eqidv2/outputs_v17D_backtest/<run-tag>

Frozen v17D-r1 production config:
    - Drop SHORT G_LOWER_LOW_BREAK
    - Pareto rule: LONG B_HUGE_C1_CLOSE_RECLAIM_BREAK requires atr_pct_rank_60d >= 0.30
    - Per-row realistic costs from v17D_cost_model
    - Universe: ADV >= Rs 50 cr (top100 + mid only)
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eqidv2 import v17D_cost_model as cm
from eqidv2 import v17D_exit_resolver as er
from eqidv2 import v17D_enrich_features as enrich_mod
from eqidv2 import v17D_reresolve_with_mae_mfe as reresolve_mod
from eqidv2 import v17D_apply_realistic_costs as cost_mod

# ---------------------------------------------------------------------------
# v17D-r1 production config — frozen from 2026-05-13 run
# ---------------------------------------------------------------------------
DROP_SETUPS = {
    ("SHORT", "G_LOWER_LOW_BREAK"),
    ("SHORT", "C_OR_BREAKDOWN"),
    ("SHORT", "D_EMA20_REJECTION"),
    ("SHORT", "C_OR_BREAKOUT"),
}

# Pareto-derived filter rules (Step 2.0 output for v17D-r1)
PARETO_FILTERS = {
    ("LONG", "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): [
        ("atr_pct_rank_60d", ">=", 0.30),
    ],
}

# Universe filter — drop long_tail ADV bucket (< Rs 50 cr)
KEEP_ADV_BUCKETS = {"top100", "mid"}


def _pf(s: pd.Series) -> float:
    s = pd.to_numeric(s, errors="coerce").dropna()
    w, l = float(s[s > 0].sum()), float(-s[s < 0].sum())
    if l <= 0:
        return float("inf") if w > 0 else 0.0
    return w / l


def step1_drop_setups(df: pd.DataFrame) -> pd.DataFrame:
    n_in = len(df)
    df = df.copy()
    df["side"] = df["side"].astype(str).str.upper()
    drop_mask = df.apply(
        lambda r: (r["side"], r["setup"]) in DROP_SETUPS, axis=1
    )
    n_dropped = int(drop_mask.sum())
    out = df[~drop_mask].copy()
    print(f"[1/6] Drop disabled setups: {n_in} -> {len(out)} (dropped {n_dropped})")
    for (side, setup) in DROP_SETUPS:
        n = int(((df["side"] == side) & (df["setup"] == setup)).sum())
        if n:
            print(f"        - {side} {setup}: {n} trades removed")
    return out


def step2_enrich(df: pd.DataFrame, indicator_dir: Path, daily_dir: Path) -> pd.DataFrame:
    print(f"[2/6] Enrich features (DI+/-, atr_pct_rank_60d) ...")
    return enrich_mod.enrich(df, indicator_dir, daily_dir)


def step3_reresolve(df: pd.DataFrame, picks: pd.DataFrame, onemin_dir: Path,
                    leverage: float) -> pd.DataFrame:
    print(f"[3/6] Re-resolve with MAE/MFE SL/TGT picks ...")
    return reresolve_mod.reresolve(df, picks, onemin_dir, leverage=leverage)


def step4_pareto(df: pd.DataFrame) -> pd.DataFrame:
    n_in = len(df)
    keep_mask = pd.Series(True, index=df.index)
    dropped = 0
    for (side, setup), rules in PARETO_FILTERS.items():
        mask = (df["side"] == side) & (df["setup"] == setup)
        sub = df[mask]
        for feat, op, thr in rules:
            if feat not in sub.columns:
                continue
            col = pd.to_numeric(sub[feat], errors="coerce")
            rule = (col >= thr) if op == ">=" else (col <= thr)
            rule = rule.fillna(True)
            dropped += int((~rule).sum())
            keep_mask.loc[sub.index] &= rule
    out = df[keep_mask].copy()
    print(f"[4/6] Apply Pareto filters: {n_in} -> {len(out)} (dropped {dropped})")
    for (side, setup), rules in PARETO_FILTERS.items():
        rules_str = ", ".join(f"{f} {o} {t}" for f, o, t in rules)
        print(f"        - {side} {setup}: requires {rules_str}")
    return out


def step5_costs(df: pd.DataFrame, universe: pd.DataFrame, leverage: float) -> pd.DataFrame:
    print(f"[5/6] Apply per-row realistic costs (v17D_cost_model) ...")
    return cost_mod.apply_costs(df, universe, leverage=leverage)


def step6_universe(df: pd.DataFrame) -> pd.DataFrame:
    n_in = len(df)
    mask = df["adv_bucket"].isin(KEEP_ADV_BUCKETS)
    n_dropped = int((~mask).sum())
    out = df[mask].copy()
    print(f"[6/6] Universe filter (keep {sorted(KEEP_ADV_BUCKETS)}): "
          f"{n_in} -> {len(out)} (dropped {n_dropped} long_tail)")
    return out


def emit_summary(df: pd.DataFrame, out_dir: Path) -> str:
    if df.empty:
        return "EMPTY"

    pf = _pf(df["pnl_pct_eff_net"])
    win = (df["outcome"] == "TARGET").mean() * 100
    df = df.copy()
    df["d"] = pd.to_datetime(df["trade_date"]).dt.date
    n_days = max(df["d"].nunique(), 1)
    daily = df.groupby("d")["pnl_pct_eff_net"].sum()
    day_win = (daily > 0).mean() * 100
    sum_rs = float(pd.to_numeric(df["pnl_rs_eff_net"], errors="coerce").sum())

    lines = []
    sep = "=" * 78
    lines.append(sep)
    lines.append("v17D HONEST PRODUCTION (realistic costs, sizing-aware, ADV>=Rs50cr)")
    lines.append(sep)
    lines.append(f"  n_trades        = {len(df)}")
    lines.append(f"  n_days          = {n_days}")
    lines.append(f"  trades/day      = {len(df)/n_days:.2f}")
    lines.append(f"  PF              = {pf:.3f}")
    lines.append(f"  win %           = {win:.2f}")
    lines.append(f"  day-win %       = {day_win:.2f}")
    lines.append(f"  sum_pnl_rs_eff  = Rs {sum_rs:>+12,.0f}")
    lines.append("")

    # Verdict bands per Phase 0 exit decision
    if pf >= 1.50:
        verdict = "GREEN — proceed to Phase 1 build"
    elif pf >= 1.30:
        verdict = "YELLOW — proceed at 0.3x pilot size"
    elif pf >= 1.20:
        verdict = "YELLOW-LOW — re-tune before any live capital"
    else:
        verdict = "RED — strategy edge is largely a costs illusion; STOP"
    lines.append(f"  Verdict: {verdict}")
    lines.append("")

    # Count-floor check
    per_day = len(df) / n_days
    cf = "PASS" if per_day >= 4.5 else f"FAIL (need >=4.5/day, got {per_day:.2f})"
    lines.append(f"  Count floor (>=4.5/day): {cf}")
    lines.append("")
    lines.append("Per-setup breakdown:")
    for (side, setup), g in df.groupby(["side", "setup"]):
        pf_g = _pf(g["pnl_pct_eff_net"])
        win_g = (g["outcome"] == "TARGET").mean() * 100
        rs_g = float(pd.to_numeric(g["pnl_rs_eff_net"], errors="coerce").sum())
        lines.append(
            f"  {side:5s} {setup:<32s}  n={len(g):<5d}  "
            f"PF={pf_g:.3f}  win%={win_g:5.2f}  Rs={rs_g:>+11,.0f}"
        )

    lines.append("")
    lines.append("Per-ADV-bucket breakdown:")
    for bucket, g in df.groupby("adv_bucket"):
        pf_b = _pf(g["pnl_pct_eff_net"])
        rs_b = float(pd.to_numeric(g["pnl_rs_eff_net"], errors="coerce").sum())
        lines.append(
            f"  {bucket:<10s}  n={len(g):<5d}  PF={pf_b:.3f}  Rs={rs_b:>+11,.0f}"
        )

    text = "\n".join(lines)
    (out_dir / "summary.txt").write_text(text, encoding="utf-8")
    return text


# Canonical v17D-r1 defaults — match today's frozen production run.
# Override any of these by passing CLI flags; or rely on them when running
# via Spyder runfile() / IDE "Run File" with no arguments.
_REPO_ROOT = Path(__file__).resolve().parents[1]
_RUNTIME = Path(r"C:\TradingData\eqidv2")
DEFAULT_TRADES = str(_RUNTIME / "outputs_v17C_noNF_live_5min" /
    "avwap_longshort_trades_v16_5min_ALL_DAYS_20260505_080858_candE3.csv")
DEFAULT_UNIVERSE = str(_REPO_ROOT / "eqidv2" / "configs" / "universe.csv")
DEFAULT_MAE_MFE = str(_REPO_ROOT / "eqidv2" / "outputs_v17D_phase0" /
                       "mae_mfe_v17C_candE4_summary.csv")
DEFAULT_INDICATOR_DIR = str(_RUNTIME / "stocks_indicators_5min_eq_live2")
DEFAULT_DAILY_DIR = str(_RUNTIME / "stocks_indicators_daily_eq")
DEFAULT_ONEMIN_DIR = str(_RUNTIME / "stocks_indicators_1min_eq")
DEFAULT_OUTPUT_DIR = str(_REPO_ROOT / "eqidv2" / "outputs_v17D_backtest" /
                          datetime.now().strftime("run_%Y%m%d_%H%M%S"))


def main():
    p = argparse.ArgumentParser(
        description="v17D honest production backtest (post-processes v17C trades)"
    )
    p.add_argument("--trades", default=DEFAULT_TRADES,
                   help=f"v17C runner trades CSV (default: {DEFAULT_TRADES})")
    p.add_argument("--universe", default=DEFAULT_UNIVERSE,
                   help="universe.csv with ticker, last_close, adv_rs_cr")
    p.add_argument("--mae-mfe", default=DEFAULT_MAE_MFE,
                   help="MAE/MFE summary CSV with recommended_sl_pct/tgt_pct")
    p.add_argument("--indicator-dir", default=DEFAULT_INDICATOR_DIR,
                   help="5min indicator parquet dir")
    p.add_argument("--daily-dir", default=DEFAULT_DAILY_DIR,
                   help="daily indicator parquet dir")
    p.add_argument("--onemin-dir", default=DEFAULT_ONEMIN_DIR,
                   help="1min OHLC parquet dir")
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--leverage", type=float, default=5.0)
    p.add_argument("--keep-intermediates", action="store_true",
                   help="write per-step CSVs (not just final)")
    args = p.parse_args()

    # Validate the resolved paths exist before doing real work
    missing = []
    for label, path in [
        ("--trades", args.trades), ("--universe", args.universe),
        ("--mae-mfe", args.mae_mfe), ("--indicator-dir", args.indicator_dir),
        ("--daily-dir", args.daily_dir), ("--onemin-dir", args.onemin_dir),
    ]:
        if not Path(path).exists():
            missing.append(f"  {label}  {path}")
    if missing:
        raise SystemExit("Inputs missing — pass CLI args or place files at "
                         "canonical defaults:\n" + "\n".join(missing))

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 78)
    print(f"v17D BACKTEST — honest production pipeline   {ts}")
    print("=" * 78)
    print(f"  trades:        {args.trades}")
    print(f"  universe:      {args.universe}")
    print(f"  MAE/MFE picks: {args.mae_mfe}")
    print(f"  output:        {out_dir}")
    print()

    df = pd.read_csv(args.trades)
    universe = pd.read_csv(args.universe)
    picks = pd.read_csv(args.mae_mfe)
    ind_dir = Path(args.indicator_dir)
    d_dir = Path(args.daily_dir)
    om_dir = Path(args.onemin_dir)

    print(f"  Input trades: {len(df)}")
    print()

    df = step1_drop_setups(df)
    if args.keep_intermediates:
        df.to_csv(out_dir / f"step1_dropped_{ts}.csv", index=False)

    df = step2_enrich(df, ind_dir, d_dir)
    if args.keep_intermediates:
        df.to_csv(out_dir / f"step2_enriched_{ts}.csv", index=False)

    df = step3_reresolve(df, picks, om_dir, args.leverage)
    if args.keep_intermediates:
        df.to_csv(out_dir / f"step3_reresolved_{ts}.csv", index=False)

    df = step4_pareto(df)
    if args.keep_intermediates:
        df.to_csv(out_dir / f"step4_pareto_{ts}.csv", index=False)

    df = step5_costs(df, universe, args.leverage)
    if args.keep_intermediates:
        df.to_csv(out_dir / f"step5_costed_{ts}.csv", index=False)

    df = step6_universe(df)

    final_csv = out_dir / f"v17D_production_{ts}.csv"
    df.to_csv(final_csv, index=False)
    print(f"\nWrote final: {final_csv}  rows={len(df)}")

    print()
    text = emit_summary(df, out_dir)
    print(text)
    print(f"\nSummary written to: {out_dir / 'summary.txt'}")


if __name__ == "__main__":
    main()
