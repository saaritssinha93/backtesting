"""
Round-4 diagnostic: find cells that improve all six axes simultaneously.

Six-axis objective (user, 2026-04-21):
  1. trades/day up
  2. total trades up (both L and S, especially S)
  3. PF up
  4. MaxDD down
  5. win rate up
  6. total PnL up

Approach
--------
Cross-tabulate every trade from the production v17g CSV (plus v17b for richer
SHORT sample) across these axes:

  side x nifty_context_mode x setup x time_bucket x quality_score_bucket

For each cell compute: n, PF, win%, sum_pnl, avg_pnl, sl_rate.

Output three rankings:

  A. "add throughput" cells — cells where v17b had trades but v17g filtered them
     out. Rank by (PF x sum_pnl) — if PF >= 1.8 these are CANDIDATES to re-admit.

  B. "DD culprits" — cells contributing disproportionately to v17g's 37.89% DD.
     Rank by contribution to max-loss streaks.

  C. "marginal cost" cells — cells with PF in [1.0, 1.3]. These pay costs
     without meaningfully contributing edge. Candidates for removal.

The three rankings converge on a C11 candidate: v17g + add-throughput cells
(from A, filtered by B/C) + regime-gate drops (from B/C).

Usage
-----
  python -m eqidv2.research.diag_round4 \
      --v17g <v17g.csv> \
      --v17b <v17b.csv> \
      --outdir <dir>
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd

from .core import compute_metrics, load_trades

# Standard intraday buckets (IST)
TIME_BUCKETS: List[Tuple[str, int, int]] = [
    ("0915-1000", 9 * 60 + 15, 10 * 60),
    ("1000-1100", 10 * 60, 11 * 60),
    ("1100-1200", 11 * 60, 12 * 60),
    ("1200-1300", 12 * 60, 13 * 60),
    ("1300-1400", 13 * 60, 14 * 60),
    ("1400-1520", 14 * 60, 15 * 60 + 20),
]

QS_BUCKETS = [
    ("<50", -np.inf, 50),
    ("50-60", 50, 60),
    ("60-70", 60, 70),
    ("70-80", 70, 80),
    (">=80", 80, np.inf),
]


def _label_time(m: int) -> str:
    for name, lo, hi in TIME_BUCKETS:
        if lo <= m < hi:
            return name
    return "other"


def _label_qs(q: float) -> str:
    if pd.isna(q):
        return "nan"
    for name, lo, hi in QS_BUCKETS:
        if lo <= q < hi:
            return name
    return "other"


def _cell_metrics(df: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    g = df.groupby(keys, dropna=False)
    rows = []
    for key_vals, sub in g:
        pnl = sub["pnl_pct"].astype(float)
        n = len(sub)
        pos = pnl[pnl > 0].sum()
        neg = -pnl[pnl < 0].sum()
        pf = float(pos / neg) if neg > 0 else (float("inf") if pos > 0 else 0.0)
        outcome = sub["outcome"].astype(str).str.upper()
        win = 100.0 * (outcome == "TARGET").sum() / n
        sl = 100.0 * (outcome == "SL").sum() / n
        sum_pnl = float(pnl.sum())
        avg = float(pnl.mean())
        row = dict(zip(keys, key_vals if isinstance(key_vals, tuple) else (key_vals,)))
        row.update(n=n, pf=round(pf, 3) if pf != float("inf") else 999.0,
                   win_pct=round(win, 2), sl_pct=round(sl, 2),
                   sum_pnl=round(sum_pnl, 2), avg_pnl=round(avg, 4))
        rows.append(row)
    return pd.DataFrame(rows).sort_values("sum_pnl", ascending=False).reset_index(drop=True)


def _prep(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["time_bucket"] = df["entry_minute"].apply(_label_time)
    df["qs_bucket"] = df["quality_score"].astype(float).apply(_label_qs) if "quality_score" in df.columns else "n/a"
    df["setup_u"] = df["setup"].astype(str).str.upper().str.strip()
    df["regime"] = df["nifty_context_mode"].astype(str).str.upper().str.strip() if "nifty_context_mode" in df.columns else "n/a"
    df["side_u"] = df["side"].astype(str).str.upper().str.strip()
    return df


def _anti_join(big: pd.DataFrame, small: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
    """Rows in `big` with identity tuple NOT present in `small`."""
    b = big.set_index(keys)
    s = small.set_index(keys)
    return big.loc[~b.index.isin(s.index)].copy()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--v17g", type=Path, required=True)
    p.add_argument("--v17b", type=Path, required=True,
                   help="v17b reference for SHORT throughput analysis (richer sample)")
    p.add_argument("--outdir", type=Path, required=True)
    args = p.parse_args()

    args.outdir.mkdir(parents=True, exist_ok=True)

    g = _prep(load_trades(args.v17g))
    b = _prep(load_trades(args.v17b))

    print(f"v17g: {len(g)} trades ({len(g[g.side_u=='LONG'])} L / {len(g[g.side_u=='SHORT'])} S) over {g['trade_day'].nunique()} days")
    print(f"v17b: {len(b)} trades ({len(b[b.side_u=='LONG'])} L / {len(b[b.side_u=='SHORT'])} S) over {b['trade_day'].nunique()} days")

    # ---------------------------------------------------------------
    # BASELINE METRICS
    # ---------------------------------------------------------------
    mg = compute_metrics(g).as_row()
    print(f"\n=== V17G BASELINE ===")
    print(f"  n={mg['n_trades']}  L={mg['n_long']}  S={mg['n_short']}  "
          f"PF={mg['profit_factor']:.3f}  DD={mg['max_drawdown_pct']:.2f}%  "
          f"Win={mg['win_rate']:.2f}%  PnL={mg['sum_pnl_pct']:.2f}%  "
          f"trades/day={mg['trades_per_day']:.2f}  Sharpe={mg['sharpe_ann']:.2f}")

    # ---------------------------------------------------------------
    # A. REGIME x SIDE x SETUP x TIME matrix on v17g
    # ---------------------------------------------------------------
    print("\n=== A. V17G CELL MATRIX (regime x side x setup x time) ===")
    mat_g = _cell_metrics(g, ["regime", "side_u", "setup_u", "time_bucket"])
    mat_g.to_csv(args.outdir / "r4_cell_matrix_v17g.csv", index=False)
    top_cells = mat_g.sort_values("pf", ascending=False).head(15)
    print("  Top 15 cells by PF (v17g):")
    print(top_cells.to_string(index=False))
    print(f"\n  [wrote {args.outdir / 'r4_cell_matrix_v17g.csv'}]")

    # ---------------------------------------------------------------
    # B. THROUGHPUT CANDIDATES — v17b trades NOT in v17g (filtered out)
    # ---------------------------------------------------------------
    print("\n=== B. THROUGHPUT CANDIDATES (v17b trades filtered by v17g stack) ===")
    keys = ["signal_time_ist", "ticker", "side_u", "setup_u"]
    # keep only trades in v17b whose (signal_time, ticker, side, setup) is not in v17g
    filtered_out = _anti_join(b, g, keys)
    print(f"  v17b has {len(b)} trades, v17g has {len(g)} — v17b-minus-v17g = {len(filtered_out)} filtered trades")

    # What are those filtered trades by side?
    fo_long = filtered_out[filtered_out["side_u"] == "LONG"]
    fo_short = filtered_out[filtered_out["side_u"] == "SHORT"]
    print(f"  filtered LONGs: {len(fo_long)}  (expected AMCC ~= 111)")
    print(f"  filtered SHORTs: {len(fo_short)} (expected 0 — v17g is superset on SHORT)")

    if len(fo_long) > 0:
        fo_mat = _cell_metrics(fo_long, ["regime", "setup_u", "time_bucket"])
        fo_mat.to_csv(args.outdir / "r4_filtered_out_long_cells.csv", index=False)
        print("  Top filtered-LONG cells by PF:")
        print(fo_mat.sort_values("pf", ascending=False).head(10).to_string(index=False))
        print(f"  [wrote {args.outdir / 'r4_filtered_out_long_cells.csv'}]")

    # ---------------------------------------------------------------
    # C. SHORT THROUGHPUT ANALYSIS on v17g (where are our shorts?)
    # ---------------------------------------------------------------
    print("\n=== C. V17G SHORT DISTRIBUTION (165 trades) ===")
    s = g[g.side_u == "SHORT"]
    short_cells = _cell_metrics(s, ["regime", "setup_u", "time_bucket"])
    short_cells.to_csv(args.outdir / "r4_short_cells_v17g.csv", index=False)
    print("  All SHORT cells, sorted by PF:")
    print(short_cells.sort_values("pf", ascending=False).head(20).to_string(index=False))

    # SHORT days breakdown
    s_days = s.groupby("trade_day").size()
    print(f"\n  Days with SHORT trades    : {len(s_days)}/{g['trade_day'].nunique()} = {100*len(s_days)/g['trade_day'].nunique():.1f}%")
    print(f"  Avg SHORTs per trading day: {len(s)/g['trade_day'].nunique():.2f}")
    print(f"  Avg SHORTs per SHORT day  : {s_days.mean():.2f}")
    print(f"  Max SHORTs in a day       : {s_days.max()}")

    # ---------------------------------------------------------------
    # D. DD CULPRIT CELLS — where does v17g's 37.89% DD come from?
    # ---------------------------------------------------------------
    print("\n=== D. DD CULPRIT CELLS (contribution to v17g loss streaks) ===")
    # Sort by entry_time_ist (CSV order proxy), compute rolling DD attribution
    g_sorted = g.sort_values("entry_time_ist").reset_index(drop=True)
    equity = g_sorted["pnl_pct"].cumsum()
    peak = equity.cummax()
    dd_series = peak - equity
    max_dd_end = dd_series.idxmax()
    # Find start of this drawdown: last peak index before max_dd_end
    peak_val_at_end = peak.iloc[max_dd_end]
    max_dd_start_candidates = g_sorted.index[(equity == peak_val_at_end) & (g_sorted.index <= max_dd_end)]
    max_dd_start = int(max_dd_start_candidates.min()) if len(max_dd_start_candidates) else 0
    dd_trades = g_sorted.iloc[max_dd_start:max_dd_end + 1]
    print(f"  Max-DD window: trade {max_dd_start} -> {max_dd_end} ({len(dd_trades)} trades, "
          f"{dd_trades['trade_day'].min().date()} -> {dd_trades['trade_day'].max().date()})")
    print(f"  DD magnitude : {dd_series.max():.2f}%")

    dd_mat = _cell_metrics(dd_trades, ["side_u", "setup_u", "regime"])
    dd_mat = dd_mat.sort_values("sum_pnl").reset_index(drop=True)
    dd_mat.to_csv(args.outdir / "r4_dd_culprits.csv", index=False)
    print("  Cells contributing most loss during DD window:")
    print(dd_mat.head(10).to_string(index=False))
    print(f"  [wrote {args.outdir / 'r4_dd_culprits.csv'}]")

    # ---------------------------------------------------------------
    # E. MARGINAL COST CELLS (PF in [1.0, 1.3])
    # ---------------------------------------------------------------
    print("\n=== E. MARGINAL CELLS in v17g (PF in [1.0, 1.3], candidates for removal) ===")
    marg = mat_g[(mat_g["pf"] >= 1.0) & (mat_g["pf"] <= 1.3) & (mat_g["n"] >= 8)]
    marg = marg.sort_values("n", ascending=False)
    marg.to_csv(args.outdir / "r4_marginal_cells.csv", index=False)
    print(f"  {len(marg)} marginal cells (PF 1.0-1.3, n>=8):")
    print(marg.head(15).to_string(index=False))
    print(f"  [wrote {args.outdir / 'r4_marginal_cells.csv'}]")

    # ---------------------------------------------------------------
    # F. HIGH-PF UNDERSUPPLIED CELLS (PF >= 2.5 but n <= 20 in v17g)
    # ---------------------------------------------------------------
    print("\n=== F. HIGH-PF UNDERSUPPLIED CELLS (PF>=2.0, n<=20) ===")
    thin = mat_g[(mat_g["pf"] >= 2.0) & (mat_g["n"] <= 20) & (mat_g["n"] >= 4)]
    thin = thin.sort_values("pf", ascending=False)
    thin.to_csv(args.outdir / "r4_high_pf_thin_cells.csv", index=False)
    print(f"  {len(thin)} thin high-PF cells:")
    print(thin.head(15).to_string(index=False))
    print(f"  [wrote {args.outdir / 'r4_high_pf_thin_cells.csv'}]")

    # ---------------------------------------------------------------
    # G. SUMMARY — what does this imply for C11?
    # ---------------------------------------------------------------
    print("\n=== G. SYNTHESIS — C11 CANDIDATE DIRECTIONS ===")
    marg_n = int(marg["n"].sum()) if len(marg) else 0
    marg_pnl = float(marg["sum_pnl"].sum()) if len(marg) else 0.0
    print(f"  DROP marginal cells  : would remove {marg_n} trades, {marg_pnl:+.2f}% PnL (should be near-zero)")
    print(f"  DD window attribution : {len(dd_trades)} trades over "
          f"{(dd_trades['trade_day'].max() - dd_trades['trade_day'].min()).days+1} days contributed {dd_series.max():.2f}% DD")
    print(f"  SHORT undersupply    : {len(s)}/{g['trade_day'].nunique()} = {len(s)/g['trade_day'].nunique():.2f}/day — "
          f"target 0.5+/day needs +{max(0, int(0.5*g['trade_day'].nunique())-len(s))} shorts")

    print("\n[OK] diagnostic run complete")


if __name__ == "__main__":
    main()
