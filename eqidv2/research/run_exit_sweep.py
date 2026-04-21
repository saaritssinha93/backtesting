"""
Exit-policy sweep on the top round-2 candidate sets.

Re-resolves each trade's exit against the 5-min parquet universe using an
alternative (sl_pct, tgt_pct, time_stop, breakeven, trailing) configuration.
The *entry* signals, ticker, and entry time are unchanged — only the exit path
is recomputed with no lookahead.

Design note:
  - Keeps the same LEVERAGE=5x and round-trip cost (0.16% on notional) as prod.
  - Ambiguous-bar rule assumes SL-first (prod convention).

Because this is slower (one 5-min parquet read per ticker, cached), we only
sweep exits on the two best filter candidates from round-2.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from .core import compute_metrics, rank_variants
from .exits import ExitPolicy, resolve_exits
from .experiments_v2 import build_candidates

RESULTS_DIR = Path(__file__).resolve().parent / "results"


def _policies() -> List[Tuple[str, ExitPolicy]]:
    """Focused grid — 10 policies across SL/TGT/RR, time-stop, BE, trail."""
    out = []
    out.append(("P00_baseline_0.75_1.00", ExitPolicy(sl_pct=0.0075, tgt_pct=0.0100)))
    out.append(("P01_0.60_1.00",           ExitPolicy(sl_pct=0.0060, tgt_pct=0.0100)))
    out.append(("P02_0.75_1.25",           ExitPolicy(sl_pct=0.0075, tgt_pct=0.0125)))
    out.append(("P03_0.75_1.50",           ExitPolicy(sl_pct=0.0075, tgt_pct=0.0150)))
    out.append(("P04_0.60_1.25",           ExitPolicy(sl_pct=0.0060, tgt_pct=0.0125)))
    out.append(("P05_0.75_1.00_bars12",    ExitPolicy(sl_pct=0.0075, tgt_pct=0.0100, max_bars=12)))
    out.append(("P06_0.75_1.00_BE_0.5",    ExitPolicy(sl_pct=0.0075, tgt_pct=0.0100, be_trigger_pct=0.0050)))
    out.append(("P07_trail0.30_act0.40",
                ExitPolicy(sl_pct=0.0075, tgt_pct=0.0150, trail_pct=0.0030, trail_activate_pct=0.0040)))
    out.append(("P08_trail0.40_act0.50",
                ExitPolicy(sl_pct=0.0075, tgt_pct=0.0200, trail_pct=0.0040, trail_activate_pct=0.0050)))
    out.append(("P09_0.50_1.00",           ExitPolicy(sl_pct=0.0050, tgt_pct=0.0100)))
    return out


def run_sweep_on(candidate_name: str, trades: pd.DataFrame) -> pd.DataFrame:
    pols = _policies()
    print(f"[{candidate_name}]  n_trades={len(trades)}  running {len(pols)} exit policies ...")
    rows = []
    for pname, policy in pols:
        t0 = time.time()
        resolved = resolve_exits(trades, policy)
        dt = time.time() - t0
        m = compute_metrics(resolved)
        row = {"candidate": candidate_name, "policy": pname, **m.as_row(), "secs": round(dt, 1)}
        rows.append(row)
        print(f"    {pname:30s}  n={m.n_trades:4d}  pf={m.profit_factor:.3f}  dd={m.max_drawdown_pct:6.2f}  pnl={m.sum_pnl_pct:8.2f}  win={m.win_rate:5.2f}  sharpe={m.sharpe_ann:.2f}  secs={dt:.1f}")
    return pd.DataFrame(rows)


def main():
    cands = build_candidates()
    target_names = [
        "C10_v17b_short_no_AMCC_plus_L_noRSI6065_noAMCC",
        "C3_v17f_drop_AMCC_plus_L_noRSI6065",
    ]
    all_rows: List[pd.DataFrame] = []
    for name in target_names:
        trades = cands[name]
        df = run_sweep_on(name, trades)
        all_rows.append(df)

    result = pd.concat(all_rows, ignore_index=True)
    result = rank_variants(result)
    out = RESULTS_DIR / "exit_sweep_results.csv"
    result.to_csv(out, index=False)
    print(f"\n[OK] wrote {out}  ({len(result)} rows)")

    cols = ["candidate", "policy", "n_trades", "n_long", "n_short", "win_rate",
            "profit_factor", "max_drawdown_pct", "sum_pnl_pct", "sharpe_ann",
            "balance_score"]
    print("\n=== TOP 15 exit-policy variants ===")
    print(result.head(15)[cols].to_string(index=False))
    return result


if __name__ == "__main__":
    main()
