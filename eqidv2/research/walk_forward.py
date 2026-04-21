"""
Walk-forward split check for v17f baseline, C10, C3, C4.

Splits the 191-day sample into train/test halves by calendar date and reports
metrics on each half.  This is a first-order robustness check — the filter
decisions (AMCC drop, RSI[60,65) drop) were discovered on the full sample, so
we want to see that each half still exhibits the same direction of effect.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from .core import compute_metrics, load_all_v17
from .experiments_v2 import build_candidates

RESULTS_DIR = Path(__file__).resolve().parent / "results"


def _split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.to_datetime(df["entry_date"]).sort_values().unique()
    mid = len(dates) // 2
    cut = dates[mid]
    first = df[pd.to_datetime(df["entry_date"]) < cut].copy()
    second = df[pd.to_datetime(df["entry_date"]) >= cut].copy()
    return first, second


def _row(label: str, half: str, df: pd.DataFrame) -> dict:
    m = compute_metrics(df)
    return {"variant": label, "half": half, **m.as_row()}


def main():
    cands = build_candidates()
    bundles = load_all_v17()
    v17f = bundles["v17f"]

    targets = {
        "v17f_baseline": v17f,
        "C10": cands["C10_v17b_short_no_AMCC_plus_L_noRSI6065_noAMCC"],
        "C3": cands["C3_v17f_drop_AMCC_plus_L_noRSI6065"],
        "C4": cands["C4_v17b_short_plus_L_noRSI6065"],
    }

    rows = []
    for name, df in targets.items():
        first, second = _split(df)
        rows.append(_row(name, "H1", first))
        rows.append(_row(name, "H2", second))
        rows.append(_row(name, "ALL", df))

    out = pd.DataFrame(rows)
    path = RESULTS_DIR / "walk_forward_results.csv"
    out.to_csv(path, index=False)

    cols = ["variant", "half", "n_trades", "n_long", "n_short", "win_rate",
            "profit_factor", "max_drawdown_pct", "sum_pnl_pct", "sharpe_ann"]
    print(out[cols].to_string(index=False))
    print(f"\n[OK] wrote {path}")


if __name__ == "__main__":
    main()
