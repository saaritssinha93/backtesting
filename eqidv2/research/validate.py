"""Reproduce v17b/c/d/e/f baseline metrics via the research framework.
Must match the production comparison report within rounding.
"""
from __future__ import annotations

import pandas as pd

from .core import compute_metrics, load_all_v17

EXPECTED = {
    # from v17b_v17c_v17d_v17e_v17f_latest_comparison_20260419.md
    "v17b": {"n_trades": 1078, "n_long": 914, "n_short": 164, "sum_pnl_pct": 1157.8342, "profit_factor": 1.892, "max_drawdown_pct": 43.5182},
    "v17c": {"n_trades": 1219, "n_long": 960, "n_short": 259, "sum_pnl_pct": 1201.8242, "profit_factor": 1.786, "max_drawdown_pct": 58.9204},
    "v17d": {"n_trades": 1254, "n_long": 960, "n_short": 294, "sum_pnl_pct": 1209.8867, "profit_factor": 1.767, "max_drawdown_pct": 48.2659},
    "v17e": {"n_trades": 1118, "n_long": 960, "n_short": 158, "sum_pnl_pct": 1172.3875, "profit_factor": 1.862, "max_drawdown_pct": 52.8541},
    "v17f": {"n_trades": 1229, "n_long": 960, "n_short": 269, "sum_pnl_pct": 1246.8526, "profit_factor": 1.826, "max_drawdown_pct": 44.8046},
}


def main() -> int:
    bundles = load_all_v17()
    rows = []
    all_ok = True
    for name, df in bundles.items():
        m = compute_metrics(df)
        row = {"variant": name, **m.as_row()}
        rows.append(row)
        exp = EXPECTED[name]
        tol_count = 0  # trade count must match exactly
        tol_pnl = 0.05  # pnl within 5bps
        tol_pf = 0.01
        tol_dd = 0.10
        ok = (
            m.n_trades == exp["n_trades"]
            and m.n_long == exp["n_long"]
            and m.n_short == exp["n_short"]
            and abs(m.sum_pnl_pct - exp["sum_pnl_pct"]) <= tol_pnl
            and abs(m.profit_factor - exp["profit_factor"]) <= tol_pf
            and abs(m.max_drawdown_pct - exp["max_drawdown_pct"]) <= tol_dd
        )
        if not ok:
            all_ok = False
            print(f"[FAIL] {name}")
            print(f"    got   n={m.n_trades} L={m.n_long} S={m.n_short} pnl={m.sum_pnl_pct:.4f} pf={m.profit_factor:.3f} dd={m.max_drawdown_pct:.4f}")
            print(f"    want  n={exp['n_trades']} L={exp['n_long']} S={exp['n_short']} pnl={exp['sum_pnl_pct']:.4f} pf={exp['profit_factor']:.3f} dd={exp['max_drawdown_pct']:.4f}")
        else:
            print(f"[ OK ] {name}  n={m.n_trades}  pnl={m.sum_pnl_pct:.2f}%  pf={m.profit_factor:.3f}  dd={m.max_drawdown_pct:.2f}%  short_pf={m.short_pf:.3f}")

    pd.DataFrame(rows).to_csv("eqidv2/research/results/baseline_validation.csv", index=False)
    print("=== baseline_validation.csv written" if all_ok else "=== MISMATCH — framework math needs review")
    return 0 if all_ok else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
