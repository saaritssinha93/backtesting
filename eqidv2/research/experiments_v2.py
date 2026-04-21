"""
V17 research framework — Round 2 experiments.

Combines the best insights from Round 1:
  - E6 showed: dropping A_MOD_CLOSE_CONTINUATION_BREAK entirely cuts MaxDD from
    44.8% to 30.7% on v17f while keeping PF > 1.86.
  - E8_L3 showed: dropping long RSI[60,65) lowers DD 48.9%->44.2%, raises PF
    1.80->1.88, with near-zero PnL loss.
  - E7_S7 (v17b shorts) remains the high-PF short leg (PF 2.46).
  - E7_S8 (v17f shorts) remains the high-PnL short leg.

Builds 'clean' composites and then tests them alongside round-1 baselines.
Also runs the EXIT-POLICY sweep on the winner candidate using 5-min bars.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import numpy as np

from .core import compute_metrics, load_all_v17, rank_variants
from .exits import ExitPolicy, resolve_exits

RESULTS_DIR = Path(__file__).resolve().parent / "results"


def _record(label: str, trades: pd.DataFrame) -> Dict:
    m = compute_metrics(trades)
    return {"variant": label, **m.as_row()}


def _combine(shorts: pd.DataFrame, longs: pd.DataFrame) -> pd.DataFrame:
    if shorts.empty and longs.empty:
        return shorts
    out = pd.concat([shorts, longs], ignore_index=True).sort_values(
        ["entry_time_ist", "ticker"], kind="stable"
    ).reset_index(drop=True)
    return out


def _apply_long_rsi60_65_drop(longs: pd.DataFrame) -> pd.DataFrame:
    rsi = pd.to_numeric(longs["rsi_signal"], errors="coerce")
    return longs[~((rsi >= 60) & (rsi < 65))].copy()


def _apply_drop_setup(df: pd.DataFrame, setup: str) -> pd.DataFrame:
    return df[df["setup"].astype(str).str.upper() != setup.upper()].copy()


def build_candidates() -> Dict[str, pd.DataFrame]:
    """Construct the round-2 candidate variants — combined SHORT+LONG trade sets."""
    bundles = load_all_v17()
    v17b, v17c, v17d, v17e, v17f = (bundles[k] for k in ["v17b", "v17c", "v17d", "v17e", "v17f"])

    shorts_b = v17b[v17b["side"] == "SHORT"].copy()
    shorts_e = v17e[v17e["side"] == "SHORT"].copy()
    shorts_f = v17f[v17f["side"] == "SHORT"].copy()
    longs_f = v17f[v17f["side"] == "LONG"].copy()

    longs_f_no_rsi = _apply_long_rsi60_65_drop(longs_f)
    # drop A_MOD_CLOSE_CONTINUATION_BREAK setup for each leg
    shorts_b_no_amcc = _apply_drop_setup(shorts_b, "A_MOD_CLOSE_CONTINUATION_BREAK")
    shorts_e_no_amcc = _apply_drop_setup(shorts_e, "A_MOD_CLOSE_CONTINUATION_BREAK")
    shorts_f_no_amcc = _apply_drop_setup(shorts_f, "A_MOD_CLOSE_CONTINUATION_BREAK")
    longs_f_no_amcc = _apply_drop_setup(longs_f, "A_MOD_CLOSE_CONTINUATION_BREAK")
    longs_f_no_both = _apply_drop_setup(longs_f_no_rsi, "A_MOD_CLOSE_CONTINUATION_BREAK")

    # Additional cleanups: drop short late-day (>=14:00) trades from v17f shorts
    late_shorts = shorts_f[shorts_f["entry_minute"] >= 14 * 60]
    shorts_f_no_late = shorts_f[shorts_f["entry_minute"] < 14 * 60].copy()
    # short QS >= 5 on v17f shorts
    shorts_f_qs5 = shorts_f[pd.to_numeric(shorts_f["quality_score"], errors="coerce") >= 5]

    candidates = {
        # Baselines first
        "C0_baseline_v17f": v17f,
        "C1_baseline_v17b": v17b,
        # Round-2 composites
        "C2_v17f_drop_AMCC_all": _combine(shorts_f_no_amcc, longs_f_no_amcc),
        "C3_v17f_drop_AMCC_plus_L_noRSI6065": _combine(shorts_f_no_amcc, longs_f_no_both),
        "C4_v17b_short_plus_L_noRSI6065": _combine(shorts_b, longs_f_no_rsi),
        "C5_v17e_short_plus_L_noRSI6065": _combine(shorts_e, longs_f_no_rsi),
        "C6_v17f_short_plus_L_noRSI6065": _combine(shorts_f, longs_f_no_rsi),
        "C7_v17f_short_no_AMCC_plus_L_noRSI6065": _combine(shorts_f_no_amcc, longs_f_no_rsi),
        "C8_v17f_short_no_late_plus_L_noRSI6065": _combine(shorts_f_no_late, longs_f_no_rsi),
        "C9_v17f_short_QSge5_plus_L_noRSI6065": _combine(shorts_f_qs5, longs_f_no_rsi),
        "C10_v17b_short_no_AMCC_plus_L_noRSI6065_noAMCC":
            _combine(shorts_b_no_amcc, longs_f_no_both),
        "C11_v17e_short_no_AMCC_plus_L_noRSI6065_noAMCC":
            _combine(shorts_e_no_amcc, longs_f_no_both),
        "C12_v17f_short_no_AMCC_no_late_plus_L_noRSI6065_noAMCC":
            _combine(shorts_f_no_amcc[shorts_f_no_amcc["entry_minute"] < 14 * 60], longs_f_no_both),
    }
    return candidates


def run_round2():
    candidates = build_candidates()
    rows = [_record(k, v) for k, v in candidates.items()]
    summary = pd.DataFrame(rows)
    ranked = rank_variants(summary)
    out = RESULTS_DIR / "round2_candidates_summary.csv"
    ranked.to_csv(out, index=False)
    print(f"[OK] wrote {out}")
    cols = ["variant", "n_trades", "n_long", "n_short", "trades_per_day", "win_rate",
            "profit_factor", "max_drawdown_pct", "sum_pnl_pct", "sharpe_ann",
            "short_pf", "short_n", "long_pf", "balance_score"]
    print(ranked[cols].to_string(index=False))
    return candidates, ranked


def run_exit_sweep(trade_set: pd.DataFrame, label: str, policies: List[Tuple[str, ExitPolicy]]) -> pd.DataFrame:
    """Re-resolve exits for a candidate trade set under each policy and return a summary dataframe."""
    rows = []
    for pname, policy in policies:
        resolved = resolve_exits(trade_set, policy)
        rows.append({"policy": pname, "source": label, **compute_metrics(resolved).as_row()})
    return pd.DataFrame(rows)


def main():
    candidates, ranked = run_round2()
    # Save detail of top-3 ranked
    for name in ranked["variant"].head(5):
        trades = candidates[name]
        safe = name.replace(" ", "_").replace("+", "and").replace("/", "_")
        trades.to_csv(RESULTS_DIR / f"round2_{safe}.csv", index=False)
    return candidates, ranked


if __name__ == "__main__":
    main()
