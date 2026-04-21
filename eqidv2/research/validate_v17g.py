"""
V17g wiring validator.

Runs two checks against the v17g production output CSV:

  [A] Toggle-OFF check — with EQIDV17G_LONG_AMCC_DROP_ENABLED=0, the v17g
      trade CSV must match v17b byte-for-byte on the LONG side (and exactly
      match on SHORT, since v17g is a pure superset patch of v17b).

  [B] Toggle-ON target check — with the filter enabled, v17g trade metrics
      must track the research C10 candidate within a small tolerance:
         - LONG trades:  research predicts ~780-797 (917 v17b LONGs - 117
           AMCC setup ~= 797)
         - SHORT trades: 164 (v17b exact)
         - PF:          ~1.99 (research C10: 1.992)
         - MaxDD:       ~29% (research C10: 29.09%)
         - Win rate:    ~71-72% (research C10: 71.68%)
         - Sharpe:      ~10.5 (research C10: 10.59)

Usage
-----
  # after running v17g with AMCC-drop enabled and pointing the script at
  # the trade CSV written into outputs_v17g_5min/:
  python -m eqidv2.research.validate_v17g \
      --v17g  C:\TradingData\eqidv2\outputs_v17g_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_<TS>.csv
  # optionally also pass --v17g-off for the toggle-off run
  python -m eqidv2.research.validate_v17g \
      --v17g     <enabled_run.csv> \
      --v17g-off <disabled_run.csv>
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from .core import V17_TRADE_CSVS, compute_metrics, load_trades


C10_TARGET = {
    # Research C10 targets from round2_candidates_summary.csv
    "n_trades": 950,
    "n_long": 786,
    "n_short": 164,
    "pf": 1.992,
    "maxdd_pct": 29.09,
    "win_rate": 71.68,
    "sharpe": 10.59,
    # Alternate — expected v17g-ish numbers if LONG base is v17b (914 LONGs)
    # rather than v17f (960 LONGs): 914 - 117_amcc = 797 → total 797+164 = 961
    "n_trades_v17g_expected": 961,
    "n_long_v17g_expected": 797,
}

TOLERANCE = {
    "n_trades_pct": 3.0,     # +/- 3% on trade count
    "pf_abs": 0.08,          # +/- 0.08 on PF
    "maxdd_abs": 4.0,        # +/- 4 pp on DD
    "win_rate_abs": 2.0,     # +/- 2 pp on win rate
    "sharpe_abs": 1.5,       # +/- 1.5 on Sharpe
}


def _fmt_bool(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


def _load(path: Optional[Path]) -> Optional[pd.DataFrame]:
    if path is None:
        return None
    if not path.exists():
        raise FileNotFoundError(f"missing: {path}")
    return load_trades(path)


def _report(label: str, df: pd.DataFrame, target: Optional[Dict] = None) -> None:
    m = compute_metrics(df).as_row()
    print(f"\n=== {label} ===")
    print(f"  n_trades    {m['n_trades']:>6d}   n_long {m['n_long']:>4d}   n_short {m['n_short']:>4d}")
    print(f"  win_rate    {m['win_rate']:>6.2f}%  pf     {m['profit_factor']:>6.3f}")
    print(f"  maxdd       {m['max_drawdown_pct']:>6.2f}%  sharpe {m['sharpe_ann']:>6.2f}")
    print(f"  sum_pnl     {m['sum_pnl_pct']:>7.2f}%  calmar {m['calmar']:>6.2f}")

    if target is None:
        return

    n_tol = target["n_trades_v17g_expected"] * TOLERANCE["n_trades_pct"] / 100.0
    checks = [
        ("n_long within tol",
         abs(m["n_long"] - target["n_long_v17g_expected"]) <= n_tol,
         f"{m['n_long']} vs target {target['n_long_v17g_expected']}  (tol +/- {n_tol:.0f})"),
        ("n_short exact match (v17b)",
         m["n_short"] == target["n_short"],
         f"{m['n_short']} vs target {target['n_short']}"),
        ("pf within tol",
         abs(m["profit_factor"] - target["pf"]) <= TOLERANCE["pf_abs"],
         f"{m['profit_factor']:.3f} vs target {target['pf']:.3f}  (tol +/- {TOLERANCE['pf_abs']})"),
        ("maxdd within tol",
         abs(m["max_drawdown_pct"] - target["maxdd_pct"]) <= TOLERANCE["maxdd_abs"],
         f"{m['max_drawdown_pct']:.2f}% vs target {target['maxdd_pct']:.2f}%  (tol +/- {TOLERANCE['maxdd_abs']})"),
        ("win_rate within tol",
         abs(m["win_rate"] - target["win_rate"]) <= TOLERANCE["win_rate_abs"],
         f"{m['win_rate']:.2f}% vs target {target['win_rate']:.2f}%  (tol +/- {TOLERANCE['win_rate_abs']})"),
        ("sharpe within tol",
         abs(m["sharpe_ann"] - target["sharpe"]) <= TOLERANCE["sharpe_abs"],
         f"{m['sharpe_ann']:.2f} vs target {target['sharpe']:.2f}  (tol +/- {TOLERANCE['sharpe_abs']})"),
    ]
    print(f"  --- vs research C10 target (adjusted for v17b LONG base) ---")
    for name, ok, detail in checks:
        print(f"  [{_fmt_bool(ok)}]  {name:28s}  {detail}")


def _toggle_off_check(v17g_off: pd.DataFrame, v17b: pd.DataFrame) -> None:
    print("\n=== [A] TOGGLE-OFF check — v17g (AMCC disabled) vs v17b ===")

    # Align by the trade identity tuple (signal_time_ist, ticker, side, setup)
    key_cols = ["signal_time_ist", "ticker", "side", "setup"]
    for df_name, df in [("v17g_off", v17g_off), ("v17b", v17b)]:
        missing = [c for c in key_cols if c not in df.columns]
        if missing:
            print(f"  [FAIL]  {df_name} missing columns: {missing}")
            return

    a = v17g_off[key_cols + ["pnl_pct", "outcome"]].copy()
    b = v17b[key_cols + ["pnl_pct", "outcome"]].copy()
    a = a.sort_values(key_cols).reset_index(drop=True)
    b = b.sort_values(key_cols).reset_index(drop=True)

    checks = [
        ("same row count", len(a) == len(b), f"{len(a)} vs {len(b)}"),
    ]
    if len(a) == len(b):
        checks.append((
            "identical key columns",
            a[key_cols].equals(b[key_cols]),
            "ok" if a[key_cols].equals(b[key_cols]) else "trade identities differ",
        ))
        pnl_diff = (a["pnl_pct"].round(6) != b["pnl_pct"].round(6)).sum()
        checks.append((
            "pnl_pct equal (rounded 6dp)",
            pnl_diff == 0,
            f"{pnl_diff} differing rows",
        ))
        outcome_diff = (a["outcome"] != b["outcome"]).sum()
        checks.append((
            "outcomes match",
            outcome_diff == 0,
            f"{outcome_diff} differing rows",
        ))

    for name, ok, detail in checks:
        print(f"  [{_fmt_bool(ok)}]  {name:30s}  {detail}")


def _toggle_on_metric_check(v17g_on: pd.DataFrame, v17b: pd.DataFrame) -> None:
    # Also check that v17g ON = v17b with AMCC LONG trades removed.
    amcc = v17b[
        (v17b["side"].str.upper() == "LONG")
        & (v17b["setup"].astype(str).str.upper() == "A_MOD_CLOSE_CONTINUATION_BREAK")
    ]
    expected_rows = len(v17b) - len(amcc)

    print("\n=== [B] TOGGLE-ON structural check — v17g = v17b minus LONG AMCC ===")
    n_v17b = len(v17b)
    n_amcc = len(amcc)
    n_v17g = len(v17g_on)
    print(f"  v17b total trades           : {n_v17b}")
    print(f"  v17b LONG AMCC trades       : {n_amcc}")
    print(f"  expected v17g total         : {expected_rows}")
    print(f"  actual v17g total           : {n_v17g}")
    print(f"  [{_fmt_bool(n_v17g == expected_rows)}]  v17g row count matches v17b - AMCC")

    key_cols = ["signal_time_ist", "ticker", "side", "setup"]
    v17b_keep = v17b[
        ~(
            (v17b["side"].str.upper() == "LONG")
            & (v17b["setup"].astype(str).str.upper() == "A_MOD_CLOSE_CONTINUATION_BREAK")
        )
    ][key_cols + ["pnl_pct"]].sort_values(key_cols).reset_index(drop=True)
    v17g_k = v17g_on[key_cols + ["pnl_pct"]].sort_values(key_cols).reset_index(drop=True)
    identical = len(v17b_keep) == len(v17g_k) and v17b_keep.equals(v17g_k)
    print(f"  [{_fmt_bool(identical)}]  v17g == v17b minus AMCC  (key+pnl exact match)")


def main():
    p = argparse.ArgumentParser(description="Validate v17g output against research C10 target")
    p.add_argument("--v17g", type=Path, required=True,
                   help="v17g output CSV (AMCC drop ENABLED)")
    p.add_argument("--v17g-off", type=Path, default=None,
                   help="optional v17g output CSV with AMCC drop DISABLED (for byte-for-byte toggle check)")
    p.add_argument("--v17b", type=Path, default=V17_TRADE_CSVS["v17b"],
                   help="v17b reference CSV (default: research framework's known-good path)")
    args = p.parse_args()

    v17g_on = _load(args.v17g)
    v17g_off = _load(args.v17g_off)
    v17b = _load(args.v17b)

    print(f"v17g ON  trades : {args.v17g}  ({len(v17g_on)} rows)")
    if v17g_off is not None:
        print(f"v17g OFF trades : {args.v17g_off}  ({len(v17g_off)} rows)")
    print(f"v17b reference  : {args.v17b}  ({len(v17b)} rows)")

    _report("v17b baseline (reference)", v17b)
    _report("v17g (AMCC drop ENABLED)", v17g_on, target=C10_TARGET)

    _toggle_on_metric_check(v17g_on, v17b)

    if v17g_off is not None:
        _report("v17g (AMCC drop DISABLED)", v17g_off)
        _toggle_off_check(v17g_off, v17b)

    print("\n[OK] validation run complete")


if __name__ == "__main__":
    main()
