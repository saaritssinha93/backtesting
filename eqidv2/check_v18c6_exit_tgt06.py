# -*- coding: utf-8 -*-
"""
check_v18c6_exit_tgt06.py
=========================
Re-runs v18c6 with a flat 0.60% target for BOTH short and long across
ALL setups (including B_HUGE_C1_CLOSE_RECLAIM_BREAK which normally uses
1.20%).  Everything else — entry filters, RS context, shortlist, data
directory — is identical to the canonical v18c6 run.

What changes vs canonical v18c6
--------------------------------
  Canonical v18c6  ->  This run
  SHORT TGT=0.95%  ->  0.60%
  LONG  TGT=1.00%  ->  0.60%
  RECLAIM TGT=1.20%->  0.60%  (exit-profile patch overrides v18c6's own override)
  SL unchanged     :   SHORT 0.75%  |  LONG 0.75%  |  RECLAIM 0.85%

Output saved to:  C:\\TradingData\\eqidv2\\outputs_v18c6_tgt06_5min\\

At the end, prints a side-by-side comparison vs the canonical v18c6 CSV.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import numpy as np
import pandas as pd

# ── Output directory for this test run ──────────────────────────────────────
_OUTPUT_DIR = Path(r"C:\TradingData\eqidv2\outputs_v18c6_tgt06_5min")
_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Canonical v18c6 CSV for comparison (latest canonical run)
_CANONICAL_CSV = Path(
    r"C:\TradingData\eqidv2\outputs_v18c6_5min"
    r"\avwap_longshort_trades_v18c6_5min_ALL_DAYS_20260414_083551.csv"
)

NEW_TARGET_PCT = 0.0060  # 0.60%

# ── 1. Import v18c6 — applies all its module-level patches ──────────────────
import avwap_combined_runner_v18c6_5min as v18c6
import avwap_combined_runner_v18c5_5min as _base

# ── 2. Override output directory ─────────────────────────────────────────────
_base.runtime_dir = lambda _name: _OUTPUT_DIR
print(f"[PATCH] runtime_dir -> {_OUTPUT_DIR}")

# ── 3. Enable TEST_TARGET_OVERRIDE for base configs (SHORT + LONG A_MOD) ────
_base.TEST_TARGET_OVERRIDE    = True
_base.TEST_SHORT_TARGET_PCT   = NEW_TARGET_PCT
_base.TEST_LONG_TARGET_PCT    = NEW_TARGET_PCT
print(f"[PATCH] TEST_TARGET_OVERRIDE -> True  SHORT={NEW_TARGET_PCT:.4f}  LONG={NEW_TARGET_PCT:.4f}")

# ── 4. Override v18c6's reclaim exit profile so target is also 0.60% ────────
# v18c6 normally patches _base._v17c_setup_exit_profile to use target=1.20%
# for B_HUGE_RECLAIM. We re-patch it after the import to force 0.60%.
_ORIG_V18C6_EXIT_PROFILE = _base._v17c_setup_exit_profile


def _tgt06_setup_exit_profile(side_val, setup_val, entry_time_ist=None):
    profile = dict(_ORIG_V18C6_EXIT_PROFILE(side_val, setup_val, entry_time_ist=entry_time_ist))
    # Force 0.60% target for every setup / side
    profile["target_pct"] = NEW_TARGET_PCT
    return profile


_base._v17c_setup_exit_profile = _tgt06_setup_exit_profile
print(f"[PATCH] _v17c_setup_exit_profile -> target_pct forced to {NEW_TARGET_PCT:.4f} for all setups")

# ── 5. Force serial execution (process pool spawn fails from script context) ─
_base.MAX_WORKERS = 1
print("[PATCH] MAX_WORKERS -> 1 (serial, avoids multiprocessing spawn error)")

# ── 6. Disable chart generation crash (wrap main) ────────────────────────────
print(f"\n[RUN] Starting v18c6 TGT={NEW_TARGET_PCT*100:.1f}% run ...\n")
_before = {p.name for p in _OUTPUT_DIR.iterdir() if p.is_file()}

try:
    v18c6.main()
except Exception as _exc:
    print(f"[WARN] v18c6.main() raised (likely chart generation): {_exc}")
    print("[WARN] Proceeding to find output CSV anyway.")

# ── 6. Find new CSV ──────────────────────────────────────────────────────────
_after = {p.name for p in _OUTPUT_DIR.iterdir() if p.is_file()}
_new_files = [
    _OUTPUT_DIR / n
    for n in (_after - _before)
    if "trades" in n and n.endswith(".csv")
]

if not _new_files:
    print("[ERROR] No trades CSV found in output dir. Aborting comparison.")
    sys.exit(1)

_new_csv = sorted(_new_files)[-1]
print(f"\n[RESULT] New CSV: {_new_csv.name}")


# ── 7. Side-by-side comparison ───────────────────────────────────────────────
def _stats(df: pd.DataFrame) -> dict:
    df = df.copy()
    df["pnl_rs"]  = pd.to_numeric(df["pnl_rs"],  errors="coerce").fillna(0)
    df["pnl_pct"] = pd.to_numeric(df["pnl_pct"], errors="coerce").fillna(0)
    df["side"]    = df["side"].str.upper()
    df["outcome"] = df["outcome"].str.upper()
    df["setup"]   = df["setup"].str.upper()
    df = df.sort_values("trade_date").reset_index(drop=True)

    n      = len(df)
    wins   = (df["pnl_rs"] > 0).sum()
    losses = (df["pnl_rs"] < 0).sum()
    wr     = wins / n * 100 if n else 0
    gw     = df.loc[df["pnl_rs"] > 0, "pnl_rs"].sum()
    gl     = abs(df.loc[df["pnl_rs"] < 0, "pnl_rs"].sum())
    pf     = gw / gl if gl else float("inf")
    total_rs = df["pnl_rs"].sum()

    days     = df["trade_date"].dt.date.nunique()
    day_pnl  = df.groupby(df["trade_date"].dt.date)["pnl_rs"].sum()
    dwr      = (day_pnl > 0).sum() / len(day_pnl) * 100 if len(day_pnl) else 0

    cum_pct  = df["pnl_pct"].cumsum()
    max_dd   = (cum_pct - cum_pct.cummax()).min()

    daily_ret = df.groupby(df["trade_date"].dt.date)["pnl_pct"].sum()
    sharpe    = (daily_ret.mean() / daily_ret.std() * np.sqrt(252)) if daily_ret.std() > 0 else 0

    avg_win  = df.loc[df["pnl_rs"] > 0, "pnl_pct"].mean() if wins else 0
    avg_loss = df.loc[df["pnl_rs"] < 0, "pnl_pct"].mean() if losses else 0

    oc = df["outcome"].value_counts().to_dict()
    tgt_rate = oc.get("TARGET", 0) / n * 100
    sl_rate  = oc.get("SL",     0) / n * 100
    be_rate  = oc.get("BE",     0) / n * 100
    eod_rate = oc.get("EOD",    0) / n * 100

    longs  = df[df["side"] == "LONG"]
    shorts = df[df["side"] == "SHORT"]
    lwr = (longs["pnl_rs"] > 0).sum() / len(longs) * 100 if len(longs) else 0
    swr = (shorts["pnl_rs"] > 0).sum() / len(shorts) * 100 if len(shorts) else 0
    l_rs = longs["pnl_rs"].sum()
    s_rs = shorts["pnl_rs"].sum()

    # Setup-level
    setup_pf = {}
    setup_rs = {}
    for setup, g in df.groupby("setup"):
        gw2 = g.loc[g["pnl_rs"] > 0, "pnl_rs"].sum()
        gl2 = abs(g.loc[g["pnl_rs"] < 0, "pnl_rs"].sum())
        setup_pf[setup] = gw2 / gl2 if gl2 else float("inf")
        setup_rs[setup] = g["pnl_rs"].sum()

    return dict(
        n=n, wins=wins, losses=losses, wr=wr, pf=pf, total_rs=total_rs,
        days=days, dwr=dwr, max_dd=max_dd, sharpe=sharpe,
        avg_win=avg_win, avg_loss=avg_loss,
        tgt_rate=tgt_rate, sl_rate=sl_rate, be_rate=be_rate, eod_rate=eod_rate,
        longs=len(longs), shorts=len(shorts), lwr=lwr, swr=swr,
        l_rs=l_rs, s_rs=s_rs,
        setup_pf=setup_pf, setup_rs=setup_rs,
    )


new_df  = pd.read_csv(_new_csv)
new_df["trade_date"] = pd.to_datetime(new_df["trade_date"], errors="coerce")

can_df  = pd.read_csv(_CANONICAL_CSV)
can_df["trade_date"] = pd.to_datetime(can_df["trade_date"], errors="coerce")

# Align to same date range for fair comparison
_min_date = max(can_df["trade_date"].min(), new_df["trade_date"].min())
_max_date = min(can_df["trade_date"].max(), new_df["trade_date"].max())
can_df = can_df[(can_df["trade_date"] >= _min_date) & (can_df["trade_date"] <= _max_date)].copy()
new_df = new_df[(new_df["trade_date"] >= _min_date) & (new_df["trade_date"] <= _max_date)].copy()

A = _stats(can_df)
B = _stats(new_df)

print(f"\n{'='*72}")
print(f"  COMPARISON: v18c6 canonical (TGT 0.95%/1.00%/1.20%)  vs  TGT 0.60%")
print(f"  Date range: {_min_date.date()} -> {_max_date.date()}")
print(f"{'='*72}")
FMT = "{:<32} {:>16} {:>16}"
print(FMT.format("Metric", "v18c6 canonical", "v18c6 TGT 0.60%"))
print("-" * 66)

rows = [
    ("Total trades",          f"{A['n']}",                   f"{B['n']}"),
    ("Trading days",          f"{A['days']}",                f"{B['days']}"),
    ("Trades / day",          f"{A['n']/A['days']:.1f}",     f"{B['n']/B['days']:.1f}"),
    ("Win rate",              f"{A['wr']:.1f}%",             f"{B['wr']:.1f}%"),
    ("Day-win rate",          f"{A['dwr']:.1f}%",            f"{B['dwr']:.1f}%"),
    ("Profit factor",         f"{A['pf']:.3f}",              f"{B['pf']:.3f}"),
    ("Total Rs PnL",          f"Rs.{A['total_rs']:+,.0f}",   f"Rs.{B['total_rs']:+,.0f}"),
    ("Avg win (%)",           f"+{A['avg_win']:.2f}%",       f"+{B['avg_win']:.2f}%"),
    ("Avg loss (%)",          f"{A['avg_loss']:.2f}%",       f"{B['avg_loss']:.2f}%"),
    ("Max drawdown",          f"{A['max_dd']:.1f}%",         f"{B['max_dd']:.1f}%"),
    ("Sharpe (ann)",          f"{A['sharpe']:.2f}",          f"{B['sharpe']:.2f}"),
    ("LONG  trades / WR",     f"{A['longs']} / {A['lwr']:.0f}%", f"{B['longs']} / {B['lwr']:.0f}%"),
    ("SHORT trades / WR",     f"{A['shorts']} / {A['swr']:.0f}%",f"{B['shorts']} / {B['swr']:.0f}%"),
    ("LONG  Rs PnL",          f"Rs.{A['l_rs']:+,.0f}",       f"Rs.{B['l_rs']:+,.0f}"),
    ("SHORT Rs PnL",          f"Rs.{A['s_rs']:+,.0f}",       f"Rs.{B['s_rs']:+,.0f}"),
    ("Outcome: TARGET%",      f"{A['tgt_rate']:.0f}%",       f"{B['tgt_rate']:.0f}%"),
    ("Outcome: SL%",          f"{A['sl_rate']:.0f}%",        f"{B['sl_rate']:.0f}%"),
    ("Outcome: BE%",          f"{A['be_rate']:.0f}%",        f"{B['be_rate']:.0f}%"),
    ("Outcome: EOD%",         f"{A['eod_rate']:.0f}%",       f"{B['eod_rate']:.0f}%"),
]
for label, va, vb in rows:
    print(FMT.format(label, va, vb))

print(f"\n{'─'*66}")
print("  Per-setup breakdown")
print(f"{'─'*66}")
FMT2 = "{:<42} {:>11} {:>11}"
print(FMT2.format("Setup", "canonical PF", "TGT0.6% PF"))
print("-" * 66)
all_setups = sorted(set(A["setup_pf"]) | set(B["setup_pf"]))
for s in all_setups:
    apf = A["setup_pf"].get(s, float("nan"))
    bpf = B["setup_pf"].get(s, float("nan"))
    ars = A["setup_rs"].get(s, 0)
    brs = B["setup_rs"].get(s, 0)
    apf_s = f"{apf:.3f}" if np.isfinite(apf) else "inf"
    bpf_s = f"{bpf:.3f}" if np.isfinite(bpf) else "inf"
    print(f"  {s:<40} {apf_s:>10} {bpf_s:>10}   Rs: {ars:+,.0f} -> {brs:+,.0f}")

print(f"\n{'─'*66}")
print("  Monthly Rs PnL comparison")
print(f"{'─'*66}")
can_m = can_df.copy()
can_m["pnl_rs"] = pd.to_numeric(can_m["pnl_rs"], errors="coerce").fillna(0)
new_m = new_df.copy()
new_m["pnl_rs"] = pd.to_numeric(new_m["pnl_rs"], errors="coerce").fillna(0)
can_monthly = can_m.groupby(can_m["trade_date"].dt.to_period("M"))["pnl_rs"].sum()
new_monthly = new_m.groupby(new_m["trade_date"].dt.to_period("M"))["pnl_rs"].sum()
all_months = sorted(set(can_monthly.index) | set(new_monthly.index))
FMT3 = "  {:<10} {:>14} {:>14} {:>12}"
print(FMT3.format("Month", "canonical Rs", "TGT0.6% Rs", "Delta"))
print("  " + "-" * 52)
for m in all_months:
    ca = can_monthly.get(m, 0)
    nb = new_monthly.get(m, 0)
    print(FMT3.format(str(m), f"Rs.{ca:+,.0f}", f"Rs.{nb:+,.0f}", f"Rs.{nb-ca:+,.0f}"))

print(f"\n[DONE] New CSV: {_new_csv}")
print(f"[DONE] Canonical: {_CANONICAL_CSV.name}")
