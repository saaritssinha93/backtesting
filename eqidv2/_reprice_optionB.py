"""
Exact reprice of or_gate_off_strict_rs entries at SL=0.75%, TGT=0.80%
Uses saved post-filter entries — no re-scan needed.
"""
from __future__ import annotations
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
import avwap_combined_runner_v16_5min as base
import v16_5min_sltarget_search as slsearch

MARKET_DAYS = 69
SL_PCT  = 0.0075   # 0.75%
TGT_PCT = 0.0080   # 0.80%

ENTRIES_CSV = ROOT / "outputs_v16_5min_long_scenario_search" / "or_gate_off_strict_rs" / "or_gate_off_strict_rs_post_filter_entries.csv"
OUT_CSV     = ROOT / "outputs_v16_5min_long_scenario_search" / "or_gate_off_strict_rs" / "or_gate_off_strict_rs_exact_sl_0.0075_tgt_0.0080.csv"


def _pf(pnl: pd.Series) -> float:
    pos = float(pnl[pnl > 0].sum())
    neg = float(-pnl[pnl < 0].sum())
    return pos / neg if neg > 0 else (999.0 if pos > 0 else 0.0)


def _dd(pnl: pd.Series) -> float:
    if pnl.empty:
        return 0.0
    eq = pnl.cumsum()
    return float((eq.cummax() - eq).max())


def main() -> None:
    print(f"Loading entries from: {ENTRIES_CSV}")
    entries = pd.read_csv(ENTRIES_CSV)
    print(f"Entries loaded: {len(entries)} trades")

    tickers = entries["ticker"].astype(str).unique()
    print(f"Loading price caches for {len(tickers)} tickers...")
    cache_1m = slsearch._load_1m_cache(tickers)
    cache_5m = slsearch._load_5m_cache(tickers)

    paths = slsearch._build_trade_paths(entries, cache_1m, cache_5m)
    print(f"Paths built: {len(paths)} (dropped {len(entries)-len(paths)} with no bar data)")

    print(f"Simulating exits: SL={SL_PCT*100:.2f}%, TGT={TGT_PCT*100:.2f}%")
    trades = pd.DataFrame(
        slsearch._simulate_trade(p, SL_PCT, TGT_PCT) for p in paths
    )

    # --- metrics ---
    pnl = trades["pnl_pct"].astype(float)
    day_pnl = trades.groupby("trade_date")["pnl_pct"].sum()
    outcome = trades["outcome"].str.upper()
    n = len(trades)
    td = int(trades["trade_date"].nunique())

    print()
    print("=" * 60)
    print(f"  or_gate_off_strict_rs  |  SL={SL_PCT*100:.2f}%  TGT={TGT_PCT*100:.2f}%  RR={TGT_PCT/SL_PCT:.3f}")
    print("=" * 60)
    print(f"  Trades          : {n}")
    print(f"  Trade days      : {td}")
    print(f"  Market days     : {MARKET_DAYS}")
    print(f"  TPD (market)    : {n/MARKET_DAYS:.2f}")
    print(f"  TPD (trade day) : {n/td:.2f}")
    print(f"  Total PnL       : {pnl.sum():.2f}%")
    print(f"  Avg PnL/trade   : {pnl.mean():.4f}%")
    print(f"  Trade Win%      : {(pnl>0).mean()*100:.1f}%")
    print(f"  Day Win%        : {(day_pnl>0).mean()*100:.1f}%")
    print(f"  Profit Factor   : {_pf(pnl):.3f}")
    print(f"  Max Drawdown    : {_dd(pnl):.2f}%")
    print(f"  TGT hit%        : {(outcome=='TARGET').mean()*100:.1f}%")
    print(f"  SL hit%         : {(outcome=='SL').mean()*100:.1f}%")
    print(f"  EOD exit%       : {(outcome=='EOD').mean()*100:.1f}%")
    print()

    # --- comparison vs baseline ---
    baseline = ROOT / "outputs_v16_5min_long_scenario_search" / "baseline" / "baseline_exact_sl_0.0065_tgt_0.0090.csv"
    if baseline.exists():
        b = pd.read_csv(baseline)
        bp = b["pnl_pct"].astype(float)
        bdp = b.groupby("trade_date")["pnl_pct"].sum()
        print("--- vs baseline (SL=0.65% TGT=0.90%) ---")
        print(f"  Trades   : {len(b)} -> {n}  ({n-len(b):+d})")
        print(f"  TPD      : {len(b)/MARKET_DAYS:.2f} -> {n/MARKET_DAYS:.2f}")
        print(f"  PF       : {_pf(bp):.3f} -> {_pf(pnl):.3f}  ({_pf(pnl)-_pf(bp):+.3f})")
        print(f"  PnL      : {bp.sum():.1f}% -> {pnl.sum():.1f}%  ({pnl.sum()-bp.sum():+.1f}%)")
        print(f"  Win%     : {(bp>0).mean()*100:.1f}% -> {(pnl>0).mean()*100:.1f}%")
        print(f"  DayWin%  : {(bdp>0).mean()*100:.1f}% -> {(day_pnl>0).mean()*100:.1f}%")
        print(f"  MaxDD    : {_dd(bp):.2f}% -> {_dd(pnl):.2f}%")

    trades.to_csv(OUT_CSV, index=False)
    print(f"\nTrades saved to: {OUT_CSV}")


if __name__ == "__main__":
    main()
