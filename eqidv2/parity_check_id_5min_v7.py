"""Phase 3: parity harness for the ID 5min v7 live adapter.

Replays a historical trading day slot-by-slot through avwap_5min_ID_v7_live_scan
and diffs the output signals against v7 backtester's v5_stage/trades.csv (the
pre-PF-expansion baseline). Reports matched / extra-live / missing-live counts
on the (ticker, side, setup, signal_ts) join key.

Usage:
    python parity_check_id_5min_v7.py --date 2026-04-27 [--limit-tickers N]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Set, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import avwap_5min_ID_v7_live_scan as v7_live


V5_STAGE_TRADES = Path(r"C:\TradingData\eqidv2\outputs_ID_v7_5min\v5_stage\native_all_setups_raw\trades.csv")
# NOTE: this points at the PURE fresh-scan output (native_all_setups_raw/trades.csv),
# NOT v5_stage/trades.csv. v5_stage/trades.csv is a merge of an old frozen baseline
# + mined additions from the fresh scan; the live adapter cannot reproduce the
# baseline portion since it was generated against parquet data that no longer
# exists. native_all_setups_raw/trades.csv is the apples-to-apples target.


def _load_baseline(test_date: str) -> pd.DataFrame:
    if not V5_STAGE_TRADES.exists():
        raise FileNotFoundError(f"baseline not found at {V5_STAGE_TRADES}")
    df = pd.read_csv(V5_STAGE_TRADES)
    # native_all_setups_raw/trades.csv uses 'date' + 'signal_ts'; v5_stage/trades.csv
    # uses 'trade_date' + 'signal_time_ist'. Detect and normalise.
    if "trade_date" in df.columns:
        df["trade_date"] = df["trade_date"].astype(str)
        date_col = "trade_date"
        ts_col = "signal_time_ist"
    else:
        df["date"] = df["date"].astype(str)
        date_col = "date"
        ts_col = "signal_ts"
    out = df[df[date_col] == test_date].copy()
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["side"] = out["side"].astype(str).str.upper().str.strip()
    out["setup"] = out["setup"].astype(str).str.strip()
    out["signal_time_ist"] = out[ts_col].astype(str).str.strip()
    return out


def _row_key(ticker: str, side: str, setup: str, signal_ts: str) -> Tuple[str, str, str, str]:
    return (str(ticker).upper().strip(),
            str(side).upper().strip(),
            str(setup).strip(),
            str(signal_ts).strip())


def _baseline_keys(baseline_df: pd.DataFrame) -> Set[Tuple[str, str, str, str]]:
    keys: Set[Tuple[str, str, str, str]] = set()
    for _, row in baseline_df.iterrows():
        keys.add(_row_key(row["ticker"], row["side"], row["setup"], row["signal_time_ist"]))
    return keys


def _live_keys_for_day(test_date: str, universe: list[str], market_ctx: dict) -> tuple[
    Set[Tuple[str, str, str, str]], pd.DataFrame
]:
    """Replay all 5-min slots from 09:15 to 15:30 for test_date, calling
    v7_live.scan_slot at each slot. Returns (set of keys, full live DF)."""
    base_day = pd.Timestamp(f"{test_date} 09:15:00", tz="Asia/Kolkata")
    end_day = pd.Timestamp(f"{test_date} 15:30:00", tz="Asia/Kolkata")

    all_live_frames: list[pd.DataFrame] = []
    live_keys: Set[Tuple[str, str, str, str]] = set()

    slot = base_day
    slots_scanned = 0
    t0 = time.perf_counter()
    while slot <= end_day:
        try:
            short_df, long_df = v7_live.scan_slot(slot, universe, market_ctx)
        except Exception as exc:
            print(f"  [WARN] slot {slot} scan failed: {type(exc).__name__}: {exc}")
            short_df = pd.DataFrame()
            long_df = pd.DataFrame()
        slots_scanned += 1
        for df in (short_df, long_df):
            if df is None or df.empty:
                continue
            all_live_frames.append(df)
            for _, r in df.iterrows():
                live_keys.add(_row_key(r["ticker"], r["side"], r["setup"], r["bar_time_ist"]))
        if slots_scanned % 12 == 0:
            elapsed = time.perf_counter() - t0
            print(f"  ...scanned {slots_scanned} slots in {elapsed:.1f}s ({slots_scanned/elapsed:.1f} slots/s)")
        slot = slot + pd.Timedelta(minutes=5)

    live_df = pd.concat(all_live_frames, ignore_index=True) if all_live_frames else pd.DataFrame()
    print(f"  scanned {slots_scanned} slots total in {time.perf_counter() - t0:.1f}s")
    return live_keys, live_df


def _format_key_set(keys: Set[Tuple[str, str, str, str]], sample: int = 8) -> str:
    if not keys:
        return "(none)"
    sorted_keys = sorted(keys)
    out_lines = [f"  {k[0]:>14} {k[1]:>6} {k[2]:<35} {k[3]}" for k in sorted_keys[:sample]]
    if len(sorted_keys) > sample:
        out_lines.append(f"  ... and {len(sorted_keys) - sample} more")
    return "\n".join(out_lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Parity harness for ID 5min v7 live adapter")
    ap.add_argument("--date", required=True, help="Test date YYYY-MM-DD (must exist in v5_stage/trades.csv)")
    ap.add_argument("--limit-tickers", type=int, default=0, help="Limit universe to first N tickers (for fast iteration)")
    args = ap.parse_args()

    print(f"=== Parity check: {args.date} ===")
    print(f"baseline: {V5_STAGE_TRADES}")

    baseline_df = _load_baseline(args.date)
    print(f"baseline trades on {args.date}: {len(baseline_df)}")
    if baseline_df.empty:
        print("No baseline trades — nothing to compare.")
        return 1

    print(f"setup mix: {dict(baseline_df['setup'].value_counts())}")
    print(f"side mix: {dict(baseline_df['side'].value_counts())}")
    print()

    print("Loading market context + universe...")
    market_ctx = v7_live.build_market_context_once()
    universe = sorted({str(t).strip().upper() for t in v7_live.v2._load_universe() if str(t).strip()})
    if args.limit_tickers > 0:
        # Always include all baseline tickers + cap with N additional
        baseline_tickers = sorted(baseline_df["ticker"].unique())
        extras = [t for t in universe if t not in baseline_tickers][: max(0, args.limit_tickers - len(baseline_tickers))]
        universe = sorted(set(baseline_tickers) | set(extras))
    print(f"universe size: {len(universe)}")
    print()

    print(f"Replaying slots for {args.date}...")
    live_keys, live_df = _live_keys_for_day(args.date, universe, market_ctx)
    print(f"live signals total: {len(live_df)}  unique keys: {len(live_keys)}")
    print()

    base_keys = _baseline_keys(baseline_df)
    matched = base_keys & live_keys
    missing_live = base_keys - live_keys   # in backtest but live didn't fire
    extra_live = live_keys - base_keys     # live fired but backtest didn't

    coverage = len(matched) / max(1, len(base_keys)) * 100
    print("=== PARITY RESULTS ===")
    print(f"baseline keys      : {len(base_keys)}")
    print(f"live keys          : {len(live_keys)}")
    print(f"matched            : {len(matched)}  ({coverage:.1f}% of baseline)")
    print(f"missing in live    : {len(missing_live)}  (backtest yes, live no)")
    print(f"extra in live      : {len(extra_live)}  (live yes, backtest no)")
    print()
    print("--- MISSING (backtest but not live, first 8) ---")
    print(_format_key_set(missing_live, sample=8))
    print()
    print("--- EXTRA (live but not backtest, first 8) ---")
    print(_format_key_set(extra_live, sample=8))
    print()

    out_path = ROOT / f"parity_report_{args.date}.csv"
    rows = []
    for k in matched:
        rows.append({"status": "matched", "ticker": k[0], "side": k[1], "setup": k[2], "signal_time_ist": k[3]})
    for k in missing_live:
        rows.append({"status": "missing_live", "ticker": k[0], "side": k[1], "setup": k[2], "signal_time_ist": k[3]})
    for k in extra_live:
        rows.append({"status": "extra_live", "ticker": k[0], "side": k[1], "setup": k[2], "signal_time_ist": k[3]})
    pd.DataFrame(rows).sort_values(["status", "ticker", "signal_time_ist"]).to_csv(out_path, index=False)
    print(f"Wrote full report: {out_path}")

    if coverage >= 80:
        print(f"\n[PASS] coverage {coverage:.1f}% >= 80% target")
        return 0
    else:
        print(f"\n[FAIL] coverage {coverage:.1f}% < 80% target")
        return 2


if __name__ == "__main__":
    sys.exit(main())
