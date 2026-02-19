# -*- coding: utf-8 -*-
"""
EQIDV2 v3 Off-market "row-by-row" signal scan for ONE date (default: today IST)

What it does
- Loads each ticker's 15m indicator parquet from a directory (default: stocks_indicators_15min_eq)
- Filters bars to a single session day (IST) and session hours (re-uses in_session from the v2 live analyser)
- Simulates the LIVE scanner on EVERY completed 15m bar (row-by-row):
    for each bar, it truncates df_day up to that bar and calls the live analyser's
    latest-bar entry logic (so behaviour matches live 15m runs).
- Writes:
    1) full CSV with bar_time_ist + prices + score + setup
    2) compact CSV (ticker, side, setup, impulse_type, quality_score) matching your daily signals style

This is meant to be run AFTER market hours to reproduce what the live scanner
*should* have emitted during the day.

Usage (from inside eqidv2 folder)
    python eqidv2_offmarket_today_rowbyrow_signals_v2.py
    python eqidv2_offmarket_today_rowbyrow_signals_v2.py --date 2026-02-18
"""

import os
import argparse
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import Any, Dict, List

import pandas as pd


IST_TZ = ZoneInfo("Asia/Kolkata")


def _today_ist() -> date:
    return datetime.now(IST_TZ).date()


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _to_ist_ts(x) -> pd.Timestamp:
    t = pd.Timestamp(x)
    if t.tzinfo is None:
        return t.tz_localize(IST_TZ)
    return t.tz_convert(IST_TZ)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="stocks_indicators_15min_eq", help="Directory with 15m parquet files")
    ap.add_argument("--suffix", default="_stocks_indicators_15min.parquet", help="Parquet suffix")
    ap.add_argument("--out-dir", default="daily_signals_offmarket", help="Output directory for CSVs")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (IST). Default: today IST")
    ap.add_argument("--tail-rows", type=int, default=2000, help="Tail rows to read per parquet (>= 500 is plenty)")
    ap.add_argument("--min-bars", type=int, default=7, help="Minimum bars required before running logic (v2 uses 7)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    target_day = _today_ist() if not args.date else datetime.strptime(args.date, "%Y-%m-%d").date()
    target_day_str = str(target_day)

    _ensure_dir(args.out_dir)

    # Import the v2 live analyser (the one with _latest_entry_signals_for_ticker)
    import eqidv2_live_combined_analyser_v3 as live

    # Point it at the same dir/suffix the user wants (for tickers list)
    live.DIR_15M = args.data_dir

    tickers = live.list_tickers_15m()
    if args.verbose:
        print(f"[INFO] tickers={len(tickers)}  data_dir={args.data_dir}  date={target_day_str}")

    # State to enforce per-ticker/day caps across the row-by-row simulation
    state: Dict[str, Any] = {"count": {}, "last_signal": {}}

    signals_rows: List[Dict[str, Any]] = []

    for tkr in tickers:
        fpath = os.path.join(args.data_dir, f"{tkr}{args.suffix}")
        df = live.read_parquet_tail(fpath, n=int(args.tail_rows))
        if df is None or df.empty:
            continue

        df = live.normalize_dates(df)
        if df.empty or "date" not in df.columns:
            continue

        # Keep only session bars
        df = df[df["date"].apply(live.in_session)].copy()
        if df.empty:
            continue

        df["date_ist"] = df["date"].apply(_to_ist_ts)
        df["day"] = df["date_ist"].dt.date

        df_day = df[df["day"] == target_day].copy()
        if df_day.empty:
            continue

        df_day = df_day.sort_values("date_ist").reset_index(drop=True)

        # Simulate live scanner bar-by-bar
        for k in range(max(int(args.min_bars) - 1, 0), len(df_day)):
            df_part = df_day.iloc[: k + 1].copy()

            sigs, _checks = live._latest_entry_signals_for_ticker(tkr, df_part, state)  # type: ignore
            if not sigs:
                continue

            for s in sigs:
                # Enforce per-side caps (live mark happens when writing signals; we replicate here)
                cap = int(getattr(live, "SHORT_CAP_PER_TICKER_PER_DAY", 1) if s.side.upper()=="SHORT" else getattr(live, "LONG_CAP_PER_TICKER_PER_DAY", 1))
                if not live.allow_signal_today(state, s.ticker, s.side, target_day_str, cap):
                    continue

                live.mark_signal(state, s.ticker, s.side, target_day_str)

                signals_rows.append(
                    {
                        "date": target_day_str,
                        "ticker": s.ticker,
                        "side": s.side,
                        "bar_time_ist": str(_to_ist_ts(s.bar_time_ist)),
                        "setup": s.setup,
                        "entry_price": float(s.entry_price),
                        "sl_price": float(s.sl_price),
                        "target_price": float(s.target_price),
                        "quality_score": float(s.score),
                        "impulse_type": str(s.diagnostics.get("impulse_type", "")),
                        "impulse_idx": int(s.diagnostics.get("impulse_idx", -1)),
                    }
                )

    if not signals_rows:
        print(f"[INFO] No signals found for {target_day_str}.")
        return

    out_full = os.path.join(args.out_dir, f"signals_{target_day_str}_rowbyrow_full.csv")
    out_compact = os.path.join(args.out_dir, f"signals_{target_day_str}_rowbyrow_compact.csv")

    full_df = pd.DataFrame(signals_rows).sort_values(["bar_time_ist", "side", "ticker"]).reset_index(drop=True)
    full_df.to_csv(out_full, index=False)

    compact_df = full_df[["date", "ticker", "side", "setup", "impulse_type", "quality_score"]].copy()
    compact_df.to_csv(out_compact, index=False)

    print(f"[OK] Wrote {len(full_df)} signals for {target_day_str}")
    print(f"     full   : {out_full}")
    print(f"     compact: {out_compact}")


if __name__ == "__main__":
    main()
