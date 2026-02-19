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
    python eqidv2_live_combined_analyser_v3_rowbyrow_dateinput.py --date 2026-02-18
    python eqidv2_live_combined_analyser_v3_rowbyrow_dateinput.py --date 2026-02-18,2026-02-19
    python eqidv2_live_combined_analyser_v3_rowbyrow_dateinput.py --start-date 2026-02-18 --end-date 2026-02-21
"""

import os
import argparse
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import Any, Dict, List

import pandas as pd


IST_TZ = ZoneInfo("Asia/Kolkata")

def _parse_dates_arg(s: str):
    """Parse comma-separated YYYY-MM-DD dates."""
    dates = []
    for part in (s or "").split(","):
        part = part.strip()
        if not part:
            continue
        dates.append(datetime.strptime(part, "%Y-%m-%d").date())
    return dates

def _date_range(start_d: date, end_d: date):
    """Inclusive date range."""
    if end_d < start_d:
        raise ValueError("end date is before start date")
    out = []
    cur = start_d
    while cur <= end_d:
        out.append(cur)
        cur = cur.fromordinal(cur.toordinal() + 1)
    return out



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

    ap.add_argument(
        "--date",
        default=None,
        help="YYYY-MM-DD (IST). You can also pass comma-separated dates, e.g. 2026-02-18,2026-02-19",
    )
    ap.add_argument("--start-date", default=None, help="YYYY-MM-DD (IST). Use with --end-date for an inclusive range.")
    ap.add_argument("--end-date", default=None, help="YYYY-MM-DD (IST). Use with --start-date for an inclusive range.")

    ap.add_argument("--tail-rows", type=int, default=2000, help="Tail rows to read per parquet (>= 500 is plenty)")
    ap.add_argument("--min-bars", type=int, default=7, help="Minimum bars required before running logic (v2 uses 7)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    # Decide which date(s) to scan (IST)
    target_days = []
    if args.date:
        target_days.extend(_parse_dates_arg(args.date))

    if args.start_date or args.end_date:
        if not (args.start_date and args.end_date):
            raise SystemExit("ERROR: Provide both --start-date and --end-date (inclusive).")
        start_d = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_d = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        target_days.extend(_date_range(start_d, end_d))

    # Deduplicate, preserve order
    seen = set()
    target_days = [d for d in target_days if not (d in seen or seen.add(d))]

    if not target_days:
        raise SystemExit(
            "ERROR: Please provide --date YYYY-MM-DD (or --start-date/--end-date). Example: --date 2026-02-18"
        )

    _ensure_dir(args.out_dir)

    # Import the v3 live analyser (the one with _latest_entry_signals_for_ticker)
    import eqidv2_live_combined_analyser_v3 as live

    # Point it at the same dir/suffix the user wants (for tickers list)
    live.DIR_15M = args.data_dir
    # IMPORTANT: list_tickers_15m typically uses END_15M suffix — set it too
    if hasattr(live, "END_15M"):
        live.END_15M = args.suffix

    tickers = live.list_tickers_15m()

    if args.verbose:
        print(f"[INFO] tickers={len(tickers)}  data_dir={args.data_dir}  dates={[str(d) for d in target_days]}")

    for target_day in target_days:
        target_day_str = str(target_day)
        if args.verbose:
            print(f"[RUN ] scanning date={target_day_str}")

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
            start_k = max(int(args.min_bars) - 1, 0)
            for k in range(start_k, len(df_day)):
                df_part = df_day.iloc[: k + 1].copy()

                sigs, _checks = live._latest_entry_signals_for_ticker(tkr, df_part, state)  # type: ignore
                if not sigs:
                    continue

                for s in sigs:
                    # Enforce per-side caps
                    short_cap = int(getattr(live, "SHORT_CAP_PER_TICKER_PER_DAY", 1))
                    long_cap = int(getattr(live, "LONG_CAP_PER_TICKER_PER_DAY", 1))
                    cap = short_cap if str(s.side).upper() == "SHORT" else long_cap

                    if not live.allow_signal_today(state, s.ticker, s.side, target_day_str, cap):
                        continue

                    live.mark_signal(state, s.ticker, s.side, target_day_str)

                    signals_rows.append(
                        {
                            "date": target_day_str,
                            "ticker": s.ticker,
                            "side": s.side,
                            "bar_time_ist": str(_to_ist_ts(s.bar_time_ist)) if hasattr(s, "bar_time_ist") else "",
                            "setup": s.setup,
                            "entry_price": float(s.entry_price),
                            "sl_price": float(s.sl_price),
                            "target_price": float(s.target_price),
                            "quality_score": float(s.score),
                            "impulse_type": str(getattr(s, "diagnostics", {}).get("impulse_type", "")),
                            "impulse_idx": int(getattr(s, "diagnostics", {}).get("impulse_idx", -1)),
                        }
                    )

        if not signals_rows:
            print(f"[INFO] No signals found for {target_day_str}.")
            continue

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
