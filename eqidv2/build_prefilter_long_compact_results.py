"""Create focused CSV views from the audited six-month LONG research tables."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\prefilter_long_5m_gt5pct_20260205_20260804"
)

COMPACT_COLUMNS = [
    "trade_date",
    "ticker",
    "split",
    "membership_slot_ist",
    "membership_hour",
    "selection_rank",
    "selection_bucket",
    "primary_family",
    "signal_time_ist",
    "entry_execution_time_ist",
    "entry_price_source_bar_end_ist",
    "entry_price",
    "atr_pct",
    "RSI",
    "ADX",
    "hit_5pct",
    "first_hit_5pct_time_ist",
    "first_hit_5pct_interval_start_ist",
    "first_hit_5pct_interval_end_ist",
    "first_hit_5pct_time_source",
    "daily_max_price",
    "daily_max_time_ist",
    "daily_max_interval_start_ist",
    "daily_max_interval_end_ist",
    "daily_max_time_source",
    "max_time_resolution",
    "daily_max_tie_count",
    "max_forward_return_pct",
    "eod_close",
    "eod_return_pct",
    "cross_tf_target_agreement",
    "cross_tf_max_diff_bps",
    "large_gap_review_flag",
    "future_extreme_review_flag",
]


def compact(source: str, destination: str) -> int:
    frame = pd.read_csv(BASE / source, low_memory=False)
    missing = sorted(set(COMPACT_COLUMNS) - set(frame.columns))
    if missing:
        raise RuntimeError(f"{source} missing compact columns: {missing}")
    frame[COMPACT_COLUMNS].to_csv(BASE / destination, index=False)
    return len(frame)


def main() -> int:
    outputs = {
        "setup_entry_and_peak_times_compact.csv": compact(
            "setup_entries_with_daily_max.csv",
            "setup_entry_and_peak_times_compact.csv",
        ),
        "setup_gt5pct_movers_entry_and_peak_times.csv": compact(
            "setup_gt5pct_movers.csv",
            "setup_gt5pct_movers_entry_and_peak_times.csv",
        ),
        "all_long_gt5pct_movers_entry_and_peak_times.csv": compact(
            "gt5pct_movers_full_list.csv",
            "all_long_gt5pct_movers_entry_and_peak_times.csv",
        ),
    }
    for name, rows in outputs.items():
        print(f"{name}: {rows:,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
