"""Temporary raw-data rebuild for V13-v5 confirmation-policy audit."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import fno_oi_backtest_provenance as provenance
import fno_oi_ema_confirm_sweep as sweep
import fno_v13_corrected_v3_backtest as v3


OUT = Path(__file__).with_name("v13_v5_v7cache")


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    eligibility, calendar, _, regimes, _ = v3._load_eligibility(False, 0.99)
    eligibility = eligibility.loc[
        eligibility["eligible"] & eligibility["day"].le(date(2026, 9, 3))
    ]
    for month, group in eligibility.groupby("required_contract"):
        stem = OUT / str(month)
        if stem.with_suffix(".parquet").is_file() and stem.with_suffix(".npz").is_file():
            print(f"[V13-v5][V7 CACHE] {month}", flush=True)
            continue
        mapped, _ = provenance.load_backtest_universe(
            universe_path=regimes[str(month)], contract_month_contains=str(month)
        )
        days = set(group["day"])
        print(f"[V13-v5][V7 BUILD] {month}: {len(mapped)} contracts/{len(days)} sessions", flush=True)
        signals, paths = sweep.build_signal_table(
            days,
            square_off="1530",
            max_forward_bars=400,
            mapped_universe=mapped,
            confirmation_policy=sweep.CONFIRMATION_POLICY_V7_BREAKOUT,
        )
        signals["contract_month"] = str(month)
        v3.v6._store_cached(stem, signals, paths)
        print(f"[V13-v5][V7 STORED] {month}: {len(signals)} signals", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
