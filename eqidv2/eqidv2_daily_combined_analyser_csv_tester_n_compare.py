# -*- coding: utf-8 -*-
"""
Run replay mode and compare replay signals with latest runner CSV.
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

import pandas as pd


def run_replay(root: Path, date_str: str) -> Path:
    live_file = root / "eqidv2_live_combined_analyser_csv.py"
    replay_csv = root / "out_eqidv2_live_signals_15m" / f"replay_signals_{date_str}.csv"

    cmd = ["python", str(live_file), "--replay-date", date_str]
    print("RUN:", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=str(root))

    if not replay_csv.exists():
        raise SystemExit(f"Replay CSV not found: {replay_csv}")
    return replay_csv


def latest_runner_csv(root: Path) -> Path:
    files = sorted(
        (root / "outputs").glob("avwap_longshort_trades_ALL_DAYS_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise SystemExit("No runner CSV found in outputs/")
    return files[0]


def compare(date_str: str, replay_csv: Path, runner_csv: Path) -> None:
    rep = pd.read_csv(replay_csv)
    run = pd.read_csv(runner_csv)

    rep["ticker"] = rep["ticker"].astype(str).str.upper()
    rep["side"] = rep["side"].astype(str).str.upper()
    rep["setup"] = rep["setup"].astype(str)
    rep["entry_ts"] = pd.to_datetime(
        rep["signal_entry_datetime_ist"], errors="coerce"
    ).dt.strftime("%Y-%m-%d %H:%M:%S%z")

    run["date"] = pd.to_datetime(run["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    run = run[run["date"] == date_str].copy()
    run["ticker"] = run["ticker"].astype(str).str.upper()
    run["side"] = run["side"].astype(str).str.upper()
    run["setup"] = run["setup"].astype(str)
    run["entry_ts"] = pd.to_datetime(run["entry_time_ist"], errors="coerce").dt.strftime(
        "%Y-%m-%d %H:%M:%S%z"
    )

    rep_set = set(zip(rep["ticker"], rep["side"], rep["setup"], rep["entry_ts"]))
    run_set = set(zip(run["ticker"], run["side"], run["setup"], run["entry_ts"]))

    only_run = sorted(run_set - rep_set)
    only_rep = sorted(rep_set - run_set)

    merged = run[
        ["ticker", "side", "setup", "entry_ts", "entry_price", "sl_price", "target_price"]
    ].merge(
        rep[["ticker", "side", "setup", "entry_ts", "entry_price", "sl_price", "target_price"]],
        on=["ticker", "side", "setup", "entry_ts"],
        how="inner",
        suffixes=("_run", "_rep"),
    )

    price_mismatch = 0
    if not merged.empty:
        mismatch = (
            (merged["entry_price_run"] - merged["entry_price_rep"]).abs() > 1e-6
        ) | (
            (merged["sl_price_run"] - merged["sl_price_rep"]).abs() > 1e-6
        ) | (
            (merged["target_price_run"] - merged["target_price_rep"]).abs() > 1e-6
        )
        price_mismatch = int(mismatch.sum())

    print("\nRESULT")
    print("runner_csv :", runner_csv)
    print("replay_csv :", replay_csv)
    print(
        {
            "date": date_str,
            "runner_count": len(run_set),
            "replay_count": len(rep_set),
            "runner_only": len(only_run),
            "replay_only": len(only_rep),
            "price_mismatch": price_mismatch,
        }
    )
    if only_run:
        print("runner_only_sample:", only_run[:10])
    if only_rep:
        print("replay_only_sample:", only_rep[:10])


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay and compare signals against runner CSV.")
    parser.add_argument("--date", required=True, help="IST date in YYYY-MM-DD format.")
    parser.add_argument(
        "--root",
        default=str(Path(__file__).resolve().parent),
        help="Path to eqidv2 project root.",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    replay_csv = run_replay(root, args.date)
    runner_csv = latest_runner_csv(root)
    compare(args.date, replay_csv, runner_csv)


if __name__ == "__main__":
    main()

