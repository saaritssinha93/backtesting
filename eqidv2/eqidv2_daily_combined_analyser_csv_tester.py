# -*- coding: utf-8 -*-
"""
Off-market replay tester for eqidv2_live_combined_analyser_csv.py.

Generates a deterministic per-slot replay CSV for one IST date.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

import eqidv2_live_combined_analyser_csv as live


def run_replay(date_str: str, dir_15m: Path, out_dir: Path) -> Path:
    live.DIR_15M = str(dir_15m)

    slots = pd.date_range(
        f"{date_str} 09:15",
        f"{date_str} 15:30",
        freq="15min",
        tz="Asia/Kolkata",
    )
    state = {"count": {}, "last_signal": {}}
    rows = []
    seen = set()

    tickers = live.list_tickers_15m()
    print("tickers:", len(tickers))

    for ticker in tickers:
        parquet_file = Path(live.DIR_15M) / f"{ticker}{live.END_15M}"
        df = live.read_parquet_tail(str(parquet_file), n=live.TAIL_ROWS)
        if df is None or df.empty:
            continue

        for slot in slots:
            sigs, _ = live._latest_entry_signals_for_ticker(ticker, df, state, slot)
            for sig in sigs:
                key = (sig.ticker, sig.side, str(sig.bar_time_ist), sig.setup)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    {
                        "date": date_str,
                        "ticker": sig.ticker,
                        "side": sig.side,
                        "bar_time_ist": str(sig.bar_time_ist),
                        "setup": sig.setup,
                        "entry_price": float(sig.entry_price),
                        "sl_price": float(sig.sl_price),
                        "target_price": float(sig.target_price),
                        "quality_score": float(sig.score),
                    }
                )

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"live_replay_{date_str}.csv"
    df_out = pd.DataFrame(rows)
    if not df_out.empty:
        df_out = df_out.sort_values(["bar_time_ist", "side", "ticker"])
    df_out.to_csv(out_path, index=False)
    print("signals:", len(rows))
    print("saved:", out_path)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate off-market replay CSV for one date.")
    parser.add_argument("--date", required=True, help="IST date in YYYY-MM-DD format.")
    parser.add_argument(
        "--dir-15m",
        default=str(Path.cwd() / "stocks_indicators_15min_eq"),
        help="Directory containing 15-min parquet files.",
    )
    parser.add_argument(
        "--out-dir",
        default="daily_signals_offmarket",
        help="Output directory for replay CSV.",
    )
    args = parser.parse_args()

    run_replay(
        date_str=args.date,
        dir_15m=Path(args.dir_15m),
        out_dir=Path(args.out_dir),
    )


if __name__ == "__main__":
    main()

