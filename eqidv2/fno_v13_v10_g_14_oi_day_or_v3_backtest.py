"""Run the isolated V14 V3 OI-day OR and body/wick backtest.

The inherited G7 market permissions are always enforced. The original
``fno_v13_v10_g_14_backtest.py`` remains the V14 V1 rollback entrypoint.
"""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from research.g14_oi_day_or_backtest import IST, OUTROOT, run


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    out = args.output_dir or OUTROOT / (
        datetime.now(IST).strftime("%Y%m%dT%H%M%S") + "_oi_day_or_v3_strict"
    )
    run(out, market_mode="strict")
    print(out.resolve())


if __name__ == "__main__":
    main()
