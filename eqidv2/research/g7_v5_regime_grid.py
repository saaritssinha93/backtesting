"""Count causal market-regime coverage for a small, declared parameter grid."""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


GRID = (
    (0.50, 0.10, 65, 60),
    (0.50, 0.05, 60, 55),
    (0.50, 0.00, 55, 50),
    (0.50, -0.05, 55, 45),
    (0.25, 0.00, 55, 50),
    (0.25, 0.00, 50, 50),
    (0.25, -0.05, 50, 45),
    (0.00, 0.00, 55, 50),
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("market_csv", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    frame = pd.read_csv(args.market_csv).sort_values(["day", "clock"])
    results = []
    for day_gain, momentum, breadth_prev, breadth_open in GRID:
        first = (frame.nifty_day_gain_pct.ge(day_gain) & frame.nifty_from_open_pct.gt(0)
                 & frame.nifty_last15_pct.ge(momentum) & frame.nifty_above_ema20
                 & frame.breadth_above_prior_pct.ge(breadth_prev)
                 & frame.breadth_above_open_pct.ge(breadth_open)
                 & frame.coverage_pct.ge(95))
        active = first & first.groupby(frame.day).shift(fill_value=False) & frame.clock.ge("09:40")
        results.append(dict(day_gain_min_pct=day_gain, momentum_min_pct=momentum,
                            breadth_prior_min_pct=breadth_prev,
                            breadth_open_min_pct=breadth_open, active_checks=int(active.sum()),
                            active_days=int(active.groupby(frame.day).any().sum()),
                            active_0950_days=int((active & frame.clock.eq("09:50")).sum()),
                            active_1045_days=int((active & frame.clock.eq("10:45")).sum())))
    result = pd.DataFrame(results)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(args.output, index=False)
    print(result.to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
