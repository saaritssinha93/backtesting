"""Run the V6 strategy with one-lot futures execution (V6_F)."""

from __future__ import annotations

import argparse

import fno_v6_derivative_backtest as engine


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("canonical", "paper"), default="canonical")
    parser.add_argument("--from-day", default="")
    parser.add_argument("--through-day", default="")
    parser.add_argument("--max-execution-delay-minutes", type=int, default=5)
    parser.add_argument("--fetch-missing", action="store_true")
    parser.add_argument(
        "--require-positive-volume",
        action="store_true",
        help="For canonical history, wait up to the delay limit for positive-volume entry/exit bars.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.source == "canonical":
        from_day = engine._parse_day(args.from_day) if args.from_day else engine.DEFAULT_HISTORY_FROM
        through_day = engine._parse_day(args.through_day) if args.through_day else engine.DEFAULT_HISTORY_THROUGH
        _, summary, baseline, out_dir = engine.run_canonical_futures(
            from_day=from_day,
            through_day=through_day,
            max_delay_minutes=args.max_execution_delay_minutes,
            require_positive_volume=args.require_positive_volume,
        )
        print(f"[V6_F] output={out_dir}")
        print(f"[V6] fills={baseline['fills']} net_pct_points={baseline['net_return_sum_pct_points']:.6f}")
    else:
        from_day = engine._parse_day(args.from_day) if args.from_day else engine.DEFAULT_PAPER_FROM
        through_day = engine._parse_day(args.through_day) if args.through_day else engine.common.now_ist().date()
        _, summary, out_dir = engine.run_paper_derivative(
            instrument="FUTURES",
            from_day=from_day,
            through_day=through_day,
            fetch_missing=args.fetch_missing,
            max_delay_minutes=args.max_execution_delay_minutes,
        )
        print(f"[V6_F] output={out_dir}")
    print(
        f"[V6_F] executed={summary['executed_trades']}/{summary['input_trades']} "
        f"net_rs={summary['estimated_net_pnl_rs']:.2f} coverage={summary['coverage_pct']:.2f}%"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
