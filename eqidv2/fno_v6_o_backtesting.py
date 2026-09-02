"""Run V6 with one-lot ATM options: LONG buys CE, SHORT buys PE (V6_O)."""

from __future__ import annotations

import argparse

import fno_oi_common as common
import fno_v6_derivative_backtest as engine


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("paper", "canonical"), default="paper")
    parser.add_argument("--from-day", default="")
    parser.add_argument("--through-day", default="")
    parser.add_argument("--max-execution-delay-minutes", type=int, default=5)
    parser.add_argument("--fetch-missing", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.source == "canonical":
        from_day = engine._parse_day(args.from_day) if args.from_day else engine.DEFAULT_HISTORY_FROM
        through_day = engine._parse_day(args.through_day) if args.through_day else engine.DEFAULT_HISTORY_THROUGH
        status = engine.historical_options_availability(from_day=from_day, through_day=through_day)
        out_dir = engine.RESULT_ROOT / f"canonical_{from_day.strftime('%Y%m%d')}_{through_day.strftime('%Y%m%d')}"
        common.atomic_write_json(out_dir / "v6_o_data_availability.json", status)
        print(f"[V6_O] {status['status']}: expired option masters/candles are unavailable; no synthetic result created.")
        print(f"[V6_O] output={out_dir}")
        return 2 if status["status"].startswith("BLOCKED") else 0
    from_day = engine._parse_day(args.from_day) if args.from_day else engine.DEFAULT_PAPER_FROM
    through_day = engine._parse_day(args.through_day) if args.through_day else common.now_ist().date()
    _, summary, out_dir = engine.run_paper_derivative(
        instrument="OPTIONS",
        from_day=from_day,
        through_day=through_day,
        fetch_missing=args.fetch_missing,
        max_delay_minutes=args.max_execution_delay_minutes,
    )
    print(f"[V6_O] output={out_dir}")
    print(
        f"[V6_O] executed={summary['executed_trades']}/{summary['input_trades']} "
        f"net_rs={summary['estimated_net_pnl_rs']:.2f} coverage={summary['coverage_pct']:.2f}%"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
