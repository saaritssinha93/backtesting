"""Build an apples-to-apples V6 cash, V6_F and V6_O comparison."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import pandas as pd

import fno_oi_common as common
import fno_v6_derivative_backtest as engine


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _money(value: object) -> str:
    return f"Rs {float(value or 0):,.2f}"


def write_full_report(*, from_day: date, through_day: date, paper_dir: Path) -> Path:
    history_dir = engine.RESULT_ROOT / "canonical_20260527_20260810"
    cash = _load_json(history_dir / "v6_cash_baseline_summary.json")
    futures = _load_json(history_dir / "v6_f_summary.json")
    strict = _load_json(history_dir / "v6_f_liquidity_checked_summary.json")
    common_summary = _load_json(paper_dir / "v6_common_trade_comparison_summary.json")
    comparison = pd.read_csv(paper_dir / "v6_cash_futures_options_comparison.csv")
    option_availability = engine.historical_options_availability(
        from_day=engine.DEFAULT_HISTORY_FROM,
        through_day=engine.DEFAULT_HISTORY_THROUGH,
    )
    lines = [
        "# V6, V6_F and V6_O Backtest Comparison",
        "",
        f"Generated through {through_day.isoformat()} from real local/Kite candles. No synthetic option premiums were used.",
        "",
        "## Execution contract",
        "",
        "- V6 signals, rankings, cash triggers, stops, targets and exit events are unchanged.",
        "- V6_F trades one exchange lot: LONG buys the mapped future; SHORT sells it.",
        "- V6_O trades one exchange lot: LONG buys nearest-expiry ATM CE; SHORT buys nearest-expiry ATM PE.",
        "- ATM is measured from the V6 cash entry price. Equal-distance ties select the lower strike and are recorded.",
        "- Derivative entry/exit uses the next causal one-minute price. The liquidity-checked modes require positive volume within five minutes.",
        "",
        "## Full common historical window: 2026-05-27 to 2026-08-10",
        "",
        "| Variant | Coverage | Wins/Losses | Gross P&L | Costs/charges | Net P&L | Peak capital basis | PF |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
        (
            f"| V6 cash, Rs50k exposure proxy | {cash['fills']}/{cash['orders']} | "
            f"{cash['wins']}/{cash['losses']} | {_money(cash['gross_pnl_rs'])} | "
            f"{_money(cash['comparable_5bps_cost_rs'])} | {_money(cash['cash_50k_exposure_proxy_net_pnl_rs'])} | "
            f"{_money(cash['peak_concurrent_capital_rs'])} recorded capital | {cash['profit_factor']:.4f} |"
        ),
        (
            f"| V6_F exact-event observed-bar parity | {futures['executed_trades']}/{futures['input_trades']} | "
            f"{futures['wins']}/{futures['losses']} | {_money(futures['gross_pnl_rs'])} | "
            f"{_money(futures['estimated_charges_rs'])} | {_money(futures['estimated_net_pnl_rs'])} | "
            f"{_money(futures['peak_concurrent_notional_or_premium_rs'])} full notional | {futures['profit_factor']:.4f} |"
        ),
        (
            f"| V6_F positive-volume sensitivity | {strict['executed_trades']}/{strict['input_trades']} | "
            f"{strict['wins']}/{strict['losses']} | {_money(strict['gross_pnl_rs'])} | "
            f"{_money(strict['estimated_charges_rs'])} | {_money(strict['estimated_net_pnl_rs'])} | "
            f"{_money(strict['peak_concurrent_notional_or_premium_rs'])} full notional | {strict['profit_factor']:.4f} |"
        ),
        f"| V6_O historical | {option_availability['status']} | - | - | - | - | - | - |",
        "",
        f"The exact V6_F replay flags {futures['early_source_path_squareoff_trades']} canonical cash paths that ended at 15:15. "
        f"The positive-volume sensitivity excludes/delays stale event bars and blocks {strict['input_trades'] - strict['executed_trades']} orders in total, including the one original cash no-fill.",
        "",
        "Historical V6_O is unavailable because the May-August expired option masters/tokens and one-minute CE/PE candles were never archived. Fabricating premiums would invalidate the result.",
        "",
        f"## Real-contract common comparison: {from_day.isoformat()} to {through_day.isoformat()}",
        "",
        comparison.to_markdown(index=False, floatfmt=",.2f"),
        "",
        f"The common table contains {len(common_summary['common_signal_ids'])} trades. Before intersection, V6_F covered "
        f"{common_summary['all_signal_futures']['executed_trades']}/{common_summary['all_signal_futures']['input_trades']} and V6_O covered "
        f"{common_summary['all_signal_options']['executed_trades']}/{common_summary['all_signal_options']['input_trades']}. "
        "HYUNDAI PE was rejected because neither entry nor exit had positive-volume evidence within five minutes.",
        "",
        "## Capital interpretation and limits",
        "",
        "- Cash capital is the Rs10,000 per-order PAPER allocation; cash exposure is approximately Rs50,000 per trade.",
        "- Futures capital shown is full contract notional, not actual broker margin. Historical SPAN/exposure snapshots are unavailable.",
        "- Option investment is the premium paid (entry premium x exchange lot).",
        "- Charges use the current normal-funded Zerodha/NSE schedule. Contract-note rounding can differ slightly.",
        "- Bid/ask history is unavailable. Every derivative summary includes an adverse 10-bps-per-leg slippage sensitivity.",
        "- The four-trade option comparison is a smoke test, not statistical evidence of profitability.",
        "",
        "## Re-run commands",
        "",
        "```powershell",
        "python fno_v6_f_backtesting.py --source canonical --through-day 2026-08-10",
        "python fno_v6_f_backtesting.py --source canonical --through-day 2026-08-10 --require-positive-volume",
        f"python fno_v6_f_backtesting.py --source paper --from-day {from_day} --through-day {through_day} --fetch-missing",
        f"python fno_v6_o_backtesting.py --source paper --from-day {from_day} --through-day {through_day} --fetch-missing",
        f"python fno_v6_derivative_comparison.py --from-day {from_day} --through-day {through_day}",
        "```",
        "",
    ]
    path = engine.RESULT_ROOT / f"V6_F_V6_O_COMPARISON_{through_day.strftime('%Y%m%d')}.md"
    common.atomic_write_text(path, "\n".join(lines))
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-day", default=engine.DEFAULT_PAPER_FROM.isoformat())
    parser.add_argument("--through-day", default=common.now_ist().date().isoformat())
    args = parser.parse_args(argv)
    from_day = engine._parse_day(args.from_day)
    through_day = engine._parse_day(args.through_day)
    out_dir = engine.RESULT_ROOT / f"paper_{from_day.strftime('%Y%m%d')}_{through_day.strftime('%Y%m%d')}"
    futures_path = out_dir / "v6_f_summary.json"
    options_path = out_dir / "v6_o_summary.json"
    if not futures_path.exists() or not options_path.exists():
        raise FileNotFoundError("Run both V6_F and V6_O PAPER-source backtests before comparison.")
    futures_summary = json.loads(futures_path.read_text(encoding="utf-8"))
    options_summary = json.loads(options_path.read_text(encoding="utf-8"))
    orders = engine.load_paper_orders(from_day=from_day, through_day=through_day)
    path = engine.write_comparison_report(
        orders=orders,
        futures_summary=futures_summary,
        options_summary=options_summary,
        out_dir=out_dir,
    )
    print(f"[V6_COMPARE] output={path}")
    full_report = write_full_report(from_day=from_day, through_day=through_day, paper_dir=out_dir)
    print(f"[V6_COMPARE] full_report={full_report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
