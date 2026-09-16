"""SL/target sweep for the V13-v5 one-lot ATM option replay.

The optimizer deliberately reuses ``simulate_option_native_trade`` so every
candidate follows the same entry, liquidity, intrabar-ordering, time-exit and
cost assumptions as the main options backtest.  It optimizes the full-lot exit
variant because a single exchange lot cannot be split into partial exits.
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v13_v5_options_backtest as backtest


OUTPUT_ROOT = backtest.V13_V5_ROOT / "options_sl_target_optimization"


def grid_values(start: float, stop: float, step: float) -> list[float]:
    count = int(round((stop - start) / step))
    return [round(start + index * step, 10) for index in range(count + 1)]


def safe_profit_factor(values: pd.Series) -> float:
    return backtest.profit_factor(pd.to_numeric(values, errors="coerce"))


def subset_metrics(frame: pd.DataFrame, prefix: str) -> dict[str, float | int]:
    pnl = pd.to_numeric(frame["net_pnl_rupees"], errors="coerce")
    return {
        f"{prefix}_trades": int(len(frame)),
        f"{prefix}_wins": int((pnl > 0).sum()),
        f"{prefix}_win_rate_pct": float((pnl > 0).mean() * 100.0) if len(frame) else np.nan,
        f"{prefix}_net_pnl_rupees": float(pnl.sum()),
        f"{prefix}_profit_factor": safe_profit_factor(pnl),
    }


def evaluate_candidate(
    ready: pd.DataFrame,
    option_cache: dict[str, pd.DataFrame],
    *,
    stop_pct: float,
    target_pct: float,
    cost_bps: float,
) -> dict[str, float | int]:
    variant = backtest.OPTION_VARIANTS[0]
    results: list[dict[str, object]] = []
    for _, row in ready.iterrows():
        symbol = str(row["option_tradingsymbol"]).strip().upper()
        results.append(
            backtest.simulate_option_native_trade(
                row,
                option_cache[symbol],
                variant,
                cost_bps=cost_bps,
                initial_stop_pct=stop_pct,
                first_target_pct=target_pct,
                runner_target_pct=target_pct,
            )
        )
    trades = pd.DataFrame(results)
    executed = trades.loc[trades["execution_status"].eq("EXECUTED")].copy()
    pnl = pd.to_numeric(executed["net_pnl_rupees"], errors="coerce")
    outlay = pd.to_numeric(executed["one_lot_premium_outlay_rupees"], errors="coerce")
    daily = executed.groupby("day", sort=True)["net_pnl_rupees"].sum().astype(float)
    cumulative = daily.cumsum()
    running_peak = np.maximum.accumulate(np.r_[0.0, cumulative.to_numpy(float)])[1:]
    drawdowns = cumulative.to_numpy(float) - running_peak
    peak_cash, peak_positions, _ = backtest.peak_concurrent_premium(executed)
    periods = executed["day"].map(backtest._period)
    validation = executed.loc[periods.eq("VALIDATION")]
    pseudo_test = executed.loc[periods.eq("PSEUDO_TEST")]
    time_exit_count = int(executed["exit_reason"].astype(str).str.startswith("TIME_EXIT").sum())

    metrics: dict[str, float | int] = {
        "stop_pct": stop_pct,
        "target_pct": target_pct,
        "reward_risk_ratio": target_pct / stop_pct,
        "executed_trades": int(len(executed)),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl < 0).sum()),
        "win_rate_pct": float((pnl > 0).mean() * 100.0),
        "target_hits": int(executed["target_hit"].astype(bool).sum()),
        "target_hit_rate_pct": float(executed["target_hit"].astype(bool).mean() * 100.0),
        "stop_hits": int(executed["stop_hit"].astype(bool).sum()),
        "stop_hit_rate_pct": float(executed["stop_hit"].astype(bool).mean() * 100.0),
        "time_exits": time_exit_count,
        "net_pnl_rupees": float(pnl.sum()),
        "average_trade_net_pnl_rupees": float(pnl.mean()),
        "profit_factor": safe_profit_factor(pnl),
        "sum_entry_premium_outlay_rupees": float(outlay.sum()),
        "net_return_on_sum_entry_premium_pct": float(pnl.sum() / outlay.sum() * 100.0),
        "peak_concurrent_premium_outlay_rupees": peak_cash,
        "maximum_concurrent_positions": peak_positions,
        "maximum_drawdown_rupees": float(-drawdowns.min()) if len(drawdowns) else 0.0,
        "positive_days": int((daily > 0).sum()),
        "negative_days": int((daily < 0).sum()),
        "positive_day_rate_pct": float((daily > 0).mean() * 100.0),
        "worst_day_pnl_rupees": float(daily.min()),
        "same_bar_ambiguous_trades": int(executed["same_bar_ambiguous"].astype(bool).sum()),
    }
    metrics.update(subset_metrics(validation, "validation"))
    metrics.update(subset_metrics(pseudo_test, "pseudo_test"))
    return metrics


def add_ranking_fields(results: pd.DataFrame) -> pd.DataFrame:
    ranked = results.copy()
    finite_pf = ranked["profit_factor"].replace([np.inf, -np.inf], np.nan)
    pf_cap = float(finite_pf.quantile(0.99)) if finite_pf.notna().any() else 1.0
    ranked["profit_factor_for_rank"] = ranked["profit_factor"].replace(np.inf, pf_cap).clip(upper=pf_cap)
    ranked["balanced_score"] = (
        0.30 * ranked["net_pnl_rupees"].rank(pct=True)
        + 0.20 * ranked["profit_factor_for_rank"].rank(pct=True)
        + 0.15 * ranked["win_rate_pct"].rank(pct=True)
        + 0.15 * ranked["target_hit_rate_pct"].rank(pct=True)
        + 0.10 * ranked["validation_net_pnl_rupees"].rank(pct=True)
        + 0.05 * ranked["positive_day_rate_pct"].rank(pct=True)
        + 0.05 * (-ranked["maximum_drawdown_rupees"]).rank(pct=True)
    )
    ranked["passes_robustness_filter"] = (
        ranked["net_pnl_rupees"].gt(0)
        & ranked["validation_net_pnl_rupees"].gt(0)
        & ranked["pseudo_test_net_pnl_rupees"].gt(0)
        & ranked["profit_factor"].ge(1.5)
        & ranked["win_rate_pct"].ge(60.0)
        & ranked["target_hit_rate_pct"].ge(20.0)
        & ranked["same_bar_ambiguous_trades"].eq(0)
    )
    ranked["balanced_rank"] = ranked["balanced_score"].rank(method="min", ascending=False).astype(int)
    return ranked.sort_values(["balanced_score", "net_pnl_rupees"], ascending=False).reset_index(drop=True)


def render_report(results: pd.DataFrame, *, stop_step: float, target_step: float) -> str:
    robust = results.loc[results["passes_robustness_filter"]].copy()
    recommended = robust.head(1) if not robust.empty else results.head(1)
    columns = [
        "stop_pct", "target_pct", "reward_risk_ratio", "win_rate_pct",
        "target_hit_rate_pct", "profit_factor", "net_pnl_rupees",
        "average_trade_net_pnl_rupees", "maximum_drawdown_rupees",
        "validation_net_pnl_rupees", "pseudo_test_net_pnl_rupees",
        "same_bar_ambiguous_trades", "balanced_score",
    ]
    leaders = {
        "Balanced recommendation": recommended,
        "Highest net profit": results.sort_values("net_pnl_rupees", ascending=False).head(10),
        "Highest profit factor": results.loc[results["net_pnl_rupees"].gt(0)].sort_values(
            ["profit_factor", "net_pnl_rupees"], ascending=False
        ).head(10),
        "Highest target-hit rate among profitable PF >= 1.5": results.loc[
            results["profit_factor"].ge(1.5) & results["net_pnl_rupees"].gt(0)
        ].sort_values(["target_hit_rate_pct", "net_pnl_rupees"], ascending=False).head(10),
        "Highest win rate among profitable PF >= 1.5": results.loc[
            results["profit_factor"].ge(1.5) & results["net_pnl_rupees"].gt(0)
        ].sort_values(["win_rate_pct", "net_pnl_rupees"], ascending=False).head(10),
    }
    parts = [
        "# V13-V5 Options SL/Target Optimization\n",
        "## Scope\n",
        f"- Full-lot, one-lot option exit; cost proxy {backtest.DEFAULT_COST_BPS:.1f} bps.\n",
        f"- Practical grid steps: SL {stop_step:g} percentage points; target {target_step:g} percentage points.\n",
        "- The same causal entry, one-minute OHLC ordering and 15:16 time exit as the main option runner are reused.\n",
        "- Only 18 READY option trades are available. Rankings are exploratory and are not out-of-sample proof.\n",
        "- The balanced score weights net P&L 30%, PF 20%, win rate 15%, target-hit rate 15%, validation P&L 10%, positive-day rate 5%, and drawdown 5%.\n",
        f"- Robustness-filtered candidates: {int(results['passes_robustness_filter'].sum())} of {len(results)}.\n",
    ]
    for heading, frame in leaders.items():
        parts.extend([f"\n## {heading}\n", frame[columns].to_markdown(index=False, floatfmt=".3f"), "\n"])
    return "\n".join(parts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=backtest.DATA_ROOT)
    parser.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    parser.add_argument("--stop-min", type=float, default=5.0)
    parser.add_argument("--stop-max", type=float, default=50.0)
    parser.add_argument("--stop-step", type=float, default=2.5)
    parser.add_argument("--target-min", type=float, default=5.0)
    parser.add_argument("--target-max", type=float, default=100.0)
    parser.add_argument("--target-step", type=float, default=2.5)
    parser.add_argument("--cost-bps", type=float, default=backtest.DEFAULT_COST_BPS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_root = args.data_root.resolve()
    output_root = args.output_root.resolve()
    coverage_path = data_root / "audit" / "option_trade_coverage_and_capital.csv"
    raw_options_dir = data_root / "raw_options_1m"
    coverage = pd.read_csv(coverage_path)
    coverage["day"] = pd.to_datetime(coverage["day"], errors="coerce").dt.date
    ready = coverage.loc[coverage["coverage_state"].eq(backtest.DEFAULT_READY_COVERAGE_STATE)].copy()
    backtest.RAW_OPTIONS_DIR = raw_options_dir
    option_cache: dict[str, pd.DataFrame] = {}
    for symbol in sorted(ready["option_tradingsymbol"].astype(str).str.upper().unique()):
        option_cache[symbol] = backtest.load_option_candles(symbol, option_cache)

    rows: list[dict[str, float | int]] = []
    for stop_pct in grid_values(args.stop_min, args.stop_max, args.stop_step):
        for target_pct in grid_values(args.target_min, args.target_max, args.target_step):
            if target_pct < stop_pct:
                continue
            rows.append(
                evaluate_candidate(
                    ready,
                    option_cache,
                    stop_pct=stop_pct,
                    target_pct=target_pct,
                    cost_bps=args.cost_bps,
                )
            )
    results = add_ranking_fields(pd.DataFrame(rows))
    output_root.mkdir(parents=True, exist_ok=True)
    csv_path = output_root / "fno_v13_v5_options_sl_target_sweep.csv"
    report_path = output_root / "V13_V5_OPTIONS_SL_TARGET_OPTIMIZATION.md"
    common.atomic_write_csv(results, csv_path)
    common.atomic_write_text(
        report_path,
        render_report(results, stop_step=args.stop_step, target_step=args.target_step),
    )
    best = results.loc[results["passes_robustness_filter"]].head(1)
    if best.empty:
        best = results.head(1)
    row = best.iloc[0]
    print(
        f"[OPTIONS OPT] candidates={len(results)} recommended=SL {row['stop_pct']:.1f}% / "
        f"target {row['target_pct']:.1f}% WR={row['win_rate_pct']:.3f}% "
        f"target_hit={row['target_hit_rate_pct']:.3f}% PF={row['profit_factor']:.6f} "
        f"net={row['net_pnl_rupees']:+.2f}"
    )
    print(f"[OPTIONS OPT][CSV] {csv_path}")
    print(f"[OPTIONS OPT][REPORT] {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
