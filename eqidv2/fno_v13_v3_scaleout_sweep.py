"""Research two-stage exits for V13-v3 with an explicit >50% T1-hit goal.

Selections, entry triggers, NIFTY gate, 15:30 square-off, and pessimistic
same-minute ordering remain frozen.  This script does not modify V13-v3.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v13_corrected_v3_backtest as v3
import fno_v13_v3_exit_sweep as uniform


RESULT_DIR = common.FNO_ROOT / "strategy_research" / "v13_corrected_v3_scaleout_sweep"
GRID_PATH = RESULT_DIR / "fno_v13_v3_scaleout_grid.csv"
TRADES_PATH = RESULT_DIR / "fno_v13_v3_scaleout_selected_trades.csv"
REPORT_PATH = RESULT_DIR / "FNO_V13_V3_TARGET_HIT_OVER_50_RESULTS.md"
WORKSPACE_REPORT_PATH = Path(__file__).with_name(
    "FNO_V13_V3_TARGET_HIT_OVER_50_RESULTS.md"
)

TRAIN_END = date(2026, 8, 13)
TEST_END = date(2026, 9, 1)


def simulate_scaleout(
    orders: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    *,
    initial_stop_pct: float,
    t1_pct: float,
    partial_pct: float,
    runner_target_pct: float,
    runner_stop: str,
    cost_bps: float,
) -> pd.DataFrame:
    """Pessimistic two-stage replay; cost is charged once to total notional."""

    if runner_stop not in {"BREAKEVEN", "ORIGINAL"}:
        raise ValueError("runner_stop must be BREAKEVEN or ORIGINAL")
    if not 0 < partial_pct <= 1:
        raise ValueError("partial_pct must be in (0, 1]")
    result = orders.copy()
    returns = np.full(len(result), np.nan)
    reasons = np.full(len(result), "UNFILLED", dtype=object)
    t1_hits_out = np.zeros(len(result), dtype=bool)
    cost = cost_bps / 10000.0
    missing = np.iinfo(np.int32).max

    for position, row in enumerate(result.itertuples(index=False)):
        path = paths.get(int(row.sid))
        if path is None:
            continue
        high = np.asarray(path["high"], dtype=float)
        low = np.asarray(path["low"], dtype=float)
        close = np.asarray(path["close"], dtype=float)
        if high.size == 0:
            continue
        trigger = float(row.trigger)
        is_long = row.side == "LONG"
        entries = np.flatnonzero(high >= trigger) if is_long else np.flatnonzero(low <= trigger)
        if not entries.size:
            continue
        entry_index = int(entries[0])
        if is_long:
            initial_stop = trigger * (1 - initial_stop_pct / 100)
            t1_price = trigger * (1 + t1_pct / 100)
            runner_target = trigger * (1 + runner_target_pct / 100)
            stop_hits = np.flatnonzero(low[entry_index:] <= initial_stop)
            t1_hits = np.flatnonzero(high[entry_index:] >= t1_price)
        else:
            initial_stop = trigger * (1 + initial_stop_pct / 100)
            t1_price = trigger * (1 - t1_pct / 100)
            runner_target = trigger * (1 - runner_target_pct / 100)
            stop_hits = np.flatnonzero(high[entry_index:] >= initial_stop)
            t1_hits = np.flatnonzero(low[entry_index:] <= t1_price)
        stop_index = int(stop_hits[0]) if stop_hits.size else missing
        t1_index = int(t1_hits[0]) if t1_hits.size else missing

        if stop_index == t1_index == missing:
            exit_price = float(close[-1])
            gross = exit_price / trigger - 1 if is_long else 1 - exit_price / trigger
            reasons[position] = "EOD_NO_T1"
        elif stop_index <= t1_index:
            gross = -initial_stop_pct / 100
            reasons[position] = "FULL_STOP"
        else:
            t1_hits_out[position] = True
            t1_absolute_index = entry_index + t1_index
            booked_gross = t1_pct / 100
            runner_stop_price = trigger if runner_stop == "BREAKEVEN" else initial_stop
            if is_long:
                runner_stops = np.flatnonzero(low[t1_absolute_index:] <= runner_stop_price)
                runner_targets = np.flatnonzero(high[t1_absolute_index:] >= runner_target)
            else:
                runner_stops = np.flatnonzero(high[t1_absolute_index:] >= runner_stop_price)
                runner_targets = np.flatnonzero(low[t1_absolute_index:] <= runner_target)
            runner_stop_index = int(runner_stops[0]) if runner_stops.size else missing
            runner_target_index = int(runner_targets[0]) if runner_targets.size else missing
            if runner_stop_index == runner_target_index == missing:
                exit_price = float(close[-1])
                runner_gross = (
                    exit_price / trigger - 1 if is_long else 1 - exit_price / trigger
                )
                reasons[position] = "T1_THEN_EOD"
            elif runner_stop_index <= runner_target_index:
                runner_gross = (
                    0.0 if runner_stop == "BREAKEVEN" else -initial_stop_pct / 100
                )
                reasons[position] = (
                    "T1_THEN_BREAKEVEN" if runner_stop == "BREAKEVEN" else "T1_THEN_STOP"
                )
            elif runner_target_index < missing:
                runner_gross = runner_target_pct / 100
                reasons[position] = "RUNNER_TARGET"
            else:
                raise AssertionError("unreachable runner-exit state")
            gross = partial_pct * booked_gross + (1 - partial_pct) * runner_gross
        returns[position] = (gross - cost) * 100

    result["net_return_pct"] = returns
    result["filled"] = result["net_return_pct"].notna()
    result["t1_hit"] = t1_hits_out
    result["exit_reason"] = reasons
    result["initial_stop_pct"] = initial_stop_pct
    result["t1_pct"] = t1_pct
    result["partial_pct"] = partial_pct
    result["runner_target_pct"] = runner_target_pct
    result["runner_stop"] = runner_stop
    return result


def metrics(audit: pd.DataFrame, days: list[date]) -> dict[str, float | int]:
    filled = audit.loc[audit["filled"]]
    values = filled["net_return_pct"].to_numpy(float)
    gains = float(values[values > 0].sum())
    losses = float(-values[values < 0].sum())
    daily = filled.groupby("day")["net_return_pct"].sum().reindex(days, fill_value=0.0)
    curve = np.r_[0.0, daily.to_numpy(float).cumsum()]
    dd = curve - np.maximum.accumulate(curve)
    return {
        "fills": int(values.size),
        "wins": int((values > 0).sum()),
        "win_rate_pct": float((values > 0).mean() * 100),
        "t1_hits": int(filled["t1_hit"].sum()),
        "t1_hit_rate_pct": float(filled["t1_hit"].mean() * 100),
        "profit_factor": gains / losses if losses else np.inf,
        "net_pct": float(values.sum()),
        "expectancy_pct": float(values.mean()),
        "max_drawdown_pct": float(dd.min()),
    }


def period_metrics(audit: pd.DataFrame, days: list[date], prefix: str) -> dict[str, float | int]:
    subset = audit.loc[audit["day"].isin(days)]
    values = subset.loc[subset["filled"], "net_return_pct"].to_numpy(float)
    gains = float(values[values > 0].sum())
    losses = float(-values[values < 0].sum())
    filled = subset.loc[subset["filled"]]
    return {
        f"{prefix}_fills": int(values.size),
        f"{prefix}_win_rate_pct": float((values > 0).mean() * 100) if values.size else np.nan,
        f"{prefix}_t1_hit_rate_pct": float(filled["t1_hit"].mean() * 100) if values.size else np.nan,
        f"{prefix}_pf": gains / losses if losses else (np.inf if gains else 0.0),
        f"{prefix}_net_pct": float(values.sum()),
    }


def load_orders(through_day: date) -> tuple[pd.DataFrame, dict, list[date]]:
    eligibility, calendar, _, regimes, _ = v3._load_eligibility(False, 0.99)
    eligibility = eligibility.loc[
        eligibility["eligible"] & eligibility["day"].le(through_day)
    ]
    by_month: dict[str, list[date]] = {}
    for row in eligibility.to_dict("records"):
        month = str(row["required_contract"])
        if month in regimes:
            by_month.setdefault(month, []).append(row["day"])
    parts = []
    for month in sorted(by_month, key=lambda value: calendar[value]):
        signals, paths, _ = v3._load_or_build_regime(
            month,
            regimes[month],
            sorted(by_month[month]),
            square_off="1530",
            max_forward_bars=400,
            rebuild=False,
        )
        parts.append((signals, paths))
    signals, paths = v3.v6.concat_regimes(parts)
    context = v3.load_nifty_first_bar_context(signals["contract_month"].unique())
    return uniform.selected_orders(signals, context), paths, sorted(set(signals["day"]))


def _csv_grid(spec: str) -> list[float]:
    return [float(value) for value in spec.split(",") if value.strip()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--through-day", default="2026-09-03")
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--stops", default="1.25,1.50,1.75,2.00")
    parser.add_argument(
        "--t1s", default="0.90,0.95,1.00,1.025,1.05,1.075,1.10,1.125,1.15,1.20,1.25"
    )
    parser.add_argument("--partials", default="0.10,0.15,0.20,0.25,0.33,0.50")
    parser.add_argument(
        "--runner-targets", default="2.00,2.25,2.50,2.55,2.60,2.65,2.75,3.00"
    )
    args = parser.parse_args(argv)
    v3.validate_configuration()
    orders, paths, days = load_orders(pd.Timestamp(args.through_day).date())
    train_days = [day for day in days if day <= TRAIN_END]
    test_days = [day for day in days if TRAIN_END < day <= TEST_END]
    latest_days = [day for day in days if day > TEST_END]

    rows: list[dict[str, float | int | str]] = []
    for stop in _csv_grid(args.stops):
        for t1 in _csv_grid(args.t1s):
            for partial in _csv_grid(args.partials):
                for runner_target in _csv_grid(args.runner_targets):
                    for runner_stop in ("BREAKEVEN", "ORIGINAL"):
                        audit = simulate_scaleout(
                            orders,
                            paths,
                            initial_stop_pct=stop,
                            t1_pct=t1,
                            partial_pct=partial,
                            runner_target_pct=runner_target,
                            runner_stop=runner_stop,
                            cost_bps=args.cost_bps,
                        )
                        row = {
                            "initial_stop_pct": stop,
                            "t1_pct": t1,
                            "partial_pct": partial,
                            "runner_target_pct": runner_target,
                            "runner_stop": runner_stop,
                            **metrics(audit, days),
                        }
                        row.update(period_metrics(audit, train_days, "train"))
                        row.update(period_metrics(audit, test_days, "test"))
                        row.update(period_metrics(audit, latest_days, "latest"))
                        rows.append(row)
    grid = pd.DataFrame(rows)
    eligible = grid.loc[
        grid["t1_hit_rate_pct"].gt(50)
        & grid["train_t1_hit_rate_pct"].gt(50)
        & grid["test_t1_hit_rate_pct"].gt(50)
    ].copy()
    eligible = eligible.sort_values(["profit_factor", "net_pct"], ascending=False)
    if eligible.empty:
        raise RuntimeError("No configuration cleared the target-hit constraint.")

    numeric = eligible.iloc[0]
    # Prefer a meaningful first trim and a deployable 0.05%-increment T1 over
    # the numerically best 0.025%-grid point.
    rounded_t1 = np.isclose(eligible["t1_pct"] * 20.0, np.round(eligible["t1_pct"] * 20.0))
    operational_pool = eligible.loc[
        eligible["partial_pct"].ge(0.20) & rounded_t1
    ]
    operational = operational_pool.iloc[0] if not operational_pool.empty else numeric
    selected = simulate_scaleout(
        orders,
        paths,
        initial_stop_pct=float(operational.initial_stop_pct),
        t1_pct=float(operational.t1_pct),
        partial_pct=float(operational.partial_pct),
        runner_target_pct=float(operational.runner_target_pct),
        runner_stop=str(operational.runner_stop),
        cost_bps=args.cost_bps,
    )

    cost_rows = []
    for cost in (5.0, 10.0, 15.0, 20.0):
        audit = simulate_scaleout(
            orders,
            paths,
            initial_stop_pct=float(operational.initial_stop_pct),
            t1_pct=float(operational.t1_pct),
            partial_pct=float(operational.partial_pct),
            runner_target_pct=float(operational.runner_target_pct),
            runner_stop=str(operational.runner_stop),
            cost_bps=cost,
        )
        cost_rows.append({"cost_bps": cost, **metrics(audit, days)})
    cost_frame = pd.DataFrame(cost_rows)

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    common.atomic_write_csv(grid.sort_values("profit_factor", ascending=False), GRID_PATH)
    common.atomic_write_csv(selected, TRADES_PATH)
    display_columns = [
        "initial_stop_pct", "t1_pct", "partial_pct", "runner_target_pct",
        "runner_stop", "t1_hit_rate_pct", "win_rate_pct", "profit_factor",
        "net_pct", "max_drawdown_pct", "train_pf", "test_pf", "latest_net_pct",
    ]
    report = "\n".join(
        [
            "# FNO V13-v3 target-hit-over-50 scale-out research",
            "",
            f"Frozen selections: **{days[0]} through {days[-1]}**, {len(days)} sessions and {int(numeric.fills)} fills at {args.cost_bps:.1f} bps.",
            "",
            "T1 hit rate must exceed 50% in the full, original-train, and original-test samples. Same-minute ambiguity is pessimistic: stop/breakeven wins every tie.",
            "",
            "## Numerical best",
            "",
            pd.DataFrame([numeric])[display_columns].to_markdown(index=False, floatfmt=".3f"),
            "",
            "## Recommended operational shadow (at least 20% booked; T1 on a 0.05% increment)",
            "",
            pd.DataFrame([operational])[display_columns].to_markdown(index=False, floatfmt=".3f"),
            "",
            "## Cost stress for operational shadow",
            "",
            cost_frame[["cost_bps", "win_rate_pct", "t1_hit_rate_pct", "profit_factor", "net_pct", "max_drawdown_pct"]].to_markdown(index=False, floatfmt=".3f"),
            "",
            "## Top qualifying configurations",
            "",
            eligible[display_columns].head(25).to_markdown(index=False, floatfmt=".3f"),
            "",
            "## Warning",
            "",
            "This is a multi-parameter in-sample exit search over only 25 sessions. Keep native V13-v3 unchanged and evaluate this as a frozen forward-only shadow across new sessions and expiries.",
            "",
        ]
    )
    common.atomic_write_text(REPORT_PATH, report)
    common.atomic_write_text(WORKSPACE_REPORT_PATH, report)
    print(
        f"[NUMERIC] SL={numeric.initial_stop_pct:.3f} T1={numeric.t1_pct:.3f} "
        f"partial={numeric.partial_pct:.2f} runner={numeric.runner_target_pct:.3f} "
        f"stop={numeric.runner_stop} T1hit={numeric.t1_hit_rate_pct:.3f}% "
        f"WR={numeric.win_rate_pct:.3f}% PF={numeric.profit_factor:.6f} "
        f"net={numeric.net_pct:+.6f}%"
    )
    print(
        f"[OPERATIONAL] SL={operational.initial_stop_pct:.3f} T1={operational.t1_pct:.3f} "
        f"partial={operational.partial_pct:.2f} runner={operational.runner_target_pct:.3f} "
        f"stop={operational.runner_stop} T1hit={operational.t1_hit_rate_pct:.3f}% "
        f"WR={operational.win_rate_pct:.3f}% PF={operational.profit_factor:.6f} "
        f"net={operational.net_pct:+.6f}%"
    )
    print(f"[REPORT] {REPORT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
