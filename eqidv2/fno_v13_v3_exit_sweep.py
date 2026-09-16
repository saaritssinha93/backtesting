"""Uniform stop/target sweep for the frozen V13-v3 signal selections.

This is an isolated research diagnostic.  It does not edit V13-v3.  Every
candidate uses the same orders, fill trigger, pessimistic same-bar tie rule,
15:30 square-off, and 5 bps cost as V13-v3; only SL and target distances vary.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_ema_confirm_sweep as simulator
import fno_v5_hybrid_backtest as replay
import fno_v13_corrected_v2_backtest as v13_v2
import fno_v13_corrected_v3_backtest as v3


RESULT_DIR = common.FNO_ROOT / "strategy_research" / "v13_corrected_v3_exit_sweep"
ALL_RESULTS_PATH = RESULT_DIR / "fno_v13_v3_uniform_exit_sweep.csv"
PARETO_PATH = RESULT_DIR / "fno_v13_v3_uniform_exit_pareto.csv"
REPORT_PATH = RESULT_DIR / "FNO_V13_V3_EXIT_SWEEP_RESULTS.md"
WORKSPACE_REPORT_PATH = Path(__file__).with_name("FNO_V13_V3_EXIT_SWEEP_RESULTS.md")

TRAIN_END = date(2026, 8, 13)
TEST_START = date(2026, 8, 14)
TEST_END = date(2026, 9, 1)


def _grid(spec: str) -> list[float]:
    values = sorted({round(float(value), 6) for value in spec.split(",") if value.strip()})
    if not values or min(values) <= 0:
        raise ValueError("SL and target grids require positive values.")
    return values


def selected_orders(signals: pd.DataFrame, context: pd.DataFrame) -> pd.DataFrame:
    annotated = v3.annotate_nifty_gate(signals, context)
    gated = annotated.loc[annotated["nifty_first_bar_gate_pass"]].copy()
    policy = v13_v2.POLICIES[v3.BASE_POLICY_NAME]
    policy_signals = v13_v2.apply_policy(gated, policy)
    parts: list[pd.DataFrame] = []
    for setup in v3.active_setups():
        selected = replay.select_setup_rows(policy_signals, setup).copy()
        if selected.empty:
            continue
        selected["setup_id"] = setup.setup_id
        selected["native_stop_pct"] = setup.stop_pct
        selected["native_target_pct"] = setup.target_pct
        parts.append(selected)
    return pd.concat(parts, ignore_index=True).sort_values(
        ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"], kind="stable"
    ).reset_index(drop=True)


def simulate_with_reasons(
    orders: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    *,
    cost_bps: float,
    stop_pct: float | None = None,
    target_pct: float | None = None,
) -> pd.DataFrame:
    """Replay exits and retain TARGET/STOP/SQUARE_OFF/UNFILLED explicitly."""

    result = orders.copy()
    returns = np.full(len(result), np.nan)
    reasons = np.full(len(result), "UNFILLED", dtype=object)
    cost = cost_bps / 10000.0
    for position, row in enumerate(result.itertuples(index=False)):
        path = paths.get(int(row.sid))
        if path is None:
            continue
        high, low, close = path["high"], path["low"], path["close"]
        if high.size == 0:
            continue
        trigger = float(row.trigger)
        long_side = row.side == "LONG"
        touched = np.flatnonzero(high >= trigger) if long_side else np.flatnonzero(low <= trigger)
        if touched.size == 0:
            continue
        entry_index = int(touched[0])
        sl = float(stop_pct if stop_pct is not None else row.native_stop_pct)
        target = float(target_pct if target_pct is not None else row.native_target_pct)
        if long_side:
            stop_price = trigger * (1.0 - sl / 100.0)
            target_price = trigger * (1.0 + target / 100.0)
            stop_hits = np.flatnonzero(low[entry_index:] <= stop_price)
            target_hits = np.flatnonzero(high[entry_index:] >= target_price)
        else:
            stop_price = trigger * (1.0 + sl / 100.0)
            target_price = trigger * (1.0 - target / 100.0)
            stop_hits = np.flatnonzero(high[entry_index:] >= stop_price)
            target_hits = np.flatnonzero(low[entry_index:] <= target_price)
        missing = np.iinfo(np.int32).max
        stop_index = int(stop_hits[0]) if stop_hits.size else missing
        target_index = int(target_hits[0]) if target_hits.size else missing
        if stop_index == target_index == missing:
            exit_price = float(close[-1])
            reason = "SQUARE_OFF"
        elif stop_index <= target_index:
            exit_price = stop_price
            reason = "STOP"
        else:
            exit_price = target_price
            reason = "TARGET"
        gross = exit_price / trigger - 1.0 if long_side else 1.0 - exit_price / trigger
        returns[position] = (gross - cost) * 100.0
        reasons[position] = reason
    result["net_return_pct"] = returns
    result["exit_reason"] = reasons
    result["filled"] = result["net_return_pct"].notna()
    return result


def metrics(audit: pd.DataFrame, days: list[date]) -> dict[str, float | int]:
    filled = audit.loc[audit["filled"]].copy()
    values = filled["net_return_pct"].to_numpy(float)
    profits = float(values[values > 0].sum())
    losses = float(-values[values < 0].sum())
    daily = filled.groupby("day")["net_return_pct"].sum().reindex(days, fill_value=0.0)
    curve = np.r_[0.0, daily.to_numpy(float).cumsum()]
    drawdown = curve - np.maximum.accumulate(curve)
    wins = int((values > 0).sum())
    targets = int(filled["exit_reason"].eq("TARGET").sum())
    stops = int(filled["exit_reason"].eq("STOP").sum())
    eod = int(filled["exit_reason"].eq("SQUARE_OFF").sum())
    count = int(len(filled))
    return {
        "fills": count,
        "wins": wins,
        "losses": int((values < 0).sum()),
        "win_rate_pct": wins / count * 100.0 if count else np.nan,
        "target_hits": targets,
        "target_hit_rate_pct": targets / count * 100.0 if count else np.nan,
        "stop_hits": stops,
        "stop_hit_rate_pct": stops / count * 100.0 if count else np.nan,
        "eod_exits": eod,
        "eod_exit_rate_pct": eod / count * 100.0 if count else np.nan,
        "profit_factor": profits / losses if losses else np.inf,
        "net_pct": float(values.sum()),
        "expectancy_pct": float(values.mean()) if count else np.nan,
        "max_drawdown_pct": float(drawdown.min()),
    }


def _period_metrics(audit: pd.DataFrame, days: list[date], prefix: str) -> dict[str, float]:
    subset = audit.loc[audit["day"].isin(days)]
    values = subset.loc[subset["filled"], "net_return_pct"].to_numpy(float)
    profit = float(values[values > 0].sum())
    loss = float(-values[values < 0].sum())
    return {
        f"{prefix}_fills": int(values.size),
        f"{prefix}_win_rate_pct": float((values > 0).mean() * 100.0) if values.size else np.nan,
        f"{prefix}_target_hit_rate_pct": float(
            subset.loc[subset["filled"], "exit_reason"].eq("TARGET").mean() * 100.0
        ) if values.size else np.nan,
        f"{prefix}_pf": profit / loss if loss else np.inf,
        f"{prefix}_net_pct": float(values.sum()),
    }


def _pareto(frame: pd.DataFrame) -> pd.DataFrame:
    objectives = frame[["win_rate_pct", "target_hit_rate_pct", "profit_factor", "net_pct"]].to_numpy(float)
    keep = np.ones(len(frame), dtype=bool)
    for index, row in enumerate(objectives):
        dominates = np.all(objectives >= row, axis=1) & np.any(objectives > row, axis=1)
        dominates[index] = False
        if dominates.any():
            keep[index] = False
    return frame.loc[keep].sort_values(
        ["win_rate_pct", "target_hit_rate_pct", "profit_factor"], ascending=False
    )


def _table(frame: pd.DataFrame, columns: list[str], rows: int = 10) -> str:
    return frame.loc[:, columns].head(rows).to_markdown(index=False, floatfmt=".3f")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--through-day", default="2026-09-03")
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--stops", default="0.25,0.40,0.50,0.60,0.75,0.80,1.00,1.25,1.50,2.00")
    parser.add_argument("--targets", default="0.50,0.75,1.00,1.25,1.50,1.75,2.00,2.25,2.50,2.75,3.00,3.25,3.50,4.00")
    args = parser.parse_args(argv)
    v3.validate_configuration()
    through_day = pd.Timestamp(args.through_day).date()
    eligibility, calendar, _, regimes, _ = v3._load_eligibility(False, 0.99)
    eligibility = eligibility.loc[
        eligibility["eligible"] & eligibility["day"].le(through_day)
    ].copy()
    days_by_month: dict[str, list[date]] = {}
    for row in eligibility.to_dict("records"):
        month = str(row["required_contract"])
        if month in regimes:
            days_by_month.setdefault(month, []).append(row["day"])
    parts = []
    for month in sorted(days_by_month, key=lambda value: calendar[value]):
        signals, paths, _ = v3._load_or_build_regime(
            month,
            regimes[month],
            sorted(days_by_month[month]),
            square_off="1530",
            max_forward_bars=400,
            rebuild=False,
        )
        parts.append((signals, paths))
    signals, paths = v3.v6.concat_regimes(parts)
    days = sorted(set(signals["day"]))
    context = v3.load_nifty_first_bar_context(signals["contract_month"].unique())
    orders = selected_orders(signals, context)

    train_days = [day for day in days if day <= TRAIN_END]
    test_days = [day for day in days if TEST_START <= day <= TEST_END]
    latest_days = [day for day in days if day > TEST_END]
    native_audit = simulate_with_reasons(orders, paths, cost_bps=args.cost_bps)
    native = metrics(native_audit, days)
    rows = []
    for stop in _grid(args.stops):
        for target in _grid(args.targets):
            audit = simulate_with_reasons(
                orders, paths, cost_bps=args.cost_bps, stop_pct=stop, target_pct=target
            )
            row = {"stop_pct": stop, "target_pct": target, **metrics(audit, days)}
            row.update(_period_metrics(audit, train_days, "train"))
            row.update(_period_metrics(audit, test_days, "test"))
            row.update(_period_metrics(audit, latest_days, "latest"))
            row["win_rate_delta_pp"] = row["win_rate_pct"] - native["win_rate_pct"]
            row["target_hit_delta_pp"] = row["target_hit_rate_pct"] - native["target_hit_rate_pct"]
            row["pf_delta"] = row["profit_factor"] - native["profit_factor"]
            row["net_delta_pct"] = row["net_pct"] - native["net_pct"]
            rows.append(row)
    results = pd.DataFrame(rows)
    results["improves_win_and_target"] = (
        results["win_rate_delta_pp"].gt(0) & results["target_hit_delta_pp"].gt(0)
    )
    results["improves_both_and_pf"] = (
        results["improves_win_and_target"]
        & results["profit_factor"].gt(native["profit_factor"])
    )
    results = results.sort_values(
        ["improves_both_and_pf", "profit_factor", "net_pct", "win_rate_pct"],
        ascending=False,
    ).reset_index(drop=True)
    pareto = _pareto(results)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    common.atomic_write_csv(results, ALL_RESULTS_PATH)
    common.atomic_write_csv(pareto, PARETO_PATH)

    improving_all = results.loc[results["improves_both_and_pf"]].sort_values(
        ["profit_factor", "net_pct", "win_rate_pct"], ascending=False
    )
    improving_win_target = results.loc[results["improves_win_and_target"]].sort_values(
        ["profit_factor", "net_pct", "win_rate_pct"], ascending=False
    )
    guarded = improving_win_target.loc[
        improving_win_target["profit_factor"].ge(2.40)
        & improving_win_target["net_pct"].ge(43.0)
        & improving_win_target["max_drawdown_pct"].ge(native["max_drawdown_pct"])
    ].sort_values(
        ["win_rate_pct", "target_hit_rate_pct", "profit_factor"], ascending=False
    )
    recommended = guarded.iloc[0] if not guarded.empty else None
    larger_target = results.loc[results["target_pct"].ge(3.0)].sort_values(
        ["profit_factor", "net_pct"], ascending=False
    )
    cols = [
        "stop_pct", "target_pct", "fills", "wins", "win_rate_pct",
        "target_hits", "target_hit_rate_pct", "profit_factor", "net_pct",
        "max_drawdown_pct", "train_pf", "test_pf", "latest_net_pct",
    ]
    delta_cols = [
        "stop_pct", "target_pct", "win_rate_pct", "win_rate_delta_pp",
        "target_hit_rate_pct", "target_hit_delta_pp", "profit_factor",
        "pf_delta", "net_pct", "net_delta_pct", "max_drawdown_pct",
        "train_pf", "test_pf", "latest_net_pct",
    ]
    if recommended is None:
        decision = "No pair passed the balanced guardrails. Keep the native exits."
    else:
        decision = (
            f"Best higher-win/higher-target-hit shadow under the declared guardrails: "
            f"**SL {recommended.stop_pct:.2f}% / target {recommended.target_pct:.2f}%**. "
            f"Win rate {recommended.win_rate_pct:.2f}%, target-hit rate "
            f"{recommended.target_hit_rate_pct:.2f}%, PF {recommended.profit_factor:.3f}, "
            f"net {recommended.net_pct:+.3f}%, max drawdown "
            f"{recommended.max_drawdown_pct:.3f}%."
        )
    report = "\n".join(
        [
            "# FNO V13-v3 uniform SL/target sweep",
            "",
            f"History: **{days[0]} through {days[-1]}** ({len(days)} sessions); cost: **{args.cost_bps:.1f} bps**.",
            "",
            "This sweep freezes V13-v3 selections and changes only exits. Target-hit rate counts actual TARGET exits; profitable EOD exits remain wins but are not target hits.",
            "",
            "## Native setup-specific exits",
            "",
            pd.DataFrame([native]).to_markdown(index=False, floatfmt=".3f"),
            "",
            "## Decision",
            "",
            decision,
            "",
            "Guardrails: improve win rate and target-hit rate, PF >= 2.40, net >= +43%, and max drawdown no worse than native. These are descriptive research guardrails, not out-of-sample proof.",
            "",
            "## Best pairs improving win rate, target-hit rate, and PF",
            "",
            _table(improving_all, cols, 15) if not improving_all.empty else "No uniform pair improved all three metrics; higher win/target-hit settings gave up some PF.",
            "",
            "## Best win-rate and target-hit improvements (PF trade-off)",
            "",
            _table(improving_win_target, delta_cols, 15),
            "",
            "## Best pairs with target distance at least 3%",
            "",
            _table(larger_target, cols, 10),
            "",
            "## Multi-objective Pareto frontier",
            "",
            _table(pareto, cols, 20),
            "",
            "## Interpretation warning",
            "",
            "This is an in-sample sweep over only 25 sessions. A winning pair is a research challenger, not evidence for production. Freeze it and validate on new sessions and another expiry regime.",
            "",
        ]
    )
    common.atomic_write_text(REPORT_PATH, report)
    common.atomic_write_text(WORKSPACE_REPORT_PATH, report)
    print(f"[NATIVE] WR={native['win_rate_pct']:.3f}% target={native['target_hit_rate_pct']:.3f}% PF={native['profit_factor']:.3f} net={native['net_pct']:+.3f}%")
    if improving_all.empty:
        print("[BEST] No uniform pair improved win rate, target-hit rate, and PF.")
    else:
        best = improving_all.iloc[0]
        print(f"[BEST] SL={best.stop_pct:.2f}% target={best.target_pct:.2f}% WR={best.win_rate_pct:.3f}% target_hit={best.target_hit_rate_pct:.3f}% PF={best.profit_factor:.3f} net={best.net_pct:+.3f}%")
    if recommended is not None:
        print(f"[GUARDED] SL={recommended.stop_pct:.2f}% target={recommended.target_pct:.2f}% WR={recommended.win_rate_pct:.3f}% target_hit={recommended.target_hit_rate_pct:.3f}% PF={recommended.profit_factor:.3f} net={recommended.net_pct:+.3f}% DD={recommended.max_drawdown_pct:.3f}%")
    print(f"[REPORT] {REPORT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
