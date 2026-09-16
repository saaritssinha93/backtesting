"""FNO V13 corrected v4: V13-v3 entries with a frozen two-stage exit.

Entry selection is exactly V13-v3.  Every filled position uses a 1.50% initial
stop, books 20% at +1.05%, immediately moves the remaining 80% to breakeven,
and targets +2.60% on the runner.  Stop/breakeven wins ambiguous same-minute
ties and unresolved runners square off at the final cached close (15:30).

This remains an experimental shadow backtest selected on a short history.
"""

from __future__ import annotations

import argparse
import hashlib
import time
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v13_corrected_v3_backtest as v3
import fno_v13_v3_exit_sweep as uniform
import fno_v13_v3_scaleout_sweep as scaleout


STRATEGY_VERSION = "FNO_V13_CORRECTED_V4_TWO_STAGE_150_105P20_BE_260"
EVIDENCE_STATUS = "EXPERIMENTAL_SHADOW_NOT_PROMOTED"
EXPECTED_V13_V3_SHA256 = "85c2ff1c37a342e8e0bc4b73eb115de8ebc5aa7e990db261e46036b68aeafbab"
EXPECTED_SCALEOUT_SHA256 = "2b29d985d747047f2ec270d051b59aea180d131a9903d678e754acdcf0baefa5"

INITIAL_STOP_PCT = 1.50
T1_PCT = 1.05
T1_PARTIAL_PCT = 0.20
RUNNER_TARGET_PCT = 2.60
RUNNER_STOP = "BREAKEVEN"
SQUARE_OFF = "1530"
ORIGINAL_TRAIN_END = date(2026, 8, 13)
ORIGINAL_TEST_END = date(2026, 9, 1)

RESULT_DIR = common.FNO_ROOT / "strategy_research" / "v13_corrected_v4"
TRADES_PATH = RESULT_DIR / "fno_v13_corrected_v4_trades.csv"
DAILY_PATH = RESULT_DIR / "fno_v13_corrected_v4_daily.csv"
DAYWISE_PATH = RESULT_DIR / "fno_v13_corrected_v4_daywise_comparison.csv"
SETUPS_PATH = RESULT_DIR / "fno_v13_corrected_v4_setup_metrics.csv"
PERIOD_PATH = RESULT_DIR / "fno_v13_corrected_v4_period_metrics.csv"
COST_PATH = RESULT_DIR / "fno_v13_corrected_v4_cost_stress.csv"
EXIT_PATH = RESULT_DIR / "fno_v13_corrected_v4_exit_breakdown.csv"
BASELINE_TRADES_PATH = RESULT_DIR / "fno_v13_corrected_v4_v13_v3_trades.csv"
REPORT_PATH = RESULT_DIR / "FNO_V13_CORRECTED_V4_DETAILED_RESULTS.md"
WORKSPACE_REPORT_PATH = Path(__file__).with_name(
    "FNO_V13_CORRECTED_V4_DETAILED_RESULTS.md"
)
PROVENANCE_PATH = RESULT_DIR / "fno_v13_corrected_v4_provenance.json"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def exit_config() -> dict[str, Any]:
    return {
        "initial_stop_pct": INITIAL_STOP_PCT,
        "t1_pct": T1_PCT,
        "partial_pct": T1_PARTIAL_PCT,
        "runner_size_pct": 1.0 - T1_PARTIAL_PCT,
        "runner_target_pct": RUNNER_TARGET_PCT,
        "runner_stop": RUNNER_STOP,
        "square_off": SQUARE_OFF,
        "same_minute_policy": "STOP_OR_BREAKEVEN_WINS_TIES",
    }


def validate_configuration() -> None:
    v3.validate_configuration()
    observed_v3 = _sha256(Path(v3.__file__).resolve())
    observed_scaleout = _sha256(Path(scaleout.__file__).resolve())
    if observed_v3 != EXPECTED_V13_V3_SHA256:
        raise RuntimeError(
            f"V13-v3 source drift: expected {EXPECTED_V13_V3_SHA256}, observed {observed_v3}"
        )
    if observed_scaleout != EXPECTED_SCALEOUT_SHA256:
        raise RuntimeError(
            "Scale-out engine source drift: "
            f"expected {EXPECTED_SCALEOUT_SHA256}, observed {observed_scaleout}"
        )
    expected = {
        "initial_stop_pct": 1.50,
        "t1_pct": 1.05,
        "partial_pct": 0.20,
        "runner_size_pct": 0.80,
        "runner_target_pct": 2.60,
        "runner_stop": "BREAKEVEN",
        "square_off": "1530",
        "same_minute_policy": "STOP_OR_BREAKEVEN_WINS_TIES",
    }
    if exit_config() != expected:
        raise AssertionError("V13-v4 exit configuration drifted.")
    if RESULT_DIR.resolve() in {v3.RESULT_DIR.resolve(), scaleout.RESULT_DIR.resolve()}:
        raise AssertionError("V13-v4 outputs must be isolated.")


def _profit_factor(values: np.ndarray) -> float:
    gains = float(values[values > 0].sum()) if values.size else 0.0
    losses = float(-values[values < 0].sum()) if values.size else 0.0
    return gains / losses if losses else (float("inf") if gains else float("nan"))


def _metrics(
    audit: pd.DataFrame,
    days: list[date],
    *,
    target_mask: pd.Series,
) -> dict[str, Any]:
    filled_mask = audit["filled"].astype(bool)
    filled = audit.loc[filled_mask]
    values = filled["net_return_pct"].to_numpy(float)
    daily = (
        filled.groupby("day")["net_return_pct"]
        .sum()
        .reindex(days, fill_value=0.0)
    )
    curve = np.r_[0.0, daily.to_numpy(float).cumsum()]
    drawdown = curve - np.maximum.accumulate(curve)
    day_values = daily.to_numpy(float)
    return {
        "sessions": len(days),
        "orders": len(audit),
        "fills": int(values.size),
        "wins": int((values > 0).sum()),
        "losses": int((values < 0).sum()),
        "win_rate_pct": float((values > 0).mean() * 100) if values.size else np.nan,
        "target_hits": int((target_mask & filled_mask).sum()),
        "target_hit_rate_pct": float(target_mask.loc[filled_mask].mean() * 100)
        if values.size
        else np.nan,
        "profit_factor": _profit_factor(values),
        "day_profit_factor": _profit_factor(day_values),
        "net_pct": float(values.sum()),
        "expectancy_pct": float(values.mean()) if values.size else np.nan,
        "positive_days": int((day_values > 0).sum()),
        "negative_days": int((day_values < 0).sum()),
        "flat_days": int((day_values == 0).sum()),
        "max_drawdown_pct": float(drawdown.min()),
    }


def _run_v4(
    orders: pd.DataFrame,
    paths: dict,
    *,
    cost_bps: float,
) -> pd.DataFrame:
    audit = scaleout.simulate_scaleout(
        orders,
        paths,
        initial_stop_pct=INITIAL_STOP_PCT,
        t1_pct=T1_PCT,
        partial_pct=T1_PARTIAL_PCT,
        runner_target_pct=RUNNER_TARGET_PCT,
        runner_stop=RUNNER_STOP,
        cost_bps=cost_bps,
    )
    audit["strategy_version"] = STRATEGY_VERSION
    audit["evidence_status"] = EVIDENCE_STATUS
    return audit


def _run_native(
    orders: pd.DataFrame,
    paths: dict,
    *,
    cost_bps: float,
) -> pd.DataFrame:
    audit = uniform.simulate_with_reasons(orders, paths, cost_bps=cost_bps)
    audit["strategy_version"] = v3.STRATEGY_VERSION
    return audit


def _periods(days: list[date]) -> dict[str, list[date]]:
    return {
        "ORIGINAL_TRAIN": [day for day in days if day <= ORIGINAL_TRAIN_END],
        "ORIGINAL_TEST": [
            day for day in days if ORIGINAL_TRAIN_END < day <= ORIGINAL_TEST_END
        ],
        "SEP02_PLUS": [day for day in days if day > ORIGINAL_TEST_END],
        "ALL": days,
    }


def _period_frame(
    native: pd.DataFrame, audit: pd.DataFrame, days: list[date]
) -> pd.DataFrame:
    rows = []
    for period, period_days in _periods(days).items():
        native_subset = native.loc[native["day"].isin(period_days)].copy()
        current_subset = audit.loc[audit["day"].isin(period_days)].copy()
        rows.append(
            {
                "strategy": "V13-v3",
                "period": period,
                **_metrics(
                    native_subset,
                    period_days,
                    target_mask=native_subset["exit_reason"].eq("TARGET"),
                ),
            }
        )
        rows.append(
            {
                "strategy": "V13-v4",
                "period": period,
                **_metrics(
                    current_subset,
                    period_days,
                    target_mask=current_subset["t1_hit"].astype(bool),
                ),
            }
        )
    return pd.DataFrame(rows)


def _cost_frame(
    orders: pd.DataFrame, paths: dict, days: list[date]
) -> pd.DataFrame:
    rows = []
    for cost in (5.0, 10.0, 15.0, 20.0):
        native = _run_native(orders, paths, cost_bps=cost)
        audit = _run_v4(orders, paths, cost_bps=cost)
        rows.append(
            {
                "strategy": "V13-v3",
                "cost_bps": cost,
                **_metrics(
                    native, days, target_mask=native["exit_reason"].eq("TARGET")
                ),
            }
        )
        rows.append(
            {
                "strategy": "V13-v4",
                "cost_bps": cost,
                **_metrics(audit, days, target_mask=audit["t1_hit"].astype(bool)),
            }
        )
    return pd.DataFrame(rows)


def _daily(audit: pd.DataFrame, days: list[date], prefix: str) -> pd.DataFrame:
    frame = audit.copy()
    frame["win"] = frame["filled"] & frame["net_return_pct"].gt(0)
    frame["loss"] = frame["filled"] & frame["net_return_pct"].lt(0)
    if prefix == "v13_v4":
        frame["target_hit"] = frame["filled"] & frame["t1_hit"].astype(bool)
    else:
        frame["target_hit"] = frame["filled"] & frame["exit_reason"].eq("TARGET")
    grouped = frame.groupby("day").agg(
        orders=("sid", "size"),
        fills=("filled", "sum"),
        wins=("win", "sum"),
        losses=("loss", "sum"),
        target_hits=("target_hit", "sum"),
        net_pct=("net_return_pct", "sum"),
    )
    grouped = grouped.reindex(days, fill_value=0).reset_index()
    grouped["cumulative_net_pct"] = grouped["net_pct"].cumsum()
    return grouped.rename(
        columns={column: f"{prefix}_{column}" for column in grouped.columns if column != "day"}
    )


def _session_context(through_day: date) -> pd.DataFrame:
    eligibility, _, _, _, _ = v3._load_eligibility(False, 0.99)
    eligible = eligibility.loc[
        eligibility["eligible"] & eligibility["day"].le(through_day),
        ["day", "required_contract"],
    ].rename(columns={"required_contract": "contract_month"})
    context = v3.load_nifty_first_bar_context(eligible["contract_month"].unique())
    return eligible.merge(
        context[["day", "contract_month", "nifty_first_bar_return_pct"]],
        on=["day", "contract_month"],
        how="left",
        validate="one_to_one",
    )


def _setup_frame(audit: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for setup_id, group in audit.groupby("setup_id", sort=True):
        filled = group.loc[group["filled"]]
        values = filled["net_return_pct"].to_numpy(float)
        first = group.iloc[0]
        rows.append(
            {
                "setup_id": setup_id,
                "signal_end": first["hhmm_int"],
                "side": first["side"],
                "orders": len(group),
                "fills": len(filled),
                "wins": int((values > 0).sum()),
                "losses": int((values < 0).sum()),
                "win_rate_pct": float((values > 0).mean() * 100) if values.size else np.nan,
                "t1_hits": int(filled["t1_hit"].sum()),
                "t1_hit_rate_pct": float(filled["t1_hit"].mean() * 100)
                if values.size
                else np.nan,
                "profit_factor": _profit_factor(values),
                "net_pct": float(values.sum()),
            }
        )
    return pd.DataFrame(rows)


def _exit_frame(audit: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for reason, group in audit.groupby("exit_reason", sort=True):
        values = group["net_return_pct"].dropna().to_numpy(float)
        rows.append(
            {
                "exit_reason": reason,
                "orders": len(group),
                "fills": int(group["filled"].sum()),
                "wins": int((values > 0).sum()),
                "losses": int((values < 0).sum()),
                "net_pct": float(values.sum()),
            }
        )
    return pd.DataFrame(rows)


def _markdown(frame: pd.DataFrame, columns: list[str]) -> str:
    if frame.empty:
        return "_No rows._"
    return frame[[column for column in columns if column in frame.columns]].to_markdown(
        index=False, floatfmt=".3f"
    )


def render_report(
    *,
    days: list[date],
    cost_bps: float,
    native_stats: dict[str, Any],
    stats: dict[str, Any],
    periods: pd.DataFrame,
    costs: pd.DataFrame,
    setups: pd.DataFrame,
    exits: pd.DataFrame,
    daywise: pd.DataFrame,
    audit: pd.DataFrame,
) -> str:
    comparison = pd.DataFrame(
        [
            {
                "metric": metric,
                "v13_v3": native_stats[metric],
                "v13_v4": stats[metric],
                "delta": stats[metric] - native_stats[metric],
            }
            for metric in (
                "orders",
                "fills",
                "wins",
                "losses",
                "win_rate_pct",
                "target_hits",
                "target_hit_rate_pct",
                "profit_factor",
                "net_pct",
                "expectancy_pct",
                "max_drawdown_pct",
            )
        ]
    )
    trade_columns = [
        "day", "hhmm_int", "tradingsymbol", "side", "setup_id", "filled",
        "t1_hit", "exit_reason", "net_return_pct", "price_change_pct",
        "oi_change_pct", "volume_ratio", "body_ratio",
        "nifty_first_bar_return_pct",
    ]
    lines = [
        "# FNO V13 corrected v4 - detailed historical results",
        "",
        "## Verdict",
        "",
        (
            f"Across **{len(days)} sessions** ({days[0]} through {days[-1]}), V13-v4 "
            f"produced **{stats['fills']} fills**, **{stats['win_rate_pct']:.2f}% win rate**, "
            f"**{stats['target_hit_rate_pct']:.2f}% T1-hit rate**, **{stats['profit_factor']:.3f} PF**, "
            f"and **{stats['net_pct']:+.3f}%** summed net return at {cost_bps:.1f} bps."
        ),
        "",
        "This is an experimental shadow selected from the same 25-session history, not a production promotion.",
        "",
        "## Frozen V13-v4 exit configuration",
        "",
        "1. Keep every V13-v3 entry, NIFTY gate, confirmation rule, picker, and OI cap unchanged.",
        "2. Initial stop: 1.50% from the stop-entry trigger.",
        "3. At +1.05%, book 20% and immediately move the remaining 80% stop to breakeven.",
        "4. Runner target: +2.60%; otherwise square off at the final cached close through 15:30.",
        "5. Initial stop wins a same-minute tie with T1; breakeven wins a same-minute tie with the runner target.",
        "6. The configured round-trip cost is deducted once from the weighted whole-position return.",
        "",
        "## V13-v3 versus V13-v4",
        "",
        _markdown(comparison, ["metric", "v13_v3", "v13_v4", "delta"]),
        "",
        "T1-hit rate means the trade reached +1.05% and booked 20%; it does not mean the full position reached +2.60%.",
        "",
        "## Period results",
        "",
        _markdown(
            periods,
            ["strategy", "period", "sessions", "fills", "wins", "win_rate_pct", "target_hits", "target_hit_rate_pct", "profit_factor", "net_pct", "max_drawdown_pct"],
        ),
        "",
        "## Cost stress",
        "",
        _markdown(
            costs,
            ["strategy", "cost_bps", "fills", "win_rate_pct", "target_hit_rate_pct", "profit_factor", "net_pct", "max_drawdown_pct"],
        ),
        "",
        "## Exit breakdown",
        "",
        _markdown(exits, ["exit_reason", "orders", "fills", "wins", "losses", "net_pct"]),
        "",
        "## Per-setup results",
        "",
        _markdown(
            setups,
            ["setup_id", "signal_end", "side", "orders", "fills", "wins", "losses", "win_rate_pct", "t1_hits", "t1_hit_rate_pct", "profit_factor", "net_pct"],
        ),
        "",
        "## Day-wise V13-v3 versus V13-v4",
        "",
        _markdown(
            daywise,
            ["day", "contract_month", "nifty_first_bar_return_pct", "v13_v3_fills", "v13_v3_wins", "v13_v3_target_hits", "v13_v3_net_pct", "v13_v4_fills", "v13_v4_wins", "v13_v4_target_hits", "v13_v4_net_pct", "delta_net_pct", "v13_v4_cumulative_net_pct"],
        ),
        "",
        "## Complete V13-v4 order ledger",
        "",
        _markdown(audit, trade_columns),
        "",
        "## Interpretation",
        "",
        "- Returns are summed filled-trade percentage returns, not a capital-constrained or lot-sized portfolio simulation.",
        "- T1 frequency varies materially by contract regime; the observed result is not yet statistically established above 50%.",
        "- Partial exits add operational complexity and may incur more real slippage than the single round-trip cost model captures.",
        "- Freeze this configuration and forward-test it across at least two new expiry regimes before considering promotion.",
        "",
    ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--through-day",
        default="",
        help="inclusive YYYY-MM-DD; blank uses the latest eligible stored session",
    )
    parser.add_argument("--cost-bps", type=float, default=5.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    validate_configuration()
    eligibility, _, _, _, _ = v3._load_eligibility(False, 0.99)
    eligible_days = eligibility.loc[eligibility["eligible"], "day"]
    if eligible_days.empty:
        raise RuntimeError("No eligible V13-v4 sessions.")
    through_day = (
        pd.Timestamp(args.through_day).date()
        if args.through_day
        else max(eligible_days)
    )
    orders, paths, days = scaleout.load_orders(through_day)
    native = _run_native(orders, paths, cost_bps=args.cost_bps)
    audit = _run_v4(orders, paths, cost_bps=args.cost_bps)
    if len(native) != len(audit) or not native["sid"].equals(audit["sid"]):
        raise AssertionError("V13-v4 changed V13-v3 selections or ordering.")

    native_stats = _metrics(
        native, days, target_mask=native["exit_reason"].eq("TARGET")
    )
    stats = _metrics(audit, days, target_mask=audit["t1_hit"].astype(bool))
    periods = _period_frame(native, audit, days)
    costs = _cost_frame(orders, paths, days)
    setups = _setup_frame(audit)
    exits = _exit_frame(audit)
    daily_v3 = _daily(native, days, "v13_v3")
    daily_v4 = _daily(audit, days, "v13_v4")
    context = _session_context(through_day)
    daywise = daily_v3.merge(daily_v4, on="day", how="outer").merge(
        context, on="day", how="left", validate="one_to_one"
    )
    daywise["delta_net_pct"] = daywise["v13_v4_net_pct"] - daywise["v13_v3_net_pct"]
    daily = daily_v4.copy()
    daily["strategy_version"] = STRATEGY_VERSION

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    common.atomic_write_csv(native, BASELINE_TRADES_PATH)
    common.atomic_write_csv(audit, TRADES_PATH)
    common.atomic_write_csv(daily, DAILY_PATH)
    common.atomic_write_csv(daywise, DAYWISE_PATH)
    common.atomic_write_csv(setups, SETUPS_PATH)
    common.atomic_write_csv(periods, PERIOD_PATH)
    common.atomic_write_csv(costs, COST_PATH)
    common.atomic_write_csv(exits, EXIT_PATH)
    report = render_report(
        days=days,
        cost_bps=args.cost_bps,
        native_stats=native_stats,
        stats=stats,
        periods=periods,
        costs=costs,
        setups=setups,
        exits=exits,
        daywise=daywise,
        audit=audit,
    )
    common.atomic_write_text(REPORT_PATH, report)
    common.atomic_write_text(WORKSPACE_REPORT_PATH, report)
    common.atomic_write_json(
        PROVENANCE_PATH,
        {
            "strategy_version": STRATEGY_VERSION,
            "evidence_status": EVIDENCE_STATUS,
            "generated_at_ist": common.now_ist().isoformat(timespec="seconds"),
            "through_day": str(through_day),
            "sessions": [str(day) for day in days],
            "entry_strategy": v3.STRATEGY_VERSION,
            "exit_config": exit_config(),
            "cost_bps": float(args.cost_bps),
            "v13_v3_source_sha256": _sha256(Path(v3.__file__).resolve()),
            "scaleout_source_sha256": _sha256(Path(scaleout.__file__).resolve()),
            "headline": stats,
            "baseline_v13_v3": native_stats,
            "outputs": {
                "report": str(REPORT_PATH.resolve()),
                "trades": str(TRADES_PATH.resolve()),
                "daily": str(DAILY_PATH.resolve()),
                "daywise": str(DAYWISE_PATH.resolve()),
            },
        },
    )
    print(
        f"[V13-v4] sessions={len(days)} orders={stats['orders']} fills={stats['fills']} "
        f"wins={stats['wins']} WR={stats['win_rate_pct']:.3f}% "
        f"T1={stats['target_hits']}/{stats['fills']} ({stats['target_hit_rate_pct']:.3f}%) "
        f"PF={stats['profit_factor']:.6f} net={stats['net_pct']:+.6f}% "
        f"maxDD={stats['max_drawdown_pct']:.6f}%"
    )
    print(f"[REPORT] {REPORT_PATH}")
    print(f"[DONE] {time.monotonic() - started:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
