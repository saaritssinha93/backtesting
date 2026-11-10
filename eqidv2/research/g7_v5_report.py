"""Combine independently replayed G7 V5 trials and disclose calendar limits."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


EXPERIMENTS = {
    "intraday": "portfolio_sweep_59",
    "opening_025": "portfolio_opening_025",
    "opening_050": "portfolio_opening_050",
}
KEY_VARIANTS = {
    "intraday": "oi_0_body035_strong_moderate",
    "opening_025": "oi_0_body035_opening_250",
    "opening_050": "oi_0_body035_opening_500",
}


def run(root: Path, *, experiments: dict[str, str] | None = None) -> dict:
    experiments = EXPERIMENTS if experiments is None else experiments
    if set(experiments) != set(EXPERIMENTS):
        raise ValueError("Expected the intraday and both opening trial families")
    tables = []
    daywise = []
    for experiment, folder in experiments.items():
        path = root / folder
        summary = json.loads((path / "summary.json").read_text(encoding="utf-8"))
        if summary["sessions"] != 59 or summary["last_day"] != "2026-10-27":
            raise ValueError(f"Incomplete V4 comparison in {experiment}")
        if not summary["baseline_parity"]["passed"]:
            raise ValueError(f"Frozen V4 portfolio control failed: {experiment}")
        table = pd.read_csv(path / "overall_comparison.csv")
        table.insert(0, "experiment", experiment)
        tables.append(table)
        day = pd.read_csv(path / "daywise_comparison.csv")
        day.insert(0, "experiment", experiment)
        daywise.append(day)
    all_rows = pd.concat(tables, ignore_index=True)
    daily_rows = pd.concat(daywise, ignore_index=True)
    control = all_rows.loc[all_rows.variant.eq("baseline")]
    for (_, period), sample in control.groupby(["cost_bps", "period"]):
        if sample.net_pnl_rs.max() - sample.net_pnl_rs.min() > .01:
            raise ValueError("Baseline portfolio varies across V5 experiments")
    all_rows.to_csv(root / "all_experiments_comparison.csv", index=False)
    selected = daily_rows.loc[daily_rows.variant.eq("baseline") & daily_rows.experiment.eq("intraday"),
                              ["day", "net_pnl_rs", "trades", "wins", "losses"]].copy()
    selected = selected.rename(columns={"net_pnl_rs": "v4_net_pnl_rs", "trades": "v4_trades",
                                        "wins": "v4_wins", "losses": "v4_losses"})
    for experiment, variant in KEY_VARIANTS.items():
        sample = daily_rows.loc[daily_rows.experiment.eq(experiment) & daily_rows.variant.eq(variant),
                                ["day", "net_pnl_rs", "delta_net_pnl_rs", "trades", "wins", "losses"]].copy()
        sample = sample.rename(columns={col: f"{experiment}_{col}" for col in sample if col != "day"})
        selected = selected.merge(sample, on="day", how="left", validate="one_to_one")
    if len(selected) != 59 or selected.isna().any().any():
        raise ValueError("Daywise V5 comparison is incomplete")
    selected.to_csv(root / "daywise_key_variants.csv", index=False)
    five = all_rows.loc[all_rows.period.eq("all") & all_rows.cost_bps.eq(5)]
    best = five.sort_values("delta_net_pnl_rs", ascending=False).iloc[0]
    initial_sessions = set(selected.day.iloc[:8])
    best_day = daily_rows.loc[daily_rows.experiment.eq(best.experiment) & daily_rows.variant.eq(best.variant)]
    result = dict(scope="BACKTEST_ONLY", execution_authority=False,
                  selected_policy="V4_UNCHANGED", cutoff_requested="2026-10-30",
                  verified_first_session="2026-07-29", verified_last_session="2026-10-27",
                  verified_sessions=59, unverified_sessions=["2026-10-28", "2026-10-29", "2026-10-30"],
                  independent_holdout=False, trial_families=list(experiments),
                  best_observed_5bps=dict(experiment=best.experiment, variant=best.variant,
                    net_pnl_rs=float(best.net_pnl_rs), delta_net_pnl_rs=float(best.delta_net_pnl_rs),
                    incremental_pnl_after_first_eight_sessions_rs=float(best_day.loc[~best_day.day.isin(initial_sessions), "delta_net_pnl_rs"].sum()),
                    october_incremental_pnl_rs=float(best_day.loc[best_day.day.str.startswith("2026-10"), "delta_net_pnl_rs"].sum())),
                  reason_for_no_promotion="Best apparent gain is two trades, with almost all profit on an after-dated-universe session; October and losing-day count do not improve.")
    (root / "report_summary.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("outputs/g7_v5_20261102"))
    args = parser.parse_args()
    print(json.dumps(run(args.root), indent=2))


if __name__ == "__main__":
    main()
