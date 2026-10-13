"""Independent accounting checks for the frozen midday target study outputs."""
from pathlib import Path
import hashlib
import json
import sys

import numpy as np
import pandas as pd


def read(path):
    return pd.read_csv(path, float_precision="round_trip")


def near(actual, expected):
    assert np.isclose(float(actual), float(expected), rtol=0, atol=1e-6), (actual, expected)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def audit(folder):
    root = Path(folder)
    summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    for filename, expected in manifest["files"].items():
        assert digest(root / filename) == expected, filename
    reference = Path(summary["source"]["run_dir"])
    orders = read(reference / "selected_orders.csv")
    baseline = read(reference / "baseline_g4_trades.csv").set_index("sid").sort_index()
    trials = read(root / "target_sweep.csv")
    choices = read(root / "best_targets.csv")
    source = pd.read_parquet(root / "minute_replay_all_targets.parquet")
    assert summary["setups"] == 24 and len(orders) == 345
    assert summary["potential_fills"] == 287
    assert len(trials) == 24 * summary["target_levels"]
    assert len(source) == 345 * summary["target_levels"]
    assert source.groupby("target_pct").filled.sum().eq(287).all()
    assert source.groupby("sid").size().eq(summary["target_levels"]).all()
    for field in ("filled", "entry_path_index", "entry_price", "entry_ts"):
        assert source.groupby("sid")[field].nunique(dropna=False).le(1).all(), field
    for period in ("all", "train", "holdout"):
        part = trials.loc[trials[f"{period}_fills"].gt(0)]
        assert np.allclose(part[f"{period}_win_rate_pct"],
                           100 * part[f"{period}_wins"] / part[f"{period}_fills"], atol=1e-10)
    for _, choice in choices.iterrows():
        part = trials.loc[trials.setup_id.eq(choice.setup_id)]
        for label, period, minimum in (("historical", "all", 5), ("training", "train", 5)):
            eligible = part.loc[part[f"{period}_fills"].ge(minimum)
                & part[f"{period}_win_rate_pct"].ge(70)
                & part[f"{period}_net_pnl_rs"].gt(0)].copy()
            if eligible.empty:
                assert pd.isna(choice[f"{label}_target_pct"])
                continue
            eligible["rounded_pnl"] = eligible[f"{period}_net_pnl_rs"].round(6)
            eligible["rounded_dd"] = eligible[f"{period}_max_drawdown_rs"].round(6)
            best = eligible.sort_values(["rounded_pnl", "rounded_dd", f"{period}_win_rate_pct", "target_pct"],
                                         ascending=[False, True, False, True]).iloc[0]
            near(best.target_pct, choice[f"{label}_target_pct"])
    portfolio = read(root / "portfolio_results.csv")
    setup_results = read(root / "scenario_setup_results.csv")
    daily = read(root / "daily_results.csv")
    monthly = read(root / "monthly_results.csv")
    days = sorted(daily.day.unique())
    assert len(days) == 49
    periods = {"all": days, "train": days[:38], "holdout": days[38:]}
    for scenario in portfolio.scenario.unique():
        frame = read(root / f"{scenario}_trades.csv")
        raw = read(root / f"{scenario}_added_raw_trades.csv")
        added = frame.loc[frame.sid.isin(raw.sid)]
        assert not frame.sid.duplicated().any()
        old = frame.loc[frame.sid.isin(baseline.index)].set_index("sid").sort_index()
        assert old.index.equals(baseline.index)
        for field in ("entry_price", "exit_price", "net_return_pct"):
            assert np.allclose(old[field], baseline[field], rtol=0, atol=1e-10, equal_nan=True)
        actual_changed = int((old.portfolio_executed.ne(baseline.portfolio_executed)
            | ~np.isclose(old.portfolio_net_profit_rupees, baseline.portfolio_net_profit_rupees,
                          rtol=0, atol=1e-6)).sum())
        for field in ("entry_price", "entry_path_index", "entry_ts", "filled"):
            expected = source.loc[source.target_pct.eq(1.)].set_index("sid")[field]
            got = added.set_index("sid")[field]
            if field == "entry_ts":
                expected = pd.to_datetime(expected, utc=True)
                got = pd.to_datetime(got, utc=True)
            pd.testing.assert_series_equal(got.sort_index(), expected.reindex(got.index).sort_index(),
                                           check_names=False, check_dtype=False)
        for scope, subset in (("combined", frame), ("additions", added)):
            for period, cohort in periods.items():
                selected = subset.loc[subset.day.isin(cohort)]
                executed = selected.loc[selected.portfolio_executed]
                stats = portfolio.loc[portfolio.scenario.eq(scenario) & portfolio.scope.eq(scope)
                    & portfolio.period.eq(period)].iloc[0]
                assert stats.selected_orders == len(selected)
                assert stats.native_fills == int(selected.filled.sum())
                assert stats.executed_trades == len(executed)
                pnl = executed.net_return_pct * 5000.
                assert np.allclose(pnl, executed.portfolio_net_profit_rupees, rtol=0, atol=1e-6)
                assert stats.wins == int(pnl.gt(0).sum())
                near(stats.net_pnl_rs, pnl.sum())
                if len(executed):
                    near(stats.win_rate_pct, 100 * pnl.gt(0).mean())
                assert stats.baseline_trades_changed == actual_changed
        for _, row in setup_results.loc[setup_results.scenario.eq(scenario)].iterrows():
            executed = added.loc[added.setup_id.eq(row.setup_id) & added.day.isin(periods[row.period])
                                 & added.portfolio_executed]
            pnl = executed.net_return_pct * 5000.
            assert row.executed_trades == len(executed)
            near(row.net_pnl_rs, pnl.sum())
            passes = bool(len(executed) >= (3 if row.period == "holdout" else 5)
                and pnl.gt(0).mean() >= .7 and pnl.sum() > 0)
            assert bool(row.observed_70_positive) == passes
        near(daily.loc[daily.scenario.eq(scenario)].net_pnl_rs.sum(), frame.portfolio_net_profit_rupees.sum())
        near(daily.loc[daily.scenario.eq(scenario)].added_net_pnl_rs.sum(), added.portfolio_net_profit_rupees.sum())
        near(monthly.loc[monthly.scenario.eq(scenario)].net_pnl_rs.sum(), frame.portfolio_net_profit_rupees.sum())
    ranking = read(root / "scenario_comparison.csv")
    for label, flag in (("portfolio70", "portfolio_70_positive"), ("every_setup70", "every_setup_70_positive")):
        eligible = ranking.loc[ranking[flag]].sort_values(["combined_net_pnl_rs", "scenario"], ascending=[False, True])
        assert summary[f"best_tested_{label}_scenario"] == (eligible.iloc[0].scenario if len(eligible) else None)
    return {"run": str(root), "status": "PASS", "artifacts_hash_verified": len(manifest["files"]),
            "target_levels": summary["target_levels"], "target_trials": len(trials),
            "orders_per_target": len(orders), "native_fills_per_target": 287,
            "scenarios_accounting_verified": int(portfolio.scenario.nunique()),
            "setup_period_results_verified": len(setup_results),
            "best_tested_portfolio70": summary["best_tested_portfolio70_scenario"],
            "best_tested_every_setup70": summary["best_tested_every_setup70_scenario"]}


if __name__ == "__main__":
    print(json.dumps(audit(sys.argv[1]), indent=2))
