"""Recompute every refinement candidate and independently check final accounting."""
from pathlib import Path
import hashlib
import json
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from research.g3_0920_backtest import allocate

read = lambda path: pd.read_csv(path, float_precision="round_trip")
sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()


def audit(folder):
    output = Path(folder)
    summary = json.loads((output / "summary.json").read_text())
    manifest = json.loads((output / "manifest.json").read_text())
    for name, expected in manifest["files"].items():
        assert sha(output / name) == expected, name
    workspace = Path(__file__).resolve().parents[2]
    for name, expected in manifest["code_sha256"].items():
        assert sha(workspace / name) == expected, name
    source = Path(summary["source"]["run_dir"])
    assert sha(source / "manifest.json") == summary["source"]["manifest_sha256"]
    baseline = read(output / "baseline_g4_trades.csv")
    raw = read(source / "historical_profitable70_only_added_raw_trades.csv")
    active = set(raw.setup_id)
    trials = read(output / "refinement_trials.csv")
    previous = summary["initial"]["combined_net_pnl_rs"]
    for iteration, batch in trials.loc[trials.iteration.gt(0)].groupby("iteration", sort=True):
        assert set(batch.removed_setup) == active
        eligible = []
        for _, row in batch.iterrows():
            candidate = raw.loc[raw.setup_id.isin(active-{row.removed_setup})]
            ledger = allocate(pd.concat([baseline, candidate], ignore_index=True))
            executed = ledger.loc[ledger.portfolio_executed]
            added = executed.loc[executed.sid.isin(candidate.sid)]
            assert np.isclose(row.combined_net_pnl_rs, executed.portfolio_net_profit_rupees.sum(), rtol=0, atol=1e-6)
            assert np.isclose(row.added_net_pnl_rs, added.portfolio_net_profit_rupees.sum(), rtol=0, atol=1e-6)
            assert np.isclose(row.improvement_rs, row.combined_net_pnl_rs-previous, rtol=0, atol=1e-6)
            by_setup = added.groupby("setup_id").portfolio_net_profit_rupees
            passing = len(added) >= 5 and added.portfolio_net_profit_rupees.sum() > 0
            passing &= added.portfolio_net_profit_rupees.gt(0).mean() >= .7
            passing &= executed.portfolio_net_profit_rupees.sum() > 0 and executed.portfolio_net_profit_rupees.gt(0).mean() >= .7
            passing &= set(added.setup_id) == active-{row.removed_setup}
            passing &= all(len(pnl) >= 5 and pnl.gt(0).mean() >= .7 and pnl.sum() > 0 for _, pnl in by_setup)
            assert bool(row.eligible) == bool(passing)
            if passing and row.combined_net_pnl_rs-previous > 1e-6:
                eligible.append(row)
        accepted = batch.loc[batch.accepted]
        if eligible:
            chosen = sorted(eligible, key=lambda row: (-row.combined_net_pnl_rs, row.removed_setup))[0]
            assert len(accepted) == 1 and accepted.iloc[0].removed_setup == chosen.removed_setup
            active.remove(chosen.removed_setup)
            previous = chosen.combined_net_pnl_rs
        else:
            assert accepted.empty
    assert active == set(summary["targets"])
    final = read(output / "refined_qualified_trades.csv")
    config = json.loads((output / "refined_config.json").read_text())
    assert {row["setup_id"]: row["target_pct"] for row in config["setups"]} == summary["targets"]
    added = final.loc[final.setup_id.isin(active)]
    for field in ("entry_price", "exit_price", "net_return_pct"):
        expected = raw.loc[raw.setup_id.isin(active)].set_index("sid").sort_index()[field]
        actual = added.set_index("sid").sort_index()[field]
        assert np.allclose(actual, expected, rtol=0, atol=1e-10, equal_nan=True)
    assert final.loc[final.portfolio_executed].portfolio_reserved_capital_before_rupees.max() <= 900000.000001
    assert np.allclose(final.loc[final.portfolio_executed].net_return_pct * 5000,
                       final.loc[final.portfolio_executed].portfolio_net_profit_rupees, rtol=0, atol=1e-6)
    portfolio = read(output / "portfolio_results.csv")
    daily = read(output / "daily_results.csv")
    monthly = read(output / "monthly_results.csv")
    days = sorted(daily.day.unique())
    periods = {"all": days, "train": days[:38], "holdout": days[38:]}
    for scenario in portfolio.scenario.unique():
        ledger = read(output / f"{scenario}_trades.csv")
        for _, row in portfolio.loc[portfolio.scenario.eq(scenario)].iterrows():
            part = ledger.loc[ledger.day.isin(periods[row.period]) & ledger.portfolio_executed]
            if row.scope == "additions":
                part = part.loc[~part.sid.isin(baseline.sid)]
            pnl = part.net_return_pct * 5000.
            assert row.executed_trades == len(part)
            assert row.wins == int(pnl.gt(0).sum())
            assert np.isclose(row.win_rate_pct, 100*pnl.gt(0).mean(), rtol=0, atol=1e-10)
            assert np.isclose(row.net_pnl_rs, pnl.sum(), rtol=0, atol=1e-6)
        assert np.isclose(daily.loc[daily.scenario.eq(scenario)].net_pnl_rs.sum(), ledger.portfolio_net_profit_rupees.sum(), rtol=0, atol=1e-6)
        assert np.isclose(monthly.loc[monthly.scenario.eq(scenario)].net_pnl_rs.sum(), ledger.portfolio_net_profit_rupees.sum(), rtol=0, atol=1e-6)
    return {"status": "PASS", "artifact_hashes": len(manifest["files"]), "code_hashes": len(manifest["code_sha256"]),
            "deletion_trials_recomputed": len(trials)-1, "final_setups": len(active),
            "retained_targets_and_entries_unchanged": True, "capital_limit_verified": True,
            "all_portfolio_and_daily_monthly_totals_reconciled": True, "manifest_sha256": sha(output / "manifest.json")}


if __name__ == "__main__":
    print(json.dumps(audit(sys.argv[1]), indent=2))
