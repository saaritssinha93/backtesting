"""Backtest-only sweep of the G-3 09:50 LONG target; all other rules stay fixed."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_3_contract as contract
import fno_v13_v10_g_3_daily_replay as replay
from research import g3_0950_long_backtest as base


DEFAULT_REFERENCE = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\v13_v10_g_3_0950_long_backtesting\run_20261012_full49_v1"
)
DEFAULT_OCTOBER12 = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\v13_v10_g_3_0950_long_backtesting\oct12_baseline_source_20261012T1600"
)


def verify_reference(run: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    manifest = json.loads((run / "manifest.json").read_text(encoding="utf-8"))
    summary = json.loads((run / "summary.json").read_text(encoding="utf-8"))
    if (manifest.get("variant_id") != "V13-V10-G-3-BT-0950-LONG-V1"
            or manifest.get("execution_authority") is not False
            or summary.get("sessions") != 49 or summary.get("scope") != "BACKTEST_ONLY"):
        raise ValueError("Expected completed 49-session backtest-only reference")
    for filename, digest in manifest["files"].items():
        if Path(filename).name != filename or base.sha(run / filename) != digest:
            raise RuntimeError(f"Reference artifact changed: {filename}")
    return (pd.read_csv(run / "baseline_orders.csv", low_memory=False),
            pd.read_csv(run / "added_0950_orders.csv", low_memory=False))


def _same_orders(actual: pd.DataFrame, expected: pd.DataFrame, name: str) -> None:
    left = actual.set_index("sid").sort_index()
    right = expected.set_index("sid").sort_index()
    if not left.index.equals(right.index) or not left.signal_id.astype(str).equals(right.signal_id.astype(str)):
        raise RuntimeError(f"{name} orders changed from the verified reference")


def _same_reference_ledger(actual: pd.DataFrame, reference: Path) -> None:
    prior = pd.read_csv(reference / "variant_trades.csv", low_memory=False)
    left, right = actual.set_index("sid").sort_index(), prior.set_index("sid").sort_index()
    if not left.index.equals(right.index):
        raise RuntimeError("1% target order identities changed")
    for field in ("filled", "portfolio_executed", "exit_reason"):
        if not left[field].astype(str).equals(right[field].astype(str)):
            raise RuntimeError(f"1% target replay drift: {field}")
    if not np.allclose(left.portfolio_net_profit_rupees.astype(float),
                       right.portfolio_net_profit_rupees.astype(float), atol=1e-6, rtol=0):
        raise RuntimeError("1% target P&L drift")


def _summary(ledger: pd.DataFrame, added_sids: set[int], days: list[date], target: float) -> dict:
    added = ledger.loc[ledger.sid.astype(int).isin(added_sids)]
    original = ledger.loc[~ledger.sid.astype(int).isin(added_sids)]
    all_metric, added_metric, old_metric = (base.metrics(frame) for frame in (ledger, added, original))
    split = days[:33], days[33:]
    day_values = pd.to_datetime(ledger.day).dt.date
    added_days = pd.to_datetime(added.day).dt.date
    row = {"target_pct": target, "sessions": len(days)}
    for prefix, metric in (("all", all_metric), ("added", added_metric), ("existing", old_metric)):
        row.update({f"{prefix}_{key}": value for key, value in metric.items()})
    for label, period in zip(("first33", "last16"), split):
        period_set = set(period)
        for prefix, frame in (("all", ledger.loc[day_values.isin(period_set)]),
                              ("added", added.loc[added_days.isin(period_set)])):
            metric = base.metrics(frame)
            row.update({f"{label}_{prefix}_{key}": value for key, value in metric.items()})
    return row


def run(reference: Path, october12: Path, output: Path, targets: list[float]) -> pd.DataFrame:
    reference, october12, output = (Path(path).resolve() for path in (reference, october12, output))
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"Target sweep output must be new or empty: {output}")
    targets = sorted(set(round(float(value), 6) for value in targets))
    if not targets or any(not math.isfinite(value) or not 0.25 <= value <= 3.0 for value in targets):
        raise ValueError("Targets must be finite percentages between 0.25 and 3.00")
    config = base.configuration(base.DEFAULT_CONFIG)
    replay.verify_production_contract()
    expected_baseline, expected_added = verify_reference(reference)
    signals, days, sources, evidence = base.load_sources(
        base.DEFAULT_HISTORY_PROVENANCE, base.DEFAULT_RECENT_PROVENANCE, october12)
    baseline = contract.select_orders(signals)
    _same_orders(baseline, expected_baseline, "Existing G-3")
    with base.backtest_setup(config):
        pool = signals.loc[signals.signal_end.eq("09:50") & signals.side.eq("LONG")].copy()
        audit = contract.selection_audit(pool)
        added = audit.loc[audit.setup_id.eq("0951_LONG") & audit.v9_selected.eq(True)].copy()
    _same_orders(added, expected_added, "09:50 LONG")
    orders = pd.concat((baseline, added), ignore_index=True)
    added_sids = set(added.sid.astype(int))
    paths = base.selected_paths(orders, sources)
    print(f"Verified {len(days)} sessions and minute paths for {len(orders)} orders", flush=True)
    rows, daily_rows = [], []
    reference_net = None
    for target in targets:
        trial = orders.copy()
        mask = trial.sid.astype(int).isin(added_sids)
        for field in ("native_target_pct", "target_pct"):
            trial.loc[mask, field] = target
        trial_config = copy.deepcopy(config)
        trial_config["new_setup"]["target_pct"] = target
        with base.backtest_setup(trial_config):
            ledger, _ = replay.simulate(trial, paths, days)
        if target == 1.0:
            _same_reference_ledger(ledger, reference)
            reference_net = base.metrics(ledger)["net_pnl_rs"]
        row = _summary(ledger, added_sids, days, target)
        rows.append(row)
        day_values = pd.to_datetime(ledger.day).dt.date
        for day in days:
            subset = ledger.loc[day_values.eq(day)]
            added_subset = subset.loc[subset.sid.astype(int).isin(added_sids)]
            daily_rows.append({"target_pct": target, "day": str(day),
                "all_fills": base.metrics(subset)["fills"],
                "all_net_pnl_rs": base.metrics(subset)["net_pnl_rs"],
                "added_fills": base.metrics(added_subset)["fills"],
                "added_net_pnl_rs": base.metrics(added_subset)["net_pnl_rs"]})
        print(f"target={target:.2f}% added={row['added_fills']} fills "
              f"{row['added_win_rate_pct']:.2f}% wins "
              f"net=Rs {row['all_net_pnl_rs']:.2f}", flush=True)
    if reference_net is None:
        raise ValueError("Target grid must contain the 1.00% reference")
    output.mkdir(parents=True, exist_ok=True)
    table = pd.DataFrame(rows)
    table["delta_vs_1pct_rs"] = table.all_net_pnl_rs - reference_net
    table.to_csv(output / "target_sweep.csv", index=False)
    pd.DataFrame(daily_rows).to_csv(output / "target_daily.csv", index=False)
    summary = {"scope": "BACKTEST_ONLY", "execution_authority": False,
        "created_at_ist": datetime.now(contract.IST).isoformat(),
        "source_reference": str(reference), "source_reference_manifest_sha256": base.sha(reference / "manifest.json"),
        "source_evidence": evidence, "sessions": len(days), "first_date": str(days[0]), "last_date": str(days[-1]),
        "source_gap": "2026-10-01 incomplete immutable input",
        "fixed_rules": {"entry_setup": "09:50 LONG, unchanged", "initial_stop_pct": 1.25,
                        "tightened_stop_pct": 1.0, "tighten_after_minutes": 120,
                        "cost_bps_round_trip": contract.ROUND_TRIP_COST_BPS,
                        "capital_per_entry_rs": contract.CAPITAL_PER_ENTRY_RS,
                        "leverage": contract.LEVERAGE},
        "target_grid_pct": targets, "reference_target_pct": 1.0,
        "replay_reference_parity": True}
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    hashes = {path.name: base.sha(path) for path in output.iterdir() if path.is_file()}
    (output / "manifest.json").write_text(json.dumps({"execution_authority": False,
        "code_sha256": base.sha(Path(__file__)), "files": hashes}, indent=2) + "\n", encoding="utf-8")
    return table


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, default=DEFAULT_REFERENCE)
    parser.add_argument("--october12-run", type=Path, default=DEFAULT_OCTOBER12)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--targets", type=float, nargs="+", required=True)
    args = parser.parse_args()
    table = run(args.reference, args.october12_run, args.output_dir, args.targets)
    winner = table.loc[table.all_net_pnl_rs.idxmax()]
    print(f"Best observed full-period net: {winner.target_pct:.2f}% Rs {winner.all_net_pnl_rs:.2f}", flush=True)
    print(args.output_dir.resolve(), flush=True)


if __name__ == "__main__":
    main()
