"""Reapply frozen native fills to compare simple V21 entry reductions.

The checkpoint books contain already simulated, point-in-time native fills and
their original portfolio outcomes. Filtering their *orders* then calling the
same accepted portfolio constraint engine accounts for freed capital and
reproduces the expanded baseline exactly before any experiment is reported.
No path, stop, target, or execution simulation is altered.
"""
from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import date
import hashlib
import json
from pathlib import Path
import pickle

import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as simulator
from g3_v18_20261102.run import metrics
from g3_v21_expanded_20261104 import selection
from research.g3_backtest_profile import contract


PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parent
CHECKPOINTS = ROOT / "results_full_20261104_checkpoints"
PARENT_RESULTS = ROOT / "results_full_20261104"
POLICY_NAMES = (
    "frozen_v21",
    "expanded_all",
    "existing34_only",
    "existing34_plus_new_late12",
    "existing34_plus_donor_new_after_11_and_late",
    "existing34_plus_donor_new_after_12_and_late",
    "new_donor_cap1",
    "all73_cap2",
    "all73_cap1",
    "expanded_max_positions8",
    "expanded_max_positions9",
)


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _native_config():
    v9 = simulator.g.v9.V9Config(
        portfolio_capital_rupees=contract.PORTFOLIO_CAPITAL_RS,
        capital_per_entry_rupees=contract.CAPITAL_PER_ENTRY_RS,
        leverage_factor=contract.LEVERAGE,
        max_positions=contract.MAX_POSITIONS,
        cost_bps=contract.ROUND_TRIP_COST_BPS)
    return v9.portfolio_config()


def _load_books() -> tuple[pd.DataFrame, pd.DataFrame, list[str], dict]:
    files = sorted(CHECKPOINTS.glob("[0-9][0-9]_*.pkl"))
    parent = json.loads((PARENT_RESULTS / "summary.json").read_text(encoding="utf-8"))
    days = parent["days"]
    if len(files) != len(days) or len(days) != 64:
        raise ValueError("Expected all 64 expanded V21 daily checkpoints")
    frozen, expanded = [], []
    for i, (day, path) in enumerate(zip(days, files)):
        if path.name != f"{i:02d}_{day}.pkl":
            raise ValueError(f"Checkpoint day/order mismatch: {path}")
        record = pickle.loads(path.read_bytes())
        if record["day"] != day or record["policy_fingerprint"] != selection.fingerprint():
            raise ValueError(f"Checkpoint policy mismatch: {path}")
        frozen.append(record["books"]["v21_frozen"])
        expanded.append(record["books"]["v21_expanded_cap3"])
    return (pd.concat(frozen, ignore_index=True, sort=False),
            pd.concat(expanded, ignore_index=True, sort=False), days, parent)


def _core_first_prefix(book: pd.DataFrame, cap: int, *, only_donor_new: bool = False) -> pd.DataFrame:
    if cap not in (1, 2):
        raise ValueError("Ablation cap must be 1 or 2")
    keep = []
    for (_, setup_id), group in book.groupby(["day", "setup_id"], sort=False):
        if only_donor_new and setup_id not in (selection.NEWLY_ENABLED - frozenset(selection.EXTRA_SETUPS)):
            keep.extend(group.index)
            continue
        ranked = group.copy()
        ranked["_rank"] = pd.to_numeric(ranked.v9_rank_in_setup_day, errors="raise")
        ranked = ranked.sort_values(["_rank", "sid"], kind="stable")
        core = ranked.loc[ranked.v10_g_f_core.eq(True)]
        other = ranked.loc[ranked.v10_g_f_core.ne(True)]
        picked = core.head(cap)
        if len(picked) < cap:
            picked = pd.concat([picked, other.head(cap-len(picked))])
        keep.extend(picked.index)
    if len(set(keep)) != len(keep):
        raise ValueError("Duplicate core-first prefix identity")
    out = book.loc[sorted(keep)].copy()
    if out.groupby(["day", "setup_id"]).size().gt(3).any():
        raise ValueError("Prefix exceeded baseline cap")
    if not only_donor_new and out.groupby(["day", "setup_id"]).size().gt(cap).any():
        raise ValueError("Prefix exceeded requested cap")
    return out


def select_policy_rows(expanded: pd.DataFrame, policy: str) -> pd.DataFrame:
    """Pure structural order filter. No P&L or future outcome fields are read."""
    if policy not in POLICY_NAMES or policy == "frozen_v21":
        raise ValueError(f"Unknown expanded policy: {policy}")
    if policy in ("expanded_all", "expanded_max_positions8", "expanded_max_positions9"):
        return expanded.copy()
    if policy == "all73_cap2":
        return _core_first_prefix(expanded, 2)
    if policy == "all73_cap1":
        return _core_first_prefix(expanded, 1)
    if policy == "new_donor_cap1":
        return _core_first_prefix(expanded, 1, only_donor_new=True)
    existing = frozenset(selection.ENABLED_BEFORE)
    extra = frozenset(selection.EXTRA_SETUPS)
    if policy == "existing34_only":
        keep = existing
    elif policy == "existing34_plus_new_late12":
        keep = existing | extra
    elif policy == "existing34_plus_donor_new_after_11_and_late":
        keep = existing | extra | frozenset(k for k in selection.DONOR_SETUPS
                                            if k not in existing and selection.SPECS[k].signal_end >= "11:00")
    elif policy == "existing34_plus_donor_new_after_12_and_late":
        keep = existing | extra | frozenset(k for k in selection.DONOR_SETUPS
                                            if k not in existing and selection.SPECS[k].signal_end >= "12:00")
    else:
        raise ValueError(f"Unhandled policy: {policy}")
    return expanded.loc[expanded.setup_id.isin(keep)].copy()


def _reapply(book: pd.DataFrame, *, max_positions: int | None = None) -> pd.DataFrame:
    config = _native_config()
    if max_positions is not None:
        config = replace(config, max_positions=max_positions)
    out, _ = simulator.g.v9.v6.apply_portfolio_constraints(book, config)
    return out


def _assert_exact_baseline(original: pd.DataFrame, replayed: pd.DataFrame) -> None:
    if len(original) != len(replayed) or not original.sid.eq(replayed.sid).all():
        raise ValueError("Baseline identity changed on portfolio reapplication")
    for column in ("portfolio_executed", "portfolio_status", "portfolio_reject_reason"):
        if not original[column].equals(replayed[column]):
            raise ValueError(f"Baseline portfolio {column} did not reproduce")
    for column in ("portfolio_net_profit_rupees", "portfolio_cost_rupees"):
        if not np.allclose(original[column].to_numpy(float), replayed[column].to_numpy(float),
                           equal_nan=True, atol=.001, rtol=0):
            raise ValueError(f"Baseline portfolio {column} did not reproduce")


def _metric(book: pd.DataFrame, days: list[str]) -> dict:
    value = metrics(book, days)
    daily = (book.loc[book.portfolio_executed.eq(True)]
             .groupby(book.loc[book.portfolio_executed.eq(True), "day"].astype(str).str[:10])
             .portfolio_net_profit_rupees.sum().reindex(days, fill_value=0.0))
    value["negative_days"] = int(daily.lt(0).sum())
    value["positive_days"] = int(daily.gt(0).sum())
    return value


def run(output: Path) -> None:
    output = output.resolve()
    if output.exists() or not output.is_relative_to(PACKAGE) or output == PACKAGE:
        raise ValueError("Output must be a new child of the isolated V21 refinement package")
    frozen, expanded, days, parent = _load_books()
    baseline = _reapply(expanded)
    _assert_exact_baseline(expanded, baseline)
    _assert_exact_baseline(frozen, _reapply(frozen))
    parent_metric = parent["comparison"]["v21_expanded_cap3"]
    observed = _metric(baseline, days)
    for field in ("selected_orders", "trades", "net_pnl_rs", "daily_close_drawdown_rs"):
        if not np.isclose(float(observed[field]), float(parent_metric[field]), rtol=0, atol=.01):
            raise ValueError(f"Expanded baseline metric drift: {field}")
    books = {"frozen_v21": frozen, "expanded_all": baseline}
    for policy in POLICY_NAMES:
        if policy in books:
            continue
        subset = select_policy_rows(expanded, policy)
        if subset.sid.duplicated().any() or not set(subset.sid).issubset(set(expanded.sid)):
            raise ValueError(f"Invalid native order subset: {policy}")
        max_positions = 8 if policy == "expanded_max_positions8" else 9 if policy == "expanded_max_positions9" else None
        books[policy] = _reapply(subset, max_positions=max_positions)
    periods = {
        "full_64": days,
        "through_oct27_59": [d for d in days if d <= "2026-10-27"],
        "after_oct27_5": [d for d in days if d > "2026-10-27"],
    }
    overall = []
    for period, cohort in periods.items():
        for policy in POLICY_NAMES:
            overall.append(dict(period=period, policy=policy, **_metric(books[policy], cohort)))
    daywise = []
    for day in days:
        for policy in POLICY_NAMES:
            outcome = _metric(books[policy], [day])
            daywise.append(dict(day=day, policy=policy,
                selected_orders=outcome["selected_orders"], trades=outcome["trades"],
                wins=outcome["wins"], losses=outcome["losses"],
                pnl_rs=outcome["net_pnl_rs"], negative_day=outcome["negative_days"]))
    output.mkdir(parents=True)
    pd.DataFrame(overall).to_csv(output / "overall.csv", index=False)
    pd.DataFrame(daywise).to_csv(output / "daily.csv", index=False)
    (output / "config.json").write_text(json.dumps(dict(
        policies=list(POLICY_NAMES), periods={k: [v[0], v[-1], len(v)] for k,v in periods.items()},
        selection_fingerprint=selection.fingerprint(),
        portfolio_config=_native_config().__dict__,
        method="Reuse fixed point-in-time staged/native rows, filter by setup/time/core-first rank, then reapply the identical causal portfolio constraints; no new path or exit simulation",
        baseline_reapplication="EXACT_ALL_1320_ORDERS_AND_940_EXECUTIONS",
        execution_authority=False), indent=2, default=str) + "\n", encoding="utf-8")
    (output / "provenance.json").write_text(json.dumps(dict(
        code={str(p):_sha(p) for p in (Path(__file__),Path(selection.__file__))},
        checkpoint_hashes={p.name:_sha(p) for p in sorted(CHECKPOINTS.glob("*.pkl"))},
        parent_summary=str(PARENT_RESULTS / "summary.json"),
        parent_summary_sha256=_sha(PARENT_RESULTS / "summary.json")),
        indent=2) + "\n", encoding="utf-8")
    report = ["# Expanded G3 V21: structural trade-reduction ablations", "",
        "All arms reuse the same saved native fills, targets, stops, costs and minute paths. A filtered arm reapplies the original causal portfolio constraints so freed capital can admit remaining fills. Exact expanded baseline reapplication passed on all 1,320 orders.", "",
        "These policies were devised after inspecting the 64-session history; the five sessions after 27 October are a small later cohort already used for research, not an independent validation set.", "",
        "## Full 64 sessions", "",
        "| Policy | Orders | Executed | Win rate | Net P&L (₹) | Negative days | Worst day (₹) | Daily-close drawdown (₹) |",
        "|:---|---:|---:|---:|---:|---:|---:|---:|"]
    table = pd.DataFrame(overall)
    for row in table.loc[table.period.eq("full_64")].itertuples(index=False):
        report.append(f"| {row.policy} | {row.selected_orders} | {row.trades} | "
                      f"{row.win_rate_pct:.2f}% | {row.net_pnl_rs:,.2f} | {row.negative_days} | "
                      f"{row.worst_day_rs:,.2f} | {row.daily_close_drawdown_rs:,.2f} |")
    report.extend(["", "## Five sessions after 27 October", "",
        "| Policy | Orders | Executed | Win rate | Net P&L (₹) | Negative days | Daily-close drawdown (₹) |",
        "|:---|---:|---:|---:|---:|---:|---:|"])
    for row in table.loc[table.period.eq("after_oct27_5")].itertuples(index=False):
        rate = "n/a" if pd.isna(row.win_rate_pct) else f"{row.win_rate_pct:.2f}%"
        report.append(f"| {row.policy} | {row.selected_orders} | {row.trades} | "
                      f"{rate} | {row.net_pnl_rs:,.2f} | {row.negative_days} | "
                      f"{row.daily_close_drawdown_rs:,.2f} |")
    report.extend(["", "[Overall CSV](overall.csv) | [Day-wise CSV](daily.csv) | "
                   "[Policy config](config.json) | [Provenance](provenance.json)", ""])
    (output / "REPORT.md").write_text("\n".join(report), encoding="utf-8")
    (output / "manifest.json").write_text(json.dumps(
        {p.name:_sha(p) for p in output.iterdir() if p.is_file()}, indent=2) + "\n", encoding="utf-8")
    print(pd.DataFrame(overall).loc[lambda x:x.period.eq("full_64"),
        ["policy","selected_orders","trades","win_rate_pct","net_pnl_rs","negative_days",
         "worst_day_rs","daily_close_drawdown_rs"]].to_string(index=False), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path,
        default=PACKAGE / "results_structural_20261104")
    args = parser.parse_args()
    run(args.output)


if __name__ == "__main__":
    main()
