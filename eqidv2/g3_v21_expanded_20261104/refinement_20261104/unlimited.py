"""Unlimited-capital primary comparison of fixed structural V21 reductions."""
from __future__ import annotations

import argparse
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as simulator
from . import run as finite


PACKAGE = Path(__file__).resolve().parent
POLICIES = tuple(name for name in finite.POLICY_NAMES if not name.startswith("expanded_max_positions"))
CAPITAL = 1_000_000_000_000.0


def _sha(path: Path) -> str:
    h=hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024*1024), b""):
            h.update(block)
    return h.hexdigest()


def _unlimited(book: pd.DataFrame) -> pd.DataFrame:
    cfg = replace(finite._native_config(), portfolio_capital_rupees=CAPITAL,
                  max_positions=None, max_positions_per_symbol=None,
                  max_gross_exposure_rupees=None, max_open_risk_rupees=None)
    out, _ = simulator.g.v9.v6.apply_portfolio_constraints(book, cfg)
    filled = out.filled.astype(bool)
    executed = out.portfolio_executed.astype(bool)
    if not filled.equals(executed):
        raise ValueError("Unlimited capital did not execute every native fill")
    expected = np.where(filled, pd.to_numeric(out.net_profit_rupees).to_numpy(float), 0.0)
    if not np.allclose(out.portfolio_net_profit_rupees.to_numpy(float),expected,
                       rtol=0,atol=.001):
        raise ValueError("Unlimited portfolio P&L differs from direct native fills")
    return out


def run(output: Path) -> None:
    output = output.resolve()
    if output.exists() or not output.is_relative_to(PACKAGE) or output == PACKAGE:
        raise ValueError("Output must be a new child of V21 refinement")
    frozen, expanded, days, parent = finite._load_books()
    books={"frozen_v21":_unlimited(frozen), "expanded_all":_unlimited(expanded)}
    if books["expanded_all"].portfolio_executed.sum()!=expanded.filled.sum():
        raise ValueError("Expanded unlimited fill parity failed")
    for policy in POLICIES:
        if policy in books:
            continue
        subset=finite.select_policy_rows(expanded,policy)
        if subset.sid.duplicated().any() or not set(subset.sid).issubset(set(expanded.sid)):
            raise ValueError(f"Noncausal or duplicate subset: {policy}")
        books[policy]=_unlimited(subset)
    periods={
        "full_64":days,
        "through_oct27_59":[day for day in days if day <= "2026-10-27"],
        "after_oct27_5":[day for day in days if day > "2026-10-27"],
    }
    overall=[dict(period=period,policy=policy,**finite._metric(books[policy],cohort))
             for period,cohort in periods.items() for policy in POLICIES]
    daily=[dict(day=day,policy=policy,**finite._metric(books[policy],[day]))
           for day in days for policy in POLICIES]
    baseline=next(row for row in overall if row["period"]=="full_64" and row["policy"]=="expanded_all")
    if (baseline["trades"]!=int(expanded.filled.sum()) or
        abs(baseline["net_pnl_rs"]-float(expanded.net_profit_rupees.sum()))>.01):
        raise ValueError("Expanded direct native-fill totals did not reproduce")
    output.mkdir(parents=True)
    pd.DataFrame(overall).to_csv(output/"overall.csv",index=False)
    pd.DataFrame(daily).to_csv(output/"daily.csv",index=False)
    config=dict(scope="BACKTEST_ONLY",execution_authority=False,
        source="Expanded V21 checkpoint native staged trades",
        capital_policy="UNLIMITED_PORTFOLIO_CAPITAL_WITH_FIXED_PER_ENTRY_EXPOSURE",
        simulated_portfolio_capital_rs=CAPITAL,
        per_entry_capital_rs=100_000, leverage=5,
        all_native_fills_execute=True,
        source_fills=int(expanded.filled.sum()),
        source_direct_native_net_pnl_rs=float(expanded.net_profit_rupees.sum()),
        baseline_exact_native_fill_reapplication="PASS",
        periods={name:dict(first=cohort[0],last=cohort[-1],sessions=len(cohort))
                 for name,cohort in periods.items()},
        policies=list(POLICIES),
        policy_definitions={
            "frozen_v21":"Original 34 setup pairs and V21 uncapped selection, direct native fills",
            "expanded_all":"All 73 pairs, cap3 per pair, direct native fills",
            "existing34_only":"Only the 34 original V21 pairs, cap3 per pair",
            "existing34_plus_new_late12":"34 original pairs plus 12 new 13:35-14:00 pairs, cap3",
            "existing34_plus_donor_new_after_11_and_late":"34 original plus 27 donor-new pairs with signal >=11:00 plus 12 late pairs",
            "existing34_plus_donor_new_after_12_and_late":"34 original plus 27 donor-new pairs with signal >=12:00 plus 12 late pairs",
            "new_donor_cap1":"27 newly enabled donor pairs capped at1; original34 and new late12 remain cap3",
            "all73_cap2":"All 73 pairs, core-first cap2 per pair",
            "all73_cap1":"All 73 pairs, core-first cap1 per pair",
        },
        warning="Exploratory historical policy search; five later sessions were already observed and are not a fresh holdout")
    (output/"config.json").write_text(json.dumps(config,indent=2,allow_nan=False)+"\n",encoding="utf-8")
    provenance=dict(code={str(p):_sha(p) for p in (Path(__file__),Path(finite.__file__))},
        checkpoint_hashes={p.name:_sha(p) for p in sorted(finite.CHECKPOINTS.glob("*.pkl"))},
        parent_summary=str(finite.PARENT_RESULTS/"summary.json"),
        parent_summary_sha256=_sha(finite.PARENT_RESULTS/"summary.json"))
    (output/"provenance.json").write_text(json.dumps(provenance,indent=2)+"\n",encoding="utf-8")
    table=pd.DataFrame(overall)
    lines=["# Expanded G3 V21 under unlimited portfolio capital", "",
        "All native filled orders execute. Each order retains its saved ₹100,000 capital, 5× exposure, target, stop, cost and execution path. The accepted portfolio constraint engine was reapplied at a ₹1 trillion capital ceiling; executed flags equal native filled flags and portfolio P&L equals direct native fill P&L for every order.", "",
        "These structural policies were selected after examining the 64-session history. The five later sessions are a small retrospective check, not a fresh holdout.", "",
        "## Full 64 sessions", "",
        "| Policy | Orders | Trades | Losing trades | Win rate | Net P&L (₹) | Negative days | Worst day (₹) | Daily-close drawdown (₹) |",
        "|:---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in table.loc[table.period.eq("full_64")].itertuples(index=False):
        lines.append(f"| {row.policy} | {row.selected_orders} | {row.trades} | {row.losses} | "
            f"{row.win_rate_pct:.2f}% | {row.net_pnl_rs:,.2f} | {row.negative_days} | "
            f"{row.worst_day_rs:,.2f} | {row.daily_close_drawdown_rs:,.2f} |")
    lines += ["", "## Five sessions after 27 October", "",
        "| Policy | Trades | Losing trades | Win rate | Net P&L (₹) | Negative days | Daily-close drawdown (₹) |",
        "|:---|---:|---:|---:|---:|---:|---:|"]
    for row in table.loc[table.period.eq("after_oct27_5")].itertuples(index=False):
        rate="n/a" if pd.isna(row.win_rate_pct) else f"{row.win_rate_pct:.2f}%"
        lines.append(f"| {row.policy} | {row.trades} | {row.losses} | {rate} | {row.net_pnl_rs:,.2f} | "
            f"{row.negative_days} | {row.daily_close_drawdown_rs:,.2f} |")
    lines += ["", "## Interpretation", "",
        "For a risk-first reduction, `existing34_plus_new_late12` substantially cuts trades, losing trades, losing days, and daily-close drawdown compared with `expanded_all`, while giving up some net P&L. The higher-P&L `new_donor_cap1` reduces trades and drawdown but still has more negative days than `expanded_all`. These are retrospective comparisons, not a validated trading recommendation.", "",
        "[Overall CSV](overall.csv) | [Day-wise CSV](daily.csv) | "
             "[Policy definitions](config.json) | [Provenance](provenance.json)", ""]
    (output/"REPORT.md").write_text("\n".join(lines),encoding="utf-8")
    (output/"manifest.json").write_text(json.dumps(
        {p.name:_sha(p) for p in output.iterdir() if p.is_file()},indent=2)+"\n",encoding="utf-8")
    print(table.loc[table.period.eq("full_64"),
        ["policy","selected_orders","trades","win_rate_pct","net_pnl_rs",
         "negative_days","worst_day_rs","daily_close_drawdown_rs"]].to_string(index=False),flush=True)


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output",type=Path,default=PACKAGE/"results_unlimited_20261104")
    args=parser.parse_args()
    run(args.output)


if __name__=="__main__":
    main()
