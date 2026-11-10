"""Append the sealed 4 November session to the isolated V21/G7 confirmation replay."""
from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v21_expanded_20261104.refinement_20261104 import run as finite
from g3_v21_expanded_20261104.refinement_20261104 import unlimited
from g3_v21_expanded_20261104.refinement_20261104.strict_new_pairs_20261104 import policy as v21
from . import policy
from .latest_day import DAY, build_expanded_day
from .nifty_features import build_nifty_audit
from .run import PACKAGE, _sha, _slot_results
from .stock_features import build_stock_audit


PRIOR_RESULT = PACKAGE / "results_full_20261104"
GATES = policy.ARM_GATES[policy.ARMS[-1]]


def _flags(values: pd.Series, key: str) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        if values.isna().any():
            raise ValueError(f"Missing saved G7 decision: {key}")
        return values.astype(bool)
    text = values.astype(str).str.strip().str.lower()
    if not text.isin(("true", "false")).all():
        raise ValueError(f"Invalid saved G7 decision: {key}")
    return text.eq("true")


def _report(output: Path, overall: pd.DataFrame, summary: dict) -> None:
    lines = ["# G3 V21 with G7 V4 entry confirmations, through 4 November 2026", "",
             "The first 64 sessions reproduce the isolated 3 November V21/G7 replay. "
             "A new, hash-verified sealed 4 November G3 source supplies V21's same 73 "
             "configured setup pairs and original native execution engine. The 39 new pairs "
             "retain cap one plus the 5-minute volume increment; the 34 original pairs "
             "retain cap three. G7 Nifty and extreme/efficiency/RSI checks cover every "
             "selected pair; G7 ATR and OI/value guards cover their named pairs. Vetoes "
             "do not promote the next ranked stock. V21 exits and unlimited-capital "
             "per-entry sizing are unchanged.", "",
             "## All 65 sealed sessions", "",
             "| Entry policy | Selected | Trades | Losses | Win rate | Net P&L (₹) | Losing days | Worst day (₹) | Daily-close drawdown (₹) |",
             "|:---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in overall.loc[overall.period.eq("full_65")].itertuples(index=False):
        lines.append(f"| {row.policy} | {row.selected_orders} | {row.trades} | "
                     f"{row.losses} | {row.win_rate_pct:.2f}% | {row.net_pnl_rs:,.2f} | "
                     f"{row.negative_days} | {row.worst_day_rs:,.2f} | "
                     f"{row.daily_close_drawdown_rs:,.2f} |")
    lines += ["", "## 4 November alone", "",
              "| Entry policy | Selected | Trades | Wins | Losses | Net P&L (₹) |",
              "|:---|---:|---:|---:|---:|---:|"]
    for row in overall.loc[overall.period.eq("nov04_only")].itertuples(index=False):
        lines.append(f"| {row.policy} | {row.selected_orders} | {row.trades} | "
                     f"{row.wins} | {row.losses} | {row.net_pnl_rs:,.2f} |")
    lines += ["", f"**64-session parity:** {summary['prior_64_parity']}. "
              f"**4 November selected after V21 cap/volume:** {summary['latest_v21_selected']}. "
              f"**4 November admitted after all G7 entry checks:** {summary['latest_g7_admitted']}.", "",
              "G7's retrospective win-rate slot allowlist, daily two-loss lock and profit "
              "protection are not included. This is an entry-confirmation comparison, "
              "using historical rules examined on overlapping sessions; 4 November is "
              "a newly sealed session in this run, not a prospective live trial. The "
              "source proofs record whether a dated Nifty decision used an immutable "
              "snapshot or a hashed historical futures file.", "",
              "[Overall results](overall.csv) · [Daily results](daily.csv) · "
              "[73-pair results](slot_results.csv) · [Per-order decisions](admission_audit.csv) · "
              "[Executed trades](trades.csv) · [Source proof](provenance.json)", ""]
    (output / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def run(output: Path) -> dict:
    output = output.resolve()
    if output.exists() or not output.is_relative_to(PACKAGE) or output == PACKAGE:
        raise ValueError("Output must be a fresh child of isolated V21 G7 research")
    if not (PRIOR_RESULT / "manifest.json").is_file():
        raise ValueError("The verified 64-session G7 overlay must finish first")
    _, expanded64, days64, _ = finite._load_books()
    if len(days64) != 64 or days64[-1] != "2026-11-03":
        raise ValueError("Pinned 64-session V21 history changed")
    expanded_latest, latest_proof = build_expanded_day(DAY)
    if set(expanded64.sid.astype(int)) & set(expanded_latest.sid.astype(int)):
        raise ValueError("Latest-day V21 SID collisions")
    all_expanded = pd.concat((expanded64, expanded_latest), ignore_index=True, sort=False)
    v21_selected = v21.apply(all_expanded)
    old_ids = set(v21.apply(expanded64).sid.astype(int))
    latest_orders = v21_selected.loc[v21_selected.day.astype(str).eq(DAY)].copy()
    if (set(v21_selected.loc[~v21_selected.day.astype(str).eq(DAY)].sid.astype(int)) != old_ids
            or not len(latest_orders)):
        raise ValueError("Appending 4 November changed prior V21 admissions")
    stock, stock_proof = build_stock_audit(latest_orders)
    nifty, nifty_proof = build_nifty_audit(latest_orders)
    latest_audit = policy.decisions(latest_orders, stock, nifty)
    latest_audit["pair_class"] = np.where(latest_audit.setup_id.isin(v21.NEW_IDS),
                                           "new39", "original34")
    old_audit = pd.read_csv(PRIOR_RESULT / "admission_audit.csv", low_memory=False)
    if set(old_audit.sid.astype(int)) != old_ids:
        raise ValueError("Prior G7 decision audit changed")
    if not old_audit.research_policy_fingerprint.eq(policy.fingerprint()).all():
        raise ValueError("Prior G7 decision policy changed")
    for key in GATES:
        old_audit[key] = _flags(old_audit[key], key)
    audit = pd.concat((old_audit, latest_audit), ignore_index=True, sort=False)
    if audit.sid.duplicated().any() or set(audit.sid.astype(int)) != set(v21_selected.sid.astype(int)):
        raise ValueError("Combined G7 audit did not cover every V21 order")
    selections = {arm: policy.subset(v21_selected, audit, arm) for arm in policy.ARMS}
    books = {arm: unlimited._unlimited(selections[arm]) for arm in policy.ARMS}
    periods = {"full_65": [*days64, DAY], "through_nov03_64": days64, "nov04_only": [DAY]}
    overall = pd.DataFrame([dict(period=period, policy=arm,
                                 **finite._metric(books[arm], cohort))
                            for period, cohort in periods.items() for arm in policy.ARMS])
    daily = pd.DataFrame([dict(day=day, policy=arm, **finite._metric(books[arm], [day]))
                          for day in periods["full_65"] for arm in policy.ARMS])
    prior = pd.read_csv(PRIOR_RESULT / "overall.csv")
    for arm in policy.ARMS:
        expected = prior.loc[prior.period.eq("full_64") & prior.policy.eq(arm)]
        actual = overall.loc[overall.period.eq("through_nov03_64") & overall.policy.eq(arm)]
        if len(expected) != 1 or len(actual) != 1:
            raise ValueError(f"Prior 64-session arm missing: {arm}")
        for key in ("selected_orders", "trades", "wins", "losses", "net_pnl_rs",
                    "daily_close_drawdown_rs", "negative_days"):
            if not np.isclose(float(actual.iloc[0][key]), float(expected.iloc[0][key]),
                              rtol=0, atol=.01):
                raise ValueError(f"Prior 64-session G7 overlay parity failed: {arm} {key}")
        full = overall.loc[overall.period.eq("full_65") & overall.policy.eq(arm)].iloc[0]
        day_rows = daily.loc[daily.policy.eq(arm)]
        if (abs(float(day_rows.net_pnl_rs.sum()) - float(full.net_pnl_rs)) > .01 or
                int(day_rows.trades.sum()) != int(full.trades)):
            raise ValueError(f"65-session daily reconciliation failed: {arm}")
    final = books[policy.ARMS[-1]]
    slots = _slot_results(books[policy.ARMS[0]], final, audit)
    summary = dict(scope="BACKTEST_ONLY", execution_authority=False,
                   version=policy.VERSION, policy_fingerprint=policy.fingerprint(),
                   source_64_result=str(PRIOR_RESULT), prior_64_parity="PASS_ALL_SIX_ARMS",
                   latest_day=DAY, latest_expanded_orders=len(expanded_latest),
                   latest_v21_selected=len(latest_orders),
                   latest_g7_admitted=int(selections[policy.ARMS[-1]].day.astype(str).eq(DAY).sum()),
                   latest_stock_low_efficiency_vetoes=int(latest_audit.g7_low_efficiency_veto.sum()),
                   latest_nifty_vetoes=int((~latest_audit.g7_nifty_allowed).sum()),
                   latest_stock_proof_records=len(stock_proof),
                   latest_nifty_source_kind_counts=dict(Counter(x["source_kind"] for x in nifty_proof)),
                   configuration=policy.configuration(),
                   periods={key: dict(first=value[0], last=value[-1], sessions=len(value))
                            for key, value in periods.items()},
                   comparison=overall.to_dict("records"))
    output.mkdir(parents=True)
    overall.to_csv(output / "overall.csv", index=False)
    daily.to_csv(output / "daily.csv", index=False)
    slots.to_csv(output / "slot_results.csv", index=False)
    audit.to_csv(output / "admission_audit.csv", index=False)
    trade_columns = ["day", "setup_id", "tradingsymbol", "side", "entry_ts", "exit_ts",
                     "entry_price", "exit_price", "portfolio_net_profit_rupees", "exit_reason", "sid"]
    final.loc[final.portfolio_executed.eq(True), trade_columns].to_csv(output / "trades.csv", index=False)
    (output / "latest_stock_source_proof.json").write_text(json.dumps(
        stock_proof, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")
    (output / "latest_nifty_source_proof.json").write_text(json.dumps(
        nifty_proof, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")
    (output / "config.json").write_text(json.dumps(summary, indent=2,
        default=str, allow_nan=False) + "\n", encoding="utf-8")
    (output / "latest_source_proof.json").write_text(json.dumps(latest_proof, indent=2,
        default=str, allow_nan=False) + "\n", encoding="utf-8")
    (output / "provenance.json").write_text(json.dumps(dict(
        code_sha256={str(path): _sha(path) for path in (Path(__file__), Path(policy.__file__),
                           Path(build_expanded_day.__code__.co_filename),
                           Path(build_stock_audit.__code__.co_filename),
                           Path(build_nifty_audit.__code__.co_filename))},
        prior_result_manifest=str(PRIOR_RESULT / "manifest.json"),
        prior_result_manifest_sha256=_sha(PRIOR_RESULT / "manifest.json"),
        source_4nov_proof=str(output / "latest_source_proof.json"),
        source_4nov_proof_sha256=_sha(output / "latest_source_proof.json")),
        indent=2) + "\n", encoding="utf-8")
    _report(output, overall, summary)
    (output / "manifest.json").write_text(json.dumps(
        {path.name: _sha(path) for path in output.iterdir() if path.is_file()}, indent=2) + "\n",
        encoding="utf-8")
    print(overall.loc[overall.period.isin(("full_65", "nov04_only")),
        ["period", "policy", "selected_orders", "trades", "losses", "win_rate_pct",
         "net_pnl_rs", "negative_days", "daily_close_drawdown_rs"]].to_string(index=False), flush=True)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PACKAGE / "results_through_20261104")
    args = parser.parse_args()
    run(args.output)


if __name__ == "__main__":
    main()
