"""Cap all 39 new V21 setup pairs at one, then test small causal entry vetoes.

The 34 original V21 pairs retain cap three. All arms reuse the saved staged
native fills, stops, targets, costs and execution paths, with unlimited
portfolio capital and fixed per-entry sizing. The extra gate is applied after
core-first cap-one selection: a veto never promotes the next ranked stock.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import pickle

import numpy as np
import pandas as pd

from g3_v21_expanded_20261104 import selection
from g3_v21_expanded_20261104.refinement_20261104 import run as finite
from g3_v21_expanded_20261104.refinement_20261104 import unlimited
from . import policy as strict_policy


PACKAGE = Path(__file__).resolve().parent
PRIOR_UNLIMITED = PACKAGE.parent / "results_unlimited_final_20261104"
NEW_IDS = strict_policy.NEW_IDS
OLD_IDS = strict_policy.OLD_IDS

POLICIES = (
    "frozen_v21",
    "expanded_all73_cap3",
    "donor27_cap1_late12_cap3",
    "new39_cap1",
    "new39_cap1_5m_volume_plus_0p10",
    "new39_cap1_body_plus_0p05",
    "new39_cap1_volume_and_body",
)
VOLUME_INCREMENT = strict_policy.VOLUME_INCREMENT
BODY_INCREMENT = 0.05


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _admit(book: pd.DataFrame, policy: str) -> pd.DataFrame:
    """Apply a completed-signal/confirmation veto only to new pairs."""
    if policy == "new39_cap1":
        return book.copy()
    if policy not in POLICIES[4:]:
        raise ValueError(f"Unknown stricter policy: {policy}")
    ids = book.setup_id.astype(str)
    new = ids.isin(NEW_IDS)
    required_volume = ids.map({key: spec.volume_ratio for key, spec in selection.SPECS.items()})
    required_body = ids.map({key: spec.body_ratio for key, spec in selection.SPECS.items()})
    observed_volume = pd.to_numeric(book.volume_ratio, errors="raise")
    observed_body = pd.to_numeric(book.body_ratio, errors="raise")
    if not np.isfinite(observed_volume.loc[new]).all() or not np.isfinite(observed_body.loc[new]).all():
        raise ValueError("New setup strength feature is missing")
    volume_pass = observed_volume.ge(required_volume + VOLUME_INCREMENT - 1e-12)
    body_pass = observed_body.ge(required_body + BODY_INCREMENT - 1e-12)
    if policy == "new39_cap1_5m_volume_plus_0p10":
        admitted = ~new | volume_pass
    elif policy == "new39_cap1_body_plus_0p05":
        admitted = ~new | body_pass
    else:
        admitted = ~new | (volume_pass & body_pass)
    return book.loc[admitted].copy()


def _audit(expanded: pd.DataFrame, capped: pd.DataFrame, volume_arm: pd.DataFrame) -> pd.DataFrame:
    fields = ["day", "sid", "signal_ts", "confirmation_ts", "setup_id", "side",
              "tradingsymbol", "v9_rank_in_setup_day", "v10_g_f_core", "volume_ratio",
              "body_ratio", "v9_5m_feature_ts", "v9_feature_available_ts"]
    audit = expanded[fields].copy()
    audit["pair_class"] = np.where(audit.setup_id.isin(OLD_IDS), "original34",
                                   np.where(audit.setup_id.isin(selection.EXTRA_SETUPS),
                                            "new_late12", "new_donor27"))
    audit["cap1_new39_selected"] = audit.sid.isin(capped.sid)
    audit["base_5m_volume_minimum"] = audit.setup_id.map(
        {key: spec.volume_ratio for key, spec in selection.SPECS.items()})
    audit["extra_5m_volume_required"] = np.where(
        audit.setup_id.isin(NEW_IDS), audit.base_5m_volume_minimum + VOLUME_INCREMENT,
        audit.base_5m_volume_minimum)
    audit["volume_arm_admitted"] = audit.sid.isin(volume_arm.sid)
    audit["decision"] = np.where(~audit.cap1_new39_selected, "RANKED_OUT_CAP1_NEW_PAIR",
                                 np.where(~audit.volume_arm_admitted,
                                          "VETO_5M_VOLUME_PLUS_0P10", "ADMITTED"))
    return audit.sort_values(["day", "signal_ts", "setup_id", "tradingsymbol"],
                             kind="stable").reset_index(drop=True)


def _report(output: Path, overall: pd.DataFrame, summary: dict) -> None:
    lines = ["# G3 V21: cap one and a small strength gate on all 39 new pairs", "",
             "Research-only, unlimited-capital replay. The 34 original V21 pairs retain cap three. "
             "The 27 newly enabled donor pairs and 12 added 13:35–14:00 pairs each receive cap one. "
             "The tested entry gate raises each new pair's completed 5-minute volume-ratio minimum "
             "by 0.10 absolute. It runs after core-first cap-one ranking: a veto does not promote "
             "the next stock. Original V21 pairs and all saved target/stop rules are unchanged.", "",
             "Every native fill executes with ₹100,000 capital per entry, 5× exposure and no "
             "effective portfolio capital limit. The same accepted portfolio engine exactly "
             "reproduced the previously saved unlimited expanded V21 totals.", "",
             "## Full 64 sealed sessions", "",
             "| Policy | Selected | Trades | Losing trades | Win rate | Net P&L (₹) | Losing days | Worst day (₹) | Daily-close drawdown (₹) |",
             "|:---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in overall.loc[overall.period.eq("full_64")].itertuples(index=False):
        lines.append(f"| {row.policy} | {row.selected_orders} | {row.trades} | {row.losses} | "
                     f"{row.win_rate_pct:.2f}% | {row.net_pnl_rs:,.2f} | {row.negative_days} | "
                     f"{row.worst_day_rs:,.2f} | {row.daily_close_drawdown_rs:,.2f} |")
    lines += ["", "## Five later sessions (28 October–3 November)", "",
              "| Policy | Selected | Trades | Losing trades | Win rate | Net P&L (₹) | Losing days | Daily-close drawdown (₹) |",
              "|:---|---:|---:|---:|---:|---:|---:|---:|"]
    for row in overall.loc[overall.period.eq("after_oct27_5")].itertuples(index=False):
        lines.append(f"| {row.policy} | {row.selected_orders} | {row.trades} | {row.losses} | "
                     f"{row.win_rate_pct:.2f}% | {row.net_pnl_rs:,.2f} | {row.negative_days} | "
                     f"{row.daily_close_drawdown_rs:,.2f} |")
    lines += ["", "## Interpretation", "",
              "The 5-minute volume +0.10 arm is the smallest tested strength adjustment that "
              "reduces selected orders, native trades, and losing trades versus new39 cap one "
              "without selecting individual stocks or dates. The body +0.05 and combined arms "
              "are shown to make the trade-off reviewable. The rule is retrospective: these "
              "variants were tested after seeing historical results, and the five later sessions "
              "are a small retrospective check, not independent validation. October 1 was "
              "excluded from the inherited sealed source; 116 added stock-clock rows on October "
              "28 remained unavailable because four stocks lacked prior sealed warm-up minutes.", "",
              f"**Baseline parity:** {summary['baseline_parity']}. "
              f"**New-pair cap:** {summary['cap_new_pair_count']} pairs at one. "
              f"**Original cap:** {summary['cap_original_pair_count']} pairs at three.", "",
              "[Overall CSV](overall.csv) | [Day-wise CSV](daily.csv) | "
              "[Admission audit](admission_audit.csv) | [Configuration](config.json) | "
              "[Source provenance](provenance.json)", ""]
    (output / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def run(output: Path) -> None:
    output = output.resolve()
    if output.exists() or not output.is_relative_to(PACKAGE) or output == PACKAGE:
        raise ValueError("Output must be a new child of isolated strict V21 research")
    frozen, expanded, days, parent = finite._load_books()
    if days[-1] != "2026-11-03" or len(days) != 64:
        raise ValueError("The sealed V21 cohort changed")
    capped = strict_policy.cap_new39(expanded)
    donor27 = finite.select_policy_rows(expanded, "new_donor_cap1")
    # The policy can be applied directly to a future expanded V21 selected
    # audit. Verify a saved day gives the same selected identities as its
    # staged/native book, which the replay uses for exit outcomes.
    first_checkpoint = sorted(finite.CHECKPOINTS.glob("[0-9][0-9]_*.pkl"))[0]
    first_record = pickle.loads(first_checkpoint.read_bytes())
    selected_audit = first_record["audit"].loc[lambda frame: frame.v9_selected.eq(True)]
    first_book = first_record["books"]["v21_expanded_cap3"]
    if set(selected_audit.sid) != set(first_book.sid):
        raise ValueError("One-day expanded V21 audit/native-book identities differ")
    if set(strict_policy.apply(selected_audit).sid) != set(strict_policy.apply(first_book).sid):
        raise ValueError("Pure policy selected different SIDs from audit and staged book")
    subsets = {
        "frozen_v21": frozen,
        "expanded_all73_cap3": expanded,
        "donor27_cap1_late12_cap3": donor27,
        "new39_cap1": capped,
        "new39_cap1_5m_volume_plus_0p10": strict_policy.apply(expanded),
    }
    for policy in POLICIES[5:]:
        subsets[policy] = _admit(capped, policy)
    if tuple(subsets) != POLICIES:
        raise ValueError("Policy result order changed")
    if set(subsets["new39_cap1_5m_volume_plus_0p10"].sid) != set(
            _admit(capped, "new39_cap1_5m_volume_plus_0p10").sid):
        raise ValueError("Reusable policy disagrees with independent volume-gate comparison")
    for policy, subset in subsets.items():
        if subset.sid.duplicated().any() or not set(subset.sid).issubset(set(expanded.sid) | set(frozen.sid)):
            raise ValueError(f"Invalid native candidate identity: {policy}")
    books = {policy: unlimited._unlimited(subset) for policy, subset in subsets.items()}
    prior = pd.read_csv(PRIOR_UNLIMITED / "overall.csv")
    for policy, reference in (("frozen_v21", "frozen_v21"),
                              ("expanded_all73_cap3", "expanded_all"),
                              ("donor27_cap1_late12_cap3", "new_donor_cap1")):
        observed = finite._metric(books[policy], days)
        expected = prior.loc[prior.period.eq("full_64") & prior.policy.eq(reference)].iloc[0]
        for key in ("selected_orders", "trades", "wins", "losses", "net_pnl_rs",
                    "daily_close_drawdown_rs", "negative_days"):
            if not np.isclose(float(observed[key]), float(expected[key]), atol=.01, rtol=0):
                raise ValueError(f"Saved unlimited baseline parity failure: {policy} {key}")
    if len(capped) != 1115 or finite._metric(books["new39_cap1"], days)["trades"] != 978:
        raise ValueError("All-new-pair cap-one reproduction changed")
    volume_check = finite._metric(books["new39_cap1_5m_volume_plus_0p10"], days)
    if (volume_check["trades"] != 943 or volume_check["losses"] != 314 or
            abs(volume_check["net_pnl_rs"] - 1_261_419.10) > .02 or
            abs(volume_check["daily_close_drawdown_rs"] - 40_721.16) > .02):
        raise ValueError("Independent +0.10 volume-gate result did not reproduce")
    periods = {
        "full_64": days,
        "through_oct27_59": [day for day in days if day <= "2026-10-27"],
        "after_oct27_5": [day for day in days if day > "2026-10-27"],
    }
    overall = pd.DataFrame([dict(period=period, policy=policy,
                                 **finite._metric(books[policy], cohort))
                            for period, cohort in periods.items() for policy in POLICIES])
    daily = pd.DataFrame([dict(day=day, policy=policy,
                               **finite._metric(books[policy], [day]))
                          for day in days for policy in POLICIES])
    for policy in POLICIES:
        full = overall.loc[overall.period.eq("full_64") & overall.policy.eq(policy)].iloc[0]
        part = daily.loc[daily.policy.eq(policy)]
        if abs(float(part.net_pnl_rs.sum()) - float(full.net_pnl_rs)) > .01 or int(part.trades.sum()) != int(full.trades):
            raise ValueError(f"Daily metric reconciliation failure: {policy}")
    volume_arm = subsets["new39_cap1_5m_volume_plus_0p10"]
    admission = _audit(expanded, capped, volume_arm)
    summary = dict(scope="BACKTEST_ONLY", execution_authority=False,
                   capital_policy="UNLIMITED_PORTFOLIO_CAPITAL_FIXED_PER_ENTRY_SIZE",
                   baseline_parity="PASS_SAVED_FROZEN_EXPANDED_AND_DONOR_CAP1",
                   source_native_fills=int(expanded.filled.sum()),
                   cap_new_pair_count=len(NEW_IDS), cap_original_pair_count=len(OLD_IDS),
                   new_pair_cap=1, original_pair_cap=3,
                   rule="NEW_PAIR_5M_VOLUME_RATIO_GE_SETUP_MINIMUM_PLUS_0P10_POST_CAP1_VETO",
                   no_fallback_promotion=True,
                   policy_configuration=strict_policy.configuration(),
                   policy_fingerprint=strict_policy.fingerprint(),
                   one_day_audit_staged_sid_parity="PASS",
                   recommended_research_policy="new39_cap1_5m_volume_plus_0p10",
                   days=days,
                   comparison=overall.loc[overall.period.eq("full_64")].to_dict("records"))
    output.mkdir(parents=True)
    overall.to_csv(output / "overall.csv", index=False)
    daily.to_csv(output / "daily.csv", index=False)
    admission.to_csv(output / "admission_audit.csv", index=False)
    (output / "config.json").write_text(json.dumps(summary, indent=2, default=str,
                                                  allow_nan=False) + "\n", encoding="utf-8")
    (output / "provenance.json").write_text(json.dumps(dict(
        code={str(path): _sha(path) for path in (Path(__file__), Path(strict_policy.__file__), Path(selection.__file__),
            Path(finite.__file__), Path(unlimited.__file__))},
        checkpoint_hashes={path.name: _sha(path) for path in sorted(finite.CHECKPOINTS.glob("*.pkl"))},
        prior_unlimited_result=str(PRIOR_UNLIMITED / "overall.csv"),
        prior_unlimited_result_sha256=_sha(PRIOR_UNLIMITED / "overall.csv")),
        indent=2) + "\n", encoding="utf-8")
    _report(output, overall, summary)
    (output / "manifest.json").write_text(json.dumps(
        {path.name: _sha(path) for path in output.iterdir() if path.is_file()}, indent=2) + "\n",
        encoding="utf-8")
    print(overall.loc[overall.period.eq("full_64"),
        ["policy", "selected_orders", "trades", "losses", "win_rate_pct",
         "net_pnl_rs", "negative_days", "daily_close_drawdown_rs"]].to_string(index=False), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PACKAGE / "results_full_20261104")
    args = parser.parse_args()
    run(args.output)


if __name__ == "__main__":
    main()
