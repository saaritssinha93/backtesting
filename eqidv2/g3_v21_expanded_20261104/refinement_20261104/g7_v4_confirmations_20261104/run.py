"""Full-history V21 replay with the G7 V4 entry-confirmation stack.

The frozen V21 native fill/exit paths and per-trade economics are reused. G7
entry checks are applied to the selected V21 cap-one/volume-gated opportunities,
then the same unlimited-capital V21 portfolio allocator is reapplied. This is
an entry ablation, not a replay of G7's separate stops, loss lock or slot list.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v21_expanded_20261104 import selection as expanded_selection
from g3_v21_expanded_20261104.refinement_20261104 import run as finite
from g3_v21_expanded_20261104.refinement_20261104 import unlimited
from g3_v21_expanded_20261104.refinement_20261104.strict_new_pairs_20261104 import policy as v21
from . import policy
from .nifty_features import build_nifty_audit
from .stock_features import build_stock_audit


PACKAGE = Path(__file__).resolve().parent
BASELINE = PACKAGE.parent / "strict_new_pairs_20261104" / "results_full_20261104"
PERIODS = ("full_64", "through_oct27_59", "after_oct27_5")


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for part in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(part)
    return digest.hexdigest()


def _baseline_parity(book: pd.DataFrame, days: list[str]) -> dict:
    current = finite._metric(book, days)
    saved = pd.read_csv(BASELINE / "overall.csv")
    reference = saved.loc[saved.period.eq("full_64") &
                          saved.policy.eq("new39_cap1_5m_volume_plus_0p10")]
    if len(reference) != 1:
        raise ValueError("Saved V21 new39 cap-one volume baseline is missing")
    for key in ("selected_orders", "trades", "wins", "losses", "net_pnl_rs",
                "daily_close_drawdown_rs", "negative_days"):
        if not np.isclose(float(current[key]), float(reference.iloc[0][key]), rtol=0, atol=.01):
            raise ValueError(f"G7 overlay did not reproduce saved V21 baseline: {key}")
    return current


def _periods(days: list[str]) -> dict[str, list[str]]:
    return {"full_64": days,
            "through_oct27_59": [day for day in days if day <= "2026-10-27"],
            "after_oct27_5": [day for day in days if day > "2026-10-27"]}


def _comparison(books: dict[str, pd.DataFrame], days: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    periods = _periods(days)
    overall = pd.DataFrame([dict(period=period, policy=arm,
                                 **finite._metric(books[arm], cohort))
                            for period, cohort in periods.items() for arm in policy.ARMS])
    daily = pd.DataFrame([dict(day=day, policy=arm,
                               **finite._metric(books[arm], [day]))
                          for day in days for arm in policy.ARMS])
    for arm in policy.ARMS:
        full = overall.loc[overall.period.eq("full_64") & overall.policy.eq(arm)].iloc[0]
        part = daily.loc[daily.policy.eq(arm)]
        if (abs(float(part.net_pnl_rs.sum()) - float(full.net_pnl_rs)) > .01 or
                int(part.trades.sum()) != int(full.trades)):
            raise ValueError(f"G7 overlay daily reconciliation failed: {arm}")
    return overall, daily


def _slot_results(base: pd.DataFrame, final: pd.DataFrame, audit: pd.DataFrame) -> pd.DataFrame:
    ids = set(final.sid.astype(int))
    allowed = audit.assign(g7_admitted=audit.sid.isin(ids))
    rows = []
    for setup_id, part in allowed.groupby("setup_id", sort=True):
        previous = base.loc[base.setup_id.eq(setup_id)]
        after = final.loc[final.setup_id.eq(setup_id)]
        before_filled = previous.loc[previous.portfolio_executed.eq(True)]
        after_filled = after.loc[after.portfolio_executed.eq(True)]
        rows.append(dict(setup_id=setup_id, side=str(part.side.iloc[0]),
                         pair_class=str(part.pair_class.iloc[0]),
                         baseline_selected=len(previous), g7_admitted=len(after),
                         baseline_fills=len(before_filled), g7_fills=len(after_filled),
                         baseline_net_pnl_rs=float(before_filled.portfolio_net_profit_rupees.sum()),
                         g7_net_pnl_rs=float(after_filled.portfolio_net_profit_rupees.sum()),
                         nifty_vetoes=int((~part.g7_nifty_allowed).sum()),
                         low_efficiency_vetoes=int(part.g7_low_efficiency_veto.sum()),
                         atr_vetoes=int((~part.g7_atr_allowed).sum()),
                         slot_postrank_vetoes=int((~part.g7_slot_postrank_allowed).sum())))
    frame = pd.DataFrame(rows)
    if len(frame) != 73 or frame.baseline_selected.sum() != len(base) or frame.g7_admitted.sum() != len(final):
        raise ValueError("G7 overlay did not account for all 73 V21 setup pairs")
    return frame


def _report(output: Path, overall: pd.DataFrame, summary: dict) -> None:
    full = overall.loc[overall.period.eq("full_64")]
    later = overall.loc[overall.period.eq("after_oct27_5")]
    rows = ["# G3 V21 with G7 V4 entry confirmations", "",
            "Research-only replay of the 64 sealed V21 sessions, 29 July–3 November 2026. "
            "The V21 policy first caps its 39 new pairs at one and requires their completed "
            "5-minute volume ratio to exceed the setup minimum by 0.10. The 34 original pairs "
            "retain cap three. G7's Nifty and low-efficiency checks then apply to **every one "
            "of the 73 pairs**. G7's ATR and OI/traded-value thresholds apply to the specific "
            "G7 setup IDs that define them. Vetoes do not replace a selected stock.", "",
            "The G7 historical ≥70% slot allowlist, daily two-loss lock, profit-protection "
            "stop and different targets/stops are not entry confirmations and are not included. "
            "All arms reuse the same V21 native fill and exit paths, ₹100,000 capital per "
            "order, 5× exposure, 5 bps modeled round-trip costs and unlimited portfolio capital.", "",
            "## Full 64 sessions", "",
            "| Entry policy | Selected | Trades | Losses | Win rate | Net P&L (₹) | Losing days | Worst day (₹) | Daily-close drawdown (₹) |",
            "|:---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in full.itertuples(index=False):
        rows.append(f"| {row.policy} | {row.selected_orders} | {row.trades} | "
                    f"{row.losses} | {row.win_rate_pct:.2f}% | {row.net_pnl_rs:,.2f} | "
                    f"{row.negative_days} | {row.worst_day_rs:,.2f} | "
                    f"{row.daily_close_drawdown_rs:,.2f} |")
    rows += ["", "## Five later sessions, 28 October–3 November", "",
             "| Entry policy | Trades | Losses | Win rate | Net P&L (₹) | Losing days | Daily-close drawdown (₹) |",
             "|:---|---:|---:|---:|---:|---:|---:|"]
    for row in later.itertuples(index=False):
        rate = f"{row.win_rate_pct:.2f}%" if np.isfinite(row.win_rate_pct) else "n/a"
        rows.append(f"| {row.policy} | {row.trades} | {row.losses} | {rate} | "
                    f"{row.net_pnl_rs:,.2f} | {row.negative_days} | "
                    f"{row.daily_close_drawdown_rs:,.2f} |")
    rows += ["", "## Gate coverage and interpretation", "",
             f"- Saved V21 cap-one/volume baseline reproduction: **{summary['baseline_parity']}**.",
             f"- Universal Nifty decisions: {summary['gate_counts']['nifty_checked']} checked; "
             f"{summary['gate_counts']['nifty_vetoed']} vetoed.",
             f"- Universal G7 V4 low-efficiency decisions: {summary['gate_counts']['stock_checked']} checked; "
             f"{summary['gate_counts']['low_efficiency_vetoed']} vetoed.",
             f"- G7 named ATR guards: {summary['gate_counts']['atr_applicable']} applicable; "
             f"{summary['gate_counts']['atr_vetoed']} vetoed. G7 named post-ranking OI/value guards: "
             f"{summary['gate_counts']['slot_postrank_applicable']} applicable; "
             f"{summary['gate_counts']['slot_postrank_vetoed']} vetoed.",
             "- G7's 09:26 SHORT minimum lower wick of 0.20 was already present in V21 and was verified, not added twice.", "",
             "The historical thresholds and the V21 volume policy were developed using overlapping "
             "history; the five later sessions have also been inspected. This is a retrospective "
             "comparison, not an independent forward result. Early stock history comes from "
             "hash-verified retrospective snapshots, with each decision restricted to completed "
             "bars and prior same-stock sessions. Dated Nifty bars use immutable day snapshots "
             "where available; otherwise the hashed historical futures file is used. The source "
             "kind for each day is recorded in the proof files. No completed 4 November V21 "
             "session is present in this sealed source.", "",
             "[Overall results](overall.csv) · [Day-wise results](daily.csv) · "
             "[73-pair results](slot_results.csv) · [Per-order gate audit](admission_audit.csv) · "
             "[Final trades](trades.csv) · [Configuration](config.json) · "
             "[Stock source proof](stock_source_proof.json) · [Nifty source proof](nifty_source_proof.json)", ""]
    (output / "REPORT.md").write_text("\n".join(rows), encoding="utf-8")


def run(output: Path) -> dict:
    output = output.resolve()
    if output.exists() or not output.is_relative_to(PACKAGE) or output == PACKAGE:
        raise ValueError("Output must be a fresh child of isolated V21 G7 confirmation research")
    _, expanded, days, _ = finite._load_books()
    if len(days) != 64 or days[0] != "2026-07-29" or days[-1] != "2026-11-03":
        raise ValueError("Sealed V21 session inventory changed")
    base_orders = v21.apply(expanded)
    if len(base_orders) != 1074 or base_orders.setup_id.nunique() != 73:
        raise ValueError("Saved V21 cap-one/volume selected opportunity inventory changed")
    baseline_book = unlimited._unlimited(base_orders)
    base_metric = _baseline_parity(baseline_book, days)
    stock, stock_proof = build_stock_audit(base_orders)
    nifty, nifty_proof = build_nifty_audit(base_orders)
    audit = policy.decisions(base_orders, stock, nifty)
    audit["pair_class"] = np.where(audit.setup_id.isin(v21.NEW_IDS), "new39", "original34")
    if audit.setup_id.nunique() != 73 or len(audit) != len(base_orders):
        raise ValueError("Not all V21 pairs received G7 confirmation decisions")
    selections = {arm: policy.subset(base_orders, audit, arm) for arm in policy.ARMS}
    books = {policy.ARMS[0]: baseline_book}
    for arm in policy.ARMS[1:]:
        subset = selections[arm]
        if subset.sid.duplicated().any() or not set(subset.sid).issubset(set(base_orders.sid)):
            raise ValueError(f"Invalid G7 entry overlay selection: {arm}")
        books[arm] = unlimited._unlimited(subset)
    overall, daily = _comparison(books, days)
    final = books[policy.ARMS[-1]]
    final_selected = selections[policy.ARMS[-1]]
    slots = _slot_results(baseline_book, final, audit)
    gate_counts = dict(nifty_checked=len(audit), nifty_vetoed=int((~audit.g7_nifty_allowed).sum()),
                       stock_checked=len(audit), low_efficiency_vetoed=int(audit.g7_low_efficiency_veto.sum()),
                       atr_applicable=int(audit.g7_atr_max.notna().sum()),
                       atr_vetoed=int((~audit.g7_atr_allowed).sum()),
                       slot_postrank_applicable=int(audit.g7_slot_postrank_minimum.notna().sum()),
                       slot_postrank_vetoed=int((~audit.g7_slot_postrank_allowed).sum()),
                       all_vetoed=int((~audit.all_g7_entry_allowed).sum()))
    if len(base_orders) - len(final_selected) != gate_counts["all_vetoed"]:
        raise ValueError("Combined G7 veto count does not reconcile")
    summary = dict(scope="BACKTEST_ONLY", execution_authority=False,
                   version=policy.VERSION, policy_fingerprint=policy.fingerprint(),
                   baseline_parity="PASS_SAVED_V21_NEW39_CAP1_VOLUME_GATE",
                   baseline_metric=base_metric, days=days, gate_counts=gate_counts,
                   stock_proof_records=len(stock_proof), nifty_proof_records=len(nifty_proof),
                   nifty_source_kind_counts=dict(Counter(item["source_kind"] for item in nifty_proof)),
                   configuration=policy.configuration(),
                   source_selection_fingerprint=expanded_selection.fingerprint(),
                   source_v21_strict_fingerprint=v21.fingerprint(),
                   source_g7_variant="V13-V10-G-7-V4-WICK020-LOSS-LOCK2-ALL-SLOTS-WIN70-BACKTEST-V4",
                   periods={name: dict(first=cohort[0], last=cohort[-1], sessions=len(cohort))
                            for name, cohort in _periods(days).items()},
                   comparison=overall.loc[overall.period.eq("full_64")].to_dict("records"))
    output.mkdir(parents=True)
    overall.to_csv(output / "overall.csv", index=False)
    daily.to_csv(output / "daily.csv", index=False)
    slots.to_csv(output / "slot_results.csv", index=False)
    audit.to_csv(output / "admission_audit.csv", index=False)
    trade_columns = ["day", "setup_id", "tradingsymbol", "side", "signal_ts",
                     "confirmation_ts", "entry_ts", "exit_ts", "entry_price", "exit_price",
                     "exit_reason", "portfolio_net_profit_rupees", "net_return_pct", "target_pct",
                     "stop_pct", "sid"]
    final.loc[final.portfolio_executed.eq(True), trade_columns].to_csv(output / "trades.csv", index=False)
    (output / "stock_source_proof.json").write_text(json.dumps(stock_proof, indent=2,
                                                  default=str, allow_nan=False) + "\n", encoding="utf-8")
    (output / "nifty_source_proof.json").write_text(json.dumps(nifty_proof, indent=2,
                                                  default=str, allow_nan=False) + "\n", encoding="utf-8")
    (output / "config.json").write_text(json.dumps(summary, indent=2, default=str,
                                           allow_nan=False) + "\n", encoding="utf-8")
    source_files = (Path(__file__), Path(policy.__file__), Path(v21.__file__),
                    Path(build_stock_audit.__code__.co_filename),
                    Path(build_nifty_audit.__code__.co_filename))
    provenance = dict(code_sha256={str(path): _sha(path) for path in source_files},
                      source_checkpoint_sha256={path.name: _sha(path) for path in
                          sorted(finite.CHECKPOINTS.glob("[0-9][0-9]_*.pkl"))},
                      saved_v21_baseline=str(BASELINE / "overall.csv"),
                      saved_v21_baseline_sha256=_sha(BASELINE / "overall.csv"))
    (output / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n",
                                                 encoding="utf-8")
    _report(output, overall, summary)
    (output / "manifest.json").write_text(json.dumps(
        {path.name: _sha(path) for path in output.iterdir() if path.is_file()}, indent=2) + "\n",
        encoding="utf-8")
    print(overall.loc[overall.period.eq("full_64"),
        ["policy", "selected_orders", "trades", "losses", "win_rate_pct",
         "net_pnl_rs", "negative_days", "worst_day_rs",
         "daily_close_drawdown_rs"]].to_string(index=False), flush=True)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PACKAGE / "results_full_20261104")
    args = parser.parse_args()
    run(args.output)


if __name__ == "__main__":
    main()
