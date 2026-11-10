"""Replay all fixed combinations of four saved G7 entry checks on G3 V21.

This research reads only pre-entry decisions when admitting an order. Native
fills, exits, costs, and fixed per-order sizing come from the sealed V21 replay.
It never changes a live scanner, source backtest, or saved earlier result.
"""
from __future__ import annotations

import argparse
from itertools import combinations
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v21_expanded_20261104.refinement_20261104 import run as finite
from g3_v21_expanded_20261104.refinement_20261104 import unlimited
from g3_v21_expanded_20261104.refinement_20261104.strict_new_pairs_20261104 import policy as v21
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104 import policy as g7
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104 import run as original_run
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104 import run_latest
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104.latest_day import DAY, build_expanded_day


PACKAGE = Path(__file__).resolve().parent
SOURCE = PACKAGE.parent / "results_through_20261104"
PRIOR = PACKAGE.parent / "results_full_20261104"
GATE_COLUMNS = {
    "N": "g7_nifty_allowed",
    "L": "g7_low_efficiency_allowed",
    "A": "g7_atr_allowed",
    "S": "g7_slot_postrank_allowed",
}
GATE_DESCRIPTIONS = {
    "N": "G7 dated Nifty both-counter veto",
    "L": "G7 V4 extreme / low-efficiency / weak-RSI veto",
    "A": "G7 named setup confirmation-extension ATR maximum",
    "S": "G7 named setup post-ranking OI / traded-value minimum",
}
ARMS = ("",) + tuple("".join(items) for count in range(1, 5)
                     for items in combinations(GATE_COLUMNS, count))
ORIGINAL_ARMS = {
    "": g7.ARMS[0], "N": g7.ARMS[1], "L": g7.ARMS[2],
    "A": g7.ARMS[3], "S": g7.ARMS[4], "NLAS": g7.ARMS[5],
}


def _label(code: str) -> str:
    """Write unambiguous CSV labels (bare 'NA' is parsed as missing by pandas)."""
    return "+".join(code) if code else "none"


def _verify_manifest(folder: Path) -> dict[str, str]:
    path = folder / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or not manifest:
        raise ValueError(f"Empty source manifest: {path}")
    for name, expected in manifest.items():
        item = folder / name
        if Path(name).name != name or not item.is_file() or original_run._sha(item) != expected:
            raise ValueError(f"Source result hash mismatch: {item}")
    return manifest


def _metrics(book: pd.DataFrame, days: list[str]) -> dict:
    metrics = finite._metric(book, days)
    executed = book.loc[book.portfolio_executed.eq(True) &
                        book.day.astype(str).str[:10].isin(days), "portfolio_net_profit_rupees"]
    daily = (book.loc[book.portfolio_executed.eq(True)]
             .groupby(book.loc[book.portfolio_executed.eq(True), "day"].astype(str).str[:10])
             .portfolio_net_profit_rupees.sum().reindex(days, fill_value=0.0))
    metrics["median_trade_rs"] = float(executed.median()) if len(executed) else float("nan")
    metrics["median_daily_pnl_rs"] = float(daily.median())
    metrics["average_daily_pnl_rs"] = float(daily.mean())
    if len(executed) != int(metrics["trades"]):
        raise ValueError("Executed trade and metric denominators differ")
    return metrics


def _check_original_parity(overall: pd.DataFrame) -> None:
    saved = pd.read_csv(SOURCE / "overall.csv")
    for gate_code, original_arm in ORIGINAL_ARMS.items():
        for current_period, saved_period in (("full_65", "full_65"),
                                             ("nov04_only", "nov04_only")):
            actual = overall.loc[overall.period.eq(current_period) &
                                 overall.gates.eq(_label(gate_code))]
            expected = saved.loc[saved.period.eq(saved_period) &
                                 saved.policy.eq(original_arm)]
            if len(actual) != 1 or len(expected) != 1:
                raise ValueError(f"Saved arm/period missing: {original_arm} {saved_period}")
            for field in ("selected_orders", "native_fills", "trades", "wins", "losses",
                          "negative_days", "net_pnl_rs", "average_trade_rs",
                          "daily_close_drawdown_rs"):
                if not np.isclose(float(actual.iloc[0][field]), float(expected.iloc[0][field]),
                                  rtol=0, atol=.01):
                    raise ValueError(f"Existing 65-day G7 result parity failed: {gate_code} {field}")


def _report(output: Path, overall: pd.DataFrame, days: list[str],
            exploratory: pd.DataFrame, chop_summary: dict) -> None:
    full = overall.loc[overall.period.eq("full_65")].set_index("gates")
    early = overall.loc[overall.period.eq("early_59")].set_index("gates")
    later = overall.loc[overall.period.eq("later_6")].set_index("gates")
    base, all_g7, atr_slot = (full.loc[_label(key)] for key in ("", "NLAS", "AS"))
    lines = [
        "# G3 V21 entry-check combination audit through 4 November 2026", "",
        "Backtest-only replay of all 16 fixed combinations of four previously computed G7 V4 "
        "entry checks on V21's 73 setup pairs. The baseline first applies the V21 39-new-pair "
        "cap-one / completed-5-minute-volume gate; the 34 original pairs retain cap three. "
        "Each G7 check is applied only where its source policy defines it. Rejected leaders "
        "are not replaced. The same native entries, targets, stops, costs, paths and fixed "
        "per-entry sizing are replayed with unlimited portfolio capital.", "",
        "Gate codes: **N** = dated Nifty both-counter veto; **L** = G7 V4 extreme / "
        "efficiency / weak-RSI veto; **A** = G7 named setup ATR maximum; **S** = G7 "
        "named setup OI or traded-value minimum. **none** is the V21 baseline. "
        "**N+L+A+S** is the current all-G7 entry-check research variant.", "",
        "## All 65 sessions", "",
        "| Gates | Orders | Trades | Losses | Win rate | Mean trade (Rs) | Median trade (Rs) | Net P&L (Rs) | Losing days | Daily-close DD (Rs) |",
        "|:---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for code in ARMS:
        row = full.loc[_label(code)]
        lines.append(f"| {_label(code)} | {int(row.selected_orders)} | {int(row.trades)} | "
                     f"{int(row.losses)} | {row.win_rate_pct:.2f}% | "
                     f"{row.average_trade_rs:,.2f} | {row.median_trade_rs:,.2f} | "
                     f"{row.net_pnl_rs:,.2f} | {int(row.negative_days)} | "
                     f"{row.daily_close_drawdown_rs:,.2f} |")
    lines += ["", "## Tradeoffs visible in the saved history", "",
              f"- Compared with V21 baseline, **A+S** changes {int(base.trades)} to "
              f"{int(atr_slot.trades)} fills, {int(base.losses)} to {int(atr_slot.losses)} "
              f"losses, win rate {base.win_rate_pct:.2f}% to {atr_slot.win_rate_pct:.2f}%, "
              f"mean trade Rs {base.average_trade_rs:,.2f} to Rs "
              f"{atr_slot.average_trade_rs:,.2f}, median Rs {base.median_trade_rs:,.2f} "
              f"to Rs {atr_slot.median_trade_rs:,.2f}, and total net Rs "
              f"{base.net_pnl_rs:,.2f} to Rs {atr_slot.net_pnl_rs:,.2f}.",
              f"- Compared with **N+L+A+S**, A+S yields Rs "
              f"{atr_slot.net_pnl_rs - all_g7.net_pnl_rs:,.2f} more total net, but "
              f"{int(atr_slot.losses-all_g7.losses)} more losing trades; its mean trade "
              f"changes Rs {all_g7.average_trade_rs:,.2f} to Rs "
              f"{atr_slot.average_trade_rs:,.2f}. These objectives trade off.", "",
              "## Later six observed sessions", "",
              "The first 59 sessions run through 27 October; the later six are 28 October "
              "through 4 November. They have already been observed and are a sensitivity "
              "check, not a prospective test.", "",
              "| Gates | Early 59 trades | Early net (Rs) | Later 6 trades | Later losses | Later win rate | Later mean trade (Rs) | Later median trade (Rs) | Later net (Rs) |",
              "|:---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for code in ("", "AS", "NLAS"):
        a, b = early.loc[_label(code)], later.loc[_label(code)]
        lines.append(f"| {_label(code)} | {int(a.trades)} | {a.net_pnl_rs:,.2f} | "
                     f"{int(b.trades)} | {int(b.losses)} | {b.win_rate_pct:.2f}% | "
                     f"{b.average_trade_rs:,.2f} | {b.median_trade_rs:,.2f} | "
                     f"{b.net_pnl_rs:,.2f} |")
    chop_full = exploratory.loc[exploratory.period.eq("full_65")].set_index("policy")
    chop_later = exploratory.loc[exploratory.period.eq("later_6")].set_index("policy")
    lines += ["", "## Separate exploratory CHOP14 check", "",
              "This is a **new, exploratory round-number** trend filter, not a locked G7 "
              "threshold. It starts with all four G7 entry checks and vetoes a selected "
              "candidate when its confirmation-minute CHOP14 exceeds 50. Missing CHOP14 "
              "passes through; it is not inferred from later bars. The 50 threshold was "
              "checked once here, without a sweep. The locked G7 low-efficiency veto "
              "does not use CHOP14; a separate unselected G7 research candidate "
              "uses a different CHOP threshold.", "",
              f"Among the {chop_summary['all_g7_selected']} all-G7 selected orders, "
              f"{chop_summary['missing_chop_selected']} lack a ready CHOP14 value; "
              f"{chop_summary['missing_chop_filled']} of those filled. "
              f"The extra gate vetoes {chop_summary['extra_vetoes']} selected orders.", "",
              "| Policy | Trades | Losses | Win rate | Mean trade (Rs) | Median trade (Rs) | Net P&L (Rs) | Daily-close DD (Rs) | Later six net (Rs) |",
              "|:---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, label in (("all_g7", "Existing all G7"),
                        ("all_g7_plus_chop14_le50", "All G7 + CHOP14 <=50"),
                        ("all_g7_plus_noon_chop14_le50", "All G7 + noon CHOP14 <=50")):
        row, last = chop_full.loc[name], chop_later.loc[name]
        lines.append(f"| {label} | {int(row.trades)} | {int(row.losses)} | "
                     f"{row.win_rate_pct:.2f}% | {row.average_trade_rs:,.2f} | "
                     f"{row.median_trade_rs:,.2f} | {row.net_pnl_rs:,.2f} | "
                     f"{row.daily_close_drawdown_rs:,.2f} | {last.net_pnl_rs:,.2f} |")
    lines += ["", "The global extra CHOP check can improve win rate, loss count and "
              "per-trade profit while reducing total net P&L, so it does not dominate "
              "all G7. The noon-only variant applies the same CHOP check only from "
              "12:00 IST onward. **Its clock condition was chosen after inspecting "
              "history; its full, early and later results are in-sample and do not "
              "validate the rule. It is not recommended for live activation.** "
              f"The noon rule changes P&L on {chop_summary['noon_only_changed_days']} "
              f"of 65 days: {chop_summary['noon_only_improved_days']} improve and "
              f"{chop_summary['noon_only_worse_days']} worsen. Its paired daily "
              f"difference has standard deviation Rs "
              f"{chop_summary['noon_only_daily_difference_std_rs']:,.2f} versus "
              f"a total net gain of Rs {chop_summary['noon_only_net_change_rs']:,.2f}. "
              f"Only {chop_summary['noon_only_later_six_vetoed_fills']} filled trades "
              "are vetoed in the later six sessions.", "",
              "[Its period results](exploratory_overall.csv) | "
              "[day-wise results](exploratory_daily.csv) | "
              "[per-order decisions](exploratory_admission_audit.csv).", "",
              "## Method and limits", "",
              f"The 65 sealed sessions span {days[0]} to {days[-1]}. After-cost trade "
              "mean and median use **executed trades**, not selected orders. Daily-close "
              "drawdown uses realized daily P&L; it does not measure intraday equity lows. "
              "All 16 combinations share the same historical observations, and inspecting "
              "their outcomes can overfit. No combination is a verified live improvement. "
              "The G7 historical slot allowlist, two-loss lock and profit-protection exits "
              "are outside this entry-check comparison.", "",
              "Existing baseline and all-G7 arms reproduce their saved 65-day metrics "
              "after verifying both earlier result manifests. Admission uses only saved "
              "pre-entry gate decisions; no P&L field is used by the filter.", "",
              "[All period results](overall.csv) | [Day-wise results](daily.csv) | "
              "[Per-order decisions](admission_audit.csv) | [Configuration](config.json) | "
              "[Provenance](provenance.json)", ""]
    (output / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def run(output: Path) -> dict:
    output = output.resolve()
    if output.exists() or not output.is_relative_to(PACKAGE) or output == PACKAGE:
        raise ValueError("Output must be a fresh child of isolated improvement research")
    source_manifest = _verify_manifest(SOURCE)
    prior_manifest = _verify_manifest(PRIOR)
    _, expanded64, days64, _ = finite._load_books()
    if len(days64) != 64 or days64[0] != "2026-07-29" or days64[-1] != "2026-11-03":
        raise ValueError("Sealed 64-day V21 inventory changed")
    expanded_latest, latest_proof = build_expanded_day(DAY)
    if set(expanded64.sid.astype(int)) & set(expanded_latest.sid.astype(int)):
        raise ValueError("Latest-day native SID collision")
    expanded = pd.concat((expanded64, expanded_latest), ignore_index=True, sort=False)
    selected = v21.apply(expanded)
    if len(selected) != 1092 or selected.setup_id.nunique() != 73 or selected.sid.duplicated().any():
        raise ValueError("65-day strict V21 selected inventory changed")
    audit = pd.read_csv(SOURCE / "admission_audit.csv", low_memory=False)
    prior_audit = pd.read_csv(PRIOR / "admission_audit.csv", low_memory=False)
    if (len(audit) != len(selected) or audit.sid.duplicated().any() or
            set(audit.sid.astype(int)) != set(selected.sid.astype(int)) or
            set(prior_audit.sid.astype(int)) != set(audit.loc[audit.day.astype(str).ne(DAY), "sid"].astype(int))):
        raise ValueError("Saved pre-entry audit does not cover exact V21 selected SIDs")
    if (not audit.research_policy_fingerprint.eq(g7.fingerprint()).all() or
            not prior_audit.research_policy_fingerprint.eq(g7.fingerprint()).all()):
        raise ValueError("G7 saved decision policy fingerprint changed")
    for column in ("day", "setup_id", "tradingsymbol", "side"):
        left = selected.set_index("sid")[column].astype(str).sort_index()
        right = audit.set_index("sid")[column].astype(str).sort_index()
        if not left.equals(right):
            raise ValueError(f"Saved audit identity mismatch: {column}")
    audit = audit.sort_values("sid", kind="stable").reset_index(drop=True)
    for code, column in GATE_COLUMNS.items():
        audit[column] = run_latest._flags(audit[column], column)
    if not audit[list(GATE_COLUMNS.values())].all(axis=1).equals(
            run_latest._flags(audit["all_g7_entry_allowed"], "all_g7_entry_allowed")):
        raise ValueError("Saved all-G7 veto decision is inconsistent")
    selected_ids = selected.sid.astype(int)
    periods = {
        "full_65": [*days64, DAY],
        "early_59": [day for day in days64 if day <= "2026-10-27"],
        "later_6": [day for day in [*days64, DAY] if day > "2026-10-27"],
        "nov04_only": [DAY],
    }
    if tuple(map(len, periods.values())) != (65, 59, 6, 1):
        raise ValueError("Research period inventory changed")
    overall_rows, daily_rows = [], []
    all_g7_book: pd.DataFrame | None = None
    admission = audit[["sid", "day", "setup_id", "tradingsymbol", "side", "pair_class",
                       *GATE_COLUMNS.values()]].copy()
    for code in ARMS:
        allowed = audit[[GATE_COLUMNS[part] for part in code]].all(axis=1) if code else pd.Series(True, index=audit.index)
        admitted_ids = set(audit.loc[allowed, "sid"].astype(int))
        subset = selected.loc[selected_ids.isin(admitted_ids)].copy()
        if len(subset) != int(allowed.sum()) or subset.sid.duplicated().any():
            raise ValueError(f"Gate combination selected SID mismatch: {code}")
        book = unlimited._unlimited(subset)
        if code == "NLAS":
            all_g7_book = book
        admission[f"admit_{_label(code)}"] = audit.sid.astype(int).isin(admitted_ids).to_numpy(bool)
        for name, days in periods.items():
            overall_rows.append(dict(period=name, gates=_label(code), **_metrics(book, days)))
        for day in periods["full_65"]:
            daily_rows.append(dict(day=day, gates=_label(code), **_metrics(book, [day])))
    overall = pd.DataFrame(overall_rows)
    daily = pd.DataFrame(daily_rows)
    _check_original_parity(overall)
    if all_g7_book is None:
        raise ValueError("All-G7 reference book is missing")
    chop14 = pd.to_numeric(audit.chop14, errors="coerce")
    ready = np.isfinite(chop14)
    if (ready & ~run_latest._flags(audit.indicator_ready, "indicator_ready")).any():
        raise ValueError("CHOP14 has values marked unavailable")
    feature_ts = pd.to_datetime(audit.loc[ready, "indicator_available_at"], utc=True)
    confirmation_ts = pd.to_datetime(audit.loc[ready, "confirmation_ts"], utc=True)
    if feature_ts.isna().any() or not feature_ts.le(confirmation_ts).all():
        raise ValueError("CHOP14 was unavailable at candidate confirmation")
    all_g7_allowed = audit[list(GATE_COLUMNS.values())].all(axis=1)
    chop_allowed = ~ready | chop14.le(50.0)
    extra_allowed = all_g7_allowed & chop_allowed
    confirmation_clock = pd.to_datetime(audit.confirmation_ts, utc=True).dt.tz_convert("Asia/Kolkata")
    after_noon = confirmation_clock.dt.hour.mul(60).add(confirmation_clock.dt.minute).ge(720)
    noon_allowed = all_g7_allowed & (~after_noon | chop_allowed)
    extra_ids = set(audit.loc[extra_allowed, "sid"].astype(int))
    extra_subset = selected.loc[selected_ids.isin(extra_ids)].copy()
    extra_book = unlimited._unlimited(extra_subset)
    noon_ids = set(audit.loc[noon_allowed, "sid"].astype(int))
    noon_subset = selected.loc[selected_ids.isin(noon_ids)].copy()
    noon_book = unlimited._unlimited(noon_subset)
    exploratory_rows, exploratory_daily = [], []
    for name, book in (("all_g7", all_g7_book),
                       ("all_g7_plus_chop14_le50", extra_book),
                       ("all_g7_plus_noon_chop14_le50", noon_book)):
        for period, days in periods.items():
            exploratory_rows.append(dict(period=period, policy=name, **_metrics(book, days)))
        for day in periods["full_65"]:
            exploratory_daily.append(dict(day=day, policy=name, **_metrics(book, [day])))
    exploratory_overall = pd.DataFrame(exploratory_rows)
    exploratory_daywise = pd.DataFrame(exploratory_daily)
    paired = (exploratory_daywise.pivot(index="day", columns="policy", values="net_pnl_rs")
              .reindex(periods["full_65"]))
    difference = paired["all_g7_plus_noon_chop14_le50"].sub(paired["all_g7"])
    noon_vetoed_fills = all_g7_book.loc[all_g7_book.portfolio_executed.eq(True) &
                                         ~all_g7_book.sid.astype(int).isin(noon_ids)]
    missing_ids = set(audit.loc[all_g7_allowed & ~ready, "sid"].astype(int))
    chop_summary = dict(threshold=50.0, threshold_status="EXPLORATORY_NOT_LOCKED_G7",
                        missing_policy="PASS_WHEN_UNAVAILABLE_AT_CONFIRMATION",
                        all_g7_selected=int(all_g7_allowed.sum()),
                        missing_chop_selected=len(missing_ids),
                        missing_chop_filled=int((all_g7_book.portfolio_executed.eq(True) &
                                                 all_g7_book.sid.astype(int).isin(missing_ids)).sum()),
                        extra_vetoes=int((all_g7_allowed & ~chop_allowed).sum()),
                        noon_only_status="POST_HOC_CLOCK_CONDITION_NOT_VALIDATED",
                        noon_only_clock="confirmation_time_at_or_after_12:00_Asia_Kolkata",
                        noon_only_extra_vetoes=int((all_g7_allowed & ~noon_allowed).sum()),
                        noon_only_vetoed_fills=len(noon_vetoed_fills),
                        noon_only_later_six_vetoed_fills=int(noon_vetoed_fills.day.astype(str).gt("2026-10-27").sum()),
                        noon_only_nov04_vetoed_fills=int(noon_vetoed_fills.day.astype(str).eq(DAY).sum()),
                        noon_only_changed_days=int(difference.abs().gt(.000001).sum()),
                        noon_only_improved_days=int(difference.gt(.000001).sum()),
                        noon_only_worse_days=int(difference.lt(-.000001).sum()),
                        noon_only_daily_difference_std_rs=float(difference.std(ddof=1)),
                        noon_only_net_change_rs=float(difference.sum()))
    exploratory_admission = audit[["sid", "day", "setup_id", "tradingsymbol", "side",
                                   "confirmation_ts", "indicator_available_at", "chop14"]].copy()
    exploratory_admission["chop14_ready"] = ready.to_numpy(bool)
    exploratory_admission["all_g7_admitted"] = all_g7_allowed.to_numpy(bool)
    exploratory_admission["chop14_le50_or_missing"] = chop_allowed.to_numpy(bool)
    exploratory_admission["all_g7_plus_chop14_admitted"] = extra_allowed.to_numpy(bool)
    exploratory_admission["confirmation_at_or_after_noon_ist"] = after_noon.to_numpy(bool)
    exploratory_admission["all_g7_plus_noon_chop14_admitted"] = noon_allowed.to_numpy(bool)
    for code in ARMS:
        full = overall.loc[overall.period.eq("full_65") & overall.gates.eq(_label(code))].iloc[0]
        rows = daily.loc[daily.gates.eq(_label(code))]
        if (abs(float(rows.net_pnl_rs.sum()) - float(full.net_pnl_rs)) > .01 or
                int(rows.trades.sum()) != int(full.trades) or
                int(rows.wins.sum()) != int(full.wins) or
                int(rows.losses.sum()) != int(full.losses)):
            raise ValueError(f"Day-wise metrics failed reconciliation: {code}")
    summary = dict(scope="BACKTEST_ONLY", execution_authority=False,
                   method="All 16 fixed combinations of four previously computed pre-entry G7 vetoes; no rejected leader refill",
                   source_v21_policy_fingerprint=v21.fingerprint(),
                   source_g7_policy_fingerprint=g7.fingerprint(),
                   gates=GATE_DESCRIPTIONS, arm_codes=[_label(code) for code in ARMS],
                   all_73_pairs=True, original_pair_cap=3, new_pair_cap=1,
                   new_pair_5m_rvol_increment=0.10,
                   portfolio="unlimited capital, same native V21 per-entry size, fill/exit paths and costs",
                   periods={name: dict(first=days[0], last=days[-1], sessions=len(days))
                            for name, days in periods.items()},
                   source_result_manifest_files=len(source_manifest),
                   prior_result_manifest_files=len(prior_manifest),
                   baseline_and_all_g7_saved_parity="PASS_ALL_SIX_EXISTING_ARMS_FULL65_AND_NOV04",
                   exploratory_chop14=chop_summary,
                   latest_source=latest_proof)
    output.mkdir(parents=True)
    overall.to_csv(output / "overall.csv", index=False)
    daily.to_csv(output / "daily.csv", index=False)
    admission.to_csv(output / "admission_audit.csv", index=False)
    exploratory_overall.to_csv(output / "exploratory_overall.csv", index=False)
    exploratory_daywise.to_csv(output / "exploratory_daily.csv", index=False)
    exploratory_admission.to_csv(output / "exploratory_admission_audit.csv", index=False)
    (output / "config.json").write_text(json.dumps(summary, indent=2,
                                                       default=str, allow_nan=False) + "\n", encoding="utf-8")
    provenance = dict(
        code_sha256={str(path): original_run._sha(path) for path in
                     (Path(__file__), Path(v21.__file__), Path(g7.__file__),
                      Path(finite.__file__), Path(unlimited.__file__),
                      Path(build_expanded_day.__code__.co_filename))},
        source_65_manifest_sha256=original_run._sha(SOURCE / "manifest.json"),
        source_64_manifest_sha256=original_run._sha(PRIOR / "manifest.json"),
        source_65_admission_audit_sha256=original_run._sha(SOURCE / "admission_audit.csv"),
        checkpoint_sha256={path.name: original_run._sha(path) for path in
                           sorted(finite.CHECKPOINTS.glob("[0-9][0-9]_*.pkl"))})
    (output / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
    _report(output, overall, periods["full_65"], exploratory_overall, chop_summary)
    (output / "manifest.json").write_text(json.dumps(
        {path.name: original_run._sha(path) for path in output.iterdir() if path.is_file()},
        indent=2) + "\n", encoding="utf-8")
    print(overall.loc[overall.period.eq("full_65"),
          ["gates", "selected_orders", "trades", "losses", "win_rate_pct",
           "average_trade_rs", "median_trade_rs", "net_pnl_rs",
           "daily_close_drawdown_rs"]].to_string(index=False), flush=True)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=PACKAGE / "results_final_20261104_labeled")
    args = parser.parse_args()
    run(args.output)


if __name__ == "__main__":
    main()
