"""Replay G3 V19 and one-body-gate V20 on every sealed source session."""
from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from fno_v13_v10_g_3_daily_replay import simulate
from g3_v18_20261102 import data
from g3_v18_20261102.run import metrics
from g3_v18_20261102.selection import strict_signals
from g3_v19_uncapped_20261103.run import sha
from . import diagnostics, selection


PACKAGE = Path(__file__).resolve().parent
V19_RESULT = PACKAGE.parent / "g3_v19_uncapped_20261103/results_full_20261103"


def _dump(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")


def _fmt_status(status: str) -> str:
    return {"EXECUTED": "Trade", "SELECTED_ONLY": "Order only",
            "NOT_SELECTED": "Missed"}[status]


def _report(output: Path, days: list[str], summary: dict, top: pd.DataFrame) -> None:
    comparison = summary["comparison"]
    base, changed = (comparison[arm] for arm in ("v19_uncapped", "v20_body_cap_030"))
    lines = ["# G3 daily top-two movers: selection and missed-signal audit", "",
             f"**Coverage:** {len(days)} complete sealed G3 sessions, {days[0]} through {days[-1]}. "
             f"Each day examines the two largest qualifying moves in each direction. "
             f"There are {len(top)} stock/day/direction cases; a direction with fewer than two "
             "stocks moving at least 3% has fewer rows.", "",
             "**Mover definition:** First available 09:16 IST one-minute open to the day's "
             "highest high for LONG or lowest low for SHORT, threshold 3%. First 3% and "
             "extreme times are hindsight diagnostics. G3 uses only its completed signal "
             "bar and next-minute confirmation; the latest signal is 13:30.", "",
             "**Selection meaning:** Trade = portfolio-executed entry; Order only = "
             "selected but its trigger was not hit or portfolio capital was unavailable; "
             "Missed = no order selected in that direction. One stock can have multiple "
             "slot orders. The reason column names the closest enabled slot by fewest "
             "failed gates, or a disabled slot that produced a native signal. Other slots "
             "can have different failures.", "",
             "**One tested change:** V20 caps each enabled slot's minimum next-minute "
             "confirmation candle body ratio at 0.30. A slot already requiring 0.30 or "
             "less is unchanged. All other V19 filters, enabled slots, no-quota policy "
             "and portfolio rules stay fixed. Daily high/low rank is never used in "
             "selecting G3 orders. The 0.30 threshold was examined against these dates, "
             "so this is an in-sample test.", "",
             "## Full-history replay", "",
             "| Metric | V19 no slot cap | V20 body minimum cap 0.30 |", "|:---|---:|---:|",
             f"| Selected orders | {base['selected_orders']} | {changed['selected_orders']} |",
             f"| Executed trades | {base['trades']} | {changed['trades']} |",
             f"| Win rate | {base['win_rate_pct']:.2f}% | {changed['win_rate_pct']:.2f}% |",
             f"| Net P&L (Rs) | {base['net_pnl_rs']:,.2f} | {changed['net_pnl_rs']:,.2f} |",
             f"| Daily-close drawdown (Rs) | {base['daily_close_drawdown_rs']:,.2f} | "
             f"{changed['daily_close_drawdown_rs']:,.2f} |", "",
             f"V19 selected **{summary['top2']['v19_selected']}** of {len(top)} top-two "
             f"cases and executed **{summary['top2']['v19_executed']}**. V20 selected "
             f"**{summary['top2']['v20_selected']}** and executed "
             f"**{summary['top2']['v20_executed']}**. V20 newly selected "
             f"**{summary['top2']['newly_selected']}** cases. "
             f"The full-history net P&L change is Rs "
             f"{changed['net_pnl_rs']-base['net_pnl_rs']:+,.2f}.", "",
             "The report covers the stocks with complete minute data in the sealed G3 "
             "historical sources. It does not cover every NSE security. Some dates have "
             "a dated/research cohort that differs from complete minute coverage; "
             "those source limits are recorded in `source_provenance.json`. "
             "The previously chosen slot allowlist and this body threshold were both "
             "inspected using historical outcomes. Full-day extreme ranking cannot "
             "guarantee a causal entry or a profitable fill.", "",
             "[Full audit CSV](top2_audit.csv) | [V19 trade ledger](v19_trades.csv) | "
             "[V20 trade ledger](v20_trades.csv) | [Daywise P&L](daily_comparison.csv)", "",
             "## Daily top-two audit", ""]
    for day in days:
        part = top.loc[top.day.eq(day)].sort_values(["side", "rank"])
        label = pd.Timestamp(day).strftime("%d %b %Y (%a)")
        lines += [f"### {label}", "",
                  "| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |",
                  "|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|"]
        for row in part.itertuples():
            reason = str(row.v19_reason).replace("|", "/") if row.v19_reason else "-"
            lines.append(f"| {row.side} | {row.rank} | {row.tradingsymbol} | {row.move_pct:+.2f}% | "
                         f"{pd.Timestamp(row.first_3pct_time):%H:%M} | "
                         f"{pd.Timestamp(row.extreme_time):%H:%M} | "
                         f"{_fmt_status(row.v19_status)} | {_fmt_status(row.v20_status)} | {reason} |")
        lines += [""]
    (output / "DAILY_TOP2_G3_SELECTION_AUDIT.md").write_text("\n".join(lines), encoding="utf-8")


def run(output: Path, through: date) -> None:
    output = output.resolve()
    if output.exists() or not output.is_relative_to(PACKAGE) or output == PACKAGE:
        raise ValueError("Output must be a new child of G3 V20")
    days = data.available_days(through.isoformat())
    if not days:
        raise ValueError("No sealed G3 source days")
    output.mkdir(parents=True)
    books = {"v19_uncapped": [], "v20_body_cap_030": []}
    audits, top_rows, sources = [], [], []
    for i, day in enumerate(days):
        raw, minutes, proof = data.load_day(day)
        raw = raw.reset_index(drop=True)
        raw["sid"] = i * 1_000_000 + np.arange(len(raw), dtype=np.int64)
        v20_orders, audit, v19_orders = selection.select(raw)
        if not v19_orders.sid.isin(v20_orders.sid).all():
            raise ValueError(f"V20 lost V19 order on {day}")
        daily_books = {}
        for arm, orders in (("v19_uncapped", v19_orders), ("v20_body_cap_030", v20_orders)):
            paths = data.execution_paths(orders, minutes)
            book, _ = simulate(orders, paths, [date.fromisoformat(day)])
            book["research_arm"] = arm
            book["execution_authority"] = False
            books[arm].append(book)
            daily_books[arm] = book
        audits.append(audit)
        strict = strict_signals(raw)
        v19_audit = audit.copy()
        v19_audit["v9_selected"] = v19_audit.v19_selected
        v19_audit["v9_filter_pass"] = v19_audit.v19_filter_pass
        movers = diagnostics.top_two(minutes, day)
        for mover in movers.to_dict("records"):
            symbol, side = mover["tradingsymbol"], mover["side"]
            for arm, audit_frame, body_cap in (("v19", v19_audit, None),
                                               ("v20", audit, selection.BODY_CAP)):
                book = daily_books["v19_uncapped" if arm == "v19" else "v20_body_cap_030"]
                state, pnl, reject = diagnostics.status(book, symbol, side)
                mover[f"{arm}_status"] = state
                mover[f"{arm}_pnl_rs"] = pnl
                mover[f"{arm}_reason"] = (reject if state == "SELECTED_ONLY" else
                    diagnostics.explain_miss(raw, audit_frame, symbol, side,
                                             strict=strict, body_cap=body_cap)
                    if state == "NOT_SELECTED" else "")
            top_rows.append(mover)
        sources.append(dict(day=day, loader_source=proof["loader_source"],
                            feature_stocks=proof["feature_stocks"],
                            minute_stocks=proof["minute_stocks"],
                            breadth_universe=proof["breadth_universe"],
                            feature_path=proof["feature_path"],
                            run_manifest=proof.get("run_manifest"),
                            run_manifest_sha256=proof.get("run_manifest_sha256"),
                            provenance_path=proof.get("provenance_path"),
                            provenance_sha256=proof.get("provenance_sha256")))
        print(f"{day} [{i+1}/{len(days)}] V19={len(v19_orders)} V20={len(v20_orders)} "
              f"top2={len(movers)}", flush=True)
    books = {arm: pd.concat(parts, ignore_index=True, sort=False)
             for arm, parts in books.items()}
    prior = pd.read_csv(V19_RESULT / "comparison.csv")
    old = prior.loc[prior.period.eq("full_available_history") & prior.arm.eq("no_slot_limit")].iloc[0]
    old_days = [day for day in days if day <= "2026-11-02"]
    reproduced = metrics(books["v19_uncapped"], old_days)
    if (reproduced["trades"] != int(old.trades)
            or reproduced["selected_orders"] != int(old.selected_orders)
            or abs(reproduced["net_pnl_rs"] - float(old.net_pnl_rs)) > .01):
        raise ValueError("V19 63-day result did not reproduce")
    comparisons = {arm: metrics(book, days) for arm, book in books.items()}
    top = pd.DataFrame(top_rows)
    if top.duplicated(["day", "side", "tradingsymbol"]).any():
        raise ValueError("Duplicate top-two mover case")
    counts = dict(v19_selected=int(top.v19_status.ne("NOT_SELECTED").sum()),
                  v19_executed=int(top.v19_status.eq("EXECUTED").sum()),
                  v20_selected=int(top.v20_status.ne("NOT_SELECTED").sum()),
                  v20_executed=int(top.v20_status.eq("EXECUTED").sum()),
                  newly_selected=int((top.v19_status.eq("NOT_SELECTED") &
                                      top.v20_status.ne("NOT_SELECTED")).sum()))
    for arm, book in books.items():
        book.to_csv(output / f"{'v19' if arm == 'v19_uncapped' else 'v20'}_trades.csv", index=False)
    pd.concat(audits, ignore_index=True, sort=False).to_csv(output / "v20_selection_audit.csv", index=False)
    top.to_csv(output / "top2_audit.csv", index=False)
    daily = []
    for day in days:
        base, changed = (metrics(books[arm], [day]) for arm in books)
        daily.append(dict(day=day, v19_pnl_rs=base["net_pnl_rs"],
                          v20_pnl_rs=changed["net_pnl_rs"],
                          delta_pnl_rs=changed["net_pnl_rs"]-base["net_pnl_rs"],
                          v19_trades=base["trades"], v20_trades=changed["trades"],
                          v19_orders=base["selected_orders"], v20_orders=changed["selected_orders"]))
    pd.DataFrame(daily).to_csv(output / "daily_comparison.csv", index=False)
    pd.DataFrame([dict(arm=arm, **result) for arm, result in comparisons.items()]).to_csv(
        output / "overall_comparison.csv", index=False)
    _dump(output / "config.json", selection.configuration() | dict(days=days, through=through.isoformat(),
        cost_bps_round_trip=5, initial_portfolio_capital_rs=1_000_000,
        per_entry_capital_rs=100_000, exposure_multiple=5))
    _dump(output / "source_provenance.json", dict(sessions=sources,
        code={str(path):sha(path) for path in (Path(__file__), Path(selection.__file__),
            Path(diagnostics.__file__), Path(data.__file__))},
        v19_reference=str(V19_RESULT / "comparison.csv"),
        v19_reference_sha256=sha(V19_RESULT / "comparison.csv")))
    summary = dict(days=days, latest_sealed=days[-1], comparison=comparisons,
                   top2=counts, v19_prior_63_day_reproduction="PASS",
                   execution_authority=False, retrospective_threshold_choice=True)
    _dump(output / "summary.json", summary)
    _report(output, days, summary, top)
    _dump(output / "manifest.json", {p.name:hashlib.sha256(p.read_bytes()).hexdigest()
                                    for p in output.iterdir() if p.is_file()})
    print(json.dumps(dict(output=str(output), sessions=len(days), top2=counts,
                          comparison=comparisons), indent=2, default=str), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--through", type=date.fromisoformat, default=date.today())
    args = parser.parse_args()
    run(args.output, args.through)


if __name__ == "__main__":
    main()
