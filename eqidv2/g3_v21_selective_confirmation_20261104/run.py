"""Compare two small G3 confirmation exceptions with unchanged uncapped V19."""
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
from . import selection


PACKAGE = Path(__file__).resolve().parent
V19 = PACKAGE.parent / "g3_v19_uncapped_20261103/results_full_20261103"
TOP2 = PACKAGE.parent / "g3_v20_body_cap_20261104/results_full_20261104/top2_audit.csv"


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _dump(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")


def _report(output: Path, summary: dict, additions: pd.DataFrame) -> None:
    lines = ["# G3 V21: two small confirmation changes", "",
             "This is an isolated backtest based on uncapped G3 V19. Only two enabled "
             "SHORT slots have new confirmation thresholds:", "",
             "| Slot | Confirmation rule | Frozen | V21 | Absolute change |",
             "|:---|:---|---:|---:|---:|",
             "| 1016_SHORT | Minimum 1m candle body ratio | 0.40 | 0.35 | -0.05 |",
             "| 1046_SHORT | Maximum 1m wick ratio | 0.60 | 0.65 | +0.05 |", "",
             "Both slots were chosen from dates through 30 September: each had at least "
             "two orders failing only that confirmation check within 0.05, and the "
             "candidate-only native replay had positive net P&L. The rule is keyed "
             "only to setup and completed confirmation data, never to a future mover "
             "rank or a stock name. All other G3 V19 rules and portfolio constraints "
             "remain fixed.", "",
             "## Portfolio comparison", "",
             "| Period | Arm | Sessions | Orders | Executed | Win rate | Net P&L (Rs) | Daily close drawdown (Rs) |",
             "|:---|:---|---:|---:|---:|---:|---:|---:|"]
    for period in ("training_through_sep30", "threshold_eval_oct_nov", "after_slot_cutoff", "full_64"):
        for arm in ("v19_uncapped", "v21_selective_confirmation"):
            row = summary["comparison"][period][arm]
            lines.append(f"| {period} | {arm} | {row['sessions']} | {row['selected_orders']} | "
                         f"{row['trades']} | {row['win_rate_pct']:.2f}% | "
                         f"{row['net_pnl_rs']:,.2f} | {row['daily_close_drawdown_rs']:,.2f} |")
    lines += ["", f"V19's first 63 days reproduce the saved V19 daily P&L and trade counts: "
              f"**{summary['v19_prior_63_day_reproduction']}**. The 64th sealed "
              "session is 3 November; 4 November is not a completed source day "
              "in this replay.", "",
              f"**New orders:** {len(additions)} across all 64 sessions; "
              f"{summary['new_orders']['executed']} executed. "
              f"New daily top-two movers selected: {summary['top2']['newly_selected']}. "
              "A new order may displace another through portfolio capital limits, "
              "so its own P&L is not the full portfolio difference.", "",
              "## Newly admitted orders", "",
              "| Day | Slot | Stock | Actual confirmation | Frozen threshold | V21 threshold | Portfolio result | Net P&L (Rs) |",
              "|:---|:---|:---|---:|---:|---:|:---|---:|"]
    for row in additions.sort_values(["day", "setup_id", "tradingsymbol"]).itertuples():
        if row.setup_id == "1016_SHORT":
            actual, frozen, effective = row.body_ratio, row.original_body_minimum, row.effective_body_minimum
        else:
            actual, frozen, effective = row.wick_ratio, row.original_wick_maximum, row.effective_wick_maximum
        lines.append(f"| {row.day} | {row.setup_id} | {row.tradingsymbol} | "
                     f"{actual:.4f} | {frozen:.2f} | {effective:.2f} | "
                     f"{row.portfolio_status} | {row.portfolio_net_profit_rupees:,.2f} |")
    lines += ["", "**Interpretation:** These settings were chosen using earlier outcomes. "
              "The frozen G3 slot allowlist itself used outcomes through 27 October. "
              "The five sessions after that date offer only a small later check. "
              "If the October-November evaluation period has no newly admitted "
              "orders, its identical P&L does not validate the adjustment. "
              "A full-day +3% move does not imply a profitable G3 entry.", "",
              "[Daily results](daily_comparison.csv) | [Added orders](new_orders.csv) | "
              "[Full selection audit](selection_audit.csv) | "
              "[V19 trades](v19_trades.csv) | [V21 trades](v21_trades.csv)", ""]
    (output / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def run(output: Path, through: date) -> None:
    output = output.resolve()
    if output.exists() or not output.is_relative_to(PACKAGE) or output == PACKAGE:
        raise ValueError("Output must be a new child of G3 V21")
    days = data.available_days(through.isoformat())
    if not days:
        raise ValueError("No sealed G3 sessions")
    output.mkdir(parents=True)
    books = {"v19_uncapped": [], "v21_selective_confirmation": []}
    audits, sources = [], []
    for i, day in enumerate(days):
        raw, minutes, proof = data.load_day(day)
        raw = raw.reset_index(drop=True)
        raw["sid"] = i * 1_000_000 + np.arange(len(raw), dtype=np.int64)
        new_orders, audit, base_orders = selection.select(raw)
        if not base_orders.sid.isin(new_orders.sid).all():
            raise ValueError("V21 lost baseline selection")
        for arm, orders in (("v19_uncapped", base_orders),
                            ("v21_selective_confirmation", new_orders)):
            paths = data.execution_paths(orders, minutes)
            book, _ = simulate(orders, paths, [date.fromisoformat(day)])
            book["research_arm"] = arm
            book["execution_authority"] = False
            books[arm].append(book)
        audits.append(audit)
        sources.append(dict(day=day, loader_source=proof["loader_source"],
                            feature_stocks=proof["feature_stocks"],
                            minute_stocks=proof["minute_stocks"],
                            breadth_universe=proof["breadth_universe"],
                            feature_path=proof["feature_path"],
                            provenance_path=proof.get("provenance_path"),
                            provenance_sha256=proof.get("provenance_sha256"),
                            run_manifest=proof.get("run_manifest"),
                            run_manifest_sha256=proof.get("run_manifest_sha256")))
        print(f"{day} [{i+1}/{len(days)}] V19={len(base_orders)} V21={len(new_orders)}", flush=True)
    books = {arm: pd.concat(parts, ignore_index=True, sort=False)
             for arm, parts in books.items()}
    old = pd.read_csv(V19 / "daily_results.csv")
    now = pd.DataFrame([dict(day=day, **{
        f"{arm}_pnl_rs": metrics(book, [day])["net_pnl_rs"]
        for arm, book in books.items()}) for day in days])
    matched = old.merge(now, on="day")
    if (len(matched) != 63 or
            not (matched.no_slot_limit_pnl_rs - matched.v19_uncapped_pnl_rs).abs().le(.01).all()):
        raise ValueError("V19 daily P&L did not reproduce")
    periods = {
        "training_through_sep30": [day for day in days if day <= "2026-09-30"],
        "threshold_eval_oct_nov": [day for day in days if day >= "2026-10-05"],
        "after_slot_cutoff": [day for day in days if day > "2026-10-27"],
        "full_64": days,
    }
    comparison = {period: {arm: metrics(book, cohort) for arm, book in books.items()}
                  for period, cohort in periods.items()}
    audit = pd.concat(audits, ignore_index=True, sort=False)
    new = audit.loc[audit.v9_selected.eq(True) & audit.v19_selected.eq(False)].copy()
    ledger = books["v21_selective_confirmation"]
    added = new.merge(ledger[["signal_id", "filled", "portfolio_executed", "portfolio_status",
                              "portfolio_reject_reason", "portfolio_net_profit_rupees",
                              "net_profit_rupees", "entry_ts", "exit_ts"]],
                      on="signal_id", how="left", validate="one_to_one")
    if added.portfolio_status.isna().any():
        raise ValueError("New orders missing replay outcome")
    top = pd.read_csv(TOP2)
    keys = set(zip(new.day.astype(str), new.side, new.tradingsymbol))
    top_new = top.loc[[tuple(row) in keys for row in top[["day", "side", "tradingsymbol"]].itertuples(index=False, name=None)]]
    summary = dict(days=days, latest_sealed=days[-1], policy=selection.configuration(),
                   comparison=comparison, v19_prior_63_day_reproduction="PASS",
                   new_orders=dict(total=len(added), executed=int(added.portfolio_executed.astype(str).str.lower().eq("true").sum()),
                                   training=int(added.day.astype(str).le("2026-09-30").sum()),
                                   threshold_eval=int(added.day.astype(str).ge("2026-10-05").sum())),
                   top2=dict(newly_selected=len(top_new), cases=len(top)),
                   execution_authority=False,
                   limitations=["Retrospective train-selection of slots", "Frozen G3 allowlist tuned through 2026-10-27",
                                "Only five sealed sessions after allowlist cutoff"])
    daily = []
    for day in days:
        base, changed = (metrics(books[arm], [day]) for arm in books)
        daily.append(dict(day=day, v19_orders=base["selected_orders"],
                          v21_orders=changed["selected_orders"],
                          v19_trades=base["trades"], v21_trades=changed["trades"],
                          v19_pnl_rs=base["net_pnl_rs"], v21_pnl_rs=changed["net_pnl_rs"],
                          delta_pnl_rs=changed["net_pnl_rs"]-base["net_pnl_rs"]))
    for arm, book in books.items():
        book.to_csv(output / f"{'v19' if arm == 'v19_uncapped' else 'v21'}_trades.csv", index=False)
    audit.to_csv(output / "selection_audit.csv", index=False)
    added.to_csv(output / "new_orders.csv", index=False)
    pd.DataFrame(daily).to_csv(output / "daily_comparison.csv", index=False)
    pd.DataFrame([dict(period=period, arm=arm, **measure)
                  for period, entries in comparison.items() for arm, measure in entries.items()]).to_csv(
                      output / "overall_comparison.csv", index=False)
    _dump(output / "config.json", selection.configuration() | dict(
        days=days, through=through.isoformat(), cost_bps_round_trip=5,
        initial_portfolio_capital_rs=1_000_000,
        per_entry_capital_rs=100_000, exposure_multiple=5))
    _dump(output / "source_provenance.json", dict(sessions=sources,
        code={str(path):_sha(path) for path in (Path(__file__), Path(selection.__file__), Path(data.__file__))},
        prior_v19_reference=str(V19 / "daily_results.csv"),
        prior_v19_reference_sha256=_sha(V19 / "daily_results.csv"),
        top2_reference=str(TOP2), top2_reference_sha256=_sha(TOP2)))
    _dump(output / "summary.json", summary)
    _report(output, summary, added)
    _dump(output / "manifest.json", {path.name:_sha(path) for path in output.iterdir() if path.is_file()})
    print(json.dumps(dict(sessions=len(days), comparison=comparison,
                          new_orders=summary["new_orders"], top2=summary["top2"]),
                     indent=2, default=str), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--through", type=date.fromisoformat, default=date.today())
    args = parser.parse_args()
    run(args.output, args.through)


if __name__ == "__main__":
    main()
