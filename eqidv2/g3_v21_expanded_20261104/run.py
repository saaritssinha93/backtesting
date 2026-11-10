"""Compare frozen V21 with an isolated cap-three, all-slot-through-14:00 replay."""
from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import pickle

import numpy as np
import pandas as pd

from fno_v13_v10_g_3_daily_replay import simulate
from g3_v18_20261102 import data
from g3_v18_20261102.run import metrics
from . import selection, source


PACKAGE = Path(__file__).resolve().parent
FROZEN = PACKAGE.parent / "g3_v21_selective_confirmation_20261104/results_full_20261104"


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _dump(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")


def _daily(book: pd.DataFrame, day: str) -> dict:
    return metrics(book, [day])


def _one_day(i: int, day: str, prior: pd.Series) -> dict:
    raw, minutes, proof = source.load_day(day)
    raw = raw.reset_index(drop=True)
    raw["sid"] = i * 1_000_000 + np.arange(len(raw), dtype=np.int64)
    orders, audit, frozen_orders = selection.select(raw)
    if orders.groupby(["day", "setup_id"]).size().gt(selection.CAP).any():
        raise ValueError(f"Cap breached: {day}")
    added_specs = orders.loc[orders.setup_id.isin(selection.NEWLY_ENABLED)]
    if (not added_specs.native_target_pct.eq(selection.NEW_TARGET_PCT).all()
            or not added_specs.native_stop_pct.eq(selection.NEW_STOP_PCT).all()):
        raise ValueError(f"New setup exit rule drift: {day}")
    new_clocks = orders.loc[orders.setup_id.isin(selection.EXTRA_SETUPS)]
    if len(new_clocks) and not (pd.to_datetime(new_clocks.confirmation_ts, utc=True)
            - pd.to_datetime(new_clocks.signal_ts, utc=True)).eq(pd.Timedelta(minutes=1)).all():
        raise ValueError(f"New setup confirmation clock drift: {day}")
    day_books = {}
    for arm, frame in (("v21_frozen", frozen_orders), ("v21_expanded_cap3", orders)):
        paths = data.execution_paths(frame, minutes)
        book, _ = simulate(frame, paths, [date.fromisoformat(day)])
        book["research_arm"] = arm
        book["execution_authority"] = False
        day_books[arm] = book
    base, changed = (_daily(day_books[arm], day) for arm in day_books)
    if (base["selected_orders"] != prior.v21_orders or base["trades"] != prior.v21_trades
            or abs(base["net_pnl_rs"] - prior.v21_pnl_rs) > .01):
        raise ValueError(f"Frozen V21 reproduction failed on {day}: {base}")
    daily_row = dict(day=day, v21_orders=base["selected_orders"],
                     expanded_orders=changed["selected_orders"],
                     v21_trades=base["trades"], expanded_trades=changed["trades"],
                     v21_pnl_rs=base["net_pnl_rs"], expanded_pnl_rs=changed["net_pnl_rs"],
                     delta_pnl_rs=changed["net_pnl_rs"] - base["net_pnl_rs"])
    expansion = proof.get("v21_expansion", {})
    source_detail = dict(day=day, baseline=dict(
        feature_path=proof.get("feature_path"),
        features_sha256=proof.get("features_sha256"),
        run_manifest=proof.get("run_manifest"),
        run_manifest_sha256=proof.get("run_manifest_sha256"),
        snapshot_manifest=proof.get("snapshot_manifest"),
        snapshot_manifest_sha256=proof.get("snapshot_manifest_sha256")),
        expansion=expansion)
    source_row = dict(day=day, loader_source=proof.get("loader_source"),
        feature_stocks=proof.get("feature_stocks"), feature_rows=len(raw),
        minutes_stocks=proof.get("minute_stocks"),
        added_clock_rows=expansion.get("appended_feature_rows"),
        missing_clock_rows=expansion.get("missing_append_rows"),
        snapshot_manifest=proof.get("snapshot_manifest"),
        source_manifest=proof.get("source_manifest"))
    return dict(day=day, books=day_books, audit=audit, daily=daily_row,
                source_row=source_row, source_detail=source_detail)


def _report(output: Path, summary: dict, daily: pd.DataFrame, slots: pd.DataFrame) -> None:
    old = summary["comparison"]["v21_frozen"]
    new = summary["comparison"]["v21_expanded_cap3"]
    lines = [
        "# G3 V21 expanded backtest through 14:00",
        "",
        "Research-only V21 variant. The frozen V21 code and saved results were not edited. ",
        "The replay covers the same 64 sealed sessions with expanded setup selection and new-pair exits. ",
        "Every setup/direction pair can select at most three stocks per day.",
        "",
        "## Rules",
        "",
        "- All 61 configured G3 donor setup/direction pairs through 13:30 are enabled. The 34 already enabled V21 pairs retain their targets and V21 confirmation rules.",
        "- The 27 newly enabled donor pairs use 0.8% target and 1.25% initial stop.",
        "- Six new five-minute signal clocks, 13:35 through 14:00, add LONG and SHORT pairs (12 total). All have 0.8% target and 1.25% initial stop. A 14:00 signal is confirmed at 14:01.",
        "- New LONG pairs clone the untuned 13:30 LONG entry gates; new SHORT pairs clone the 13:25 SHORT gates. Exact next-minute confirmation, EMA stack, price, OI and volume gates remain required. Orders are ranked by liquidity with core gates first.",
        "- The inherited G3 stop starts at 1.25% and tightens to 1.0% after 120 minutes. The shared portfolio simulator retains ₹100,000 capital per entry, 5× exposure, ₹1,000,000 capital, 10-minute entry expiry, 5 bps round-trip costs and 15:15 square-off.",
        "",
        "| New 13:35–14:00 side | Directional price | OI rise | 5m volume ratio | 1m body | Directional wick max | 1m volume ratio |",
        "|:---|---:|---:|---:|---:|---:|---:|",
        "| LONG | ≥0.15% | ≥0.07% | ≥1.40 | ≥0.55 | ≤0.45 | ≥0.80 |",
        "| SHORT | ≥0.13% | ≥0.05% | ≥1.00 | ≥0.40 | ≤0.60 | ≥1.20 |",
        "",
        "## Full-history comparison",
        "",
        "| Arm | Sessions | Orders | Executed | Win rate | Net P&L (₹) | Daily-close drawdown (₹) | Worst day (₹) |",
        "|:---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label, row in (("Frozen V21", old), ("V21 expanded cap 3", new)):
        lines.append(f"| {label} | {row['sessions']} | {row['selected_orders']} | {row['trades']} | "
                     f"{row['win_rate_pct']:.2f}% | {row['net_pnl_rs']:,.2f} | "
                     f"{row['daily_close_drawdown_rs']:,.2f} | {row['worst_day_rs']:,.2f} |")
    lines.extend(["", f"**P&L change:** ₹{new['net_pnl_rs'] - old['net_pnl_rs']:,.2f}. "
                  f"**Executed-trade change:** {new['trades'] - old['trades']:+d}. "
                  f"**Frozen V21 reproduction:** {summary['frozen_v21_reproduction']}.", "",
                  "## Source coverage", "",
                  f"The replay appended {summary['coverage']['appended_feature_rows']:,} completed-clock stock rows "
                  f"from dated archives or hash-checked daily snapshots. "
                  f"Expected rows still missing: {summary['coverage']['missing_clock_rows']:,} "
                  f"on {summary['coverage']['days_with_missing_rows']} sessions. "
                  "Original V21 feature rows and execution minutes were preserved. "
                  "A feature row may still lack futures OI; approximately 2.62% of the six late-clock "
                  "stock rows in the first 38 archive sessions had unusable OI and failed the strict gate. "
                  "October 1 is excluded from both arms because its immutable source is incomplete.", "",
                  "## Five sessions after the V21 allowlist cutoff", "",
                  "| Arm | Orders | Executed | Win rate | Net P&L (₹) | Worst day (₹) |",
                  "|:---|---:|---:|---:|---:|---:|"])
    for label, row in (("Frozen V21", summary["after_allowlist_cutoff"]["v21_frozen"]),
                       ("V21 expanded cap 3", summary["after_allowlist_cutoff"]["v21_expanded_cap3"])):
        lines.append(f"| {label} | {row['selected_orders']} | {row['trades']} | "
                     f"{row['win_rate_pct']:.2f}% | {row['net_pnl_rs']:,.2f} | "
                     f"{row['worst_day_rs']:,.2f} |")
    lines.extend(["", "## Selection changes", "",
                  f"- Newly selected versus frozen V21: {summary['selection_changes']['added_orders']} orders.",
                  f"- Removed by the three-entry cap/ranking: {summary['selection_changes']['removed_orders']} frozen V21 orders.",
                  f"- New 13:35–14:00 pairs: {summary['selection_changes']['new_clock_orders']} orders; "
                  f"{summary['selection_changes']['new_clock_executed']} portfolio executions.",
                  "", "## Day-wise results", "",
                  "| Day | V21 orders | Expanded orders | V21 trades | Expanded trades | V21 P&L (₹) | Expanded P&L (₹) | Change (₹) |",
                  "|:---|---:|---:|---:|---:|---:|---:|---:|"])
    for row in daily.itertuples(index=False):
        lines.append(f"| {row.day} | {row.v21_orders} | {row.expanded_orders} | "
                     f"{row.v21_trades} | {row.expanded_trades} | {row.v21_pnl_rs:,.2f} | "
                     f"{row.expanded_pnl_rs:,.2f} | {row.delta_pnl_rs:,.2f} |")
    lines.extend(["", "## Newly enabled setup activity", "",
                  "| Setup | Class | Selected | Executed | Portfolio net P&L (₹) |",
                  "|:---|:---|---:|---:|---:|"])
    for row in slots.loc[slots.slot_class.ne("existing_v21")].itertuples(index=False):
        lines.append(f"| {row.setup_id} | {row.slot_class} | {row.selected} | "
                     f"{row.executed} | {row.portfolio_pnl_rs:,.2f} |")
    lines.extend(["", "**Interpretation:** The new rules and slots were evaluated on historical sessions; "
                  "their P&L is in-sample evidence, not a forward expectation. The frozen V21 "
                  "allowlist used outcomes through 27 October, leaving five later sealed sessions. "
                  "Per-slot P&L is the expanded portfolio's realized contribution and cannot be "
                  "summed to infer the strategy change because orders compete for capital.", "",
                  "[Daily CSV](daily_comparison.csv) · [Slot CSV](slot_results.csv) · "
                  "[Expanded trades](expanded_trades.csv) · [Selection audit](selection_audit.csv) · "
                  "[Source coverage](source_coverage.csv)", ""])
    (output / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def run(output: Path, through: date) -> None:
    output = output.resolve()
    if output.exists() or not output.is_relative_to(PACKAGE) or output == PACKAGE:
        raise ValueError("Output must be a new child of the isolated V21 package")
    days = source.available_days(through.isoformat())
    if not days or days[-1] != "2026-11-03" or len(days) != 64:
        raise ValueError(f"Expected 64 sealed sessions through 2026-11-03; got {len(days)}")
    saved = pd.read_csv(FROZEN / "daily_comparison.csv")
    if saved.day.astype(str).tolist() != days:
        raise ValueError("Frozen V21 day cohort changed")
    books = {"v21_frozen": [], "v21_expanded_cap3": []}
    audits, source_rows, source_details, daily = [], [], [], []
    checkpoint_dir = PACKAGE / f"{output.name}_checkpoints"
    checkpoint_dir.mkdir(exist_ok=True)
    source_sha, selection_sha, engine_sha, policy_fingerprint, frozen_sha = (
        _sha(Path(source.__file__)), _sha(Path(selection.__file__)), _sha(Path(__file__)),
        selection.fingerprint(), _sha(FROZEN / "daily_comparison.csv"))
    for i, day in enumerate(days):
        if (_sha(Path(source.__file__)) != source_sha or
                _sha(Path(selection.__file__)) != selection_sha or _sha(Path(__file__)) != engine_sha):
            raise ValueError("Replay code changed during the run")
        checkpoint_path = checkpoint_dir / f"{i:02d}_{day}.pkl"
        if checkpoint_path.is_file():
            record = pickle.loads(checkpoint_path.read_bytes())
            if (record.get("day") != day or record.get("source_sha") != source_sha
                    or record.get("selection_sha") != selection_sha
                    or record.get("engine_sha") != engine_sha
                    or record.get("policy_fingerprint") != policy_fingerprint
                    or record.get("frozen_sha") != frozen_sha):
                raise ValueError(f"Checkpoint provenance mismatch: {checkpoint_path}")
            cached = True
        else:
            record = _one_day(i, day, saved.iloc[i])
            record.update(source_sha=source_sha, selection_sha=selection_sha,
                          engine_sha=engine_sha, policy_fingerprint=policy_fingerprint,
                          frozen_sha=frozen_sha)
            temporary = checkpoint_path.with_suffix(".tmp")
            temporary.write_bytes(pickle.dumps(record, protocol=pickle.HIGHEST_PROTOCOL))
            temporary.replace(checkpoint_path)
            cached = False
        for arm in books:
            books[arm].append(record["books"][arm])
        audits.append(record["audit"])
        daily.append(record["daily"])
        source_rows.append(record["source_row"])
        source_details.append(record["source_detail"])
        print(f"{day} [{i+1}/{len(days)}] V21={record['daily']['v21_orders']} "
              f"expanded={record['daily']['expanded_orders']} "
              f"PnL delta={record['daily']['delta_pnl_rs']:.2f} "
              f"{'checkpoint' if cached else 'computed'}", flush=True)
    books = {key: pd.concat(value, ignore_index=True, sort=False) for key, value in books.items()}
    audit = pd.concat(audits, ignore_index=True, sort=False)
    comparison = {arm: metrics(book, days) for arm, book in books.items()}
    selection_changes = dict(
        added_orders=int((audit.v9_selected & ~audit.v21_selected).sum()),
        removed_orders=int((~audit.v9_selected & audit.v21_selected).sum()),
        new_clock_orders=int((audit.v9_selected & audit.setup_id.isin(selection.EXTRA_SETUPS)).sum()),
        new_clock_executed=int(books["v21_expanded_cap3"].loc[
            books["v21_expanded_cap3"].setup_id.isin(selection.EXTRA_SETUPS),
            "portfolio_executed"].astype(str).str.lower().eq("true").sum()),
    )
    ledger = books["v21_expanded_cap3"].copy()
    ledger["executed_flag"] = ledger.portfolio_executed.astype(str).str.lower().eq("true")
    ledger["realized_pnl"] = pd.to_numeric(ledger.portfolio_net_profit_rupees, errors="coerce").fillna(0).where(ledger.executed_flag, 0)
    slot = ledger.groupby("setup_id", dropna=False).agg(selected=("sid", "size"),
        executed=("executed_flag", "sum"), portfolio_pnl_rs=("realized_pnl", "sum")).reset_index()
    slot = pd.DataFrame({"setup_id": sorted(selection.SPECS)}).merge(
        slot, on="setup_id", how="left", validate="one_to_one")
    slot[["selected", "executed", "portfolio_pnl_rs"]] = slot[
        ["selected", "executed", "portfolio_pnl_rs"]].fillna(0)
    slot[["selected", "executed"]] = slot[["selected", "executed"]].astype(int)
    slot["slot_class"] = ["new_1335_to_1400" if key in selection.EXTRA_SETUPS else
                          "newly_enabled_donor" if key in selection.NEWLY_ENABLED else "existing_v21"
                          for key in slot.setup_id]
    slot = slot.sort_values(["slot_class", "setup_id"]).reset_index(drop=True)
    coverage = dict(appended_feature_rows=int(sum(row["added_clock_rows"] or 0 for row in source_rows)),
                    missing_clock_rows=int(sum(row["missing_clock_rows"] or 0 for row in source_rows)),
                    days_with_missing_rows=int(sum(bool(row["missing_clock_rows"]) for row in source_rows)))
    later_days = [day for day in days if day > "2026-10-27"]
    after_allowlist_cutoff = {arm: metrics(book, later_days) for arm, book in books.items()}
    summary = dict(days=days, latest_sealed=days[-1], frozen_v21_reproduction="PASS_64_OF_64_DAYS",
        policy=selection.configuration(), comparison=comparison, selection_changes=selection_changes,
        coverage=coverage, after_allowlist_cutoff=after_allowlist_cutoff,
        execution_authority=False,
        limitations=["Retrospective historical evaluation of new setups and clock expansion",
                     "V21 allowlist selected using outcomes through 2026-10-27",
                     "Only five sealed sessions after that date",
                     "2026-10-01 excluded: incomplete immutable source",
                     "Some older late-clock rows lack futures OI and therefore fail the strict OI gate"])
    output.mkdir(parents=True)
    pd.DataFrame(daily).to_csv(output / "daily_comparison.csv", index=False)
    pd.DataFrame(source_rows).to_csv(output / "source_coverage.csv", index=False)
    slot.to_csv(output / "slot_results.csv", index=False)
    audit.to_csv(output / "selection_audit.csv", index=False)
    for arm, book in books.items():
        book.to_csv(output / ("v21_replayed_trades.csv" if arm == "v21_frozen" else "expanded_trades.csv"), index=False)
    pd.DataFrame([dict(arm=arm, **value) for arm, value in comparison.items()]).to_csv(
        output / "overall_comparison.csv", index=False)
    _dump(output / "config.json", selection.configuration() | dict(days=days, through=through.isoformat()))
    _dump(output / "source_provenance.json", dict(code={str(path): _sha(path) for path in
        (Path(__file__), Path(selection.__file__), Path(source.__file__), Path(data.__file__))},
        frozen_reference=str(FROZEN / "daily_comparison.csv"),
        frozen_reference_sha256=_sha(FROZEN / "daily_comparison.csv"), sessions=source_details))
    _dump(output / "summary.json", summary)
    _report(output, summary, pd.DataFrame(daily), slot)
    _dump(output / "manifest.json", {path.name: _sha(path) for path in output.iterdir() if path.is_file()})
    print(json.dumps(dict(comparison=comparison, selection_changes=selection_changes), indent=2), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--through", type=date.fromisoformat, default=date(2026, 11, 4))
    args = parser.parse_args()
    run(args.output, args.through)


if __name__ == "__main__":
    main()
