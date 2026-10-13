"""Reallocate the current G3 backtest with all twelve selected G4 late setups.

This is a reproducible, backtest-only combination of hash-verified minute-trade
ledgers. G4 chose its thresholds on the first 38 sessions. The last 11 remain
the original chronological holdout; this script does not optimize them again.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import secrets
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_3_contract as contract
from eqidv2_runtime_paths import runtime_dir
from research.g3_0920_backtest import allocate, _stats


ROOT = runtime_dir("backtesting_result_v13_v10_g_3")
G3_RUN = ROOT / "backtests/runs/2026-10-12/20261013T0920_long_t300_current"
G4_RUN = runtime_dir("backtesting_result_v13_v10_g_4") / (
    "runs/2026-10-12/20261013T010748590154_5e72f1aa_late_slots")
VARIANT = "V13-V10-G-3-BT-V3-PLUS-G4-LATE-12-V1"
CLOCKS = ("10:05", "10:10", "10:15", "10:20", "10:25", "10:30")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verified_run(path: Path, variant: str) -> dict:
    manifest_path = path / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("variant_id") != variant or manifest.get("execution_authority") is not False:
        raise ValueError(f"Unexpected backtest identity: {path}")
    for name, digest in manifest["files"].items():
        if Path(name).name != name or not (path / name).is_file() or sha256(path / name) != digest:
            raise ValueError(f"Backtest source artifact changed: {path / name}")
    return {"run_dir": str(path.resolve()), "manifest_sha256": sha256(manifest_path)}


def same_raw(left: pd.DataFrame, right: pd.DataFrame) -> None:
    a = left.set_index("sid").sort_index()
    b = right.set_index("sid").sort_index()
    if not a.index.equals(b.index):
        raise ValueError("G3 and G4 shared baseline IDs differ")
    for field in ("signal_id", "filled", "exit_reason"):
        if not a[field].astype(str).equals(b[field].astype(str)):
            raise ValueError(f"G3 and G4 raw baseline differ: {field}")
    for field in ("entry_price", "exit_price", "net_return_pct"):
        if not np.allclose(pd.to_numeric(a[field], errors="coerce"),
                           pd.to_numeric(b[field], errors="coerce"),
                           rtol=0, atol=1e-7, equal_nan=True):
            raise ValueError(f"G3 and G4 raw baseline differ: {field}")


def checked_allocate(source: pd.DataFrame, published: pd.DataFrame) -> pd.DataFrame:
    ledger = allocate(source.reset_index(drop=True))
    a = ledger.set_index("sid").sort_index()
    b = published.set_index("sid").sort_index()
    if not a.index.equals(b.index):
        raise ValueError("Reallocated source IDs differ from published ledger")
    if not a.portfolio_executed.astype(str).equals(b.portfolio_executed.astype(str)):
        raise ValueError("Saved portfolio decisions cannot be reproduced")
    if not np.allclose(a.portfolio_net_profit_rupees.astype(float),
                       b.portfolio_net_profit_rupees.astype(float), rtol=0, atol=1e-5):
        raise ValueError("Saved portfolio P&L cannot be reproduced")
    return ledger


def daily_table(days: list, baseline: pd.DataFrame, variant: pd.DataFrame) -> pd.DataFrame:
    result = pd.DataFrame({"day": [str(day) for day in days]})
    for label, frame in (("g3_v3", baseline), ("g3_plus_late_12", variant)):
        executed = frame.loc[frame.portfolio_executed.eq(True)]
        day = frame.day.astype(str)
        result[f"{label}_selected"] = frame.groupby(day).size().reindex(result.day, fill_value=0).to_numpy()
        result[f"{label}_fills"] = executed.groupby(executed.day.astype(str)).size().reindex(result.day, fill_value=0).to_numpy()
        result[f"{label}_wins"] = executed.loc[executed.portfolio_net_profit_rupees.gt(0)].groupby(
            executed.loc[executed.portfolio_net_profit_rupees.gt(0)].day.astype(str)).size().reindex(
                result.day, fill_value=0).to_numpy()
        result[f"{label}_net_pnl_rs"] = frame.groupby(day).portfolio_net_profit_rupees.sum().reindex(
            result.day, fill_value=0).to_numpy()
    result["delta_net_pnl_rs"] = result.g3_plus_late_12_net_pnl_rs - result.g3_v3_net_pnl_rs
    return result


def run(g3_run: Path, g4_run: Path, output: Path) -> dict:
    g3_run, g4_run, output = Path(g3_run).resolve(), Path(g4_run).resolve(), Path(output).resolve()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"Output directory is not empty: {output}")
    g3_evidence = verified_run(g3_run, "V13-V10-G-3-BT-0920-LONG-T300-V1")
    g4_evidence = verified_run(g4_run, "V13-V10-G-4-BACKTEST-LATE-SLOTS-V1")
    g3_summary = json.loads((g3_run / "summary.json").read_text(encoding="utf-8"))
    g4_summary = json.loads((g4_run / "summary.json").read_text(encoding="utf-8"))
    if (g3_summary["sessions"] != 49 or g4_summary["sessions"] != 49
            or g3_summary["period_start"] != g4_summary["first_date"]
            or g3_summary["period_end"] != g4_summary["last_date"]
            or g4_summary["g3_baseline_trades_changed_by_portfolio"] != 0):
        raise ValueError("G3 and G4 session cohorts or baseline parity differ")
    g3 = pd.read_csv(g3_run / "variant_trades.csv")
    g4 = pd.read_csv(g4_run / "g4_all_12_trades.csv")
    late = pd.read_csv(g4_run / "g4_added_trades.csv")
    decisions = pd.read_csv(g4_run / "setup_decisions.csv")
    expected_setups = {f"{clock}_{side}" for clock in CLOCKS for side in ("LONG", "SHORT")}
    if (set(decisions.setup) != expected_setups or len(decisions) != 12
            or set(late.setup_id) - {f"{clock.replace(':', '')[0:2]}{int(clock[-2:])+1:02d}_{side}"
                                      for clock in CLOCKS for side in ("LONG", "SHORT")}
            or len(late) != 89 or len(g3) != 131):
        raise ValueError("The selected twelve G4 setups or current G3 orders changed")
    g4_old = g4.loc[~g4.sid.isin(late.sid)].copy()
    g3_old = g3.loc[g3.sid.isin(g4_old.sid)].copy()
    same_raw(g3_old, g4_old)
    baseline = checked_allocate(g3, g3)
    checked_allocate(g4, g4)
    combined_source = pd.concat((g3, late), ignore_index=True, sort=False)
    if combined_source.sid.duplicated().any() or combined_source.signal_id.duplicated().any():
        raise ValueError("G3 and G4 orders overlap")
    variant = allocate(combined_source)
    g4_daily = pd.read_csv(g4_run / "daily_results.csv")
    days = sorted(pd.to_datetime(g4_daily.day).dt.date.unique())
    if len(days) != 49:
        raise ValueError("Current G3 ledger does not cover 49 sessions")
    late_variant = variant.loc[variant.sid.isin(late.sid)].copy()
    current = variant.loc[variant.sid.isin(g3.sid)].set_index("sid").sort_index()
    old = baseline.set_index("sid").sort_index()
    same_raw(current.reset_index(), old.reset_index())
    old_effect = float(current.portfolio_net_profit_rupees.sum() - old.portfolio_net_profit_rupees.sum())
    changed_old = int((current.portfolio_executed.astype(str) != old.portfolio_executed.astype(str)).sum())
    baseline_stats, combined_stats, added_stats = (_stats(frame, days) for frame in (baseline, variant, late_variant))
    delta = combined_stats["net_pnl_rs"] - baseline_stats["net_pnl_rs"]
    if not np.isclose(delta, added_stats["net_pnl_rs"] + old_effect, rtol=0, atol=1e-5):
        raise ValueError("Combined portfolio P&L does not reconcile")
    train, holdout = set(days[:38]), set(days[38:])
    periods = {}
    for name, cohort in (("training", train), ("holdout", holdout)):
        periods[name] = {label: _stats(frame.loc[pd.to_datetime(frame.day).dt.date.isin(cohort)], sorted(cohort))
                         for label, frame in (("g3_v3", baseline), ("g3_plus_late_12", variant),
                                              ("added_late_12", late_variant))}
    setup_rows = []
    for row in decisions.itertuples(index=False):
        part = late_variant.loc[late_variant.hhmm_int.eq(int(row.signal_end.replace(":", "")))
                                & late_variant.side.eq(row.side)]
        setup_rows.append(dict(setup=row.setup, variant=row.variant,
                               thresholds=json.loads(row.thresholds),
                               source_train_fills=int(row.train_fills),
                               source_holdout_fills=int(row.holdout_fills),
                               g4_qualification=bool(row.portfolio_qualified),
                               **_stats(part, days)))
    summary = dict(variant_id=VARIANT, scope="BACKTEST_ONLY", execution_authority=False,
                   created_at_ist=datetime.now(contract.IST).isoformat(), sessions=49,
                   first_date=str(days[0]), last_date=str(days[-1]),
                   excluded_session=g4_summary["excluded_session"],
                   g3_v3=baseline_stats, g3_plus_late_12=combined_stats,
                   added_late_12=added_stats, net_pnl_delta_rs=delta,
                   existing_g3_orders_with_changed_portfolio_acceptance=changed_old,
                   existing_g3_portfolio_pnl_effect_rs=old_effect,
                   periods=periods, g4_qualified_setups=g4_summary["qualified_setups"],
                   all_12_g4_qualification_met=g4_summary["all_12_production_ready"],
                   result_status="EXPLORATORY_BACKTEST_ONLY",
                   g3_source=g3_evidence, g4_source=g4_evidence)
    output.mkdir(parents=True, exist_ok=True)
    baseline.to_csv(output / "g3_v3_trades.csv", index=False)
    variant.to_csv(output / "combined_trades.csv", index=False)
    late_variant.to_csv(output / "added_late_trades.csv", index=False)
    daily_table(days, baseline, variant).to_csv(output / "daily_results.csv", index=False)
    pd.DataFrame([dict((k, json.dumps(v, sort_keys=True) if isinstance(v, dict) else v)
                       for k, v in row.items()) for row in setup_rows]).to_csv(output / "setup_results.csv", index=False)
    (output / "selected_late_config.json").write_bytes((g4_run / "selected_config.json").read_bytes())
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    report = [f"# G3 V3 plus twelve G4 late setups ({days[0]} through {days[-1]})", "",
              "Backtest only. The six signal clocks each include LONG and SHORT. Thresholds are the G4 training-selected levels; no holdout retuning was performed.", "",
              "| Portfolio | Orders | Fills | Wins | Win rate | Net P&L | Max drawdown |", "|---|---:|---:|---:|---:|---:|---:|"]
    for label, stat in (("Current G3 V3", baseline_stats), ("G3 V3 plus late twelve", combined_stats),
                        ("Late twelve contribution", added_stats)):
        report.append(f"| {label} | {stat['selected_orders']} | {stat['executed_trades']} | {stat['wins']} | "
                      f"{stat['win_rate_pct']:.2f}% | ₹{stat['net_pnl_rs']:,.2f} | ₹{stat['max_drawdown_rs']:,.2f} |")
    report += ["", f"Net P&L difference: ₹{delta:,.2f}.",
               f"Existing G3 portfolio decisions changed: {changed_old}; P&L effect: ₹{old_effect:,.2f}.",
               f"G4 setups that met its training and holdout qualification rule: {len(g4_summary['qualified_setups'])}/12.",
               "", "| Period | G3 V3 fills | G3 V3 net P&L | Combined fills | Combined net P&L | Difference |",
               "|---|---:|---:|---:|---:|---:|"]
    for name, label in (("training", "First 38 sessions"), ("holdout", "Later 11 sessions")):
        base, new = periods[name]["g3_v3"], periods[name]["g3_plus_late_12"]
        report.append(f"| {label} | {base['executed_trades']} | ₹{base['net_pnl_rs']:,.2f} | "
                      f"{new['executed_trades']} | ₹{new['net_pnl_rs']:,.2f} | "
                      f"₹{new['net_pnl_rs']-base['net_pnl_rs']:,.2f} |")
    report += [
               "", "| Setup | Orders | Fills | Wins | Win rate | Net P&L | G4 qualified |",
               "|---|---:|---:|---:|---:|---:|---|"]
    for row in setup_rows:
        report.append(f"| {row['setup']} | {row['selected_orders']} | {row['executed_trades']} | "
                      f"{row['wins']} | {row['win_rate_pct']:.2f}% | ₹{row['net_pnl_rs']:,.2f} | "
                      f"{'yes' if row['g4_qualification'] else 'no'} |")
    report += ["", "The 09:20 LONG baseline uses retrospective opening OI. Its live tradability was not verified in the published G3 V3 backtest.",
               "The late setups were chosen on 38 training sessions and assessed on 11 later sessions. The 2026-10-01 session is excluded for incomplete immutable input.",
               "", "Detailed ledgers: `combined_trades.csv`, `added_late_trades.csv`, `g3_v3_trades.csv`, `daily_results.csv`, and `setup_results.csv`.", ""]
    (output / "report.md").write_text("\n".join(report), encoding="utf-8")
    stat_table = pd.DataFrame([
        {"Portfolio": label, "Orders": stat["selected_orders"], "Fills": stat["executed_trades"],
         "Wins": stat["wins"], "Win rate %": round(stat["win_rate_pct"], 2),
         "Net P&L Rs": round(stat["net_pnl_rs"], 2), "Max drawdown Rs": round(stat["max_drawdown_rs"], 2)}
        for label, stat in (("G3 V3", baseline_stats), ("G3 V3 + G4 late twelve", combined_stats),
                            ("Late twelve", added_stats))])
    period_table = pd.DataFrame([
        {"Period": name, "Portfolio": label, "Fills": stat["executed_trades"],
         "Win rate %": round(stat["win_rate_pct"], 2) if stat["win_rate_pct"] is not None else None,
         "Net P&L Rs": round(stat["net_pnl_rs"], 2)}
        for name, group in periods.items() for label, stat in group.items()])
    setup_table = pd.DataFrame([{k: row[k] for k in ("setup", "selected_orders", "executed_trades",
                         "wins", "win_rate_pct", "net_pnl_rs", "g4_qualification")}
                        for row in setup_rows])
    day_table = daily_table(days, baseline, variant)
    page = """<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>G3 V3 + G4 late twelve backtest</title><style>body{font:15px/1.5 system-ui,sans-serif;background:#101827;color:#edf3fa;max-width:1200px;margin:auto;padding:28px}h1,h2{line-height:1.2}p{color:#b8c7d8}strong{color:#fff}.notice{background:#263243;border-left:4px solid #f5b546;padding:12px 16px;margin:22px 0}table{border-collapse:collapse;width:100%;background:#1a2636;margin:14px 0 34px}th,td{padding:8px 10px;border-bottom:1px solid #354356;text-align:right}th:first-child,td:first-child{text-align:left}th{position:sticky;top:0;background:#26374b}a{color:#89c6ff}.scroll{overflow:auto;max-height:520px}code{color:#d3e4fa}</style></head><body>"""
    page += f"<h1>G3 V3 + G4 late twelve</h1><p>49 sessions, {days[0]} through {days[-1]}. Signal clocks 10:05 to 10:30, every five minutes, LONG and SHORT.</p>"
    page += (f"<div class=notice><strong>Exploratory backtest:</strong> net P&amp;L rose by ₹{delta:,.2f}, "
             f"while combined win rate fell to {combined_stats['win_rate_pct']:.2f}% and max drawdown rose to "
             f"₹{combined_stats['max_drawdown_rs']:,.2f}. No late setup met the G4 training and holdout qualification rule.</div>")
    for title, table in (("Portfolio", stat_table), ("Training and holdout", period_table),
                         ("Late setup results", setup_table), ("Daily results", day_table)):
        page += f"<h2>{html.escape(title)}</h2><div class=scroll>{table.to_html(index=False, border=0, escape=True, na_rep='—')}</div>"
    page += "<p>Full CSV ledgers and source hashes are alongside this report. The 09:20 LONG baseline uses retrospective opening OI. October 1 was excluded for incomplete input.</p></body></html>"
    (output / "report.html").write_text(page, encoding="utf-8")
    manifest = dict(variant_id=VARIANT, scope="BACKTEST_ONLY", execution_authority=False,
                    code=str(Path(__file__).resolve()), code_sha256=sha256(Path(__file__)),
                    files={p.name: sha256(p) for p in output.iterdir() if p.is_file()})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--g3-run", type=Path, default=G3_RUN)
    parser.add_argument("--g4-run", type=Path, default=G4_RUN)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    now = datetime.now(contract.IST)
    output = args.output_dir or ROOT / "backtests/runs/2026-10-12" / (
        f"{now:%Y%m%dT%H%M%S%f}_{secrets.token_hex(4)}_v3_plus_g4_late_12")
    if not output.resolve().is_relative_to((ROOT / "backtests/runs").resolve()):
        parser.error("Combined G3 output must be under the G3 backtests/runs root")
    summary = run(args.g3_run, args.g4_run, output)
    print(json.dumps({key: summary[key] for key in ("sessions", "g3_v3", "g3_plus_late_12",
                                                   "added_late_12", "net_pnl_delta_rs",
                                                   "all_12_g4_qualification_met")}, indent=2))
    print(output)


if __name__ == "__main__":
    main()
