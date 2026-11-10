"""Re-run frozen G3 control and the no-slot-cap variant on identical sealed inputs."""
from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v18_20261102 import data
from g3_v18_20261102.run import metrics
from fno_v13_v10_g_3_daily_replay import simulate
from . import selection

PACKAGE = Path(__file__).resolve().parent
PUBLISHED = PACKAGE.parent / "g3_v18_20261102/results_20261102_complete"
IST = "Asia/Kolkata"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def dump(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")


def _true(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin(("true", "1", "1.0"))


def _published_control_check(book: pd.DataFrame, days: list[str]) -> dict:
    matched = [day for day in days if day <= "2026-10-30"]
    if len(matched) != 62:
        return {"state": "UNAVAILABLE", "reason": "Published reference has 62 sessions through October 30"}
    reference = pd.read_csv(PUBLISHED / "overall_comparison.csv")
    row = reference.loc[reference.variant.eq("control") & reference.period.eq("all")
                        & reference.cost_bps.eq(5)]
    if len(row) != 1:
        raise ValueError("Published control reference is ambiguous")
    actual = metrics(book, matched)
    expected = row.iloc[0]
    fields = ("sessions", "selected_orders", "native_fills", "trades", "wins", "losses")
    failures = {key: (actual[key], int(expected[key])) for key in fields if actual[key] != int(expected[key])}
    if abs(actual["net_pnl_rs"] - float(expected.net_pnl_rs)) > .01:
        failures["net_pnl_rs"] = (actual["net_pnl_rs"], float(expected.net_pnl_rs))
    if abs(actual["daily_close_drawdown_rs"] - float(expected.daily_close_drawdown_rs)) > .01:
        failures["daily_close_drawdown_rs"] = (actual["daily_close_drawdown_rs"], float(expected.daily_close_drawdown_rs))
    if failures:
        raise ValueError("Control failed published baseline reproduction: " + repr(failures))
    return dict(state="PASS", published_reference=str(PUBLISHED / "overall_comparison.csv"),
                published_reference_sha256=sha(PUBLISHED / "overall_comparison.csv"),
                matched_sessions=len(matched), metrics=actual)


def _report(output: Path, summary: dict, comparison: pd.DataFrame,
            daily: pd.DataFrame, setup: pd.DataFrame) -> None:
    def table(frame):
        return frame.to_html(index=False, border=0, classes="result", escape=True,
                             float_format=lambda x: f"{x:,.2f}")
    core = comparison.loc[comparison.period.eq("full_available_history")]
    title = "G3 V19: unlimited eligible entries per slot"
    document = f"""<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{title}</title>
<style>body{{font:15px/1.55 system-ui;max-width:1200px;margin:35px auto;padding:0 20px;color:#172334;background:#f5f7fb}}h1{{line-height:1.2}}.tag{{background:#d8ebf9;padding:8px 12px;border-radius:6px}}.cards{{display:flex;gap:12px;flex-wrap:wrap}}.card{{background:white;padding:15px;border-radius:8px;min-width:160px;box-shadow:0 1px 3px #ccd}}.card strong{{display:block;font-size:22px}}table{{border-collapse:collapse;background:white;width:100%;font-size:13px}}td,th{{padding:8px;border-bottom:1px solid #dde3ec;text-align:left}}th{{background:#e5ecf6}}.scroll{{overflow:auto;margin:14px 0 32px}}a{{margin-right:18px}}section{{margin-top:32px}}</style>
<h1>{title}</h1><p class="tag">Backtest research only. The only strategy change is removal of the per-slot entry quota. Portfolio capital and position limits still apply.</p>
<div class="cards"><div class="card">Sessions<strong>{len(summary['days'])}</strong></div><div class="card">First<strong>{summary['days'][0]}</strong></div><div class="card">Latest sealed<strong>{summary['days'][-1]}</strong></div><div class="card">Baseline match<strong>{summary['published_control_reproduction']['state']}</strong></div></div>
<section><h2>Full available history, 5 bps round-trip cost</h2><div class="scroll">{table(core)}</div></section>
<p>The 62-session published comparison uses exactly the dates in the main G3 V18 control report. The full-history table adds the sealed November 2 session to both arms. November 3 has no complete sealed historical replay source and is excluded.</p>
<p>Both arms use the same completed five-minute features, next-minute confirmation, stops, targets, 10-minute trigger expiry and fixed portfolio model. Every eligible stock in an enabled slot can enter the order pool. Capital, open-position and duplicate-position constraints can still reject an order.</p>
<section><h2>All comparison periods</h2><div class="scroll">{table(comparison)}</div></section>
<section><h2>Per-slot results</h2><div class="scroll">{table(setup)}</div></section>
<section><h2>Day-by-day results</h2><div class="scroll">{table(daily)}</div></section>
<p>Past outcomes influenced the frozen V17 slot allowlist through October 27; this is an in-sample comparison, not prospective proof. October 28 uses the previously disclosed restricted 215-stock cohort. Drawdown is based on end-of-day realized profit. One-minute OHLC stop/target ambiguity uses the existing conservative simulator. Costs use the inherited 5 bps round-trip model.</p>
<p><a href="comparison.csv">Comparison</a><a href="daily_results.csv">Daily results</a><a href="setup_results.csv">Per slot</a><a href="control_trades.csv">Baseline trades</a><a href="no_slot_limit_trades.csv">Uncapped trades</a><a href="no_slot_limit_selection_audit.csv">Selection audit</a><a href="summary.json">Summary</a><a href="config.json">Configuration</a><a href="source_provenance.json">Source proof</a></p>
</html>"""
    (output / "report.html").write_text(document, encoding="utf-8")


def run(output: Path, through: date, selected_days: list[str] | None = None):
    output = output.resolve()
    if not output.is_relative_to(PACKAGE) or output == PACKAGE or output.exists():
        raise ValueError("Output must be a new child directory under G3 V19")
    available = [str(day) for day in data.available_days(through.isoformat())]
    days = [day for day in available if selected_days is None or day in selected_days]
    if selected_days is not None and set(days) != set(selected_days):
        raise ValueError("Requested days lack sealed source")
    if not days:
        raise ValueError("No complete replay sessions")
    output.mkdir(parents=True)
    config = selection.configuration() | dict(requested_through=through.isoformat(),
        days=days, cost_bps_round_trip=5, initial_portfolio_capital_rs=1_000_000,
        per_entry_capital_rs=100_000, exposure_multiple=5,
        comparison_arms=["control", "no_slot_limit"])
    dump(output / "config.json", config)
    books = {"control": [], "no_slot_limit": []}
    audits = []
    provenance = []
    for day_index, day in enumerate(days):
        raw, minute_bars, proof = data.load_day(day)
        raw = raw.reset_index(drop=True)
        raw["sid"] = day_index * 1_000_000 + np.arange(len(raw), dtype=np.int64)
        new_orders, audit, control_orders = selection.select(raw)
        if not control_orders.sid.isin(new_orders.sid).all():
            raise ValueError("No-slot-limit arm lost a baseline order")
        for arm, orders in (("control", control_orders), ("no_slot_limit", new_orders)):
            paths = data.execution_paths(orders, minute_bars)
            ledger, _ = simulate(orders, paths, [date.fromisoformat(day)])
            ledger["research_arm"] = arm
            ledger["execution_authority"] = False
            books[arm].append(ledger)
        audits.append(audit)
        provenance.append(dict(day=day, proof=proof, source_rows=len(raw),
            control_orders=len(control_orders), no_slot_limit_orders=len(new_orders)))
        print(f"{day} [{day_index+1}/{len(days)}] baseline={len(control_orders)} uncapped={len(new_orders)}", flush=True)
    books = {arm: pd.concat(parts, ignore_index=True, sort=False) for arm, parts in books.items()}
    verification = _published_control_check(books["control"], days)
    periods = dict(full_available_history=days,
                   published_62=[day for day in days if day <= "2026-10-30"],
                   november_02=[day for day in days if day == "2026-11-02"],
                   after_october_27=[day for day in days if day > "2026-10-27"])
    comparison_rows = []
    for period, cohort in periods.items():
        if not cohort:
            continue
        base, new = (metrics(books[arm], cohort) for arm in ("control", "no_slot_limit"))
        for arm, metric in (("control", base), ("no_slot_limit", new)):
            comparison_rows.append(dict(period=period, arm=arm, **metric,
                delta_net_rs=metric["net_pnl_rs"]-base["net_pnl_rs"],
                delta_trades=metric["trades"]-base["trades"],
                delta_daily_drawdown_rs=metric["daily_close_drawdown_rs"]-base["daily_close_drawdown_rs"]))
    comparison = pd.DataFrame(comparison_rows)
    daily_rows = []
    for day in days:
        base, new = (metrics(books[arm], [day]) for arm in ("control", "no_slot_limit"))
        daily_rows.append(dict(day=day, baseline_pnl_rs=base["net_pnl_rs"],
            no_slot_limit_pnl_rs=new["net_pnl_rs"], delta_pnl_rs=new["net_pnl_rs"]-base["net_pnl_rs"],
            baseline_trades=base["trades"], no_slot_limit_trades=new["trades"],
            baseline_selected=base["selected_orders"], no_slot_limit_selected=new["selected_orders"]))
    daily = pd.DataFrame(daily_rows)
    setups = sorted(set(pd.concat(books.values(), ignore_index=True).setup_id.dropna()))
    setup_rows = []
    for setup in setups:
        base, new = (metrics(books[arm].loc[books[arm].setup_id.eq(setup)], days)
                     for arm in ("control", "no_slot_limit"))
        setup_rows.append(dict(setup_id=setup, baseline_orders=base["selected_orders"],
            no_slot_limit_orders=new["selected_orders"], baseline_trades=base["trades"],
            no_slot_limit_trades=new["trades"], baseline_pnl_rs=base["net_pnl_rs"],
            no_slot_limit_pnl_rs=new["net_pnl_rs"], delta_pnl_rs=new["net_pnl_rs"]-base["net_pnl_rs"]))
    setup_frame = pd.DataFrame(setup_rows)
    for arm, book in books.items():
        book.to_csv(output / f"{arm}_trades.csv", index=False)
    pd.concat(audits, ignore_index=True, sort=False).to_csv(output / "no_slot_limit_selection_audit.csv", index=False)
    comparison.to_csv(output / "comparison.csv", index=False)
    daily.to_csv(output / "daily_results.csv", index=False)
    setup_frame.to_csv(output / "setup_results.csv", index=False)
    dump(output / "source_provenance.json", dict(sessions=provenance,
        code={str(p):sha(p) for p in (Path(__file__), Path(selection.__file__),
            Path(data.__file__), Path(baseline_contract_file()))},
        published_baseline=verification))
    summary = dict(version=selection.VERSION, created_at_ist=datetime.now().astimezone().isoformat(),
        execution_authority=False, days=days, source_last_day=days[-1],
        published_control_reproduction=verification, comparison=comparison_rows,
        original_files_edited=False, interpretation="In-sample slot policy; portfolio constraints retained")
    dump(output / "summary.json", summary)
    _report(output, summary, comparison, daily, setup_frame)
    dump(output / "manifest.json", {p.name:sha(p) for p in output.iterdir() if p.is_file()})
    print(json.dumps(dict(output=str(output), sessions=len(days), latest=days[-1],
        published_reference=verification["state"],
        full_history=comparison.loc[comparison.period.eq("full_available_history")].to_dict("records")),
        indent=2, default=str), flush=True)


def baseline_contract_file() -> Path:
    from research import g3_slot_win70_contract
    return Path(g3_slot_win70_contract.__file__)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--through", type=date.fromisoformat, default=date.today())
    parser.add_argument("--days", nargs="+")
    args = parser.parse_args()
    run(args.output, args.through, args.days)


if __name__ == "__main__":
    main()
