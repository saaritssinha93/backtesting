"""Closed-session G4 V14 replay from a verified current-profile daily source."""
from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime
import html
from pathlib import Path
import secrets

import numpy as np
import pandas as pd

import fno_v13_v10_g_3_daily_replay as replay
from research import g4_complete_inventory_backtest as g4
from research import g6_daily_backtest as source
from research.g4_extended_target_sweep import read_frozen_csv

SCHEMA = "g4_daily_backtest_v1"
DAILY_ROOT = g4.bounded.G4_ROOT / "daily" / "runs"
IST = "Asia/Kolkata"


def stats(book, day):
    result = g4.bounded.stats(book, [str(day)])
    trades = book.loc[book.portfolio_executed.eq(True)]
    result.update(gross_pnl_rs=float(trades.portfolio_gross_profit_rupees.sum()),
                  costs_rs=float(trades.portfolio_cost_rupees.sum()))
    return result


def trade_results(book):
    fields = ["sid", "day", "setup_id", "tradingsymbol", "side", "confirmation_ts",
              "portfolio_executed", "portfolio_status", "entry_ts", "exit_ts", "exit_reason",
              "entry_price", "exit_price", "gross_return_pct", "net_return_pct",
              "portfolio_gross_profit_rupees", "portfolio_cost_rupees",
              "portfolio_net_profit_rupees"]
    rows = book[fields].copy()
    rows["status"] = np.where(rows.portfolio_executed, rows.exit_reason, "NOT_EXECUTED")
    return rows.sort_values(["confirmation_ts", "tradingsymbol"]).reset_index(drop=True)


def preserve_frozen_1011_short(day, directory, payload, orders, identity):
    """Reselect the G4 0.13% short from the complete daily pre-quota pool."""
    record = payload["artifacts"]["candidate_signals"]
    path = directory / record["path"]
    if g4.mirror.sha256(path) != record["sha256"]:
        raise ValueError("Daily 10:11 candidate source changed")
    raw = g4.bounded.morning.normalize_signals(read_frozen_csv(path))
    raw = raw.loc[raw.hhmm_int.eq(1010) & raw.side.eq("SHORT")].copy()
    raw["sid"] = np.arange(210_000_000, 210_000_000 + len(raw))
    audit = g4.donor.selection_audit(raw)
    frozen_spec = replace(g4.donor._ACTIVE_BY_ID["1011_SHORT"], price_change_pct=.13)
    chosen = g4.donor.rank_candidates(audit.to_dict("records"), frozen_spec,
                                      session_date=day)
    existing = orders.loc[orders.setup_id.eq("1011_SHORT")]
    kept, added = [], []
    for row in chosen:
        match = existing.loc[existing.tradingsymbol.eq(row["tradingsymbol"])]
        if len(match) > 1:
            raise ValueError("Duplicate daily G4 10:11 SHORT")
        (kept if len(match) else added).append(match.iloc[0].to_dict() if len(match) else row)
    extra = (identity.identify(pd.DataFrame(added).reindex(columns=orders.columns))
             if added else orders.iloc[:0].copy())
    revised = pd.concat([orders.loc[orders.setup_id.ne("1011_SHORT")],
                         pd.DataFrame(kept).reindex(columns=orders.columns), extra],
                        ignore_index=True, sort=False)
    if revised.sid.duplicated().any():
        raise ValueError("Duplicate frozen G4 daily order SID")
    return revised


def render(summary, trades, sides):
    display = lambda frame: '<div class="scroll">' + frame.to_html(index=False, border=0,
        float_format=lambda value: f"{value:,.2f}") + '</div>'
    view = trades.copy()
    for field in ("confirmation_ts", "entry_ts", "exit_ts"):
        view[field] = pd.to_datetime(view[field], utc=True).dt.tz_convert(IST).dt.strftime("%H:%M").fillna("")
    columns = ["setup_id", "tradingsymbol", "side", "status", "entry_ts", "exit_ts",
               "entry_price", "exit_price", "gross_return_pct", "net_return_pct",
               "portfolio_net_profit_rupees"]
    coverage = html.escape(source.json.dumps(summary["source_coverage"], indent=2))
    totals = pd.DataFrame([dict(policy="g4_bounded_exit", **summary["result"])])
    return ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
        '<title>G4 daily backtest</title><style>body{font:15px/1.5 system-ui;margin:28px;color:#13243b;background:#f6f8fc}'
        'table{border-collapse:collapse;font-size:13px}td,th{padding:8px;border-bottom:1px solid #ccd6e2;text-align:right;white-space:nowrap}'
        'th{background:#e5edf8}.scroll{overflow:auto}pre{white-space:pre-wrap}a{color:#175bb7}</style></head><body>'
        f'<h1>G4 daily backtest: {summary["session_date"]}</h1>'
        f'<p>Completed session. Backtest only. All 51 {summary["variant_id"]} setup rules evaluated; 10:11 SHORT requires a {summary["short_1011_move_pct"]:.2f}% downward move. No market-direction filter.</p>'
        '<p>INR 100,000 capital per entry, 5x exposure, INR 1,000,000 portfolio capital and 5bps round-trip modeled cost. '
        'The 1.20% initial stop tightens to 1.00% after 120 minutes. Setup targets remain clipped to 0.60–2.00%. '
        'Stop-first ambiguous-bar handling and 15:15 square-off are unchanged.</p>'
        '<h2>Daily result</h2>' + display(totals) + '<h2>Long and short</h2>' + display(sides) +
        '<h2>Every selected order</h2><p>Entry/exit fields are blank for orders that did not fill or receive capital.</p>' +
        display(view[columns]) + '<h2>Inherited input coverage</h2><p>Source eligibility exclusions are retained. '
        'After-close stock-day exclusions are not a live causal entry filter.</p><details><summary>Coverage and exclusions</summary><pre>' +
        coverage + '</pre></details><p><a href="trade_results.csv">Trade results</a> | '
        '<a href="slot_side_results.csv">All slots and sides</a> | <a href="summary.json">Evidence and summary</a></p>'
        '<p>This is a single-session replay. It does not replace the cumulative G4 history pointer.</p></body></html>')


def run(config_path, output, *, day):
    config = g4.configuration(config_path)
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to(DAILY_ROOT.resolve()):
        raise ValueError("Require a new directory inside G4 daily/runs")
    directory, payload, provenance = source.load_source(day)
    source_hash = g4.mirror.sha256(directory / "run_manifest.json")
    snapshot_evidence = {key: provenance[key] for key in ("snapshot_manifest", "snapshot_manifest_sha256")}
    prices = g4.Prices(dict(daily_path_evidence=[snapshot_evidence]))
    identity = g4.Identity(config)
    orders, pool, changes = source.select_orders(day, directory, payload, prices, config, identity=identity)
    if config['variant_id'] == g4.PREVIOUS_VERSION:
        orders = preserve_frozen_1011_short(day, directory, payload, orders, identity)
    print(f"G4 {day}: verified daily source; {len(orders)} selected orders after unchanged guards", flush=True)
    paths = prices.paths(orders.sort_values(["tradingsymbol", "confirmation_ts"]))
    native = g4.bounded.history._simulate(orders, paths, [day], identity)
    book = g4._bounded(native, paths, config)
    result = stats(book, day)
    trades = trade_results(book)
    sides = pd.DataFrame([dict(side=side, **stats(book.loc[book.side.eq(side)], day))
                          for side in ("LONG", "SHORT")])
    slots = pd.DataFrame([dict(setup_id=setup["setup_id"], signal_end=setup["signal_end"],
        side=setup["side"], **stats(book.loc[book.setup_id.eq(setup["setup_id"])], day))
        for setup in config["active_setups"]])
    for path,item in prices.proof.items():
        if g4.mirror.sha256(Path(path)) != item["sha256"]:
            raise ValueError("Minute input changed during G4 replay")
    if g4.mirror.sha256(directory / "run_manifest.json") != source_hash:
        raise ValueError("Daily source changed during G4 replay")
    output.mkdir(parents=True)
    replay.atomic_json(output / "config.json", config)
    replay.atomic_json(output / "source_manifest.json", provenance)
    replay.atomic_json(output / "source_run_manifest.json", payload)
    replay.atomic_json(output / "price_sources.json", prices.proof)
    for name,frame in {"selected_orders":orders, "guard_candidate_audit":pool,
            "guard_selection_changes":changes, "portfolio_trades":book,
            "trade_results":trades, "side_results":sides, "slot_side_results":slots}.items():
        frame.to_csv(output / (name + ".csv"), index=False)
    path_rows = [pd.DataFrame(value).assign(sid=sid) for sid,value in paths.items()]
    pd.concat(path_rows, ignore_index=True).to_parquet(output / "minute_paths.parquet", index=False)
    summary = dict(schema_version=SCHEMA, variant_id=g4.VERSION, scope="BACKTEST_ONLY",
        execution_authority=False, complete=True, sessions=1, session_date=str(day),
        first_date=str(day), last_date=str(day), days=[str(day)], active_setups=51,
        selected_policy="g4_bounded_exit", short_1011_move_pct=(.11 if config['variant_id']==g4.VERSION else .13),
        source_run=str(directory),
        source_manifest_sha256=source_hash, source_coverage=payload.get("coverage", {}), result=result,
        source_input_snapshot=snapshot_evidence, cumulative_history_pointer_changed=False,
        source_rule="Same appended-day guard-before-quota selection and two approved G3-added setups; 10:11 SHORT uses the configured threshold; full one-minute bounded-exit replay.")
    replay.atomic_json(output / "summary.json", summary)
    (output / "report.html").write_text(render(summary, trades, sides), encoding="utf-8")
    code_paths = [Path(__file__), Path(config_path), Path(g4.__file__), Path(source.__file__),
                  Path(g4.bounded.__file__), Path(g4.mirror.__file__), Path(g4.bounded.history.__file__)]
    replay.atomic_json(output / "manifest.json", dict(schema_version=SCHEMA,
        code_sha256={str(p.resolve()):g4.mirror.sha256(p) for p in code_paths},
        files={p.relative_to(output).as_posix():g4.mirror.sha256(p) for p in output.rglob("*") if p.is_file()}))
    verify(output)
    replay.atomic_json(g4.bounded.G4_ROOT / "latest" / "daily_backtest.json", dict(
        schema_version=SCHEMA, run_dir=output.relative_to(g4.bounded.G4_ROOT).as_posix(),
        manifest_sha256=g4.mirror.sha256(output / "manifest.json"), session_date=str(day),
        selected_policy="g4_bounded_exit", execution_authority=False))
    print("VERIFIED", output, flush=True)
    return summary


def verify(output):
    output = Path(output).resolve()
    manifest, summary = g4.mirror.read(output / "manifest.json"), g4.mirror.read(output / "summary.json")
    if summary["schema_version"] != SCHEMA or summary["execution_authority"] is not False:
        raise ValueError("Invalid G4 daily package")
    for name,digest in manifest["files"].items():
        path = (output / name).resolve()
        if not path.is_relative_to(output) or g4.mirror.sha256(path) != digest:
            raise ValueError("G4 daily artifact changed: " + name)
    if g4.mirror.sha256(Path(summary["source_run"]) / "run_manifest.json") != summary["source_manifest_sha256"]:
        raise ValueError("G3 daily source changed")
    config, orders = g4.mirror.read(output / "config.json"), read_frozen_csv(output / "selected_orders.csv")
    raw = pd.read_parquet(output / "minute_paths.parquet")
    paths = {int(sid): {field:part[field].to_numpy() for field in
        ("timestamp_ns", "open", "high", "low", "close")} for sid,part in raw.groupby("sid")}
    day = date.fromisoformat(summary["session_date"])
    identity = g4.Identity(config)
    native = g4.bounded.history._simulate(orders, paths, [day], identity)
    book = g4._bounded(native, paths, config)
    saved = read_frozen_csv(output / "portfolio_trades.csv")
    g4.bounded.history._same_native(book, saved)
    g4.bounded.history._same_allocation(book, saved)
    for key,value in stats(book, day).items():
        if not np.isclose(value, summary["result"][key], rtol=0, atol=1e-6):
            raise ValueError("G4 daily accounting differs")
    recorded = read_frozen_csv(output / "trade_results.csv")
    rebuilt = trade_results(book)
    for field in ("portfolio_gross_profit_rupees", "portfolio_cost_rupees", "portfolio_net_profit_rupees"):
        np.testing.assert_allclose(recorded[field], rebuilt[field], rtol=0, atol=1e-6)
    sides = pd.read_csv(output / "side_results.csv")
    slots = pd.read_csv(output / "slot_side_results.csv")
    for field in ("trades", "wins", "losses", "net_pnl_rs", "gross_pnl_rs", "costs_rs"):
        if not np.isclose(sides[field].sum(), summary["result"][field], rtol=0, atol=1e-6):
            raise ValueError("G4 side totals differ")
        if not np.isclose(slots[field].sum(), summary["result"][field], rtol=0, atol=1e-6):
            raise ValueError("G4 slot totals differ")
    return summary


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--config", type=Path, default=g4.DEFAULT_CONFIG)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    now = datetime.now(pd.Timestamp.now(tz=IST).tzinfo)
    output = args.output_dir or DAILY_ROOT / str(args.date) / (
        f"{now:%Y%m%dT%H%M%S%f}_{secrets.token_hex(4)}_g4_bounded_exit")
    result = run(args.config, output, day=args.date)
    print(source.json.dumps({key:result[key] for key in
        ("variant_id", "session_date", "active_setups", "result")}, indent=2))
    print(output)


if __name__ == "__main__":
    main()
