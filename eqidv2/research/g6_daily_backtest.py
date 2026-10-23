"""Closed-session G6 replay using the approved Nifty-futures entry filter.

Daily results have their own pointer. Historical packages and G3 publications
are read-only. No primary cash-index data is needed by this selected policy.
"""
from __future__ import annotations

from datetime import date
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_3_daily_replay as replay
from backtesting_result_v13_v10_g_3_daily import _verified_daily
from research import g6_g4_replica_backtest as g6
from research import g6_nifty_direction as nifty
from research import g6_primary_index_direction as modes
from research.g4_extended_target_sweep import read_frozen_csv

SCHEMA = "g6_daily_nifty_backtest_v1"
base = g6.mirror
DAILY_ROOT = g6.G6_ROOT / "daily" / "runs"


def load_source(day):
    replay.require_closed_session(day)
    member = _verified_daily(g6.bounded.G3_ROOT, day)
    if member is None:
        raise ValueError(f"No verified complete current-profile G3 daily source for {day}")
    directory, payload = member
    source = base.read(directory / payload["artifacts"]["source_manifest"]["path"])
    snapshot_manifest = Path(source["snapshot_manifest"]).resolve()
    if (not snapshot_manifest.is_relative_to(directory.resolve()) or
            base.sha256(snapshot_manifest) != source["snapshot_manifest_sha256"]):
        raise ValueError("Daily snapshot provenance differs")
    snapshot = base.read(snapshot_manifest)
    replay.source._verify_input_snapshot(snapshot_manifest.parent, snapshot)
    if snapshot["session_date"] != str(day):
        raise ValueError("Wrong snapshot session")
    return directory, payload, source


def select_orders(day, directory, payload, prices, config, *, identity=None):
    """Same appended-day guard-before-quota branch as G6's historical runner."""
    donor = base.configuration()
    scenario = donor["primary_scenario"]
    guards = donor["scenarios"][scenario]
    volumes = donor["five_minute_volume_ratio_overrides"][scenario]
    guard_ids = set(guards) | set(volumes)
    clocks = {int(base.frozen_donor._ACTIVE_BY_ID[s].signal_end.replace(":", "")) for s in guard_ids}
    original = read_frozen_csv(directory / payload["artifacts"]["selected_orders"]["path"])
    candidates = read_frozen_csv(directory / payload["artifacts"]["candidate_signals"]["path"])
    raw = g6.bounded.morning.normalize_signals(candidates)
    if (not original.day.astype(str).str[:10].eq(str(day)).all() or
            not raw.day.astype(str).str[:10].eq(str(day)).all()):
        raise ValueError("Daily candidates escaped the requested session")
    raw = raw.loc[raw.hhmm_int.isin(clocks)].copy()
    raw["sid"] = np.arange(97_000_000, 97_000_000 + len(raw))
    pool = base.donor.selection_audit(raw)
    pool = prices.enrich(pool.loc[pool.setup_id.isin(guard_ids)].copy())
    guarded = base.reselect(pool, guards, volumes)
    combined = pd.concat([original.loc[~original.setup_id.isin(guard_ids)], guarded],
                         ignore_index=True, sort=False)
    if (combined.sid.duplicated().any() or
            combined.duplicated(["day", "setup_id", "tradingsymbol"]).any() or
            not set(combined.setup_id).issubset(s["setup_id"] for s in config["active_setups"])):
        raise ValueError("G6 daily selection identity differs")
    changes = []
    before, after = base.selected_keys(original), base.selected_keys(combined)
    for action, keys in (("REMOVED_BY_G4_GUARD", before - after),
                         ("ADDED_BY_G4_GUARD", after - before)):
        changes.extend(dict(day=d, setup_id=s, tradingsymbol=t, action=action)
                       for d, s, t in sorted(keys))
    orders = (identity or g6.Identity(config)).identify(combined)
    return orders, pool, pd.DataFrame(changes, columns=["day", "setup_id", "tradingsymbol", "action"])


def bounded_replay(orders, paths, day, config):
    native = g6.bounded.history._simulate(orders, paths, [day], g6.Identity(config))
    book = g6._bounded(native, paths, config) if len(native) else native.copy()
    if "exit_reason" not in book:
        book["exit_reason"] = pd.Series(index=book.index, dtype=str)
    book["strategy"] = "V13-V10-G-6"
    book["g6_direction_filter_mode"] = modes.OFF
    return book


def filter_book(unfiltered, audit, config):
    if len(unfiltered):
        book, _ = nifty.apply_filter(unfiltered, audit, config)
    else:
        book = unfiltered.copy()
    book["g6_direction_filter_mode"] = modes.NIFTY_FILTER
    return book


def stats(book, day):
    result = g6.bounded.stats(book, [str(day)])
    traded = book.loc[book.portfolio_executed.eq(True)]
    result.update(gross_pnl_rs=float(traded.portfolio_gross_profit_rupees.sum()),
                  costs_rs=float(traded.portfolio_cost_rupees.sum()))
    return result


def trade_comparison(unfiltered, filtered, audit):
    rows = unfiltered[["sid", "day", "setup_id", "tradingsymbol", "side", "confirmation_ts"]].copy()
    before = unfiltered.set_index("sid").loc[rows.sid]
    after = filtered.set_index("sid").loc[rows.sid]
    decisions = audit.set_index("sid").loc[rows.sid]
    rows["nifty_allowed"] = decisions.both_counter_allowed.to_numpy()
    rows["nifty_slot_change_pct"] = decisions.slot_candle.to_numpy()
    rows["nifty_session_change_pct"] = decisions.session_open.to_numpy()
    rows["unfiltered_executed"] = before.portfolio_executed.to_numpy()
    rows["filtered_executed"] = after.portfolio_executed.to_numpy()
    rows["unfiltered_net_pnl_rs"] = before.portfolio_net_profit_rupees.to_numpy()
    rows["filtered_net_pnl_rs"] = after.portfolio_net_profit_rupees.to_numpy()
    rows["filter_net_change_rs"] = rows.filtered_net_pnl_rs - rows.unfiltered_net_pnl_rs
    rows["status"] = np.where(~rows.nifty_allowed, "NIFTY_REJECTED",
        np.where(rows.filtered_executed, after.exit_reason.to_numpy(), "NOT_EXECUTED"))
    for column in ("entry_ts", "exit_ts", "entry_price", "exit_price"):
        rows[column] = after[column].where(after.portfolio_executed).to_numpy()
    return rows.sort_values(["confirmation_ts", "tradingsymbol"]).reset_index(drop=True)


def render(summary, comparison, trades, sides):
    display = lambda frame: '<div class="scroll">' + frame.to_html(index=False, border=0,
        float_format=lambda value: f"{value:,.2f}") + '</div>'
    trade_view = trades.copy()
    for field in ("confirmation_ts", "entry_ts", "exit_ts"):
        trade_view[field] = pd.to_datetime(trade_view[field], utc=True).dt.tz_convert(
            "Asia/Kolkata").dt.strftime("%H:%M").fillna("")
    fields = ["setup_id", "tradingsymbol", "side", "status", "entry_ts", "exit_ts",
              "entry_price", "exit_price", "filtered_net_pnl_rs", "unfiltered_net_pnl_rs",
              "filter_net_change_rs", "nifty_slot_change_pct", "nifty_session_change_pct"]
    coverage = html.escape(json.dumps(summary["source_coverage"], indent=2))
    return ('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
        '<title>G6 daily backtest</title><style>body{font:15px/1.5 system-ui;margin:28px;color:#13243b;background:#f6f8fc}'
        'table{border-collapse:collapse;font-size:13px}td,th{padding:8px;border-bottom:1px solid #ccd6e2;text-align:right;white-space:nowrap}'
        'th{background:#e5edf8}.scroll{overflow:auto}pre{white-space:pre-wrap}a{color:#175bb7}</style></head><body>'
        f'<h1>G6 daily backtest: {summary["session_date"]}</h1>'
        f'<p>Completed session. Backtest only. Selected policy: {summary["selected_policy"]}. All 51 setup rules evaluated.</p>'
        '<p>Reject a long only when both Nifty-futures slot and session directions are down; reject a short only when both are up. '
        'Mixed/flat directions allow entry. Decisions use exact completed five-minute candles at confirmation. '
        'No replacement after index rejection and no new entry delay.</p>'
        '<p>INR 100,000 capital per entry, 5x exposure, INR 1,000,000 portfolio capital. '
        '5bps round-trip modeled cost. Existing 1.20% initial stop tightens to 1.00% after 120 minutes; '
        'setup targets remain clipped to 0.60–2.00%. Stop-first ambiguous-bar handling and 15:15 square-off are unchanged.</p>'
        '<h2>Daily comparison</h2>' + display(comparison) + '<h2>Long and short</h2>' + display(sides) +
        '<h2>Every selected order</h2><p>Entry/exit fields are blank for rejected or unexecuted orders. '
        'Unfiltered P&amp;L is a counterfactual comparison, not an actual filtered trade.</p>' + display(trade_view[fields]) +
        '<h2>Inherited input coverage</h2><p>Source eligibility exclusions are retained. After-close stock-day exclusions '
        'are not a live causal entry filter.</p><details><summary>Coverage and exclusions</summary><pre>' + coverage + '</pre></details>'
        '<p><a href="trade_comparison.csv">Trade comparison</a> | <a href="slot_side_results.csv">All slots and sides</a> | '
        '<a href="nifty_direction_audit.csv">Nifty decisions</a> | <a href="summary.json">Evidence and summary</a></p>'
        '<p>This is a single-session replay, not a new full-history result. Historical reports and G3 pointers are unchanged.</p></body></html>')


def run(config_path, output, *, day, direction_filter_mode=None):
    config = g6.configuration(config_path)
    mode = modes.canonical_mode(direction_filter_mode or config["direction_filter"]["mode"])
    if mode not in (modes.NIFTY_FILTER, modes.OFF):
        raise ValueError("Daily G6 replay supports Nifty-futures or off; use the full-history runner for primary-index studies")
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to(DAILY_ROOT.resolve()):
        raise ValueError("Require a new directory inside G6 daily/runs")
    directory, payload, provenance = load_source(day)
    source_hash = base.sha256(directory / "run_manifest.json")
    snapshot_evidence = {key: provenance[key] for key in ("snapshot_manifest", "snapshot_manifest_sha256")}
    prices = g6.Prices(dict(daily_path_evidence=[snapshot_evidence]))
    orders, pool, changes = select_orders(day, directory, payload, prices, config)
    print(f"G6 {day}: verified daily source; {len(orders)} selected orders after unchanged G4 guards", flush=True)
    paths = prices.paths(orders.sort_values(["tradingsymbol", "confirmation_ts"]))
    unfiltered = bounded_replay(orders, paths, day, config)
    if len(unfiltered):
        audit, proof, inputs = nifty.order_audit(unfiltered, prices)
    else:
        audit = pd.DataFrame({key: pd.Series(dtype=dtype) for key,dtype in
            (("sid", int), ("both_counter_allowed", bool), ("slot_candle", float), ("session_open", float))})
        proof, inputs = pd.DataFrame(columns=["day", "path", "sha256"]), {}
    filtered = filter_book(unfiltered, audit, config)
    policy = g6.NIFTY_POLICY if mode == modes.NIFTY_FILTER else "g6_unfiltered"
    books = {"g6_unfiltered": unfiltered, g6.NIFTY_POLICY: filtered}
    comparison = pd.DataFrame([dict(policy=name, **stats(book, day)) for name,book in books.items()])
    trades = trade_comparison(unfiltered, filtered, audit)
    sides = pd.DataFrame([dict(policy=name, side=side, **stats(book.loc[book.side.eq(side)], day))
                          for name,book in books.items() for side in ("LONG", "SHORT")])
    slots = pd.DataFrame([dict(policy=name, setup_id=setup["setup_id"], signal_end=setup["signal_end"],
        side=setup["side"], **stats(book.loc[book.setup_id.eq(setup["setup_id"])], day))
        for name,book in books.items() for setup in config["active_setups"]])
    for path,item in prices.proof.items():
        if base.sha256(Path(path)) != item["sha256"]:
            raise ValueError("Minute input changed during replay")
    for row in proof.itertuples():
        if base.sha256(Path(row.path)) != row.sha256:
            raise ValueError("Nifty input changed during replay")
    if base.sha256(directory / "run_manifest.json") != source_hash:
        raise ValueError("Daily source changed during replay")
    output.mkdir(parents=True)
    replay.atomic_json(output / "config.json", config)
    replay.atomic_json(output / "source_manifest.json", provenance)
    replay.atomic_json(output / "source_run_manifest.json", payload)
    replay.atomic_json(output / "price_sources.json", prices.proof)
    for name, frame in {"selected_orders": orders, "guard_candidate_audit": pool,
            "guard_selection_changes": changes, "comparison": comparison,
            "trade_comparison": trades, "side_results": sides, "slot_side_results": slots,
            "nifty_direction_audit": audit, "nifty_source_proof": proof}.items():
        frame.to_csv(output / (name + ".csv"), index=False)
    path_rows = [pd.DataFrame(value).assign(sid=sid) for sid,value in paths.items()]
    path_frame = pd.concat(path_rows, ignore_index=True) if path_rows else pd.DataFrame(
        columns=["sid", "timestamp_ns", "open", "high", "low", "close"])
    path_frame.to_parquet(output / "minute_paths.parquet", index=False)
    for name,book in books.items():
        (output / name).mkdir()
        book.to_csv(output / name / "portfolio_trades.csv", index=False)
    (output / "nifty_inputs").mkdir()
    for source_day,frame in inputs.items():
        frame.to_parquet(output / "nifty_inputs" / (source_day + ".parquet"), index=False)
    summary = dict(schema_version=SCHEMA, variant_id=g6.VERSION, scope="BACKTEST_ONLY",
        execution_authority=False, complete=True, sessions=1, session_date=str(day),
        first_date=str(day), last_date=str(day), days=[str(day)], active_setups=51,
        selected_policy=policy, direction_filter_mode=mode, source_run=str(directory),
        source_manifest_sha256=source_hash, source_coverage=payload.get("coverage", {}),
        comparison=comparison.to_dict("records"), selected_result=stats(books[policy], day),
        nifty_rejected_orders=int((~audit.both_counter_allowed).sum()),
        source_input_snapshot=snapshot_evidence, history_pointer_changed=False,
        source_rule="Same appended-day pre-quota G4 guards and two G3-added setups as full G6 history; fixed original exits; Nifty filter at confirmation.")
    replay.atomic_json(output / "summary.json", summary)
    (output / "report.html").write_text(render(summary, comparison, trades, sides), encoding="utf-8")
    code_paths = [Path(__file__), Path(config_path), Path(g6.__file__), Path(nifty.__file__),
                  Path(base.__file__), Path(g6.bounded.__file__), Path(g6.bounded.history.__file__)]
    replay.atomic_json(output / "manifest.json", dict(schema_version=SCHEMA,
        code_sha256={str(p.resolve()):base.sha256(p) for p in code_paths},
        files={p.relative_to(output).as_posix():base.sha256(p) for p in output.rglob("*") if p.is_file()}))
    verify(output)
    replay.atomic_json(g6.G6_ROOT / "latest" / "daily_backtest.json", dict(
        schema_version=SCHEMA, run_dir=output.relative_to(g6.G6_ROOT).as_posix(),
        manifest_sha256=base.sha256(output / "manifest.json"), session_date=str(day),
        direction_filter_mode=mode, selected_policy=policy, execution_authority=False))
    print("VERIFIED", output, flush=True)
    return summary


def verify(output):
    output = Path(output).resolve()
    manifest, summary = base.read(output / "manifest.json"), base.read(output / "summary.json")
    if summary["schema_version"] != SCHEMA or summary["execution_authority"] is not False:
        raise ValueError("Invalid G6 daily package")
    for name,digest in manifest["files"].items():
        path = (output / name).resolve()
        if not path.is_relative_to(output) or base.sha256(path) != digest:
            raise ValueError("G6 daily artifact changed: " + name)
    source = Path(summary["source_run"])
    if base.sha256(source / "run_manifest.json") != summary["source_manifest_sha256"]:
        raise ValueError("G3 daily source changed")
    config, orders = base.read(output / "config.json"), read_frozen_csv(output / "selected_orders.csv")
    day = date.fromisoformat(summary["session_date"])
    raw_paths = pd.read_parquet(output / "minute_paths.parquet")
    paths = {int(sid): {field:part[field].to_numpy() for field in
        ("timestamp_ns", "open", "high", "low", "close")} for sid,part in raw_paths.groupby("sid")}
    unfiltered = bounded_replay(orders, paths, day, config)
    saved = read_frozen_csv(output / "g6_unfiltered" / "portfolio_trades.csv")
    g6.bounded.history._same_allocation(unfiltered, saved)
    if len(unfiltered):
        g6.bounded.history._same_native(unfiltered, saved)
    audit = pd.read_csv(output / "nifty_direction_audit.csv")
    if len(audit):
        frame = pd.read_parquet(output / "nifty_inputs" / (str(day) + ".parquet"))
        for row in audit.itertuples():
            values = nifty.nifty.direction_at(frame, row.confirmation_ts)
            for field in ("slot_candle", "session_open"):
                if not np.isclose(values[field], getattr(row, field), atol=1e-10, rtol=0):
                    raise ValueError("Nifty direction replay differs")
            if row.both_counter_allowed != nifty.allowed(row.side, values["slot_candle"], values["session_open"]):
                raise ValueError("Nifty decision replay differs")
    filtered = filter_book(unfiltered, audit, config)
    saved_filtered = read_frozen_csv(output / g6.NIFTY_POLICY / "portfolio_trades.csv")
    g6.bounded.history._same_allocation(filtered, saved_filtered)
    if len(filtered) and (filtered.portfolio_executed & ~filtered.sid.map(audit.set_index("sid").both_counter_allowed)).any():
        raise ValueError("Rejected entry received capital")
    comparison = pd.read_csv(output / "comparison.csv").set_index("policy")
    for name, book in (("g6_unfiltered", unfiltered), (g6.NIFTY_POLICY, filtered)):
        for field,value in stats(book, day).items():
            if not np.isclose(comparison.loc[name, field], value, rtol=0, atol=1e-6):
                raise ValueError("Daily accounting differs")
    rebuilt = trade_comparison(unfiltered, filtered, audit)
    recorded = read_frozen_csv(output / "trade_comparison.csv")
    for field in ("filtered_net_pnl_rs", "unfiltered_net_pnl_rs", "filter_net_change_rs"):
        np.testing.assert_allclose(recorded[field], rebuilt[field], rtol=0, atol=1e-6)
    for filename in ("side_results.csv", "slot_side_results.csv"):
        grouped = pd.read_csv(output / filename).groupby("policy")
        for field in ("trades", "wins", "losses", "net_pnl_rs", "gross_pnl_rs", "costs_rs"):
            np.testing.assert_allclose(grouped[field].sum().loc[comparison.index], comparison[field], rtol=0, atol=1e-6)
    return summary
