"""Extend the frozen G archive with a verified dated session.

This intentionally does not claim a regenerated V9 feature dataset. The source
archive remains the authority for prior results; a sealed daily ledger proves
the added session with the original retained G selector and settings. Genuine
missing futures-OI exclusions remain visible for dated sessions with trades.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime
from io import StringIO
import json
from pathlib import Path
import shutil
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fno_v13_v10_g_backtest as g
import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext
import fno_v13_v10_g_daily_replay as replay
import fno_oi_backtest_provenance as provenance
from research.g2_session_forensic_audit import normalize
from research.g3_dated_replay import _require_closed, _verified_source, IST


def write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")


def retained_selection(folder: Path, settings: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    path = folder / "feature_ledger.csv"
    manifest = g2.read_json(folder / "feature_ledger.csv.manifest.json")
    if g2.sha256(path) != manifest["artifact_sha256"]:
        raise ValueError("Daily feature ledger hash changed")
    pool = normalize(pd.read_csv(path))
    nifty = pool.nifty_first_bar_return_pct.dropna()
    if nifty.empty or nifty.nunique() != 1:
        raise ValueError("Missing or inconsistent first NIFTY bar return")
    if not (pool.confirmation_ts - pool.signal_ts).eq(pd.Timedelta(minutes=1)).all():
        raise ValueError("Dated feature ledger lacks exact next-minute confirmation")
    strict = replay._strict_signals(pool, float(nifty.iloc[0]))
    orders = g.select_orders(strict, ext._portfolio_config(settings),
        g.SelectionChange(**settings["selection_change"]), core_first=settings["core_first"],
        morning_slots=settings.get("morning_slots", False),
        two_bar_continuation=settings.get("two_bar_continuation", False))
    return pool, strict, orders


def require_zero_complete(source: dict, pool: pd.DataFrame, orders: pd.DataFrame, day: date) -> None:
    coverage = source["coverage"]
    if (source.get("state") != "SUCCESS" or source.get("complete") is not True
            or source.get("session_date") != str(day)):
        raise ValueError("A completed same-day source is required")
    count = coverage["universe_stocks"]
    if (count <= 0 or coverage["included_stocks"] != count or coverage["checked_stocks"] != count
            or coverage.get("excluded_stocks") or coverage.get("problems")):
        raise ValueError("Zero-order publication requires complete universe coverage")
    if set(pool.day) != {day} or pool.tradingsymbol.nunique() != count:
        raise ValueError("Dated ledger does not cover the reported universe and date")
    if pool.duplicated(["tradingsymbol", "signal_ts"]).any():
        raise ValueError("Duplicate dated feature observations")
    if len(orders):
        raise ValueError("This publisher only handles a proven zero-order session")


def require_trade_source(source: dict, pool: pd.DataFrame, orders: pd.DataFrame, day: date) -> None:
    """A dated trade may use the native replay's disclosed missing-OI exclusions."""
    coverage = source["coverage"]
    count = int(coverage["universe_stocks"])
    excluded = coverage.get("excluded_stocks") or []
    ignored = coverage.get("ignored_problems") or []
    excluded_symbols = {str(row["symbol"]) for row in excluded}
    if (source.get("state") != "SUCCESS" or source.get("complete") is not True
            or source.get("session_date") != str(day) or not len(orders)
            or count <= 0 or coverage.get("checked_stocks") != count
            or coverage.get("included_stocks") != count - len(excluded_symbols)
            or coverage.get("problems")
            or len(excluded_symbols) != len(excluded)
            or any(set(row.get("reasons", [])) != {"MISSING_SIGNAL_OR_PRIOR_FUTURES_OI_BAR"}
                   for row in excluded)
            or {str(row.get("symbol")) for row in ignored} != excluded_symbols
            or any(row.get("reason") != "MISSING_SIGNAL_OR_PRIOR_FUTURES_OI_BAR" for row in ignored)):
        raise ValueError("Dated trade source lacks complete replay with only disclosed OI exclusions")
    if (set(pool.day) != {day} or pool.tradingsymbol.nunique() != count - len(excluded_symbols)
            or set(pool.tradingsymbol) & excluded_symbols
            or pool.duplicated(["tradingsymbol", "signal_ts"]).any()):
        raise ValueError("Dated trade features do not match the included stock universe")


def verified_dataset_lineage(base_root: Path, last_session: str) -> tuple[Path, dict, list[str], list[Path]]:
    """Find the full frozen dataset beneath prior sealed dated extensions."""
    cursor = base_root.resolve()
    expected_cutoff = last_session
    extensions: list[str] = []
    evidence: list[Path] = []
    visited: set[Path] = set()
    while True:
        if cursor in visited:
            raise ValueError("Circular G archive source lineage")
        visited.add(cursor)
        metadata = g2.read_json(cursor / "g_backtest/run_metadata.json")
        if metadata["last_session"] != expected_cutoff:
            raise ValueError("G archive source lineage cutoff differs")
        dataset = cursor / "dataset"
        manifest_path = dataset / "dataset_manifest.json"
        if manifest_path.is_file():
            manifest = g2.read_json(manifest_path)
            if manifest["through_day"] != expected_cutoff:
                raise ValueError("Full G dataset cutoff differs from source archive")
            return dataset, manifest, sorted(extensions), evidence
        proof_path = cursor / "g_backtest/dated_extension_proof.json"
        extension_path = cursor / "extension_manifest.json"
        proof = g2.read_json(proof_path)
        extension = g2.read_json(extension_path)
        if (proof.get("complete") is not True or proof.get("session_date") != expected_cutoff
                or int(proof.get("selected_orders", -1)) < 0 or int(proof.get("trades", -1)) < 0
                or not (proof.get("historical_rows_unchanged") is True
                        or proof.get("historical_ledger_unchanged") is True)
                or extension.get("complete") is not True
                or extension.get("through_day") != expected_cutoff):
            raise ValueError("Prior G dated extension is not a sealed session")
        for relative, expected in extension["artifacts"].items():
            path = (cursor / relative).resolve()
            if not path.is_relative_to(cursor) or g2.sha256(path) != expected:
                raise ValueError(f"Prior G dated extension artifact drift: {relative}")
        for name, expected in proof["input_hashes"].items():
            if g2.sha256(Path(name)) != expected:
                raise ValueError(f"Prior G dated extension input drift: {name}")
        prior = Path(metadata["source_archive"]).resolve()
        if prior == cursor or Path(proof["source_archive"]).resolve() != prior:
            raise ValueError("Prior G dated extension source archive differs")
        extensions.append(expected_cutoff)
        evidence.extend((proof_path, extension_path))
        cursor = prior
        expected_cutoff = g2.read_json(cursor / "g_backtest/run_metadata.json")["last_session"]


def prepare(base_root: Path, day: date, staging: Path) -> dict:
    _require_closed(day, datetime.now(IST))
    if staging.exists():
        raise FileExistsError(f"Refusing to overwrite staging: {staging}")
    base_root = base_root.resolve()
    old = base_root / "g_backtest"
    metadata = g2.read_json(old / "run_metadata.json")
    if str(day) <= metadata["last_session"]:
        raise ValueError("Dated session must extend the archived cutoff")
    config = Path(metadata["frozen_g_config"])
    if g2.sha256(config) != metadata["frozen_g_config_sha256"]:
        raise ValueError("Archived frozen G configuration changed")
    settings = g.checked_settings(g2.read_json(config))
    if settings.get("morning_slots") or settings.get("two_bar_continuation"):
        raise ValueError("This dated source supports the original retained setup book")
    dataset, dataset_manifest, prior_dated_days, lineage_evidence = verified_dataset_lineage(
        base_root, metadata["last_session"]
    )
    # Verify persisted historical inputs, not their mutable upstream raw files.
    for name, expected in dataset_manifest["output_sha256"].items():
        if g2.sha256(dataset / name) != expected:
            raise ValueError(f"Archived dataset artifact drift: {name}")
    calendar_path = base_root / "dataset/source_session_eligibility.csv"
    archived_calendar = pd.read_csv(calendar_path)
    eligible = archived_calendar.loc[archived_calendar.eligible.astype(str).str.lower().eq("true")]
    old_days = sorted(eligible.loc[eligible.day.between(metadata["first_session"], metadata["last_session"]), "day"].tolist())
    if (old_days != sorted([*dataset_manifest["days"], *prior_dated_days])
            or len(old_days) != int(metadata["session_count"])):
        raise ValueError("Archived session calendar does not reconcile")
    ledger = pd.read_csv(old / "portfolio_trades.csv")
    metrics = g.r.metric(ledger, old_days)
    for key, expected in metadata["metrics"]["full_history"].items():
        if expected is not None and not np.isclose(metrics[key], expected, rtol=0, atol=1e-7):
            raise ValueError(f"Archived result metrics differ: {key}")
    run, source, snapshot, ledger_path = _verified_source(day)
    source_manifest = g2.read_json(run / "source_manifest.json")
    if source_manifest["frozen_config_sha256"] != g2.sha256(config):
        raise ValueError("Dated source references a different retained G configuration")
    pool, strict, orders = retained_selection(run, settings)
    if len(orders):
        require_trade_source(source, pool, orders, day)
    else:
        require_zero_complete(source, pool, orders, day)
    setups = g.v9.v5.profile_setups(g.v9.v5.PROFILES["higher_frequency"])
    required_slots = {int(setup.signal_end.replace(":", "")) for setup in setups}
    if any(set(part.hhmm_int) != required_slots for _, part in pool.groupby("tradingsymbol")):
        raise ValueError("Dated features do not cover every retained setup slot")
    if len(pool) != source["coverage"]["included_stocks"] * len(required_slots):
        raise ValueError("Dated features do not have one row per stock and setup slot")
    # Retain evidence that the same dated selector reproduces recent full-data
    # selections, including native feature values at those selected decisions.
    identity = ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"]
    overlap = []
    for prior_day in old_days[-3:]:
        prior_run, _ = ext._latest_successful_run(ext.DEFAULT_DAILY_ROOT, date.fromisoformat(prior_day))
        _, _, current = retained_selection(prior_run, settings)
        expected = ledger.loc[ledger.day.eq(prior_day)].copy()
        # The retained selector can return duplicate column labels on an
        # empty day; there is no selected row to compare in that case.
        if current.empty:
            current = current.loc[:, ~current.columns.duplicated()].copy()
        current["day"] = current.day.astype(str)
        current, expected = [part.sort_values(identity).reset_index(drop=True) for part in (current, expected)]
        if current[identity].astype(str).to_dict("records") != expected[identity].astype(str).to_dict("records"):
            raise ValueError(f"Dated retained selection differs from archive: {prior_day}")
        for field in ("trigger", "body_ratio", "wick_ratio", "v9_1m_volume_ratio"):
            if len(current) and not np.allclose(current[field], expected[field], rtol=0, atol=1e-7, equal_nan=True):
                raise ValueError(f"Dated feature differs from archive: {prior_day}/{field}")
        overlap.append(dict(day=prior_day, selected_orders=len(expected), selection_identity_verified=True,
            selected_features_verified=True, feature_ledger=str(prior_run / "feature_ledger.csv"),
            feature_ledger_sha256=g2.sha256(prior_run / "feature_ledger.csv")))
    # Use the actual raw eligibility scan, with the same V5 revalidation as
    # the dataset builder. Do not infer a session merely from weekday/date.
    eligibility, _, _, regimes, scan_source = g.v9.v5.v13_v3._load_eligibility(True, g.v9.v5.MIN_CONTRACT_COVERAGE)
    intervening = eligibility.loc[eligibility.day.gt(date.fromisoformat(metadata["last_session"]))
        & eligibility.day.le(day) & eligibility.coverage.ge(g.v9.v5.MIN_CONTRACT_COVERAGE)
        & eligibility.contracts_with_data.gt(0) & eligibility.required_contract.isin(regimes)]
    if intervening.day.tolist() != [day]:
        raise ValueError("Dated publication would skip an independently eligible session")
    row = eligibility.loc[eligibility.day.eq(day)].copy()
    if len(row) != 1:
        raise ValueError("Dated source has no unique independently observed eligible session")
    row["seed_eligible"] = row.eligible.astype(bool)
    row["v13_v5_min_contract_coverage"] = g.v9.v5.MIN_CONTRACT_COVERAGE
    row["eligible"] = (row.coverage.ge(g.v9.v5.MIN_CONTRACT_COVERAGE)
        & row.contracts_with_data.gt(0) & row.required_contract.isin(regimes))
    row["v13_v5_eligibility_reason"] = "OK_REVALIDATED"
    if not row.eligible.all() or int(row.universe_size.iloc[0]) != source["coverage"]["universe_stocks"]:
        raise ValueError("Raw eligibility does not match the sealed dated universe")
    snapshot_manifest = g2.read_json(snapshot / "snapshot_manifest.json")
    universe_record = next(item for item in snapshot_manifest["sources"] if item["role"] == "DATED_UNIVERSE")
    universe, _ = provenance.load_backtest_universe(
        universe_path=snapshot / universe_record["snapshot_relative_path"], universe_date=day,
        contract_month_contains=str(row.required_contract.iloc[0]), require_persisted_mapping=True)
    if len(universe) != source["coverage"]["universe_stocks"]:
        raise ValueError("Snapshot universe size differs from completed replay coverage")
    excluded = {row["symbol"] for row in source["coverage"]["excluded_stocks"]}
    expected_futures = set(universe.loc[~universe.equity_symbol.isin(excluded), "futures_tradingsymbol"])
    if expected_futures != set(pool.futures_tradingsymbol):
        raise ValueError("Dated features differ from the snapshot stock universe")
    if not pool.futures_tradingsymbol.str.contains(str(row.required_contract.iloc[0]), regex=False).all():
        raise ValueError("Dated futures contract differs from eligibility calendar")
    paths = [config, old / "portfolio_trades.csv", old / "summary.json", old / "run_metadata.json", calendar_path,
        dataset / "dataset_manifest.json", run / "replay_result.json", run / "source_manifest.json", ledger_path,
        run / "feature_ledger.csv.manifest.json", snapshot / "snapshot_manifest.json", *lineage_evidence]
    input_hashes = {str(path): g2.sha256(path) for path in paths}
    days = old_days + [str(day)]
    if len(orders):
        selected = ext._apply_retained_g_exits(orders, settings)
        execution_paths = ext._selected_paths(selected, day, snapshot)
        dated_trades, _, _ = ext._simulate(
            selected, execution_paths, ext._portfolio_config(settings), stop_pct=None
        )
        combined, combined_summary = g.v9.v6.apply_portfolio_constraints(
            pd.concat([ledger, dated_trades], ignore_index=True, sort=False),
            ext._portfolio_config(settings).portfolio_config(),
        )
        # Recompute portfolio allocation, then verify every archived row after
        # CSV normalization. New orders must not change any earlier result.
        historical = pd.read_csv(StringIO(combined.iloc[:len(ledger)][ledger.columns].to_csv(index=False)))
        pd.testing.assert_frame_equal(
            ledger.reset_index(drop=True), historical.reset_index(drop=True),
            check_dtype=False, check_exact=False, rtol=0, atol=1e-7,
        )
        orders = selected
    else:
        combined, combined_summary = ledger, None
    dated_ledger = combined.loc[combined.day.astype(str).eq(str(day))]
    if len(dated_ledger) != len(orders):
        raise ValueError("Dated portfolio ledger does not match selected orders")
    combined_metrics = g.r.metric(combined, days)
    source_kind = ("VERIFIED_FROZEN_G_ARCHIVE_PLUS_DATED_SESSION" if len(orders)
                   else "VERIFIED_FROZEN_G_ARCHIVE_PLUS_DATED_ZERO_SESSION")
    proof = dict(schema_version="RETAINED_G_DATED_SESSION_V2" if len(orders)
                 else "RETAINED_G_DATED_ZERO_SESSION_V1", complete=True, session_date=str(day),
        strategy="V13-V10-G", frozen_g_config=str(config), frozen_g_config_sha256=g2.sha256(config),
        selection_rule="Original retained G strict raw signals and frozen setup selection; no revised production policy",
        raw_features=len(pool), strict_signals=len(strict), selected_orders=len(orders),
        trades=int(dated_ledger.portfolio_executed.sum()),
        net_profit_rupees=float(dated_ledger.portfolio_net_profit_rupees.sum()),
        cost_rupees=float(dated_ledger.portfolio_cost_rupees.sum()),
        coverage=source["coverage"], source_g_daily_run=str(run),
        snapshot_manifest=str(snapshot / "snapshot_manifest.json"), input_snapshot_fully_verified=True,
        exact_next_minute_confirmation_verified=True, all_retained_setup_slots_covered=True,
        historical_selection_parity=overlap, source_eligibility_scan=scan_source,
        observed_eligibility=row.to_dict("records"), input_hashes=input_hashes,
        source_archive=str(base_root), historical_ledger_unchanged=not len(orders),
        historical_rows_unchanged=True, execution_authority=False)
    staging.mkdir(parents=True, exist_ok=False)
    output = staging / "g_backtest"
    output.mkdir()
    (staging / "dataset").mkdir()
    if len(orders):
        combined.to_csv(output / "portfolio_trades.csv", index=False)
    else:
        shutil.copy2(old / "portfolio_trades.csv", output / "portfolio_trades.csv")
    shutil.copy2(config, output / "frozen_config.json")
    source_calendar = calendar_path.read_bytes()
    addition = StringIO(newline="")
    writer = csv.DictWriter(addition, fieldnames=archived_calendar.columns.tolist(), lineterminator="\n")
    writer.writerow({key: row.iloc[0][key] for key in archived_calendar.columns})
    if not source_calendar.endswith(b"\n"):
        source_calendar += b"\n"
    (staging / "dataset/source_session_eligibility.csv").write_bytes(source_calendar + addition.getvalue().encode("utf-8"))
    orders.to_csv(output / "dated_selected_orders.csv", index=False)
    strict.to_csv(output / "dated_strict_signals.csv", index=False)
    g.selection_audit(strict, ext._portfolio_config(settings), g.SelectionChange(**settings["selection_change"]),
        core_first=settings["core_first"]).to_csv(output / "dated_selection_audit.csv", index=False)
    write_json(output / "dated_extension_proof.json", proof)
    summary = g2.read_json(old / "summary.json")
    if combined_summary is not None:
        summary.update(combined_summary)
    summary.update(first_session=min(days), last_session=max(days), through_day=str(day), session_count=len(days),
        source_kind=source_kind, historical_results_unchanged=True,
        regenerated_feature_dataset=False, dated_extension_proof="dated_extension_proof.json")
    write_json(output / "summary.json", summary)
    metadata.update(through_day=str(day), last_session=str(day), session_count=len(days),
        generated_at=datetime.now(IST).isoformat(), source_kind=summary["source_kind"],
        source_archive=str(base_root), regenerated_feature_dataset=False,
        historical_overlap_parity=True, historical_overlap_cutoff=max(old_days),
        dated_extension_proof="dated_extension_proof.json", metrics={"full_history": combined_metrics})
    write_json(output / "run_metadata.json", metadata)
    coverage = g2.read_json(old / "data_coverage_audit.json")
    coverage["dates"][str(day)] = dict(coverage_basis="Completed sealed daily replay at every retained setup slot",
        **source["coverage"], raw_features=len(pool), strict_signals=len(strict),
        selected_orders=len(orders), executed_trades=proof["trades"])
    coverage["scope"] = "Historical dataset coverage preserved; added session coverage from its verified sealed daily replay"
    write_json(output / "data_coverage_audit.json", coverage)
    daily = []
    for session in days:
        part = combined.loc[combined.day.astype(str).eq(session) & combined.portfolio_executed.eq(True)]
        daily.append(dict(day=session, trades=len(part), wins=int(part.portfolio_net_profit_rupees.gt(0).sum()),
            losses=int(part.portfolio_net_profit_rupees.lt(0).sum()), net_pnl=float(part.portfolio_net_profit_rupees.sum()),
            cost=float(part.portfolio_cost_rupees.sum()), gross_pnl=float(part.portfolio_gross_profit_rupees.sum())))
    daily = pd.DataFrame(daily)
    daily["cumulative_net_pnl"] = daily.net_pnl.cumsum()
    daily.to_csv(output / "daily_results.csv", index=False)
    report = (
        '<!doctype html><html><head><meta charset="utf-8"><title>V13-V10-G dated extension</title>'
        '<style>body{font:15px system-ui;margin:40px;color:#173b42}table{border-collapse:collapse}td,th{padding:8px;border:1px solid #ccc}</style></head><body>'
        '<h1>V13-V10-G through ' + str(day) + '</h1><p>Earlier archived trade rows are unchanged. '
        'The added session uses the original retained G selector and sealed daily minute inputs. '
        'Stock exclusions for missing futures OI are disclosed in the dated proof. '
        'The historical feature dataset was not regenerated.</p>'
        '<p>' + str(len(days)) + ' sessions; ' + str(combined_metrics['trades']) +
        ' trades; modeled net P&amp;L INR ' + f"{combined_metrics['net_profit_rupees']:,.2f}" + '</p>'
        '<p><a href="dated_extension_proof.json">Dated source and selection proof</a> | '
        '<a href="portfolio_trades.csv">Extended trade archive</a> | '
        '<a href="daily_results.csv">Daily results</a></p>'
        + daily.to_html(index=False) + '</body></html>')
    (output / "report.html").write_text(report, encoding="utf-8")
    for path, expected in input_hashes.items():
        if g2.sha256(Path(path)) != expected:
            raise ValueError(f"Source changed during publication: {path}")
    published_ledger = pd.read_csv(output / "portfolio_trades.csv")
    if len(published_ledger) != len(combined):
        raise ValueError("Dated trade archive row count differs")
    pd.testing.assert_frame_equal(
        ledger.reset_index(drop=True),
        published_ledger.iloc[:len(ledger)][ledger.columns].reset_index(drop=True),
        check_dtype=False, check_exact=False, rtol=0, atol=1e-7,
    )
    if not len(orders) and g2.sha256(output / "portfolio_trades.csv") != input_hashes[str(old / "portfolio_trades.csv")]:
        raise ValueError("Copied historical trade archive differs")
    inventory = {p.relative_to(staging).as_posix(): g2.sha256(p) for p in staging.rglob("*") if p.is_file()}
    write_json(staging / "extension_manifest.json", dict(complete=True, through_day=str(day), source_kind=summary["source_kind"],
        historical_ledger_unchanged=not len(orders), historical_rows_unchanged=True,
        config_bytes_unchanged=g2.sha256(output / "frozen_config.json") == g2.sha256(config),
        artifacts=inventory, helper_sha256=g2.sha256(Path(__file__))))
    return proof


def publish(staging: Path, output: Path) -> None:
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite output: {output}")
    manifest = g2.read_json(staging / "extension_manifest.json")
    if manifest.get("complete") is not True:
        raise ValueError("Prepared extension is incomplete")
    for relative, expected in manifest["artifacts"].items():
        path = (staging / relative).resolve()
        if not path.is_relative_to(staging.resolve()) or g2.sha256(path) != expected:
            raise ValueError(f"Prepared extension hash drift: {relative}")
    proof = g2.read_json(staging / "g_backtest/dated_extension_proof.json")
    for name, expected in proof["input_hashes"].items():
        if g2.sha256(Path(name)) != expected:
            raise ValueError(f"Source changed since preparation: {name}")
    # Discovery requires metadata plus the calendar. Commit metadata last so
    # readers never observe a partially copied run as a complete backtest.
    def defer_metadata(directory: str, names: list[str]) -> list[str]:
        return ["run_metadata.json"] if Path(directory) == staging / "g_backtest" else []
    shutil.copytree(staging, output, ignore=defer_metadata)
    for relative, expected in manifest["artifacts"].items():
        if relative != "g_backtest/run_metadata.json" and g2.sha256(output / relative) != expected:
            raise ValueError(f"Published extension hash drift: {relative}")
    target = output / "g_backtest/run_metadata.json"
    temporary = target.with_suffix(".json.tmp")
    shutil.copy2(staging / "g_backtest/run_metadata.json", temporary)
    temporary.replace(target)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--date", type=date.fromisoformat)
    parser.add_argument("--staging", type=Path, required=True)
    parser.add_argument("--publish", type=Path)
    args = parser.parse_args()
    if args.publish:
        publish(args.staging, args.publish)
        print(json.dumps({"published": str(args.publish)}))
    else:
        if not args.base_root or not args.date:
            parser.error("--base-root and --date are required to prepare")
        proof = prepare(args.base_root, args.date, args.staging)
        print(json.dumps({key: proof[key] for key in ("complete", "session_date", "raw_features", "strict_signals", "selected_orders")}, indent=2))
