"""Re-evaluate fixed archived selections with independently verified coverage.

No candidate selection, tuning, order submission or default-profile promotion
occurs here. Original reports and canonical candles remain immutable.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
import html
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from . import baseline, data, gap_repair, interval_evidence, portfolio_evidence, selection
from .features import clean_minutes
from .path_evidence import assess as assess_prefix, EVIDENCE_VERSION as PREFIX_VERSION
from .research import daily_metrics, metric, sha256, split_for_day, write_json

VERSION = "V14_FIXED_SELECTION_COVERAGE_PROOF_V1"
DEFAULT_SOURCE = data.DEFAULT_ROOT / "runs/20261010_participation_uncapped_v1"
DEFAULT_REPAIR = data.DEFAULT_ROOT / "repairs/20261010_uncapped_gaps_v1"
DEFAULT_OUTPUT = data.DEFAULT_ROOT / "runs/20261010_coverage_repaired_v1"
ADDITIONAL_BPS = (0., 5., 10., 20.)


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_fixed_selections(source: Path) -> dict[str, pd.DataFrame]:
    """Round-trip archived prices exactly; authenticate them against Parquet.

    CSV's default float parser can move a trigger across an exact high/low
    touch. Selection membership and ordering come solely from archived CSVs;
    the immutable feature Parquet verifies their preselection source values.
    """
    source = Path(source)
    protocol = _read(source / "protocol.json")["protocol"]
    if protocol.get("slot_selection_policy") != selection.UNLIMITED_SELECTION:
        raise ValueError("Coverage repair requires explicitly recorded uncapped selections")
    features = pd.read_parquet(source / "feature_ledger.parquet")
    if features.sid.duplicated().any():
        raise ValueError("Archived feature IDs are ambiguous")
    features = features.set_index("sid", drop=False)
    selected = {}
    for variant in protocol["variants"]:
        name = variant["name"]
        if name in selected or Path(name).name != name:
            raise ValueError("Invalid or duplicate archived variant name")
        orders = pd.read_csv(source / f"selected_{name}.csv", float_precision="round_trip")
        if orders.sid.duplicated().any() or not orders.sid.isin(features.index).all():
            raise ValueError(f"Invalid archived selected IDs: {name}")
        reference = features.loc[orders.sid].reset_index(drop=True)
        keys = ["sid", "day", "tradingsymbol", "signal_ts", "confirmation_ts", "side", "trigger",
                "traded_value", "volume_ratio", "price_change_pct"]
        for column in keys:
            if column not in orders or column not in reference:
                raise ValueError(f"Missing archived selection source field: {column}")
            left, right = orders[column], reference[column]
            if column.endswith("_ts"):
                same = pd.to_datetime(left, utc=True).eq(pd.to_datetime(right, utc=True)).all()
            elif pd.api.types.is_numeric_dtype(right.dtype):
                same = np.array_equal(left.to_numpy(), right.to_numpy(), equal_nan=True)
            else:
                same = left.astype(str).eq(right.astype(str)).all()
            if not same:
                raise ValueError(f"Archived {name} {column} differs from exact feature values")
        if "slot_selection_policy" not in orders or not orders.slot_selection_policy.eq(selection.UNLIMITED_SELECTION).all():
            raise ValueError(f"Archived selection policy missing or changed: {name}")
        selected[name] = orders
    if "no_oi_control" not in selected:
        raise ValueError("Archived no-OI control is required")
    # Candidate membership/rank can differ by variant; a shared SID must still
    # describe precisely the same native order before we price the union once.
    shared = ["sid", "day", "tradingsymbol", "signal_ts", "confirmation_ts", "side", "trigger",
              "setup_id", "picker", "traded_value", "volume_ratio", "price_change_pct",
              "native_stop_pct", "native_target_pct", "slot_selection_policy"]
    for name, frame in selected.items():
        if set(shared).difference(frame):
            raise ValueError(f"Missing native archived order fields for {name}: {sorted(set(shared).difference(frame))}")
    joined = pd.concat([frame[shared] for frame in selected.values()], ignore_index=True)
    for column in shared[1:]:
        if joined.groupby("sid", sort=False)[column].nunique(dropna=False).gt(1).any():
            raise ValueError(f"Conflicting shared selected-order field across variants: {column}")
    return selected


def verify_inputs(root: Path, source: Path, repairdir: Path) -> dict:
    if (root / "data/fetch.lock").exists() or (source / "precompute.lock").exists():
        raise RuntimeError("Finish canonical data preparation before coverage replay")
    baseline.verify_snapshot()
    feature = _read(source / "feature_manifest.json")
    checks = {"feature_sha256": source / "feature_ledger.parquet",
              "data_manifest_sha256": root / "data/manifest.json",
              "nifty_context_sha256": source / "nifty_context.csv",
              "session_coverage_sha256": source / "session_data_coverage.csv"}
    for field, path in checks.items():
        if sha256(path) != feature[field]:
            raise RuntimeError(f"Archived input changed: {field}")
    manifest = _read(root / "data/manifest.json")
    if sha256(root / "data/universe_manifest.json") != manifest["universe_sha256"]:
        raise RuntimeError("Canonical universe manifest changed")
    if sha256(source / "input_manifest.json") != feature["data_manifest_sha256"]:
        raise RuntimeError("Archived data manifest copy differs")
    for record in manifest["files"]:
        if sha256(root / "data/1m" / f"{record['symbol']}.parquet") != record["sha256"]:
            raise RuntimeError(f"Canonical cash data changed: {record['symbol']}")
    coverage = _read(source / "evaluation_coverage.json")
    if manifest["study_sessions"] != coverage["requested_days"]:
        raise RuntimeError("Archived and canonical study calendars differ")
    inventory = _read(repairdir / "inventory.json")
    if Path(inventory["canonical_root"]).resolve() != root.resolve() or Path(inventory["source_run"]).resolve() != source.resolve():
        raise RuntimeError("Repair inventory belongs to a different source study")
    for name in ("fetch_summary.json", "fetch_5m_summary.json"):
        fetch = _read(repairdir / name)
        if fetch.get("status") != "COMPLETE" or fetch.get("errors") != 0 or not fetch.get("canonical_hashes_unchanged"):
            raise RuntimeError(f"Incomplete or invalid repair fetch: {name}")
    if _read(repairdir / "fetch_summary.json").get("recovered_missing_minutes") != 0:
        raise RuntimeError("This recorded coverage protocol requires zero recovered one-minute bars")
    profile = _read(source / "selected_profile.json")
    if profile.get("cost_bps") != 5 or profile.get("slot_selection_policy") != selection.UNLIMITED_SELECTION:
        raise RuntimeError("Archived selection profile cost or slot policy differs")
    return dict(manifest=manifest, coverage=coverage, inventory=inventory, profile=profile)


def preregister(root: Path, source: Path, repairdir: Path, output: Path, selected: dict, inputs: dict) -> dict:
    names = list(selected)
    source_files = ["protocol.json", "feature_manifest.json", "feature_ledger.parquet", "input_manifest.json",
        "input_universe_manifest.json", "evaluation_coverage.json", "path_evidence.json", "unknown_outcomes.csv",
        "selected_profile.json", "nifty_context.csv", "session_data_coverage.csv"]
    source_files += [f"{kind}_{name}.csv" for name in names for kind in ("selected", "trades")]
    repair_files = [repairdir / name for name in ("inventory.json", "fetch_summary.json", "fetch_5m_summary.json")]
    for pair in inputs["inventory"]["pairs"]:
        for unit in ("1m", "5m"):
            stem = f"{pair['symbol']}_{pair['day']}"
            repair_files.extend((repairdir / f"broker_{unit}" / f"{stem}.json",
                repairdir / f"broker_{unit}" / f"{stem}.parquet", repairdir / f"broker_raw_{unit}" / f"{stem}.parquet"))
    code = {path.name: sha256(path) for path in Path(__file__).parent.glob("*.py")}
    if "evidence_execution.py" not in code:
        raise RuntimeError("Frozen execution proof bridge must exist before preregistration")
    protocol = dict(version=VERSION, source_run=str(source), canonical_root=str(root), repair_directory=str(repairdir),
        variants=names, selected_counts={name: len(frame) for name, frame in selected.items()},
        fixed_profile_variant=inputs["profile"]["selected_variant"]["name"],
        source_sha256={name: sha256(source / name) for name in source_files},
        canonical_manifest_sha256=sha256(root / "data/manifest.json"),
        canonical_universe_sha256=sha256(root / "data/universe_manifest.json"),
        repair_sha256={str(path.relative_to(repairdir)): sha256(path) for path in repair_files},
        implementation_sha256=code, requested_days=inputs["coverage"]["requested_days"],
        original_evaluated_days=inputs["coverage"]["evaluated_days"],
        missing_index_days=inputs["coverage"]["missing_index_days"],
        insufficient_cash_coverage_days=inputs["coverage"].get("insufficient_cash_coverage_days", []),
        slot_selection_policy=selection.UNLIMITED_SELECTION, prefix_policy=PREFIX_VERSION,
        independent_interval_policy=interval_evidence.EVIDENCE_VERSION,
        portfolio_evidence_policy=portfolio_evidence.EVIDENCE_VERSION,
        primary_roundtrip_bps=5., additional_roundtrip_bps=list(ADDITIONAL_BPS),
        total_roundtrip_bps=[5. + value for value in ADDITIONAL_BPS], expected_real_minute_additions=0,
        selection="Identical archived orders; no reselection, ranking changes, tuning or promotion",
        coverage="Own exact portfolio dates and their all-variant intersection; an admitted unresolved order blocks its whole day",
        historical_outcomes_already_exposed=True, fresh_holdout=False, default_profile_promoted=False,
        canonical_mutations=False, execution_authority=False)
    target = output / "protocol.json"
    if target.exists():
        if _read(target)["protocol"] != protocol:
            raise RuntimeError("Recorded coverage protocol or inputs changed; use a new output directory")
    else:
        write_json(target, dict(recorded_at=datetime.now(timezone.utc).isoformat(), protocol=protocol))
    write_json(output / "executed_source_hashes.json", code)
    return protocol


def _symbol_worker(task: tuple) -> dict:
    root_text, source_text, repair_text, symbol, rows, expected_hash, repair_days = task
    root, source, repairdir = map(Path, (root_text, source_text, repair_text))
    filename = root / "data/1m" / f"{symbol}.parquet"
    if sha256(filename) != expected_hash:
        raise RuntimeError(f"Canonical cash changed before proof: {symbol}")
    minute = clean_minutes(data.load_symbol_1m(symbol, root=root, start=min(rows.day), end=max(rows.day)))
    if sha256(filename) != expected_hash:
        raise RuntimeError(f"Canonical cash changed during proof loading: {symbol}")
    day_frames = {str(day): group.copy() for day, group in minute.groupby(minute.ts.dt.strftime("%Y-%m-%d"), sort=False)}
    proofs, attempts, repairs = {}, [], {}
    for row in rows.itertuples(index=False):
        day = str(row.day)
        frame = day_frames.get(day, minute.iloc[:0])
        path = baseline.native_paths(frame, row.confirmation_ts, day)
        proof = assess_prefix(row, path)
        if not proof["resolved"] and day in repair_days:
            if day not in repairs:
                additions, envelopes, provenance = gap_repair.load_verified_repair(symbol, day, repairdir,
                                                                                  root=root, source_run=source)
                if len(additions):
                    raise RuntimeError("Verified one-minute additions violate the recorded zero-addition protocol")
                repairs[day] = (envelopes, provenance)
            envelopes, provenance = repairs[day]
            candidate = interval_evidence.assess(row, path, envelopes)
            attempts.append(dict(sid=int(row.sid), day=day, symbol=symbol, strict_reason=proof["reason"],
                interval_reason=candidate["reason"], interval_resolved=candidate["resolved"],
                price_scale_compatible=provenance["price_scale_compatible"],
                certified_missing_minutes=candidate["evidence"].get("missing_minutes_certified",0)))
            if candidate["resolved"]:
                proof = candidate
            # An unsuccessful envelope attempt must not replace an earlier
            # trustworthy strict entry with an unproved sparse candidate entry.
        proofs[int(row.sid)] = proof
    return dict(proofs=proofs, interval_attempts=attempts,
                repair_verification=[provenance for _, provenance in repairs.values()])


def _known_trades(orders: pd.DataFrame, proofs: dict) -> pd.DataFrame:
    from .evidence_execution import simulate_proven
    known = orders.loc[orders.sid.map(lambda sid: proofs[int(sid)]["resolved"])].copy()
    return simulate_proven(known, proofs, cost_bps=5.)


def _old_date_parity(source: Path, name: str, ledger: pd.DataFrame, days: list[str]) -> dict:
    original = pd.read_csv(source / f"trades_{name}.csv", float_precision="round_trip").set_index("sid").sort_index()
    replay = ledger.loc[ledger.day.astype(str).isin(days)].set_index("sid").sort_index()
    pd.testing.assert_index_equal(replay.index, original.index, exact=False)
    numeric = ["entry_price", "exit_price", "net_return_pct", "portfolio_net_profit_rupees",
               "portfolio_gross_profit_rupees", "portfolio_cost_rupees",
               "portfolio_open_positions_before", "portfolio_reserved_capital_before_rupees"]
    for column in numeric:
        np.testing.assert_allclose(pd.to_numeric(replay[column]), pd.to_numeric(original[column]),
                                   rtol=1e-10, atol=1e-8, equal_nan=True, err_msg=f"{name}: {column}")
    for column in ("filled", "portfolio_executed", "portfolio_status", "portfolio_reject_reason", "exit_reason"):
        pd.testing.assert_series_equal(replay[column].fillna("").astype(str), original[column].fillna("").astype(str),
                                       check_names=False, check_dtype=False)
    for column in ("entry_ts", "exit_ts"):
        pd.testing.assert_series_equal(pd.to_datetime(replay[column], utc=True), pd.to_datetime(original[column], utc=True),
                                       check_names=False, check_dtype=False)
    return dict(variant=name, sessions=len(days), selected_orders=len(original),
                executed_trades=int(original.portfolio_executed.eq(True).sum()), exact_native_replay=True)


def write_report(output: Path, summary: dict, comparison: pd.DataFrame) -> None:
    base = comparison.loc[comparison.additional_bps.eq(0) & comparison.split.eq("ALL")]
    sections = []
    for scope in ("common_all_dates", "own_coverage_only", "original_evaluated_dates"):
        rows = base.loc[base.scope.eq(scope), ["variant", "sessions", "trades", "wins", "win_rate_pct", "net_pnl",
                                                  "profit_factor", "max_drawdown"]]
        sections.append(f"<h2>{html.escape(scope)}</h2>" + rows.to_html(index=False, float_format=lambda x:f"{x:,.2f}"))
    focus = html.escape(summary["fixed_profile_variant"])
    content = ("<!doctype html><html lang='en'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'>"
        "<title>V14 fixed-selection coverage replay</title><style>body{font:16px system-ui;margin:28px;background:#101721;color:#edf2fa;max-width:1550px}"
        "a{color:#9dccff}table{border-collapse:collapse;width:100%;font-size:14px}th,td{padding:8px;border:1px solid #526176;text-align:right}"
        "th:first-child,td:first-child{text-align:left}.notice{background:#26344b;padding:16px}.scroll{overflow-x:auto}h2{margin-top:30px}</style></head><body>"
        "<h1>V14: fixed selections, improved coverage evidence</h1>"
        "<p class='notice'>Retrospective replay of the same 16 uncapped selections. No indicators, entries, exits, profile choice or canonical candles changed. "
        "These dates were previously exposed; they are not fresh out-of-sample evidence. No default profile was promoted.</p>"
        f"<p>Primary focus: no-OI control and the previously recorded candidate <strong>{focus}</strong>. "
        f"Common evaluated sessions: <strong>{summary['common_evaluated_sessions']}</strong> of {summary['requested_sessions']}; "
        f"previously {summary['original_evaluated_sessions']}. Original-date execution and P&amp;L parity passed for every variant.</p>"
        "<p>A missing minute is crossed only when an independently fetched and verified five-minute price range rules out every relevant trigger, stop or target touch. "
        "An unresolved exit can be ignored for portfolio P&amp;L only when native capital rules definitively rejected that observed entry. "
        "Unknown admitted trades exclude their entire date; rejected unknown trades remain visible in the ledger with null hypothetical returns.</p>"
        "<p>Costs: 5 bps round trip; sensitivity rows use 10, 15 and 25 bps total on the same fills. "
        "Net PF uses positive after-cost executed-trade P&amp;L divided by absolute negative after-cost P&amp;L. "
        "Drawdown uses daily closing equity, including initial zero.</p><div class='scroll'>" + "".join(sections) + "</div>"
        "<p>Own-coverage dates differ between variants and cannot rank them fairly. Common-date comparison also remains subject to coverage-selection bias. "
        "Sparse-path MFE/MAE are left unavailable where not exactly observed. Two source index-context exclusions remain unchanged.</p>"
        "<p><a href='variant_comparison.csv'>All metrics and cost stress</a> · <a href='evaluation_coverage.json'>Exact evaluated dates</a> · "
        "<a href='old_date_parity.json'>Archived-date parity</a> · <a href='protocol.json'>Recorded protocol</a></p></body></html>")
    (output / "report.html").write_text(content, encoding="utf-8")


def run(root: Path, source: Path, repairdir: Path, output: Path, *, workers: int = 3) -> dict:
    root, source, repairdir, output = [Path(p).resolve() for p in (root,source,repairdir,output)]
    if (output in (root, root / "data", source, repairdir) or (output / "summary.json").exists()
            or root / "data" in output.parents or source in output.parents or repairdir in output.parents):
        raise FileExistsError("Use a new result directory; existing studies and canonical data are immutable")
    output.mkdir(parents=True, exist_ok=True)
    print("Verifying fixed selections, canonical candles and repair provenance", flush=True)
    inputs = verify_inputs(root, source, repairdir)
    selected = load_fixed_selections(source)
    protocol = preregister(root, source, repairdir, output, selected, inputs)
    profile_path = root / "selected_profile.json"
    profile_hash = sha256(profile_path) if profile_path.exists() else None
    shutil.copyfile(source / "selected_profile.json", output / "source_selected_profile.json")
    for name in selected:
        shutil.copyfile(source / f"selected_{name}.csv", output / f"selected_{name}.csv")
    union = pd.concat(selected.values(), ignore_index=True).drop_duplicates("sid")
    files = {record["symbol"]: record for record in inputs["manifest"]["files"]}
    repair_pairs = {(p["symbol"],p["day"]) for p in inputs["inventory"]["pairs"]}
    tasks = [(str(root),str(source),str(repairdir),symbol,group,files[symbol]["sha256"],
              {day for stock,day in repair_pairs if stock==symbol})
             for symbol,group in union.groupby("tradingsymbol",sort=True)]
    proofs, attempts, repairs = {}, [], []
    with ProcessPoolExecutor(max_workers=max(1,min(3,int(workers)))) as executor:
        for count,result in enumerate(executor.map(_symbol_worker,tasks),1):
            proofs.update(result["proofs"])
            attempts.extend(result["interval_attempts"])
            repairs.extend(result["repair_verification"])
            if count%25==0 or count==len(tasks):
                print(f"Coverage proofs {count}/{len(tasks)} symbols",flush=True)
    if set(proofs)!=set(union.sid):
        raise RuntimeError("Coverage proof IDs do not match frozen selections")
    flat = {str(sid): dict(sid=int(sid), resolved=p["resolved"],reason=p["reason"],**p["evidence"])
            for sid,p in proofs.items()}
    write_json(output / "path_evidence.json",flat)
    pd.DataFrame(attempts).to_csv(output / "interval_attempts.csv",index=False)
    write_json(output / "verified_repair_usage.json",dict(pairs=len(repairs),minute_additions=sum(r["minute_additions"] for r in repairs),
        compatible_pairs=sum(r["price_scale_compatible"] for r in repairs),verification=repairs))
    current_code={path.name:sha256(path) for path in Path(__file__).parent.glob("*.py")}
    if current_code!=protocol["implementation_sha256"]:
        raise RuntimeError("Implementation changed after coverage preregistration")
    known_union = _known_trades(union,proofs)
    days = protocol["requested_days"]
    unavailable = set(protocol["missing_index_days"])|set(protocol["insufficient_cash_coverage_days"])
    ledgers, own, rejected_counts, blocked_counts, parity = {}, {}, {}, {}, []
    for name,orders in selected.items():
        known = known_union.loc[known_union.sid.isin(orders.sid)].copy()
        result = portfolio_evidence.resolve_portfolio(orders,proofs,known,days=days,unavailable_days=unavailable,cost_bps=5.)
        own[name], ledgers[name] = result["resolved_days"],result["ledger"]
        ledger = ledgers[name]
        ledger["variant"],ledger["split"] = name,ledger.day.astype(str).map(split_for_day)
        ledger.to_csv(output/f"trades_{name}.csv",index=False)
        result["rejected_unknown"].to_csv(output/f"rejected_unknown_{name}.csv",index=False)
        result["blocked_days"].to_csv(output/f"blocked_days_{name}.csv",index=False)
        rejected_counts[name]=len(result["rejected_unknown"])
        blocked_counts[name]=len(result["blocked_days"])
        daily_metrics(ledger,own[name]).to_csv(output/f"daily_{name}.csv",index=False)
        if not set(protocol["original_evaluated_days"]).issubset(own[name]):
            raise RuntimeError(f"Coverage replay lost an originally evaluated day: {name}")
        parity.append(_old_date_parity(source,name,ledger,protocol["original_evaluated_days"]))
        print(f"Exact portfolio {name}: {len(own[name])} dates; {int(ledger.portfolio_executed.sum())} executed; {rejected_counts[name]} unknown exits proved rejected",flush=True)
    common = [day for day in days if all(day in allowed for allowed in own.values())]
    comparison=[]
    for name,ledger in ledgers.items():
        for scope,scope_days in {"common_all_dates":common,"own_coverage_only":own[name],
                                "original_evaluated_dates":protocol["original_evaluated_days"]}.items():
            for stress in ADDITIONAL_BPS:
                for split in ("TRAIN","VALIDATION","TEST","ALL"):
                    use = scope_days if split=="ALL" else [day for day in scope_days if split_for_day(day)==split]
                    comparison.append(dict(scope=scope,variant=name,split=split,additional_bps=stress,total_bps=5+stress,
                                           **metric(ledger,use,additional_bps=stress)))
        daily_metrics(ledger,common).to_csv(output/f"daily_common_{name}.csv",index=False)
    comparison=pd.DataFrame(comparison)
    comparison.to_csv(output/"variant_comparison.csv",index=False)
    write_json(output/"old_date_parity.json",dict(status="PASS",results=parity,tolerance_rupees=1e-8))
    coverage=dict(requested_days=days,evaluated_days=common,evaluation_sets=dict(common_all_dates=common,
        original_evaluated_dates=protocol["original_evaluated_days"]),own_days=own,
        original_evaluated_days=protocol["original_evaluated_days"],missing_index_days=protocol["missing_index_days"],
        insufficient_cash_coverage_days=protocol["insufficient_cash_coverage_days"],
        unknown_after_price_proof=sum(not p["resolved"] for p in proofs.values()),
        rejected_unknown_by_variant=rejected_counts,blocked_dates_by_variant=blocked_counts,
        selection_unchanged=True,slot_selection_policy=selection.UNLIMITED_SELECTION,
        interval_policy=interval_evidence.EVIDENCE_VERSION,portfolio_policy=portfolio_evidence.EVIDENCE_VERSION)
    write_json(output/"evaluation_coverage.json",coverage)
    focus=protocol["fixed_profile_variant"]
    ledgers[focus].loc[ledgers[focus].day.astype(str).isin(common)].to_csv(output/"trades.csv",index=False)
    daily_metrics(ledgers[focus],common).to_csv(output/"daily_results.csv",index=False)
    summary=dict(status="COMPLETE_FIXED_SELECTION_COVERAGE_REPLAY",version=VERSION,
        source_run=str(source),repair_directory=str(repairdir),output=str(output),
        requested_sessions=len(days),common_evaluated_sessions=len(common),original_evaluated_sessions=len(protocol["original_evaluated_days"]),
        own_evaluated_sessions={name:len(ds) for name,ds in own.items()},fixed_profile_variant=focus,
        original_profile_qualified=inputs["profile"]["selection"]["qualified"],profile_retuned=False,
        primary_roundtrip_bps=5.,total_roundtrip_bps=[5.,10.,15.,25.],
        focus_metrics={name:{scope:metric(ledgers[name],dates) for scope,dates in
                      {"common_all_dates":common,"own_coverage_only":own[name]}.items()}
                       for name in ("no_oi_control",focus)},
        strict_resolved_orders=sum(p["resolved"] and p["evidence"]["version"]==PREFIX_VERSION for p in proofs.values()),
        interval_resolved_orders=sum(p["resolved"] and p["evidence"]["version"]==interval_evidence.EVIDENCE_VERSION for p in proofs.values()),
        remaining_unknown_price_outcomes=sum(not p["resolved"] for p in proofs.values()),
        rejected_unknown_by_variant=rejected_counts,canonical_mutations=False,minute_additions=0,
        old_date_parity="PASS",fresh_holdout=False,default_profile_promoted=False,execution_authority=False,
        limitations=["All historical dates were already exposed; this is no fresh holdout",
                     "Own-date coverage differs between variants; comparisons require identical dates",
                     "Admissible unresolved orders still exclude whole dates; coverage-selection bias remains",
                     "Five-minute envelopes certify non-events only; every fill/exit comes from an observed minute",
                     "MFE/MAE can be unknown across certified minute gaps",
                     "Inherited 5bps costs and modeled5x exposure do not establish executable non-F&O profitability"])
    if (sha256(profile_path) if profile_path.exists() else None)!=profile_hash:
        raise RuntimeError("Default profile changed during coverage replay")
    for filename,expected in protocol["source_sha256"].items():
        if sha256(source/filename)!=expected:
            raise RuntimeError(f"Archived input changed during coverage replay: {filename}")
    if sha256(root/"data/manifest.json")!=protocol["canonical_manifest_sha256"]:
        raise RuntimeError("Canonical data manifest changed during coverage replay")
    baseline.verify_snapshot()
    if {path.name:sha256(path) for path in Path(__file__).parent.glob("*.py")}!=protocol["implementation_sha256"]:
        raise RuntimeError("Implementation changed before coverage publication")
    for record in inputs["manifest"]["files"]:
        if sha256(root/"data/1m"/f"{record['symbol']}.parquet")!=record["sha256"]:
            raise RuntimeError(f"Canonical cash changed during coverage replay: {record['symbol']}")
    for filename,expected in protocol["repair_sha256"].items():
        if sha256(repairdir/filename)!=expected:
            raise RuntimeError(f"Verified repair input changed during coverage replay: {filename}")
    write_report(output,summary,comparison)
    write_json(output/"summary.json",summary)
    print(json.dumps({k:summary[k] for k in ("common_evaluated_sessions","own_evaluated_sessions","fixed_profile_variant","interval_resolved_orders")},indent=2),flush=True)
    return summary


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root",type=Path,default=data.DEFAULT_ROOT)
    parser.add_argument("--source",type=Path,default=DEFAULT_SOURCE)
    parser.add_argument("--repairdir",type=Path,default=DEFAULT_REPAIR)
    parser.add_argument("--output",type=Path,default=DEFAULT_OUTPUT)
    parser.add_argument("--workers",type=int,default=3)
    args=parser.parse_args()
    run(args.root,args.source,args.repairdir,args.output,workers=args.workers)


if __name__=="__main__":
    main()
