"""Bounded retrospective entry-quality study; never promotes a live/default profile.

Reuses the verified V2 candidate pool and cash source hashes, filters BEFORE
native ranking, and evaluates only real observed native execution outcomes.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import html
import json
from pathlib import Path

import pandas as pd

from . import baseline, data, improvement_spec, selection
from .features import clean_minutes
from .path_evidence import assess, EVIDENCE_VERSION
from .research import (daily_metrics, feature_code_signature,
                       metric, sha256, split_for_day, write_json)


def evaluation_sets(days, selected, unknown, missing_index, unavailable, original_days):
    """No order-level omission/refill: an unknown removes its whole session."""
    blocked = set(missing_index) | set(unavailable)
    unknown_ids = set(unknown.sid)
    own = {name: [d for d in days if d not in blocked | set(
        orders.loc[orders.sid.isin(unknown_ids), "day"].astype(str))]
        for name, orders in selected.items()}
    common = [d for d in days if all(d in allowed for allowed in own.values())]
    return dict(common_all_dates=common,
                matched_original_dates=[d for d in common if d in set(original_days)]), own


def archived_selection_policy(source: Path) -> str:
    """Archived profiles without an explicit policy retain their original caps."""
    profile = Path(source) / "selected_profile.json"
    value = (json.loads(profile.read_text(encoding="utf-8")).get("slot_selection_policy",
             selection.FROZEN_G3_SELECTION) if profile.exists() else selection.FROZEN_G3_SELECTION)
    if value not in (selection.FROZEN_G3_SELECTION, selection.UNLIMITED_SELECTION):
        raise ValueError(f"Unsupported archived slot selection policy: {value}")
    return value


def replay_archived_control(source: Path, orders: pd.DataFrame, paths: dict, original_days: list,
                            output: Path, *, selection_policy: str) -> None:
    """Reconcile an independent historical-policy replay, not the new control."""
    original = pd.read_csv(source / "trades_no_oi_control.csv").set_index("sid")
    replay = baseline.simulate(orders.loc[orders.day.astype(str).isin(original_days)].copy(), paths,
                               allow_observed_prefix=True)
    replay.to_csv(output / "archived_control_replay.csv", index=False)
    replay = replay.set_index("sid")
    for column in ("portfolio_executed", "portfolio_net_profit_rupees", "portfolio_gross_profit_rupees", "portfolio_cost_rupees"):
        pd.testing.assert_series_equal(original[column].sort_index(), replay[column].sort_index(),
                                       check_dtype=False, check_names=False, rtol=1e-10, atol=1e-8)
    write_json(output / "control_reconciliation.json", dict(selected_order_ids_match=True,
        original_sessions=len(original_days), original_orders=len(original),
        archived_slot_selection_policy=selection_policy, study_slot_selection_policy=selection.UNLIMITED_SELECTION,
        separate_historical_policy_replay=True, executed_and_pnl_match=True, tolerance_rupees=1e-8))


def select_candidate(metrics):
    """Use declared historical partitions; their outcomes are already exposed."""
    source = metrics.loc[metrics.scope.eq("common_all_dates")
                         & metrics.additional_bps.eq(0)
                         & metrics.split.isin(["TRAIN", "VALIDATION"])].copy()
    names = [variant.name for variant in improvement_spec.variants()]
    options = []
    for order, name in enumerate(names):
        rows = source.loc[source.variant.eq(name)].set_index("split")
        if set(rows.index) != {"TRAIN", "VALIDATION"} or len(rows) != 2:
            raise ValueError(f"Expected one train/validation row for {name}")
        train, validation = rows.loc["TRAIN"], rows.loc["VALIDATION"]
        options.append(dict(name=name, grid_order=order,
            qualified=bool(train.trades >= 20 and train.net_pnl > 0 and validation.trades >= 10 and validation.net_pnl > 0),
            train_net=float(train.net_pnl), validation_net=float(validation.net_pnl),
            train_trades=int(train.trades), validation_trades=int(validation.trades),
            validation_drawdown=float(validation.max_drawdown)))
    qualified = [row for row in options if row["qualified"]]
    result = dict(sorted(qualified or options,
        key=lambda x: (-x["validation_net"], x["validation_drawdown"], x["grid_order"]))[0])
    result.update(candidate_count=len(options), qualified_candidates=len(qualified), selection_used_test=False,
                  reason="HISTORICAL_SCREEN_WINNER" if qualified else "NO_CANDIDATE_PASSED_SCREEN_RESEARCH_ONLY")
    result.update(historical_partitions_reused=True, fresh_out_of_sample=False,
                  execution_authority=False, default_profile_promoted=False)
    return result


def verify_source(root: Path, source: Path):
    if (root / "data/fetch.lock").exists() or (source / "precompute.lock").exists():
        raise RuntimeError("Finish data preparation before starting an improvement study")
    feature = json.loads((source / "feature_manifest.json").read_text())
    expected = {
        "feature_sha256": source / "feature_ledger.parquet",
        "session_coverage_sha256": source / "session_data_coverage.csv",
        "verified_symbol_coverage_sha256": source / "verified_symbol_coverage.json",
        "nifty_context_sha256": source / "nifty_context.csv",
        "data_manifest_sha256": root / "data/manifest.json",
    }
    for key, path in expected.items():
        if sha256(path) != feature[key]:
            raise RuntimeError(f"Source study input changed: {key}")
    if feature["feature_construction_sha256"] != feature_code_signature():
        raise RuntimeError("Current feature construction differs from the archived candidate pool")
    baseline.verify_snapshot()
    manifest = json.loads((root / "data/manifest.json").read_text())
    if sha256(root / "data/universe_manifest.json") != manifest["universe_sha256"]:
        raise RuntimeError("Universe manifest differs from the archived study")
    for record in manifest["files"]:
        if sha256(root / "data/1m" / f"{record['symbol']}.parquet") != record["sha256"]:
            raise RuntimeError(f"Cash data changed: {record['symbol']}")
    return manifest


def write_report(output: Path, summary: dict, metrics: pd.DataFrame):
    base = metrics.loc[metrics.additional_bps.eq(0)]
    sections = []
    for scope, rows in base.groupby("scope", sort=False):
        table = rows.pivot(index="variant", columns="split", values="net_pnl")
        counts = rows.loc[rows.split.eq("ALL"), ["variant", "sessions", "trades", "max_drawdown"]].set_index("variant")
        table = counts.join(table[["TRAIN", "VALIDATION", "TEST", "ALL"]])
        sections.append(f"<h2>{html.escape(scope)}</h2>" + table.to_html(float_format=lambda x: f"{x:,.2f}"))
    content = ("<!doctype html><html><head><meta charset='utf-8'><title>V14 improvement audit</title>"
        "<style>body{font:16px system-ui;margin:32px;max-width:1450px;background:#101721;color:#e2e8f0}"
        "table{border-collapse:collapse;width:100%;font-size:14px}th,td{padding:8px;border:1px solid #415064;text-align:right}"
        "th:first-child,td:first-child{text-align:left}h2{margin-top:32px}.notice{padding:16px;background:#313247}"
        "a{color:#9dccff}</style></head><body><h1>V14: bounded entry-quality experiment</h1>"
        "<p class='notice'>Retrospective research: every historical partition was already observed. "
        "The TEST label preserves the earlier calendar split; it is not a fresh holdout. "
        "Original baseline, reports and default profile remain unchanged. No live execution authority.</p>"
        f"<p>Historical selection: <strong>{html.escape(summary['selection']['name'])}</strong>. "
        f"Passed the declared historical screen: {summary['selection']['qualified']}. "
        f"Common sessions: {len(summary['evaluation_sets']['common_all_dates'])}; "
        f"matched original sessions: {len(summary['evaluation_sets']['matched_original_dates'])}. "
        "Each trade retains the native ₹500,000 modeled notional and 5 bps base cost.</p>"
        "<p>Capacity: planned notional ≤5% of signal five-minute turnover and ≤10% of confirmation minute turnover. "
        "Anti-chase: absolute five-minute move ≤1.25%, range ≤2.5%, distance from EMA9 ≤2.5%. "
        "Gates apply before unchanged native stock rankings. Every eligible stock is selected per slot; "
        "entries, exits and portfolio capital constraints remain unchanged.</p>"
        + "".join(sections) + "<p>Primary comparisons use identical dates. Own-coverage rows describe each arm separately "
        "and must not be ranked against one another. Unresolved selected paths remove whole dates; no lower-ranked "
        "replacement is made after execution outcomes are inspected.</p>"
        "<p>See variant_comparison.csv for additional 5/10/20 bps round-trip cost stress.</p></body></html>")
    (output / "report.html").write_text(content, encoding="utf-8")


def run(root: Path, source: Path, output: Path):
    root, source, output = map(Path, (root, source, output))
    if source.resolve() == output.resolve() or (output / "summary.json").exists():
        raise FileExistsError("Use a new output directory; completed studies are immutable")
    output.mkdir(parents=True, exist_ok=True)
    archived_policy = archived_selection_policy(source)
    archived_profile = source / "selected_profile.json"
    protocol = dict(version="V14_ENTRY_QUALITY_RETROSPECTIVE_V1",
        slot_selection_policy=selection.UNLIMITED_SELECTION,
        archived_control_slot_selection_policy=archived_policy,
        archived_profile_sha256=sha256(archived_profile) if archived_profile.exists() else None,
        specification=improvement_spec.specification(), source_study=str(source.resolve()),
        source_feature_sha256=sha256(source / "feature_ledger.parquet"),
        source_comparison_sha256={name: sha256(source / name) for name in (
            "evaluation_coverage.json", "selected_no_oi_control.csv", "trades_no_oi_control.csv", "summary.json")},
        recorded_before_rerun_pnl=True, historical_outcomes_already_exposed=True,
        fresh_holdout=False, primary_comparison="common_all_dates across all six arms",
        secondary_comparison="intersection with original18 evaluated sessions",
        own_coverage="Descriptive only; not comparable across variants",
        selection="Original TRAIN/VALIDATION positive-net and min20/10-trade screen; highest validation net then drawdown then declared grid order",
        path_policy=EVIDENCE_VERSION, missing_policy="Whole-date exclusion; no refill",
        additional_roundtrip_bps=[0., 5., 10., 20.], execution_authority=False,
        default_profile_promoted=False)
    protocol_path = output / "protocol.json"
    if protocol_path.exists():
        if json.loads(protocol_path.read_text())["protocol"] != protocol:
            raise RuntimeError("Protocol changed; use a new output directory")
    else:
        write_json(protocol_path, dict(recorded_at=datetime.now(timezone.utc).isoformat(), protocol=protocol))
    code_hashes = {p.name: sha256(p) for p in Path(__file__).parent.glob("*.py")}
    code_path = output / "executed_source_hashes.json"
    if code_path.exists() and json.loads(code_path.read_text()) != code_hashes:
        raise RuntimeError("Executed source changed since preregistration; create a new output directory")
    write_json(code_path, code_hashes)
    print("Verifying archived candidates and cash inputs", flush=True)
    manifest = verify_source(root, source)
    days = manifest["study_sessions"]
    pool = pd.read_parquet(source / "feature_ledger.parquet")
    common = pool.loc[pool.common_feature_coverage].copy()
    frozen = baseline.frozen_config()
    notional = frozen["capital_per_entry_rupees"] * frozen["leverage_factor"]
    diagnostics = common[["sid", "day", "tradingsymbol", "signal_ts", "confirmation_ts", "side"]].join(
        improvement_spec.diagnostic_features(common, planned_notional_rupees=notional))
    diagnostics.to_parquet(output / "candidate_quality_features.parquet", index=False)
    selected = {}
    for variant in improvement_spec.variants():
        orders = selection.select_orders(common, lambda rows, setup, v=variant:
            improvement_spec.participation_mask(rows, v, planned_notional_rupees=notional),
            selection_policy=selection.UNLIMITED_SELECTION)
        selected[variant.name] = orders
        orders.to_csv(output / f"selected_{variant.name}.csv", index=False)
    # Anchor a separate control using the archive's policy; new studies are uncapped.
    archived_control = pd.read_csv(source / "selected_no_oi_control.csv")
    archived_orders = selection.select_orders(common, selection_policy=archived_policy)
    if archived_orders.sid.astype(int).tolist() != archived_control.sid.astype(int).tolist():
        raise RuntimeError("Historical-policy control selections differ from the archived V2 control")
    archived_orders.to_csv(output / "archived_control_selections.csv", index=False)
    study_union = pd.concat(selected.values(), ignore_index=True).drop_duplicates("sid")
    union = pd.concat([study_union, archived_orders], ignore_index=True).drop_duplicates("sid")
    files = {r["symbol"]: r for r in manifest["files"]}
    paths, evidence, unknown = {}, {}, []
    groups = list(union.groupby("tradingsymbol", sort=True))
    for i, (symbol, group) in enumerate(groups, 1):
        path_file = root / "data/1m" / f"{symbol}.parquet"
        if sha256(path_file) != files[symbol]["sha256"]:
            raise RuntimeError(f"Cash input changed after verification: {symbol}")
        minute = clean_minutes(data.load_symbol_1m(symbol, root=root))
        if sha256(path_file) != files[symbol]["sha256"]:
            raise RuntimeError(f"Cash input changed during execution loading: {symbol}")
        for row in group.itertuples(index=False):
            path = baseline.native_paths(minute, row.confirmation_ts, row.day)
            proof = assess(row, path)
            evidence[str(row.sid)] = dict(sid=int(row.sid), day=str(row.day), symbol=symbol,
                resolved=proof["resolved"], reason=proof["reason"], **proof["evidence"])
            if proof["resolved"]:
                paths[int(row.sid)] = proof["path"]
            else:
                unknown.append(dict(sid=int(row.sid), day=str(row.day), symbol=symbol,
                    setup_id=row.setup_id, reason=proof["reason"]))
        if i % 50 == 0:
            print(f"Execution paths {i}/{len(groups)} symbols", flush=True)
    write_json(output / "path_evidence.json", evidence)
    unknown = pd.DataFrame(unknown, columns=["sid", "day", "symbol", "setup_id", "reason"])
    # Comparator-only uncertainty cannot change new-arm evaluation coverage.
    unknown = unknown.loc[unknown.sid.isin(study_union.sid)].copy()
    unknown.to_csv(output / "unknown_outcomes.csv", index=False)
    index = pd.read_csv(source / "nifty_context.csv")
    missing_index = set(days) - set(index.dropna(subset=["nifty_first_bar_return_pct"]).day)
    coverage = pd.read_csv(source / "session_data_coverage.csv")
    unavailable = set(coverage.loc[coverage.presence_fraction.lt(.95), "day"])
    original = json.loads((source / "evaluation_coverage.json").read_text())["evaluated_days"]
    scopes, own = evaluation_sets(days, selected, unknown, missing_index, unavailable, original)
    write_json(output / "evaluation_coverage.json", dict(evaluation_sets=scopes, own_days=own,
        slot_selection_policy=selection.UNLIMITED_SELECTION,
        missing_index_days=sorted(missing_index), unavailable_days=sorted(unavailable),
        unknown_selected_paths=len(unknown), original_evaluated_days=original,
        requested_days=days, no_refill=True))
    metrics, ledgers = [], {}
    for name, orders in selected.items():
        # Simulate own valid dates once; capital resets daily in the baseline.
        eligible = orders.loc[orders.day.astype(str).isin(own[name])].copy()
        ledger = baseline.simulate(eligible, paths, allow_observed_prefix=True)
        ledger["variant"] = name
        ledger["split"] = ledger.day.astype(str).map(split_for_day)
        ledgers[name] = ledger
        ledger.to_csv(output / f"trades_{name}.csv", index=False)
        for scope, scope_days in {**scopes, "own_coverage_only": own[name]}.items():
            for stress in (0., 5., 10., 20.):
                for split in ("TRAIN", "VALIDATION", "TEST", "ALL"):
                    split_days = scope_days if split == "ALL" else [d for d in scope_days if split_for_day(d) == split]
                    metrics.append(dict(scope=scope, variant=name, split=split, additional_bps=stress,
                                        **metric(ledger, split_days, additional_bps=stress)))
        daily_metrics(ledger, own[name]).to_csv(output / f"daily_{name}.csv", index=False)
        print(f"Evaluated {name}: selected={len(orders)} own_dates={len(own[name])}", flush=True)
    metrics = pd.DataFrame(metrics)
    metrics.to_csv(output / "variant_comparison.csv", index=False)
    replay_archived_control(source, archived_orders, paths, original, output, selection_policy=archived_policy)
    chosen = select_candidate(metrics)
    chosen_spec = next(v for v in improvement_spec.variants() if v.name == chosen["name"])
    profile = dict(version="V14_ENTRY_QUALITY_RETROSPECTIVE_V1", selection=chosen,
        slot_selection_policy=selection.UNLIMITED_SELECTION,
        variant=chosen_spec.to_dict(), specification=improvement_spec.specification(),
        baseline="frozen_G3_W1_V1p1", planned_notional_rupees=notional,
        protocol_sha256=sha256(protocol_path), source_feature_sha256=protocol["source_feature_sha256"],
        execution_authority=False, default_profile_promoted=False,
        next_evidence="Freeze these rules before at least20 genuinely unseen evaluated future sessions; report all outcomes and sample size")
    write_json(output / "research_candidate.json", profile)
    summary = dict(status="COMPLETE_RETROSPECTIVE_DIAGNOSTIC", selection=chosen,
        slot_selection_policy=selection.UNLIMITED_SELECTION,
        archived_control_slot_selection_policy=archived_policy,
        evaluation_sets=scopes, own_evaluated_sessions={n:len(ds) for n,ds in own.items()},
        unknown_selected_paths=len(unknown), requested_sessions=len(days),
        source_study=str(source.resolve()), output=str(output.resolve()),
        execution_authority=False, default_profile_promoted=False,
        limitations=["All historical outcomes already exposed; not fresh out-of-sample evidence",
          "Missing outcomes cause whole-session exclusions and possible coverage-selection bias",
          "Own-date coverage differs across arms and is descriptive only",
          "Current MIS membership, modeled5x leverage and flat5bps costs do not establish historical executable fills",
          "Capacity is based on observed turnover, not order book depth or guaranteed liquidity"])
    write_report(output, summary, metrics)
    write_json(output / "summary.json", summary)
    print(json.dumps(chosen, indent=2), flush=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=data.DEFAULT_ROOT)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    run(args.root, args.source, args.output)


if __name__ == "__main__":
    main()
