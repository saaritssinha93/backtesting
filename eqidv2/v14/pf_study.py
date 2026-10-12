"""Predeclared indicator research targeting net PF2, with causal walk-forward selection.

All dates in this first study have previously been inspected. This is a
retrospective robustness experiment, never a fresh holdout or live authority.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
import hashlib
import html
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

from . import baseline, data, pf_indicators, pf_validation, selection
from .features import clean_minutes
from .improvement_spec import diagnostic_features
from .improvement_study import verify_source
from .path_evidence import assess, EVIDENCE_VERSION
from .research import sha256, write_json

PRIMARY_ROUNDTRIP_BPS = 10.
INITIAL_TRAIN_SESSIONS = 20
FORWARD_SESSIONS = 5


def protocol(source: Path):
    return dict(version="V14_PF_INDICATORS_V1", indicator_specification=pf_indicators.specification(),
        slot_selection_policy=selection.UNLIMITED_SELECTION,
        source_study=str(source.resolve()), source_feature_sha256=sha256(source / "feature_ledger.parquet"),
        common_original_features="Original V2 common six-family finite feature pool",
        common_base="Pre-rank capacity and anti-chase gates frozen in the preceding six-arm study",
        new_common_features=pf_indicators.COMMON_FEATURES,
        baseline_changes="New pre-rank indicator eligibility, all eligible stocks selected per slot and explicitly higher modeled costs; execution and portfolio constraints retained",
        primary_roundtrip_bps=PRIMARY_ROUNDTRIP_BPS, inherited_roundtrip_bps=5.,
        stress_total_roundtrip_bps=[10., 15., 20.], cost_assumption="Scenario, not a measured spread or broker charge estimate",
        folds=dict(initial_train_sessions=INITIAL_TRAIN_SESSIONS, forward_sessions=FORWARD_SESSIONS,
            calendar="Original requested dates before exclusions; expand training, never shift folds to improve coverage",
            train_min_trades=20, train_min_losses=3, train_min_net_pf=1.3,
            selector="Highest prior TRAIN net PF, then net P&L, then declared grid order; otherwise remain in cash"),
        target=dict(net_profit_factor=2., minimum_forward_trades=50, minimum_forward_evaluated_sessions=20,
            minimum_forward_losing_trades=5, zero_loss_pf="Undefined, never automatic success",
            robustness="95% five-session-block bootstrap lower PF>1 and total15bps PF>=1.5",
            bootstrap_replicates=2000, seed=1729),
        path_policy=EVIDENCE_VERSION, missing_policy="Any selected unresolved path excludes its date across every arm; never refill",
        minimum_symbol_presence=.95, historical_outcomes_previously_exposed=True, fresh_holdout=False,
        multiple_testing="One fixed14-arm batch; no outcome-driven threshold adjustment; report every arm",
        execution_authority=False, default_profile_promoted=False)


def preregister(source: Path, output: Path):
    record = protocol(source)
    target = output / "protocol.json"
    hashes = {name: sha256(Path(__file__).with_name(name)) for name in (
        "pf_study.py", "pf_indicators.py", "pf_validation.py", "improvement_spec.py",
        "baseline.py", "selection.py", "path_evidence.py", "features.py", "data.py")}
    if target.exists():
        old = json.loads(target.read_text(encoding="utf-8"))
        if old["protocol"] != record or old["source_sha256"] != hashes:
            raise RuntimeError("Protocol or implementation changed; choose a new study directory")
    else:
        write_json(target, dict(recorded_at=datetime.now(timezone.utc).isoformat(), protocol=record, source_sha256=hashes))
    return record


def _feature_worker(task):
    root_text, cache_text, symbol, candidates, source_hash, code_hash = task
    root, cache = Path(root_text), Path(cache_text)
    source = root / "data/1m" / f"{symbol}.parquet"
    target, metadata = cache / f"{symbol}.parquet", cache / f"{symbol}.json"
    candidate_hash = hashlib.sha256(pd.util.hash_pandas_object(candidates, index=True).values.tobytes()).hexdigest()
    key = dict(raw_source_sha256=source_hash, code_sha256=code_hash, candidates_sha256=candidate_hash)
    if sha256(source) != source_hash:
        raise RuntimeError(f"Cash input changed before indicator features: {symbol}")
    if target.exists() and metadata.exists():
        stored = json.loads(metadata.read_text())
        if stored["key"] == key and stored["sha256"] == sha256(target):
            return pd.read_parquet(target)
    minute = data.load_symbol_1m(symbol, root=root)
    result = pf_indicators.add_features(minute, candidates)
    if sha256(source) != source_hash:
        raise RuntimeError(f"Cash input changed while constructing indicators: {symbol}")
    temp = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    result.to_parquet(temp, index=False)
    os.replace(temp, target)
    write_json(metadata, dict(key=key, sha256=sha256(target), rows=len(result)))
    return result


def build_pool(root: Path, source: Path, output: Path, manifest: dict):
    original = pd.read_parquet(source / "feature_ledger.parquet")
    original = original.loc[original.common_feature_coverage].copy()
    frozen = baseline.frozen_config()
    notional = frozen["capital_per_entry_rupees"] * frozen["leverage_factor"]
    measurements = diagnostic_features(original, planned_notional_rupees=notional)
    base = original.loc[measurements.capacity_pass & measurements.anti_chase_pass].copy()
    files = {r["symbol"]: r for r in manifest["files"]}
    cache = output / "feature_cache"
    cache.mkdir(exist_ok=True)
    code_hash = sha256(Path(pf_indicators.__file__))
    tasks = [(str(root), str(cache), symbol, group, files[symbol]["sha256"], code_hash)
             for symbol, group in base.groupby("tradingsymbol", sort=True)]
    parts = []
    with ProcessPoolExecutor(max_workers=3) as workers:
        for i, result in enumerate(workers.map(_feature_worker, tasks, chunksize=1), 1):
            parts.append(result)
            if i % 25 == 0 or i == len(tasks):
                print(f"Indicator features {i}/{len(tasks)} symbols", flush=True)
    enriched = (pd.concat(parts, ignore_index=True).sort_values("sid", kind="stable") if parts
                else pf_indicators.add_features(pd.DataFrame(), base))
    common_mask = np.isfinite(enriched[pf_indicators.COMMON_FEATURES]).all(axis=1)
    enriched["pf_common_coverage"] = common_mask
    enriched.to_parquet(output / "feature_ledger.parquet", index=False)
    counts = enriched.groupby("day").agg(base_candidates=("sid", "size"),
        common_candidates=("pf_common_coverage", "sum"))
    counts.to_csv(output / "feature_coverage.csv")
    write_json(output / "feature_manifest.json", dict(original_common_candidates=len(original),
        base_candidates=len(base), base_symbols=base.tradingsymbol.nunique(),
        new_common_candidates=int(common_mask.sum()), new_common_features=pf_indicators.COMMON_FEATURES,
        feature_sha256=sha256(output / "feature_ledger.parquet"),
        data_manifest_sha256=sha256(root / "data/manifest.json"),
        source_feature_sha256=sha256(source / "feature_ledger.parquet"), indicator_code_sha256=code_hash))
    return enriched.loc[common_mask].copy()


def _execution_worker(task):
    root_text, symbol, rows, source_hash = task
    root = Path(root_text)
    source = root / "data/1m" / f"{symbol}.parquet"
    if sha256(source) != source_hash:
        raise RuntimeError(f"Cash input changed before execution: {symbol}")
    minute = clean_minutes(data.load_symbol_1m(symbol, root=root,
        start=min(rows.day.astype(str)), end=max(rows.day.astype(str))))
    if sha256(source) != source_hash:
        raise RuntimeError(f"Cash input changed while loading execution: {symbol}")
    paths, evidence, unknown = {}, [], []
    for row in rows.itertuples(index=False):
        path = baseline.native_paths(minute, row.confirmation_ts, row.day)
        proof = assess(row, path)
        evidence.append(dict(sid=int(row.sid), day=str(row.day), symbol=symbol,
            resolved=proof["resolved"], reason=proof["reason"], **proof["evidence"]))
        if proof["resolved"]:
            paths[int(row.sid)] = proof["path"]
        else:
            unknown.append(dict(sid=int(row.sid), day=str(row.day), symbol=symbol,
                setup_id=row.setup_id, reason=proof["reason"]))
    return paths, evidence, unknown


def report(output: Path, summary: dict, comparison: pd.DataFrame, decisions: list[dict]):
    base = comparison.loc[comparison.additional_roundtrip_bps.eq(0)].copy()
    table = base.loc[base.scope.eq("ALL_COMMON"), ["variant", "trades", "losing_trades", "net_pf", "net_pnl", "daily_close_drawdown"]]
    folds = pd.DataFrame([dict(fold=d["fold"], first_forward_day=d["test_days"][0],
        last_forward_day=d["test_days"][-1], selected=d["selected_variant"] or "CASH",
        train_pf=d["winner"]["train_pf"] if d["winner"] else None,
        evaluated_sessions=len(d["evaluated_test_days"])) for d in decisions])
    assessment = summary["walk_forward_assessment"]
    stress = pd.DataFrame(assessment["cost_stress"])[["additional_roundtrip_bps", "sessions", "active_sessions", "trades", "losing_trades", "net_pf", "net_pnl", "daily_close_drawdown"]]
    stress["total_roundtrip_bps"] = stress.additional_roundtrip_bps + PRIMARY_ROUNDTRIP_BPS
    stress = stress.drop(columns="additional_roundtrip_bps")
    interval = assessment["bootstrap"]
    document = ("<!doctype html><html lang='en'><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'><title>V14 PF2 indicator study</title>"
        "<style>body{font:16px/1.5 system-ui;background:#101721;color:#e6edf7;margin:30px}main{max-width:1350px;margin:auto}"
        "table{border-collapse:collapse;width:100%;font-size:14px}td,th{padding:8px;border:1px solid #425065;text-align:right}"
        "td:first-child,th:first-child{text-align:left}.table{overflow:auto}.notice{padding:16px;background:#313247;border-left:4px solid #fbbf24}"
        "a{color:#9dccff}h2{margin-top:30px}</style></head><body><main><h1>V14: indicator research for net PF 2+</h1>"
        f"<p class='notice'><strong>{html.escape(summary['status'])}</strong>. All historical dates were already inspected. "
        "Walk-forward choices use earlier dates only, but this remains retrospective evidence. No live/default profile is promoted.</p>"
        "<p>Profit factor = positive net trade P&amp;L / absolute negative net trade P&amp;L. All trade costs are included. "
        "A no-loss sample has undefined PF and cannot qualify. Primary costs are 10 bps round trip, twice the inherited 5 bps assumption; "
        "15/20 bps scenarios are also shown. These are modeled cost scenarios, not measured fills.</p>"
        f"<p>Requested sessions: {summary['requested_sessions']}; common evaluated sessions: {summary['evaluated_sessions']}. "
        f"Unknown selected paths: {summary['unknown_selected_paths']}. Every selected unknown removes its date from all arms; no refill.</p>"
        "<h2>Primary result: chronological walk-forward</h2>"
        "<p>Initial 20 requested sessions train the selector; it chooses once for each following 5-session block. "
        "At each choice, eligible rules require at least 20 earlier trades, 3 losses, positive net P&amp;L and PF at least 1.3. "
        "Rank by earlier PF, net P&amp;L, then declared rule order. Stay in cash if none qualifies. Missing dates never move these boundaries.</p>"
        f"<div class='table'>{stress.to_html(index=False, float_format=lambda x:f'{x:,.3f}')}</div>"
        f"<p>PF 2 target with minimum sample passed: <strong>{assessment['retrospective_target_and_sample_pass']}</strong>. "
        f"Stronger cost/uncertainty check passed: <strong>{assessment['retrospective_target_sample_and_robustness_pass']}</strong>. "
        "Minimum evidence is 50 forward executed trades, 20 evaluated sessions and 5 losing trades. "
        f"Conditional 95% five-session-block bootstrap PF interval: {interval['lower']} to {interval['upper']} "
        f"(upper unbounded: {interval['upper_unbounded']}). This interval does not correct for previous strategy searches.</p>"
        f"<div class='table'>{folds.to_html(index=False, float_format=lambda x:f'{x:,.3f}')}</div>"
        "<h2>All 14 fixed rules: descriptive full-period results</h2>"
        "<p>This table must not select a winner from the forward results. All rules share the same candidate feature availability "
        "and dates. Capacity and price-extension gates precede original G3 rankings. Every eligible stock is selected per slot; "
        "entries, stops, targets, sizing and portfolio capital constraints are unchanged.</p>"
        f"<div class='table'>{table.to_html(index=False, float_format=lambda x:f'{x:,.3f}')}</div>"
        "<p><a href='protocol.json'>Preregistered rules</a> | <a href='variant_comparison.csv'>All scopes and cost stresses</a> | "
        "<a href='walk_forward_decisions.json'>Each historical decision</a> | <a href='walk_forward_trades.csv'>Forward trade audit</a> | "
        "<a href='evaluation_coverage.json'>Excluded dates and reasons</a> | <a href='summary.json'>Complete evidence</a></p>"
        "<p>Limitations: reused research history; approximate historical MIS/F&amp;O snapshots where exact membership is unavailable; "
        "modeled uniform 5x exposure; unknown-path exclusions and associated coverage-selection bias; OHLCV capacity is not order-book depth. "
        "No stock, direction, setup or threshold was removed after viewing this batch's outcomes.</p></main></body></html>")
    (output / "report.html").write_text(document, encoding="utf-8")


def run(root: Path, source: Path, output: Path, *, features_only=False):
    root, source, output = map(Path, (root, source, output))
    if source.resolve() == output.resolve() or (output / "summary.json").exists():
        raise FileExistsError("Use a new output directory; completed studies are immutable")
    output.mkdir(parents=True, exist_ok=True)
    record = preregister(source, output)
    print("Verifying original study and cash source hashes", flush=True)
    manifest = verify_source(root, source)
    days = manifest["study_sessions"]
    pool = build_pool(root, source, output, manifest)
    if features_only:
        return dict(status="FEATURES_READY_NO_PNL", candidates=len(pool), slot_selection_policy=selection.UNLIMITED_SELECTION)
    selected = {variant.name: selection.select_orders(pool,
        lambda rows, setup, variant=variant: pf_indicators.participation_mask(rows, variant),
        selection_policy=selection.UNLIMITED_SELECTION)
        for variant in pf_indicators.variants()}
    for name, orders in selected.items():
        orders.to_csv(output / f"selected_{name}.csv", index=False)
    union = pd.concat(selected.values(), ignore_index=True).drop_duplicates("sid")
    files = {r["symbol"]: r for r in manifest["files"]}
    tasks = [(str(root), symbol, rows, files[symbol]["sha256"])
             for symbol, rows in union.groupby("tradingsymbol", sort=True)]
    paths, evidence, unknown = {}, [], []
    with ProcessPoolExecutor(max_workers=3) as workers:
        for i, (new_paths, new_evidence, new_unknown) in enumerate(workers.map(_execution_worker, tasks), 1):
            paths.update(new_paths)
            evidence.extend(new_evidence)
            unknown.extend(new_unknown)
            if i % 50 == 0 or i == len(tasks):
                print(f"Execution evidence {i}/{len(tasks)} symbols", flush=True)
    write_json(output / "path_evidence.json", evidence)
    unknown = pd.DataFrame(unknown, columns=["sid", "day", "symbol", "setup_id", "reason"])
    unknown.to_csv(output / "unknown_outcomes.csv", index=False)
    index = pd.read_csv(source / "nifty_context.csv")
    missing_index = set(days) - set(index.dropna(subset=["nifty_first_bar_return_pct"]).day)
    coverage = pd.read_csv(source / "session_data_coverage.csv")
    missing_cash = set(coverage.loc[coverage.presence_fraction.lt(.95), "day"])
    excluded = missing_index | missing_cash | set(unknown.day)
    evaluated = [d for d in days if d not in excluded]
    forward_days = [d for d in days[INITIAL_TRAIN_SESSIONS:] if d in evaluated]
    write_json(output / "evaluation_coverage.json", dict(requested_days=days, evaluated_days=evaluated,
        slot_selection_policy=selection.UNLIMITED_SELECTION,
        forward_evaluated_days=forward_days, excluded_days=sorted(excluded),
        missing_index_days=sorted(missing_index), insufficient_cash_days=sorted(missing_cash),
        unknown_selected_paths=len(unknown), selected_union=len(union), common_candidates=len(pool),
        no_refill=True, source_data_manifest_sha256=sha256(root / "data/manifest.json")))
    ledgers, metrics = {}, []
    for name, orders in selected.items():
        ledger = baseline.simulate(orders.loc[orders.day.astype(str).isin(evaluated)].copy(), paths,
            cost_bps=PRIMARY_ROUNDTRIP_BPS, allow_observed_prefix=True)
        ledger["variant"] = name
        # Empty controls still need explicit modeled-exposure schema for stats.
        if "portfolio_trade_exposure_rupees" not in ledger:
            ledger["portfolio_trade_exposure_rupees"] = pd.Series(dtype=float)
        ledgers[name] = ledger
        ledger.to_csv(output / f"trades_{name}.csv", index=False)
        scopes = {"ALL_COMMON": evaluated, "INITIAL_TRAIN": [d for d in days[:INITIAL_TRAIN_SESSIONS] if d in evaluated],
                  "FORWARD_DATES_DESCRIPTIVE": forward_days}
        for scope, dates in scopes.items():
            for stress in (0., 5., 10.):
                metrics.append(dict(variant=name, scope=scope, **pf_validation.day_metrics(ledger, dates, stress)))
        print(f"Simulated fixed arm {name}", flush=True)
    comparison = pd.DataFrame(metrics)
    comparison.to_csv(output / "variant_comparison.csv", index=False)
    forward, decisions = pf_validation.walk_forward_select(ledgers, days,
        initial_train_sessions=INITIAL_TRAIN_SESSIONS, test_sessions=FORWARD_SESSIONS,
        available_days=evaluated, min_train_trades=20, min_train_pf=1.3, min_train_losing_trades=3)
    forward.to_csv(output / "walk_forward_trades.csv", index=False)
    write_json(output / "walk_forward_decisions.json", decisions)
    assessment = pf_validation.assess_pf_target(forward, forward_days)
    train_all = comparison.loc[comparison.scope.eq("ALL_COMMON") & comparison.additional_roundtrip_bps.eq(0)]
    next_choice = pf_validation.select_train_variant(train_all)
    write_json(output / "next_research_candidate.json", dict(selection=next_choice,
        slot_selection_policy=selection.UNLIMITED_SELECTION,
        note="Full observed history can choose a future research rule only; it does not alter prior fold decisions or validate this candidate",
        variant=next((v.to_dict() for v in pf_indicators.variants() if v.name == next_choice["selected_variant"]), None),
        protocol_sha256=sha256(output / "protocol.json"), execution_authority=False, default_profile_promoted=False))
    status = "RETROSPECTIVE_PF2_AND_ROBUSTNESS_PASS_FRESH_VALIDATION_REQUIRED" if assessment["retrospective_target_sample_and_robustness_pass"] else (
        "RETROSPECTIVE_PF2_SAMPLE_PASS_ROBUSTNESS_NOT_PASSED" if assessment["retrospective_target_and_sample_pass"] else "PF2_TARGET_NOT_ESTABLISHED")
    summary = dict(status=status, requested_sessions=len(days), evaluated_sessions=len(evaluated),
        slot_selection_policy=selection.UNLIMITED_SELECTION,
        forward_evaluated_sessions=len(forward_days), unknown_selected_paths=len(unknown),
        fixed_variants=len(selected), walk_forward_assessment=assessment, next_research_choice=next_choice,
        source_study=str(source.resolve()), output=str(output.resolve()),
        primary_roundtrip_bps=PRIMARY_ROUNDTRIP_BPS, source_protocol_sha256=sha256(output / "protocol.json"),
        fresh_holdout=False, execution_authority=False, default_profile_promoted=False,
        selection_uses_later_fold_outcomes=False, historical_design_informed_by_prior_studies=True)
    report(output, summary, comparison, decisions)
    write_json(output / "summary.json", summary)
    print(json.dumps(dict(status=status, assessment=assessment), indent=2), flush=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=data.DEFAULT_ROOT)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--features-only", action="store_true")
    args = parser.parse_args()
    run(args.root, args.source, args.output, features_only=args.features_only)


if __name__ == "__main__":
    main()
