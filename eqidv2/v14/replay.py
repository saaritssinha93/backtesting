"""Replay one verified frozen V14 replacement; never select or retune a variant."""
from __future__ import annotations

from datetime import date, datetime, timezone
import html
import json
from pathlib import Path
import shutil

import pandas as pd

from . import baseline, data, pullbacks, selection
from .features import Variant, clean_minutes, participation_mask
from .monitoring import attach_monitoring
from .path_evidence import assess, EVIDENCE_VERSION

STRICT_PROTOCOL_VERSION = "V14_CASH_PARTICIPATION_STUDY_V1"
PREFIX_PROTOCOL_VERSION = "V14_CASH_PARTICIPATION_STUDY_V2"
STRICT_PATH_POLICY = "V14_FULL_SESSION_PATH_V1"


def _read(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object: {path}")
    return value


def _verified_profile(path: Path) -> tuple[dict, dict, dict]:
    from . import research
    profile = _read(path)
    frozen = baseline.frozen_config()
    baseline.verify_snapshot()
    if profile.get("version") != "V14" or profile.get("baseline") != frozen["version"]:
        raise ValueError("Selected profile does not use the sealed accepted G-3 baseline")
    if profile.get("execution_authority") is not False or profile.get("state") != "FROZEN_RESEARCH_CANDIDATE":
        raise ValueError("Expected a frozen V14 research profile without execution authority")
    for key in ("capital_per_entry_rupees", "portfolio_capital_rupees", "leverage_factor", "cost_bps"):
        if profile.get(key) != frozen[key]:
            raise ValueError(f"Frozen profile changed baseline sizing or costs: {key}")
    if profile.get("common_feature_coverage") != research.COMMON_FEATURES:
        raise ValueError("Frozen profile candidate availability differs from the current feature contract")
    source = Path(profile["output_dir"])
    source_profile = source / "selected_profile.json"
    source_protocol = source / "protocol.json"
    if not (source / "summary.json").is_file():
        raise ValueError("The profile's selecting study is not complete")
    if _read(source_profile) != profile:
        raise ValueError("Selected profile differs from its completed selecting study")
    if research.sha256(source_protocol) != profile["protocol_sha256"]:
        raise ValueError("Selecting study protocol hash differs from the frozen profile")
    protocol = _read(source_protocol)
    spec = protocol["protocol"]
    profile_selection = profile.get("slot_selection_policy")
    protocol_selection = spec.get("slot_selection_policy")
    profile_has_selection = "slot_selection_policy" in profile
    protocol_has_selection = "slot_selection_policy" in spec
    if not profile_has_selection and not protocol_has_selection:
        # Existing artifacts predate the explicit field and were capped G3.
        # The current V14 default must never change that historical meaning.
        slot_policy = selection.FROZEN_G3_SELECTION
    elif profile_has_selection != protocol_has_selection or profile_selection != protocol_selection:
        raise ValueError("Profile and source protocol must agree on slot selection policy")
    elif profile_selection not in {selection.UNLIMITED_SELECTION, selection.FROZEN_G3_SELECTION}:
        raise ValueError("Unsupported frozen slot selection policy")
    else:
        slot_policy = profile_selection
    protocol_version = spec.get("version")
    if protocol_version not in {STRICT_PROTOCOL_VERSION, PREFIX_PROTOCOL_VERSION}:
        raise ValueError("Unsupported selecting-study protocol version")
    if protocol_version == PREFIX_PROTOCOL_VERSION:
        if spec.get("path_policy") != EVIDENCE_VERSION or profile.get("path_policy") != EVIDENCE_VERSION:
            raise ValueError("V2 profile and source protocol must agree on observed-prefix path policy")
        path_policy = EVIDENCE_VERSION
    else:
        if spec.get("path_policy") not in (None, STRICT_PATH_POLICY) or profile.get("path_policy") not in (None, STRICT_PATH_POLICY):
            raise ValueError("A V1 selecting study may only replay its strict full-session path policy")
        path_policy = STRICT_PATH_POLICY
    if profile["selected_variant"] not in spec["variants"]:
        raise ValueError("Selected variant was not part of the recorded selecting-study protocol")
    if profile.get("selection", {}).get("name") != profile["selected_variant"]["name"]:
        raise ValueError("Profile selection and variant names differ")
    code_path = source / "executed_source_hashes.json"
    code = _read(code_path) if code_path.exists() else protocol.get("source_sha256", {})
    bound_code = ["features.py", "baseline.py"]
    if slot_policy == selection.UNLIMITED_SELECTION:
        bound_code.append("selection.py")
    if path_policy == EVIDENCE_VERSION:
        bound_code.extend(["path_evidence.py", "data.py", "index_data.py"])
    for name in bound_code:
        expected = code.get(name)
        if not expected or research.sha256(Path(__file__).with_name(name)) != expected:
            raise ValueError(f"Frozen profile feature/execution adapter changed: {name}")
    lineage = dict(profile_path=str(path.resolve()), profile_sha256=research.sha256(path),
        source_study=str(source.resolve()), source_study_profile_sha256=research.sha256(source_profile),
        source_protocol_sha256=research.sha256(source_protocol),
        source_data_manifest_sha256=profile["data_manifest_sha256"],
        baseline_snapshot_sha256=research.sha256(baseline.SNAPSHOT / "snapshot_manifest.json"),
        path_policy=path_policy, slot_selection_policy=slot_policy, source_protocol_version=protocol_version,
        checked_code_sha256={name: code[name] for name in bound_code})
    return profile, protocol, lineage


def _report(output: Path, summary: dict, daily: pd.DataFrame, monitoring: pd.DataFrame) -> None:
    selected = html.escape(summary["selected_variant"]["name"])
    source = html.escape(summary["lineage"]["source_study"])
    reasons = html.escape(", ".join(summary["excluded_days"]) or "None")
    markup = f'''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>V14 frozen-profile replay</title><style>body{{background:#101827;color:#e5edf7;font:16px/1.6 system-ui;margin:0}}main{{max-width:1200px;margin:auto;padding:32px}}h1{{font-size:34px}}a{{color:#66e3c4}}table{{border-collapse:collapse;width:100%;font-size:14px}}th,td{{border-bottom:1px solid #354257;padding:8px;text-align:right}}.scroll{{overflow:auto}}.notice{{background:#202d42;padding:18px;border-radius:10px}}</style>
<main><p>V14 · completed-session research replay</p><h1>Frozen profile: {selected}</h1>
<p class="notice">This replay uses the already selected profile without comparing or retuning alternatives. No trade orders were sent.</p>
<p>Recorded slot selection policy: {html.escape(summary['slot_selection_policy'])}. Historical capped profiles retain their original quotas; new uncapped profiles select every eligible candidate. Existing portfolio capital constraints still govern modeled executions.</p>
<p>Evaluated {summary['evaluated_sessions']} of {summary['requested_sessions']} requested sessions; {summary['metrics']['trades']} modeled executions. Net P&amp;L: ₹{summary['metrics']['net_pnl']:,.2f}. Inherited flat 5bps round-trip cost and modeled 5× exposure are unchanged.</p>
<p>Source selecting study: {source}. Profile qualification: {html.escape(str(summary['profile_qualified']))}. Historical replay does not establish executable profitability or current MIS margin availability.</p>
<h2>Daywise results</h2><div class="scroll">{daily.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')}</div>
<h2>Coverage</h2><p>Execution evidence policy: {html.escape(summary['path_policy'])}. Excluded dates: {reasons}. A selected outcome that cannot be proven under that frozen policy excludes its whole date. Another stock is not substituted.</p>
<h2>Observational pullbacks</h2><p>The copied deterministic G-3 rules are uncalibrated for non-F&amp;O stocks. Warnings do not change trade P&amp;L. Future outcome labels are separate; unknown intrabar ordering remains unresolved.</p><div class="scroll">{monitoring.to_html(index=False,border=0,float_format=lambda x:f'{x:.3f}')}</div>
<p><a href="daily_results.csv">Daily results</a> · <a href="trades.csv">Trades</a> · <a href="selected_orders.csv">Selected orders</a> · <a href="selected_profile.json">Frozen profile copy</a> · <a href="evaluation_coverage.json">Coverage</a> · <a href="pullback_report.html">Pullback observations</a> · <a href="replay_lineage.json">Lineage</a></p></main></html>'''
    (output / "report.html").write_text(markup, encoding="utf-8")


def replay_selected(root: Path, output: Path, *, start_date: str, end_date: str,
                    profile_path: Path | None = None, fetch_missing: bool = False,
                    warmup_sessions: int = 30) -> dict:
    """Stage and replay exactly one hash-verified selected profile."""
    from . import research
    start_date, end_date = date.fromisoformat(start_date).isoformat(), date.fromisoformat(end_date).isoformat()
    if start_date > end_date or warmup_sessions < 0:
        raise ValueError("Invalid replay date range or warmup length")
    root, output = Path(root), Path(output)
    if (output / "summary.json").exists():
        raise FileExistsError("Completed replay is immutable; use a new output directory")
    profile_path = Path(profile_path) if profile_path is not None else root / "selected_profile.json"
    profile, source_protocol, lineage = _verified_profile(profile_path)
    allow_prefix = lineage["path_policy"] == EVIDENCE_VERSION
    output.mkdir(parents=True, exist_ok=True)
    # Persist lineage before mutable data staging. The completed selecting run
    # and root selected_profile.json are never changed by this function.
    shutil.copyfile(profile_path, output / "selected_profile.json")
    research.write_json(output / "source_protocol.json", source_protocol)
    research.write_json(output / "replay_lineage.json", lineage)
    for name in ("manifest.json", "universe_manifest.json"):
        previous = root / "data" / name
        if previous.is_file():
            shutil.copyfile(previous, output / f"prior_data_{name}")
    research.write_json(output / "replay_protocol.json", dict(
        version="V14_FROZEN_PROFILE_REPLAY_V1", recorded_at=datetime.now(timezone.utc).isoformat(),
        start_date=start_date, end_date=end_date, warmup_sessions=warmup_sessions,
        fetch_missing=fetch_missing, selected_variant=profile["selected_variant"], retuned=False,
        path_policy=lineage["path_policy"], slot_selection_policy=lineage["slot_selection_policy"],
        minimum_session_symbol_presence=source_protocol["protocol"]["minimum_session_symbol_presence"],
        missing_path_policy="SELECT_ONCE_NO_REFILL_EXCLUDE_WHOLE_DATE_IF_SELECTED_OUTCOME_UNRESOLVED",
        execution_authority=False))
    manifest_path = data.prepare_data(start_date, end_date, warmup_sessions=warmup_sessions,
                                     fetch_missing=fetch_missing, root=root)
    if fetch_missing:
        from .index_fetch import refresh_nifty_context
        refresh_nifty_context(_read(manifest_path)["study_sessions"], root)
    shutil.copyfile(manifest_path, output / "input_data_manifest.json")
    shutil.copyfile(root / "data/universe_manifest.json", output / "input_universe_manifest.json")
    pool, manifest, days = research.build_dataset(root, output)
    if any(day < start_date or day > end_date for day in days):
        raise ValueError("Prepared research sessions fall outside the requested replay interval")
    variant = Variant(**profile["selected_variant"])
    common = pool.loc[pool.common_feature_coverage].copy()
    orders = selection.select_orders(common, lambda rows, setup: participation_mask(rows, variant),
                                     selection_policy=lineage["slot_selection_policy"])
    orders.to_csv(output / "selected_orders.csv", index=False)
    paths, unknown, evidence = {}, [], {}
    files = {record["symbol"]: record for record in manifest["files"]}
    for symbol, group in orders.groupby("tradingsymbol", sort=True):
        source = root / "data/1m" / f"{symbol}.parquet"
        if research.sha256(source) != files[symbol]["sha256"]:
            raise RuntimeError(f"Cash data changed after selected-profile feature calculation: {symbol}")
        minute = clean_minutes(data.load_symbol_1m(str(symbol), root=root))
        if research.sha256(source) != files[symbol]["sha256"]:
            raise RuntimeError(f"Cash data changed while loading selected-profile execution: {symbol}")
        for row in group.itertuples(index=False):
            path = baseline.native_paths(minute, row.confirmation_ts, row.day)
            if allow_prefix:
                proof = assess(row, path)
                resolved, reason = proof["resolved"], proof["reason"]
                evidence[str(row.sid)] = dict(sid=int(row.sid), day=str(row.day), symbol=str(symbol),
                    setup_id=row.setup_id, resolved=resolved, reason=reason, **proof["evidence"])
                proven_path = proof["path"]
            else:
                resolved = research.path_is_complete(path, row.confirmation_ts)
                reason = "FULL_SESSION_OBSERVED" if resolved else "INCOMPLETE_SELECTED_EXECUTION_PATH_NO_REFILL"
                evidence[str(row.sid)] = dict(sid=int(row.sid), day=str(row.day), symbol=str(symbol),
                    setup_id=row.setup_id, version=STRICT_PATH_POLICY, resolved=resolved, reason=reason,
                    input_minutes=len(path["timestamp_ns"]), selected_before_assessment=True,
                    full_session_observed=resolved, synthetic_bars=False)
                proven_path = path
            if resolved:
                paths[int(row.sid)] = proven_path
            else:
                unknown.append(dict(sid=int(row.sid), day=str(row.day), symbol=str(symbol), setup_id=row.setup_id,
                    path_minutes=len(path["timestamp_ns"]), reason=reason))
    research.write_json(output / "path_evidence.json", dict(path_policy=lineage["path_policy"], orders=evidence))
    audit_columns = ["sid", "day", "symbol", "setup_id", "resolved", "reason", "input_minutes",
                     "continuous_valid_minutes", "required_minutes", "full_session_observed", "proof_available_ts", "prefix_sha256"]
    pd.DataFrame([{key: record.get(key) for key in audit_columns} for record in evidence.values()],
                 columns=audit_columns).to_csv(output / "path_evidence.csv", index=False)
    unknown_frame = pd.DataFrame(unknown, columns=["sid", "day", "symbol", "setup_id", "path_minutes", "reason"])
    unknown_frame.to_csv(output / "unknown_outcomes.csv", index=False)
    index = pd.read_csv(output / "nifty_context.csv")
    missing_index = set(days) - set(index.dropna(subset=["nifty_first_bar_return_pct"]).day.astype(str))
    coverage = pd.read_csv(output / "session_data_coverage.csv")
    required_presence = source_protocol["protocol"]["minimum_session_symbol_presence"]
    unavailable = set(coverage.loc[coverage.presence_fraction.lt(required_presence), "day"].astype(str))
    excluded = set(unknown_frame.day.astype(str)) | missing_index | unavailable
    evaluated_days = [day for day in days if day not in excluded]
    research.write_json(output / "evaluation_coverage.json", dict(requested_days=days,
        evaluated_days=evaluated_days, excluded_days=sorted(excluded), missing_index_days=sorted(missing_index),
        insufficient_cash_coverage_days=sorted(unavailable), unknown_selected_paths=len(unknown),
        raw_strict_candidates=len(pool), common_candidates=len(common), selected_orders=len(orders),
        path_policy=lineage["path_policy"], slot_selection_policy=lineage["slot_selection_policy"],
        policy="No refill; exclude whole date when the frozen profile has an unresolved selected outcome"))
    evaluable = orders.loc[orders.day.astype(str).isin(evaluated_days)].copy()
    ledger = baseline.simulate(evaluable, paths, allow_observed_prefix=allow_prefix)
    ledger["variant"] = variant.name
    ledger["profile_reselected"] = False
    ledger.to_csv(output / "trades.csv", index=False)
    attach_monitoring(root, output)
    monitoring = pd.read_csv(output / "pullback_summary.csv")
    daily = research.daily_metrics(ledger, evaluated_days)
    daily["split"] = "FROZEN_PROFILE_REPLAY"
    daily.to_csv(output / "daily_results.csv", index=False)
    metrics = research.metric(ledger, evaluated_days)
    summary = dict(status="COMPLETE_FROZEN_PROFILE_REPLAY" if evaluated_days else "COMPLETE_REPLAY_NO_EVALUABLE_SESSIONS",
        version="V14", selected_variant=profile["selected_variant"], profile_qualified=profile["selection"].get("qualified", False),
        profile_reselected=False, requested_sessions=len(days), evaluated_sessions=len(evaluated_days),
        path_policy=lineage["path_policy"], slot_selection_policy=lineage["slot_selection_policy"],
        excluded_days=sorted(excluded), selected_orders=len(orders), metrics=metrics, lineage=lineage,
        current_data_manifest_sha256=research.sha256(manifest_path),
        source_profile_sha256=lineage["profile_sha256"], monitor_version=pullbacks.MONITOR_VERSION,
        limitations=manifest.get("limitations", []), execution_authority=False)
    _report(output, summary, daily, monitoring)
    research.write_json(output / "summary.json", summary)
    research.write_json(root / "latest.json", dict(run=str(output.resolve()), report=str((output / "report.html").resolve()),
        selected_variant=variant.name, status=summary["status"], mode="FROZEN_PROFILE_REPLAY",
        completed_at=datetime.now(timezone.utc).isoformat()))
    print(f"REPLAY {variant.name} sessions={len(evaluated_days)}/{len(days)} net={metrics['net_pnl']:.2f}", flush=True)
    return summary
