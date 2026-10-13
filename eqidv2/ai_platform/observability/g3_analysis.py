"""Read-only G3 analysis of promoted, hash-verified canonical replay packages.

No donor G trade/model output or live execution module is imported. Statistical
buckets below reuse pure descriptive functions; they never drive entry orders.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

from research.g3_backtest_profile import contract as backtest_contract
IST = backtest_contract.IST
STRATEGY_VERSION = backtest_contract.STRATEGY_VERSION
strategy_fingerprint = backtest_contract.strategy_fingerprint
def supported_identity(version, fingerprint, *, allow_legacy=False):
    from fno_v13_v10_g_3_contract import supported_identity as production_identity
    return (backtest_contract.supported_identity(version, fingerprint, allow_legacy=allow_legacy)
            or production_identity(version, fingerprint, allow_legacy=allow_legacy))
from ai_platform.observability.strategy_research import (
    classify_regime, performance, point_in_time_audit,
)


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verified_package(root: Path, *, history: bool = True, allow_legacy: bool = False,
                     archived_version: str | None = None) -> tuple[Path, dict, dict[str, Path]]:
    """Verify current evidence, or explicitly inspect a retained prior revision.

    Legacy evidence is readable only with opt-in and is never treated as active
    strategy evidence by research/execution consumers using this default API.
    """
    root = Path(root).resolve()
    role = "cumulative_history" if history else "production_daily"
    pointer_root = root / "latest"
    if archived_version is not None:
        if not archived_version or Path(archived_version).name != archived_version or archived_version in (".", ".."):
            raise ValueError("Invalid archived strategy version")
        pointer_root = pointer_root / "archived" / archived_version
    pointer_path = pointer_root / ("current_history.json" if history else "current_run.json")
    pointer = json.loads(pointer_path.read_text(encoding="utf-8-sig"))
    run = (root / pointer["run_dir"]).resolve()
    if Path(pointer["run_dir"]).is_absolute() or not run.is_relative_to(root):
        raise ValueError("G3 pointer escapes its source root")
    manifest_path = run / "run_manifest.json"
    if _sha(manifest_path) != pointer["manifest_sha256"]:
        raise ValueError("G3 manifest hash mismatch")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    expected_tree = "history" if history else "production"
    if (manifest.get("schema_version") != "fno_g3_production_run_v1"
            or manifest.get("execution_authority") is not False
            or pointer.get("session_date") != manifest.get("session_date")
            or run.relative_to(root).parts[:2] != (expected_tree, "runs")):
        raise ValueError("G3 schema, authority, date or publication tree mismatch")
    for record in (pointer, manifest):
        if (record.get("role") != role or not supported_identity(record.get("strategy_version"),
                record.get("strategy_fingerprint"), allow_legacy=allow_legacy)):
            raise ValueError("G3 package identity/role mismatch")
    if any(pointer.get(key) != manifest.get(key) for key in ("strategy_version", "strategy_fingerprint")):
        raise ValueError("G3 pointer and package strategy revision differ")
    if archived_version is not None and manifest.get("strategy_version") != archived_version:
        raise ValueError("G3 archived pointer strategy revision differs")
    if (manifest.get("complete") is not True or manifest.get("state") not in
            {"SUCCESS", "SUCCESS_WITH_EXCLUDED_SESSIONS"}):
        raise ValueError("G3 package is incomplete")
    if manifest.get("state") == "SUCCESS_WITH_EXCLUDED_SESSIONS" and not manifest.get("coverage", {}).get("excluded_sessions"):
        raise ValueError("G3 excluded sessions are undocumented")
    paths = {}
    for name, artifact in manifest["artifacts"].items():
        path = (run / artifact["path"]).resolve()
        if Path(artifact["path"]).is_absolute() or not path.is_relative_to(run) or _sha(path) != artifact["sha256"]:
            raise ValueError(f"G3 artifact integrity failed: {name}")
        paths[name] = path
    if not {"portfolio_trades", "daily_results", "source_manifest"}.issubset(paths):
        raise ValueError("G3 canonical evidence is missing")
    return run, manifest, paths


def _rows(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def _table(rows: list[dict], columns: list[str]) -> str:
    def cell(value):
        if value is None:
            return "UNAVAILABLE"
        if isinstance(value, float):
            return f"{value:,.2f}" if math.isfinite(value) else "UNAVAILABLE"
        return str(value).replace("|", "/").replace("\n", " ")
    return "\n".join(["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
                     + ["| " + " | ".join(cell(row.get(key)) for key in columns) + " |" for row in rows])


def _atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + f".{os.getpid()}.tmp")
    temp.write_text(content, encoding="utf-8")
    os.replace(temp, path)


def read_g3_execution_diagnostics(root: Path | None, *, source_run: Path, source_sha256: str,
                                  version=None, fingerprint=None) -> dict:
    """Consume one committed G3 diagnostic package; never borrow G calibration."""
    pending = dict(state="AWAITING_G3_EXECUTION_DIAGNOSTICS", verified=False,
                   calibrated=False, execution_authority=False, safe_for_live_selection=False,
                   reason="No matching G3 execution diagnostic package is available.",
                   paper_terminal_orders=None, paper_fills=None, paper_session_dates=[],
                   path_stress_state="UNAVAILABLE")
    if root is None:
        return pending
    root = Path(root).resolve()
    latest = root / "latest/manifest.json"
    if not latest.is_file():
        return pending
    try:
        if latest.stat().st_size > 16 * 1024 * 1024:
            raise ValueError("Execution manifest exceeds the read limit")
        raw = latest.read_bytes()
        value = json.loads(raw)
        run = Path(value["run_dir"]).resolve()
        if not run.is_relative_to(root / "runs") or run == root / "runs":
            raise ValueError("Execution run leaves its immutable publication tree")
        if _sha(run / "manifest.json") != hashlib.sha256(raw).hexdigest():
            raise ValueError("Execution latest and immutable manifests differ")
        if (value.get("schema_version") != "eqidv2.v13_v10_g_3.execution_research.v1"
                or value.get("strategy_profile") != "V13_V10_G_3"
                or not supported_identity(value.get("strategy_version"), value.get("strategy_fingerprint"))
                or version is not None and value.get("strategy_version") != version
                or fingerprint is not None and value.get("strategy_fingerprint") != fingerprint
                or value.get("mode") != "READ_ONLY_EXECUTION_DIAGNOSTICS"
                or value.get("source_role") != "cumulative_history"
                or any(value.get(key) is not False for key in
                       ("execution_authority", "broker_access", "safe_for_live_selection", "live_configuration_changed", "calibrated"))):
            raise ValueError("Execution diagnostic identity or authority contract differs")
        name = "latest_fno_v13_v10_g_3_execution_realism.md"
        reports = value.get("reports", {})
        if value.get("report_filename") != name or set(reports) != {name}:
            raise ValueError("Execution report contract differs")
        artifact = reports[name]
        path = (run / artifact["path"]).resolve()
        if (Path(artifact["path"]).is_absolute() or not path.is_relative_to(run)
                or path.stat().st_size > 2 * 1024 * 1024
                or _sha(path) != artifact["sha256"] or _sha(root / "latest" / name) != artifact["sha256"]):
            raise ValueError("Execution report integrity failed")
        paper, stress = value["paper_calibration"], value["path_stress"]
        terminal, fills = paper["terminal_orders"], paper["fills"]
        dates, fill_ids, observed = paper["session_dates"], paper["filled_signal_ids"], paper["source_artifacts"]
        if (type(terminal) is not int or type(fills) is not int or not 0 <= fills <= terminal
                or not isinstance(dates, list) or dates != sorted(set(dates))
                or not isinstance(fill_ids, list) or len(set(fill_ids)) != fills
                or len(fill_ids) != fills or not isinstance(observed, dict) or len(observed) != terminal
                or paper.get("calibrated") is not False or stress.get("calibrated") is not False
                or stress.get("state") != "NOT_RUN" or value.get("scenarios") != []):
            raise ValueError("Execution calibration/count/path-stress contract differs")
        expected_state = "INSUFFICIENT_G3_PATH_STRESS_EVIDENCE" if fills else "AWAITING_G3_PAPER_CALIBRATION"
        if value.get("state") != expected_state:
            raise ValueError("Execution diagnostic claims unsupported readiness")
        if (value.get("source_manifest_sha256") != source_sha256
                or Path(value.get("source_run", "")).resolve() != source_run.resolve()):
            return {**pending, "state": "STALE_G3_EXECUTION_EVIDENCE",
                    "reason": "Execution diagnostics belong to a different historical generation; regenerate them for the current history."}
        return {**pending, "state": expected_state, "verified": True,
                "reason": "Hash-verified G3 PAPER observations; complete prospective sessions and path-stress calibration remain unproven.",
                "paper_terminal_orders": terminal, "paper_fills": fills, "paper_session_dates": dates,
                "path_stress_state": stress["state"], "manifest_sha256": hashlib.sha256(raw).hexdigest(),
                "report_sha256": artifact["sha256"], "source_manifest_sha256": source_sha256,
                "generated_at_ist": value.get("generated_at_ist")}
    except (OSError, UnicodeError, ValueError, TypeError, KeyError, AttributeError):
        return {**pending, "state": "INVALID_G3_EXECUTION_EVIDENCE",
                "reason": "G3 execution manifest/report integrity, identity or calibration contract failed. Regenerate diagnostics; no counts were accepted."}


def generate_g3_analysis(*, source_root: Path, output_root: Path,
                         execution_research_root: Path | None = None) -> dict:
    run, manifest, paths = verified_package(source_root)
    version, fingerprint = manifest["strategy_version"], manifest["strategy_fingerprint"]
    rows, daily = _rows(paths["portfolio_trades"]), _rows(paths["daily_results"])
    for row in rows:
        if row.get("strategy_version") != version or row.get("strategy_fingerprint") != fingerprint:
            raise ValueError("G3 ledger row identity mismatch")
        row.update(classify_regime(row))
    baseline = performance(rows)
    # Reconcile the independent report against the publisher, never silently
    # promote a merely plausible chart from the wrong portfolio column.
    metrics = manifest["metrics"]
    declared_net = next((metrics[key] for key in ("net_pnl", "net_profit_rupees", "net_pnl_rupees") if key in metrics), None)
    if declared_net is None or not math.isclose(baseline["net_profit_rupees"], float(declared_net), abs_tol=.02):
        raise ValueError("G3 analysis P&L does not reconcile with its manifest")
    if baseline["executed_trades"] != int(metrics.get("trades", metrics.get("fills", -1))):
        raise ValueError("G3 analysis executed count does not reconcile")
    groups = {}
    for key in ("side", "setup_id", "tradingsymbol", "market_regime", "side_alignment", "volatility_regime", "oi_regime"):
        bucket = defaultdict(list)
        for row in rows:
            bucket[row.get(key) or "UNKNOWN"].append(row)
        groups[key] = [{key: name, **performance(group)} for name, group in sorted(bucket.items())]
    audit = point_in_time_audit(rows)
    exclusions = manifest.get("coverage", {}).get("excluded_sessions", [])
    execution = read_g3_execution_diagnostics(execution_research_root, source_run=run,
        source_sha256=_sha(run / "run_manifest.json"), version=version, fingerprint=fingerprint)
    execution_section = ("\n\n## G3 execution validation gate\n\n"
        f"- Diagnostic state: **{execution['state']}**\n"
        f"- Current-history package verified: `{str(execution['verified']).lower()}`\n"
        f"- Calibration established: `false`\n- Path-stress state: `{execution['path_stress_state']}`\n\n"
        + _table([{"terminal_orders": execution['paper_terminal_orders'], "fills": execution['paper_fills'],
                   "observed_dates": len(execution['paper_session_dates']) if execution['verified'] else None}],
                 ["terminal_orders", "fills", "observed_dates"])
        + "\n\n" + execution['reason']
        + " Counts describe observed terminal G3 PAPER states, not reconciled complete sessions. "
          "Quote-distance observations do not establish broker slippage or LIVE readiness.\n")
    generated = datetime.now(IST).isoformat()
    header = (f"- Strategy: `{version}`\n- Fingerprint: `{fingerprint}`\n"
              f"- Generated: `{generated}`\n- Source run: `{run}`\n"
              f"- Through: `{manifest.get('session_date')}`\n- Eligible sessions: `{len(daily)}`\n"
              f"- Excluded source sessions: `{len(exclusions)}`\n- Execution authority: `false`\n\n")
    reports = {}
    def report(key, title, body, state="READY"):
        reports[key] = f"# V13-V10-G-3 {title}\n\n- State: **{state}**\n" + header + body + "\n"
    columns = ["selected_orders", "executed_trades", "wins", "losses", "net_profit_rupees", "profit_factor"]
    report("research_baseline", "Baseline", _table([baseline], columns) +
           "\n\nHistorical OHLC replay; cost estimate is 5 bps round trip. INR 100,000 capital per entry, 5x exposure and INR 1,000,000 portfolio capital. This is not a broker fill simulation. Daily-close drawdown is in the canonical daily report; row-order trade drawdown is not interchangeable.")
    report("research_data_quality", "Data Quality", json.dumps(manifest.get("coverage", {}), indent=2, default=str).join(["```json\n", "\n```\n"]) +
           "\nAll source artifacts were hash verified. Excluded sessions are not treated as zero-trade sessions.")
    report("research_dataset", "Dataset", _table([{"artifact": name, "sha256": value["sha256"]} for name, value in manifest["artifacts"].items()], ["artifact", "sha256"]) +
           "\n\nSelection consumes pre-quota features and minute paths. Completed G trades are not inputs. Reused research history is not new out-of-sample evidence.")
    attribution = "\n\n".join(f"## {key}\n\n" + _table(groups[key], [key, *columns]) for key in ("side", "setup_id", "tradingsymbol"))
    report("research_attribution", "Attribution", attribution)
    report("observability_pnl_attribution", "P&L Attribution", attribution)
    regimes = "\n\n".join(f"## {key}\n\n" + _table(groups[key], [key, *columns]) for key in ("market_regime", "side_alignment", "volatility_regime", "oi_regime"))
    for key in ("research_regimes", "observability_market_regime", "observability_regime_profitability"):
        report(key, "Descriptive Regime Analysis", "Fixed decision-time buckets; unknown inputs stay UNKNOWN. No regime bucket changes G3 decisions.\n\n" + regimes)
    audit_rows = _rows(paths["selection_audit"]) if "selection_audit" in paths else []
    reasons = Counter(str(row.get("v9_decision") or row.get("v9_rejection_reason") or row.get("rejection_reason") or row.get("reason") or "RECORDED_WITHOUT_REASON") for row in audit_rows)
    report("observability_selection_funnel", "Selection Funnel", f"- Audit rows: `{len(audit_rows)}`\n- Selected orders: `{baseline['selected_orders']}`\n- Filled orders: `{baseline['filled_orders']}`\n- Portfolio executions: `{baseline['executed_trades']}`\n\n" + _table([{"reason": k, "rows": v} for k, v in sorted(reasons.items())], ["reason", "rows"]))
    exits = Counter(row.get("exit_reason") or "UNKNOWN" for row in rows)
    report("observability_entry_execution", "Historical Entry and Exit Evidence", _table([{"exit_reason": k, "orders": v} for k, v in sorted(exits.items())], ["exit_reason", "orders"]) +
           f"\n\nPoint-in-time violations: `{audit['violation_count']}`. Missing timestamp counts: `{json.dumps(audit['missing_timestamps'], sort_keys=True)}`.\n\nG3 uses the setup inventory, targets and setup-specific confirmation thresholds recorded in this run's source contract. All use the exact next minute, staged 1.25% to 1.00% stop at 120 minutes, and 15:15 square-off. Runtime stop timing starts at actual fill; historical replay uses the accepted candle clock."
           + execution_section, execution["state"])
    report("research_walkforward", "Chronological Historical Review", "The accepted parameters are held fixed across these sessions. This daily review is descriptive; it is not a newly trained walk-forward model or new out-of-sample validation.\n\n" + _table(daily, [key for key in ("day", "trades", "wins", "losses", "net_pnl", "net_profit_rupees", "net_pnl_rupees") if daily and key in daily[0]]))
    pending = "A separate G3 prospective cohort must accumulate under this exact fingerprint. Prior G model, calibration, and live/PAPER evidence cannot establish G3 readiness. No simulated prospective observations are created."
    for key, title in (("research_predictions", "Prediction Readiness"), ("research_shadow", "Prospective Shadow"), ("observability_live_finalized_drift", "Live / Finalized Drift")):
        report(key, title, pending, "AWAITING_G3_PROSPECTIVE_EVIDENCE")
    report("research_improvements", "Validation Gates", "1. Resolve documented source exclusions using immutable dated inputs.\n2. Collect three successful G3 PAPER sessions and reconcile signal IDs, selections, fills, exits and costs.\n3. Refit and validate any G3 prediction model on a separately versioned dataset before displaying predictions.\n4. G3 LIVE uses a separate approved sizing policy: INR 10,000 capital per entry at 5x exposure, with whole-share quantity floored from INR 50,000 divided by the recorded sizing price. Verify its execution-policy fingerprint and activation controls separately. These historical results and PAPER sizing remain unchanged; this report grants no LIVE authority.\n\n" + pending + execution_section, execution["state"])
    output_root = Path(output_root).resolve()
    package_id = datetime.now(IST).strftime("%Y%m%dT%H%M%S%f")
    out = output_root / "runs" / package_id
    out.mkdir(parents=True, exist_ok=False)
    report_files = {}
    for key, body in reports.items():
        name = f"latest_fno_v13_v10_g_3_{key}.md"
        _atomic(out / name, body)
        report_files[name] = {"path": name, "sha256": _sha(out / name)}
    payload = dict(schema_version="g3_read_only_analysis_v1", state="READY", generated_at=generated,
                   strategy_version=version, strategy_fingerprint=fingerprint,
                   source_run=str(run), source_manifest_sha256=_sha(run / "run_manifest.json"),
                   execution_authority=False, baseline=baseline, groups=groups, point_in_time_audit=audit,
                   excluded_sessions=exclusions, execution_diagnostics=execution,
                   reports=report_files, run_dir=str(out))
    _atomic(out / "analysis_manifest.json", json.dumps(payload, indent=2, allow_nan=False))
    for name in report_files:
        _atomic(output_root / "latest" / name, (out / name).read_text(encoding="utf-8"))
    _atomic(output_root / "latest" / "analysis_manifest.json", json.dumps(payload, indent=2, allow_nan=False))
    return payload
