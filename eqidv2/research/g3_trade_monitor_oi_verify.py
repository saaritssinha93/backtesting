"""Reconcile a complete OI study from frozen inputs after workspace source drift.

No resampling, threshold changes, raw-data refresh, or outcome selection occurs.
The captured historical verifier is loaded explicitly. All other used code
must still match the run's recorded hashes. Failed checks never seal a run.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import importlib.util
import json
from pathlib import Path
import shutil
import sys

import numpy as np
import pandas as pd

from research import g3_trade_monitor_oi_research as study
from research import g3_trade_monitor_oi_rules as rules
from research import g3_trade_monitor_parameter_search as search
from research import g3_trade_monitor_backtest as fixed
from research import g3_trade_monitor_robustness as robust
from research.g3_trade_monitor_risk import risk_diagnostics


def _read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def _compare_records(actual, expected, keys):
    left, right = pd.DataFrame(actual).set_index(keys).sort_index(), pd.DataFrame(expected).set_index(keys).sort_index()
    pd.testing.assert_frame_equal(left[right.columns], right, check_dtype=False,
        check_exact=False, atol=1e-6, rtol=0)


def _eligible(frame):
    # Exact necessary conditions only; full frozen rules still validate each
    # retained row. This avoids materializing 73,264 dictionaries per arm.
    return frame.loc[(frame.completed_bar_count >= 6) & (frame.consecutive_adverse_closes >= 3)
        & (frame.momentum_5m_pct <= -.15) & (frame.signed_move_pct <= -.30)].copy()


def verify(directory):
    global study, rules, search, fixed, robust, risk_diagnostics
    directory = Path(directory).resolve()
    if (directory / "manifest.json").exists():
        raise ValueError("Refuse to replace an already sealed run")
    provenance = _read(directory / "source_provenance.json")
    repository = study.REPO
    isolated_names = ["research.archived_g3_v7.contract", "research.archived_g3_v9.contract",
        "research.g3_target_cutoff_backtest", "research.g3_trade_monitor_backtest",
        "research.g3_trade_monitor_parameter_search", "research.g3_trade_monitor_risk",
        "research.g3_trade_monitor_robustness", "research.g3_trade_monitor_oi_data",
        "research.g3_trade_monitor_oi_rules", "research.g3_trade_monitor_oi_research"]
    isolated_paths = {repository / (name.replace(".", "/") + ".py") for name in isolated_names}
    drift = []
    for original, digest in provenance["code_sha256"].items():
        path = Path(original)
        captured = directory / "source_code" / path.relative_to(repository)
        if search.history.sha256(captured) != digest:
            raise ValueError("Captured source differs from recorded hash: " + str(captured))
        current = search.history.sha256(path)
        if current != digest:
            if path not in isolated_paths:
                raise ValueError("Unisolated code drift: " + str(path))
            drift.append(dict(path=str(path), captured_sha256=digest, workspace_sha256=current))
    old_globals = study, rules, search, fixed, robust, risk_diagnostics
    restored = []
    for name in isolated_names:
        frozen_path = directory / "source_code" / (name.replace(".", "/") + ".py")
        parent_name, attribute = name.rsplit(".", 1)
        parent = importlib.import_module(parent_name)
        restored.append((name, sys.modules.get(name), parent, attribute, getattr(parent, attribute, None)))
        spec = importlib.util.spec_from_file_location(name, frozen_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        setattr(parent, attribute, module)
        spec.loader.exec_module(module)
    study = sys.modules["research.g3_trade_monitor_oi_research"]
    rules = sys.modules["research.g3_trade_monitor_oi_rules"]
    search = sys.modules["research.g3_trade_monitor_parameter_search"]
    fixed = sys.modules["research.g3_trade_monitor_backtest"]
    robust = sys.modules["research.g3_trade_monitor_robustness"]
    risk_diagnostics = sys.modules["research.g3_trade_monitor_risk"].risk_diagnostics
    history = sys.modules["research.g3_target_cutoff_backtest"]
    from research.archived_g3_v7 import contract as production
    from research.archived_g3_v9 import contract
    history.production, history.contract = production, contract
    old = search.history, fixed.history
    try:
        search.history = fixed.history = history
        source = Path(provenance["source"])
        if history.sha256(source / "manifest.json") != provenance["source_manifest_sha256"]:
            raise ValueError("Original study source changed")
        original_summary, _ = search.verified_package(source)
        baseline = search.read_frozen_csv(directory / "ledgers/baseline.csv")
        pd.testing.assert_frame_equal(baseline, search.read_frozen_csv(source / "ledgers/baseline.csv"))
        protocol = _read(directory / "protocol.json")
        study.validate_protocol(protocol)
        if rules.candidate_registry() != _read(directory / "candidate_registry.json"):
            raise ValueError("Candidate registry changed")
        candidates = rules.candidate_registry()
        days = [date.fromisoformat(day) for day in original_summary["days"]]
        periods = search._periods(days)
        features = pd.read_parquet(directory / "decision_features_oi.parquet")
        selected = _eligible(features)
        book = search.FastBook(baseline)
        rows, daily_rows = [], []
        for period in ("calibration25", "validation10", "development35", "evaluation15", "all"):
            local_book, local_features = study._subset(baseline, selected, periods[period])
            rows.extend(study._rows(local_book, local_features, candidates, periods[period], period))
            if period in {"evaluation15", "all"}:
                rows.extend(study._rows(local_book, local_features, candidates, periods[period], period, "reallocated"))
        _compare_records(rows, pd.read_csv(directory / "policy_results.csv"), ["candidate_id", "period", "allocation"])
        original_lock = _read(directory / "selection_lock.json")
        new_lock = study._lock(rows, candidates)
        for key in new_lock:
            if key != "locked_at_utc" and new_lock[key] != original_lock[key]:
                raise ValueError("Development selection changed: " + key)
        for candidate in candidates:
            identifier = candidate["candidate_id"]
            events = rules.candidate_events(selected, candidate)
            if search.history.replay.json_ready(events) != _read(directory / "events" / (identifier + ".json")):
                raise ValueError("Frozen decisions do not reproduce events: " + identifier)
            for allocation in ("paired", "reallocated"):
                outcome = study.apply_events(book, events, allocation)
                ledger = study.canonical_ledger(baseline, outcome, events, identifier, allocation)
                saved = search.read_frozen_csv(directory / "ledgers" / f"{identifier}_{allocation}.csv")
                search._assert_fast_parity(book, outcome, saved, identifier + allocation)
                if risk_diagnostics(ledger) != risk_diagnostics(saved):
                    raise ValueError("Risk diagnostics changed")
                if allocation == "paired":
                    daily_rows.extend(robust._daily(book, outcome, days, identifier))
        _compare_records(daily_rows, pd.read_csv(directory / "daily_comparison.csv"), ["candidate_id", "day"])
        verification = directory / "verification"
        verification.mkdir(exist_ok=False)
        new_locks, rolling = study._rolling(baseline, selected, candidates, days, verification)
        old_locks = _read(directory / "rolling_locks.json")
        for after, before in zip(new_locks, old_locks, strict=True):
            after = search.history.replay.json_ready(after)
            if {k:v for k,v in after.items() if k != "locked_at_utc"} != {k:v for k,v in before.items() if k != "locked_at_utc"}:
                raise ValueError("Rolling selection changed")
        _compare_records(rolling, pd.read_csv(directory / "rolling_test_daily.csv"), ["day"])
        _compare_records(pd.read_csv(verification / "rolling_development_metrics.csv"),
            pd.read_csv(directory / "rolling_development_metrics.csv"), ["fold_id", "candidate_id", "period", "allocation"])
        sensitivity_rows = []
        for mode, lag in [("bar_end_plus_lag",0),("bar_end_plus_lag",1),("bar_end_plus_lag",5),("received_time",1)]:
            suffix = "oi" if (mode,lag) == ("bar_end_plus_lag",1) else f"{mode}_{lag}"
            cached = pd.read_parquet(directory / f"decision_features_{suffix}.parquet")
            for row in study._rows(book, _eligible(cached), candidates, days, "all"):
                sensitivity_rows.append({key:row[key] for key in ["candidate_id","monitor_exit_count","net_pnl_delta_vs_baseline_rs","stress_10bps_net_pnl_delta_vs_baseline_rs"]} |
                    dict(availability_mode=mode,lag_minutes=lag,valid_decision_rows=int(cached.oi_valid.sum())))
        _compare_records(sensitivity_rows, pd.read_csv(directory / "availability_sensitivity.csv"), ["candidate_id","availability_mode","lag_minutes"])
        # Captured historical verifier and its dependencies must remain exact.
        for original, digest in provenance["code_sha256"].items():
            captured = directory / "source_code" / Path(original).relative_to(repository)
            if history.sha256(captured) != digest:
                raise ValueError("Captured source changed during verification")
            if Path(original) not in isolated_paths and history.sha256(Path(original)) != digest:
                raise ValueError("Code changed during cached verification")
        evidence = dict(status="VERIFIED_FROM_FROZEN_INPUTS_AND_CAPTURED_HISTORICAL_CODE",
            verified_at_utc=datetime.now(timezone.utc).isoformat(), workspace_source_drift=drift,
            original_source_package_verified=True, policy_metric_rows=len(rows), candidate_ledgers_verified=16,
            primary_event_files_reproduced=8, availability_metric_rows=len(sensitivity_rows), rolling_folds_verified=len(new_locks),
            thresholds_changed=False, outcomes_reselected=False, raw_data_refreshed=False,
            execution_authority=False, verification_script_sha256=history.sha256(Path(__file__)))
        shutil.copy2(__file__, verification / Path(__file__).name)
        study._json(directory / "verification.json", evidence)
        summary = _read(directory / "summary.json")
        summary["completion_verification"] = evidence
        study._json(directory / "summary.json", summary)
        report_path = directory / "report.html"
        report = report_path.read_text(encoding="utf-8")
        report = report.replace("<h2>Evidence</h2>", "<h2>Evidence</h2><p>The workspace historical backtest source changed while this run was finishing. All results were independently reproduced from frozen decision inputs and the captured historical verifier before sealing. No rule or threshold changed. See verification.json for the source drift and reconciliation record.</p>")
        report_path.write_text(report, encoding="utf-8")
        files = {path.relative_to(directory).as_posix():history.sha256(path) for path in directory.rglob("*") if path.is_file()}
        study._json(directory / "manifest.json", dict(version=study.VERSION, execution_authority=False,
            configuration_promoted=False, verification_status=evidence["status"], files=files))
        if any(history.sha256(directory/name) != digest for name,digest in files.items()):
            raise ValueError("Final artifact hash verification failed")
        print(json.dumps(dict(output=str(directory),verified_artifacts=len(files),**evidence)),flush=True)
    finally:
        search.history, fixed.history = old
        study, rules, search, fixed, robust, risk_diagnostics = old_globals
        for name, prior, parent, attribute, prior_attribute in reversed(restored):
            if prior is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = prior
            if prior_attribute is None:
                delattr(parent, attribute)
            else:
                setattr(parent, attribute, prior_attribute)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    verify(parser.parse_args().directory)
