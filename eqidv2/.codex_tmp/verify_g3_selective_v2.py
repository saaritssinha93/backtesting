"""Independently audit selective G-3 artifacts without importing research code.

Writes independent_validation.json and refreshes artifact_hashes.json only.
The separate root pytest execution is recorded from explicitly supplied CLI
arguments or the root's test_validation.json, not rerun by this utility.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def assert_equal(actual, expected, context):
    if expected is None or (isinstance(expected, float) and math.isnan(expected)):
        assert pd.isna(actual), (context, actual, expected)
    elif isinstance(expected, (int, float, np.number)):
        assert math.isclose(float(actual), float(expected), rel_tol=1e-11, abs_tol=1e-11), (context, actual, expected)
    else:
        assert actual == expected, (context, actual, expected)


def ratio(a, b):
    return a / b if b else math.nan


def verify_hashes(expected):
    records = []
    for name, wanted in sorted(expected.items()):
        observed = digest(name)
        assert observed == wanted, f"SHA256 mismatch: {name}"
        records.append(dict(path=str(name), expected_sha256=wanted, observed_sha256=observed, matches=True))
    return records


def normalized(frame):
    result = frame.copy()
    result["decision_ts"] = pd.to_datetime(result.decision_ts, utc=True)
    if "event_ts" in result:
        result["event_ts"] = pd.to_datetime(result.event_ts, utc=True)
    return result.set_index(["trade_id", "horizon_minutes", "decision_ts"]).sort_index()


def run(out, root_tests=None, root_test_seconds=None):
    out = Path(out).resolve()
    summary = read_json(out / "summary.json")
    source_path = next(Path(p).parent for p in summary["input_hashes"] if Path(p).name == "predictions_1m.csv")
    provenance = read_json(source_path / "provenance.json")
    frozen = Path(provenance["frozen_path"])
    manifest = read_json(frozen / "manifest.json")
    frozen_provenance = read_json(frozen / "provenance.json")
    source_hashes = verify_hashes(summary["input_hashes"])
    frozen_hashes = verify_hashes({str(frozen / name): r["sha256"] for name, r in manifest["artifacts"].items()})
    code_hashes = verify_hashes({str(out / "source_snapshot" / name): value for name, value in summary["source_sha256"].items()})
    minute_expected = {r["minute_path"]: r["minute_sha256"] for r in frozen_provenance["source_inputs"]}
    minute_hashes = verify_hashes(minute_expected)
    historical_provenance = read_json(frozen / "source_study_provenance.json")
    snapshot_hashes = verify_hashes(historical_provenance["verified_snapshot_manifest_hashes"])
    assert digest(out / "locked_selection.json") == summary["selection_sha256"], "Frozen selection hash drift"

    dense = pd.read_csv(out / "predictions_1m.csv")
    source = pd.read_csv(source_path / "predictions_1m.csv")
    new_indexed, source_indexed = normalized(dense), normalized(source)
    assert new_indexed.index.equals(source_indexed.index), "Prediction keys differ from source"
    assert new_indexed.index.is_unique, "Duplicate prediction keys"
    label_columns = ["threshold_pct", "event", "event_ts", "event_source", "lead_minutes",
                     "observed_exposure_minutes", "max_future_adverse_pct", "early_exit", "censored",
                     "outcome_available", "unknown_reason", "full_horizon_eligible", "recovered_by_later_close"]
    pd.testing.assert_frame_equal(new_indexed[label_columns], source_indexed[label_columns], check_dtype=False)
    # All original causal feature and warning values are also required to match.
    retained_columns = [c for c in source_indexed if c in new_indexed and c not in label_columns and c != "split"]
    pd.testing.assert_frame_equal(new_indexed[retained_columns], source_indexed[retained_columns], check_dtype=False)
    dense["stamp"] = pd.to_datetime(dense.decision_ts, utc=True)
    anchors = dense.groupby(["trade_id", "horizon_minutes"])["stamp"].transform("min")
    grid_mask = (dense.stamp - anchors).dt.total_seconds().div(60).mod(dense.horizon_minutes).eq(0)
    assert np.array_equal(grid_mask.to_numpy(), dense.is_grid.to_numpy()), "Grid anchor mismatch"
    policy_cols = {"ORIGINAL": "warning", "RESEARCH_WATCH": "research_watch", "HIGH_CONFIDENCE": "high_confidence"}
    independent_alerts = {}
    for policy, column in policy_cols.items():
        chosen = []
        for _, group in dense.sort_values("stamp").groupby(["trade_id", "horizon_minutes"], sort=False):
            next_time = None
            for row in group.loc[group[column]].itertuples():
                if next_time is None or row.stamp >= next_time:
                    chosen.append(row.Index)
                    next_time = row.stamp + pd.Timedelta(minutes=row.horizon_minutes)
        independent_alerts[policy] = dense.index.isin(chosen)
        assert np.array_equal(independent_alerts[policy], dense["alert_" + policy].to_numpy()), policy
    original = pd.read_csv(source_path / "alert_episodes.csv")
    original["stamp"] = pd.to_datetime(original.decision_ts, utc=True)
    episode_keys = ["trade_id", "horizon_minutes", "stamp"]
    assert set(map(tuple, original[episode_keys].to_numpy())) == set(map(tuple, dense.loc[independent_alerts["ORIGINAL"], episode_keys].to_numpy()))

    metrics = pd.read_csv(out / "metrics.csv")
    calendars = {"ALL": set(manifest["days"]), "FIT20": set(summary["train_days"]),
                 "CHECK10": set(summary["check_days"]), "LATER16": set(summary["evaluation_days"])}
    count_checks = rate_checks = 0
    for row in metrics.itertuples():
        mask = dense.day.isin(calendars[row.split]) & dense.horizon_minutes.eq(row.horizon_minutes)
        if row.side != "BOTH":
            mask &= dense.side.eq(row.side)
        if row.model_stage != "ALL":
            mask &= dense.model_stage.eq(row.model_stage)
        part = dense.loc[mask]
        grid = part.loc[grid_mask.loc[part.index] & part.outcome_available]
        w, y = grid[policy_cols[row.policy]], grid.event
        tp, fp, fn, tn = [int(x.sum()) for x in (w & y, w & ~y, ~w & y, ~w & ~y)]
        alerts = dense.loc[mask & independent_alerts[row.policy]]
        resolved = alerts.loc[alerts.outcome_available]
        first = alerts.sort_values("stamp").drop_duplicates(["trade_id", "horizon_minutes"])
        first_resolved = first.loc[first.outcome_available]
        hits, n = int(resolved.event.sum()), len(resolved)
        full_no = grid.loc[grid.full_horizon_eligible & ~w]
        counts = dict(grid_rows=int(grid_mask.loc[part.index].sum()), resolved_grid_rows=len(grid),
                      grid_events=tp + fn, grid_warnings=tp + fp, grid_tp=tp, grid_fp=fp, grid_fn=fn,
                      grid_tn=tn, alert_count=len(alerts), resolved_alerts=n, successful_alerts=hits,
                      false_alerts=n - hits, warned_trades=alerts.trade_id.nunique(), warned_days=alerts.day.nunique(),
                      unknown_alerts=len(alerts) - n, first_alert_count=len(first), first_resolved_alerts=len(first_resolved),
                      full_horizon_no_warning_rows=len(full_no))
        rates = dict(grid_precision=ratio(tp, tp + fp), grid_recall=ratio(tp, tp + fn),
                     grid_prevalence=ratio(tp + fn, len(grid)), grid_f05=ratio(1.25 * tp, 1.25 * tp + .25 * fn + fp),
                     no_warning_event_rate=ratio(fn, fn + tn), npv=ratio(tn, fn + tn),
                     alert_precision=ratio(hits, n), precision_identification_lower=ratio(hits, len(alerts)),
                     first_alert_precision=ratio(int(first_resolved.event.sum()), len(first_resolved)),
                     full_horizon_no_warning_event_rate=ratio(int(full_no.event.sum()), len(full_no)),
                     median_lead_minutes=float(resolved.loc[resolved.event, "lead_minutes"].median()))
        for key, expected in {**counts, **rates}.items():
            assert_equal(getattr(row, key), expected, (row.split, row.policy, row.side, row.horizon_minutes, row.model_stage, key))
        count_checks += len(counts)
        rate_checks += len(rates)
    summary_checks = 0
    for row in summary["primary_results"]:
        mask = pd.Series(True, index=metrics.index)
        for key in ("split", "policy", "side", "horizon_minutes", "model_stage"):
            mask &= metrics[key].eq(row[key])
        assert mask.sum() == 1
        actual = metrics.loc[mask].iloc[0]
        for key, expected in row.items():
            assert_equal(actual[key], expected, key)
            summary_checks += 1

    trades = pd.read_csv(out / "trade_signal_coverage.csv")
    daily = pd.read_csv(out / "daywise_signal_coverage.csv")
    frozen_trades = pd.read_csv(frozen / "trades.csv")
    executed = frozen_trades.loc[frozen_trades.portfolio_executed.astype(str).str.lower().isin(["true", "1"])]
    assert len(executed) == summary["trades"] == 93
    assert_equal(summary["frozen_net_pnl"], float(executed.portfolio_net_profit_rupees.sum()), "frozen P&L")
    assert len(trades) == 2 * len(executed) and len(daily) == 2 * len(manifest["days"])
    coverage_checks = 0
    for row in trades.itertuples():
        part = dense.loc[dense.trade_id.eq(row.trade_id) & dense.horizon_minutes.eq(row.horizon_minutes)]
        grid = part.loc[grid_mask.loc[part.index] & part.outcome_available]
        expected = dict(monitoring_rows=len(part), high_confidence_alerts=int(part.alert_HIGH_CONFIDENCE.sum()),
                        research_watch_alerts=int(part.alert_RESEARCH_WATCH.sum()),
                        no_high_confidence_signal=not bool(part.high_confidence.any()),
                        no_qualifying_watch=not bool(part.research_watch.any()),
                        insufficient_evidence_rows=int(part.signal_state.eq("INSUFFICIENT_EVIDENCE").sum()),
                        grid_events=int(grid.event.sum()), missed_grid_events=int((grid.event & ~grid.research_watch).sum()))
        for key, wanted in expected.items():
            assert_equal(getattr(row, key), wanted, (row.trade_id, row.horizon_minutes, key))
            coverage_checks += 1
    for row in daily.itertuples():
        part = trades.loc[trades.day.eq(row.day) & trades.horizon_minutes.eq(row.horizon_minutes)]
        expected = dict(trades=len(part), monitored_trades=int(part.monitoring_rows.gt(0).sum()),
                        high_confidence_alerts=int(part.high_confidence_alerts.sum()), research_watch_alerts=int(part.research_watch_alerts.sum()),
                        trades_without_watch=int(part.no_qualifying_watch.sum()), trades_without_high_confidence=int(part.no_high_confidence_signal.sum()),
                        grid_events=int(part.grid_events.sum()), missed_grid_events=int(part.missed_grid_events.sum()))
        for key, wanted in expected.items():
            assert_equal(getattr(row, key), wanted, (row.day, row.horizon_minutes, key))
            coverage_checks += 1
    watch = dense.loc[dense.day.isin(calendars["LATER16"]) & independent_alerts["RESEARCH_WATCH"]]
    report = dict(status="PASS", audited_at_utc=datetime.now(timezone.utc).isoformat(),
                  audit_script=str(Path(__file__).resolve()), audit_script_sha256=digest(__file__),
                  research_modules_imported=False, output=str(out), labels_recomputed_from_prices=False,
                  labels_compared_to_existing_audited_monitor=True,
                  prediction_rows=len(dense), label_columns_compared=label_columns,
                  label_cells_matched=len(dense) * len(label_columns), original_feature_columns_matched=retained_columns,
                  grid_anchor_checks=len(dense), policy_cooldown_checks=len(dense) * len(policy_cols),
                  original_alert_keys_matched=len(original), metric_count_cells_matched=count_checks,
                  metric_rate_cells_matched=rate_checks, summary_fields_matched=summary_checks,
                  trade_daily_coverage_cells_matched=coverage_checks, trade_horizon_rows=len(trades), daily_horizon_rows=len(daily),
                  frozen_executed_trades=len(executed), frozen_net_pnl=float(executed.portfolio_net_profit_rupees.sum()),
                  locked_selection_sha256=summary["selection_sha256"],
                  later16_watch=dict(alerts=len(watch), successful=int(watch.event.sum()),
                    unique_trades=watch.trade_id.nunique(), unique_dates=watch.day.nunique(),
                    median_lead_minutes=float(watch.loc[watch.event, "lead_minutes"].median()),
                    by_side={side: dict(alerts=len(g), successful=int(g.event.sum())) for side, g in watch.groupby("side")}),
                  source_input_hash_checks=source_hashes, frozen_artifact_hash_checks=frozen_hashes,
                  archived_code_snapshot_hash_checks=code_hashes, source_minute_snapshot_hash_checks=minute_hashes,
                  source_snapshot_manifest_hash_checks=snapshot_hashes,
                  test_execution=dict(source="Root agent explicitly confirmed separate pytest execution",
                                      passed=root_tests, elapsed_seconds=root_test_seconds, rerun_by_this_audit=False))
    test_path = out / "test_validation.json"
    if test_path.is_file():
        report["test_validation_artifact"] = dict(path=test_path.name, sha256=digest(test_path), contents=read_json(test_path))
    (out / "independent_validation.json").write_text(json.dumps(report, indent=2, allow_nan=False), encoding="utf-8")
    artifacts = {p.relative_to(out).as_posix(): digest(p) for p in sorted(out.rglob("*"))
                 if p.is_file() and p != out / "artifact_hashes.json"}
    (out / "artifact_hashes.json").write_text(json.dumps(artifacts, indent=2, allow_nan=False), encoding="utf-8")
    print(json.dumps(dict(status="PASS", metric_count_cells=count_checks, metric_rate_cells=rate_checks,
                         source_label_cells=len(dense) * len(label_columns), original_alert_keys=len(original),
                         snapshot_minute_files=len(minute_hashes), frozen_artifacts=len(frozen_hashes),
                         archived_code_snapshots=len(code_hashes), artifact_hashes=len(artifacts),
                         report=str(out / "independent_validation.json")), indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out", type=Path)
    parser.add_argument("--root-tests", type=int)
    parser.add_argument("--root-test-seconds", type=float)
    args = parser.parse_args()
    run(args.out, args.root_tests, args.root_test_seconds)
