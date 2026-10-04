from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ai_platform.observability import strategy_research as subject


def _row(day: str, index: int, *, filled: bool, pnl: float) -> dict[str, str]:
    side = "LONG" if index % 2 == 0 else "SHORT"
    signal = f"{day}T09:25:00+05:30"
    confirmation = f"{day}T09:26:00+05:30"
    entry = f"{day}T09:27:00+05:30" if filled else ""
    return {
        "day": day,
        "tradingsymbol": f"S{index}",
        "side": side,
        "setup_id": f"0926_{side}",
        "signal_ts": signal,
        "confirmation_ts": confirmation,
        "v9_5m_feature_ts": signal,
        "v9_1m_feature_ts": confirmation,
        "v9_feature_available_ts": confirmation,
        "entry_ts": entry,
        "filled": str(filled),
        "portfolio_executed": str(filled),
        "portfolio_net_profit_rupees": str(pnl if filled else 0.0),
        "portfolio_trade_capital_rupees": "100000" if filled else "0",
        "net_return_on_capital_pct": str(pnl / 1000.0 if filled else 0.0),
        "target_hit": str(filled and pnl > 0),
        "stop_hit": str(filled and pnl < 0),
        "same_bar_ambiguous": "False",
        "mfe_pct": "1.2" if filled else "",
        "mae_pct": "-0.4" if filled else "",
        "nifty_first_bar_return_pct": "0.2" if side == "LONG" else "-0.2",
        "oi_change_pct": "0.6",
        "volume_ratio": "2.0",
        "traded_value": "150000000",
        "v9_5m_range_pct": "0.9",
        "v9_5m_distance_vwap_pct": "0.2",
        "v9_5m_ema_spread_pct": "0.3",
        "v9_1m_volume_ratio": "1.5",
        "body_ratio": "0.8",
        "wick_ratio": "0.1",
    }


def _source_run(tmp_path: Path) -> Path:
    root = tmp_path / "source" / "run_1"
    (root / "dataset").mkdir(parents=True)
    (root / "g_backtest").mkdir()
    rows = []
    for day_index in range(1, 7):
        day = f"2026-01-{day_index:02d}"
        rows.extend(
            [
                _row(day, day_index * 2, filled=True, pnl=1000.0),
                _row(day, day_index * 2 + 1, filled=day_index % 2 == 0, pnl=-500.0),
            ]
        )
    portfolio = root / "g_backtest" / "portfolio_trades.csv"
    with portfolio.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (root / "dataset" / "dataset_manifest.json").write_text(
        json.dumps(
            {
                "schema": "test.causal.v1",
                "through_day": "2026-01-06",
                "source_fingerprint": "a" * 64,
                "sources": [{"path": "fixture", "exists": True}],
                "days": [f"2026-01-{day:02d}" for day in range(1, 7)],
            }
        ),
        encoding="utf-8",
    )
    (root / "dataset" / "source_session_eligibility.csv").write_text(
        "day,eligible\n2026-01-01,True\n2026-01-02,True\n",
        encoding="utf-8",
    )
    # An unreadable optional parquet remains explicit in the attribution report;
    # it must not make the other evidence disappear.
    (root / "dataset" / "setup_audit.parquet").write_bytes(b"fixture")
    baseline = subject.performance(rows)
    (root / "g_backtest" / "summary.json").write_text(
        json.dumps({"evidence": subject.EXPLORATORY_EVIDENCE}), encoding="utf-8"
    )
    (root / "g_backtest" / "run_metadata.json").write_text(
        json.dumps(
            {
                "through_day": "2026-01-06",
                "session_count": 6,
                "first_session": "2026-01-01",
                "last_session": "2026-01-06",
                "metrics": {
                    "full_history": {
                        "selected_orders": baseline["selected_orders"],
                        "trades": baseline["executed_trades"],
                        "net_profit_rupees": baseline["net_profit_rupees"],
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (root / "g_backtest" / "data_coverage_audit.json").write_text(
        json.dumps({"dates": {}}), encoding="utf-8"
    )
    return root


def _execution_research(tmp_path: Path, source_run: Path) -> Path:
    root = tmp_path / "execution_research"
    run_id = "20260210T000000000000Z"
    run = root / "runs" / run_id
    latest = root / "latest"
    run.mkdir(parents=True)
    latest.mkdir()
    (source_run / "dataset/paths.npz").write_bytes(b"frozen-path-fixture")
    files = {
        "latest_fno_v13_v10_g_execution_realism.md": "# verified execution\n",
        "v13_v10_g_execution_scenarios.csv": (
            "scenario,net_profit_rupees\nFROZEN_BASELINE,6000\n"
            "COARSE_1M_DELAY_PLUS_MEDIAN_DISTANCE,4500\n"
        ),
        "v13_v10_g_missed_entry_counterfactuals.csv": "sid,safe_for_selection\n7,False\n",
    }
    for directory in (run, latest):
        for name, value in files.items():
            (directory / name).write_text(value, encoding="utf-8")
    scenarios = [
        {
            "scenario": "FROZEN_BASELINE",
            "delay_bars": 0,
            "distance_proxy_bps": 0.0,
            "selected_orders": 12,
            "mechanical_fills": 9,
            "executed_trades": 9,
            "wins": 6,
            "losses": 3,
            "win_rate_pct": 66.6666666667,
            "gross_profit_rupees": 6500.0,
            "cost_rupees": 500.0,
            "net_profit_rupees": 6000.0,
            "net_delta_vs_baseline_rupees": 0.0,
            "profit_factor": 2.0,
            "daily_close_drawdown_rupees": 1000.0,
            "evidence_quality": "SENSITIVITY_ONLY_SMALL_OBSERVED_SAMPLE",
            "safe_for_live_selection": False,
            "interpretation": "Exact frozen entry model",
        },
        {
            "scenario": "COARSE_1M_DELAY_PLUS_MEDIAN_DISTANCE",
            "delay_bars": 1,
            "distance_proxy_bps": 1.5,
            "selected_orders": 12,
            "mechanical_fills": 8,
            "executed_trades": 8,
            "wins": 5,
            "losses": 3,
            "win_rate_pct": 62.5,
            "gross_profit_rupees": 4950.0,
            "cost_rupees": 450.0,
            "net_profit_rupees": 4500.0,
            "net_delta_vs_baseline_rupees": -1500.0,
            "profit_factor": 1.5,
            "daily_close_drawdown_rupees": 1250.0,
            "evidence_quality": "SENSITIVITY_ONLY_SMALL_OBSERVED_SAMPLE",
            "safe_for_live_selection": False,
            "interpretation": "One-minute activation stress",
        },
    ]
    scenario_fields = list(scenarios[0])
    for directory in (run, latest):
        with (directory / "v13_v10_g_execution_scenarios.csv").open(
            "w", encoding="utf-8", newline=""
        ) as handle:
            writer = csv.DictWriter(handle, fieldnames=scenario_fields)
            writer.writeheader()
            writer.writerows(scenarios)
        (directory / "v13_v10_g_missed_entry_counterfactuals.csv").write_text(
            "sid,post_expiry_trigger_touched,safe_for_selection\n7,True,False\n",
            encoding="utf-8",
        )
    artifact_names = {
        "report": "latest_fno_v13_v10_g_execution_realism.md",
        "scenarios": "v13_v10_g_execution_scenarios.csv",
        "missed_entries": "v13_v10_g_missed_entry_counterfactuals.csv",
    }
    manifest = {
        "schema_version": subject.EXECUTION_RESEARCH_SCHEMA_VERSION,
        "run_id": run_id,
        "generated_at_utc": "2026-02-10T00:00:00Z",
        "mode": "READ_ONLY_RESEARCH",
        "execution_authority": False,
        "live_configuration_changed": False,
        "source_run": str(source_run.resolve()),
        "source_through_day": "2026-01-06",
        "source_artifacts": {
            relative: {
                "bytes": (source_run / relative).stat().st_size,
                "sha256": subject.sha256_file(source_run / relative),
            }
            for relative in (
                "g_backtest/portfolio_trades.csv",
                "dataset/paths.npz",
                "dataset/dataset_manifest.json",
                "g_backtest/run_metadata.json",
            )
        },
        "paper_calibration": {"terminal_orders": 2, "fills": 1},
        "frozen_baseline_reconciliation": {
            "selected_orders": True,
            "executed_trades": True,
            "net_profit_rupees": True,
        },
        "scenarios": scenarios,
        "missed_entry_counterfactuals": {
            "rows": 1,
            "late_trigger_touches": 1,
            "safe_for_selection": False,
        },
        "artifacts": {
            key: {
                "filename": name,
                "sha256": subject.sha256_file(run / name),
            }
            for key, name in artifact_names.items()
        },
        "conclusion": "INSUFFICIENT_EVIDENCE_FOR_LIVE_CHANGE",
        "safe_for_live_selection": False,
    }
    manifest["content_sha256"] = subject._canonical_sha256(manifest)
    for directory in (run, latest):
        (directory / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return root


def test_regime_classifier_uses_fixed_decision_time_buckets() -> None:
    result = subject.classify_regime(_row("2026-01-01", 2, filled=True, pnl=1))
    assert result == {
        "market_regime": "BULLISH",
        "side_alignment": "ALIGNED",
        "volatility_regime": "HIGH",
        "oi_regime": "ELEVATED",
        "liquidity_regime": "HIGH",
    }


def test_point_in_time_audit_detects_future_feature_leakage() -> None:
    clean = _row("2026-01-01", 1, filled=True, pnl=1)
    assert subject.point_in_time_audit([clean])["violation_count"] == 0
    leaked = dict(clean, v9_5m_feature_ts="2026-01-01T09:26:01+05:30")
    audit = subject.point_in_time_audit([leaked])
    assert audit["violation_count"] == 1
    assert audit["violations"][0]["check"] == "5m_feature_by_signal"
    assert audit["outcome_fields_used_for_regime"] == []


def test_predictions_never_train_on_same_day_or_future_rows() -> None:
    rows = []
    for day_index in range(1, 5):
        day = f"2026-02-{day_index:02d}"
        rows.extend(
            [_row(day, day_index * 2, filled=True, pnl=100),
             _row(day, day_index * 2 + 1, filled=False, pnl=0)]
        )
    predictions = subject.prior_only_predictions(
        rows, minimum_history=4, minimum_context=2
    )
    day_three = [row for row in predictions if row["day"] == "2026-02-03"]
    assert len(day_three) == 2
    assert {row["training_rows"] for row in day_three} == {4}
    assert {row["training_cutoff_day"] for row in day_three} == {"2026-02-02"}
    assert all(row["prediction_state"] == "RESEARCH_ESTIMATE" for row in day_three)
    assert all(row["training_cutoff_day"] < row["day"] for row in predictions[4:])


def test_bundle_is_immutable_read_only_and_promotion_stays_blocked(tmp_path: Path) -> None:
    source = _source_run(tmp_path)
    output = tmp_path / "output"
    before = {path.relative_to(source): path.read_bytes() for path in source.rglob("*") if path.is_file()}
    bundle = subject.generate_research_bundle(
        source_run=source,
        output_root=output,
        registry_root=tmp_path / "empty_registry",
        now=datetime(2026, 2, 10, tzinfo=timezone.utc),
        minimum_history=4,
        minimum_context=2,
    )
    after = {path.relative_to(source): path.read_bytes() for path in source.rglob("*") if path.is_file()}
    assert before == after
    assert bundle.state == "READY_RESEARCH_ONLY"
    assert bundle.as_dict()["execution_authority"] is False
    assert set(path.name for path in bundle.latest_dir.glob("*.md")) == set(
        subject.ALL_REPORT_FILES.values()
    )
    manifest = json.loads(bundle.manifest_path.read_text(encoding="utf-8"))
    assert manifest["execution_authority"] is False
    assert manifest["live_configuration_changed"] is False
    assert manifest["analysis"]["promotion_ready_for_manual_review"] is False
    assert manifest["analysis"]["promotion_gates"]["untouched_holdout_evidence"] is False
    assert manifest["analysis"]["shadow"]["sessions"] == 0
    for card_id, record in manifest["reports"].items():
        assert card_id in subject.ALL_REPORT_FILES
        assert subject.sha256_file(bundle.latest_dir / record["filename"]) == record["sha256"]
    assert set(manifest["reports"]) == set(subject.ALL_REPORT_FILES)
    assert manifest["analysis"]["operational_observability"]["state"] == "UNAVAILABLE"
    improvements = manifest["analysis"]["improvement_opportunities"]
    assert improvements["state"] == "RESEARCH_PROPOSALS_ONLY"
    assert improvements["execution_authority"] is False
    assert improvements["estimated_profit_improvement_rupees"] is None
    improvement_report = bundle.latest_dir / subject.IMPROVEMENT_REPORT_FILES[
        "fno_v13_v10_g_research_improvements"
    ]
    assert "#card-fno_v13_v10_g_research_shadow" in improvement_report.read_text(encoding="utf-8")


def test_verified_execution_research_is_rendered_but_has_no_authority(
    tmp_path: Path,
) -> None:
    source = _source_run(tmp_path)
    execution_root = _execution_research(tmp_path, source)
    verified = subject.load_verified_execution_research(
        execution_root, source_run=source
    )
    assert verified["state"] == "READY_VERIFIED"
    assert verified["immutable_run_verified"] is True
    assert verified["execution_authority"] is False
    assert verified["safe_for_live_selection"] is False

    bundle = subject.generate_research_bundle(
        source_run=source,
        output_root=tmp_path / "output",
        execution_research_root=execution_root,
        registry_root=tmp_path / "empty_registry",
        now=datetime(2026, 2, 10, tzinfo=timezone.utc),
        minimum_history=4,
        minimum_context=2,
    )
    entry = (
        bundle.latest_dir
        / subject.OBSERVABILITY_REPORT_FILES[
            "fno_v13_v10_g_observability_entry_execution"
        ]
    ).read_text(encoding="utf-8")
    pnl = (
        bundle.latest_dir
        / subject.OBSERVABILITY_REPORT_FILES[
            "fno_v13_v10_g_observability_pnl_attribution"
        ]
    ).read_text(encoding="utf-8")
    assert "READY_VERIFIED" in entry
    assert "COARSE_1M_DELAY_PLUS_MEDIAN_DISTANCE" in entry
    assert "-1,500.00" in entry
    assert "COARSE_1M_DELAY_PLUS_MEDIAN_DISTANCE" in pnl
    manifest = json.loads(bundle.manifest_path.read_text(encoding="utf-8"))
    integrated = manifest["analysis"]["execution_research"]
    assert integrated["state"] == "READY_VERIFIED"
    assert integrated["execution_authority"] is False
    assert manifest["execution_authority"] is False

    (execution_root / "latest/v13_v10_g_execution_scenarios.csv").write_text(
        "tampered\n", encoding="utf-8"
    )
    invalid = subject.load_verified_execution_research(
        execution_root, source_run=source
    )
    assert invalid["state"] == "INVALID"
    assert invalid["scenarios"] == []


def test_execution_loader_rejects_semantic_csv_drift_even_when_rehashed(
    tmp_path: Path,
) -> None:
    source = _source_run(tmp_path)
    execution_root = _execution_research(tmp_path, source)
    run_id = "20260210T000000000000Z"
    run = execution_root / "runs" / run_id
    latest = execution_root / "latest"
    for directory in (run, latest):
        path = directory / "v13_v10_g_execution_scenarios.csv"
        path.write_text(
            path.read_text(encoding="utf-8").replace("6000.0", "9999.0"),
            encoding="utf-8",
        )
    manifest = json.loads((latest / "manifest.json").read_text(encoding="utf-8"))
    manifest["artifacts"]["scenarios"]["sha256"] = subject.sha256_file(
        latest / "v13_v10_g_execution_scenarios.csv"
    )
    manifest.pop("content_sha256")
    manifest["content_sha256"] = subject._canonical_sha256(manifest)
    encoded = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    for directory in (run, latest):
        (directory / "manifest.json").write_text(encoded, encoding="utf-8")

    result = subject.load_verified_execution_research(
        execution_root, source_run=source
    )

    assert result["state"] == "INVALID"
    assert "scenario field mismatch" in result["reason"]


def test_bundle_flags_post_cutoff_metadata_and_partial_oi_coverage(
    tmp_path: Path,
) -> None:
    source = _source_run(tmp_path)
    (source / "dataset" / "source_session_eligibility.csv").write_text(
        "day,eligible\n2026-01-06,True\n2026-01-07,True\n",
        encoding="utf-8",
    )
    (source / "g_backtest" / "data_coverage_audit.json").write_text(
        json.dumps(
            {
                "dates": {
                    "2026-01-06": {
                        "equity_1m": {"symbols_present": 2},
                        "equity_5m": {"symbols_present": 2},
                        "futures_oi_5m": {
                            "contracts_present": 2,
                            "partial_contracts": {"TEST26JANFUT": 74},
                        },
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    bundle = subject.generate_research_bundle(
        source_run=source,
        output_root=tmp_path / "output",
        registry_root=tmp_path / "empty_registry",
        now=datetime(2026, 2, 10, tzinfo=timezone.utc),
        minimum_history=4,
        minimum_context=2,
    )
    manifest = json.loads(bundle.manifest_path.read_text(encoding="utf-8"))
    assert bundle.state == "PARTIAL_EVIDENCE"
    assert manifest["analysis"]["eligibility_cutoff"] == {
        "raw_rows": 2,
        "raw_eligible_sessions": 2,
        "eligible_sessions_through_cutoff": 1,
        "post_cutoff_eligible_days": ["2026-01-07"],
    }
    assert (
        manifest["analysis"]["promotion_gates"]
        ["eligibility_metadata_bounded_by_source_cutoff"]
        is False
    )
    quality = (bundle.latest_dir / subject.REPORT_FILES[
        "fno_v13_v10_g_research_data_quality"
    ]).read_text(encoding="utf-8")
    dataset = (bundle.latest_dir / subject.REPORT_FILES[
        "fno_v13_v10_g_research_dataset"
    ]).read_text(encoding="utf-8")
    baseline = (bundle.latest_dir / subject.REPORT_FILES[
        "fno_v13_v10_g_research_baseline"
    ]).read_text(encoding="utf-8")
    assert "Gate: **BLOCKED**" in quality
    assert "2026-01-07" in quality
    assert "partial OI contracts" in dataset
    assert "| 2026-01-06 | 2 | 2 | 2 | 1 |" in dataset
    assert "row-order closed-trade drawdown" in baseline
    assert "Source daily-close drawdown" in baseline


def test_completed_zero_trade_session_is_explicit_in_daywise_report(
    tmp_path: Path,
) -> None:
    source = _source_run(tmp_path)
    portfolio = source / "g_backtest" / "portfolio_trades.csv"
    with portfolio.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    rows = [row for row in rows if row["day"] != "2026-01-03"]
    with portfolio.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    metrics = subject.performance(rows)
    metadata_path = source / "g_backtest" / "run_metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["metrics"]["full_history"].update(
        selected_orders=metrics["selected_orders"],
        trades=metrics["executed_trades"],
        net_profit_rupees=metrics["net_profit_rupees"],
    )
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    bundle = subject.generate_research_bundle(
        source_run=source,
        output_root=tmp_path / "output",
        registry_root=tmp_path / "empty_registry",
        now=datetime(2026, 2, 10, tzinfo=timezone.utc),
        minimum_history=4,
        minimum_context=2,
    )
    manifest = json.loads(bundle.manifest_path.read_text(encoding="utf-8"))
    assert manifest["analysis"]["daywise_coverage"] == {
        "completed_sessions": 6,
        "zero_selected_sessions": 1,
        "zero_selected_days": ["2026-01-03"],
    }
    report = (bundle.latest_dir / subject.REPORT_FILES[
        "fno_v13_v10_g_research_baseline"
    ]).read_text(encoding="utf-8")
    assert "| 2026-01-03 | 0 | 0 | 0 | 0 | 0.00 | UNAVAILABLE |" in report


def test_discovery_refuses_incomplete_runs(tmp_path: Path) -> None:
    (tmp_path / "run_incomplete").mkdir()
    with pytest.raises(FileNotFoundError, match="no complete"):
        subject.discover_source_run(tmp_path)


def test_shadow_sessions_reject_journal_only_or_incomplete_manifests(
    tmp_path: Path,
) -> None:
    root = tmp_path / "shadow_sessions"
    root.mkdir()
    manifest = root / "2026-02-01.json"
    manifest.write_text('{"complete":true}\n', encoding="utf-8")
    journal = tmp_path / "shadow_observations.jsonl"
    valid = {
        "schema_version": "eqidv2.v13_v10_g.prospective_shadow_session.v1",
        "mode": "PROSPECTIVE_SHADOW",
        "completed": True,
        "execution_authority": False,
        "outcome_join_state": "COMPLETE",
        "strategy_fingerprint": "strategy-hash",
        "dataset_sha256": "d" * 64,
        "model_sha256": "m" * 64,
        "session_date": "2026-02-01",
        "manifest_path": str(manifest),
        "manifest_sha256": subject.sha256_file(manifest),
    }
    forged = {"mode": "PROSPECTIVE_SHADOW", "completed": True, "session_date": "2026-02-02"}
    journal.write_text(
        json.dumps(forged) + "\n" + json.dumps(valid) + "\n", encoding="utf-8"
    )
    result = subject._shadow_summary(journal, allowed_manifest_root=root)
    assert result["sessions"] == 0
    assert result["invalid_records"] == 2
    assert result["state"] == "PARTIAL"


def test_shadow_promotion_gate_blocks_any_invalid_evidence() -> None:
    clean = {
        "state": "READY",
        "sessions": 20,
        "invalid_records": 0,
        "invalid_sessions": 0,
    }
    assert subject._shadow_promotion_gate(clean)
    assert not subject._shadow_promotion_gate({**clean, "state": "PARTIAL"})
    assert not subject._shadow_promotion_gate({**clean, "invalid_records": 1})
    assert not subject._shadow_promotion_gate({**clean, "invalid_sessions": 1})
    assert not subject._shadow_promotion_gate({**clean, "sessions": 19})


def test_new_prepared_cohort_supersedes_old_completed_cohort() -> None:
    statuses = [
        {
            "session_date": f"2026-08-{day:02d}",
            "state": "COMPLETE",
            "cohort_sha256": "old-cohort",
        }
        for day in range(1, 21)
    ]
    statuses.append(
        {
            "session_date": "2026-08-21",
            "state": "PREPARED",
            "cohort_sha256": "new-cohort",
        }
    )

    active, sessions, completed_cohorts = subject._active_shadow_cohort(statuses)

    assert active == "new-cohort"
    assert sessions == 0
    assert completed_cohorts == 1


def test_operational_view_retains_first_live_failure_and_current_broker_parity(
    tmp_path: Path,
) -> None:
    live = tmp_path / "live"
    order_dir = live / "orders" / "LIVE" / "live_kite_qty1" / "2026-02-02"
    order_dir.mkdir(parents=True)
    (order_dir / "signal-1.json").write_text(
        json.dumps(
            {
                "signal_id": "signal-1",
                "session_date": "2026-02-02",
                "confirmation_end": "09:31",
                "status": "CANCELLED",
                "status_reason": "LATE_START_NO_RETROACTIVE_ENTRY",
                "created_at_ist": "2026-02-02T09:31:02+05:30",
                "updated_at_ist": "2026-02-02T10:00:01+05:30",
                "entry_order_activated_at_ist": "",
                "entry_at_ist": "",
            }
        ),
        encoding="utf-8",
    )
    event_dir = live / "order_events" / "LIVE"
    event_dir.mkdir(parents=True)
    events = [
        {
            "context": {"signal_id": "signal-1"},
            "data": {
                "reason": "TokenException: Incorrect api_key or access_token.",
                "state_before": "UNSEEN",
                "state_after": "PENDING_ENTRY",
            },
        },
        {
            "context": {"signal_id": "signal-1"},
            "data": {
                "reason": "LATE_START_NO_RETROACTIVE_ENTRY",
                "state_before": "PENDING_ENTRY",
                "state_after": "CANCELLED",
            },
        },
    ]
    (event_dir / "2026-02-02.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in events), encoding="utf-8"
    )
    status_dir = live / "live_kite"
    status_dir.mkdir(parents=True)
    (status_dir / "status.json").write_text(
        json.dumps(
            {
                "updated_at_ist": "2026-02-02T15:20:00+05:30",
                "children": {
                    "broker_reconciliation": {
                        "broker_truth_available": True,
                        "scope_complete": True,
                        "mismatch_count": 0,
                        "active_order_parity_complete": True,
                        "active_order_mismatch_count": 0,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    result = subject.collect_operational_observability(
        live_root=live,
        replay_root=None,
        historical_rows=[],
    )

    live_execution = result["execution"]["LIVE"]
    assert live_execution["status_reason_counts"] == {
        "LATE_START_NO_RETROACTIVE_ENTRY": 1
    }
    assert live_execution["first_observed_reason_counts"] == {
        "TokenException: Incorrect api_key or access_token.": 1
    }
    assert live_execution["last_observed_reason_counts"] == {
        "LATE_START_NO_RETROACTIVE_ENTRY": 1
    }
    assert live_execution["first_reason_signal_coverage"] == 1
    assert result["broker_reconciliation"]["state"] == (
        "READY_POSITION_AND_ACTIVE_ORDER_PARITY"
    )
    assert result["broker_reconciliation"]["mismatch_count"] == 0
    assert "live/order_events/LIVE/2026-02-02.jsonl" in result["source_artifacts"]
