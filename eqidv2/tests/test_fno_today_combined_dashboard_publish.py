from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from tools import fno_today_combined_dashboard_publish as publisher


def _make_run(root: Path) -> Path:
    root.mkdir(parents=True)
    comparison = []
    contracts = {}
    checks = {}
    for index, strategy in enumerate(publisher.EXPECTED_STRATEGIES):
        comparison.append(
            {
                "session_date": publisher.SESSION_DATE,
                "strategy": strategy,
                "candidates": "1",
                "fills": "1",
                "wins": "1",
                "losses": "0",
                "flat_trades": "0",
                "win_rate_pct": "100",
                "profit_factor": "2.0",
                "net_return_points": str(1.0 + index),
                "net_pnl_rs": str(100.0 + index),
                "explicit_uniform_cutoff_ist": publisher.EXPECTED_CUTOFF,
                "all_input_candidates": "1" if strategy == "V12_SELECTED" else "",
            }
        )
        contracts[strategy] = [
            {
                "candidate_id": f"candidate-{index}",
                "confirmation_time": "2026-08-31 09:26:00+05:30",
                "entry_time": "2026-08-31 09:27:00+05:30",
                "entry_price": 100.0,
                "stop_price": 99.0,
                "target_price": 102.0,
                "exit_time": "2026-08-31 10:00:00+05:30",
                "exit_reason": "TARGET",
                "net_return_pct": 1.0,
                "net_pnl_rs": 100.0 + index,
            }
        ]
        checks[strategy] = {
            "same_fill_identity": True,
            "confirmation_entry_stop_target_unchanged": True,
        }
    source_evidence = {
        symbol: {"used_rows": 360, "used_max_ist": publisher.EXPECTED_CUTOFF}
        for symbol in publisher.EXPECTED_SYMBOLS
    }
    manifest = {
        "schema_version": "fno_today_current_refresh_replay_v1",
        "complete": True,
        "session_date": publisher.SESSION_DATE,
        "explicit_uniform_cutoff_ist": publisher.EXPECTED_CUTOFF,
        "source_evidence": source_evidence,
        "checks_vs_1144": checks,
        "source_complete": False,
        "headline_valid": False,
        "research_only": True,
        "promotion_eligible": False,
        "economics": {
            "cost_bps": 15.0,
            "slippage_bps": 0.0,
            "target_exposure_per_entry_rs": 50000.0,
        },
    }
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (root / "trade_contracts.json").write_text(json.dumps(contracts), encoding="utf-8")
    with (root / "comparison.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(comparison[0]))
        writer.writeheader()
        writer.writerows(comparison)
    return root


def test_validation_rejects_incomplete_manifest(tmp_path: Path) -> None:
    run_root = _make_run(tmp_path / "run")
    manifest_path = run_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["complete"] = False
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(publisher.PublicationError, match="complete"):
        publisher.validate_run(run_root)


def test_validation_rejects_missing_strategy(tmp_path: Path) -> None:
    run_root = _make_run(tmp_path / "run")
    rows = list(csv.DictReader((run_root / "comparison.csv").open(encoding="utf-8")))[:-1]
    with (run_root / "comparison.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    with pytest.raises(publisher.PublicationError, match="strategy set mismatch"):
        publisher.validate_run(run_root)


def test_publish_updates_dashboard_outputs_without_touching_legacy_json(tmp_path: Path) -> None:
    run_root = _make_run(tmp_path / "run")
    dashboard_root = tmp_path / "dashboard"
    log_dir = tmp_path / "logs"
    legacy_json = dashboard_root / "latest" / "latest_backtesting_result_v11.json"
    legacy_json.parent.mkdir(parents=True)
    legacy_json.write_text('{"legacy": true}\n', encoding="utf-8")

    paths = publisher.publish_validated_run(
        run_root,
        dashboard_root=dashboard_root,
        log_dir=log_dir,
        console_log="test console output\n",
    )

    report = paths["latest_report"].read_text(encoding="utf-8")
    for friendly_name in publisher.FRIENDLY_NAMES.values():
        assert friendly_name in report
    assert "COMPLETED AND VALIDATED" in report
    assert paths["latest_log"].is_file()
    assert json.loads(legacy_json.read_text(encoding="utf-8")) == {"legacy": True}
    combined = json.loads(paths["combined_json"].read_text(encoding="utf-8"))
    assert combined["schema_version"] == "fno_combined_dashboard_publication_v1"
