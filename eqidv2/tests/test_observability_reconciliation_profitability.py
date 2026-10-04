from __future__ import annotations

import json

from ai_platform.observability.profitability import (
    execution_drag,
    group_performance,
    summarize_performance,
)
from ai_platform.observability.reconciliation import (
    ComparisonState,
    compare_stage,
    load_stage_bundle,
    reconcile_live_observed_final,
    write_report,
)


def _row(symbol: str = "AAA", **values):
    return {
        "session_date": "2026-09-24",
        "slot": "0925",
        "symbol": symbol,
        **values,
    }


def test_stage_comparison_respects_tolerance_and_finds_first_difference():
    within = compare_stage(
        "feature",
        [_row(ema9=100.0)],
        [_row(ema9=100.0000001)],
        tolerances={"ema9": 1e-6},
    )
    assert within.state is ComparisonState.EXACT

    mismatch = compare_stage(
        "feature",
        [_row(ema9=100.0)],
        [_row(ema9=100.1)],
        tolerances={"ema9": 1e-6},
    )
    assert mismatch.state is ComparisonState.MISMATCH
    assert mismatch.differences[0].field == "ema9"


def test_duplicate_keys_make_comparison_indeterminate():
    duplicate = [_row(value=1), _row(value=2)]
    result = compare_stage("raw_equity", duplicate, [_row(value=1)])
    assert result.state is ComparisonState.INDETERMINATE
    assert result.reason == "duplicate_comparison_keys"


def test_two_replay_model_separates_system_and_data_difference():
    live = {
        "raw_equity": [_row(close=100.0)],
        "feature": [_row(ema9=99.0, selected=True)],
    }
    observed = {
        "raw_equity": [_row(close=100.0)],
        "feature": [_row(ema9=99.0, selected=True)],
    }
    finalized = {
        "raw_equity": [_row(close=101.0)],
        "feature": [_row(ema9=99.2, selected=False)],
    }
    report = reconcile_live_observed_final(
        session_date="2026-09-24",
        live=live,
        observed=observed,
        finalized=finalized,
        generated_at_ist="2026-09-24T16:30:00+05:30",
    )
    assert report["comparisons"]["live_vs_observed"]["state"] == "EXACT"
    changed = report["comparisons"]["observed_vs_finalized"]
    assert changed["state"] == "MISMATCH"
    assert changed["first_divergence_stage"] == "raw_equity"
    assert changed["classification"] == "LATE_DATA_OR_PROVIDER_REVISION"
    assert len(report["report_sha256"]) == 64


def test_missing_stage_is_not_reported_as_parity():
    result = compare_stage("feature", None, [_row(ema9=1.0)])
    assert result.state is ComparisonState.INDETERMINATE


def test_native_feature_keys_are_inferred_and_correlation_ids_are_ignored():
    live = {
        "session_date": "2026-09-24",
        "signal_ts": "2026-09-24T09:25:00+05:30",
        "tradingsymbol": "AAA",
        "ema9": 100.0,
        "run_id": "live-run",
        "replay_id": "",
        "trace_id": "1" * 32,
        "ledger_row_sha256": "a" * 64,
    }
    replay = {
        **live,
        "run_id": "replay-run",
        "replay_id": "observed-replay",
        "trace_id": "2" * 32,
        "ledger_row_sha256": "b" * 64,
    }
    report = reconcile_live_observed_final(
        session_date="2026-09-24",
        live={"feature": [live]},
        observed={"feature": [replay]},
        finalized={"feature": [replay]},
    )
    assert report["comparisons"]["live_vs_observed"]["state"] == "EXACT"


def test_unkeyed_multirow_stage_is_indeterminate_not_accidentally_equal():
    result = compare_stage("feature", [{"ema9": 1}, {"ema9": 2}], [{"ema9": 1}])
    assert result.state is ComparisonState.INDETERMINATE
    assert result.reason == "comparison_key_fields_unavailable"


def test_bundle_loader_and_atomic_report(tmp_path):
    bundle = tmp_path / "bundle.json"
    bundle.write_text(json.dumps({"stages": {"feature": [_row(ema9=1.0)]}}))
    assert load_stage_bundle(bundle)["feature"][0]["ema9"] == 1.0

    target = write_report(tmp_path / "report.json", {"state": "EXACT"})
    assert json.loads(target.read_text())["state"] == "EXACT"


def test_bundle_loader_rejects_a_tampered_content_digest(tmp_path):
    payload = {
        "schema_version": "eqidv2_observability_stage_bundle_v1",
        "session_date": "2026-09-24",
        "kind": "live",
        "stages": {"feature": [_row(ema9=1.0)]},
    }
    from ai_platform.observability.reconciliation import canonical_sha256

    payload["content_sha256"] = canonical_sha256(payload)
    bundle = tmp_path / "bundle.json"
    bundle.write_text(json.dumps(payload), encoding="utf-8")
    assert load_stage_bundle(bundle)["feature"][0]["ema9"] == 1.0

    payload["stages"]["feature"][0]["ema9"] = 2.0
    bundle.write_text(json.dumps(payload), encoding="utf-8")
    import pytest

    with pytest.raises(ValueError, match="digest mismatch"):
        load_stage_bundle(bundle)


def test_profitability_keeps_unresolved_records_explicit():
    records = [
        {"status": "CLOSED", "net_pnl_rs": 100.0, "gross_pnl_rs": 110.0, "fees_rs": 10.0},
        {"status": "CLOSED", "net_pnl_rs": -50.0, "gross_pnl_rs": -45.0, "fees_rs": 5.0},
        {"status": "OPEN", "net_pnl_rs": 9999.0},
    ]
    summary = summarize_performance(records, minimum_closed_trades=2)
    assert summary.closed_trades == 2
    assert summary.unresolved == 1
    assert summary.net_pnl_rs == 50.0
    assert summary.profit_factor == 2.0
    assert summary.maximum_drawdown_rs == -50.0
    assert summary.evidence_state == "INSUFFICIENT_EVIDENCE"


def test_grouping_and_execution_drag():
    groups = group_performance(
        [
            {"status": "CLOSED", "net_pnl_rs": 10, "setup": "A"},
            {"status": "CLOSED", "net_pnl_rs": -5, "setup": "B"},
        ],
        group_by=("setup",),
        minimum_closed_trades=1,
    )
    assert [row.dimensions["setup"] for row in groups] == ["A", "B"]

    drag = execution_drag(
        {
            "signal_id": "s1",
            "side": "LONG",
            "quantity": 10,
            "trigger_price": 100,
            "entry_price": 100.5,
            "expected_exit_price": 102,
            "exit_price": 101.8,
            "fees_rs": 2,
        }
    )
    assert drag["entry_slippage_cost_rs"] == 5.0
    assert round(drag["exit_slippage_cost_rs"], 10) == 2.0
    assert round(drag["known_execution_drag_rs"], 10) == 9.0
