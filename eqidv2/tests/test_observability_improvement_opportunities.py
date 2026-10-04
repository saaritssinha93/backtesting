from __future__ import annotations

from copy import deepcopy

from ai_platform.observability.improvement_opportunities import (
    build_improvement_opportunities,
    render_improvement_opportunities,
)


def _inputs():
    return dict(
        baseline={"executed_trades": 77, "net_profit_rupees": 176236.32},
        by_setup=[
            ("0956_LONG", {"executed_trades": 6, "net_profit_rupees": -4500, "profit_factor": .654}),
            ("0926_LONG", {"executed_trades": 8, "net_profit_rupees": 29000, "profit_factor": 12.2}),
        ],
        regimes={"OI": [("ELEVATED", {
            "executed_trades": 15, "net_profit_rupees": 12178, "profit_factor": 1.407,
        })]},
        operational={
            "execution": {"LIVE": {
                "orders": 13, "filled": 0, "cancelled": 13,
                "first_reason_signal_coverage": 2,
                "status_reason_counts": {"LATE_START_NO_RETROACTIVE_ENTRY": 11},
                "first_observed_reason_counts": {"TokenException": 2},
            }, "PAPER": {"orders": 13, "filled": 10}},
            "gate_failures": {"gate_oi_increasing": 1153},
            "drift": [{
                "day": "2026-09-24", "live_selected": 3, "finalized_selected": 2,
                "common": 1, "live_only": 2, "finalized_only": 1,
                "first_divergence": "UNAVAILABLE_NO_STAGE_BUNDLES",
            }],
        },
        execution_research={"state": "READY_VERIFIED", "scenarios": [
            {"scenario": "FROZEN_BASELINE", "delay_bars": 0,
             "net_profit_rupees": 176236, "net_delta_vs_baseline_rupees": 0},
            {"scenario": "OBSERVED_MEDIAN_DISTANCE_PROXY", "delay_bars": 0,
             "net_profit_rupees": 153252, "net_delta_vs_baseline_rupees": -22984},
            {"scenario": "COARSE_1M_DELAY_PLUS_MEDIAN_DISTANCE", "delay_bars": 1,
             "net_profit_rupees": 97122, "net_delta_vs_baseline_rupees": -79114},
        ]},
        prediction_metrics={"eligible_predictions": 62, "total_rows": 85,
                            "fill_brier_score": .1056, "net_return_mae_pct_points": 4.1395},
        coverage={"dates": {"2026-09-23": {"futures_oi_5m": {"partial_contracts": {"ABC": {}}}}}},
        audit={"rows": 111216},
        point_in_time={"missing_timestamps": {}, "violation_count": 0},
        shadow={"state": "NOT_STARTED", "sessions": 0},
        promotion_gates={"untouched_holdout_evidence": False},
        historical_rows=[{"native_stop_pct": .6}, {"native_stop_pct": .89}],
    )


def _items(payload):
    return {item["id"]: item for item in payload["items"]}


def test_observed_losses_generate_proposals_without_claiming_profit_or_authority():
    inputs = _inputs()
    before = deepcopy(inputs)
    result = build_improvement_opportunities(**inputs)
    assert inputs == before
    items = _items(result)
    assert items["execution_availability"]["evidence_state"] == "OBSERVED_ISSUE"
    assert items["signal_parity"]["evidence_state"] == "OBSERVED_ISSUE"
    assert items["data_coverage"]["evidence_state"] == "OBSERVED_ISSUE"
    assert items["setup_selection"]["evidence_state"] == "RESEARCH_HYPOTHESIS"
    assert "0956_LONG" in str(items["setup_selection"]["observations"])
    assert "0926_LONG" not in str(items["setup_selection"]["observations"])
    assert "small sample (<20)" in str(items["setup_selection"]["observations"])
    assert result["estimated_profit_improvement_rupees"] is None
    assert result["live_configuration_changed"] is False
    for item in result["items"]:
        assert item["safe_for_live_selection"] is False
        assert item["execution_authority"] is False
        assert item["estimated_profit_improvement_rupees"] is None
        assert item["sources"] and item["acceptance_criteria"] and item["limitations"]


def test_delayed_scenarios_are_not_presented_as_verified_recoverable_profit():
    item = _items(build_improvement_opportunities(**_inputs()))["execution_costs"]
    observations = str(item["observations"])
    assert "153,252.00" in observations
    assert "97,122" not in observations
    assert "COARSE_1M_DELAY" not in observations
    assert "absolute-expiry" in item["limitations"]
    assert "never extend" in item["acceptance_criteria"]


def test_unverified_scenarios_and_missing_live_inputs_remain_unavailable():
    inputs = _inputs()
    inputs["execution_research"]["state"] = "BLOCKED_INTEGRITY"
    inputs["operational"] = {}
    result = _items(build_improvement_opportunities(**inputs))
    assert result["execution_availability"]["evidence_state"] == "UNAVAILABLE"
    assert "0/0" not in str(result["execution_availability"]["observations"])
    assert result["signal_parity"]["evidence_state"] == "UNAVAILABLE"
    assert result["execution_costs"]["evidence_state"] == "UNAVAILABLE"
    assert "153,252" not in str(result["execution_costs"]["observations"])


def test_candidates_refresh_with_evidence_and_never_become_automatic_promotion():
    inputs = _inputs()
    inputs["by_setup"][0][1]["net_profit_rupees"] = 4500
    inputs["promotion_gates"] = {"untouched_holdout_evidence": True}
    inputs["shadow"] = {"state": "COMPLETE", "sessions": 20}
    result = build_improvement_opportunities(**inputs)
    items = _items(result)
    assert items["setup_selection"]["evidence_state"] == "NO_CURRENT_CANDIDATE"
    assert items["prospective_validation"]["evidence_state"] == "MANUAL_REVIEW_REQUIRED"
    assert result["safe_for_live_selection"] is False
    assert "minimum lifecycle requirement, not statistical proof" in items["prospective_validation"]["limitations"]


def test_report_has_proof_test_acceptance_and_no_invented_indicator_value():
    result = build_improvement_opportunities(**_inputs())
    report = render_improvement_opportunities(result)
    assert "#card-fno_v13_v10_g_observability_entry_execution" in report
    assert "no validated replacement EMA period" in report
    assert "UNQUANTIFIED" in report
    assert "PROPOSED_NOT_APPLIED" in report
    assert report.count("- Proposed test:") == len(result["items"])
    assert report.count("- Acceptance criteria:") == len(result["items"])
    assert report.count("- Proof:") == len(result["items"])


def test_observed_html_and_table_delimiters_are_not_rendered_as_markup():
    inputs = _inputs()
    inputs["operational"]["execution"]["LIVE"]["status_reason_counts"] = {
        '<script>alert(1)</script>|a\nb': 1,
    }
    report = render_improvement_opportunities(build_improvement_opportunities(**inputs))
    assert "<script>" not in report
    assert "&lt;script&gt;" in report
    assert "|a\nb" not in report


def test_legitimate_market_nonfill_is_not_called_an_execution_failure():
    inputs = _inputs()
    inputs["operational"]["execution"]["LIVE"] = {
        "orders": 1, "filled": 0, "cancelled": 1, "first_reason_signal_coverage": 1,
        "status_reason_counts": {"ENTRY_WINDOW_EXPIRED_TRIGGER_NOT_TOUCHED": 1},
    }
    item = _items(build_improvement_opportunities(**inputs))["execution_availability"]
    assert item["evidence_state"] == "MONITOR"
    assert "first-reason journal coverage" in str(item["observations"])


def test_absent_evidence_never_looks_like_a_pass_or_zero_failures():
    inputs = {key: ([] if isinstance(value, list) else {}) for key, value in _inputs().items()}
    payload = build_improvement_opportunities(**inputs)
    items = _items(payload)
    for key in ("prospective_validation", "prediction_validation", "setup_selection", "data_coverage"):
        assert items[key]["evidence_state"] == "UNAVAILABLE"
    report = render_improvement_opportunities(payload)
    assert "Missing selected-order timestamps: UNAVAILABLE" in report
    assert "Eligible predictions UNAVAILABLE/UNAVAILABLE" in report
    assert "0/0" not in report


def test_partial_reads_and_nonfinite_metrics_are_explicit():
    inputs = _inputs()
    inputs["operational"]["errors"] = ["live:status.json:JSONDecodeError"]
    inputs["prediction_metrics"]["fill_brier_score"] = float("nan")
    inputs["prediction_metrics"]["net_return_mae_pct_points"] = float("inf")
    report = render_improvement_opportunities(build_improvement_opportunities(**inputs))
    assert "1 read/parse errors" in report
    assert "live:status.json:JSONDecodeError" in report
    assert "fill Brier UNAVAILABLE" in report
    assert "return MAE UNAVAILABLE" in report
