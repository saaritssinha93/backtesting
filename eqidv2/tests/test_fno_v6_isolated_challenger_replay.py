from __future__ import annotations

from datetime import date
import json
import math

import pandas as pd
import pytest

from tools import fno_v6_isolated_challenger_replay as research


def _candidates() -> pd.DataFrame:
    rows = []
    for setup_id, values in (
        ("09:35_LONG", [("A", 0.30, 20.0), ("B", 0.55, 10.0)]),
        ("09:40_LONG", [("C", 0.25, 5.0), ("D", 0.45, 4.0)]),
    ):
        for rank, (symbol, move, picker_value) in enumerate(values, start=1):
            rows.append(
                {
                    "candidate_id": f"2026-08-27|{setup_id}|{symbol}",
                    "session_date": date(2026, 8, 27),
                    "signal_time": pd.Timestamp(
                        f"2026-08-27 {setup_id[:5]}", tz="Asia/Kolkata"
                    ),
                    "setup_id": setup_id,
                    "side": "LONG",
                    "symbol": symbol,
                    "price_change_pct": move,
                    "picker": "max_liquidity",
                    "picker_value": picker_value,
                    "traded_value": picker_value,
                    "frozen_rank": rank,
                }
            )
    return pd.DataFrame(rows)


def test_registry_is_predeclared_and_valid() -> None:
    research.validate_registry()
    assert len(research.CHALLENGERS) == 8
    assert research.CHALLENGERS[0].variant == "CONTROL"
    assert [item.scenario for item in research.COST_SCENARIOS] == [
        "BASE_15BPS_0SLIP",
        "STRESS_20BPS_2SLIP",
        "STRESS_25BPS_5SLIP",
    ]
    assert set(research.STRESS_VARIANT_NAMES) <= set(research.CHALLENGER_BY_NAME)


def test_control_preserves_all_candidates_and_ranks() -> None:
    source = _candidates()
    filtered, decisions = research.apply_selection_overlay(
        source, research.CHALLENGER_BY_NAME["CONTROL"]
    )
    assert set(filtered["candidate_id"]) == set(source["candidate_id"])
    assert decisions["selection_passed"].all()
    assert decisions["recalculated_frozen_rank"].tolist() == [1, 2, 1, 2]


def test_a1_filters_only_weak_0940_long() -> None:
    filtered, decisions = research.apply_selection_overlay(
        _candidates(), research.CHALLENGER_BY_NAME["A1_0940_LONG_MIN_040"]
    )
    assert set(filtered["symbol"]) == {"A", "B", "D"}
    rejected = decisions.loc[~decisions["selection_passed"]]
    assert rejected["symbol"].tolist() == ["C"]
    assert rejected["selection_reason"].tolist() == [
        "0940_LONG_MOVE_BELOW_MINIMUM"
    ]


def test_a2_filters_only_extended_0935_long_and_reranks() -> None:
    filtered, decisions = research.apply_selection_overlay(
        _candidates(), research.CHALLENGER_BY_NAME["A2_0935_LONG_MAX_050"]
    )
    assert set(filtered["symbol"]) == {"A", "C", "D"}
    rejected = decisions.loc[~decisions["selection_passed"]]
    assert rejected["symbol"].tolist() == ["B"]
    assert rejected["selection_reason"].tolist() == [
        "0935_LONG_MOVE_ABOVE_MAXIMUM"
    ]
    ranks = filtered.loc[
        filtered["setup_id"].eq("09:35_LONG"), "frozen_rank"
    ].tolist()
    assert ranks == [1]


def test_layered_variant_applies_both_isolated_rules() -> None:
    filtered, decisions = research.apply_selection_overlay(
        _candidates(),
        research.CHALLENGER_BY_NAME["A1_A2_0935_LONG_MAX_040"],
    )
    assert set(filtered["symbol"]) == {"A", "D"}
    reasons = dict(zip(decisions["symbol"], decisions["selection_reason"]))
    assert reasons["B"] == "0935_LONG_MOVE_ABOVE_MAXIMUM"
    assert reasons["C"] == "0940_LONG_MOVE_BELOW_MINIMUM"


def test_closed_parses_resumed_csv_booleans_safely() -> None:
    audit = pd.DataFrame(
        {
            "candidate_id": ["A", "B", "C", "D"],
            "filled": ["False", "TRUE", "0", "1"],
            "net_return_pct": [1.0, 2.0, 3.0, 4.0],
            "net_pnl_rs": [10.0, 20.0, 30.0, 40.0],
        }
    )
    assert research._closed(audit)["candidate_id"].tolist() == ["B", "D"]


def test_metric_drift_matches_equal_infinity_and_marks_missing_key() -> None:
    def row(variant: str, profit_factor: float) -> dict[str, object]:
        values: dict[str, object] = {
            "dataset": "TODAY",
            "period": "FULL",
            "variant": variant,
        }
        values.update({metric: 0.0 for metric in research._DRIFT_METRICS})
        values["profit_factor"] = profit_factor
        return values

    reference = pd.DataFrame([row("CONTROL", math.inf), row("OLD_ONLY", 1.0)])
    repaired = pd.DataFrame([row("CONTROL", math.inf)])
    drift = research._metric_drift(
        repaired,
        reference,
        keys=("dataset", "period", "variant"),
    )
    control = drift.loc[drift["variant"].eq("CONTROL")].iloc[0]
    assert bool(control["profit_factor_matches"])
    assert float(control["delta_profit_factor_repaired_minus_reference"]) == 0.0
    assert bool(control["row_matches"])
    missing = drift.loc[drift["variant"].eq("OLD_ONLY")].iloc[0]
    assert str(missing["row_presence"]) == "left_only"
    assert not bool(missing["row_matches"])


def test_candidate_state_drift_compares_filled_and_masks_unfilled_pnl() -> None:
    common = {
        "candidate_id": "A",
        "session_date": "2026-08-27",
        "setup_id": "09:35_LONG",
        "symbol": "ABC",
        "status": "CLOSED",
        "portfolio_decision": "FILLED",
        "entry_time": "2026-08-27T09:37:00+05:30",
        "entry_price": 100.0,
        "exit_time": "2026-08-27T09:50:00+05:30",
        "exit_price": 101.0,
        "exit_reason": "TARGET",
        "net_return_pct": 1.0,
    }
    reference = pd.DataFrame([{**common, "filled": "False", "net_pnl_rs": 999.0}])
    repaired = pd.DataFrame([{**common, "filled": "TRUE", "net_pnl_rs": 10.0}])
    summary, detail = research._reference_audit_drift(
        reference,
        repaired,
        dataset="TODAY",
        variant="CONTROL",
    )
    assert summary["reference_fills"] == 0
    assert summary["repaired_fills"] == 1
    assert summary["reference_net_pnl_rs"] == 0.0
    assert summary["delta_net_pnl_rs_repaired_minus_reference"] == 10.0
    assert len(detail) == 1


def test_snapshot_identity_rejects_august_snapshot_in_today_slot(tmp_path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": "fno_backtest_source_snapshot_v1",
                "complete": True,
                "physical_copy": True,
                "universe": {
                    "master_date": "2026-08-11",
                    "contract_month_filter": "26AUG",
                    "mapped_stock_futures": 208,
                },
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="master date"):
        research._snapshot_identity(
            manifest,
            expected_master_date="2026-08-27",
            expected_contract_month_filter="26SEP",
            expected_mapped_stock_futures=210,
        )


def test_replay_completion_detects_tampered_output(tmp_path) -> None:
    output = tmp_path / "audit.csv"
    output.write_text("a\n1\n", encoding="utf-8")
    binding = {"variant": "CONTROL"}
    outputs = {"audit": output}
    completion_path = tmp_path / "completion.json"
    completion_path.write_text(
        json.dumps(
            research._replay_completion_payload(
                binding=binding,
                output_paths=outputs,
            )
        ),
        encoding="utf-8",
    )
    research._verify_replay_completion(
        completion_path,
        expected_binding=binding,
        output_paths=outputs,
    )
    output.write_text("a\n2\n", encoding="utf-8")
    with pytest.raises(AssertionError, match="SHA-256 changed"):
        research._verify_replay_completion(
            completion_path,
            expected_binding=binding,
            output_paths=outputs,
        )


def test_direct_parity_bundle_computes_both_before_publication(
    tmp_path, monkeypatch
) -> None:
    events: list[str] = []

    def fake_run(candidates, minute_paths, *, variant, policy):
        dataset = str(candidates.iloc[0]["dataset"])
        events.append(f"run:{dataset}")
        return pd.DataFrame({"candidate_id": [dataset]})

    original_write = research.common.atomic_write_csv

    def tracked_write(frame, path):
        events.append(f"write:{path.name}")
        original_write(frame, path)

    def fake_parity(replay, direct, **kwargs):
        return {"passed": True, "dataset": kwargs["dataset"]}

    monkeypatch.setattr(research.engine, "run_v8_backtest", fake_run)
    monkeypatch.setattr(research.common, "atomic_write_csv", tracked_write)
    monkeypatch.setattr(research, "_fresh_control_parity", fake_parity)
    result = research._fresh_control_parity_bundle(
        historical_candidates=pd.DataFrame({"dataset": ["HISTORICAL"]}),
        historical_paths=pd.DataFrame(),
        historical_manifest={},
        historical_coverage=pd.DataFrame(),
        today_candidates=pd.DataFrame({"dataset": ["TODAY"]}),
        today_paths=pd.DataFrame(),
        today_manifest={},
        today_coverage=pd.DataFrame(),
        replay_results={
            ("HISTORICAL", "CONTROL"): {"audit": pd.DataFrame()},
            ("TODAY", "CONTROL"): {"audit": pd.DataFrame()},
        },
        policy=object(),
        target=tmp_path,
    )
    assert events[:2] == ["run:HISTORICAL", "run:TODAY"]
    assert events[2:] == [
        "write:historical_direct_strict_v6_audit.csv",
        "write:today_direct_strict_v6_audit.csv",
    ]
    assert result["historical"]["dataset"] == "HISTORICAL"
    assert result["today"]["dataset"] == "TODAY"


def test_reference_drift_report_uses_fixture_aggregation_tables() -> None:
    metric = pd.DataFrame(
        [
            {
                "dataset": "HISTORICAL",
                "period": "FULL",
                "variant": "CONTROL",
                "reference_fills": 1,
                "repaired_fills": 2,
                "delta_fills_repaired_minus_reference": 1,
                "reference_profit_factor": 1.0,
                "repaired_profit_factor": 2.0,
                "delta_profit_factor_repaired_minus_reference": 1.0,
                "reference_net_return_points": 1.0,
                "repaired_net_return_points": 2.0,
                "delta_net_return_points_repaired_minus_reference": 1.0,
                "reference_net_pnl_rs": 10.0,
                "repaired_net_pnl_rs": 20.0,
                "delta_net_pnl_rs_repaired_minus_reference": 10.0,
            }
        ]
    )
    cost = metric.assign(cost_scenario="STRESS_20BPS_2SLIP")
    state = pd.DataFrame(
        [
            {
                "dataset": "HISTORICAL",
                "variant": "CONTROL",
                "candidates_added": 0,
                "candidates_removed": 0,
                "common_candidate_states_changed": 1,
                "fills_added": 1,
                "fills_removed": 0,
                "delta_net_pnl_rs_repaired_minus_reference": 10.0,
            }
        ]
    )
    cost_state = state.assign(cost_scenario="STRESS_20BPS_2SLIP")
    report = research._reference_drift_markdown(
        metric, cost, state, cost_state
    )
    assert "Repaired-data drift versus frozen v2" in report
    assert "Base candidate-state drift" in report
    assert "STRESS_20BPS_2SLIP" in report
