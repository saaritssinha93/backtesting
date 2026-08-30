from __future__ import annotations

import ast
import hashlib
import inspect
import json
import textwrap
from collections import Counter
from datetime import date

import pandas as pd
import pytest

import fno_v11_execution_runtime as execution_runtime
import fno_v11_staged_backtest as staged


SCENARIOS = ("REFERENCE_15_0", "STRESS_20_2", "STRESS_25_5")


def test_catalog_is_unique_and_has_the_reviewed_stage_counts() -> None:
    staged._validate_catalog()

    definitions = staged.ALL_PREDECLARED_EXPERIMENTS
    identifiers = [definition.variant_id for definition in definitions]
    assert definitions[0] is staged.CONTROL
    assert identifiers[0] == "V10_STAGE0_FROZEN_CONTROL"
    assert len(identifiers) == len(set(identifiers)) == 30
    assert Counter(definition.stage_id for definition in definitions) == {
        "STAGE_00_FROZEN_V10": 1,
        "STAGE_03_REBASELINE": 1,
        "STAGE_04_ENTRY_TIMING": 2,
        "STAGE_05_SELECTION": 10,
        "STAGE_06_SETUP_PICKER_CAP": 9,
        "STAGE_07_EXIT_AND_GAP": 6,
        "STAGE_09_PORTFOLIO": 1,
    }


def test_stage0_remains_frozen_and_stage3_is_a_distinct_v11_rebaseline() -> None:
    assert staged.ALL_PREDECLARED_EXPERIMENTS[:2] == (
        staged.CONTROL,
        staged.DEVELOPMENT_BASELINE,
    )
    assert staged.CONTROL.variant_id == "V10_STAGE0_FROZEN_CONTROL"
    assert staged.CONTROL.stage_id == "STAGE_00_FROZEN_V10"
    assert staged.CONTROL.family == "CONTROL"
    assert staged.CONTROL.is_control
    assert not staged.CONTROL.is_development_baseline
    assert staged.DEVELOPMENT_BASELINE.variant_id == (
        "V11_STAGE3_DETERMINISTIC_GAP_REBASELINE"
    )
    assert staged.DEVELOPMENT_BASELINE.stage_id == "STAGE_03_REBASELINE"
    assert not staged.DEVELOPMENT_BASELINE.is_control
    assert staged.DEVELOPMENT_BASELINE.is_development_baseline
    assert (
        staged.DEVELOPMENT_BASELINE.registry_variant_id
        == staged.CONTROL.registry_variant_id
    )
    assert staged.DEVELOPMENT_BASELINE.gap_variant == staged.CONTROL.gap_variant


def test_catalog_validates_every_frozen_v10_reference_binding_hash(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    audit_relative = "scenarios/reference_15_0/candidate_order_audit.csv"
    audit_payload = b"candidate_id,net_pnl_rs\nC1,1.0\n"
    audit_sha256 = hashlib.sha256(audit_payload).hexdigest()
    inventory_payload = json.dumps(
        {
            "schema_version": staged.FROZEN_V10_REFERENCE_SCHEMA_VERSION,
            "artifacts": [
                {
                    "relative_path": audit_relative,
                    "bytes": len(audit_payload),
                    "sha256": audit_sha256,
                }
            ],
        },
        sort_keys=True,
    ).encode()
    inventory_sha256 = hashlib.sha256(inventory_payload).hexdigest()
    provenance_payload = json.dumps(
        {
            "schema_version": staged.FROZEN_V10_REFERENCE_SCHEMA_VERSION,
            "complete": True,
            "command": [
                "python",
                "fno_v10_backtest.py",
                "--reference-only",
                "--all-usable-history",
            ],
            "scenarios": {staged.FROZEN_STAGE0_SCENARIO: {}},
            "artifact_inventory": {"sha256": inventory_sha256},
        },
        sort_keys=True,
    ).encode()
    payloads = {
        "provenance.json": provenance_payload,
        "artifact_inventory.json": inventory_payload,
        audit_relative: audit_payload,
    }
    expected: dict[str, str] = {}
    for relative, payload in payloads.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        expected[relative] = hashlib.sha256(payload).hexdigest()

    monkeypatch.setattr(staged, "FROZEN_V10_REFERENCE_RUN", tmp_path)
    monkeypatch.setattr(staged, "FROZEN_V10_REFERENCE_BINDING_SHA256", expected)

    staged._validate_catalog()

    bound_audit = tmp_path / "scenarios/reference_15_0/candidate_order_audit.csv"
    bound_audit.write_bytes(b"tampered\n")
    with pytest.raises(AssertionError, match="(?i)frozen.*binding|hash.*drift"):
        staged._validate_catalog()


@pytest.mark.parametrize("workers", [1, 4])
def test_worker_pool_always_uses_a_fresh_spawn_process_per_variant(
    workers: int,
) -> None:
    kwargs = staged._worker_pool_kwargs(workers)

    assert kwargs["max_workers"] == workers
    assert kwargs["mp_context"].get_start_method() == "spawn"
    assert kwargs["initializer"] is staged._worker_initialize
    assert kwargs["max_tasks_per_child"] == 1


def test_run_all_publishes_complete_provenance_once_after_inventory_validation() -> None:
    tree = ast.parse(textwrap.dedent(inspect.getsource(staged.run_all)))
    provenance_writes: list[ast.Call] = []
    inventory_validations: list[ast.Call] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "atomic_write_json"
            and node.args
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id == "provenance_path"
        ):
            provenance_writes.append(node)
        if (
            isinstance(node.func, ast.Name)
            and node.func.id == "_validate_completed_run_artifact_inventory"
        ):
            inventory_validations.append(node)

    assert len(provenance_writes) == 1
    assert len(inventory_validations) == 1
    assert inventory_validations[0].lineno < provenance_writes[0].lineno


def test_blocked_validity_tests_are_explicit_unique_and_stage_complete() -> None:
    records = staged.BLOCKED_TESTS
    identities = [(record["stage_id"], record["test_id"]) for record in records]

    assert len(records) == len(set(identities)) == 9
    assert {record["status"] for record in records} == {"BLOCKED_VALIDITY"}
    assert all(record["reason"].strip() for record in records)
    assert Counter(record["stage_id"] for record in records) == {
        "STAGE_01_DATA_VALIDITY": 3,
        "STAGE_02_FUTURES_EXECUTION": 2,
        "STAGE_08_STRUCTURAL_FILTERS": 3,
        "STAGE_09_PORTFOLIO": 1,
    }
    assert (
        "STAGE_09_PORTFOLIO",
        "ACTUAL_FUTURES_RISK_SIZING",
    ) in identities


def test_periods_are_sorted_deduplicated_and_split_at_the_frozen_extension() -> None:
    sessions = (
        date(2026, 8, 21),
        date(2026, 7, 31),
        date(2026, 8, 20),
        date(2026, 6, 30),
        date(2026, 8, 19),
        date(2026, 8, 21),
    )
    segments = (
        {
            "segment": {"segment_id": "SYNTHETIC_A"},
            "sessions": ["2026-06-30", "2026-08-20"],
        },
    )

    periods = dict(staged._periods(sessions, segments))

    ordered = (
        date(2026, 6, 30),
        date(2026, 7, 31),
        date(2026, 8, 19),
        date(2026, 8, 20),
        date(2026, 8, 21),
    )
    assert periods["FULL_USABLE"] == ordered
    assert periods["CORE_59"] == ordered[:3]
    assert periods["FORWARD_EXTENSION"] == ordered[3:]
    assert periods["FIRST_HALF"] == ordered[:2]
    assert periods["SECOND_HALF"] == ordered[2:]
    assert periods["LAST_14"] == ordered
    assert periods["MONTH_2026_06"] == ordered[:1]
    assert periods["MONTH_2026_07"] == ordered[1:2]
    assert periods["MONTH_2026_08"] == ordered[2:]
    assert periods["SEGMENT_SYNTHETIC_A"] == (
        date(2026, 6, 30),
        date(2026, 8, 20),
    )


def _variant_metric_rows(
    variant_id: str,
    *,
    full_net: tuple[float, float, float],
    full_pf: tuple[float, float, float],
    reference_mdd: float,
    forward_net: tuple[float, float, float],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, scenario in enumerate(SCENARIOS):
        rows.append(
            {
                "variant_id": variant_id,
                "stage_id": "STAGE_TEST",
                "family": "SYNTHETIC",
                "period": "FULL_USABLE",
                "scenario": scenario,
                "net_return_points": full_net[index],
                "profit_factor": full_pf[index],
                "max_daily_drawdown_points": (
                    reference_mdd if scenario == "REFERENCE_15_0" else 999.0
                ),
            }
        )
        rows.append(
            {
                "variant_id": variant_id,
                "stage_id": "STAGE_TEST",
                "family": "SYNTHETIC",
                "period": "FORWARD_EXTENSION",
                "scenario": scenario,
                "net_return_points": forward_net[index],
                "profit_factor": 1.0,
                "max_daily_drawdown_points": 0.0,
            }
        )
    return rows


def test_development_gates_require_all_cost_cases_and_forward_cases() -> None:
    rows = [
        row
        for row in _variant_metric_rows(
            staged.CONTROL.variant_id,
            full_net=(120.0, 100.0, 80.0),
            full_pf=(2.5, 2.2, 2.0),
            reference_mdd=8.0,
            forward_net=(5.0, 3.0, 1.0),
        )
        if row["scenario"] == staged.FROZEN_STAGE0_SCENARIO
    ]
    rows += _variant_metric_rows(
        staged.DEVELOPMENT_BASELINE.variant_id,
        full_net=(100.0, 80.0, 60.0),
        full_pf=(2.0, 1.8, 1.6),
        reference_mdd=10.0,
        forward_net=(5.0, 3.0, 1.0),
    )
    rows += _variant_metric_rows(
        "V11_PASS_AT_EXACT_BOUNDARIES",
        full_net=(100.0, 80.0, 60.0),
        full_pf=(2.0, 1.8, 1.6),
        reference_mdd=10.5,
        forward_net=(0.0, 0.0, 0.0),
    )
    rows += _variant_metric_rows(
        "V11_FAILS_IN_DIFFERENT_SCENARIOS",
        full_net=(99.0, 90.0, 70.0),
        full_pf=(2.1, 1.79, 1.7),
        reference_mdd=10.500001,
        forward_net=(1.0, 1.0, -0.000001),
    )

    result = staged._development_gates(pd.DataFrame(rows)).set_index("variant_id")

    baseline = result.loc[staged.DEVELOPMENT_BASELINE.variant_id]
    assert bool(baseline["development_gate_passed"])
    assert not bool(baseline["development_improvement_passed"])
    assert baseline["gate_classification"] == "DETERMINISTIC_COMPARISON_BASELINE"
    assert (
        baseline["comparison_baseline_variant_id"]
        == staged.DEVELOPMENT_BASELINE.variant_id
    )
    passing = result.loc["V11_PASS_AT_EXACT_BOUNDARIES"]
    assert bool(passing["development_gate_passed"])
    assert bool(passing["development_improvement_passed"])
    assert passing["failed_check_count"] == 0
    assert passing["worst_case_net_ratio_vs_baseline"] == pytest.approx(1.0)
    assert passing["reference_net_ratio_vs_frozen_stage0"] == pytest.approx(
        100.0 / 120.0
    )
    assert passing["reference_mdd_ratio_vs_baseline"] == pytest.approx(1.05)
    assert passing["reference_mdd_ratio_vs_frozen_stage0"] == pytest.approx(
        1.3125
    )

    failing = result.loc["V11_FAILS_IN_DIFFERENT_SCENARIOS"]
    assert not bool(failing["development_gate_passed"])
    assert failing["failed_check_count"] == 4
    assert set(str(failing["failed_checks"]).split(";")) == {
        "net_at_least_baseline_REFERENCE_15_0",
        "pf_at_least_baseline_STRESS_20_2",
        "forward_nonnegative_STRESS_25_5",
        "reference_mdd_within_105pct_baseline",
    }
    assert not bool(failing["promotion_gate_passed"])


def test_stage0_gate_row_accepts_reference_only_but_v11_uses_all_cost_cases() -> None:
    stage0_rows = _variant_metric_rows(
        staged.CONTROL.variant_id,
        full_net=(120.0, 100.0, 80.0),
        full_pf=(2.5, 2.2, 2.0),
        reference_mdd=8.0,
        forward_net=(5.0, 3.0, 1.0),
    )
    stage0_reference_only = [
        row for row in stage0_rows if row["scenario"] == "REFERENCE_15_0"
    ]
    stage3_rows = _variant_metric_rows(
        staged.DEVELOPMENT_BASELINE.variant_id,
        full_net=(100.0, 80.0, 60.0),
        full_pf=(2.0, 1.8, 1.6),
        reference_mdd=10.0,
        forward_net=(5.0, 3.0, 1.0),
    )
    challenger_rows = _variant_metric_rows(
        "V11_ALL_SCENARIO_CHALLENGER",
        full_net=(101.0, 81.0, 61.0),
        full_pf=(2.1, 1.9, 1.7),
        reference_mdd=10.0,
        forward_net=(1.0, 1.0, 1.0),
    )

    result = staged._development_gates(
        pd.DataFrame(stage0_reference_only + stage3_rows + challenger_rows)
    ).set_index("variant_id")

    stage0 = result.loc[staged.CONTROL.variant_id]
    challenger = result.loc["V11_ALL_SCENARIO_CHALLENGER"]
    assert stage0["gate_classification"] == "FROZEN_LEGACY_CONTROL_REFERENCE_ONLY"
    assert not bool(stage0["development_gate_passed"])
    assert not bool(stage0["development_improvement_passed"])
    assert bool(challenger["development_improvement_passed"])


@pytest.mark.parametrize("incomplete_variant", ["STAGE3", "CHALLENGER"])
def test_stage3_and_v11_challengers_still_require_all_three_cost_scenarios(
    incomplete_variant: str,
) -> None:
    stage0_rows = [
        row
        for row in _variant_metric_rows(
            staged.CONTROL.variant_id,
            full_net=(120.0, 100.0, 80.0),
            full_pf=(2.5, 2.2, 2.0),
            reference_mdd=8.0,
            forward_net=(5.0, 3.0, 1.0),
        )
        if row["scenario"] == "REFERENCE_15_0"
    ]
    stage3_rows = _variant_metric_rows(
        staged.DEVELOPMENT_BASELINE.variant_id,
        full_net=(100.0, 80.0, 60.0),
        full_pf=(2.0, 1.8, 1.6),
        reference_mdd=10.0,
        forward_net=(5.0, 3.0, 1.0),
    )
    challenger_rows = _variant_metric_rows(
        "V11_INCOMPLETE_CHALLENGER",
        full_net=(101.0, 81.0, 61.0),
        full_pf=(2.1, 1.9, 1.7),
        reference_mdd=10.0,
        forward_net=(1.0, 1.0, 1.0),
    )
    target = stage3_rows if incomplete_variant == "STAGE3" else challenger_rows
    target[:] = [
        row for row in target if row["scenario"] != "STRESS_25_5"
    ]

    with pytest.raises(AssertionError, match="(?i)incomplete"):
        staged._development_gates(
            pd.DataFrame(stage0_rows + stage3_rows + challenger_rows)
        )


def _definition(
    variant_id: str,
    family: str,
    **kwargs: object,
) -> staged.ExperimentDefinition:
    return staged.ExperimentDefinition(
        variant_id,
        "STAGE_TEST",
        family,
        f"Synthetic {variant_id}",
        **kwargs,
    )


def _gate_record(
    definition: staged.ExperimentDefinition,
    *,
    improvement_passed: bool,
) -> dict[str, object]:
    return {
        "variant_id": definition.variant_id,
        "development_gate_passed": improvement_passed,
        "development_improvement_passed": improvement_passed,
        "is_control": definition.is_control,
        "is_development_baseline": definition.is_development_baseline,
    }


def test_combination_compatibility_rejects_overlapping_mechanism_families() -> None:
    neutral_a = _definition("A", "A")
    neutral_b = _definition("B", "B")
    assert staged._compatible_for_combination(neutral_a, neutral_b)
    assert not staged._compatible_for_combination(
        neutral_a, _definition("SAME", "A")
    )
    assert not staged._compatible_for_combination(
        _definition("REG_A", "A", registry_variant_id="REG_A"),
        _definition("REG_B", "B", registry_variant_id="REG_B"),
    )
    assert not staged._compatible_for_combination(
        _definition(
            "ENTRY_A",
            "A",
            runtime_spec=execution_runtime.RuntimeSpec(
                entry_setup_id="09:30_SHORT", entry_not_before_minute=3
            ),
        ),
        _definition(
            "EXIT_B",
            "B",
            runtime_spec=execution_runtime.RuntimeSpec(
                exit_rule="BREAK_EVEN_NEXT_BAR", exit_activation_r=1.0
            ),
        ),
    )
    assert not staged._compatible_for_combination(
        _definition("GAP_A", "A", gap_variant="REJECT_ALL_GAP_FILLS"),
        _definition("GAP_B", "B", gap_variant="REJECT_ALL_GAP_FILLS"),
    )
    assert not staged._compatible_for_combination(
        _definition("OFF_A", "A", disabled_setup_id="09:30_SHORT"),
        _definition("OFF_B", "B", disabled_setup_id="09:40_SHORT"),
    )


def test_combination_compatibility_allows_only_disjoint_entry_and_portfolio_runtime() -> None:
    entry = _definition(
        "ENTRY",
        "ENTRY_TIMING",
        runtime_spec=execution_runtime.RuntimeSpec(
            entry_setup_id="09:30_SHORT", entry_not_before_minute=3
        ),
    )
    portfolio = _definition(
        "PORTFOLIO",
        "PORTFOLIO_SYMBOL_LIMIT",
        runtime_spec=execution_runtime.RuntimeSpec(same_side_symbol_limit=2),
    )
    exit_rule = _definition(
        "EXIT",
        "EXIT_RULE",
        runtime_spec=execution_runtime.RuntimeSpec(
            exit_rule="BREAK_EVEN_NEXT_BAR", exit_activation_r=1.0
        ),
    )

    assert staged._compatible_for_combination(entry, portfolio)
    assert staged._compatible_for_combination(portfolio, entry)
    assert not staged._compatible_for_combination(entry, exit_rule)
    assert not staged._compatible_for_combination(portfolio, exit_rule)


@pytest.mark.parametrize(
    "spec",
    [
        execution_runtime.RuntimeSpec(
            entry_setup_id="09:30_SHORT",
            entry_not_before_minute=3,
            exit_rule="BREAK_EVEN_NEXT_BAR",
            exit_activation_r=1.0,
        ),
        execution_runtime.RuntimeSpec(
            exit_rule="BREAK_EVEN_NEXT_BAR",
            exit_activation_r=1.0,
            same_side_symbol_limit=2,
        ),
        execution_runtime.RuntimeSpec(
            entry_setup_id="09:30_SHORT",
            entry_not_before_minute=3,
            exit_rule="BREAK_EVEN_NEXT_BAR",
            exit_activation_r=1.0,
            same_side_symbol_limit=2,
        ),
    ],
)
def test_composite_opt_in_still_rejects_every_other_runtime_pairing(
    spec: execution_runtime.RuntimeSpec,
) -> None:
    with pytest.raises(ValueError, match="only permitted post-hoc composite"):
        spec.validate(allow_composite=True)


def test_combined_definition_uses_first_compatible_passers_and_records_components() -> None:
    selection = _definition(
        "V11_SELECTION_PASSER",
        "FIVE_MINUTE_SELECTION",
        registry_variant_id="V11_S1_0930_SHORT_BREADTH_MIN_3",
    )
    entry_runtime = execution_runtime.RuntimeSpec(
        entry_setup_id="09:30_SHORT", entry_not_before_minute=3
    )
    entry = _definition(
        "V11_ENTRY_PASSER",
        "ENTRY_TIMING",
        runtime_spec=entry_runtime,
    )
    gates = pd.DataFrame(
        [
            _gate_record(selection, improvement_passed=True),
            _gate_record(entry, improvement_passed=True),
            _gate_record(staged.CONTROL, improvement_passed=False),
        ]
    )

    combined = staged._combined_definition(
        gates,
        {
            selection.variant_id: selection,
            entry.variant_id: entry,
            staged.CONTROL.variant_id: staged.CONTROL,
        },
    )

    assert combined is not None
    digest = hashlib.sha256(
        f"{selection.variant_id}|{entry.variant_id}".encode()
    ).hexdigest()[:10].upper()
    assert combined.variant_id == f"V11_S10_POST_HOC_TOP2_{digest}"
    assert combined.component_variant_ids == (
        selection.variant_id,
        entry.variant_id,
    )
    assert combined.registry_variant_id == selection.registry_variant_id
    assert combined.runtime_spec == entry_runtime
    assert combined.gap_variant == staged.BASE_GAP_VARIANT
    assert combined.disabled_setup_id is None
    assert combined.post_hoc is True
    assert combined.payload()["promotion_eligible"] is False


def test_combined_definition_returns_none_without_a_compatible_pair() -> None:
    first = _definition("FIRST", "SAME")
    second = _definition("SECOND", "SAME")
    gates = pd.DataFrame(
        [
            _gate_record(first, improvement_passed=True),
            _gate_record(second, improvement_passed=True),
        ]
    )
    assert staged._combined_definition(
        gates, {first.variant_id: first, second.variant_id: second}
    ) is None


def test_stage10_never_uses_stage0_or_stage3_as_components() -> None:
    selection = _definition(
        "V11_SELECTION_PASSER",
        "FIVE_MINUTE_SELECTION",
        registry_variant_id="V11_S1_0930_SHORT_BREADTH_MIN_3",
    )
    entry = _definition(
        "V11_ENTRY_PASSER",
        "ENTRY_TIMING",
        runtime_spec=execution_runtime.RuntimeSpec(
            entry_setup_id="09:30_SHORT", entry_not_before_minute=3
        ),
    )
    definitions = {
        definition.variant_id: definition
        for definition in (
            staged.CONTROL,
            staged.DEVELOPMENT_BASELINE,
            selection,
            entry,
        )
    }
    baseline_rows = [
        {
            **_gate_record(staged.CONTROL, improvement_passed=True),
            "development_improvement_passed": True,
        },
        {
            **_gate_record(staged.DEVELOPMENT_BASELINE, improvement_passed=True),
            "development_improvement_passed": True,
        },
    ]
    gates = pd.DataFrame(
        baseline_rows
        + [
            _gate_record(selection, improvement_passed=True),
            _gate_record(entry, improvement_passed=True),
        ]
    )

    combined = staged._combined_definition(gates, definitions)

    assert combined is not None
    assert combined.component_variant_ids == (selection.variant_id, entry.variant_id)
    assert staged.CONTROL.variant_id not in combined.component_variant_ids
    assert staged.DEVELOPMENT_BASELINE.variant_id not in combined.component_variant_ids
    assert staged._combined_definition(pd.DataFrame(baseline_rows), definitions) is None


@pytest.mark.parametrize("reverse", [False, True])
def test_combined_definition_merges_disjoint_entry_and_portfolio_runtime(
    reverse: bool,
) -> None:
    entry = _definition(
        "V11_ENTRY_PASSER",
        "ENTRY_TIMING",
        runtime_spec=execution_runtime.RuntimeSpec(
            entry_setup_id="09:30_SHORT", entry_not_before_minute=3
        ),
    )
    portfolio = _definition(
        "V11_PORTFOLIO_PASSER",
        "PORTFOLIO_SYMBOL_LIMIT",
        runtime_spec=execution_runtime.RuntimeSpec(same_side_symbol_limit=2),
    )
    ordered = (portfolio, entry) if reverse else (entry, portfolio)
    gates = pd.DataFrame(
        [
            _gate_record(definition, improvement_passed=True)
            for definition in ordered
        ]
    )

    combined = staged._combined_definition(
        gates, {entry.variant_id: entry, portfolio.variant_id: portfolio}
    )

    assert combined is not None
    assert combined.component_variant_ids == tuple(
        definition.variant_id for definition in ordered
    )
    assert combined.runtime_spec.active_mechanisms == (
        "ENTRY_NOT_BEFORE",
        "PORTFOLIO_SYMBOL_LIMIT",
    )
    assert combined.runtime_spec.entry_setup_id == "09:30_SHORT"
    assert combined.runtime_spec.entry_not_before_minute == 3
    assert combined.runtime_spec.exit_rule is None
    assert combined.runtime_spec.exit_activation_r is None
    assert combined.runtime_spec.same_side_symbol_limit == 2
    with pytest.raises(ValueError, match="isolated"):
        combined.runtime_spec.validate()
    combined.runtime_spec.validate(allow_composite=True)


def test_stage0_parity_fallback_allows_only_status_and_reason_drift(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    rows = [
        {
            "candidate_id": "C2",
            "filled": False,
            "entry_time": None,
            "entry_price": None,
            "stop_price": None,
            "target_price": None,
            "exit_time": None,
            "exit_price": None,
            "exit_reason": None,
            "gross_return_pct": 0.0,
            "net_return_pct": 0.0,
            "quantity": 0.0,
            "gross_pnl_rs": 0.0,
            "estimated_cost_rs": 0.0,
            "net_pnl_rs": 0.0,
            "status": "REJECTED",
            "reason": "OLD_TERMINAL_LABEL",
        },
        {
            "candidate_id": "C1",
            "filled": True,
            "entry_time": "2026-08-20 09:33:00+05:30",
            "entry_price": 100.0,
            "stop_price": 99.0,
            "target_price": 102.0,
            "exit_time": "2026-08-20 10:00:00+05:30",
            "exit_price": 102.0,
            "exit_reason": "TARGET",
            "gross_return_pct": 2.0,
            "net_return_pct": 1.85,
            "quantity": 500.0,
            "gross_pnl_rs": 1000.0,
            "estimated_cost_rs": 75.0,
            "net_pnl_rs": 925.0,
            "status": "CLOSED",
            "reason": "TARGET",
        },
    ]
    reference = pd.DataFrame(rows)
    reference_path = tmp_path / "frozen_stage0.csv"
    reference.to_csv(reference_path, index=False)
    observed = reference.copy(deep=True)
    observed.loc[observed["candidate_id"].eq("C2"), "status"] = "EXPIRED"
    observed.loc[observed["candidate_id"].eq("C2"), "reason"] = (
        "NEW_TERMINAL_LABEL"
    )

    def strict_parity_fails(*_args, **_kwargs):
        raise AssertionError("strict status mismatch")

    monkeypatch.setattr(staged.gaps, "validate_control_parity", strict_parity_fails)

    result = staged._control_parity_record(observed, reference_path)

    assert result["passed"] is True
    assert result["parity_level"] == (
        "ECONOMIC_PLUS_UNFILLED_STATUS_REASON_ALLOWLIST"
    )
    assert result["legacy_parity_contract_passed"] is False
    assert result["status_reason_only_fallback_passed"] is True
    assert result["economic_parity"] is True
    assert result["status_mismatches"] == 1
    assert result["reason_mismatches"] == 1
    assert result["non_economic_status_reason_mismatches"] == 1
    assert result["mismatch_candidate_ids"] == ["C2"]


def test_stage0_parity_fallback_rejects_status_drift_on_a_filled_candidate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    reference = pd.DataFrame(
        [
            {
                "candidate_id": "C1",
                "filled": True,
                "entry_time": "2026-08-20 09:33:00+05:30",
                "entry_price": 100.0,
                "stop_price": 99.0,
                "target_price": 102.0,
                "exit_time": "2026-08-20 10:00:00+05:30",
                "exit_price": 102.0,
                "exit_reason": "TARGET",
                "gross_return_pct": 2.0,
                "net_return_pct": 1.85,
                "quantity": 500.0,
                "gross_pnl_rs": 1000.0,
                "estimated_cost_rs": 75.0,
                "net_pnl_rs": 925.0,
                "status": "CLOSED",
                "reason": "TARGET",
            }
        ]
    )
    reference_path = tmp_path / "frozen_stage0.csv"
    reference.to_csv(reference_path, index=False)
    observed = reference.copy(deep=True)
    observed.loc[0, "status"] = "OTHER_FILLED_STATUS"

    monkeypatch.setattr(
        staged.gaps,
        "validate_control_parity",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("strict mismatch")
        ),
    )

    with pytest.raises(AssertionError, match="filled candidate"):
        staged._control_parity_record(observed, reference_path)


def test_stage0_parity_fallback_fails_on_an_economic_difference(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    reference = pd.DataFrame(
        [
            {
                "candidate_id": "C1",
                "filled": True,
                "entry_time": "2026-08-20 09:33:00+05:30",
                "entry_price": 100.0,
                "stop_price": 99.0,
                "target_price": 102.0,
                "exit_time": "2026-08-20 10:00:00+05:30",
                "exit_price": 102.0,
                "exit_reason": "TARGET",
                "gross_return_pct": 2.0,
                "net_return_pct": 1.85,
                "quantity": 500.0,
                "gross_pnl_rs": 1000.0,
                "estimated_cost_rs": 75.0,
                "net_pnl_rs": 925.0,
                "status": "CLOSED",
                "reason": "TARGET",
            }
        ]
    )
    reference_path = tmp_path / "frozen_stage0.csv"
    reference.to_csv(reference_path, index=False)
    observed = reference.copy(deep=True)
    observed.loc[0, "net_pnl_rs"] = 924.0

    monkeypatch.setattr(
        staged.gaps,
        "validate_control_parity",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("strict mismatch")
        ),
    )

    with pytest.raises(AssertionError, match="economic parity failed"):
        staged._control_parity_record(observed, reference_path)


def _write_minimal_determinism_run(
    root,
    *,
    scenario_order: tuple[str, ...],
    reference_economic_parity: bool,
    include_provenance: bool,
    input_binding_sha256: str = "synthetic-input-binding",
) -> None:
    root.mkdir(parents=True)
    (root / "source").mkdir()
    for filename in (
        "all_input_candidates.csv",
        "v10_stage0_selected_candidates.csv",
        "v10_stage0_selection_decisions.csv",
        "source_segments.json",
    ):
        (root / filename).write_bytes(b"same-across-runs\n")
    parity_path = (
        root
        / "stages"
        / staged.CONTROL.stage_id
        / staged.CONTROL.variant_id
        / "control_parity.json"
    )
    parity_path.parent.mkdir(parents=True)
    parity_path.write_text(
        json.dumps(
            {
                "REFERENCE_15_0": {
                    "economic_parity": reference_economic_parity,
                    "parity_level": "PINNED_REFERENCE_ONLY",
                }
            }
        ),
        encoding="utf-8",
    )
    if include_provenance:
        inventory_path = root / "artifact_inventory.json"
        records = []
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.name in {
                "artifact_inventory.json",
                "provenance.json",
            }:
                continue
            payload = path.read_bytes()
            records.append(
                {
                    "relative_path": str(path.relative_to(root)).replace("\\", "/"),
                    "bytes": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                }
            )
        inventory_payload = json.dumps(
            {
                "schema_version": staged.SCHEMA_VERSION,
                "artifacts": records,
            },
            sort_keys=True,
        ).encode()
        inventory_path.write_bytes(inventory_payload)
        provenance_payload = json.dumps(
            {
                "complete": True,
                "schema_version": staged.SCHEMA_VERSION,
                "scenario_order": list(scenario_order),
                "v11_scenario_order": list(scenario_order),
                "stage0_scenario_order": ["REFERENCE_15_0"],
                "input_binding_sha256": input_binding_sha256,
                "frozen_v10_reference_binding": {
                    "validated": True,
                    "bound_file_sha256": dict(
                        staged.FROZEN_V10_REFERENCE_BINDING_SHA256
                    ),
                },
                "artifact_inventory": {
                    "path": str(inventory_path.resolve()),
                    "sha256": hashlib.sha256(inventory_payload).hexdigest(),
                },
                "source_hashes": {},
            }
        ).encode()
        provenance_path = root / "provenance.json"
        provenance_path.write_bytes(provenance_payload)
        (root.parent / "latest.json").write_text(
            json.dumps(
                {
                    "schema_version": staged.SCHEMA_VERSION,
                    "run_dir": str(root.resolve()),
                    "provenance_sha256": hashlib.sha256(
                        provenance_payload
                    ).hexdigest(),
                }
            ),
            encoding="utf-8",
        )


def test_determinism_requires_only_frozen_stage0_reference_parity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    prior = tmp_path / "prior"
    current = tmp_path / "current"
    current_order = SCENARIOS
    _write_minimal_determinism_run(
        prior,
        scenario_order=tuple(reversed(current_order)),
        reference_economic_parity=True,
        include_provenance=True,
    )
    _write_minimal_determinism_run(
        current,
        scenario_order=current_order,
        reference_economic_parity=True,
        include_provenance=False,
    )
    monkeypatch.setattr(
        staged,
        "_sorted_frame",
        lambda *_args, **_kwargs: pd.DataFrame({"same": [1]}),
    )

    result = staged._determinism_attestation(
        prior,
        current,
        current_order,
        "synthetic-input-binding",
    )

    assert result["passed"] is True
    assert result["frozen_stage0_attested_by_pinned_economic_contract"] is True
    assert result["frozen_stage0_parity_levels"] == {
        "prior": {"REFERENCE_15_0": "PINNED_REFERENCE_ONLY"},
        "current": {"REFERENCE_15_0": "PINNED_REFERENCE_ONLY"},
    }


def test_determinism_still_fails_when_stage0_reference_parity_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    prior = tmp_path / "prior"
    current = tmp_path / "current"
    current_order = SCENARIOS
    _write_minimal_determinism_run(
        prior,
        scenario_order=tuple(reversed(current_order)),
        reference_economic_parity=False,
        include_provenance=True,
    )
    _write_minimal_determinism_run(
        current,
        scenario_order=current_order,
        reference_economic_parity=True,
        include_provenance=False,
    )
    monkeypatch.setattr(
        staged,
        "_sorted_frame",
        lambda *_args, **_kwargs: pd.DataFrame({"same": [1]}),
    )

    with pytest.raises(AssertionError, match="REFERENCE_15_0"):
        staged._determinism_attestation(
            prior,
            current,
            current_order,
            "synthetic-input-binding",
        )


def test_determinism_rejects_a_tampered_prior_inventory_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    prior = tmp_path / "prior"
    current = tmp_path / "current"
    current_order = SCENARIOS
    _write_minimal_determinism_run(
        prior,
        scenario_order=tuple(reversed(current_order)),
        reference_economic_parity=True,
        include_provenance=True,
    )
    _write_minimal_determinism_run(
        current,
        scenario_order=current_order,
        reference_economic_parity=True,
        include_provenance=False,
    )
    # Same byte count as the inventoried payload, so the SHA check itself must
    # catch the modification.
    (prior / "all_input_candidates.csv").write_bytes(b"evil-across-runs\n")
    monkeypatch.setattr(
        staged,
        "_sorted_frame",
        lambda *_args, **_kwargs: pd.DataFrame({"same": [1]}),
    )

    with pytest.raises(AssertionError, match="artifact hash drifted"):
        staged._determinism_attestation(
            prior,
            current,
            current_order,
            "synthetic-input-binding",
        )


def test_determinism_rejects_a_file_omitted_from_prior_inventory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    prior = tmp_path / "prior"
    current = tmp_path / "current"
    current_order = SCENARIOS
    _write_minimal_determinism_run(
        prior,
        scenario_order=tuple(reversed(current_order)),
        reference_economic_parity=True,
        include_provenance=True,
    )
    _write_minimal_determinism_run(
        current,
        scenario_order=current_order,
        reference_economic_parity=True,
        include_provenance=False,
    )
    (prior / "unlisted-output.csv").write_text("unexpected\n", encoding="utf-8")
    monkeypatch.setattr(
        staged,
        "_sorted_frame",
        lambda *_args, **_kwargs: pd.DataFrame({"same": [1]}),
    )

    with pytest.raises(AssertionError, match="inventory set is incomplete"):
        staged._determinism_attestation(
            prior,
            current,
            current_order,
            "synthetic-input-binding",
        )


def test_determinism_rejects_an_input_binding_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    prior = tmp_path / "prior"
    current = tmp_path / "current"
    current_order = SCENARIOS
    _write_minimal_determinism_run(
        prior,
        scenario_order=tuple(reversed(current_order)),
        reference_economic_parity=True,
        include_provenance=True,
        input_binding_sha256="prior-input-binding",
    )
    _write_minimal_determinism_run(
        current,
        scenario_order=current_order,
        reference_economic_parity=True,
        include_provenance=False,
    )
    monkeypatch.setattr(
        staged,
        "_sorted_frame",
        lambda *_args, **_kwargs: pd.DataFrame({"same": [1]}),
    )

    with pytest.raises(AssertionError, match="different input bindings"):
        staged._determinism_attestation(
            prior,
            current,
            current_order,
            "current-input-binding",
        )


def test_determinism_rejects_a_tampered_latest_provenance_binding(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    prior = tmp_path / "prior"
    current = tmp_path / "current"
    current_order = SCENARIOS
    _write_minimal_determinism_run(
        prior,
        scenario_order=tuple(reversed(current_order)),
        reference_economic_parity=True,
        include_provenance=True,
    )
    _write_minimal_determinism_run(
        current,
        scenario_order=current_order,
        reference_economic_parity=True,
        include_provenance=False,
    )
    latest_path = prior.parent / "latest.json"
    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    latest["provenance_sha256"] = "0" * 64
    latest_path.write_text(json.dumps(latest), encoding="utf-8")
    monkeypatch.setattr(
        staged,
        "_sorted_frame",
        lambda *_args, **_kwargs: pd.DataFrame({"same": [1]}),
    )

    with pytest.raises(AssertionError, match="provenance hash differs"):
        staged._determinism_attestation(
            prior,
            current,
            current_order,
            "synthetic-input-binding",
        )
