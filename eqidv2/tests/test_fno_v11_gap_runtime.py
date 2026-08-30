from __future__ import annotations

from contextlib import contextmanager

import pandas as pd
import pytest

import fno_v10_gap_guard_research as legacy_gap
import fno_v11_gap_runtime as gap_runtime
import fno_v11_staged_backtest as staged
import fno_v8_windowed_1m_entry_backtest as engine


class _EqualButUnrelated:
    def __eq__(self, other: object) -> bool:
        return isinstance(other, _EqualButUnrelated)

    def __hash__(self) -> int:
        return 1


def test_strong_identity_registry_never_confuses_an_unrelated_object() -> None:
    registry = gap_runtime._StrongIdentityRegistry()
    rejected = _EqualButUnrelated()
    unrelated = _EqualButUnrelated()

    registry.add(rejected)

    assert rejected == unrelated
    assert registry.contains(rejected)
    assert not registry.contains(unrelated)
    assert len(registry) == 1

    # Even a deliberately corrupted/colliding lookup key cannot pass the
    # second half of the identity contract because the retained object is not
    # the queried object.
    registry._objects[id(unrelated)] = rejected
    assert not registry.contains(unrelated)


def test_v11_gap_context_does_not_mutate_legacy_guard_and_restores_engine() -> None:
    legacy_installer = legacy_gap.installed_gap_guard
    originals = {
        "entry_fill": engine._entry_fill,
        "invalidation": engine._postconfirmation_invalidated,
        "transition": engine._CandidateRuntime.transition,
        "audit_record": engine._audit_record,
    }
    spec = next(
        candidate
        for candidate in legacy_gap.GAP_GUARDS
        if candidate.variant == staged.BASE_GAP_VARIANT
    )

    with gap_runtime.installed_gap_guard(spec):
        assert legacy_gap.installed_gap_guard is legacy_installer
        assert engine._entry_fill is not originals["entry_fill"]
        assert engine._postconfirmation_invalidated is not originals["invalidation"]
        assert engine._CandidateRuntime.transition is not originals["transition"]
        assert engine._audit_record is not originals["audit_record"]

    assert legacy_gap.installed_gap_guard is legacy_installer
    assert engine._entry_fill is originals["entry_fill"]
    assert engine._postconfirmation_invalidated is originals["invalidation"]
    assert engine._CandidateRuntime.transition is originals["transition"]
    assert engine._audit_record is originals["audit_record"]


def test_stage0_routes_to_legacy_guard_while_stage3_routes_to_v11_guard(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    guard_calls: list[str] = []

    @contextmanager
    def legacy_context(_spec):
        guard_calls.append("LEGACY_STAGE0")
        yield

    @contextmanager
    def stable_context(_spec):
        guard_calls.append("V11_STRONG_IDENTITY")
        yield

    @contextmanager
    def neutral_runtime_context(*_args, **_kwargs):
        yield

    def prepare(definition, baseline_selected):
        return staged.PreparedExperiment(
            definition=definition,
            candidates=baseline_selected.copy(),
            decisions=pd.DataFrame(),
            setups=(),
            setup_patch={},
        )

    frozen_root = tmp_path / "frozen"
    frozen_audit = (
        frozen_root
        / "scenarios"
        / "reference_15_0"
        / "candidate_order_audit.csv"
    )
    frozen_audit.parent.mkdir(parents=True)
    frozen_audit.write_text("candidate_id\n", encoding="utf-8")
    monkeypatch.setattr(staged, "FROZEN_V10_REFERENCE_RUN", frozen_root)
    monkeypatch.setattr(
        staged,
        "_control_parity_record",
        lambda *_args, **_kwargs: {"economic_parity": True},
    )
    monkeypatch.setattr(staged, "_prepare_experiment", prepare)
    monkeypatch.setattr(staged.gaps, "installed_gap_guard", legacy_context)
    monkeypatch.setattr(staged.gap_runtime, "installed_gap_guard", stable_context)
    monkeypatch.setattr(
        staged.execution_runtime,
        "installed_runtime_hooks",
        neutral_runtime_context,
    )
    monkeypatch.setattr(
        staged.experiment,
        "_entry_policy_for_variant",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        staged.experiment,
        "_NEUTRAL_RUN_BACKTEST",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        staged,
        "_metric_rows",
        lambda *_args, **_kwargs: ([{"period": "FULL_USABLE"}], pd.DataFrame()),
    )
    monkeypatch.setattr(staged, "_bootstrap_row", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(staged, "_scenario_output", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(staged.common, "atomic_write_csv", lambda *_args: None)
    monkeypatch.setattr(staged.common, "atomic_write_json", lambda *_args: None)
    monkeypatch.setattr(
        staged.v10_backtest,
        "validate_current_mixed_benchmark",
        lambda *_args, **_kwargs: {},
    )

    baseline_selected = pd.DataFrame({"candidate_id": pd.Series(dtype=str)})
    scenario = (("REFERENCE_15_0", 15.0, 0.0),)
    for definition in (staged.CONTROL, staged.DEVELOPMENT_BASELINE):
        staged._run_experiment(
            definition,
            baseline_selected,
            pd.DataFrame(),
            (),
            (),
            tmp_path,
            scenarios=scenario,
        )

    assert guard_calls == ["LEGACY_STAGE0", "V11_STRONG_IDENTITY"]
