"""Deterministic V11-only gap guard for the frozen V10 strategy rules.

The legacy V10 research guard intentionally remains untouched so Stage 0 can
retain exact parity with its pinned artifact.  V11 holds a strong reference to
every rejected CandidateInput and verifies object identity on lookup.  This
prevents CPython from recycling an object id and falsely cancelling a later,
unrelated candidate during a long replay.
"""

from __future__ import annotations

import contextlib
from typing import Any, Iterator

import pandas as pd

import fno_v10_gap_guard_research as legacy
import fno_v8_windowed_1m_entry_backtest as engine


RUNTIME_SCHEMA_VERSION = "fno_v11_strong_identity_gap_guard_v1"
IDENTITY_POLICY = "STRONG_REFERENCE_AND_IS_CHECK"


class _StrongIdentityRegistry:
    """Identity set that prevents id reuse by retaining the original object."""

    def __init__(self) -> None:
        self._objects: dict[int, Any] = {}

    def add(self, value: Any) -> None:
        self._objects[id(value)] = value

    def contains(self, value: Any) -> bool:
        return self._objects.get(id(value)) is value

    def __len__(self) -> int:
        return len(self._objects)


@contextlib.contextmanager
def installed_gap_guard(spec: legacy.GapGuardSpec) -> Iterator[None]:
    """Install the intended gap rule without the legacy object-id collision."""

    spec.validate()
    original_entry_fill = engine._entry_fill
    original_invalidation = engine._postconfirmation_invalidated
    original_transition = engine._CandidateRuntime.transition
    original_audit_record = engine._audit_record
    rejected_candidates = _StrongIdentityRegistry()

    def guarded_entry_fill(
        setup: engine.V8Setup,
        runtime: engine._CandidateRuntime,
        bar: engine.MinuteBar,
        policy: engine.EntryPolicy,
    ) -> tuple[float, bool] | None:
        fill = original_entry_fill(setup, runtime, bar, policy)
        if fill is None:
            return None
        entry_price, is_gap_fill = fill
        if not is_gap_fill:
            return fill
        assert runtime.trigger is not None
        distance = legacy.adverse_gap_bps(
            setup.side, float(bar.open), float(runtime.trigger)
        )
        if distance is None:
            raise AssertionError("neutral engine labelled a non-gap as a gap fill")
        runtime._gap_guard_observed = True
        runtime._gap_guard_bar_open = float(bar.open)
        runtime._gap_guard_trigger = float(runtime.trigger)
        runtime._gap_guard_adverse_bps = float(distance)
        runtime._gap_guard_event_ts = bar.ts
        runtime._gap_guard_rejected = legacy.gap_is_rejected(spec, distance)
        if not runtime._gap_guard_rejected:
            return entry_price, is_gap_fill
        rejected_candidates.add(runtime.candidate)
        return None

    def guarded_invalidation(
        setup: engine.V8Setup,
        candidate: engine.CandidateInput,
        bar: engine.MinuteBar,
    ) -> bool:
        if rejected_candidates.contains(candidate):
            return True
        return original_invalidation(setup, candidate, bar)

    def guarded_transition(
        runtime: engine._CandidateRuntime,
        new_state: engine.SignalState,
        *,
        event_ts: pd.Timestamp,
        reason: str,
    ) -> None:
        effective_reason = reason
        if (
            new_state == engine.SignalState.POSTCONF_CANCELLED
            and bool(getattr(runtime, "_gap_guard_rejected", False))
        ):
            effective_reason = "ADVERSE_GAP_GUARD_REJECTED"
        original_transition(
            runtime,
            new_state,
            event_ts=event_ts,
            reason=effective_reason,
        )

    def guarded_audit_record(
        setup: engine.V8Setup, runtime: engine._CandidateRuntime
    ) -> dict[str, Any]:
        record = original_audit_record(setup, runtime)
        record.update(
            {
                "gap_guard_variant": spec.variant,
                "gap_guard_max_adverse_bps": spec.max_adverse_gap_bps,
                "gap_guard_reject_all": spec.reject_all_gap_fills,
                "gap_guard_observed": bool(
                    getattr(runtime, "_gap_guard_observed", False)
                ),
                "gap_guard_rejected": bool(
                    getattr(runtime, "_gap_guard_rejected", False)
                ),
                "gap_guard_bar_open": getattr(
                    runtime, "_gap_guard_bar_open", None
                ),
                "gap_guard_trigger": getattr(runtime, "_gap_guard_trigger", None),
                "gap_guard_adverse_bps": getattr(
                    runtime, "_gap_guard_adverse_bps", None
                ),
                "gap_guard_event_ts": getattr(
                    runtime, "_gap_guard_event_ts", pd.NaT
                ),
                "v11_gap_runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                "v11_gap_identity_policy": IDENTITY_POLICY,
                "v11_gap_rejected_identity_count": len(rejected_candidates),
            }
        )
        return record

    engine._entry_fill = guarded_entry_fill
    engine._postconfirmation_invalidated = guarded_invalidation
    engine._CandidateRuntime.transition = guarded_transition
    engine._audit_record = guarded_audit_record
    try:
        yield
    finally:
        engine._entry_fill = original_entry_fill
        engine._postconfirmation_invalidated = original_invalidation
        engine._CandidateRuntime.transition = original_transition
        engine._audit_record = original_audit_record
