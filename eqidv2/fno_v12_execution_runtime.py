"""Process-local, research-only execution adapters for FNO V12.

The frozen V11 runner remains the parent strategy.  This module only supplies
small causal deltas that can be installed inside the V11 runtime context and
outside the deterministic Gap2 context.  Every patched engine seam is restored
in ``finally``; callers must still use fresh spawned worker processes because
the underlying engine exposes module-global seams.

The S+2 SHORT reconfirmation modes intentionally target only ``09:25_SHORT``
and ``09:30_SHORT``:

``DELAY_S4``
    Keep the S+2 confirmation and trigger, but suppress any S+3 fill.  Capacity
    therefore remains reserved from S+2 and the first eligible fill is S+4.

``RECONFIRM_S3``
    Treat an otherwise-valid S+2 confirmation as provisional.  The candidate
    remains in monitoring state and must pass the ordinary strict gate again on
    S+3.  Ranking/cap reservation and trigger construction then occur only
    after S+3 closes, so the first eligible fill is S+4.

``RECONFIRM_EXTEND_1TICK`` / ``RECONFIRM_EXTEND_2BPS``
    Apply ``RECONFIRM_S3`` and additionally require the S+3 SHORT close to
    extend below the provisional S+2 close by one tick or two basis points.

LONG expiry is applied through ``policy_for_setup`` rather than by suppressing
fills.  This lets the neutral state machine expire and release setup capacity
at the correct candle.  An S+3 LONG expiry necessarily limits confirmation to
S+2 because same-confirmation-bar fills are forbidden and the engine requires
``entry_expiry_minute > max_confirmation_minute``.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import math
from dataclasses import dataclass, replace
from datetime import date
from typing import Any, Iterator, Mapping

import pandas as pd

import fno_v8_windowed_1m_entry_backtest as engine


RUNTIME_SCHEMA_VERSION = "fno_v12_causal_execution_runtime_v1"

M2_MODE_DELAY_S4 = "DELAY_S4"
M2_MODE_RECONFIRM_S3 = "RECONFIRM_S3"
M2_MODE_RECONFIRM_EXTEND_1TICK = "RECONFIRM_EXTEND_1TICK"
M2_MODE_RECONFIRM_EXTEND_2BPS = "RECONFIRM_EXTEND_2BPS"

M2_SHORT_MODES = frozenset(
    {
        M2_MODE_DELAY_S4,
        M2_MODE_RECONFIRM_S3,
        M2_MODE_RECONFIRM_EXTEND_1TICK,
        M2_MODE_RECONFIRM_EXTEND_2BPS,
    }
)
M2_RECONFIRM_MODES = frozenset(
    {
        M2_MODE_RECONFIRM_S3,
        M2_MODE_RECONFIRM_EXTEND_1TICK,
        M2_MODE_RECONFIRM_EXTEND_2BPS,
    }
)
M2_SHORT_SETUP_IDS = frozenset({"09:25_SHORT", "09:30_SHORT"})
DEFAULT_M2_SHORT_SETUP_IDS = ("09:25_SHORT", "09:30_SHORT")

EQUAL_RANK_PICKER = "v12_equal_rank"
EQUAL_RANK_SETUP_ID = "09:40_SHORT"


def _candidate_id(setup: engine.V8Setup, candidate: engine.CandidateInput) -> str:
    return (
        f"{candidate.session_date.isoformat()}|{setup.setup_id}|"
        f"{candidate.symbol}"
    )


def _validated_score_mapping(
    values: Mapping[str, float] | None,
) -> dict[str, float]:
    if values is None:
        return {}
    if not isinstance(values, Mapping):
        raise TypeError("equal_rank_picker_scores must be a mapping")

    scores: dict[str, float] = {}
    for raw_candidate_id, raw_score in values.items():
        candidate_id = str(raw_candidate_id)
        if candidate_id != candidate_id.strip() or not candidate_id:
            raise ValueError("equal-rank candidate IDs must be non-empty and trimmed")
        pieces = candidate_id.split("|")
        if len(pieces) != 3:
            raise ValueError(
                "equal-rank candidate IDs must be YYYY-MM-DD|SETUP_ID|SYMBOL"
            )
        day_text, setup_id, symbol = pieces
        try:
            date.fromisoformat(day_text)
        except ValueError as exc:
            raise ValueError(
                f"equal-rank candidate ID has invalid date: {candidate_id!r}"
            ) from exc
        if setup_id != EQUAL_RANK_SETUP_ID:
            raise ValueError(
                "equal-rank picker scores may target only "
                f"{EQUAL_RANK_SETUP_ID}: {candidate_id!r}"
            )
        if not symbol or symbol != symbol.strip():
            raise ValueError(
                f"equal-rank candidate ID has invalid symbol: {candidate_id!r}"
            )
        if isinstance(raw_score, bool):
            raise ValueError("equal-rank picker scores must be finite numbers")
        try:
            score = float(raw_score)
        except (TypeError, ValueError) as exc:
            raise ValueError("equal-rank picker scores must be finite numbers") from exc
        if not math.isfinite(score):
            raise ValueError("equal-rank picker scores must be finite numbers")
        scores[candidate_id] = score
    return scores


def _score_mapping_sha256(scores: Mapping[str, float]) -> str:
    payload = [[key, float(scores[key])] for key in sorted(scores)]
    return hashlib.sha256(
        json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class RuntimeSpec:
    """One V12 execution delta layered over the frozen V11 runtime."""

    m2_short_mode: str | None = None
    m2_short_setup_ids: tuple[str, ...] = DEFAULT_M2_SHORT_SETUP_IDS
    long_entry_expiry_minute: int | None = None
    equal_rank_picker_scores: Mapping[str, float] | None = None

    @property
    def active_mechanisms(self) -> tuple[str, ...]:
        active: list[str] = []
        if self.m2_short_mode is not None:
            active.append("M2_SHORT")
        if self.long_entry_expiry_minute is not None:
            active.append("LONG_ENTRY_EXPIRY")
        if self.equal_rank_picker_scores:
            active.append("EQUAL_RANK_PICKER")
        return tuple(active)

    @property
    def is_neutral(self) -> bool:
        return not self.active_mechanisms

    def validated_picker_scores(self) -> dict[str, float]:
        return _validated_score_mapping(self.equal_rank_picker_scores)

    def validate(self) -> None:
        if self.m2_short_mode is not None and self.m2_short_mode not in M2_SHORT_MODES:
            raise ValueError(
                f"unsupported V12 M2 SHORT mode: {self.m2_short_mode!r}"
            )
        if not isinstance(self.m2_short_setup_ids, tuple):
            raise ValueError("m2_short_setup_ids must be a tuple")
        if len(self.m2_short_setup_ids) != len(set(self.m2_short_setup_ids)):
            raise ValueError("m2_short_setup_ids must be unique")
        unknown_m2_setups = set(self.m2_short_setup_ids) - M2_SHORT_SETUP_IDS
        if unknown_m2_setups:
            raise ValueError(
                "m2_short_setup_ids contains unsupported setups: "
                f"{sorted(unknown_m2_setups)}"
            )
        if self.m2_short_mode is not None and not self.m2_short_setup_ids:
            raise ValueError("an active M2 SHORT mode requires at least one setup")
        if self.long_entry_expiry_minute is not None:
            value = self.long_entry_expiry_minute
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError("long_entry_expiry_minute must be an integer")
            if value not in {3, 4}:
                raise ValueError("V12 LONG entry expiry must be S+3 or S+4")
        self.validated_picker_scores()

    def payload(self) -> dict[str, Any]:
        self.validate()
        scores = self.validated_picker_scores()
        return {
            "schema_version": RUNTIME_SCHEMA_VERSION,
            "m2_short_mode": self.m2_short_mode,
            "m2_short_setup_ids": list(self.m2_short_setup_ids),
            "long_entry_expiry_minute": self.long_entry_expiry_minute,
            "equal_rank_picker_score_count": len(scores),
            "equal_rank_picker_scores_sha256": _score_mapping_sha256(scores),
            "active_mechanisms": list(self.active_mechanisms),
            "research_only": True,
            "live_or_paper_authority": False,
        }


def runtime_spec_from_rule(
    rule: Any,
    *,
    equal_rank_picker_scores: Mapping[str, float] | None = None,
) -> RuntimeSpec:
    """Adapt a registry-owned neutral runtime rule without importing it."""

    scores = (
        equal_rank_picker_scores
        if equal_rank_picker_scores is not None
        else getattr(rule, "equal_rank_picker_scores", None)
    )
    spec = RuntimeSpec(
        m2_short_mode=getattr(rule, "m2_short_mode", None),
        m2_short_setup_ids=tuple(
            getattr(rule, "m2_short_setup_ids", DEFAULT_M2_SHORT_SETUP_IDS)
        ),
        long_entry_expiry_minute=getattr(
            rule, "long_entry_expiry_minute", None
        ),
        equal_rank_picker_scores=scores,
    )
    spec.validate()
    return spec


@dataclass
class _ReconfirmationState:
    s2_base_passed: bool = False
    s2_time: pd.Timestamp | None = None
    s2_close: float | None = None
    s3_evaluated: bool = False
    s3_time: pd.Timestamp | None = None
    s3_close: float | None = None
    s3_base_passed: bool | None = None
    extension_threshold: float | None = None
    extension_passed: bool | None = None


class _StrongIdentityState:
    """Retain candidate objects so CPython object IDs cannot be recycled."""

    def __init__(self) -> None:
        self._values: dict[int, tuple[Any, _ReconfirmationState]] = {}

    def ensure(self, candidate: Any) -> _ReconfirmationState:
        item = self._values.get(id(candidate))
        if item is not None and item[0] is candidate:
            return item[1]
        state = _ReconfirmationState()
        self._values[id(candidate)] = (candidate, state)
        return state

    def get(self, candidate: Any) -> _ReconfirmationState | None:
        item = self._values.get(id(candidate))
        if item is None or item[0] is not candidate:
            return None
        return item[1]


def _m2_targeted(
    setup: engine.V8Setup,
    mode: str | None,
    target_setup_ids: tuple[str, ...],
) -> bool:
    return mode is not None and setup.setup_id in target_setup_ids


def _extension_threshold(
    mode: str,
    *,
    s2_close: float,
    tick_size: float,
) -> float | None:
    if mode == M2_MODE_RECONFIRM_EXTEND_1TICK:
        return float(s2_close) - float(tick_size)
    if mode == M2_MODE_RECONFIRM_EXTEND_2BPS:
        return float(s2_close) * (1.0 - 2.0 / 10_000.0)
    return None


def _trigger_touched(
    setup: engine.V8Setup,
    runtime: engine._CandidateRuntime,
    bar: engine.MinuteBar,
) -> bool:
    if runtime.trigger is None:
        return False
    trigger = float(runtime.trigger)
    if setup.side == "LONG":
        return float(bar.open) >= trigger or float(bar.high) >= trigger
    return float(bar.open) <= trigger or float(bar.low) <= trigger


@contextlib.contextmanager
def installed_runtime_hooks(spec: RuntimeSpec) -> Iterator[None]:
    """Install and restore a V12 execution delta in the current process."""

    spec.validate()
    scores = spec.validated_picker_scores()
    score_hash = _score_mapping_sha256(scores)

    original_confirmation_check = engine._confirmation_check
    original_entry_fill = engine._entry_fill
    original_policy_for_setup = engine.policy_for_setup
    original_picker_value = engine._picker_value
    original_audit_record = engine._audit_record

    reconfirmation = _StrongIdentityState()
    effective_policy_by_setup: dict[str, dict[str, Any]] = {}

    def v12_confirmation_check(
        setup: engine.V8Setup,
        candidate: engine.CandidateInput,
        bar: engine.MinuteBar,
        policy: engine.EntryPolicy | None = None,
    ) -> dict[str, Any]:
        record = original_confirmation_check(setup, candidate, bar, policy)
        mode = spec.m2_short_mode
        if mode not in M2_RECONFIRM_MODES or not _m2_targeted(
            setup, mode, spec.m2_short_setup_ids
        ):
            return record

        relative_minute = engine._relative_minute(candidate, bar.ts)
        if relative_minute == 2 and bool(record.get("passed", False)):
            state = reconfirmation.ensure(candidate)
            state.s2_base_passed = True
            state.s2_time = bar.ts
            state.s2_close = float(bar.close)
            record["v12_m2_s2_base_passed"] = True
            record["v12_m2_s2_provisional"] = True
            record["v12_m2_short_mode"] = mode
            rejection_codes = list(record.get("rejection_codes", []))
            rejection_codes.append("S2_PROVISIONAL_REQUIRES_S3")
            record["rejection_codes"] = rejection_codes
            record["passed"] = False
            return record

        state = reconfirmation.get(candidate)
        if relative_minute != 3 or state is None or not state.s2_base_passed:
            return record

        state.s3_evaluated = True
        state.s3_time = bar.ts
        state.s3_close = float(bar.close)
        state.s3_base_passed = bool(record.get("passed", False))
        record["v12_m2_s3_reconfirmation_evaluated"] = True
        record["v12_m2_s3_base_passed"] = state.s3_base_passed
        record["v12_m2_short_mode"] = mode

        threshold = _extension_threshold(
            mode,
            s2_close=float(state.s2_close),
            tick_size=float(candidate.tick_size),
        )
        state.extension_threshold = threshold
        if threshold is None:
            state.extension_passed = None
            record["v12_m2_extension_threshold"] = None
            record["v12_m2_extension_passed"] = None
            return record

        extension_passed = float(bar.close) <= float(threshold) + 1e-12
        state.extension_passed = extension_passed
        record["v12_m2_extension_threshold"] = float(threshold)
        record["v12_m2_extension_passed"] = extension_passed
        if bool(record.get("passed", False)) and not extension_passed:
            code = (
                "S3_RECONFIRMATION_EXTENSION_1TICK_NOT_MET"
                if mode == M2_MODE_RECONFIRM_EXTEND_1TICK
                else "S3_RECONFIRMATION_EXTENSION_2BPS_NOT_MET"
            )
            rejection_codes = list(record.get("rejection_codes", []))
            rejection_codes.append(code)
            record["rejection_codes"] = rejection_codes
            record["passed"] = False
        return record

    def v12_entry_fill(
        setup: engine.V8Setup,
        runtime: engine._CandidateRuntime,
        bar: engine.MinuteBar,
        policy: engine.EntryPolicy,
    ) -> tuple[float, bool] | None:
        mode = spec.m2_short_mode
        if (
            mode == M2_MODE_DELAY_S4
            and _m2_targeted(setup, mode, spec.m2_short_setup_ids)
            and runtime.confirmation_minute == 2
            and engine._relative_minute(runtime.candidate, bar.ts) < 4
        ):
            runtime._v12_m2_short_mode = mode
            runtime._v12_m2_delay_applied = True
            runtime._v12_m2_delay_fill_checks_suppressed = int(
                getattr(runtime, "_v12_m2_delay_fill_checks_suppressed", 0)
            ) + 1
            if _trigger_touched(setup, runtime, bar):
                runtime._v12_m2_delay_touch_observed = True
            # Do not call an inherited gap wrapper for a deliberately ineligible
            # S+3 bar: doing so could register a rejected gap before S+4.
            return None
        return original_entry_fill(setup, runtime, bar, policy)

    def v12_policy_for_setup(
        setup: engine.V8Setup,
        base_policy: engine.EntryPolicy,
    ) -> engine.EntryPolicy:
        inherited = original_policy_for_setup(setup, base_policy)
        effective = inherited
        clamped = False
        requested_expiry = spec.long_entry_expiry_minute
        if requested_expiry is not None and setup.side == "LONG":
            effective_max_confirmation = min(
                int(inherited.max_confirmation_minute),
                int(requested_expiry) - 1,
            )
            clamped = effective_max_confirmation != int(
                inherited.max_confirmation_minute
            )
            effective = replace(
                inherited,
                entry_expiry_minute=int(requested_expiry),
                max_confirmation_minute=effective_max_confirmation,
            )
            effective.validate()
        effective_policy_by_setup[setup.setup_id] = {
            "entry_expiry_minute": int(effective.entry_expiry_minute),
            "max_confirmation_minute": int(effective.max_confirmation_minute),
            "long_expiry_applied": bool(
                requested_expiry is not None and setup.side == "LONG"
            ),
            "confirmation_window_clamped": clamped,
        }
        return effective

    def v12_picker_value(
        setup: engine.V8Setup,
        candidate: engine.CandidateInput,
    ) -> float:
        candidate_id = _candidate_id(setup, candidate)
        if setup.setup_id == EQUAL_RANK_SETUP_ID:
            if candidate_id in scores:
                return float(scores[candidate_id])
            if setup.picker == EQUAL_RANK_PICKER:
                raise ValueError(
                    "V12 equal-rank picker has no score for targeted candidate: "
                    f"{candidate_id}"
                )
        return original_picker_value(setup, candidate)

    def v12_audit_record(
        setup: engine.V8Setup,
        runtime: engine._CandidateRuntime,
    ) -> dict[str, Any]:
        record = original_audit_record(setup, runtime)
        state = reconfirmation.get(runtime.candidate)
        policy_state = effective_policy_by_setup.get(setup.setup_id, {})
        candidate_id = _candidate_id(setup, runtime.candidate)
        mapped_score = scores.get(candidate_id)
        record.update(
            {
                "v12_runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                "v12_m2_short_mode": spec.m2_short_mode,
                "v12_m2_short_setup_ids": list(spec.m2_short_setup_ids),
                "v12_m2_targeted": _m2_targeted(
                    setup, spec.m2_short_mode, spec.m2_short_setup_ids
                ),
                "v12_m2_delay_applied": bool(
                    getattr(runtime, "_v12_m2_delay_applied", False)
                ),
                "v12_m2_delay_fill_checks_suppressed": int(
                    getattr(
                        runtime, "_v12_m2_delay_fill_checks_suppressed", 0
                    )
                ),
                "v12_m2_delay_touch_observed": bool(
                    getattr(runtime, "_v12_m2_delay_touch_observed", False)
                ),
                "v12_m2_s2_base_passed": bool(
                    state.s2_base_passed if state is not None else False
                ),
                "v12_m2_s2_time": (
                    state.s2_time if state is not None else pd.NaT
                ),
                "v12_m2_s2_close": (
                    state.s2_close if state is not None else None
                ),
                "v12_m2_s3_reconfirmation_evaluated": bool(
                    state.s3_evaluated if state is not None else False
                ),
                "v12_m2_s3_time": (
                    state.s3_time if state is not None else pd.NaT
                ),
                "v12_m2_s3_close": (
                    state.s3_close if state is not None else None
                ),
                "v12_m2_s3_base_passed": (
                    state.s3_base_passed if state is not None else None
                ),
                "v12_m2_extension_threshold": (
                    state.extension_threshold if state is not None else None
                ),
                "v12_m2_extension_passed": (
                    state.extension_passed if state is not None else None
                ),
                "v12_requested_long_entry_expiry_minute": (
                    spec.long_entry_expiry_minute
                ),
                "v12_long_expiry_applied": bool(
                    policy_state.get("long_expiry_applied", False)
                ),
                "v12_effective_entry_expiry_minute": policy_state.get(
                    "entry_expiry_minute"
                ),
                "v12_effective_max_confirmation_minute": policy_state.get(
                    "max_confirmation_minute"
                ),
                "v12_long_confirmation_window_clamped": bool(
                    policy_state.get("confirmation_window_clamped", False)
                ),
                "v12_equal_rank_picker_applied": mapped_score is not None,
                "v12_equal_rank_picker_score": mapped_score,
                "v12_equal_rank_picker_score_count": len(scores),
                "v12_equal_rank_picker_scores_sha256": score_hash,
            }
        )
        return record

    engine._confirmation_check = v12_confirmation_check
    engine._entry_fill = v12_entry_fill
    engine.policy_for_setup = v12_policy_for_setup
    engine._picker_value = v12_picker_value
    engine._audit_record = v12_audit_record
    try:
        yield
    finally:
        engine._confirmation_check = original_confirmation_check
        engine._entry_fill = original_entry_fill
        engine.policy_for_setup = original_policy_for_setup
        engine._picker_value = original_picker_value
        engine._audit_record = original_audit_record


__all__ = [
    "EQUAL_RANK_PICKER",
    "EQUAL_RANK_SETUP_ID",
    "M2_MODE_DELAY_S4",
    "M2_MODE_RECONFIRM_EXTEND_1TICK",
    "M2_MODE_RECONFIRM_EXTEND_2BPS",
    "M2_MODE_RECONFIRM_S3",
    "M2_RECONFIRM_MODES",
    "DEFAULT_M2_SHORT_SETUP_IDS",
    "M2_SHORT_MODES",
    "M2_SHORT_SETUP_IDS",
    "RUNTIME_SCHEMA_VERSION",
    "RuntimeSpec",
    "installed_runtime_hooks",
    "runtime_spec_from_rule",
]
