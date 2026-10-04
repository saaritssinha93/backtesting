"""Deterministic, stage-wise live/replay reconciliation.

The reconciler deliberately knows nothing about broker credentials or trading
processes.  It compares already captured point-in-time records and reports the
first stage at which two views stop agreeing.  This keeps the diagnostic safe
to run after market close and makes an incomplete comparison explicit rather
than silently treating missing evidence as equality.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SCHEMA_VERSION = "v13_v10_g_reconciliation_v1"

STAGE_ORDER: tuple[str, ...] = (
    "universe",
    "raw_equity",
    "raw_futures_oi",
    "aggregate_5m",
    "feature",
    "base_gate",
    "confirmation",
    "setup_gate",
    "ranking",
    "selection",
    "execution",
    "exit",
    "pnl",
)

# Ordered key candidates cover the repository's native live/replay artifact
# schemas. Correlation identifiers are valid evidence keys here (unlike
# Prometheus labels). A comparison becomes INDETERMINATE when none are
# available; it must never collapse unrelated rows onto a tuple of ``None``.
_STAGE_KEY_CANDIDATES: dict[str, tuple[tuple[str, ...], ...]] = {
    "universe": (
        ("session_date", "tradingsymbol"),
        ("session_date", "symbol"),
        ("tradingsymbol",),
        ("symbol",),
        ("instrument_token",),
    ),
    "raw_equity": (
        ("session_date", "signal_ts", "tradingsymbol"),
        ("session_date", "ts", "tradingsymbol"),
        ("session_date", "timestamp", "symbol"),
        ("ts", "tradingsymbol"),
        ("timestamp", "symbol"),
        ("session_date", "slot", "symbol"),
    ),
    "raw_futures_oi": (
        ("session_date", "signal_ts", "futures_tradingsymbol"),
        ("session_date", "ts", "tradingsymbol"),
        ("session_date", "timestamp", "symbol"),
        ("ts", "tradingsymbol"),
        ("timestamp", "symbol"),
        ("session_date", "slot", "symbol"),
    ),
    "aggregate_5m": (
        ("session_date", "signal_ts", "tradingsymbol"),
        ("session_date", "slot", "symbol"),
        ("session_date", "slot_ist", "symbol"),
    ),
    "feature": (
        ("session_date", "signal_ts", "tradingsymbol"),
        ("session_date", "slot", "symbol"),
        ("session_date", "slot_ist", "symbol"),
    ),
    "base_gate": (
        ("session_date", "signal_ts", "tradingsymbol"),
        ("session_date", "slot", "symbol"),
    ),
    "confirmation": (
        ("signal_id",),
        ("session_date", "signal_ts", "tradingsymbol"),
        ("session_date", "slot", "symbol"),
    ),
    "setup_gate": (
        ("signal_id",),
        ("session_date", "signal_ts", "tradingsymbol"),
        ("session_date", "slot", "symbol"),
    ),
    "ranking": (
        ("signal_id",),
        ("session_date", "signal_ts", "tradingsymbol"),
        ("session_date", "slot", "symbol"),
    ),
    "selection": (
        ("signal_id",),
        ("session_date", "signal_ts", "tradingsymbol"),
        ("session_date", "slot", "symbol"),
    ),
    "execution": (("signal_id",), ("order_id",), ("broker_order_id",)),
    "exit": (("signal_id",), ("order_id",), ("broker_order_id",)),
    "pnl": (("signal_id",), ("order_id",), ("broker_order_id",)),
}

_GENERIC_KEY_CANDIDATES: tuple[tuple[str, ...], ...] = (
    ("session_date", "slot", "symbol"),
    ("session_date", "signal_ts", "tradingsymbol"),
    ("signal_id",),
    ("order_id",),
    ("session_date", "symbol"),
    ("session_date", "tradingsymbol"),
)


class ComparisonState(str, Enum):
    EXACT = "EXACT"
    MISMATCH = "MISMATCH"
    INDETERMINATE = "INDETERMINATE"


@dataclass(frozen=True)
class FieldDifference:
    key: tuple[str, ...]
    field: str
    left: Any
    right: Any
    absolute_delta: float | None = None
    tolerance: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": list(self.key),
            "field": self.field,
            "left": self.left,
            "right": self.right,
            "absolute_delta": self.absolute_delta,
            "tolerance": self.tolerance,
        }


@dataclass(frozen=True)
class StageComparison:
    stage: str
    state: ComparisonState
    left_rows: int
    right_rows: int
    matched_rows: int
    left_only: tuple[tuple[str, ...], ...] = ()
    right_only: tuple[tuple[str, ...], ...] = ()
    differences: tuple[FieldDifference, ...] = ()
    reason: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "state": self.state.value,
            "left_rows": self.left_rows,
            "right_rows": self.right_rows,
            "matched_rows": self.matched_rows,
            "left_only": [list(key) for key in self.left_only],
            "right_only": [list(key) for key in self.right_only],
            "differences": [item.as_dict() for item in self.differences],
            "reason": self.reason,
        }


@dataclass(frozen=True)
class PairComparison:
    name: str
    state: ComparisonState
    first_divergence_stage: str | None
    classification: str
    stages: tuple[StageComparison, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state.value,
            "first_divergence_stage": self.first_divergence_stage,
            "classification": self.classification,
            "stages": [stage.as_dict() for stage in self.stages],
        }


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except (TypeError, ValueError):
            pass
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except (TypeError, ValueError):
            pass
    return str(value)


def canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        _json_safe(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(math.isnan(value))
    except (TypeError, ValueError):
        return False


def _as_number(value: Any) -> float | None:
    if isinstance(value, bool) or _is_missing(value):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _key(row: Mapping[str, Any], key_fields: Sequence[str]) -> tuple[str, ...]:
    return tuple(str(_json_safe(row.get(name))) for name in key_fields)


def _index(
    records: Iterable[Mapping[str, Any]], key_fields: Sequence[str]
) -> tuple[dict[tuple[str, ...], Mapping[str, Any]], list[tuple[str, ...]]]:
    indexed: dict[tuple[str, ...], Mapping[str, Any]] = {}
    duplicates: list[tuple[str, ...]] = []
    for row in records:
        item_key = _key(row, key_fields)
        if item_key in indexed:
            duplicates.append(item_key)
        indexed[item_key] = row
    return indexed, duplicates


def _infer_key_fields(
    stage: str,
    left: Sequence[Mapping[str, Any]],
    right: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    rows = [*left, *right]
    if not rows:
        return ()
    candidates = _STAGE_KEY_CANDIDATES.get(stage, ()) + _GENERIC_KEY_CANDIDATES
    for candidate in candidates:
        if all(
            all(field in row and not _is_missing(row.get(field)) for field in candidate)
            for row in rows
        ):
            return candidate
    return ()


def compare_stage(
    stage: str,
    left_records: Iterable[Mapping[str, Any]] | None,
    right_records: Iterable[Mapping[str, Any]] | None,
    *,
    key_fields: Sequence[str] | None = None,
    tolerances: Mapping[str, float] | None = None,
    ignored_fields: Iterable[str] = (),
    max_differences: int = 200,
) -> StageComparison:
    """Compare one causal stage with deterministic keys and bounded output."""

    if left_records is None or right_records is None:
        return StageComparison(
            stage=stage,
            state=ComparisonState.INDETERMINATE,
            left_rows=0 if left_records is None else len(list(left_records)),
            right_rows=0 if right_records is None else len(list(right_records)),
            matched_rows=0,
            reason="required_stage_evidence_missing",
        )
    left_rows = list(left_records)
    right_rows = list(right_records)
    if not left_rows and not right_rows:
        return StageComparison(
            stage=stage,
            state=ComparisonState.EXACT,
            left_rows=0,
            right_rows=0,
            matched_rows=0,
        )
    resolved_keys = tuple(key_fields or _infer_key_fields(stage, left_rows, right_rows))
    if not resolved_keys:
        return StageComparison(
            stage=stage,
            state=ComparisonState.INDETERMINATE,
            left_rows=len(left_rows),
            right_rows=len(right_rows),
            matched_rows=0,
            reason="comparison_key_fields_unavailable",
        )
    if any(
        any(field not in row or _is_missing(row.get(field)) for field in resolved_keys)
        for row in (*left_rows, *right_rows)
    ):
        return StageComparison(
            stage=stage,
            state=ComparisonState.INDETERMINATE,
            left_rows=len(left_rows),
            right_rows=len(right_rows),
            matched_rows=0,
            reason="comparison_key_fields_missing",
        )
    left, left_duplicates = _index(left_rows, resolved_keys)
    right, right_duplicates = _index(right_rows, resolved_keys)
    if left_duplicates or right_duplicates:
        return StageComparison(
            stage=stage,
            state=ComparisonState.INDETERMINATE,
            left_rows=len(left_rows),
            right_rows=len(right_rows),
            matched_rows=len(set(left).intersection(right)),
            reason="duplicate_comparison_keys",
            left_only=tuple(sorted(set(left_duplicates)))[:max_differences],
            right_only=tuple(sorted(set(right_duplicates)))[:max_differences],
        )

    left_keys, right_keys = set(left), set(right)
    left_only = tuple(sorted(left_keys - right_keys))
    right_only = tuple(sorted(right_keys - left_keys))
    ignored = set(ignored_fields).union(resolved_keys)
    tolerance_map = dict(tolerances or {})
    differences: list[FieldDifference] = []

    for item_key in sorted(left_keys.intersection(right_keys)):
        left_row, right_row = left[item_key], right[item_key]
        fields = sorted(set(left_row).union(right_row) - ignored)
        for name in fields:
            left_value, right_value = left_row.get(name), right_row.get(name)
            if _is_missing(left_value) and _is_missing(right_value):
                continue
            left_number, right_number = _as_number(left_value), _as_number(right_value)
            tolerance = float(tolerance_map.get(name, 0.0))
            if left_number is not None and right_number is not None:
                delta = abs(left_number - right_number)
                if delta <= tolerance:
                    continue
                difference = FieldDifference(
                    item_key,
                    name,
                    _json_safe(left_value),
                    _json_safe(right_value),
                    delta,
                    tolerance,
                )
            elif _json_safe(left_value) == _json_safe(right_value):
                continue
            else:
                difference = FieldDifference(
                    item_key, name, _json_safe(left_value), _json_safe(right_value)
                )
            if len(differences) < max_differences:
                differences.append(difference)

    state = (
        ComparisonState.MISMATCH
        if left_only or right_only or differences
        else ComparisonState.EXACT
    )
    return StageComparison(
        stage=stage,
        state=state,
        left_rows=len(left_rows),
        right_rows=len(right_rows),
        matched_rows=len(left_keys.intersection(right_keys)),
        left_only=left_only[:max_differences],
        right_only=right_only[:max_differences],
        differences=tuple(differences),
        reason="stage_values_differ" if state is ComparisonState.MISMATCH else "",
    )


def _classification(name: str, stage: str | None, state: ComparisonState) -> str:
    if state is ComparisonState.INDETERMINATE:
        return "INCOMPLETE_EVIDENCE"
    if state is ComparisonState.EXACT:
        return "EXACT_PARITY"
    if name == "live_vs_observed":
        if stage in {"universe", "raw_equity", "raw_futures_oi"}:
            return "LIVE_CAPTURE_OR_AVAILABILITY_DIFFERENCE"
        if stage in {"aggregate_5m", "feature", "base_gate", "confirmation", "setup_gate"}:
            return "CODE_CONFIG_OR_WARMUP_DIFFERENCE"
        if stage in {"ranking", "selection"}:
            return "RANKING_OR_QUOTA_DIFFERENCE"
        return "EXECUTION_OR_ACCOUNTING_DIFFERENCE"
    if stage in {"universe", "raw_equity", "raw_futures_oi", "aggregate_5m"}:
        return "LATE_DATA_OR_PROVIDER_REVISION"
    if stage in {"feature", "base_gate", "confirmation", "setup_gate", "ranking", "selection"}:
        return "FINAL_DATA_FEATURE_IMPACT"
    return "FINAL_DATA_EXECUTION_IMPACT"


def compare_stage_sets(
    name: str,
    left: Mapping[str, Iterable[Mapping[str, Any]] | None],
    right: Mapping[str, Iterable[Mapping[str, Any]] | None],
    *,
    key_fields_by_stage: Mapping[str, Sequence[str]] | None = None,
    tolerances_by_stage: Mapping[str, Mapping[str, float]] | None = None,
    ignored_fields: Iterable[str] = (
        "observed_at_ist",
        "received_at_ist",
        "persisted_at_ist",
        "published_at_ist",
        "run_id",
        "replay_id",
        "request_id",
        "trace_id",
        "span_id",
        # This integrity digest intentionally covers run/replay identity. The
        # semantic fields and feature/input hashes are compared separately.
        "ledger_row_sha256",
    ),
) -> PairComparison:
    keys = key_fields_by_stage or {}
    tolerances = tolerances_by_stage or {}
    comparisons: list[StageComparison] = []
    first_divergence: str | None = None
    overall = ComparisonState.EXACT

    configured_stages = [stage for stage in STAGE_ORDER if stage in left or stage in right]
    configured_stages.extend(
        sorted(set(left).union(right).difference(configured_stages))
    )
    if not configured_stages:
        return PairComparison(
            name,
            ComparisonState.INDETERMINATE,
            None,
            "INCOMPLETE_EVIDENCE",
            (),
        )

    for stage in configured_stages:
        comparison = compare_stage(
            stage,
            left.get(stage),
            right.get(stage),
            key_fields=keys.get(stage),
            tolerances=tolerances.get(stage),
            ignored_fields=ignored_fields,
        )
        comparisons.append(comparison)
        if comparison.state is not ComparisonState.EXACT and first_divergence is None:
            first_divergence = stage
        if comparison.state is ComparisonState.INDETERMINATE:
            overall = ComparisonState.INDETERMINATE
        elif comparison.state is ComparisonState.MISMATCH and overall is ComparisonState.EXACT:
            overall = ComparisonState.MISMATCH

    return PairComparison(
        name=name,
        state=overall,
        first_divergence_stage=first_divergence,
        classification=_classification(name, first_divergence, overall),
        stages=tuple(comparisons),
    )


def reconcile_live_observed_final(
    *,
    session_date: str,
    live: Mapping[str, Iterable[Mapping[str, Any]] | None],
    observed: Mapping[str, Iterable[Mapping[str, Any]] | None],
    finalized: Mapping[str, Iterable[Mapping[str, Any]] | None],
    key_fields_by_stage: Mapping[str, Sequence[str]] | None = None,
    tolerances_by_stage: Mapping[str, Mapping[str, float]] | None = None,
    generated_at_ist: str | None = None,
) -> dict[str, Any]:
    """Build the two comparisons needed to separate system and data drift."""

    live_observed = compare_stage_sets(
        "live_vs_observed",
        live,
        observed,
        key_fields_by_stage=key_fields_by_stage,
        tolerances_by_stage=tolerances_by_stage,
    )
    observed_final = compare_stage_sets(
        "observed_vs_finalized",
        observed,
        finalized,
        key_fields_by_stage=key_fields_by_stage,
        tolerances_by_stage=tolerances_by_stage,
    )
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "session_date": session_date,
        "generated_at_ist": generated_at_ist or datetime.now().astimezone().isoformat(),
        "comparisons": {
            live_observed.name: live_observed.as_dict(),
            observed_final.name: observed_final.as_dict(),
        },
        "diagnosis": {
            "live_system_state": live_observed.classification,
            "data_revision_state": observed_final.classification,
            "actionable": (
                live_observed.state is not ComparisonState.INDETERMINATE
                and observed_final.state is not ComparisonState.INDETERMINATE
            ),
        },
    }
    payload["report_sha256"] = canonical_sha256(payload)
    return payload


def write_report(path: Path | str, report: Mapping[str, Any]) -> Path:
    """Atomically publish a reconciliation report."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(_json_safe(report), indent=2, sort_keys=True) + "\n"
    descriptor, temp_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent)
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, target)
    except BaseException:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise
    return target


def load_stage_bundle(path: Path | str) -> dict[str, list[dict[str, Any]] | None]:
    """Load a CLI/API stage bundle with a strict, intentionally small schema."""

    source = Path(path)
    payload = json.loads(source.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and payload.get("content_sha256") is not None:
        claimed = str(payload.get("content_sha256"))
        unsigned = {
            key: value for key, value in payload.items() if key != "content_sha256"
        }
        if claimed != canonical_sha256(unsigned):
            raise ValueError(f"{source} stage-bundle content digest mismatch")
    stages = payload.get("stages") if isinstance(payload, dict) else None
    if not isinstance(stages, dict):
        raise ValueError(f"{source} must contain a 'stages' object")
    result: dict[str, list[dict[str, Any]] | None] = {}
    for name, rows in stages.items():
        if rows is None:
            result[str(name)] = None
            continue
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise ValueError(f"Stage {name!r} in {source} must be a list of objects or null")
        result[str(name)] = rows
    return result


__all__ = [
    "ComparisonState",
    "FieldDifference",
    "PairComparison",
    "SCHEMA_VERSION",
    "STAGE_ORDER",
    "StageComparison",
    "canonical_sha256",
    "compare_stage",
    "compare_stage_sets",
    "load_stage_bundle",
    "reconcile_live_observed_final",
    "write_report",
]
