"""Causal selection and setup-patching runtime for predeclared FNO V11 specs.

The input candidate frame must already contain the candidates selected by the
complete V10 Stage7 + max-0.50 overlay.  V11 can only remove candidates from
that frame; it cannot reconstruct candidates excluded upstream by V10.

Selection challengers emit one decision row for every incoming candidate.
Picker and cap challengers never pre-truncate candidates.  Picker challengers
do refresh the cache-facing ``picker``, ``picker_value`` and ``frozen_rank``
columns because the portfolio ledger consumes those ranks after the neutral
engine has independently reranked candidates.  ``min_volume`` is not a native
V8/V10 picker, so the patched setup declares it and mandatory runner-hook
metadata tells the caller to extend the neutral engine's picker function.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Any, Sequence

import pandas as pd

import fno_v11_variant_registry as registry


ENGINE_NATIVE_PICKERS = frozenset(
    {"max_move", "max_volume", "max_liquidity", "max_oi"}
)

DECISION_COLUMNS = (
    "candidate_id",
    "variant_id",
    "stage_id",
    "setup_id",
    "kept",
    "reason",
    "metric",
    "measured_value",
    "threshold",
    "comparator",
)


@dataclass(frozen=True)
class SetupFieldOverride:
    setup_id: str
    field_name: str
    old_value: Any
    new_value: Any


@dataclass(frozen=True)
class PickerHook:
    """Ordering contract required when the neutral engine lacks a picker."""

    setup_id: str
    picker: str
    value_field: str
    value_multiplier: float
    descending: bool
    secondary_field: str
    secondary_descending: bool
    final_tiebreaker_field: str
    final_tiebreaker_descending: bool


@dataclass(frozen=True)
class SetupPatchMetadata:
    variant_id: str
    stage_id: str
    external_selection_required: bool
    disabled_setup_ids: tuple[str, ...]
    field_overrides: tuple[SetupFieldOverride, ...]
    picker_hook: PickerHook | None

    @property
    def requires_runner_picker_hook(self) -> bool:
        return self.picker_hook is not None


def _resolve_spec(spec: registry.VariantSpec | str) -> registry.VariantSpec:
    """Return only the canonical registered instance; reject ad-hoc specs."""

    registry.validate_registry(
        require_pinned_hash=True,
        require_parent_contract=False,
    )
    variant_id = spec.variant_id if isinstance(spec, registry.VariantSpec) else spec
    resolved = registry.get_spec(variant_id)
    if isinstance(spec, registry.VariantSpec) and spec != resolved:
        raise ValueError(
            f"FNO V11 spec {spec.variant_id!r} differs from the pinned registry"
        )
    return resolved


def _require_columns(frame: pd.DataFrame, columns: Sequence[str]) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"FNO V11 candidate frame missing columns: {missing}")


def _validated_input(candidates: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(candidates, pd.DataFrame):
        raise TypeError("FNO V11 candidates must be a pandas DataFrame")
    _require_columns(candidates, ("candidate_id", "setup_id"))
    work = candidates.copy(deep=True)
    if work["candidate_id"].isna().any():
        raise ValueError("FNO V11 candidate_id cannot be null")
    candidate_ids = work["candidate_id"].astype(str)
    if candidate_ids.str.strip().eq("").any():
        raise ValueError("FNO V11 candidate_id cannot be empty")
    if candidate_ids.duplicated().any():
        duplicates = sorted(candidate_ids[candidate_ids.duplicated()].unique())
        raise ValueError(f"FNO V11 candidate_id must be unique: {duplicates}")
    if work["setup_id"].isna().any():
        raise ValueError("FNO V11 setup_id cannot be null")
    unknown = sorted(set(work["setup_id"].astype(str)) - registry.VALID_SETUP_IDS)
    if unknown:
        raise ValueError(f"FNO V11 candidate frame has unknown setup IDs: {unknown}")
    work["candidate_id"] = candidate_ids
    return work.reset_index(drop=True)


def _finite_numeric(
    frame: pd.DataFrame,
    column: str,
    target: pd.Series,
) -> pd.Series:
    _require_columns(frame, (column,))
    values = pd.to_numeric(frame[column], errors="coerce")
    finite = values.notna() & values.map(
        lambda value: False if pd.isna(value) else math.isfinite(float(value))
    )
    invalid = target & ~finite
    if invalid.any():
        ids = frame.loc[invalid, "candidate_id"].astype(str).tolist()
        raise ValueError(
            f"FNO V11 {column} must be finite for targeted candidates: {ids}"
        )
    return values.astype(float)


def _picker_measure(
    frame: pd.DataFrame,
    picker: str,
    target: pd.Series,
) -> pd.Series:
    """Return engine-convention picker values (larger values rank first)."""

    if picker == "max_move":
        return _finite_numeric(frame, "price_change_pct", target).abs()
    if picker == "max_volume":
        return _finite_numeric(frame, "volume_ratio", target)
    if picker == "max_liquidity":
        return _finite_numeric(frame, "traded_value", target)
    if picker == "max_oi":
        return _finite_numeric(frame, "oi_change_pct", target)
    if picker == "min_volume":
        # The neutral ranking convention is descending picker_value, so a
        # negative RVOL makes the lowest adequate volume rank first.
        return -_finite_numeric(frame, "volume_ratio", target)
    raise AssertionError(f"Unsupported FNO V11 picker: {picker}")


def _apply_picker_override(
    frame: pd.DataFrame,
    override: registry.PickerOverride,
) -> pd.DataFrame:
    """Refresh target-setup rank fields without removing any candidates."""

    _require_columns(
        frame,
        (
            "session_date",
            "symbol",
            "traded_value",
            "picker",
            "picker_value",
            "frozen_rank",
        ),
    )
    work = frame.copy(deep=True)
    target = work["setup_id"].astype(str).eq(override.setup_id)
    if not target.any():
        return work

    missing_session = target & work["session_date"].isna()
    missing_symbol = target & (
        work["symbol"].isna() | work["symbol"].astype(str).str.strip().eq("")
    )
    if missing_session.any() or missing_symbol.any():
        invalid = missing_session | missing_symbol
        ids = work.loc[invalid, "candidate_id"].astype(str).tolist()
        raise ValueError(
            "FNO V11 picker ranking requires session_date and symbol: "
            f"{ids}"
        )

    traded_value = _finite_numeric(work, "traded_value", target)
    picker_value = _picker_measure(work, override.picker, target)
    work.loc[target, "picker"] = override.picker
    work.loc[target, "picker_value"] = picker_value.loc[target]

    ranked = work.loc[target, ["session_date", "setup_id"]].copy()
    ranked["_symbol"] = work.loc[target, "symbol"].astype(str)
    ranked["_picker_value"] = picker_value.loc[target]
    ranked["_traded_value"] = traded_value.loc[target]
    ranked = ranked.sort_values(
        [
            "session_date",
            "setup_id",
            "_picker_value",
            "_traded_value",
            "_symbol",
        ],
        ascending=[True, True, False, False, True],
        kind="stable",
    )
    ranked["_frozen_rank"] = (
        ranked.groupby(["session_date", "setup_id"], sort=False)
        .cumcount()
        .add(1)
    )
    work.loc[ranked.index, "frozen_rank"] = ranked["_frozen_rank"]
    return work


def _selection_measure(
    frame: pd.DataFrame,
    rule: registry.SelectionRule,
    field_name: str,
    target: pd.Series,
) -> tuple[pd.Series, str, str]:
    measured = pd.Series(float("nan"), index=frame.index, dtype="float64")

    if field_name.startswith("price_move_"):
        _require_columns(frame, ("side", "price_change_pct"))
        expected_side = rule.setup_id.rsplit("_", 1)[1]
        bad_side = target & frame["side"].astype(str).ne(expected_side)
        if bad_side.any():
            ids = frame.loc[bad_side, "candidate_id"].astype(str).tolist()
            raise ValueError(
                "FNO V11 targeted candidate side does not match setup_id: "
                f"{ids}"
            )
        raw = _finite_numeric(frame, "price_change_pct", target)
        directional = raw if expected_side == "LONG" else raw.abs()
        measured.loc[target] = directional.loc[target]
        return measured, "directional_move_pct", (
            "GE" if "_min_" in field_name else "LT"
        )

    if field_name.startswith("volume_ratio_"):
        values = _finite_numeric(frame, "volume_ratio", target)
        measured.loc[target] = values.loc[target]
        return measured, "volume_ratio", (
            "GE" if "_min_" in field_name else "LT"
        )

    if field_name.startswith("range_pct_"):
        _require_columns(
            frame,
            ("five_min_high", "five_min_low", "five_min_close"),
        )
        high = _finite_numeric(frame, "five_min_high", target)
        low = _finite_numeric(frame, "five_min_low", target)
        close = _finite_numeric(frame, "five_min_close", target)
        invalid_geometry = target & ((close <= 0) | (high < low))
        if invalid_geometry.any():
            ids = frame.loc[invalid_geometry, "candidate_id"].astype(str).tolist()
            raise ValueError(
                "FNO V11 range requires close > 0 and high >= low: "
                f"{ids}"
            )
        range_pct = (high - low) / close * 100.0
        measured.loc[target] = range_pct.loc[target]
        return measured, "five_min_range_pct", (
            "GE" if "_min_" in field_name else "LT"
        )

    if field_name == "min_setup_breadth_inclusive":
        _require_columns(frame, ("session_date",))
        missing_session = target & frame["session_date"].isna()
        if missing_session.any():
            ids = frame.loc[missing_session, "candidate_id"].astype(str).tolist()
            raise ValueError(
                f"FNO V11 breadth requires session_date for candidates: {ids}"
            )
        # This transform is deliberately computed over the complete incoming
        # V10-selected frame before any V11 row is removed.
        breadth = frame.groupby(
            ["session_date", "setup_id"],
            sort=False,
            dropna=False,
        )["candidate_id"].transform("size")
        measured.loc[target] = breadth.loc[target].astype(float)
        return measured, "setup_breadth", "GE"

    raise AssertionError(f"Unsupported FNO V11 selection field: {field_name}")


def _decision_frame(
    frame: pd.DataFrame,
    spec: registry.VariantSpec,
) -> tuple[pd.Series, pd.DataFrame]:
    kept = pd.Series(True, index=frame.index, dtype="bool")
    reason = pd.Series("V11_PASSTHROUGH", index=frame.index, dtype="object")
    metric = pd.Series("", index=frame.index, dtype="object")
    measured = pd.Series(float("nan"), index=frame.index, dtype="float64")
    threshold = pd.Series(float("nan"), index=frame.index, dtype="float64")
    comparator = pd.Series("", index=frame.index, dtype="object")

    if spec.variant_id == registry.CONTROL_VARIANT_ID:
        reason.loc[:] = "V11_CONTROL_PASSTHROUGH"
    elif spec.selection_rule is not None:
        rule = spec.selection_rule
        target = frame["setup_id"].astype(str).eq(rule.setup_id)
        field_name, value = rule.active_thresholds()[0]
        measured, metric_name, comparison = _selection_measure(
            frame,
            rule,
            field_name,
            target,
        )
        threshold.loc[target] = float(value)
        comparator.loc[target] = comparison
        metric.loc[target] = metric_name
        passed = measured.ge(float(value)) if comparison == "GE" else measured.lt(
            float(value)
        )
        kept.loc[target] = passed.loc[target]
        reason.loc[~target] = "V11_NOT_TARGET_SETUP"
        reason.loc[target & passed] = f"V11_{field_name.upper()}_PASSED"
        reason.loc[target & ~passed] = f"V11_{field_name.upper()}_REJECTED"
    elif spec.disabled_setup_id is not None:
        target = frame["setup_id"].astype(str).eq(spec.disabled_setup_id)
        kept.loc[target] = False
        reason.loc[~target] = "V11_NOT_TARGET_SETUP"
        reason.loc[target] = "V11_DISABLED_SETUP_REJECTED"
        metric.loc[:] = "setup_enabled"
        measured.loc[:] = 1.0
        measured.loc[target] = 0.0
        threshold.loc[:] = 1.0
        comparator.loc[:] = "EQ"
    elif spec.picker_override is not None:
        target = frame["setup_id"].astype(str).eq(spec.picker_override.setup_id)
        reason.loc[~target] = "V11_NOT_TARGET_SETUP"
        reason.loc[target] = "V11_PICKER_OVERRIDE_PASSTHROUGH"
        metric.loc[target] = "picker_value"
        measured.loc[target] = pd.to_numeric(
            frame.loc[target, "picker_value"], errors="raise"
        ).astype(float)
    else:
        # Cap effects belong to the engine setup tuple.  Passing all candidates
        # here prevents accidental pre-truncation.
        reason.loc[:] = "V11_SETUP_PATCH_PASSTHROUGH"

    decisions = pd.DataFrame(
        {
            "candidate_id": frame["candidate_id"].astype(str),
            "variant_id": spec.variant_id,
            "stage_id": spec.stage_id,
            "setup_id": frame["setup_id"].astype(str),
            "kept": kept,
            "reason": reason,
            "metric": metric,
            "measured_value": measured,
            "threshold": threshold,
            "comparator": comparator,
        },
        index=frame.index,
    )
    return kept, decisions.loc[:, DECISION_COLUMNS].reset_index(drop=True)


def apply_variant_to_selected_candidates(
    candidates: pd.DataFrame,
    spec: registry.VariantSpec | str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply one pinned V11 variant to the already-V10-selected rows."""

    resolved = _resolve_spec(spec)
    work = _validated_input(candidates)
    if resolved.picker_override is not None:
        work = _apply_picker_override(work, resolved.picker_override)
    kept, decisions = _decision_frame(work, resolved)
    selected = work.loc[kept].copy(deep=True).reset_index(drop=True)
    return selected, decisions


def _validated_base_setups(base_setups: Sequence[Any]) -> tuple[Any, ...]:
    setups = tuple(base_setups)
    ids = [str(getattr(setup, "setup_id", "")) for setup in setups]
    if len(ids) != len(set(ids)):
        raise ValueError("FNO V11 base setup IDs must be unique")
    if set(ids) != registry.VALID_SETUP_IDS:
        raise ValueError(
            "FNO V11 requires the complete ten-leg V10 setup book; "
            f"observed={sorted(ids)}"
        )
    caps = {setup.setup_id: int(setup.max_entries) for setup in setups}
    pickers = {setup.setup_id: str(setup.picker) for setup in setups}
    if caps != dict(registry.BASE_SETUP_CAPS):
        raise ValueError("FNO V11 base setup mixed caps differ from Stage 0")
    if pickers != dict(registry.BASE_SETUP_PICKERS):
        raise ValueError("FNO V11 base setup pickers differ from Stage 0")
    return setups


def derive_patched_engine_setups(
    base_setups: Sequence[Any],
    spec: registry.VariantSpec | str,
) -> tuple[tuple[Any, ...], SetupPatchMetadata]:
    """Return the variant setup tuple and any mandatory runner-hook metadata."""

    resolved = _resolve_spec(spec)
    setups = _validated_base_setups(base_setups)
    patched = list(setups)
    disabled: tuple[str, ...] = ()
    overrides: list[SetupFieldOverride] = []
    picker_hook: PickerHook | None = None

    if resolved.disabled_setup_id is not None:
        disabled = (resolved.disabled_setup_id,)
        patched = [
            setup for setup in patched if setup.setup_id != resolved.disabled_setup_id
        ]

    elif resolved.picker_override is not None:
        requested = resolved.picker_override
        index = next(
            i for i, setup in enumerate(patched) if setup.setup_id == requested.setup_id
        )
        current = patched[index]
        if requested.picker in ENGINE_NATIVE_PICKERS:
            patched[index] = replace(current, picker=requested.picker)
            overrides.append(
                SetupFieldOverride(
                    requested.setup_id,
                    "picker",
                    current.picker,
                    requested.picker,
                )
            )
        elif requested.picker == "min_volume":
            patched[index] = replace(current, picker=requested.picker)
            overrides.append(
                SetupFieldOverride(
                    requested.setup_id,
                    "picker",
                    current.picker,
                    requested.picker,
                )
            )
            picker_hook = PickerHook(
                setup_id=requested.setup_id,
                picker="min_volume",
                value_field="volume_ratio",
                value_multiplier=-1.0,
                descending=True,
                secondary_field="traded_value",
                secondary_descending=True,
                final_tiebreaker_field="symbol",
                final_tiebreaker_descending=False,
            )
        else:  # registry validation should make this unreachable.
            raise AssertionError(f"Unsupported FNO V11 picker: {requested.picker}")

    elif resolved.cap_override is not None:
        requested = resolved.cap_override
        index = next(
            i for i, setup in enumerate(patched) if setup.setup_id == requested.setup_id
        )
        current = patched[index]
        patched[index] = replace(current, max_entries=requested.max_entries)
        overrides.append(
            SetupFieldOverride(
                requested.setup_id,
                "max_entries",
                current.max_entries,
                requested.max_entries,
            )
        )

    elif resolved.selection_rule is not None:
        rule = resolved.selection_rule
        active = dict(rule.active_thresholds())
        if "volume_ratio_min_inclusive" in active:
            index = next(
                i for i, setup in enumerate(patched) if setup.setup_id == rule.setup_id
            )
            current = patched[index]
            new_minimum = float(active["volume_ratio_min_inclusive"])
            if new_minimum <= float(current.volume_ratio):
                raise ValueError(
                    "FNO V11 cached-candidate volume minimum must tighten Stage 0"
                )
            patched[index] = replace(current, volume_ratio=new_minimum)
            overrides.append(
                SetupFieldOverride(
                    rule.setup_id,
                    "volume_ratio",
                    current.volume_ratio,
                    new_minimum,
                )
            )

    metadata = SetupPatchMetadata(
        variant_id=resolved.variant_id,
        stage_id=resolved.stage_id,
        external_selection_required=resolved.selection_rule is not None,
        disabled_setup_ids=disabled,
        field_overrides=tuple(overrides),
        picker_hook=picker_hook,
    )
    return tuple(patched), metadata


__all__ = [
    "DECISION_COLUMNS",
    "ENGINE_NATIVE_PICKERS",
    "PickerHook",
    "SetupFieldOverride",
    "SetupPatchMetadata",
    "apply_variant_to_selected_candidates",
    "derive_patched_engine_setups",
]
