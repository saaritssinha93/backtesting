"""Frozen Stage 1 experiment registry for the V10 F&O research backtester.

Only the predeclared variants in this module are executable.  There are no
free-form threshold flags, which prevents an accidental parameter sweep from
silently changing the research question after results are observed.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from typing import Any


CONFIG_SCHEMA_VERSION = "fno_v10_stage1_experiment_config_v1"
RUN_SCHEMA_VERSION = "fno_v10_stage1_experiment_run_v1"
SLOT_RVOL_SCHEMA_VERSION = "fno_v10_same_slot_rvol20_sidecar_v1"
EXPERIMENT_ROOT_VERSION = "v10_stage1_isolated_experiments_v1"


@dataclass(frozen=True)
class ExperimentSpec:
    variant: str
    description: str
    confirmation_volume_ratio_min: float | None = None
    entry_expiry_minute: int = 5
    disabled_setup_ids: tuple[str, ...] = ()
    price_threshold_overrides: tuple[tuple[str, float], ...] = ()
    slot_rvol20_min: float | None = None

    @property
    def selection_overlay_id(self) -> str:
        if (
            not self.disabled_setup_ids
            and not self.price_threshold_overrides
            and self.slot_rvol20_min is None
        ):
            return "BASE_V10B_SELECTION"
        return self.variant

    @property
    def uses_rv1(self) -> bool:
        return self.confirmation_volume_ratio_min is not None

    @property
    def uses_slot_rvol20(self) -> bool:
        return self.slot_rvol20_min is not None

    def payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["price_threshold_overrides"] = [
            {"setup_id": setup_id, "price_change_pct": threshold}
            for setup_id, threshold in self.price_threshold_overrides
        ]
        payload["selection_overlay_id"] = self.selection_overlay_id
        return payload


EXPERIMENT_SPECS: tuple[ExperimentSpec, ...] = (
    ExperimentSpec(
        "V10B",
        "Frozen V10B parity control: current 5m selection and 1m execution",
    ),
    ExperimentSpec(
        "RV1_100",
        "V10B plus confirmation 1m volume / signal 5m per-minute volume >= 1.00",
        confirmation_volume_ratio_min=1.0,
    ),
    ExperimentSpec(
        "EXPIRY_S4",
        "V10B with pending stop orders expiring after S+4 instead of S+5",
        entry_expiry_minute=4,
    ),
    ExperimentSpec(
        "RV1_100_S4",
        "RV1 >= 1.00 confirmation plus S+4 pending-order expiry",
        confirmation_volume_ratio_min=1.0,
        entry_expiry_minute=4,
    ),
    ExperimentSpec(
        "NO_0935_LONG",
        "V10B with the 09:35 LONG selection leg disabled as an ablation",
        disabled_setup_ids=("09:35_LONG",),
    ),
    ExperimentSpec(
        "0940_LONG_MOVE_030",
        "V10B with 09:40 LONG directional 5m price change >= 0.30%",
        price_threshold_overrides=(("09:40_LONG", 0.30),),
    ),
    ExperimentSpec(
        "0940_LONG_MOVE_040",
        "V10B with 09:40 LONG directional 5m price change >= 0.40%",
        price_threshold_overrides=(("09:40_LONG", 0.40),),
    ),
    ExperimentSpec(
        "SLOT_RVOL_150",
        "V10B plus causal same-HH:MM prior-20-session median volume ratio >= 1.50",
        slot_rvol20_min=1.5,
    ),
    ExperimentSpec(
        "SLOT_RVOL_200",
        "V10B plus causal same-HH:MM prior-20-session median volume ratio >= 2.00",
        slot_rvol20_min=2.0,
    ),
)

EXPERIMENT_REGISTRY: dict[str, ExperimentSpec] = {
    spec.variant: spec for spec in EXPERIMENT_SPECS
}

# Filled after the registry was reviewed.  validate_registry() fails closed if
# any executable value changes without an explicit schema/version update.
EXPECTED_EXPERIMENT_REGISTRY_SHA256 = (
    "105935648a67ff126b73b98233efd6c10f40a5706f971a75dc22540251cc843b"
)


def canonical_json_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def registry_payload() -> dict[str, Any]:
    return {
        "schema_version": CONFIG_SCHEMA_VERSION,
        "variants": [spec.payload() for spec in EXPERIMENT_SPECS],
        "research_only": True,
        "promotion_eligible": False,
    }


def registry_sha256() -> str:
    return canonical_json_sha256(registry_payload())


def variant_config_payload(spec: ExperimentSpec | str) -> dict[str, Any]:
    resolved = get_spec(spec) if isinstance(spec, str) else spec
    return {
        "schema_version": CONFIG_SCHEMA_VERSION,
        "variant": resolved.payload(),
        "research_only": True,
        "promotion_eligible": False,
    }


def variant_config_sha256(spec: ExperimentSpec | str) -> str:
    return canonical_json_sha256(variant_config_payload(spec))


def get_spec(variant: str) -> ExperimentSpec:
    key = str(variant).upper().strip()
    if key not in EXPERIMENT_REGISTRY:
        raise ValueError(
            f"Unknown V10 experiment variant {variant!r}; "
            f"allowed={sorted(EXPERIMENT_REGISTRY)}"
        )
    return EXPERIMENT_REGISTRY[key]


def validate_registry(*, require_pinned_hash: bool = True) -> None:
    if len(EXPERIMENT_REGISTRY) != len(EXPERIMENT_SPECS):
        raise AssertionError("V10 experiment variant names must be unique")
    expected_names = {
        "V10B",
        "RV1_100",
        "EXPIRY_S4",
        "RV1_100_S4",
        "NO_0935_LONG",
        "0940_LONG_MOVE_030",
        "0940_LONG_MOVE_040",
        "SLOT_RVOL_150",
        "SLOT_RVOL_200",
    }
    if set(EXPERIMENT_REGISTRY) != expected_names:
        raise AssertionError("The predeclared Stage 1 variant set changed")
    variant_hashes = {variant_config_sha256(spec) for spec in EXPERIMENT_SPECS}
    if len(variant_hashes) != len(EXPERIMENT_SPECS):
        raise AssertionError("Every Stage 1 variant config hash must be unique")
    for spec in EXPERIMENT_SPECS:
        if spec.entry_expiry_minute not in {4, 5}:
            raise AssertionError(f"Unsupported entry expiry: {spec.variant}")
        for value in (
            spec.confirmation_volume_ratio_min,
            spec.slot_rvol20_min,
        ):
            if value is not None and (
                not math.isfinite(float(value)) or float(value) <= 0
            ):
                raise AssertionError(f"Invalid positive threshold: {spec.variant}")
        if set(spec.disabled_setup_ids) - {"09:35_LONG"}:
            raise AssertionError(f"Unexpected disabled leg: {spec.variant}")
        for setup_id, threshold in spec.price_threshold_overrides:
            if setup_id != "09:40_LONG" or threshold not in {0.30, 0.40}:
                raise AssertionError(f"Unexpected price override: {spec.variant}")
        mechanism_count = sum(
            (
                spec.confirmation_volume_ratio_min is not None,
                spec.entry_expiry_minute != 5,
                bool(spec.disabled_setup_ids),
                bool(spec.price_threshold_overrides),
                spec.slot_rvol20_min is not None,
            )
        )
        if spec.variant == "RV1_100_S4":
            if mechanism_count != 2:
                raise AssertionError("RV1_100_S4 must contain exactly two mechanisms")
        elif spec.variant == "V10B":
            if mechanism_count != 0:
                raise AssertionError("V10B control cannot contain an experiment")
        elif mechanism_count != 1:
            raise AssertionError(
                f"Isolated challenger must contain one mechanism: {spec.variant}"
            )
    if require_pinned_hash and registry_sha256() != EXPECTED_EXPERIMENT_REGISTRY_SHA256:
        raise AssertionError(
            "V10 experiment registry hash changed: "
            f"expected {EXPECTED_EXPERIMENT_REGISTRY_SHA256}, "
            f"observed {registry_sha256()}"
        )


if __name__ == "__main__":
    validate_registry()
    print(registry_sha256())
