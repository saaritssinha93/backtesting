"""Predeclared, research-only FNO V11 variant registry.

V11 is intentionally configuration-only at this stage.  It inherits the
hash-bound V10 Stage-7 + 09:35 LONG max-0.50 + Gap2 current-mixed control and
declares isolated setup-specific hypotheses for a future causal runner.

There are no free-form thresholds and no combined challengers.  Unknown
variants, parent-profile drift, registry drift, multi-mechanism variants and
global experiments fail closed.  In particular, this registry does not repeat
the already-tested global RV1, S+4 expiry, historical same-slot RVOL or uniform
max-entry sweeps.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from types import MappingProxyType
from typing import Any, Mapping


CONFIG_SCHEMA_VERSION = "fno_v11_setup_specific_variant_registry_v1"
REGISTRY_ID = "FNO_V11_SETUP_SPECIFIC_RESEARCH_20260830"
AUTHORITY = "BACKTEST_RESEARCH_ONLY"

CONTROL_VARIANT_ID = "V11_STAGE0_CONTROL"
CONTROL_STAGE_ID = "STAGE_00"

VALID_SETUP_IDS = frozenset(
    f"{slot}_{side}"
    for slot in ("09:25", "09:30", "09:35", "09:40", "09:45")
    for side in ("LONG", "SHORT")
)
VALID_PICKERS = frozenset(
    {"max_move", "max_volume", "max_liquidity", "max_oi", "min_volume"}
)


@dataclass(frozen=True)
class BaselineBinding:
    """Hashes and identifiers that bind Stage 0 to the honest V10 control."""

    profile_id: str
    selection_variant: str
    gap_variant: str
    setup_book_sha256: str
    locked_stage7_profile_sha256: str
    max050_gap2_profile_sha256: str
    benchmark_sha256: str
    benchmark_contract: str


BASELINE_BINDING = BaselineBinding(
    profile_id="V10_STAGE7_0935_LONG_MAX_050_GAP2",
    selection_variant="0935_LONG_MOVE_MAX_050",
    gap_variant="MAX_2_BPS",
    setup_book_sha256=(
        "ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675"
    ),
    locked_stage7_profile_sha256=(
        "f2b3291903dfb1f2c95f1d24b63285d527dc7a9a6aa3d6334caed03d0834e59c"
    ),
    max050_gap2_profile_sha256=(
        "299b5d05771bcb0cf7b699e1b6a8cd563e8878734518c45de9c33542a2316174"
    ),
    benchmark_sha256=(
        "c3d7c9ff60eb1b705260bee52525c9118c01095e2185882747c04981b1bbdd8d"
    ),
    benchmark_contract="PINNED_65_SESSION_REFERENCE_15_0",
)

# Only fields relevant to the V11 hypotheses are duplicated here.  The full
# setup book remains bound by BASELINE_BINDING.setup_book_sha256.
BASE_SETUP_CAPS: Mapping[str, int] = MappingProxyType(
    {
        "09:25_LONG": 4,
        "09:25_SHORT": 4,
        "09:30_LONG": 1,
        "09:30_SHORT": 4,
        "09:35_LONG": 1,
        "09:35_SHORT": 2,
        "09:40_LONG": 1,
        "09:40_SHORT": 1,
        "09:45_LONG": 1,
        "09:45_SHORT": 1,
    }
)
BASE_SETUP_PICKERS: Mapping[str, str] = MappingProxyType(
    {
        "09:25_LONG": "max_move",
        "09:25_SHORT": "max_move",
        "09:30_LONG": "max_move",
        "09:30_SHORT": "max_volume",
        "09:35_LONG": "max_liquidity",
        "09:35_SHORT": "max_liquidity",
        "09:40_LONG": "max_liquidity",
        "09:40_SHORT": "max_move",
        "09:45_LONG": "max_move",
        "09:45_SHORT": "max_volume",
    }
)


@dataclass(frozen=True)
class SelectionRule:
    """One setup-local five-minute eligibility rule.

    Minimum bounds are inclusive and maximum bounds are exclusive.  Price move
    is directional magnitude: positive LONG move or absolute SHORT decline.
    ``min_setup_breadth_inclusive`` counts eligible symbols in the completed
    setup/side slot and is known before one-minute confirmation begins.
    """

    setup_id: str
    price_move_min_pct_inclusive: float | None = None
    price_move_max_pct_exclusive: float | None = None
    volume_ratio_min_inclusive: float | None = None
    volume_ratio_max_exclusive: float | None = None
    range_pct_min_inclusive: float | None = None
    range_pct_max_exclusive: float | None = None
    min_setup_breadth_inclusive: int | None = None

    def active_thresholds(self) -> tuple[tuple[str, float | int], ...]:
        return tuple(
            (name, value)
            for name, value in (
                (
                    "price_move_min_pct_inclusive",
                    self.price_move_min_pct_inclusive,
                ),
                (
                    "price_move_max_pct_exclusive",
                    self.price_move_max_pct_exclusive,
                ),
                ("volume_ratio_min_inclusive", self.volume_ratio_min_inclusive),
                ("volume_ratio_max_exclusive", self.volume_ratio_max_exclusive),
                ("range_pct_min_inclusive", self.range_pct_min_inclusive),
                ("range_pct_max_exclusive", self.range_pct_max_exclusive),
                (
                    "min_setup_breadth_inclusive",
                    self.min_setup_breadth_inclusive,
                ),
            )
            if value is not None
        )


@dataclass(frozen=True)
class PickerOverride:
    setup_id: str
    picker: str


@dataclass(frozen=True)
class CapOverride:
    setup_id: str
    max_entries: int


@dataclass(frozen=True)
class VariantSpec:
    variant_id: str
    stage_id: str
    description: str
    selection_rule: SelectionRule | None = None
    disabled_setup_id: str | None = None
    picker_override: PickerOverride | None = None
    cap_override: CapOverride | None = None

    @property
    def mechanism_count(self) -> int:
        return sum(
            mechanism is not None
            for mechanism in (
                self.selection_rule,
                self.disabled_setup_id,
                self.picker_override,
                self.cap_override,
            )
        )

    @property
    def mechanism_type(self) -> str:
        active = [
            name
            for name, value in (
                ("SELECTION_RULE", self.selection_rule),
                ("DISABLED_SETUP", self.disabled_setup_id),
                ("PICKER_OVERRIDE", self.picker_override),
                ("CAP_OVERRIDE", self.cap_override),
            )
            if value is not None
        ]
        return "CONTROL" if not active else active[0]

    def payload(self) -> dict[str, Any]:
        return {
            "variant_id": self.variant_id,
            "stage_id": self.stage_id,
            "description": self.description,
            "mechanism_type": self.mechanism_type,
            "selection_rule": (
                asdict(self.selection_rule) if self.selection_rule else None
            ),
            "disabled_setup_id": self.disabled_setup_id,
            "picker_override": (
                asdict(self.picker_override) if self.picker_override else None
            ),
            "cap_override": asdict(self.cap_override) if self.cap_override else None,
        }


VARIANT_SPECS: tuple[VariantSpec, ...] = (
    VariantSpec(
        CONTROL_VARIANT_ID,
        CONTROL_STAGE_ID,
        "Unchanged V10 Stage7 + 09:35 LONG max-0.50 + Gap2 mixed-cap control",
    ),
    VariantSpec(
        "V11_S1_0930_SHORT_BREADTH_MIN_3",
        "STAGE_01_01",
        "09:30 SHORT requires at least three same-setup eligible symbols",
        selection_rule=SelectionRule(
            "09:30_SHORT", min_setup_breadth_inclusive=3
        ),
    ),
    VariantSpec(
        "V11_S1_0930_SHORT_BREADTH_MIN_4",
        "STAGE_01_02",
        "09:30 SHORT requires at least four same-setup eligible symbols",
        selection_rule=SelectionRule(
            "09:30_SHORT", min_setup_breadth_inclusive=4
        ),
    ),
    VariantSpec(
        "V11_S1_0940_SHORT_MOVE_MAX_050",
        "STAGE_01_03",
        "09:40 SHORT directional five-minute move must remain below 0.50%",
        selection_rule=SelectionRule(
            "09:40_SHORT", price_move_max_pct_exclusive=0.50
        ),
    ),
    VariantSpec(
        "V11_S1_0935_LONG_VOLUME_MIN_150",
        "STAGE_01_04",
        "09:35 LONG five-minute volume ratio minimum raised to 1.50",
        selection_rule=SelectionRule(
            "09:35_LONG", volume_ratio_min_inclusive=1.50
        ),
    ),
    VariantSpec(
        "V11_S1_0930_LONG_VOLUME_MIN_200",
        "STAGE_01_05",
        "09:30 LONG five-minute volume ratio minimum raised to 2.00",
        selection_rule=SelectionRule(
            "09:30_LONG", volume_ratio_min_inclusive=2.00
        ),
    ),
    VariantSpec(
        "V11_S1_0930_LONG_VOLUME_MIN_300",
        "STAGE_01_06",
        "09:30 LONG five-minute volume ratio minimum raised to 3.00",
        selection_rule=SelectionRule(
            "09:30_LONG", volume_ratio_min_inclusive=3.00
        ),
    ),
    VariantSpec(
        "V11_S1_0930_SHORT_RANGE_MIN_050",
        "STAGE_01_07",
        "09:30 SHORT five-minute range must be at least 0.50%",
        selection_rule=SelectionRule(
            "09:30_SHORT", range_pct_min_inclusive=0.50
        ),
    ),
    VariantSpec(
        "V11_S2_0925_SHORT_RANGE_MAX_150",
        "STAGE_02_01",
        "09:25 SHORT five-minute range must remain below 1.50%",
        selection_rule=SelectionRule(
            "09:25_SHORT", range_pct_max_exclusive=1.50
        ),
    ),
    VariantSpec(
        "V11_S2_0925_SHORT_RANGE_MAX_100",
        "STAGE_02_02",
        "09:25 SHORT five-minute range must remain below 1.00%",
        selection_rule=SelectionRule(
            "09:25_SHORT", range_pct_max_exclusive=1.00
        ),
    ),
    VariantSpec(
        "V11_S2_0925_LONG_VOLUME_MAX_500",
        "STAGE_02_03",
        "09:25 LONG five-minute volume ratio must remain below 5.00",
        selection_rule=SelectionRule(
            "09:25_LONG", volume_ratio_max_exclusive=5.00
        ),
    ),
    VariantSpec(
        "V11_S2_DISABLE_0945_SHORT",
        "STAGE_02_04",
        "Disable only the weak 09:45 SHORT setup as an isolated ablation",
        disabled_setup_id="09:45_SHORT",
    ),
    VariantSpec(
        "V11_S3_0930_SHORT_PICK_MAX_LIQUIDITY",
        "STAGE_03_01",
        "09:30 SHORT ranks confirmed candidates by traded value",
        picker_override=PickerOverride("09:30_SHORT", "max_liquidity"),
    ),
    VariantSpec(
        "V11_S3_0930_SHORT_PICK_MIN_VOLUME",
        "STAGE_03_02",
        "09:30 SHORT ranks adequate-volume candidates from lowest RVOL upward",
        picker_override=PickerOverride("09:30_SHORT", "min_volume"),
    ),
    VariantSpec(
        "V11_S3_0940_SHORT_PICK_MAX_LIQUIDITY",
        "STAGE_03_03",
        "09:40 SHORT ranks confirmed candidates by traded value",
        picker_override=PickerOverride("09:40_SHORT", "max_liquidity"),
    ),
    VariantSpec(
        "V11_S3_0945_SHORT_PICK_MAX_LIQUIDITY",
        "STAGE_03_04",
        "09:45 SHORT ranks confirmed candidates by traded value if leg stays on",
        picker_override=PickerOverride("09:45_SHORT", "max_liquidity"),
    ),
    VariantSpec(
        "V11_S4_0940_LONG_CAP_2",
        "STAGE_04_01",
        "Increase only 09:40 LONG maximum entries from one to two",
        cap_override=CapOverride("09:40_LONG", 2),
    ),
    VariantSpec(
        "V11_S4_0945_LONG_CAP_2",
        "STAGE_04_02",
        "Increase only 09:45 LONG maximum entries from one to two",
        cap_override=CapOverride("09:45_LONG", 2),
    ),
)

VARIANT_REGISTRY: Mapping[str, VariantSpec] = MappingProxyType(
    {spec.variant_id: spec for spec in VARIANT_SPECS}
)

EXPECTED_VARIANT_IDS = frozenset(
    {
        "V11_STAGE0_CONTROL",
        "V11_S1_0930_SHORT_BREADTH_MIN_3",
        "V11_S1_0930_SHORT_BREADTH_MIN_4",
        "V11_S1_0940_SHORT_MOVE_MAX_050",
        "V11_S1_0935_LONG_VOLUME_MIN_150",
        "V11_S1_0930_LONG_VOLUME_MIN_200",
        "V11_S1_0930_LONG_VOLUME_MIN_300",
        "V11_S1_0930_SHORT_RANGE_MIN_050",
        "V11_S2_0925_SHORT_RANGE_MAX_150",
        "V11_S2_0925_SHORT_RANGE_MAX_100",
        "V11_S2_0925_LONG_VOLUME_MAX_500",
        "V11_S2_DISABLE_0945_SHORT",
        "V11_S3_0930_SHORT_PICK_MAX_LIQUIDITY",
        "V11_S3_0930_SHORT_PICK_MIN_VOLUME",
        "V11_S3_0940_SHORT_PICK_MAX_LIQUIDITY",
        "V11_S3_0945_SHORT_PICK_MAX_LIQUIDITY",
        "V11_S4_0940_LONG_CAP_2",
        "V11_S4_0945_LONG_CAP_2",
    }
)

# Filled after review.  Any executable or descriptive payload drift requires a
# deliberate schema/version update and a new reviewed hash.
EXPECTED_REGISTRY_SHA256 = (
    "812097a895c69175e5f217485939d332129f63f27e7a8dc1ba1ec7d95652e531"
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
        "registry_id": REGISTRY_ID,
        "authority": AUTHORITY,
        "baseline_binding": asdict(BASELINE_BINDING),
        "bounds_contract": {
            "minimum_bounds": "INCLUSIVE",
            "maximum_bounds": "EXCLUSIVE",
            "price_move": "DIRECTIONAL_MAGNITUDE_PCT",
            "breadth": "POST_BASE_AND_EXISTING_V10_OVERLAYS_SAME_SETUP_COUNT",
        },
        "variants": [spec.payload() for spec in VARIANT_SPECS],
        "execution_contract": {
            "one_factor_per_challenger": True,
            "full_causal_portfolio_replay_required": True,
            "free_form_thresholds_allowed": False,
            "combined_challengers_allowed": False,
            "excluded_already_tested_mechanisms": [
                "GLOBAL_RV1",
                "EXPIRY_S4",
                "GLOBAL_RV1_PLUS_S4",
                "HISTORICAL_SAME_SLOT_RVOL",
                "UNIFORM_MAX_ENTRIES",
            ],
        },
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }


def registry_sha256() -> str:
    return canonical_json_sha256(registry_payload())


def variant_config_payload(spec: VariantSpec | str) -> dict[str, Any]:
    resolved = get_spec(spec) if isinstance(spec, str) else spec
    return {
        "schema_version": CONFIG_SCHEMA_VERSION,
        "registry_id": REGISTRY_ID,
        "baseline_binding": asdict(BASELINE_BINDING),
        "variant": resolved.payload(),
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }


def variant_config_sha256(spec: VariantSpec | str) -> str:
    return canonical_json_sha256(variant_config_payload(spec))


def get_spec(variant_id: str) -> VariantSpec:
    key = str(variant_id).strip().upper()
    if key not in VARIANT_REGISTRY:
        raise ValueError(
            f"Unknown FNO V11 variant {variant_id!r}; "
            f"allowed={sorted(VARIANT_REGISTRY)}"
        )
    return VARIANT_REGISTRY[key]


def _validate_positive_finite(value: float | int, *, label: str) -> None:
    if isinstance(value, bool) or not math.isfinite(float(value)) or value <= 0:
        raise AssertionError(f"{label} must be finite and positive")


def validate_variant_spec(spec: VariantSpec) -> None:
    """Validate one spec independently so malformed future additions fail."""

    if not spec.variant_id or spec.variant_id != spec.variant_id.upper():
        raise AssertionError("V11 variant IDs must be non-empty uppercase strings")
    if not spec.description.strip():
        raise AssertionError(f"V11 variant description is empty: {spec.variant_id}")

    if spec.variant_id == CONTROL_VARIANT_ID:
        if spec.stage_id != CONTROL_STAGE_ID or spec.mechanism_count != 0:
            raise AssertionError("V11 Stage 0 control cannot contain a mechanism")
        return

    if not re.fullmatch(r"STAGE_0[1-4]_\d{2}", spec.stage_id):
        raise AssertionError(f"Invalid V11 stage ID: {spec.stage_id}")
    if spec.mechanism_count != 1:
        raise AssertionError(
            f"V11 challenger must contain exactly one mechanism: {spec.variant_id}"
        )

    if spec.selection_rule is not None:
        rule = spec.selection_rule
        if not spec.stage_id.startswith(("STAGE_01_", "STAGE_02_")):
            raise AssertionError("Selection rules belong only to V11 Stages 1-2")
        if rule.setup_id not in VALID_SETUP_IDS:
            raise AssertionError(f"Unknown selection setup: {rule.setup_id}")
        thresholds = rule.active_thresholds()
        if len(thresholds) != 1:
            raise AssertionError(
                "Each V11 selection challenger must change exactly one threshold"
            )
        field, value = thresholds[0]
        _validate_positive_finite(value, label=f"{spec.variant_id}.{field}")
        if field == "min_setup_breadth_inclusive" and (
            isinstance(value, bool) or not isinstance(value, int) or value < 2
        ):
            raise AssertionError("Setup breadth minimum must be an integer >= 2")

    if spec.disabled_setup_id is not None:
        if not spec.stage_id.startswith("STAGE_02_"):
            raise AssertionError("Setup ablations belong only to V11 Stage 2")
        if spec.disabled_setup_id not in VALID_SETUP_IDS:
            raise AssertionError(f"Unknown disabled setup: {spec.disabled_setup_id}")

    if spec.picker_override is not None:
        override = spec.picker_override
        if not spec.stage_id.startswith("STAGE_03_"):
            raise AssertionError("Picker overrides belong only to V11 Stage 3")
        if override.setup_id not in VALID_SETUP_IDS:
            raise AssertionError(f"Unknown picker setup: {override.setup_id}")
        if override.picker not in VALID_PICKERS:
            raise AssertionError(f"Unknown V11 picker: {override.picker}")
        if override.picker == BASE_SETUP_PICKERS[override.setup_id]:
            raise AssertionError("Picker challenger must differ from its control")

    if spec.cap_override is not None:
        override = spec.cap_override
        if not spec.stage_id.startswith("STAGE_04_"):
            raise AssertionError("Cap overrides belong only to V11 Stage 4")
        if override.setup_id not in VALID_SETUP_IDS:
            raise AssertionError(f"Unknown cap setup: {override.setup_id}")
        if isinstance(override.max_entries, bool) or not isinstance(
            override.max_entries, int
        ):
            raise AssertionError("V11 max_entries must be an integer")
        if not 1 <= override.max_entries <= 5:
            raise AssertionError("V11 max_entries must be in [1, 5]")
        if override.max_entries == BASE_SETUP_CAPS[override.setup_id]:
            raise AssertionError("Cap challenger must differ from its control")


def validate_v10_baseline_bindings() -> None:
    """Fail closed if the parent V10 control no longer matches Stage 0."""

    import fno_v10_backtest as v10_backtest
    import fno_v10_backtest_config as v10_locked
    import fno_v10_unified_5m_1m_backtest as v10

    v10.validate_launcher_configuration()
    v10_locked.validate_locked_profile()
    v10_backtest.validate_max050_gap2_contract(require_files=False)

    observed = {
        "profile_id": v10_backtest.MAX050_GAP2_PROFILE_ID,
        "selection_variant": v10_backtest.MAX050_GAP2_SELECTION_VARIANT,
        "gap_variant": v10_backtest.MAX050_GAP2_GAP_VARIANT,
        "setup_book_sha256": v10.ACTIVE_SETUP_BOOK_SHA256,
        "locked_stage7_profile_sha256": v10_locked.profile_sha256(),
        "max050_gap2_profile_sha256": canonical_json_sha256(
            v10_backtest.max050_gap2_profile_payload()
        ),
        "benchmark_sha256": canonical_json_sha256(
            v10_backtest.MAX050_GAP2_CURRENT_MIXED_BENCHMARK
        ),
    }
    expected = asdict(BASELINE_BINDING)
    expected.pop("benchmark_contract")
    mismatches = {
        field: {"expected": expected[field], "observed": value}
        for field, value in observed.items()
        if value != expected[field]
    }
    if mismatches:
        raise AssertionError(f"FNO V11 parent V10 control drifted: {mismatches}")

    setups = {setup.setup_id: setup for setup in v10.ACTIVE_SETUPS}
    if set(setups) != VALID_SETUP_IDS:
        raise AssertionError("FNO V11 parent setup IDs drifted")
    observed_caps = {name: setup.max_entries for name, setup in setups.items()}
    observed_pickers = {name: setup.picker for name, setup in setups.items()}
    if observed_caps != dict(BASE_SETUP_CAPS):
        raise AssertionError("FNO V11 parent mixed caps drifted")
    if observed_pickers != dict(BASE_SETUP_PICKERS):
        raise AssertionError("FNO V11 parent pickers drifted")


def validate_registry(
    *,
    require_pinned_hash: bool = True,
    require_parent_contract: bool = True,
) -> None:
    if len(VARIANT_REGISTRY) != len(VARIANT_SPECS):
        raise AssertionError("FNO V11 variant IDs must be unique")
    if set(VARIANT_REGISTRY) != EXPECTED_VARIANT_IDS:
        raise AssertionError("The predeclared FNO V11 variant set changed")
    if VARIANT_SPECS[0].variant_id != CONTROL_VARIANT_ID:
        raise AssertionError("FNO V11 Stage 0 control must be first")

    for spec in VARIANT_SPECS:
        validate_variant_spec(spec)

    hashes = {variant_config_sha256(spec) for spec in VARIANT_SPECS}
    if len(hashes) != len(VARIANT_SPECS):
        raise AssertionError("Every FNO V11 variant config hash must be unique")

    payload = registry_payload()
    excluded = set(
        payload["execution_contract"]["excluded_already_tested_mechanisms"]
    )
    if excluded != {
        "GLOBAL_RV1",
        "EXPIRY_S4",
        "GLOBAL_RV1_PLUS_S4",
        "HISTORICAL_SAME_SLOT_RVOL",
        "UNIFORM_MAX_ENTRIES",
    }:
        raise AssertionError("FNO V11 excluded-mechanism contract changed")
    if payload["research_only"] is not True:
        raise AssertionError("FNO V11 lost research-only status")
    if payload["promotion_eligible"] is not False:
        raise AssertionError("FNO V11 acquired promotion eligibility")
    if payload["live_or_paper_authority"] is not False:
        raise AssertionError("FNO V11 acquired live or paper authority")

    if require_parent_contract:
        validate_v10_baseline_bindings()
    if require_pinned_hash and registry_sha256() != EXPECTED_REGISTRY_SHA256:
        raise AssertionError(
            "FNO V11 registry hash changed: "
            f"expected {EXPECTED_REGISTRY_SHA256}, observed {registry_sha256()}"
        )


if __name__ == "__main__":
    validate_registry()
    print(registry_sha256())
