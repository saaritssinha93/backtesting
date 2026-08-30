"""Immutable, research-only FNO V12 staged variant registry.

V12 starts from the locked standalone V11 Stage-10 profile.  Every declared
challenger changes one *mechanism family* while a fully resolved configuration
retains every other V11 value.  The registry is intentionally data-only: the
selection and execution runtimes consume these declarations, but neither may
invent thresholds from command-line arguments.

Stage-12 combinations are allowed only for variants whose resolved field
paths are disjoint.  A combination is still post-hoc research and never gains
paper- or live-trading authority.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, replace
from types import MappingProxyType
from typing import Any, Mapping, Sequence


CONFIG_SCHEMA_VERSION = "fno_v12_staged_variant_registry_v1"
REGISTRY_ID = "FNO_V12_V11_STAGE0_RESEARCH_20260830"
AUTHORITY = "BACKTEST_RESEARCH_ONLY"

CONTROL_VARIANT_ID = "V11_STAGE0_FROZEN_CONTROL"
CONTROL_STAGE_ID = "STAGE_00_FROZEN_V11"

VALID_SETUP_IDS = frozenset(
    f"{slot}_{side}"
    for slot in ("09:25", "09:30", "09:35", "09:40", "09:45")
    for side in ("LONG", "SHORT")
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

M2_SHORT_MODES = frozenset(
    {
        "DELAY_S4",
        "RECONFIRM_S3",
        "RECONFIRM_EXTEND_1TICK",
        "RECONFIRM_EXTEND_2BPS",
    }
)


@dataclass(frozen=True)
class ParentBinding:
    profile_id: str
    profile_sha256: str
    input_binding_sha256: str
    setup_book_sha256: str
    selection_variant: str
    gap_variant: str
    gap_identity_policy: str
    usable_sessions: int
    selected_candidates: int
    reference_trade_fingerprint_sha256: str
    stress_trade_fingerprint_sha256: str
    harsh_trade_fingerprint_sha256: str


PARENT_BINDING = ParentBinding(
    profile_id="V11_S10_POST_HOC_TOP2_1436C7D363",
    profile_sha256=(
        "8dfc162701705c0daa89d7ba2faa8dd7ddd3ff8eb6605370d96de1fdaa1f6fe1"
    ),
    input_binding_sha256=(
        "24e4da6c580693637bd7ce9c50c618b07d2e8a6a8dfded4498658d8eab113f2b"
    ),
    setup_book_sha256=(
        "ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675"
    ),
    selection_variant="0935_LONG_MOVE_MAX_050",
    gap_variant="MAX_2_BPS",
    gap_identity_policy="STRONG_REFERENCE_AND_IS_CHECK",
    usable_sessions=65,
    selected_candidates=1134,
    reference_trade_fingerprint_sha256=(
        "f171f7741aad48b7b50d634a8a63ef5b3c070669ea910818a6a569ed143d9833"
    ),
    stress_trade_fingerprint_sha256=(
        "cc85008deeefcf9085daa0aa16743367662bf0b8999c586a11bfd89ca656eabe"
    ),
    harsh_trade_fingerprint_sha256=(
        "c352beb835ae4035bee53a6d1c6854108055a726cce0ca83802dda885ad21afb"
    ),
)


@dataclass(frozen=True)
class SelectionConfig:
    """Complete V12 five-minute selection state.

    Maximum move filters are inclusive.  ``None`` means that the additional
    maximum is absent; the setup's frozen directional minimum still applies.
    """

    move_0935_long_max_pct: float | None = 0.50
    move_0940_long_min_pct: float = 0.40
    move_0925_long_max_pct: float | None = None
    move_0925_short_max_pct: float | None = None
    volume_0935_long_min: float = 1.00
    volume_0940_short_min: float = 1.00
    volume_0945_short_min: float = 1.00
    ema_gap_0925_short_persistence_min_ratio: float | None = None
    picker_0940_short: str = "max_move"


@dataclass(frozen=True)
class RuntimeConfig:
    """Complete V12 runtime overlay resolved over locked V11.

    ``None`` means the inherited V11 S+5 entry expiry.  The target tuple is
    inert while the M2 mode is ``None`` and supplies the reviewed BOTH default
    when a Stage-4 mode is activated.
    """

    m2_short_mode: str | None = None
    m2_short_setup_ids: tuple[str, ...] = ("09:25_SHORT", "09:30_SHORT")
    long_entry_expiry_minute: int | None = None


@dataclass(frozen=True)
class GapConfig:
    max_adverse_gap_bps: float = 2.0


BASE_SELECTION_CONFIG = SelectionConfig()
BASE_RUNTIME_CONFIG = RuntimeConfig()
BASE_GAP_CONFIG = GapConfig()

EMA_GAP_PERSISTENCE_ALGORITHM = {
    "algorithm_id": "V12_EMA_ADJUST_FALSE_RECURRENCE_SIDE_AWARE_GAPS_V1",
    "ema_recurrence": "EMA_T=ALPHA*CLOSE_T+(1-ALPHA)*EMA_T_MINUS_1",
    "prior_ema_formula": "(CURRENT_EMA-ALPHA*FIVE_MIN_CLOSE)/(1-ALPHA)",
    "alpha_formula": "2/(SPAN+1)",
    "spans": [9, 20, 50],
    "target_setup": "09:25_SHORT",
    "short_side_gaps": ["EMA20-EMA9", "EMA50-EMA20"],
    "prior_gap_requirement": "GT_0",
    "persistence_requirement": "CURRENT_GAP_GE_RATIO_TIMES_PRIOR_GAP",
    "bounds": "INCLUSIVE_WITH_1E_12_TOLERANCE",
}


@dataclass(frozen=True)
class ConfigOverride:
    path: str
    value: Any

    def payload(self) -> dict[str, Any]:
        return {"path": self.path, "value": self.value}


@dataclass(frozen=True)
class VariantSpec:
    variant_id: str
    stage_id: str
    family: str
    description: str
    overrides: tuple[ConfigOverride, ...] = ()

    @property
    def is_control(self) -> bool:
        return self.variant_id == CONTROL_VARIANT_ID

    @property
    def changed_fields(self) -> tuple[str, ...]:
        return tuple(item.path for item in self.overrides)

    def payload(self) -> dict[str, Any]:
        return {
            "variant_id": self.variant_id,
            "stage_id": self.stage_id,
            "family": self.family,
            "description": self.description,
            "overrides": [item.payload() for item in self.overrides],
            "changed_fields": list(self.changed_fields),
        }


@dataclass(frozen=True)
class ResolvedConfig:
    variant_id: str
    stage_id: str
    family: str
    description: str
    selection: SelectionConfig
    runtime: RuntimeConfig
    gap: GapConfig
    changed_fields: tuple[str, ...]
    component_variant_ids: tuple[str, ...] = ()
    post_hoc: bool = False

    def payload(self) -> dict[str, Any]:
        return {
            "variant_id": self.variant_id,
            "stage_id": self.stage_id,
            "family": self.family,
            "description": self.description,
            "selection": asdict(self.selection),
            "runtime": asdict(self.runtime),
            "gap": asdict(self.gap),
            "changed_fields": list(self.changed_fields),
            "component_variant_ids": list(self.component_variant_ids),
            "post_hoc": self.post_hoc,
            "research_only": True,
            "promotion_eligible": False,
            "live_or_paper_authority": False,
        }


def _o(path: str, value: Any) -> ConfigOverride:
    return ConfigOverride(path, value)


def _m2_short_variants() -> tuple[VariantSpec, ...]:
    targets = (
        ("0925_SHORT", "STAGE_04A_M2_0925_SHORT", ("09:25_SHORT",)),
        ("0930_SHORT", "STAGE_04B_M2_0930_SHORT", ("09:30_SHORT",)),
        (
            "BOTH_SHORT",
            "STAGE_04C_M2_BOTH_SHORT",
            ("09:25_SHORT", "09:30_SHORT"),
        ),
    )
    modes = (
        ("DELAY_S4", "cannot fill before S+4"),
        ("RECONFIRM_S3", "requires strict S+3 reconfirmation"),
        (
            "RECONFIRM_EXTEND_1TICK",
            "requires strict S+3 reconfirmation plus one-tick continuation",
        ),
        (
            "RECONFIRM_EXTEND_2BPS",
            "requires strict S+3 reconfirmation plus two-bps continuation",
        ),
    )
    variants: list[VariantSpec] = []
    for target_label, stage_id, setup_ids in targets:
        for mode, description in modes:
            overrides = [_o("runtime.m2_short_mode", mode)]
            if setup_ids != BASE_RUNTIME_CONFIG.m2_short_setup_ids:
                overrides.append(_o("runtime.m2_short_setup_ids", setup_ids))
            variants.append(
                VariantSpec(
                    f"V12_S04_M2_{target_label}_{mode}",
                    stage_id,
                    "ENTRY_M2_SHORT",
                    f"S+2-confirmed {', '.join(setup_ids)} {description}",
                    tuple(overrides),
                )
            )
    return tuple(variants)


def _opening_stretch_variants() -> tuple[VariantSpec, ...]:
    targets = (
        (
            "LONG_ONLY",
            "STAGE_05A_0925_LONG_STRETCH",
            ("selection.move_0925_long_max_pct",),
            "09:25 LONG",
        ),
        (
            "SHORT_ONLY",
            "STAGE_05B_0925_SHORT_STRETCH",
            ("selection.move_0925_short_max_pct",),
            "09:25 SHORT",
        ),
        (
            "BOTH",
            "STAGE_05C_0925_BOTH_STRETCH",
            (
                "selection.move_0925_long_max_pct",
                "selection.move_0925_short_max_pct",
            ),
            "both 09:25 sides",
        ),
    )
    thresholds = ((1.25, "125"), (1.00, "100"))
    return tuple(
        VariantSpec(
            f"V12_S05_0925_{target_label}_MOVE_MAX_{suffix}",
            stage_id,
            "SELECTION_0925_OPENING_STRETCH",
            f"{description} directional move maximum {threshold:.2f}% inclusive",
            tuple(_o(path, threshold) for path in paths),
        )
        for target_label, stage_id, paths, description in targets
        for threshold, suffix in thresholds
    )


def _late_volume_variants() -> tuple[VariantSpec, ...]:
    targets = (
        (
            "0935_LONG",
            "STAGE_06A_0935_LONG_VOLUME",
            ("selection.volume_0935_long_min",),
            "09:35 LONG",
        ),
        (
            "0940_SHORT",
            "STAGE_06B_0940_SHORT_VOLUME",
            ("selection.volume_0940_short_min",),
            "09:40 SHORT",
        ),
        (
            "0945_SHORT",
            "STAGE_06C_0945_SHORT_VOLUME",
            ("selection.volume_0945_short_min",),
            "09:45 SHORT",
        ),
        (
            "LATE_SHORT",
            "STAGE_06D_LATE_SHORT_VOLUME",
            (
                "selection.volume_0940_short_min",
                "selection.volume_0945_short_min",
            ),
            "09:40 and 09:45 SHORT",
        ),
    )
    thresholds = ((1.25, "125"), (1.50, "150"))
    return tuple(
        VariantSpec(
            f"V12_S06_{target_label}_VOLUME_MIN_{suffix}",
            stage_id,
            "SELECTION_FIVE_MINUTE_VOLUME_MIN",
            f"{description} five-minute volume ratio minimum {threshold:.2f} inclusive",
            tuple(_o(path, threshold) for path in paths),
        )
        for target_label, stage_id, paths, description in targets
        for threshold, suffix in thresholds
    )


VARIANT_SPECS: tuple[VariantSpec, ...] = (
    VariantSpec(
        CONTROL_VARIANT_ID,
        CONTROL_STAGE_ID,
        "CONTROL",
        "Frozen standalone V11 Stage-10 control",
    ),
    VariantSpec(
        "V12_S03A_0935_LONG_MOVE_MAX_060",
        "STAGE_03A_0935_LONG_MAX",
        "SELECTION_0935_LONG_MOVE_MAX",
        "09:35 LONG directional move maximum relaxed to 0.60% inclusive",
        (_o("selection.move_0935_long_max_pct", 0.60),),
    ),
    VariantSpec(
        "V12_S03A_0935_LONG_MOVE_MAX_075",
        "STAGE_03A_0935_LONG_MAX",
        "SELECTION_0935_LONG_MOVE_MAX",
        "09:35 LONG directional move maximum relaxed to 0.75% inclusive",
        (_o("selection.move_0935_long_max_pct", 0.75),),
    ),
    VariantSpec(
        "V12_S03A_0935_LONG_MOVE_NO_MAX",
        "STAGE_03A_0935_LONG_MAX",
        "SELECTION_0935_LONG_MOVE_MAX",
        "Remove only the additional 09:35 LONG directional move maximum",
        (_o("selection.move_0935_long_max_pct", None),),
    ),
    VariantSpec(
        "V12_S03B_0940_LONG_MOVE_MIN_030",
        "STAGE_03B_0940_LONG_MIN",
        "SELECTION_0940_LONG_MOVE_MIN",
        "09:40 LONG directional move minimum relaxed to 0.30% inclusive",
        (_o("selection.move_0940_long_min_pct", 0.30),),
    ),
    VariantSpec(
        "V12_S03B_0940_LONG_MOVE_MIN_050",
        "STAGE_03B_0940_LONG_MIN",
        "SELECTION_0940_LONG_MOVE_MIN",
        "09:40 LONG directional move minimum tightened to 0.50% inclusive",
        (_o("selection.move_0940_long_min_pct", 0.50),),
    ),
    *_m2_short_variants(),
    *_opening_stretch_variants(),
    *_late_volume_variants(),
    VariantSpec(
        "V12_S07_LONG_ENTRY_EXPIRY_4",
        "STAGE_07_LONG_ENTRY_EXPIRY",
        "ENTRY_LONG_EXPIRY",
        "All LONG pending entries expire after the S+4 fill check",
        (_o("runtime.long_entry_expiry_minute", 4),),
    ),
    VariantSpec(
        "V12_S07_LONG_ENTRY_EXPIRY_3",
        "STAGE_07_LONG_ENTRY_EXPIRY",
        "ENTRY_LONG_EXPIRY",
        "All LONG pending entries expire after the S+3 fill check",
        (_o("runtime.long_entry_expiry_minute", 3),),
    ),
    VariantSpec(
        "V12_S08A_0925_SHORT_EMA_GAP_PERSISTENCE_095",
        "STAGE_08A_0925_SHORT_EMA_PERSISTENCE",
        "SELECTION_0925_SHORT_EMA_GAP_PERSISTENCE",
        "09:25 SHORT current EMA gaps must retain at least 95% of prior gaps",
        (_o("selection.ema_gap_0925_short_persistence_min_ratio", 0.95),),
    ),
    VariantSpec(
        "V12_S08A_0925_SHORT_EMA_GAP_PERSISTENCE_100",
        "STAGE_08A_0925_SHORT_EMA_PERSISTENCE",
        "SELECTION_0925_SHORT_EMA_GAP_PERSISTENCE",
        "09:25 SHORT current EMA gaps must be non-contracting versus prior gaps",
        (_o("selection.ema_gap_0925_short_persistence_min_ratio", 1.00),),
    ),
    VariantSpec(
        "V12_S08_0940_SHORT_EQUAL_RANK_PICKER",
        "STAGE_08B_0940_SHORT_PICKER",
        "PICKER_0940_SHORT_EQUAL_RANK",
        "09:40 SHORT uses equal-weight move, volume, and liquidity ranks",
        (_o("selection.picker_0940_short", "v12_equal_rank"),),
    ),
    VariantSpec(
        "V12_S09_GAP_MAX_1_BPS",
        "STAGE_09_GAP_SENSITIVITY",
        "GAP_MAX_ADVERSE_BPS",
        "Maximum adverse trigger gap tightened to 1 bps",
        (_o("gap.max_adverse_gap_bps", 1.0),),
    ),
    VariantSpec(
        "V12_S09_GAP_MAX_3_BPS",
        "STAGE_09_GAP_SENSITIVITY",
        "GAP_MAX_ADVERSE_BPS",
        "Maximum adverse trigger gap relaxed to 3 bps",
        (_o("gap.max_adverse_gap_bps", 3.0),),
    ),
    VariantSpec(
        "V12_S09_GAP_MAX_5_BPS",
        "STAGE_09_GAP_SENSITIVITY",
        "GAP_MAX_ADVERSE_BPS",
        "Maximum adverse trigger gap relaxed to 5 bps",
        (_o("gap.max_adverse_gap_bps", 5.0),),
    ),
)

VARIANT_REGISTRY: Mapping[str, VariantSpec] = MappingProxyType(
    {spec.variant_id: spec for spec in VARIANT_SPECS}
)

EXPECTED_VARIANT_IDS = frozenset(
    {
        CONTROL_VARIANT_ID,
        "V12_S03A_0935_LONG_MOVE_MAX_060",
        "V12_S03A_0935_LONG_MOVE_MAX_075",
        "V12_S03A_0935_LONG_MOVE_NO_MAX",
        "V12_S03B_0940_LONG_MOVE_MIN_030",
        "V12_S03B_0940_LONG_MOVE_MIN_050",
        "V12_S07_LONG_ENTRY_EXPIRY_4",
        "V12_S07_LONG_ENTRY_EXPIRY_3",
        "V12_S08A_0925_SHORT_EMA_GAP_PERSISTENCE_095",
        "V12_S08A_0925_SHORT_EMA_GAP_PERSISTENCE_100",
        "V12_S08_0940_SHORT_EQUAL_RANK_PICKER",
        "V12_S09_GAP_MAX_1_BPS",
        "V12_S09_GAP_MAX_3_BPS",
        "V12_S09_GAP_MAX_5_BPS",
    }
    | {
        f"V12_S04_M2_{target}_{mode}"
        for target in ("0925_SHORT", "0930_SHORT", "BOTH_SHORT")
        for mode in M2_SHORT_MODES
    }
    | {
        f"V12_S05_0925_{target}_MOVE_MAX_{suffix}"
        for target in ("LONG_ONLY", "SHORT_ONLY", "BOTH")
        for suffix in ("125", "100")
    }
    | {
        f"V12_S06_{target}_VOLUME_MIN_{suffix}"
        for target in ("0935_LONG", "0940_SHORT", "0945_SHORT", "LATE_SHORT")
        for suffix in ("125", "150")
    }
)


@dataclass(frozen=True)
class BlockedTest:
    stage_id: str
    test_id: str
    reason: str
    status: str = "BLOCKED_VALIDITY"

    def payload(self) -> dict[str, str]:
        return asdict(self)


BLOCKED_TESTS: tuple[BlockedTest, ...] = (
    BlockedTest(
        "STAGE_01_DATA_VALIDITY",
        "POINT_IN_TIME_UNIVERSE_FULL_HISTORY",
        "The core history reuses a later static futures universe backward.",
    ),
    BlockedTest(
        "STAGE_01_DATA_VALIDITY",
        "AUG_26_COMPLETE_REPLAY",
        "2026-08-26 has no validated comparable full-session cache.",
    ),
    BlockedTest(
        "STAGE_01_DATA_VALIDITY",
        "UNIFORM_EXACT_1530_PATHS",
        "246 selected paths stop at 15:15 rather than the intended 15:30.",
    ),
    BlockedTest(
        "STAGE_02_FUTURES_EXECUTION",
        "ROLLING_FRONT_MONTH_FUTURES_1M",
        "Complete dated rolling futures one-minute price paths are absent.",
    ),
    BlockedTest(
        "STAGE_02_FUTURES_EXECUTION",
        "DATED_LOT_TICK_MARGIN_COSTS",
        "Historical lot, tick, margin, spread, and full cost snapshots are absent.",
    ),
    BlockedTest(
        "STAGE_08_STRUCTURAL_FILTERS",
        "FUTURES_OI_PERSISTENCE",
        "The cache has one signal OI observation but no causal two-bar OI sidecar.",
    ),
    BlockedTest(
        "STAGE_09_MARKET_CONTEXT",
        "INDEX_SECTOR_VWAP_ALIGNMENT",
        "Point-in-time index, sector, and dated membership histories are absent.",
    ),
    BlockedTest(
        "STAGE_09_MARKET_CONTEXT",
        "OPENING_MARKET_BREADTH",
        "A snapshot-bound causal opening-breadth series is absent.",
    ),
    BlockedTest(
        "STAGE_09_MARKET_CONTEXT",
        "HISTORICAL_FUTURES_SPREAD_DEPTH",
        "Historical bid/ask spread, depth, and impact observations are absent.",
    ),
    BlockedTest(
        "STAGE_10_PORTFOLIO_RISK",
        "ACTUAL_FUTURES_RISK_SIZING",
        "Dated futures prices, lots, and historical margins are incomplete.",
    ),
    BlockedTest(
        "STAGE_10_PORTFOLIO_RISK",
        "AGGREGATE_MARGIN_AND_STOP_RISK_CAP",
        "An executable futures capital ledger cannot be reconstructed honestly.",
    ),
    BlockedTest(
        "STAGE_11_EXIT_RESEARCH",
        "EXACT_1530_EXIT_GRID",
        "The mixed 15:15/15:30 path boundary invalidates an exact clock grid.",
    ),
    BlockedTest(
        "STAGE_11_EXIT_RESEARCH",
        "PATH_SAFE_MFE_MAE_EXIT_RULES",
        "Most stored excursion paths have boundary ambiguity.",
    ),
)


_SELECTION_FIELDS = frozenset(SelectionConfig.__dataclass_fields__)
_RUNTIME_FIELDS = frozenset(RuntimeConfig.__dataclass_fields__)
_GAP_FIELDS = frozenset(GapConfig.__dataclass_fields__)


def canonical_json_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


EMA_GAP_PERSISTENCE_ALGORITHM_SHA256 = canonical_json_sha256(
    EMA_GAP_PERSISTENCE_ALGORITHM
)


def get_spec(variant_id: str) -> VariantSpec:
    key = str(variant_id).strip().upper()
    if key not in VARIANT_REGISTRY:
        raise ValueError(
            f"Unknown FNO V12 variant {variant_id!r}; "
            f"allowed={sorted(VARIANT_REGISTRY)}"
        )
    return VARIANT_REGISTRY[key]


def _validate_number(value: Any, *, label: str, allow_none: bool = False) -> None:
    if value is None and allow_none:
        return
    if isinstance(value, bool):
        raise AssertionError(f"{label} must be numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise AssertionError(f"{label} must be numeric") from exc
    if not math.isfinite(number) or number <= 0:
        raise AssertionError(f"{label} must be finite and positive")


def _replace_path(
    selection: SelectionConfig,
    runtime: RuntimeConfig,
    gap: GapConfig,
    override: ConfigOverride,
) -> tuple[SelectionConfig, RuntimeConfig, GapConfig]:
    try:
        section, field = override.path.split(".", 1)
    except ValueError as exc:
        raise AssertionError(f"invalid V12 override path: {override.path}") from exc
    if section == "selection" and field in _SELECTION_FIELDS:
        return replace(selection, **{field: override.value}), runtime, gap
    if section == "runtime" and field in _RUNTIME_FIELDS:
        return selection, replace(runtime, **{field: override.value}), gap
    if section == "gap" and field in _GAP_FIELDS:
        return selection, runtime, replace(gap, **{field: override.value})
    raise AssertionError(f"unknown V12 override path: {override.path}")


def validate_resolved_config(config: ResolvedConfig) -> None:
    selection = config.selection
    for field in (
        "move_0935_long_max_pct",
        "move_0925_long_max_pct",
        "move_0925_short_max_pct",
        "ema_gap_0925_short_persistence_min_ratio",
    ):
        _validate_number(
            getattr(selection, field), label=f"selection.{field}", allow_none=True
        )
    for field in (
        "move_0940_long_min_pct",
        "volume_0935_long_min",
        "volume_0940_short_min",
        "volume_0945_short_min",
    ):
        _validate_number(getattr(selection, field), label=f"selection.{field}")
    if selection.picker_0940_short not in {"max_move", "v12_equal_rank"}:
        raise AssertionError("unsupported V12 09:40 SHORT picker")
    if config.runtime.m2_short_mode not in M2_SHORT_MODES | {None}:
        raise AssertionError("unsupported V12 M2 SHORT mode")
    allowed_m2_targets = {"09:25_SHORT", "09:30_SHORT"}
    targets = config.runtime.m2_short_setup_ids
    if (
        not targets
        or len(targets) != len(set(targets))
        or not set(targets).issubset(allowed_m2_targets)
    ):
        raise AssertionError("V12 M2 SHORT targets must be a unique non-empty subset")
    if config.runtime.long_entry_expiry_minute not in {None, 3, 4}:
        raise AssertionError("V12 LONG entry expiry must be inherited, 3, or 4")
    _validate_number(config.gap.max_adverse_gap_bps, label="gap max bps")
    if len(config.changed_fields) != len(set(config.changed_fields)):
        raise AssertionError("resolved V12 changed fields must be unique")


def validate_variant_spec(spec: VariantSpec) -> None:
    if not spec.variant_id or spec.variant_id != spec.variant_id.upper():
        raise AssertionError("V12 variant IDs must be non-empty uppercase strings")
    if not spec.stage_id or not spec.family or not spec.description.strip():
        raise AssertionError(f"incomplete V12 declaration: {spec.variant_id}")
    if spec.is_control:
        if spec.stage_id != CONTROL_STAGE_ID or spec.overrides:
            raise AssertionError("V12 Stage0 cannot contain an override")
        return
    if not spec.variant_id.startswith("V12_") or not spec.stage_id.startswith(
        "STAGE_"
    ):
        raise AssertionError(f"invalid V12 challenger identity: {spec.variant_id}")
    if not spec.overrides:
        raise AssertionError("each V12 challenger must change one mechanism")
    if len(spec.changed_fields) != len(set(spec.changed_fields)):
        raise AssertionError("one V12 variant cannot override a field twice")
    resolved = resolve_variant(spec, _skip_validation=True)
    validate_resolved_config(resolved)
    baseline = resolve_variant(CONTROL_VARIANT_ID, _skip_validation=True)
    for path in spec.changed_fields:
        section, field = path.split(".", 1)
        if getattr(getattr(resolved, section), field) == getattr(
            getattr(baseline, section), field
        ):
            raise AssertionError(
                f"V12 challenger repeats its control value: {spec.variant_id}.{path}"
            )


def resolve_variant(
    spec: VariantSpec | str, *, _skip_validation: bool = False
) -> ResolvedConfig:
    resolved_spec = get_spec(spec) if isinstance(spec, str) else spec
    if not _skip_validation:
        canonical = get_spec(resolved_spec.variant_id)
        if resolved_spec != canonical:
            raise ValueError(
                f"FNO V12 spec {resolved_spec.variant_id!r} differs from registry"
            )
    selection = BASE_SELECTION_CONFIG
    runtime = BASE_RUNTIME_CONFIG
    gap = BASE_GAP_CONFIG
    for override in resolved_spec.overrides:
        selection, runtime, gap = _replace_path(selection, runtime, gap, override)
    config = ResolvedConfig(
        variant_id=resolved_spec.variant_id,
        stage_id=resolved_spec.stage_id,
        family=resolved_spec.family,
        description=resolved_spec.description,
        selection=selection,
        runtime=runtime,
        gap=gap,
        changed_fields=resolved_spec.changed_fields,
        component_variant_ids=(),
        post_hoc=False,
    )
    if not _skip_validation:
        validate_resolved_config(config)
    return config


def compatible_for_merge(first: VariantSpec | str, second: VariantSpec | str) -> bool:
    left = get_spec(first) if isinstance(first, str) else get_spec(first.variant_id)
    right = get_spec(second) if isinstance(second, str) else get_spec(second.variant_id)
    if left.is_control or right.is_control or left.variant_id == right.variant_id:
        return False
    return not bool(set(left.changed_fields) & set(right.changed_fields))


def merge_resolved_configs(component_variant_ids: Sequence[str]) -> ResolvedConfig:
    requested = tuple(dict.fromkeys(str(value).strip().upper() for value in component_variant_ids))
    if len(requested) < 2:
        raise ValueError("V12 Stage12 requires at least two distinct components")
    catalog_order = {spec.variant_id: index for index, spec in enumerate(VARIANT_SPECS)}
    specs = [get_spec(variant_id) for variant_id in requested]
    if any(spec.is_control for spec in specs):
        raise ValueError("frozen V11 Stage0 cannot be a Stage12 component")
    specs.sort(key=lambda item: catalog_order[item.variant_id])
    occupied: set[str] = set()
    selection = BASE_SELECTION_CONFIG
    runtime = BASE_RUNTIME_CONFIG
    gap = BASE_GAP_CONFIG
    for spec in specs:
        overlap = occupied & set(spec.changed_fields)
        if overlap:
            raise ValueError(
                "incompatible V12 Stage12 components overlap fields: "
                f"{sorted(overlap)}"
            )
        occupied.update(spec.changed_fields)
        for override in spec.overrides:
            selection, runtime, gap = _replace_path(selection, runtime, gap, override)
    components = tuple(spec.variant_id for spec in specs)
    digest = hashlib.sha256("|".join(components).encode("utf-8")).hexdigest()[:12].upper()
    config = ResolvedConfig(
        variant_id=f"V12_S12_POST_HOC_{digest}",
        stage_id="STAGE_12_POST_HOC_COMBINATION",
        family="POST_HOC_COMBINATION",
        description="Post-hoc compatible combination of " + ", ".join(components),
        selection=selection,
        runtime=runtime,
        gap=gap,
        changed_fields=tuple(
            path for spec in specs for path in spec.changed_fields
        ),
        component_variant_ids=components,
        post_hoc=True,
    )
    validate_resolved_config(config)
    return config


def resolved_config_payload(config: ResolvedConfig | VariantSpec | str) -> dict[str, Any]:
    resolved = config if isinstance(config, ResolvedConfig) else resolve_variant(config)
    return {
        "schema_version": CONFIG_SCHEMA_VERSION,
        "registry_id": REGISTRY_ID,
        "parent_binding": asdict(PARENT_BINDING),
        "resolved_config": resolved.payload(),
    }


def resolved_config_sha256(config: ResolvedConfig | VariantSpec | str) -> str:
    return canonical_json_sha256(resolved_config_payload(config))


def registry_payload() -> dict[str, Any]:
    return {
        "schema_version": CONFIG_SCHEMA_VERSION,
        "registry_id": REGISTRY_ID,
        "authority": AUTHORITY,
        "parent_binding": asdict(PARENT_BINDING),
        "base_selection_config": asdict(BASE_SELECTION_CONFIG),
        "base_runtime_config": asdict(BASE_RUNTIME_CONFIG),
        "base_gap_config": asdict(BASE_GAP_CONFIG),
        "ema_gap_persistence_algorithm": {
            "sha256": EMA_GAP_PERSISTENCE_ALGORITHM_SHA256,
            "payload": EMA_GAP_PERSISTENCE_ALGORITHM,
        },
        "bounds_contract": {
            "selection_minimums": "INCLUSIVE",
            "selection_maximums": "INCLUSIVE",
            "price_move": "DIRECTIONAL_MAGNITUDE_PCT",
            "all_variants_start_from": "ALL_1241_INPUT_CANDIDATES",
            "rerank_after_selection": True,
        },
        "variants": [
            {
                **spec.payload(),
                "resolved_config_sha256": resolved_config_sha256(spec),
            }
            for spec in VARIANT_SPECS
        ],
        "blocked_tests": [record.payload() for record in BLOCKED_TESTS],
        "combination_contract": {
            "stage": "STAGE_12_POST_HOC_COMBINATION",
            "requires_disjoint_resolved_fields": True,
            "control_allowed_as_component": False,
            "post_hoc": True,
        },
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }


def registry_sha256() -> str:
    return canonical_json_sha256(registry_payload())


# Filled after the reviewed catalog is finalized.  A future change requires a
# new schema/registry ID and an explicitly reviewed replacement hash.
EXPECTED_REGISTRY_SHA256 = (
    "4948ba186095a5baea6b538a64255bc7304e96720ba98da512d6d21490328c35"
)


def validate_parent_binding(*, require_files: bool = False) -> None:
    import fno_v11_backtest as v11
    import fno_v11_gap_runtime as v11_gap

    observed = {
        "profile_id": v11.PROFILE_ID,
        "profile_sha256": v11.profile_sha256(),
        "input_binding_sha256": v11.EXPECTED_INPUT_BINDING_SHA256,
        "setup_book_sha256": v11.EXPECTED_SETUP_BOOK_SHA256,
        "selection_variant": v11.SELECTION_VARIANT,
        "gap_variant": v11.GAP_VARIANT,
        "gap_identity_policy": v11_gap.IDENTITY_POLICY,
        "usable_sessions": v11.EXPECTED_SESSION_COUNT,
        "selected_candidates": v11.EXPECTED_SELECTED_CANDIDATES,
        "reference_trade_fingerprint_sha256": (
            v11.EXPECTED_CLOSED_TRADE_FINGERPRINTS["REFERENCE_15_0"]
        ),
        "stress_trade_fingerprint_sha256": (
            v11.EXPECTED_CLOSED_TRADE_FINGERPRINTS["STRESS_20_2"]
        ),
        "harsh_trade_fingerprint_sha256": (
            v11.EXPECTED_CLOSED_TRADE_FINGERPRINTS["STRESS_25_5"]
        ),
    }
    expected = asdict(PARENT_BINDING)
    if observed != expected:
        mismatches = {
            key: {"expected": expected[key], "observed": observed[key]}
            for key in expected
            if expected[key] != observed[key]
        }
        raise AssertionError(f"frozen V11 parent binding drifted: {mismatches}")
    if require_files:
        v11.validate_fixed_contract(require_files=True)


def validate_registry(
    *, require_pinned_hash: bool = True, require_parent_contract: bool = True
) -> None:
    if len(VARIANT_REGISTRY) != len(VARIANT_SPECS):
        raise AssertionError("FNO V12 variant IDs must be unique")
    if set(VARIANT_REGISTRY) != EXPECTED_VARIANT_IDS:
        raise AssertionError("the predeclared FNO V12 variant set changed")
    if VARIANT_SPECS[0].variant_id != CONTROL_VARIANT_ID:
        raise AssertionError("frozen V11 Stage0 must be first")
    for spec in VARIANT_SPECS:
        validate_variant_spec(spec)
    hashes = {resolved_config_sha256(spec) for spec in VARIANT_SPECS}
    if len(hashes) != len(VARIANT_SPECS):
        raise AssertionError("every V12 resolved config hash must be unique")
    identities = [(item.stage_id, item.test_id) for item in BLOCKED_TESTS]
    if len(identities) != len(set(identities)):
        raise AssertionError("blocked V12 test identities must be unique")
    required_blocked_stages = {
        "STAGE_01_DATA_VALIDITY",
        "STAGE_02_FUTURES_EXECUTION",
        "STAGE_08_STRUCTURAL_FILTERS",
        "STAGE_09_MARKET_CONTEXT",
        "STAGE_10_PORTFOLIO_RISK",
        "STAGE_11_EXIT_RESEARCH",
    }
    if {item.stage_id for item in BLOCKED_TESTS} != required_blocked_stages:
        raise AssertionError("V12 blocked-stage coverage changed")
    payload = registry_payload()
    if payload["research_only"] is not True:
        raise AssertionError("V12 lost research-only status")
    if payload["promotion_eligible"] is not False:
        raise AssertionError("V12 gained promotion eligibility")
    if payload["live_or_paper_authority"] is not False:
        raise AssertionError("V12 gained live/paper authority")
    if require_parent_contract:
        validate_parent_binding(require_files=False)
    if require_pinned_hash and registry_sha256() != EXPECTED_REGISTRY_SHA256:
        raise AssertionError(
            "FNO V12 registry hash changed: "
            f"expected={EXPECTED_REGISTRY_SHA256} observed={registry_sha256()}"
        )


if __name__ == "__main__":
    validate_registry()
    print(registry_sha256())
