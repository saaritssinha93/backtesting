"""Run the complete research-only FNO V11 staged comparison.

Stage 0 is the hash-bound V10 Stage-7 + 09:35 LONG max-0.50 + Gap2
current-mixed control.  Every executable V11 challenger is replayed in its own
directory through the same 65-session candidate cache, one-minute state
machine, chronological portfolio ledger, and three cost scenarios.  Stages
that require unavailable point-in-time or futures-execution data fail closed
and are recorded as blocked rather than being approximated silently.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import multiprocessing
import shutil
import sys
from contextlib import nullcontext
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v10_backtest as v10_backtest
import fno_v10_backtest_config as locked_config
import fno_v10_experiment_backtest as experiment
import fno_v10_followup_challenger_research as filters
import fno_v10_gap_guard_research as gaps
import fno_v11_execution_runtime as execution_runtime
import fno_v11_gap_runtime as gap_runtime
import fno_v11_selection_runtime as selection_runtime
import fno_v11_variant_registry as registry
import fno_v8_windowed_1m_entry_backtest as engine


SCHEMA_VERSION = "fno_v11_full_staged_comparison_v2"
OUTPUT_ROOT = common.FNO_ROOT / "strategy_research" / "v11_fno_staged_research_v2"
WORKER_ISOLATION_POLICY = "FRESH_SPAWN_PROCESS_PER_NON_STAGE0_VARIANT"
FROZEN_V10_REFERENCE_RUN = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
    r"\v10_max050_gap2_full_history_v1"
    r"\run_20260830T163837220506+0530"
)
FROZEN_V10_REFERENCE_BINDING_SHA256 = {
    "provenance.json": "25a4cbdd5c362559991bd55c6329acd5ace35802033d523bbe350caef6af24ff",
    "artifact_inventory.json": "c5397a6c8a37198164c445d1acfb507bada053159b652e6762e6589d7aa05130",
    "scenarios/reference_15_0/candidate_order_audit.csv": "4e085a946eb133455fdca0df61a8c716791f3f8f9d176c268730a9635e648172",
}
FROZEN_V10_REFERENCE_SCHEMA_VERSION = "fno_v10_max050_gap2_full_history_v1"
FROZEN_STAGE0_SCENARIO = "REFERENCE_15_0"
BASE_GAP_VARIANT = "MAX_2_BPS"
BOOTSTRAP_REPLICATES = 4_000

_WORKER_BASELINE_SELECTED: pd.DataFrame | None = None
_WORKER_MINUTE_PATHS: pd.DataFrame | None = None
_WORKER_SESSIONS: list[date] | None = None
_WORKER_SEGMENTS: list[dict[str, Any]] | None = None
_WORKER_INPUT_BINDING_SHA256: str | None = None


@dataclass(frozen=True)
class ExperimentDefinition:
    variant_id: str
    stage_id: str
    family: str
    description: str
    registry_variant_id: str | None = None
    runtime_spec: execution_runtime.RuntimeSpec = execution_runtime.RuntimeSpec()
    gap_variant: str = BASE_GAP_VARIANT
    disabled_setup_id: str | None = None
    component_variant_ids: tuple[str, ...] = ()
    post_hoc: bool = False

    @property
    def is_control(self) -> bool:
        return self.variant_id == "V10_STAGE0_FROZEN_CONTROL"

    @property
    def is_development_baseline(self) -> bool:
        return self.variant_id == "V11_STAGE3_DETERMINISTIC_GAP_REBASELINE"

    def payload(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "runtime_spec": asdict(self.runtime_spec),
            "research_only": True,
            "promotion_eligible": False,
            "live_or_paper_authority": False,
        }


@dataclass(frozen=True)
class PreparedExperiment:
    definition: ExperimentDefinition
    candidates: pd.DataFrame
    decisions: pd.DataFrame
    setups: tuple[engine.V8Setup, ...]
    setup_patch: Mapping[str, Any]


def _selection_experiments() -> tuple[ExperimentDefinition, ...]:
    definitions: list[ExperimentDefinition] = []
    for spec in registry.VARIANT_SPECS:
        if spec.variant_id == registry.CONTROL_VARIANT_ID:
            continue
        if spec.selection_rule is not None:
            definitions.append(
                ExperimentDefinition(
                    variant_id=spec.variant_id,
                    stage_id="STAGE_05_SELECTION",
                    family="FIVE_MINUTE_SELECTION",
                    description=spec.description,
                    registry_variant_id=spec.variant_id,
                )
            )
    return tuple(definitions)


def _setup_experiments() -> tuple[ExperimentDefinition, ...]:
    definitions: list[ExperimentDefinition] = []
    for spec in registry.VARIANT_SPECS:
        if not any(
            (
                spec.disabled_setup_id is not None,
                spec.picker_override is not None,
                spec.cap_override is not None,
            )
        ):
            continue
        definitions.append(
            ExperimentDefinition(
                variant_id=spec.variant_id,
                stage_id="STAGE_06_SETUP_PICKER_CAP",
                family=spec.mechanism_type,
                description=spec.description,
                registry_variant_id=spec.variant_id,
            )
        )
    definitions.extend(
        (
            ExperimentDefinition(
                "V11_S6_DISABLE_0930_SHORT",
                "STAGE_06_SETUP_PICKER_CAP",
                "DISABLED_SETUP",
                "Disable only 09:30 SHORT as an isolated global-ledger ablation",
                disabled_setup_id="09:30_SHORT",
            ),
            ExperimentDefinition(
                "V11_S6_DISABLE_0940_SHORT",
                "STAGE_06_SETUP_PICKER_CAP",
                "DISABLED_SETUP",
                "Disable only 09:40 SHORT as an isolated global-ledger ablation",
                disabled_setup_id="09:40_SHORT",
            ),
        )
    )
    return tuple(definitions)


CONTROL = ExperimentDefinition(
    "V10_STAGE0_FROZEN_CONTROL",
    "STAGE_00_FROZEN_V10",
    "CONTROL",
    "Frozen V10 Stage7 + 09:35 LONG max-0.50 + Gap2 mixed-cap control",
    registry_variant_id=registry.CONTROL_VARIANT_ID,
)

DEVELOPMENT_BASELINE = ExperimentDefinition(
    "V11_STAGE3_DETERMINISTIC_GAP_REBASELINE",
    "STAGE_03_REBASELINE",
    "DETERMINISTIC_REBASELINE",
    "V10 intended rules replayed with V11 strong-reference gap identity",
    registry_variant_id=registry.CONTROL_VARIANT_ID,
)

ENTRY_EXPERIMENTS = (
    ExperimentDefinition(
        "V11_S4_0930_SHORT_ENTRY_NOT_BEFORE_S3",
        "STAGE_04_ENTRY_TIMING",
        "ENTRY_TIMING",
        "09:30 SHORT pending stop cannot fill before S+3",
        runtime_spec=execution_runtime.RuntimeSpec(
            entry_setup_id="09:30_SHORT", entry_not_before_minute=3
        ),
    ),
    ExperimentDefinition(
        "V11_S4_0940_SHORT_ENTRY_NOT_BEFORE_S3",
        "STAGE_04_ENTRY_TIMING",
        "ENTRY_TIMING",
        "09:40 SHORT pending stop cannot fill before S+3",
        runtime_spec=execution_runtime.RuntimeSpec(
            entry_setup_id="09:40_SHORT", entry_not_before_minute=3
        ),
    ),
)

EXIT_EXPERIMENTS = (
    ExperimentDefinition(
        "V11_S7_BREAK_EVEN_AFTER_050R_NEXT_BAR",
        "STAGE_07_EXIT_AND_GAP",
        "EXIT_RULE",
        "Arm entry-price stop after a completed +0.50R bar; effective next bar",
        runtime_spec=execution_runtime.RuntimeSpec(
            exit_rule="BREAK_EVEN_NEXT_BAR", exit_activation_r=0.50
        ),
    ),
    ExperimentDefinition(
        "V11_S7_BREAK_EVEN_AFTER_075R_NEXT_BAR",
        "STAGE_07_EXIT_AND_GAP",
        "EXIT_RULE",
        "Arm entry-price stop after a completed +0.75R bar; effective next bar",
        runtime_spec=execution_runtime.RuntimeSpec(
            exit_rule="BREAK_EVEN_NEXT_BAR", exit_activation_r=0.75
        ),
    ),
    ExperimentDefinition(
        "V11_S7_BREAK_EVEN_AFTER_100R_NEXT_BAR",
        "STAGE_07_EXIT_AND_GAP",
        "EXIT_RULE",
        "Arm entry-price stop after a completed +1.00R bar; effective next bar",
        runtime_spec=execution_runtime.RuntimeSpec(
            exit_rule="BREAK_EVEN_NEXT_BAR", exit_activation_r=1.00
        ),
    ),
    ExperimentDefinition(
        "V11_S7_LATE_1430_BE_AFTER_100R_NEXT_BAR",
        "STAGE_07_EXIT_AND_GAP",
        "EXIT_RULE",
        "At/after completed 14:30 bar, arm break-even after +1R; next bar only",
        runtime_spec=execution_runtime.RuntimeSpec(
            exit_rule="LATE_1430_BREAK_EVEN_NEXT_BAR", exit_activation_r=1.00
        ),
    ),
    ExperimentDefinition(
        "V11_S7_TRAIL_1R_AFTER_2R_NEXT_BAR",
        "STAGE_07_EXIT_AND_GAP",
        "EXIT_RULE",
        "After completed +2R bar, trail one initial R behind best price next bar",
        runtime_spec=execution_runtime.RuntimeSpec(
            exit_rule="TRAIL_1R_AFTER_2R_NEXT_BAR", exit_activation_r=2.00
        ),
    ),
    ExperimentDefinition(
        "V11_S7_REJECT_ALL_GAP_FILLS",
        "STAGE_07_EXIT_AND_GAP",
        "GAP_POLICY",
        "Reject every gap-through-trigger fill instead of the frozen Gap2 guard",
        gap_variant="REJECT_ALL_GAP_FILLS",
    ),
)

PORTFOLIO_EXPERIMENTS = (
    ExperimentDefinition(
        "V11_S9_SAME_SYMBOL_SAME_SIDE_MAX_2",
        "STAGE_09_PORTFOLIO",
        "PORTFOLIO_SYMBOL_LIMIT",
        "Allow two concurrent same-side reservations per symbol; opposite side remains prohibited",
        runtime_spec=execution_runtime.RuntimeSpec(same_side_symbol_limit=2),
    ),
)

INDIVIDUAL_EXPERIMENTS = (
    ENTRY_EXPERIMENTS
    + _selection_experiments()
    + _setup_experiments()
    + EXIT_EXPERIMENTS
    + PORTFOLIO_EXPERIMENTS
)
ALL_PREDECLARED_EXPERIMENTS = (
    CONTROL,
    DEVELOPMENT_BASELINE,
) + INDIVIDUAL_EXPERIMENTS


BLOCKED_TESTS: tuple[dict[str, str], ...] = (
    {
        "stage_id": "STAGE_01_DATA_VALIDITY",
        "test_id": "POINT_IN_TIME_UNIVERSE_FULL_HISTORY",
        "status": "BLOCKED_VALIDITY",
        "reason": "Dated masters/universes exist for only 13 recent sessions; the 59-session core reuses an Aug-11 universe backward.",
    },
    {
        "stage_id": "STAGE_01_DATA_VALIDITY",
        "test_id": "AUG_26_EXACT_1530_REPLAY",
        "status": "BLOCKED_VALIDITY",
        "reason": "Aug-26 equity 1-minute paths end at 15:15; no exact 15:30 current-strategy replay is available.",
    },
    {
        "stage_id": "STAGE_01_DATA_VALIDITY",
        "test_id": "FULL_EXACT_1530_PATHS",
        "status": "BLOCKED_VALIDITY",
        "reason": "246 of 1,134 selected candidate paths stop at 15:15; only 888 reach 15:30.",
    },
    {
        "stage_id": "STAGE_02_FUTURES_EXECUTION",
        "test_id": "ROLLING_FRONT_MONTH_FUTURES_1M",
        "status": "BLOCKED_VALIDITY",
        "reason": "Complete actual front-month futures 1-minute histories for MAY through SEP are absent.",
    },
    {
        "stage_id": "STAGE_02_FUTURES_EXECUTION",
        "test_id": "DATED_LOT_TICK_MARGIN_COSTS",
        "status": "BLOCKED_VALIDITY",
        "reason": "Full-history dated masters and historical SPAN/exposure margin snapshots are absent.",
    },
    {
        "stage_id": "STAGE_08_STRUCTURAL_FILTERS",
        "test_id": "FUTURES_PRICE_OI_PERSISTENCE",
        "status": "BLOCKED_VALIDITY",
        "reason": "Current immutable cache has futures OI selection but no complete rolling futures 1-minute price/execution path or causal OI-persistence sidecar.",
    },
    {
        "stage_id": "STAGE_08_STRUCTURAL_FILTERS",
        "test_id": "INDEX_SECTOR_VWAP_ALIGNMENT",
        "status": "BLOCKED_VALIDITY",
        "reason": "No snapshot-bound point-in-time index/sector histories and dated membership mappings cover all 65 sessions.",
    },
    {
        "stage_id": "STAGE_08_STRUCTURAL_FILTERS",
        "test_id": "ATR_NORMALIZED_RISK",
        "status": "BLOCKED_VALIDITY",
        "reason": "The frozen candidate cache lacks a bound prior-session ATR history; deriving it from partial paths would change the data contract.",
    },
    {
        "stage_id": "STAGE_09_PORTFOLIO",
        "test_id": "ACTUAL_FUTURES_RISK_SIZING",
        "status": "BLOCKED_VALIDITY",
        "reason": "Actual contract prices, dated lot sizes, and historical margins are not complete for full history.",
    },
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_frozen_v10_reference_binding(
    reference_run: Path | None = None,
) -> dict[str, Any]:
    """Fail closed unless the entire frozen V10 reference run is still intact."""

    root = (
        FROZEN_V10_REFERENCE_RUN if reference_run is None else reference_run
    ).expanduser().resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"frozen V10 reference run is missing: {root}")

    observed_hashes: dict[str, str] = {}
    for relative, expected_sha256 in FROZEN_V10_REFERENCE_BINDING_SHA256.items():
        path = (root / Path(relative)).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise FileNotFoundError(f"frozen V10 bound artifact is missing: {path}")
        observed_sha256 = _sha256_file(path)
        if observed_sha256 != expected_sha256:
            raise AssertionError(
                "frozen V10 bound artifact hash drifted: "
                f"{relative}: expected={expected_sha256} observed={observed_sha256}"
            )
        observed_hashes[relative] = observed_sha256

    provenance_path = root / "provenance.json"
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    if provenance.get("schema_version") != FROZEN_V10_REFERENCE_SCHEMA_VERSION:
        raise AssertionError("frozen V10 provenance schema drifted")
    if provenance.get("complete") is not True:
        raise AssertionError("frozen V10 reference run is not complete")
    command = tuple(str(value) for value in provenance.get("command", ()))
    if "--reference-only" not in command or "--all-usable-history" not in command:
        raise AssertionError(
            "frozen V10 reference run does not declare reference-only full-history scope"
        )
    scenario_ids = tuple(dict(provenance.get("scenarios", {})))
    if scenario_ids != (FROZEN_STAGE0_SCENARIO,):
        raise AssertionError(
            "frozen V10 scenario scope drifted: "
            f"expected={[FROZEN_STAGE0_SCENARIO]} observed={list(scenario_ids)}"
        )
    inventory_binding = dict(provenance.get("artifact_inventory", {}))
    if (
        inventory_binding.get("sha256")
        != FROZEN_V10_REFERENCE_BINDING_SHA256["artifact_inventory.json"]
    ):
        raise AssertionError("frozen V10 provenance inventory binding drifted")

    inventory_path = root / "artifact_inventory.json"
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    if inventory.get("schema_version") != FROZEN_V10_REFERENCE_SCHEMA_VERSION:
        raise AssertionError("frozen V10 artifact inventory schema drifted")
    artifacts = list(inventory.get("artifacts", ()))
    if not artifacts:
        raise AssertionError("frozen V10 artifact inventory is empty")
    inventory_hashes: dict[str, str] = {}
    for record in artifacts:
        relative = Path(str(record["relative_path"]))
        if relative.is_absolute():
            raise AssertionError("frozen V10 inventory contains an absolute path")
        path = (root / relative).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise FileNotFoundError(f"frozen V10 inventory artifact is missing: {path}")
        expected_bytes = int(record["bytes"])
        if path.stat().st_size != expected_bytes:
            raise AssertionError(
                f"frozen V10 inventory artifact size drifted: {relative}"
            )
        expected_sha256 = str(record["sha256"])
        observed_sha256 = _sha256_file(path)
        if observed_sha256 != expected_sha256:
            raise AssertionError(
                f"frozen V10 inventory artifact hash drifted: {relative}"
            )
        inventory_hashes[str(relative).replace("\\", "/")] = observed_sha256

    audit_relative = "scenarios/reference_15_0/candidate_order_audit.csv"
    if inventory_hashes.get(audit_relative) != observed_hashes[audit_relative]:
        raise AssertionError("frozen V10 audit hash disagrees with its inventory")
    return {
        "validated": True,
        "reference_run": str(root),
        "schema_version": FROZEN_V10_REFERENCE_SCHEMA_VERSION,
        "execution_scope": "REFERENCE_ONLY",
        "scenario_ids": [FROZEN_STAGE0_SCENARIO],
        "command": list(command),
        "bound_file_sha256": observed_hashes,
        "inventory_artifact_count": len(artifacts),
        "inventory_all_artifacts_valid": True,
    }


def _validate_completed_run_artifact_inventory(
    run_root: Path,
    provenance: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate a completed V11 run's inventory, including omission checks."""

    root = run_root.expanduser().resolve()
    inventory_path = (root / "artifact_inventory.json").resolve()
    if not inventory_path.is_relative_to(root) or not inventory_path.is_file():
        raise FileNotFoundError(f"V11 artifact inventory is missing: {inventory_path}")
    binding = dict(provenance.get("artifact_inventory", {}))
    bound_path_text = str(binding.get("path", "")).strip()
    if not bound_path_text or Path(bound_path_text).expanduser().resolve() != inventory_path:
        raise AssertionError("V11 provenance inventory path binding drifted")
    observed_inventory_sha256 = _sha256_file(inventory_path)
    if binding.get("sha256") != observed_inventory_sha256:
        raise AssertionError("V11 provenance inventory SHA-256 binding drifted")

    payload = json.loads(inventory_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise AssertionError("V11 artifact inventory schema drifted")
    records = list(payload.get("artifacts", ()))
    if not records:
        raise AssertionError("V11 artifact inventory is empty")
    listed_paths: set[str] = set()
    for record in records:
        relative = Path(str(record["relative_path"]))
        relative_text = str(relative).replace("\\", "/")
        if relative.is_absolute() or relative_text in listed_paths:
            raise AssertionError(
                f"V11 artifact inventory path is invalid or duplicated: {relative}"
            )
        path = (root / relative).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise FileNotFoundError(f"V11 inventoried artifact is missing: {path}")
        if path.stat().st_size != int(record["bytes"]):
            raise AssertionError(f"V11 artifact size drifted: {relative_text}")
        if _sha256_file(path) != str(record["sha256"]):
            raise AssertionError(f"V11 artifact hash drifted: {relative_text}")
        listed_paths.add(relative_text)

    excluded = {"artifact_inventory.json", "provenance.json"}
    actual_paths = {
        str(path.relative_to(root)).replace("\\", "/")
        for path in root.rglob("*")
        if path.is_file()
        and str(path.relative_to(root)).replace("\\", "/") not in excluded
    }
    if listed_paths != actual_paths:
        raise AssertionError(
            "V11 artifact inventory set is incomplete: "
            f"missing={sorted(actual_paths - listed_paths)[:10]} "
            f"extra={sorted(listed_paths - actual_paths)[:10]}"
        )
    return {
        "validated": True,
        "path": str(inventory_path),
        "sha256": observed_inventory_sha256,
        "artifact_count": len(records),
        "listed_set_matches_run_files": True,
        "all_sizes_and_hashes_valid": True,
    }


def _gap_spec(variant: str) -> gaps.GapGuardSpec:
    return next(spec for spec in gaps.GAP_GUARDS if spec.variant == variant)


def _passthrough_decisions(
    candidates: pd.DataFrame, definition: ExperimentDefinition
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "candidate_id": candidates["candidate_id"].astype(str),
            "variant_id": definition.variant_id,
            "stage_id": definition.stage_id,
            "setup_id": candidates["setup_id"].astype(str),
            "kept": True,
            "reason": "V11_RUNTIME_OR_GAP_PASSTHROUGH",
            "metric": "",
            "measured_value": np.nan,
            "threshold": np.nan,
            "comparator": "",
        }
    )


def _prepare_experiment(
    definition: ExperimentDefinition,
    baseline_selected: pd.DataFrame,
) -> PreparedExperiment:
    experiment.configure_engine(locked_config.ACTIVE_VARIANT)
    engine._confirmation_check = experiment._NEUTRAL_CONFIRMATION_CHECK
    base_setups = tuple(engine.ACTIVE_SETUPS)
    candidates = baseline_selected.copy(deep=True)
    decisions = _passthrough_decisions(candidates, definition)
    setups = base_setups
    patch_payload: dict[str, Any] = {
        "registry_variant_id": definition.registry_variant_id,
        "field_overrides": [],
        "disabled_setup_ids": [],
        "requires_runner_picker_hook": False,
    }

    if definition.registry_variant_id is not None:
        candidates, decisions = selection_runtime.apply_variant_to_selected_candidates(
            candidates, definition.registry_variant_id
        )
        setups, metadata = selection_runtime.derive_patched_engine_setups(
            base_setups, definition.registry_variant_id
        )
        patch_payload = {
            "registry_variant_id": metadata.variant_id,
            "registry_micro_stage_id": metadata.stage_id,
            "field_overrides": [asdict(item) for item in metadata.field_overrides],
            "disabled_setup_ids": list(metadata.disabled_setup_ids),
            "requires_runner_picker_hook": metadata.requires_runner_picker_hook,
            "picker_hook": (
                asdict(metadata.picker_hook) if metadata.picker_hook else None
            ),
        }

    if definition.disabled_setup_id is not None:
        target = candidates["setup_id"].astype(str).eq(definition.disabled_setup_id)
        decisions = _passthrough_decisions(candidates, definition)
        decisions.loc[target, "kept"] = False
        decisions.loc[target, "reason"] = "V11_DISABLED_SETUP_REJECTED"
        candidates = candidates.loc[~target].copy().reset_index(drop=True)
        setups = tuple(
            setup for setup in setups if setup.setup_id != definition.disabled_setup_id
        )
        patch_payload["disabled_setup_ids"] = [definition.disabled_setup_id]

    engine.ACTIVE_SETUPS = tuple(setups)
    if any(setup.picker == "min_volume" for setup in setups):
        if not patch_payload.get("requires_runner_picker_hook"):
            raise AssertionError("min_volume setup is missing its mandatory V11 hook")
    return PreparedExperiment(
        definition=definition,
        candidates=candidates,
        decisions=decisions,
        setups=tuple(setups),
        setup_patch=patch_payload,
    )


def _periods(
    sessions: Sequence[date], segments: Sequence[Mapping[str, Any]]
) -> list[tuple[str, tuple[date, ...]]]:
    ordered = tuple(sorted(set(sessions)))
    midpoint = len(ordered) // 2
    periods: list[tuple[str, tuple[date, ...]]] = [
        ("FULL_USABLE", ordered),
        (
            "CORE_59",
            tuple(
                day
                for day in ordered
                if day < v10_backtest.MAX050_GAP2_EXTENSION_DAY
            ),
        ),
        (
            "FORWARD_EXTENSION",
            tuple(
                day
                for day in ordered
                if day >= v10_backtest.MAX050_GAP2_EXTENSION_DAY
            ),
        ),
        ("FIRST_HALF", ordered[:midpoint]),
        ("SECOND_HALF", ordered[midpoint:]),
        ("LAST_14", ordered[-14:]),
    ]
    for year, month in sorted({(day.year, day.month) for day in ordered}):
        periods.append(
            (
                f"MONTH_{year:04d}_{month:02d}",
                tuple(day for day in ordered if (day.year, day.month) == (year, month)),
            )
        )
    for segment in segments:
        segment_id = str(dict(segment["segment"])["segment_id"])
        periods.append(
            (
                f"SEGMENT_{segment_id}",
                tuple(date.fromisoformat(value) for value in segment["sessions"]),
            )
        )
    return [(name, days) for name, days in periods if days]


def _metric_rows(
    audit: pd.DataFrame,
    sessions: Sequence[date],
    segments: Sequence[Mapping[str, Any]],
    *,
    definition: ExperimentDefinition,
    scenario: str,
    cost_bps: float,
    slippage_bps: float,
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    audit_days = audit["session_date"].map(engine._parse_day)
    metric_rows: list[dict[str, Any]] = []
    full_daily = pd.DataFrame()
    spec = _gap_spec(definition.gap_variant)
    for period, days in _periods(sessions, segments):
        subset = audit.loc[audit_days.isin(set(days))].copy()
        row, daily = gaps.metric_row(
            subset,
            days,
            dataset="ALL_USABLE_HISTORY",
            period=period,
            scenario=scenario,
            spec=spec,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
        )
        row.update(
            {
                "variant_id": definition.variant_id,
                "stage_id": definition.stage_id,
                "family": definition.family,
                "description": definition.description,
                "gap_variant": definition.gap_variant,
                "post_hoc": definition.post_hoc,
            }
        )
        metric_rows.append(row)
        if period == "FULL_USABLE":
            full_daily = daily.copy()
            full_daily["variant_id"] = definition.variant_id
            full_daily["stage_id"] = definition.stage_id
            full_daily["family"] = definition.family
    return metric_rows, full_daily


def _bootstrap_row(
    audit: pd.DataFrame,
    sessions: Sequence[date],
    *,
    definition: ExperimentDefinition,
    scenario: str,
) -> dict[str, Any]:
    ordered = tuple(sorted(set(sessions)))
    closed = audit.loc[gaps._closed_mask(audit)].copy()
    closed["_day"] = closed["session_date"].map(engine._parse_day)
    closed["_return"] = pd.to_numeric(closed["net_return_pct"], errors="coerce")
    closed["_pnl"] = pd.to_numeric(closed["net_pnl_rs"], errors="coerce")
    by_day = closed.groupby("_day", sort=False).agg(
        positive_points=("_return", lambda values: float(values[values > 0].sum())),
        loss_points=("_return", lambda values: float(-values[values < 0].sum())),
        net_points=("_return", "sum"),
        net_pnl_rs=("_pnl", "sum"),
    )
    positive = np.asarray(
        [float(by_day.at[day, "positive_points"]) if day in by_day.index else 0.0 for day in ordered]
    )
    losses = np.asarray(
        [float(by_day.at[day, "loss_points"]) if day in by_day.index else 0.0 for day in ordered]
    )
    points = np.asarray(
        [float(by_day.at[day, "net_points"]) if day in by_day.index else 0.0 for day in ordered]
    )
    pnl = np.asarray(
        [float(by_day.at[day, "net_pnl_rs"]) if day in by_day.index else 0.0 for day in ordered]
    )
    seed_material = f"{SCHEMA_VERSION}|{definition.variant_id}|{scenario}".encode()
    seed = int.from_bytes(hashlib.sha256(seed_material).digest()[:8], "big")
    rng = np.random.default_rng(seed)
    sampled = rng.integers(0, len(ordered), size=(BOOTSTRAP_REPLICATES, len(ordered)))
    sampled_profit = positive[sampled].sum(axis=1)
    sampled_loss = losses[sampled].sum(axis=1)
    sampled_pf = np.divide(
        sampled_profit,
        sampled_loss,
        out=np.full_like(sampled_profit, np.inf),
        where=sampled_loss > 0,
    )
    finite_pf = sampled_pf[np.isfinite(sampled_pf)]
    daily_pnl = pnl[sampled].mean(axis=1)
    net_points = points[sampled].sum(axis=1)
    daily_total = pd.Series(pnl, index=ordered)
    total_pnl = float(daily_total.sum())
    positive_days = daily_total.loc[daily_total > 0].sort_values(ascending=False)
    best10_share = (
        float(positive_days.head(10).sum() / total_pnl * 100.0)
        if total_pnl > 0
        else math.nan
    )
    monthly = daily_total.groupby([day.strftime("%Y-%m") for day in ordered]).sum()
    best_month_share = (
        float(monthly.max() / total_pnl * 100.0) if total_pnl > 0 else math.nan
    )
    return {
        "variant_id": definition.variant_id,
        "stage_id": definition.stage_id,
        "scenario": scenario,
        "bootstrap_replicates": BOOTSTRAP_REPLICATES,
        "bootstrap_unit": "SESSION",
        "pf_p025": (
            float(np.quantile(finite_pf, 0.025)) if len(finite_pf) else math.nan
        ),
        "pf_median": (
            float(np.quantile(finite_pf, 0.50)) if len(finite_pf) else math.nan
        ),
        "mean_daily_pnl_rs_p025": float(np.quantile(daily_pnl, 0.025)),
        "mean_daily_pnl_rs_median": float(np.quantile(daily_pnl, 0.50)),
        "net_points_p025": float(np.quantile(net_points, 0.025)),
        "net_points_median": float(np.quantile(net_points, 0.50)),
        "best_10_positive_days_share_pct": best10_share,
        "best_month_share_pct": best_month_share,
        "research_only": True,
        "promotion_eligible": False,
    }


def _scenario_output(
    run_dir: Path,
    prepared: PreparedExperiment,
    audit: pd.DataFrame,
    daily: pd.DataFrame,
    summary: Mapping[str, Any],
    scenario: str,
) -> dict[str, str]:
    scenario_dir = (
        run_dir
        / "stages"
        / prepared.definition.stage_id
        / prepared.definition.variant_id
        / "scenarios"
        / scenario.lower()
    )
    scenario_dir.mkdir(parents=True, exist_ok=True)
    audit_path = scenario_dir / "candidate_order_audit.csv"
    trades_path = scenario_dir / "closed_trades.csv"
    daily_path = scenario_dir / "daywise.csv"
    summary_path = scenario_dir / "summary.json"
    common.atomic_write_csv(audit, audit_path)
    common.atomic_write_csv(audit.loc[gaps._closed_mask(audit)], trades_path)
    common.atomic_write_csv(daily, daily_path)
    common.atomic_write_json(summary_path, gaps._json_ready(dict(summary)))
    return {
        "audit": str(audit_path.resolve()),
        "closed_trades": str(trades_path.resolve()),
        "daywise": str(daily_path.resolve()),
        "summary": str(summary_path.resolve()),
    }


def _control_parity_record(
    observed: pd.DataFrame, reference_path: Path
) -> dict[str, Any]:
    """Require economic parity while losslessly reporting state-label drift."""

    try:
        strict = gaps.validate_control_parity(observed, reference_path)
        return {
            **strict,
            "parity_level": "LEGACY_PARITY_CONTRACT_EXACT",
            "legacy_parity_contract_passed": True,
            "status_reason_only_fallback_passed": False,
            "economic_parity": True,
            "non_economic_status_reason_mismatches": 0,
            "mismatch_candidate_ids": [],
        }
    except AssertionError as exc:
        reference = pd.read_csv(reference_path)
        columns = (
            "candidate_id",
            "filled",
            "entry_time",
            "entry_price",
            "stop_price",
            "target_price",
            "exit_time",
            "exit_price",
            "exit_reason",
            "gross_return_pct",
            "net_return_pct",
            "quantity",
            "gross_pnl_rs",
            "estimated_cost_rs",
            "net_pnl_rs",
        )
        left = observed.loc[:, columns].copy().sort_values("candidate_id").reset_index(
            drop=True
        )
        right = reference.loc[:, columns].copy().sort_values("candidate_id").reset_index(
            drop=True
        )
        if left["candidate_id"].astype(str).tolist() != right["candidate_id"].astype(
            str
        ).tolist():
            raise AssertionError("Stage0 candidate IDs differ from frozen V10") from exc
        numeric = {
            "entry_price",
            "stop_price",
            "target_price",
            "exit_price",
            "gross_return_pct",
            "net_return_pct",
            "quantity",
            "gross_pnl_rs",
            "estimated_cost_rs",
            "net_pnl_rs",
        }
        mismatches: dict[str, int] = {}
        for column in columns:
            if column == "candidate_id":
                continue
            if column in numeric:
                equal = np.isclose(
                    pd.to_numeric(left[column], errors="coerce").to_numpy(float),
                    pd.to_numeric(right[column], errors="coerce").to_numpy(float),
                    rtol=0.0,
                    atol=1e-9,
                    equal_nan=True,
                )
            else:
                equal = (
                    left[column].fillna("<NA>").astype(str).to_numpy()
                    == right[column].fillna("<NA>").astype(str).to_numpy()
                )
            count = int((~equal).sum())
            if count:
                mismatches[column] = count
        if mismatches:
            raise AssertionError(
                f"Stage0 economic parity failed after strict-state drift: {mismatches}"
            ) from exc
        state_left = observed.sort_values("candidate_id").reset_index(drop=True)
        state_right = reference.sort_values("candidate_id").reset_index(drop=True)
        status_count = int(
            (
                state_left["status"].fillna("<NA>").astype(str).to_numpy()
                != state_right["status"].fillna("<NA>").astype(str).to_numpy()
            ).sum()
        )
        reason_count = int(
            (
                state_left["reason"].fillna("<NA>").astype(str).to_numpy()
                != state_right["reason"].fillna("<NA>").astype(str).to_numpy()
            ).sum()
        )
        status_mismatch = (
            state_left["status"].fillna("<NA>").astype(str).to_numpy()
            != state_right["status"].fillna("<NA>").astype(str).to_numpy()
        )
        reason_mismatch = (
            state_left["reason"].fillna("<NA>").astype(str).to_numpy()
            != state_right["reason"].fillna("<NA>").astype(str).to_numpy()
        )
        mismatch_mask = status_mismatch | reason_mismatch
        mismatch_ids = (
            state_left.loc[mismatch_mask, "candidate_id"].astype(str).tolist()
        )
        observed_filled = state_left["filled"].fillna(False).astype(bool).to_numpy()
        reference_filled = state_right["filled"].fillna(False).astype(bool).to_numpy()
        if bool((mismatch_mask & (observed_filled | reference_filled)).any()):
            raise AssertionError(
                "Stage0 status/reason fallback touched a filled candidate"
            ) from exc
        return {
            "passed": True,
            "parity_level": "ECONOMIC_PLUS_UNFILLED_STATUS_REASON_ALLOWLIST",
            "candidate_rows": len(left),
            "columns_compared": [*columns, "status", "reason"],
            "reference_path": str(reference_path.resolve()),
            "reference_sha256": _sha256_file(reference_path),
            "legacy_parity_contract_passed": False,
            "status_reason_only_fallback_passed": True,
            "economic_parity": True,
            "strict_parity_error": str(exc),
            "status_mismatches": status_count,
            "reason_mismatches": reason_count,
            "non_economic_status_reason_mismatches": len(mismatch_ids),
            "mismatch_candidate_ids": mismatch_ids,
            "note": (
                "The replay differs only in status/reason for "
                f"{len(mismatch_ids)} unfilled candidate(s); candidate IDs, "
                "fills, orders, exits, returns, P&L, and the pinned benchmark "
                "are identical under the legacy parity contract."
            ),
        }


_REPAIR_AUDIT_COLUMNS = (
    "setup_id",
    "symbol",
    "side",
    "status",
    "reason",
    "filled",
    "confirmation_time",
    "entry_time",
    "entry_price",
    "stop_price",
    "target_price",
    "exit_time",
    "exit_price",
    "exit_reason",
    "gross_return_pct",
    "net_return_pct",
    "quantity",
    "gross_pnl_rs",
    "estimated_cost_rs",
    "net_pnl_rs",
    "portfolio_decision",
    "portfolio_reject_reason",
    "gap_guard_observed",
    "gap_guard_rejected",
    "gap_guard_adverse_bps",
)

_REPAIR_NUMERIC_COLUMNS = frozenset(
    {
        "entry_price",
        "stop_price",
        "target_price",
        "exit_price",
        "gross_return_pct",
        "net_return_pct",
        "quantity",
        "gross_pnl_rs",
        "estimated_cost_rs",
        "net_pnl_rs",
        "gap_guard_adverse_bps",
    }
)


def _stage0_stage3_repair_delta(
    run_records: Mapping[str, Any],
    metrics: pd.DataFrame,
    run_dir: Path,
    scenarios: Sequence[tuple[str, float, float]],
) -> tuple[Path, Path, dict[str, Any]]:
    """Persist every Stage0-to-Stage3 candidate/economic repair difference."""

    control_record = run_records[CONTROL.variant_id]
    baseline_record = run_records[DEVELOPMENT_BASELINE.variant_id]
    delta_parts: list[pd.DataFrame] = []
    scenario_summaries: dict[str, Any] = {}
    comparison_columns_used: tuple[str, ...] | None = None
    for scenario, _, _ in scenarios:
        control_path = Path(control_record["artifacts"][scenario]["audit"])
        baseline_path = Path(baseline_record["artifacts"][scenario]["audit"])
        control_audit = pd.read_csv(control_path).sort_values("candidate_id").reset_index(
            drop=True
        )
        baseline_audit = pd.read_csv(baseline_path).sort_values("candidate_id").reset_index(
            drop=True
        )
        control_ids = control_audit["candidate_id"].astype(str)
        baseline_ids = baseline_audit["candidate_id"].astype(str)
        if control_ids.tolist() != baseline_ids.tolist():
            raise AssertionError(
                f"Stage0/Stage3 candidate identity drift in {scenario}"
            )
        compare_columns = tuple(
            column
            for column in _REPAIR_AUDIT_COLUMNS
            if column in control_audit.columns and column in baseline_audit.columns
        )
        if comparison_columns_used is None:
            comparison_columns_used = compare_columns
        elif comparison_columns_used != compare_columns:
            raise AssertionError("Stage0/Stage3 repair columns drifted by scenario")
        changed_by_column: dict[str, np.ndarray] = {}
        changed_any = np.zeros(len(control_audit), dtype=bool)
        for column in compare_columns:
            if column in _REPAIR_NUMERIC_COLUMNS:
                left = pd.to_numeric(control_audit[column], errors="coerce").to_numpy(
                    dtype=float
                )
                right = pd.to_numeric(baseline_audit[column], errors="coerce").to_numpy(
                    dtype=float
                )
                changed = ~np.isclose(
                    left,
                    right,
                    rtol=0.0,
                    atol=1e-12,
                    equal_nan=True,
                )
            else:
                left = control_audit[column].fillna("<NA>").astype(str).to_numpy()
                right = baseline_audit[column].fillna("<NA>").astype(str).to_numpy()
                changed = left != right
            changed_by_column[column] = changed
            changed_any |= changed

        changed_indices = np.flatnonzero(changed_any)
        records: list[dict[str, Any]] = []
        for index in changed_indices:
            changed_columns = [
                column
                for column in compare_columns
                if bool(changed_by_column[column][index])
            ]
            record: dict[str, Any] = {
                "scenario": scenario,
                "candidate_id": str(control_ids.iloc[index]),
                "changed_columns": ";".join(changed_columns),
            }
            for column in compare_columns:
                record[f"stage0_{column}"] = control_audit.iloc[index][column]
                record[f"stage3_{column}"] = baseline_audit.iloc[index][column]
            records.append(record)
        if records:
            delta_parts.append(pd.DataFrame(records))

        full = metrics.loc[
            metrics["period"].eq("FULL_USABLE")
            & metrics["scenario"].eq(scenario)
        ].set_index("variant_id")
        stage0_metric = full.loc[CONTROL.variant_id]
        stage3_metric = full.loc[DEVELOPMENT_BASELINE.variant_id]
        scenario_summaries[scenario] = {
            "candidate_rows": len(control_audit),
            "changed_candidate_rows": len(changed_indices),
            "stage0_fills": int(stage0_metric["fills"]),
            "stage3_fills": int(stage3_metric["fills"]),
            "fill_delta": int(stage3_metric["fills"] - stage0_metric["fills"]),
            "net_return_points_delta": float(
                stage3_metric["net_return_points"]
                - stage0_metric["net_return_points"]
            ),
            "net_pnl_rs_delta": float(
                stage3_metric["net_pnl_rs"] - stage0_metric["net_pnl_rs"]
            ),
            "profit_factor_delta": float(
                stage3_metric["profit_factor"] - stage0_metric["profit_factor"]
            ),
            "max_daily_drawdown_points_delta": float(
                stage3_metric["max_daily_drawdown_points"]
                - stage0_metric["max_daily_drawdown_points"]
            ),
        }

    delta = (
        pd.concat(delta_parts, ignore_index=True)
        if delta_parts
        else pd.DataFrame(columns=("scenario", "candidate_id", "changed_columns"))
    )
    delta_path = run_dir / "stage0_to_stage3_candidate_delta.csv"
    summary_path = run_dir / "stage0_to_stage3_repair_summary.json"
    summary = {
        "schema_version": SCHEMA_VERSION,
        "frozen_control_variant_id": CONTROL.variant_id,
        "comparison_baseline_variant_id": DEVELOPMENT_BASELINE.variant_id,
        "legacy_identity_policy": "UNRETAINED_OBJECT_ID_SET",
        "repaired_identity_policy": gap_runtime.IDENTITY_POLICY,
        "candidate_delta_path": str(delta_path.resolve()),
        "comparison_columns": list(comparison_columns_used or ()),
        "changed_candidate_scenario_rows": len(delta),
        "scenarios": scenario_summaries,
    }
    common.atomic_write_csv(delta, delta_path)
    common.atomic_write_json(summary_path, gaps._json_ready(summary))
    return delta_path, summary_path, summary


def _sorted_frame(path: Path, keys: Sequence[str]) -> pd.DataFrame:
    frame = pd.read_csv(path)
    missing = [key for key in keys if key not in frame.columns]
    if missing:
        raise AssertionError(f"determinism frame missing keys {missing}: {path}")
    return frame.sort_values(list(keys), kind="stable").reset_index(drop=True)


def _determinism_attestation(
    prior_run: Path,
    current_run: Path,
    current_scenario_order: Sequence[str],
    current_input_binding_sha256: str,
) -> dict[str, Any]:
    """Fail closed unless two full runs are economically and artifact identical."""

    prior = prior_run.expanduser().resolve()
    current = current_run.resolve()
    if prior == current:
        raise ValueError("determinism attestation requires two distinct runs")
    prior_provenance_path = prior / "provenance.json"
    if not prior_provenance_path.is_file():
        raise FileNotFoundError(f"prior provenance missing: {prior_provenance_path}")
    prior_provenance = json.loads(prior_provenance_path.read_text(encoding="utf-8"))
    if not bool(prior_provenance.get("complete")):
        raise AssertionError("prior V11 run is not complete")
    if prior_provenance.get("schema_version") != SCHEMA_VERSION:
        raise AssertionError("determinism runs use different schema versions")
    prior_latest_path = prior.parent / "latest.json"
    if not prior_latest_path.is_file():
        raise FileNotFoundError(
            f"prior V11 latest pointer is missing: {prior_latest_path}"
        )
    prior_latest = json.loads(prior_latest_path.read_text(encoding="utf-8"))
    if prior_latest.get("schema_version") != SCHEMA_VERSION:
        raise AssertionError("prior V11 latest pointer schema drifted")
    if Path(str(prior_latest.get("run_dir", ""))).expanduser().resolve() != prior:
        raise AssertionError("prior V11 latest pointer targets a different run")
    prior_provenance_sha256 = _sha256_file(prior_provenance_path)
    if prior_latest.get("provenance_sha256") != prior_provenance_sha256:
        raise AssertionError("prior V11 provenance hash differs from latest pointer")
    prior_inventory_validation = _validate_completed_run_artifact_inventory(
        prior, prior_provenance
    )
    prior_input_binding_sha256 = str(
        prior_provenance.get("input_binding_sha256", "")
    )
    if not prior_input_binding_sha256:
        raise AssertionError("prior V11 run lacks an input binding")
    if prior_input_binding_sha256 != current_input_binding_sha256:
        raise AssertionError("determinism runs use different input bindings")
    required_scenarios = tuple(scenario for scenario, _, _ in gaps.COST_SCENARIOS)
    prior_scenario_order = tuple(prior_provenance.get("scenario_order", ()))
    prior_v11_scenario_order = tuple(
        prior_provenance.get("v11_scenario_order", prior_scenario_order)
    )
    prior_stage0_scenario_order = tuple(
        prior_provenance.get("stage0_scenario_order", ())
    )
    observed_current_order = tuple(current_scenario_order)
    if set(prior_scenario_order) != set(required_scenarios):
        raise AssertionError("prior run does not contain the required scenario set")
    if set(observed_current_order) != set(required_scenarios):
        raise AssertionError("current run does not contain the required scenario set")
    if prior_scenario_order != tuple(reversed(observed_current_order)):
        raise AssertionError(
            "scenario-order attestation requires exact reverse run order"
        )
    if prior_v11_scenario_order != prior_scenario_order:
        raise AssertionError("prior V11 scenario-order declaration is inconsistent")
    if prior_stage0_scenario_order != (FROZEN_STAGE0_SCENARIO,):
        raise AssertionError("prior frozen Stage0 scope is not reference-only")
    prior_frozen_binding = dict(
        prior_provenance.get("frozen_v10_reference_binding", {})
    )
    if prior_frozen_binding.get("validated") is not True:
        raise AssertionError("prior run lacks a validated frozen V10 binding")
    if dict(prior_frozen_binding.get("bound_file_sha256", {})) != dict(
        FROZEN_V10_REFERENCE_BINDING_SHA256
    ):
        raise AssertionError("prior frozen V10 reference binding hashes drifted")

    prior_sources = {
        name: dict(record)["sha256"]
        for name, record in dict(prior_provenance["source_hashes"]).items()
    }
    current_sources = {
        path.name: _sha256_file(path)
        for path in sorted((current / "source").glob("*.py"))
    }
    if prior_sources != current_sources:
        raise AssertionError("determinism runs use different source snapshots")

    def stage_hashes(root: Path) -> dict[str, str]:
        return {
            str(path.relative_to(root)).replace("\\", "/"): _sha256_file(path)
            for path in sorted((root / "stages").rglob("*"))
            if path.is_file()
            and "STAGE_00_FROZEN_V10" not in path.parts
        }

    prior_stage_hashes = stage_hashes(prior)
    current_stage_hashes = stage_hashes(current)
    if set(prior_stage_hashes) != set(current_stage_hashes):
        missing_current = sorted(set(prior_stage_hashes) - set(current_stage_hashes))
        extra_current = sorted(set(current_stage_hashes) - set(prior_stage_hashes))
        raise AssertionError(
            "determinism stage artifact set drifted: "
            f"missing={missing_current[:5]} extra={extra_current[:5]}"
        )
    mismatched_stage_files = [
        relative
        for relative, digest in prior_stage_hashes.items()
        if current_stage_hashes[relative] != digest
    ]
    if mismatched_stage_files:
        raise AssertionError(
            "determinism stage artifacts changed: "
            f"{mismatched_stage_files[:10]}"
        )

    aggregate_specs: dict[str, tuple[str, ...]] = {
        "all_period_metrics.csv": ("variant_id", "scenario", "period"),
        "all_daywise.csv": ("variant_id", "scenario", "session_date"),
        "bootstrap_and_concentration.csv": ("variant_id", "scenario"),
        "development_gates.csv": ("variant_id",),
    }
    aggregate_rows: dict[str, int] = {}
    for filename, keys in aggregate_specs.items():
        left = _sorted_frame(prior / filename, keys)
        right = _sorted_frame(current / filename, keys)
        try:
            pd.testing.assert_frame_equal(
                left,
                right,
                check_dtype=False,
                check_exact=False,
                rtol=0.0,
                atol=1e-12,
            )
        except AssertionError as exc:
            raise AssertionError(
                f"determinism aggregate changed: {filename}: {exc}"
            ) from exc
        aggregate_rows[filename] = len(left)

    exact_root_files = (
        "all_input_candidates.csv",
        "v10_stage0_selected_candidates.csv",
        "v10_stage0_selection_decisions.csv",
        "source_segments.json",
    )
    for filename in exact_root_files:
        if _sha256_file(prior / filename) != _sha256_file(current / filename):
            raise AssertionError(f"determinism root artifact changed: {filename}")

    control_parity_relative = Path(
        "stages",
        CONTROL.stage_id,
        CONTROL.variant_id,
        "control_parity.json",
    )
    parity_levels: dict[str, dict[str, str]] = {}
    for label, root in (("prior", prior), ("current", current)):
        parity_payload = json.loads(
            (root / control_parity_relative).read_text(encoding="utf-8")
        )
        parity_scenarios = set(parity_payload) - {"benchmark"}
        if parity_scenarios != {FROZEN_STAGE0_SCENARIO}:
            raise AssertionError(
                f"{label} frozen Stage0 parity scope is not reference-only"
            )
        scenario_parity = dict(parity_payload[FROZEN_STAGE0_SCENARIO])
        if not bool(scenario_parity.get("economic_parity")):
            raise AssertionError(
                f"{label} frozen Stage0 lacks economic parity in "
                f"{FROZEN_STAGE0_SCENARIO}"
            )
        parity_levels[label] = {
            FROZEN_STAGE0_SCENARIO: str(
                scenario_parity.get("parity_level", "UNKNOWN")
            )
        }

    return {
        "schema_version": SCHEMA_VERSION,
        "passed": True,
        "prior_run": str(prior),
        "prior_provenance_sha256": prior_provenance_sha256,
        "prior_latest_pointer": str(prior_latest_path.resolve()),
        "prior_latest_pointer_validated": True,
        "current_run": str(current),
        "input_binding_sha256": current_input_binding_sha256,
        "prior_artifact_inventory_validation": prior_inventory_validation,
        "source_snapshot_count": len(current_sources),
        "stage_artifact_count": len(current_stage_hashes),
        "stage_artifact_byte_hash_parity": True,
        "aggregate_economic_parity": True,
        "aggregate_rows": aggregate_rows,
        "scenario_order_invariance_tested": True,
        "prior_scenario_order": list(prior_scenario_order),
        "current_scenario_order": list(observed_current_order),
        "frozen_stage0_attested_by_pinned_economic_contract": True,
        "frozen_stage0_attestation_scope": FROZEN_STAGE0_SCENARIO,
        "frozen_stage0_stress_audits_available": False,
        "frozen_stage0_parity_levels": parity_levels,
        "stage_byte_hash_scope": "STAGE3_AND_ALL_V11_VARIANTS_EXCLUDING_LEGACY_STAGE0",
        "worker_isolation_policy": WORKER_ISOLATION_POLICY,
    }


def _run_experiment(
    definition: ExperimentDefinition,
    baseline_selected: pd.DataFrame,
    minute_paths: pd.DataFrame,
    sessions: Sequence[date],
    segments: Sequence[Mapping[str, Any]],
    run_dir: Path,
    *,
    scenarios: Sequence[tuple[str, float, float]],
) -> tuple[list[dict[str, Any]], list[pd.DataFrame], list[dict[str, Any]], dict[str, Any]]:
    scenario_ids = tuple(scenario for scenario, _, _ in scenarios)
    if definition.is_control and scenario_ids != (FROZEN_STAGE0_SCENARIO,):
        raise AssertionError(
            "frozen Stage0 must run only its pinned REFERENCE_15_0 scenario"
        )
    prepared = _prepare_experiment(definition, baseline_selected)
    variant_dir = run_dir / "stages" / definition.stage_id / definition.variant_id
    variant_dir.mkdir(parents=True, exist_ok=True)
    common.atomic_write_csv(prepared.candidates, variant_dir / "selected_candidates.csv")
    common.atomic_write_csv(prepared.decisions, variant_dir / "selection_decisions.csv")
    common.atomic_write_json(
        variant_dir / "resolved_experiment.json",
        gaps._json_ready(
            {
                "schema_version": SCHEMA_VERSION,
                "experiment": definition.payload(),
                "setup_patch": dict(prepared.setup_patch),
                "setups": [asdict(setup) for setup in prepared.setups],
                "selected_candidate_count": len(prepared.candidates),
            }
        ),
    )

    metric_rows: list[dict[str, Any]] = []
    daily_parts: list[pd.DataFrame] = []
    robustness_rows: list[dict[str, Any]] = []
    artifacts: dict[str, Any] = {}
    parity: dict[str, Any] = {}
    gap_spec = _gap_spec(definition.gap_variant)
    for scenario, cost_bps, slippage_bps in scenarios:
        print(
            f"[FNO-V11] {definition.stage_id} {definition.variant_id} "
            f"scenario={scenario} candidates={len(prepared.candidates)}",
            flush=True,
        )
        policy = experiment._entry_policy_for_variant(
            locked_config.ACTIVE_VARIANT,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
            square_off="15:30",
            eod_policy="LAST_REAL_BAR_SENSITIVITY",
        )
        hook_context = (
            nullcontext()
            if definition.is_control
            else execution_runtime.installed_runtime_hooks(
                definition.runtime_spec,
                allow_composite=definition.post_hoc,
            )
        )
        gap_context = (
            gaps.installed_gap_guard(gap_spec)
            if definition.is_control
            else gap_runtime.installed_gap_guard(gap_spec)
        )
        with hook_context:
            with gap_context:
                audit = experiment._NEUTRAL_RUN_BACKTEST(
                    prepared.candidates,
                    minute_paths,
                    variant=definition.variant_id,
                    policy=policy,
                    target_exposure_per_entry_rs=50_000.0,
                )
        audit = audit.copy()
        audit["v11_variant_id"] = definition.variant_id
        audit["v11_stage_id"] = definition.stage_id
        audit["v11_family"] = definition.family
        audit["v11_scenario"] = scenario
        audit["research_only"] = True
        audit["promotion_eligible"] = False
        rows, daily = _metric_rows(
            audit,
            sessions,
            segments,
            definition=definition,
            scenario=scenario,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
        )
        metric_rows.extend(rows)
        daily_parts.append(daily)
        robustness_rows.append(
            _bootstrap_row(
                audit,
                sessions,
                definition=definition,
                scenario=scenario,
            )
        )
        full_summary = next(row for row in rows if row["period"] == "FULL_USABLE")
        artifacts[scenario] = _scenario_output(
            run_dir, prepared, audit, daily, full_summary, scenario
        )
        if definition.is_control:
            reference_path = (
                FROZEN_V10_REFERENCE_RUN
                / "scenarios"
                / scenario.lower()
                / "candidate_order_audit.csv"
            )
            if not reference_path.is_file():
                raise FileNotFoundError(
                    f"frozen Stage0 parity audit is missing: {reference_path}"
                )
            parity[scenario] = _control_parity_record(audit, reference_path)
            parity["benchmark"] = v10_backtest.validate_current_mixed_benchmark(
                full_summary
            )
    if definition.is_control and set(parity) != {
        FROZEN_STAGE0_SCENARIO,
        "benchmark",
    }:
        raise AssertionError("frozen Stage0 reference parity record is incomplete")
    common.atomic_write_json(variant_dir / "control_parity.json", parity)
    return metric_rows, daily_parts, robustness_rows, {
        "definition": definition.payload(),
        "artifacts": artifacts,
        "control_parity": parity,
    }


def _frame_content_sha256(frame: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    digest.update(
        json.dumps(
            {
                "columns": [str(column) for column in frame.columns],
                "dtypes": [str(dtype) for dtype in frame.dtypes],
                "rows": len(frame),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    try:
        row_hashes = pd.util.hash_pandas_object(
            frame,
            index=True,
            categorize=False,
        ).to_numpy(dtype="uint64", copy=False)
    except TypeError:
        normalized = frame.copy()
        for column in normalized.columns:
            if normalized[column].dtype == "object":
                normalized[column] = normalized[column].map(repr)
        row_hashes = pd.util.hash_pandas_object(
            normalized,
            index=True,
            categorize=False,
        ).to_numpy(dtype="uint64", copy=False)
    digest.update(row_hashes.tobytes())
    return digest.hexdigest()


def _input_binding_sha256(
    selected: pd.DataFrame,
    minute_paths: pd.DataFrame,
    sessions: Sequence[date],
    segments: Sequence[Mapping[str, Any]],
) -> str:
    payload = {
        "sessions": [day.isoformat() for day in sessions],
        "segments": gaps._json_ready(list(segments)),
        "selected_sha256": _frame_content_sha256(selected),
        "minute_paths_sha256": _frame_content_sha256(minute_paths),
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _worker_initialize() -> None:
    global _WORKER_BASELINE_SELECTED
    global _WORKER_MINUTE_PATHS
    global _WORKER_SESSIONS
    global _WORKER_SEGMENTS
    global _WORKER_INPUT_BINDING_SHA256
    (
        candidates,
        minute_paths,
        segments,
        sessions,
        _,
        _,
    ) = v10_backtest._load_all_usable_max050_gap2_history()
    selection_spec = filters.SPEC_BY_NAME[
        v10_backtest.MAX050_GAP2_SELECTION_VARIANT
    ]
    selected, _ = filters.selection_overlay(candidates, selection_spec)
    _WORKER_BASELINE_SELECTED = selected
    _WORKER_MINUTE_PATHS = minute_paths
    _WORKER_SESSIONS = list(sessions)
    _WORKER_SEGMENTS = list(segments)
    _WORKER_INPUT_BINDING_SHA256 = _input_binding_sha256(
        selected,
        minute_paths,
        sessions,
        segments,
    )


def _worker_run_experiment(
    task: tuple[
        ExperimentDefinition,
        str,
        tuple[tuple[str, float, float], ...],
        str,
    ]
) -> tuple[
    str,
    list[dict[str, Any]],
    list[pd.DataFrame],
    list[dict[str, Any]],
    dict[str, Any],
]:
    definition, run_dir_text, scenarios, expected_input_binding = task
    if any(
        item is None
        for item in (
            _WORKER_BASELINE_SELECTED,
            _WORKER_MINUTE_PATHS,
            _WORKER_SESSIONS,
            _WORKER_SEGMENTS,
            _WORKER_INPUT_BINDING_SHA256,
        )
    ):
        raise AssertionError("FNO V11 worker dataset was not initialized")
    if _WORKER_INPUT_BINDING_SHA256 != expected_input_binding:
        raise AssertionError(
            "FNO V11 worker input binding differs from the parent snapshot"
        )
    rows, daily, robustness, record = _run_experiment(
        definition,
        _WORKER_BASELINE_SELECTED,
        _WORKER_MINUTE_PATHS,
        _WORKER_SESSIONS,
        _WORKER_SEGMENTS,
        Path(run_dir_text),
        scenarios=scenarios,
    )
    record["worker_input_binding_sha256"] = _WORKER_INPUT_BINDING_SHA256
    return definition.variant_id, rows, daily, robustness, record


def _worker_pool_kwargs(workers: int) -> dict[str, Any]:
    """Return a process-pool contract that cannot reuse mutable engine state."""

    if isinstance(workers, bool) or not isinstance(workers, int):
        raise ValueError("workers must be an integer")
    if workers < 1 or workers > 4:
        raise ValueError("workers must be in [1, 4]")
    return {
        "max_workers": workers,
        "mp_context": multiprocessing.get_context("spawn"),
        "initializer": _worker_initialize,
        "max_tasks_per_child": 1,
    }


def _safe_ratio(value: Any, baseline: Any) -> float:
    try:
        numerator = float(value)
        denominator = float(baseline)
    except (TypeError, ValueError):
        return math.nan
    if not math.isfinite(numerator) or not math.isfinite(denominator) or denominator == 0:
        return math.nan
    return numerator / denominator


_CLOSED_TRADE_FINGERPRINT_COLUMNS = (
    "candidate_id",
    "setup_id",
    "symbol",
    "side",
    "entry_time",
    "entry_price",
    "stop_price",
    "target_price",
    "exit_time",
    "exit_price",
    "exit_reason",
    "gross_return_pct",
    "net_return_pct",
    "quantity",
    "gross_pnl_rs",
    "estimated_cost_rs",
    "net_pnl_rs",
)


def _closed_trade_economic_fingerprints(
    run_records: Mapping[str, Any],
    scenarios: Sequence[tuple[str, float, float]],
) -> dict[str, dict[str, str]]:
    """Hash only closed-trade membership and economics, not variant labels."""

    fingerprints: dict[str, dict[str, str]] = {}
    for variant_id, run_record in run_records.items():
        scenario_hashes: dict[str, str] = {}
        artifacts = dict(run_record["artifacts"])
        for scenario, _, _ in scenarios:
            if scenario not in artifacts:
                continue
            trades_path = Path(artifacts[scenario]["closed_trades"])
            trades = pd.read_csv(trades_path)
            missing = sorted(
                set(_CLOSED_TRADE_FINGERPRINT_COLUMNS) - set(trades.columns)
            )
            if missing:
                raise AssertionError(
                    f"closed-trade fingerprint columns missing for {variant_id}: {missing}"
                )
            canonical = trades.loc[:, _CLOSED_TRADE_FINGERPRINT_COLUMNS].copy()
            canonical["candidate_id"] = canonical["candidate_id"].astype(str)
            canonical = canonical.sort_values("candidate_id", kind="stable").reset_index(
                drop=True
            )
            payload = canonical.to_csv(
                index=False,
                lineterminator="\n",
                float_format="%.17g",
            ).encode("utf-8")
            scenario_hashes[scenario] = hashlib.sha256(payload).hexdigest()
        if not scenario_hashes:
            raise AssertionError(
                f"no closed-trade fingerprints were produced for {variant_id}"
            )
        fingerprints[str(variant_id)] = scenario_hashes
    return fingerprints


def _development_gates(
    metrics: pd.DataFrame,
    economic_fingerprints: Mapping[str, Mapping[str, str]] | None = None,
) -> pd.DataFrame:
    full = metrics.loc[metrics["period"].eq("FULL_USABLE")].copy()
    forward = metrics.loc[metrics["period"].eq("FORWARD_EXTENSION")].copy()
    baseline_full = full.loc[
        full["variant_id"].eq(DEVELOPMENT_BASELINE.variant_id)
    ].set_index("scenario")
    baseline_forward = forward.loc[
        forward["variant_id"].eq(DEVELOPMENT_BASELINE.variant_id)
    ].set_index("scenario")
    frozen_control_full = full.loc[
        full["variant_id"].eq(CONTROL.variant_id)
    ].set_index("scenario")
    frozen_control_forward = forward.loc[
        forward["variant_id"].eq(CONTROL.variant_id)
    ].set_index("scenario")
    required_scenarios = tuple(scenario for scenario, _, _ in gaps.COST_SCENARIOS)
    if set(baseline_full.index) != set(required_scenarios):
        raise AssertionError("V11 Stage3 comparison baseline is incomplete")
    if set(baseline_forward.index) != set(required_scenarios):
        raise AssertionError("V11 Stage3 forward comparison baseline is incomplete")
    if set(frozen_control_full.index) != {FROZEN_STAGE0_SCENARIO}:
        raise AssertionError("frozen V10 Stage0 must contain reference metrics only")
    if set(frozen_control_forward.index) != {FROZEN_STAGE0_SCENARIO}:
        raise AssertionError("frozen V10 Stage0 forward metrics must be reference only")
    rows: list[dict[str, Any]] = []
    for variant_id, variant_rows in full.groupby("variant_id", sort=False):
        definition_row = variant_rows.iloc[0]
        indexed = variant_rows.set_index("scenario")
        forward_indexed = forward.loc[forward["variant_id"].eq(variant_id)].set_index(
            "scenario"
        )
        is_control = variant_id == CONTROL.variant_id
        is_baseline = variant_id == DEVELOPMENT_BASELINE.variant_id
        if is_control:
            if set(indexed.index) != {FROZEN_STAGE0_SCENARIO}:
                raise AssertionError("frozen Stage0 full-period scope is not reference-only")
            if set(forward_indexed.index) != {FROZEN_STAGE0_SCENARIO}:
                raise AssertionError("frozen Stage0 forward scope is not reference-only")
            observed = indexed.loc[FROZEN_STAGE0_SCENARIO]
            baseline = baseline_full.loc[FROZEN_STAGE0_SCENARIO]
            observed_mdd = float(observed["max_daily_drawdown_points"])
            baseline_mdd = float(baseline["max_daily_drawdown_points"])
            material_columns = tuple(
                column
                for column in (
                    "fills",
                    "wins",
                    "losses",
                    "profit_factor",
                    "net_return_points",
                    "net_pnl_rs",
                    "max_daily_drawdown_points",
                )
                if column in indexed.columns and column in baseline_full.columns
            )
            material_result_change = any(
                not math.isclose(
                    float(observed[column]),
                    float(baseline[column]),
                    rel_tol=0.0,
                    abs_tol=1e-12,
                )
                for column in material_columns
            )
            reference_fingerprint_equal: bool | None = None
            if economic_fingerprints is not None:
                control_fingerprints = economic_fingerprints[CONTROL.variant_id]
                baseline_fingerprints = economic_fingerprints[
                    DEVELOPMENT_BASELINE.variant_id
                ]
                if set(control_fingerprints) != {FROZEN_STAGE0_SCENARIO}:
                    raise AssertionError(
                        "frozen Stage0 fingerprint scope is not reference-only"
                    )
                if set(baseline_fingerprints) != set(required_scenarios):
                    raise AssertionError("Stage3 fingerprints are incomplete")
                reference_fingerprint_equal = (
                    control_fingerprints[FROZEN_STAGE0_SCENARIO]
                    == baseline_fingerprints[FROZEN_STAGE0_SCENARIO]
                )
                material_result_change = not reference_fingerprint_equal
            rows.append(
                {
                    "variant_id": variant_id,
                    "stage_id": definition_row["stage_id"],
                    "family": definition_row["family"],
                    "comparison_baseline_variant_id": DEVELOPMENT_BASELINE.variant_id,
                    "gate_evaluation_scope": "ARCHIVAL_REFERENCE_ONLY_NOT_DEVELOPMENT_ELIGIBLE",
                    "is_control": True,
                    "is_development_baseline": False,
                    "development_gate_passed": False,
                    "material_result_change": material_result_change,
                    "closed_trade_economic_parity_all_scenarios": np.nan,
                    "closed_trade_economic_parity_reference": (
                        reference_fingerprint_equal
                        if reference_fingerprint_equal is not None
                        else not material_result_change
                    ),
                    "development_improvement_passed": False,
                    "gate_classification": "FROZEN_LEGACY_CONTROL_REFERENCE_ONLY",
                    "failed_check_count": 0,
                    "failed_checks": "",
                    "worst_case_net_ratio_vs_baseline": np.nan,
                    "reference_net_ratio_vs_baseline": _safe_ratio(
                        observed["net_return_points"],
                        baseline["net_return_points"],
                    ),
                    "reference_mdd_ratio_vs_baseline": _safe_ratio(
                        observed_mdd, baseline_mdd
                    ),
                    "reference_net_ratio_vs_frozen_stage0": 1.0,
                    "reference_mdd_ratio_vs_frozen_stage0": 1.0,
                    "robust_mdd_within_105pct_all_scenarios": np.nan,
                    "promotion_gate_passed": False,
                    "promotion_blocker": "FROZEN_REFERENCE_ONLY_ARCHIVAL_CONTROL",
                }
            )
            continue
        if set(indexed.index) != set(required_scenarios):
            raise AssertionError(f"incomplete full-period scenarios for {variant_id}")
        if set(forward_indexed.index) != set(required_scenarios):
            raise AssertionError(f"incomplete forward scenarios for {variant_id}")
        checks: dict[str, bool] = {}
        net_ratios: list[float] = []
        robust_mdd_checks: dict[str, bool] = {}
        material_result_change = False
        material_columns = tuple(
            column
            for column in (
                "fills",
                "wins",
                "losses",
                "profit_factor",
                "net_return_points",
                "net_pnl_rs",
                "max_daily_drawdown_points",
            )
            if column in indexed.columns and column in baseline_full.columns
        )
        for scenario in required_scenarios:
            observed = indexed.loc[scenario]
            baseline = baseline_full.loc[scenario]
            checks[f"net_at_least_baseline_{scenario}"] = float(
                observed["net_return_points"]
            ) >= float(baseline["net_return_points"]) - 1e-12
            checks[f"pf_at_least_baseline_{scenario}"] = float(
                observed["profit_factor"]
            ) >= float(baseline["profit_factor"]) - 1e-12
            net_ratios.append(
                _safe_ratio(observed["net_return_points"], baseline["net_return_points"])
            )
            checks[f"forward_nonnegative_{scenario}"] = float(
                forward_indexed.loc[scenario, "net_return_points"]
            ) >= -1e-12
            observed_mdd_scenario = float(observed["max_daily_drawdown_points"])
            baseline_mdd_scenario = float(
                baseline["max_daily_drawdown_points"]
            )
            robust_mdd_checks[
                f"mdd_within_105pct_baseline_{scenario}"
            ] = observed_mdd_scenario <= baseline_mdd_scenario * 1.05 + 1e-12
            for column in material_columns:
                observed_value = float(observed[column])
                baseline_value = float(baseline[column])
                if not math.isclose(
                    observed_value,
                    baseline_value,
                    rel_tol=0.0,
                    abs_tol=1e-12,
                ):
                    material_result_change = True
        fingerprint_equal_all_scenarios: bool | None = None
        reference_fingerprint_equal: bool | None = None
        if economic_fingerprints is not None:
            baseline_fingerprints = economic_fingerprints[
                DEVELOPMENT_BASELINE.variant_id
            ]
            observed_fingerprints = economic_fingerprints[str(variant_id)]
            if set(baseline_fingerprints) != set(required_scenarios):
                raise AssertionError("Stage3 fingerprints are incomplete")
            if set(observed_fingerprints) != set(required_scenarios):
                raise AssertionError(f"fingerprints are incomplete for {variant_id}")
            fingerprint_equal_all_scenarios = all(
                observed_fingerprints[scenario]
                == baseline_fingerprints[scenario]
                for scenario in required_scenarios
            )
            reference_fingerprint_equal = (
                observed_fingerprints[FROZEN_STAGE0_SCENARIO]
                == baseline_fingerprints[FROZEN_STAGE0_SCENARIO]
            )
            material_result_change = not fingerprint_equal_all_scenarios
        observed_mdd = float(
            indexed.loc[FROZEN_STAGE0_SCENARIO, "max_daily_drawdown_points"]
        )
        baseline_mdd = float(
            baseline_full.loc[
                FROZEN_STAGE0_SCENARIO, "max_daily_drawdown_points"
            ]
        )
        checks["reference_mdd_within_105pct_baseline"] = (
            observed_mdd <= baseline_mdd * 1.05 + 1e-12
        )
        passed = bool(all(checks.values()))
        failures = [name for name, value in checks.items() if not value]
        improvement_passed = bool(
            passed and material_result_change and not is_baseline
        )
        if is_baseline:
            classification = "DETERMINISTIC_COMPARISON_BASELINE"
        elif improvement_passed:
            classification = "PASS_IMPROVEMENT"
        elif passed:
            classification = "PARITY_NO_EFFECT"
        else:
            classification = "FAIL"
        rows.append(
            {
                "variant_id": variant_id,
                "stage_id": definition_row["stage_id"],
                "family": definition_row["family"],
                "comparison_baseline_variant_id": DEVELOPMENT_BASELINE.variant_id,
                "gate_evaluation_scope": "ALL_THREE_V11_COST_SCENARIOS",
                "is_control": False,
                "is_development_baseline": is_baseline,
                "development_gate_passed": passed,
                "material_result_change": material_result_change,
                "closed_trade_economic_parity_all_scenarios": (
                    fingerprint_equal_all_scenarios
                    if fingerprint_equal_all_scenarios is not None
                    else not material_result_change
                ),
                "closed_trade_economic_parity_reference": (
                    reference_fingerprint_equal
                    if reference_fingerprint_equal is not None
                    else not material_result_change
                ),
                "development_improvement_passed": improvement_passed,
                "gate_classification": classification,
                "failed_check_count": len(failures),
                "failed_checks": ";".join(failures),
                "worst_case_net_ratio_vs_baseline": float(np.nanmin(net_ratios)),
                "reference_net_ratio_vs_baseline": _safe_ratio(
                    indexed.loc[FROZEN_STAGE0_SCENARIO, "net_return_points"],
                    baseline_full.loc[
                        FROZEN_STAGE0_SCENARIO, "net_return_points"
                    ],
                ),
                "reference_mdd_ratio_vs_baseline": _safe_ratio(
                    observed_mdd, baseline_mdd
                ),
                "reference_net_ratio_vs_frozen_stage0": _safe_ratio(
                    indexed.loc[FROZEN_STAGE0_SCENARIO, "net_return_points"],
                    frozen_control_full.loc[
                        FROZEN_STAGE0_SCENARIO, "net_return_points"
                    ],
                ),
                "reference_mdd_ratio_vs_frozen_stage0": _safe_ratio(
                    observed_mdd,
                    frozen_control_full.loc[
                        FROZEN_STAGE0_SCENARIO, "max_daily_drawdown_points"
                    ],
                ),
                "robust_mdd_within_105pct_all_scenarios": bool(
                    all(robust_mdd_checks.values())
                ),
                **robust_mdd_checks,
                **checks,
                "promotion_gate_passed": False,
                "promotion_blocker": "NO_UNTOUCHED_PROSPECTIVE_SAMPLE_AND_EXECUTION_DATA_INVALID",
            }
        )
    return pd.DataFrame(rows).sort_values(
        [
            "development_improvement_passed",
            "development_gate_passed",
            "worst_case_net_ratio_vs_baseline",
            "reference_mdd_ratio_vs_baseline",
        ],
        ascending=[False, False, False, True],
        kind="stable",
    ).reset_index(drop=True)


def _definition_setup_effects(
    definition: ExperimentDefinition,
) -> tuple[set[str], set[str]]:
    """Return (disabled setups, setups modified by a non-disable mechanism)."""

    disabled: set[str] = set()
    modified: set[str] = set()
    if definition.disabled_setup_id is not None:
        disabled.add(definition.disabled_setup_id)
    if definition.runtime_spec.entry_setup_id is not None:
        modified.add(definition.runtime_spec.entry_setup_id)
    if definition.registry_variant_id is not None:
        spec = registry.get_spec(definition.registry_variant_id)
        if spec.disabled_setup_id is not None:
            disabled.add(spec.disabled_setup_id)
        if spec.selection_rule is not None:
            modified.add(spec.selection_rule.setup_id)
        if spec.picker_override is not None:
            modified.add(spec.picker_override.setup_id)
        if spec.cap_override is not None:
            modified.add(spec.cap_override.setup_id)
    return disabled, modified


def _compatible_for_combination(
    first: ExperimentDefinition, second: ExperimentDefinition
) -> bool:
    if first.family == second.family:
        return False
    if first.registry_variant_id and second.registry_variant_id:
        return False
    first_runtime = set(first.runtime_spec.active_mechanisms)
    second_runtime = set(second.runtime_spec.active_mechanisms)
    if first_runtime and second_runtime:
        if first_runtime & second_runtime:
            return False
        if first_runtime | second_runtime != {
            "ENTRY_NOT_BEFORE",
            "PORTFOLIO_SYMBOL_LIMIT",
        }:
            return False
    if first.gap_variant != BASE_GAP_VARIANT and second.gap_variant != BASE_GAP_VARIANT:
        return False
    if first.disabled_setup_id and second.disabled_setup_id:
        return False
    first_disabled, first_modified = _definition_setup_effects(first)
    second_disabled, second_modified = _definition_setup_effects(second)
    if first_disabled & second_modified:
        return False
    if second_disabled & first_modified:
        return False
    return True


def _combined_definition(
    gates_frame: pd.DataFrame,
    definitions: Mapping[str, ExperimentDefinition],
) -> ExperimentDefinition | None:
    eligible = gates_frame.loc[
        gates_frame["development_improvement_passed"]
        & ~gates_frame["is_control"]
        & ~gates_frame["is_development_baseline"]
    ]
    ids = eligible["variant_id"].astype(str).tolist()
    for index, first_id in enumerate(ids):
        for second_id in ids[index + 1 :]:
            first = definitions[first_id]
            second = definitions[second_id]
            if not _compatible_for_combination(first, second):
                continue
            registry_variant = first.registry_variant_id or second.registry_variant_id
            runtime_specs = (first.runtime_spec, second.runtime_spec)
            entry_runtime = next(
                (
                    spec
                    for spec in runtime_specs
                    if "ENTRY_NOT_BEFORE" in spec.active_mechanisms
                ),
                execution_runtime.RuntimeSpec(),
            )
            portfolio_runtime = next(
                (
                    spec
                    for spec in runtime_specs
                    if "PORTFOLIO_SYMBOL_LIMIT" in spec.active_mechanisms
                ),
                execution_runtime.RuntimeSpec(),
            )
            exit_runtime = next(
                (
                    spec
                    for spec in runtime_specs
                    if "EXIT_RULE" in spec.active_mechanisms
                ),
                execution_runtime.RuntimeSpec(),
            )
            runtime_spec = execution_runtime.RuntimeSpec(
                entry_setup_id=entry_runtime.entry_setup_id,
                entry_not_before_minute=entry_runtime.entry_not_before_minute,
                exit_rule=exit_runtime.exit_rule,
                exit_activation_r=exit_runtime.exit_activation_r,
                same_side_symbol_limit=portfolio_runtime.same_side_symbol_limit,
            )
            runtime_spec.validate(allow_composite=True)
            gap_variant = (
                first.gap_variant
                if first.gap_variant != BASE_GAP_VARIANT
                else second.gap_variant
            )
            disabled = first.disabled_setup_id or second.disabled_setup_id
            short_hash = hashlib.sha256(
                f"{first_id}|{second_id}".encode()
            ).hexdigest()[:10].upper()
            return ExperimentDefinition(
                variant_id=f"V11_S10_POST_HOC_TOP2_{short_hash}",
                stage_id="STAGE_10_POST_HOC_COMBINATION",
                family="POST_HOC_COMBINATION",
                description=f"Post-hoc combination of {first_id} and {second_id}",
                registry_variant_id=registry_variant,
                runtime_spec=runtime_spec,
                gap_variant=gap_variant,
                disabled_setup_id=disabled,
                component_variant_ids=(first_id, second_id),
                post_hoc=True,
            )
    return None


def _stage_status_frame(
    executed: Sequence[ExperimentDefinition],
    *,
    stage10_status: Mapping[str, str],
    repair_summary: Mapping[str, Any],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = [dict(record) for record in BLOCKED_TESTS]
    counts = pd.Series([item.stage_id for item in executed]).value_counts()
    for stage_id, count in counts.items():
        if stage_id == CONTROL.stage_id:
            reason = (
                f"{int(count)} frozen-control variant completed in pinned "
                f"{FROZEN_STAGE0_SCENARIO} only; the frozen V10 run has no "
                "stress-scenario audits."
            )
        else:
            reason = (
                f"{int(count)} variant(s) completed under all three V11 cost "
                "scenarios."
            )
        rows.append(
            {
                "stage_id": stage_id,
                "test_id": "ALL_PREDECLARED_EXECUTABLE_VARIANTS",
                "status": "EXECUTED_RESEARCH_ONLY",
                "reason": reason,
            }
        )
    rows.append(
        {
            "stage_id": "STAGE_00_FROZEN_V10",
            "test_id": "V10_STAGE0_PINNED_REFERENCE_ECONOMIC_PARITY",
            "status": "VERIFIED_BY_STAGE0",
            "reason": "The REFERENCE_15_0 Stage0 replay is hash-, benchmark-, and legacy economic-contract-parity checked against the frozen reference-only V10 run; no frozen V10 stress audit exists.",
        }
    )
    reference_repair = dict(repair_summary["scenarios"])["REFERENCE_15_0"]
    rows.append(
        {
            "stage_id": "STAGE_03_REBASELINE",
            "test_id": "STRONG_IDENTITY_GAP_REBASELINE",
            "status": "EXECUTED_DETERMINISTIC_REPAIR",
            "reason": (
                "Stage3 uses the V11 strong-reference identity guard; "
                f"{int(reference_repair['changed_candidate_rows'])} reference "
                "candidate row(s) differ from frozen legacy Stage0 and are "
                "listed in stage0_to_stage3_candidate_delta.csv."
            ),
        }
    )
    rows.append(dict(stage10_status))
    return pd.DataFrame(rows).sort_values(
        ["stage_id", "test_id"], kind="stable"
    ).reset_index(drop=True)


def _fmt(value: Any, decimals: int = 4) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if not math.isfinite(number):
        return "n/a"
    return f"{number:.{decimals}f}"


def _report(
    metrics: pd.DataFrame,
    gates_frame: pd.DataFrame,
    stage_status: pd.DataFrame,
    robustness: pd.DataFrame,
    *,
    sessions: Sequence[date],
    missing_sessions: Sequence[date],
    repair_summary: Mapping[str, Any],
    stage10_definition: ExperimentDefinition | None,
    stage0_parity: Mapping[str, Any],
) -> str:
    reference = metrics.loc[
        metrics["period"].eq("FULL_USABLE")
        & metrics["scenario"].eq("REFERENCE_15_0")
    ].copy()
    harsh = metrics.loc[
        metrics["period"].eq("FULL_USABLE")
        & metrics["scenario"].eq("STRESS_25_5")
    ][["variant_id", "profit_factor", "net_return_points"]].rename(
        columns={
            "profit_factor": "harsh_pf",
            "net_return_points": "harsh_net_points",
        }
    )
    forward = metrics.loc[
        metrics["period"].eq("FORWARD_EXTENSION")
        & metrics["scenario"].eq("STRESS_25_5")
    ][["variant_id", "net_return_points"]].rename(
        columns={"net_return_points": "forward_harsh_net"}
    )
    table = (
        reference.merge(harsh, on="variant_id", how="left")
        .merge(forward, on="variant_id", how="left")
        .merge(
            gates_frame[
                [
                    "variant_id",
                    "development_gate_passed",
                    "development_improvement_passed",
                    "gate_classification",
                    "worst_case_net_ratio_vs_baseline",
                    "is_control",
                    "is_development_baseline",
                    "robust_mdd_within_105pct_all_scenarios",
                ]
            ],
            on="variant_id",
            how="left",
        )
        .sort_values(
            [
                "development_improvement_passed",
                "development_gate_passed",
                "worst_case_net_ratio_vs_baseline",
            ],
            ascending=[False, False, False],
            kind="stable",
        )
    )
    isolated_eligible = gates_frame.loc[
        gates_frame["development_improvement_passed"]
        & ~gates_frame["is_control"]
        & ~gates_frame["is_development_baseline"]
        & gates_frame["stage_id"].ne("STAGE_10_POST_HOC_COMBINATION")
    ]
    best_isolated_id = (
        str(isolated_eligible.iloc[0]["variant_id"])
        if not isolated_eligible.empty
        else None
    )
    observed_challengers = gates_frame.loc[
        ~gates_frame["is_control"]
        & ~gates_frame["is_development_baseline"]
        & gates_frame["stage_id"].ne("STAGE_10_POST_HOC_COMBINATION")
    ]
    best_observed = (
        str(observed_challengers.iloc[0]["variant_id"])
        if not observed_challengers.empty
        else "none"
    )
    post_hoc_eligible = gates_frame.loc[
        gates_frame["development_improvement_passed"]
        & gates_frame["stage_id"].eq("STAGE_10_POST_HOC_COMBINATION")
    ]
    best_post_hoc_id = (
        str(post_hoc_eligible.iloc[0]["variant_id"])
        if not post_hoc_eligible.empty
        else "none"
    )
    reference_parity_level = str(
        dict(stage0_parity.get(FROZEN_STAGE0_SCENARIO, {})).get(
            "parity_level", "UNKNOWN"
        )
    )
    lines = [
        "# FNO V11 staged full-history research comparison",
        "",
        f"Usable sessions: **{len(sessions)}** ({min(sessions)} through {max(sessions)}).",
        f"Missing regular sessions inside the span: **{', '.join(map(str, missing_sessions)) or 'none'}**.",
        "",
        f"Frozen archival control: `{CONTROL.variant_id}`. Deterministic V11 comparison baseline: `{DEVELOPMENT_BASELINE.variant_id}`.",
        "Stage0 alone retains the legacy V10 gap-identity behavior for pinned legacy economic parity; Stage3 and every V11 challenger use the strong-reference identity repair.",
        (
            "Frozen Stage0 scope: REFERENCE_15_0 only "
            f"({reference_parity_level}). The pinned V10 run was created with "
            "`--reference-only`, so Stage0 stress results are intentionally not "
            "invented; Stage3 and every V11 challenger run all three cost scenarios."
        ),
        "",
        (
            f"**Best isolated predeclared challenger: `{best_isolated_id}`.**"
            if best_isolated_id is not None
            else "**No isolated challenger passed; retain deterministic Stage3 as the comparison baseline.**"
        ),
        f"Best post-hoc combination: `{best_post_hoc_id}`.",
        f"Best observed isolated challenger by worst cost-scenario net ratio: `{best_observed}`.",
        "",
        "No result in this report is promotion-eligible: the execution remains cash-bar based, most of the historical universe is not point-in-time, 26-Aug is absent from the comparable cache, and an untouched prospective sample does not exist.",
        "",
        "## Full-period comparison",
        "",
        "| Stage | Variant | Fills | WR | PF | Net pts | Net P&L Rs | MDD | Harsh PF | Harsh net | Forward harsh net | Gate | All-cost MDD |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in table.to_dict("records"):
        robust_mdd_value = row["robust_mdd_within_105pct_all_scenarios"]
        robust_mdd_label = (
            "n/a"
            if pd.isna(robust_mdd_value)
            else ("PASS" if bool(robust_mdd_value) else "FAIL")
        )
        lines.append(
            "| {stage} | `{variant}` | {fills} | {wr}% | {pf} | {net} | {pnl} | {mdd} | {hpf} | {hnet} | {fwd} | {gate} | {robust_mdd} |".format(
                stage=row["stage_id"],
                variant=row["variant_id"],
                fills=int(row["fills"]),
                wr=_fmt(row["win_rate_pct"], 2),
                pf=_fmt(row["profit_factor"]),
                net=_fmt(row["net_return_points"]),
                pnl=_fmt(row["net_pnl_rs"], 2),
                mdd=_fmt(row["max_daily_drawdown_points"]),
                hpf=_fmt(row["harsh_pf"]),
                hnet=_fmt(row["harsh_net_points"]),
                fwd=_fmt(row["forward_harsh_net"]),
                gate=row["gate_classification"],
                robust_mdd=robust_mdd_label,
            )
        )
    lines.extend(
        [
            "",
            "Stage0 harsh-cost and all-cost-MDD cells are intentionally `n/a`: the frozen V10 evidence contains only REFERENCE_15_0. All-cost eligibility begins at deterministic Stage3.",
        ]
    )
    if stage10_definition is not None:
        lines.extend(
            [
                "",
                "## Post-hoc combination components",
                "",
                f"`{stage10_definition.variant_id}` combines: "
                + ", ".join(
                    f"`{variant_id}`"
                    for variant_id in stage10_definition.component_variant_ids
                )
                + ".",
                "It is reported separately from the best isolated challenger and remains ineligible for promotion without new data.",
            ]
        )
    finalist_ids = [CONTROL.variant_id, DEVELOPMENT_BASELINE.variant_id]
    if best_isolated_id is not None:
        finalist_ids.append(best_isolated_id)
    if best_post_hoc_id != "none":
        finalist_ids.append(best_post_hoc_id)
    finalist_ids = list(dict.fromkeys(finalist_ids))
    lines.extend(
        [
            "",
            "## Finalist core and forward checks",
            "",
            "| Variant | Period | Scenario | Fills | PF | Net pts | Net P&L Rs | MDD |",
            "|---|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for variant_id in finalist_ids:
        for period in ("CORE_59", "FORWARD_EXTENSION"):
            for scenario, _, _ in gaps.COST_SCENARIOS:
                selected = metrics.loc[
                    metrics["variant_id"].eq(variant_id)
                    & metrics["period"].eq(period)
                    & metrics["scenario"].eq(scenario)
                ]
                if selected.empty:
                    continue
                row = selected.iloc[0]
                lines.append(
                    f"| `{variant_id}` | {period} | {scenario} | "
                    f"{int(row['fills'])} | {_fmt(row['profit_factor'])} | "
                    f"{_fmt(row['net_return_points'])} | "
                    f"{_fmt(row['net_pnl_rs'], 2)} | "
                    f"{_fmt(row['max_daily_drawdown_points'])} |"
                )
    lines.extend(
        [
            "",
            "## Finalist bootstrap and concentration diagnostics",
            "",
            "| Variant | Scenario | PF p2.5 | Net pts p2.5 | Mean daily P&L p2.5 Rs | Best month share | Best 10 positive days share |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for variant_id in finalist_ids:
        for scenario in ("REFERENCE_15_0", "STRESS_25_5"):
            selected = robustness.loc[
                robustness["variant_id"].eq(variant_id)
                & robustness["scenario"].eq(scenario)
            ]
            if selected.empty:
                continue
            row = selected.iloc[0]
            lines.append(
                f"| `{variant_id}` | {scenario} | {_fmt(row['pf_p025'])} | "
                f"{_fmt(row['net_points_p025'])} | "
                f"{_fmt(row['mean_daily_pnl_rs_p025'], 2)} | "
                f"{_fmt(row['best_month_share_pct'], 2)}% | "
                f"{_fmt(row['best_10_positive_days_share_pct'], 2)}% |"
            )
    reference_repair = dict(repair_summary["scenarios"])["REFERENCE_15_0"]
    lines.extend(
        [
            "",
            "## Frozen Stage0 to deterministic Stage3 repair",
            "",
            f"Reference candidate rows changed: **{int(reference_repair['changed_candidate_rows'])}** of **{int(reference_repair['candidate_rows'])}**.",
            f"Reference fill delta: **{int(reference_repair['fill_delta']):+d}**; net-points delta: **{float(reference_repair['net_return_points_delta']):+.4f}**; modeled P&L delta: **Rs {float(reference_repair['net_pnl_rs_delta']):+.2f}**.",
            "Every change across the declared repair-comparison fields is stored in `stage0_to_stage3_candidate_delta.csv`; this repair is kept separate from strategy improvements.",
        ]
    )
    lines.extend(
        [
            "",
            "## Stage execution and validity gates",
            "",
            "| Stage | Test | Status | Reason |",
            "|---|---|---|---|",
        ]
    )
    for row in stage_status.to_dict("records"):
        lines.append(
            f"| {row['stage_id']} | {row['test_id']} | {row['status']} | {row['reason']} |"
        )
    baseline_bootstrap = robustness.loc[
        robustness["variant_id"].eq(DEVELOPMENT_BASELINE.variant_id)
        & robustness["scenario"].eq("REFERENCE_15_0")
    ]
    if not baseline_bootstrap.empty:
        row = baseline_bootstrap.iloc[0]
        lines.extend(
            [
                "",
                "## Statistical context",
                "",
                f"Stage3 session-bootstrap PF 2.5th percentile: **{_fmt(row['pf_p025'])}**; mean daily P&L 2.5th percentile: **Rs {_fmt(row['mean_daily_pnl_rs_p025'], 2)}**.",
                f"Best month share of Stage3 net P&L: **{_fmt(row['best_month_share_pct'], 2)}%**; best ten positive days share: **{_fmt(row['best_10_positive_days_share_pct'], 2)}%**.",
                "",
                "Bootstrap intervals are diagnostics on the same development sample, not an out-of-sample guarantee.",
            ]
        )
    lines.extend(
        [
            "",
            "## Decision rule",
            "",
            "A challenger passes only if full-period net points and PF are at least deterministic Stage3 in all three cost scenarios, reference drawdown is no worse than 105% of Stage3, and every forward-extension cost case remains non-negative. Improving win rate alone is never sufficient.",
            "The original reference-MDD gate is retained. MDD within 105% of Stage3 in all three cost scenarios is reported separately as a robustness diagnostic and was not added post-hoc to the pass rule.",
            "Exact economic ties are labelled PARITY_NO_EFFECT and cannot enter Stage10.",
            "",
            "Stage10 is post-hoc and is run only when at least two compatible isolated challengers pass. Even a passing Stage10 combination remains development-only.",
        ]
    )
    return "\n".join(lines) + "\n"


def _validate_catalog() -> dict[str, Any]:
    registry.validate_registry()
    identifiers = [definition.variant_id for definition in ALL_PREDECLARED_EXPERIMENTS]
    if len(identifiers) != len(set(identifiers)):
        raise AssertionError("FNO V11 experiment IDs must be unique")
    for definition in ALL_PREDECLARED_EXPERIMENTS:
        definition.runtime_spec.validate()
        _gap_spec(definition.gap_variant)
        if definition.registry_variant_id is not None:
            registry.get_spec(definition.registry_variant_id)
        if definition.disabled_setup_id is not None and (
            definition.disabled_setup_id not in registry.VALID_SETUP_IDS
        ):
            raise AssertionError(f"unknown disabled setup: {definition.disabled_setup_id}")
    return _validate_frozen_v10_reference_binding()


def run_all(argv: Sequence[str]) -> Path:
    parser = argparse.ArgumentParser(
        prog="fno_v11_staged_backtest.py run-all",
        description="Run frozen Stage0 plus every executable isolated FNO V11 stage.",
    )
    parser.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    parser.add_argument(
        "--reverse-scenarios",
        action="store_true",
        help="Run cost scenarios in reverse order for determinism attestation.",
    )
    parser.add_argument(
        "--attest-against",
        type=Path,
        help="Fail unless this run matches a prior complete V2 run.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=3,
        help=(
            "Concurrent fresh-process slots for isolated challengers "
            "(default: 3; processes are never reused across variants)."
        ),
    )
    args = parser.parse_args(list(argv))
    if args.workers < 1 or args.workers > 4:
        raise ValueError("--workers must be in [1, 4]")

    frozen_v10_reference_binding = _validate_catalog()
    (
        all_candidates,
        minute_paths,
        segment_records,
        sessions,
        expected_span,
        missing_sessions,
    ) = v10_backtest._load_all_usable_max050_gap2_history()
    selection_spec = filters.SPEC_BY_NAME[
        v10_backtest.MAX050_GAP2_SELECTION_VARIANT
    ]
    baseline_selected, baseline_decisions = filters.selection_overlay(
        all_candidates, selection_spec
    )
    if len(baseline_selected) != int(
        v10_backtest.MAX050_GAP2_CURRENT_MIXED_BENCHMARK["candidates"]
    ):
        raise AssertionError("V11 Stage0 selected-candidate count drifted")
    parent_input_binding_sha256 = _input_binding_sha256(
        baseline_selected,
        minute_paths,
        sessions,
        segment_records,
    )

    output_root = args.output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(gaps.IST).strftime("%Y%m%dT%H%M%S%f%z")
    run_dir = output_root / f"run_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=False)
    source_dir = run_dir / "source"
    source_dir.mkdir()
    module_dir = Path(__file__).resolve().parent
    sources = (
        Path(__file__).resolve(),
        Path(registry.__file__).resolve(),
        Path(selection_runtime.__file__).resolve(),
        Path(execution_runtime.__file__).resolve(),
        Path(gap_runtime.__file__).resolve(),
        Path(v10_backtest.__file__).resolve(),
        Path(locked_config.__file__).resolve(),
        Path(filters.__file__).resolve(),
        Path(gaps.__file__).resolve(),
        Path(experiment.__file__).resolve(),
        Path(engine.__file__).resolve(),
        module_dir / "fno_v10_experiment_config.py",
        module_dir / "fno_v10_unified_5m_1m_backtest.py",
        module_dir / "fno_oi_backtest_provenance.py",
        module_dir / "fno_oi_common.py",
        module_dir / "fno_oi_hybrid_data.py",
        module_dir / "eqidv2_runtime_paths.py",
    )
    source_hashes: dict[str, dict[str, str]] = {}
    for source in sources:
        live_sha256 = _sha256_file(source)
        snapshot_path = source_dir / source.name
        shutil.copy2(source, snapshot_path)
        snapshot_sha256 = _sha256_file(snapshot_path)
        if snapshot_sha256 != live_sha256:
            raise AssertionError(f"source snapshot hash drifted while copying {source.name}")
        source_hashes[source.name] = {
            "path": str(source),
            "snapshot_path": str(snapshot_path.resolve()),
            "sha256": live_sha256,
        }
    common.atomic_write_csv(all_candidates, run_dir / "all_input_candidates.csv")
    common.atomic_write_csv(
        baseline_selected, run_dir / "v10_stage0_selected_candidates.csv"
    )
    common.atomic_write_csv(
        baseline_decisions, run_dir / "v10_stage0_selection_decisions.csv"
    )
    common.atomic_write_json(
        run_dir / "source_segments.json",
        {"schema_version": SCHEMA_VERSION, "segments": segment_records},
    )

    scenarios = tuple(gaps.COST_SCENARIOS)
    if args.reverse_scenarios:
        scenarios = tuple(reversed(scenarios))
    control_scenarios = tuple(
        scenario_spec
        for scenario_spec in scenarios
        if scenario_spec[0] == FROZEN_STAGE0_SCENARIO
    )
    if len(control_scenarios) != 1:
        raise AssertionError("the pinned Stage0 reference cost scenario is missing")
    all_metrics: list[dict[str, Any]] = []
    all_daily: list[pd.DataFrame] = []
    all_robustness: list[dict[str, Any]] = []
    run_records: dict[str, Any] = {}
    executed: list[ExperimentDefinition] = []
    definition_by_id = {
        definition.variant_id: definition for definition in ALL_PREDECLARED_EXPERIMENTS
    }

    # Stage0 always runs first and must pass its pinned benchmark before any
    # challenger consumes compute or writes a result claiming comparability.
    for definition in (CONTROL,):
        rows, daily, robustness_rows, record = _run_experiment(
            definition,
            baseline_selected,
            minute_paths,
            sessions,
            segment_records,
            run_dir,
            scenarios=control_scenarios,
        )
        all_metrics.extend(rows)
        all_daily.extend(daily)
        all_robustness.extend(robustness_rows)
        record["parent_input_binding_sha256"] = parent_input_binding_sha256
        run_records[definition.variant_id] = record
        executed.append(definition)

    tasks = [
        (
            definition,
            str(run_dir),
            tuple(scenarios),
            parent_input_binding_sha256,
        )
        for definition in (DEVELOPMENT_BASELINE,) + INDIVIDUAL_EXPERIMENTS
    ]
    with concurrent.futures.ProcessPoolExecutor(
        **_worker_pool_kwargs(args.workers)
    ) as pool:
        challenger_results = list(pool.map(_worker_run_experiment, tasks))
    for variant_id, rows, daily, robustness_rows, record in challenger_results:
        definition = definition_by_id[variant_id]
        all_metrics.extend(rows)
        all_daily.extend(daily)
        all_robustness.extend(robustness_rows)
        run_records[variant_id] = record
        executed.append(definition)

    metrics = pd.DataFrame(all_metrics)
    economic_fingerprints = _closed_trade_economic_fingerprints(
        run_records, scenarios
    )
    gates_frame = _development_gates(metrics, economic_fingerprints)
    stage10 = _combined_definition(gates_frame, definition_by_id)
    if stage10 is None:
        stage10_status = {
            "stage_id": "STAGE_10_POST_HOC_COMBINATION",
            "test_id": "TOP_TWO_COMPATIBLE_PASSERS",
            "status": "NOT_RUN_GATE_FAILED",
            "reason": "Fewer than two compatible, materially changed isolated challengers passed every predeclared development gate.",
        }
    else:
        stage10_task = (
            stage10,
            str(run_dir),
            tuple(scenarios),
            parent_input_binding_sha256,
        )
        with concurrent.futures.ProcessPoolExecutor(
            **_worker_pool_kwargs(1)
        ) as pool:
            stage10_results = list(
                pool.map(_worker_run_experiment, (stage10_task,))
            )
        if len(stage10_results) != 1:
            raise AssertionError("Stage10 fresh-process replay did not return once")
        stage10_variant_id, rows, daily, robustness_rows, record = (
            stage10_results[0]
        )
        if stage10_variant_id != stage10.variant_id:
            raise AssertionError("Stage10 fresh-process result identity drifted")
        all_metrics.extend(rows)
        all_daily.extend(daily)
        all_robustness.extend(robustness_rows)
        run_records[stage10.variant_id] = record
        executed.append(stage10)
        metrics = pd.DataFrame(all_metrics)
        economic_fingerprints = _closed_trade_economic_fingerprints(
            run_records, scenarios
        )
        gates_frame = _development_gates(metrics, economic_fingerprints)
        stage10_status = {
            "stage_id": "STAGE_10_POST_HOC_COMBINATION",
            "test_id": stage10.variant_id,
            "status": "EXECUTED_POST_HOC_RESEARCH_ONLY",
            "reason": "Two compatible isolated improvement-passers were combined; this mined result cannot be promoted without new data.",
        }

    repair_delta_path, repair_summary_path, repair_summary = (
        _stage0_stage3_repair_delta(
            run_records,
            metrics,
            run_dir,
            control_scenarios,
        )
    )
    daywise = pd.concat(all_daily, ignore_index=True)
    robustness = pd.DataFrame(all_robustness)
    stage_status = _stage_status_frame(
        executed,
        stage10_status=stage10_status,
        repair_summary=repair_summary,
    )
    metrics_path = run_dir / "all_period_metrics.csv"
    daily_path = run_dir / "all_daywise.csv"
    robustness_path = run_dir / "bootstrap_and_concentration.csv"
    gates_path = run_dir / "development_gates.csv"
    status_path = run_dir / "stage_status.csv"
    report_path = run_dir / "FNO_V11_ALL_STAGES_COMPARISON.md"
    common.atomic_write_csv(metrics, metrics_path)
    common.atomic_write_csv(daywise, daily_path)
    common.atomic_write_csv(robustness, robustness_path)
    common.atomic_write_csv(gates_frame, gates_path)
    common.atomic_write_csv(stage_status, status_path)
    common.atomic_write_text(
        report_path,
        _report(
            metrics,
            gates_frame,
            stage_status,
            robustness,
            sessions=sessions,
            missing_sessions=missing_sessions,
            repair_summary=repair_summary,
            stage10_definition=stage10,
            stage0_parity=run_records[CONTROL.variant_id]["control_parity"],
        ),
    )

    determinism_attestation_path: Path | None = None
    determinism_attestation: dict[str, Any] | None = None
    if args.attest_against is not None:
        determinism_attestation = _determinism_attestation(
            args.attest_against,
            run_dir,
            [scenario for scenario, _, _ in scenarios],
            parent_input_binding_sha256,
        )
        determinism_attestation_path = run_dir / "determinism_attestation.json"
        common.atomic_write_json(
            determinism_attestation_path,
            gaps._json_ready(determinism_attestation),
        )
        report_text = report_path.read_text(encoding="utf-8")
        report_text += (
            "\n## Determinism attestation\n\n"
            f"PASS against `{determinism_attestation['prior_run']}`: "
            f"{int(determinism_attestation['stage_artifact_count'])} stage "
            "artifacts from Stage3 onward have byte-identical hashes, "
            "aggregate economic frames match by key, frozen Stage0 satisfies "
            "its pinned REFERENCE_15_0 economic contract, and reverse V11 "
            "scenario order is "
            "invariant.\n"
        )
        common.atomic_write_text(report_path, report_text)

    provenance_path = run_dir / "provenance.json"
    inventory_path = run_dir / "artifact_inventory.json"
    for source in sources:
        expected_source_hash = source_hashes[source.name]["sha256"]
        if _sha256_file(source) != expected_source_hash:
            raise AssertionError(
                f"live source changed during V11 run: {source.name}"
            )
        snapshot_path = Path(source_hashes[source.name]["snapshot_path"])
        if _sha256_file(snapshot_path) != expected_source_hash:
            raise AssertionError(
                f"archived source changed during V11 run: {source.name}"
            )
    ending_frozen_binding = _validate_frozen_v10_reference_binding()
    if ending_frozen_binding != frozen_v10_reference_binding:
        raise AssertionError("frozen V10 reference binding changed during V11 run")
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "created_at_ist": datetime.now(gaps.IST),
        "command": [
            "python",
            "-u",
            str(Path(__file__).resolve()),
            "run-all",
            *argv,
        ],
        "registry_sha256": registry.registry_sha256(),
        "worker_isolation_policy": WORKER_ISOLATION_POLICY,
        "worker_concurrency": args.workers,
        "input_binding_sha256": parent_input_binding_sha256,
        "frozen_control_variant_id": CONTROL.variant_id,
        "comparison_baseline_variant_id": DEVELOPMENT_BASELINE.variant_id,
        "v11_gap_identity_policy": gap_runtime.IDENTITY_POLICY,
        "closed_trade_economic_fingerprints": economic_fingerprints,
        "scenario_order": [scenario for scenario, _, _ in scenarios],
        "v11_scenario_order": [scenario for scenario, _, _ in scenarios],
        "stage0_scenario_order": [
            scenario for scenario, _, _ in control_scenarios
        ],
        "frozen_stage0_cost_scope": "REFERENCE_ONLY_NO_FROZEN_STRESS_AUDITS",
        "determinism_attested": determinism_attestation is not None,
        "determinism_attestation": determinism_attestation,
        "v10_stage0_binding": asdict(registry.BASELINE_BINDING),
        "frozen_v10_reference_binding": frozen_v10_reference_binding,
        "source_hashes": source_hashes,
        "usable_session_dates": [day.isoformat() for day in sessions],
        "usable_session_count": len(sessions),
        "calendar_span_session_count": len(expected_span),
        "missing_regular_session_dates": [day.isoformat() for day in missing_sessions],
        "source_segments": segment_records,
        "experiments": run_records,
        "blocked_tests": list(BLOCKED_TESTS),
        "outputs": {
            "metrics": str(metrics_path.resolve()),
            "daywise": str(daily_path.resolve()),
            "robustness": str(robustness_path.resolve()),
            "development_gates": str(gates_path.resolve()),
            "stage_status": str(status_path.resolve()),
            "report": str(report_path.resolve()),
            "stage0_to_stage3_candidate_delta": str(repair_delta_path.resolve()),
            "stage0_to_stage3_repair_summary": str(
                repair_summary_path.resolve()
            ),
            **(
                {
                    "determinism_attestation": str(
                        determinism_attestation_path.resolve()
                    )
                }
                if determinism_attestation_path is not None
                else {}
            ),
        },
        "limitations": [
            "RESEARCH_ONLY_POST_HOC_DEVELOPMENT_SAMPLE",
            "POINT_IN_TIME_UNIVERSE_INCOMPLETE",
            "STATIC_26AUG_CONTRACT_USED_BACKWARD_IN_CORE",
            "CASH_EQUITY_1M_EXECUTION_NOT_FUTURES_EXECUTION",
            "LAST_REAL_BAR_SENSITIVITY_FOR_246_PATHS_ENDING_1515",
            "2026_08_26_NOT_IN_COMPARABLE_65_SESSION_CACHE",
            "NO_HISTORICAL_MARGIN_SPREAD_DEPTH_OR_PARTIAL_FILL_DATA",
            "NO_UNTOUCHED_PROSPECTIVE_VALIDATION_SAMPLE",
            "FROZEN_STAGE0_RETAINS_LEGACY_OBJECT_ID_GAP_BEHAVIOR",
            "FROZEN_STAGE0_PIN_HAS_REFERENCE_ONLY_NO_STRESS_AUDITS",
        ],
        "headline_valid": False,
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
    }
    common.atomic_write_json(
        inventory_path,
        {
            "schema_version": SCHEMA_VERSION,
            "artifacts": gaps._inventory_files(
                run_dir, exclude={provenance_path, inventory_path}
            ),
        },
    )
    provenance["artifact_inventory"] = {
        "path": str(inventory_path.resolve()),
        "sha256": _sha256_file(inventory_path),
    }
    provenance["artifact_inventory_validation"] = (
        _validate_completed_run_artifact_inventory(run_dir, provenance)
    )
    # This is the only publication of complete=True.  A crash before this
    # atomic write leaves no provenance that can be mistaken for a valid run.
    common.atomic_write_json(provenance_path, gaps._json_ready(provenance))
    common.atomic_write_json(
        output_root / "latest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_dir": str(run_dir.resolve()),
            "provenance_sha256": _sha256_file(provenance_path),
            "usable_session_count": len(sessions),
            "research_only": True,
            "determinism_attested": determinism_attestation is not None,
        },
    )
    print(f"[FNO-V11] complete: {run_dir}", flush=True)
    return run_dir


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        raise SystemExit("command required: validate or run-all")
    command = args[0]
    if command == "validate":
        _validate_catalog()
        print(
            json.dumps(
                {
                    "schema_version": SCHEMA_VERSION,
                    "registry_sha256": registry.registry_sha256(),
                    "predeclared_experiments": len(ALL_PREDECLARED_EXPERIMENTS),
                    "blocked_tests": len(BLOCKED_TESTS),
                    "worker_isolation_policy": WORKER_ISOLATION_POLICY,
                    "valid": True,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if command == "run-all":
        run_all(args[1:])
        return 0
    raise SystemExit(f"unknown command: {command!r}")


if __name__ == "__main__":
    raise SystemExit(main())
