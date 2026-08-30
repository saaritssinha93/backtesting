"""Run isolated V6 selection challengers on the frozen V8 causal engine.

This is a research-only adapter.  It does not mutate the V6/V8 setup book,
the frozen 59-session cache, a production configuration, or a live runner.
Every challenger filters the baseline V6 candidate superset, recomputes rank,
and then invokes the complete confirmation/entry/exit/portfolio state machine.

Predeclared experiments:

* A1: 09:40 LONG minimum directional move 0.20 -> 0.40 percent;
* A2: 09:35 LONG maximum directional move 0.40/0.50/0.60 percent;
* A1+A2: each A2 ceiling layered on A1.

The standalone A2 runs are intentional: they separate the A2 main effect
from its interaction with A1.  This avoids attributing an interaction result
to the 09:35 filter itself.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

TOOLS_ROOT = Path(__file__).resolve().parent
WORKSPACE_ROOT = TOOLS_ROOT.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

import fno_oi_common as common
import fno_v8_strict_v6_logic_backtest as strict
import fno_v8_windowed_1m_entry_backtest as engine


SCHEMA_VERSION = "fno_v6_isolated_challenger_replay_v1"
SELECTION_SCHEMA_VERSION = "fno_v6_challenger_selection_decision_v1"
VALIDATION_SCHEMA_VERSION = "fno_v6_challenger_validation_v1"
RUN_CONTRACT_SCHEMA_VERSION = "fno_v6_challenger_run_contract_v1"
REPLAY_COMPLETION_SCHEMA_VERSION = "fno_v6_challenger_replay_completion_v1"
METRIC_DRIFT_SCHEMA_VERSION = "fno_v6_reference_metric_drift_v1"
COST_METRIC_DRIFT_SCHEMA_VERSION = "fno_v6_reference_cost_metric_drift_v1"
DAYWISE_DRIFT_SCHEMA_VERSION = "fno_v6_reference_daywise_drift_v1"
CANDIDATE_STATE_DRIFT_SCHEMA_VERSION = "fno_v6_reference_candidate_state_drift_v1"
COST_CANDIDATE_STATE_DRIFT_SCHEMA_VERSION = (
    "fno_v6_reference_cost_candidate_state_drift_v1"
)
SEED_CACHE_SCHEMA_VERSION = "fno_v6_cache_recovery_seed_v1"
SEED_CACHE_REASON = "RECOVERED_FROM_INCOMPLETE_RESEARCH_PACKAGE_WITHOUT_ROOT_MANIFEST"
STRICT_V6_CACHE_SCHEMA_VERSION = "fno_v8_strict_cache_manifest_v1"
HISTORICAL_FROM_DAY = "2026-05-27"
HISTORICAL_THROUGH_DAY = "2026-08-19"
SPLIT_DAY = "2026-08-06"
TODAY = "2026-08-27"
COST_BPS = 15.0
SLIPPAGE_BPS = 0.0
SQUARE_OFF = "15:30"
EOD_POLICY = "LAST_REAL_BAR_SENSITIVITY"


@dataclass(frozen=True)
class ChallengerSpec:
    variant: str
    description: str
    min_0940_long_pct: float | None = None
    max_0935_long_pct: float | None = None

    def payload(self) -> dict[str, Any]:
        return asdict(self)


CHALLENGERS: tuple[ChallengerSpec, ...] = (
    ChallengerSpec("CONTROL", "Frozen V6 strict selection control"),
    ChallengerSpec(
        "A1_0940_LONG_MIN_040",
        "Only 09:40 LONG minimum directional move becomes 0.40%",
        min_0940_long_pct=0.40,
    ),
    ChallengerSpec(
        "A2_0935_LONG_MAX_040",
        "Only 09:35 LONG receives a 0.40% directional-move ceiling",
        max_0935_long_pct=0.40,
    ),
    ChallengerSpec(
        "A2_0935_LONG_MAX_050",
        "Only 09:35 LONG receives a 0.50% directional-move ceiling",
        max_0935_long_pct=0.50,
    ),
    ChallengerSpec(
        "A2_0935_LONG_MAX_060",
        "Only 09:35 LONG receives a 0.60% directional-move ceiling",
        max_0935_long_pct=0.60,
    ),
    ChallengerSpec(
        "A1_A2_0935_LONG_MAX_040",
        "A1 plus a 0.40% ceiling on 09:35 LONG",
        min_0940_long_pct=0.40,
        max_0935_long_pct=0.40,
    ),
    ChallengerSpec(
        "A1_A2_0935_LONG_MAX_050",
        "A1 plus a 0.50% ceiling on 09:35 LONG",
        min_0940_long_pct=0.40,
        max_0935_long_pct=0.50,
    ),
    ChallengerSpec(
        "A1_A2_0935_LONG_MAX_060",
        "A1 plus a 0.60% ceiling on 09:35 LONG",
        min_0940_long_pct=0.40,
        max_0935_long_pct=0.60,
    ),
)
CHALLENGER_BY_NAME = {item.variant: item for item in CHALLENGERS}


@dataclass(frozen=True)
class CostScenario:
    scenario: str
    cost_bps: float
    slippage_bps: float


COST_SCENARIOS: tuple[CostScenario, ...] = (
    CostScenario("BASE_15BPS_0SLIP", 15.0, 0.0),
    CostScenario("STRESS_20BPS_2SLIP", 20.0, 2.0),
    CostScenario("STRESS_25BPS_5SLIP", 25.0, 5.0),
)
STRESS_VARIANT_NAMES: tuple[str, ...] = (
    "CONTROL",
    "A1_0940_LONG_MIN_040",
    "A1_A2_0935_LONG_MAX_040",
    "A1_A2_0935_LONG_MAX_050",
)


def sha256_file(path: Path | str) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def registry_payload() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "variants": [item.payload() for item in CHALLENGERS],
        "research_only": True,
        "promotion_eligible": False,
    }


def validate_registry() -> None:
    if len(CHALLENGER_BY_NAME) != len(CHALLENGERS):
        raise AssertionError("V6 challenger names must be unique")
    if CHALLENGERS[0].variant != "CONTROL":
        raise AssertionError("The first challenger must be CONTROL")
    for item in CHALLENGERS:
        if item.min_0940_long_pct not in {None, 0.40}:
            raise AssertionError(f"Unexpected A1 threshold: {item.variant}")
        if item.max_0935_long_pct not in {None, 0.40, 0.50, 0.60}:
            raise AssertionError(f"Unexpected A2 threshold: {item.variant}")
        if item.variant == "CONTROL" and (
            item.min_0940_long_pct is not None
            or item.max_0935_long_pct is not None
        ):
            raise AssertionError("CONTROL cannot contain an overlay")
    if len({item.scenario for item in COST_SCENARIOS}) != len(COST_SCENARIOS):
        raise AssertionError("Cost-stress scenario names must be unique")
    if set(STRESS_VARIANT_NAMES) - set(CHALLENGER_BY_NAME):
        raise AssertionError("Cost stress references an unknown challenger")


def _required_candidate_columns() -> set[str]:
    return {
        "candidate_id",
        "session_date",
        "signal_time",
        "setup_id",
        "side",
        "symbol",
        "price_change_pct",
        "picker",
        "picker_value",
        "traded_value",
        "frozen_rank",
    }


def apply_selection_overlay(
    candidates: pd.DataFrame,
    spec: ChallengerSpec,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply a challenger before state-machine replay and recompute rank."""

    missing = sorted(_required_candidate_columns() - set(candidates.columns))
    if missing:
        raise ValueError(f"V6 candidate cache is missing columns: {missing}")
    if candidates["candidate_id"].duplicated().any():
        raise AssertionError("V6 baseline candidate IDs must be unique")

    base = candidates.copy()
    reasons = pd.Series("PASSED", index=base.index, dtype=object)
    move = pd.to_numeric(base["price_change_pct"], errors="coerce")
    setup_id = base["setup_id"].astype(str)

    if spec.min_0940_long_pct is not None:
        rejected = (
            reasons.eq("PASSED")
            & setup_id.eq("09:40_LONG")
            & move.add(1e-12).lt(float(spec.min_0940_long_pct))
        )
        reasons.loc[rejected] = "0940_LONG_MOVE_BELOW_MINIMUM"

    if spec.max_0935_long_pct is not None:
        rejected = (
            reasons.eq("PASSED")
            & setup_id.eq("09:35_LONG")
            & move.sub(1e-12).gt(float(spec.max_0935_long_pct))
        )
        reasons.loc[rejected] = "0935_LONG_MOVE_ABOVE_MAXIMUM"

    passed = reasons.eq("PASSED")
    filtered = base.loc[passed].copy()
    filtered = filtered.sort_values(
        [
            "session_date",
            "setup_id",
            "picker_value",
            "traded_value",
            "symbol",
        ],
        ascending=[True, True, False, False, True],
        kind="stable",
    ).reset_index(drop=True)
    filtered["frozen_rank"] = (
        filtered.groupby(["session_date", "setup_id"], sort=False)
        .cumcount()
        .add(1)
    )
    rank_map = filtered.set_index("candidate_id")["frozen_rank"]

    decision_columns = [
        "candidate_id",
        "session_date",
        "signal_time",
        "setup_id",
        "side",
        "symbol",
        "price_change_pct",
        "picker",
        "picker_value",
        "traded_value",
        "frozen_rank",
    ]
    decisions = base[decision_columns].copy().rename(
        columns={"frozen_rank": "original_frozen_rank"}
    )
    decisions["recalculated_frozen_rank"] = decisions["candidate_id"].map(
        rank_map
    )
    decisions["selection_passed"] = passed.to_numpy(dtype=bool)
    decisions["selection_reason"] = reasons.to_numpy(dtype=object)
    decisions["research_variant"] = spec.variant
    decisions["variant_config_sha256"] = canonical_sha256(spec.payload())
    decisions["schema_version"] = SELECTION_SCHEMA_VERSION

    if spec.variant == "CONTROL":
        if len(filtered) != len(base) or not decisions["selection_passed"].all():
            raise AssertionError("CONTROL must preserve every baseline candidate")
        expected_rank = pd.to_numeric(base.set_index("candidate_id")["frozen_rank"])
        observed_rank = pd.to_numeric(filtered.set_index("candidate_id")["frozen_rank"])
        if not expected_rank.sort_index().equals(observed_rank.sort_index()):
            raise AssertionError("CONTROL rank changed")
    return filtered, decisions


def _artifact_record(path: Path | str) -> dict[str, Any]:
    resolved = Path(path).resolve()
    return {
        "path": str(resolved),
        "sha256": sha256_file(resolved),
        "size": int(resolved.stat().st_size),
    }


def _require_artifact(record: Mapping[str, Any], label: str) -> Path:
    path = Path(str(record.get("path", ""))).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Missing {label}: {path}")
    if sha256_file(path) != str(record.get("sha256", "")):
        raise AssertionError(f"{label} SHA-256 changed: {path}")
    if int(path.stat().st_size) != int(record.get("size", -1)):
        raise AssertionError(f"{label} size changed: {path}")
    return path


def _snapshot_identity(
    snapshot_path: Path,
    *,
    expected_master_date: str,
    expected_contract_month_filter: str,
    expected_mapped_stock_futures: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Fail closed before mutating engine globals or touching a cache."""

    resolved_manifest = snapshot_path.resolve()
    snapshot = json.loads(resolved_manifest.read_text(encoding="utf-8"))
    if snapshot.get("schema_version") != "fno_backtest_source_snapshot_v1":
        raise ValueError(f"Unsupported source snapshot schema: {resolved_manifest}")
    if snapshot.get("complete") is not True or snapshot.get("physical_copy") is not True:
        raise ValueError(f"Source snapshot is not a complete physical copy: {resolved_manifest}")
    universe = dict(snapshot.get("universe", {}))
    capture = dict(snapshot.get("universe_capture", {}))
    observed_master_date = str(universe.get("master_date", ""))
    observed_contract = str(universe.get("contract_month_filter", "")).upper()
    observed_mapped = int(universe.get("mapped_stock_futures", -1))
    if observed_master_date != expected_master_date:
        raise ValueError(
            f"Snapshot master date {observed_master_date!r} is not "
            f"{expected_master_date!r}: {resolved_manifest}"
        )
    if observed_contract != expected_contract_month_filter.upper():
        raise ValueError(
            f"Snapshot contract {observed_contract!r} is not "
            f"{expected_contract_month_filter.upper()!r}: {resolved_manifest}"
        )
    if observed_mapped != expected_mapped_stock_futures:
        raise ValueError(
            f"Snapshot mapped-stock count {observed_mapped} is not "
            f"{expected_mapped_stock_futures}: {resolved_manifest}"
        )
    required_hashes = {
        key: str(universe.get(key, ""))
        for key in (
            "file_sha256",
            "universe_sha256",
            "mapped_universe_sha256",
            "mapped_symbol_set_sha256",
        )
    }
    if not all(required_hashes.values()):
        raise ValueError("Snapshot lacks the complete universe hash binding")
    top_universe_path = Path(str(snapshot.get("universe_path", ""))).resolve()
    capture_universe_path = Path(str(capture.get("snapshot_path", ""))).resolve()
    if top_universe_path != capture_universe_path:
        raise AssertionError("Snapshot top-level and captured universe paths differ")
    if not capture_universe_path.is_file():
        raise FileNotFoundError(
            f"Snapshot universe is missing: {capture_universe_path}"
        )
    capture_sha256 = str(capture.get("sha256", ""))
    if capture_sha256 != required_hashes["file_sha256"]:
        raise AssertionError("Snapshot universe capture and universe hashes differ")
    if sha256_file(capture_universe_path) != required_hashes["file_sha256"]:
        raise AssertionError("Snapshot universe hash changed")
    captures = list(snapshot.get("captures", []))
    inventory = dict(snapshot.get("source_inventory", {}))
    expected_capture_count = expected_mapped_stock_futures * 2
    if len(captures) != expected_capture_count:
        raise ValueError(
            f"Snapshot has {len(captures)} physical captures, expected "
            f"{expected_capture_count}"
        )
    if (
        int(inventory.get("entry_count", -1)) != expected_capture_count
        or int(inventory.get("existing_count", -1)) != expected_capture_count
        or int(inventory.get("missing_count", -1)) != 0
    ):
        raise ValueError("Snapshot source inventory is incomplete")
    return snapshot, required_hashes


def _run_contract_payload(
    *,
    historical_provenance: Path | None,
    historical_snapshot: Path | None,
    today_snapshot: Path,
    reference_package: Path | None,
    rejected_today_snapshot: Path | None,
    seed_cache_package: Path | None,
) -> dict[str, Any]:
    inputs: dict[str, Any] = {
        "today_snapshot_manifest": _artifact_record(today_snapshot),
    }
    if historical_provenance is not None:
        inputs["historical_provenance"] = _artifact_record(historical_provenance)
    if historical_snapshot is not None:
        inputs["historical_snapshot_manifest"] = _artifact_record(
            historical_snapshot
        )
    if reference_package is not None:
        inputs["reference_package_manifest"] = _artifact_record(
            reference_package.resolve() / "manifest.json"
        )
    if rejected_today_snapshot is not None:
        inputs["rejected_today_snapshot_manifest"] = _artifact_record(
            rejected_today_snapshot
        )
    if seed_cache_package is not None:
        seed_run_contract = seed_cache_package.resolve() / "run_contract.json"
        inputs["seed_source_run_contract"] = _artifact_record(seed_run_contract)
        for dataset in ("historical", "today"):
            manifests = sorted(
                (seed_cache_package.resolve() / f"{dataset}_baseline_cache").glob(
                    "*/manifest.json"
                )
            )
            if len(manifests) != 1:
                raise ValueError(
                    f"Seed package requires one {dataset} cache manifest, "
                    f"observed {len(manifests)}"
                )
            inputs[f"seed_{dataset}_cache_manifest"] = _artifact_record(
                manifests[0]
            )
    return {
        "schema_version": RUN_CONTRACT_SCHEMA_VERSION,
        "inputs": inputs,
        "windows": {
            "historical_from_day": HISTORICAL_FROM_DAY,
            "historical_through_day": HISTORICAL_THROUGH_DAY,
            "split_day": SPLIT_DAY,
            "today": TODAY,
        },
        "economics": {
            "cost_bps": COST_BPS,
            "slippage_bps": SLIPPAGE_BPS,
            "square_off": SQUARE_OFF,
            "eod_policy": EOD_POLICY,
        },
        "registry_sha256": canonical_sha256(registry_payload()),
        "engine_source": _artifact_record(Path(engine.__file__)),
        "strict_launcher_source": _artifact_record(Path(strict.__file__)),
        "research_runner_source": _artifact_record(Path(__file__)),
        "research_only": True,
        "promotion_eligible": False,
    }


def _establish_run_contract(
    target: Path,
    payload: Mapping[str, Any],
    *,
    resume: bool,
) -> Path:
    path = target / "run_contract.json"
    expected_hash = canonical_sha256(payload)
    if resume:
        if not path.is_file():
            raise FileNotFoundError(f"Resume run contract is missing: {path}")
        observed = json.loads(path.read_text(encoding="utf-8"))
        if canonical_sha256(observed) != expected_hash:
            raise AssertionError("Resume arguments or immutable source bindings changed")
    else:
        common.atomic_write_json(path, dict(payload))
    return path


def _archive_sources(
    target: Path,
    run_contract: Mapping[str, Any],
    *,
    resume: bool,
) -> dict[str, dict[str, Any]]:
    archive_dir = target / "provenance_sources"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archived: dict[str, dict[str, Any]] = {}
    for key, filename in (
        ("engine_source", "fno_v8_windowed_1m_entry_backtest.py"),
        ("strict_launcher_source", "fno_v8_strict_v6_logic_backtest.py"),
        ("research_runner_source", "fno_v6_isolated_challenger_replay.py"),
    ):
        source_record = dict(run_contract[key])
        source_path = _require_artifact(source_record, key)
        destination = archive_dir / filename
        if resume:
            if not destination.is_file():
                raise FileNotFoundError(f"Resume source archive is missing: {destination}")
        else:
            temporary = destination.with_name(f".{destination.name}.tmp")
            shutil.copyfile(source_path, temporary)
            os.replace(temporary, destination)
        archived_record = _artifact_record(destination)
        if (
            archived_record["sha256"] != source_record["sha256"]
            or archived_record["size"] != source_record["size"]
        ):
            raise AssertionError(f"Archived source does not match {key}")
        archived[key] = archived_record
    return archived


def _seed_validated_caches(
    seed_package: Path,
    target: Path,
) -> dict[str, Any]:
    """Copy only fully sealed cache bytes from a failed downstream research run."""

    provenance_payload: dict[str, Any] = {
        "schema_version": SEED_CACHE_SCHEMA_VERSION,
        "source_package": str(seed_package.resolve()),
        "reason": SEED_CACHE_REASON,
        "source_run_contract": _artifact_record(
            seed_package.resolve() / "run_contract.json"
        ),
        "source_root_manifest_absent": not (
            seed_package.resolve() / "manifest.json"
        ).exists(),
        "source_completion_marker_count": len(
            list(seed_package.resolve().rglob("completion.json"))
        ),
        "cache_bytes_replayed_by_engine_contract_validation": True,
        "datasets": {},
    }
    if provenance_payload["source_root_manifest_absent"] is not True:
        raise ValueError("Seed package is already complete and cannot be recovered")
    if provenance_payload["source_completion_marker_count"] != 32:
        raise ValueError("Seed package does not contain 32 sealed replay checkpoints")
    for dataset in ("historical", "today"):
        source_manifests = sorted(
            (seed_package.resolve() / f"{dataset}_baseline_cache").glob(
                "*/manifest.json"
            )
        )
        if len(source_manifests) != 1:
            raise ValueError(
                f"Seed package requires one {dataset} cache manifest"
            )
        source_manifest_path = source_manifests[0]
        source_manifest = json.loads(
            source_manifest_path.read_text(encoding="utf-8")
        )
        if (
            source_manifest.get("schema_version") != STRICT_V6_CACHE_SCHEMA_VERSION
            or source_manifest.get("complete") is not True
        ):
            raise ValueError(f"Seed {dataset} cache is not complete")
        source_artifacts = dict(source_manifest.get("artifacts", {}))
        source_paths = {
            name: _require_artifact(
                dict(source_artifacts[name]), f"seed {dataset} cache {name}"
            )
            for name in ("candidates", "paths", "coverage")
        }
        destination_root = (
            target
            / f"{dataset}_baseline_cache"
            / source_manifest_path.parent.name
        )
        if destination_root.exists():
            raise FileExistsError(
                f"Seed cache destination exists: {destination_root}"
            )
        destination_root.mkdir(parents=True, exist_ok=False)
        destination_paths: dict[str, Path] = {}
        for name, source_path in source_paths.items():
            destination = destination_root / source_path.name
            shutil.copyfile(source_path, destination)
            destination_paths[name] = destination
            if (
                sha256_file(destination) != sha256_file(source_path)
                or destination.stat().st_size != source_path.stat().st_size
            ):
                raise AssertionError(f"Seed {dataset} cache copy changed: {name}")
        copied_manifest = dict(source_manifest)
        copied_manifest["artifacts"] = {
            name: _artifact_record(destination_paths[name])
            for name in ("candidates", "paths", "coverage")
        }
        copied_manifest_path = destination_root / "manifest.json"
        common.atomic_write_json(copied_manifest_path, copied_manifest)
        provenance_payload["datasets"][dataset] = {
            "source_manifest": _artifact_record(source_manifest_path),
            "source_artifacts": {
                name: _artifact_record(path)
                for name, path in source_paths.items()
            },
            "copied_manifest": _artifact_record(copied_manifest_path),
            "copied_artifacts": {
                name: _artifact_record(path)
                for name, path in destination_paths.items()
            },
            "input_fingerprint": source_manifest.get("input_fingerprint"),
        }
    common.atomic_write_json(
        target / "seed_cache_provenance.json", provenance_payload
    )
    return provenance_payload


def _verify_seed_cache_provenance(
    *,
    seed_package: Path,
    target: Path,
    payload: Mapping[str, Any],
    run_contract: Mapping[str, Any],
    returned_caches: Mapping[str, tuple[Mapping[str, Any], Path]] | None = None,
) -> None:
    seed_root = seed_package.resolve()
    target_root = target.resolve()
    value = dict(payload)
    if value.get("schema_version") != SEED_CACHE_SCHEMA_VERSION:
        raise ValueError("Unsupported cache-seed provenance schema")
    if value.get("reason") != SEED_CACHE_REASON:
        raise AssertionError("Cache-seed recovery reason changed")
    if Path(str(value.get("source_package", ""))).resolve() != seed_root:
        raise AssertionError("Cache-seed source package changed")
    if (seed_root / "manifest.json").exists():
        raise AssertionError("A completed package cannot be used as a cache seed")
    completions = sorted(seed_root.rglob("completion.json"))
    if (
        value.get("source_root_manifest_absent") is not True
        or int(value.get("source_completion_marker_count", -1)) != 32
        or len(completions) != 32
    ):
        raise AssertionError("Cache-seed incomplete-package evidence changed")
    contract_inputs = dict(run_contract.get("inputs", {}))
    source_run_contract = dict(value.get("source_run_contract", {}))
    if canonical_sha256(source_run_contract) != canonical_sha256(
        contract_inputs.get("seed_source_run_contract", {})
    ):
        raise AssertionError("Cache-seed run contract binding changed")
    _require_artifact(source_run_contract, "cache-seed source run contract")
    datasets = dict(value.get("datasets", {}))
    if set(datasets) != {"historical", "today"}:
        raise AssertionError("Cache-seed dataset set changed")

    for dataset in ("historical", "today"):
        record = dict(datasets[dataset])
        source_manifest_record = dict(record.get("source_manifest", {}))
        if canonical_sha256(source_manifest_record) != canonical_sha256(
            contract_inputs.get(f"seed_{dataset}_cache_manifest", {})
        ):
            raise AssertionError(f"Seed {dataset} source manifest binding changed")
        source_manifest_path = _require_artifact(
            source_manifest_record, f"seed {dataset} source manifest"
        )
        source_manifest = json.loads(
            source_manifest_path.read_text(encoding="utf-8")
        )
        if (
            source_manifest.get("schema_version") != STRICT_V6_CACHE_SCHEMA_VERSION
            or source_manifest.get("complete") is not True
        ):
            raise AssertionError(f"Seed {dataset} source cache changed")
        if record.get("input_fingerprint") != source_manifest.get(
            "input_fingerprint"
        ):
            raise AssertionError(f"Seed {dataset} fingerprint changed")
        source_artifacts = dict(record.get("source_artifacts", {}))
        copied_artifacts = dict(record.get("copied_artifacts", {}))
        if set(source_artifacts) != {"candidates", "paths", "coverage"} or set(
            copied_artifacts
        ) != {"candidates", "paths", "coverage"}:
            raise AssertionError(f"Seed {dataset} artifact set changed")
        if canonical_sha256(source_artifacts) != canonical_sha256(
            source_manifest.get("artifacts", {})
        ):
            raise AssertionError(f"Seed {dataset} source artifacts changed")
        for name in ("candidates", "paths", "coverage"):
            source_path = _require_artifact(
                dict(source_artifacts[name]), f"seed {dataset} source {name}"
            )
            copied_path = _require_artifact(
                dict(copied_artifacts[name]), f"seed {dataset} copied {name}"
            )
            try:
                copied_path.relative_to(target_root)
            except ValueError as exc:
                raise AssertionError("Copied seed artifact escapes target") from exc
            if (
                sha256_file(source_path) != sha256_file(copied_path)
                or source_path.stat().st_size != copied_path.stat().st_size
            ):
                raise AssertionError(f"Seed {dataset} copied {name} changed")
        copied_manifest_record = dict(record.get("copied_manifest", {}))
        copied_manifest_path = _require_artifact(
            copied_manifest_record, f"seed {dataset} copied manifest"
        )
        try:
            copied_manifest_path.relative_to(target_root)
        except ValueError as exc:
            raise AssertionError("Copied seed manifest escapes target") from exc
        copied_manifest = json.loads(
            copied_manifest_path.read_text(encoding="utf-8")
        )
        if (
            copied_manifest.get("complete") is not True
            or copied_manifest.get("input_fingerprint")
            != source_manifest.get("input_fingerprint")
            or canonical_sha256(copied_manifest.get("input_contract", {}))
            != canonical_sha256(source_manifest.get("input_contract", {}))
            or canonical_sha256(copied_manifest.get("artifacts", {}))
            != canonical_sha256(copied_artifacts)
        ):
            raise AssertionError(f"Seed {dataset} copied manifest changed")
        if returned_caches is not None:
            returned_manifest, returned_path = returned_caches[dataset]
            if returned_path.resolve() != copied_manifest_path.resolve():
                raise AssertionError(f"Seed {dataset} cache was silently rebuilt")
            if canonical_sha256(returned_manifest) != canonical_sha256(
                copied_manifest
            ):
                raise AssertionError(f"Seed {dataset} returned manifest changed")


def _read_frozen_historical_inputs(
    provenance_path: Path,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    dict[str, Any],
    Path,
    dict[str, Any],
    pd.DataFrame,
]:
    validated = engine.validate_v8_run_provenance(provenance_path)
    parameters = dict(validated.get("parameters", {}))
    window = dict(validated.get("backtest_window", {}))
    entry = dict(parameters.get("entry_policy", {}))
    expected = {
        "from_day": HISTORICAL_FROM_DAY,
        "through_day": HISTORICAL_THROUGH_DAY,
    }
    if {key: str(window.get(key)) for key in expected} != expected:
        raise ValueError("Historical provenance does not cover the frozen window")
    if str(window.get("split_day")) != SPLIT_DAY:
        raise ValueError("Historical provenance uses a different TRAIN/TEST split")
    if not math.isclose(float(entry.get("cost_bps", math.nan)), COST_BPS):
        raise ValueError("Historical provenance does not use 15 bps cost")
    if not math.isclose(
        float(entry.get("slippage_bps", math.nan)), SLIPPAGE_BPS
    ):
        raise ValueError("Historical provenance does not use zero slippage")
    if str(entry.get("eod_policy")) != EOD_POLICY:
        raise ValueError("Historical provenance uses a different EOD policy")

    artifacts = dict(validated.get("cache_artifacts", {}))
    candidate_path = _require_artifact(dict(artifacts["candidates"]), "candidate cache")
    minute_path = _require_artifact(dict(artifacts["paths"]), "minute-path cache")
    coverage_path = _require_artifact(dict(artifacts["coverage"]), "coverage cache")
    manifest_path = Path(str(validated["cache_manifest_path"])).resolve()
    if sha256_file(manifest_path) != str(validated["cache_manifest_sha256"]):
        raise AssertionError("Frozen historical cache manifest changed")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    frozen_audit_path = _require_artifact(
        dict(dict(validated["outputs"])["candidate_order_audit"]),
        "frozen historical audit",
    )
    return (
        pd.read_parquet(candidate_path),
        pd.read_parquet(minute_path),
        pd.read_parquet(coverage_path),
        manifest,
        manifest_path,
        validated,
        pd.read_csv(frozen_audit_path),
    )


def _build_snapshot_inputs(
    snapshot_path: Path,
    cache_root: Path,
    *,
    from_day: str,
    through_day: str,
    run_label: str,
    expected_master_date: str,
    expected_contract_month_filter: str,
    expected_mapped_stock_futures: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any], Path]:
    # Strict configuration is already active.  Bind the immutable snapshot's
    # complete persisted universe identity process-locally; do not edit the
    # frozen strict launcher or its historical constants.  This supports both
    # the historical August universe and a separately supplied rollover-day
    # September universe without inferring either identity from a directory
    # name.
    snapshot, required_hashes = _snapshot_identity(
        snapshot_path,
        expected_master_date=expected_master_date,
        expected_contract_month_filter=expected_contract_month_filter,
        expected_mapped_stock_futures=expected_mapped_stock_futures,
    )
    universe = dict(snapshot.get("universe", {}))
    master_date = date.fromisoformat(str(universe["master_date"]))
    snapshot_universe_path = Path(str(snapshot["universe_path"])).resolve()
    engine.BACKTEST_UNIVERSE_DATE = master_date
    engine.BACKTEST_UNIVERSE_PATH = snapshot_universe_path
    engine.BACKTEST_CONTRACT_MONTH_FILTER = str(universe["contract_month_filter"])
    engine.BACKTEST_UNIVERSE_HASHES = required_hashes
    safe_label = "".join(
        character if character.isalnum() else "_"
        for character in str(run_label).upper().strip()
    ).strip("_")
    if not safe_label:
        raise ValueError("Snapshot run label cannot be empty")
    engine.STRATEGY_VERSION = (
        f"{strict.STRATEGY_FAMILY}_{strict.launcher_sha256()[:12]}_{safe_label}"
    )
    contract_label = str(universe["contract_month_filter"]).upper().strip()
    engine.OI_INSTRUMENT = (
        f"STATIC_{contract_label}_NFO_FUTURE_REPAIRED_RESEARCH_ONLY"
    )
    engine.SOURCE_LIMITATION_LABELS = (
        f"STATIC_{master_date.isoformat()}_UNIVERSE_RESEARCH",
        f"STATIC_{contract_label}_FUTURES_OI_NOT_HISTORICAL_ROLLING",
        "LEGACY_EQUITY_1M_HAS_NO_ROW_LINEAGE_FLAGS",
        "SOURCE_SNAPSHOT_IS_PER_FILE_STABLE_NOT_GLOBAL_TRANSACTION",
    )

    # Redirect every cache write to the research package before asking the
    # neutral engine to build today.
    engine.CACHE_DIR = cache_root.resolve()
    engine.CACHE_MANIFEST_PATH = engine.CACHE_DIR / "manifest.json"
    engine.CANDIDATE_CACHE_PATH = engine.CACHE_DIR / "five_minute_candidates.parquet"
    engine.PATH_CACHE_PATH = engine.CACHE_DIR / "same_session_minute_paths.parquet"
    return engine.load_or_build_v8_cache(
        source_snapshot_path=snapshot_path,
        from_day=from_day,
        through_day=through_day,
        symbols=None,
        rebuild=False,
    )


def _read_cache_from_manifest(
    manifest_path: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any], Path]:
    resolved = manifest_path.resolve()
    manifest = json.loads(resolved.read_text(encoding="utf-8"))
    artifacts = dict(manifest.get("artifacts", {}))
    candidate_path = _require_artifact(dict(artifacts["candidates"]), "candidate cache")
    minute_path = _require_artifact(dict(artifacts["paths"]), "minute-path cache")
    coverage_path = _require_artifact(dict(artifacts["coverage"]), "coverage cache")
    if int(manifest.get("candidate_count", -1)) != len(pd.read_parquet(candidate_path)):
        raise AssertionError("Resumed cache candidate count changed")
    candidates = pd.read_parquet(candidate_path)
    minute_paths = pd.read_parquet(minute_path)
    coverage = pd.read_parquet(coverage_path)
    if int(manifest.get("path_row_count", -1)) != len(minute_paths):
        raise AssertionError("Resumed cache path-row count changed")
    return candidates, minute_paths, coverage, manifest, resolved


def _coverage_summary(
    coverage: pd.DataFrame,
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    contract = dict(manifest.get("input_contract", {}))
    derived = engine.derive_coverage_completeness(
        coverage,
        selected_symbols=contract.get("symbols", []),
        expected_session_dates=dict(contract.get("session_calendar", {})).get(
            "expected_session_dates", []
        ),
    )
    return dict(derived)


def _replay_binding_payload(
    *,
    spec: ChallengerSpec,
    manifest: Mapping[str, Any],
    policy: engine.EntryPolicy,
    cost_scenario: str,
) -> dict[str, Any]:
    snapshot_manifest = Path(
        str(dict(manifest.get("source_snapshot", {})).get("manifest_path", ""))
    ).resolve()
    if not snapshot_manifest.is_file():
        raise FileNotFoundError(
            f"Replay source-snapshot manifest is missing: {snapshot_manifest}"
        )
    return {
        "schema_version": "fno_v6_challenger_replay_binding_v1",
        "variant": spec.variant,
        "variant_config_sha256": canonical_sha256(spec.payload()),
        "cost_scenario": cost_scenario,
        "policy": asdict(policy),
        "policy_sha256": canonical_sha256(asdict(policy)),
        "cache_input_fingerprint": manifest.get("input_fingerprint"),
        "cache_manifest_contract_sha256": canonical_sha256(
            manifest.get("input_contract", {})
        ),
        "source_snapshot_manifest": _artifact_record(snapshot_manifest),
        "engine_source_sha256": sha256_file(Path(engine.__file__)),
        "strict_launcher_source_sha256": sha256_file(Path(strict.__file__)),
        "research_runner_source_sha256": sha256_file(Path(__file__)),
        "research_only": True,
        "promotion_eligible": False,
    }


def _replay_completion_payload(
    *,
    binding: Mapping[str, Any],
    output_paths: Mapping[str, Path],
) -> dict[str, Any]:
    return {
        "schema_version": REPLAY_COMPLETION_SCHEMA_VERSION,
        "binding_sha256": canonical_sha256(binding),
        "artifacts": {
            label: _artifact_record(path)
            for label, path in sorted(output_paths.items())
        },
        "complete": True,
    }


def _verify_replay_completion(
    completion_path: Path,
    *,
    expected_binding: Mapping[str, Any],
    output_paths: Mapping[str, Path],
) -> dict[str, Any]:
    completion = json.loads(completion_path.read_text(encoding="utf-8"))
    if completion.get("schema_version") != REPLAY_COMPLETION_SCHEMA_VERSION:
        raise ValueError(f"Unsupported replay completion schema: {completion_path}")
    if completion.get("complete") is not True:
        raise AssertionError(f"Replay completion is not complete: {completion_path}")
    if completion.get("binding_sha256") != canonical_sha256(expected_binding):
        raise AssertionError(f"Replay completion binding changed: {completion_path}")
    records = dict(completion.get("artifacts", {}))
    if set(records) != set(output_paths):
        raise AssertionError(f"Replay completion artifact set changed: {completion_path}")
    for label, expected_path in output_paths.items():
        record = dict(records[label])
        if Path(str(record.get("path", ""))).resolve() != expected_path.resolve():
            raise AssertionError(
                f"Replay completion path binding changed: {completion_path}/{label}"
            )
        _require_artifact(record, f"replay {label}")
    return completion


def _closed(audit: pd.DataFrame) -> pd.DataFrame:
    if audit.empty:
        return audit.copy()
    filled = _filled_series(audit)
    finite = (
        np.isfinite(pd.to_numeric(audit["net_return_pct"], errors="coerce"))
        & np.isfinite(pd.to_numeric(audit["net_pnl_rs"], errors="coerce"))
    )
    return audit.loc[filled & finite].copy()


def _period_row(
    audit: pd.DataFrame,
    daily: pd.DataFrame,
    *,
    dataset: str,
    variant: str,
    period: str,
) -> dict[str, Any]:
    if period == "FULL":
        period_daily = daily.copy()
    else:
        period_daily = daily.loc[daily["period"].astype(str).eq(period)].copy()
    days = set(period_daily["session_date"].astype(str))
    period_audit = audit.loc[audit["session_date"].astype(str).isin(days)].copy()
    closed = _closed(period_audit)
    returns = pd.to_numeric(closed.get("net_return_pct"), errors="coerce")
    profits = float(returns.loc[returns.gt(0)].sum())
    losses = float(-returns.loc[returns.lt(0)].sum())
    cumulative = np.concatenate(
        ([0.0], period_daily["net_return_pct"].cumsum().to_numpy(dtype=float))
    )
    drawdown = cumulative - np.maximum.accumulate(cumulative)
    return {
        "dataset": dataset,
        "variant": variant,
        "period": period,
        "sessions": int(len(period_daily)),
        "candidates": int(len(period_audit)),
        "fills": int(len(closed)),
        "wins": int(returns.gt(0).sum()),
        "losses": int(returns.lt(0).sum()),
        "win_rate_pct": float(returns.gt(0).mean() * 100.0) if len(returns) else None,
        "profit_factor": (
            profits / losses
            if losses > 0
            else math.inf
            if profits > 0
            else None
        ),
        "net_return_points": float(returns.sum()),
        "net_pnl_rs": float(
            pd.to_numeric(closed.get("net_pnl_rs"), errors="coerce").sum()
        ),
        "max_daily_drawdown_points": (
            max(0.0, float(-drawdown.min())) if len(drawdown) else 0.0
        ),
        "positive_days": int(period_daily["net_return_pct"].gt(0).sum()),
        "negative_days": int(period_daily["net_return_pct"].lt(0).sum()),
        "flat_days": int(period_daily["net_return_pct"].eq(0).sum()),
    }


def _replay_one(
    *,
    dataset: str,
    spec: ChallengerSpec,
    candidates: pd.DataFrame,
    minute_paths: pd.DataFrame,
    coverage: pd.DataFrame,
    manifest: Mapping[str, Any],
    policy: engine.EntryPolicy,
    output_dir: Path,
    split_day: str | None,
    resume: bool = False,
) -> dict[str, Any]:
    variant_dir = output_dir / dataset.lower() / spec.variant.lower()
    expected_binding = _replay_binding_payload(
        spec=spec,
        manifest=manifest,
        policy=policy,
        cost_scenario="BASE_15BPS_0SLIP",
    )
    expected_outputs = {
        "audit": variant_dir / "candidate_order_audit.csv",
        "decisions": variant_dir / "selection_decisions.csv",
        "daily": variant_dir / "daily.csv",
        "diagnostic": variant_dir / "diagnostic_breakdowns.csv",
        "summary": variant_dir / "summary.json",
        "binding": variant_dir / "replay_binding.json",
    }
    completion_path = variant_dir / "completion.json"
    if resume and completion_path.is_file():
        _verify_replay_completion(
            completion_path,
            expected_binding=expected_binding,
            output_paths=expected_outputs,
        )
        audit = pd.read_csv(expected_outputs["audit"])
        decisions = pd.read_csv(expected_outputs["decisions"])
        daily = pd.read_csv(expected_outputs["daily"])
        summary = json.loads(expected_outputs["summary"].read_text(encoding="utf-8"))
        binding = json.loads(expected_outputs["binding"].read_text(encoding="utf-8"))
        if canonical_sha256(binding) != canonical_sha256(expected_binding):
            raise AssertionError(f"Resume replay binding changed: {spec.variant}")
        if (
            not audit["research_variant"].astype(str).eq(spec.variant).all()
            or not decisions["research_variant"].astype(str).eq(spec.variant).all()
        ):
            raise AssertionError(f"Resume artifact variant mismatch: {spec.variant}")
        return {
            "audit": audit,
            "decisions": decisions,
            "daily": daily,
            "summary": summary,
            "directory": variant_dir,
        }
    filtered, decisions = apply_selection_overlay(candidates, spec)
    audit = engine.run_v8_backtest(
        filtered,
        minute_paths,
        variant="VS",
        policy=policy,
    )
    audit = audit.copy()
    audit["research_variant"] = spec.variant
    audit["variant_config_sha256"] = canonical_sha256(spec.payload())
    completeness = _coverage_summary(coverage, manifest)
    summary, daily = engine.summarize_v8_results(
        audit,
        session_dates=manifest.get("session_dates", []),
        split_day=split_day,
        eod_policy=policy.eod_policy,
        source_complete=bool(completeness["headline_source_complete"]),
        source_incomplete_symbol_sessions=int(
            completeness["source_incomplete_symbol_sessions"]
        ),
        unexpected_source_symbol_sessions=int(
            completeness["unexpected_source_symbol_sessions"]
        ),
    )
    diagnostic = engine.build_v8_diagnostic_breakdowns(
        audit,
        session_dates=manifest.get("session_dates", []),
    )
    variant_dir.mkdir(parents=True, exist_ok=resume)
    common.atomic_write_csv(audit, variant_dir / "candidate_order_audit.csv")
    common.atomic_write_csv(decisions, variant_dir / "selection_decisions.csv")
    common.atomic_write_csv(daily, variant_dir / "daily.csv")
    common.atomic_write_csv(diagnostic, variant_dir / "diagnostic_breakdowns.csv")
    common.atomic_write_json(variant_dir / "summary.json", summary)
    common.atomic_write_json(variant_dir / "replay_binding.json", expected_binding)
    common.atomic_write_json(
        completion_path,
        _replay_completion_payload(
            binding=expected_binding,
            output_paths=expected_outputs,
        ),
    )
    return {
        "audit": audit,
        "decisions": decisions,
        "daily": daily,
        "summary": summary,
        "directory": variant_dir,
    }


def _replay_cost_stress_one(
    *,
    dataset: str,
    spec: ChallengerSpec,
    scenario: CostScenario,
    candidates: pd.DataFrame,
    minute_paths: pd.DataFrame,
    coverage: pd.DataFrame,
    manifest: Mapping[str, Any],
    output_dir: Path,
    split_day: str | None,
    resume: bool = False,
) -> dict[str, Any]:
    variant_dir = (
        output_dir
        / "cost_stress"
        / dataset.lower()
        / scenario.scenario.lower()
        / spec.variant.lower()
    )
    expected_outputs = {
        "audit": variant_dir / "candidate_order_audit.csv",
        "decisions": variant_dir / "selection_decisions.csv",
        "daily": variant_dir / "daily.csv",
        "diagnostic": variant_dir / "diagnostic_breakdowns.csv",
        "summary": variant_dir / "summary.json",
        "binding": variant_dir / "replay_binding.json",
    }
    policy = engine.entry_policy_for_variant(
        "VS",
        cost_bps=scenario.cost_bps,
        slippage_bps=scenario.slippage_bps,
        square_off=SQUARE_OFF,
        eod_policy=EOD_POLICY,
    )
    expected_binding = _replay_binding_payload(
        spec=spec,
        manifest=manifest,
        policy=policy,
        cost_scenario=scenario.scenario,
    )
    completion_path = variant_dir / "completion.json"
    if resume and completion_path.is_file():
        _verify_replay_completion(
            completion_path,
            expected_binding=expected_binding,
            output_paths=expected_outputs,
        )
        audit = pd.read_csv(expected_outputs["audit"])
        decisions = pd.read_csv(expected_outputs["decisions"])
        daily = pd.read_csv(expected_outputs["daily"])
        summary = json.loads(expected_outputs["summary"].read_text(encoding="utf-8"))
        binding = json.loads(expected_outputs["binding"].read_text(encoding="utf-8"))
        if canonical_sha256(binding) != canonical_sha256(expected_binding):
            raise AssertionError(
                f"Cost-stress resume binding changed: {scenario.scenario}/{spec.variant}"
            )
        if not audit["research_variant"].astype(str).eq(spec.variant).all():
            raise AssertionError(f"Cost-stress resume variant mismatch: {spec.variant}")
        if not audit["cost_scenario"].astype(str).eq(scenario.scenario).all():
            raise AssertionError(
                f"Cost-stress resume scenario mismatch: {scenario.scenario}"
            )
        return {
            "audit": audit,
            "decisions": decisions,
            "daily": daily,
            "summary": summary,
            "directory": variant_dir,
        }

    filtered, decisions = apply_selection_overlay(candidates, spec)
    audit = engine.run_v8_backtest(
        filtered,
        minute_paths,
        variant="VS",
        policy=policy,
    ).copy()
    audit["research_variant"] = spec.variant
    audit["variant_config_sha256"] = canonical_sha256(spec.payload())
    audit["cost_scenario"] = scenario.scenario
    decisions = decisions.copy()
    decisions["cost_scenario"] = scenario.scenario
    completeness = _coverage_summary(coverage, manifest)
    summary, daily = engine.summarize_v8_results(
        audit,
        session_dates=manifest.get("session_dates", []),
        split_day=split_day,
        eod_policy=policy.eod_policy,
        source_complete=bool(completeness["headline_source_complete"]),
        source_incomplete_symbol_sessions=int(
            completeness["source_incomplete_symbol_sessions"]
        ),
        unexpected_source_symbol_sessions=int(
            completeness["unexpected_source_symbol_sessions"]
        ),
    )
    diagnostic = engine.build_v8_diagnostic_breakdowns(
        audit,
        session_dates=manifest.get("session_dates", []),
    )
    variant_dir.mkdir(parents=True, exist_ok=resume)
    common.atomic_write_csv(audit, variant_dir / "candidate_order_audit.csv")
    common.atomic_write_csv(decisions, variant_dir / "selection_decisions.csv")
    common.atomic_write_csv(daily, variant_dir / "daily.csv")
    common.atomic_write_csv(diagnostic, variant_dir / "diagnostic_breakdowns.csv")
    common.atomic_write_json(variant_dir / "summary.json", summary)
    common.atomic_write_json(variant_dir / "replay_binding.json", expected_binding)
    common.atomic_write_json(
        completion_path,
        _replay_completion_payload(
            binding=expected_binding,
            output_paths=expected_outputs,
        ),
    )
    return {
        "audit": audit,
        "decisions": decisions,
        "daily": daily,
        "summary": summary,
        "directory": variant_dir,
    }


def _normalise_time(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True).astype(str)


def _boolean_values(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False).astype(bool)
    return series.fillna("").astype(str).str.strip().str.lower().isin(
        {"true", "1", "yes"}
    )


def _normalise_engine_audit_for_parity(
    frame: pd.DataFrame,
    *,
    reference: pd.DataFrame,
) -> pd.DataFrame:
    out = frame.copy()
    for column in out.columns:
        if column == "session_date":
            out[column] = pd.to_datetime(out[column], errors="raise").dt.strftime(
                "%Y-%m-%d"
            )
        elif column.endswith("_time") or column in {"signal_time"}:
            out[column] = _normalise_time(out[column])
        elif pd.api.types.is_bool_dtype(reference[column]):
            out[column] = _boolean_values(out[column])
        elif pd.api.types.is_numeric_dtype(reference[column]):
            out[column] = pd.to_numeric(out[column], errors="coerce")
        else:
            out[column] = out[column].fillna("").astype(str)
    return out


def _control_parity(
    replay: pd.DataFrame,
    frozen: pd.DataFrame,
    frozen_provenance: Mapping[str, Any],
) -> dict[str, Any]:
    columns = [
        "candidate_id",
        "status",
        "portfolio_decision",
        "entry_time",
        "exit_time",
        "exit_reason",
        "net_pnl_rs",
    ]
    missing = sorted(set(columns) - set(replay.columns) | (set(columns) - set(frozen.columns)))
    if missing:
        raise ValueError(f"Cannot prove control parity; missing columns: {missing}")
    left = replay[columns].copy().sort_values("candidate_id", kind="stable").reset_index(drop=True)
    right = frozen[columns].copy().sort_values("candidate_id", kind="stable").reset_index(drop=True)
    if left["candidate_id"].tolist() != right["candidate_id"].astype(str).tolist():
        raise AssertionError("Historical CONTROL candidate IDs differ from frozen V6")
    categorical = ["status", "portfolio_decision", "exit_reason"]
    categorical_mismatches = 0
    for column in categorical:
        equal = left[column].fillna("").astype(str).eq(
            right[column].fillna("").astype(str)
        )
        categorical_mismatches += int((~equal).sum())
    time_mismatches = int(
        (~_normalise_time(left["entry_time"]).eq(_normalise_time(right["entry_time"]))).sum()
        + (~_normalise_time(left["exit_time"]).eq(_normalise_time(right["exit_time"]))).sum()
    )
    left_pnl = pd.to_numeric(left["net_pnl_rs"], errors="coerce")
    right_pnl = pd.to_numeric(right["net_pnl_rs"], errors="coerce")
    pnl_equal = np.isclose(left_pnl, right_pnl, rtol=1e-10, atol=1e-8, equal_nan=True)
    pnl_mismatches = int((~pnl_equal).sum())
    replay_for_summary = replay.copy()
    replay_for_summary["session_date"] = pd.to_datetime(
        replay_for_summary["session_date"], errors="raise"
    ).dt.date
    observed_summary = engine.summarize_v8_results(
        replay_for_summary,
        session_dates=dict(frozen_provenance["results"])["sessions"]
        and json.loads(
            Path(str(frozen_provenance["cache_manifest_path"])).read_text(encoding="utf-8")
        )["session_dates"],
        split_day=SPLIT_DAY,
        eod_policy=EOD_POLICY,
        source_complete=False,
        source_incomplete_symbol_sessions=int(
            dict(frozen_provenance["results"])["source_incomplete_symbol_sessions"]
        ),
    )[0]
    expected_diagnostic = dict(
        dict(frozen_provenance["results"])["diagnostic_closed_trade_metrics"]
    )
    observed_diagnostic = dict(observed_summary["diagnostic_closed_trade_metrics"])
    aggregate_mismatches = []
    for key in (
        "profit_factor",
        "net_return_percentage_points",
        "net_pnl_rs",
        "max_daily_drawdown_percentage_points",
    ):
        if not math.isclose(
            float(observed_diagnostic[key]),
            float(expected_diagnostic[key]),
            rel_tol=1e-10,
            abs_tol=1e-8,
        ):
            aggregate_mismatches.append(key)
    passed = not (
        categorical_mismatches
        or time_mismatches
        or pnl_mismatches
        or aggregate_mismatches
    )
    if not passed:
        raise AssertionError("Historical CONTROL replay failed frozen parity")
    return {
        "passed": True,
        "candidate_rows": int(len(left)),
        "categorical_mismatches": categorical_mismatches,
        "time_mismatches": time_mismatches,
        "pnl_mismatches": pnl_mismatches,
        "aggregate_mismatches": aggregate_mismatches,
        "frozen_diagnostic_metrics": expected_diagnostic,
        "replay_diagnostic_metrics": observed_diagnostic,
    }


def _fresh_control_parity(
    replay: pd.DataFrame,
    direct: pd.DataFrame,
    *,
    manifest: Mapping[str, Any],
    coverage: pd.DataFrame,
    split_day: str | None,
    dataset: str,
) -> dict[str, Any]:
    """Prove the no-overlay CONTROL equals a direct strict-V6 replay."""

    categorical = [
        "status",
        "portfolio_decision",
        "portfolio_reject_reason",
        "exit_reason",
    ]
    times = ["confirmation_time", "entry_time", "exit_time"]
    numeric = [
        "confirmation_minute",
        "entry_minute",
        "trigger",
        "entry_price",
        "stop_price",
        "target_price",
        "exit_price",
        "net_return_pct",
        "net_pnl_rs",
    ]
    required = {"candidate_id", *categorical, *times, *numeric}
    missing = sorted((required - set(replay.columns)) | (required - set(direct.columns)))
    if missing:
        raise ValueError(f"Cannot prove fresh CONTROL parity; missing: {missing}")
    left = replay.sort_values("candidate_id", kind="stable").reset_index(drop=True)
    right = direct.sort_values("candidate_id", kind="stable").reset_index(drop=True)
    left_ids = left["candidate_id"].astype(str)
    right_ids = right["candidate_id"].astype(str)
    if not left_ids.equals(right_ids):
        raise AssertionError(f"{dataset} fresh CONTROL candidate IDs differ")
    wrapper_only = {
        "research_variant",
        "variant_config_sha256",
        "cost_scenario",
    }
    direct_columns = [column for column in right.columns if column not in wrapper_only]
    missing_direct_columns = sorted(set(direct_columns) - set(left.columns))
    if missing_direct_columns:
        raise ValueError(
            f"{dataset} CONTROL wrapper dropped engine audit columns: "
            f"{missing_direct_columns}"
        )
    full_audit_mismatch = ""
    try:
        left_engine = _normalise_engine_audit_for_parity(
            left[direct_columns], reference=right[direct_columns]
        )
        right_engine = _normalise_engine_audit_for_parity(
            right[direct_columns], reference=right[direct_columns]
        )
        pd.testing.assert_frame_equal(
            left_engine,
            right_engine,
            check_dtype=False,
            check_exact=False,
            rtol=1e-10,
            atol=1e-8,
        )
    except AssertionError as exc:
        full_audit_mismatch = str(exc)[:2000]
    categorical_mismatches = {
        column: int(
            (~left[column].fillna("").astype(str).eq(
                right[column].fillna("").astype(str)
            )).sum()
        )
        for column in categorical
    }
    time_mismatches = {
        column: int(
            (~_normalise_time(left[column]).eq(_normalise_time(right[column]))).sum()
        )
        for column in times
    }
    numeric_mismatches: dict[str, int] = {}
    for column in numeric:
        left_value = pd.to_numeric(left[column], errors="coerce")
        right_value = pd.to_numeric(right[column], errors="coerce")
        numeric_mismatches[column] = int(
            (~np.isclose(
                left_value,
                right_value,
                rtol=1e-10,
                atol=1e-8,
                equal_nan=True,
            )).sum()
        )
    completeness = _coverage_summary(coverage, manifest)

    def summarize(frame: pd.DataFrame) -> dict[str, Any]:
        value = frame.copy()
        value["session_date"] = pd.to_datetime(
            value["session_date"], errors="raise"
        ).dt.date
        return engine.summarize_v8_results(
            value,
            session_dates=manifest.get("session_dates", []),
            split_day=split_day,
            eod_policy=EOD_POLICY,
            source_complete=bool(completeness["headline_source_complete"]),
            source_incomplete_symbol_sessions=int(
                completeness["source_incomplete_symbol_sessions"]
            ),
            unexpected_source_symbol_sessions=int(
                completeness["unexpected_source_symbol_sessions"]
            ),
        )[0]

    replay_summary = summarize(left)
    direct_summary = summarize(right)
    replay_diagnostic = dict(replay_summary["diagnostic_closed_trade_metrics"])
    direct_diagnostic = dict(direct_summary["diagnostic_closed_trade_metrics"])
    aggregate_mismatches = []
    for key in (
        "profit_factor",
        "net_return_percentage_points",
        "net_pnl_rs",
        "max_daily_drawdown_percentage_points",
    ):
        left_value = replay_diagnostic.get(key)
        right_value = direct_diagnostic.get(key)
        if left_value is None or right_value is None:
            matches = left_value is right_value
        else:
            matches = math.isclose(
                float(left_value),
                float(right_value),
                rel_tol=1e-10,
                abs_tol=1e-8,
            )
        if not matches:
            aggregate_mismatches.append(key)
    passed = not (
        full_audit_mismatch
        or
        sum(categorical_mismatches.values())
        or sum(time_mismatches.values())
        or sum(numeric_mismatches.values())
        or aggregate_mismatches
    )
    if not passed:
        raise AssertionError(f"{dataset} fresh CONTROL failed direct parity")
    return {
        "passed": True,
        "dataset": dataset,
        "candidate_rows": int(len(left)),
        "full_engine_audit_columns_compared": int(len(direct_columns)),
        "full_engine_audit_mismatch": full_audit_mismatch,
        "categorical_mismatches": categorical_mismatches,
        "time_mismatches": time_mismatches,
        "numeric_mismatches": numeric_mismatches,
        "aggregate_mismatches": aggregate_mismatches,
        "replay_diagnostic_metrics": replay_diagnostic,
        "direct_diagnostic_metrics": direct_diagnostic,
        "cache_input_fingerprint": manifest.get("input_fingerprint"),
    }


def _fresh_control_parity_bundle(
    *,
    historical_candidates: pd.DataFrame,
    historical_paths: pd.DataFrame,
    historical_manifest: Mapping[str, Any],
    historical_coverage: pd.DataFrame,
    today_candidates: pd.DataFrame,
    today_paths: pd.DataFrame,
    today_manifest: Mapping[str, Any],
    today_coverage: pd.DataFrame,
    replay_results: Mapping[tuple[str, str], Mapping[str, Any]],
    policy: engine.EntryPolicy,
    target: Path,
) -> dict[str, Any]:
    """Compute both direct audits before publishing either parity artifact."""

    direct_historical = engine.run_v8_backtest(
        historical_candidates,
        historical_paths,
        variant="VS",
        policy=policy,
    )
    direct_today = engine.run_v8_backtest(
        today_candidates,
        today_paths,
        variant="VS",
        policy=policy,
    )
    direct_dir = target / "direct_control_parity"
    direct_dir.mkdir(parents=True, exist_ok=True)
    historical_path = direct_dir / "historical_direct_strict_v6_audit.csv"
    today_path = direct_dir / "today_direct_strict_v6_audit.csv"
    common.atomic_write_csv(direct_historical, historical_path)
    common.atomic_write_csv(direct_today, today_path)
    historical_parity = _fresh_control_parity(
        replay_results[("HISTORICAL", "CONTROL")]["audit"],
        direct_historical,
        manifest=historical_manifest,
        coverage=historical_coverage,
        split_day=SPLIT_DAY,
        dataset="HISTORICAL",
    )
    today_parity = _fresh_control_parity(
        replay_results[("TODAY", "CONTROL")]["audit"],
        direct_today,
        manifest=today_manifest,
        coverage=today_coverage,
        split_day=None,
        dataset="TODAY",
    )
    return {
        "passed": bool(historical_parity["passed"] and today_parity["passed"]),
        "parity_kind": "DIRECT_STRICT_V6_FRESH_CACHE_CONTROL",
        "historical": historical_parity,
        "today": today_parity,
        "direct_audits": {
            "historical": _artifact_record(historical_path),
            "today": _artifact_record(today_path),
        },
    }


def _delta_rows(summary: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    metrics = [
        "candidates",
        "fills",
        "wins",
        "losses",
        "win_rate_pct",
        "profit_factor",
        "net_return_points",
        "net_pnl_rs",
        "max_daily_drawdown_points",
    ]
    for (dataset, period), group in summary.groupby(["dataset", "period"], sort=False):
        control = group.loc[group["variant"].eq("CONTROL")]
        if len(control) != 1:
            raise AssertionError(f"Missing unique control for {dataset}/{period}")
        baseline = control.iloc[0]
        for _, row in group.iterrows():
            item = row.to_dict()
            for metric in metrics:
                value = row.get(metric)
                base = baseline.get(metric)
                item[f"delta_{metric}_vs_control"] = (
                    float(value) - float(base)
                    if pd.notna(value) and pd.notna(base)
                    else None
                )
            rows.append(item)
    return pd.DataFrame(rows)


def _cost_stress_delta_rows(summary: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    metrics = [
        "candidates",
        "fills",
        "wins",
        "losses",
        "win_rate_pct",
        "profit_factor",
        "net_return_points",
        "net_pnl_rs",
        "max_daily_drawdown_points",
    ]
    for (dataset, period, scenario), group in summary.groupby(
        ["dataset", "period", "cost_scenario"], sort=False
    ):
        control = group.loc[group["variant"].eq("CONTROL")]
        if len(control) != 1:
            raise AssertionError(
                f"Missing unique cost-stress control for {dataset}/{period}/{scenario}"
            )
        baseline = control.iloc[0]
        for _, row in group.iterrows():
            item = row.to_dict()
            for metric in metrics:
                value = row.get(metric)
                base = baseline.get(metric)
                item[f"delta_{metric}_vs_scenario_control"] = (
                    float(value) - float(base)
                    if pd.notna(value) and pd.notna(base)
                    else None
                )
            rows.append(item)
    out = pd.DataFrame(rows)
    base = out.loc[out["cost_scenario"].eq("BASE_15BPS_0SLIP"), [
        "dataset", "period", "variant", *metrics
    ]].rename(columns={metric: f"base_{metric}" for metric in metrics})
    out = out.merge(
        base,
        on=["dataset", "period", "variant"],
        how="left",
        validate="many_to_one",
    )
    for metric in metrics:
        out[f"delta_{metric}_vs_base_economics"] = (
            pd.to_numeric(out[metric], errors="coerce")
            - pd.to_numeric(out[f"base_{metric}"], errors="coerce")
        )
    return out


_DRIFT_METRICS = (
    "sessions",
    "candidates",
    "fills",
    "wins",
    "losses",
    "win_rate_pct",
    "profit_factor",
    "net_return_points",
    "net_pnl_rs",
    "max_daily_drawdown_points",
    "positive_days",
    "negative_days",
    "flat_days",
)


def _numeric_match(left: pd.Series, right: pd.Series) -> pd.Series:
    left_value = pd.to_numeric(left, errors="coerce")
    right_value = pd.to_numeric(right, errors="coerce")
    left_array = left_value.to_numpy(dtype=float)
    right_array = right_value.to_numpy(dtype=float)
    matches = (
        (np.isnan(left_array) & np.isnan(right_array))
        | (
            np.isinf(left_array)
            & np.isinf(right_array)
            & (np.signbit(left_array) == np.signbit(right_array))
        )
        | np.isclose(
            left_array,
            right_array,
            rtol=1e-10,
            atol=1e-8,
            equal_nan=False,
        )
    )
    return pd.Series(matches, index=left.index, dtype=bool)


def _validate_cost_scenario_economics(frame: pd.DataFrame, *, label: str) -> None:
    required = {"cost_scenario", "cost_bps", "slippage_bps"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{label} cost table is missing columns: {missing}")
    observed = frame[["cost_scenario", "cost_bps", "slippage_bps"]].drop_duplicates()
    if observed["cost_scenario"].duplicated().any():
        raise AssertionError(f"{label} cost scenario maps to multiple economics")
    expected = {
        item.scenario: (float(item.cost_bps), float(item.slippage_bps))
        for item in COST_SCENARIOS
    }
    if set(observed["cost_scenario"].astype(str)) != set(expected):
        raise AssertionError(f"{label} cost scenario set changed")
    for row in observed.itertuples(index=False):
        expected_cost, expected_slip = expected[str(row.cost_scenario)]
        if not math.isclose(float(row.cost_bps), expected_cost) or not math.isclose(
            float(row.slippage_bps), expected_slip
        ):
            raise AssertionError(
                f"{label} economics changed for {row.cost_scenario}"
            )


def _metric_drift(
    current: pd.DataFrame,
    reference: pd.DataFrame,
    *,
    keys: Sequence[str],
) -> pd.DataFrame:
    required = set(keys) | set(_DRIFT_METRICS)
    for label, frame in (("current", current), ("reference", reference)):
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"{label} metric table is missing columns: {missing}")
        if frame.duplicated(list(keys)).any():
            raise AssertionError(f"{label} metric table has duplicate keys")
    left = reference[[*keys, *_DRIFT_METRICS]].copy().rename(
        columns={metric: f"reference_{metric}" for metric in _DRIFT_METRICS}
    )
    right = current[[*keys, *_DRIFT_METRICS]].copy().rename(
        columns={metric: f"repaired_{metric}" for metric in _DRIFT_METRICS}
    )
    out = left.merge(
        right,
        on=list(keys),
        how="outer",
        validate="one_to_one",
        indicator="row_presence",
    )
    schema_version = (
        COST_METRIC_DRIFT_SCHEMA_VERSION
        if "cost_scenario" in keys
        else METRIC_DRIFT_SCHEMA_VERSION
    )
    out.insert(0, "schema_version", schema_version)
    match_columns = []
    for metric in _DRIFT_METRICS:
        reference_values = pd.to_numeric(
            out[f"reference_{metric}"], errors="coerce"
        )
        repaired_values = pd.to_numeric(
            out[f"repaired_{metric}"], errors="coerce"
        )
        matches = _numeric_match(reference_values, repaired_values)
        match_column = f"{metric}_matches"
        match_columns.append(match_column)
        out[match_column] = matches
        delta = repaired_values - reference_values
        out[f"delta_{metric}_repaired_minus_reference"] = delta.mask(
            matches & (~np.isfinite(delta)), 0.0
        )
    out["row_matches"] = (
        out["row_presence"].astype(str).eq("both")
        & out[match_columns].all(axis=1)
    )
    return out


def _filled_series(frame: pd.DataFrame) -> pd.Series:
    if "filled" not in frame.columns:
        return pd.Series(False, index=frame.index, dtype=bool)
    return _boolean_values(frame["filled"])


def _reference_audit_drift(
    reference: pd.DataFrame,
    repaired: pd.DataFrame,
    *,
    dataset: str,
    variant: str,
    cost_scenario: str = "BASE_15BPS_0SLIP",
    include_detail: bool = True,
    cost_comparison: bool = False,
) -> tuple[dict[str, Any], pd.DataFrame]:
    fields = [
        "candidate_id",
        "session_date",
        "setup_id",
        "symbol",
        "status",
        "portfolio_decision",
        "entry_time",
        "entry_price",
        "exit_time",
        "exit_price",
        "exit_reason",
        "filled",
        "net_return_pct",
        "net_pnl_rs",
    ]
    required = set(fields)
    for label, frame in (("reference", reference), ("repaired", repaired)):
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"{label} audit is missing drift fields: {missing}")
        if frame["candidate_id"].astype(str).duplicated().any():
            raise AssertionError(f"{label} audit contains duplicate candidate IDs")
    old = reference[fields].copy()
    new = repaired[fields].copy()
    old["candidate_id"] = old["candidate_id"].astype(str)
    new["candidate_id"] = new["candidate_id"].astype(str)
    old["filled"] = _filled_series(old)
    new["filled"] = _filled_series(new)
    paired = old.merge(
        new,
        on="candidate_id",
        how="outer",
        suffixes=("_reference", "_repaired"),
        indicator="candidate_presence",
        validate="one_to_one",
    )
    added = paired["candidate_presence"].eq("right_only")
    removed = paired["candidate_presence"].eq("left_only")
    common_rows = paired["candidate_presence"].eq("both")

    state_changed = added | removed
    old_filled = paired["filled_reference"].fillna(False).astype(bool)
    new_filled = paired["filled_repaired"].fillna(False).astype(bool)
    state_changed |= common_rows & old_filled.ne(new_filled)
    for field in ("status", "portfolio_decision", "exit_reason"):
        left = paired[f"{field}_reference"].fillna("").astype(str)
        right = paired[f"{field}_repaired"].fillna("").astype(str)
        state_changed |= common_rows & left.ne(right)
    for field in ("entry_time", "exit_time"):
        left = _normalise_time(paired[f"{field}_reference"])
        right = _normalise_time(paired[f"{field}_repaired"])
        state_changed |= common_rows & left.ne(right)
    for field in (
        "entry_price",
        "exit_price",
        "net_return_pct",
        "net_pnl_rs",
    ):
        left = pd.to_numeric(paired[f"{field}_reference"], errors="coerce")
        right = pd.to_numeric(paired[f"{field}_repaired"], errors="coerce")
        state_changed |= common_rows & ~np.isclose(
            left,
            right,
            rtol=1e-10,
            atol=1e-8,
            equal_nan=True,
        )
    old_pnl = pd.to_numeric(paired["net_pnl_rs_reference"], errors="coerce")
    new_pnl = pd.to_numeric(paired["net_pnl_rs_repaired"], errors="coerce")
    old_filled_pnl = old_pnl.where(old_filled & np.isfinite(old_pnl), 0.0)
    new_filled_pnl = new_pnl.where(new_filled & np.isfinite(new_pnl), 0.0)
    summary = {
        "schema_version": (
            COST_CANDIDATE_STATE_DRIFT_SCHEMA_VERSION
            if cost_comparison
            else CANDIDATE_STATE_DRIFT_SCHEMA_VERSION
        ),
        "dataset": dataset,
        "cost_scenario": cost_scenario,
        "variant": variant,
        "reference_candidates": int(len(old)),
        "repaired_candidates": int(len(new)),
        "candidates_added": int(added.sum()),
        "candidates_removed": int(removed.sum()),
        "common_candidates": int(common_rows.sum()),
        "common_candidate_states_changed": int((common_rows & state_changed).sum()),
        "reference_fills": int(old_filled.sum()),
        "repaired_fills": int(new_filled.sum()),
        "fills_added": int((~old_filled & new_filled).sum()),
        "fills_removed": int((old_filled & ~new_filled).sum()),
        "reference_net_pnl_rs": float(old_filled_pnl.sum()),
        "repaired_net_pnl_rs": float(new_filled_pnl.sum()),
        "delta_net_pnl_rs_repaired_minus_reference": float(
            new_filled_pnl.sum() - old_filled_pnl.sum()
        ),
    }
    if not include_detail:
        return summary, pd.DataFrame()
    detail = paired.loc[state_changed].copy()
    detail.insert(0, "schema_version", CANDIDATE_STATE_DRIFT_SCHEMA_VERSION)
    detail.insert(0, "variant", variant)
    detail.insert(0, "cost_scenario", cost_scenario)
    detail.insert(0, "dataset", dataset)
    detail["state_changed"] = True
    detail["delta_net_pnl_rs_repaired_minus_reference"] = (
        pd.to_numeric(detail["net_pnl_rs_repaired"], errors="coerce").fillna(0.0)
        - pd.to_numeric(detail["net_pnl_rs_reference"], errors="coerce").fillna(0.0)
    )
    return summary, detail


def _reference_audit_path(
    reference_root: Path,
    *,
    dataset: str,
    variant: str,
    cost_scenario: str = "BASE_15BPS_0SLIP",
) -> Path:
    if cost_scenario == "BASE_15BPS_0SLIP":
        path = (
            reference_root
            / dataset.lower()
            / variant.lower()
            / "candidate_order_audit.csv"
        )
    else:
        path = (
            reference_root
            / "cost_stress"
            / dataset.lower()
            / cost_scenario.lower()
            / variant.lower()
            / "candidate_order_audit.csv"
        )
    if not path.is_file():
        raise FileNotFoundError(f"Reference audit is missing: {path}")
    return path


def _paired_candidate_changes(
    control: pd.DataFrame,
    challenger: pd.DataFrame,
    *,
    dataset: str,
    variant: str,
) -> pd.DataFrame:
    columns = [
        "candidate_id",
        "session_date",
        "setup_id",
        "symbol",
        "status",
        "filled",
        "net_return_pct",
        "net_pnl_rs",
    ]
    left = control[[column for column in columns if column in control.columns]].copy()
    right = challenger[[column for column in columns if column in challenger.columns]].copy()
    paired = left.merge(
        right,
        on=["candidate_id"],
        how="outer",
        suffixes=("_control", "_challenger"),
        indicator=True,
        validate="one_to_one",
    )
    paired.insert(0, "variant", variant)
    paired.insert(0, "dataset", dataset)
    for column in ("status_control", "status_challenger"):
        if column in paired:
            paired[column] = paired[column].fillna("SELECTION_REJECTED")
    changed = paired["_merge"].ne("both")
    if "status_control" in paired and "status_challenger" in paired:
        changed |= paired["status_control"].astype(str).ne(
            paired["status_challenger"].astype(str)
        )
    for column in ("net_pnl_rs", "net_return_pct"):
        left_name = f"{column}_control"
        right_name = f"{column}_challenger"
        if left_name in paired and right_name in paired:
            left_value = pd.to_numeric(paired[left_name], errors="coerce").fillna(0.0)
            right_value = pd.to_numeric(paired[right_name], errors="coerce").fillna(0.0)
            paired[f"delta_{column}"] = right_value - left_value
            changed |= ~np.isclose(left_value, right_value, rtol=1e-10, atol=1e-8)
    return paired.loc[changed].copy()


def _report_markdown(
    summary: pd.DataFrame,
    parity: Mapping[str, Any],
    cost_stress_summary: pd.DataFrame,
) -> str:
    historical = summary.loc[
        summary["dataset"].eq("HISTORICAL") & summary["period"].eq("FULL")
    ].copy()
    today = summary.loc[
        summary["dataset"].eq("TODAY") & summary["period"].eq("FULL")
    ].copy()
    columns = [
        "variant",
        "fills",
        "wins",
        "losses",
        "win_rate_pct",
        "profit_factor",
        "net_return_points",
        "net_pnl_rs",
        "max_daily_drawdown_points",
    ]
    stress_full = cost_stress_summary.loc[
        cost_stress_summary["dataset"].eq("HISTORICAL")
        & cost_stress_summary["period"].eq("FULL")
    ].copy()
    stress_columns = [
        "cost_scenario",
        "variant",
        "fills",
        "win_rate_pct",
        "profit_factor",
        "net_return_points",
        "net_pnl_rs",
        "max_daily_drawdown_points",
        "delta_net_return_points_vs_scenario_control",
        "delta_net_return_points_vs_base_economics",
    ]
    lines = [
        "# V6 isolated challenger replay",
        "",
        "Research-only; not a promotion decision.",
        "",
        "## Contract",
        "",
        f"- Historical window: {HISTORICAL_FROM_DAY} through {HISTORICAL_THROUGH_DAY}",
        f"- TRAIN/TEST split: {SPLIT_DAY}",
        f"- Today snapshot: {TODAY}",
        f"- Economics: {COST_BPS:g} bps cost, {SLIPPAGE_BPS:g} bps slippage",
        f"- EOD policy: {EOD_POLICY}",
        "- Mechanism: filter baseline candidates, rerank, then replay the full state machine",
        f"- CONTROL parity: {'PASS' if parity.get('passed') else 'FAIL'}",
        "",
        "## Historical FULL",
        "",
        historical[columns].to_markdown(index=False),
        "",
        "## Today",
        "",
        today[columns].to_markdown(index=False),
        "",
        "## Historical cost stress",
        "",
        stress_full[stress_columns].to_markdown(index=False),
        "",
        "## Interpretation limits",
        "",
        "- Results remain non-headline diagnostics under LAST_REAL_BAR_SENSITIVITY; exact source-completeness fields are preserved in the package manifest.",
        "- The historical universe and August futures OI are static, not rolling point-in-time inputs.",
        "- Today's causal research replay is not the paper-live readiness gate: the live gate may block a whole slot while the replay excludes incomplete symbol paths individually.",
        "- A2 was inspected after prior results; standalone A2 and A1+A2 runs are reported to expose its main effect and interaction, not to claim out-of-sample discovery.",
        "- No challenger is promotion-eligible without untouched prospective validation.",
        "",
    ]
    return "\n".join(lines)


def _reference_drift_markdown(
    metric_drift: pd.DataFrame,
    cost_metric_drift: pd.DataFrame,
    candidate_state_drift: pd.DataFrame,
    cost_candidate_state_drift: pd.DataFrame,
) -> str:
    base = metric_drift.loc[metric_drift["period"].eq("FULL")].copy()
    base_columns = [
        "dataset",
        "variant",
        "reference_fills",
        "repaired_fills",
        "delta_fills_repaired_minus_reference",
        "reference_profit_factor",
        "repaired_profit_factor",
        "delta_profit_factor_repaired_minus_reference",
        "reference_net_return_points",
        "repaired_net_return_points",
        "delta_net_return_points_repaired_minus_reference",
        "reference_net_pnl_rs",
        "repaired_net_pnl_rs",
        "delta_net_pnl_rs_repaired_minus_reference",
    ]
    stress = cost_metric_drift.loc[
        cost_metric_drift["dataset"].eq("HISTORICAL")
        & cost_metric_drift["period"].eq("FULL")
    ].copy()
    stress_columns = [
        "cost_scenario",
        "variant",
        "reference_profit_factor",
        "repaired_profit_factor",
        "delta_profit_factor_repaired_minus_reference",
        "reference_net_return_points",
        "repaired_net_return_points",
        "delta_net_return_points_repaired_minus_reference",
    ]
    state_columns = [
        "dataset",
        "variant",
        "candidates_added",
        "candidates_removed",
        "common_candidate_states_changed",
        "fills_added",
        "fills_removed",
        "delta_net_pnl_rs_repaired_minus_reference",
    ]
    cost_state_columns = [
        "dataset",
        "cost_scenario",
        "variant",
        "candidates_added",
        "candidates_removed",
        "common_candidate_states_changed",
        "fills_added",
        "fills_removed",
        "delta_net_pnl_rs_repaired_minus_reference",
    ]
    lines = [
        "# Repaired-data drift versus frozen v2",
        "",
        "Positive deltas mean the repaired replay is higher than the frozen-v2 replay.",
        "",
        "## Base economics, FULL",
        "",
        base[base_columns].to_markdown(index=False),
        "",
        "## Historical cost stress, FULL",
        "",
        stress[stress_columns].to_markdown(index=False),
        "",
        "## Base candidate-state drift",
        "",
        candidate_state_drift[state_columns].to_markdown(index=False),
        "",
        "## Cost-stress candidate-state drift",
        "",
        cost_candidate_state_drift[cost_state_columns].to_markdown(index=False),
        "",
    ]
    return "\n".join(lines)


def _atomic_write_text(path: Path, text: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(text, encoding="utf-8", newline="\n")
    os.replace(temporary, path)


def validate_package(path: Path | str) -> dict[str, Any]:
    root = Path(path).resolve()
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("Unsupported V6 challenger package schema")
    if manifest.get("research_only") is not True:
        raise ValueError("V6 challenger package must be research-only")
    if manifest.get("promotion_eligible") is not False:
        raise ValueError("V6 challenger package cannot be promotion-eligible")
    if manifest.get("registry_sha256") != canonical_sha256(registry_payload()):
        raise AssertionError("V6 challenger registry binding changed")
    artifacts = dict(manifest.get("artifacts", {}))
    for relative, record in artifacts.items():
        resolved = (root / relative).resolve()
        try:
            resolved.relative_to(root)
        except ValueError as exc:
            raise AssertionError(f"Artifact escapes package root: {relative}") from exc
        if Path(str(record.get("path", ""))).resolve() != resolved.resolve():
            raise AssertionError(f"Artifact path binding changed: {relative}")
        _require_artifact(dict(record), relative)
    summary = pd.read_csv(root / "comparison_summary.csv")
    expected_rows = len(CHALLENGERS) * (3 + 1)
    if len(summary) != expected_rows:
        raise AssertionError(
            f"Expected {expected_rows} comparison rows, observed {len(summary)}"
        )
    historical = summary.loc[
        summary["dataset"].eq("HISTORICAL") & summary["period"].eq("FULL")
    ]
    today = summary.loc[
        summary["dataset"].eq("TODAY") & summary["period"].eq("FULL")
    ]
    if set(historical["variant"]) != set(CHALLENGER_BY_NAME):
        raise AssertionError("Historical variant set is incomplete")
    if set(today["variant"]) != set(CHALLENGER_BY_NAME):
        raise AssertionError("Today variant set is incomplete")
    cost_stress = pd.read_csv(root / "cost_stress_summary.csv")
    expected_cost_rows = len(STRESS_VARIANT_NAMES) * len(COST_SCENARIOS) * 4
    if len(cost_stress) != expected_cost_rows:
        raise AssertionError(
            f"Expected {expected_cost_rows} cost-stress rows, observed {len(cost_stress)}"
        )
    if set(cost_stress["cost_scenario"]) != {
        item.scenario for item in COST_SCENARIOS
    }:
        raise AssertionError("Cost-stress scenario set is incomplete")
    if set(cost_stress["variant"]) != set(STRESS_VARIANT_NAMES):
        raise AssertionError("Cost-stress variant set is incomplete")
    fresh_mode = manifest.get("data_mode") == "REPAIRED_FRESH_IMMUTABLE_SNAPSHOTS"
    parity_path = root / (
        "control_parity.json" if fresh_mode else "historical_control_parity.json"
    )
    parity = json.loads(parity_path.read_text(encoding="utf-8"))
    if parity.get("passed") is not True:
        raise AssertionError("CONTROL parity is not proven")
    fresh_validation: dict[str, Any] = {}
    if fresh_mode:
        if parity.get("parity_kind") != "DIRECT_STRICT_V6_FRESH_CACHE_CONTROL":
            raise AssertionError("Fresh package does not prove direct strict-V6 parity")
        for dataset in ("historical", "today"):
            if dict(parity.get(dataset, {})).get("passed") is not True:
                raise AssertionError(f"Fresh {dataset} CONTROL parity is not proven")
            direct_record = dict(dict(parity.get("direct_audits", {})).get(dataset, {}))
            direct_path = _require_artifact(
                direct_record, f"fresh {dataset} direct CONTROL audit"
            )
            try:
                direct_path.relative_to(root)
            except ValueError as exc:
                raise AssertionError("Direct parity audit escapes package root") from exc

        source_bindings = dict(manifest.get("source_bindings", {}))
        required_source_bindings = {
            "historical_cache_manifest",
            "today_cache_manifest",
            "historical_snapshot_manifest",
            "today_snapshot_manifest",
            "reference_package_manifest",
            "rejected_today_snapshot_manifest",
            "engine_source_archive",
            "strict_launcher_source_archive",
            "research_runner_source_archive",
        }
        missing_source_bindings = sorted(
            required_source_bindings - set(source_bindings)
        )
        if missing_source_bindings:
            raise AssertionError(
                f"Fresh source bindings are incomplete: {missing_source_bindings}"
            )
        for label in required_source_bindings:
            _require_artifact(dict(source_bindings[label]), label)

        run_contract = json.loads((root / "run_contract.json").read_text(encoding="utf-8"))
        if run_contract.get("schema_version") != RUN_CONTRACT_SCHEMA_VERSION:
            raise ValueError("Fresh run contract schema changed")
        contract_inputs = dict(run_contract.get("inputs", {}))
        for label in (
            "historical_snapshot_manifest",
            "today_snapshot_manifest",
            "reference_package_manifest",
            "rejected_today_snapshot_manifest",
        ):
            if canonical_sha256(contract_inputs.get(label, {})) != canonical_sha256(
                source_bindings[label]
            ):
                raise AssertionError(f"Fresh run-contract input changed: {label}")
        for contract_key, binding_key in (
            ("engine_source", "engine_source_archive"),
            ("strict_launcher_source", "strict_launcher_source_archive"),
            ("research_runner_source", "research_runner_source_archive"),
        ):
            original = dict(run_contract.get(contract_key, {}))
            archived = dict(source_bindings[binding_key])
            if (
                original.get("sha256") != archived.get("sha256")
                or original.get("size") != archived.get("size")
            ):
                raise AssertionError(f"Fresh source archive changed: {contract_key}")

        source_summary = dict(manifest.get("source_contract_summary", {}))
        seed_binding_keys = {
            "seed_source_run_contract",
            "seed_historical_cache_manifest",
            "seed_today_cache_manifest",
        }
        observed_seed_keys = seed_binding_keys & set(source_bindings)
        if observed_seed_keys and observed_seed_keys != seed_binding_keys:
            raise AssertionError("Fresh cache-seed bindings are incomplete")
        if observed_seed_keys:
            for label in seed_binding_keys:
                _require_artifact(dict(source_bindings[label]), label)
            seed_provenance = json.loads(
                (root / "seed_cache_provenance.json").read_text(encoding="utf-8")
            )
            if (
                seed_provenance.get("schema_version")
                != SEED_CACHE_SCHEMA_VERSION
                or seed_provenance.get(
                    "cache_bytes_replayed_by_engine_contract_validation"
                )
                is not True
            ):
                raise AssertionError("Fresh cache-seed provenance changed")
            if canonical_sha256(seed_provenance) != canonical_sha256(
                source_summary.get("cache_seed_recovery", {})
            ):
                raise AssertionError("Fresh cache-seed summary is missing")
            _verify_seed_cache_provenance(
                seed_package=Path(str(seed_provenance["source_package"])),
                target=root,
                payload=seed_provenance,
                run_contract=run_contract,
                returned_caches={
                    "historical": (
                        json.loads(
                            Path(
                                str(
                                    source_bindings[
                                        "historical_cache_manifest"
                                    ]["path"]
                                )
                            ).read_text(encoding="utf-8")
                        ),
                        Path(
                            str(
                                source_bindings["historical_cache_manifest"][
                                    "path"
                                ]
                            )
                        ),
                    ),
                    "today": (
                        json.loads(
                            Path(
                                str(
                                    source_bindings["today_cache_manifest"][
                                        "path"
                                    ]
                                )
                            ).read_text(encoding="utf-8")
                        ),
                        Path(
                            str(
                                source_bindings["today_cache_manifest"]["path"]
                            )
                        ),
                    ),
                },
            )
        historical_universe = dict(source_summary.get("historical_universe", {}))
        today_universe = dict(source_summary.get("today_universe", {}))
        expected_identities = (
            (historical_universe, "2026-08-11", "26AUG", 208),
            (today_universe, "2026-08-27", "26SEP", 210),
        )
        for universe, master_date, contract, mapped_count in expected_identities:
            if (
                str(universe.get("master_date", "")) != master_date
                or str(universe.get("contract_month_filter", "")).upper()
                != contract
                or int(universe.get("mapped_stock_futures", -1)) != mapped_count
            ):
                raise AssertionError("Fresh source universe identity changed")
        if source_summary.get("rejected_today_snapshot_recorded") is not True:
            raise AssertionError("Rejected AUG today snapshot is not recorded")

        completion_paths = sorted(root.rglob("completion.json"))
        if len(completion_paths) != 32:
            raise AssertionError(
                f"Expected 32 completed full-state replays, observed "
                f"{len(completion_paths)}"
            )
        for completion_path in completion_paths:
            completion = json.loads(
                completion_path.read_text(encoding="utf-8")
            )
            if (
                completion.get("schema_version")
                != REPLAY_COMPLETION_SCHEMA_VERSION
                or completion.get("complete") is not True
            ):
                raise AssertionError(
                    f"Replay completion marker changed: {completion_path}"
                )
            for label, record in dict(completion.get("artifacts", {})).items():
                artifact_path = _require_artifact(
                    dict(record), f"replay completion {label}"
                )
                try:
                    artifact_path.relative_to(root)
                except ValueError as exc:
                    raise AssertionError(
                        "Replay completion artifact escapes package root"
                    ) from exc

        def require_drift_table(
            filename: str,
            *,
            schema_version: str,
            keys: Sequence[str],
            expected_keys: set[tuple[Any, ...]] | None = None,
            expected_rows: int | None = None,
        ) -> pd.DataFrame:
            frame = pd.read_csv(root / filename)
            if "schema_version" not in frame.columns or not frame[
                "schema_version"
            ].astype(str).eq(schema_version).all():
                raise AssertionError(f"Drift schema changed: {filename}")
            if frame.duplicated(list(keys)).any():
                raise AssertionError(f"Drift keys are not unique: {filename}")
            if expected_rows is not None and len(frame) != expected_rows:
                raise AssertionError(
                    f"Expected {expected_rows} rows in {filename}, observed {len(frame)}"
                )
            if expected_keys is not None:
                observed_keys = set(frame[list(keys)].itertuples(index=False, name=None))
                if observed_keys != expected_keys:
                    raise AssertionError(f"Drift key set changed: {filename}")
            return frame

        comparison_keys = set(
            summary[["dataset", "period", "variant"]].itertuples(
                index=False, name=None
            )
        )
        metric_drift = require_drift_table(
            "reference_metric_drift.csv",
            schema_version=METRIC_DRIFT_SCHEMA_VERSION,
            keys=("dataset", "period", "variant"),
            expected_keys=comparison_keys,
            expected_rows=32,
        )
        cost_keys = set(
            cost_stress[
                ["dataset", "period", "cost_scenario", "variant"]
            ].itertuples(index=False, name=None)
        )
        cost_metric_drift = require_drift_table(
            "reference_cost_stress_metric_drift.csv",
            schema_version=COST_METRIC_DRIFT_SCHEMA_VERSION,
            keys=("dataset", "period", "cost_scenario", "variant"),
            expected_keys=cost_keys,
            expected_rows=48,
        )
        daywise_current = pd.read_csv(root / "daywise.csv")
        daywise_keys = set(
            daywise_current[["dataset", "variant", "session_date"]].itertuples(
                index=False, name=None
            )
        )
        daily_drift = require_drift_table(
            "reference_daywise_drift.csv",
            schema_version=DAYWISE_DRIFT_SCHEMA_VERSION,
            keys=("dataset", "variant", "session_date"),
            expected_keys=daywise_keys,
            expected_rows=480,
        )
        base_state_keys = {
            (dataset, variant)
            for dataset in ("HISTORICAL", "TODAY")
            for variant in CHALLENGER_BY_NAME
        }
        candidate_state = require_drift_table(
            "reference_candidate_state_drift_summary.csv",
            schema_version=CANDIDATE_STATE_DRIFT_SCHEMA_VERSION,
            keys=("dataset", "variant"),
            expected_keys=base_state_keys,
            expected_rows=16,
        )
        detail = pd.read_csv(root / "reference_candidate_state_drift_detail.csv")
        if "schema_version" not in detail.columns or (
            not detail.empty
            and not detail["schema_version"].astype(str).eq(
                CANDIDATE_STATE_DRIFT_SCHEMA_VERSION
            ).all()
        ):
            raise AssertionError("Candidate-state drift detail schema changed")
        cost_state_keys = {
            (dataset, scenario.scenario, variant)
            for dataset in ("HISTORICAL", "TODAY")
            for scenario in COST_SCENARIOS
            for variant in STRESS_VARIANT_NAMES
        }
        cost_candidate_state = require_drift_table(
            "reference_cost_stress_candidate_state_drift_summary.csv",
            schema_version=COST_CANDIDATE_STATE_DRIFT_SCHEMA_VERSION,
            keys=("dataset", "cost_scenario", "variant"),
            expected_keys=cost_state_keys,
            expected_rows=24,
        )
        for label, frame in (
            ("metric", metric_drift),
            ("cost metric", cost_metric_drift),
            ("daywise", daily_drift),
        ):
            if not frame["row_presence"].astype(str).eq("both").all():
                raise AssertionError(f"Reference/repaired {label} keys differ")
        fresh_validation = {
            "fresh_direct_control_parity": True,
            "reference_metric_drift_rows": int(len(metric_drift)),
            "reference_cost_metric_drift_rows": int(len(cost_metric_drift)),
            "reference_daywise_drift_rows": int(len(daily_drift)),
            "reference_candidate_state_rows": int(len(candidate_state)),
            "reference_cost_candidate_state_rows": int(len(cost_candidate_state)),
        }
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "status": "PASS",
        "artifacts_verified": len(artifacts),
        "comparison_rows": int(len(summary)),
        "historical_variants": int(len(historical)),
        "today_variants": int(len(today)),
        "cost_stress_rows": int(len(cost_stress)),
        "historical_control_parity": True,
        **fresh_validation,
    }


def execute(
    *,
    historical_provenance: Path | None,
    historical_snapshot: Path | None,
    today_snapshot: Path,
    reference_package: Path | None,
    rejected_today_snapshot: Path | None,
    seed_cache_package: Path | None,
    output_dir: Path,
    command: Sequence[str],
    resume: bool = False,
) -> Path:
    validate_registry()
    strict.configure_engine()
    if (historical_provenance is None) == (historical_snapshot is None):
        raise ValueError(
            "Supply exactly one of historical_provenance or historical_snapshot"
        )
    if historical_snapshot is not None and reference_package is None:
        raise ValueError(
            "Fresh repaired-snapshot mode requires --reference-package"
        )
    rejected_snapshot_identity: dict[str, Any] | None = None
    if rejected_today_snapshot is not None:
        rejected_snapshot, _ = _snapshot_identity(
            rejected_today_snapshot.resolve(),
            expected_master_date="2026-08-11",
            expected_contract_month_filter="26AUG",
            expected_mapped_stock_futures=208,
        )
        rejected_universe = dict(rejected_snapshot["universe"])
        rejected_snapshot_identity = {
            "rejection_reason": "TODAY_SLOT_REQUIRES_2026-08-27_26SEP_210_MAPPED",
            "observed_master_date": rejected_universe["master_date"],
            "observed_contract_month_filter": rejected_universe[
                "contract_month_filter"
            ],
            "observed_mapped_stock_futures": rejected_universe[
                "mapped_stock_futures"
            ],
            "observed_snapshot_fingerprint": rejected_snapshot.get(
                "snapshot_fingerprint"
            ),
        }
    run_contract = _run_contract_payload(
        historical_provenance=historical_provenance,
        historical_snapshot=historical_snapshot,
        today_snapshot=today_snapshot,
        reference_package=reference_package,
        rejected_today_snapshot=rejected_today_snapshot,
        seed_cache_package=seed_cache_package,
    )
    target = output_dir.resolve()
    if seed_cache_package is not None:
        seed_root = seed_cache_package.resolve()
        try:
            target.relative_to(seed_root)
        except ValueError:
            pass
        else:
            raise ValueError("Output target cannot be inside the cache-seed package")
        try:
            seed_root.relative_to(target)
        except ValueError:
            pass
        else:
            raise ValueError("Cache-seed package cannot be inside the output target")
    if target.exists() and not resume:
        raise FileExistsError(f"Output directory already exists: {target}")
    if resume and (target / "manifest.json").is_file():
        raise FileExistsError("A completed package cannot be resumed")
    target.mkdir(parents=True, exist_ok=resume)
    _establish_run_contract(target, run_contract, resume=resume)
    archived_sources = _archive_sources(target, run_contract, resume=resume)
    cache_seed_provenance: dict[str, Any] | None = None
    if seed_cache_package is not None:
        if resume:
            seed_record_path = target / "seed_cache_provenance.json"
            if not seed_record_path.is_file():
                raise FileNotFoundError(
                    f"Resume cache-seed provenance is missing: {seed_record_path}"
                )
            cache_seed_provenance = json.loads(
                seed_record_path.read_text(encoding="utf-8")
            )
        else:
            cache_seed_provenance = _seed_validated_caches(
                seed_cache_package.resolve(), target
            )
        _verify_seed_cache_provenance(
            seed_package=seed_cache_package.resolve(),
            target=target,
            payload=cache_seed_provenance,
            run_contract=run_contract,
        )

    historical_provenance_payload: dict[str, Any] | None = None
    frozen_historical_audit: pd.DataFrame | None = None
    if historical_snapshot is not None:
        historical_inputs = _build_snapshot_inputs(
            historical_snapshot.resolve(),
            target / "historical_baseline_cache",
            from_day=HISTORICAL_FROM_DAY,
            through_day=HISTORICAL_THROUGH_DAY,
            run_label="REPAIRED_HISTORICAL_AUG_FULL59",
            expected_master_date="2026-08-11",
            expected_contract_month_filter="26AUG",
            expected_mapped_stock_futures=208,
        )
        (
            historical_candidates,
            historical_paths,
            historical_coverage,
            historical_manifest,
            historical_manifest_path,
        ) = historical_inputs
    else:
        assert historical_provenance is not None
        historical = _read_frozen_historical_inputs(
            historical_provenance.resolve()
        )
        (
            historical_candidates,
            historical_paths,
            historical_coverage,
            historical_manifest,
            historical_manifest_path,
            historical_provenance_payload,
            frozen_historical_audit,
        ) = historical
    today_inputs = _build_snapshot_inputs(
        today_snapshot.resolve(),
        target / "today_baseline_cache",
        from_day=TODAY,
        through_day=TODAY,
        run_label="REPAIRED_TODAY_ROLLOVER_DIAGNOSTIC",
        expected_master_date="2026-08-27",
        expected_contract_month_filter="26SEP",
        expected_mapped_stock_futures=210,
    )
    (
        today_candidates,
        today_paths,
        today_coverage,
        today_manifest,
        today_manifest_path,
    ) = today_inputs
    if seed_cache_package is not None:
        assert cache_seed_provenance is not None
        _verify_seed_cache_provenance(
            seed_package=seed_cache_package.resolve(),
            target=target,
            payload=cache_seed_provenance,
            run_contract=run_contract,
            returned_caches={
                "historical": (historical_manifest, historical_manifest_path),
                "today": (today_manifest, today_manifest_path),
            },
        )
    policy = engine.entry_policy_for_variant(
        "VS",
        cost_bps=COST_BPS,
        slippage_bps=SLIPPAGE_BPS,
        square_off=SQUARE_OFF,
        eod_policy=EOD_POLICY,
    )

    replay_results: dict[tuple[str, str], dict[str, Any]] = {}
    summary_rows: list[dict[str, Any]] = []
    daywise_parts: list[pd.DataFrame] = []
    for dataset, values in (
        (
            "HISTORICAL",
            (
                historical_candidates,
                historical_paths,
                historical_coverage,
                historical_manifest,
                SPLIT_DAY,
            ),
        ),
        (
            "TODAY",
            (
                today_candidates,
                today_paths,
                today_coverage,
                today_manifest,
                None,
            ),
        ),
    ):
        candidates, paths, coverage, manifest, split = values
        for spec in CHALLENGERS:
            result = _replay_one(
                dataset=dataset,
                spec=spec,
                candidates=candidates,
                minute_paths=paths,
                coverage=coverage,
                manifest=manifest,
                policy=policy,
                output_dir=target,
                split_day=split,
                resume=resume,
            )
            replay_results[(dataset, spec.variant)] = result
            periods = ("FULL", "TRAIN", "TEST") if dataset == "HISTORICAL" else ("FULL",)
            for period in periods:
                summary_rows.append(
                    _period_row(
                        result["audit"],
                        result["daily"],
                        dataset=dataset,
                        variant=spec.variant,
                        period=period,
                    )
                )
            daywise = result["daily"].copy()
            daywise.insert(0, "variant", spec.variant)
            daywise.insert(0, "dataset", dataset)
            daywise_parts.append(daywise)

    cost_stress_results: dict[tuple[str, str, str], dict[str, Any]] = {}
    cost_stress_summary_rows: list[dict[str, Any]] = []
    cost_stress_daywise_parts: list[pd.DataFrame] = []
    for dataset, values in (
        (
            "HISTORICAL",
            (
                historical_candidates,
                historical_paths,
                historical_coverage,
                historical_manifest,
                SPLIT_DAY,
            ),
        ),
        (
            "TODAY",
            (
                today_candidates,
                today_paths,
                today_coverage,
                today_manifest,
                None,
            ),
        ),
    ):
        candidates, paths, coverage, manifest, split = values
        for scenario in COST_SCENARIOS:
            for variant_name in STRESS_VARIANT_NAMES:
                spec = CHALLENGER_BY_NAME[variant_name]
                if scenario.scenario == "BASE_15BPS_0SLIP":
                    result = replay_results[(dataset, variant_name)]
                else:
                    result = _replay_cost_stress_one(
                        dataset=dataset,
                        spec=spec,
                        scenario=scenario,
                        candidates=candidates,
                        minute_paths=paths,
                        coverage=coverage,
                        manifest=manifest,
                        output_dir=target,
                        split_day=split,
                        resume=resume,
                    )
                cost_stress_results[(dataset, scenario.scenario, variant_name)] = result
                periods = (
                    ("FULL", "TRAIN", "TEST")
                    if dataset == "HISTORICAL"
                    else ("FULL",)
                )
                for period in periods:
                    row = _period_row(
                        result["audit"],
                        result["daily"],
                        dataset=dataset,
                        variant=variant_name,
                        period=period,
                    )
                    row["cost_scenario"] = scenario.scenario
                    row["cost_bps"] = scenario.cost_bps
                    row["slippage_bps"] = scenario.slippage_bps
                    cost_stress_summary_rows.append(row)
                stress_daily = result["daily"].copy()
                stress_daily.insert(0, "slippage_bps", scenario.slippage_bps)
                stress_daily.insert(0, "cost_bps", scenario.cost_bps)
                stress_daily.insert(0, "cost_scenario", scenario.scenario)
                stress_daily.insert(0, "variant", variant_name)
                stress_daily.insert(0, "dataset", dataset)
                cost_stress_daywise_parts.append(stress_daily)

    if historical_snapshot is not None:
        parity = _fresh_control_parity_bundle(
            historical_candidates=historical_candidates,
            historical_paths=historical_paths,
            historical_manifest=historical_manifest,
            historical_coverage=historical_coverage,
            today_candidates=today_candidates,
            today_paths=today_paths,
            today_manifest=today_manifest,
            today_coverage=today_coverage,
            replay_results=replay_results,
            policy=policy,
            target=target,
        )
    else:
        assert frozen_historical_audit is not None
        assert historical_provenance_payload is not None
        parity = _control_parity(
            replay_results[("HISTORICAL", "CONTROL")]["audit"],
            frozen_historical_audit,
            historical_provenance_payload,
        )
    parity_filename = (
        "control_parity.json"
        if historical_snapshot is not None
        else "historical_control_parity.json"
    )
    common.atomic_write_json(target / parity_filename, parity)

    summary = _delta_rows(pd.DataFrame(summary_rows))
    common.atomic_write_csv(summary, target / "comparison_summary.csv")
    daywise = pd.concat(daywise_parts, ignore_index=True)
    controls = daywise.loc[daywise["variant"].eq("CONTROL"), [
        "dataset", "session_date", "net_return_pct", "net_pnl_rs", "fills"
    ]].rename(columns={
        "net_return_pct": "control_net_return_pct",
        "net_pnl_rs": "control_net_pnl_rs",
        "fills": "control_fills",
    })
    daywise = daywise.merge(
        controls,
        on=["dataset", "session_date"],
        how="left",
        validate="many_to_one",
    )
    daywise["delta_net_return_pct_vs_control"] = (
        daywise["net_return_pct"] - daywise["control_net_return_pct"]
    )
    daywise["delta_net_pnl_rs_vs_control"] = (
        daywise["net_pnl_rs"] - daywise["control_net_pnl_rs"]
    )
    daywise["delta_fills_vs_control"] = daywise["fills"] - daywise["control_fills"]
    common.atomic_write_csv(daywise, target / "daywise.csv")

    cost_stress_summary = _cost_stress_delta_rows(
        pd.DataFrame(cost_stress_summary_rows)
    )
    common.atomic_write_csv(
        cost_stress_summary, target / "cost_stress_summary.csv"
    )
    cost_stress_daywise = pd.concat(
        cost_stress_daywise_parts, ignore_index=True
    )
    base_daily = cost_stress_daywise.loc[
        cost_stress_daywise["cost_scenario"].eq("BASE_15BPS_0SLIP"),
        [
            "dataset",
            "variant",
            "session_date",
            "net_return_pct",
            "net_pnl_rs",
            "fills",
        ],
    ].rename(
        columns={
            "net_return_pct": "base_net_return_pct",
            "net_pnl_rs": "base_net_pnl_rs",
            "fills": "base_fills",
        }
    )
    cost_stress_daywise = cost_stress_daywise.merge(
        base_daily,
        on=["dataset", "variant", "session_date"],
        how="left",
        validate="many_to_one",
    )
    cost_stress_daywise["delta_net_return_pct_vs_base_economics"] = (
        cost_stress_daywise["net_return_pct"]
        - cost_stress_daywise["base_net_return_pct"]
    )
    cost_stress_daywise["delta_net_pnl_rs_vs_base_economics"] = (
        cost_stress_daywise["net_pnl_rs"]
        - cost_stress_daywise["base_net_pnl_rs"]
    )
    common.atomic_write_csv(
        cost_stress_daywise, target / "cost_stress_daywise.csv"
    )

    changes = []
    for dataset in ("HISTORICAL", "TODAY"):
        control_audit = replay_results[(dataset, "CONTROL")]["audit"]
        for spec in CHALLENGERS[1:]:
            changes.append(
                _paired_candidate_changes(
                    control_audit,
                    replay_results[(dataset, spec.variant)]["audit"],
                    dataset=dataset,
                    variant=spec.variant,
                )
            )
    paired_changes = pd.concat(changes, ignore_index=True) if changes else pd.DataFrame()
    common.atomic_write_csv(paired_changes, target / "paired_candidate_changes.csv")

    reference_validation: dict[str, Any] | None = None
    if reference_package is not None:
        reference_root = reference_package.resolve()
        reference_validation = validate_package(reference_root)
        reference_summary = pd.read_csv(reference_root / "comparison_summary.csv")
        reference_cost_summary = pd.read_csv(
            reference_root / "cost_stress_summary.csv"
        )
        _validate_cost_scenario_economics(
            cost_stress_summary, label="repaired"
        )
        _validate_cost_scenario_economics(
            reference_cost_summary, label="reference"
        )
        reference_daywise = pd.read_csv(reference_root / "daywise.csv")
        metric_drift = _metric_drift(
            summary,
            reference_summary,
            keys=("dataset", "period", "variant"),
        )
        cost_metric_drift = _metric_drift(
            cost_stress_summary,
            reference_cost_summary,
            keys=("dataset", "period", "cost_scenario", "variant"),
        )
        common.atomic_write_csv(
            metric_drift, target / "reference_metric_drift.csv"
        )
        common.atomic_write_csv(
            cost_metric_drift,
            target / "reference_cost_stress_metric_drift.csv",
        )

        day_keys = ["dataset", "variant", "session_date"]
        day_metrics = ["candidates", "fills", "net_return_pct", "net_pnl_rs"]
        reference_daily = reference_daywise[[*day_keys, *day_metrics]].rename(
            columns={item: f"reference_{item}" for item in day_metrics}
        )
        repaired_daily = daywise[[*day_keys, *day_metrics]].rename(
            columns={item: f"repaired_{item}" for item in day_metrics}
        )
        reference_daily["session_date"] = pd.to_datetime(
            reference_daily["session_date"], errors="raise"
        ).dt.strftime("%Y-%m-%d")
        repaired_daily["session_date"] = pd.to_datetime(
            repaired_daily["session_date"], errors="raise"
        ).dt.strftime("%Y-%m-%d")
        daily_drift = reference_daily.merge(
            repaired_daily,
            on=day_keys,
            how="outer",
            validate="one_to_one",
            indicator="row_presence",
        )
        daily_drift.insert(0, "schema_version", DAYWISE_DRIFT_SCHEMA_VERSION)
        daily_match_columns = []
        for metric in day_metrics:
            reference_values = pd.to_numeric(
                daily_drift[f"reference_{metric}"], errors="coerce"
            )
            repaired_values = pd.to_numeric(
                daily_drift[f"repaired_{metric}"], errors="coerce"
            )
            match_column = f"{metric}_matches"
            daily_match_columns.append(match_column)
            matches = _numeric_match(reference_values, repaired_values)
            daily_drift[match_column] = matches
            delta = repaired_values - reference_values
            daily_drift[f"delta_{metric}_repaired_minus_reference"] = delta.mask(
                matches & (~np.isfinite(delta)), 0.0
            )
        daily_drift["row_matches"] = (
            daily_drift["row_presence"].astype(str).eq("both")
            & daily_drift[daily_match_columns].all(axis=1)
        )
        common.atomic_write_csv(
            daily_drift, target / "reference_daywise_drift.csv"
        )

        audit_drift_summaries: list[dict[str, Any]] = []
        audit_drift_details: list[pd.DataFrame] = []
        for dataset in ("HISTORICAL", "TODAY"):
            for spec in CHALLENGERS:
                reference_audit = pd.read_csv(
                    _reference_audit_path(
                        reference_root,
                        dataset=dataset,
                        variant=spec.variant,
                    )
                )
                drift_summary, drift_detail = _reference_audit_drift(
                    reference_audit,
                    replay_results[(dataset, spec.variant)]["audit"],
                    dataset=dataset,
                    variant=spec.variant,
                )
                audit_drift_summaries.append(drift_summary)
                audit_drift_details.append(drift_detail)
        candidate_state_drift = pd.DataFrame(audit_drift_summaries)
        common.atomic_write_csv(
            candidate_state_drift,
            target / "reference_candidate_state_drift_summary.csv",
        )
        common.atomic_write_csv(
            pd.concat(audit_drift_details, ignore_index=True),
            target / "reference_candidate_state_drift_detail.csv",
        )

        cost_trade_drift_summaries: list[dict[str, Any]] = []
        for dataset in ("HISTORICAL", "TODAY"):
            for scenario in COST_SCENARIOS:
                for variant_name in STRESS_VARIANT_NAMES:
                    reference_audit = pd.read_csv(
                        _reference_audit_path(
                            reference_root,
                            dataset=dataset,
                            variant=variant_name,
                            cost_scenario=scenario.scenario,
                        )
                    )
                    drift_summary, _ = _reference_audit_drift(
                        reference_audit,
                        cost_stress_results[
                            (dataset, scenario.scenario, variant_name)
                        ]["audit"],
                        dataset=dataset,
                        variant=variant_name,
                        cost_scenario=scenario.scenario,
                        include_detail=False,
                        cost_comparison=True,
                    )
                    cost_trade_drift_summaries.append(drift_summary)
        cost_candidate_state_drift = pd.DataFrame(cost_trade_drift_summaries)
        common.atomic_write_csv(
            cost_candidate_state_drift,
            target / "reference_cost_stress_candidate_state_drift_summary.csv",
        )
        _atomic_write_text(
            target / "reference_drift_report.md",
            _reference_drift_markdown(
                metric_drift,
                cost_metric_drift,
                candidate_state_drift,
                cost_candidate_state_drift,
            ),
        )

    today_trades = []
    for spec in CHALLENGERS:
        filled = _closed(replay_results[("TODAY", spec.variant)]["audit"])
        if filled.empty:
            continue
        filled = filled.copy()
        filled["research_variant"] = spec.variant
        today_trades.append(filled)
    common.atomic_write_csv(
        pd.concat(today_trades, ignore_index=True) if today_trades else pd.DataFrame(),
        target / "today_trades.csv",
    )
    today_cost_stress_trades = []
    for scenario in COST_SCENARIOS:
        for variant_name in STRESS_VARIANT_NAMES:
            filled = _closed(
                cost_stress_results[("TODAY", scenario.scenario, variant_name)][
                    "audit"
                ]
            )
            if filled.empty:
                continue
            filled = filled.copy()
            filled["research_variant"] = variant_name
            filled["cost_scenario"] = scenario.scenario
            filled["scenario_cost_bps"] = scenario.cost_bps
            filled["scenario_slippage_bps"] = scenario.slippage_bps
            today_cost_stress_trades.append(filled)
    common.atomic_write_csv(
        (
            pd.concat(today_cost_stress_trades, ignore_index=True)
            if today_cost_stress_trades
            else pd.DataFrame()
        ),
        target / "today_cost_stress_trades.csv",
    )
    _atomic_write_text(
        target / "report.md",
        _report_markdown(summary, parity, cost_stress_summary),
    )

    if seed_cache_package is not None:
        assert cache_seed_provenance is not None
        _verify_seed_cache_provenance(
            seed_package=seed_cache_package.resolve(),
            target=target,
            payload=cache_seed_provenance,
            run_contract=run_contract,
            returned_caches={
                "historical": (historical_manifest, historical_manifest_path),
                "today": (today_manifest, today_manifest_path),
            },
        )
    final_run_contract = _run_contract_payload(
        historical_provenance=historical_provenance,
        historical_snapshot=historical_snapshot,
        today_snapshot=today_snapshot,
        reference_package=reference_package,
        rejected_today_snapshot=rejected_today_snapshot,
        seed_cache_package=seed_cache_package,
    )
    if canonical_sha256(final_run_contract) != canonical_sha256(run_contract):
        raise RuntimeError("An immutable input manifest or source file changed during run")
    contract_inputs = dict(run_contract["inputs"])
    source_bindings = {
        "historical_cache_manifest": _artifact_record(historical_manifest_path),
        "today_snapshot_manifest": dict(
            contract_inputs["today_snapshot_manifest"]
        ),
        "today_cache_manifest": _artifact_record(today_manifest_path),
        "engine_source_archive": archived_sources["engine_source"],
        "strict_launcher_source_archive": archived_sources[
            "strict_launcher_source"
        ],
        "research_runner_source_archive": archived_sources[
            "research_runner_source"
        ],
    }
    if historical_provenance is not None:
        source_bindings["historical_provenance"] = dict(
            contract_inputs["historical_provenance"]
        )
    if historical_snapshot is not None:
        source_bindings["historical_snapshot_manifest"] = dict(
            contract_inputs["historical_snapshot_manifest"]
        )
    if reference_package is not None:
        source_bindings["reference_package_manifest"] = dict(
            contract_inputs["reference_package_manifest"]
        )
    if rejected_today_snapshot is not None:
        source_bindings["rejected_today_snapshot_manifest"] = dict(
            contract_inputs["rejected_today_snapshot_manifest"]
        )
    if seed_cache_package is not None:
        source_bindings["seed_source_run_contract"] = dict(
            contract_inputs["seed_source_run_contract"]
        )
        source_bindings["seed_historical_cache_manifest"] = dict(
            contract_inputs["seed_historical_cache_manifest"]
        )
        source_bindings["seed_today_cache_manifest"] = dict(
            contract_inputs["seed_today_cache_manifest"]
        )
    historical_completeness = _coverage_summary(
        historical_coverage, historical_manifest
    )
    today_completeness = _coverage_summary(today_coverage, today_manifest)
    historical_universe = dict(historical_manifest.get("universe", {}))
    today_universe = dict(today_manifest.get("universe", {}))
    limitations = [
        "STATIC_LATER_DATED_UNIVERSE_SURVIVORSHIP_RESEARCH",
        "STATIC_FUTURES_OI_NOT_POINT_IN_TIME_ROLLING",
        "LEGACY_EQUITY_1M_ROW_LINEAGE_UNPROVEN",
        "LAST_REAL_BAR_SENSITIVITY_NOT_HEADLINE",
        "TODAY_REPLAY_IS_NOT_THE_PAPER_LIVE_READINESS_GATE",
        "POST_SELECTION_RESEARCH_REQUIRES_PROSPECTIVE_VALIDATION",
    ]
    if not bool(historical_completeness["headline_source_complete"]):
        limitations.append("HISTORICAL_SOURCE_SLOT_COVERAGE_INCOMPLETE")
    if not bool(today_completeness["headline_source_complete"]):
        limitations.append("TODAY_SOURCE_SLOT_COVERAGE_INCOMPLETE")
    if rejected_today_snapshot is not None:
        limitations.append(
            "REJECTED_SUPPLIED_TODAY_SNAPSHOT_WAS_26AUG_NOT_26SEP"
        )
    if seed_cache_package is not None:
        limitations.append(
            "CACHE_BYTES_RECOVERED_FROM_FAILED_DOWNSTREAM_RESEARCH_RUN_AND_REVALIDATED"
        )
    validation = {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "status": "PRE_MANIFEST_PASS",
        "historical_control_parity": parity,
        "variant_count": len(CHALLENGERS),
        "historical_replays": len(CHALLENGERS),
        "today_replays": len(CHALLENGERS),
        "cost_stress_full_state_replays": (
            2 * 2 * len(STRESS_VARIANT_NAMES)
        ),
        "full_state_machine_replay": True,
        "selection_applied_before_rerank": True,
        "selection_applied_before_confirmation_entry_portfolio": True,
        "fresh_snapshot_mode": historical_snapshot is not None,
        "reference_package_validation": reference_validation,
        "reference_drift_generated": reference_package is not None,
        "historical_source_completeness": historical_completeness,
        "today_source_completeness": today_completeness,
    }
    common.atomic_write_json(target / "validation.json", validation)

    artifacts: dict[str, dict[str, Any]] = {}
    for path in sorted(
        (candidate for candidate in target.rglob("*") if candidate.is_file()),
        key=lambda item: item.relative_to(target).as_posix(),
    ):
        relative = path.relative_to(target).as_posix()
        if relative == "manifest.json":
            continue
        artifacts[relative] = _artifact_record(path)
    package_manifest = {
        "schema_version": SCHEMA_VERSION,
        "created_at_ist": common.now_ist().isoformat(timespec="microseconds"),
        "research_only": True,
        "promotion_eligible": False,
        "decision": "COMPLETED_RESEARCH_ONLY_NOT_PROMOTION",
        "data_mode": (
            "REPAIRED_FRESH_IMMUTABLE_SNAPSHOTS"
            if historical_snapshot is not None
            else "FROZEN_HISTORICAL_CACHE_PLUS_TODAY_SNAPSHOT"
        ),
        "command": list(command),
        "registry": registry_payload(),
        "registry_sha256": canonical_sha256(registry_payload()),
        "cost_stress_registry": {
            "scenarios": [asdict(item) for item in COST_SCENARIOS],
            "variants": list(STRESS_VARIANT_NAMES),
            "base_scenario_reuses_identical_base_full_state_replay": True,
            "stress_scenarios_use_full_state_machine_replay": True,
        },
        "economics": {
            "cost_bps": COST_BPS,
            "slippage_bps": SLIPPAGE_BPS,
            "square_off": SQUARE_OFF,
            "eod_policy": EOD_POLICY,
        },
        "windows": {
            "historical_from_day": HISTORICAL_FROM_DAY,
            "historical_through_day": HISTORICAL_THROUGH_DAY,
            "split_day": SPLIT_DAY,
            "today": TODAY,
        },
        "source_bindings": source_bindings,
        "source_contract_summary": {
            "historical_universe": historical_universe,
            "today_universe": today_universe,
            "historical_completeness": historical_completeness,
            "today_completeness": today_completeness,
            "rejected_today_snapshot_recorded": (
                rejected_today_snapshot is not None
            ),
            "rejected_today_snapshot_identity": rejected_snapshot_identity,
            "cache_seed_recovery": cache_seed_provenance,
        },
        "limitations": limitations,
        "artifacts": artifacts,
    }
    common.atomic_write_json(target / "manifest.json", package_manifest)
    final_validation = validate_package(target)
    print(json.dumps({**final_validation, "output_dir": str(target)}, indent=2))
    return target


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run")
    historical_group = run.add_mutually_exclusive_group(required=True)
    historical_group.add_argument("--historical-provenance", type=Path)
    historical_group.add_argument("--historical-snapshot", type=Path)
    run.add_argument("--today-snapshot", type=Path, required=True)
    run.add_argument("--reference-package", type=Path)
    run.add_argument("--rejected-today-snapshot", type=Path)
    run.add_argument(
        "--seed-cache-package",
        type=Path,
        help=(
            "Research-only recovery: copy and revalidate sealed fresh caches "
            "from a failed downstream package before replay"
        ),
    )
    run.add_argument("--output-dir", type=Path, required=True)
    run.add_argument(
        "--resume",
        action="store_true",
        help="Resume a fail-closed incomplete package from verified artifacts",
    )
    build_cache = subparsers.add_parser("build-cache")
    build_cache.add_argument("--snapshot", type=Path, required=True)
    build_cache.add_argument("--cache-dir", type=Path, required=True)
    build_cache.add_argument("--from-day", required=True)
    build_cache.add_argument("--through-day", required=True)
    build_cache.add_argument("--run-label", required=True)
    build_cache.add_argument("--expected-master-date", required=True)
    build_cache.add_argument("--expected-contract", required=True)
    build_cache.add_argument("--expected-mapped-count", type=int, required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--package", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args_list = list(sys.argv[1:] if argv is None else argv)
    args = parse_args(args_list)
    if args.command == "validate":
        validate_registry()
        print(json.dumps(validate_package(args.package), indent=2))
        return 0
    if args.command == "build-cache":
        validate_registry()
        strict.configure_engine()
        candidates, paths, coverage, manifest, manifest_path = (
            _build_snapshot_inputs(
                args.snapshot.resolve(),
                args.cache_dir.resolve(),
                from_day=args.from_day,
                through_day=args.through_day,
                run_label=args.run_label,
                expected_master_date=args.expected_master_date,
                expected_contract_month_filter=args.expected_contract,
                expected_mapped_stock_futures=args.expected_mapped_count,
            )
        )
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "manifest_path": str(manifest_path),
                    "input_fingerprint": manifest.get("input_fingerprint"),
                    "candidates": int(len(candidates)),
                    "path_rows": int(len(paths)),
                    "coverage_rows": int(len(coverage)),
                    "headline_source_complete": manifest.get(
                        "headline_source_complete"
                    ),
                    "source_incomplete_symbol_sessions": manifest.get(
                        "source_incomplete_symbol_sessions"
                    ),
                },
                indent=2,
            )
        )
        return 0
    execute(
        historical_provenance=args.historical_provenance,
        historical_snapshot=args.historical_snapshot,
        today_snapshot=args.today_snapshot,
        reference_package=args.reference_package,
        rejected_today_snapshot=args.rejected_today_snapshot,
        seed_cache_package=args.seed_cache_package,
        output_dir=args.output_dir,
        command=[sys.executable, str(Path(__file__).resolve()), *args_list],
        resume=bool(args.resume),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
