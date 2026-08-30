#!/usr/bin/env python3
"""Read-only robustness audit for the isolated V6/V10 challenger packages.

The utility deliberately does not import a strategy engine.  Its only inputs are
completed research artifacts (daily CSVs and their provenance).  Historical
series must contain every session in the authoritative 59-session calendar;
missing days are never silently replaced with zero returns.

CSCV/PBO is reported only when all observations can be divided into equal,
contiguous blocks.  DSR is withheld unless the caller explicitly declares that
the loaded variants are the complete trial universe for a family.  Even then it
is labelled illustrative because independence and iid daily-return assumptions
cannot be established from these artifacts.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import NormalDist
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


SCHEMA_VERSION = "fno_v6_v10_challenger_robustness_audit_v1"
REPAIRED_V10_SCHEMA_VERSION = "fno_v10_repaired_snapshot_rerun_v1"
REPAIRED_V10_ROOT_NAME = "v10_repaired_snapshot_reruns_20260827_v1"
DEFAULT_RESEARCH_ROOT = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research"
)
DEFAULT_SPLIT_DAY = "2026-08-06"
REFERENCE_SCENARIO = "REFERENCE_15_0"
IST = timezone(timedelta(hours=5, minutes=30))


@dataclass
class DailySeries:
    family: str
    definition_id: str
    variant: str
    scenario: str
    dataset: str
    frame: pd.DataFrame
    source_path: Path
    cost_bps: float | None = None
    slippage_bps: float | None = None
    description: str = ""
    limitations: tuple[str, ...] = ()
    source_sha256: str = ""
    families: set[str] = field(default_factory=set)
    source_paths: list[Path] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.family = str(self.family).upper()
        self.definition_id = str(self.definition_id).upper()
        self.variant = str(self.variant).upper()
        self.scenario = str(self.scenario).upper()
        self.dataset = str(self.dataset).upper()
        if not self.families:
            self.families = {self.family}
        if not self.source_paths:
            self.source_paths = [self.source_path]
        if not self.source_sha256 and self.source_path.is_file():
            self.source_sha256 = sha256_file(self.source_path)

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.dataset, self.scenario, self.definition_id)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object: {path}")
    return value


def iso_session(value: Any) -> str:
    text = str(value).strip()
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Invalid session_date value: {value!r}")
    return parsed.date().isoformat()


def normalise_daily_frame(frame: pd.DataFrame, source: Path) -> pd.DataFrame:
    if "session_date" not in frame.columns:
        raise ValueError(f"Missing session_date in {source}")
    return_column = next(
        (
            name
            for name in ("net_return_points", "net_return_pct", "return_points")
            if name in frame.columns
        ),
        None,
    )
    if return_column is None:
        raise ValueError(f"No supported daily return column in {source}")
    result = pd.DataFrame(
        {
            "session_date": frame["session_date"].map(iso_session),
            "return_points": pd.to_numeric(frame[return_column], errors="coerce"),
            "pnl_rs": (
                pd.to_numeric(frame["net_pnl_rs"], errors="coerce")
                if "net_pnl_rs" in frame.columns
                else np.nan
            ),
            "fills": (
                pd.to_numeric(frame["fills"], errors="coerce")
                if "fills" in frame.columns
                else np.nan
            ),
        }
    )
    if result["session_date"].duplicated().any():
        duplicates = sorted(
            result.loc[result["session_date"].duplicated(False), "session_date"].unique()
        )
        raise ValueError(f"Duplicate sessions in {source}: {duplicates}")
    if result["return_points"].isna().any():
        bad = result.loc[result["return_points"].isna(), "session_date"].tolist()
        raise ValueError(f"Non-numeric daily returns in {source}: {bad}")
    return result.sort_values("session_date", kind="stable").reset_index(drop=True)


def _definition_for_v10_gap(variant: str) -> str:
    variant = variant.upper()
    if variant == "CONTROL":
        return "V10_STAGE7_CONTROL"
    return f"V10_STAGE7_GAP_{variant}"


def _definition_for_v10_filter(variant: str) -> str:
    variant = variant.upper()
    if variant == "STAGE7_CONTROL":
        return "V10_STAGE7_CONTROL"
    return f"V10_STAGE7_FILTER_{variant}"


def _find_named_ancestor(path: Path, name: str) -> Path | None:
    for candidate in (path, *path.parents):
        if candidate.name.lower() == name.lower():
            return candidate
    return None


def _artifact_size(record: Mapping[str, Any]) -> int:
    value = record.get("bytes", record.get("size", -1))
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def _require_artifact_record(
    path: Path,
    record: Mapping[str, Any],
    *,
    label: str,
) -> Path:
    resolved = path.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"Missing {label}: {resolved}")
    if _artifact_size(record) != int(resolved.stat().st_size):
        raise AssertionError(f"{label} byte size changed: {resolved}")
    if str(record.get("sha256", "")) != sha256_file(resolved):
        raise AssertionError(f"{label} SHA-256 changed: {resolved}")
    return resolved


def _is_repaired_v10_provenance(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        return read_json(path).get("schema_version") == REPAIRED_V10_SCHEMA_VERSION
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return False


def _looks_like_repaired_v10_package(path: Path) -> bool:
    resolved = path.resolve()
    if REPAIRED_V10_ROOT_NAME in {part.lower() for part in resolved.parts}:
        return True
    provenance_path = (
        resolved
        if resolved.is_file() and resolved.name.lower() == "provenance.json"
        else resolved / "provenance.json"
    )
    if _is_repaired_v10_provenance(provenance_path):
        return True
    if resolved.is_file() and resolved.name.lower() == "latest.json":
        try:
            return (
                read_json(resolved).get("schema_version")
                == REPAIRED_V10_SCHEMA_VERSION
            )
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return False
    return False


def resolve_repaired_v10_run(path: Path) -> Path:
    """Resolve an explicitly supplied sealed repaired-V10 wrapper run."""

    supplied = path.resolve()
    if supplied.is_file():
        if supplied.name.lower() == "provenance.json" and _is_repaired_v10_provenance(
            supplied
        ):
            return supplied.parent
        if supplied.name.lower() == "latest.json":
            latest = read_json(supplied)
            if latest.get("schema_version") != REPAIRED_V10_SCHEMA_VERSION:
                raise ValueError(f"Unexpected repaired-V10 latest schema: {supplied}")
            run = Path(str(latest.get("run_dir", ""))).resolve()
            provenance_path = run / "provenance.json"
            _require_artifact_record(
                provenance_path,
                {
                    "bytes": provenance_path.stat().st_size
                    if provenance_path.is_file()
                    else -1,
                    "sha256": latest.get("provenance_sha256", ""),
                },
                label="repaired-V10 provenance",
            )
            return run

    for candidate in (supplied, *supplied.parents):
        provenance_path = candidate / "provenance.json"
        if _is_repaired_v10_provenance(provenance_path):
            return candidate.resolve()

    root = _find_named_ancestor(supplied, REPAIRED_V10_ROOT_NAME)
    if root is None and (supplied / "latest.json").is_file():
        root = supplied
    if root is not None:
        latest_path = root / "latest.json"
        if latest_path.is_file():
            return resolve_repaired_v10_run(latest_path)
    raise FileNotFoundError(f"Cannot resolve repaired-V10 wrapper run from {supplied}")


def _load_repaired_v10_seal(
    run: Path,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    provenance_path = run / "provenance.json"
    provenance = read_json(provenance_path)
    if provenance.get("schema_version") != REPAIRED_V10_SCHEMA_VERSION:
        raise ValueError(f"Unexpected repaired-V10 schema in {provenance_path}")
    if provenance.get("complete") is not True:
        raise ValueError(f"Repaired-V10 wrapper is incomplete: {provenance_path}")
    if (
        provenance.get("research_only") is not True
        or provenance.get("promotion_eligible") is not False
        or provenance.get("live_or_paper_authority") is not False
    ):
        raise ValueError(
            f"Repaired-V10 wrapper safety flags are invalid: {provenance_path}"
        )

    inventory_path = (run / "artifact_inventory.json").resolve()
    inventory_record = dict(provenance.get("artifact_inventory", {}))
    if Path(str(inventory_record.get("path", ""))).resolve() != inventory_path:
        raise AssertionError("Repaired-V10 inventory path is not bound to the wrapper")
    _require_artifact_record(
        inventory_path,
        {
            "bytes": inventory_path.stat().st_size if inventory_path.is_file() else -1,
            "sha256": inventory_record.get("sha256", ""),
        },
        label="repaired-V10 artifact inventory",
    )
    inventory = read_json(inventory_path)
    if inventory.get("schema_version") != REPAIRED_V10_SCHEMA_VERSION:
        raise ValueError(f"Unexpected repaired-V10 inventory schema: {inventory_path}")

    records: dict[str, dict[str, Any]] = {}
    for raw_record in inventory.get("artifacts", []):
        record = dict(raw_record)
        raw_relative = str(record.get("relative_path", ""))
        relative = Path(raw_relative.replace("\\", "/"))
        if not raw_relative or relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"Unsafe repaired-V10 artifact path: {raw_relative!r}")
        key = relative.as_posix()
        if key in records:
            raise ValueError(f"Duplicate repaired-V10 inventory path: {key}")
        resolved = (run / relative).resolve()
        if run != resolved and run not in resolved.parents:
            raise ValueError(f"Repaired-V10 artifact escapes wrapper: {raw_relative}")
        records[key] = record
    if not records:
        raise ValueError(f"Repaired-V10 artifact inventory is empty: {inventory_path}")
    return provenance, records


def _require_repaired_v10_artifact(
    run: Path,
    records: Mapping[str, Mapping[str, Any]],
    path: Path,
    *,
    label: str,
) -> Path:
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(run).as_posix()
    except ValueError as exc:
        raise ValueError(
            f"{label} is outside repaired-V10 wrapper: {resolved}"
        ) from exc
    record = records.get(relative)
    if record is None:
        raise AssertionError(
            f"{label} is not sealed by repaired-V10 inventory: {relative}"
        )
    return _require_artifact_record(resolved, record, label=label)


def resolve_gap_run(path: Path) -> Path:
    path = path.resolve()
    if (path / "provenance.json").is_file() and (path / "variants").is_dir():
        return path
    root = _find_named_ancestor(path, "v10_stage7_gap_guard_research_v1")
    if root is None:
        root = path
    latest = root / "latest.json"
    if latest.is_file():
        run = Path(str(read_json(latest).get("run_dir", "")))
        if run.is_dir():
            return run.resolve()
    runs = sorted(
        (item for item in (root / "runs").glob("gap_guard_*") if item.is_dir()),
        key=lambda item: item.stat().st_mtime_ns,
        reverse=True,
    )
    if not runs:
        raise FileNotFoundError(f"No completed gap-guard run under {root}")
    return runs[0].resolve()


def load_gap_package(path: Path) -> list[DailySeries]:
    run = resolve_gap_run(path)
    provenance = read_json(run / "provenance.json")
    scenarios = {
        str(row["scenario"]).upper(): (
            float(row["cost_bps"]),
            float(row["slippage_bps"]),
        )
        for row in provenance.get("cost_scenarios", [])
    }
    limitations = tuple(str(value) for value in provenance.get("limitations", []))
    loaded: list[DailySeries] = []
    for dataset_dir in (run / "variants").iterdir():
        if not dataset_dir.is_dir():
            continue
        dataset = "HISTORICAL" if dataset_dir.name.lower() == "historical" else "TODAY"
        for scenario_dir in dataset_dir.iterdir():
            if not scenario_dir.is_dir():
                continue
            scenario = scenario_dir.name.upper()
            cost_bps, slippage_bps = scenarios.get(scenario, (None, None))
            for variant_dir in scenario_dir.iterdir():
                daily_path = variant_dir / "daily.csv"
                if not daily_path.is_file():
                    continue
                variant = variant_dir.name.upper()
                loaded.append(
                    DailySeries(
                        family="V10_STAGE7_GAP_GUARD",
                        definition_id=_definition_for_v10_gap(variant),
                        variant=variant,
                        scenario=scenario,
                        dataset=dataset,
                        frame=normalise_daily_frame(pd.read_csv(daily_path), daily_path),
                        source_path=daily_path,
                        cost_bps=cost_bps,
                        slippage_bps=slippage_bps,
                        description=f"Stage-7 gap-guard variant {variant}",
                        limitations=limitations,
                    )
                )
    if not loaded:
        raise ValueError(f"No daily series found in gap package {run}")
    return loaded


def resolve_filter_root(path: Path) -> Path:
    path = path.resolve()
    root = _find_named_ancestor(path, "v10_stage7_followup_challengers_v1")
    if root is not None:
        return root
    if (path / "runs" / "historical_59_sessions").is_dir():
        return path
    raise FileNotFoundError(f"Cannot resolve V10 filter package root from {path}")


def _latest_variant_directories(dataset_root: Path) -> list[Path]:
    chosen: dict[str, tuple[int, Path]] = {}
    if not dataset_root.is_dir():
        return []
    for item in dataset_root.iterdir():
        provenance_path = item / "provenance.json"
        daily_path = item / "daily.csv"
        if not item.is_dir() or not provenance_path.is_file() or not daily_path.is_file():
            continue
        provenance = read_json(provenance_path)
        if provenance.get("complete") is not True:
            continue
        variant_payload = provenance.get("variant", {})
        variant = str(variant_payload.get("variant", item.name)).upper()
        stamp = item.stat().st_mtime_ns
        if variant not in chosen or stamp > chosen[variant][0]:
            chosen[variant] = (stamp, item)
    return [chosen[key][1] for key in sorted(chosen)]


def load_filter_package(path: Path) -> list[DailySeries]:
    root = resolve_filter_root(path)
    loaded: list[DailySeries] = []
    for dataset_name, dataset in (
        ("historical_59_sessions", "HISTORICAL"),
        ("today_2026_08_27", "TODAY"),
    ):
        for variant_dir in _latest_variant_directories(root / "runs" / dataset_name):
            provenance = read_json(variant_dir / "provenance.json")
            variant_payload = dict(provenance.get("variant", {}))
            variant = str(variant_payload.get("variant", variant_dir.name)).upper()
            execution = dict(provenance.get("execution", {}))
            daily_path = variant_dir / "daily.csv"
            loaded.append(
                DailySeries(
                    family="V10_STAGE7_FILTERS",
                    definition_id=_definition_for_v10_filter(variant),
                    variant=variant,
                    scenario=REFERENCE_SCENARIO,
                    dataset=dataset,
                    frame=normalise_daily_frame(pd.read_csv(daily_path), daily_path),
                    source_path=daily_path,
                    cost_bps=_optional_float(execution.get("cost_bps")),
                    slippage_bps=_optional_float(execution.get("slippage_bps")),
                    description=str(variant_payload.get("description", "")),
                    limitations=tuple(
                        str(value) for value in provenance.get("known_limitations", [])
                    ),
                )
            )
    if not loaded:
        raise ValueError(f"No daily series found in V10 filter package {root}")
    return loaded


def resolve_combo_run(path: Path) -> Path:
    path = path.resolve()
    if (path / "provenance.json").is_file() and (path / "daywise.csv").is_file():
        return path
    root = _find_named_ancestor(
        path, "v10_stage7_postselection_0935max050_gap_combo_v1"
    )
    if root is None:
        root = path
    candidates = sorted(
        (item for item in (root / "runs").glob("combo_*") if item.is_dir()),
        key=lambda item: item.stat().st_mtime_ns,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No completed post-selection combo run under {root}")
    return candidates[0].resolve()


def _definition_for_combo(profile_id: str) -> str:
    aliases = {
        "STAGE7": "V10_STAGE7_CONTROL",
        "STAGE7_GAP2": "V10_STAGE7_GAP_MAX_2_BPS",
        "MAX050": "V10_STAGE7_FILTER_0935_LONG_MOVE_MAX_050",
    }
    profile = profile_id.upper()
    return aliases.get(profile, f"V10_STAGE7_COMBO_{profile}")


def load_combo_package(path: Path) -> list[DailySeries]:
    run = resolve_combo_run(path)
    provenance = read_json(run / "provenance.json")
    if provenance.get("research_design") != "EXPLORATORY_POST_SELECTION_COMBINATION":
        raise ValueError(f"Unexpected combo research design in {run / 'provenance.json'}")
    cost_map = {
        str(row["scenario"]).upper(): (
            _optional_float(row.get("cost_bps")),
            _optional_float(row.get("slippage_bps")),
        )
        for row in provenance.get("cost_scenarios", [])
    }
    limitations = tuple(str(value) for value in provenance.get("limitations", []))
    daywise_path = run / "daywise.csv"
    raw = pd.read_csv(daywise_path)
    required = {"dataset", "scenario", "profile_id", "session_date"}
    if not required.issubset(raw.columns):
        raise ValueError(f"Combo daywise schema missing {sorted(required - set(raw.columns))}")
    if "period_label" in raw.columns:
        # The package intentionally includes FULL plus repeated TRAIN/TEST slices.
        # Only FULL is a unique 59-session daily series; TRAIN/TEST are recomputed
        # below from the authoritative calendar.
        labels = raw["period_label"].astype(str).str.upper()
        datasets = raw["dataset"].astype(str).str.upper()
        raw = raw.loc[
            (datasets.eq("HISTORICAL") & labels.eq("FULL"))
            | (datasets.str.startswith("TODAY") & labels.eq("TODAY"))
        ].copy()
    loaded: list[DailySeries] = []
    for (dataset, scenario, profile_id), group in raw.groupby(
        ["dataset", "scenario", "profile_id"], sort=True, dropna=False
    ):
        scenario_name = str(scenario).upper()
        profile = str(profile_id).upper()
        cost, slip = cost_map.get(scenario_name, (None, None))
        loaded.append(
            DailySeries(
                family="V10_STAGE7_POSTSELECTION_COMBO",
                definition_id=_definition_for_combo(profile),
                variant=profile,
                scenario=scenario_name,
                dataset=(
                    "TODAY" if str(dataset).upper().startswith("TODAY") else "HISTORICAL"
                ),
                frame=normalise_daily_frame(group, daywise_path),
                source_path=daywise_path,
                cost_bps=cost,
                slippage_bps=slip,
                description=f"Exploratory post-selection profile {profile}",
                limitations=limitations,
            )
        )
    if not loaded:
        raise ValueError(f"No daily series found in combo package {run}")
    return loaded


def _resolve_repaired_combo_run(
    run: Path,
    records: Mapping[str, Mapping[str, Any]],
) -> Path:
    latest_path = _require_repaired_v10_artifact(
        run,
        records,
        run / "combo_suite" / "latest.json",
        label="repaired-V10 combo latest pointer",
    )
    latest = read_json(latest_path)
    if (
        latest.get("research_only") is not True
        or latest.get("promotion_eligible") is not False
    ):
        raise ValueError(
            f"Repaired-V10 combo latest safety flags are invalid: {latest_path}"
        )
    combo_run = Path(str(latest.get("run_dir", ""))).resolve()
    combo_runs_root = (run / "combo_suite" / "runs").resolve()
    if combo_runs_root not in combo_run.parents:
        raise ValueError(f"Repaired-V10 combo run escapes wrapper: {combo_run}")
    provenance_path = _require_repaired_v10_artifact(
        run,
        records,
        combo_run / "provenance.json",
        label="repaired-V10 combo provenance",
    )
    if sha256_file(provenance_path) != str(latest.get("provenance_sha256", "")):
        raise AssertionError(
            f"Repaired-V10 combo latest hash changed: {provenance_path}"
        )
    _require_repaired_v10_artifact(
        run,
        records,
        combo_run / "daywise.csv",
        label="repaired-V10 combo daywise series",
    )
    return combo_run


def _with_wrapper_limitations(
    series: Iterable[DailySeries], wrapper_limitations: Sequence[str]
) -> list[DailySeries]:
    values = list(series)
    for item in values:
        item.limitations = tuple(
            dict.fromkeys(
                [*item.limitations, *(str(value) for value in wrapper_limitations)]
            )
        )
    return values


def _load_repaired_v8_comparator(
    run: Path,
    provenance: Mapping[str, Any],
    records: Mapping[str, Mapping[str, Any]],
) -> list[DailySeries]:
    comparator = dict(provenance.get("v8_comparator", {}))
    if not comparator:
        return []
    profile_id = str(comparator.get("profile_id", "")).upper().strip()
    if profile_id != "V8_COMBINED_CONTROL":
        raise ValueError(f"Unexpected repaired V8 comparator profile: {profile_id!r}")
    scenarios = {
        str(row.get("scenario", "")).upper(): (
            _optional_float(row.get("cost_bps")),
            _optional_float(row.get("slippage_bps")),
        )
        for row in provenance.get("cost_scenarios", [])
    }
    if not scenarios or any(not name for name in scenarios):
        raise ValueError("Repaired-V10 wrapper has no complete cost-scenario registry")
    limitations = tuple(str(value) for value in provenance.get("limitations", []))
    description = str(comparator.get("five_minute_contract", ""))
    loaded: list[DailySeries] = []
    for dataset_dir, dataset in (("historical", "HISTORICAL"), ("today", "TODAY")):
        for scenario, (cost_bps, slippage_bps) in sorted(scenarios.items()):
            scenario_root = (
                run
                / "v8_combined_control"
                / dataset_dir
                / scenario.lower()
            )
            daily_path = _require_repaired_v10_artifact(
                run,
                records,
                scenario_root / "daily.csv",
                label=f"repaired V8 {dataset}/{scenario} daily series",
            )
            summary_path = _require_repaired_v10_artifact(
                run,
                records,
                scenario_root / "summary.json",
                label=f"repaired V8 {dataset}/{scenario} summary",
            )
            summary = read_json(summary_path)
            if (
                str(summary.get("dataset", "")).upper() != dataset
                or str(summary.get("scenario", "")).upper() != scenario
                or str(summary.get("profile_id", "")).upper() != profile_id
                or summary.get("research_only") is not True
                or summary.get("promotion_eligible") is not False
                or not _optional_numbers_equal(
                    _optional_float(summary.get("cost_bps")), cost_bps
                )
                or not _optional_numbers_equal(
                    _optional_float(summary.get("slippage_bps")), slippage_bps
                )
            ):
                raise ValueError(
                    f"Repaired V8 summary contract mismatch: {summary_path}"
                )
            loaded.append(
                DailySeries(
                    family="V10_REPAIRED_V8_COMPARATOR",
                    definition_id=profile_id,
                    variant=profile_id,
                    scenario=scenario,
                    dataset=dataset,
                    frame=normalise_daily_frame(pd.read_csv(daily_path), daily_path),
                    source_path=daily_path,
                    cost_bps=cost_bps,
                    slippage_bps=slippage_bps,
                    description=description,
                    limitations=limitations,
                )
            )
    return loaded


def load_repaired_v10_package(path: Path) -> list[DailySeries]:
    """Load only the sealed completed outputs nested in a repaired-V10 wrapper."""

    run = resolve_repaired_v10_run(path)
    provenance, records = _load_repaired_v10_seal(run)
    wrapper_limitations = tuple(
        str(value) for value in provenance.get("limitations", [])
    )

    expected_variants = {
        str(row.get("variant", "")).upper()
        for row in provenance.get("individual_variants", [])
        if str(row.get("variant", "")).strip()
    }
    expected_individual = {
        (dataset, variant)
        for dataset in ("HISTORICAL", "TODAY")
        for variant in expected_variants
    }
    completed_individual: dict[tuple[str, str], list[Path]] = {}
    for dataset_dir, dataset in (
        ("historical_59_sessions", "HISTORICAL"),
        ("today_2026_08_27", "TODAY"),
    ):
        outputs_root = run / "individual_filter_suite" / "runs" / dataset_dir
        if not outputs_root.is_dir():
            raise FileNotFoundError(
                f"Missing repaired-V10 individual output root: {outputs_root}"
            )
        for output in sorted(item for item in outputs_root.iterdir() if item.is_dir()):
            provenance_path = output / "provenance.json"
            daily_path = output / "daily.csv"
            if not provenance_path.is_file() or not daily_path.is_file():
                continue
            _require_repaired_v10_artifact(
                run,
                records,
                provenance_path,
                label=f"repaired-V10 individual {dataset} provenance",
            )
            _require_repaired_v10_artifact(
                run,
                records,
                daily_path,
                label=f"repaired-V10 individual {dataset} daily series",
            )
            output_provenance = read_json(provenance_path)
            if output_provenance.get("complete") is not True:
                continue
            variant = str(
                dict(output_provenance.get("variant", {})).get(
                    "variant", output.name
                )
            ).upper()
            completed_individual.setdefault((dataset, variant), []).append(output)
    observed_completed = set(completed_individual)
    duplicates = {
        key: [str(item) for item in outputs]
        for key, outputs in completed_individual.items()
        if len(outputs) != 1
    }
    if (
        not expected_variants
        or observed_completed != expected_individual
        or duplicates
    ):
        raise AssertionError(
            "Repaired-V10 completed individual-filter output mismatch: "
            f"expected={sorted(expected_individual)}, "
            f"observed={sorted(observed_completed)}, duplicates={duplicates}"
        )

    individual = load_filter_package(run / "individual_filter_suite")
    observed_individual = {
        (item.dataset, item.variant)
        for item in individual
    }
    if observed_individual != expected_individual:
        raise AssertionError(
            "Repaired-V10 individual-filter registry/output mismatch: "
            f"expected={sorted(expected_individual)}, "
            f"observed={sorted(observed_individual)}"
        )
    for item in individual:
        _require_repaired_v10_artifact(
            run,
            records,
            item.source_path,
            label=f"repaired-V10 individual {item.dataset}/{item.variant} daily series",
        )
        _require_repaired_v10_artifact(
            run,
            records,
            item.source_path.parent / "provenance.json",
            label=f"repaired-V10 individual {item.dataset}/{item.variant} provenance",
        )

    combo_run = _resolve_repaired_combo_run(run, records)
    combos = load_combo_package(combo_run)
    expected_profiles = {
        str(row.get("profile_id", "")).upper()
        for row in provenance.get("combo_profiles", [])
        if str(row.get("profile_id", "")).strip()
    }
    expected_scenarios = {
        str(row.get("scenario", "")).upper()
        for row in provenance.get("cost_scenarios", [])
        if str(row.get("scenario", "")).strip()
    }
    observed_combos = {
        (item.dataset, item.scenario, item.variant)
        for item in combos
    }
    expected_combos = {
        (dataset, scenario, profile)
        for dataset in ("HISTORICAL", "TODAY")
        for scenario in expected_scenarios
        for profile in expected_profiles
    }
    if (
        not expected_profiles
        or not expected_scenarios
        or observed_combos != expected_combos
    ):
        raise AssertionError(
            "Repaired-V10 combo registry/output mismatch: "
            f"expected={sorted(expected_combos)}, observed={sorted(observed_combos)}"
        )

    v8_comparator = _load_repaired_v8_comparator(run, provenance, records)
    return _with_wrapper_limitations(
        [*individual, *combos, *v8_comparator], wrapper_limitations
    )


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def load_v6_package(path: Path) -> list[DailySeries]:
    path = path.resolve()
    daywise_path = path / "daywise.csv"
    if not daywise_path.is_file():
        raise FileNotFoundError(f"V6 daywise.csv is not ready under {path}")
    raw = pd.read_csv(daywise_path)
    required = {"dataset", "variant", "session_date"}
    if not required.issubset(raw.columns):
        raise ValueError(f"V6 daywise schema missing {sorted(required - set(raw.columns))}")
    manifest = read_json(path / "manifest.json") if (path / "manifest.json").is_file() else {}
    economics = dict(manifest.get("economics", {}))
    default_cost = _optional_float(economics.get("cost_bps"))
    default_slippage = _optional_float(economics.get("slippage_bps"))
    limitations = tuple(str(value) for value in manifest.get("limitations", []))
    group_columns = ["dataset", "variant"]
    if "scenario" in raw.columns:
        group_columns.append("scenario")
    loaded: list[DailySeries] = []
    for keys, group in raw.groupby(group_columns, sort=True, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        dataset = str(keys[0]).upper()
        variant = str(keys[1]).upper()
        scenario = (
            str(keys[2]).upper() if len(keys) > 2 and pd.notna(keys[2]) else REFERENCE_SCENARIO
        )
        cost = (
            _optional_float(group["cost_bps"].iloc[0])
            if "cost_bps" in group.columns
            else default_cost
        )
        slip = (
            _optional_float(group["slippage_bps"].iloc[0])
            if "slippage_bps" in group.columns
            else default_slippage
        )
        loaded.append(
            DailySeries(
                family="V6_CHALLENGERS",
                definition_id=f"V6_{variant}",
                variant=variant,
                scenario=scenario,
                dataset="TODAY" if dataset.startswith("TODAY") else "HISTORICAL",
                frame=normalise_daily_frame(group, daywise_path),
                source_path=daywise_path,
                cost_bps=cost,
                slippage_bps=slip,
                description=f"V6 isolated challenger {variant}",
                limitations=limitations,
            )
        )
    stress_path = path / "cost_stress_daywise.csv"
    if stress_path.is_file():
        stress = pd.read_csv(stress_path)
        stress_required = {
            "dataset",
            "variant",
            "cost_scenario",
            "session_date",
            "cost_bps",
            "slippage_bps",
        }
        if not stress_required.issubset(stress.columns):
            raise ValueError(
                "V6 cost-stress schema missing "
                f"{sorted(stress_required - set(stress.columns))}"
            )
        scenario_aliases = {
            "BASE_15BPS_0SLIP": REFERENCE_SCENARIO,
            "STRESS_20BPS_2SLIP": "STRESS_20_2",
            "STRESS_25BPS_5SLIP": "STRESS_25_5",
        }
        for (dataset, variant, raw_scenario), group in stress.groupby(
            ["dataset", "variant", "cost_scenario"], sort=True, dropna=False
        ):
            raw_name = str(raw_scenario).upper()
            scenario = scenario_aliases.get(raw_name, raw_name)
            variant_name = str(variant).upper()
            loaded.append(
                DailySeries(
                    family="V6_CHALLENGERS",
                    definition_id=f"V6_{variant_name}",
                    variant=variant_name,
                    scenario=scenario,
                    dataset=(
                        "TODAY" if str(dataset).upper().startswith("TODAY") else "HISTORICAL"
                    ),
                    frame=normalise_daily_frame(group, stress_path),
                    source_path=stress_path,
                    cost_bps=_optional_float(group["cost_bps"].iloc[0]),
                    slippage_bps=_optional_float(group["slippage_bps"].iloc[0]),
                    description=f"V6 isolated challenger {variant_name}",
                    limitations=limitations,
                )
            )
    return loaded


def discover_packages(research_root: Path) -> tuple[list[Path], list[str]]:
    packages: list[Path] = []
    notices: list[str] = []
    gap = research_root / "v10_stage7_gap_guard_research_v1"
    filters = research_root / "v10_stage7_followup_challengers_v1"
    combo = research_root / "v10_stage7_postselection_0935max050_gap_combo_v1"
    if gap.is_dir():
        packages.append(gap)
    else:
        notices.append(f"MISSING_GAP_PACKAGE:{gap}")
    if filters.is_dir():
        packages.append(filters)
    else:
        notices.append(f"MISSING_FILTER_PACKAGE:{filters}")
    if combo.is_dir():
        packages.append(combo)
    else:
        notices.append(f"MISSING_POSTSELECTION_COMBO_PACKAGE:{combo}")
    v6_candidates = sorted(
        (
            item
            for item in research_root.glob("v6_isolated_challengers_full59_today_*")
            if item.is_dir() and (item / "daywise.csv").is_file()
        ),
        key=lambda item: item.stat().st_mtime_ns,
        reverse=True,
    )
    if v6_candidates:
        packages.append(v6_candidates[0])
    else:
        notices.append("V6_PACKAGE_NOT_READY:NO_NONEMPTY_DAYWISE_PACKAGE_FOUND")
    return packages, notices


def load_package(path: Path) -> list[DailySeries]:
    resolved = path.resolve()
    lower_parts = {part.lower() for part in resolved.parts}
    text = str(path).lower()
    if _looks_like_repaired_v10_package(path):
        return load_repaired_v10_package(path)
    if "v10_stage7_postselection_0935max050_gap_combo_v1" in lower_parts or (
        "postselection_0935max050_gap_combo" in text
    ):
        return load_combo_package(path)
    if "v10_stage7_gap_guard_research_v1" in lower_parts or "gap_guard" in text:
        return load_gap_package(path)
    if "v10_stage7_followup_challengers_v1" in lower_parts or "followup_challengers" in text:
        return load_filter_package(path)
    if "v6_isolated_challengers" in text or (path / "daywise.csv").is_file():
        return load_v6_package(path)
    raise ValueError(f"Unrecognised research package: {path}")


def _frames_equal(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    columns = ["session_date", "return_points", "pnl_rs", "fills"]
    a = left[columns].sort_values("session_date").reset_index(drop=True)
    b = right[columns].sort_values("session_date").reset_index(drop=True)
    if a["session_date"].tolist() != b["session_date"].tolist():
        return False
    for column in columns[1:]:
        av = pd.to_numeric(a[column], errors="coerce").to_numpy(float)
        bv = pd.to_numeric(b[column], errors="coerce").to_numpy(float)
        if not np.allclose(av, bv, rtol=1e-10, atol=1e-8, equal_nan=True):
            return False
    return True


def deduplicate_series(
    series: Sequence[DailySeries],
) -> tuple[list[DailySeries], list[dict[str, Any]]]:
    canonical: dict[tuple[str, str, str], DailySeries] = {}
    inventory: list[dict[str, Any]] = []
    for item in series:
        existing = canonical.get(item.key)
        duplicate_of = ""
        if existing is None:
            canonical[item.key] = item
        else:
            if not _frames_equal(existing.frame, item.frame):
                raise ValueError(
                    "Strategy-definition collision with different daily series: "
                    f"{item.key}; {existing.source_path} vs {item.source_path}"
                )
            if not _optional_numbers_equal(existing.cost_bps, item.cost_bps):
                raise ValueError(f"Cost collision for duplicate definition {item.key}")
            if not _optional_numbers_equal(existing.slippage_bps, item.slippage_bps):
                raise ValueError(f"Slippage collision for duplicate definition {item.key}")
            existing.families.update(item.families)
            existing.source_paths.extend(item.source_paths)
            existing.limitations = tuple(sorted(set(existing.limitations) | set(item.limitations)))
            duplicate_of = "|".join(item.key)
        inventory.append(
            {
                "family": item.family,
                "definition_id": item.definition_id,
                "variant": item.variant,
                "scenario": item.scenario,
                "dataset": item.dataset,
                "source_path": str(item.source_path),
                "source_sha256": item.source_sha256,
                "rows": len(item.frame),
                "duplicate_definition_merged": bool(existing is not None),
                "canonical_key": "|".join(item.key),
                "duplicate_of": duplicate_of,
            }
        )
    return list(canonical.values()), inventory


def _optional_numbers_equal(left: float | None, right: float | None) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False
    return math.isclose(left, right, rel_tol=1e-12, abs_tol=1e-12)


def _historical_sessions_from_provenance(provenance: Mapping[str, Any]) -> list[str]:
    source = dict(provenance.get("source_inputs", {})).get("HISTORICAL", {})
    values = source.get("session_dates")
    if values is None:
        values = source.get("sessions", [])
    if not isinstance(values, list):
        return []
    return [iso_session(value) for value in values]


def calendar_from_gap(packages: Sequence[Path]) -> tuple[list[str], Path]:
    candidates: list[Path] = []
    for package in packages:
        if _looks_like_repaired_v10_package(package):
            repaired_run = resolve_repaired_v10_run(package)
            _, records = _load_repaired_v10_seal(repaired_run)
            combo_run = _resolve_repaired_combo_run(repaired_run, records)
            provenance_path = combo_run / "provenance.json"
            dates = _historical_sessions_from_provenance(read_json(provenance_path))
            if len(dates) == 59 and len(set(dates)) == 59:
                return dates, provenance_path
            raise ValueError(
                "Repaired-V10 combo provenance does not bind 59 unique historical "
                f"sessions: {provenance_path}"
            )
        try:
            run = resolve_gap_run(package)
        except (FileNotFoundError, ValueError):
            continue
        provenance_path = run / "provenance.json"
        if provenance_path.is_file():
            candidates.append(provenance_path)
    for provenance_path in candidates:
        provenance = read_json(provenance_path)
        dates = _historical_sessions_from_provenance(provenance)
        if len(dates) == 59 and len(set(dates)) == 59:
            return dates, provenance_path
    raise ValueError(
        "No authoritative 59-session calendar found. Supply --calendar-json pointing "
        "to provenance containing source_inputs.HISTORICAL.session_dates or .sessions."
    )


def calendar_from_json(path: Path) -> list[str]:
    payload = read_json(path)
    values: Any = payload.get("session_dates")
    if values is None:
        values = payload.get("sessions")
    if values is None:
        source = dict(payload.get("source_inputs", {})).get("HISTORICAL", {})
        values = source.get("session_dates")
        if values is None:
            values = source.get("sessions")
    if not isinstance(values, list):
        raise ValueError(f"No session_dates or sessions list in {path}")
    dates = [iso_session(value) for value in values]
    if len(dates) != 59 or len(set(dates)) != 59:
        raise ValueError(f"Expected 59 unique official sessions in {path}; got {len(dates)}")
    return dates


def max_additive_drawdown(values: Sequence[float]) -> float:
    array = np.asarray(values, dtype=float)
    if array.size == 0:
        return 0.0
    cumulative = np.concatenate(([0.0], np.cumsum(array)))
    peaks = np.maximum.accumulate(cumulative)
    return float(np.max(peaks - cumulative))


def sample_sharpe(values: Sequence[float]) -> float | None:
    array = np.asarray(values, dtype=float)
    array = array[np.isfinite(array)]
    if array.size < 2:
        return None
    standard_deviation = float(np.std(array, ddof=1))
    if standard_deviation <= 0.0:
        return None
    return float(np.mean(array) / standard_deviation)


def daily_profit_factor(values: Sequence[float]) -> float | None:
    array = np.asarray(values, dtype=float)
    profits = float(array[array > 0].sum())
    losses = float(-array[array < 0].sum())
    if losses > 0:
        return profits / losses
    if profits > 0:
        return math.inf
    return None


def metrics_for_values(
    frame: pd.DataFrame,
    expected_dates: Sequence[str],
) -> dict[str, Any]:
    expected = list(expected_dates)
    observed = set(frame["session_date"])
    missing = [value for value in expected if value not in observed]
    extra = sorted(observed - set(expected))
    eligible = not missing and not extra and len(frame) == len(expected)
    base = {
        "official_sessions_expected": len(expected),
        "observations": len(frame),
        "missing_sessions": len(missing),
        "extra_sessions": len(extra),
        "missing_session_dates": "|".join(missing),
        "extra_session_dates": "|".join(extra),
        "aligned_eligible": eligible,
    }
    if not eligible:
        return base
    indexed = frame.set_index("session_date").loc[expected]
    returns = indexed["return_points"].to_numpy(float)
    pnl = indexed["pnl_rs"].to_numpy(float)
    fills = indexed["fills"].to_numpy(float)
    sharpe = sample_sharpe(returns)
    positive = int(np.sum(returns > 0))
    negative = int(np.sum(returns < 0))
    flat = int(np.sum(returns == 0))
    base.update(
        {
            "active_days": positive + negative,
            "positive_days": positive,
            "negative_days": negative,
            "flat_days": flat,
            "positive_day_share_pct": positive / len(returns) * 100.0,
            "mean_daily_return_points": float(np.mean(returns)),
            "daily_return_std": float(np.std(returns, ddof=1)) if len(returns) > 1 else None,
            "daily_sharpe": sharpe,
            "annualized_daily_sharpe": sharpe * math.sqrt(252.0) if sharpe is not None else None,
            "daily_profit_factor": daily_profit_factor(returns),
            "net_return_points": float(np.sum(returns)),
            "max_daily_drawdown_points": max_additive_drawdown(returns),
            "fills": int(np.nansum(fills)) if np.isfinite(fills).any() else None,
            "net_pnl_rs": float(np.nansum(pnl)) if np.isfinite(pnl).any() else None,
            "max_daily_drawdown_rs": (
                max_additive_drawdown(pnl[np.isfinite(pnl)])
                if np.isfinite(pnl).any()
                else None
            ),
        }
    )
    return base


def metric_rows(
    series: Sequence[DailySeries], calendar: Sequence[str], split_day: str
) -> list[dict[str, Any]]:
    train = [value for value in calendar if value < split_day]
    test = [value for value in calendar if value >= split_day]
    rows: list[dict[str, Any]] = []
    for item in sorted(series, key=lambda value: value.key):
        common = {
            "families": "|".join(sorted(item.families)),
            "definition_id": item.definition_id,
            "variant": item.variant,
            "scenario": item.scenario,
            "dataset": item.dataset,
            "cost_bps": item.cost_bps,
            "slippage_bps": item.slippage_bps,
            "description": item.description,
        }
        if item.dataset == "HISTORICAL":
            for period, dates in (("FULL", calendar), ("TRAIN", train), ("TEST", test)):
                subset = item.frame.loc[item.frame["session_date"].isin(dates)].copy()
                rows.append({**common, "period": period, **metrics_for_values(subset, dates)})
        else:
            dates = sorted(item.frame["session_date"].unique())
            rows.append({**common, "period": "TODAY", **metrics_for_values(item.frame, dates)})
    return rows


def _finite_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def build_cost_stress(metrics: pd.DataFrame) -> pd.DataFrame:
    if metrics.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for (definition, dataset, period), group in metrics.groupby(
        ["definition_id", "dataset", "period"], sort=True, dropna=False
    ):
        reference = group.loc[group["scenario"].eq(REFERENCE_SCENARIO)]
        if len(reference) != 1:
            continue
        base = reference.iloc[0]
        for _, stress in group.loc[~group["scenario"].eq(REFERENCE_SCENARIO)].iterrows():
            item: dict[str, Any] = {
                "definition_id": definition,
                "dataset": dataset,
                "period": period,
                "stress_scenario": stress["scenario"],
                "reference_cost_bps": base.get("cost_bps"),
                "reference_slippage_bps": base.get("slippage_bps"),
                "stress_cost_bps": stress.get("cost_bps"),
                "stress_slippage_bps": stress.get("slippage_bps"),
            }
            for metric in (
                "net_return_points",
                "net_pnl_rs",
                "max_daily_drawdown_points",
                "positive_day_share_pct",
                "daily_sharpe",
            ):
                base_value = _finite_or_none(base.get(metric))
                stress_value = _finite_or_none(stress.get(metric))
                item[f"reference_{metric}"] = base_value
                item[f"stress_{metric}"] = stress_value
                item[f"delta_{metric}"] = (
                    stress_value - base_value
                    if base_value is not None and stress_value is not None
                    else None
                )
            base_net = _finite_or_none(base.get("net_return_points"))
            stress_net = _finite_or_none(stress.get("net_return_points"))
            item["net_return_retention_pct"] = (
                stress_net / base_net * 100.0
                if base_net not in (None, 0.0) and stress_net is not None
                else None
            )
            item["remains_positive"] = stress_net is not None and stress_net > 0
            rows.append(item)
    return pd.DataFrame(rows)


def average_ranks(values: Sequence[float]) -> np.ndarray:
    array = np.asarray(values, dtype=float)
    order = np.argsort(array, kind="stable")
    ranks = np.empty(len(array), dtype=float)
    position = 0
    while position < len(order):
        end = position + 1
        while end < len(order) and math.isclose(
            array[order[end]], array[order[position]], rel_tol=1e-12, abs_tol=1e-12
        ):
            end += 1
        average = ((position + 1) + end) / 2.0
        ranks[order[position:end]] = average
        position = end
    return ranks


def choose_exact_cscv_partitions(
    observations: int, preferred: int = 6, maximum: int = 16
) -> int | None:
    candidates = [
        value
        for value in range(4, min(maximum, observations) + 1, 2)
        if observations % value == 0 and observations // value >= 5
    ]
    if preferred in candidates:
        return preferred
    return max(candidates) if candidates else None


def cscv_pbo(
    returns: pd.DataFrame, partitions: int
) -> tuple[dict[str, Any], pd.DataFrame]:
    if partitions < 4 or partitions % 2:
        raise ValueError("CSCV partitions must be even and at least four")
    observations, strategies = returns.shape
    if strategies < 2:
        raise ValueError("CSCV requires at least two strategies")
    if observations % partitions:
        raise ValueError("Strict CSCV requires equal blocks; observations are not divisible")
    block_size = observations // partitions
    if block_size < 5:
        raise ValueError("CSCV block size is too small for this audit")
    values = returns.to_numpy(float)
    if not np.isfinite(values).all():
        raise ValueError("CSCV input contains missing/non-finite values")
    blocks = np.arange(observations).reshape(partitions, block_size)
    records: list[dict[str, Any]] = []
    weighted_failures = 0.0
    total_weight = 0.0
    for combination in itertools.combinations(range(partitions), partitions // 2):
        chosen = set(combination)
        train_idx = np.concatenate([blocks[index] for index in combination])
        test_idx = np.concatenate(
            [blocks[index] for index in range(partitions) if index not in chosen]
        )
        train_scores = np.array(
            [sample_sharpe(values[train_idx, column]) for column in range(strategies)],
            dtype=float,
        )
        test_scores = np.array(
            [sample_sharpe(values[test_idx, column]) for column in range(strategies)],
            dtype=float,
        )
        if not np.isfinite(train_scores).all() or not np.isfinite(test_scores).all():
            continue
        best = np.flatnonzero(
            np.isclose(train_scores, np.max(train_scores), rtol=1e-12, atol=1e-12)
        )
        ranks = average_ranks(test_scores)
        weight = 1.0 / len(best)
        for selected in best:
            omega = float(ranks[selected] / (strategies + 1.0))
            logit = math.log(omega / (1.0 - omega))
            failure = omega <= 0.5
            weighted_failures += weight * float(failure)
            total_weight += weight
            records.append(
                {
                    "train_blocks": "|".join(str(value) for value in combination),
                    "selected_definition_id": str(returns.columns[selected]),
                    "train_sharpe": float(train_scores[selected]),
                    "test_sharpe": float(test_scores[selected]),
                    "test_rank_worst_to_best": float(ranks[selected]),
                    "omega": omega,
                    "logit": logit,
                    "pbo_failure": failure,
                    "tie_weight": weight,
                }
            )
    if total_weight == 0:
        raise ValueError("CSCV produced no finite train/test combinations")
    detail = pd.DataFrame(records)
    return (
        {
            "pbo": weighted_failures / total_weight,
            "partitions": partitions,
            "block_size": block_size,
            "combinations": math.comb(partitions, partitions // 2),
            "weighted_selections": total_weight,
        },
        detail,
    )


def standardized_moments(values: Sequence[float]) -> tuple[float, float]:
    array = np.asarray(values, dtype=float)
    mean = float(np.mean(array))
    centered = array - mean
    variance = float(np.mean(centered**2))
    if variance <= 0:
        raise ValueError("Moments require non-zero variance")
    scale = math.sqrt(variance)
    skewness = float(np.mean((centered / scale) ** 3))
    kurtosis = float(np.mean((centered / scale) ** 4))
    return skewness, kurtosis


def expected_max_sharpe(trial_sharpes: Sequence[float]) -> float:
    values = np.asarray(trial_sharpes, dtype=float)
    if values.size < 2 or not np.isfinite(values).all():
        raise ValueError("Expected maximum Sharpe requires at least two finite trials")
    variance = float(np.var(values, ddof=1))
    if variance <= 0:
        return float(np.max(values))
    trials = len(values)
    euler_gamma = 0.5772156649015329
    normal = NormalDist()
    first = normal.inv_cdf(1.0 - 1.0 / trials)
    second = normal.inv_cdf(1.0 - 1.0 / (trials * math.e))
    return math.sqrt(variance) * ((1.0 - euler_gamma) * first + euler_gamma * second)


def deflated_sharpe_probability(
    observed_sharpe: float,
    benchmark_sharpe: float,
    observations: int,
    skewness: float,
    kurtosis: float,
) -> float:
    if observations < 2:
        raise ValueError("DSR requires at least two observations")
    denominator_squared = (
        1.0
        - skewness * observed_sharpe
        + ((kurtosis - 1.0) / 4.0) * observed_sharpe**2
    )
    if denominator_squared <= 0 or not math.isfinite(denominator_squared):
        raise ValueError("DSR denominator is non-positive")
    statistic = (
        (observed_sharpe - benchmark_sharpe)
        * math.sqrt(observations - 1.0)
        / math.sqrt(denominator_squared)
    )
    return NormalDist().cdf(statistic)


def family_reference_matrices(
    series: Sequence[DailySeries], calendar: Sequence[str]
) -> dict[str, pd.DataFrame]:
    members: dict[str, dict[str, pd.Series]] = {}
    for item in series:
        if (
            item.dataset != "HISTORICAL"
            or item.scenario != REFERENCE_SCENARIO
            or set(item.frame["session_date"]) != set(calendar)
            or len(item.frame) != len(calendar)
        ):
            continue
        aligned = item.frame.set_index("session_date").loc[list(calendar), "return_points"]
        for family in item.families:
            members.setdefault(family, {})[item.definition_id] = aligned
    return {
        family: pd.DataFrame(columns, index=list(calendar), dtype=float)
        for family, columns in members.items()
    }


def fixed_holdout_rows(
    matrices: Mapping[str, pd.DataFrame], calendar: Sequence[str], split_day: str
) -> list[dict[str, Any]]:
    train_mask = np.array([value < split_day for value in calendar])
    test_mask = ~train_mask
    rows: list[dict[str, Any]] = []
    for family, matrix in sorted(matrices.items()):
        if matrix.shape[1] < 2:
            rows.append(
                {
                    "family": family,
                    "available": False,
                    "reason": "FEWER_THAN_TWO_UNIQUE_STRATEGY_DEFINITIONS",
                }
            )
            continue
        train_scores = {
            column: sample_sharpe(matrix.loc[train_mask, column]) for column in matrix.columns
        }
        finite = {key: value for key, value in train_scores.items() if value is not None}
        if not finite:
            rows.append(
                {"family": family, "available": False, "reason": "NO_FINITE_TRAIN_SHARPE"}
            )
            continue
        winner = sorted(finite, key=lambda key: (-float(finite[key]), key))[0]
        test_scores = np.array(
            [
                sample_sharpe(matrix.loc[test_mask, column])
                if sample_sharpe(matrix.loc[test_mask, column]) is not None
                else -math.inf
                for column in matrix.columns
            ]
        )
        ranks = average_ranks(test_scores)
        selected_position = list(matrix.columns).index(winner)
        omega = float(ranks[selected_position] / (matrix.shape[1] + 1.0))
        rows.append(
            {
                "family": family,
                "available": True,
                "reason": "SINGLE_PREDECLARED_49_10_SPLIT_NOT_PBO",
                "strategies": matrix.shape[1],
                "train_sessions": int(train_mask.sum()),
                "test_sessions": int(test_mask.sum()),
                "train_winner": winner,
                "train_winner_sharpe": train_scores[winner],
                "train_winner_net_return_points": float(matrix.loc[train_mask, winner].sum()),
                "test_sharpe": _finite_or_none(test_scores[selected_position]),
                "test_net_return_points": float(matrix.loc[test_mask, winner].sum()),
                "test_rank_worst_to_best": float(ranks[selected_position]),
                "test_relative_rank_omega": omega,
                "test_below_or_at_median": omega <= 0.5,
            }
        )
    return rows


def multiple_testing_outputs(
    matrices: Mapping[str, pd.DataFrame],
    complete_trial_families: set[str],
    preferred_partitions: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    audit_rows: list[dict[str, Any]] = []
    dsr_rows: list[dict[str, Any]] = []
    pbo_details: list[pd.DataFrame] = []
    for family, matrix in sorted(matrices.items()):
        observations, strategies = matrix.shape
        partitions = choose_exact_cscv_partitions(observations, preferred_partitions)
        audit: dict[str, Any] = {
            "family": family,
            "observations": observations,
            "unique_strategy_definitions": strategies,
            "pooled_with_other_families": False,
            "post_selection_conditional": True,
        }
        if strategies < 2:
            audit.update(
                {
                    "cscv_pbo_available": False,
                    "cscv_reason": "FEWER_THAN_TWO_UNIQUE_STRATEGY_DEFINITIONS",
                }
            )
        elif partitions is None:
            audit.update(
                {
                    "cscv_pbo_available": False,
                    "cscv_reason": (
                        "NO_EVEN_EQUAL_BLOCK_PARTITION_WITH_AT_LEAST_5_SESSIONS_PER_BLOCK;"
                        "NO_DATES_TRIMMED"
                    ),
                }
            )
        else:
            try:
                result, detail = cscv_pbo(matrix, partitions)
                audit.update(
                    {
                        "cscv_pbo_available": True,
                        "cscv_reason": "STRICT_EQUAL_BLOCK_CSCV",
                        **result,
                    }
                )
                detail.insert(0, "family", family)
                pbo_details.append(detail)
            except ValueError as error:
                audit.update(
                    {
                        "cscv_pbo_available": False,
                        "cscv_reason": f"CSCV_UNAVAILABLE:{error}",
                    }
                )
        complete = family in complete_trial_families
        audit["dsr_available"] = bool(complete and strategies >= 2)
        audit["dsr_reason"] = (
            "CALLER_DECLARED_COMPLETE_TRIAL_UNIVERSE;ILLUSTRATIVE_ONLY"
            if audit["dsr_available"]
            else "UNKNOWN_TOTAL_TRIAL_UNIVERSE_AND_POST_SELECTION"
        )
        audit_rows.append(audit)
        if complete and strategies >= 2:
            trial_sharpes = [sample_sharpe(matrix[column]) for column in matrix.columns]
            if any(value is None for value in trial_sharpes):
                for definition in matrix.columns:
                    dsr_rows.append(
                        {
                            "family": family,
                            "definition_id": definition,
                            "available": False,
                            "reason": "NON_FINITE_SHARPE_IN_DECLARED_TRIAL_FAMILY",
                        }
                    )
                continue
            benchmark = expected_max_sharpe([float(value) for value in trial_sharpes])
            for definition, sharpe in zip(matrix.columns, trial_sharpes):
                values = matrix[definition].to_numpy(float)
                try:
                    skewness, kurtosis = standardized_moments(values)
                    probability = deflated_sharpe_probability(
                        float(sharpe), benchmark, observations, skewness, kurtosis
                    )
                    dsr_rows.append(
                        {
                            "family": family,
                            "definition_id": definition,
                            "available": True,
                            "reason": "ILLUSTRATIVE_CALLER_DECLARED_ASSUMPTIONS",
                            "decision_usable": False,
                            "observations": observations,
                            "declared_trials": strategies,
                            "daily_sharpe": sharpe,
                            "expected_max_sharpe_benchmark": benchmark,
                            "skewness_population_moment": skewness,
                            "kurtosis_population_moment": kurtosis,
                            "deflated_sharpe_probability": probability,
                        }
                    )
                except ValueError as error:
                    dsr_rows.append(
                        {
                            "family": family,
                            "definition_id": definition,
                            "available": False,
                            "reason": f"DSR_UNAVAILABLE:{error}",
                        }
                    )
        else:
            for definition in matrix.columns:
                dsr_rows.append(
                    {
                        "family": family,
                        "definition_id": definition,
                        "available": False,
                        "reason": "UNKNOWN_TOTAL_TRIAL_UNIVERSE_AND_POST_SELECTION",
                        "decision_usable": False,
                        "observations": observations,
                        "loaded_variants_not_asserted_complete": strategies,
                    }
                )
    return (
        pd.DataFrame(audit_rows),
        pd.DataFrame(dsr_rows),
        pd.concat(pbo_details, ignore_index=True) if pbo_details else pd.DataFrame(),
    )


def aligned_daily_rows(
    series: Sequence[DailySeries], calendar: Sequence[str]
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in series:
        expected = calendar if item.dataset == "HISTORICAL" else sorted(item.frame["session_date"])
        indexed = item.frame.set_index("session_date")
        for session in expected:
            if session not in indexed.index:
                continue
            row = indexed.loc[session]
            rows.append(
                {
                    "dataset": item.dataset,
                    "session_date": session,
                    "families": "|".join(sorted(item.families)),
                    "definition_id": item.definition_id,
                    "variant": item.variant,
                    "scenario": item.scenario,
                    "return_points": row["return_points"],
                    "pnl_rs": row["pnl_rs"],
                    "fills": row["fills"],
                }
            )
    return pd.DataFrame(rows)


def _clean_csv_value(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return "INF" if value > 0 else "-INF" if value < 0 else ""
    if value is None or value is pd.NA:
        return ""
    return value


def write_csv(path: Path, rows: pd.DataFrame | Sequence[Mapping[str, Any]]) -> None:
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))
    cleaned = frame.copy()
    for column in cleaned.columns:
        cleaned[column] = cleaned[column].map(_clean_csv_value)
    cleaned.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL)


def unique_output_dir(root: Path) -> Path:
    stamp = datetime.now(IST).strftime("%Y%m%dT%H%M%S%f%z")
    base = root / f"audit_{stamp}"
    candidate = base
    suffix = 1
    while candidate.exists():
        candidate = Path(f"{base}_{suffix}")
        suffix += 1
    candidate.mkdir(parents=True, exist_ok=False)
    return candidate


def markdown_table(frame: pd.DataFrame, columns: Sequence[str], limit: int = 12) -> str:
    if frame.empty:
        return "_No rows available._"
    available = [column for column in columns if column in frame.columns]
    view = frame.loc[:, available].head(limit).copy()
    header = "| " + " | ".join(available) + " |"
    separator = "| " + " | ".join("---" for _ in available) + " |"
    lines = [header, separator]
    for _, row in view.iterrows():
        values = []
        for column in available:
            value = row[column]
            if isinstance(value, float):
                value = "" if math.isnan(value) else f"{value:.4f}"
            values.append(str(value).replace("|", "/"))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def build_report(
    *,
    metrics: pd.DataFrame,
    costs: pd.DataFrame,
    holdout: pd.DataFrame,
    multiple: pd.DataFrame,
    notices: Sequence[str],
    calendar: Sequence[str],
    split_day: str,
) -> str:
    reference_full = metrics.loc[
        metrics["dataset"].eq("HISTORICAL")
        & metrics["scenario"].eq(REFERENCE_SCENARIO)
        & metrics["period"].eq("FULL")
        & metrics["aligned_eligible"].eq(True)
    ].sort_values("net_return_points", ascending=False)
    today = metrics.loc[
        metrics["dataset"].eq("TODAY")
        & metrics["scenario"].eq(REFERENCE_SCENARIO)
        & metrics["period"].eq("TODAY")
        & metrics["aligned_eligible"].eq(True)
    ].sort_values("net_return_points", ascending=False)
    lines = [
        "# V6/V10 challenger robustness audit",
        "",
        "Research-only descriptive audit. It does not authorize live or paper promotion.",
        "",
        "## Alignment",
        "",
        f"- Official sessions: {len(calendar)} ({calendar[0]} through {calendar[-1]}).",
        f"- Fixed split: TRAIN dates before {split_day}; TEST dates on/after {split_day}.",
        "- Missing sessions are rejected, never imputed as zero-return days.",
        "- Duplicate Stage-7 controls are merged by strategy definition only after exact daily-series parity.",
        "",
        "## Reference-economics full-window ranking",
        "",
        markdown_table(
            reference_full,
            [
                "definition_id",
                "families",
                "net_return_points",
                "positive_day_share_pct",
                "daily_sharpe",
                "max_daily_drawdown_points",
                "fills",
            ],
        ),
        "",
        "## Today snapshot ranking",
        "",
        markdown_table(
            today,
            [
                "definition_id",
                "families",
                "net_return_points",
                "net_pnl_rs",
                "fills",
            ],
        ),
        "",
        "Today rows inherit each source package's end-of-day and data-completeness caveats.",
        "",
        "## Fixed holdout diagnostic",
        "",
        markdown_table(
            holdout,
            [
                "family",
                "train_winner",
                "train_winner_net_return_points",
                "test_net_return_points",
                "test_relative_rank_omega",
                "test_below_or_at_median",
            ],
        ),
        "",
        "This is one pre-existing 49/10 split, not PBO and not an independent prospective test.",
        "",
        "## Multiple-testing audit",
        "",
        markdown_table(
            multiple,
            [
                "family",
                "observations",
                "unique_strategy_definitions",
                "cscv_pbo_available",
                "cscv_reason",
                "dsr_available",
                "dsr_reason",
            ],
        ),
        "",
        "Strict CSCV uses equal contiguous blocks and does not trim the 59-session calendar. "
        "Because 59 is prime, no even equal-block partition exists. A reported PBO would therefore "
        "require dropping dates or using a non-CSCV approximation, neither of which this audit does.",
        "",
        "DSR is unavailable by default because the artifacts do not establish the total number of "
        "configurations tried before these challengers, and Stage 7 itself was selected after earlier "
        "stages. Loaded families are not pooled: gap guards, entry filters, exploratory combined "
        "profiles, and V6 challengers are different search families. Declaring only the visible "
        "variants as the trial universe would "
        "understate selection bias.",
        "",
        "## Cost stress",
        "",
        f"Cost-stress rows supplied by source packages: {len(costs)}.",
        "",
        "## Material caveats",
        "",
        "- Historical findings remain conditional on the frozen data/universe limitations in each source package.",
        "- Positive-day share and daily profit factor are day-level statistics; they are not trade win rate or trade profit factor.",
        "- Daily Sharpe uses additive strategy percentage-point returns with a zero benchmark; annualization assumes 252 sessions and is descriptive.",
        "- Cross-family rank comparisons are descriptive and are not evidence that the pooled set was a predeclared trial family.",
        "- Post-selection results need an untouched prospective period before any strategy change is considered.",
    ]
    if notices:
        lines.extend(["", "## Package notices", ""])
        lines.extend(f"- {notice}" for notice in notices)
    return "\n".join(lines) + "\n"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-root", type=Path, default=DEFAULT_RESEARCH_ROOT)
    parser.add_argument(
        "--package",
        action="append",
        type=Path,
        default=[],
        help="Package/run/comparison path. Repeat for multiple packages; omitted means auto-discover.",
    )
    parser.add_argument("--calendar-json", type=Path)
    parser.add_argument("--split-day", default=DEFAULT_SPLIT_DAY)
    parser.add_argument("--cscv-partitions", type=int, default=6)
    parser.add_argument(
        "--complete-trial-family",
        action="append",
        default=[],
        help="Explicitly declare a family as the complete trial universe for illustrative DSR.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        help="Defaults to <research-root>/v6_v10_challenger_robustness_audit_v1/comparisons.",
    )
    return parser.parse_args(argv)


def _expected_family_notices(
    loaded_families: set[str],
    *,
    repaired_v10_wrapper_loaded: bool,
) -> list[str]:
    expected_families = [
        "V10_STAGE7_GAP_GUARD",
        "V10_STAGE7_FILTERS",
        "V10_STAGE7_POSTSELECTION_COMBO",
        "V6_CHALLENGERS",
    ]
    if repaired_v10_wrapper_loaded:
        # The sealed repaired wrapper intentionally exposes its gap profiles
        # through the nested post-selection combo suite.  Do not mix in, or
        # demand, the legacy standalone gap package in this package mode.
        expected_families.remove("V10_STAGE7_GAP_GUARD")
    return [
        f"EXPECTED_FAMILY_NOT_LOADED:{family}"
        for family in expected_families
        if family not in loaded_families
    ]


def run_audit(args: argparse.Namespace) -> Path:
    research_root = args.research_root.resolve()
    notices: list[str] = []
    packages = [path.resolve() for path in args.package]
    if not packages:
        packages, notices = discover_packages(research_root)
    if not packages:
        raise ValueError("No completed packages supplied or discovered")
    loaded: list[DailySeries] = []
    loaded_package_paths: list[str] = []
    repaired_v10_wrapper_loaded = False
    for package in packages:
        try:
            package_series = load_package(package)
        except FileNotFoundError as error:
            notices.append(f"PACKAGE_NOT_READY:{package}:{error}")
            continue
        loaded.extend(package_series)
        loaded_package_paths.append(str(package))
        repaired_v10_wrapper_loaded = (
            repaired_v10_wrapper_loaded
            or _looks_like_repaired_v10_package(package)
        )
    if not loaded:
        raise ValueError("No completed daily result series were loaded")
    loaded_families = {item.family for item in loaded}
    notices.extend(
        _expected_family_notices(
            loaded_families,
            repaired_v10_wrapper_loaded=repaired_v10_wrapper_loaded,
        )
    )
    if args.calendar_json:
        calendar = calendar_from_json(args.calendar_json.resolve())
        calendar_source = args.calendar_json.resolve()
    else:
        calendar, calendar_source = calendar_from_gap(packages)
    if args.split_day not in calendar:
        raise ValueError(f"Split day {args.split_day} is not an official session")
    canonical, inventory = deduplicate_series(loaded)
    metrics = pd.DataFrame(metric_rows(canonical, calendar, args.split_day))
    costs = build_cost_stress(metrics)
    matrices = family_reference_matrices(canonical, calendar)
    holdout = pd.DataFrame(fixed_holdout_rows(matrices, calendar, args.split_day))
    multiple, dsr, pbo_detail = multiple_testing_outputs(
        matrices,
        {str(value).upper() for value in args.complete_trial_family},
        args.cscv_partitions,
    )
    aligned = aligned_daily_rows(canonical, calendar)
    output_root = (
        args.output_root.resolve()
        if args.output_root
        else research_root / "v6_v10_challenger_robustness_audit_v1" / "comparisons"
    )
    output_root.mkdir(parents=True, exist_ok=True)
    output = unique_output_dir(output_root)
    outputs: dict[str, pd.DataFrame | Sequence[Mapping[str, Any]]] = {
        "input_inventory.csv": inventory,
        "variant_metrics.csv": metrics,
        "today_metrics.csv": metrics.loc[metrics["dataset"].eq("TODAY")].copy(),
        "cost_stress.csv": costs,
        "fixed_holdout_audit.csv": holdout,
        "multiple_testing_audit.csv": multiple,
        "deflated_sharpe_audit.csv": dsr,
        "cscv_pbo_detail.csv": pbo_detail,
        "aligned_daily_returns.csv": aligned,
    }
    for name, value in outputs.items():
        write_csv(output / name, value)
    report = build_report(
        metrics=metrics,
        costs=costs,
        holdout=holdout,
        multiple=multiple,
        notices=notices,
        calendar=calendar,
        split_day=args.split_day,
    )
    (output / "report.md").write_text(report, encoding="utf-8")
    artifact_names = sorted([*outputs, "report.md"])
    artifacts = {
        name: {
            "path": str((output / name).resolve()),
            "bytes": (output / name).stat().st_size,
            "sha256": sha256_file(output / name),
        }
        for name in artifact_names
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "created_at_ist": datetime.now(IST).isoformat(),
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
        "packages": loaded_package_paths,
        "package_notices": notices,
        "calendar": {
            "source": str(calendar_source),
            "source_sha256": sha256_file(calendar_source),
            "session_dates": list(calendar),
            "calendar_sha256": canonical_sha256(list(calendar)),
            "split_day": args.split_day,
            "train_sessions": sum(value < args.split_day for value in calendar),
            "test_sessions": sum(value >= args.split_day for value in calendar),
        },
        "deduplication": {
            "raw_series": len(loaded),
            "canonical_series": len(canonical),
            "rule": "DATASET_SCENARIO_STRATEGY_DEFINITION_PLUS_EXACT_DAILY_SERIES_PARITY",
        },
        "multiple_testing": {
            "families_not_pooled": sorted(matrices),
            "preferred_cscv_partitions": args.cscv_partitions,
            "dates_trimmed": 0,
            "complete_trial_families_declared_by_caller": sorted(
                str(value).upper() for value in args.complete_trial_family
            ),
            "post_selection_conditional": True,
        },
        "limitations": [
            "UNKNOWN_TOTAL_HISTORICAL_TRIAL_UNIVERSE",
            "STAGE7_RETROSPECTIVELY_SELECTED_BEFORE_FOLLOWUP_CHALLENGERS",
            "STRICT_CSCV_UNAVAILABLE_WHEN_59_SESSIONS_CANNOT_FORM_EQUAL_EVEN_BLOCKS",
            "DSR_UNAVAILABLE_WITHOUT_DEFENSIBLE_COMPLETE_TRIAL_UNIVERSE",
            "SOURCE_PACKAGE_DATA_AND_UNIVERSE_LIMITATIONS_PROPAGATE",
        ],
        "artifacts": artifacts,
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(output)
    return output


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    run_audit(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
