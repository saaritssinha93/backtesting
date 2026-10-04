"""Create an immutable V13-V10-G bundle with cutoff-scoped eligibility metadata.

The source bundle is treated as immutable. A complete copy is built in a
hidden sibling staging directory, the two eligibility artifacts are bounded to
the already-declared ``through_day``, provenance is rewritten, and the result
is published under a new name only after every invariant passes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import uuid
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


SCHEMA_VERSION = "V13_V10_G_CUTOFF_CORRECTED_BUNDLE_V1"
DATASET_MANIFEST = Path("dataset/dataset_manifest.json")
ELIGIBILITY_PARQUET = Path("dataset/eligibility.parquet")
ELIGIBILITY_CSV = Path("dataset/source_session_eligibility.csv")
SOURCE_MANIFEST_CSV = Path("dataset/source_manifest.csv")
RUN_METADATA = Path("g_backtest/run_metadata.json")
STRATEGY_OUTPUTS = (
    Path("g_backtest/selected_trades.csv"),
    Path("g_backtest/portfolio_trades.csv"),
    Path("g_backtest/summary.json"),
)
ALLOWED_CHANGED_FILES = {
    DATASET_MANIFEST.as_posix(),
    ELIGIBILITY_PARQUET.as_posix(),
    ELIGIBILITY_CSV.as_posix(),
    RUN_METADATA.as_posix(),
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
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


def write_json(path: Path, value: Any) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def artifact_inventory(root: Path, *, exclude: set[str] | None = None) -> dict[str, dict[str, Any]]:
    excluded = exclude or set()
    result: dict[str, dict[str, Any]] = {}
    for path in sorted((item for item in root.rglob("*") if item.is_file()), key=lambda item: item.as_posix()):
        relative = path.relative_to(root).as_posix()
        if relative in excluded:
            continue
        result[relative] = {"bytes": path.stat().st_size, "sha256": sha256_file(path)}
    return result


def _parse_day_series(frame: pd.DataFrame, label: str) -> pd.Series:
    if "day" not in frame:
        raise ValueError(f"{label} is missing day")
    parsed = pd.to_datetime(frame["day"], errors="coerce")
    if parsed.isna().any():
        raise ValueError(f"{label} contains invalid day values")
    return parsed.dt.date


def _normalize_eligibility(frame: pd.DataFrame, label: str) -> pd.DataFrame:
    result = frame.copy()
    result["day"] = _parse_day_series(result, label).astype(str)
    for column in result.select_dtypes(include=["object", "string"]).columns:
        result[column] = result[column].map(
            lambda value: None
            if pd.isna(value)
            else value.isoformat()
            if isinstance(value, (date, datetime, pd.Timestamp))
            else str(value)
        )
    return result.reset_index(drop=True)


def _assert_eligibility_parity(parquet: pd.DataFrame, csv: pd.DataFrame) -> None:
    left = _normalize_eligibility(parquet, "eligibility.parquet")
    right = _normalize_eligibility(csv, "source_session_eligibility.csv")
    if list(left.columns) != list(right.columns):
        raise ValueError("Eligibility parquet/CSV columns differ")
    try:
        pd.testing.assert_frame_equal(
            left,
            right,
            check_dtype=False,
            check_exact=False,
            rtol=1e-12,
            atol=1e-12,
        )
    except AssertionError as exc:
        raise ValueError("Eligibility parquet/CSV rows differ") from exc


def _verify_dataset_hashes(root: Path, manifest: dict[str, Any]) -> None:
    checksums = manifest.get("output_sha256")
    if not isinstance(checksums, dict) or not checksums:
        raise ValueError("Dataset manifest has no output_sha256 map")
    for name, expected in checksums.items():
        path = root / "dataset" / str(name)
        if not path.is_file():
            raise FileNotFoundError(f"Dataset output is missing: {path}")
        if sha256_file(path) != str(expected):
            raise ValueError(f"Dataset output hash mismatch: {name}")


def _assert_frame_cutoff(path: Path, cutoff: date) -> None:
    frame = pd.read_parquet(path, columns=["day"])
    days = _parse_day_series(frame, path.name)
    if not days.empty and days.max() > cutoff:
        raise ValueError(f"Non-eligibility dataset rows exceed cutoff: {path.name}")


def _assert_csv_cutoff(path: Path, cutoff: date) -> None:
    frame = pd.read_csv(path, usecols=["day"])
    days = _parse_day_series(frame, path.name)
    if not days.empty and days.max() > cutoff:
        raise ValueError(f"Strategy output rows exceed cutoff: {path.name}")


def _assert_path_cutoff(path: Path, cutoff: date) -> None:
    if not path.is_file():
        return
    exclusive_end = pd.Timestamp(datetime.combine(cutoff + timedelta(days=1), time.min), tz="Asia/Kolkata").value
    with np.load(path, allow_pickle=False) as archive:
        for name in archive.files:
            if not name.endswith("_timestamp_ns"):
                continue
            values = archive[name]
            if len(values) and int(np.max(values)) >= exclusive_end:
                raise ValueError(f"Forward path timestamps exceed cutoff: {name}")


def _assert_non_eligibility_scope(root: Path, cutoff: date) -> None:
    for name in ("signals", "annotated", "all_5m_features", "setup_audit", "path_quality"):
        path = root / "dataset" / f"{name}.parquet"
        if path.is_file():
            _assert_frame_cutoff(path, cutoff)
    _assert_path_cutoff(root / "dataset" / "paths.npz", cutoff)
    for relative in STRATEGY_OUTPUTS[:2]:
        _assert_csv_cutoff(root / relative, cutoff)


def _safe_cleanup_staging(staging: Path, parent: Path) -> None:
    resolved = staging.resolve()
    if resolved.parent != parent.resolve() or not resolved.name.startswith(".cutoff-correction-building-"):
        raise RuntimeError(f"Refusing unsafe staging cleanup: {resolved}")
    if resolved.exists():
        shutil.rmtree(resolved)


def _default_output(source: Path, generated: datetime) -> Path:
    stamp = generated.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return source.parent / f"{source.name}_cutoff_corrected_{stamp}"


def correct_bundle(
    source: Path | str,
    output: Path | str | None = None,
    *,
    through_day: str | date | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    source = Path(source).resolve()
    if not source.is_dir():
        raise FileNotFoundError(f"Source bundle does not exist: {source}")
    generated = now or datetime.now(timezone.utc)
    if generated.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    output = Path(output).resolve() if output is not None else _default_output(source, generated).resolve()
    if output.parent != source.parent or output == source:
        raise ValueError("Corrected output must be a new sibling of the source bundle")
    if output.exists():
        raise FileExistsError(f"Corrected output already exists: {output}")

    required = [DATASET_MANIFEST, ELIGIBILITY_PARQUET, ELIGIBILITY_CSV, SOURCE_MANIFEST_CSV, RUN_METADATA, *STRATEGY_OUTPUTS]
    missing = [relative.as_posix() for relative in required if not (source / relative).is_file()]
    if missing:
        raise FileNotFoundError(f"Source bundle is incomplete: {', '.join(missing)}")

    dataset_manifest = read_json(source / DATASET_MANIFEST)
    declared_text = str(dataset_manifest.get("through_day") or "")
    try:
        declared = date.fromisoformat(declared_text)
    except ValueError as exc:
        raise ValueError("Dataset manifest has an invalid through_day") from exc
    requested = date.fromisoformat(through_day) if isinstance(through_day, str) else through_day
    cutoff = requested or declared
    if cutoff != declared:
        raise ValueError(
            f"Requested cutoff {cutoff} differs from declared dataset cutoff {declared}"
        )

    run_metadata = read_json(source / RUN_METADATA)
    if str(run_metadata.get("through_day") or "") != declared_text:
        raise ValueError("Run metadata and dataset cutoff differ")
    recorded_manifest_hash = str(run_metadata.get("source_dataset_manifest_sha256") or "")
    observed_manifest_hash = sha256_file(source / DATASET_MANIFEST)
    if recorded_manifest_hash != observed_manifest_hash:
        raise ValueError("Run metadata does not identify the source dataset manifest")
    _verify_dataset_hashes(source, dataset_manifest)

    eligibility_parquet = pd.read_parquet(source / ELIGIBILITY_PARQUET)
    eligibility_csv = pd.read_csv(source / ELIGIBILITY_CSV)
    _assert_eligibility_parity(eligibility_parquet, eligibility_csv)
    days = _parse_day_series(eligibility_parquet, "eligibility.parquet")
    keep = days.le(cutoff)
    removed = eligibility_parquet.loc[~keep].copy()
    if removed.empty:
        raise ValueError("Source eligibility metadata is already bounded by through_day")
    _assert_non_eligibility_scope(source, cutoff)

    parent_inventory = artifact_inventory(source)
    parent_inventory_hash = canonical_sha256(parent_inventory)
    staging = source.parent / f".cutoff-correction-building-{uuid.uuid4().hex}"
    if staging.exists():
        raise FileExistsError(f"Staging path already exists: {staging}")

    try:
        shutil.copytree(source, staging, copy_function=shutil.copy2)
        copied_inventory = artifact_inventory(staging)
        if copied_inventory != parent_inventory:
            raise RuntimeError("Staged copy does not match the immutable parent")

        bounded = eligibility_parquet.loc[keep].copy().reset_index(drop=True)
        bounded["day"] = days.loc[keep].tolist()
        bounded.to_parquet(staging / ELIGIBILITY_PARQUET, index=False)
        bounded.to_csv(staging / ELIGIBILITY_CSV, index=False)
        _assert_eligibility_parity(
            pd.read_parquet(staging / ELIGIBILITY_PARQUET),
            pd.read_csv(staging / ELIGIBILITY_CSV),
        )
        bounded_days = _parse_day_series(bounded, "bounded eligibility")
        if not bounded_days.empty and bounded_days.max() > cutoff:
            raise RuntimeError("Corrected eligibility still exceeds cutoff")

        corrected_dataset_manifest = read_json(staging / DATASET_MANIFEST)
        corrected_dataset_manifest.setdefault("rows", {})["eligibility"] = int(len(bounded))
        corrected_dataset_manifest["eligibility_scope"] = {
            "through_day": cutoff.isoformat(),
            "rows": int(len(bounded)),
            "max_day": bounded_days.max().isoformat() if not bounded_days.empty else None,
            "post_cutoff_rows": 0,
        }
        checksums = corrected_dataset_manifest.setdefault("output_sha256", {})
        checksums["eligibility.parquet"] = sha256_file(staging / ELIGIBILITY_PARQUET)
        checksums["source_manifest.csv"] = sha256_file(staging / SOURCE_MANIFEST_CSV)
        checksums["source_session_eligibility.csv"] = sha256_file(staging / ELIGIBILITY_CSV)
        write_json(staging / DATASET_MANIFEST, corrected_dataset_manifest)
        _verify_dataset_hashes(staging, corrected_dataset_manifest)

        corrected_run_metadata = read_json(staging / RUN_METADATA)
        corrected_run_metadata["source_dataset"] = str(output / "dataset")
        corrected_run_metadata["source_dataset_manifest_sha256"] = sha256_file(
            staging / DATASET_MANIFEST
        )
        corrected_run_metadata["cutoff_metadata_correction"] = {
            "schema_version": SCHEMA_VERSION,
            "through_day": cutoff.isoformat(),
            "parent_bundle": str(source),
            "parent_dataset_manifest_sha256": observed_manifest_hash,
            "removed_rows": int(len(removed)),
            "removed_eligible_rows": int(
                removed.get("eligible", pd.Series(False, index=removed.index))
                .astype(bool)
                .sum()
            ),
            "strategy_outputs_changed": False,
        }
        write_json(staging / RUN_METADATA, corrected_run_metadata)

        current_parent = artifact_inventory(source)
        if current_parent != parent_inventory:
            raise RuntimeError("Parent bundle changed during correction")
        staged_before_manifest = artifact_inventory(staging)
        for relative, record in parent_inventory.items():
            if relative in ALLOWED_CHANGED_FILES:
                continue
            if staged_before_manifest.get(relative) != record:
                raise RuntimeError(f"Unexpected artifact changed: {relative}")
        strategy_hashes = {}
        for relative in STRATEGY_OUTPUTS:
            name = relative.as_posix()
            parent_hash = parent_inventory[name]["sha256"]
            corrected_hash = staged_before_manifest[name]["sha256"]
            if parent_hash != corrected_hash:
                raise RuntimeError(f"Strategy output changed: {name}")
            strategy_hashes[name] = parent_hash

        tool_path = Path(__file__).resolve()
        bundle_manifest = {
            "schema_version": SCHEMA_VERSION,
            "state": "COMPLETE",
            "generated_at_utc": generated.astimezone(timezone.utc).isoformat(),
            "bundle_path": str(output),
            "execution_authority": False,
            "promotion_eligible": False,
            "live_configuration_changed": False,
            "parent": {
                "path": str(source),
                "artifact_count": len(parent_inventory),
                "inventory_sha256": parent_inventory_hash,
                "artifacts": parent_inventory,
            },
            "transform": {
                "tool": str(tool_path),
                "tool_sha256": sha256_file(tool_path),
                "through_day": cutoff.isoformat(),
                "rows_before": int(len(eligibility_parquet)),
                "rows_after": int(len(bounded)),
                "removed_rows": int(len(removed)),
                "removed_days": sorted({value.isoformat() for value in days.loc[~keep]}),
                "removed_eligible_rows": int(
                    removed.get("eligible", pd.Series(False, index=removed.index))
                    .astype(bool)
                    .sum()
                ),
                "allowed_changed_files": sorted(ALLOWED_CHANGED_FILES),
                "strategy_outputs_unchanged": True,
                "strategy_output_sha256": strategy_hashes,
            },
            "artifact_manifest_excludes": ["bundle_manifest.json"],
            "artifacts": staged_before_manifest,
        }
        write_json(staging / "bundle_manifest.json", bundle_manifest)
        verified_artifacts = artifact_inventory(
            staging, exclude={"bundle_manifest.json"}
        )
        if verified_artifacts != bundle_manifest["artifacts"]:
            raise RuntimeError("Final artifact manifest verification failed")
        if output.exists():
            raise FileExistsError(f"Corrected output appeared during build: {output}")
        staging.rename(output)
    except BaseException:
        _safe_cleanup_staging(staging, source.parent)
        raise

    return {
        "schema_version": SCHEMA_VERSION,
        "state": "COMPLETE",
        "source": str(source),
        "output": str(output),
        "through_day": cutoff.isoformat(),
        "rows_before": int(len(eligibility_parquet)),
        "rows_after": int(keep.sum()),
        "removed_rows": int((~keep).sum()),
        "strategy_outputs_unchanged": True,
        "bundle_manifest": str(output / "bundle_manifest.json"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--through-day")
    args = parser.parse_args(argv)
    result = correct_bundle(
        args.source,
        args.output,
        through_day=args.through_day,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
