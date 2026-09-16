"""Fail-closed manifest and run-vintage gate for V13 research dashboard data."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

import fno_oi_common as common


SCHEMA_VERSION = "FNO_V13_DASHBOARD_RUN_VINTAGE_V1"
DEFAULT_STATUS_DIR = common.FNO_ROOT / "strategy_research" / "v13_dashboard_vintage"
DEFAULT_FAMILIES = {
    "v13_v6_portfolio": common.FNO_ROOT / "strategy_research" / "v13_corrected_v6",
    "v13_v6_options": common.FNO_ROOT / "strategy_research" / "v13_corrected_v6_options",
    "v13_v6_option_selector": common.FNO_ROOT / "strategy_research" / "v13_corrected_v6_option_selector",
    "v13_v7_exit_shadow": common.FNO_ROOT / "strategy_research" / "v13_corrected_v7",
    "v13_v8_feature_shadow": common.FNO_ROOT / "strategy_research" / "v13_corrected_v8_feature_shadow",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _output_records(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        if "path" in value or "sha256" in value:
            yield value
        else:
            for child in value.values():
                yield from _output_records(child)
    elif isinstance(value, list):
        for child in value:
            yield from _output_records(child)


def validate_manifest(
    manifest_path: Path,
    *,
    required_data_through: str | None = None,
    max_age_hours: float | None = None,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    manifest_path = manifest_path.resolve()
    result: dict[str, Any] = {
        "manifest_path": str(manifest_path),
        "valid": False,
        "errors": [],
        "warnings": [],
    }
    if not manifest_path.is_file():
        result["errors"].append("MANIFEST_MISSING")
        return result
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        result["errors"].append("MANIFEST_UNREADABLE_OR_INVALID_JSON")
        return result
    if not isinstance(manifest, dict):
        result["errors"].append("MANIFEST_NOT_AN_OBJECT")
        return result

    run_dir = manifest_path.parent
    run_id = str(manifest.get("run_id", "")).strip()
    result.update(
        run_id=run_id,
        schema_version=str(manifest.get("schema_version", "")),
        generated_at_ist=manifest.get("generated_at_ist"),
        data_through_date=manifest.get("data_through_date"),
    )
    if manifest.get("complete") is not True:
        result["errors"].append("RUN_NOT_MARKED_COMPLETE")
    if not run_id:
        result["errors"].append("RUN_ID_MISSING")
    elif run_id != run_dir.name:
        result["errors"].append("RUN_ID_DIRECTORY_MISMATCH")
    if not result["schema_version"]:
        result["errors"].append("SCHEMA_VERSION_MISSING")

    try:
        generated = dt.datetime.fromisoformat(str(manifest.get("generated_at_ist")))
        if generated.tzinfo is None:
            raise ValueError("timezone missing")
        result["generated_at_ist"] = generated.isoformat()
    except (TypeError, ValueError):
        generated = None
        result["errors"].append("GENERATED_AT_INVALID_OR_TIMEZONE_MISSING")
    if max_age_hours is not None and generated is not None:
        clock = now or common.now_ist()
        age_hours = (clock.astimezone(generated.tzinfo) - generated).total_seconds() / 3600.0
        result["age_hours"] = age_hours
        if age_hours < -0.1:
            result["errors"].append("RUN_GENERATED_IN_FUTURE")
        elif age_hours > max_age_hours:
            result["errors"].append("RUN_EXCEEDS_MAX_AGE")

    data_through = manifest.get("data_through_date")
    if data_through is None:
        result["errors"].append("DATA_THROUGH_DATE_MISSING")
    else:
        try:
            normalized_data_through = dt.date.fromisoformat(str(data_through)).isoformat()
            result["data_through_date"] = normalized_data_through
        except ValueError:
            result["errors"].append("DATA_THROUGH_DATE_INVALID")
    if required_data_through is not None and result.get("data_through_date") != required_data_through:
        result["errors"].append("DATA_THROUGH_DATE_MISMATCH")

    output_records = list(_output_records(manifest.get("outputs")))
    if not output_records:
        result["errors"].append("OUTPUT_RECORDS_MISSING")
    checked_outputs: list[dict[str, Any]] = []
    for record in output_records:
        reported_path = str(record.get("path", "")).strip()
        expected_hash = str(record.get("sha256", "")).strip().lower()
        check: dict[str, Any] = {"path": reported_path, "valid": False}
        if not reported_path or not expected_hash:
            check["error"] = "OUTPUT_PATH_OR_HASH_MISSING"
            result["errors"].append("OUTPUT_PATH_OR_HASH_MISSING")
            checked_outputs.append(check)
            continue
        output_path = Path(reported_path).resolve()
        try:
            output_path.relative_to(run_dir)
        except ValueError:
            check["error"] = "OUTPUT_OUTSIDE_RUN_DIRECTORY"
            result["errors"].append("OUTPUT_OUTSIDE_RUN_DIRECTORY")
            checked_outputs.append(check)
            continue
        if not output_path.is_file():
            check["error"] = "OUTPUT_MISSING"
            result["errors"].append("OUTPUT_MISSING")
            checked_outputs.append(check)
            continue
        actual_hash = _sha256(output_path)
        check["actual_sha256"] = actual_hash
        if actual_hash != expected_hash:
            check["error"] = "OUTPUT_HASH_MISMATCH"
            result["errors"].append("OUTPUT_HASH_MISMATCH")
        else:
            check["valid"] = True
        checked_outputs.append(check)
    result["outputs"] = checked_outputs
    result["errors"] = sorted(set(result["errors"]))
    result["valid"] = not result["errors"]
    return result


def inspect_family(
    root: Path,
    *,
    required_data_through: str | None = None,
    max_age_hours: float | None = None,
) -> dict[str, Any]:
    root = root.resolve()
    manifests = sorted(
        root.glob("*/manifest.json") if root.is_dir() else [],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not manifests:
        return {
            "root": str(root),
            "status": "MISSING",
            "valid": False,
            "errors": ["NO_RUN_MANIFESTS"],
        }
    # Deliberately validate the newest run only. An invalid newest publication
    # must not silently fall back to an older green result on the dashboard.
    checked = validate_manifest(
        manifests[0],
        required_data_through=required_data_through,
        max_age_hours=max_age_hours,
    )
    checked["root"] = str(root)
    checked["candidate_manifest_count"] = len(manifests)
    checked["status"] = "READY" if checked["valid"] else "BLOCKED_INVALID_LATEST_RUN"
    return checked


def build_vintage_status(
    families: dict[str, Path],
    *,
    required_data_through: str | None = None,
    max_age_hours: float | None = None,
) -> dict[str, Any]:
    family_status = {
        name: inspect_family(
            root,
            required_data_through=required_data_through,
            max_age_hours=max_age_hours,
        )
        for name, root in families.items()
    }
    valid_dates = {
        str(record.get("data_through_date"))
        for record in family_status.values()
        if record.get("valid") and record.get("data_through_date")
    }
    all_valid = all(record.get("valid") for record in family_status.values())
    coherent = len(valid_dates) <= 1
    if not all_valid:
        status = "BLOCKED_INVALID_OR_MISSING_RUN"
    elif not coherent:
        status = "BLOCKED_MIXED_DATA_VINTAGES"
    else:
        status = "READY"
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_ist": common.now_ist().isoformat(timespec="seconds"),
        "status": status,
        "ready": status == "READY",
        "required_data_through": required_data_through,
        "coherent_data_through": next(iter(valid_dates)) if len(valid_dates) == 1 else None,
        "families": family_status,
    }


def render_markdown(status: dict[str, Any]) -> str:
    lines = [
        "# V13 Research Run Vintage Gate",
        "",
        f"- Gate: **{status['status']}**",
        f"- Generated: `{status['generated_at_ist']}`",
        f"- Coherent data through: `{status.get('coherent_data_through') or '-'}`",
        "",
        "| Family | Status | Run ID | Data through | Evidence |",
        "|---|---|---|---|---|",
    ]
    for name, record in status["families"].items():
        errors = ", ".join(record.get("errors", [])) or "hashes verified"
        lines.append(
            f"| {name} | {record.get('status', '-')} | {record.get('run_id', '-')} | "
            f"{record.get('data_through_date', '-')} | {errors} |"
        )
    lines.extend(
        [
            "",
            "The gate validates only the newest run in each family and never falls back to an older run when the newest manifest or output hashes fail.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--family",
        action="append",
        help="Family in NAME=ROOT form. Repeat to replace the default family set.",
    )
    parser.add_argument("--required-data-through")
    parser.add_argument("--max-age-hours", type=float)
    parser.add_argument("--status-dir", type=Path, default=DEFAULT_STATUS_DIR)
    parser.add_argument("--strict", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    families = DEFAULT_FAMILIES
    if args.family:
        families = {}
        for value in args.family:
            if "=" not in value:
                raise ValueError("--family must use NAME=ROOT")
            name, root = value.split("=", 1)
            families[name.strip()] = Path(root.strip())
    status = build_vintage_status(
        families,
        required_data_through=args.required_data_through,
        max_age_hours=args.max_age_hours,
    )
    status_dir = args.status_dir.resolve()
    status_dir.mkdir(parents=True, exist_ok=True)
    json_path = status_dir / "latest_v13_run_vintage.json"
    report_path = status_dir / "latest_v13_run_vintage.md"
    common.atomic_write_json(json_path, status)
    common.atomic_write_text(report_path, render_markdown(status))
    print(render_markdown(status))
    return 0 if status["ready"] or not args.strict else 2


if __name__ == "__main__":
    raise SystemExit(main())
