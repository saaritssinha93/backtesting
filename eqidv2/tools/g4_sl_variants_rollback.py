"""Preview, archive, or restore only the files introduced by the G-4 SL study.

No Git operations or deletions are performed. Results and this utility survive
rollback, and content hashes protect later user edits from being moved.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sys
from typing import Any
import uuid


WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_ROOT = Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g_4/research")
ALLOWED_FILES = frozenset({
    "research/g4_separate_sl_variants.py",
    "tests/test_g4_separate_sl_variants.py",
    "configs/research/g4_profit_floor_020_v1.json",
    "configs/research/g4_atr_trailing_150_v1.json",
    "configs/research/g4_profit_floor_atr_150_v1.json",
    "docs/G4_SL_VARIANTS_2026-10-17.md",
})


class RollbackError(ValueError):
    """An unsafe or inconsistent rollback request."""


def sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise RollbackError(f"Expected a JSON object: {path}")
    return value


def _within(path: Path, root: Path) -> bool:
    return path != root and path.is_relative_to(root)


def _safe_path(value: str | Path, root: Path, *, must_exist: bool = True) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise RollbackError(f"An absolute path is required: {path}")
    resolved = path.resolve(strict=must_exist)
    if not _within(resolved, root):
        raise RollbackError(f"Path is outside the permitted directory: {path}")
    # Reject links/junctions even when they currently resolve inside the root.
    cursor = path
    while cursor != root and cursor != cursor.parent:
        if cursor.is_symlink() or getattr(cursor, "is_junction", lambda: False)():
            raise RollbackError(f"Links and junctions are not allowed: {cursor}")
        cursor = cursor.parent
    return resolved


def _manifest(
    run_dir: str | Path, workspace_root: Path, research_root: Path,
) -> tuple[Path, dict[str, Any], list[dict[str, str]]]:
    workspace_root = workspace_root.resolve(strict=True)
    research_root = research_root.resolve(strict=True)
    run_dir = _safe_path(run_dir, research_root)
    if not run_dir.is_dir() or run_dir.parent != research_root:
        raise RollbackError("--run-dir must be one result directory directly below the research root")
    manifest_path = _safe_path(run_dir / "rollback.json", run_dir)
    manifest = _load_json(manifest_path)
    declared_root = Path(manifest.get("workspace_root", ""))
    if not declared_root.is_absolute() or declared_root.resolve() != workspace_root:
        raise RollbackError("Manifest workspace_root does not match this workspace")
    if manifest.get("production_or_latest_files_changed") is not False:
        raise RollbackError("This utility only supports runs that left production/latest files unchanged")
    files = manifest.get("new_repository_files")
    hashes = manifest.get("new_repository_file_sha256")
    if not isinstance(files, list) or not files or not isinstance(hashes, dict):
        raise RollbackError("Manifest must contain a nonempty file list and its SHA-256 hashes")
    if any(not isinstance(item, str) for item in files) or set(files) != set(hashes):
        raise RollbackError("Manifest files and hash keys must match exactly")
    entries = []
    seen: set[Path] = set()
    for item in files:
        source = _safe_path(item, workspace_root, must_exist=False)
        relative = source.relative_to(workspace_root).as_posix()
        if relative not in ALLOWED_FILES or source in seen:
            raise RollbackError(f"Unapproved or duplicate rollback target: {source}")
        expected_hash = hashes[item]
        if (not isinstance(expected_hash, str) or len(expected_hash) != 64
                or any(char not in "0123456789abcdef" for char in expected_hash)):
            raise RollbackError(f"Invalid SHA-256 hash: {source}")
        seen.add(source)
        entries.append({"original": str(source), "relative": relative, "sha256": expected_hash})
    return run_dir, manifest, entries


def _check_hash(path: Path, expected: str) -> None:
    if not path.is_file() or sha256(path) != expected:
        raise RollbackError(f"File is missing or changed; preserving it: {path}")


def _write_receipt(path: Path, receipt: dict[str, Any]) -> None:
    pending = path.with_suffix(".pending.json")
    with pending.open("x", encoding="utf-8") as stream:
        json.dump(receipt, stream, indent=2)
        stream.write("\n")
    pending.replace(path)


def archive_run(
    run_dir: str | Path, *, apply: bool = False,
    workspace_root: Path = WORKSPACE_ROOT, research_root: Path = RESEARCH_ROOT,
) -> dict[str, Any]:
    """Validate the entire plan before moving anything; root overrides are for tests."""
    run_dir, _, entries = _manifest(run_dir, workspace_root, research_root)
    for entry in entries:
        _check_hash(Path(entry["original"]), entry["sha256"])
    plan: dict[str, Any] = {
        "action": "archive", "applied": False, "run_dir": str(run_dir),
        "files": entries,
        "note": "Research results, production files, rollback utility and its tests are retained.",
    }
    if not apply:
        return plan
    archive_parent = _safe_path(run_dir / "rollback_archive", run_dir, must_exist=False)
    archive_dir = archive_parent / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ") + "_" + uuid.uuid4().hex[:8]
    )
    _safe_path(archive_dir, run_dir, must_exist=False)
    archive_dir.mkdir(parents=True, exist_ok=False)
    receipt_path = archive_dir / "receipt.json"
    receipt = {
        "schema_version": 1, "state": "prepared", "run_dir": str(run_dir),
        "workspace_root": str(workspace_root.resolve()),
        "manifest_sha256": sha256(run_dir / "rollback.json"),
        "files": [{**entry, "archive": str(archive_dir / entry["relative"])} for entry in entries],
    }
    # Write first so an interrupted move still has a complete recovery plan.
    _write_receipt(receipt_path, receipt)
    try:
        for entry in receipt["files"]:
            source = _safe_path(entry["original"], workspace_root.resolve())
            _check_hash(source, entry["sha256"])
            destination = _safe_path(entry["archive"], archive_dir, must_exist=False)
            if destination.exists():
                raise RollbackError(f"Refusing to overwrite archive file: {destination}")
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source), str(destination))
        receipt["state"] = "archived"
        _write_receipt(receipt_path, receipt)
    except Exception:
        receipt["state"] = "interrupted"
        _write_receipt(receipt_path, receipt)
        raise
    return {**plan, "applied": True, "receipt": str(receipt_path)}


def restore_run(
    run_dir: str | Path, receipt_path: str | Path, *, apply: bool = False,
    workspace_root: Path = WORKSPACE_ROOT, research_root: Path = RESEARCH_ROOT,
) -> dict[str, Any]:
    run_dir, _, manifest_entries = _manifest(run_dir, workspace_root, research_root)
    receipt_path = _safe_path(receipt_path, run_dir)
    archive_dir = receipt_path.parent
    if (receipt_path.name != "receipt.json" or archive_dir.parent != run_dir / "rollback_archive"
            or archive_dir.name in {"", ".", ".."}):
        raise RollbackError("Receipt must be run-dir/rollback_archive/<archive-id>/receipt.json")
    receipt = _load_json(receipt_path)
    if (receipt.get("schema_version") != 1
            or receipt.get("state") not in {"prepared", "archived", "interrupted", "restored"}
            or receipt.get("workspace_root") != str(workspace_root.resolve())
            or receipt.get("run_dir") != str(run_dir)
            or receipt.get("manifest_sha256") != sha256(run_dir / "rollback.json")):
        raise RollbackError("Receipt identity or manifest hash does not match this run")
    expected = {entry["original"]: entry for entry in manifest_entries}
    entries = receipt.get("files")
    if not isinstance(entries, list) or len(entries) != len(expected):
        raise RollbackError("Receipt file list does not match the manifest")
    seen = set()
    restore = []
    retained = []
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("original") not in expected:
            raise RollbackError("Receipt contains an unapproved original file")
        original = entry["original"]
        if original in seen or any(entry.get(key) != value for key, value in expected[original].items()):
            raise RollbackError("Receipt contains duplicate or changed file metadata")
        seen.add(original)
        source = _safe_path(entry.get("archive", ""), archive_dir, must_exist=False)
        destination = _safe_path(original, workspace_root.resolve(), must_exist=False)
        if source != archive_dir / entry["relative"]:
            raise RollbackError(f"Unexpected archive path: {source}")
        if source.exists():
            _check_hash(source, entry["sha256"])
            if destination.exists():
                raise RollbackError(f"Refusing to overwrite a workspace file: {destination}")
            restore.append(entry)
        else:
            # Supports retries after an interrupted archive or restore.
            _check_hash(destination, entry["sha256"])
            retained.append(original)
    plan: dict[str, Any] = {
        "action": "restore", "applied": False, "run_dir": str(run_dir),
        "receipt": str(receipt_path), "files": restore, "already_present": retained,
    }
    if apply:
        for entry in restore:
            source = _safe_path(entry["archive"], archive_dir)
            _check_hash(source, entry["sha256"])
            destination = _safe_path(entry["original"], workspace_root.resolve(), must_exist=False)
            if destination.exists():
                raise RollbackError(f"Refusing to overwrite a workspace file: {destination}")
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source), str(destination))
        receipt["state"] = "restored"
        _write_receipt(receipt_path, receipt)
        plan["applied"] = True
    return plan


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path, help="Absolute study result directory")
    parser.add_argument("--restore", type=Path, metavar="RECEIPT", help="Restore a prior archive receipt")
    parser.add_argument("--apply", action="store_true", help="Apply the validated plan; default is preview")
    args = parser.parse_args(argv)
    try:
        result = (restore_run(args.run_dir, args.restore, apply=args.apply) if args.restore
                  else archive_run(args.run_dir, apply=args.apply))
    except (RollbackError, OSError, TypeError, json.JSONDecodeError) as error:
        print(f"Rollback refused: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
