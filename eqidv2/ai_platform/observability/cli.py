"""Command-line diagnostics for the observability evidence layer.

The module intentionally imports only the Python standard library at import
time.  In particular, pandas-backed data-quality helpers are loaded only when
``data-quality`` is invoked, so the API's smaller virtual environment can
still import :mod:`ai_platform.observability` and this CLI module.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import os
import platform
import sys
import tempfile
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SCHEMA_VERSION = "eqidv2_observability_cli_v1"
BUNDLE_SCHEMA_VERSION = "eqidv2_observability_stage_bundle_v1"

# Bundle input is intentionally bounded.  This command is normally pointed at
# small decision snapshots, not at an unbounded market-data lake.  Keeping the
# limits here (rather than accepting command-line overrides) makes an operator
# invocation safe and repeatable.
MAX_STAGE_INPUTS = 256
MAX_STAGE_FILES = 512
MAX_STAGE_FILE_BYTES = 32 * 1024 * 1024
MAX_STAGE_TOTAL_BYTES = 128 * 1024 * 1024
MAX_STAGE_ROWS = 250_000
MAX_DIRECTORY_ENTRIES = 4_096
MAX_DIRECTORY_DEPTH = 8


# Ordered aliases are deliberately stage-specific.  In particular, a scanner
# snapshot's ``candidates`` must not accidentally become confirmation or P&L
# evidence.  ``rows`` and an existing ``stages`` bundle are handled before
# these native aliases.
_NATIVE_STAGE_FIELDS: dict[str, tuple[str, ...]] = {
    "universe": ("universe", "instruments", "contracts"),
    "raw_equity": ("raw_equity", "equity_bars", "bars"),
    "raw_futures_oi": ("raw_futures_oi", "futures_oi", "oi_bars"),
    "aggregate_5m": ("aggregate_5m", "five_minute_bars", "bars_5m"),
    "feature": ("feature_evaluations", "features", "feature_rows"),
    "base_gate": (
        "base_gate_evaluations",
        "base_evaluations",
        "feature_evaluations",
        "candidates",
    ),
    "confirmation": (
        "confirmation_evaluations",
        "confirmed_rows",
        "feature_evaluations",
    ),
    "setup_gate": (
        "setup_gate_evaluations",
        "setup_evaluations",
        "feature_evaluations",
        "candidates",
    ),
    "ranking": ("ranked_candidates", "ranking", "candidates"),
    # Prefer the durable IDs present in an on-disk confirmation snapshot.  An
    # in-memory snapshot may also contain full selected signal objects.
    "selection": (
        "selected_signal_ids",
        "selected_signals",
        "selected_records",
        "selected",
        "signals",
        "_selected_signals",
    ),
    "execution": ("executions", "orders", "order_states"),
    "exit": ("exits", "closed_orders", "order_states"),
    "pnl": ("pnl", "trades", "closed_trades"),
}

_DIRECT_RECORD_IDENTIFIERS: dict[str, tuple[str, ...]] = {
    "universe": ("tradingsymbol", "symbol", "instrument_token"),
    "raw_equity": ("tradingsymbol", "symbol"),
    "raw_futures_oi": ("futures_tradingsymbol", "tradingsymbol", "symbol"),
    "aggregate_5m": ("tradingsymbol", "symbol"),
    "feature": ("tradingsymbol", "symbol"),
    "base_gate": ("tradingsymbol", "symbol"),
    "confirmation": ("signal_id", "tradingsymbol", "symbol"),
    "setup_gate": ("signal_id", "tradingsymbol", "symbol"),
    "ranking": ("signal_id", "tradingsymbol", "symbol"),
    "selection": ("signal_id",),
    "execution": ("order_id", "broker_order_id", "signal_id"),
    "exit": ("order_id", "broker_order_id", "signal_id"),
    "pnl": ("order_id", "broker_order_id", "signal_id"),
}


class CommandError(ValueError):
    """A concise, user-correctable CLI error."""


def _emit(payload: Mapping[str, Any], *, stream: Any = None) -> None:
    target = stream if stream is not None else sys.stdout
    target.write(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError) as exc:
        raise CommandError(f"cannot read {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise CommandError(f"invalid JSON in {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CommandError(f"{path} must contain a JSON object")
    return value


def _read_csv_records(path: Path) -> list[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise CommandError(f"{path} has no CSV header")
            return [dict(row) for row in reader]
    except (OSError, UnicodeError, csv.Error) as exc:
        raise CommandError(f"cannot read {path}: {exc}") from exc


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _native_payload_value(value: Any) -> Any:
    """Mirror data_quality.canonical_payload_sha256 for JSON-loaded values."""

    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if value != value:
            return {"__float__": "nan"}
        if value == float("inf"):
            return {"__float__": "inf"}
        if value == float("-inf"):
            return {"__float__": "-inf"}
        return 0.0 if value == 0 else value
    if isinstance(value, Mapping):
        return {
            str(key): _native_payload_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, list):
        return [_native_payload_value(item) for item in value]
    raise TypeError(f"unsupported native payload value: {type(value).__name__}")


def _native_payload_sha256(value: Any) -> str:
    encoded = json.dumps(
        _native_payload_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _evidence_payload_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        encoded = json.dumps(
            payload, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False
        ) + "\n"
    except (TypeError, ValueError) as exc:
        raise CommandError(f"bundle contains a non-JSON value: {exc}") from exc
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent)
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise
    return target


def _is_link_like(path: Path) -> bool:
    """Return true for symlinks and Windows directory junctions."""

    try:
        if path.is_symlink():
            return True
        is_junction = getattr(path, "is_junction", None)
        return bool(is_junction and is_junction())
    except OSError as exc:
        raise CommandError(f"cannot inspect stage input {path}: {exc}") from exc


def _path_is_within(path: Path, root: Path) -> bool:
    try:
        return os.path.commonpath((str(root), str(path))) == str(root)
    except ValueError:
        # Windows paths on different drives cannot share a common path.
        return False


def _file_size(path: Path) -> int:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise CommandError(f"cannot inspect stage input {path}: {exc}") from exc
    if size > MAX_STAGE_FILE_BYTES:
        raise CommandError(
            f"stage input {path} is {size} bytes; maximum is "
            f"{MAX_STAGE_FILE_BYTES} bytes per file"
        )
    return size


def _directory_files(path: Path) -> list[Path]:
    """Return bounded, deterministic CSV/JSON files without following links."""

    root = path.resolve(strict=True)
    pending: list[tuple[Path, int]] = [(root, 0)]
    files: list[Path] = []
    entries_seen = 0
    while pending:
        current, depth = pending.pop()
        if depth > MAX_DIRECTORY_DEPTH:
            raise CommandError(
                f"stage input directory {path} exceeds maximum depth "
                f"{MAX_DIRECTORY_DEPTH}"
            )
        try:
            entries = sorted(current.iterdir(), key=lambda item: item.name.casefold())
        except OSError as exc:
            raise CommandError(f"cannot list stage input directory {current}: {exc}") from exc
        entries_seen += len(entries)
        if entries_seen > MAX_DIRECTORY_ENTRIES:
            raise CommandError(
                f"stage input directory {path} exceeds maximum of "
                f"{MAX_DIRECTORY_ENTRIES} entries"
            )
        child_directories: list[Path] = []
        for entry in entries:
            if _is_link_like(entry):
                raise CommandError(f"stage input directories may not contain links: {entry}")
            try:
                resolved = entry.resolve(strict=True)
                if not _path_is_within(resolved, root):
                    raise CommandError(
                        f"stage input escapes its declared directory: {entry}"
                    )
                if entry.is_dir():
                    child_directories.append(entry)
                elif entry.is_file() and entry.suffix.lower() in {".csv", ".json"}:
                    files.append(entry)
                elif not entry.is_file():
                    raise CommandError(f"unsupported stage input filesystem entry: {entry}")
            except OSError as exc:
                raise CommandError(f"cannot inspect stage input {entry}: {exc}") from exc
        # Reverse because this is a stack; the final file list is sorted again
        # below, but deterministic traversal also makes limit failures stable.
        pending.extend((child, depth + 1) for child in reversed(child_directories))
    if not files:
        raise CommandError(f"stage input directory {path} contains no CSV/JSON files")
    return sorted(files, key=lambda item: item.relative_to(root).as_posix().casefold())


def _expand_stage_input(path: Path, output: Path) -> list[Path]:
    if _is_link_like(path):
        raise CommandError(f"stage inputs may not be links: {path}")
    if not path.exists():
        raise CommandError(f"stage input does not exist: {path}")
    resolved_output = output.resolve(strict=False)
    resolved_input = path.resolve(strict=True)
    if path.is_file():
        if resolved_input == resolved_output:
            raise CommandError("bundle output must not overwrite a stage input file")
        if path.suffix.lower() not in {".csv", ".json"}:
            raise CommandError(
                f"stage input {path} must have a .csv or .json extension"
            )
        return [path]
    if not path.is_dir():
        raise CommandError(f"stage input must be a file or directory: {path}")
    if _path_is_within(resolved_output, resolved_input):
        raise CommandError(
            "bundle output must be outside every stage input directory"
        )
    return _directory_files(path)


def _read_json_bounded(path: Path) -> Any:
    _file_size(path)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError) as exc:
        raise CommandError(f"cannot read {path}: {exc}") from exc
    except (json.JSONDecodeError, RecursionError) as exc:
        raise CommandError(f"invalid JSON in {path}: {exc}") from exc


def _validate_claimed_digest(
    path: Path,
    *,
    label: str,
    claimed: Any,
    value: Any,
    hasher: Any = _canonical_sha256,
) -> None:
    try:
        observed = hasher(value)
    except (TypeError, ValueError, RecursionError) as exc:
        raise CommandError(f"cannot hash {label} in {path}: {exc}") from exc
    if str(claimed).strip().lower() != observed:
        raise CommandError(f"{path} {label} digest mismatch")


def _unwrap_json_integrity(value: Any, path: Path) -> Any:
    """Validate and unwrap a native immutable-evidence envelope or bundle."""

    if not isinstance(value, dict):
        return value
    if value.get("content_sha256") is not None:
        unsigned = {key: item for key, item in value.items() if key != "content_sha256"}
        _validate_claimed_digest(
            path,
            label="stage-bundle content_sha256",
            claimed=value["content_sha256"],
            value=unsigned,
        )
    if value.get("payload_sha256") is not None and isinstance(value.get("payload"), dict):
        _validate_claimed_digest(
            path,
            label="evidence payload_sha256",
            claimed=value["payload_sha256"],
            value=value["payload"],
            hasher=_evidence_payload_sha256,
        )
        return value["payload"]
    return value


def _native_records(
    stage: str, value: Any, path: Path
) -> tuple[list[dict[str, Any]], str]:
    """Extract one stage from a generic file or a native live snapshot."""

    value = _unwrap_json_integrity(value, path)
    if isinstance(value, list):
        if not all(isinstance(row, dict) for row in value):
            raise CommandError(f"{path} JSON list must contain only objects")
        return [dict(row) for row in value], "list"
    if not isinstance(value, dict):
        raise CommandError(f"{path} must contain a JSON object or list of objects")

    stages = value.get("stages")
    if isinstance(stages, dict) and stage in stages:
        rows = stages[stage]
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise CommandError(
                f"stage {stage!r} in bundle {path} must be a list of objects"
            )
        return [dict(row) for row in rows], f"stages.{stage}"

    if "rows" in value:
        rows = value["rows"]
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise CommandError(f"{path} field 'rows' must be a list of objects")
        return [dict(row) for row in rows], "rows"

    for field in _NATIVE_STAGE_FIELDS[stage]:
        if field not in value:
            continue
        native = value[field]
        if not isinstance(native, list):
            raise CommandError(f"{path} field {field!r} must be a list")
        digest_field = f"{field}_sha256"
        if digest_field in value:
            _validate_claimed_digest(
                path,
                label=digest_field,
                claimed=value[digest_field],
                value=native,
                hasher=_native_payload_sha256,
            )
        count_fields = [f"{field}_count"]
        if field == "feature_evaluations":
            count_fields.insert(0, "feature_evaluation_count")
        for count_field in count_fields:
            if count_field in value:
                try:
                    claimed_count = int(value[count_field])
                except (TypeError, ValueError) as exc:
                    raise CommandError(
                        f"{path} field {count_field!r} must be an integer"
                    ) from exc
                if claimed_count != len(native):
                    raise CommandError(f"{path} field {count_field!r} count mismatch")
                break

        if all(isinstance(row, dict) for row in native):
            rows = [dict(row) for row in native]
        elif stage == "selection" and field == "selected_signal_ids" and all(
            isinstance(item, str) and item.strip() for item in native
        ):
            rows = [{"signal_id": item.strip()} for item in native]
        else:
            raise CommandError(
                f"{path} field {field!r} must contain objects"
                + (" or non-empty signal IDs" if stage == "selection" else "")
            )

        # Carry only causal identity needed to compare native child records.
        # This turns scanner candidates (which use ``signal_timestamp``) into
        # directly keyable records without inventing feature values.
        session_date = value.get("session_date")
        signal_end = value.get("signal_end")
        confirmation_end = value.get("confirmation_end")
        for row in rows:
            if session_date is not None:
                row.setdefault("session_date", session_date)
            if signal_end is not None:
                row.setdefault("signal_end", signal_end)
            if confirmation_end is not None:
                row.setdefault("confirmation_end", confirmation_end)
            if "signal_ts" not in row and row.get("signal_timestamp") is not None:
                row["signal_ts"] = row["signal_timestamp"]
        return rows, field

    identifiers = _DIRECT_RECORD_IDENTIFIERS[stage]
    if any(value.get(field) not in (None, "") for field in identifiers):
        return [dict(value)], "record"

    expected = ", ".join(_NATIVE_STAGE_FIELDS[stage])
    raise CommandError(
        f"{path} has no records for stage {stage!r}; expected a JSON list, "
        f"rows, stages.{stage}, or one of: {expected}"
    )


def _read_stage_file(
    stage: str, path: Path, *, session_date: str
) -> tuple[list[dict[str, Any]], str, int]:
    size = _file_size(path)
    if path.suffix.lower() == ".csv":
        rows: list[dict[str, Any]] = list(_read_csv_records(path))
        selector = "csv"
    else:
        value = _read_json_bounded(path)
        declared_dates: set[str] = set()
        if isinstance(value, dict):
            if value.get("session_date") not in (None, ""):
                declared_dates.add(str(value["session_date"])[:10])
            payload = value.get("payload")
            if isinstance(payload, dict) and payload.get("session_date") not in (
                None,
                "",
            ):
                declared_dates.add(str(payload["session_date"])[:10])
        wrong_declared_dates = sorted(declared_dates.difference({session_date}))
        if wrong_declared_dates:
            raise CommandError(
                f"stage input {path} declares session date(s) outside "
                f"{session_date}: " + ", ".join(wrong_declared_dates)
            )
        rows, selector = _native_records(stage, value, path)
    mismatched_dates = sorted(
        {
            str(row.get("session_date"))[:10]
            for row in rows
            if row.get("session_date") not in (None, "")
            and str(row.get("session_date"))[:10] != session_date
        }
    )
    if mismatched_dates:
        raise CommandError(
            f"stage input {path} contains session date(s) outside {session_date}: "
            + ", ".join(mismatched_dates)
        )
    return rows, selector, size


def _record_sort_key(row: Mapping[str, Any]) -> str:
    try:
        return json.dumps(
            row,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (TypeError, ValueError, RecursionError) as exc:
        raise CommandError(f"stage record is not canonical JSON: {exc}") from exc


def _split_fields(values: Iterable[str] | None) -> tuple[str, ...]:
    fields: list[str] = []
    for value in values or ():
        for part in value.split(","):
            name = part.strip()
            if name and name not in fields:
                fields.append(name)
    return tuple(fields)


def _doctor(_: argparse.Namespace) -> int:
    capabilities: dict[str, dict[str, Any]] = {
        "core": {"available": True, "required": True},
        "reconciliation": {"available": True, "required": False},
        "event_journal": {"available": True, "required": False},
        "profitability": {"available": True, "required": False},
    }
    for package in ("numpy", "pandas"):
        capabilities[package] = {
            "available": importlib.util.find_spec(package) is not None,
            "required": False,
        }
    capabilities["data_quality"] = {
        "available": capabilities["numpy"]["available"]
        and capabilities["pandas"]["available"],
        "required": False,
        "requires": ["numpy", "pandas"],
    }
    optional_missing = sorted(
        name
        for name, state in capabilities.items()
        if not state["available"] and not state["required"]
    )
    _emit(
        {
            "schema_version": SCHEMA_VERSION,
            "command": "doctor",
            "status": "READY" if not optional_missing else "READY_WITH_OPTIONAL_GAPS",
            "python": platform.python_version(),
            "capabilities": capabilities,
            "optional_missing": optional_missing,
        }
    )
    return 0


def _bundle_session_date(path: Path) -> str | None:
    value = _read_json_object(path).get("session_date")
    return str(value).strip() if value is not None and str(value).strip() else None


def _bundle(args: argparse.Namespace) -> int:
    from .reconciliation import STAGE_ORDER, compare_stage

    try:
        parsed_date = date.fromisoformat(args.session_date)
    except ValueError as exc:
        raise CommandError("--session-date must be YYYY-MM-DD") from exc
    if parsed_date.isoformat() != args.session_date:
        raise CommandError("--session-date must be YYYY-MM-DD")

    if len(args.stage) > MAX_STAGE_INPUTS:
        raise CommandError(
            f"bundle accepts at most {MAX_STAGE_INPUTS} --stage specifications"
        )

    stage_paths: dict[str, list[Path]] = {}
    for specification in args.stage:
        stage, separator, raw_path = specification.partition("=")
        stage = stage.strip()
        raw_path = raw_path.strip()
        if not separator or not stage or not raw_path:
            raise CommandError("each --stage must use STAGE=PATH")
        if stage not in STAGE_ORDER:
            raise CommandError(
                f"unknown stage {stage!r}; expected one of: {', '.join(STAGE_ORDER)}"
            )
        stage_paths.setdefault(stage, []).append(Path(raw_path))

    stages: dict[str, list[dict[str, Any]]] = {}
    stage_file_counts: dict[str, int] = {}
    selectors: dict[str, dict[str, int]] = {}
    seen_files: set[tuple[str, str]] = set()
    total_files = 0
    total_bytes = 0
    total_rows = 0
    for stage in STAGE_ORDER:
        if stage not in stage_paths:
            continue
        files: list[Path] = []
        for path in stage_paths[stage]:
            files.extend(_expand_stage_input(path, args.output))
        # Input argument order and filesystem enumeration order must not affect
        # the resulting bundle digest.
        files = sorted(
            files,
            key=lambda item: os.path.normcase(str(item.resolve(strict=True))),
        )
        if len(files) > MAX_STAGE_FILES:
            raise CommandError(
                f"stage {stage!r} expands to {len(files)} files; maximum is "
                f"{MAX_STAGE_FILES}"
            )
        stage_rows: list[dict[str, Any]] = []
        selector_counts: dict[str, int] = {}
        stage_bytes = 0
        for path in files:
            identity = os.path.normcase(str(path.resolve(strict=True)))
            stage_identity = (stage, identity)
            if stage_identity in seen_files:
                raise CommandError(
                    f"stage input file was selected more than once for "
                    f"{stage!r}: {path}"
                )
            seen_files.add(stage_identity)
            rows, selector, size = _read_stage_file(
                stage, path, session_date=args.session_date
            )
            stage_rows.extend(rows)
            stage_bytes += size
            selector_counts[selector] = selector_counts.get(selector, 0) + 1
            if stage_bytes > MAX_STAGE_TOTAL_BYTES:
                raise CommandError(
                    f"stage {stage!r} exceeds maximum input size of "
                    f"{MAX_STAGE_TOTAL_BYTES} bytes"
                )
            if len(stage_rows) > MAX_STAGE_ROWS:
                raise CommandError(
                    f"stage {stage!r} exceeds maximum of {MAX_STAGE_ROWS} records"
                )
        stage_rows.sort(key=_record_sort_key)
        stages[stage] = stage_rows
        stage_file_counts[stage] = len(files)
        selectors[stage] = dict(sorted(selector_counts.items()))
        total_files += len(files)
        total_bytes += stage_bytes
        total_rows += len(stage_rows)
        if total_files > MAX_STAGE_FILES:
            raise CommandError(
                f"bundle expands to more than {MAX_STAGE_FILES} input files"
            )
        if total_bytes > MAX_STAGE_TOTAL_BYTES:
            raise CommandError(
                f"bundle exceeds maximum input size of {MAX_STAGE_TOTAL_BYTES} bytes"
            )
        if total_rows > MAX_STAGE_ROWS:
            raise CommandError(
                f"bundle exceeds maximum of {MAX_STAGE_ROWS} records"
            )

    diagnostics: dict[str, dict[str, Any]] = {}
    for stage, rows in stages.items():
        self_comparison = compare_stage(stage, rows, rows)
        diagnostic: dict[str, Any] = {
            "record_count": len(rows),
            "comparison_state": self_comparison.state.value,
            "reason": self_comparison.reason,
        }
        if self_comparison.reason == "duplicate_comparison_keys":
            diagnostic["duplicate_keys"] = [
                list(key) for key in self_comparison.left_only
            ]
        diagnostics[stage] = diagnostic

    payload: dict[str, Any] = {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "session_date": args.session_date,
        "kind": args.kind,
        "stages": stages,
        "stage_diagnostics": diagnostics,
    }
    payload["content_sha256"] = _canonical_sha256(payload)
    target = _write_json_atomic(args.output, payload)
    _emit(
        {
            "schema_version": SCHEMA_VERSION,
            "command": "bundle",
            "status": "COMPLETE",
            "session_date": args.session_date,
            "kind": args.kind,
            "stage_count": len(stages),
            "row_count": sum(len(rows) for rows in stages.values()),
            "source_file_count": total_files,
            "source_bytes": total_bytes,
            "stage_file_counts": stage_file_counts,
            "native_selectors": selectors,
            "indeterminate_stages": [
                stage
                for stage, diagnostic in diagnostics.items()
                if diagnostic["comparison_state"] == "INDETERMINATE"
            ],
            "content_sha256": payload["content_sha256"],
            "output": str(target),
        }
    )
    return 0


def _reconcile(args: argparse.Namespace) -> int:
    from .reconciliation import (
        load_stage_bundle,
        reconcile_live_observed_final,
        write_report,
    )

    paths = (args.live, args.observed, args.finalized)
    declared_dates = {date for path in paths if (date := _bundle_session_date(path))}
    if args.session_date:
        if declared_dates and declared_dates != {args.session_date}:
            raise CommandError(
                "--session-date conflicts with a bundle session_date: "
                + ", ".join(sorted(declared_dates))
            )
        session_date = args.session_date
    elif len(declared_dates) == 1:
        session_date = next(iter(declared_dates))
    elif not declared_dates:
        raise CommandError(
            "session date is missing; add session_date to a bundle or pass --session-date"
        )
    else:
        raise CommandError(
            "bundle session dates disagree: " + ", ".join(sorted(declared_dates))
        )

    report = reconcile_live_observed_final(
        session_date=session_date,
        live=load_stage_bundle(args.live),
        observed=load_stage_bundle(args.observed),
        finalized=load_stage_bundle(args.finalized),
    )
    target = write_report(args.output, report)
    _emit(
        {
            "schema_version": SCHEMA_VERSION,
            "command": "reconcile",
            "status": "COMPLETE" if report["diagnosis"]["actionable"] else "INDETERMINATE",
            "session_date": session_date,
            "output": str(target),
            "report_sha256": report["report_sha256"],
            "diagnosis": report["diagnosis"],
        }
    )
    return 0 if report["diagnosis"]["actionable"] else 1


def _data_quality(args: argparse.Namespace) -> int:
    try:
        import pandas as pd

        from .data_quality import evaluate_ohlcv
    except ImportError as exc:
        raise CommandError(
            "data-quality requires pandas and numpy in the active Python environment"
        ) from exc

    try:
        frame = pd.read_csv(args.input)
    except (OSError, ValueError) as exc:
        raise CommandError(f"cannot read CSV {args.input}: {exc}") from exc
    if args.timestamp_column not in frame.columns:
        raise CommandError(
            f"timestamp column {args.timestamp_column!r} is missing from {args.input}"
        )

    expected = None
    if args.expected_interval:
        stamps = pd.to_datetime(frame[args.timestamp_column], errors="coerce").dropna()
        if len(stamps):
            try:
                expected = pd.date_range(
                    start=stamps.min(), end=stamps.max(), freq=args.expected_interval
                )
            except (TypeError, ValueError) as exc:
                raise CommandError(
                    f"invalid --expected-interval {args.expected_interval!r}: {exc}"
                ) from exc

    report = evaluate_ohlcv(
        frame,
        timestamp_column=args.timestamp_column,
        expected_timestamps=expected,
        source=args.source,
        symbol=args.symbol,
    )
    payload = report.to_dict()
    payload.update(
        {
            "command": "data-quality",
            "input": str(args.input),
            "expected_interval": args.expected_interval,
        }
    )
    _emit(payload)
    return 0 if report.status == "GOOD" else 1


def _verify_observations(args: argparse.Namespace) -> int:
    try:
        from .data_quality import AppendOnlyObservationLedger
    except ImportError as exc:
        raise CommandError(
            "verify-observations requires pandas and numpy in the active Python environment"
        ) from exc

    verification = AppendOnlyObservationLedger(args.root).verify()
    payload = verification.to_dict()
    payload.update(
        {
            "schema_version": SCHEMA_VERSION,
            "command": "verify-observations",
            "root": str(args.root),
            "status": "VALID" if verification.valid else "INVALID",
        }
    )
    _emit(payload)
    return 0 if verification.valid else 1


def _verify_events(args: argparse.Namespace) -> int:
    from .journal import AppendOnlyEventJournal

    verification = AppendOnlyEventJournal(
        args.journal, service=args.service, strict=True
    ).verify()
    payload = asdict(verification)
    payload.update(
        {
            "schema_version": SCHEMA_VERSION,
            "command": "verify-events",
            "journal": str(args.journal),
            "service": args.service,
            "status": "VALID" if verification.valid else "INVALID",
        }
    )
    _emit(payload)
    return 0 if verification.valid else 1


def _profitability(args: argparse.Namespace) -> int:
    from .profitability import group_performance, summarize_performance

    records = _read_csv_records(args.input)
    group_by = _split_fields(args.group_by)
    missing = [name for name in group_by if records and name not in records[0]]
    if missing:
        raise CommandError("group-by columns are missing: " + ", ".join(missing))
    summary = summarize_performance(
        records, minimum_closed_trades=args.min_trades
    )
    groups = (
        group_performance(
            records,
            group_by=group_by,
            minimum_closed_trades=args.min_trades,
        )
        if group_by
        else []
    )
    _emit(
        {
            "schema_version": SCHEMA_VERSION,
            "command": "profitability",
            "input": str(args.input),
            "minimum_closed_trades": args.min_trades,
            "group_by": list(group_by),
            "summary": summary.as_dict(),
            "groups": [group.as_dict() for group in groups],
        }
    )
    return 0 if summary.evidence_state == "SUFFICIENT" else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ai_platform.observability",
        description="Inspect observability evidence without starting trading processes.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    doctor = commands.add_parser(
        "doctor", help="report core and optional observability capabilities"
    )
    doctor.set_defaults(handler=_doctor)

    bundle = commands.add_parser(
        "bundle",
        help="assemble CSV/JSON and native live snapshots for reconciliation",
        description=(
            "Build one digest-verified reconciliation bundle. A stage PATH may "
            "be a CSV/JSON file or a directory; repeat the same STAGE for "
            "multiple inputs."
        ),
        epilog=(
            "Native JSON extraction is stage-aware: scanner/confirmation "
            "feature_evaluations feed feature and gate stages; candidates feed "
            "base/ranking stages; selected_signal_ids, selected_signals, "
            "signals, and selected records feed selection; orders and trades "
            "feed execution/exit/P&L. Immutable evidence payload_sha256, "
            "embedded list hashes, and existing bundle content_sha256 values "
            "are verified. Directories are scanned recursively in deterministic "
            f"order (maximum {MAX_STAGE_FILES} files, {MAX_DIRECTORY_DEPTH} "
            f"levels, {MAX_STAGE_ROWS} total records); links are rejected. "
            "Duplicate comparison keys are retained and reported as "
            "INDETERMINATE, never silently deduplicated."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    bundle.add_argument("--session-date", required=True, help="YYYY-MM-DD")
    bundle.add_argument(
        "--kind", choices=("live", "observed", "finalized"), required=True
    )
    bundle.add_argument("--output", type=Path, required=True)
    bundle.add_argument(
        "--stage",
        action="append",
        required=True,
        metavar="STAGE=PATH",
        help=(
            "stage name plus CSV/JSON file or directory; repeat for another "
            "stage or for additional files in the same stage"
        ),
    )
    bundle.set_defaults(handler=_bundle)

    reconcile = commands.add_parser(
        "reconcile", help="compare live, observed-replay, and finalized-replay stages"
    )
    reconcile.add_argument("--live", type=Path, required=True)
    reconcile.add_argument("--observed", type=Path, required=True)
    reconcile.add_argument("--finalized", type=Path, required=True)
    reconcile.add_argument("--output", type=Path, required=True)
    reconcile.add_argument(
        "--session-date",
        help="YYYY-MM-DD; optional when a bundle declares session_date",
    )
    reconcile.set_defaults(handler=_reconcile)

    quality = commands.add_parser(
        "data-quality", help="validate and fingerprint one OHLCV/OI CSV"
    )
    quality.add_argument("--input", type=Path, required=True)
    quality.add_argument("--timestamp-column", default="ts")
    quality.add_argument(
        "--expected-interval",
        help="pandas interval such as 1min or 5min; detects internal time gaps",
    )
    quality.add_argument("--source", default="")
    quality.add_argument("--symbol", default="")
    quality.set_defaults(handler=_data_quality)

    observations = commands.add_parser(
        "verify-observations", help="verify immutable raw-observation records"
    )
    observations.add_argument("--root", type=Path, required=True)
    observations.set_defaults(handler=_verify_observations)

    events = commands.add_parser(
        "verify-events", help="verify an append-only event journal hash chain"
    )
    events.add_argument("--journal", type=Path, required=True)
    events.add_argument("--service", required=True)
    events.set_defaults(handler=_verify_events)

    profit = commands.add_parser(
        "profitability", help="summarize closed and unresolved trade evidence"
    )
    profit.add_argument("--input", type=Path, required=True)
    profit.add_argument(
        "--group-by",
        action="append",
        help="column or comma-separated columns; may be repeated",
    )
    profit.add_argument("--min-trades", type=int, default=20)
    profit.set_defaults(handler=_profitability)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "min_trades", 1) < 1:
        parser.error("--min-trades must be at least 1")
    try:
        return int(args.handler(args))
    except CommandError as exc:
        _emit(
            {
                "schema_version": SCHEMA_VERSION,
                "command": getattr(args, "command", None),
                "status": "ERROR",
                "error": str(exc),
            },
            stream=sys.stderr,
        )
        return 2


__all__ = [
    "BUNDLE_SCHEMA_VERSION",
    "CommandError",
    "SCHEMA_VERSION",
    "build_parser",
    "main",
]
