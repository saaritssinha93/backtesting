"""Pure file and value helpers shared by the artifact adapters."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")


class ArtifactError(ValueError):
    """An artifact violates the declared integration contract."""


def now_ist() -> datetime:
    return datetime.now(tz=IST)


def source_metadata(path: Path, sha256: str, source_id: str):
    from ai_platform.schemas import SourceMetadata

    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=IST)
    return SourceMetadata(path, sha256, now_ist(), source_id, modified)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_json_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def read_json(path: Path | str) -> tuple[dict[str, Any], Path, str]:
    source = Path(path)
    if not source.is_file():
        raise ArtifactError(f"Artifact does not exist: {source}")
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ArtifactError(f"Cannot read JSON artifact {source}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ArtifactError(f"Expected a JSON object in {source}")
    return payload, source, file_sha256(source)


def decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ArtifactError(f"Invalid decimal value: {value!r}") from exc


def required_decimal(value: Any, field: str) -> Decimal:
    result = decimal_or_none(value)
    if result is None:
        raise ArtifactError(f"Missing decimal field: {field}")
    return result


def parse_timestamp(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise ArtifactError(f"Invalid timestamp in {field}: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ArtifactError(f"Timestamp must include a timezone in {field}: {value!r}")
    return parsed.astimezone(IST)


def optional_timestamp(value: Any, field: str) -> datetime | None:
    if value is None or value == "":
        return None
    return parse_timestamp(value, field)


def require_equal(actual: Any, expected: Any, field: str, path: Path) -> None:
    if expected is not None and actual != expected:
        raise ArtifactError(
            f"{field} mismatch in {path}: expected {expected!r}, got {actual!r}"
        )
