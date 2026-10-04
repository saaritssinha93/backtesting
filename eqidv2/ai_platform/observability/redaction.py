"""Central, recursive secret redaction for all observability outputs."""

from __future__ import annotations

import dataclasses
import json
import math
import re
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Mapping


REDACTED = "[REDACTED]"

_SENSITIVE_KEY = re.compile(
    r"(?:^|_)(?:password|passwd|pwd|secret|credential|authorization|auth_header|"
    r"cookie|set_cookie|private_key|client_secret|api_key|apikey|access_token|"
    r"refresh_token|bearer_token)(?:$|_)",
    re.IGNORECASE,
)
_BEARER = re.compile(r"(?i)\b(bearer\s+)[^\s,;]+")
_QUERY_SECRET = re.compile(
    r"(?i)([?&](?:api[_-]?key|[a-z0-9_-]*token|secret|password)=)[^&#\s]+"
)
_ASSIGNMENT_SECRET = re.compile(
    r"(?i)(\b(?:api[_-]?key|[a-z0-9_-]*token|password|secret|authorization)"
    r"\s*[:=]\s*)[^\s,;}]+"
)


def is_sensitive_key(key: object) -> bool:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", str(key)).strip("_")
    return normalized.lower().endswith("token") or bool(
        _SENSITIVE_KEY.search(normalized)
    )


def redact_text(value: str) -> str:
    """Redact common credential forms embedded in otherwise useful text."""

    value = _BEARER.sub(lambda match: f"{match.group(1)}{REDACTED}", value)
    value = _QUERY_SECRET.sub(lambda match: f"{match.group(1)}{REDACTED}", value)
    return _ASSIGNMENT_SECRET.sub(lambda match: f"{match.group(1)}{REDACTED}", value)


def redact(value: Any, *, max_depth: int = 12, _depth: int = 0) -> Any:
    """Return a JSON-safe copy with secrets removed.

    Unknown objects are represented by their type, not by ``repr``; this keeps
    objects with credential-bearing representations out of logs.
    """

    if _depth > max_depth:
        return "[MAX_DEPTH]"
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return redact(value.value, max_depth=max_depth, _depth=_depth + 1)
    if isinstance(value, bytes):
        return f"[BYTES:{len(value)}]"
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return redact(dataclasses.asdict(value), max_depth=max_depth, _depth=_depth + 1)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            text_key = str(key)
            result[text_key] = (
                REDACTED
                if is_sensitive_key(text_key)
                else redact(item, max_depth=max_depth, _depth=_depth + 1)
            )
        return result
    if isinstance(value, (list, tuple)):
        return [redact(item, max_depth=max_depth, _depth=_depth + 1) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted(
            (redact(item, max_depth=max_depth, _depth=_depth + 1) for item in value),
            key=str,
        )
    if isinstance(value, BaseException):
        return {"type": type(value).__name__, "message": redact_text(str(value))}
    return f"[{type(value).__name__}]"


def safe_json_dumps(value: Any, *, sort_keys: bool = True) -> str:
    return json.dumps(
        redact(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=sort_keys,
        allow_nan=False,
    )


__all__ = ["REDACTED", "is_sensitive_key", "redact", "redact_text", "safe_json_dumps"]
