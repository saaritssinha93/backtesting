"""Bounded JSON-safe output helpers that omit raw producer payloads."""

from __future__ import annotations

import json
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Mapping


_SENSITIVE_PARTS = (
    "authorization",
    "access_token",
    "api_key",
    "apikey",
    "password",
    "request_token",
    "secret",
)
_LOCAL_PATH_KEYS = {"path", "source_equity_path", "master_path", "frozen_config_path"}


def public_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return {
            item.name: public_value(getattr(value, item.name))
            for item in fields(value)
            if item.name not in {"raw", "path"}
        }
    if isinstance(value, Mapping):
        return {
            str(key): (
                "[REDACTED]"
                if _is_sensitive(str(key))
                else "[OMITTED]"
                if str(key).lower() in _LOCAL_PATH_KEYS or str(key).lower().endswith("_path")
                else public_value(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [public_value(item) for item in value]
    return value


def _is_sensitive(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    return any(part in normalized for part in _SENSITIVE_PARTS)


def bounded_payload(value: Any, max_bytes: int) -> Any:
    cleaned = public_value(value)
    size = len(json.dumps(cleaned, ensure_ascii=True, separators=(",", ":")).encode("utf-8"))
    if size > max_bytes:
        raise ValueError(f"Response exceeds configured {max_bytes}-byte limit")
    return cleaned
