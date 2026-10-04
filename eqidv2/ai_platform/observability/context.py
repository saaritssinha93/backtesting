"""Correlation context shared by logs, metrics, traces and evidence journals.

The implementation uses :mod:`contextvars`, so bindings are isolated between
threads and asyncio tasks.  No global mutable request/session state is used.
"""

from __future__ import annotations

import os
import re
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass, fields, replace
from typing import Iterator, Mapping


_MAX_VALUE_LENGTH = 512
_ID_PREFIX = re.compile(r"[^a-z0-9_-]+")


@dataclass(frozen=True, slots=True)
class CorrelationContext:
    """Low-cardinality identity and high-cardinality correlation fields.

    These values belong in structured logs, traces and evidence records.  Only
    deliberately bounded subsets should be copied into metric labels.
    """

    service: str | None = None
    profile: str | None = None
    strategy_version: str | None = None
    strategy_fingerprint: str | None = None
    mode: str | None = None
    asset: str | None = None
    session_date: str | None = None
    session_id: str | None = None
    run_id: str | None = None
    replay_id: str | None = None
    slot_id: str | None = None
    request_id: str | None = None
    trace_id: str | None = None
    span_id: str | None = None
    signal_id: str | None = None
    order_id: str | None = None

    def as_dict(self, *, exclude_none: bool = True) -> dict[str, str | None]:
        values = asdict(self)
        if exclude_none:
            return {key: value for key, value in values.items() if value is not None}
        return values

    def derive(self, **updates: object) -> "CorrelationContext":
        unknown = set(updates) - _FIELD_NAMES
        if unknown:
            raise ValueError(f"Unknown correlation field(s): {', '.join(sorted(unknown))}")
        cleaned = {key: _clean_value(value) for key, value in updates.items()}
        return replace(self, **cleaned)

    @classmethod
    def from_mapping(cls, values: Mapping[str, object]) -> "CorrelationContext":
        return cls(
            **{
                key: _clean_value(value)
                for key, value in values.items()
                if key in _FIELD_NAMES and value is not None
            }
        )


_FIELD_NAMES = frozenset(field.name for field in fields(CorrelationContext))
_CONTEXT: ContextVar[CorrelationContext] = ContextVar(
    "eqidv2_observability_context", default=CorrelationContext()
)


def _clean_value(value: object) -> str | None:
    if value is None:
        return None
    # Prevent control characters from forging log lines or propagation fields.
    text = "".join(character if character >= " " else "?" for character in str(value)).strip()
    if not text:
        return None
    return text[:_MAX_VALUE_LENGTH]


def current_context() -> CorrelationContext:
    """Return the context active in the current thread or async task."""

    return _CONTEXT.get()


@contextmanager
def bind_context(
    context: CorrelationContext | Mapping[str, object] | None = None,
    **updates: object,
) -> Iterator[CorrelationContext]:
    """Temporarily merge correlation values into the current context.

    Passing ``None`` for an update clears that field for the nested scope.
    Bindings are always reset, including when the wrapped operation raises.
    """

    if context is None:
        base = current_context()
    elif isinstance(context, CorrelationContext):
        base = context
    else:
        base = CorrelationContext.from_mapping(context)
    bound = base.derive(**updates) if updates else base
    token = _CONTEXT.set(bound)
    try:
        yield bound
    finally:
        _CONTEXT.reset(token)


def new_run_id(prefix: str = "run") -> str:
    """Create a sortable-enough, collision-resistant correlation identifier."""

    cleaned = _ID_PREFIX.sub("-", prefix.strip().lower()).strip("-_") or "run"
    return f"{cleaned}_{uuid.uuid4().hex}"


def context_to_env(
    context: CorrelationContext | None = None,
    *,
    prefix: str = "EQIDV2_OBS_",
) -> dict[str, str]:
    """Serialize correlation context for a child process environment."""

    active = context or current_context()
    return {
        f"{prefix}{key.upper()}": value
        for key, value in active.as_dict().items()
        if value is not None
    }


def context_from_env(
    environment: Mapping[str, str] | None = None,
    *,
    prefix: str = "EQIDV2_OBS_",
) -> CorrelationContext:
    """Read context propagated by a supervising parent process."""

    source = os.environ if environment is None else environment
    return CorrelationContext.from_mapping(
        {
            key: source[f"{prefix}{key.upper()}"]
            for key in _FIELD_NAMES
            if source.get(f"{prefix}{key.upper()}")
        }
    )


__all__ = [
    "CorrelationContext",
    "bind_context",
    "context_from_env",
    "context_to_env",
    "current_context",
    "new_run_id",
]
