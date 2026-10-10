"""Stable cross-run identities for retained V13-V10-G signals.

This module is deliberately pure: importing it performs no file, network,
broker, or clock access.  Live and replay producers can therefore share one
identity contract without coupling their execution paths.
"""

from __future__ import annotations

import hashlib
import re
from datetime import date


SIGNAL_ID_SCHEMA_VERSION = "eqidv2.v13_v10_g.signal_id.v1"


def _slot(value: str) -> str:
    digits = str(value).strip().replace(":", "")
    if len(digits) != 4 or not digits.isdigit():
        raise ValueError(f"invalid HH:MM slot: {value!r}")
    hour, minute = int(digits[:2]), int(digits[2:])
    if hour > 23 or minute > 59:
        raise ValueError(f"invalid HH:MM slot: {value!r}")
    return f"{hour:02d}:{minute:02d}"


def _safe_contract_stem(symbol: str) -> str:
    # This intentionally matches fno_oi_common.safe_contract_stem while
    # keeping the identity helper free of runtime-directory side effects.
    normalized = str(symbol).strip().upper()
    stem = re.sub(r"[^A-Z0-9._-]+", "_", normalized).strip("._") or "CONTRACT"
    if stem != normalized:
        suffix = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:8]
        stem = f"{stem}_{suffix}"
    return stem


def canonical_signal_id(
    strategy_version: str,
    session_date: date,
    signal_end: str,
    confirmation_end: str,
    side: str,
    symbol: str,
) -> str:
    """Return the exact deterministic ID historically emitted by live G."""

    if type(session_date) is not date:
        raise TypeError("session_date must be a datetime.date")
    version = str(strategy_version).strip()
    if not version:
        raise ValueError("strategy_version is required")
    normalized_signal = _slot(signal_end)
    normalized_confirmation = _slot(confirmation_end)
    normalized_side = str(side).strip().upper()
    if normalized_side not in {"LONG", "SHORT"}:
        raise ValueError(f"invalid side: {side!r}")
    normalized_symbol = str(symbol).strip()
    if not normalized_symbol:
        raise ValueError("symbol is required")
    raw = (
        f"{version}|{session_date}|{normalized_signal}|"
        f"{normalized_confirmation}|{normalized_side}|{normalized_symbol}"
    )
    digest = hashlib.sha1(raw.encode("ascii")).hexdigest()[:12]
    return (
        f"{session_date.strftime('%Y%m%d')}_"
        f"{normalized_confirmation.replace(':', '')}_{normalized_side}_"
        f"{_safe_contract_stem(normalized_symbol)}_{digest}"
    )


__all__ = ["SIGNAL_ID_SCHEMA_VERSION", "canonical_signal_id"]
