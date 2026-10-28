"""G3 entry restriction shared by PAPER fills and LIVE reservations.

This execution guard does not alter signal identity, sizing or exit management.
Callers compare active symbols under their existing cross-worker admission lock.
"""
from __future__ import annotations

SCHEMA_VERSION = "g3_symbol_entry_guard_v1"
POLICY = "ONE_ACTIVE_POSITION_PER_SYMBOL"
BLOCK_REASON = "SYMBOL_ALREADY_ACTIVE"


def symbol_key(row: dict) -> str:
    """Use the underlying equity symbol, independent of setup, side or venue."""
    value = row.get("tradingsymbol")
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Missing or invalid active equity symbol")
    result = value.strip().upper()
    if any(character.isspace() or ord(character) < 32 for character in result):
        raise ValueError("Invalid active equity symbol")
    return result
