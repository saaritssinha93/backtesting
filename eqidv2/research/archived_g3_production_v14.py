"""Frozen V14 G3 identity for managing recorded positions after the all-slot cutover.

This module reads the immutable V14 configuration and cannot create new entries.
"""
from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path

from research import archived_g3_production_v13 as previous

STRATEGY_VERSION = "V13-V10-G-3-PRODUCTION-V14"
EXPECTED_FINGERPRINT = "4466490c49ab261ba88f61ab8054bb284eadbd2ba32d720265ecd8b5594fc0c3"
CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs/strategies/v13_v10_g_3_production_v14.json"
_CONFIG = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
_ENABLED = frozenset(s["confirmation_end"].replace(":", "") + "_" + s["side"]
    for s in _CONFIG["active_setups"])
ACTIVE_SETUPS = tuple(replace(s, source_version=STRATEGY_VERSION)
    for s in previous.ACTIVE_SETUPS if s.setup_id in _ENABLED)
_BY_SIGNAL_SIDE = {(s.signal_end, s.side): s for s in ACTIVE_SETUPS}
canonical_signal_id = previous.canonical_signal_id


def strategy_payload():
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def strategy_fingerprint():
    payload = strategy_payload()
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    if digest != EXPECTED_FINGERPRINT or payload["strategy_version"] != STRATEGY_VERSION:
        raise ValueError("Archived G3 V14 identity changed")
    if {s.setup_id for s in ACTIVE_SETUPS} != _ENABLED:
        raise ValueError("Archived G3 V14 setups changed")
    return digest


def setup_for(signal_end, side, *, session_date=None):
    return _BY_SIGNAL_SIDE.get((signal_end, str(side).upper()))
