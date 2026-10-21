"""Read-only verifier for the pre-two-slot G3 V11 historical profile."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace


STRATEGY_VERSION = "V13-V10-G-3-BACKTEST-TARGET-GT075-RESTORED3-V11"
FINGERPRINT = "fb0a2838879366715687f41700d8fe244cdbc0853805902de6806765c7369e27"
CONFIG_SHA256 = "2c0d7d2c641a4a7cb7debfeb0840c76d7ef151268e983b3ba4a48b9fec30f892"
CONFIG = Path(__file__).resolve().parents[2] / "configs/strategies/v13_v10_g_3_backtest_target_gt075_restored3_v11.json"


def strategy_payload() -> dict:
    raw = CONFIG.read_bytes()
    if hashlib.sha256(raw).hexdigest() != CONFIG_SHA256:
        raise ValueError("Archived G3 V11 configuration changed")
    payload = json.loads(raw)
    if payload.get("strategy_version") != STRATEGY_VERSION:
        raise ValueError("Archived G3 V11 identity changed")
    return payload


def strategy_fingerprint() -> str:
    payload = strategy_payload()
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"),
                                       allow_nan=False).encode()).hexdigest()
    if digest != FINGERPRINT:
        raise ValueError("Archived G3 V11 fingerprint changed")
    return digest


_PAYLOAD = strategy_payload()
ACTIVE_SETUPS = tuple(SimpleNamespace(**row,
    setup_id=f"{row['confirmation_end'].replace(':', '')}_{row['side']}")
    for row in _PAYLOAD["active_setups"])
DISABLED_IDS = frozenset(_PAYLOAD["setup_enablement"]["disabled_setup_ids"])
_ACTIVE_BY_ID = {row.setup_id: row for row in ACTIVE_SETUPS}
