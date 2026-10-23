"""Read-only verifier for the G3 V13 historical profile before 10:11 SHORT 0.11%."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

STRATEGY_VERSION = "V13-V10-G-3-BACKTEST-TARGET-GT075-RESTORED3-V13"
FINGERPRINT = "77eefabdebbd580e798e7584d206214df5f383983c5053230a3171ef939e2883"
CONFIG_SHA256 = "72133370810a2706c6d1b3dcb7551ca3ff419893af2951e0b46407a9544c36ea"
CONFIG = (Path(__file__).resolve().parents[2] /
          "configs/strategies/v13_v10_g_3_backtest_target_gt075_restored3_v13.json")


def strategy_payload() -> dict:
    raw = CONFIG.read_bytes()
    if hashlib.sha256(raw).hexdigest() != CONFIG_SHA256:
        raise ValueError("Archived G3 V13 configuration changed")
    payload = json.loads(raw)
    if payload.get("strategy_version") != STRATEGY_VERSION:
        raise ValueError("Archived G3 V13 identity changed")
    return payload


def strategy_fingerprint() -> str:
    payload = strategy_payload()
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"),
                                       allow_nan=False).encode()).hexdigest()
    if digest != FINGERPRINT:
        raise ValueError("Archived G3 V13 fingerprint changed")
    return digest


_PAYLOAD = strategy_payload()
ACTIVE_SETUPS = tuple(SimpleNamespace(**row,
    setup_id=f"{row['confirmation_end'].replace(':', '')}_{row['side']}")
    for row in _PAYLOAD["active_setups"])
DISABLED_IDS = frozenset(_PAYLOAD["setup_enablement"]["disabled_setup_ids"])
_ACTIVE_BY_ID = {row.setup_id: row for row in ACTIVE_SETUPS}
