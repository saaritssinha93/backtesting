"""Read-only verifier for the G3 V12 historical profile before 09:26 LONG volume 1.2x."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace


STRATEGY_VERSION = "V13-V10-G-3-BACKTEST-TARGET-GT075-RESTORED3-V12"
FINGERPRINT = "2c85fb7c1aebbea8cfe1c5ef9ab2e9f3b22aedb0bd824bc69e1fc468db96c01a"
CONFIG_SHA256 = "7f8e3eddbbe545c8e82404564fa3d1a6664490f87dd7ff9a222c5a975acdc462"
CONFIG = Path(__file__).resolve().parents[2] / "configs/strategies/v13_v10_g_3_backtest_target_gt075_restored3_v12.json"


def strategy_payload() -> dict:
    raw = CONFIG.read_bytes()
    if hashlib.sha256(raw).hexdigest() != CONFIG_SHA256:
        raise ValueError("Archived G3 V12 configuration changed")
    payload = json.loads(raw)
    if payload.get("strategy_version") != STRATEGY_VERSION:
        raise ValueError("Archived G3 V12 identity changed")
    return payload


def strategy_fingerprint() -> str:
    payload = strategy_payload()
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"),
                                      allow_nan=False).encode()).hexdigest()
    if digest != FINGERPRINT:
        raise ValueError("Archived G3 V12 fingerprint changed")
    return digest


_PAYLOAD = strategy_payload()
ACTIVE_SETUPS = tuple(SimpleNamespace(**row,
    setup_id=f"{row['confirmation_end'].replace(':', '')}_{row['side']}")
    for row in _PAYLOAD["active_setups"])
DISABLED_IDS = frozenset(_PAYLOAD["setup_enablement"]["disabled_setup_ids"])
_ACTIVE_BY_ID = {row.setup_id: row for row in ACTIVE_SETUPS}
