"""Read-only verifier for the pre-extension G3 V10 historical profile."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace


STRATEGY_VERSION = "V13-V10-G-3-BACKTEST-TARGET-GT075-RESTORED3-V10"
FINGERPRINT = "99ac5d11d297340dbf4c6dd65408dd9c90054c1a3b254dd979fa2c73515ebb01"
CONFIG_SHA256 = "9c8fc71b0e98d7fe9bd82bcd86d077afe63af117298e88aab3f6d3e59b5fc42a"
CONFIG = Path(__file__).resolve().parents[2] / "configs/strategies/v13_v10_g_3_backtest_target_gt075_restored3_v10.json"


def strategy_payload() -> dict:
    raw = CONFIG.read_bytes()
    if hashlib.sha256(raw).hexdigest() != CONFIG_SHA256:
        raise ValueError("Archived G3 V10 configuration changed")
    payload = json.loads(raw)
    if payload.get("strategy_version") != STRATEGY_VERSION:
        raise ValueError("Archived G3 V10 identity changed")
    return payload


def strategy_fingerprint() -> str:
    payload = strategy_payload()
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"),
                                       allow_nan=False).encode()).hexdigest()
    if digest != FINGERPRINT:
        raise ValueError("Archived G3 V10 fingerprint changed")
    return digest


_PAYLOAD = strategy_payload()
ACTIVE_SETUPS = tuple(SimpleNamespace(**row,
    setup_id=f"{row['confirmation_end'].replace(':', '')}_{row['side']}")
    for row in _PAYLOAD["active_setups"])
DISABLED_IDS = frozenset(_PAYLOAD["setup_enablement"]["disabled_setup_ids"])
_ACTIVE_BY_ID = {row.setup_id: row for row in ACTIVE_SETUPS}
