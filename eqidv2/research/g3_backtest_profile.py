"""Normal historical/daily backtests share the G3 PAPER/LIVE decision contract.

The archived selection file pins the historical source evidence only. Current
orders and run configs always use the production V5 normalized 32-setup rules.
"""
from dataclasses import asdict
from pathlib import Path

import fno_v13_v10_g_3_contract as contract
from research.archived_g3_v4 import selected_profile as archived

production = contract
ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs/strategies/v13_v10_g_3_production_v5.json"
VERSION = contract.STRATEGY_VERSION
ADDED_IDS = contract.selected_setups.SETUP_IDS


def configuration():
    value = archived.configuration()
    # Promotion must preserve every researched rule, including core ranking,
    # confirmation volumes and exits. Only version/identity metadata changes.
    for name in ("CORE_SETUPS", "ACTIVE_SETUPS"):
        old = {s.setup_id: asdict(s) for s in getattr(archived.contract, name)}
        current = {s.setup_id: asdict(s) for s in getattr(contract, name)}
        for rows in (old, current):
            for row in rows.values():
                row.pop("source_version")
        if current != old:
            raise ValueError("Promoted G3 rules differ from the selected historical configuration")
    if contract.MIN_CONFIRMATION_VOLUME_BY_SETUP != archived.contract.MIN_CONFIRMATION_VOLUME_BY_SETUP:
        raise ValueError("Promoted G3 confirmation volume differs from historical selection")
    contract.validate_contract()
    return value
