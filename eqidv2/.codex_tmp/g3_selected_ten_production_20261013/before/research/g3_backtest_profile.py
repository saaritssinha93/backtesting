"""The normal G3 offline profile: unchanged V4 plus the selected ten setups.

Load the shared pure decision code in a private module namespace. Its functions
then use offline globals; PAPER/LIVE's imported module is never patched. This
also prevents concurrent dashboard verification from altering live decisions.
"""
from dataclasses import replace
import copy
import importlib.util
import json
from pathlib import Path
import sys

import fno_v13_v10_g_3_contract as production

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs/strategies/v13_v10_g_3_backtest_selected_v5.json"
VERSION = "V13-V10-G-3-BACKTEST-SELECTED-TEN-V5"


def configuration():
    value = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    SELECTED_TARGETS = {"1036_LONG": 2.12, "1036_SHORT": 2.86, "1041_LONG": 1.71,
        "1046_LONG": .79, "1046_SHORT": 1.01, "1051_LONG": .77, "1056_SHORT": 1.53,
        "1111_LONG": 1.76, "1111_SHORT": 1.84, "1126_SHORT": 4.20}
    if (value.get("variant_id") != VERSION or value.get("scope") != "BACKTEST_ONLY"
            or value.get("execution_authority") is not False
            or value.get("preserve_existing_g3_setups") is not True
            or value.get("allow_unlisted_additions") is not False
            or value.get("base_strategy_version") != production.STRATEGY_VERSION
            or value.get("base_strategy_fingerprint") != production.strategy_fingerprint()
            or len(value["setups"]) != 10
            or {s["setup_id"]: s["target_pct"] for s in value["setups"]} != SELECTED_TARGETS):
        raise ValueError("G3 backtest profile differs from the selected ten or its pinned V4 base")
    donor = json.loads((ROOT / "configs/strategies/v13_v10_g_4_backtest_selected_v3.json").read_text())
    for key in ("setups", "initial_stop_pct", "tightened_stop_pct", "tighten_after_minutes",
                "confirmation_offset_minutes", "entry_expiry_minutes", "square_off",
                "round_trip_cost_bps", "capital_per_entry_rs", "leverage", "portfolio_capital_rs"):
        if value[key] != donor[key]:
            raise ValueError(f"G3 backtest changed the selected G4 configuration: {key}")
    return value


_name = "research._g3_offline_decisions"
_spec = importlib.util.spec_from_file_location(_name, production.__file__)
contract = importlib.util.module_from_spec(_spec)
sys.modules[_name] = contract
_spec.loader.exec_module(contract)
_config = configuration()
_extras = []
for row in _config["setups"]:
    t = row["thresholds"]
    donor = contract.setup_for("09:50", row["side"])
    _extras.append(replace(donor, signal_end=row["signal_end"], confirmation_end=row["confirmation_end"],
        max_entries=1, picker="max_liquidity", price_change_pct=t["price_change_pct"],
        oi_change_pct=t["oi_change_pct"], volume_ratio=t["five_minute_volume_ratio_min"],
        body_ratio=t["one_minute_body_ratio_min"], max_wick_ratio=t["one_minute_wick_ratio_max"],
        target_pct=row["target_pct"], source_version=VERSION))
    contract.MIN_CONFIRMATION_VOLUME_BY_SETUP[row["setup_id"]] = t["one_minute_volume_ratio_min"]
ADDED_IDS = frozenset(s.setup_id for s in _extras)
contract.CORE_SETUPS = tuple(replace(s, source_version=VERSION) for s in contract.CORE_SETUPS) + tuple(_extras)
contract.ACTIVE_SETUPS = tuple(replace(s, source_version=VERSION) for s in contract.ACTIVE_SETUPS) + tuple(_extras)
contract._CORE_BY_ID = {s.setup_id: s for s in contract.CORE_SETUPS}
contract._ACTIVE_BY_ID = {s.setup_id: s for s in contract.ACTIVE_SETUPS}
contract.SIGNAL_TO_CONFIRMATION = dict(sorted({s.signal_end: s.confirmation_end for s in contract.ACTIVE_SETUPS}.items()))
contract.STRATEGY_VERSION = VERSION
contract.ACCEPTED_VARIANT = "G3_V4_PLUS_SELECTED_TEN_BACKTEST"
_base_payload = contract.strategy_payload


def strategy_payload():
    value = _base_payload()
    value.update(schema_version="fno_g3_backtest_contract_v1", scope="BACKTEST_ONLY",
                 source_selection_config=copy.deepcopy(_config))
    return value


def validate_contract(value=None):
    if (configuration() != _config or strategy_payload() != _expected_payload
            or (value is not None and value != strategy_payload())):
        raise ValueError("G3 backtest differs from the accepted normalized contract; restart with the reviewed configuration")
    if len(contract.ACTIVE_SETUPS) != 32 or len(contract._ACTIVE_BY_ID) != 32:
        raise ValueError("G3 backtest must contain the original 22 plus exactly ten additions")


def load_contract(path=None):
    # This profile's file records additions; exported run config contains the
    # complete normalized 32-setup contract.
    if path is not None and Path(path).resolve() == CONFIG_PATH.resolve():
        validate_contract()
        return strategy_payload()
    value = strategy_payload() if path is None else json.loads(Path(path).read_text())
    validate_contract(value)
    return value


def supported_identity(version, fingerprint, *, allow_legacy=False):
    return ((version, fingerprint) == (VERSION, contract.strategy_fingerprint())
        or allow_legacy and production.supported_identity(version, fingerprint, allow_legacy=True))


contract.strategy_payload = strategy_payload
contract.validate_contract = validate_contract
contract.load_contract = load_contract
contract.supported_identity = supported_identity
_expected_payload = strategy_payload()
