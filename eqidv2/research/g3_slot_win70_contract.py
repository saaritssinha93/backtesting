"""Current G3 backtest: preserved V15 rules with all >=70% slots."""
from __future__ import annotations

import copy
from dataclasses import asdict, replace
from datetime import date
import hashlib
import json
from pathlib import Path
import sys

from research import g3_target_cutoff_contract as previous
import strategy_slot_selection as slots

STRATEGY_VERSION = "V13-V10-G-3-BACKTEST-SLOT-WIN70-V17"
ACCEPTED_VARIANT = previous.ACCEPTED_VARIANT + "_SLOT_NET_WIN_GE70"
_ENABLED_IDS = slots.enabled_setup_ids("G3")
ACTIVE_SETUPS = tuple(replace(s, source_version=STRATEGY_VERSION)
    for s in previous.ACTIVE_SETUPS if s.setup_id in _ENABLED_IDS)
CORE_SETUPS = tuple(replace(s, source_version=STRATEGY_VERSION)
    for s in previous.CORE_SETUPS if s.setup_id in _ENABLED_IDS)
_ACTIVE_BY_ID = {s.setup_id: s for s in ACTIVE_SETUPS}
_CORE_BY_ID = {s.setup_id: s for s in CORE_SETUPS}
DISABLED_SETUPS = tuple(s for s in previous.production.ACTIVE_SETUPS if s.setup_id not in _ENABLED_IDS)
DISABLED_IDS = frozenset(s.setup_id for s in DISABLED_SETUPS)
SIGNAL_TO_CONFIRMATION = dict(sorted({s.signal_end: s.confirmation_end for s in ACTIVE_SETUPS}.items()))
MIN_CONFIRMATION_VOLUME_BY_SETUP = {k: v for k, v in previous.MIN_CONFIRMATION_VOLUME_BY_SETUP.items() if k in _ENABLED_IDS}
MIN_DIRECTIONAL_WICK_RATIO_BY_SETUP = {k: v for k, v in previous.MIN_DIRECTIONAL_WICK_RATIO_BY_SETUP.items() if k in _ENABLED_IDS}


def __getattr__(name):
    return getattr(previous, name)


def setup_for(signal_end, side, *, session_date=None):
    return next((s for s in ACTIVE_SETUPS if (s.signal_end, s.side) == (signal_end, side.upper())), None)


def passes_selected_filters(candidate, setup, *, session_date=None, original_only=False):
    return setup.setup_id in _ACTIVE_BY_ID and previous.passes_selected_filters(
        candidate, setup, session_date=session_date, original_only=original_only)


def rank_candidates(candidates, setup, *, session_date=None):
    return previous.rank_candidates(candidates, setup, session_date=session_date) if setup.setup_id in _ACTIVE_BY_ID else []


def identify_orders(orders):
    out = orders.copy()
    if len(out) and not set(out.setup_id).issubset(_ACTIVE_BY_ID):
        raise ValueError("Slot disabled by >=70% selection in new G3 orders")
    if "signal_id" in out and "source_signal_id" not in out:
        out["source_signal_id"] = out.signal_id
    for index, row in out.iterrows():
        setup = _ACTIVE_BY_ID[row.setup_id]
        day = date.fromisoformat(str(row.day)[:10])
        out.loc[index, "signal_id"] = previous.canonical_signal_id(STRATEGY_VERSION,
            day, setup.signal_end, setup.confirmation_end, setup.side, row.tradingsymbol)
    out["strategy_version"] = STRATEGY_VERSION
    out["strategy_fingerprint"] = strategy_fingerprint()
    return out


def selection_audit(signals):
    audit = previous.selection_audit(signals)
    return identify_orders(audit.loc[audit.setup_id.isin(_ACTIVE_BY_ID)].reset_index(drop=True))


def select_orders(signals):
    audit = selection_audit(signals)
    return audit.loc[audit.v9_selected.eq(True)].reset_index(drop=True)


def strategy_payload():
    value = previous.strategy_payload()
    value.update(strategy_version=STRATEGY_VERSION, accepted_variant=ACCEPTED_VARIANT,
        source_strategy_version=previous.STRATEGY_VERSION,
        source_strategy_fingerprint=previous.strategy_fingerprint(),
        slot_selection=slots.policy_payload("G3"), slot_selection_fingerprint=slots.policy_fingerprint("G3"),
        setup_enablement={**value["setup_enablement"], "rule": "PRIOR_V15_AND_FROZEN_SLOT_WIN_GE70",
            "enabled_count": len(ACTIVE_SETUPS), "disabled_count": len(DISABLED_SETUPS),
            "disabled_setup_ids": sorted(DISABLED_IDS)},
        disabled_setups=[dict(**asdict(s), enabled=False,
            reason="TARGET_LE_075" if s.setup_id in previous.DISABLED_IDS else "SLOT_WIN_RATE_BELOW70_OR_NO_TRADES") for s in DISABLED_SETUPS],
        core_setups=[asdict(s) for s in CORE_SETUPS], active_setups=[asdict(s) for s in ACTIVE_SETUPS],
        signal_to_confirmation=dict(SIGNAL_TO_CONFIRMATION),
        minimum_confirmation_1m_volume_ratio_by_setup=dict(MIN_CONFIRMATION_VOLUME_BY_SETUP),
        minimum_directional_wick_ratio_by_setup=dict(MIN_DIRECTIONAL_WICK_RATIO_BY_SETUP))
    value["exit"]["setups"] = {k: v for k, v in value["exit"]["setups"].items() if k in _ENABLED_IDS}
    return value


def strategy_fingerprint():
    return hashlib.sha256(json.dumps(strategy_payload(), sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


config_fingerprint = strategy_fingerprint


def contract_for_identity(version, fingerprint):
    if (version, fingerprint) == (STRATEGY_VERSION, strategy_fingerprint()):
        return sys.modules[__name__]
    return previous.contract_for_identity(version, fingerprint)


def supported_identity(version, fingerprint, *, allow_legacy=False):
    return ((version, fingerprint) == (STRATEGY_VERSION, strategy_fingerprint())
        or allow_legacy and previous.supported_identity(version, fingerprint, allow_legacy=True))


def validate_contract(value=None):
    previous.validate_contract()
    if slots.enabled_setup_ids("G3") != _ENABLED_IDS or set(_ACTIVE_BY_ID) != _ENABLED_IDS:
        raise ValueError("Reload required: G3 slot policy changed or unknown setup selected")
    if not _ENABLED_IDS.issubset(previous._ACTIVE_BY_ID):
        raise ValueError("Slot selection cannot introduce a new setup")
    if value is not None and value != strategy_payload():
        raise ValueError("G3 slot selection contract mismatch")


def load_contract(path=None):
    value = strategy_payload() if path is None else json.loads(Path(path).read_text(encoding="utf-8"))
    validate_contract(value)
    return copy.deepcopy(value)
