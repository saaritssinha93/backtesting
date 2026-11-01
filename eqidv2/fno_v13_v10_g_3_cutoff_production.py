"""G3 production V15: V13 entry rules with the all-slot selection.

V12 remains the immutable donor for old orders and historical backtests.  This
module changes setup enablement and strategy identity; it never grants trading
authority or changes the separately approved LIVE sizing policy.
"""
from __future__ import annotations

from dataclasses import asdict, replace
from datetime import date
import hashlib
import json
from pathlib import Path

import fno_v13_v10_g_3_contract as donor
from research import g3_slot_win70_contract as backtest
from research import archived_g3_production_v13 as previous
from research import archived_g3_production_v14 as previous_selected

from fno_v13_v10_g_3_contract import *  # noqa: F401,F403 - unchanged helpers

STRATEGY_VERSION = "V13-V10-G-3-PRODUCTION-V15"
ACCEPTED_VARIANT = previous.ACCEPTED_VARIANT + "_ALL_SLOT_NET_WIN_GE70"
PREVIOUS_STRATEGY_VERSION = previous.STRATEGY_VERSION
PREVIOUS_STRATEGY_FINGERPRINT = previous.strategy_fingerprint()
DISABLED_IDS = backtest.DISABLED_IDS
RESTORED_TARGETS = dict(backtest.RESTORED_TARGETS)
CORE_SETUPS = tuple(replace(s, source_version=STRATEGY_VERSION) for s in backtest.CORE_SETUPS)
ACTIVE_SETUPS = tuple(replace(s, source_version=STRATEGY_VERSION) for s in backtest.ACTIVE_SETUPS)
_CORE_BY_ID = {s.setup_id: s for s in CORE_SETUPS}
_ACTIVE_BY_ID = {s.setup_id: s for s in ACTIVE_SETUPS}
_ACTIVE_BY_SIGNAL_SIDE = {(s.signal_end, s.side): s for s in ACTIVE_SETUPS}
SIGNAL_TO_CONFIRMATION = dict(backtest.SIGNAL_TO_CONFIRMATION)
MIN_CONFIRMATION_VOLUME_BY_SETUP = dict(backtest.MIN_CONFIRMATION_VOLUME_BY_SETUP)
MIN_DIRECTIONAL_WICK_RATIO_BY_SETUP = dict(previous.strategy_payload().get("minimum_directional_wick_ratio_by_setup", {}))


def setup_for(signal_end: str, side: str, *, session_date: date | None = None):
    return _ACTIVE_BY_SIGNAL_SIDE.get((signal_end, side.upper()))


def passes_selected_filters(candidate, setup, *, session_date=None, original_only=False):
    return setup.setup_id in _ACTIVE_BY_ID and previous.passes_selected_filters(
        candidate, setup, session_date=session_date, original_only=original_only)


def rank_candidates(candidates, setup, *, session_date=None):
    if setup.setup_id not in _ACTIVE_BY_ID:
        return []
    return previous.rank_candidates(candidates, setup, session_date=session_date)


def selection_audit(signals):
    audit = previous.selection_audit(signals)
    audit = audit.loc[audit.setup_id.isin(_ACTIVE_BY_ID)].reset_index(drop=True)
    if audit.empty:
        return audit
    audit = audit.copy()
    audit["strategy_version"] = STRATEGY_VERSION
    audit["strategy_fingerprint"] = strategy_fingerprint()
    audit["signal_id"] = [donor.canonical_signal_id(
        STRATEGY_VERSION, date.fromisoformat(str(row.day)[:10]),
        _ACTIVE_BY_ID[row.setup_id].signal_end,
        _ACTIVE_BY_ID[row.setup_id].confirmation_end, row.side, row.tradingsymbol)
        for row in audit.itertuples()]
    return audit


def select_orders(signals):
    audit = selection_audit(signals)
    return audit.loc[audit.v9_selected.eq(True)].reset_index(drop=True)


def strategy_payload() -> dict:
    value = previous.strategy_payload()
    value.pop("scope", None)
    value.update(strategy_version=STRATEGY_VERSION, accepted_variant=ACCEPTED_VARIANT,
                 source_strategy_version=PREVIOUS_STRATEGY_VERSION,
                 source_strategy_fingerprint=PREVIOUS_STRATEGY_FINGERPRINT,
                 slot_selection=backtest.slots.policy_payload("G3"),
                 slot_selection_fingerprint=backtest.slots.policy_fingerprint("G3"),
                 signal_to_confirmation=dict(SIGNAL_TO_CONFIRMATION),
                 minimum_confirmation_1m_volume_ratio_by_setup=dict(MIN_CONFIRMATION_VOLUME_BY_SETUP),
                 core_setups=[asdict(s) for s in CORE_SETUPS],
                 active_setups=[asdict(s) for s in ACTIVE_SETUPS])
    value["setup_enablement"].update(rule="PRIOR_PRODUCTION_AND_ALL_SLOTS_WIN_GE70",
        enabled_count=len(ACTIVE_SETUPS), disabled_count=len(DISABLED_IDS), disabled_setup_ids=sorted(DISABLED_IDS))
    value["disabled_setups"] = backtest.strategy_payload()["disabled_setups"]
    value["exit"]["setups"] = {k:v for k,v in value["exit"]["setups"].items() if k in _ACTIVE_BY_ID}
    return value


def strategy_fingerprint() -> str:
    return hashlib.sha256(json.dumps(strategy_payload(), sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


config_fingerprint = strategy_fingerprint


def supported_identity(version, fingerprint, *, allow_legacy=False):
    return (version, fingerprint) == (STRATEGY_VERSION, strategy_fingerprint()) or (
        allow_legacy and ((version, fingerprint) ==
            (previous_selected.STRATEGY_VERSION, previous_selected.strategy_fingerprint()) or
            previous.supported_identity(version, fingerprint, allow_legacy=True)))


def validate_contract(value=None):
    donor.validate_contract()
    backtest.validate_contract()
    if set(_ACTIVE_BY_ID) != set(backtest.slots.enabled_setup_ids("G3")):
        raise ValueError("G3 production slots must match the shared >=70% selection")
    if any(asdict(_ACTIVE_BY_ID[s.setup_id]) != {**asdict(s), "source_version": STRATEGY_VERSION}
           for s in backtest.ACTIVE_SETUPS):
        raise ValueError("Production selection diverges from G3 V17 backtest")
    payload = strategy_payload()
    if value is not None and value != payload:
        raise ValueError("G3 V15 production configuration differs from the selected contract")
    if payload["execution_authority"] is not False or payload["default_armed"] is not False:
        raise ValueError("G3 V15 contract cannot authorize trading")
    return payload


def load_contract(path: Path | str | None = None):
    value = strategy_payload() if path is None else json.loads(Path(path).read_text(encoding="utf-8"))
    return validate_contract(value)
