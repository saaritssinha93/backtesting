"""G3 production V13: the exact 51 setup decisions of G3 backtest V14.

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
from research import g3_target_cutoff_contract as backtest

from fno_v13_v10_g_3_contract import *  # noqa: F401,F403 - unchanged helpers

STRATEGY_VERSION = "V13-V10-G-3-PRODUCTION-V13"
ACCEPTED_VARIANT = donor.ACCEPTED_VARIANT + "_TARGET_GT075_RESTORED3"
PREVIOUS_STRATEGY_VERSION = donor.STRATEGY_VERSION
PREVIOUS_STRATEGY_FINGERPRINT = donor.strategy_fingerprint()
DISABLED_IDS = backtest.DISABLED_IDS
RESTORED_TARGETS = dict(backtest.RESTORED_TARGETS)
CORE_SETUPS = tuple(replace(s, source_version=STRATEGY_VERSION) for s in backtest.CORE_SETUPS)
ACTIVE_SETUPS = tuple(replace(s, source_version=STRATEGY_VERSION) for s in backtest.ACTIVE_SETUPS)
_CORE_BY_ID = {s.setup_id: s for s in CORE_SETUPS}
_ACTIVE_BY_ID = {s.setup_id: s for s in ACTIVE_SETUPS}
SIGNAL_TO_CONFIRMATION = dict(backtest.SIGNAL_TO_CONFIRMATION)
MIN_CONFIRMATION_VOLUME_BY_SETUP = dict(backtest.MIN_CONFIRMATION_VOLUME_BY_SETUP)


def setup_for(signal_end: str, side: str, *, session_date: date | None = None):
    return _ACTIVE_BY_ID.get(f"{SIGNAL_TO_CONFIRMATION.get(signal_end, '----').replace(':', '')}_{side.upper()}")


def passes_selected_filters(candidate, setup, *, session_date=None, original_only=False):
    return setup.setup_id in _ACTIVE_BY_ID and donor.passes_selected_filters(
        candidate, setup, session_date=session_date, original_only=original_only)


def rank_candidates(candidates, setup, *, session_date=None):
    if setup.setup_id not in _ACTIVE_BY_ID:
        return []
    return donor.rank_candidates(candidates, setup, session_date=session_date)


def selection_audit(signals):
    audit = backtest.selection_audit(signals)
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
    value = backtest.strategy_payload()
    value.pop("scope", None)
    value.update(strategy_version=STRATEGY_VERSION, accepted_variant=ACCEPTED_VARIANT,
                 source_strategy_version=PREVIOUS_STRATEGY_VERSION,
                 source_strategy_fingerprint=PREVIOUS_STRATEGY_FINGERPRINT,
                 core_setups=[asdict(s) for s in CORE_SETUPS],
                 active_setups=[asdict(s) for s in ACTIVE_SETUPS])
    return value


def strategy_fingerprint() -> str:
    return hashlib.sha256(json.dumps(strategy_payload(), sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


config_fingerprint = strategy_fingerprint


def supported_identity(version, fingerprint, *, allow_legacy=False):
    return (version, fingerprint) == (STRATEGY_VERSION, strategy_fingerprint()) or (
        allow_legacy and donor.supported_identity(version, fingerprint, allow_legacy=True))


def validate_contract(value=None):
    donor.validate_contract()
    backtest.validate_contract()
    if len(ACTIVE_SETUPS) != 51 or len(DISABLED_IDS) != 10:
        raise ValueError("G3 production cutoff must have exactly 51 enabled and 10 disabled setups")
    if any(asdict(_ACTIVE_BY_ID[s.setup_id]) != {**asdict(s), "source_version": STRATEGY_VERSION}
           for s in backtest.ACTIVE_SETUPS):
        raise ValueError("Production selection diverges from G3 V14 backtest")
    payload = strategy_payload()
    if value is not None and value != payload:
        raise ValueError("G3 V13 production configuration differs from the accepted cutoff contract")
    if payload["execution_authority"] is not False or payload["default_armed"] is not False:
        raise ValueError("G3 V13 contract cannot authorize trading")
    return payload


def load_contract(path: Path | str | None = None):
    value = strategy_payload() if path is None else json.loads(Path(path).read_text(encoding="utf-8"))
    return validate_contract(value)
