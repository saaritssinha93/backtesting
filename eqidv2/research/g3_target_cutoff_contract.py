"""Backtest-only G3 subset: target cutoff with three explicit restored setups.

The production V10 rule book provides the two new 1.5% target slots. Retained rules, ranking,
stops and capital are unchanged. This module cannot grant order authority.
"""
from __future__ import annotations

import copy
from dataclasses import asdict, replace
from datetime import date
import hashlib
import json
from pathlib import Path

import fno_v13_v10_g_3_contract as production

STRATEGY_VERSION = "V13-V10-G-3-BACKTEST-TARGET-GT075-RESTORED3-V13"
ACCEPTED_VARIANT = "G3_V11_BACKTEST_TARGET_CUTOFF_RESTORE_THREE_0941_LONG_VOLUME080_1035_1M_EXTENSION_ATR3_0920_SHORT_1010_LONG_TARGET150_0926_LONG_5M_VOLUME120"
TARGET_CUTOFF_PCT = .75
SOURCE_FINGERPRINT = "82c8518f9350d27a49237b81c5b0965cf745bfefef0e2b675cecde75f8b8e4b4"
RESTORED_TARGETS = {'0951_LONG': .75, '1006_LONG': .56, '1016_SHORT': .68}
RESTORED_IDS = frozenset(RESTORED_TARGETS)
DISABLED_SETUPS = tuple(s for s in production.ACTIVE_SETUPS
                       if s.target_pct <= TARGET_CUTOFF_PCT and s.setup_id not in RESTORED_IDS)
DISABLED_IDS = frozenset(s.setup_id for s in DISABLED_SETUPS)
ACTIVE_SETUPS = tuple(replace(s, source_version=STRATEGY_VERSION)
                      for s in production.ACTIVE_SETUPS if s.setup_id not in DISABLED_IDS)
CORE_SETUPS = tuple(replace(s, source_version=STRATEGY_VERSION)
                    for s in production.CORE_SETUPS if s.setup_id not in DISABLED_IDS)
_ACTIVE_BY_ID = {s.setup_id: s for s in ACTIVE_SETUPS}
_CORE_BY_ID = {s.setup_id: s for s in CORE_SETUPS}
SIGNAL_TO_CONFIRMATION = dict(sorted({s.signal_end: s.confirmation_end for s in ACTIVE_SETUPS}.items()))
MIN_CONFIRMATION_VOLUME_BY_SETUP = {key: value for key, value in production.MIN_CONFIRMATION_VOLUME_BY_SETUP.items()
                                  if key not in DISABLED_IDS}


def __getattr__(name):
    """Share unchanged helpers/constants without copying or mutating production."""
    return getattr(production, name)


def setup_for(signal_end, side, *, session_date=None):
    return next((s for s in ACTIVE_SETUPS if s.signal_end == signal_end and s.side == side.upper()), None)


def confirmation_volume_minimum(side, *, signal_end=None, setup_id=None):
    return production.confirmation_volume_minimum(side, signal_end=signal_end, setup_id=setup_id)


def passes_selected_filters(candidate, setup, *, session_date=None, original_only=False):
    return (setup.setup_id in _ACTIVE_BY_ID and production.passes_selected_filters(
        candidate, setup, session_date=session_date, original_only=original_only))


def rank_candidates(candidates, setup, *, session_date=None):
    if setup.setup_id not in _ACTIVE_BY_ID:
        return []
    return production.rank_candidates(candidates, setup, session_date=session_date)


def identify_orders(orders):
    """Bind freshly selected/replayed retained orders to their backtest identity."""
    out = orders.copy()
    if len(out) and not set(out.setup_id).issubset(_ACTIVE_BY_ID):
        raise ValueError("Disabled or unknown setup in cutoff backtest orders")
    if 'signal_id' in out and 'source_signal_id' not in out:
        out['source_signal_id'] = out.signal_id
    for index, row in out.iterrows():
        spec = _ACTIVE_BY_ID[row.setup_id]
        day = row.day if isinstance(row.day, date) else date.fromisoformat(str(row.day)[:10])
        out.loc[index, 'signal_id'] = production.canonical_signal_id(
            STRATEGY_VERSION, day, spec.signal_end, spec.confirmation_end, spec.side, row.tradingsymbol)
    out['strategy_version'] = STRATEGY_VERSION
    out['strategy_fingerprint'] = strategy_fingerprint()
    return out


def selection_audit(signals):
    audit = production.selection_audit(signals)
    audit = audit.loc[audit.setup_id.isin(_ACTIVE_BY_ID)].copy()
    return identify_orders(audit).reset_index(drop=True)


def select_orders(signals):
    audit = selection_audit(signals)
    return audit.loc[audit.v9_selected.eq(True)].reset_index(drop=True)


def strategy_payload():
    value = production.strategy_payload()
    value.update(strategy_version=STRATEGY_VERSION, accepted_variant=ACCEPTED_VARIANT,
        scope="BACKTEST_ONLY", execution_authority=False, default_armed=False,
        source_strategy_version=production.STRATEGY_VERSION, source_strategy_fingerprint=SOURCE_FINGERPRINT,
        setup_enablement=dict(rule="TARGET_PCT_GT_OR_EXPLICITLY_RESTORED", cutoff_pct=TARGET_CUTOFF_PCT,
            disabled_when="target_pct <= cutoff_pct and setup_id not in restored_setup_ids",
            restored_setup_ids=sorted(RESTORED_IDS), restored_targets=dict(RESTORED_TARGETS),
            enabled_count=len(ACTIVE_SETUPS), disabled_count=len(DISABLED_SETUPS),
            disabled_setup_ids=sorted(DISABLED_IDS)),
        disabled_setups=[dict(**asdict(s), enabled=False, reason="TARGET_LE_075") for s in DISABLED_SETUPS],
        core_setups=[asdict(s) for s in CORE_SETUPS], active_setups=[asdict(s) for s in ACTIVE_SETUPS],
        signal_to_confirmation=dict(SIGNAL_TO_CONFIRMATION),
        minimum_confirmation_1m_volume_ratio_by_setup=dict(MIN_CONFIRMATION_VOLUME_BY_SETUP))
    value['exit']['setups'] = {key: spec for key, spec in value['exit']['setups'].items() if key not in DISABLED_IDS}
    return value


def strategy_fingerprint():
    return hashlib.sha256(json.dumps(strategy_payload(), sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


config_fingerprint = strategy_fingerprint


def contract_for_identity(version, fingerprint):
    """Resolve result identity without treating an old result as current."""
    import sys
    if (version, fingerprint) == (STRATEGY_VERSION, strategy_fingerprint()):
        return sys.modules[__name__]
    from research.archived_g3_v12 import contract as before_volume_change
    if (version, fingerprint) == (before_volume_change.STRATEGY_VERSION, before_volume_change.strategy_fingerprint()):
        return before_volume_change
    from research.archived_g3_v11 import contract as before_additions
    if (version, fingerprint) == (before_additions.STRATEGY_VERSION, before_additions.strategy_fingerprint()):
        return before_additions
    from research.archived_g3_v10 import contract as previous
    if (version, fingerprint) == (previous.STRATEGY_VERSION, previous.strategy_fingerprint()):
        return previous
    from research.archived_g3_v9 import contract as archived
    if (version, fingerprint) == (archived.STRATEGY_VERSION, archived.strategy_fingerprint()):
        return archived
    from research.archived_g3_v8 import contract as older
    if (version, fingerprint) == (older.STRATEGY_VERSION, older.strategy_fingerprint()):
        return older
    return None


def supported_identity(version, fingerprint, *, allow_legacy=False):
    return ((version, fingerprint) == (STRATEGY_VERSION, strategy_fingerprint())
            or allow_legacy and (contract_for_identity(version, fingerprint) is not None
                or production.supported_identity(version, fingerprint, allow_legacy=True)))


def validate_contract(value=None):
    production.validate_contract()
    if production.strategy_fingerprint() != SOURCE_FINGERPRINT:
        raise ValueError("Cutoff backtest donor production configuration changed")
    if len(ACTIVE_SETUPS) != 51 or len(DISABLED_SETUPS) != 10:
        raise ValueError("Cutoff backtest must retain 51 and disable 10 setups")
    exceptions = {s.setup_id: s.target_pct for s in ACTIVE_SETUPS if s.target_pct <= TARGET_CUTOFF_PCT}
    if exceptions != RESTORED_TARGETS:
        raise ValueError("Cutoff must preserve exactly the three explicitly restored setups")
    if value is not None and value != strategy_payload():
        raise ValueError("G3 cutoff backtest differs from the accepted normalized contract")


def load_contract(path=None):
    value = strategy_payload() if path is None else json.loads(Path(path).read_text(encoding='utf-8'))
    validate_contract(value)
    return copy.deepcopy(value)
