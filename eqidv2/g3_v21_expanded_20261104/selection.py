"""Isolated V21 backtest: all G3 clocks through 14:00, three entries per side/slot."""
from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta
import hashlib
import json
import math

import pandas as pd

from g3_v18_20261102.selection import strict_signals
from g3_v21_selective_confirmation_20261104 import selection as v21
from research import g3_slot_win70_contract as v17
import fno_v13_v10_g_3_contract as donor


VERSION = "G3-V21-20261104-EXPANDED-TO-1400-CAP3-RESEARCH"
CAP = 3
NEW_TARGET_PCT = 0.8
NEW_STOP_PCT = 1.25
LAST_SIGNAL = "14:00"
ENABLED_BEFORE = frozenset(v17._ACTIVE_BY_ID)
DONOR_SETUPS = dict(donor._ACTIVE_BY_ID)
EXTRA_CLOCKS = ("13:35", "13:40", "13:45", "13:50", "13:55", "14:00")
TEMPLATES = {"LONG": "1331_LONG", "SHORT": "1326_SHORT"}


def _clone(template, clock: str):
    confirmation = (datetime.strptime(clock, "%H:%M") + timedelta(minutes=1)).strftime("%H:%M")
    return replace(template, signal_end=clock, confirmation_end=confirmation,
                   max_entries=CAP, target_pct=NEW_TARGET_PCT, stop_pct=NEW_STOP_PCT,
                   source_version=VERSION)


EXTRA_SETUPS = {spec.setup_id: spec for clock in EXTRA_CLOCKS for side, key in TEMPLATES.items()
                for spec in (_clone(DONOR_SETUPS[key], clock),)}
EXTRA_CORE = {spec.setup_id: spec for clock in EXTRA_CLOCKS for side, key in TEMPLATES.items()
              for spec in (_clone(donor._CORE_BY_ID[key], clock),)}
SPECS = DONOR_SETUPS | EXTRA_SETUPS
CORE = donor._CORE_BY_ID | EXTRA_CORE
NEWLY_ENABLED = frozenset(SPECS) - ENABLED_BEFORE
if len(SPECS) != 73 or len(ENABLED_BEFORE) != 34 or len(NEWLY_ENABLED) != 39:
    raise ValueError("Configured donor slot inventory changed")


def configuration() -> dict:
    return dict(version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        source_variant=v21.VERSION, source_fingerprint=v21.fingerprint(),
        donor_fingerprint=donor.strategy_fingerprint(), cap_per_day_setup_side=CAP,
        signal_clock_limit=LAST_SIGNAL, donor_last_signal=max(s.signal_end for s in DONOR_SETUPS.values()),
        new_signal_clocks=list(EXTRA_CLOCKS), new_setup_templates=dict(TEMPLATES),
        new_long_confirmation_volume_min=donor.confirmation_volume_minimum("LONG", setup_id=TEMPLATES["LONG"]),
        existing_setup_ids=sorted(ENABLED_BEFORE), newly_enabled_setup_ids=sorted(NEWLY_ENABLED),
        newly_enabled_target_pct=NEW_TARGET_PCT, newly_enabled_stop_pct=NEW_STOP_PCT,
        existing_targets_unchanged=True,
        existing_confirmation_exceptions=v21.RULES,
        preserved_0926_short_minimum_lower_wick=0.20,
        new_time_slot_definitions=True)


def fingerprint() -> str:
    return hashlib.sha256(json.dumps(configuration(), sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def _relaxed(spec, setup_id: str):
    rule = v21.RULES.get(setup_id)
    return replace(spec, **rule) if rule else spec


def _passes(row: dict, spec, day: date) -> bool:
    checked = row
    if spec.setup_id in EXTRA_SETUPS and spec.side == "LONG":
        # 13:30 LONG's inherited confirmation-volume floor is 0.80. The
        # production helper looks up the *new* ID and would otherwise use 1.10.
        # Preserve the observed value in output; only adapt this one predicate.
        observed = donor._number(row.get("v9_1m_volume_ratio"))
        inherited_floor = donor.confirmation_volume_minimum("LONG", setup_id=TEMPLATES["LONG"])
        if not math.isfinite(observed) or observed < inherited_floor:
            return False
        checked = {**row, "v9_1m_volume_ratio": max(observed, donor.MIN_CONFIRMATION_VOLUME_BY_SIDE["LONG"])}
    if not donor.passes_selected_filters(checked, _relaxed(spec, spec.setup_id),
                                         session_date=day):
        return False
    if spec.setup_id == "0926_SHORT":
        try:
            return float(row.get("wick_ratio")) >= 0.20
        except (TypeError, ValueError):
            return False
    return True


def _extra_candidates(strict: pd.DataFrame) -> pd.DataFrame:
    records = []
    for record in strict.to_dict("records"):
        row = donor._candidate(record)
        stamp = donor._stamp(row.get("signal_timestamp"))
        if stamp.strftime("%H:%M") not in EXTRA_CLOCKS:
            continue
        side = str(row.get("side", "")).upper()
        spec = EXTRA_SETUPS.get(f"{(stamp + timedelta(minutes=1)).strftime('%H%M')}_{side}")
        if spec is None:
            continue
        row.update(day=stamp.date(), side=side, signal_ts=stamp,
                   confirmation_ts=donor._stamp(row.get("confirmation_timestamp")),
                   hhmm=stamp.strftime("%H%M"), hhmm_int=int(stamp.strftime("%H%M")),
                   setup_id=spec.setup_id, sid=record["sid"],
                   wick_ratio=row.get("v9_1m_upper_wick_ratio" if side == "LONG" else "v9_1m_lower_wick_ratio"),
                   trigger=row.get("confirm_high" if side == "LONG" else "confirm_low"),
                   native_stop_pct=NEW_STOP_PCT, native_target_pct=NEW_TARGET_PCT,
                   stop_pct=NEW_STOP_PCT, target_pct=NEW_TARGET_PCT,
                   initial_stop_pct=donor.INITIAL_STOP_PCT,
                   tightened_stop_pct=donor.TIGHTENED_STOP_PCT,
                   tighten_after_minutes=donor.TIGHTEN_AFTER_MINUTES,
                   configured_confirmation_end=spec.confirmation_end,
                   picker=spec.picker, v10_g_morning_slot=False,
                   v9_configuration=donor.STRATEGY_PROFILE,
                   relaxed_0925_added=False, g3_confirmation_offset_minutes=1,
                   effective_confirmation_volume_min=donor.confirmation_volume_minimum(
                       side, setup_id=TEMPLATES[side]))
        records.append(row)
    return pd.DataFrame(records)


def select(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return expanded V21 research orders, full audit, and frozen V21 orders."""
    old_orders, _, _ = v21.select(raw)
    strict = strict_signals(raw)
    audit = donor.selection_audit(strict)
    extras = _extra_candidates(strict)
    if not extras.empty:
        audit = pd.concat([audit, extras], ignore_index=True, sort=False)
    audit = audit.loc[audit.setup_id.isin(SPECS)].reset_index(drop=True)
    if audit.empty:
        return audit.copy(), audit, old_orders
    if audit.sid.duplicated().any() or audit.duplicated(["day", "setup_id", "tradingsymbol"]).any():
        raise ValueError("Duplicate donor candidate")
    old_ids = set(old_orders.sid)
    if not old_ids.issubset(set(audit.sid)):
        raise ValueError("Expanded audit lost a V21 candidate")
    audit["v21_selected"] = audit.sid.isin(old_ids)
    audit["donor_selected"] = audit.v9_selected
    audit["donor_signal_id"] = audit.signal_id
    audit["newly_enabled"] = audit.setup_id.isin(NEWLY_ENABLED)
    audit["donor_target_pct"] = audit.setup_id.map({key: spec.target_pct for key, spec in SPECS.items()})
    audit["target_pct"] = [NEW_TARGET_PCT if key in NEWLY_ENABLED else SPECS[key].target_pct
                           for key in audit.setup_id]
    audit["native_target_pct"] = audit.target_pct
    audit["stop_pct"] = [NEW_STOP_PCT if key in NEWLY_ENABLED else SPECS[key].stop_pct
                         for key in audit.setup_id]
    audit["native_stop_pct"] = audit.stop_pct
    audit["max_entries"] = CAP
    audit["v9_filter_pass"] = False
    audit["v9_selected"] = False
    audit["v10_g_f_core"] = False
    audit["v9_rank_in_setup_day"] = pd.NA
    audit["v9_decision"] = "FILTER_REJECTED"
    records = audit.to_dict("records")
    for (day_value, setup_id), indices in audit.groupby(["day", "setup_id"], sort=True).groups.items():
        day = date.fromisoformat(str(day_value)[:10])
        spec = SPECS[setup_id]
        core_spec = CORE[setup_id]
        eligible = [idx for idx in indices if _passes(records[audit.index.get_loc(idx)], spec, day)]
        eligible.sort(key=lambda idx: donor._rank_key(records[audit.index.get_loc(idx)], spec))
        core = [idx for idx in eligible if _passes(records[audit.index.get_loc(idx)], core_spec, day)]
        picked = core[:CAP]
        picked_set = set(picked)
        picked += [idx for idx in eligible if idx not in picked_set][:CAP-len(picked)]
        core_ids = set(core[:CAP])
        audit.loc[eligible, "v9_filter_pass"] = True
        audit.loc[picked, "v9_selected"] = True
        audit.loc[picked, "v10_g_f_core"] = [idx in core_ids for idx in picked]
        audit.loc[eligible, "v9_rank_in_setup_day"] = list(range(1, len(eligible)+1))
        audit.loc[eligible, "v9_decision"] = "RANKED_OUT_CAP3"
        audit.loc[picked, "v9_decision"] = "SELECTED_CAP3"
    audit["strategy_version"] = VERSION
    audit["strategy_fingerprint"] = fingerprint()
    audit["research_variant"] = "v21_all_donor_and_1400_slots_cap3"
    audit["execution_authority"] = False
    audit["scope"] = "BACKTEST_ONLY"
    audit["signal_id"] = [donor.canonical_signal_id(
        VERSION, date.fromisoformat(str(row.day)[:10]),
        SPECS[row.setup_id].signal_end, SPECS[row.setup_id].confirmation_end,
        SPECS[row.setup_id].side, row.tradingsymbol) for row in audit.itertuples()]
    audit = audit.sort_values(["day", "hhmm_int", "side", "setup_id", "tradingsymbol"],
                              kind="stable").reset_index(drop=True)
    orders = audit.loc[audit.v9_selected.eq(True)].reset_index(drop=True)
    if orders.signal_id.duplicated().any() or orders.sid.duplicated().any():
        raise ValueError("Duplicate selected identity")
    if orders.groupby(["day", "setup_id"]).size().gt(CAP).any():
        raise ValueError("Per-slot cap exceeded")
    return orders, audit, old_orders
