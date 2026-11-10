"""Frozen, bounded CVD acceleration ranking adjustment; native G3 filters remain.

Read-only reuse of native candidate filtering and setup definitions. No global
monkey-patching, added rejection gate, or replacement of original picker values.
"""
from __future__ import annotations

from datetime import date
import hashlib
import json

import numpy as np
import pandas as pd

from g3_v18_20261102 import selection as native

VERSION = "G3-V18-CVD-ACCELERATION-RANK-RESEARCH-20261102"
VARIANTS = ("base_control", "acceleration_rank", "acceleration_rvol_rank")
WEIGHT = .10


def configuration():
    return dict(version=VERSION, variants=list(VARIANTS), execution_authority=False,
        source_type="ONE_MINUTE_OHLCV_CLOSE_LOCATION_PRESSURE_ESTIMATE_NOT_AGGRESSOR_DELTA",
        normalized_picker="Unmodified native setup picker divided by positive finite maximum across all eligible day/setup candidates",
        acceleration="Current completed 15m delta/volume minus preceding completed 15m delta/volume; divided by2 and signed by LONG+1/SHORT-1",
        ranking_score="normalized_picker + 0.10 * directional_acceleration * activity_multiplier",
        weight=WEIGHT, activity_multiplier=dict(acceleration_rank="1", acceleration_rvol_rank="clip(rvol20-1,0,1)"),
        neutral_policy="Whole eligible day/setup competition unchanged before09:45, if any competitor lacks exact-clock required features, invalid picker maximum, or eligible_count<=quota",
        native_ties="Original native picker descending, traded value descending, symbol ascending",
        retained="Native strict/filter eligibility, core-first selection, extras quota, all source metrics, confirmations, exits and capital",
        threshold_search=False, feature_clock="Completed signal timestamp only; never confirmation/future features",
        no_independent_holdout=True, last_three_sessions_previously_viewed=True,
        reference_strategy_fingerprint=native.control.strategy_fingerprint())


def _flags(series):
    return series.astype(str).str.lower().isin(("true", "1", "1.0"))


def _ready(rows, variant):
    fields = {"adv_ready", "adv_feature_ts", "adv_acceleration"}
    if variant == "acceleration_rvol_rank":
        fields |= {"adv_rvol_ready", "adv_rvol20"}
    if fields - set(rows):
        return False
    stamps = pd.to_datetime(rows.adv_feature_ts, utc=True, errors="coerce")
    signals = pd.to_datetime(rows.signal_ts, utc=True, errors="coerce")
    delta = pd.to_numeric(rows.adv_acceleration, errors="coerce")
    ready = _flags(rows.adv_ready) & signals.notna() & stamps.eq(signals) & np.isfinite(delta) & delta.between(-2, 2)
    if variant == "acceleration_rvol_rank":
        rvol = pd.to_numeric(rows.adv_rvol20, errors="coerce")
        ready &= _flags(rows.adv_rvol_ready) & np.isfinite(rvol) & rvol.ge(0)
    return bool(ready.all())


def _variant_audit(original, variant):
    if variant == "base_control":
        return original.copy()
    if variant not in VARIANTS:
        raise ValueError(f"Unknown advanced CVD variant: {variant}")
    audit = original.copy()
    audit["adv_rank_original_selected"] = _flags(audit.v9_selected)
    audit["adv_rank_original_position"] = audit.v9_rank_in_setup_day
    audit["adv_rank_original_decision"] = audit.v9_decision
    audit["adv_rank_selected_changed"] = False
    audit["adv_rank_group_applied"] = False
    audit["adv_rank_reason"] = "NO_ELIGIBLE_CANDIDATES"
    audit["adv_rank_eligible_count"] = 0
    audit["adv_rank_adjustment"] = 0.
    audit["adv_rank_activity_multiplier"] = 0.
    for name in ("picker_value", "picker_normalized", "directional_acceleration", "score", "position"):
        audit["adv_rank_" + name] = np.nan
    audit["adv_rank_core_eligible"] = False
    for (_, setup_id), group in audit.groupby(["day", "setup_id"], sort=True):
        setup, core_setup = native.control._ACTIVE_BY_ID[setup_id], native.control._CORE_BY_ID[setup_id]
        eligible = group.loc[_flags(group.v9_filter_pass)]
        audit.loc[group.index, "adv_rank_eligible_count"] = len(eligible)
        if eligible.empty:
            continue
        rows = {i: row for i, row in eligible.iterrows()}
        picker = pd.Series({i: native.production.picker_value(row, setup.picker) for i, row in rows.items()}, dtype=float)
        maximum = float(picker.max())
        valid_picker = bool(np.isfinite(picker).all() and np.isfinite(maximum) and maximum > 0)
        normalized = picker / maximum if valid_picker else pd.Series(np.nan, index=picker.index)
        day = date.fromisoformat(str(eligible.day.iloc[0])[:10])
        core = {i: native.control.passes_selected_filters(row.to_dict(), core_setup, session_date=day) for i, row in rows.items()}
        audit.loc[eligible.index, "adv_rank_core_eligible"] = pd.Series(core)
        audit.loc[eligible.index, "adv_rank_picker_value"] = picker
        audit.loc[eligible.index, "adv_rank_picker_normalized"] = normalized
        audit.loc[eligible.index, "adv_rank_score"] = normalized
        audit.loc[eligible.index, "adv_rank_position"] = eligible.v9_rank_in_setup_day
        reason = ("EARLY_SESSION_OR_WARMUP" if setup.signal_end < "09:45" else
                  "NO_SELECTION_COMPETITION" if len(eligible) <= setup.max_entries else
                  "INVALID_PICKER_MAXIMUM" if not valid_picker else
                  "WHOLE_GROUP_FEATURE_UNAVAILABLE" if not _ready(eligible, variant) else "ADJUSTED_RANKING")
        audit.loc[group.index, "adv_rank_reason"] = reason
        if reason != "ADJUSTED_RANKING":
            continue
        direction = 1. if setup.side == "LONG" else -1.
        acceleration = direction * pd.to_numeric(eligible.adv_acceleration, errors="raise") / 2.
        multiplier = (pd.to_numeric(eligible.adv_rvol20, errors="raise").sub(1).clip(0, 1)
                      if variant == "acceleration_rvol_rank" else pd.Series(1., index=eligible.index))
        adjustment = WEIGHT * acceleration * multiplier
        score = normalized + adjustment
        audit.loc[group.index, "adv_rank_group_applied"] = True
        audit.loc[eligible.index, "adv_rank_directional_acceleration"] = acceleration
        audit.loc[eligible.index, "adv_rank_activity_multiplier"] = multiplier
        audit.loc[eligible.index, "adv_rank_adjustment"] = adjustment
        audit.loc[eligible.index, "adv_rank_score"] = score
        ranked = sorted(rows, key=lambda i: (-float(score.loc[i]), *native.production._rank_key(rows[i], setup)))
        chosen_core = [i for i in ranked if core[i]][:core_setup.max_entries]
        chosen = chosen_core + [i for i in ranked if i not in chosen_core][:setup.max_entries - len(chosen_core)]
        audit.loc[eligible.index, "v9_selected"] = False
        audit.loc[eligible.index, "v10_g_f_core"] = False
        audit.loc[eligible.index, "v9_decision"] = "RANKED_OUT"
        audit.loc[chosen, "v9_selected"] = True
        audit.loc[chosen_core, "v10_g_f_core"] = True
        audit.loc[chosen, "v9_decision"] = "SELECTED"
        for rank, i in enumerate(ranked, 1):
            audit.loc[i, "v9_rank_in_setup_day"] = rank
            audit.loc[i, "adv_rank_position"] = rank
        audit.loc[group.index, "adv_rank_selected_changed"] = (
            _flags(audit.loc[group.index, "v9_selected"]) != audit.loc[group.index, "adv_rank_original_selected"])
    audit["research_variant"] = variant
    audit["execution_authority"] = False
    audit["scope"] = "BACKTEST_ONLY"
    audit["source_strategy_version"] = native.control.STRATEGY_VERSION
    audit["source_signal_id"] = audit.signal_id
    version = VERSION + "-" + variant.upper()
    audit["strategy_version"] = version
    audit["strategy_fingerprint"] = hashlib.sha256(json.dumps({**configuration(), "variant": variant}, sort_keys=True).encode()).hexdigest()
    for i, row in audit.iterrows():
        setup = native.control._ACTIVE_BY_ID[row.setup_id]
        audit.loc[i, "signal_id"] = native.production.canonical_signal_id(
            version, row.day, setup.signal_end, setup.confirmation_end, setup.side, row.tradingsymbol)
    return audit


def select_all(raw):
    strict = native.strict_signals(raw)
    original = native.control.selection_audit(strict)
    result = {}
    for variant in VARIANTS:
        audit = _variant_audit(original, variant)
        result[variant] = (audit.loc[audit.v9_selected.eq(True)].reset_index(drop=True), audit)
    return result


def select(raw, variant="base_control"):
    if variant not in VARIANTS:
        raise ValueError(f"Unknown advanced CVD variant: {variant}")
    return select_all(raw)[variant]
