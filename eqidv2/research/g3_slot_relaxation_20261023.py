"""Pure offline G3 V14 slot ablations; no production configuration mutation.

The baseline reproduces V14 selection, including its original-core-first quota
policy. Experiments change only the documented three morning SHORT setups.
Existing slots, targets, stops, clock checks and pickers are retained. This
module selects orders only; callers own execution and capital assumptions.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import date, timedelta
import hashlib
import json
import math
from typing import Any, Mapping

import pandas as pd

from research.g3_backtest_profile import contract

production = contract.production
PROFILES = ("baseline", "targeted_OI", "targeted_volume", "combined_morning",
            "combined_morning_two_0931")


@dataclass(frozen=True)
class SelectionProfile:
    name: str
    core_setups: tuple
    active_setups: tuple
    oi_floors: tuple[tuple[str, float], ...] = ()
    non_increasing_oi: frozenset[str] = frozenset()
    base_volume: tuple[tuple[str, float], ...] = ()
    confirmation_volume: tuple[tuple[str, float], ...] = ()
    nifty_ceiling: tuple[tuple[str, float], ...] = ()


def make_profile(name: str = "baseline") -> SelectionProfile:
    """Return fresh immutable setup copies; never patch imported contracts."""
    if name not in PROFILES:
        raise ValueError(f"Unknown slot-relaxation profile {name!r}; choose {PROFILES}")
    oi = name in {"targeted_OI", "combined_morning", "combined_morning_two_0931"}
    volume = name in {"targeted_volume", "combined_morning", "combined_morning_two_0931"}
    combined = name in {"combined_morning", "combined_morning_two_0931"}
    oi_floors = {"0921_SHORT": 0., "0931_SHORT": -3., "0936_SHORT": .05} if oi else {}
    confirmation = {"0921_SHORT": .60, "0931_SHORT": .90, "0936_SHORT": .50} if volume else {}

    def copied(spec):
        changes = {}
        if spec.setup_id in oi_floors:
            changes["oi_change_pct"] = oi_floors[spec.setup_id]
        if volume and spec.setup_id == "0936_SHORT":
            changes["volume_ratio"] = .40
        if combined and spec.setup_id == "0921_SHORT":
            changes["body_ratio"] = .30
        if name == "combined_morning_two_0931" and spec.setup_id == "0931_SHORT":
            changes["max_entries"] = 2
        return replace(spec, **changes)

    return SelectionProfile(name, tuple(copied(s) for s in contract.CORE_SETUPS),
        tuple(copied(s) for s in contract.ACTIVE_SETUPS), tuple(oi_floors.items()),
        frozenset({"0921_SHORT", "0931_SHORT"}) if oi else frozenset(),
        (("0936_SHORT", .40),) if volume else (), tuple(confirmation.items()),
        (("0921_SHORT", .05),) if combined else ())


def _profile(value: str | SelectionProfile) -> SelectionProfile:
    return value if isinstance(value, SelectionProfile) else make_profile(value)


def profile_configuration(profile: str | SelectionProfile = "baseline") -> dict:
    p = _profile(profile)
    return dict(scope="OFFLINE_SELECTION_RESEARCH", execution_authority=False,
        profile=p.name, base_strategy_version=contract.STRATEGY_VERSION,
        base_strategy_fingerprint=contract.strategy_fingerprint(),
        core_setups=[asdict(s) for s in p.core_setups],
        active_setups=[asdict(s) for s in p.active_setups],
        base_oi_minimum_by_setup=dict(p.oi_floors),
        non_increasing_oi_permitted_setup_ids=sorted(p.non_increasing_oi),
        base_oi_maximum_pct=production.MAX_OI_CHANGE_PCT,
        positive_finite_current_and_previous_oi_required=True,
        base_volume_minimum_by_setup=dict(p.base_volume),
        confirmation_volume_minimum_by_setup=dict(p.confirmation_volume),
        nifty_first_bar_max_return_pct_by_setup=dict(p.nifty_ceiling),
        unchanged="Existing slots, targets, stops, pickers, confirmation direction/chronology and core-first ranking",
        quota_change={"0931_SHORT": 2} if p.name == "combined_morning_two_0931" else {},
        added_setups=[], capital_policy="CALLER_SPECIFIED")


def _identity(p):
    if p.name == "baseline":
        return contract.STRATEGY_VERSION, contract.strategy_fingerprint()
    version = "G3-SLOT-RELAXATION-20261023-" + p.name.upper()
    fingerprint = hashlib.sha256(json.dumps(profile_configuration(p), sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    return version, fingerprint


def base_signal_side(candidate: Mapping[str, Any], signal_end: str, *,
                     profile: str | SelectionProfile = "baseline",
                     session_date: date | None = None) -> str | None:
    """Original base gate with explicit, slot-local OI/volume/Nifty overrides."""
    p = _profile(profile)
    row = production._candidate(candidate)
    if signal_end == "09:20" and (
            row.get("opening_0920_status") != "VALID"
            or row.get("price_change_basis") != "09:15_CASH_OPEN_TO_09:20_CASH_CLOSE"
            or row.get("oi_change_source") not in {
                "FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M", "PROVISIONAL_FULL_QUOTE"}):
        return None
    if session_date is not None:
        try:
            if production._stamp(row.get("signal_timestamp")).date() != session_date:
                return None
        except (TypeError, ValueError):
            return None
    values = {k: production._number(row.get(k)) for k in (
        "ema9", "ema20", "ema50", "price_change_pct", "oi_change_pct", "volume_ratio", "oi", "prev_oi")}
    if not all(math.isfinite(v) for v in values.values()):
        return None
    if (values["ema9"] > values["ema20"] > values["ema50"]
            and values["price_change_pct"] >= production.BASE_PRICE_CHANGE_PCT):
        side = "LONG"
    elif (values["ema9"] < values["ema20"] < values["ema50"]
            and values["price_change_pct"] <= -production.BASE_PRICE_CHANGE_PCT):
        side = "SHORT"
    else:
        return None
    confirmation = (pd.Timestamp("2000-01-01 " + signal_end) + pd.Timedelta(minutes=1)).strftime("%H%M")
    setup_id = confirmation + "_" + side
    floor = dict(p.oi_floors).get(setup_id, production.BASE_OI_CHANGE_PCT)
    min_volume = dict(p.base_volume).get(setup_id, production.BASE_VOLUME_RATIO)
    if (values["prev_oi"] <= 0 or values["oi"] <= 0
            or (setup_id not in p.non_increasing_oi and values["oi"] <= values["prev_oi"])
            or not floor <= values["oi_change_pct"] <= production.MAX_OI_CHANGE_PCT
            or values["volume_ratio"] < min_volume):
        return None
    if side == "SHORT" and signal_end in production.NIFTY_SHORT_SIGNAL_ENDS:
        context = production._number(row.get("nifty_first_bar_return_pct"))
        ceiling = dict(p.nifty_ceiling).get(setup_id, production.NIFTY_FIRST_BAR_MAX_RETURN_PCT)
        if not math.isfinite(context) or context > ceiling:
            return None
    return side


def passes_selected_filters(candidate: Mapping[str, Any], setup, *,
                            profile: str | SelectionProfile = "baseline",
                            session_date: date | None = None) -> bool:
    """Retain source, exact clock, direction, range and extension checks."""
    p = _profile(profile)
    row = production._candidate(candidate)
    if str(row.get("side", "")).upper() != setup.side:
        return False
    try:
        signal = production._stamp(row.get("signal_timestamp"))
        confirmation = production._stamp(row.get("confirmation_timestamp"))
        feature = production._stamp(row.get("v9_1m_feature_ts"))
        if ((session_date is not None and signal.date() != session_date)
                or signal.strftime("%H:%M") != setup.signal_end or signal != signal.floor("min")
                or confirmation != signal + timedelta(minutes=1) or feature != confirmation):
            return False
        if "v9_5m_feature_ts" in row and production._stamp(row["v9_5m_feature_ts"]) > signal:
            return False
        if "v9_feature_available_ts" in row and production._stamp(row["v9_feature_available_ts"]) > confirmation:
            return False
    except (TypeError, ValueError):
        return False
    if row.get("signal_end", setup.signal_end) != setup.signal_end:
        return False
    if (not production._confirmation_valid(row, setup.side)
            or base_signal_side(row, setup.signal_end, profile=p, session_date=session_date) != setup.side):
        return False
    vals = {k: production._number(row.get(k)) for k in (
        "price_change_pct", "oi_change_pct", "volume_ratio", "body_ratio", "wick_ratio", "traded_value", "v9_1m_volume_ratio")}
    if not all(math.isfinite(v) for v in vals.values()):
        return False
    if setup.setup_id in production.MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP:
        extension = production.extension_atr(row.get("confirm_close"), row.get("v9_1m_ema20"),
                                             row.get("v9_1m_atr14"), setup.side)
        if not math.isfinite(extension) or extension > production.MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP[setup.setup_id]:
            return False
    min_confirmation = dict(p.confirmation_volume).get(setup.setup_id,
        production.confirmation_volume_minimum(setup.side, setup_id=setup.setup_id))
    return (vals["price_change_pct"] * (1 if setup.side == "LONG" else -1) >= setup.price_change_pct
        and vals["oi_change_pct"] >= setup.oi_change_pct and vals["volume_ratio"] >= setup.volume_ratio
        and setup.body_ratio <= vals["body_ratio"] <= 1. and 0 <= vals["wick_ratio"] <= setup.max_wick_ratio
        and vals["traded_value"] >= setup.min_traded_value and vals["v9_1m_volume_ratio"] >= min_confirmation)


def selection_audit(features: pd.DataFrame, profile: str | SelectionProfile = "baseline") -> pd.DataFrame:
    """Select from complete observed features, preserving consumed native fields."""
    p = _profile(profile)
    active = {s.setup_id: s for s in p.active_setups}
    core = {s.setup_id: s for s in p.core_setups}
    mapping = {(s.signal_end, s.side): s for s in p.active_setups}
    metadata = ["setup_id", "native_stop_pct", "native_target_pct", "configured_confirmation_end",
        "picker", "max_entries", "v10_g_f_core", "v10_g_morning_slot", "v9_selected", "v9_filter_pass",
        "v9_rank_in_setup_day", "v9_decision", "v9_configuration", "strategy_version",
        "strategy_fingerprint", "signal_id", "day", "hhmm_int", "sid"]
    if "sid" in features and features.sid.duplicated().any():
        raise ValueError("Duplicate signal IDs")
    pools = {}
    for index, record in enumerate(features.to_dict("records")):
        row = production._candidate(record)
        try:
            stamp = production._stamp(row.get("signal_timestamp"))
        except (TypeError, ValueError):
            continue
        signal_end = stamp.strftime("%H:%M")
        side = str(row.get("side", "")).upper()
        if side not in ("LONG", "SHORT"):
            side = base_signal_side(row, signal_end, profile=p) or ""
        setup = mapping.get((signal_end, side))
        if setup is None:
            continue
        day = stamp.date()
        if "day" in row and production._stamp(row["day"]).date() != day:
            raise ValueError("Candidate day conflicts with signal timestamp")
        try:
            confirmation = production._stamp(row.get("confirmation_timestamp"))
        except (TypeError, ValueError):
            confirmation = pd.NaT
        if "wick_ratio" not in row:
            row["wick_ratio"] = row.get("v9_1m_upper_wick_ratio" if side == "LONG" else "v9_1m_lower_wick_ratio")
        if "trigger" not in row:
            row["trigger"] = row.get("confirm_high" if side == "LONG" else "confirm_low")
        row.update(day=day, side=side, signal_ts=stamp, confirmation_ts=confirmation,
                   hhmm=stamp.strftime("%H%M"), hhmm_int=int(stamp.strftime("%H%M")), sid=row.get("sid", index))
        if setup.setup_id in production.MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP:
            row["v9_1m_extension_atr"] = production.extension_atr(row.get("confirm_close"),
                row.get("v9_1m_ema20"), row.get("v9_1m_atr14"), side)
        pools.setdefault((day, setup.setup_id), []).append(row)
    parts = []
    production_fingerprint = production.strategy_fingerprint()
    for (day, setup_id), candidates in sorted(pools.items()):
        spec, core_spec = active[setup_id], core[setup_id]
        if len({str(r["tradingsymbol"]) for r in candidates}) != len(candidates):
            raise ValueError("Duplicate candidate for one setup/session/symbol")
        eligible = sorted([r for r in candidates if passes_selected_filters(r, spec, profile=p, session_date=day)],
                          key=lambda r: production._rank_key(r, spec))
        core_selected = [r for r in eligible if passes_selected_filters(r, core_spec, profile=p,
                         session_date=day)][:core_spec.max_entries]
        core_symbols = {str(r["tradingsymbol"]) for r in core_selected}
        extras = [r for r in eligible if str(r["tradingsymbol"]) not in core_symbols][:spec.max_entries-len(core_selected)]
        selected = core_symbols | {str(r["tradingsymbol"]) for r in extras}
        ranks = {str(r["tradingsymbol"]): n for n, r in enumerate(eligible, 1)}
        for row in candidates:
            symbol = str(row["tradingsymbol"])
            chosen, passed = symbol in selected, symbol in ranks
            row.update(setup_id=setup_id, native_stop_pct=spec.stop_pct, native_target_pct=spec.target_pct,
                stop_pct=spec.stop_pct, target_pct=spec.target_pct, initial_stop_pct=production.INITIAL_STOP_PCT,
                tightened_stop_pct=production.TIGHTENED_STOP_PCT, tighten_after_minutes=production.TIGHTEN_AFTER_MINUTES,
                configured_confirmation_end=spec.confirmation_end, picker=spec.picker, max_entries=spec.max_entries,
                v10_g_f_core=chosen and symbol in core_symbols, v10_g_morning_slot=False,
                v9_selected=chosen, v9_filter_pass=passed, v9_rank_in_setup_day=ranks.get(symbol),
                v9_decision="SELECTED" if chosen else "RANKED_OUT" if passed else "FILTER_REJECTED",
                v9_configuration=production.STRATEGY_PROFILE, strategy_version=production.STRATEGY_VERSION,
                strategy_fingerprint=production_fingerprint, relaxed_0925_added=False,
                g3_confirmation_offset_minutes=1, signal_id=production.canonical_signal_id(
                    production.STRATEGY_VERSION, day, spec.signal_end, spec.confirmation_end, spec.side, symbol))
            parts.append(row)
    audit = (pd.DataFrame(parts).sort_values(["day", "hhmm_int", "side", "setup_id", "tradingsymbol"],
             kind="stable").reset_index(drop=True) if parts else
             pd.DataFrame(columns=list(dict.fromkeys([*features.columns, *metadata]))))
    audit = contract.identify_orders(audit)
    if p.name != "baseline":
        version, fingerprint = _identity(p)
        audit["strategy_version"] = version
        audit["strategy_fingerprint"] = fingerprint
        for index, row in audit.iterrows():
            spec = active[row.setup_id]
            audit.loc[index, "signal_id"] = production.canonical_signal_id(version, row.day,
                spec.signal_end, spec.confirmation_end, spec.side, row.tradingsymbol)
        audit["research_profile"] = p.name
        audit["research_scope"] = "OFFLINE_SELECTION_RESEARCH"
        audit["execution_authority"] = False
    return audit


def select_orders(features: pd.DataFrame, profile: str | SelectionProfile = "baseline") -> pd.DataFrame:
    audit = selection_audit(features, profile)
    return audit.loc[audit.v9_selected.eq(True)].reset_index(drop=True)
