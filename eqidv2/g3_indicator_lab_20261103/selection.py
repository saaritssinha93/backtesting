"""Independent, auditable G3 selector for research-only filter ablations.

``prepare`` starts BEFORE alpha filters, matching all 51 V15 setups.  It
preserves one row per symbol/signal and exposes each filter independently.
``select`` applies fixed ablations, optional additional eligibility and an
optional ranking score, then recomputes core priority, replacement and quotas.
No production module is patched and no file or current pointer is written.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json

import numpy as np
import pandas as pd

import fno_v13_v10_g_3_contract as production
from research import g3_target_cutoff_contract as donor
from research import g3_slot_win70_contract as control

VERSION = "G3-INDICATOR-LAB-20261103"
GATES = ("ema", "price", "oi_lower", "oi_upper", "vol5", "vol1", "body",
         "max_wick", "min_wick", "extension")
VARIANTS = ("control", "no_ema", "no_oi_lower", "no_oi_upper", "no_vol5",
            "no_vol1", "no_body", "no_max_wick", "no_min_wick", "no_core",
            "all51_slots")
IST = "Asia/Kolkata"


def _numeric(frame, column):
    return pd.to_numeric(frame.get(column, pd.Series(np.nan, index=frame.index)), errors="coerce")


def _true(values):
    return values.astype(str).str.strip().str.lower().isin(("true", "1", "1.0", "yes", "on"))


def _stamps(frame, column):
    return pd.to_datetime(frame.get(column, pd.Series(pd.NaT, index=frame.index)),
                          errors="coerce", utc=True).dt.tz_convert(IST)


def _spec_table(core=False):
    specs = donor.CORE_SETUPS if core else donor.ACTIVE_SETUPS
    return {s.setup_id: asdict(s) for s in specs}


def prepare(raw: pd.DataFrame) -> pd.DataFrame:
    """Build normalized candidate rows and gates without using outcomes.

    Input is the full prefilter feature ledger, enriched with any causal
    research columns. ``sid`` is preserved and must be unique. Side comes
    from the signed five-minute move, independent of EMA/OI/volume gates.
    Zero or invalid moves cannot map to a directional setup and are omitted.
    All matched rows, including malformed/missing evidence, remain auditable.
    """
    frame = raw.copy(deep=True)
    if "sid" not in frame:
        frame["sid"] = np.arange(len(frame), dtype=np.int64)
    if frame.sid.isna().any() or frame.sid.duplicated().any():
        raise ValueError("Candidate sid must be present and globally unique")
    for canonical, aliases in {
        "signal_ts": ("signal_timestamp",), "confirmation_ts": ("confirmation_timestamp",),
        "ema9": ("v9_5m_ema9",), "ema20": ("v9_5m_ema20",), "ema50": ("v9_5m_ema50",),
        "confirmation_open": ("confirm_open",), "confirmation_high": ("confirm_high",),
        "confirmation_low": ("confirm_low",), "confirmation_close": ("confirm_close",),
    }.items():
        if canonical not in frame:
            for alias in aliases:
                if alias in frame:
                    frame[canonical] = frame[alias]
                    break
    signal = _stamps(frame, "signal_ts")
    confirmation = _stamps(frame, "confirmation_ts")
    move = _numeric(frame, "price_change_pct")
    side = pd.Series(np.where(move.gt(0), "LONG", np.where(move.lt(0), "SHORT", "")), index=frame.index)
    setup_ids = (signal + pd.Timedelta(minutes=1)).dt.strftime("%H%M").fillna("") + "_" + side
    frame["setup_id"] = setup_ids
    frame["side"] = side
    frame["signal_ts"] = signal
    frame["confirmation_ts"] = confirmation
    keep = setup_ids.isin(donor._ACTIVE_BY_ID)
    frame = frame.loc[keep].copy().reset_index(drop=True)
    signal, confirmation = frame.signal_ts, frame.confirmation_ts
    if frame.duplicated(["signal_ts", "tradingsymbol"]).any():
        raise ValueError("Duplicate candidate for one signal/session/symbol")

    old_day = frame.get("day")
    day_consistent = pd.Series(True, index=frame.index)
    if old_day is not None:
        old_dates = pd.to_datetime(old_day, errors="coerce", utc=True).dt.tz_convert(IST).dt.date
        day_consistent = old_dates.eq(signal.dt.date)
    frame["day"] = signal.dt.date
    frame["signal_timestamp"] = signal
    frame["confirmation_timestamp"] = confirmation
    frame["hhmm"] = signal.dt.strftime("%H%M")
    frame["hhmm_int"] = signal.dt.hour * 100 + signal.dt.minute
    frame["signal_end"] = signal.dt.strftime("%H:%M")
    for short, long in (("open", "confirmation_open"), ("high", "confirmation_high"),
                        ("low", "confirmation_low"), ("close", "confirmation_close")):
        frame[f"confirm_{short}"] = _numeric(frame, long)
    sign = pd.Series(np.where(frame.side.eq("LONG"), 1., -1.), index=frame.index)
    frame["direction_sign"] = sign
    frame["wick_ratio"] = np.where(sign.gt(0), _numeric(frame, "v9_1m_upper_wick_ratio"),
                                   _numeric(frame, "v9_1m_lower_wick_ratio"))
    frame["trigger"] = np.where(sign.gt(0), frame.confirm_high, frame.confirm_low)
    frame["slot_enabled"] = frame.setup_id.isin(control._ACTIVE_BY_ID)
    active_specs, core_specs = _spec_table(), _spec_table(core=True)
    for field in ("price_change_pct", "oi_change_pct", "volume_ratio", "body_ratio", "max_wick_ratio",
                  "min_traded_value", "max_entries", "picker", "stop_pct", "target_pct", "confirmation_end"):
        frame[f"setup_{field}"] = frame.setup_id.map({key: spec[field] for key, spec in active_specs.items()})
        frame[f"core_{field}"] = frame.setup_id.map({key: spec[field] for key, spec in core_specs.items()})

    # These checks are immutable even when economic thresholds are relaxed.
    feature1 = _stamps(frame, "v9_1m_feature_ts")
    integrity = signal.notna() & confirmation.notna() & day_consistent
    integrity &= signal.eq(signal.dt.floor("min"))
    integrity &= confirmation.eq(signal + pd.Timedelta(minutes=1)) & feature1.eq(confirmation)
    for column, deadline in (("v9_5m_feature_ts", signal), ("v9_feature_available_ts", confirmation)):
        if column in frame:
            stamp = _stamps(frame, column)
            integrity &= stamp.notna() & stamp.le(deadline)
    for flag in ("gap_filled", "opening_snapshot", "provisional_stale", "confirmation_source_flagged",
                 "is_synthetic", "synthetic"):
        if flag in frame:
            integrity &= ~_true(frame[flag])
    if "v9_exact_confirmation_present" in frame:
        integrity &= _true(frame.v9_exact_confirmation_present)
    if "confirmed" in frame:
        integrity &= _true(frame.confirmed)
    if "source_1m_count" in frame:
        integrity &= _numeric(frame, "source_1m_count").eq(5)
    o, h, low, close = (frame[f"confirm_{name}"] for name in ("open", "high", "low", "close"))
    for values in (o, h, low, close, _numeric(frame, "signal_close")):
        integrity &= np.isfinite(values) & values.gt(0)
    integrity &= h.gt(low) & h.ge(np.maximum(o, close)) & low.le(np.minimum(o, close))
    integrity &= ((close - o)*sign).gt(0) & ((close-_numeric(frame, "signal_close"))*sign).gt(0)
    if any(name in frame for name in ("open", "high", "low", "close")):
        so, sh, sl, sc = (_numeric(frame, name) for name in ("open", "high", "low", "close"))
        for values in (so, sh, sl, sc):
            integrity &= np.isfinite(values) & values.gt(0)
        integrity &= sh.ge(np.maximum(so, sc)) & sl.le(np.minimum(so, sc)) & sh.ge(sl)
    for field in ("oi", "prev_oi"):
        values = _numeric(frame, field)
        integrity &= np.isfinite(values) & values.gt(0)
    for field in ("price_change_pct", "oi_change_pct"):
        integrity &= np.isfinite(_numeric(frame, field))
    for field in ("volume_ratio", "traded_value", "v9_1m_volume_ratio"):
        values = _numeric(frame, field)
        integrity &= np.isfinite(values) & values.ge(0)
    for field in ("volume", "confirmation_volume"):
        if field in frame:
            values = _numeric(frame, field)
            integrity &= np.isfinite(values) & values.ge(0)
    for field in ("body_ratio", "wick_ratio"):
        integrity &= _numeric(frame, field).between(0, 1)
    integrity &= _numeric(frame, "traded_value").ge(frame.setup_min_traded_value)
    opening = frame.signal_end.eq("09:20")
    opening_ok = (frame.get("opening_0920_status", pd.Series("", index=frame.index)).eq("VALID")
                  & frame.get("price_change_basis", pd.Series("", index=frame.index)).eq(
                      "09:15_CASH_OPEN_TO_09:20_CASH_CLOSE")
                  & frame.get("oi_change_source", pd.Series("", index=frame.index)).isin(
                      ("FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M", "PROVISIONAL_FULL_QUOTE")))
    integrity &= ~opening | opening_ok
    opening_short = frame.side.eq("SHORT") & frame.signal_end.isin(production.NIFTY_SHORT_SIGNAL_ENDS)
    nifty = _numeric(frame, "nifty_first_bar_return_pct")
    integrity &= ~opening_short | (np.isfinite(nifty) & nifty.le(production.NIFTY_FIRST_BAR_MAX_RETURN_PCT))
    frame["gate_integrity"] = integrity.fillna(False).astype(bool)

    ema9, ema20, ema50 = (_numeric(frame, f"ema{span}") for span in (9,20,50))
    ema = ((ema9-ema20)*sign).gt(0) & ((ema20-ema50)*sign).gt(0)
    ema &= np.isfinite(ema9) & np.isfinite(ema20) & np.isfinite(ema50)
    # Native strict prefilter checks its archived stack booleans as well.
    if "v9_5m_ema_bull" in frame and "v9_5m_ema_bear" in frame:
        ema &= np.where(sign.gt(0), _true(frame.v9_5m_ema_bull), _true(frame.v9_5m_ema_bear))
    frame["gate_ema"] = ema
    frame["gate_oi_upper"] = _numeric(frame, "oi_change_pct").le(production.MAX_OI_CHANGE_PCT)
    frame["minimum_confirmation_volume"] = frame.setup_id.map({s.setup_id: production.confirmation_volume_minimum(
        s.side, setup_id=s.setup_id) for s in donor.ACTIVE_SETUPS})
    frame["gate_vol1"] = _numeric(frame, "v9_1m_volume_ratio").ge(frame.minimum_confirmation_volume)
    frame["minimum_directional_wick_ratio"] = frame.setup_id.map(donor.MIN_DIRECTIONAL_WICK_RATIO_BY_SETUP).fillna(0.)
    frame["gate_min_wick"] = _numeric(frame, "wick_ratio").ge(frame.minimum_directional_wick_ratio)
    extension = sign * (close - _numeric(frame, "v9_1m_ema20")) / _numeric(frame, "v9_1m_atr14")
    frame["v9_1m_extension_atr"] = extension
    extension_limit = frame.setup_id.map(production.MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP)
    frame["gate_extension"] = extension_limit.isna() | (np.isfinite(extension)
        & _numeric(frame, "v9_1m_atr14").gt(0) & extension.le(extension_limit))
    for prefix, spec in (("gate_", "setup_"), ("core_gate_", "core_")):
        frame[prefix+"price"] = (_numeric(frame, "price_change_pct")*sign).ge(
            np.maximum(production.BASE_PRICE_CHANGE_PCT, frame[spec+"price_change_pct"]))
        frame[prefix+"oi_lower"] = (_numeric(frame, "oi").gt(_numeric(frame, "prev_oi"))
            & _numeric(frame, "oi_change_pct").ge(np.maximum(production.BASE_OI_CHANGE_PCT,
                                                            frame[spec+"oi_change_pct"])))
        frame[prefix+"vol5"] = _numeric(frame, "volume_ratio").ge(
            np.maximum(production.BASE_VOLUME_RATIO, frame[spec+"volume_ratio"]))
        frame[prefix+"body"] = _numeric(frame, "body_ratio").ge(frame[spec+"body_ratio"])
        frame[prefix+"max_wick"] = _numeric(frame, "wick_ratio").le(frame[spec+"max_wick_ratio"])
    for gate in GATES:
        if "core_gate_"+gate not in frame:
            frame["core_gate_"+gate] = frame["gate_"+gate]
        frame["gate_"+gate] = frame["gate_"+gate].fillna(False).astype(bool)
        frame["core_gate_"+gate] = frame["core_gate_"+gate].fillna(False).astype(bool)
    frame["native_stop_pct"] = frame["stop_pct"] = frame.setup_stop_pct
    frame["native_target_pct"] = frame["target_pct"] = frame.setup_target_pct
    frame["initial_stop_pct"] = production.INITIAL_STOP_PCT
    frame["tightened_stop_pct"] = production.TIGHTENED_STOP_PCT
    frame["tighten_after_minutes"] = production.TIGHTEN_AFTER_MINUTES
    frame["configured_confirmation_end"] = frame.setup_confirmation_end
    frame["picker"] = frame.setup_picker
    frame["max_entries"] = frame.setup_max_entries
    frame["v10_g_morning_slot"] = False
    frame["v9_configuration"] = production.STRATEGY_PROFILE
    frame["relaxed_0925_added"] = False
    frame["g3_confirmation_offset_minutes"] = 1
    frame["execution_authority"] = False
    frame["scope"] = "BACKTEST_ONLY"
    frame["source_strategy_version"] = control.STRATEGY_VERSION
    frame["picker_value"] = np.select(
        [frame.picker.eq(name) for name in ("max_oi","max_volume","max_move","max_body","max_liquidity")],
        [_numeric(frame,"oi_change_pct"), _numeric(frame,"volume_ratio"), _numeric(frame,"price_change_pct").abs(),
         _numeric(frame,"body_ratio"), _numeric(frame,"traded_value")], default=np.nan)
    return frame


def configuration(variant="control", *, removed_gates=(), ranking=False, use_core=None, all_slots=None):
    removed = set(removed_gates)
    if variant in VARIANTS and variant.startswith("no_") and variant != "no_core":
        removed.add(variant[3:])
    if removed-set(GATES):
        raise ValueError(f"Unknown/removal of immutable gate: {sorted(removed-set(GATES))}")
    return dict(version=VERSION, variant=variant, removed_gates=sorted(removed),
                use_core=(variant != "no_core") if use_core is None else bool(use_core),
                all_slots=(variant == "all51_slots") if all_slots is None else bool(all_slots),
                ranking="SUPPLIED_SCORE_THEN_NATIVE_PICKER" if ranking else "NATIVE_PICKER",
                core_priority="MATCHING_CORE_GATES_AFTER_SAME_REMOVALS", scope="BACKTEST_ONLY",
                execution_authority=False, base_version=control.STRATEGY_VERSION)


def select(prepared: pd.DataFrame, variant="control", *, removed_gates=(), extra_mask=None,
           ranking_score=None, score_column=None, use_core=None, all_slots=None):
    """Select and audit all matched candidates, reranking after every filter.

    ``extra_mask`` may only further restrict eligibility. To build a tested OR
    policy, explicitly remove its economic gate and supply the complete OR
    mask. ``ranking_score`` (or alias ``score_column``) can be a column name,
    aligned Series, or array of equal length; nonfinite scores fail closed.
    Native core preference is retained unless ``use_core=False``. Required
    integrity and portfolio execution controls cannot be overridden here.
    """
    if score_column is not None:
        if ranking_score is not None:
            raise ValueError("Use ranking_score or score_column, not both")
        ranking_score = score_column
    cfg = configuration(variant, removed_gates=removed_gates, ranking=ranking_score is not None,
                        use_core=use_core, all_slots=all_slots)
    frame = prepared.copy(deep=True)
    if frame.sid.isna().any() or frame.sid.duplicated().any():
        raise ValueError("Candidate sid must be present and globally unique")
    gates = [g for g in GATES if g not in cfg["removed_gates"]]
    required = frame.gate_integrity & (True if cfg["all_slots"] else frame.slot_enabled)
    eligible = required & frame[["gate_"+g for g in gates]].all(axis=1)
    core = required & frame[["core_gate_"+g for g in gates]].all(axis=1)
    extra = pd.Series(True, index=frame.index)
    if extra_mask is not None:
        if isinstance(extra_mask, pd.Series) and not extra_mask.index.equals(frame.index):
            raise ValueError("extra_mask index must exactly match prepared index")
        if len(extra_mask) != len(frame):
            raise ValueError("extra_mask length differs from candidate pool")
        extra &= pd.Series(np.asarray(extra_mask), index=frame.index).fillna(False).astype(bool)
    rank = pd.Series(0., index=frame.index)
    if ranking_score is not None:
        values = frame[ranking_score] if isinstance(ranking_score, str) else ranking_score
        if isinstance(values, pd.Series) and not values.index.equals(frame.index):
            raise ValueError("ranking_score index must exactly match prepared index")
        if len(values) != len(frame):
            raise ValueError("ranking_score length differs from candidate pool")
        rank = pd.to_numeric(pd.Series(np.asarray(values), index=frame.index), errors="coerce")
        extra &= np.isfinite(rank)
    eligible &= extra
    core &= extra
    frame["gate_extra"] = extra
    frame["lab_ranking_score"] = rank
    frame["lab_required_pass"] = required
    frame["lab_core_pass"] = core
    frame["v9_filter_pass"] = eligible
    frame["v9_selected"] = False
    frame["v10_g_f_core"] = False
    frame["v9_rank_in_setup_day"] = np.nan
    frame["first_failed_gate"] = ""
    checks = [("integrity", frame.gate_integrity),
              ("slot_allowlist", pd.Series(True, index=frame.index) if cfg["all_slots"] else frame.slot_enabled),
              *((gate, frame["gate_"+gate]) for gate in gates), ("extra", extra)]
    for name, passed in checks:
        frame.loc[frame.first_failed_gate.eq("") & ~passed, "first_failed_gate"] = name
    for _, indices in frame.loc[eligible].groupby(["day", "setup_id"], sort=True).groups.items():
        ranked = frame.loc[indices].sort_values(["lab_ranking_score", "picker_value", "traded_value", "tradingsymbol"],
                                               ascending=[False, False, False, True], kind="stable")
        frame.loc[ranked.index,"v9_rank_in_setup_day"] = np.arange(1,len(ranked)+1)
        count = int(ranked.max_entries.iloc[0])
        if cfg["use_core"]:
            preferred = ranked.loc[ranked.lab_core_pass].head(int(ranked.core_max_entries.iloc[0]))
            chosen = list(preferred.index) + list(ranked.loc[~ranked.index.isin(preferred.index)].head(count-len(preferred)).index)
            frame.loc[preferred.index,"v10_g_f_core"] = True
        else:
            chosen = list(ranked.head(count).index)
        frame.loc[chosen,"v9_selected"] = True
    frame["v9_decision"] = np.select([frame.v9_selected, frame.v9_filter_pass,
        frame.first_failed_gate.eq("integrity"), frame.first_failed_gate.eq("slot_allowlist")],
        ["SELECTED","RANKED_OUT","INTEGRITY_REJECTED","SLOT_DISABLED"], default="FILTER_REJECTED")
    identity = VERSION+":"+str(variant)
    frame["strategy_version"] = identity
    frame["strategy_fingerprint"] = hashlib.sha256(json.dumps(cfg,sort_keys=True).encode()).hexdigest()
    frame["research_variant"] = variant
    frame["signal_id"] = [production.canonical_signal_id(identity, row.day, row.signal_end,
        row.configured_confirmation_end, row.side, row.tradingsymbol) for row in frame.itertuples()]
    return frame.loc[frame.v9_selected].reset_index(drop=True), frame


def native_control(raw):
    """Independent original path used for parity, never a relaxed-rule source."""
    from g3_v18_20261102.selection import strict_signals
    audit = control.selection_audit(strict_signals(raw))
    return audit.loc[audit.v9_selected.eq(True)].reset_index(drop=True)
