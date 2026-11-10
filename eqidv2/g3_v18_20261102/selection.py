"""Fixed G3 V18 research selections; no writes or production mutation.

The control calls the exact V17 selector on its native strict candidate pool.
Advanced filters run BEFORE ranking, so rejected leaders can be replaced.
The two simplified variants remove only EMA stacking and body/wick thresholds;
data integrity, directional confirmation, OI/volume/price, ranking and quotas
remain. These seven variants are specified before observing their outcomes.
"""
from __future__ import annotations

from datetime import timedelta
import hashlib
import json
import math

import numpy as np
import pandas as pd

import fno_v13_replay_source as source
import fno_v13_v10_g_3_contract as production
from research import g3_slot_win70_contract as control

VARIANTS = ("control", "market_only", "strength_only", "vwap_pressure",
            "advanced", "simplified", "simplified_advanced")
VERSION = "G3-V18-20261102-INDICATOR-RESEARCH"
MIN_BREADTH_COVERAGE_PCT = 90.0


def configuration(variant: str) -> dict:
    if variant not in VARIANTS:
        raise ValueError(f"Unknown variant {variant!r}; choose {VARIANTS}")
    return dict(version=VERSION, variant=variant, scope="BACKTEST_ONLY",
        execution_authority=False, base_strategy_version=control.STRATEGY_VERSION,
        base_strategy_fingerprint=control.strategy_fingerprint(),
        market_gate="directional breadth >=50%; coverage >=90%; exact market and stock data ready",
        strength_gate="directional stock return since open minus Nifty spot return >=0",
        vwap_pressure_gate="directional VWAP distance >=0, VWAP slope >=0, candle-pressure proxy >=0",
        missing_feature_policy="FAIL_CLOSED_FOR_REQUIRED_GATE; no opening VWAP-slope fallback",
        feature_clock="completed signal bar only; no later confirmation-bar advanced features",
        simplified_removed=["EMA9/20/50 stacking", "minimum confirmation body ratio",
            "maximum directional wick ratio", "0926_SHORT minimum lower-wick ratio"]
            if variant.startswith("simplified") else [],
        unchanged="OI/price/volume and opening Nifty gate, integrity, directional confirmation, core-first ranking, quotas, stops and targets",
        variants=list(VARIANTS), thresholds_selected_using_outcomes=False)


def fingerprint(variant: str) -> str:
    return hashlib.sha256(json.dumps(configuration(variant), sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _finite(row, name):
    value = production._number(row.get(name))
    return value if math.isfinite(value) else math.nan


def advanced_pass(row: dict, variant: str) -> bool:
    """All thresholds are fixed; missing, stale or future features fail closed."""
    market = variant in {"market_only", "advanced", "simplified_advanced"}
    strength = variant in {"strength_only", "advanced", "simplified_advanced"}
    vwap = variant in {"vwap_pressure", "advanced", "simplified_advanced"}
    if not (market or strength or vwap):
        return True
    if not production._true(row.get("v18_stock_ready", False)):
        return False
    if market or strength:
        if not production._true(row.get("v18_market_ready", False)):
            return False
    # Enrichment may provide this audit stamp; reject a later bar explicitly.
    if "v18_feature_ts" in row:
        try:
            if production._stamp(row["v18_feature_ts"]) != production._stamp(row["signal_ts"]):
                return False
        except (TypeError, ValueError):
            return False
    sign = 1.0 if row.get("side") == "LONG" else -1.0
    if market:
        field = "v18_breadth_above_open_pct" if sign > 0 else "v18_breadth_below_open_pct"
        breadth, coverage = _finite(row, field), _finite(row, "v18_breadth_coverage_pct")
        if not (50 <= breadth <= 100 and MIN_BREADTH_COVERAGE_PCT <= coverage <= 100):
            return False
    if strength and not sign * _finite(row, "v18_relative_strength_pct") >= 0:
        return False
    if vwap:
        for name in ("v18_vwap_distance_pct", "v18_vwap_slope_pct", "v18_pressure_6"):
            if not sign * _finite(row, name) >= 0:
                return False
    return True


def strict_signals(raw: pd.DataFrame) -> pd.DataFrame:
    """Native prefilter per date, preserving caller-assigned globally unique sid."""
    if raw.empty:
        empty = raw.copy()
        if "wick_ratio" not in empty:
            empty["wick_ratio"] = pd.Series(dtype=float)
        return empty
    out = raw.copy()
    if "sid" not in out:
        out["sid"] = np.arange(len(out))
    if out.sid.isna().any() or out.sid.duplicated().any():
        raise ValueError("Candidate sid must be present and globally unique")
    days = pd.to_datetime(out.signal_ts, utc=True).dt.tz_convert(production.IST).dt.date
    parts = []
    for _, rows in out.groupby(days, sort=True):
        context = pd.to_numeric(rows.get("nifty_first_bar_return_pct", pd.Series(np.nan, index=rows.index)),
                                errors="coerce").dropna().unique()
        if len(context) > 1:
            raise ValueError("Ambiguous dated Nifty opening context")
        parts.append(source._strict_signals(rows, float(context[0]) if len(context) else math.nan))
    return pd.concat(parts, ignore_index=True, sort=False)


def _simple_side(candidate: dict, signal_end: str) -> str | None:
    row = production._candidate(candidate)
    if signal_end == "09:20" and (
            row.get("opening_0920_status") != "VALID"
            or row.get("price_change_basis") != "09:15_CASH_OPEN_TO_09:20_CASH_CLOSE"
            or row.get("oi_change_source") not in {
                "FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M", "PROVISIONAL_FULL_QUOTE"}):
        return None
    vals = {name: _finite(row, name) for name in (
        "price_change_pct", "oi_change_pct", "volume_ratio", "oi", "prev_oi")}
    if not all(math.isfinite(value) for value in vals.values()):
        return None
    if (vals["prev_oi"] <= 0 or vals["oi"] <= vals["prev_oi"]
            or not production.BASE_OI_CHANGE_PCT <= vals["oi_change_pct"] <= production.MAX_OI_CHANGE_PCT
            or vals["volume_ratio"] < production.BASE_VOLUME_RATIO):
        return None
    move = vals["price_change_pct"]
    if move >= production.BASE_PRICE_CHANGE_PCT:
        return "LONG"
    if move <= -production.BASE_PRICE_CHANGE_PCT:
        if signal_end in production.NIFTY_SHORT_SIGNAL_ENDS:
            nifty = _finite(row, "nifty_first_bar_return_pct")
            if not math.isfinite(nifty) or nifty > production.NIFTY_FIRST_BAR_MAX_RETURN_PCT:
                return None
        return "SHORT"
    return None


def _simple_pass(row: dict, setup, day) -> bool:
    """Locally reduced predicate: never patch imported production functions."""
    row = production._candidate(row)
    if row.get("side") != setup.side:
        return False
    try:
        signal = production._stamp(row.get("signal_timestamp"))
        confirm = production._stamp(row.get("confirmation_timestamp"))
        feature = production._stamp(row.get("v9_1m_feature_ts"))
        if (signal.date() != day or signal.strftime("%H:%M") != setup.signal_end
                or signal != signal.floor("min") or confirm != signal + timedelta(minutes=1)
                or feature != confirm):
            return False
        if "v9_5m_feature_ts" in row and production._stamp(row["v9_5m_feature_ts"]) > signal:
            return False
        if "v9_feature_available_ts" in row and production._stamp(row["v9_feature_available_ts"]) > confirm:
            return False
    except (TypeError, ValueError):
        return False
    if row.get("signal_end", setup.signal_end) != setup.signal_end:
        return False
    if not production._confirmation_valid(row, setup.side) or _simple_side(row, setup.signal_end) != setup.side:
        return False
    names = ("price_change_pct", "oi_change_pct", "volume_ratio", "traded_value", "v9_1m_volume_ratio")
    vals = {name: _finite(row, name) for name in names}
    if not all(math.isfinite(value) for value in vals.values()):
        return False
    if setup.setup_id in production.MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP:
        extension = production.extension_atr(row.get("confirm_close"), row.get("v9_1m_ema20"),
            row.get("v9_1m_atr14"), setup.side)
        if not math.isfinite(extension) or extension > production.MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP[setup.setup_id]:
            return False
    # max_body pickers still rank the observed body metric; only its gates go.
    if setup.picker == "max_body" and not math.isfinite(_finite(row, "body_ratio")):
        return False
    return (vals["price_change_pct"] * (1 if setup.side == "LONG" else -1) >= setup.price_change_pct
        and vals["oi_change_pct"] >= setup.oi_change_pct and vals["volume_ratio"] >= setup.volume_ratio
        and vals["traded_value"] >= setup.min_traded_value
        and vals["v9_1m_volume_ratio"] >= production.confirmation_volume_minimum(setup.side, setup_id=setup.setup_id))


def _simplified_audit(raw: pd.DataFrame, variant: str) -> pd.DataFrame:
    pools = {}
    for index, record in enumerate(raw.to_dict("records")):
        row = production._candidate(record)
        try:
            stamp = production._stamp(row.get("signal_timestamp"))
        except (TypeError, ValueError):
            continue
        clock = stamp.strftime("%H:%M")
        side = _simple_side(row, clock)
        setup = control.setup_for(clock, side or "")
        if setup is None:
            continue
        if "day" in row and production._stamp(row["day"]).date() != stamp.date():
            raise ValueError("Candidate day conflicts with signal timestamp")
        try:
            confirmation = production._stamp(row.get("confirmation_timestamp"))
        except (TypeError, ValueError):
            confirmation = pd.NaT
        row.update(day=stamp.date(), side=side, signal_ts=stamp, confirmation_ts=confirmation,
            hhmm=stamp.strftime("%H%M"), hhmm_int=int(stamp.strftime("%H%M")), sid=row.get("sid", index))
        row["wick_ratio"] = row.get("v9_1m_upper_wick_ratio" if side == "LONG" else "v9_1m_lower_wick_ratio")
        row["trigger"] = row.get("confirm_high" if side == "LONG" else "confirm_low")
        row["v18_advanced_pass"] = advanced_pass(row, variant)
        if setup.setup_id in production.MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP:
            row["v9_1m_extension_atr"] = production.extension_atr(row.get("confirm_close"),
                row.get("v9_1m_ema20"), row.get("v9_1m_atr14"), side)
        pools.setdefault((stamp.date(), setup.setup_id), []).append(row)
    parts = []
    for (day, setup_id), rows in sorted(pools.items()):
        setup, core = control._ACTIVE_BY_ID[setup_id], control._CORE_BY_ID[setup_id]
        if len({str(row["tradingsymbol"]) for row in rows}) != len(rows):
            raise ValueError("Duplicate candidate for one setup/session/symbol")
        eligible = sorted([r for r in rows if r["v18_advanced_pass"] and _simple_pass(r, setup, day)],
                          key=lambda r: production._rank_key(r, setup))
        core_selected = [r for r in eligible if _simple_pass(r, core, day)][:core.max_entries]
        core_symbols = {str(r["tradingsymbol"]) for r in core_selected}
        extra = [r for r in eligible if str(r["tradingsymbol"]) not in core_symbols][:setup.max_entries-len(core_selected)]
        selected = core_symbols | {str(r["tradingsymbol"]) for r in extra}
        ranks = {str(r["tradingsymbol"]): n for n, r in enumerate(eligible, 1)}
        for row in rows:
            symbol = str(row["tradingsymbol"])
            chosen, passed = symbol in selected, symbol in ranks
            row.update(setup_id=setup_id, native_stop_pct=setup.stop_pct, native_target_pct=setup.target_pct,
                stop_pct=setup.stop_pct, target_pct=setup.target_pct, initial_stop_pct=production.INITIAL_STOP_PCT,
                tightened_stop_pct=production.TIGHTENED_STOP_PCT, tighten_after_minutes=production.TIGHTEN_AFTER_MINUTES,
                configured_confirmation_end=setup.confirmation_end, picker=setup.picker, max_entries=setup.max_entries,
                v10_g_f_core=chosen and symbol in core_symbols, v10_g_morning_slot=False,
                v9_selected=chosen, v9_filter_pass=passed, v9_rank_in_setup_day=ranks.get(symbol),
                v9_decision="SELECTED" if chosen else "RANKED_OUT" if passed else
                    "ADVANCED_FILTER_REJECTED" if not row["v18_advanced_pass"] else "FILTER_REJECTED",
                v9_configuration=production.STRATEGY_PROFILE, relaxed_0925_added=False,
                g3_confirmation_offset_minutes=1)
            parts.append(row)
    if not parts:
        return control.selection_audit(strict_signals(raw.iloc[:0])).assign(v18_advanced_pass=pd.Series(dtype=bool))
    return pd.DataFrame(parts).sort_values(["day", "hhmm_int", "side", "setup_id", "tradingsymbol"],
        kind="stable").reset_index(drop=True)


def select(raw_with_features: pd.DataFrame, variant: str = "control") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return selected orders and all matched candidate decisions, without I/O."""
    configuration(variant)
    raw = raw_with_features.copy()
    if "sid" not in raw:
        raw["sid"] = np.arange(len(raw))
    if raw.sid.isna().any() or raw.sid.duplicated().any():
        raise ValueError("Candidate sid must be present and globally unique")
    if variant.startswith("simplified"):
        audit = _simplified_audit(raw, variant)
    else:
        strict = strict_signals(raw)
        baseline = control.selection_audit(strict)
        if variant == "control":
            return baseline.loc[baseline.v9_selected.eq(True)].reset_index(drop=True), baseline
        passed = pd.Series([advanced_pass(row, variant) for row in strict.to_dict("records")],
                           index=strict.index, dtype=bool)
        audit = control.selection_audit(strict.loc[passed].copy())
        audit["v18_advanced_pass"] = True
        excluded = baseline.loc[~baseline.sid.isin(strict.loc[passed, "sid"])].copy()
        excluded["v18_advanced_pass"] = False
        excluded["v9_selected"] = False
        excluded["v9_filter_pass"] = False
        excluded["v9_rank_in_setup_day"] = np.nan
        excluded["v9_decision"] = "ADVANCED_FILTER_REJECTED"
        combined = (pd.concat([audit, excluded], ignore_index=True, sort=False)
                    if len(audit) and len(excluded) else audit if len(audit) else excluded)
        audit = combined.sort_values(
            ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"], kind="stable").reset_index(drop=True)
    audit["source_strategy_version"] = control.STRATEGY_VERSION
    if "signal_id" in audit:
        audit["source_signal_id"] = audit.signal_id
    version = VERSION + "-" + variant.upper()
    audit["strategy_version"] = version
    audit["strategy_fingerprint"] = fingerprint(variant)
    audit["research_variant"] = variant
    audit["execution_authority"] = False
    audit["scope"] = "BACKTEST_ONLY"
    for index, row in audit.iterrows():
        setup = control._ACTIVE_BY_ID[row.setup_id]
        audit.loc[index, "signal_id"] = production.canonical_signal_id(version, row.day,
            setup.signal_end, setup.confirmation_end, setup.side, row.tradingsymbol)
    return audit.loc[audit.v9_selected.eq(True)].reset_index(drop=True), audit
