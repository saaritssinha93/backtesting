"""Frozen strict CVD eligibility gates before native G3 ranking and quotas."""
from __future__ import annotations

import hashlib
import json

import numpy as np
import pandas as pd

from g3_v18_20261102 import selection as native

VERSION = "G3-V18-STRICT-CVD-GATES-RESEARCH-20261102"
VARIANTS = ("base_control", "cvd_ready_control", "rvol_ready_control", "aligned_gate", "strict_gate", "strict_rvol_gate")
MATCHED_CONTROLS = {"aligned_gate": "cvd_ready_control", "strict_gate": "cvd_ready_control", "strict_rvol_gate": "rvol_ready_control"}


def configuration():
    return dict(version=VERSION, variants=list(VARIANTS), execution_authority=False,
        source_type="VERIFIED_REUSED_ONE_MINUTE_CLOSE_LOCATION_VOLUME_ESTIMATE_NOT_AGGRESSOR_DELTA",
        matched_controls=MATCHED_CONTROLS, first_eligible_signal="09:45 Asia/Kolkata",
        gates=dict(base_control="Unchanged native G3", cvd_ready_control="Exact adv_ready and finite pressure/acceleration",
                   rvol_ready_control="CVD readiness plus exact adv_rvol_ready and valid RVOL20",
                   aligned_gate="CVD ready; signed recent15 pressure>0 AND signed raw acceleration>0",
                   strict_gate="CVD ready; signed recent15 pressure>=0.10 AND signed raw acceleration>=0.05",
                   strict_rvol_gate="Strict gate plus RVOL readiness and RVOL20>=1.20"),
        acceleration_definition="Raw current15m pressure minus prior15m pressure in[-2,2]; not divided by2",
        thresholds=dict(pressure=.10, raw_acceleration=.05, rvol20=1.20),
        eligibility="Hard individual-candidate gate on the full native strict pool before native reranking, including singleton pools",
        unavailable_policy="FAIL_CLOSED for all five gated arms; base_control retains original availability",
        unchanged="Native original filters, picker inputs, core-first/extras quotas, confirmation, execution paths, exits, capital",
        threshold_search=False, independent_holdout=False, all_sessions_previously_viewed=True,
        reference_strategy_fingerprint=native.control.strategy_fingerprint())


def flags(series):
    return series.astype(str).str.lower().isin(("true", "1", "1.0"))


def gate(raw, variant):
    """Return per-candidate eligibility and first failure reason."""
    if variant not in VARIANTS:
        raise ValueError(f"Unknown strict CVD variant: {variant}")
    passed = pd.Series(True, index=raw.index, dtype=bool)
    reason = pd.Series("PASS", index=raw.index, dtype=object)
    if variant == "base_control":
        return passed, reason.mask(passed, "BASELINE_UNFILTERED")
    def require(condition, message):
        nonlocal passed
        condition = condition.fillna(False).astype(bool)
        reason.loc[passed & ~condition] = message
        passed &= condition
    signal = pd.to_datetime(raw.get("signal_ts", pd.Series(pd.NaT, index=raw.index)), utc=True, errors="coerce")
    feature = pd.to_datetime(raw.get("adv_feature_ts", pd.Series(pd.NaT, index=raw.index)), utc=True, errors="coerce")
    require(signal.notna() & signal.dt.tz_convert("Asia/Kolkata").dt.strftime("%H:%M").ge("09:45"), "BEFORE_0945_OR_INVALID_SIGNAL")
    require(feature.eq(signal) & feature.notna(), "MISSING_EXACT_FEATURE")
    require(flags(raw.get("adv_ready", pd.Series(False, index=raw.index))), "CVD_FEATURE_UNAVAILABLE")
    pressure = pd.to_numeric(raw.get("adv_recent15_pressure", pd.Series(np.nan, index=raw.index)), errors="coerce")
    acceleration = pd.to_numeric(raw.get("adv_acceleration", pd.Series(np.nan, index=raw.index)), errors="coerce")
    require(np.isfinite(pressure) & pressure.between(-1, 1) & np.isfinite(acceleration) & acceleration.between(-2, 2), "INVALID_PRESSURE_OR_ACCELERATION")
    if variant in ("rvol_ready_control", "strict_rvol_gate"):
        rvol = pd.to_numeric(raw.get("adv_rvol20", pd.Series(np.nan, index=raw.index)), errors="coerce")
        require(flags(raw.get("adv_rvol_ready", pd.Series(False, index=raw.index))) & np.isfinite(rvol) & rvol.ge(0), "RVOL_FEATURE_UNAVAILABLE")
    if variant in ("aligned_gate", "strict_gate", "strict_rvol_gate"):
        sign = raw.get("side", pd.Series("", index=raw.index)).map({"LONG": 1., "SHORT": -1.})
        require(sign.notna(), "INVALID_DIRECTION")
        if variant == "aligned_gate":
            require((sign*pressure).gt(0), "PRESSURE_NOT_ALIGNED")
            require((sign*acceleration).gt(0), "ACCELERATION_NOT_ALIGNED")
        else:
            require((sign*pressure).ge(.10), "PRESSURE_BELOW_010")
            require((sign*acceleration).ge(.05), "RAW_ACCELERATION_BELOW_005")
        if variant == "strict_rvol_gate":
            require(rvol.ge(1.20), "RVOL_BELOW_120")
    return passed, reason


def _audit(strict, original, variant):
    if variant == "base_control":
        return original.copy()
    allowed, _ = gate(strict, variant)
    accepted = native.control.selection_audit(strict.loc[allowed].copy())
    rejected = original.loc[~original.sid.isin(strict.loc[allowed, "sid"])].copy()
    rejected["v9_selected"] = False
    rejected["v9_filter_pass"] = False
    rejected["v10_g_f_core"] = False
    rejected["v9_rank_in_setup_day"] = np.nan
    rejected["v9_decision"] = "STRICT_CVD_GATE_REJECTED"
    parts = [part for part in (accepted, rejected) if len(part)]
    audit = pd.concat(parts, ignore_index=True, sort=False) if parts else accepted
    audit = audit.sort_values(["day", "hhmm_int", "side", "setup_id", "tradingsymbol"], kind="stable").reset_index(drop=True)
    prior = original.set_index("sid")
    for source, target in (("v9_selected", "strict_original_selected"), ("v9_filter_pass", "strict_native_filter_pass"),
                           ("v9_rank_in_setup_day", "strict_original_rank"), ("v9_decision", "strict_original_decision")):
        audit[target] = audit.sid.map(prior[source])
    audit["strict_gate_pass"], audit["strict_gate_reason"] = gate(audit, variant)
    audit["strict_selected_changed"] = flags(audit.v9_selected).ne(flags(audit.strict_original_selected))
    audit["research_variant"] = variant
    audit["execution_authority"] = False
    audit["scope"] = "BACKTEST_ONLY"
    audit["source_strategy_version"] = native.control.STRATEGY_VERSION
    audit["source_signal_id"] = audit.sid.map(prior.signal_id)
    version = VERSION + "-" + variant.upper()
    audit["strategy_version"] = version
    audit["strategy_fingerprint"] = hashlib.sha256(json.dumps({**configuration(), "variant": variant}, sort_keys=True).encode()).hexdigest()
    for i, row in audit.iterrows():
        setup = native.control._ACTIVE_BY_ID[row.setup_id]
        audit.loc[i, "signal_id"] = native.production.canonical_signal_id(version, row.day,
            setup.signal_end, setup.confirmation_end, setup.side, row.tradingsymbol)
    return audit


def select_all(raw):
    strict = native.strict_signals(raw)
    original = native.control.selection_audit(strict)
    result = {}
    for variant in VARIANTS:
        audit = _audit(strict, original, variant)
        result[variant] = (audit.loc[audit.v9_selected.eq(True)].reset_index(drop=True), audit)
    return result


def select(raw, variant="base_control"):
    if variant not in VARIANTS:
        raise ValueError(f"Unknown strict CVD variant: {variant}")
    return select_all(raw)[variant]
