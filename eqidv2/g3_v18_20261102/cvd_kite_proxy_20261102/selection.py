"""Fixed candle-volume proxy gates before unchanged G3 ranking and quotas."""
from __future__ import annotations

import hashlib
import json

import numpy as np
import pandas as pd

from g3_v18_20261102 import selection as base

VERSION = "G3-V18-5M-CVD-PROXY-RESEARCH-20261102"
VARIANTS = ("base_control", "covered_control", "cvd_session", "cvd_recent15", "cvd_both", "body_both")


def configuration():
    return dict(version=VERSION, variants=list(VARIANTS), execution_authority=False,
        source_type="FIVE_MINUTE_CANDLE_VOLUME_ESTIMATE_NOT_AGGRESSOR_DELTA",
        gates={"base_control": "Unmodified native control on the replay dates",
               "covered_control": "Native control restricted to the common proxy-ready pool",
               "cvd_session": "Covered pool; directional session close-location-weighted volume proxy > 0",
               "cvd_recent15": "Covered pool; directional three-bar close-location-weighted volume proxy > 0",
               "cvd_both": "Covered pool; both directional close-location-weighted volume proxies > 0",
               "body_both": "Covered pool; directional session AND three-bar body/range-weighted proxies > 0"},
        coverage="Continuous valid 5m prefix from 09:20 IST; three-bar warmup; same availability for all covered arms",
        primary_delta_formula="volume * (2*close-high-low)/(high-low); zero when high=low",
        sensitivity_delta_formula="volume * (close-open)/(high-low); zero when high=low",
        threshold_search=False, feature_clock="Completed signal bar; entry after next-minute confirmation",
        reference_strategy_fingerprint=base.control.strategy_fingerprint(),
        unchanged="Native prefilter, EMA/body/wick gates, ranking, quotas, confirmation, exits and capital")


def gate(raw: pd.DataFrame, variant: str) -> pd.Series:
    if variant not in VARIANTS:
        raise ValueError(f"Unknown proxy variant: {variant}")
    if variant == "base_control":
        return pd.Series(True, index=raw.index, dtype=bool)
    required = {"cvd_proxy_ready", "cvd_proxy_feature_ts", "signal_ts", "cvd_proxy_session",
                "cvd_proxy_recent15", "cvd_proxy_body_session", "cvd_proxy_body_recent15"}
    if required - set(raw):
        return pd.Series(False, index=raw.index, dtype=bool)
    ready = raw.cvd_proxy_ready.astype(str).str.lower().isin(("true", "1", "1.0"))
    signal = pd.to_datetime(raw.signal_ts, utc=True, errors="coerce")
    feature = pd.to_datetime(raw.cvd_proxy_feature_ts, utc=True, errors="coerce")
    values = {name: pd.to_numeric(raw[name], errors="coerce") for name in (
        "cvd_proxy_session", "cvd_proxy_recent15", "cvd_proxy_body_session", "cvd_proxy_body_recent15")}
    ready &= signal.notna() & feature.eq(signal)
    for value in values.values():
        ready &= np.isfinite(value)
    if variant == "covered_control":
        return ready
    sign = raw.get("side", pd.Series("", index=raw.index)).map({"LONG": 1., "SHORT": -1.})
    ready &= sign.notna()
    fields = {
        "cvd_session": ("cvd_proxy_session",),
        "cvd_recent15": ("cvd_proxy_recent15",),
        "cvd_both": ("cvd_proxy_session", "cvd_proxy_recent15"),
        "body_both": ("cvd_proxy_body_session", "cvd_proxy_body_recent15"),
    }[variant]
    for name in fields:
        ready &= sign * values[name] > 0
    return ready


def _select_strict(strict: pd.DataFrame, original: pd.DataFrame, variant: str):
    if variant not in VARIANTS:
        raise ValueError(f"Unknown proxy variant: {variant}")
    if variant == "base_control":
        return original.loc[original.v9_selected.eq(True)].reset_index(drop=True), original.copy()
    passed = gate(strict, variant)
    accepted = base.control.selection_audit(strict.loc[passed].copy())
    accepted["cvd_proxy_gate_pass"] = True
    rejected = original.loc[~original.sid.isin(strict.loc[passed, "sid"])].copy()
    rejected["source_v9_decision"] = rejected.v9_decision
    rejected["cvd_proxy_gate_pass"] = False
    rejected["v9_selected"] = False
    rejected["v9_filter_pass"] = False
    rejected["v9_rank_in_setup_day"] = np.nan
    covered_ids = set(strict.loc[gate(strict, "covered_control"), "sid"])
    rejected["v9_decision"] = np.where(rejected.sid.isin(covered_ids),
                                          "CVD_PROXY_DIRECTION_REJECTED", "CVD_PROXY_DATA_UNAVAILABLE")
    parts = [part for part in (accepted, rejected) if len(part)]
    audit = pd.concat(parts, ignore_index=True, sort=False) if parts else accepted
    audit["research_variant"] = variant
    audit["execution_authority"] = False
    audit["scope"] = "BACKTEST_ONLY"
    audit["source_strategy_version"] = base.control.STRATEGY_VERSION
    if "signal_id" in audit:
        audit["source_signal_id"] = audit.signal_id
    version = VERSION + "-" + variant.upper()
    audit["strategy_version"] = version
    audit["strategy_fingerprint"] = hashlib.sha256(json.dumps(
        {**configuration(), "variant": variant}, sort_keys=True).encode()).hexdigest()
    for index, row in audit.iterrows():
        setup = base.control._ACTIVE_BY_ID[row.setup_id]
        audit.loc[index, "signal_id"] = base.production.canonical_signal_id(
            version, row.day, setup.signal_end, setup.confirmation_end, setup.side, row.tradingsymbol)
    if len(audit):
        audit = audit.sort_values(["day", "hhmm_int", "side", "setup_id", "tradingsymbol"],
                                 kind="stable").reset_index(drop=True)
    return audit.loc[audit.v9_selected.eq(True)].reset_index(drop=True), audit


def select(raw: pd.DataFrame, variant: str):
    strict = base.strict_signals(raw)
    return _select_strict(strict, base.control.selection_audit(strict), variant)


def select_all(raw: pd.DataFrame):
    """Compute the native full strict pool once, then rerank each variant."""
    strict = base.strict_signals(raw)
    original = base.control.selection_audit(strict)
    return {variant: _select_strict(strict, original, variant) for variant in VARIANTS}
