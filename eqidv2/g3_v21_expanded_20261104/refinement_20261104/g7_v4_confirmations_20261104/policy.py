"""Causal G7 V4 entry vetoes on V21's already ranked selected opportunities.

Universal Nifty and low-efficiency checks apply to all 73 V21 setup pairs.
The G7 OI, traded-value and ATR guards retain their exact named setup IDs.
No rejected leader is replaced: V21's cap-one/volume policy has already run.
Exit, portfolio-loss-lock and historical slot-allowlist rules are out of scope.
"""
from __future__ import annotations

import hashlib
import json

import numpy as np
import pandas as pd

from g3_v21_expanded_20261104.refinement_20261104.strict_new_pairs_20261104 import policy as v21
from research.g7_g6_replica_backtest import configuration as g7_configuration


VERSION = "G3-V21-G7-V4-ENTRY-CONFIRMATIONS-20261104"
G7 = g7_configuration()
ATR_LIMITS = {str(k): float(v) for k, v in G7["maximum_confirmation_extension_atr_by_setup"].items()}
POST_RANK = G7["slot_entry_filters"]
if len(ATR_LIMITS) != 12 or set(POST_RANK) != {"1011_LONG", "1206_LONG", "1226_SHORT"}:
    raise ValueError("Pinned G7 confirmation inventory changed")

ARMS = (
    "v21_new39_cap1_volume_gate",
    "plus_g7_nifty",
    "plus_g7_low_efficiency",
    "plus_g7_atr",
    "plus_g7_slot_postrank",
    "all_g7_entry_confirmations",
)
ARM_GATES = {
    ARMS[0]: (),
    ARMS[1]: ("g7_nifty_allowed",),
    ARMS[2]: ("g7_low_efficiency_allowed",),
    ARMS[3]: ("g7_atr_allowed",),
    ARMS[4]: ("g7_slot_postrank_allowed",),
    ARMS[5]: ("g7_nifty_allowed", "g7_low_efficiency_allowed",
              "g7_atr_allowed", "g7_slot_postrank_allowed"),
}


def configuration() -> dict:
    return dict(version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
                source_v21_policy=v21.VERSION, source_v21_fingerprint=v21.fingerprint(),
                admission_order="V21_CORE_FIRST_CAP_AND_VOLUME_THEN_G7_VETOES_NO_REPLACEMENT",
                universal_gates=["G7_DATED_NIFTY_FUTURES_BOTH_COUNTER_SKIP",
                                 "G7_V4_EXTREME_AND_EFFICIENCY_AND_WEAK_RSI_VETO"],
                g7_atr_limits=ATR_LIMITS, g7_post_rank_slot_filters=POST_RANK,
                g7_v4_extreme="VWAP_Z_GT_2P5_OR_PRIOR_SAME_CLOCK_HISTORICAL_95",
                g7_v4_efficiency_below=0.30, g7_v4_rsi_weakening_minutes=3,
                g7_v4_minimum_session_bars=30,
                v21_original_pair_cap=3, v21_new_pair_cap=1,
                v21_new_pair_5m_volume_increment=0.10,
                preserve_v21_native_fills_targets_stops_costs=True,
                omit_g7_slot_allowlist_daily_loss_lock_profit_protection=True,
                arms=list(ARMS))


def fingerprint() -> str:
    return hashlib.sha256(json.dumps(configuration(), sort_keys=True,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _audit_input(frame: pd.DataFrame, name: str, expected: set[int]) -> pd.DataFrame:
    if "sid" not in frame or frame.sid.isna().any() or frame.sid.duplicated().any():
        raise ValueError(f"{name} has missing or duplicate SIDs")
    if set(frame.sid.astype(int)) != expected:
        raise ValueError(f"{name} does not cover all V21 selected orders")
    return frame.copy()


def decisions(orders: pd.DataFrame, stock: pd.DataFrame, nifty: pd.DataFrame) -> pd.DataFrame:
    """Return one pre-entry gate decision per SID; no outcome column is read."""
    ids = set(orders.sid.astype(int))
    if len(ids) != len(orders) or len(orders) == 0:
        raise ValueError("Selected V21 identities are invalid")
    stock = _audit_input(stock, "stock feature audit", ids)
    nifty = _audit_input(nifty, "Nifty feature audit", ids)
    fields = ["sid", "day", "setup_id", "tradingsymbol", "side", "signal_ts",
              "confirmation_ts", "oi_change_pct", "traded_value", "wick_ratio"]
    for name, feature in (("stock", stock), ("Nifty", nifty)):
        for identity in ("day", "setup_id", "tradingsymbol", "side"):
            if identity in feature:
                expected_identity = orders.set_index("sid")[identity].astype(str)
                observed_identity = feature.set_index("sid")[identity].astype(str).loc[expected_identity.index]
                if not observed_identity.equals(expected_identity):
                    raise ValueError(f"{name} audit {identity} differs from selected V21 identity")
    stock = stock.drop(columns=[key for key in fields if key != "sid" and key in stock])
    nifty = nifty.drop(columns=[key for key in fields if key != "sid" and key in nifty])
    overlap = (set(stock) & set(nifty)) - {"sid"}
    if overlap:
        raise ValueError(f"Stock/Nifty audit feature overlap: {sorted(overlap)}")
    audit = orders[fields].copy().merge(stock, on="sid", how="left", validate="one_to_one")
    audit = audit.merge(nifty, on="sid", how="left", validate="one_to_one")
    if len(audit) != len(orders) or audit.sid.isna().any():
        raise ValueError("G7 entry audit merge changed V21 order inventory")
    signal = pd.to_datetime(audit.signal_ts, utc=True)
    confirmation = pd.to_datetime(audit.confirmation_ts, utc=True)
    if not confirmation.sub(signal).eq(pd.Timedelta(minutes=1)).all():
        raise ValueError("V21 next-minute confirmation drift")
    for key in ("g7_nifty_allowed", "g7_low_efficiency_veto"):
        if audit[key].isna().any() or not audit[key].isin((True, False)).all():
            raise ValueError(f"Incomplete G7 decision: {key}")
    if not audit.g7_nifty_status.isin(("OK", "MISSING_PREVIOUS_NIFTY_BAR")).all():
        raise ValueError("G7 Nifty exact-clock evidence is incomplete")
    bar = pd.to_datetime(audit.g7_nifty_bar_ts, utc=True)
    if bar.isna().any() or not bar.le(confirmation).all():
        raise ValueError("G7 Nifty bar is unavailable at V21 confirmation")
    short0926 = audit.setup_id.eq("0926_SHORT")
    if not pd.to_numeric(audit.loc[short0926, "wick_ratio"]).ge(.20 - 1e-12).all():
        raise ValueError("Inherited V21 09:26 SHORT wick minimum drift")
    audit["g7_low_efficiency_allowed"] = ~audit.g7_low_efficiency_veto.astype(bool)
    audit["g7_atr_max"] = audit.setup_id.map(ATR_LIMITS)
    atr_applicable = audit.g7_atr_max.notna()
    observed_atr = pd.to_numeric(audit.g7_extension_atr, errors="coerce")
    if not np.isfinite(observed_atr.loc[atr_applicable]).all():
        raise ValueError("G7 ATR guard lacks a completed confirmation feature")
    audit["g7_atr_allowed"] = ~atr_applicable | observed_atr.le(audit.g7_atr_max + 1e-12)
    audit["g7_slot_postrank_allowed"] = True
    audit["g7_slot_postrank_minimum"] = np.nan
    audit["g7_slot_postrank_observed"] = np.nan
    audit["g7_slot_postrank_field"] = ""
    for setup_id, rule in POST_RANK.items():
        mask = audit.setup_id.eq(setup_id)
        if not mask.any():
            continue
        observed = pd.to_numeric(audit.loc[mask, rule["field"]], errors="coerce")
        if not np.isfinite(observed).all():
            raise ValueError(f"G7 slot filter feature missing: {setup_id}")
        audit.loc[mask, "g7_slot_postrank_observed"] = observed.to_numpy(float)
        audit.loc[mask, "g7_slot_postrank_minimum"] = float(rule["minimum"])
        audit.loc[mask, "g7_slot_postrank_field"] = rule["field"]
        audit.loc[mask, "g7_slot_postrank_allowed"] = observed.ge(float(rule["minimum"]) - 1e-12).to_numpy(bool)
    for key in ARM_GATES[ARMS[-1]]:
        if audit[key].isna().any() or not audit[key].isin((True, False)).all():
            raise ValueError(f"Incomplete G7 entry decision: {key}")
    audit["all_g7_entry_allowed"] = audit[list(ARM_GATES[ARMS[-1]])].all(axis=1)
    reasons = ("G7_NIFTY_BOTH_COUNTER", "G7_V4_LOW_EFFICIENCY_EXTREME_RSI",
               "G7_CONFIRMATION_EXTENSION_ATR", "G7_SLOT_POSTRANK_MINIMUM")
    audit["first_g7_veto"] = "ADMITTED"
    for gate, reason in reversed(tuple(zip(ARM_GATES[ARMS[-1]], reasons))):
        audit.loc[~audit[gate], "first_g7_veto"] = reason
    audit["research_policy_version"] = VERSION
    audit["research_policy_fingerprint"] = fingerprint()
    return audit.sort_values("sid", kind="stable").reset_index(drop=True)


def subset(orders: pd.DataFrame, audit: pd.DataFrame, arm: str) -> pd.DataFrame:
    if arm not in ARM_GATES:
        raise ValueError(f"Unknown G7 V4 overlay arm: {arm}")
    allowed = audit[list(ARM_GATES[arm])].all(axis=1) if ARM_GATES[arm] else pd.Series(True, index=audit.index)
    ids = set(audit.loc[allowed, "sid"].astype(int))
    return orders.loc[orders.sid.astype(int).isin(ids)].copy()
