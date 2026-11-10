"""Frozen V14 six-field morning candidate rules; no outcome fields are read.

Input schema is produced by the frozen six-feature candidate builder. The JSON
protocol is the sole source of thresholds. This module deliberately does not
reuse a G7 picker, score, or indicator gate. It preserves only V14's per-setup
max_entries from the six-feature V2 configuration.
"""

from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


_HERE = Path(__file__).resolve().parent
PROTOCOL_PATH = _HERE / "g14_six_feature_rule_protocol.json"
PROTOCOL_SHA256 = "642aecde6c454d7ce7d0129744a05388639a01aefc1ba7a83a7901fa0e80ef5e"
V14_CONFIG_PATH = _HERE.parent / "configs" / "strategies" / "v13_v10_g_14_six_feature_v2.json"
SIX_FIELDS = (
    "open_gap_pct",
    "pdc_pct",
    "price_5m_pct",
    "oi",
    "oi_delta_5m_pct",
    "oi_day_pct",
)
REQUIRED = (
    "sid",
    "day",
    "tradingsymbol",
    "slot",
    "side",
    "signal_ts",
    "integrity_ok",
    *SIX_FIELDS,
)


def _read_contract() -> tuple[dict[str, Any], dict[tuple[str, str], tuple[str, int]]]:
    protocol_bytes = PROTOCOL_PATH.read_bytes()
    if hashlib.sha256(protocol_bytes).hexdigest() != PROTOCOL_SHA256:
        raise ValueError("Frozen six-field protocol changed; create a new experiment version")
    protocol = json.loads(protocol_bytes)
    config = json.loads(V14_CONFIG_PATH.read_text(encoding="utf-8"))
    setups: dict[tuple[str, str], tuple[str, int]] = {}
    for setup in config["active_setups"]:
        key = (setup["signal_end"], setup["side"])
        if key in setups:
            raise ValueError(f"Duplicate V14 setup {key}")
        setups[key] = (setup["setup_id"], int(setup["max_entries"]))
    expected = {(slot, side) for slot in protocol["slots"] for side in protocol["sides"]}
    if set(setups) != expected:
        raise ValueError("V14 setup list does not match frozen protocol slots and sides")
    if len(protocol["variants"]) != protocol["candidate_variant_count"]:
        raise ValueError("Frozen protocol variant count mismatch")
    return protocol, setups


def _normalized_slot(values: pd.Series) -> pd.Series:
    raw = values.astype("string").str.strip()
    return raw.where(raw.str.contains(":", regex=False), raw.str.slice(0, 2) + ":" + raw.str.slice(2, 4))


def _path_mask(frame: pd.DataFrame, path: dict[str, Any]) -> pd.Series:
    mask = pd.Series(True, index=frame.index)
    conditions = {
        "g_min": ("g", "ge"),
        "g_max": ("g", "le"),
        "p_min": ("p", "ge"),
        "m_min": ("m", "ge"),
    }
    for condition, (column, operator) in conditions.items():
        if condition in path:
            mask &= getattr(frame[column], operator)(float(path[condition]))
    if "abs_G_lt" in path:
        mask &= frame["open_gap_pct"].abs().lt(float(path["abs_G_lt"]))
    return mask


def _rule_mask(frame: pd.DataFrame, rule: dict[str, Any]) -> pd.Series:
    common = rule["common"]
    mask = pd.Series(True, index=frame.index)
    for threshold, column in (("p_min", "p"), ("m_min", "m"), ("D_min", "oi_delta_5m_pct")):
        if threshold in common:
            mask &= frame[column].ge(float(common[threshold]))
    if "H_min" in common:
        mask &= frame["oi_day_pct"].ge(float(common["H_min"]))
    if "H_min_after_0920" in common:
        opening = frame["_slot"].eq("09:20")
        h_floor = np.where(
            opening,
            float(common["H_min_at_0920"]),
            float(common["H_min_after_0920"]),
        )
        mask &= frame["oi_day_pct"].ge(h_floor)
    gap = rule.get("gap_condition", "none")
    if gap == "g>=0":
        mask &= frame["g"].ge(0.0)
    elif gap != "none":
        raise ValueError(f"Unknown frozen gap condition {gap!r}")
    if "paths" in rule:
        paths = pd.Series(False, index=frame.index)
        for path in rule["paths"]:
            paths |= _path_mask(frame, path)
        mask &= paths
    return mask.fillna(False)


def _six_field_score(frame: pd.DataFrame) -> pd.Series:
    """Implements the score frozen in JSON; it is not a learned score."""
    return (
        0.45 * (frame["p"] / 1.00).clip(0.0, 1.0)
        + 0.35 * (frame["m"] / 0.50).clip(0.0, 1.0)
        + 0.10 * (frame["oi_delta_5m_pct"] / 0.50).clip(0.0, 1.0)
        + 0.10 * (frame["oi_day_pct"] / 1.00).clip(0.0, 1.0)
    )


def evaluate_variants(
    candidates: pd.DataFrame, *, return_audit: bool = False
) -> dict[str, pd.DataFrame] | tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """Apply the four predeclared rules and V14 setup quotas without looking at P&L.

    The ordinary return is ``{variant_id: selected_rows}``. Set
    ``return_audit=True`` to also receive ``{'decisions': ..., 'counts': ...}``.
    ``decisions`` has one row per input candidate and variant, with a mutually
    exclusive reason: selected, excluded_quota, excluded_rule,
    excluded_integrity:<source reason>, excluded_missing_feature,
    excluded_nonpositive_oi, or excluded_clock_or_setup.
    """
    missing = set(REQUIRED).difference(candidates.columns)
    if missing:
        raise ValueError(f"Candidate frame missing required columns: {sorted(missing)}")
    if candidates["sid"].duplicated().any():
        raise ValueError("Candidate sid must be unique before rule evaluation")
    protocol, setups = _read_contract()
    frame = candidates.copy().reset_index(drop=True)
    frame["_slot"] = _normalized_slot(frame["slot"])
    frame["_side"] = frame["side"].astype("string").str.upper().str.strip()
    frame["_signal_dt"] = pd.to_datetime(frame["signal_ts"], errors="coerce")
    frame["_day"] = frame["day"].astype("string").str.slice(0, 10)
    frame["_symbol"] = frame["tradingsymbol"].astype("string").str.upper().str.strip()

    # Only clock, identifier and integrity columns supplement the six inputs.
    setup_ok = pd.Series(
        [(slot, side) in setups for slot, side in zip(frame["_slot"], frame["_side"])],
        index=frame.index,
    )
    clock_ok = (
        frame["_signal_dt"].notna()
        & frame["_signal_dt"].dt.strftime("%Y-%m-%d").eq(frame["_day"])
        & frame["_signal_dt"].dt.strftime("%H:%M").eq(frame["_slot"])
        & frame["_symbol"].notna()
        & frame["_symbol"].ne("")
    )
    integrity_ok = frame["integrity_ok"].eq(True).fillna(False)  # noqa: E712
    for column in SIX_FIELDS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    finite = pd.Series(np.isfinite(frame[list(SIX_FIELDS)].to_numpy(dtype=float)).all(axis=1), index=frame.index)
    positive_oi = frame["oi"].gt(0.0)

    # Mutually exclusive first-failure audit reason.
    base_reason = pd.Series("eligible", index=frame.index, dtype="string")
    base_reason.loc[~setup_ok | ~clock_ok] = "excluded_clock_or_setup"
    bad_integrity = base_reason.eq("eligible") & ~integrity_ok
    if "integrity_reason" in frame:
        detail = frame["integrity_reason"].astype("string").fillna("unknown")
        base_reason.loc[bad_integrity] = "excluded_integrity:" + detail.loc[bad_integrity]
    else:
        base_reason.loc[bad_integrity] = "excluded_integrity"
    base_reason.loc[base_reason.eq("eligible") & ~finite] = "excluded_missing_feature"
    base_reason.loc[base_reason.eq("eligible") & ~positive_oi] = "excluded_nonpositive_oi"

    side_sign = frame["_side"].map({"LONG": 1.0, "SHORT": -1.0}).astype(float)
    frame["g"] = side_sign * frame["open_gap_pct"]
    frame["p"] = side_sign * frame["pdc_pct"]
    frame["m"] = side_sign * frame["price_5m_pct"]
    frame["rule_score"] = _six_field_score(frame)
    frame["setup_id"] = [setups.get((slot, side), (pd.NA, 0))[0] for slot, side in zip(frame["_slot"], frame["_side"])]

    selected: dict[str, pd.DataFrame] = {}
    audit_parts: list[pd.DataFrame] = []
    for rule in protocol["variants"]:
        variant_id = rule["id"]
        passes = _rule_mask(frame, rule) & base_reason.eq("eligible")
        ranked = frame.loc[passes].sort_values(
            ["_day", "_slot", "_side", "rule_score", "_symbol", "sid"],
            ascending=[True, True, True, False, True, True],
            kind="mergesort",
        ).copy()
        ranked["selection_rank"] = ranked.groupby(["_day", "_slot", "_side"], sort=False).cumcount() + 1
        ranked["_quota"] = [setups[(slot, side)][1] for slot, side in zip(ranked["_slot"], ranked["_side"])]
        admitted = ranked.loc[ranked["selection_rank"].le(ranked["_quota"])].copy()
        admitted["variant_id"] = variant_id
        # Preserve all replay fields; remove only our private helper columns.
        admitted = admitted.drop(columns=["_slot", "_side", "_signal_dt", "_day", "_symbol", "_quota"])
        selected[variant_id] = admitted

        reason = base_reason.copy()
        reason.loc[base_reason.eq("eligible")] = "excluded_rule"
        reason.loc[ranked.index] = "excluded_quota"
        reason.loc[admitted.index] = "selected"
        audit_parts.append(
            pd.DataFrame(
                {
                    "variant_id": variant_id,
                    "sid": frame["sid"],
                    "day": frame["_day"],
                    "slot": frame["_slot"],
                    "side": frame["_side"],
                    "tradingsymbol": frame["_symbol"],
                    "rule_score": frame["rule_score"],
                    "decision_reason": reason,
                }
            )
        )
    decisions = pd.concat(audit_parts, ignore_index=True)
    counts = (
        decisions.groupby(["variant_id", "day", "slot", "side", "decision_reason"], dropna=False)
        .size()
        .rename("candidate_count")
        .reset_index()
    )
    audit = {"decisions": decisions, "counts": counts}
    return (selected, audit) if return_audit else selected


__all__ = ["evaluate_variants", "SIX_FIELDS", "PROTOCOL_PATH", "PROTOCOL_SHA256", "V14_CONFIG_PATH"]
