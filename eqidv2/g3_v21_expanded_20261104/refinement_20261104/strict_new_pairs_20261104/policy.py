"""Pure backtest-only V21 selection: cap the 39 new pairs and veto weak RVOL.

Input is the expanded V21 cap-three *selected order pool*, after G3's strict
signal and exact next-minute confirmation checks. For each new setup/day,
retain its first core-ranked stock, otherwise its first eligible ranked stock.
Then require observed completed 5m volume_ratio to exceed that setup's
existing minimum by 0.10 absolute. A veto never promotes a lower ranked stock.
Original V21 pairs keep their saved cap-three orders and entry thresholds.
"""
from __future__ import annotations

import hashlib
import json

import numpy as np
import pandas as pd

from g3_v21_expanded_20261104 import selection


VERSION = "G3-V21-NEW39-CAP1-5M-RVOL-PLUS-0P10-RESEARCH-20261104"
VOLUME_INCREMENT = 0.10
NEW_IDS = frozenset(selection.NEWLY_ENABLED)
OLD_IDS = frozenset(selection.ENABLED_BEFORE)
if len(NEW_IDS) != 39 or len(OLD_IDS) != 34 or NEW_IDS & OLD_IDS:
    raise ValueError("Expanded V21 setup inventory changed")


def configuration() -> dict:
    return dict(version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
                source_variant=selection.VERSION,
                source_fingerprint=selection.fingerprint(),
                source_pool="expanded_V21_selected_cap3_orders",
                original_setup_ids=sorted(OLD_IDS), new_setup_ids=sorted(NEW_IDS),
                original_cap_per_setup_side_day=3, new_cap_per_setup_side_day=1,
                core_first=True, new_setup_5m_volume_ratio_increment=VOLUME_INCREMENT,
                gate_clock="completed_5m_signal_bar",
                gate_timing="after_core_first_cap1_selection_before_entry",
                veto_promotes_next_ranked_stock=False,
                unchanged="V21 original-pair gates and all setup targets, stops, costs, sizing and execution paths")


def fingerprint() -> str:
    return hashlib.sha256(json.dumps(configuration(), sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def cap_new39(expanded_cap3_orders: pd.DataFrame) -> pd.DataFrame:
    """Return core-first cap-one prefixes for new pairs; original pairs stay cap three."""
    if expanded_cap3_orders.empty:
        return expanded_cap3_orders.copy()
    frame = expanded_cap3_orders
    if "v9_selected" in frame and not frame.v9_selected.eq(True).all():
        raise ValueError("Input must contain selected expanded V21 orders only")
    if frame.sid.isna().any() or frame.sid.duplicated().any():
        raise ValueError("Expanded V21 selected order identities are not unique")
    if not set(frame.setup_id).issubset(NEW_IDS | OLD_IDS):
        raise ValueError("Unknown expanded V21 setup ID")
    keep: list[int] = []
    for (_, setup_id), group in frame.groupby(["day", "setup_id"], sort=False):
        if setup_id not in NEW_IDS:
            keep.extend(group.index.tolist())
            continue
        ranked = group.assign(_rank=pd.to_numeric(group.v9_rank_in_setup_day, errors="raise"))
        ranked = ranked.sort_values(["_rank", "sid"], kind="stable")
        core = ranked.loc[ranked.v10_g_f_core.eq(True)]
        chosen = core.iloc[:1] if len(core) else ranked.iloc[:1]
        keep.extend(chosen.index.tolist())
    if len(keep) != len(set(keep)):
        raise ValueError("Duplicate cap-one selected order")
    out = frame.loc[sorted(keep)].copy()
    counts = out.groupby(["day", "setup_id"]).size()
    if any(count > (1 if setup_id in NEW_IDS else 3)
           for (_, setup_id), count in counts.items()):
        raise ValueError("Per-setup/day cap exceeded")
    return out


def apply(expanded_cap3_orders: pd.DataFrame) -> pd.DataFrame:
    """Apply new39 cap-one and the +0.10 completed-5m RVOL admission veto.

    This pure decision reads only setup ID, rank/core flags, completed feature
    timestamp and 5m volume ratio. Saved native P&L and outcome columns play no
    role. The original signal_id remains unchanged for staged-fill replay.
    """
    capped = cap_new39(expanded_cap3_orders)
    if capped.empty:
        return capped
    new = capped.setup_id.isin(NEW_IDS)
    observed = pd.to_numeric(capped.volume_ratio, errors="raise")
    required = capped.setup_id.map({key: spec.volume_ratio for key, spec in selection.SPECS.items()})
    if not np.isfinite(observed.loc[new]).all() or required.loc[new].isna().any():
        raise ValueError("Missing completed 5m volume evidence")
    signal = pd.to_datetime(capped.loc[new, "signal_ts"], utc=True)
    feature = pd.to_datetime(capped.loc[new, "v9_5m_feature_ts"], utc=True)
    available = pd.to_datetime(capped.loc[new, "v9_feature_available_ts"], utc=True)
    confirmation = pd.to_datetime(capped.loc[new, "confirmation_ts"], utc=True)
    if not feature.eq(signal).all() or not available.le(confirmation).all():
        raise ValueError("Volume-ratio feature chronology failed")
    if not confirmation.sub(signal).eq(pd.Timedelta(minutes=1)).all():
        raise ValueError("Next-minute confirmation drift")
    pass_gate = observed.ge(required + VOLUME_INCREMENT - 1e-12)
    admitted = capped.loc[~new | pass_gate].copy()
    original_ids = admitted.signal_id.copy()
    admitted["research_source_signal_id"] = original_ids
    admitted["research_policy_version"] = VERSION
    admitted["research_policy_fingerprint"] = fingerprint()
    admitted["research_scope"] = "BACKTEST_ONLY"
    admitted["research_execution_authority"] = False
    if not admitted.signal_id.equals(original_ids):
        raise ValueError("Source signal identity changed")
    return admitted
