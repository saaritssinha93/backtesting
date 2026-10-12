"""V14 selection without per-slot quotas; frozen G-3 remains reproducible."""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import baseline

UNLIMITED_SELECTION = "V14_ALL_ELIGIBLE_PER_SLOT_V1"
FROZEN_G3_SELECTION = "FROZEN_G3_PER_SLOT_CAPS_V1"


def setup_rules() -> list[dict]:
    """Return the active V14 rules without a maximum-selection field."""
    rules = baseline.setup_rules()
    for pair in rules:
        for name in ("original", "core", "expanded"):
            if name in pair:
                pair[name].pop("max_entries", None)
    return rules


def select_orders(features: pd.DataFrame,
                  participation_pass: baseline.ParticipationGate | pd.Series | str | None = None,
                  *, selection_policy: str = UNLIMITED_SELECTION) -> pd.DataFrame:
    """Select all eligible V14 stocks at every configured time and side.

    Inputs must already pass the same strict non-OI preparation described by
    ``baseline.select_orders``. All price, volume, confirmation, body and wick
    gates remain. Native ranks and core eligibility are retained as metadata;
    neither restricts the count. Portfolio capital is enforced by simulation.
    The capped policy is only for explicitly replaying archived configurations.
    """
    if selection_policy == FROZEN_G3_SELECTION:
        return baseline.select_orders(features, participation_pass)
    if selection_policy != UNLIMITED_SELECTION:
        raise ValueError(f"Unsupported slot selection policy: {selection_policy}")
    # An old ledger must not reintroduce the removed column into new output.
    features = features.drop(columns="max_entries", errors="ignore")
    if features.empty:
        return features.assign(setup_id=pd.Series(dtype=str), picker=pd.Series(dtype=str),
                               native_stop_pct=pd.Series(dtype=float),
                               native_target_pct=pd.Series(dtype=float),
                               slot_selection_policy=pd.Series(dtype=str)).copy()
    required = {"sid", "day", "tradingsymbol", "signal_ts", "confirmation_ts", "side", "hhmm_int",
                "price_change_pct", "volume_ratio", "body_ratio", "wick_ratio", "traded_value",
                "v9_1m_volume_ratio", "trigger"}
    missing = required.difference(features)
    if missing:
        raise ValueError(f"Missing strict cash features: {sorted(missing)}")
    if features.sid.duplicated().any() or not features.index.is_unique:
        raise ValueError("Signal IDs and candidate row indices must be unique")
    work = features.copy()
    for column in ("signal_ts", "confirmation_ts"):
        work[column] = pd.to_datetime(work[column], utc=True).dt.tz_convert(baseline.IST)
    if not work.confirmation_ts.sub(work.signal_ts).eq(pd.Timedelta(minutes=1)).all():
        raise ValueError("Frozen G-3 requires exact next-minute confirmation")
    if not work.side.isin(["LONG", "SHORT"]).all():
        raise ValueError("Candidate side must be LONG or SHORT")
    stamp = pd.to_datetime(work.get("v9_1m_feature_ts", work.confirmation_ts), utc=True, errors="coerce")
    if (stamp.isna() | stamp.gt(work.confirmation_ts)).any():
        raise ValueError("Noncausal or missing confirmation feature timestamp")
    config = baseline.frozen_config()
    work["abs_price_change_pct"] = work.price_change_pct.abs()
    thresholds = work.side.map(config["minimum_confirmation_1m_volume_ratio"])
    work = work.loc[np.isfinite(work.v9_1m_volume_ratio) & work.v9_1m_volume_ratio.ge(thresholds)].copy()
    parts = []
    for pair in setup_rules():
        core, expanded = pair["core"], pair["expanded"]
        rows = work.loc[work.hhmm_int.eq(int(expanded["signal_end"].replace(":", "")))
                        & work.side.eq(expanded["side"])].copy()
        if rows.empty:
            continue
        rows = rows.loc[baseline._gate(rows, expanded, participation_pass)].copy()
        if rows.empty:
            continue
        picker = baseline.PICKERS[expanded["picker"]]
        ranked = rows.sort_values(["day", picker, "traded_value", "tradingsymbol"],
                                  ascending=[True, False, False, True], kind="stable")
        chosen = ranked.copy()
        chosen["setup_id"] = baseline._setup_id(expanded)
        chosen["picker"] = expanded["picker"]
        chosen["configured_confirmation_end"] = expanded["confirmation_end"]
        chosen["v10_g_f_core"] = baseline._gate(chosen, core, participation_pass)
        chosen["v9_rank_in_setup_day"] = ranked.groupby("day", sort=False).cumcount().add(1)
        chosen["v9_selected"] = True
        chosen["v9_decision"] = "SELECTED"
        chosen["baseline_version"] = config["version"]
        chosen["slot_selection_policy"] = UNLIMITED_SELECTION
        pair_exit = config["exit"]["setups"].get(baseline._setup_id(expanded), config["exit"]["default"])
        chosen["native_stop_pct"] = pair_exit["stop_pct"]
        chosen["native_target_pct"] = pair_exit["target_pct"]
        parts.append(chosen)
    if not parts:
        return select_orders(features.iloc[:0], participation_pass)
    return pd.concat(parts, ignore_index=True).sort_values(
        ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"], kind="stable").reset_index(drop=True)
