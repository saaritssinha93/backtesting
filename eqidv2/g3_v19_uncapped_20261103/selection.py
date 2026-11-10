"""One change to frozen G3 V17: admit every qualifying stock per enabled slot."""
from __future__ import annotations

from datetime import date
import hashlib
import json

import pandas as pd

from g3_v18_20261102 import selection as baseline_selection
from research import g3_slot_win70_contract as baseline_contract
import fno_v13_v10_g_3_contract as production


VERSION = "G3-V19-20261103-UNCAPPED-SLOT-BACKTEST"


def configuration() -> dict:
    return {
        "version": VERSION,
        "scope": "BACKTEST_ONLY",
        "execution_authority": False,
        "baseline_strategy_version": baseline_contract.STRATEGY_VERSION,
        "baseline_strategy_fingerprint": baseline_contract.strategy_fingerprint(),
        "one_change": "All candidates passing the existing enabled setup filters are selected; no per-slot max_entries slice.",
        "unchanged": ["enabled slot/side allowlist", "all signal and confirmation thresholds",
                      "core/expanded eligibility", "stop/target and expiry", "portfolio capital/position constraints"],
    }


def fingerprint() -> str:
    return hashlib.sha256(json.dumps(configuration(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def select(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return uncapped orders, full audit, and the exact frozen baseline orders."""
    baseline_orders, baseline_audit = baseline_selection.select(raw, "control")
    audit = baseline_audit.copy()
    if audit.empty:
        return audit.copy(), audit, baseline_orders
    eligible = audit.v9_filter_pass.eq(True)
    if audit.sid.duplicated().any() or audit.signal_id.duplicated().any():
        raise ValueError("Duplicate baseline candidate identity")
    audit["baseline_slot_max_entries"] = audit.max_entries
    audit["baseline_selected"] = audit.v9_selected
    audit["v9_selected"] = eligible
    audit.loc[eligible, "v9_decision"] = "SELECTED_UNCAPPED"
    counts = audit.loc[eligible].groupby(["day", "setup_id"], dropna=False).size()
    audit["max_entries"] = [int(counts.get((day, setup), 0))
                            for day, setup in zip(audit.day, audit.setup_id)]
    audit["v10_g_f_core"] = [
        bool(accepted and baseline_contract.passes_selected_filters(
            row, baseline_contract._CORE_BY_ID[row["setup_id"]], session_date=date.fromisoformat(str(row["day"])[:10])))
        for accepted, row in zip(eligible, audit.to_dict("records"))
    ]
    audit["source_v18_control_signal_id"] = audit.signal_id
    audit["strategy_version"] = VERSION
    audit["strategy_fingerprint"] = fingerprint()
    audit["research_variant"] = "no_slot_entry_limit"
    audit["execution_authority"] = False
    audit["scope"] = "BACKTEST_ONLY"
    for index, row in audit.iterrows():
        setup = baseline_contract._ACTIVE_BY_ID[row.setup_id]
        day = date.fromisoformat(str(row.day)[:10])
        audit.at[index, "signal_id"] = production.canonical_signal_id(
            VERSION, day, setup.signal_end, setup.confirmation_end, setup.side, row.tradingsymbol)
    orders = audit.loc[audit.v9_selected.eq(True)].reset_index(drop=True)
    if orders.signal_id.duplicated().any() or not baseline_orders.sid.isin(orders.sid).all():
        raise ValueError("Uncapped selection lost baseline orders or duplicated IDs")
    return orders, audit, baseline_orders
