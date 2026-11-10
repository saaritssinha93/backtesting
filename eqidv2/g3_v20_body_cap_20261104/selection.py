"""One-gate variant of G3 V19: cap the confirmation body minimum at 0.30."""
from __future__ import annotations

from dataclasses import replace
from datetime import date
import hashlib
import json

import pandas as pd

from g3_v19_uncapped_20261103 import selection as v19
from research import g3_slot_win70_contract as contract
import fno_v13_v10_g_3_contract as production


VERSION = "G3-V20-20261104-BODY-MIN-CAP-030-RESEARCH"
BODY_CAP = 0.30


def configuration() -> dict:
    return {
        "version": VERSION,
        "scope": "BACKTEST_ONLY",
        "execution_authority": False,
        "source_variant": v19.VERSION,
        "source_fingerprint": v19.fingerprint(),
        "one_change": "For an enabled slot, cap its minimum next-minute confirmation body ratio at 0.30; retain every passing stock per slot.",
        "body_minimum_cap": BODY_CAP,
        "unchanged": ["strict raw EMA/OI/price/volume signal", "enabled slots and sides",
                      "all other per-slot filters and exact next-minute confirmation",
                      "no per-slot entry quota", "stops/targets/expiry", "portfolio constraints"],
    }


def fingerprint() -> str:
    return hashlib.sha256(json.dumps(configuration(), sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def select(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return V20 orders/audit and exact V19 orders from identical raw features."""
    v19_orders, v19_audit, _ = v19.select(raw)
    audit = v19_audit.copy()
    if audit.empty:
        return audit.copy(), audit, v19_orders
    audit["v19_selected"] = audit.v9_selected
    audit["v19_filter_pass"] = audit.v9_filter_pass
    audit["v19_signal_id"] = audit.signal_id
    audit["original_body_minimum"] = audit.setup_id.map(
        {key: spec.body_ratio for key, spec in contract._ACTIVE_BY_ID.items()})
    audit["effective_body_minimum"] = audit.original_body_minimum.clip(upper=BODY_CAP)
    rows = audit.to_dict("records")
    selected = []
    core = []
    for row in rows:
        day = date.fromisoformat(str(row["day"])[:10])
        spec = contract._ACTIVE_BY_ID[row["setup_id"]]
        relaxed = replace(spec, body_ratio=min(spec.body_ratio, BODY_CAP))
        core_spec = contract._CORE_BY_ID[row["setup_id"]]
        relaxed_core = replace(core_spec, body_ratio=min(core_spec.body_ratio, BODY_CAP))
        passed = bool(contract.passes_selected_filters(row, relaxed, session_date=day))
        selected.append(passed)
        core.append(passed and bool(contract.passes_selected_filters(
            row, relaxed_core, session_date=day)))
    audit["v9_filter_pass"] = selected
    audit["v9_selected"] = selected
    audit["v10_g_f_core"] = core
    if not audit.loc[audit.v19_selected.eq(True), "v9_selected"].all():
        raise ValueError("Body-cap rule lost a V19 order")
    changed = audit.v9_selected & ~audit.v19_selected
    if (pd.to_numeric(audit.loc[changed, "body_ratio"], errors="coerce")
            .ge(audit.loc[changed, "effective_body_minimum"]) == False).any():
        raise ValueError("Newly selected row fails body cap")
    audit.loc[changed, "v9_decision"] = "SELECTED_BODY_CAP_030"
    audit.loc[audit.v19_selected.eq(True), "v9_decision"] = "SELECTED_V19"
    counts = audit.loc[audit.v9_selected.eq(True)].groupby(
        ["day", "setup_id"], dropna=False).size()
    audit["max_entries"] = [int(counts.get((day, setup), 0))
                            for day, setup in zip(audit.day, audit.setup_id)]
    audit["strategy_version"] = VERSION
    audit["strategy_fingerprint"] = fingerprint()
    audit["research_variant"] = "body_minimum_cap_030"
    audit["execution_authority"] = False
    audit["scope"] = "BACKTEST_ONLY"
    audit["signal_id"] = [production.canonical_signal_id(
        VERSION, date.fromisoformat(str(row.day)[:10]),
        contract._ACTIVE_BY_ID[row.setup_id].signal_end,
        contract._ACTIVE_BY_ID[row.setup_id].confirmation_end,
        contract._ACTIVE_BY_ID[row.setup_id].side, row.tradingsymbol)
        for row in audit.itertuples()]
    orders = audit.loc[audit.v9_selected.eq(True)].reset_index(drop=True)
    if orders.signal_id.duplicated().any() or orders.sid.duplicated().any():
        raise ValueError("Duplicate V20 order identity")
    return orders, audit, v19_orders
