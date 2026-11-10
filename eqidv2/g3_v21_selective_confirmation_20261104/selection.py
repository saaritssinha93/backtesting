"""Two minimal per-slot confirmation adjustments to uncapped G3 V19."""
from __future__ import annotations

from dataclasses import replace
from datetime import date
import hashlib
import json

import pandas as pd

from g3_v19_uncapped_20261103 import selection as v19
from research import g3_slot_win70_contract as contract
import fno_v13_v10_g_3_contract as production


VERSION = "G3-V21-20261104-SELECTIVE-CONFIRMATION-RESEARCH"
RULES = {
    "1016_SHORT": {"body_ratio": 0.35},  # frozen minimum 0.40
    "1046_SHORT": {"max_wick_ratio": 0.65},  # frozen maximum 0.60
}


def configuration() -> dict:
    return dict(version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        source_variant=v19.VERSION, source_fingerprint=v19.fingerprint(),
        confirmed_rule_changes={key: dict(value) for key, value in RULES.items()},
        selection_basis="Training sessions through 2026-09-30; two slots each had at least two sole-gate near-miss candidates and positive intrinsic candidate net P&L.",
        unchanged=["all other enabled setup thresholds and sides", "strict raw signal gates",
                   "exact next-minute confirmation", "no slot order quota", "stops, targets, expiry",
                   "portfolio capital and position limits"])


def fingerprint() -> str:
    return hashlib.sha256(json.dumps(configuration(), sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def _spec(original, setup_id: str):
    rule = RULES.get(setup_id)
    if not rule:
        return original
    if "body_ratio" in rule and rule["body_ratio"] >= original.body_ratio:
        raise ValueError("Body rule is not a relaxation")
    if "max_wick_ratio" in rule and rule["max_wick_ratio"] <= original.max_wick_ratio:
        raise ValueError("Wick rule is not a relaxation")
    return replace(original, **rule)


def select(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return new orders, complete strict-candidate audit, and exact V19 orders."""
    old_orders, old_audit, _ = v19.select(raw)
    audit = old_audit.copy()
    if audit.empty:
        return audit.copy(), audit, old_orders
    audit["v19_selected"] = audit.v9_selected
    audit["v19_filter_pass"] = audit.v9_filter_pass
    audit["v19_signal_id"] = audit.signal_id
    audit["original_body_minimum"] = audit.setup_id.map(
        {key: spec.body_ratio for key, spec in contract._ACTIVE_BY_ID.items()})
    audit["original_wick_maximum"] = audit.setup_id.map(
        {key: spec.max_wick_ratio for key, spec in contract._ACTIVE_BY_ID.items()})
    audit["effective_body_minimum"] = [
        _spec(contract._ACTIVE_BY_ID[key], key).body_ratio for key in audit.setup_id]
    audit["effective_wick_maximum"] = [
        _spec(contract._ACTIVE_BY_ID[key], key).max_wick_ratio for key in audit.setup_id]
    selected, core = [], []
    for row in audit.to_dict("records"):
        day = date.fromisoformat(str(row["day"])[:10])
        setup_id = row["setup_id"]
        passed = bool(contract.passes_selected_filters(
            row, _spec(contract._ACTIVE_BY_ID[setup_id], setup_id), session_date=day))
        selected.append(passed)
        core.append(passed and bool(contract.passes_selected_filters(
            row, _spec(contract._CORE_BY_ID[setup_id], setup_id), session_date=day)))
    audit["v9_selected"] = selected
    audit["v9_filter_pass"] = selected
    audit["v10_g_f_core"] = core
    if not audit.loc[audit.v19_selected.eq(True), "v9_selected"].all():
        raise ValueError("Selective confirmation policy lost a V19 order")
    changed = audit.v9_selected & ~audit.v19_selected
    if not audit.loc[changed, "setup_id"].isin(RULES).all():
        raise ValueError("Unlisted setup gained an order")
    audit.loc[changed, "v9_decision"] = "SELECTED_SELECTIVE_CONFIRMATION"
    audit.loc[audit.v19_selected.eq(True), "v9_decision"] = "SELECTED_V19"
    counts = audit.loc[audit.v9_selected.eq(True)].groupby(["day", "setup_id"], dropna=False).size()
    audit["max_entries"] = [int(counts.get((day, setup), 0))
                            for day, setup in zip(audit.day, audit.setup_id)]
    audit["strategy_version"] = VERSION
    audit["strategy_fingerprint"] = fingerprint()
    audit["research_variant"] = "two_slot_confirmation_exception"
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
        raise ValueError("Duplicate V21 order identity")
    return orders, audit, old_orders
