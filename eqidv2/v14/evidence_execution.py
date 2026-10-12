"""Run frozen financial arithmetic on independently proven real-minute paths.

This boundary does not loosen the legacy baseline's continuous-path contract.
The caller authenticates source files and obtains each proof from path_evidence
or interval_evidence. Portfolio evidence then determines which resolved trades
and provably rejected unresolved orders form an exact daily portfolio result.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import baseline, interval_evidence, path_evidence
from ._baseline import execution


def simulate_proven(orders: pd.DataFrame, proofs: dict, *, cost_bps: float | None = None) -> pd.DataFrame:
    """Price only proven outcomes, preserving native fills, costs and sizing.

    Portfolio fields here are provisional: callers evaluating a selection with
    unresolved orders must use ``portfolio_evidence.resolve_portfolio`` before
    reporting profit. Missing-bar excursions are explicitly unavailable.
    """
    config = baseline.frozen_config()
    cost = float(config["cost_bps"] if cost_bps is None else cost_bps)
    if not np.isfinite(cost) or cost < 0:
        raise ValueError("Cost override must be finite and nonnegative")
    if orders.empty:
        return interval_evidence.annotate_ledger(baseline.simulate(orders, {}), {})
    if orders.sid.duplicated().any():
        raise ValueError("Duplicate selected signal IDs")
    paths, by_sid = {}, {}
    for row in orders.itertuples(index=False):
        sid = int(row.sid)
        proof = proofs.get(sid, proofs.get(str(sid)))
        if proof is None or proof.get("resolved") is not True:
            raise ValueError(f"Unresolved execution proof: {sid}")
        detail, path = proof["evidence"], proof["path"]
        if (detail.get("version") not in {path_evidence.EVIDENCE_VERSION, interval_evidence.EVIDENCE_VERSION}
                or detail.get("synthetic_bars") is not False
                or detail.get("selected_before_assessment") is not True):
            raise ValueError(f"Unsupported execution evidence: {sid}")
        if path_evidence._hash_prefix(path) != detail.get("prefix_sha256"):
            raise ValueError(f"Execution proof path changed: {sid}")
        confirmation = execution._to_ist_timestamp(row.confirmation_ts)
        if execution._to_ist_timestamp(detail["confirmation_ts"]) != confirmation:
            raise ValueError(f"Execution proof confirmation differs: {sid}")
        if detail.get("version") == interval_evidence.EVIDENCE_VERSION:
            identity = detail.get("selected_order_identity", {})
            expected = dict(side=row.side, trigger=float(row.trigger), setup_id=row.setup_id,
                            tradingsymbol=row.tradingsymbol, confirmation_ts=str(confirmation))
            if identity != expected:
                raise ValueError(f"Execution proof selected order identity differs: {sid}")
        target = config["exit"]["setups"].get(row.setup_id, config["exit"]["default"])["target_pct"]
        if float(detail["target_pct"]) != float(target):
            raise ValueError(f"Execution proof target differs: {sid}")
        outcome = detail["outcome"]
        expiry = confirmation + pd.Timedelta(minutes=config["entry_expiry_minutes"])
        if outcome["filled"]:
            entry = detail["entry"]
            if float(entry["trigger"]) != float(row.trigger):
                raise ValueError(f"Execution proof trigger differs: {sid}")
            if not confirmation < execution._to_ist_timestamp(entry["ts"]) <= expiry:
                raise ValueError(f"Execution proof entry exceeds clock expiry: {sid}")
        elif len(path["timestamp_ns"]) and path["timestamp_ns"][-1] > expiry.value:
            raise ValueError(f"Unfilled proof extends beyond clock expiry: {sid}")
        paths[sid], by_sid[sid] = path, proof
    work = orders.copy()
    work["native_target_pct"] = work.setup_id.map({k:v["target_pct"] for k,v in config["exit"]["setups"].items()}).fillna(config["exit"]["default"]["target_pct"])
    trades = execution.simulate_staged(work, paths, cost_bps=cost,
                                       max_entry_delay_minutes=config["entry_expiry_minutes"])
    for row in trades.itertuples(index=False):
        detail = by_sid[int(row.sid)]["evidence"]
        outcome = detail["outcome"]
        if bool(row.filled) != bool(outcome["filled"]) or row.exit_reason != outcome["exit_reason"]:
            raise ValueError(f"Native outcome differs from proof: {row.sid}")
        if row.filled:
            if (float(row.entry_price) != float(outcome["entry_price"])
                    or float(row.exit_price) != float(outcome["exit_price"])
                    or execution._to_ist_timestamp(row.entry_ts) != execution._to_ist_timestamp(detail["entry"]["ts"])
                    or execution._to_ist_timestamp(row.exit_ts) != execution._to_ist_timestamp(outcome["resolution_ts"])):
                raise ValueError(f"Native prices or execution clock differ from proof: {row.sid}")
    for column in ("entry_ts", "exit_ts"):
        if column not in trades:
            trades[column] = pd.NaT
    for column in ("net_return_pct", "gross_return_pct", "cost_pct"):
        if column not in trades:
            trades[column] = np.nan
    trades = execution.apply_fixed_capital_model(trades, config["capital_per_entry_rupees"], config["leverage_factor"])
    portfolio = execution.PortfolioConfig(portfolio_capital_rupees=config["portfolio_capital_rupees"], max_positions=config["max_positions"])
    ledger = execution.apply_portfolio_constraints(trades, portfolio)[0]
    return interval_evidence.annotate_ledger(ledger, by_sid)
