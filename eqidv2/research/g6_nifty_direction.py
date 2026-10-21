"""G6 backtest entry gate: reject only when both Nifty directions oppose.

The Nifty inputs are the dated near-month futures five-minute candles. A
decision uses the exact completed candle and the 09:20 session open. An absent
or invalid candle aborts a filtered run instead of silently admitting a trade.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from research import g4_nifty_direction_backtest as nifty
from research import g4_failed_breakout_research as sealed

FILTER = "both_counter_skip"
OFF = "off"
MODES = (FILTER, OFF)
ENGINE = sealed.bounded.history.simulator.g.v9


def allowed(side, slot_change_pct, session_change_pct):
    """Flat or mixed Nifty directions allow the entry on either side."""
    return nifty.allow_direction(side, slot_change_pct) or nifty.allow_direction(side, session_change_pct)


def order_audit(orders, prices):
    if orders.sid.duplicated().any():
        raise ValueError("Duplicate G6 order SID")
    records, proofs, inputs = [], [], {}
    for day, group in orders.groupby("day", sort=True):
        months = group.contract_month.dropna().map(nifty.normalize_month).unique()
        if len(months) != 1:
            raise ValueError("Ambiguous dated Nifty contract month: " + day)
        frame, proof = nifty.nifty_day(day, months[0], prices)
        proofs.append(proof)
        inputs[day] = frame
        for row in group.itertuples():
            values = nifty.direction_at(frame, row.confirmation_ts)
            slot, session = values["slot_candle"], values["session_open"]
            if not np.isfinite([slot, session]).all():
                raise ValueError(f"Incomplete Nifty context: {day} {row.setup_id} {row.tradingsymbol}")
            stamp = pd.Timestamp(row.confirmation_ts)
            if pd.Timestamp(values["nifty_bar_ts"]) > stamp:
                raise ValueError("Future Nifty candle used")
            records.append(dict(sid=int(row.sid), day=day, setup_id=row.setup_id,
                tradingsymbol=row.tradingsymbol, side=row.side,
                confirmation_ts=str(stamp), nifty_contract=proof["contract"],
                nifty_source_sha256=proof["sha256"], **values,
                both_counter_allowed=allowed(row.side, slot, session)))
    audit = pd.DataFrame(records)
    if len(audit) != len(orders) or audit.sid.duplicated().any():
        raise ValueError("Incomplete Nifty decision audit")
    return audit, pd.DataFrame(proofs), inputs


def apply_filter(unfiltered, audit, config):
    decision = audit.set_index("sid").both_counter_allowed
    permitted = unfiltered.sid.map(decision)
    if permitted.isna().any() or set(decision.index) != set(unfiltered.sid):
        raise ValueError("Missing or extraneous Nifty decision")
    work = unfiltered.copy()
    work["g6_native_filled"] = work.filled.eq(True)
    work["g6_nifty_allowed"] = permitted.to_numpy(bool)
    work["filled"] = work.filled.eq(True) & permitted
    sized = ENGINE.v5.apply_fixed_capital_model(
        work, config["capital_per_entry_rs"], config["leverage"])
    settings = ENGINE.V9Config(
        portfolio_capital_rupees=config["portfolio_capital_rs"],
        capital_per_entry_rupees=config["capital_per_entry_rs"],
        leverage_factor=config["leverage"], max_positions=10,
        cost_bps=config["model_cost_bps_round_trip"])
    book, allocation = ENGINE.v6.apply_portfolio_constraints(sized, settings.portfolio_config())
    blocked = ~permitted.to_numpy(bool)
    book.loc[blocked, "portfolio_status"] = "G6_NIFTY_BOTH_COUNTER_BLOCKED"
    book.loc[blocked, "portfolio_reject_reason"] = "BOTH_NIFTY_DIRECTIONS_OPPOSE"
    book["strategy"] = "V13-V10-G-6"
    book["scope"] = "BACKTEST_ONLY"
    book["execution_authority"] = False
    book["g6_nifty_filter_mode"] = FILTER
    return book, allocation


def verify_saved(output, summary):
    """Recompute saved decisions from sealed dated candles and check trade gate."""
    output = Path(output)
    mode = summary.get("nifty_reference_mode", summary["nifty_direction_filter_mode"])
    policy = summary.get("nifty_reference_policy", "g6_bounded_exit")
    primary = nifty.base.read_frozen_csv(output / policy / "portfolio_trades.csv")
    original = nifty.base.read_frozen_csv(output / "g6_unfiltered" / "portfolio_trades.csv")
    if mode == OFF:
        for field in ("portfolio_executed", "portfolio_net_profit_rupees"):
            pd.testing.assert_series_equal(primary[field], original[field], check_names=False)
        return 0
    audit = pd.read_csv(output / "nifty_direction_audit.csv")
    proofs = pd.read_csv(output / "nifty_source_proof.csv")
    if len(audit) != len(primary) or audit.sid.duplicated().any() or set(audit.sid) != set(primary.sid):
        raise ValueError("Saved Nifty audit does not cover all selected orders")
    for day, group in audit.groupby("day", sort=True):
        frame = pd.read_parquet(output / "nifty_inputs" / (day + ".parquet"))
        proof = proofs.loc[proofs.day.eq(day)]
        if len(proof) != 1 or not group.nifty_source_sha256.eq(proof.sha256.iloc[0]).all():
            raise ValueError("Saved Nifty provenance differs")
        for row in group.itertuples():
            values = nifty.direction_at(frame, row.confirmation_ts)
            for key in ("slot_candle", "session_open"):
                np.testing.assert_allclose(values[key], getattr(row, key), rtol=0, atol=1e-10)
            if row.both_counter_allowed != allowed(row.side, values["slot_candle"], values["session_open"]):
                raise ValueError("Saved Nifty decision differs")
    permitted = primary.sid.map(audit.set_index("sid").both_counter_allowed)
    if (primary.portfolio_executed.eq(True) & ~permitted).any():
        raise ValueError("Blocked entry received capital")
    if not np.isclose(primary.portfolio_net_profit_rupees.sum(),
            nifty.base.read_frozen_csv(output / "comparison.csv").loc[
                lambda frame: frame.policy.eq(policy) & frame.period.eq("all"),
                "net_pnl_rs"].iloc[0], rtol=0, atol=1e-6):
        raise ValueError("Primary profit differs from comparison")
    return len(audit)
