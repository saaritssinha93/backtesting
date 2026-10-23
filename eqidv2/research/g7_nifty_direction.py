"""G7 adapter for the sealed G6 Nifty-futures direction gate."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from research import g6_nifty_direction as source


FILTER = source.FILTER
OFF = source.OFF
MODES = source.MODES
ENGINE = source.ENGINE
nifty = source.nifty
allowed = source.allowed
order_audit = source.order_audit


def apply_filter(unfiltered, audit, config):
    book, allocation = source.apply_filter(unfiltered, audit, config)
    book = book.rename(columns={
        "g6_native_filled": "g7_native_filled",
        "g6_nifty_allowed": "g7_nifty_allowed",
        "g6_nifty_filter_mode": "g7_nifty_filter_mode",
    })
    book.loc[book.portfolio_status.eq("G6_NIFTY_BOTH_COUNTER_BLOCKED"),
             "portfolio_status"] = "G7_NIFTY_BOTH_COUNTER_BLOCKED"
    book["strategy"] = "V13-V10-G-7"
    book["scope"] = "BACKTEST_ONLY"
    book["execution_authority"] = False
    return book, allocation


def verify_saved(output, summary):
    """Recompute saved decisions and verify the isolated G7 policy ledgers."""
    output = Path(output)
    mode = summary.get("nifty_reference_mode", summary["nifty_direction_filter_mode"])
    policy = summary.get("nifty_reference_policy", "g7_bounded_exit")
    primary = nifty.base.read_frozen_csv(output / policy / "portfolio_trades.csv")
    original = nifty.base.read_frozen_csv(output / "g7_unfiltered" / "portfolio_trades.csv")
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
            if row.both_counter_allowed != allowed(
                    row.side, values["slot_candle"], values["session_open"]):
                raise ValueError("Saved Nifty decision differs")
    permitted = primary.sid.map(audit.set_index("sid").both_counter_allowed)
    if (primary.portfolio_executed.eq(True) & ~permitted).any():
        raise ValueError("Blocked entry received capital")
    comparison = nifty.base.read_frozen_csv(output / "comparison.csv")
    recorded = comparison.loc[comparison.policy.eq(policy) & comparison.period.eq("all")]
    if len(recorded) != 1 or not np.isclose(
            primary.portfolio_net_profit_rupees.sum(), recorded.net_pnl_rs.iloc[0],
            rtol=0, atol=1e-6):
        raise ValueError("Primary profit differs from comparison")
    return len(audit)
