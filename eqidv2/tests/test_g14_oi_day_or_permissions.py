"""Archived G7 V4 permission parity for a newly constructed G14 candidate book."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from research.g14_oi_day_or_permissions import allowed_day


ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT / "g3_indicator_lab_20261103" / "inputs"
LOCK2 = Path(
    "C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_lock_backtest/"
    "runs/2026-10-29/20261029T082128219945_full_current_config/"
    "g7_v4_lock2_portfolio_trades.csv"
)
EXTENSION = Path(
    "C:/TradingData/eqidv2/g3_g7_v4_through_20261102/"
    "comparison_20261102_verified/g7_extension_trades.csv"
)
MORNING = {f"{clock}_{side}" for clock in
           ("0921", "0926", "0931", "0936", "0941", "0946")
           for side in ("LONG", "SHORT")}


@pytest.mark.parametrize("source,expected", [(LOCK2, 99), (EXTENSION, 6)])
def test_archived_g7_v4_morning_permission_parity(source: Path, expected: int) -> None:
    if not source.exists():
        pytest.skip(f"Archived local G7 source unavailable: {source}")
    orders = pd.read_csv(source)
    orders = orders.loc[orders.setup_id.isin(MORNING)]
    assert len(orders) == expected
    count = 0
    for day, group in orders.groupby("day"):
        minutes = pd.read_parquet(LAB / day / "minutes.parquet")
        allowed, audit = allowed_day(group, day, minutes)
        old = group.set_index("sid").slot_policy_source_base_entry_allowed.astype(bool)
        pd.testing.assert_series_equal(allowed.sort_index(),
                                       old.sort_index().rename("permission_allowed"),
                                       check_dtype=False)
        assert audit.permission_status.eq("OK").all()
        assert audit.nifty_allowed.notna().all()
        assert audit.low_efficiency_veto.notna().all()
        count += len(group)
    assert count == expected


def test_missing_previous_nifty_bar_does_not_block_both_counter() -> None:
    if not LOCK2.exists():
        pytest.skip("Archived local G7 source unavailable")
    orders = pd.read_csv(LOCK2)
    selected = orders.loc[orders.day.eq("2026-09-18")
                          & orders.setup_id.eq("0936_LONG")
                          & orders.tradingsymbol.eq("HINDZINC")]
    assert len(selected) == 1
    minutes = pd.read_parquet(LAB / "2026-09-18" / "minutes.parquet")
    allowed, audit = allowed_day(selected, "2026-09-18", minutes)
    assert audit.status.iloc[0] == "MISSING_PREVIOUS_NIFTY_BAR"
    assert bool(allowed.iloc[0])
    assert bool(audit.nifty_allowed.iloc[0])


def test_missing_exact_nifty_slot_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    if not LOCK2.exists():
        pytest.skip("Archived local G7 source unavailable")
    from research import g14_oi_day_or_permissions as permissions

    orders = pd.read_csv(LOCK2)
    selected = orders.loc[orders.day.eq("2026-09-18")
                          & orders.setup_id.eq("0936_LONG")
                          & orders.tradingsymbol.eq("HINDZINC")]
    frame, proof = permissions._nifty_day("2026-09-18")
    truncated = frame.loc[frame.ts.dt.strftime("%H:%M").ne("09:35")]
    monkeypatch.setattr(permissions, "_nifty_day", lambda day: (truncated, proof))
    minutes = pd.read_parquet(LAB / "2026-09-18" / "minutes.parquet")
    with pytest.raises(ValueError, match="Incomplete exact Nifty direction"):
        allowed_day(selected, "2026-09-18", minutes)
