"""Boundary and ablation checks for the isolated V14 OI-day OR experiment."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.g14_oi_day_or_selection import oi_day_or_branches, select_day


SOURCE = Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14/six_feature_source/candidates_by_day")


def _row(sid: int, slot: str, oi: float, prev: float, opening_oi: float,
         *, symbol: str = "TEST", side: str = "LONG", valid_opening: bool = True) -> dict:
    with np.errstate(divide="ignore", invalid="ignore"):
        day_pct = 100.0 * (oi / opening_oi - 1.0)
        delta_pct = 100.0 * (oi / prev - 1.0)
    return dict(sid=sid, day="2026-09-10", tradingsymbol=symbol, slot=slot,
                side=side, oi=oi, prev_oi=prev, opening_oi_0920=opening_oi,
                oi_day_pct=day_pct, oi_delta_5m_pct=delta_pct,
                contract_month="26SEP", opening_contract_month="26SEP",
                futures_tradingsymbol=symbol + "26SEPFUT",
                opening_futures_symbol=symbol + "26SEPFUT",
                oi_source="NFO_FUTURE",
                opening_0920_status="VALID" if valid_opening else "MISSING",
                oi_change_source="FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M")


def _decision(rows: list[dict], sid: int) -> pd.Series:
    result = oi_day_or_branches(pd.DataFrame(rows)).set_index("sid")
    return result.loc[sid]


def test_strict_day_threshold_and_independent_or_branches() -> None:
    one_percent = [
        _row(1, "0920", 1000, 990, 1000),
        _row(2, "0925", 1010, 1000, 1000),
        _row(3, "0930", 1010, 1010, 1000),
    ]
    exact = _decision(one_percent, 3)
    assert not exact.oi_day_gt_1 and not exact.oi_latest_bar_increasing
    assert not exact.oi_day_or_pass

    # Day OI just above 1% wins even though latest OI is falling.
    falling = [
        _row(4, "0920", 1000, 990, 1000, symbol="FALL", side="SHORT"),
        _row(5, "0925", 1020, 1000, 1000, symbol="FALL", side="SHORT"),
        _row(6, "0930", 1010.001, 1020, 1000, symbol="FALL", side="SHORT"),
    ]
    day_only = _decision(falling, 6)
    assert day_only.oi_day_gt_1 and not day_only.oi_latest_bar_increasing
    assert day_only.oi_day_or_pass  # OI is not sign-reversed for SHORT.

    # A small latest positive increase wins with day OI still below 1%.
    rising = [
        _row(7, "0920", 1000, 990, 1000, symbol="RISE"),
        _row(8, "0925", 1004, 1000, 1000, symbol="RISE"),
        _row(9, "0930", 1005, 1004, 1000, symbol="RISE"),
    ]
    latest_only = _decision(rising, 9)
    assert not latest_only.oi_day_gt_1 and latest_only.oi_latest_bar_increasing
    assert latest_only.oi_day_or_pass


def test_opening_four_minute_proxy_and_missing_evidence_fail_closed() -> None:
    rows = [
        _row(1, "0920", 1001, 1000, 1001, symbol="UP"),
        _row(2, "0920", 1000, 1000, 1000, symbol="FLAT"),
        _row(3, "0920", 999, 1000, 999, symbol="DOWN"),
        _row(4, "0920", 1001, 1000, 1001, symbol="STALE", valid_opening=False),
        _row(5, "0920", 1001, np.nan, 1001, symbol="NO_PREV"),
        _row(6, "0920", 1011, np.nan, 1000, symbol="BAD_BASELINE"),
    ]
    result = oi_day_or_branches(pd.DataFrame(rows)).set_index("sid")
    assert result.loc[1, "oi_latest_bar_increasing"]
    assert not result.loc[[2, 3, 4, 5, 6], "oi_day_or_pass"].any()
    assert not result.oi_day_gt_1.any()  # H is zero in the opening slot.


def test_one_missing_or_branch_does_not_invalidate_the_other() -> None:
    day_only = [
        _row(1, "0920", 1000, 990, 1000, symbol="DAY"),
        _row(2, "0925", 1011, np.nan, 1000, symbol="DAY"),
    ]
    rising_only = [
        _row(3, "0920", 1000, 990, np.nan, symbol="LATEST"),
        _row(4, "0925", 1001, 1000, np.nan, symbol="LATEST"),
    ]
    assert _decision(day_only, 2).oi_day_gt_1
    assert _decision(day_only, 2).oi_day_or_pass
    assert _decision(rising_only, 4).oi_latest_bar_increasing
    assert _decision(rising_only, 4).oi_day_or_pass


@pytest.mark.skipif(not (SOURCE / "2026-09-18.parquet").is_file(), reason="Frozen source unavailable")
def test_hindzinc_0935_stale_prior_oi_cannot_count_as_latest_bar() -> None:
    source = pd.read_parquet(SOURCE / "2026-09-18.parquet")
    rows = source.loc[source.tradingsymbol.eq("HINDZINC")]
    result = oi_day_or_branches(rows).set_index("sid")
    target = rows.loc[rows.slot.eq("0935")].iloc[0]
    assert target.oi_delta_5m_pct > 0  # Archived percentage looks attractive.
    assert target.oi_day_pct < 1
    assert pd.isna(rows.loc[rows.slot.eq("0930"), "oi"].iloc[0])
    assert not result.loc[int(target.sid), "oi_latest_bar_exact"]
    assert not result.loc[int(target.sid), "oi_day_or_pass"]


@pytest.mark.skipif(not (SOURCE / "2026-10-23.parquet").is_file(), reason="Frozen source unavailable")
def test_body_wick_ablation_and_pdc_independence_on_frozen_day() -> None:
    source = pd.read_parquet(SOURCE / "2026-10-23.parquet")
    _, base = select_day(source, "baseline_reconstructed")
    _, removed = select_day(source, "remove_body_wick")
    _, combined = select_day(source, "combined")
    assert int(removed.v9_filter_pass.sum()) >= int(base.v9_filter_pass.sum())
    # This date has an otherwise eligible candidate below the old body floor
    # and above the old maximum directional confirmation wick.
    assert (combined.v9_filter_pass & ~combined.gate_body).any()
    assert (combined.v9_filter_pass & ~combined.gate_max_wick).any()
    assert not (combined.v9_filter_pass & ~combined.gate_min_wick).any()
    icici = combined.loc[combined.tradingsymbol.eq("ICICILIFE") & combined.setup_id.eq("0946_SHORT")]
    assert len(icici) == 1
    assert "OFFICIAL_PDC_MISSING" in icici.integrity_reason.iloc[0]
    assert bool(icici.gate_integrity.iloc[0])  # V3 does not use PDC.
