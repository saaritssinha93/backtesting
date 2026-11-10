"""Prior-only and missing-data checks for V14 same-clock RVOL."""
from __future__ import annotations

import math

import pandas as pd
import pytest

from research.g14_rvol_features import PriorClockRvol


def row(day: str, sid: int, symbol: str, clock: str, volume: float,
        *, source_count: int = 5) -> dict:
    return dict(sid=sid, day=day,
                signal_ts=pd.Timestamp(f"{day} {clock}", tz="Asia/Kolkata"),
                tradingsymbol=symbol, volume=volume,
                source_1m_count=source_count,
                open=100., high=101., low=99., close=100.5)


def test_only_prior_same_stock_same_clock_median_enters_denominator() -> None:
    state = PriorClockRvol()
    dates = pd.bdate_range("2026-07-29", periods=7).strftime("%Y-%m-%d")
    for n, day in enumerate(dates):
        # Out-of-order input rows and absent derived V18 fields are intentional.
        frame = pd.DataFrame([
            row(day, 10*n+2, "ABC", "09:45", 10.*(n+1)),
            row(day, 10*n+1, "ABC", "09:20", 100.*(n+1)),
            row(day, 10*n+3, "XYZ", "09:20", 1000.*(n+1)),
        ], index=[9, 3, 7])
        result = state.enrich_day(frame, day)
        assert result.sid.tolist() == frame.sid.tolist()
        assert result.index.tolist() == frame.index.tolist()
        assert result.flow_tod_history_count.tolist() == [min(n, 20)]*3
        if n < 5:
            assert not result.flow_tod_ready.any()
            assert result.flow_tod_rvol.isna().all()
        elif n == 5:
            # Prior values are [10,20,30,40,50], median 30. Current 60
            # is excluded. Other clocks/stocks retain their own scale.
            assert result.flow_tod_prior_median_volume.tolist() == [30., 300., 3000.]
            assert result.flow_tod_rvol.tolist() == pytest.approx([2., 2., 2.])
        else:
            assert result.flow_tod_rvol.tolist() == pytest.approx([2., 2., 2.])


def test_invalid_volume_or_source_bar_is_not_used_later() -> None:
    state = PriorClockRvol(min_periods=1)
    a = state.enrich_day(pd.DataFrame([row("2026-07-29", 1, "ABC", "09:40", 10.)]), "2026-07-29")
    assert not a.flow_tod_ready.iloc[0]
    bad = state.enrich_day(pd.DataFrame([row("2026-07-30", 2, "ABC", "09:40", 100.,
                                            source_count=4)]), "2026-07-30")
    assert not bad.flow_tod_ready.iloc[0]
    good = state.enrich_day(pd.DataFrame([row("2026-07-31", 3, "ABC", "09:40", 20.)]), "2026-07-31")
    assert good.flow_tod_history_count.iloc[0] == 1
    assert good.flow_tod_rvol.iloc[0] == pytest.approx(2.)


def test_missing_stock_clock_remains_missing_and_no_future_day() -> None:
    state = PriorClockRvol(min_periods=1)
    state.enrich_day(pd.DataFrame([row("2026-07-29", 1, "ABC", "09:20", 10.)]), "2026-07-29")
    next_day = state.enrich_day(pd.DataFrame([row("2026-07-30", 2, "ABC", "09:45", 20.)]), "2026-07-30")
    assert next_day.flow_tod_history_count.iloc[0] == 0
    assert math.isnan(next_day.flow_tod_rvol.iloc[0])
    with pytest.raises(ValueError, match="strictly increasing"):
        state.enrich_day(pd.DataFrame([row("2026-07-29", 3, "ABC", "09:20", 30.)]), "2026-07-29")


def test_duplicate_symbol_clock_rejected_before_history_update() -> None:
    state = PriorClockRvol(min_periods=1)
    day = "2026-07-29"
    frame = pd.DataFrame([row(day, 1, "ABC", "09:20", 10.),
                          row(day, 2, "ABC", "09:20", 20.)])
    with pytest.raises(ValueError, match="Duplicate"):
        state.enrich_day(frame, day)
    assert state.sessions_seen == 0
