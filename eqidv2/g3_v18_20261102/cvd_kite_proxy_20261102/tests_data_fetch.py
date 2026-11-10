"""Meaningful time-boundary, validation, and membership-safety checks."""
from datetime import date
import pandas as pd
import pytest
from .data_fetch import normalize, windows, checked_output, resolve_token, cutoff_ist, PACKAGE


def candle(stamp, **extra):
    return dict(date=pd.Timestamp(stamp), open=100, high=102, low=99, close=101, volume=20, **extra)


def test_start_timestamp_is_shifted_and_incomplete_bar_excluded():
    rows=[candle('2026-11-02 09:15+05:30'), candle('2026-11-02 09:20+05:30')]
    out=normalize(rows,'ABC',pd.Timestamp('2026-11-02 09:23+05:30'))
    assert len(out)==1
    assert out.bar_end.iloc[0]==pd.Timestamp('2026-11-02 09:20+05:30')
    assert out.volume.iloc[0]==20


def test_invalid_ohlc_and_duplicate_bars_fail():
    row=candle('2026-11-02 09:15+05:30')
    with pytest.raises(ValueError,match='duplicated'):
        normalize([row,row],'ABC',pd.Timestamp('2026-11-02 15:30+05:30'))
    row['high']=99
    with pytest.raises(ValueError,match='Inconsistent'):
        normalize([row],'ABC',pd.Timestamp('2026-11-02 15:30+05:30'))


def test_naive_timestamp_fails():
    with pytest.raises(ValueError,match='naive'):
        normalize([candle('2026-11-02 09:15')],'ABC',pd.Timestamp('2026-11-02 15:30+05:30'))


def test_windows_cover_every_calendar_date_without_overlap():
    chunks=windows(date(2026,7,29),date(2026,11,2))
    assert len(chunks)==2
    days=[d.date() for left,right in chunks for d in pd.date_range(left,right)]
    assert days==[d.date() for d in pd.date_range('2026-07-29','2026-11-02')]


def test_output_cannot_modify_original_v18_files():
    with pytest.raises(ValueError):
        checked_output(PACKAGE.parent/'START_HERE.html')
    assert checked_output(PACKAGE/'kite_data'/'x.parquet').is_relative_to(PACKAGE)


def test_dated_token_alias_keeps_historical_symbol_identity():
    token, provider, basis = resolve_token('ICICIPRULI', {'ICICILIFE':4774913}, {4774913})
    assert token == 4774913 and provider == 'ICICILIFE'
    assert basis == 'DATED_EQUITY_TOKEN_CURRENT_PROVIDER_SYMBOL_ALIAS'
    assert resolve_token('ABC', {'ABC':2}, {1})[0] is None
    assert resolve_token('OLD', {'NEW':2}, {1})[0] is None


def test_resumed_json_cutoff_can_form_ist_coverage_grid():
    restored = cutoff_ist('2026-11-02 14:55:00+05:30')
    grid = pd.date_range(pd.Timestamp('2026-11-02 09:20',tz='Asia/Kolkata'), restored, freq='5min')
    assert len(grid) == 68
    assert str(grid.tz) == 'Asia/Kolkata'
