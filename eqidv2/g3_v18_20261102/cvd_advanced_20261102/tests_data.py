import pandas as pd
import pytest
from .data import normalize,aggregate_5m,reconcile,cutoff_ist,checked,PACKAGE


def rows():
    return [dict(date=stamp,open=100,high=102,low=99,close=101,volume=20)
        for stamp in pd.date_range('2026-11-02 09:15',periods=5,freq='min',tz='Asia/Kolkata')]


def test_start_timestamp_and_incomplete_minute_boundary():
    frame=normalize(rows(),'ABC','2026-11-02 09:19:30+05:30')
    assert len(frame)==4
    assert frame.minute_end.iloc[0]==pd.Timestamp('2026-11-02 09:16+05:30')
    assert frame.minute_end.iloc[-1]==pd.Timestamp('2026-11-02 09:19+05:30')


def test_five_minute_reconstruction_and_partial_group():
    minute=normalize(rows(),'ABC','2026-11-02 15:30+05:30')
    bars=aggregate_5m(minute)
    assert len(bars)==1 and bool(bars.bar_contiguous.iloc[0])
    assert bars.volume.iloc[0]==100 and bars.source_1m_count.iloc[0]==5
    assert bars.bar_end.iloc[0]==pd.Timestamp('2026-11-02 09:20+05:30')
    partial=aggregate_5m(minute.drop(index=2))
    assert partial.source_1m_count.iloc[0]==4 and not bool(partial.bar_contiguous.iloc[0])


def test_invalid_or_duplicate_provider_candles_rejected():
    sample=rows()
    with pytest.raises(ValueError,match='duplicated'):
        normalize(sample+sample[:1],'ABC','2026-11-02 15:30+05:30')
    sample[0]['volume']=-1
    with pytest.raises(ValueError,match='Invalid'):
        normalize(sample,'ABC','2026-11-02 15:30+05:30')


def test_reconciliation_compares_same_completed_clock_only():
    minute=normalize(rows(),'ABC','2026-11-02 15:30+05:30')
    bars=aggregate_5m(minute)
    native=bars[['symbol','bar_end','open','high','low','close','volume']].copy()
    status,summary=reconcile(bars,native)
    assert bool(status.native5m_reference_match.iloc[0])
    native.loc[0,'volume']+=10
    status,summary=reconcile(bars,native)
    assert not bool(status.native5m_reference_match.iloc[0])
    assert summary.volume_mismatch_groups.iloc[0]==1


def test_resumed_offset_cutoff_forms_ist_grid():
    cutoff=cutoff_ist('2026-11-02 15:30:00+05:30')
    assert len(pd.date_range(pd.Timestamp('2026-11-02 09:16',tz='Asia/Kolkata'),cutoff,freq='min'))==375


def test_old_files_are_outside_allowed_output():
    with pytest.raises(ValueError):
        checked(PACKAGE.parent/'cvd_kite_proxy_20261102'/'README.md')
