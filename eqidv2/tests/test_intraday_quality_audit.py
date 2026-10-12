from datetime import date

import pandas as pd
import pytest

from tools.audit_dashboard_flow_intraday import scan_file, valid_rows, windows
import dashboard_flow_live_data as flow


def candles():
    times = pd.date_range('2026-10-09 09:20', periods=3, freq='5min', tz='Asia/Kolkata')
    return pd.DataFrame(dict(ts=times, open=[100.,101.,102.], high=[102.,103.,104.],
        low=[99.,100.,101.], close=[101.,102.,103.], volume=[10.,20.,30.], source_1m_count=5))


@pytest.mark.parametrize('bad', [{'is_synthetic': 1}, {'interpolated': True}, {'source': 'FORWARD_FILLED'}, {'close': float('inf')}, {'volume': -1}, {'high': 1}, {'quality_state': 'PARTIAL'}])
def test_audit_and_dashboard_reject_bad_intraday_bar(bad):
    frame = candles()
    for key, value in bad.items():
        if key not in frame:
            frame[key] = None
        frame.loc[1, key] = value
    assert valid_rows(frame).ts.tolist() == [frame.ts.iloc[0], frame.ts.iloc[2]]
    assert flow._cash_valid(frame).ts.tolist() == [frame.ts.iloc[0], frame.ts.iloc[2]]


def test_duplicate_rows_cannot_supply_an_exact_minute_aggregate():
    frame = candles()
    frame = pd.concat([frame, frame.iloc[[1]]], ignore_index=True)
    assert len(valid_rows(frame)) == 2
    assert len(flow._cash_valid(frame)) == 2


def test_scan_real_oi_filename_and_exact_offsets(tmp_path):
    frame = candles().rename(columns={'ts':'timestamp'})
    frame['candle_start'] = frame.timestamp - pd.Timedelta(minutes=5)
    frame['oi'] = 123
    frame['quality_state'] = 'VALID'
    path = tmp_path / 'RELIANCE26OCTFUT_5minute.parquet'
    frame.to_parquet(path, index=False)
    rows, status = scan_file(path, date(2026,10,9), date(2026,10,9), oi=True)
    assert status == 'READ'
    assert rows[date(2026,10,9)]['slots'] == {560,565,570}


def test_missing_windows_are_exact_and_do_not_bridge_good_bar():
    assert windows({560,565,575}, 5) == '09:20-09:25;09:35'
