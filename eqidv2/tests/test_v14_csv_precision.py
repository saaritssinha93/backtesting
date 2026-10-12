"""Archived IEEE754 trigger boundaries must survive CSV replay exactly."""
from types import SimpleNamespace
import json

import numpy as np
import pandas as pd
import pytest

from v14 import baseline, selection
from v14.coverage_study import load_fixed_selections
from v14._baseline import execution
from v14.path_evidence import assess


@pytest.mark.parametrize("trigger",[469.20001220703125,235.39999389648438])
def test_round_trip_csv_preserves_observed_exact_touch_and_native_fill(tmp_path,trigger):
    confirmation=pd.Timestamp("2026-09-21 09:26",tz=baseline.IST)
    original=pd.DataFrame([dict(sid=1,day="2026-09-21",tradingsymbol="SYNTHETIC",side="LONG",
        setup_id="0926_LONG",confirmation_ts=confirmation,trigger=trigger,
        picker="max_liquidity",native_stop_pct=1.25,native_target_pct=.97,
        signal_ts=confirmation-pd.Timedelta(minutes=1),traded_value=2e7,volume_ratio=3.,
        price_change_pct=.8,slot_selection_policy=selection.UNLIMITED_SELECTION)])
    file=tmp_path/"selected_no_oi_control.csv"
    original.to_csv(file,index=False)
    original.to_parquet(tmp_path/"feature_ledger.parquet",index=False)
    (tmp_path/"protocol.json").write_text(json.dumps(dict(protocol=dict(
        slot_selection_policy=selection.UNLIMITED_SELECTION,variants=[dict(name="no_oi_control")]))))
    restored=load_fixed_selections(tmp_path)["no_oi_control"].iloc[0]
    assert restored.trigger==trigger
    assert np.float64(restored.trigger).tobytes()==np.float64(trigger).tobytes()
    times=pd.date_range(confirmation+pd.Timedelta(minutes=1),periods=10,freq="min")
    path=dict(timestamp_ns=times.as_unit("ns").asi8,open=np.full(10,trigger*.998),
        high=np.full(10,trigger*.999),low=np.full(10,trigger*.997),close=np.full(10,trigger*.998))
    path["high"][0]=trigger
    native=execution._entry(SimpleNamespace(side="LONG",trigger=trigger),path,
        delay_bars=0,trigger_buffer_pct=0.,worse_fill_bps=0.,max_entry_delay_minutes=10)
    replay=execution._entry(restored,path,
        delay_bars=0,trigger_buffer_pct=0.,worse_fill_bps=0.,max_entry_delay_minutes=10)
    assert replay==native and replay[0]==0 and replay[1]==trigger
    # One representable upward step changes an equality touch into no fill.
    moved=restored.copy()
    moved["trigger"]=np.nextafter(trigger,np.inf)
    assert execution._entry(moved,path,delay_bars=0,trigger_buffer_pct=0.,worse_fill_bps=0.,max_entry_delay_minutes=10) is None
    assert assess(restored,path)["reason"]=="POSITION_OPEN_AT_FIRST_INTERRUPTION"
    assert assess(moved,path)["reason"]=="RESOLVED_UNFILLED_ENTRY_WINDOW"
