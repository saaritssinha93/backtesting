"""Independent coarser ranges certify no-touch gaps, never fabricate fills."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from v14 import baseline, interval_evidence
from v14._baseline import execution
from v14.path_evidence import assess as prefix_assess


def order(side="LONG"):
    return dict(sid=1,day="2026-08-03",tradingsymbol="SYNTHETIC",side=side,trigger=100.,
        confirmation_ts=pd.Timestamp("2026-08-03 09:31",tz=baseline.IST),setup_id="0931_"+side)


def full_path(row,terminal="09:50"):
    times=pd.date_range(row["confirmation_ts"]+pd.Timedelta(minutes=1),pd.Timestamp("2026-08-03 15:15",tz=baseline.IST),freq="min")
    path=dict(timestamp_ns=times.as_unit("ns").asi8,open=np.full(len(times),100.),
        high=np.full(len(times),100.1),low=np.full(len(times),99.9),close=np.full(len(times),100.))
    if terminal:
        j=times.get_loc(pd.Timestamp("2026-08-03 "+terminal,tz=baseline.IST))
        path["high" if row["side"]=="LONG" else "low"][j]=110. if row["side"]=="LONG" else 90.
    return path


def drop(path,*times):
    missing=[pd.Timestamp("2026-08-03 "+time,tz=baseline.IST).value for time in times]
    mask=~np.isin(path["timestamp_ns"],missing)
    return {key:value[mask] for key,value in path.items()}


def envelopes(*ends,**changes):
    return pd.DataFrame([dict(ts=pd.Timestamp("2026-08-03 "+end,tz=baseline.IST),open=100.,high=100.1,
        low=99.9,close=100.,volume=100.,symbol="SYNTHETIC",source=interval_evidence.NATIVE_SOURCE,
        independent_interval=True,candle_minutes=5,source_hash="a"*64,price_scale_compatible=True)|changes for end in ends])


def simulate(row,path):
    work=pd.DataFrame([row])
    cfg=baseline.frozen_config()
    work["native_stop_pct"]=1.25
    work["native_target_pct"]=cfg["exit"]["setups"].get(row["setup_id"],cfg["exit"]["default"])["target_pct"]
    return execution.simulate_staged(work,{row["sid"]:path},cost_bps=5.,max_entry_delay_minutes=10).iloc[0]


@pytest.mark.parametrize("side",["LONG","SHORT"])
def test_safe_envelope_preserves_exact_financial_outcome_and_observed_times(side):
    row=order(side); complete=full_path(row); sparse=drop(complete,"09:42")
    assert not prefix_assess(row,sparse)["resolved"]
    proof=interval_evidence.assess(row,sparse,envelopes("09:45"))
    assert proof["resolved"],proof
    assert proof["evidence"]["missing_minutes_certified"]==1
    assert not proof["evidence"]["synthetic_bars"]
    assert not proof["evidence"]["mfe_mae_exact"]
    assert set(proof["path"]["timestamp_ns"]).issubset(sparse["timestamp_ns"])
    full,actual=simulate(row,complete),simulate(row,proof["path"])
    for column in ("filled","entry_price","entry_ts","exit_price","exit_ts","exit_reason","exit_event",
                   "gross_return_pct","net_return_pct","holding_minutes","active_stop_pct_at_exit"):
        assert full[column]==actual[column],column
    ledger=interval_evidence.annotate_ledger(pd.DataFrame([actual]),{1:proof})
    assert pd.isna(ledger.iloc[0].mfe_pct) and pd.isna(ledger.iloc[0].mae_pct)


@pytest.mark.parametrize("changes",[
    {"high":110.},{"low":90.},{"source":"RESAMPLED_1M"},{"independent_interval":False},
    {"candle_minutes":1},{"symbol":"OTHER"},{"source_hash":"unknown"},{"volume":np.nan},
    {"gap_filled":True},{"opening_snapshot":1},{"provisional_stale":"true"},{"source_1m_count":5},
    {"low":100.05},{"high":100.05},
    {"source":pd.NA},{"independent_interval":pd.NA},{"candle_minutes":pd.NA},{"symbol":pd.NA},
    {"price_scale_compatible":False},{"price_scale_compatible":pd.NA},
])
def test_unsafe_invalid_conflicting_or_derived_envelope_fails_closed(changes):
    row=order(); sparse=drop(full_path(row),"09:42")
    proof=interval_evidence.assess(row,sparse,envelopes("09:45",**changes))
    assert not proof["resolved"]


def test_missing_and_duplicate_envelopes_are_unknown():
    row=order(); sparse=drop(full_path(row),"09:42")
    for five in (pd.DataFrame(),envelopes("09:40"),envelopes("09:45","09:45")):
        assert not interval_evidence.assess(row,sparse,five)["resolved"]


def test_post_terminal_gap_and_envelope_mutations_cannot_change_resolved_prefix():
    row=order(); sparse=drop(full_path(row),"10:42")
    expected=prefix_assess(row,sparse)
    proof=interval_evidence.assess(row,sparse,envelopes("10:45",source="UNTRUSTED"))
    assert proof["resolved"] and proof["evidence"]==expected["evidence"]
    for key in proof["path"]:
        np.testing.assert_array_equal(proof["path"][key],expected["path"][key])


def test_pre_entry_gap_preserves_clock_expiry_and_does_not_accept_late_hit():
    row=order(); complete=full_path(row,terminal=None)
    for name,value in (("open",99.5),("high",99.6),("low",99.4),("close",99.5)):
        complete[name][:]=value
    late=np.flatnonzero(complete["timestamp_ns"]==pd.Timestamp("2026-08-03 09:42",tz=baseline.IST).value)[0]
    complete["high"][late]=110.
    sparse=drop(complete,"09:34")
    # Without clock-aware clipping, native array offset expiry would admit the late bar.
    assert simulate(row,sparse).filled
    proof=interval_evidence.assess(row,sparse,envelopes("09:35",open=99.5,high=99.6,low=99.4,close=99.5))
    assert proof["resolved"] and not proof["evidence"]["outcome"]["filled"]
    assert proof["path"]["timestamp_ns"].max()==pd.Timestamp("2026-08-03 09:41",tz=baseline.IST).value
    assert not simulate(row,proof["path"]).filled


def test_observed_entry_after_proven_pre_entry_gap_has_exact_real_timestamp():
    row=order(); complete=full_path(row)
    before=complete["timestamp_ns"]<pd.Timestamp("2026-08-03 09:40",tz=baseline.IST).value
    for name,value in (("open",99.5),("high",99.6),("low",99.4),("close",99.5)):
        complete[name][before]=value
    sparse=drop(complete,"09:34")
    proof=interval_evidence.assess(row,sparse,envelopes("09:35",open=99.5,high=99.6,low=99.4,close=99.5))
    assert proof["resolved"] and proof["evidence"]["mfe_mae_exact"]
    actual=simulate(row,proof["path"])
    assert actual.entry_ts==pd.Timestamp("2026-08-03 09:40",tz=baseline.IST)
    assert actual.net_return_pct==simulate(row,complete).net_return_pct


@pytest.mark.parametrize("gap,should_resolve",[("11:32",True),("11:33",False)])
def test_tightening_boundary_uses_missing_minute_open_clock(gap,should_resolve):
    row=order(); sparse=drop(full_path(row,terminal="11:40"),gap)
    # Entry ends09:32: stop tightens at the11:32 open, i.e. bar end11:33.
    proof=interval_evidence.assess(row,sparse,envelopes("11:35",low=98.9))
    assert proof["resolved"] is should_resolve
    if not should_resolve:
        assert proof["reason"]=="5M_ENVELOPE_PERMITS_UNKNOWN_TOUCH"


def test_missing_square_off_is_not_inferred_from_coarse_close():
    row=order(); sparse=drop(full_path(row,terminal=None),"09:42","15:15")
    proof=interval_evidence.assess(row,sparse,envelopes("09:45","15:15"))
    assert not proof["resolved"] and proof["reason"]=="SQUARE_OFF_NOT_OBSERVED"


def test_proof_records_late_envelope_availability_without_moving_actual_exit():
    row=order(); complete=full_path(row,terminal=None)
    for name,value in (("open",99.5),("high",99.6),("low",99.4),("close",99.5)):
        complete[name][:]=value
    sparse=drop(complete,"09:41")
    proof=interval_evidence.assess(row,sparse,envelopes("09:45",open=99.5,high=99.6,low=99.4,close=99.5))
    assert proof["resolved"]
    assert proof["evidence"]["proof_available_ts"]=="2026-08-03 09:45:00+05:30"
    assert proof["evidence"]["outcome"]["resolution_ts"]=="2026-08-03 09:41:00+05:30"


def test_duplicate_or_bad_real_minute_is_not_replaced_by_coarse_envelope():
    row=order(); sparse=drop(full_path(row),"09:42")
    sparse["timestamp_ns"][5]=sparse["timestamp_ns"][4]
    assert not interval_evidence.assess(row,sparse,envelopes("09:45"))["resolved"]


def test_bound_touch_and_float_rounding_uncertainty_are_not_safe():
    row=order(); sparse=drop(full_path(row),"09:42")
    for low in (98.75,98.75000001):
        assert not interval_evidence.assess(row,sparse,envelopes("09:45",low=low))["resolved"]


@pytest.mark.parametrize("time",["15:15","15:16"])
def test_confirmation_without_execution_window_cannot_be_declared_unfilled(time):
    row=order()
    row["confirmation_ts"]=pd.Timestamp("2026-08-03 "+time,tz=baseline.IST)
    path={key:np.array([],dtype=np.int64 if key=="timestamp_ns" else float)
          for key in ("timestamp_ns","open","high","low","close")}
    proof=interval_evidence.assess(row,path,envelopes("15:15"))
    assert not proof["resolved"] and proof["reason"]=="NO_EXECUTION_WINDOW"


@pytest.mark.parametrize("side",["LONG","SHORT"])
@pytest.mark.parametrize("seed",range(6))
def test_random_complete_oracle_and_deleted_real_minutes_preserve_any_proven_finances(side,seed):
    """The complete native replay is an independent oracle for accepted proofs."""
    row=order(side); complete=full_path(row,terminal=None)
    rng=np.random.default_rng(seed)
    count=len(complete["timestamp_ns"])
    close=100+np.cumsum(rng.normal(0,.045,count))
    opening=np.r_[100.,close[:-1]]
    complete.update(open=opening,close=close,high=np.maximum(opening,close)+.04,
                    low=np.minimum(opening,close)-.04)
    # Completed broker-style interval ranges retain the whole hidden trajectory.
    native=pd.DataFrame({**complete,"ts":pd.to_datetime(complete["timestamp_ns"],utc=True).tz_convert(baseline.IST)})
    native["end"]=native.ts.dt.ceil("5min")
    five=native.groupby("end").agg(open=("open","first"),high=("high","max"),
        low=("low","min"),close=("close","last")).reset_index().rename(columns={"end":"ts"})
    five=five.assign(volume=100.,symbol="SYNTHETIC",source=interval_evidence.NATIVE_SOURCE,
        independent_interval=True,candle_minutes=5,source_hash="b"*64,price_scale_compatible=True)
    missing=rng.choice(np.arange(9,count-1),size=24,replace=False)
    keep=np.ones(count,dtype=bool); keep[missing]=False
    sparse={key:value[keep] for key,value in complete.items()}
    proof=interval_evidence.assess(row,sparse,five)
    if not proof["resolved"]:
        assert proof["reason"]=="5M_ENVELOPE_PERMITS_UNKNOWN_TOUCH"
        return
    expected,actual=simulate(row,complete),simulate(row,proof["path"])
    for column in ("filled","entry_price","entry_ts","exit_price","exit_ts","exit_reason","exit_event",
                   "gross_return_pct","net_return_pct","holding_minutes","active_stop_pct_at_exit"):
        assert actual[column]==expected[column],column
