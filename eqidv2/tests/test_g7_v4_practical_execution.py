"""Independent synthetic checks for pending-gap cancellation and minute equity."""
import numpy as np
import pandas as pd
import pytest

from research import g7_v4_practical_research as study


DAY = "2026-10-01"
IST = "Asia/Kolkata"


def stamp(clock, day=DAY):
    return pd.Timestamp(f"{day} {clock}", tz=IST)


def pending(side="LONG", *, fill_index=0, gap_bps=0., filled=True, size=11):
    times = pd.date_range(stamp("10:01"), periods=size, freq="min")
    p = dict(timestamp_ns=times.as_unit("ns").asi8,
             open=np.full(size,99.8), high=np.full(size,99.9),
             low=np.full(size,99.7), close=np.full(size,99.8))
    if fill_index is not None:
        opening = 100.*(1.+gap_bps/10000.)
        p["open"][fill_index] = opening
        p["high"][fill_index] = max(opening,100.2)
        p["low"][fill_index] = min(opening,99.9)
        p["close"][fill_index] = 100.
    price = 100.*(1.+gap_bps/10000.)
    if side == "SHORT":
        p = {**p, "open":200.-p["open"], "high":200.-p["low"],
             "low":200.-p["high"], "close":200.-p["close"]}
        price = 200.-price
    row = dict(sid=1, side=side, day=DAY, tradingsymbol="TEST", setup_id="1000_"+side,
               trigger=100., confirmation_ts=stamp("10:00"),
               filled=filled, g8_baseline_filled=filled,
               entry_ts=times[fill_index] if fill_index is not None else pd.NaT,
               entry_price=price if fill_index is not None else np.nan,
               entry_path_index=fill_index if fill_index is not None else np.nan,
               exit_ts=stamp("10:20"), exit_price=101., exit_path_index=19,
               gross_return_pct=1., net_return_pct=.95, exit_reason="TARGET")
    return pd.DataFrame([row]), {1:p}


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_exact_ten_basis_point_open_is_retained_for_both_directions(side):
    native, paths = pending(side, gap_bps=10.)
    before = native.copy(deep=True)
    result = study.gap_cancel(native, paths)
    assert result.filled.tolist() == [True]
    assert result.loc[0,"gap_cancel_status"] == "NATIVE_RETAINED"
    assert pd.isna(result.loc[0,"gap_cancel_path_index"])
    assert result.loc[0,"entry_price"] == native.loc[0,"entry_price"]
    pd.testing.assert_frame_equal(native, before)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
@pytest.mark.parametrize("fill_index", [0,3])
def test_gap_cancels_before_that_bar_touch_even_if_it_recovers(side, fill_index):
    native, paths = pending(side, fill_index=fill_index, gap_bps=10.1)
    result = study.gap_cancel(native, paths)
    assert not result.loc[0,"filled"]
    assert not result.loc[0,"g8_baseline_filled"]
    assert result.loc[0,"gap_cancel_path_index"] == fill_index
    assert result.loc[0,"gap_cancel_status"] == "CANCELED_OPEN_GAP"
    assert result.loc[0,"exit_reason"] == "UNFILLED"
    for name in ("entry_ts","entry_price","entry_path_index","exit_ts","exit_price",
                 "exit_path_index","gross_return_pct","net_return_pct"):
        assert pd.isna(result.loc[0,name])


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_valid_gap_suffix_after_first_fill_cannot_cancel_an_existing_position(side):
    native, paths = pending(side, fill_index=1, gap_bps=2.)
    original = study.gap_cancel(native, paths)
    p = {k:v.copy() for k,v in paths[1].items()}
    # Later valid candles gap far beyond the allowance, after order completion.
    values = (110.,112.,108.,111.) if side == "LONG" else (90.,92.,88.,89.)
    for k,v in zip(("open","high","low","close"),values):
        p[k][2:] = v
    changed = study.gap_cancel(native,{1:p})
    pd.testing.assert_frame_equal(original,changed)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_touch_after_original_ten_minute_lifetime_never_fills(side):
    native, paths = pending(side, fill_index=10, filled="False")
    native["entry_ts"] = pd.NaT
    native["entry_price"] = np.nan
    native["entry_path_index"] = np.nan
    result = study.gap_cancel(native,paths)
    assert result.loc[0,"gap_cancel_status"] == "UNFILLED"
    assert pd.isna(result.loc[0,"gap_cancel_path_index"])


@pytest.mark.parametrize("field", ["timestamp_ns","high","low","open"])
def test_invalid_pending_window_fails_closed(field):
    native,paths = pending()
    p = {k:v.copy() for k,v in paths[1].items()}
    if field == "timestamp_ns":p[field][3] += 60_000_000_000
    elif field == "high":p[field][3] = 80.
    elif field == "low":p[field][3] = 120.
    else:p[field][3] = np.nan
    with pytest.raises(ValueError,match="order lifetime|pending-window OHLC"):
        study.gap_cancel(native,{1:p})


def decimal_touch(side, *, beyond_tolerance=False):
    """Represent one decimal threshold through independently serialized values."""
    trigger = 251.1000061035156
    tolerance = 8*np.finfo(float).eps*max(1.,abs(trigger))
    one_ulp = abs(np.nextafter(trigger,np.inf)-trigger)
    distance = 2*tolerance if beyond_tolerance else one_ulp
    times = pd.date_range(stamp("10:01"),periods=10,freq="min")
    if side == "SHORT":
        op,hi,lo,close = trigger+.1,trigger+.2,trigger+distance,trigger+.1
    else:
        op,hi,lo,close = trigger-.1,trigger-distance,trigger-.2,trigger-.1
    p = dict(timestamp_ns=times.as_unit("ns").asi8,
             open=np.full(10,op),high=np.full(10,hi),low=np.full(10,lo),close=np.full(10,close))
    row = dict(sid=1,side=side,day=DAY,tradingsymbol="DECIMAL",setup_id="1000_"+side,
               trigger=trigger,confirmation_ts=stamp("10:00"),
               filled=not beyond_tolerance,g8_baseline_filled=not beyond_tolerance,
               entry_path_index=np.nan if beyond_tolerance else 0,
               entry_price=np.nan if beyond_tolerance else trigger,
               entry_ts=pd.NaT if beyond_tolerance else times[0])
    return pd.DataFrame([row]),{1:p}


@pytest.mark.parametrize("side",["LONG","SHORT"])
def test_serialized_decimal_touch_with_one_binary_ulp_difference_retains_native_fill(side):
    native,paths = decimal_touch(side)
    before = native.copy(deep=True)
    snapshot = {k:v.copy() for k,v in paths[1].items()}
    # Comparison normalization must never rewrite the source prices/opens.
    for value in paths[1].values():value.flags.writeable = False
    result = study.gap_cancel(native,paths)
    assert result.loc[0,"gap_cancel_status"] == "NATIVE_RETAINED"
    assert result.loc[0,"entry_path_index"] == 0
    assert result.loc[0,"entry_price"] == native.loc[0,"trigger"]
    pd.testing.assert_frame_equal(native,before)
    for key,value in paths[1].items():np.testing.assert_array_equal(value,snapshot[key])


@pytest.mark.parametrize("side",["LONG","SHORT"])
def test_genuine_non_touch_outside_serialization_tolerance_stays_unfilled(side):
    native,paths = decimal_touch(side,beyond_tolerance=True)
    result = study.gap_cancel(native,paths)
    assert not result.loc[0,"filled"]
    assert result.loc[0,"gap_cancel_status"] == "UNFILLED"
    assert pd.isna(result.loc[0,"gap_cancel_path_index"])


@pytest.mark.parametrize("side",["LONG","SHORT"])
def test_touch_normalization_does_not_relax_observed_open_gap_limit(side):
    native,paths = pending(side,gap_bps=10.)
    limit = 100.*(1.+(1 if side == "LONG" else -1)*10./10000.)
    paths[1]["open"][0] = np.nextafter(limit,np.inf if side == "LONG" else -np.inf)
    result = study.gap_cancel(native,paths)
    assert result.loc[0,"gap_cancel_status"] == "CANCELED_OPEN_GAP"
    assert not result.loc[0,"filled"]


@pytest.mark.parametrize("side",["LONG","SHORT"])
def test_earlier_ulp_only_misses_do_not_retime_the_sealed_native_fill(side):
    native,paths = pending(side,fill_index=8)
    trigger = 14.100000381469728
    native["trigger"] = trigger
    native["entry_price"] = trigger
    p = paths[1]
    if side == "LONG":
        bars = (trigger-.05,trigger-.025,trigger-.1,trigger-.05)
        edge = "high"
        early_miss = np.nextafter(trigger,-np.inf)
        actual_hit = trigger+.1
    else:
        bars = (trigger+.05,trigger+.1,trigger+.025,trigger+.05)
        edge = "low"
        early_miss = np.nextafter(trigger,np.inf)
        actual_hit = trigger-.1
    for key,value in zip(("open","high","low","close"),bars):p[key][:] = value
    p[edge][6:8] = early_miss
    p[edge][8] = actual_hit
    result = study.gap_cancel(native,paths)
    assert result.loc[0,"gap_cancel_status"] == "NATIVE_RETAINED"
    assert result.loc[0,"entry_path_index"] == 8
    assert result.loc[0,"entry_ts"] == stamp("10:09")
    assert result.loc[0,"entry_price"] == trigger


def test_no_gap_cancellations_preserve_the_exact_sealed_fill_inventory():
    long,lp = pending("LONG",fill_index=1,gap_bps=2.)
    short,sp = pending("SHORT",fill_index=3,gap_bps=2.)
    unfilled,up = pending("LONG",fill_index=None,filled=False)
    short["sid"],unfilled["sid"] = 2,3
    native = pd.concat([long,short,unfilled],ignore_index=True)
    native["entry_ts"] = pd.to_datetime(native.entry_ts,utc=True).dt.tz_convert(IST)
    result = study.gap_cancel(native,{1:lp[1],2:sp[1],3:up[1]},cap_bps=1000.)
    pd.testing.assert_frame_equal(result[native.columns],native)
    assert result.gap_cancel_status.tolist() == ["NATIVE_RETAINED","NATIVE_RETAINED","UNFILLED"]
    assert result.gap_cancel_path_index.isna().all()


@pytest.mark.parametrize("side",["LONG","SHORT"])
def test_unambiguous_earlier_native_touch_cannot_be_hidden_by_sealed_fill_index(side):
    native,paths = pending(side,fill_index=8)
    if side == "LONG":paths[1]["high"][6] = 100.01
    else:paths[1]["low"][6] = 99.99
    with pytest.raises(ValueError,match="unsupported"):
        study.gap_cancel(native,paths)


def equity_trade(sid=1, *, side="LONG", day=DAY, entry="10:01", exit="10:04",
                 gross=5000., cost=250., executed=True):
    return dict(sid=sid, day=day, side=side, filled=True, g8_baseline_filled=True,
                portfolio_executed=executed, entry_ts=stamp(entry,day), exit_ts=stamp(exit,day),
                entry_price=100., portfolio_trade_exposure_rupees=500_000.,
                portfolio_gross_profit_rupees=gross if executed else 0.,
                portfolio_cost_rupees=cost if executed else 0.,
                portfolio_net_profit_rupees=gross-cost if executed else 0.)


def equity_path(side="LONG",day=DAY):
    times = pd.date_range(stamp("10:01",day),periods=5,freq="min")
    close = np.array([100.,99.8,100.5,500.,20.])
    if side == "SHORT":close = np.array([100.,100.2,99.5,20.,500.])
    return dict(timestamp_ns=times.as_unit("ns").asi8,close=close)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_minute_equity_marks_direction_quantity_and_full_cost_once_then_actual_exit(side):
    book = pd.DataFrame([equity_trade(side=side)])
    result = study.minute_equity(book,{1:equity_path(side)},[DAY]).set_index("ts")
    assert len(result) == 360
    assert result.loc[stamp("10:00"),"day_pnl_rs"] == 0.
    assert result.loc[stamp("10:01"),"day_pnl_rs"] == pytest.approx(-250.)
    assert result.loc[stamp("10:02"),"day_pnl_rs"] == pytest.approx(-1250.)
    assert result.loc[stamp("10:03"),"day_pnl_rs"] == pytest.approx(2250.)
    # Terminal/after-exit candle closes are deliberately absurd. Realized P&L
    # must replace MTM, without another cost charge or dependence on suffix.
    assert result.loc[stamp("10:04"),"day_pnl_rs"] == pytest.approx(4750.)
    assert result.loc[stamp("15:15"),"day_pnl_rs"] == pytest.approx(4750.)
    assert result.loc[stamp("10:02"),"drawdown_rs"] == pytest.approx(1250.)
    assert result.iloc[-1].cumulative_pnl_rs == pytest.approx(4750.)


def test_minute_equity_adds_concurrent_trades_and_ignores_unexecuted_inventory():
    book = pd.DataFrame([equity_trade(1),equity_trade(2,side="SHORT"),
                         equity_trade(3,executed=False)])
    paths = {1:equity_path(),2:equity_path("SHORT")}
    result = study.minute_equity(book,paths,[DAY]).set_index("ts")
    assert result.loc[stamp("10:01"),"day_pnl_rs"] == pytest.approx(-500.)
    assert result.loc[stamp("10:02"),"day_pnl_rs"] == pytest.approx(-2500.)
    assert result.iloc[-1].day_pnl_rs == pytest.approx(9500.)


def test_minute_equity_carries_equity_and_drawdown_across_a_zero_trade_day():
    book = pd.DataFrame([equity_trade(gross=-6000.)])
    result = study.minute_equity(book,{1:equity_path()},[DAY,"2026-10-02"])
    next_day = result.loc[result.day.eq("2026-10-02")]
    assert next_day.day_pnl_rs.eq(0.).all()
    assert next_day.cumulative_pnl_rs.eq(-6250.).all()
    # The prior completed-close mark reached +2250 before a -6250 final loss.
    assert next_day.drawdown_rs.eq(8500.).all()


def test_exact_open_exit_uses_realized_value_at_that_minute_boundary():
    book = pd.DataFrame([equity_trade(exit="10:02",gross=-6000.)])
    result = study.minute_equity(book,{1:equity_path()},[DAY]).set_index("ts")
    assert result.loc[stamp("10:01"),"day_pnl_rs"] == pytest.approx(-250.)
    assert result.loc[stamp("10:02"),"day_pnl_rs"] == pytest.approx(-6250.)


def test_missing_active_minute_mark_is_an_error_not_an_optimistic_skip():
    p = equity_path()
    p = {k:np.delete(v,1) for k,v in p.items()}
    with pytest.raises(ValueError,match="Missing minute-close mark"):
        study.minute_equity(pd.DataFrame([equity_trade()]),{1:p},[DAY])


def test_position_surviving_past_grid_cannot_silently_miss_terminal_reconciliation():
    row = equity_trade(exit="15:16")
    times = pd.date_range(stamp("10:01"),stamp("15:15"),freq="min")
    p = dict(timestamp_ns=times.as_unit("ns").asi8,close=np.full(len(times),100.))
    with pytest.raises(ValueError,match="MTM close does not reconcile"):
        study.minute_equity(pd.DataFrame([row]),{1:p},[DAY])
