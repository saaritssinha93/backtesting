from datetime import datetime, timedelta
import json

import pytest
from g3_trade_monitor_rules import IST, assess_trade


START = datetime(2026, 10, 14, 10, 0, tzinfo=IST)


def fixture(side="LONG", closes=None):
    closes = closes or [100, 99.9, 99.8, 99.7, 99.6, 99.5]
    trade = dict(side=side, entry_price=100., entry_at_ist=START.isoformat(),
        quantity=10, stop_price=98.75 if side == "LONG" else 101.25,
        target_price=101. if side == "LONG" else 99.)
    bars = [dict(timestamp=START + timedelta(minutes=i+1), open=c, high=c+.01, low=c-.01,
        close=c, volume=None) for i,c in enumerate(closes)]
    return trade, bars


def assess(trade, bars, **kwargs):
    return assess_trade(trade, bars, now=bars[-1]["timestamp"], mark_price=bars[-1]["close"],
        mark_timestamp=bars[-1]["timestamp"], **kwargs)


@pytest.mark.parametrize("side,closes", [("LONG", [100,99.9,99.8,99.7,99.6,99.5]),
    ("SHORT", [100,100.1,100.2,100.3,100.4,100.5])])
def test_adverse_direction_is_side_aware(side, closes):
    t,b=fixture(side,closes)
    out=assess(t,b)
    assert out["direction"] == "ADVERSE" and out["advisory_flags"] == ["TREND_FAILURE"]
    assert out["metrics"]["signed_move_pct"] == pytest.approx(-.5)
    assert out["metrics"]["gross_unrealized_pnl_rs"] == pytest.approx(-5)
    json.dumps(out,allow_nan=False)


def test_future_bars_do_not_change_decision():
    t,b=fixture()
    original=assess(t,b)
    future={**b[-1],"timestamp":b[-1]["timestamp"]+timedelta(minutes=1),"high":999.,"close":999.}
    assert assess_trade(t,b+[future],now=b[-1]["timestamp"],mark_price=99.5,mark_timestamp=b[-1]["timestamp"]) == original


def test_stale_price_and_future_quote_never_signal():
    t,b=fixture()
    for stamp,direction in [(b[-1]["timestamp"],"STALE"),(b[-1]["timestamp"]+timedelta(minutes=4),"NO_DATA")]:
        out=assess_trade(t,b,now=b[-1]["timestamp"]+timedelta(minutes=3),mark_price=99.5,mark_timestamp=stamp)
        assert out["direction"]==direction and out["advisory_flags"]==[]


def test_entry_partial_bar_and_gap_do_not_qualify():
    t,b=fixture()
    t["entry_at_ist"]=(START+timedelta(seconds=1)).isoformat()
    assert assess(t,b)["direction"]=="WARMING_UP"
    t,b=fixture()
    b[2]["timestamp"]+=timedelta(seconds=1)
    assert assess(t,b)["advisory_flags"]==[]


def test_profit_giveback_and_momentum_can_disagree_with_positive_pnl():
    t,b=fixture(closes=[100.1,100.6,100.65,100.5,100.35,100.2])
    out=assess(t,b)
    assert out["metrics"]["signed_move_pct"]>0
    assert out["advisory_flags"]==["PROFIT_GIVEBACK"]
    assert out["metrics"]["giveback_pct"]==pytest.approx(.46)


def test_conflicting_bar_and_invalid_ohlc_disable_flags():
    t,b=fixture()
    conflict={**b[2],"close":99.81,"high":99.82}
    assert assess(t,b+[conflict])["advisory_flags"]==[]
    b[1]["high"]=1
    assert assess(t,b)["advisory_flags"]==[]


def test_updated_order_time_is_not_quote_time():
    t,b=fixture()
    t.update(last_price=99.5,updated_at_ist=b[-1]["timestamp"].isoformat())
    assert assess_trade(t,b,now=b[-1]["timestamp"])["direction"]=="NO_DATA"
