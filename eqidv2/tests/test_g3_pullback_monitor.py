from types import SimpleNamespace

import numpy as np
import pandas as pd

from research.g3_pullback_monitor import features, warning_features, label_outcome, alert_episodes


def candles(n=50):
    ts = pd.date_range("2026-10-07 09:16", periods=n, freq="min", tz="Asia/Kolkata")
    close = 100+np.sin(np.arange(n)/3)
    return pd.DataFrame(dict(ts=ts, open=close+.05, high=close+.2,
                             low=close-.2, close=close, volume=100+np.arange(n)))


def test_features_are_prefix_causal():
    original = candles()
    altered = original.copy()
    altered.loc[35:, ["open","high","low","close","volume"]] *= 10
    pd.testing.assert_frame_equal(features(original).iloc[:35], features(altered).iloc[:35])


def test_mirrored_direction_has_same_warning():
    long = SimpleNamespace(open=100.5,high=100.6,low=99.9,close=100.,atr14=.4,
        sma5=100.3,sma13=100.5,prior3_low=100.1,prior3_high=101.,
        momentum3=-.4,momentum5=-.8,volume_ratio20=1.4)
    short = SimpleNamespace(open=99.5,high=100.1,low=99.4,close=100.,atr14=.4,
        sma5=99.7,sma13=99.5,prior3_low=99.,prior3_high=99.9,
        momentum3=.4,momentum5=.8,volume_ratio20=1.4)
    a,b = warning_features(long,1,101.),warning_features(short,-1,99.)
    assert a["fast_warning"] and a["slow_warning"]
    assert (a["fast_warning"],a["slow_warning"]) == (b["fast_warning"],b["slow_warning"])
    assert np.isclose(a["giveback_atr"],b["giveback_atr"])


def test_current_candle_extreme_not_a_future_hit():
    m = candles(8)
    m[["open","high","low","close"]] = [100.,100.1,99.9,100.]
    m.loc[0,"low"] = 90.
    result = label_outcome(m,m.ts.iloc[0],100.,1,5,.3,m.ts.iloc[7],m.ts.iloc[7],100.)
    assert not result["event"]
    assert result["full_horizon_eligible"]


def test_exit_candle_extreme_excluded_but_actual_fill_can_hit():
    m = candles(5)
    m[["open","high","low","close"]] = [100.,100.1,99.9,100.]
    m.loc[2,"low"] = 90.
    end = m.ts.iloc[2]
    nohit = label_outcome(m,m.ts.iloc[0],100.,1,5,.3,end,end,100.2)
    hit = label_outcome(m,m.ts.iloc[0],100.,1,5,.3,end,end,99.5)
    assert not nohit["event"] and nohit["censored"]
    assert not nohit["outcome_available"]
    assert hit["event"] and hit["event_source"] == "ACTUAL_EXIT_FILL"
    assert not hit["censored"] and hit["full_horizon_eligible"]


def test_short_future_high_labels_event():
    m = candles(8)
    m[["open","high","low","close"]] = [100.,100.1,99.9,100.]
    m.loc[2,"high"] = 100.4
    result = label_outcome(m,m.ts.iloc[0],100.,-1,5,.3,m.ts.iloc[7],m.ts.iloc[7],100.)
    assert result["event"] and result["lead_minutes"] == 2


def test_alert_cooldown_uses_only_time_and_warning():
    x = pd.DataFrame(dict(trade_id=["a"]*13,horizon_minutes=[5]*13,
        decision_ts=pd.date_range("2026-10-07 09:30",periods=13,freq="min",tz="Asia/Kolkata"),
        warning=[True]*13,event=[False]*13))
    first = alert_episodes(x)
    x["event"] = True
    second = alert_episodes(x)
    assert first.decision_ts.tolist() == second.decision_ts.tolist()
    assert first.decision_ts.tolist() == x.decision_ts.iloc[[0,5,10]].tolist()


def test_open_exit_allows_previous_completed_bar_but_not_exit_bar():
    m = candles(5)
    m[["open","high","low","close"]] = [100.,100.1,99.9,100.]
    m.loc[1,"low"] = 99.5
    result = label_outcome(m,m.ts.iloc[0],100.,1,5,.3,m.ts.iloc[1],m.ts.iloc[2],100.)
    assert result["event"] and result["lead_minutes"] == 1


def test_close_exit_uses_whole_exit_candle():
    m = candles(5)
    m[["open","high","low","close"]] = [100.,100.1,99.9,100.]
    m.loc[2,"low"] = 99.5
    result = label_outcome(m,m.ts.iloc[0],100.,1,5,.3,m.ts.iloc[2],m.ts.iloc[2],100.,"CLOSE")
    assert result["event"] and result["outcome_available"]
    assert result["event_source"] == "COMPLETED_BAR"


def test_intrabar_adverse_open_is_known_even_with_profitable_exit():
    m = candles(5)
    m[["open","high","low","close"]] = [100.,100.6,99.9,100.]
    m.loc[2,["open","low"]] = [99.6,99.5]
    result = label_outcome(m,m.ts.iloc[0],100.,1,5,.3,m.ts.iloc[2],m.ts.iloc[2],100.5,"INTRABAR")
    assert result["event"] and result["outcome_available"]
    assert result["event_source"] == "EXIT_CANDLE_OPEN"
    assert result["lead_minutes"] == 1


def test_prior_event_is_resolved_despite_unknown_exit_extreme():
    m = candles(5)
    m[["open","high","low","close"]] = [100.,100.6,99.9,100.]
    m.loc[1,"low"] = 99.5
    m.loc[2,"low"] = 99.4
    result = label_outcome(m,m.ts.iloc[0],100.,1,5,.3,m.ts.iloc[2],m.ts.iloc[2],100.5,"INTRABAR")
    assert result["event"] and result["outcome_available"]
    assert result["event_source"] == "COMPLETED_BAR"


def test_exact_threshold_is_inclusive_both_directions():
    for sign, low, high in [(1,99.7,100.1),(-1,99.9,100.3)]:
        m = candles(8)
        m[["open","high","low","close"]] = [100.,100.1,99.9,100.]
        m.loc[1,["low","high"]] = [low,high]
        result = label_outcome(m,m.ts.iloc[0],100.,sign,5,.3,m.ts.iloc[7],m.ts.iloc[7],100.)
        assert result["event"]
