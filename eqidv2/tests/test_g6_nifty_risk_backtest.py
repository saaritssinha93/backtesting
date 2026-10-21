"""Causality, symmetric market gates, daily reset and portfolio accounting."""
import json

import numpy as np
import pandas as pd
import pytest

from research import g6_nifty_risk_backtest as g6


@pytest.fixture
def config():
    return json.loads(g6.DEFAULT_CONFIG.read_text(encoding="utf-8"))


def nifty(sign=1):
    times = pd.date_range("2026-10-15 09:20", periods=4, freq="5min", tz="Asia/Kolkata")
    return pd.DataFrame(dict(ts=times, open=[100., 100.+sign*.1, 100.+sign*.2, 100.+sign*.3],
        close=[100.+sign*.1, 100.+sign*.2, 100.+sign*.3, 100.+sign*.4], quality_state="VALID"))


@pytest.mark.parametrize("side,sign", [("SHORT", 1), ("LONG", -1)])
def test_persistent_filter_is_symmetric_and_does_not_use_future_bar(side, sign, config):
    frame = nifty(sign)
    before = g6.features_at(frame, side, "2026-10-15 09:31+05:30", config)
    assert before["persistent_counter"]
    frame.loc[3, ["open", "close"]] = [1e9, 1.]
    assert before == g6.features_at(frame, side, "2026-10-15 09:31+05:30", config)
    opposite = "LONG" if side == "SHORT" else "SHORT"
    assert not g6.features_at(frame, opposite, "2026-10-15 09:31+05:30", config)["persistent_counter"]


def test_missing_previous_is_explicit_and_cannot_fabricate_persistence(config):
    frame = nifty().drop(index=1)
    result = g6.features_at(frame, "SHORT", "2026-10-15 09:31+05:30", config)
    assert result["previous_bar_status"] == "MISSING_PREVIOUS_BAR"
    assert not result["persistent_counter"]
    first = g6.features_at(frame, "SHORT", "2026-10-15 09:21+05:30", config)
    assert first["previous_bar_status"] == "BEFORE_SECOND_CANDLE"
    with pytest.raises(ValueError, match="Incomplete"):
        g6.features_at(frame.drop(index=2), "SHORT", "2026-10-15 09:31+05:30", config)


def test_flat_nifty_allows_both_sides(config):
    frame = nifty()
    frame[["open", "close"]] = 100.
    for side in ["LONG", "SHORT"]:
        result = g6.features_at(frame, side, "2026-10-15 09:31+05:30", config)
        assert not any(result[k] for k in ["slot_counter", "session_counter", "both_counter", "persistent_counter"])


def native():
    rows = []
    for sid, day, confirmation, entry, exit_clock, gross in [
        (1,"2026-10-15","09:30","09:31","10:00",-1.2),
        (2,"2026-10-15","10:00","10:01","10:03",2.),
        (3,"2026-10-15","10:05","10:06","10:10",-1.2),
        (4,"2026-10-16","09:30","09:31","10:00",1.),
    ]:
        rows.append(dict(sid=sid, day=day, tradingsymbol=str(sid), side="LONG", filled=True,
            confirmation_ts=pd.Timestamp(day+" "+confirmation, tz="Asia/Kolkata"),
            entry_ts=pd.Timestamp(day+" "+entry, tz="Asia/Kolkata"),
            exit_ts=pd.Timestamp(day+" "+exit_clock, tz="Asia/Kolkata"),
            gross_return_pct=gross, net_return_pct=gross-.05, cost_pct=.05,
            initial_stop_pct=1.2, picker="max_move", price_change_pct=1.,
            exit_reason="STOP" if gross < 0 else "TARGET", entry_price=100., exit_price=100.+gross))
    return pd.DataFrame(rows)


def features():
    return pd.DataFrame(dict(sid=[1,2,3,4], slot_counter=False, session_counter=False,
        both_counter=False, persistent_counter=False)).set_index("sid", drop=False)


BASE_CONFIG = dict(capital_per_entry_rs=100000., leverage=5., portfolio_capital_rs=1000000.)


def test_governor_waits_for_closed_trade_latches_then_resets_next_day(config):
    book, _, audit = g6.replay_variant(native(), features(), "daily_1r_half", BASE_CONFIG, config, 5.)
    # Exit exactly at 10:00 is not available to the 10:00 confirmation. A later
    # recovery does not erase the earlier -1R breach. Next day starts fresh.
    assert book.set_index("sid").g6_multiplier.to_dict() == {1:1., 2:1., 3:.5, 4:1.}
    third = audit.set_index("sid").loc[3]
    assert third.realised_before_rs > 0
    assert third.realised_floor_before_rs == pytest.approx(-6250.)
    assert book.set_index("sid").loc[3, "portfolio_net_profit_rupees"] == pytest.approx(-3125.)


def test_future_trade_outcome_cannot_change_earlier_governor(config):
    initial = native()
    _, _, before = g6.replay_variant(initial, features(), "daily_1r_half", BASE_CONFIG, config, 5.)
    initial.loc[initial.sid.eq(3), ["gross_return_pct", "net_return_pct"]] = -1e6
    _, _, after = g6.replay_variant(initial, features(), "daily_1r_half", BASE_CONFIG, config, 5.)
    pd.testing.assert_frame_equal(before, after)


def test_half_size_changes_capacity_costs_and_exposure():
    original = native().iloc[:2].copy()
    original.loc[0, "exit_ts"] = pd.Timestamp("2026-10-15 11:00+05:30")
    limited = {**BASE_CONFIG, "portfolio_capital_rs":100000.}
    full, _ = g6.allocate(original, pd.Series({1:1.,2:1.}), limited, 5., "control")
    half, _ = g6.allocate(original, pd.Series({1:.5,2:.5}), limited, 10., "uniform_half")
    assert full.portfolio_executed.tolist() == [True,False]
    assert half.portfolio_executed.all()
    np.testing.assert_allclose(half.portfolio_cost_rupees, [250.,250.])
    np.testing.assert_allclose(half.portfolio_trade_exposure_rupees, [250000.,250000.])


def test_combination_does_not_quarter_position(config):
    values = features()
    values.loc[3,"persistent_counter"] = True
    book, _, _ = g6.replay_variant(native(), values, "combined_persistent_daily_half", BASE_CONFIG, config, 5.)
    assert book.set_index("sid").loc[3,"g6_multiplier"] == .5


def test_two_r_halt_only_affects_later_new_orders(config):
    original = native()
    original.loc[original.sid.eq(2), "gross_return_pct"] = -1.2
    book, _, _ = g6.replay_variant(original, features(), "daily_2r_halt", BASE_CONFIG, config, 5.)
    assert book.set_index("sid").g6_multiplier.to_dict() == {1:1.,2:1.,3:0.,4:1.}
    assert book.set_index("sid").loc[3,"portfolio_net_profit_rupees"] == 0.


def test_skipped_trade_preserves_native_evidence_but_has_zero_portfolio_pnl():
    book, _ = g6.allocate(native(), pd.Series({1:0.,2:1.,3:1.,4:1.}), BASE_CONFIG, 5., "slot_candle")
    first = book.set_index("sid").loc[1]
    assert first.net_return_pct == pytest.approx(-1.25)
    assert not first.portfolio_executed
    assert first.portfolio_net_profit_rupees == first.portfolio_cost_rupees == 0.


def test_bootstrap_constant_delta_is_exact(config):
    result = g6.bootstrap_delta(np.full(54,-10.), config["bootstrap"])
    assert result["delta_lower_95_rs"] == result["delta_upper_95_rs"] == -540.


def test_screen_rejects_loss_reduction_bought_by_large_profit_sacrifice(config):
    control = dict(net_pnl_rs=100., loss_day_sum_abs_rs=20., losing_days=9, daily_close_drawdown_rs=10.)
    candidate = dict(net_pnl_rs=70., loss_day_sum_abs_rs=5., losing_days=5, daily_close_drawdown_rs=2.,
        original_winner_profit_retention_pct=70.)
    assert not g6.historical_screen(candidate, control, config["historical_screen"])
