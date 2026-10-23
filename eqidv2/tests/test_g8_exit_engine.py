import numpy as np
import pandas as pd
import pytest

from research import g8_exit_engine as engine
from research import g4_bounded_exit_backtest as bounded


CONFIG = dict(initial_stop_pct=1.2, tightened_stop_pct=1., tighten_after_minutes=120,
              capital_per_entry_rs=100_000., leverage=5., portfolio_capital_rs=1_000_000.,
              model_cost_bps_round_trip=5.)


def path():
    stamps = pd.date_range("2026-10-01 10:01", "2026-10-01 15:15", freq="min", tz="Asia/Kolkata")
    return dict(timestamp_ns=stamps.as_unit("ns").asi8,
                open=np.full(len(stamps), 100.), high=np.full(len(stamps), 100.1),
                low=np.full(len(stamps), 99.9), close=np.full(len(stamps), 100.))


def order(sid, *, entry=0, side="LONG", filled=True, target=.6):
    return dict(sid=sid, setup_id="1000_"+side, day="2026-10-01", tradingsymbol=f"STOCK{sid}",
                side=side, filled=filled, entry_price=100., entry_path_index=entry,
                entry_ts=pd.Timestamp("2026-10-01 10:01", tz="Asia/Kolkata")+pd.Timedelta(minutes=entry),
                confirmation_ts=pd.Timestamp("2026-10-01 10:00", tz="Asia/Kolkata"),
                exit_ts=pd.Timestamp("2026-10-01 15:15", tz="Asia/Kolkata"),
                gross_return_pct=0., net_return_pct=-.05, cost_pct=.05,
                native_target_pct=target, initial_stop_pct=1.2, picker="max_liquidity",
                traded_value=100_000., strategy_fingerprint="source-fingerprint")


def test_recreates_native_single_exit_baseline_and_does_not_mutate_input():
    long_path, short_path = path(), path()
    long_path["high"][2] = 100.7
    short_path["high"][1] = 101.3
    source = pd.DataFrame([order(11), order(22, side="SHORT"), order(33, filled=False)])
    original = source.copy(deep=True)
    paths = {11: long_path, 22: short_path}
    expected = bounded.bounded_ledger(source, paths, CONFIG)
    raw = engine.replay_exits(source, paths, 1.2, .6, CONFIG)
    actual = engine.allocate_variant(raw, pd.Series({33: True, 22: True, 11: True}), CONFIG, "baseline")
    for field in ("entry_ts", "entry_path_index", "filled", "exit_ts", "exit_price", "exit_reason",
                  "portfolio_executed", "portfolio_net_profit_rupees", "portfolio_cost_rupees"):
        pd.testing.assert_series_equal(actual[field], expected[field], check_names=False)
    pd.testing.assert_frame_equal(source, original)
    assert not any(name.startswith("portfolio_") for name in raw.columns)


def test_earlier_exit_releases_capital_for_later_frozen_order():
    first, second = path(), path()
    first["high"][1] = 100.3
    second["high"][3] = 100.3
    source = pd.DataFrame([order(1), order(2, entry=2)])
    config = {**CONFIG, "portfolio_capital_rs": 100_000.}
    allowed = pd.Series({1: True, 2: True})
    slow = engine.allocate_variant(engine.replay_exits(source, {1:first, 2:second}, 1.2, .6, config),
                                   allowed, config, "slow")
    faster = engine.allocate_variant(engine.replay_exits(source, {1:first, 2:second}, 1.2, .2, config),
                                     allowed, config, "faster")
    assert slow.portfolio_executed.tolist() == [True, False]
    assert faster.portfolio_executed.tolist() == [True, True]
    assert faster.portfolio_net_profit_rupees.sum() == pytest.approx(1500.)


def test_sid_alignment_and_csv_false_never_becomes_true():
    prices = path()
    prices["high"][1] = 100.8
    source = pd.DataFrame([order(10), order(20)])
    source.index = [3, 8]
    source["filled"] = ["True", "False"]
    raw = engine.replay_exits(source, {10:prices}, pd.Series({20:1.2, 10:1.0}),
                              pd.Series({20:.7, 10:.6}), CONFIG)
    result = engine.allocate_variant(raw, pd.Series({20:True, 10:False}), CONFIG, "veto")
    assert result.portfolio_executed.tolist() == [False, False]
    assert result.loc[3, "portfolio_status"] == "G8_REGIME_BLOCKED"
    assert result.loc[8, "portfolio_status"] == "SOURCE_UNFILLED"
    assert raw.target_pct.tolist() == [.6, .7]
    with pytest.raises(ValueError, match="exactly one"):
        engine.allocate_variant(raw, pd.Series({10:True}), CONFIG, "missing")


def test_scheduled_tightening_starts_only_at_120_minute_bar_open():
    prices = path()
    prices["low"][120:122] = 98.9
    raw = engine.replay_exits(pd.DataFrame([order(1)]), {1:prices}, 1.2, 2., CONFIG)
    assert raw.loc[0, "exit_path_index"] == 121
    assert raw.loc[0, "exit_reason"] == "TIGHTENED_STOP"
    assert raw.loc[0, "exit_price"] == pytest.approx(99.)


def test_short_gap_stop_uses_observed_open():
    prices = path()
    prices["open"][1], prices["high"][1], prices["low"][1], prices["close"][1] = 102., 102.2, 101.9, 102.1
    raw = engine.replay_exits(pd.DataFrame([order(1, side="SHORT")]), {1:prices}, 1.2, .6, CONFIG)
    assert raw.loc[0, "exit_price"] == 102.
    assert raw.loc[0, "exit_event"] == "OPEN"
    assert raw.loc[0, "gross_return_pct"] == pytest.approx(-2.)
    assert raw.loc[0, "exit_gap_through"]


def test_risk_sizing_never_exceeds_baseline_and_scales_cost_once():
    prices = path()
    prices["high"][1] = 100.8
    source = pd.DataFrame([order(1), order(2)])
    raw = engine.replay_exits(source, {1:prices, 2:prices}, pd.Series({1:2.4, 2:.6}), .6,
                              CONFIG, risk_scale=True)
    result = engine.allocate_variant(raw, pd.Series({1:True, 2:True}), CONFIG, "sized")
    assert result.g8_quantity_scale.tolist() == [.5, 1.]
    assert result.exposure_per_entry_rupees.tolist() == [250_000., 500_000.]
    assert result.portfolio_cost_rupees.tolist() == [125., 250.]
    assert result.portfolio_net_profit_rupees.tolist() == pytest.approx([1375., 2750.])


def test_tiny_target_is_losing_after_costs_and_zero_trade_days_count():
    prices = path()
    source = pd.DataFrame([order(1)])
    result = engine.allocate_variant(engine.replay_exits(source, {1:prices}, 1.2, .02, CONFIG),
                                     pd.Series({1:True}), CONFIG, "tiny")
    stats = engine.metrics(result, ["2026-10-01", "2026-10-02"])
    assert stats["trades"] == 1
    assert stats["target_exits"] == 1
    assert stats["wins"] == 0 and stats["losses"] == 1
    assert stats["net_pnl_rs"] == pytest.approx(-150.)
    assert stats["cost_rs"] == 250.
    assert stats["losing_days"] == 1 and stats["flat_days"] == 1
    assert stats["negative_trade_pnl_rs"] == pytest.approx(-150.)
    assert stats["max_drawdown_rs"] == pytest.approx(150.)
