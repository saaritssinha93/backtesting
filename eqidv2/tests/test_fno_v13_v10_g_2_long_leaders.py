"""Synthetic causality and order-budget checks for the LONG leader experiment."""
from datetime import date

import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_g_2_long_leaders_backtest as leaders


SESSION = date(2026, 10, 5)


def feature(symbol, clock="09:25", gain=2.0, passes=True):
    return dict(
        tradingsymbol=symbol,
        signal_ts=pd.Timestamp(f"{SESSION} {clock}", tz="Asia/Kolkata"),
        day_gain_pct=gain,
        signal_rule_pass=passes,
    )


def test_causal_top_ten_is_ranked_before_entry_gates():
    rows = [feature(f"LEADER{i:02}", gain=20-i, passes=False) for i in range(10)]
    rows.append(feature("ELEVENTH", gain=1.0, passes=True))
    selected, audit = leaders.select_orders(pd.DataFrame(rows), leaders.MODES[1], [])
    assert selected.empty
    last = audit.loc[audit.tradingsymbol.eq("ELEVENTH")].iloc[0]
    assert last.asof_rank == 11
    assert not last.universe_pass
    assert last.selection_reason == "OUTSIDE_WATCHLIST_OR_ASOF_TOP10"
    assert audit.iloc[:10].selection_reason.eq("ENTRY_RULES_FAILED").all()


def test_tied_ranks_and_order_selection_use_symbol_not_input_order():
    rows = pd.DataFrame([feature("ZZZ"), feature("AAA"), feature("MMM")])
    chosen, audit = leaders.select_orders(rows, leaders.MODES[1], [])
    reverse_chosen, reverse_audit = leaders.select_orders(rows.iloc[::-1], leaders.MODES[1], [])
    assert chosen.tradingsymbol.tolist() == ["AAA"]
    assert audit.tradingsymbol.tolist() == ["AAA", "MMM", "ZZZ"]
    assert audit.asof_rank.tolist() == [1, 2, 3]
    assert audit.selection_reason.tolist() == ["SELECTED", "LOWER_RANK_THIS_SLOT", "LOWER_RANK_THIS_SLOT"]
    pd.testing.assert_frame_equal(chosen, reverse_chosen)
    pd.testing.assert_frame_equal(audit, reverse_audit)


def test_three_submitted_orders_one_per_slot_no_symbol_repeat_without_fill_knowledge():
    rows = pd.DataFrame([
        feature("AAA", "09:25", 5), feature("BBB", "09:25", 4),
        feature("AAA", "09:30", 6), feature("BBB", "09:30", 3),
        feature("AAA", "09:35", 7), feature("CCC", "09:35", 4),
        feature("DDD", "09:40", 8), feature("EEE", "09:45", 9),
    ])
    selected, audit = leaders.select_orders(rows, leaders.MODES[1], [])
    assert selected.tradingsymbol.tolist() == ["AAA", "BBB", "CCC"]
    assert selected.groupby("signal_ts").size().max() == 1
    assert selected.tradingsymbol.is_unique
    assert audit.loc[audit.tradingsymbol.eq("AAA")].selection_reason.tolist() == [
        "SELECTED", "SYMBOL_ALREADY_ORDERED", "SYMBOL_ALREADY_ORDERED",
    ]
    assert audit.loc[audit.tradingsymbol.isin(["DDD", "EEE"])].selection_reason.eq("DAILY_ORDER_CAP").all()
    # Even an extra (non-input) fill column cannot free a slot or allow re-entry.
    unfilled, _ = leaders.select_orders(rows.assign(filled=False), leaders.MODES[1], [])
    filled, _ = leaders.select_orders(rows.assign(filled=True), leaders.MODES[1], [])
    assert unfilled.tradingsymbol.tolist() == filled.tradingsymbol.tolist() == ["AAA", "BBB", "CCC"]


def test_hindsight_mode_restricts_to_fixed_watchlist_not_asof_top_ten():
    rows = [feature(f"OUTSIDE{i:02}", gain=20-i) for i in range(10)]
    rows.append(feature("WATCHED", gain=1.5))
    selected, audit = leaders.select_orders(pd.DataFrame(rows), leaders.MODES[0], ["WATCHED"])
    assert selected.tradingsymbol.tolist() == ["WATCHED"]
    assert selected.iloc[0].asof_rank == 11
    assert audit.iloc[:10].universe_pass.eq(False).all()
    assert selected.iloc[0].experiment == leaders.MODES[0]


def test_unknown_experiment_is_rejected():
    with pytest.raises(ValueError, match="Unknown experiment"):
        leaders.select_orders(pd.DataFrame([feature("AAA")]), "LIVE", [])


def minute_frame():
    previous = pd.date_range("2026-10-01 09:16", periods=120, freq="min", tz="Asia/Kolkata")
    current = pd.date_range("2026-10-05 09:16", periods=66, freq="min", tz="Asia/Kolkata")
    close = np.concatenate([np.linspace(99, 100, len(previous)), np.linspace(101, 103, len(current))])
    return pd.DataFrame(dict(
        ts=previous.append(current), open=close-0.02, high=close+0.01,
        low=close-0.03, close=close, volume=np.arange(len(close), dtype=float)+1000,
        Prev_Day_Close=100.0,
    ))


def slots():
    return {
        "09:25": {"setup_id": "0926_LONG", "target_pct": 0.97},
        "09:30": {"setup_id": "0931_LONG", "target_pct": 2.00},
        "09:45": {"setup_id": "0946_LONG", "target_pct": 1.73},
        "10:00": {"setup_id": "1001_LONG", "target_pct": 2.50},
    }


@pytest.mark.parametrize("clock", ["09:26", "09:31", "09:46", "10:01"])
def test_feature_prefix_is_independent_of_later_bars(clock):
    frame = minute_frame()
    cutoff = pd.Timestamp(f"{SESSION} {clock}", tz="Asia/Kolkata")
    full, _ = leaders.symbol_features(frame, SESSION, "AAA", slots())
    prefix, _ = leaders.symbol_features(frame.loc[frame.ts.le(cutoff)].copy(), SESSION, "AAA", slots())
    expected = full.loc[full.confirmation_ts.le(cutoff)].reset_index(drop=True)
    assert len(prefix) > 0
    pd.testing.assert_frame_equal(prefix, expected)
    # A dramatic later reversal must not leak through VWAP, EMA, ranking gain,
    # volume, the opening range, or the already-observed confirmation candle.
    altered = frame.copy()
    later = altered.ts.gt(cutoff)
    altered.loc[later, ["open", "high", "low", "close"]] *= 0.1
    altered.loc[later, "volume"] *= 10000
    changed, _ = leaders.symbol_features(altered, SESSION, "AAA", slots())
    pd.testing.assert_frame_equal(changed.loc[changed.confirmation_ts.le(cutoff)].reset_index(drop=True), expected)


def test_features_keep_native_targets_and_g2_initial_stop():
    features, _ = leaders.symbol_features(minute_frame(), SESSION, "AAA", slots())
    assert features.native_stop_pct.eq(leaders.g2.INITIAL_STOP_PCT).all()
    assert features.native_target_pct.tolist() == [0.97, 2.00, 1.73, 2.50]
    assert features.side.eq("LONG").all()
    assert features.confirmation_ts.sub(features.signal_ts).eq(pd.Timedelta(minutes=1)).all()


def test_valid_source_bars_are_accepted():
    leaders.validate_bars(minute_frame())


@pytest.mark.parametrize("case", ["duplicate", "missing_timestamp", "nan_price", "infinite_price",
                                  "zero_price", "negative_volume", "high_below_close", "low_above_open"])
def test_invalid_source_bars_fail_closed(case):
    frame = minute_frame()
    if case == "duplicate":
        frame.loc[1, "ts"] = frame.loc[0, "ts"]
    elif case == "missing_timestamp":
        frame.loc[0, "ts"] = pd.NaT
    elif case == "nan_price":
        frame.loc[0, "close"] = np.nan
    elif case == "infinite_price":
        frame.loc[0, "high"] = np.inf
    elif case == "zero_price":
        frame.loc[0, "low"] = 0
    elif case == "negative_volume":
        frame.loc[0, "volume"] = -1
    elif case == "high_below_close":
        frame.loc[0, "high"] = frame.loc[0, "close"] - 1
    else:
        frame.loc[0, "low"] = frame.loc[0, "open"] + 1
    with pytest.raises(ValueError, match="Invalid|Inconsistent"):
        leaders.validate_bars(frame)


@pytest.mark.parametrize("flag", ["gap_filled", "opening_snapshot", "provisional_stale"])
@pytest.mark.parametrize("value", [True, 1, "true", "yes", "on", " true ", " YES ", " On "])
def test_flagged_source_bars_fail_closed(flag, value):
    frame = minute_frame()
    frame[flag] = value
    with pytest.raises(ValueError, match="Flagged source bars"):
        leaders.validate_bars(frame)


def test_protocol_inherits_g2_stop_schedule_and_source_long_targets():
    active_long = [setup for setup in leaders.source_config.ACTIVE_SETUPS if setup.side == "LONG"]
    source = dict(
        exit={"setups": {setup.setup_id: {"target_pct": setup.target_pct} for setup in active_long}},
        cost_bps=5, capital_per_entry_rupees=100000, leverage_factor=5,
        portfolio_capital_rupees=1000000,
    )
    protocol = leaders.protocol(source, SESSION)
    assert protocol["initial_stop_pct"] == leaders.g2.INITIAL_STOP_PCT == 1.25
    assert protocol["tightened_stop_pct"] == leaders.g2.TIGHTENED_STOP_PCT == 1.0
    assert protocol["tighten_after_minutes"] == leaders.g2.TIGHTEN_AFTER_MINUTES == 120
    assert protocol["execution_authority"] is False
    assert protocol["square_off"] == "15:15"
    assert set(protocol["long_slots"]) == {setup.signal_end for setup in active_long}
    for spec in protocol["long_slots"].values():
        assert spec["target_pct"] == source["exit"]["setups"][spec["setup_id"]]["target_pct"]
