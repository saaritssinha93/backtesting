"""Synthetic timing, availability, exit and portfolio tests for isolated G9."""
import numpy as np
import pandas as pd
import pytest

from research import g9_dual_market_backtest as backtest


SETTINGS = dict(stop_pct=.85, target_pct=1.10, risk_equity_pct=.25,
                top_k=3, max_positions=3, candidate_ttl_minutes=3,
                fill_delay_minutes=1, earliest_selection="09:30",
                new_entries_end="14:45", force_flat="15:15")
BASE = dict(portfolio_capital_rs=1_000_000., leverage=5.)


def stamp(clock):
    return pd.Timestamp("2026-10-22 " + clock, tz="Asia/Kolkata")


def candidate(sid=1, **overrides):
    value = dict(sid=sid, day="2026-10-22", setup_id="0931_LONG",
                 tradingsymbol=f"STOCK{sid}", side="LONG",
                 signal_ts=stamp("09:30"), confirmation_ts=stamp("09:31"),
                 confirmation_high=100.2, price_change_pct=.5,
                 oi_change_pct=1., oi=101_000., prev_oi=100_000.)
    value.update(overrides)
    return value


def prices(*, trigger=True):
    ends = pd.date_range(stamp("09:32"), stamp("15:15"), freq="min")
    path = dict(timestamp_ns=ends.as_unit("ns").asi8,
                open=np.full(len(ends), 100.), high=np.full(len(ends), 100.1),
                low=np.full(len(ends), 99.9), close=np.full(len(ends), 100.))
    if trigger:
        path["close"][0], path["high"][0] = 100.5, 100.6
    return path


def audit(rows, settings=SETTINGS, directions=None):
    frame = pd.DataFrame(rows)
    allowed = [True] * len(frame) if directions is None else directions
    nifty = pd.DataFrame(dict(sid=frame.sid, both_counter_allowed=allowed))
    return backtest.candidate_audit(frame, nifty, settings)


def native(sid=1, symbol=None, entry="09:33", exit_="09:45", **changes):
    row = dict(sid=sid, day="2026-10-22", tradingsymbol=symbol or f"STOCK{sid}",
               side="LONG", filled=True, entry_price=100., exit_price=99.15,
               entry_ts=stamp(entry), exit_ts=stamp(exit_))
    row.update(changes)
    return row


def test_candidate_gate_is_independent_of_future_execution_outcomes():
    frame = pd.DataFrame([candidate(1), candidate(2, side="SHORT"),
                          candidate(3, price_change_pct=-.1)])
    nifty = pd.DataFrame(dict(sid=[3, 1, 2], both_counter_allowed=[True, True, True]))
    expected = backtest.candidate_audit(frame, nifty, SETTINGS)
    frame["entry_price"], frame["exit_price"], frame["portfolio_net_profit_rupees"] = 1., 1e9, 1e12
    actual = backtest.candidate_audit(frame, nifty, SETTINGS)
    pd.testing.assert_frame_equal(actual, expected)
    assert actual.eligible.tolist() == [True, False, False]
    assert actual.loc[0, "candidate_available_at"] == str(stamp("09:31"))
    assert actual.loc[0, "candidate_expires_at"] == str(stamp("09:33"))
    assert actual.full_model_status.str.startswith("UNAVAILABLE").all()


@pytest.mark.parametrize("field", ["price_change_pct", "oi_change_pct", "oi", "prev_oi"])
@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf])
def test_nonfinite_signal_evidence_cannot_qualify(field, bad):
    result = audit([candidate(**{field: bad})])
    assert not result.loc[0, "eligible"]
    assert not result.loc[0, "top_k_allowed"]


def test_numeric_oi_rank_and_sid_aligned_csv_direction_gate():
    rows = [candidate(1, oi_change_pct="2"), candidate(2, oi_change_pct="10"),
            candidate(3, oi_change_pct="3")]
    result = audit(rows, {**SETTINGS, "top_k": 1})
    assert result.loc[result.top_k_allowed, "sid"].tolist() == [2]
    frame = pd.DataFrame(rows)
    nifty = pd.DataFrame(dict(sid=[3, 1, 2], both_counter_allowed=["False", "True", "True"]))
    gated = backtest.candidate_audit(frame, nifty, SETTINGS).set_index("sid")
    assert not gated.loc[3, "g7_direction_allowed"]
    assert gated.loc[1, "g7_direction_allowed"]


def test_futures_gate_reranks_before_top_k_and_keeps_reasons_consistent():
    initial = audit([candidate(sid, oi_change_pct=float(5-sid)) for sid in range(1, 5)])
    context = pd.DataFrame(dict(sid=[4, 2, 1, 3],
                                futures_context_available=[True, True, False, True],
                                futures_ret_5m=[.01, .01, .01, .01]))
    result = backtest.add_futures_gate(initial, context, SETTINGS).set_index("sid")
    assert set(result.index[result.top_k_allowed]) == {2, 3, 4}
    assert result.loc[4, "oi_rank"] == 3
    assert result.loc[4, "rejection_reason"].startswith("ELIGIBLE")
    assert not result.loc[1, "eligible_dual_price"]


@pytest.mark.parametrize("value", [np.nan, np.inf, -np.inf, 0., -.01])
def test_missing_nonfinite_or_nonpositive_futures_price_cannot_qualify(value):
    initial = audit([candidate()])
    context = pd.DataFrame(dict(sid=[1], futures_context_available=[True], futures_ret_5m=[value]))
    result = backtest.add_futures_gate(initial, context, SETTINGS)
    assert not result.loc[0, "top_k_allowed"]


def test_futures_gate_rejects_incomplete_or_duplicate_context():
    initial = audit([candidate(1), candidate(2)])
    context = pd.DataFrame(dict(sid=[1], futures_context_available=[True], futures_ret_5m=[.01]))
    with pytest.raises(ValueError, match="Incomplete futures context"):
        backtest.add_futures_gate(initial, context, SETTINGS)
    with pytest.raises(pd.errors.MergeError):
        backtest.add_futures_gate(initial, pd.concat([context, context]), SETTINGS)


def test_e1_uses_previous_high_and_delayed_future_open_at_exact_ttl():
    path = prices()
    # Trigger close is below its OWN high: using that high would suppress entry.
    result = backtest.e1_trade(candidate(), path, SETTINGS)
    assert result["filled"]
    assert result["trigger_ts"] == stamp("09:32")
    assert result["entry_ts"] == stamp("09:33")
    assert result["entry_path_index"] == 2
    assert result["entry_price"] == 100.
    assert result["entry_ts"] > result["trigger_ts"]
    assert result["exit_ts"] == stamp("15:15")
    assert result["exit_reason"] == "FORCE_FLAT_1515"


def test_changes_after_entry_cannot_change_trigger_or_fill():
    first = backtest.e1_trade(candidate(), prices(), SETTINGS)
    changed = prices()
    changed["high"][10] = 110.
    second = backtest.e1_trade(candidate(), changed, SETTINGS)
    for key in ("filled", "trigger_ts", "entry_ts", "entry_path_index", "entry_price"):
        assert first[key] == second[key]
    assert first["exit_reason"] != second["exit_reason"]


@pytest.mark.parametrize("settings", [
    {**SETTINGS, "candidate_ttl_minutes": 2},
    {**SETTINGS, "fill_delay_minutes": 2},
    {**SETTINGS, "new_entries_end": "09:32"},
])
def test_trigger_cannot_fill_after_ttl_or_latest_entry(settings):
    result = backtest.e1_trade(candidate(), prices(), settings)
    assert not result["filled"]
    assert result["exit_reason"] == "TRIGGER_BUT_FILL_EXPIRED"


def test_later_breakout_is_not_backdated_into_ttl():
    path = prices(trigger=False)
    path["close"][1], path["high"][1] = 100.5, 100.6
    result = backtest.e1_trade(candidate(), path, SETTINGS)
    assert not result["filled"]
    assert result["exit_reason"] == "TRIGGER_BUT_FILL_EXPIRED"


def test_same_entry_candle_stop_and_target_touch_uses_stop():
    path = prices()
    path["low"][2], path["high"][2] = 98., 102.
    result = backtest.e1_trade(candidate(), path, SETTINGS)
    assert result["exit_reason"] == "STOP"
    assert result["exit_price"] == pytest.approx(99.15)
    assert result["exit_path_index"] == result["entry_path_index"] == 2
    assert result["exit_ts"] > result["entry_ts"]


def test_later_gap_stop_uses_open_and_target_open_precedes_later_low():
    path = prices()
    path["open"][3], path["low"][3] = 98., 97.
    gap = backtest.e1_trade(candidate(), path, SETTINGS)
    assert gap["exit_reason"] == "STOP_GAP"
    assert gap["exit_price"] == 98.
    path = prices()
    path["open"][3], path["high"][3], path["low"][3] = 102., 103., 98.
    target = backtest.e1_trade(candidate(), path, SETTINGS)
    assert target["exit_reason"] == "TARGET"
    assert target["exit_price"] == pytest.approx(101.1)


def test_stop_on_forced_exit_candle_precedes_close_squareoff():
    path = prices()
    path["low"][-1] = 99.
    result = backtest.e1_trade(candidate(), path, SETTINGS)
    assert result["exit_ts"] == stamp("15:15")
    assert result["exit_reason"] == "STOP"


@pytest.mark.parametrize("problem", ["missing_minute", "missing_squareoff", "invalid_ohlc"])
def test_incomplete_or_invalid_execution_path_rejects(problem):
    path = prices()
    if problem == "missing_minute":
        path = {key: np.delete(value, 5) for key, value in path.items()}
    elif problem == "missing_squareoff":
        path = {key: value[:-1] for key, value in path.items()}
    else:
        path["low"][0] = 101.
    with pytest.raises(ValueError):
        backtest.e1_trade(candidate(), path, SETTINGS)


def test_whole_share_sizing_respects_risk_including_modeled_cost():
    original = pd.DataFrame([native()])
    snapshot = original.copy(deep=True)
    result = backtest.integer_risk_book(original, BASE, SETTINGS, 5., "test")
    row = result.iloc[0]
    expected_qty = int(np.floor(2500. / (100.*(.0085+.0005))))
    assert row.portfolio_executed and row.quantity == expected_qty
    assert row.quantity * 100. * (.0085+.0005) <= row.risk_budget_rupees == 2500.
    assert -row.portfolio_net_profit_rupees <= 2500. + 1e-8
    assert row.portfolio_net_profit_rupees == pytest.approx(
        row.portfolio_gross_profit_rupees-row.portfolio_cost_rupees)
    pd.testing.assert_frame_equal(original, snapshot)


def test_capacity_and_symbol_caps_are_replayed_chronologically():
    # The shuffled frame must allocate by timestamp/SID, never input order.
    rows = [native(4), native(2), native(1), native(3)]
    result = backtest.integer_risk_book(pd.DataFrame(rows), BASE, SETTINGS, 5., "test").set_index("sid")
    assert set(result.index[result.portfolio_executed]) == {1, 2, 3}
    assert result.loc[4, "portfolio_reject_reason"] == "MAX_POSITIONS"
    same = pd.DataFrame([native(1, symbol="SAME"), native(2, symbol="SAME")])
    result = backtest.integer_risk_book(same, BASE, SETTINGS, 5., "test")
    assert result.portfolio_executed.tolist() == [True, False]
    assert result.iloc[1].portfolio_reject_reason == "SYMBOL_ALREADY_OPEN"


def test_exit_releases_capacity_after_same_boundary_and_risk_does_not_compound():
    rows = [native(1, exit_="09:40", exit_price=110.),
            native(2, entry="09:40", exit_="09:50"),
            native(3, entry="09:41", exit_="09:50")]
    result = backtest.integer_risk_book(pd.DataFrame(rows), BASE,
                                      {**SETTINGS, "max_positions": 1}, 5., "test")
    assert result.portfolio_executed.tolist() == [True, False, True]
    assert result.iloc[0].quantity == result.iloc[2].quantity
    assert result.iloc[0].risk_budget_rupees == result.iloc[2].risk_budget_rupees == 2500.


def test_risk_sizing_cannot_use_more_than_available_leveraged_capital():
    result = backtest.integer_risk_book(pd.DataFrame([native()]),
                                      {**BASE, "portfolio_capital_rs": 1000.},
                                      {**SETTINGS, "risk_equity_pct": 50.}, 5., "test")
    row = result.iloc[0]
    assert row.quantity == 50
    assert row.portfolio_capital_rupees <= 1000.
    assert row.portfolio_exposure_rupees <= 5000.


def test_rejected_candidates_never_read_execution_paths():
    rows = pd.DataFrame([candidate(side="SHORT")])
    gate = audit(rows.to_dict("records"))
    result = backtest.replay_e1(rows, {}, gate, SETTINGS)
    assert not result.iloc[0].filled
    assert result.iloc[0].exit_reason == "CANDIDATE_REJECTED"
