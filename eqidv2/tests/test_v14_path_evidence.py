"""Observed-prefix proofs must agree with the unchanged full-path simulator."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from v14 import baseline
from v14._baseline import execution
from v14.path_evidence import assess

ARCHIVE = Path(r"C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g_3/frozen_20261008_long110_nextminute_v1")


def _row(side="LONG", **changes):
    return dict(sid=1, day="2026-08-03", tradingsymbol="CASH", side=side,
                confirmation_ts=pd.Timestamp("2026-08-03 09:26", tz=baseline.IST),
                setup_id=f"0926_{side}", trigger=100., **changes)


def _path(row=None):
    row = _row() if row is None else row
    confirmation = pd.Timestamp(row["confirmation_ts"])
    times = pd.date_range(confirmation + pd.Timedelta(minutes=1),
                          confirmation.normalize() + pd.Timedelta(hours=15, minutes=15), freq="min")
    return dict(timestamp_ns=times.as_unit("ns").asi8,
                open=np.full(len(times), 100.), high=np.full(len(times), 100.1),
                low=np.full(len(times), 99.9), close=np.full(len(times), 100.))


def _take(path, n):
    return {key: value[:n].copy() for key, value in path.items()}


def _drop(path, position):
    return {key: np.delete(value, position) for key, value in path.items()}


def _run(row, path):
    config = baseline.frozen_config()
    order = pd.DataFrame([row])
    order["native_stop_pct"] = 1.25
    order["native_target_pct"] = config["exit"]["setups"].get(row["setup_id"], config["exit"]["default"])["target_pct"]
    return execution.simulate_staged(order, {int(row["sid"]): path}, cost_bps=config["cost_bps"],
                                     max_entry_delay_minutes=config["entry_expiry_minutes"]).iloc[0]


def _assert_parity(row, complete, evidence):
    assert evidence["resolved"]
    full, prefix = _run(row, complete), _run(row, evidence["path"])
    assert bool(prefix.filled) == bool(full.filled)
    assert prefix.exit_reason == full.exit_reason
    if full.filled:
        for field in ("entry_price", "entry_ts", "exit_price", "exit_ts", "exit_path_index", "exit_event",
                      "net_return_pct", "gross_return_pct", "mfe_pct", "mae_pct", "same_bar_ambiguous",
                      "holding_minutes", "entry_gap_through", "exit_gap_through", "active_stop_pct_at_exit"):
            assert prefix[field] == full[field], field
    return prefix


@pytest.mark.parametrize("side,event", [("LONG", "TARGET"), ("SHORT", "TARGET"), ("LONG", "STOP"), ("SHORT", "STOP")])
def test_observed_terminal_event_resolves_both_sides_before_a_later_gap(side, event):
    row, path = _row(side), _path(_row(side))
    if event == "TARGET":
        path["high" if side == "LONG" else "low"][3] = 104. if side == "LONG" else 96.
    else:
        path["low" if side == "LONG" else "high"][3] = 98. if side == "LONG" else 102.
    observed = _drop(path, 5)
    result = assess(row, observed)
    assert result["reason"] == f"RESOLVED_{event}"
    assert len(result["path"]["close"]) == 4
    assert not result["evidence"]["synthetic_bars"]
    _assert_parity(row, path, result)


def test_complete_ten_minute_unfilled_window_is_final_but_nine_minutes_is_unknown():
    row, path = _row(), _path()
    path["open"][:] = 99.
    path["high"][:] = 99.5
    path["low"][:] = 98.9
    path["close"][:] = 99.
    path["high"][10] = 103.  # The 11th minute is outside the native entry window.
    result = assess(row, _take(path, 10))
    assert result["reason"] == "RESOLVED_UNFILLED_ENTRY_WINDOW"
    assert len(result["path"]["close"]) == 10
    assert not _assert_parity(row, path, result).filled
    partial = assess(row, _take(path, 9))
    assert not partial["resolved"]
    assert partial["reason"] == "ENTRY_WINDOW_INCOMPLETE"


def test_entry_on_tenth_minute_still_requires_an_observed_exit():
    row, path = _row(), _path()
    for key, value in (("open", 99.), ("high", 99.5), ("low", 98.9), ("close", 99.)):
        path[key][:9] = value
    unresolved = assess(row, _take(path, 10))
    assert not unresolved["resolved"]
    assert unresolved["reason"] == "POSITION_OPEN_AT_FIRST_INTERRUPTION"
    path["high"][10] = 102.
    result = assess(row, _take(path, 11))
    assert result["evidence"]["entry"]["index"] == 9
    _assert_parity(row, path, result)


def test_gap_before_entry_or_exit_cannot_be_repaired_using_later_terminal_price():
    row, path = _row(), _path()
    path["high"][8] = 104.
    assert not assess(row, _drop(path, 0))["resolved"]
    result = assess(row, _drop(path, 4))
    assert not result["resolved"]
    assert result["reason"] == "POSITION_OPEN_AT_FIRST_INTERRUPTION"
    assert len(result["path"]["close"]) == 4


def test_truncated_helper_time_exit_is_never_treated_as_real_square_off():
    row, path = _row(), _path()
    assert _run(row, _take(path, 30)).exit_reason == "TIME_EXIT_1515"
    assert not assess(row, _take(path, 30))["resolved"]
    complete = assess(row, path)
    assert complete["reason"] == "RESOLVED_TIME_EXIT_1515"
    assert complete["evidence"]["full_session_observed"]
    _assert_parity(row, path, complete)


def test_native_stop_first_and_adverse_gap_open_are_preserved():
    row, path = _row(), _path()
    path["high"][0], path["low"][0] = 104., 98.
    prefix = _assert_parity(row, path, assess(row, _take(path, 1)))
    assert prefix.exit_reason == "STOP" and prefix.same_bar_ambiguous
    assert prefix.exit_price == 98.75
    path = _path()
    path["open"][2], path["high"][2], path["low"][2], path["close"][2] = 98., 99., 97.9, 98.5
    proof = assess(row, _take(path, 3))
    prefix = _assert_parity(row, path, proof)
    assert prefix.exit_price == 98. and prefix.exit_event == "OPEN"
    assert pd.Timestamp(proof["evidence"]["proof_available_ts"]) == prefix.exit_ts + pd.Timedelta(minutes=1)


def test_tightening_uses_exact_original_bar_open_timing():
    row, path = _row(), _path()
    path["low"][120:122] = 98.9
    assert not assess(row, _take(path, 121))["resolved"]
    proof = assess(row, _take(path, 122))
    prefix = _assert_parity(row, path, proof)
    assert prefix.exit_reason == "TIGHTENED_STOP"
    assert prefix.exit_path_index == 121 and prefix.exit_price == 99.


def test_invalid_bar_before_exit_is_unknown_while_invalid_tail_is_irrelevant():
    row, path = _row(), _path()
    path["high"][3] = 104.
    invalid = _take(path, 8)
    invalid["close"][2] = np.nan
    assert not assess(row, invalid)["resolved"]
    invalid = _take(path, 8)
    invalid["close"][6] = np.nan
    proof = assess(row, invalid)
    _assert_parity(row, path, proof)
    before = {key: value.copy() for key, value in invalid.items()}
    proof["path"]["close"][0] = 900.
    for key in invalid:
        np.testing.assert_equal(invalid[key], before[key])


def test_conflicting_duplicate_of_a_required_bar_prevents_resolution():
    row, path = _row(), _path()
    path["high"][0] = 104.
    repeated = {key: np.r_[value[:3], value[0]] for key, value in path.items()}
    result = assess(row, repeated)
    assert not result["resolved"]
    assert result["reason"] == "AMBIGUOUS_REQUIRED_TIMESTAMP"
    later_duplicate = {key: np.r_[value[:3], value[2]] for key, value in path.items()}
    assert assess(row, later_duplicate)["resolved"]


def test_malformed_path_and_order_fail_closed_without_mutation():
    row, path = _row(), _path()
    missing = dict(path)
    missing.pop("low")
    assert assess(row, missing)["reason"] == "MISSING_PATH_FIELDS"
    mismatch = dict(path)
    mismatch["close"] = mismatch["close"][:-1]
    assert assess(row, mismatch)["reason"] == "MISMATCHED_PATH_ARRAY_LENGTHS"
    assert not assess({**row, "side": "NONE"}, path)["resolved"]
    assert not assess({**row, "trigger": np.nan}, path)["resolved"]
    assert not assess(row, {key: value[:0] for key, value in path.items()})["resolved"]


def test_prefix_adapter_is_explicit_and_rejects_unresolved_orders():
    row, path = _row(), _path()
    row.update(picker="max_liquidity", traded_value=1e7, price_change_pct=.5, volume_ratio=2.)
    orders = pd.DataFrame([row])
    path["high"][2] = 104.
    partial = _take(path, 3)
    with pytest.raises(RuntimeError, match="Non-continuous or mistimed"):
        baseline.simulate(orders, {1: partial})
    expected = baseline.simulate(orders, {1: path})
    actual = baseline.simulate(orders, {1: partial}, allow_observed_prefix=True)
    pd.testing.assert_frame_equal(actual, expected)
    with pytest.raises(ValueError, match="Unresolved cash execution path"):
        baseline.simulate(orders, {1: _take(path, 2)}, allow_observed_prefix=True)


def test_prefix_adapter_preserves_unfilled_and_losing_trade_financials():
    row, path = _row(), _path()
    row.update(picker="max_liquidity", traded_value=1e7, price_change_pct=.5, volume_ratio=2.)
    orders = pd.DataFrame([row])
    for key, value in (("open", 99.), ("high", 99.5), ("low", 98.9), ("close", 99.)):
        path[key][:] = value
    expected = baseline.simulate(orders, {1: path})
    actual = baseline.simulate(orders, {1: _take(path, 10)}, allow_observed_prefix=True)
    pd.testing.assert_frame_equal(actual, expected)
    path = _path()
    path["low"][0] = 98.
    actual = baseline.simulate(orders, {1: _take(path, 1)}, allow_observed_prefix=True)
    expected = baseline.simulate(orders, {1: path})
    pd.testing.assert_frame_equal(actual, expected)
    assert actual.portfolio_net_profit_rupees.iloc[0] == pytest.approx(-6500.)


@pytest.mark.skipif(not ARCHIVE.exists(), reason="Local sealed G-3 archive unavailable")
def test_all_93_archived_executions_have_identical_minimal_prefix_outcomes():
    trades = pd.read_csv(ARCHIVE / "trades.csv")
    trades = trades.loc[trades.portfolio_executed.astype(str).str.lower().eq("true")]
    assert len(trades) == 93
    minute = pd.read_parquet(ARCHIVE / "minute_context.parquet")
    for row in trades.to_dict("records"):
        context = minute.loc[minute.day.astype(str).eq(str(row["day"])) & minute.tradingsymbol.eq(row["tradingsymbol"])]
        path = baseline.native_paths(context, row["confirmation_ts"])
        proof = assess(row, path)
        prefix = _assert_parity(row, path, proof)
        assert prefix.exit_reason == row["exit_reason"]
        assert prefix.entry_price == pytest.approx(row["entry_price"], rel=1e-12)
        assert prefix.exit_price == pytest.approx(row["exit_price"], rel=1e-12)
        assert prefix.net_return_pct == pytest.approx(row["net_return_pct"], rel=1e-12, abs=1e-12)
        # Merely omitting everything after the witnessed terminal bar produces
        # exactly the same proof and cannot change the archived outcome.
        again = assess(row, proof["path"])
        assert again["resolved"]
        assert again["evidence"]["prefix_sha256"] == proof["evidence"]["prefix_sha256"]


@pytest.mark.skipif(not ARCHIVE.exists(), reason="Local sealed G-3 archive unavailable")
def test_all_93_archived_trades_preserve_full_portfolio_output_with_prefix_adapter():
    original = pd.read_csv(ARCHIVE / "trades.csv")
    original = original.loc[original.portfolio_executed.astype(str).str.lower().eq("true")].copy()
    minute = pd.read_parquet(ARCHIVE / "minute_context.parquet")
    full, prefixes = {}, {}
    for row in original.to_dict("records"):
        context = minute.loc[minute.day.astype(str).eq(str(row["day"])) & minute.tradingsymbol.eq(row["tradingsymbol"])]
        path = baseline.native_paths(context, row["confirmation_ts"])
        full[int(row["sid"])] = path
        proof = assess(row, path)
        assert proof["resolved"]
        prefixes[int(row["sid"])] = proof["path"]
    fields = ["sid", "day", "tradingsymbol", "side", "setup_id", "picker", "confirmation_ts",
              "trigger", "price_change_pct", "volume_ratio", "traded_value"]
    orders = original[fields].reset_index(drop=True)
    expected = baseline.simulate(orders, full)
    actual = baseline.simulate(orders, prefixes, allow_observed_prefix=True)
    assert actual.portfolio_executed.sum() == 93
    # Includes timestamps, flat charges, fixed notional, trade P&L and all
    # capital/position-state fields, not merely the final net profit.
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
